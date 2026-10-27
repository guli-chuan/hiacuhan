# 改造记录：从教程代码到可运行的工程实现

> 说明：本项目基于「黑马点评」课程代码。仓库里的基础功能（双拦截器登录、店铺/博客 CRUD、签到 BitMap、GEO 附近店铺、Feed 滚动分页）**均为课程原版**，不是我写的。
> 本文只记录**我实际改动过的部分**，以及改动的原因。改动的判断依据只有一条：教程写法在真实并发/多实例/重启场景下会出错。

---

## 1. 秒杀时间窗判断下沉到 Lua

| | |
|---|---|
| **位置** | `src/main/resources/seckill.lua` |
| **原版问题** | 时间窗在 Java 层用 `LocalDateTime.now()` 比较，再决定要不要执行 Lua。判断和执行是两次独立操作，中间隔着网络往返；更麻烦的是**多实例部署时各机器时钟不一定一致**，A 机器认为"还没开始"、B 机器认为"已经可以买"，同一张券出现不一致的判定。 |
| **我的做法** | 把 `beginTime`/`endTime` 以时间戳形式存进 Redis（key：`seckill:begin:{voucherId}` / `seckill:end:{voucherId}`），在 Lua 里用 `redis.call("time")` 取 **Redis 服务端时间**做比较。所有实例共用同一个时钟源，时间判断和库存扣减在同一段原子脚本内完成。 |
| **附带收益** | 时间窗配置（`beginTime`/`endTime`）在 `@PostConstruct` 的 `initStock()` 里从 DB 同步到 Redis，改券的时间不需要重启或改代码。 |
| **可以怎么讲** | "分布式环境下不要用单机时间做业务判断。Redis 是所有这些实例的公共依赖，它自己的 `TIME` 命令是唯一全局一致的时钟，所以我把它当唯一时间源。" |

---

## 2. Stream 消费者：从「只读新消息」到「pending 兜底」

| | |
|---|---|
| **位置** | `VoucherOrderServiceImpl.VoucherOrderHandler` |
| **原版问题** | 消费循环只从 `ReadOffset.lastConsumed()` 读新消息。一条消息被 `XREADGROUP` 取走但处理失败（进程被杀、抛异常、锁超时）后，它会永久停在 pending 列表里——**不会再被读到，也不会被重试**。表现就是"用户秒杀成功、库存已经扣了、订单永远不生成"。这是原版最致命的 bug。 |
| **我的做法** | 三层兜底：<br>① **启动时先 drain 一次 pending**：`handlePendingList()` 在进入主循环前执行，把上次进程遗留的消息补处理；<br>② **空闲时周期性 drain**：主循环 `block(2s)` 读不到新消息，且距上次 drain 超过 30s，就再扫一遍 pending；<br>③ **异常不 ACK**：`processOrder` 抛异常时不 ACK，消息留在 pending 等下一轮；业务拒绝（重复下单、库存不足）和毒消息（字段缺失）才 ACK，避免无效消息反复报错。 |
| **额外细节** | pending 里同一条消息连续失败 5 次后暂停本轮重试，等下一轮，防止一条坏消息把单线程消费者卡死。 |
| **可以怎么讲** | "Stream 是 at-least-once 语义，不是 at-most-once。只要 ACK 之前进程挂了，消息一定还在 pending 里。所以消费者必须自己保证 pending 会被重新消费，光靠 `>` 读新消息等于主动丢数据。" |

---

## 3. 库存预热：`set` 改为 `setIfAbsent`

| | |
|---|---|
| **位置** | `VoucherOrderServiceImpl.initStock()` |
| **原版问题** | 没有预热逻辑，库存 key 在第一次秒杀时才写入。补预热的时候如果用 `set`，应用重启会把**正在被扣减中的库存值覆盖回数据库里的初始值**——已经卖出去的库存被"复活"，超卖。 |
| **我的做法** | 库存用 `setIfAbsent`（`SETNX` 语义），key 已存在就绝不覆盖；时间窗是静态配置，用 `set` 始终刷新。 |
| **可以怎么讲** | "预热和运行时写同一个 key，就必须区分'初始化'和'覆盖'。" |

---

## 4. 逻辑过期真正接入主链路

| | |
|---|---|
| **位置** | `ShopServiceImpl.queryById` → `queryWithLogicalExpire` |
| **原版问题** | 教程里逻辑过期的代码是独立方法，`queryById` 走的还是普通缓存或互斥缓存，等于**写了个 demo 没接进业务**。另外原版缓存未命中时直接返回 `null`，缓存没预热就查不到数据。 |
| **我的做法** | ① `queryById` 统一走逻辑过期；② 缓存 key 不存在时**不当成"数据不存在"**，而是降级走互斥加载兜底（否则逻辑过期方案先天要求全量预热，新数据上线即查不到）；③ 只有抢到锁的线程把重建任务提交到线程池（原版 `new Thread`），其余线程立即返回过期旧值；④ 重建前做双重检查，避免多线程重复重建。 |
| **实测证据** | 首访线程 `io-18081-exec-3` 走互斥加载查库；逻辑过期后由 `cache-rebuild-1` 线程异步重建，同一时刻请求线程返回旧值、耗时 21ms 未阻塞。`TTL cache:shop:1` 返回 `-1`（永不过期，只靠业务字段 `expireTime` 判过期）。 |
| **可以怎么讲** | "逻辑过期的本质是拿'数据可能旧一点'换'请求永不阻塞'。所以 key 必须常驻，过期判断只能放在值里；而且必须接受一个后果——缓存不存在时是没有旧值可返回的，这时候必须有一个兜底加载路径。" |

---

## 5. 缓存穿透：空值缓存

| | |
|---|---|
| **位置** | `ShopServiceImpl.loadShopWithMutex()` |
| **原版问题** | 无防护。查一个不存在的 id，每次都打到 DB。 |
| **我的做法** | DB 查不到时写入 `""` 占位（不是 null，序列化后能区分"没有这个 key"和"这个 key 的值是空"），TTL 短（`CACHE_NULL_TTL`）。读取时先判断"命中空值"还是"key 不存在"，前者直接返回 null，后者才走加载。 |
| **可以怎么讲** | "空值缓存和布隆过滤器的取舍：空值缓存只会挡住已经查过的 key，攻击者换随机 id 就没用了；布隆过滤器挡的是所有不存在的 key，但会误判、且不能删除。前者成本低，后者效果强。" |

---

## 6. 互斥加载：递归改循环

| | |
|---|---|
| **位置** | `ShopServiceImpl.loadShopWithMutex()` |
| **原版问题** | 原实现抢不到锁就**递归调用自己重试**。高并发下线程数 × 重试深度会把栈打穿；而且 `try/finally` 在没拿到锁的分支里也会执行 `unlock`，等于**释放了别人的锁**。 |
| **我的做法** | 改为有限次循环（3 次）+ 每次失败后短暂 sleep；只有抢到锁的分支才进 `try/finally`；重试耗尽仍抢不到锁时降级直接查库，宁可慢也不报错。 |

---

## 7. 事务与缓存的顺序：`afterCommit` 删缓存

| | |
|---|---|
| **位置** | `ShopServiceImpl.update()` |
| **原版问题** | `@Transactional` 方法里先 `updateById` 再 `delete(cacheKey)`。如果事务随后回滚，缓存已经被删了——此时并发读会把**回滚前的旧数据**读出来重新写回缓存，脏值长期驻留。 |
| **我的做法** | 用 `TransactionSynchronizationManager.registerSynchronization` 把删缓存挂到 `afterCommit`，只在事务真正提交后执行；没有事务上下文时直接删。 |
| **可以怎么讲** | "先更库再删缓存这个顺序解决的是'并发读回填旧值'，但解决不了'事务回滚'。回滚意味着这次更新根本不存在，缓存却已经被清了。所以要等提交成功这个信号。" |

---

## 8. 点赞：Java 层两次操作合成一个 Lua

| | |
|---|---|
| **位置** | `src/main/resources/blog-like.lua` + `BlogServiceImpl` |
| **原版问题** | 先 `ZSCORE` 判断是否点过，再 `ZADD`/`ZREM`。两步之间不是原子的，同一用户并发点赞会算出错误的点赞数。 |
| **我的做法** | 一个 Lua 脚本里判断 + 增删：已存在则 `ZREM` 返回 `-1`（取消赞），不存在则 `ZADD` 返回 `1`（点赞）。返回值直接就是本次操作类型。 |

---

## 9. 清理与规范化

- 删除 `dto/RedisData.java`（与 `utils/RedisData.java` 重复，全项目零引用）
- 删除 `dto/PasswordFormDto.java`、`utils/PasswordEncoder.java`（密码登录是未完成的半成品，实际登录只有手机号+验证码）
- 修正 `UserController.login` 的注释，不再声称支持密码登录

---

## 10. 消费线程优雅停机

| 项 | 内容 |
| --- | --- |
| **原版问题** | 消费线程是 `while(true)` 包在 `static ExecutorService` 里，既不随容器关闭，也拿不到停止信号。点停止后 Spring 正常销毁 `LettuceConnectionFactory`，但线程还活着继续 `read`，于是抛 `IllegalStateException: LettuceConnectionFactory was destroyed and cannot be used anymore`；`catch` 里又调 `handlePendingList()` 重试，再炸一次，连续 5 次后回到 `while(true)` 继续炸。结果是**日志无限刷 + JVM 因非守护线程不退出**，只能强杀进程。 |
| **我的做法** | ① `static` 线程池改实例字段，生命周期跟容器走，线程显式命名 `seckill-consumer-1`；② 加 `volatile boolean running`，`while (running)` 替代 `while(true)`；③ `@PreDestroy` 中 `running = false` → `shutdown()` → `awaitTermination(5s)` → 超时才 `shutdownNow()`；④ `catch` 分支先判 `running`，关闭期间的读异常记 INFO 直接退出，不再打 ERROR；⑤ `handlePendingList()` 的循环同样受 `running` 控制。 |
| **为什么不用守护线程** | 设成 daemon 能让 JVM 立刻退出、日志也"干净"，但线程是在任意位置被硬杀的：若此刻正在扣库存/写订单，事务被粗暴中断，会留下"库存扣了、订单没落库"的脏数据。显式停机才能保证在途消息处理完再走。 |
| **时序为什么安全** | `@PreDestroy` 早于依赖 bean 的销毁执行，所以停止过程里读 Redis 仍然合法；等 `destroy()` 返回后 `LettuceConnectionFactory` 才会被销毁。 |
| **消息不丢** | 5s 内没处理完的消息留在 Stream 的 pending 列表，下次启动由 `handlePendingList()` 捞起重试——这正是第 2 节那套 pending 兜底机制的价值。 |
| **实测证据** | `ctx.close()` 后：主线程 18.906s 置标志 → 消费线程 20.185s 自然退出（阻塞读 `block(2s)` 走完即退）→ 20.186s 确认优雅退出，**容器关闭总耗时 1715ms**，全程无任何 `LettuceConnectionFactory` 相关 ERROR，`seckill-consumer-1` 状态为终止。 |

---

## 已知限制（面试被问到可以直接说，不要硬圆）

1. **Stream 消息仍可能重复消费**：没有做消费幂等表，靠 `voucher_order` 表上的唯一索引（用户+券）兜底。
2. **逻辑过期没有做全量预热任务**：只在应用启动时按需加载，冷启动后第一批请求会走互斥加载。
3. **没有压测数据**：所有"提升""更快"的说法在这份代码里都没有量化依据。
4. **单机部署**：多实例下的时钟漂移、缓存重建竞争只是设计上规避了，没有实测过。
5. **两处锁机制不统一**：秒杀下单锁用 Redisson `RLock`（`tryLock(10s)` 不传 leaseTime，走看门狗自动续期）；缓存重建锁仍是手写 `SETNX + Lua 解锁`，**没有续期**，靠重建耗时短（毫秒级）来规避锁提前释放的风险。真要统一，应该把重建锁也换成 Redisson。
