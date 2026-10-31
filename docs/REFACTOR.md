# 改造记录：从课程代码到工程实现

> 说明：本项目基础业务代码来自「黑马点评」课程。双拦截器登录、店铺/博客 CRUD、签到 BitMap、GEO 附近店铺、Feed 滚动分页等功能均为课程原版，本文不涉及。
> 本文只记录实际改动过的部分及改动原因。判断依据只有一条：课程写法在真实并发、多实例、重启场景下会出错。

---

## 1. 秒杀时间窗判断下沉到 Lua

| | |
|---|---|
| **位置** | `src/main/resources/seckill.lua` |
| **原实现问题** | 时间窗在 Java 层用 `LocalDateTime.now()` 比较，再决定是否执行 Lua。判断与执行是两次独立操作，中间隔一次网络往返；更关键的是**多实例部署时各机器时钟不一定一致**，A 机器认为"还没开始"、B 机器认为"已经可以买"，同一张券出现不一致的判定。 |
| **改动方案** | 把 `beginTime`/`endTime` 以时间戳形式存进 Redis（key：`seckill:begin:{voucherId}` / `seckill:end:{voucherId}`），在 Lua 里用 `redis.call("time")` 取 **Redis 服务端时间**做比较。所有实例共用同一个时钟源，时间判断和库存扣减在同一段原子脚本内完成。 |
| **附带收益** | 时间窗配置（`beginTime`/`endTime`）在 `@PostConstruct` 的 `initStock()` 里从 DB 同步到 Redis，改券的时间不需要重启或改代码。 |
| **要点** | 分布式环境下不能用单机时间做业务判断。Redis 是所有实例的公共依赖，其 `TIME` 命令是全局一致的时钟源。 |

---

## 2. Stream 消费者：从「只读新消息」到「pending 兜底」

| | |
|---|---|
| **位置** | `VoucherOrderServiceImpl.VoucherOrderHandler` |
| **原实现问题** | 消费循环只从 `ReadOffset.lastConsumed()` 读新消息。一条消息被 `XREADGROUP` 取走但处理失败（进程被杀、抛异常、锁超时）后，会永久停在 pending 列表里，**不会再被读到，也不会被重试**。表现为"用户秒杀成功、库存已扣、订单永远不生成"。 |
| **改动方案** | 三层兜底：<br>① **启动时先 drain 一次 pending**：`drainPendingList()` 在进入主循环前执行，把上次进程遗留的消息补处理；<br>② **固定节拍 drain**：每 30 秒扫一遍 pending，与这段时间有没有新消息无关；<br>③ **异常不 ACK**：`processOrder` 抛异常时不 ACK，消息留在 pending 等下一轮；业务拒绝（重复下单、库存不足）和毒消息（字段缺失）才 ACK，避免无效消息反复报错。 |
| **额外细节** | 单轮最多处理 20 条；同一条连续失败 3 次本轮放弃、等下一轮，防止一条坏消息把单线程消费者卡死。兜底清理刻意**与主消费共用同一个线程**，不另起定时线程——否则主线程正在处理、尚未 ACK 的消息会被兜底线程再读一次，造成重复消费。 |
| **顺带处理的隐患** | Redis 长时间不可用（虚拟机挂起、网络断开、连接被拒）时，`read` 会**立即失败**而不是阻塞 2 秒。原实现在 `catch` 里直接重试，外层循环没有任何等待，形成全速空转：每轮输出 1 条 ERROR 堆栈，进入 pending 再输出 5 条，**大约每 2 秒 6 条堆栈**，数分钟即可把日志撑到几十 MB，CPU 也被打满。改为指数退避 `1s→2s→4s→8s→16s→30s` 封顶；连续失败只有第 1 次输出完整堆栈，之后只输出一行 `e.toString()` 摘要，恢复时补一条 INFO 说明连续失败次数。 |
| **验证** | ① 测试期间遇到一次 Redis 连接中断：只有两行日志——`消费秒杀订单消息异常，1000ms 后重试`（12:26:53）+ `Redis 消费恢复正常（此前连续失败 1 次）`（12:27:31），全程无刷屏；② 用 `XADD` 写入一条字段残缺的消息并 `XREADGROUP` 取出不 ACK，使其进入 pending（`XPENDING` 显示 1 条），随后启动应用：启动兜底将其扫出、识别为字段缺失、ACK 丢弃，`XPENDING` 回到 0，全程只输出 1 条日志。 |
| **要点** | Stream 是 at-least-once 语义而非 at-most-once。只要 ACK 之前进程退出，消息一定还在 pending 里，因此消费者必须自己保证 pending 会被重新消费；只读 `>` 新消息等于主动丢数据。 |

---

## 3. 库存预热：`set` 改为 `setIfAbsent`

| | |
|---|---|
| **位置** | `VoucherOrderServiceImpl.initStock()` |
| **原实现问题** | 没有预热逻辑，库存 key 在第一次秒杀时才写入。补预热时如果用 `set`，应用重启会把**正在被扣减中的库存值覆盖回数据库里的初始值**，已经卖出去的库存被"复活"，造成超卖。 |
| **改动方案** | 库存用 `setIfAbsent`（`SETNX` 语义），key 已存在就绝不覆盖；时间窗是静态配置，用 `set` 始终刷新。 |
| **要点** | 预热与运行时写同一个 key，必须区分"初始化"与"覆盖"两种语义。 |

---

## 4. 逻辑过期接入主链路

| | |
|---|---|
| **位置** | `ShopServiceImpl.queryById` → `queryWithLogicalExpire` |
| **原实现问题** | 课程代码中逻辑过期是独立方法，`queryById` 走的还是普通缓存或互斥缓存，方案实现与业务主链路割裂。另外原实现缓存未命中时直接返回 `null`，缓存没预热就查不到数据。 |
| **改动方案** | ① `queryById` 统一走逻辑过期；② 缓存 key 不存在时不视为"数据不存在"，降级走互斥加载兜底（否则逻辑过期方案先天要求全量预热，新数据上线即查不到）；③ 只有抢到锁的线程把重建任务提交到线程池（原实现 `new Thread`），其余线程立刻返回过期旧值；④ 重建前做双重检查，避免多线程重复重建。 |
| **验证** | 首访线程 `io-18081-exec-3` 走互斥加载查库；逻辑过期后由 `cache-rebuild-1` 线程异步重建，同一时刻请求线程返回旧值、耗时 21ms 未阻塞。`TTL cache:shop:1` 返回 `-1`（永不过期，只靠业务字段 `expireTime` 判过期）。 |
| **要点** | 逻辑过期用"数据可能旧一点"换"请求永不阻塞"。代价是 key 必须常驻、过期判断只能放在值里；并且缓存不存在时没有旧值可返回，必须额外准备一条兜底加载路径。 |

---

## 5. 缓存穿透：空值缓存

| | |
|---|---|
| **位置** | `ShopServiceImpl.loadShopWithMutex()` |
| **原实现问题** | 无防护。查一个不存在的 id，每次都打到 DB。 |
| **改动方案** | DB 查不到时写入 `""` 占位（不是 null，序列化后能区分"没有这个 key"和"这个 key 的值是空"），TTL 较短（`CACHE_NULL_TTL`）。读取时先判断"命中空值"还是"key 不存在"，前者直接返回 null，后者才走加载。 |
| **要点** | 空值缓存只挡住已查过的 key，换成随机 id 即失效；布隆过滤器挡住所有不存在的 key，但存在误判且不可删除。前者成本低，后者效果强。 |

---

## 6. 互斥加载：递归改循环

| | |
|---|---|
| **位置** | `ShopServiceImpl.loadShopWithMutex()` |
| **原实现问题** | 原实现抢不到锁就**递归调用自己重试**。高并发下线程数 × 重试深度会把栈打穿；而且 `try/finally` 在没拿到锁的分支里也会执行 `unlock`，等于**释放了别人的锁**。 |
| **改动方案** | 改为有限次循环（3 次）+ 每次失败后短暂 sleep；只有抢到锁的分支才进 `try/finally`；重试耗尽仍抢不到锁时降级直接查库，宁可慢也不报错。 |

---

## 7. 事务与缓存的顺序：`afterCommit` 删缓存

| | |
|---|---|
| **位置** | `ShopServiceImpl.update()` |
| **原实现问题** | `@Transactional` 方法里先 `updateById` 再 `delete(cacheKey)`。如果事务随后回滚，缓存已经被删——此时并发读会把**回滚前的旧数据**读出来重新写回缓存，脏值长期驻留。 |
| **改动方案** | 用 `TransactionSynchronizationManager.registerSynchronization` 把删缓存挂到 `afterCommit`，只在事务真正提交后执行；没有事务上下文时直接删。 |
| **要点** | 先更库再删缓存解决的是"并发读回填旧值"，解决不了"事务回滚"——回滚意味着这次更新根本不存在，缓存却已经被清。所以要等提交成功这个信号。 |

---

## 8. 点赞：Java 层两次操作合成一个 Lua

| | |
|---|---|
| **位置** | `src/main/resources/blog-like.lua` + `BlogServiceImpl` |
| **原实现问题** | 先 `ZSCORE` 判断是否点过，再 `ZADD`/`ZREM`。两步之间不是原子的，同一用户并发点赞会算出错误的点赞数。 |
| **改动方案** | 一个 Lua 脚本里判断 + 增删：已存在则 `ZREM` 返回 `-1`（取消赞），不存在则 `ZADD` 返回 `1`（点赞）。返回值即本次操作类型。 |

---

## 9. 清理与规范化

- 删除 `dto/RedisData.java`（与 `utils/RedisData.java` 重复，全项目零引用）
- 删除 `dto/PasswordFormDto.java`、`utils/PasswordEncoder.java`（密码登录是未完成的半成品，实际登录只有手机号+验证码）
- 修正 `UserController.login` 的注释，不再声称支持密码登录

---

## 10. 消费线程优雅停机

| 项 | 内容 |
| --- | --- |
| **原实现问题** | 消费线程是 `while(true)` 包在 `static ExecutorService` 里，既不随容器关闭，也拿不到停止信号。点停止后 Spring 正常销毁 `LettuceConnectionFactory`，但线程还活着继续 `read`，于是抛 `IllegalStateException: LettuceConnectionFactory was destroyed and cannot be used anymore`；`catch` 里又调 `handlePendingList()` 重试，再触发一次；连续 5 次后回到 `while(true)` 继续。结果是**日志无限刷 + JVM 因非守护线程不退出**，只能强杀进程。 |
| **改动方案** | ① `static` 线程池改实例字段，生命周期跟容器走，线程显式命名 `seckill-consumer-1`；② 加 `volatile boolean running`，`while (running)` 替代 `while(true)`；③ `@PreDestroy` 中 `running = false` → `shutdown()` → `awaitTermination(5s)` → 超时才 `shutdownNow()`；④ `catch` 分支先判 `running`，关闭期间的读异常记 INFO 直接退出，不再打 ERROR；⑤ `handlePendingList()` 的循环同样受 `running` 控制。 |
| **不使用守护线程的原因** | 设成 daemon 能让 JVM 立刻退出、日志也干净，但线程是在任意位置被强制终止的：若此刻正在扣库存或写订单，事务被中断，会留下"库存扣了、订单没落库"的脏数据。显式停机才能保证在途消息处理完再走。 |
| **时序安全性** | `@PreDestroy` 早于依赖 bean 的销毁执行，所以停止过程中读 Redis 仍然合法；`destroy()` 返回后 `LettuceConnectionFactory` 才会被销毁。 |
| **消息不丢** | 5s 内没处理完的消息留在 Stream 的 pending 列表，下次启动由 `handlePendingList()` 捞起重试，即第 2 节的 pending 兜底机制。 |
| **验证** | `ctx.close()` 后：主线程 18.906s 置标志 → 消费线程 20.185s 自然退出（阻塞读 `block(2s)` 走完即退）→ 20.186s 确认优雅退出，**容器关闭总耗时 1715ms**，全程无任何 `LettuceConnectionFactory` 相关 ERROR，`seckill-consumer-1` 状态为终止。 |

---

## 已知限制

1. **Stream 消息仍可能重复消费**：没有做消费幂等表，靠 `voucher_order` 表上的唯一索引（用户+券）兜底。
2. **逻辑过期没有做全量预热任务**：只在应用启动时按需加载，冷启动后第一批请求会走互斥加载。
3. **没有压测数据**：代码中"提升""更快"的说法均无量化依据。
4. **单机部署**：多实例下的时钟漂移、缓存重建竞争只是设计上规避了，没有实测。
5. **两处锁机制不统一**：秒杀下单锁用 Redisson `RLock`（`tryLock(10s)` 不传 leaseTime，走看门狗自动续期）；缓存重建锁仍是手写 `SETNX + Lua` 解锁，**没有续期**，靠重建耗时短（毫秒级）规避锁提前释放的风险。统一的话应把重建锁也换成 Redisson。
