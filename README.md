# hm-dianping · 本地生活点评平台

> 基于 Spring Boot + Redis 的本地生活类应用：商户浏览、探店笔记、点赞关注、优惠券秒杀。
> Redis 在这里不只是缓存——分布式锁、消息队列、地理位置、签到位图、榜单 ZSet 都跑在它上面。

**这个仓库是我的个人实践**：基础业务代码来自「黑马点评」课程，我在此基础上针对**缓存一致性、消息可靠性、并发原子性、优雅停机**做了 8 处工程化改造。每处改造都写了「原版哪里会出错 → 我怎么改 → 为什么这么改」，见 [docs/REFACTOR.md](docs/REFACTOR.md)。

我不想把课程原版的功能说成自己写的。**哪些是我写的、哪些不是，文档里分得很清。**

---

## 技术栈

| 分类 | 选型 |
|---|---|
| 语言 / 运行时 | Java 8 |
| 框架 | Spring Boot 2.x、MyBatis-Plus 3.4 |
| 存储 | MySQL 8（事实层）、Redis 7（性能层） |
| Redis 用途 | 缓存 / 分布式锁 / Stream 消息队列 / GEO / BitMap / ZSet 榜单 |
| 分布式协调 | Redisson、手写 `SETNX + Lua` 锁、Redis Stream 消费者组 |
| 构建 | Maven |

---

## 功能模块

| 模块 | 实现要点 |
|---|---|
| 登录鉴权 | 手机号 + 验证码；双拦截器（token 续期 + 登录校验）；用户态存 Redis Hash |
| 商户 | 分类分页、按距离搜索附近商户（Redis GEO）、详情缓存 |
| 探店笔记 | 发布 / 分页 / 详情 / 点赞排行（Redis ZSet） |
| 关注 | 关注、取关、共同关注（Redis Set 求交） |
| 优惠券秒杀 | Lua 原子校验 + Stream 异步下单，库存预热、时间窗、一人一单 |
| 签到 | Redis BitMap 按月存签到记录 |

---

## 架构

```
                    ┌──────────────────────────┐
   浏览器 / App  ──▶│   Spring Boot (Tomcat)   │
                    │                          │
                    │  Interceptor 链          │
                    │   ├─ RefreshTokenInterceptor  每次请求刷新 token TTL
                    │   └─ LoginInterceptor         未登录拦截
                    │                          │
                    │  Controller → Service    │
                    │  Stream 消费线程（单线程）│
                    └──────┬──────────┬────────┘
                           │          │
             ┌─────────────┘          └─────────────────┐
             ▼                                          ▼
     ┌───────────────┐                        ┌───────────────────┐
     │    MySQL      │                        │       Redis       │
     │───────────────│◀── 异步落库 ──────────│───────────────────│
     │ 商户 / 笔记   │                        │ String / Hash     │
     │ 优惠券 / 订单 │                        │ ZSet / Set / GEO  │
     │ 用户 / 关注   │                        │ BitMap            │
     └───────────────┘                        │ Stream（订单队列）│
                                              │ Lua（原子脚本）   │
                                              └───────────────────┘
```

**一句话概括取舍**：Redis 承担所有高并发读和瞬时写，MySQL 只做事实落地。秒杀链路上，用户请求完全不碰数据库。

---

## 核心改造

### 1. 秒杀时间窗下沉到 Lua

**问题**：时间窗判断在 Java 层做，判断和执行是两次独立操作；更麻烦的是多实例部署时各机器时钟不一致，同一张券会出现"这台机器说没开始、那台说可以买"。

**做法**：把 `beginTime` / `endTime` 以时间戳存 Redis，在 Lua 里用 `redis.call("time")` 取 **Redis 服务端时间**做比较。所有实例共用同一个时钟源，判断和扣减在同一个原子脚本里完成。

```lua
-- seckill.lua
local now = redis.call("time")[1]
local beginTime = redis.call("get", beginKey)
if beginTime and tonumber(beginTime) > tonumber(now) then
    return 3  -- 尚未开始
end
```

### 2. Stream 消费者：补上 pending 兜底

**问题**（原版最致命的 bug）：消费循环只读 `ReadOffset.lastConsumed()`。一条消息被 `XREADGROUP` 取走但处理失败后，会**永久停在 pending 列表**——不会再被读到，也不会重试。表现就是"用户秒杀成功、库存扣了、订单永远不生成"。

**做法**：三层兜底。

| 时机 | 行为 |
|---|---|
| 启动时 | 先 drain 一次 pending，补处理上次进程遗留的消息 |
| 固定节拍 | **每 30 秒**扫一遍 pending，与这段时间有没有新消息无关 |
| 异常时 | 不 ACK，消息留在 pending 等下一轮；业务拒绝和毒消息才 ACK |

单轮最多处理 20 条，同一条连续失败 3 次本轮放弃、等下一轮，避免一条坏消息卡死单线程消费者。

**另外一个隐患也一起修了**：Redis 长时间不可用（虚拟机挂起、网络断）时，`read` 是**立即失败**而不是阻塞 2 秒，原实现的 `catch` 里直接重试会全速空转——每 2 秒刷 6 条 ERROR 堆栈，几分钟就能把日志撑到几十 MB。现在改为指数退避 `1s→2s→4s→8s→16s→30s` 封顶，且连续失败只有第 1 次打完整堆栈，之后只打一行摘要。

**实测**：跑测时正好撞上一次真实 Redis 抖动，日志总共两行——`消费秒杀订单消息异常，1000ms 后重试` + 38 秒后的 `Redis 消费恢复正常（此前连续失败 1 次）`。另外往 pending 里塞了一条字段残缺的毒消息，应用启动时被扫出、ACK 丢弃、`XPENDING` 回到 0，只打一条日志，没有反复重试。

### 3. 逻辑过期真正接入主链路

**问题**：教程里逻辑过期的代码是独立方法，`queryById` 走的还是普通缓存——等于写了个 demo 没接进业务。而且缓存未命中时直接返回 `null`，缓存没预热就查不到数据。

**做法**：
- `queryById` 统一走逻辑过期
- key 不存在时**不当作"数据不存在"**，降级走互斥加载兜底
- 只有抢到锁的线程提交重建任务到线程池（原版是 `new Thread`），其余线程立刻返回过期旧值
- 重建前双重检查，避免重复重建

**实测**：首访线程 `io-18081-exec-3` 走互斥加载查库；逻辑过期后由 `cache-rebuild-1` 线程异步重建，**同一时刻请求线程返回旧值、耗时 21ms、未阻塞**。`TTL cache:shop:1` 返回 `-1`，只靠业务字段 `expireTime` 判过期。

### 4. 空值缓存防穿透 + 互斥加载修 bug

- DB 查不到时写入 `""` 占位（短 TTL），读取时区分"命中空值"和"key 不存在"
- 原版互斥加载**抢不到锁就递归重试**，并发下会打穿栈；而且 `try/finally` 在未持锁分支也会执行 `unlock`，等于**释放了别人的锁**。改成有限次循环 + 只有持锁才进 `finally` + 抢不到锁降级直查 DB

### 5. 事务提交后再删缓存

**问题**：`@Transactional` 方法里先更新 DB 再删缓存。如果事务随后回滚，缓存已经被清了——并发读会把**回滚前的旧数据**读出来写回缓存，脏值长期驻留。

**做法**：`TransactionSynchronizationManager.registerSynchronization` 把删缓存挂到 `afterCommit`。

### 6. 库存预热用 `SETNX`

补预热时如果用 `set`，应用重启会把**正在扣减中的库存覆盖回数据库初始值**，等于把卖出去的库存复活。库存用 `setIfAbsent`，时间窗是静态配置用 `set` 始终刷新。

### 7. 点赞合成一个 Lua

原版先 `ZSCORE` 判断再 `ZADD`/`ZREM`，两步之间不是原子的。改成一个脚本内判断 + 增删，返回值直`-1`/`1` 表示取消赞/点赞。

### 8. 消费线程优雅停机

原版消费线程是 `while(true)` + `static ExecutorService`，容器关闭后线程不受 Spring 管理、也没人通知它停，会继续读 Redis。而 `LettuceConnectionFactory` 已经 `destroy`，于是抛 `IllegalStateException: LettuceConnectionFactory was destroyed and cannot be used anymore`，`catch` 里重试又被销毁的连接炸一次，**无限刷日志、JVM 因非守护线程不退**，只能强杀进程。

改法：`volatile boolean running` + `@PreDestroy` 里 `running = false` → `shutdown()` → `awaitTermination(5s)` → 超时才 `shutdownNow()`；循环条件与 `catch` 分支都判 `running`，关闭期间的读异常静默退出而不打 ERROR。线程池从 `static` 改实例字段，生命周期跟容器走。

不设守护线程是刻意的：守护线程在 JVM 退出瞬间被硬杀，若正在扣库存/写订单，事务会被粗暴中断，留下"库存扣了、订单没有"的脏数据。未处理完的消息留在 Stream pending 列表，下次启动由 `handlePendingList()` 捞起重试。

**实测**：`ctx.close()` 后消费线程在 1.3s 内自然退出，容器关闭总耗时 **1715ms**，全程无一条 `LettuceConnectionFactory` 相关 ERROR。

---

## 秒杀流程

```
POST /voucher-order/seckill/{voucherId}
        │
        ▼
  ① Redis 执行 seckill.lua（一次调用，全程原子）
        ├─ redis.call("time") 校验时间窗   → 失败 return 3/4
        ├─ GET 库存                        → 库存不足 return 0
        ├─ SISMEMBER 用户记录              → 重复下单 return 2
        ├─ INCRBY 库存 -1
        ├─ SADD 用户记录
        └─ XADD stream.orders（id / userId / voucherId）
        │
        ▼
  ② 立即返回 orderId（用户侧秒杀结束，不等落库）
        │
        ▼
  ③ 消费者 XREADGROUP 读取 stream.orders
        ├─ 正常            → 事务落库 voucher_order，ACK
        ├─ 业务拒绝/毒消息 → ACK 丢弃
        └─ 异常            → 不 ACK，留在 pending
        │
        ▼
  ④ pending 兜底：启动 drain 一次 + 空闲时每 30s drain 一次
```

**为什么用 Stream 而不是 List**：List 的 `BRPOP` 取出即删除，消费者崩了消息就丢；Stream 有消费者组和 ACK，消息留在 pending 可以重投。

---

## 缓存策略

| 场景 | 策略 | 为什么 |
|---|---|---|
| 商户详情（热点） | 逻辑过期 + 互斥重建 | 过期瞬间不阻塞任何请求，用旧值换响应时间 |
| 普通查询 | 逻辑过期 + 互斥加载兜底 | 缓存未预热时不能直接返回 null |
| 不存在的 id | 空值缓存（`""`，短 TTL） | 防缓存穿透 |
| 商户更新 | 先更库，`afterCommit` 删缓存 | 避免事务回滚后并发读回填脏值 |
| 点赞 | Lua 原子判断 + 增删 | 消除"先查再改"的竞态 |

---

## 快速开始

**前置**：JDK 8、Maven、MySQL 8、Redis。

```bash
# 1. 建库导数据
mysql -uroot -p < src/main/resources/db/hmdp.sql

# 2. 启动（账号密码走环境变量，仓库里不存密码）
export MYSQL_PASSWORD=你的密码
export REDIS_HOST=127.0.0.1
export REDIS_PASSWORD=你的Redis密码     # 无密码可留空

mvn spring-boot:run
```

也可以直接用参数覆盖，不改环境变量：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --spring.datasource.password=xxx \
  --spring.redis.host=192.168.1.10 \
  --spring.redis.password=xxx"
```

> **端口**：默认 **8081**（`application.yml` 里 `${SERVER_PORT:8081}`，可用环境变量 `SERVER_PORT` 覆盖）。只有 8081 被占用时才需要加 `--server.port=18081` 这类参数，启动后访问 `http://localhost:8081`。

启动时 `VoucherOrderServiceImpl.initStock()` 会把 DB 里的秒杀券库存和时间窗同步进 Redis（库存用 `SETNX`，不覆盖运行中的值）。

---

## 目录结构

```
src/main/java/com/hmdp/
├── config/       拦截器、Redisson、MyBatis-Plus、全局异常
├── controller/   接口层
├── service/impl/ 业务实现（缓存、秒杀、锁的核心逻辑都在这）
├── entity/       数据库实体
├── dto/          传输对象
└── utils/        缓存客户端、分布式锁、ID 生成、Redis 常量

src/main/resources/
├── seckill.lua   秒杀原子脚本（时间窗 + 库存 + 一人一单 + 投递消息）
├── blog-like.lua 点赞/取消赞原子脚本
├── unlock.lua    分布式锁安全释放（比对 value 再删）
└── db/hmdp.sql   建表与初始数据
```

---

## 已知限制

写出来是为了不硬撑——被问到直接说结论，比现编强。

1. **Stream 没有消费幂等表**：靠 `voucher_order` 表上的唯一索引（用户 + 券）兜底，重复消息会被索引挡掉，但没有统一的消息去重。
2. **逻辑过期没有全量预热任务**：只在启动时按需加载，冷启动后第一批请求会走互斥加载。
3. **没有压测数据**：代码里所有"更快""提升"都没有量化依据，不做数字承诺。
4. **两处锁机制不统一**：秒杀下单锁用 Redisson `RLock`（不传 leaseTime，走看门狗续期）；缓存重建锁仍是手写 `SETNX + Lua 解锁`，没有续期，靠重建耗时短规避风险。
5. **单机部署验证**：多实例下的时钟漂移、缓存重建竞争只是设计上规避了，没有实测过。

---

## 相关文档

- [docs/REFACTOR.md](docs/REFACTOR.md) — 逐条改造记录：原版问题 → 我的做法 → 面试怎么讲
