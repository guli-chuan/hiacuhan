# hm-dianping（本地生活点评）

基于 Spring Boot + Redis 的本地生活类应用，实现了商户浏览、探店笔记、点赞关注、优惠券秒杀等核心功能。

本项目源自「黑马点评」课程代码，我在其基础上针对**缓存一致性、消息可靠性、并发原子性**做了工程化改造，改造清单与原因见 [docs/REFACTOR.md](docs/REFACTOR.md)。

---

## 技术栈

| 层 | 技术 |
|---|---|
| 语言 / 运行时 | Java 8 |
| 框架 | Spring Boot 2.x、MyBatis-Plus |
| 存储 | MySQL 8（业务数据）、Redis 7（缓存 / 分布式锁 / 消息队列 / GEO / BitMap） |
| 分布式协调 | Redisson、手写 `SETNX` 分布式锁、Redis Stream 消费者组 |
| 构建 | Maven |

---

## 功能模块

| 模块 | 说明 |
|---|---|
| 登录鉴权 | 手机号 + 短信验证码登录，双拦截器（token 刷新 + 登录校验），Redis Hash 存用户态 |
| 商户 | 类型分页、按距离搜索附近商户（Redis GEO）、详情缓存 |
| 探店笔记 | 发布 / 分页 / 详情 / 点赞排行（Redis ZSet） |
| 关注 | 关注 / 取关 / 共同关注（Redis Set 求交） |
| 优惠券秒杀 | Redis Lua 原子校验 + Stream 异步下单，库存预热、时间窗、一人一单 |
| 签到 | Redis BitMap 按月存储签到记录 |

---

## 架构

```
                    ┌─────────────────────────┐
   浏览器 / App  ──▶│  Spring Boot (Tomcat)    │
                    │  ├ Interceptor 链        │
                    │  │   ├ RefreshToken      │  ← 每次请求刷新 token TTL
                    │  │   └ LoginInterceptor  │  ← 未登录拦截
                    │  ├ Controller / Service  │
                    │  └ Stream 消费者线程      │
                    └───────┬─────────┬────────┘
                            │         │
              ┌─────────────┘         └──────────────┐
              ▼                                      ▼
      ┌───────────────┐                     ┌──────────────────┐
      │    MySQL      │                     │      Redis       │
      │ 业务数据      │◀─── 异步落库 ───────│  String/Hash/Set │
      │ 订单/商户/券  │                     │  ZSet/BitMap/GEO │
      └───────────────┘                     │  Stream(订单队列) │
                                            │  Lua(原子脚本)    │
                                            └──────────────────┘
```

**关键设计**：Redis 是性能层，MySQL 是事实层。秒杀链路先写 Redis（原子扣减 + 投递消息），再由单线程消费者异步落库，把瞬时写压力从 DB 上移走。

---

## 秒杀流程

```
POST /voucher-order/seckill/{voucherId}
        │
        ▼
  ① Redis 执行 seckill.lua（单次脚本调用，全程原子）
        ├─ redis.call("time") 取服务端时间 → 校验时间窗   → 失败 return 3/4
        ├─ GET 库存            → 库存不足                 → return 0
        ├─ SISMEMBER 用户记录  → 重复下单                 → return 2
        ├─ INCRBY 库存 -1
        ├─ SADD 用户记录
        └─ XADD stream.orders（订单 id / userId / voucherId）
        │
        ▼
  ② 立即返回 orderId（用户侧秒杀结束，不等落库）
        │
        ▼
  ③ 消费者线程 XREADGROUP 读 stream.orders
        ├─ 正常 → 事务落库 voucher_order，ACK
        ├─ 业务拒绝 / 毒消息 → ACK 丢弃（不再重试）
        └─ 异常 → 不 ACK，留在 pending
        │
        ▼
  ④ pending 兜底：启动时 drain 一次 + 空闲时每 30s drain 一次
        （原版只读新消息，处理失败的消息会永久滞留 pending）
```

**为什么用 Stream 而不是 List**：List 的 `BRPOP` 取出即删除，消费者崩溃消息就丢了；Stream 有消费者组和 ACK 机制，消息会留在 pending 列表，可以重投。

---

## 缓存策略

| 场景 | 策略 | 为什么 |
|---|---|---|
| 商户详情（热点） | 逻辑过期 + 互斥重建 | 热点 key 过期瞬间不阻塞任何请求，用旧值换响应时间 |
| 商户查询（普通） | 逻辑过期 + 互斥加载兜底 | 缓存未预热时不能直接返回 null |
| 不存在的 id | 空值缓存（`""`，短 TTL） | 防缓存穿透 |
| 商户更新 | 先更库，`afterCommit` 删缓存 | 避免事务回滚后并发读回填脏值 |
| 点赞 | Lua 原子判断 + 增删 | 消除"先查再改"的竞态 |

---

## 快速启动

前置：JDK 8、Maven、MySQL、Redis。

```bash
# 1. 建库导入
mysql -uroot -p < src/main/resources/db/hmdp.sql

# 2. 改配置
#    src/main/resources/application.yml
#    → spring.datasource.* / spring.redis.*

# 3. 启动（本机 8081 被占用时用 18081）
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=18081
```

启动时 `VoucherOrderServiceImpl.initStock()` 会把 DB 里的秒杀券库存和时间窗同步进 Redis（库存用 `SETNX`，不覆盖运行中的值）。

---

## 目录结构

```
src/main/java/com/hmdp/
├── config/       # 拦截器、Redisson、MyBatis-Plus、全局异常
├── controller/   # 接口层
├── service/impl/ # 业务实现（缓存、秒杀、锁的核心逻辑都在这里）
├── entity/       # 数据库实体
├── dto/          # 传输对象
└── utils/        # 缓存客户端、分布式锁、ID 生成、Redis 常量

src/main/resources/
├── seckill.lua   # 秒杀原子脚本（时间窗 + 库存 + 一人一单 + 投递消息）
├── blog-like.lua # 点赞/取消赞原子脚本
└── unlock.lua    # 分布式锁安全释放（比对 value 再删）
```

---

## 改造记录

见 [docs/REFACTOR.md](docs/REFACTOR.md)：每一条都写明「原版问题 → 我的做法 → 可以怎么讲」。
