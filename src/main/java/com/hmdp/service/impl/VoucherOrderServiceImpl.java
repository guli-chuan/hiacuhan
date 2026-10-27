package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWork;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWork redisIdWork;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    @Lazy
    private IVoucherOrderService proxy;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * 单线程消费者，保证同一时刻只有一个线程在处理订单，天然串行。
     * <p>
     * 刻意用实例字段而非 static：线程池的生命周期必须跟随 Spring 容器一起结束（见 {@link #destroy()}）。
     * static 字段不受容器管理，容器销毁后线程仍存活，会去访问已被 destroy 的 Redis 连接工厂，
     * 报 "LettuceConnectionFactory was destroyed and cannot be used anymore"。
     * <p>
     * 也刻意不设成守护线程：守护线程在 JVM 退出瞬间被直接杀掉，
     * 若此刻正在扣库存/写订单，事务会被粗暴中断，已扣的库存没有对应订单。
     */
    private final ExecutorService seckillExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "seckill-consumer-1");
        t.setDaemon(false);
        return t;
    });

    /** 消费者运行标志。容器关闭时置 false，让消费循环尽快自然退出，而不是被强杀 */
    private volatile boolean running = true;

    @PostConstruct
    private void init() {
        // 创建消费者组(组已存在或stream不存在时会抛异常,忽略即可)
        try {
            stringRedisTemplate.opsForStream().createGroup(STREAM_KEY, STREAM_GROUP);
        } catch (Exception e) {
            log.info("消费者组可能已存在,跳过创建: {}", e.getMessage());
        }
        seckillExecutor.submit(new VoucherOrderHandler());
    }

    /**
     * 容器关闭时的优雅停机。
     * <p>
     * 时序保证：@PreDestroy 早于依赖 bean 的销毁，所以停止过程中读 Redis 仍然合法；
     * 等本方法返回后，LettuceConnectionFactory 才会被 destroy。
     * <p>
     * 步骤：置标志让循环自然退出 → shutdown 不再接新任务 → 等在途消息处理完（最多 5s）
     * → 超时才 shutdownNow 兜底。未处理完的消息留在 Stream 的 pending 列表，
     * 下次启动会被 {@code drainPendingList()} 捞起来重试，不会丢。
     */
    @PreDestroy
    public void destroy() {
        log.info("秒杀消费者准备关闭，停止接收新消息...");
        running = false;
        seckillExecutor.shutdown();
        try {
            if (seckillExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.info("秒杀消费者已优雅退出");
            } else {
                log.warn("秒杀消费者5秒内未退出，强制中断；在途消息留在pending列表，下次启动重试");
                seckillExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            seckillExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 应用启动时，把数据库中所有秒杀券的库存、秒杀时间窗口同步到Redis。
     * 兼容之前直接从数据库添加、未写入Redis的秒杀券（key不存在时会自动创建）。
     * 已存在的库存key不覆盖，避免覆盖正在扣减中的库存；时间窗口是静态配置，始终刷新。
     */
    @PostConstruct
    public void initStock() {
        List<SeckillVoucher> list = seckillVoucherService.list();
        for (SeckillVoucher sv : list) {
            stringRedisTemplate.opsForValue()
                    .setIfAbsent(SECKILL_STOCK_KEY + sv.getVoucherId(), sv.getStock().toString());
            syncTimeWindow(sv);
        }
    }

    private void syncTimeWindow(SeckillVoucher sv) {
        Long voucherId = sv.getVoucherId();
        if (sv.getBeginTime() != null) {
            stringRedisTemplate.opsForValue().set(SECKILL_BEGIN_KEY + voucherId,
                    String.valueOf(sv.getBeginTime().atZone(ZoneId.systemDefault()).toEpochSecond()));
        }
        if (sv.getEndTime() != null) {
            stringRedisTemplate.opsForValue().set(SECKILL_END_KEY + voucherId,
                    String.valueOf(sv.getEndTime().atZone(ZoneId.systemDefault()).toEpochSecond()));
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 生成订单ID
        long orderId = redisIdWork.nextId("order");
        // 1、执行lua脚本：原子地校验时间窗/库存/重复下单，扣减redis库存、记录用户、投递订单消息
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId));
        int r = result == null ? -1 : result.intValue();
        switch (r) {
            case 0:
                return Result.fail("库存不足");
            case 2:
                return Result.fail("不能重复下单");
            case 3:
                return Result.fail("秒杀尚未开始");
            case 4:
                return Result.fail("秒杀已结束");
            default:
                // 后台handler会通过事务代理将订单落库
                return Result.ok(orderId);
        }
    }

    /**
     * 异步消费者：从stream.orders消费订单消息并落库。
     * 成功或业务拒绝（重复下单/库存不足）都ACK；真正的异常不ACK，消息留在pending列表等待重试。
     */
    private class VoucherOrderHandler implements Runnable {
        /** pending 兜底清理的固定间隔：每 30 秒检查一次 */
        private static final long PENDING_DRAIN_INTERVAL_MS = 30_000L;
        /** 单轮 pending 最多处理条数，积压严重时留机会给主消费 */
        private static final int PENDING_MAX_PER_ROUND = 20;
        /** Redis 不可用时的退避上限 */
        private static final long MAX_BACKOFF_MS = 30_000L;

        private long lastPendingDrainTime = 0L;
        /** 连续消费失败次数，用于指数退避与日志降噪 */
        private int consecutiveFailures = 0;

        @Override
        public void run() {
            // 启动时先处理历史遗留的pending消息（修复旧bug留下的"redis已生效但未落库"卡单）
            log.info("秒杀消费者启动，先处理队列中的历史pending消息...");
            drainPendingSafely();
            lastPendingDrainTime = System.currentTimeMillis();

            while (running) {
                try {
                    List<MapRecord<String, Object, Object>> orderList = stringRedisTemplate.opsForStream().read(
                            Consumer.from(STREAM_GROUP, STREAM_CONSUMER),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()));
                    if (consecutiveFailures > 0) {
                        log.info("Redis 消费恢复正常（此前连续失败 {} 次）", consecutiveFailures);
                        consecutiveFailures = 0;
                    }
                    if (orderList == null || orderList.isEmpty()) {
                        if (!running) {
                            break;
                        }
                        // 固定节拍兜底清理 pending：每 30 秒一次，与这段时间有没有新消息无关
                        if (System.currentTimeMillis() - lastPendingDrainTime >= PENDING_DRAIN_INTERVAL_MS) {
                            lastPendingDrainTime = System.currentTimeMillis();
                            drainPendingSafely();
                        }
                        continue;
                    }
                    processOrder(orderList.get(0));
                } catch (Exception e) {
                    // 关闭过程中连接工厂可能已销毁，此时抛异常属于预期，静默退出即可
                    if (!running) {
                        log.info("秒杀消费者已停止，忽略关闭期间的一次读异常: {}", e.getMessage());
                        break;
                    }
                    backoffOnFailure(e);
                }
            }
            log.info("秒杀消费者线程已退出");
        }

        /**
         * 消费失败后的退避等待。
         * <p>
         * 原实现失败后立刻重试：Redis 长时间不可用时（虚拟机挂起、网络断开、连接被拒）
         * {@code read} 是立即失败而不是阻塞 2 秒，外层循环会全速空转，
         * 每轮还要进 pending 再刷 5 条 ERROR，几分钟就能把日志刷到几十 MB，CPU 也被打满。
         * <p>
         * 改为指数退避 1s→2s→4s→8s→16s→30s 封顶；连续失败只有第 1 次打完整堆栈，
         * 之后只打一行摘要，日志量从"每次 6 条堆栈"降到"半分钟 1 行"。
         */
        private void backoffOnFailure(Exception e) {
            consecutiveFailures++;
            long backoff = Math.min(MAX_BACKOFF_MS, 1000L << Math.min(consecutiveFailures - 1, 5));
            if (consecutiveFailures == 1) {
                log.error("消费秒杀订单消息异常，{}ms 后重试", backoff, e);
            } else {
                log.warn("消费异常连续第 {} 次，{}ms 后重试：{}", consecutiveFailures, backoff, e.toString());
            }
            sleep(backoff);
        }

        /** pending 兜底清理统一入口：异常吞掉等下一轮，不在兜底流程里再抛 */
        private void drainPendingSafely() {
            try {
                drainPendingList();
            } catch (Exception e) {
                if (running) {
                    log.warn("pending 兜底清理中断，{} 秒后再试：{}",
                            PENDING_DRAIN_INTERVAL_MS / 1000, e.toString());
                }
            }
        }

        private void processOrder(MapRecord<String, Object, Object> record) throws Exception {
            Map<Object, Object> value = record.getValue();
            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
            // 消息字段缺失（毒消息）直接ACK丢弃，避免一直卡在pending队列反复报异常
            if (voucherOrder.getId() == null || voucherOrder.getUserId() == null || voucherOrder.getVoucherId() == null) {
                log.error("秒杀订单消息字段缺失，ACK丢弃: {}", value);
                acknowledge(record.getId());
                return;
            }
            int status = handleVoucherOrder(voucherOrder);
            if (status == 1) {
                log.info("秒杀订单落库成功 orderId={}, voucherId={}, userId={}",
                        voucherOrder.getId(), voucherOrder.getVoucherId(), voucherOrder.getUserId());
            } else if (status == -1) {
                // 数据库库存已不足（redis/DB出现不一致）：补偿redis库存，与DB对齐
                log.warn("数据库库存不足，补偿redis库存并ACK orderId={}, voucherId={}",
                        voucherOrder.getId(), voucherOrder.getVoucherId());
                compensateStock(voucherOrder);
            } else {
                log.warn("重复下单，ACK跳过 orderId={}, userId={}, voucherId={}",
                        voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());
            }
            acknowledge(record.getId());
        }

        private void acknowledge(RecordId recordId) {
            stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, STREAM_GROUP, recordId);
        }

        /**
         * 读取并处理当前消费者 pending 列表中的消息，单轮最多 {@link #PENDING_MAX_PER_ROUND} 条。
         * <p>
         * 刻意和主消费共用同一个线程，不另起定时线程：主线程正在处理、尚未 ACK 的消息
         * 会被兜底线程再读一次，造成重复消费，只能靠数据库唯一索引兜底。单线程天然避免了这个问题。
         * <p>
         * 处理成功的消息 ACK 移出 pending；同一条连续失败 3 次则本轮放弃，等 30 秒后的下一轮，
         * 避免一条处理不了的消息把消费线程长期占住。
         * <p>
         * 读 Redis 本身的异常不在这里吞，向上抛给 {@link #drainPendingSafely()}，由主循环的退避逻辑接管。
         */
        private void drainPendingList() {
            int handled = 0;
            int failCount = 0;
            while (running && handled < PENDING_MAX_PER_ROUND) {
                List<MapRecord<String, Object, Object>> orderList = stringRedisTemplate.opsForStream().read(
                        Consumer.from(STREAM_GROUP, STREAM_CONSUMER),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(STREAM_KEY, ReadOffset.from("0")));
                if (orderList == null || orderList.isEmpty()) {
                    return;
                }
                try {
                    processOrder(orderList.get(0));
                    handled++;
                    failCount = 0;
                } catch (Exception e) {
                    if (!running) {
                        log.info("秒杀消费者已停止，pending 处理提前结束，剩余消息留待下次启动重试");
                        return;
                    }
                    if (++failCount >= 3) {
                        log.warn("pending 消息连续失败 {} 次，本轮放弃，{} 秒后重试：{}",
                                failCount, PENDING_DRAIN_INTERVAL_MS / 1000, e.toString());
                        return;
                    }
                    sleep(500);
                }
            }
            if (handled >= PENDING_MAX_PER_ROUND) {
                log.info("本轮 pending 已处理 {} 条，剩余的 {} 秒后继续", handled, PENDING_DRAIN_INTERVAL_MS / 1000);
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 消费端处理单个订单：加用户级分布式锁后，通过事务代理落库。
     * 锁获取超时抛出异常，让消息留在pending列表等待重试，而不是直接ACK丢弃。
     */
    private int handleVoucherOrder(VoucherOrder voucherOrder) throws InterruptedException {
        Long userId = voucherOrder.getUserId();
        RLock rlock = redissonClient.getLock("lock:order:" + userId);
        // 不指定leaseTime走看门狗自动续期；最多等10s
        boolean isLock = rlock.tryLock(10, TimeUnit.SECONDS);
        if (!isLock) {
            log.error("获取订单锁超时，userId={}，消息留在pending等待重试", userId);
            throw new RuntimeException("获取分布式锁超时: " + userId);
        }
        try {
            return proxy.createVoucherOrder(voucherOrder);
        } finally {
            if (rlock.isHeldByCurrentThread()) {
                rlock.unlock();
            }
        }
    }

    /**
     * 数据库侧确认库存不足（DB扣减失败 => DB库存已为0）时，将redis库存同步为0并移除用户购买记录，
     * 保证redis与DB一致，后续请求直接在lua脚本被"库存不足"拦截。
     */
    private void compensateStock(VoucherOrder voucherOrder) {
        Long voucherId = voucherOrder.getVoucherId();
        Long userId = voucherOrder.getUserId();
        stringRedisTemplate.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "0");
        stringRedisTemplate.opsForSet().remove(SECKILL_USER_KEY + voucherId + ":" + userId, userId.toString());
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public int createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 1、判断用户是否已经下单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.warn("用户已下单过，拒绝重复下单 userId={}, voucherId={}", userId, voucherId);
            return 0;
        }
        // 2、扣减库存（乐观锁，stock > 0 才扣减）
        boolean ok = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!ok) {
            log.warn("库存不足，拒绝下单 voucherId={}", voucherId);
            return -1;
        }
        // 3、保存订单，真正落库成功才返回1
        if (save(voucherOrder)) {
            return 1;
        }
        throw new RuntimeException("订单保存失败 orderId=" + voucherOrder.getId());
    }
}
