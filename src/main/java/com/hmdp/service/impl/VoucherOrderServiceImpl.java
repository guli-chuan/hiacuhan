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

    /** 单线程消费者，保证同一时刻只有一个线程在处理订单，天然串行 */
    private static final ExecutorService seckillExecutor = Executors.newSingleThreadExecutor();

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
        private long lastPendingDrainTime = 0L;
        private static final long PENDING_DRAIN_INTERVAL = 30000L;

        @Override
        public void run() {
            // 启动时先处理历史遗留的pending消息（修复旧bug留下的"redis已生效但未落库"卡单）
            log.info("秒杀消费者启动，先处理队列中的历史pending消息...");
            handlePendingList();
            while (true) {
                try {
                    List<MapRecord<String, Object, Object>> orderList = stringRedisTemplate.opsForStream().read(
                            Consumer.from(STREAM_GROUP, STREAM_CONSUMER),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()));
                    if (orderList == null || orderList.isEmpty()) {
                        // 无新消息时周期兜底处理pending，避免失败消息长期滞留
                        if (System.currentTimeMillis() - lastPendingDrainTime > PENDING_DRAIN_INTERVAL) {
                            lastPendingDrainTime = System.currentTimeMillis();
                            handlePendingList();
                        }
                        continue;
                    }
                    processOrder(orderList.get(0));
                } catch (Exception e) {
                    log.error("消费秒杀订单消息异常，转入pending重试", e);
                    handlePendingList();
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
         * 读取并处理当前消费者pending列表中的消息。
         * 处理成功的消息会被ACK移出pending；真正处理不了的消息最多连续重试5次后暂停，避免死循环。
         */
        private void handlePendingList() {
            int failCount = 0;
            while (true) {
                try {
                    List<MapRecord<String, Object, Object>> orderList = stringRedisTemplate.opsForStream().read(
                            Consumer.from(STREAM_GROUP, STREAM_CONSUMER),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_KEY, ReadOffset.from("0")));
                    if (orderList == null || orderList.isEmpty()) {
                        break;
                    }
                    processOrder(orderList.get(0));
                    failCount = 0;
                } catch (Exception e) {
                    log.error("处理pending消息异常", e);
                    if (++failCount >= 5) {
                        log.error("pending消息连续失败{}次，暂停本次重试，等待下一轮", failCount);
                        break;
                    }
                    sleep(500);
                }
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
