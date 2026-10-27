package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    /**
     * 逻辑过期时长（秒）：缓存写入后 20 秒内视为新鲜，超过后触发异步重建。
     * 注意这是业务字段，不是 Redis 的 TTL。
     */
    private static final long REBUILD_SECONDS = 20L;

    /**
     * 缓存重建线程池。
     * <p>
     * 不用 new Thread()：无界创建线程在热点 key 集中过期时会把线程数打满。
     * 队列满时用 DiscardPolicy 静默丢弃重建任务——丢弃只会让旧数据多存活一轮，
     * 不影响本次请求返回，属于可接受的降级。
     */
    private static final ThreadPoolExecutor CACHE_REBUILD_EXECUTOR = new ThreadPoolExecutor(
            2, 4, 60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(200),
            new ThreadFactory() {
                private final AtomicInteger seq = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "cache-rebuild-" + seq.getAndIncrement());
                    // 守护线程：应用关闭时不阻塞 JVM 退出
                    t.setDaemon(true);
                    return t;
                }
            },
            new ThreadPoolExecutor.DiscardPolicy());

    /**
     * 释放锁脚本：比较 value 后再删除，避免删掉其他线程/实例持有的锁。
     */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 店铺详情：采用「逻辑过期 + 异步重建」，保证热点 key 过期瞬间请求不被阻塞。
     */
    @Override
    public Shop queryById(Long id) {
        return queryWithLogicalExpire(id);
    }

    /**
     * 逻辑过期方案。
     * <p>
     * 与教程版本的差异：
     * 1) 缓存不存在时不再直接返回 null，而是走互斥加载兜底——否则缓存没预热就查不到数据；
     * 2) 只有在「抢到锁」时才异步重建，其余线程直接返回过期旧值，互斥锁真正起到削峰作用；
     * 3) 重建任务提交到线程池而非 new Thread；
     * 4) 重建前做双重检查，避免多个线程重复重建。
     */
    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String cacheJson = stringRedisTemplate.opsForValue().get(key);

        // 1、缓存为空
        if (StrUtil.isBlank(cacheJson)) {
            // 1.1 命中空值缓存（存的是 ""），说明数据库确实没有这条记录，直接返回
            if (cacheJson != null) {
                return null;
            }
            // 1.2 key 不存在（未预热或被淘汰），走互斥加载兜底
            return loadShopWithMutex(id);
        }

        // 2、命中缓存，解析逻辑过期对象
        RedisData redisData = JSONUtil.toBean(cacheJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 3、未逻辑过期，直接返回
        if (expireTime != null && expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }

        // 4、已逻辑过期：只有抢到锁的线程负责异步重建
        String lockKey = LOCK_SHOP_KEY + id;
        String lockValue = buildLockValue();
        if (tryLock(lockKey, lockValue)) {
            CACHE_REBUILD_EXECUTOR.execute(() -> {
                try {
                    // 双重检查：进线程后可能已被其他线程/实例重建完成
                    String checkJson = stringRedisTemplate.opsForValue().get(key);
                    if (StrUtil.isNotBlank(checkJson)) {
                        RedisData check = JSONUtil.toBean(checkJson, RedisData.class);
                        if (check.getExpireTime() != null
                                && check.getExpireTime().isAfter(LocalDateTime.now())) {
                            return;
                        }
                    }
                    saveShop2Redis(id, REBUILD_SECONDS);
                    log.info("店铺缓存重建完成 id={}", id);
                } catch (Exception e) {
                    log.error("店铺缓存重建失败 id={}", id, e);
                } finally {
                    unLock(lockKey, lockValue);
                }
            });
        }

        // 5、无论是否抢到锁，本次都返回过期旧值，保证接口响应时间稳定
        return shop;
    }

    /**
     * 缓存未命中时的互斥加载（原 queryWithMutex 的迭代版本）。
     * <p>
     * 原实现用递归重试，高并发下可能栈溢出，且 try/finally 会在未持锁时误释放他人的锁。
     * 改为「有限次循环 + 抢到锁才操作」。
     */
    private Shop loadShopWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String lockKey = LOCK_SHOP_KEY + id;
        String lockValue = buildLockValue();

        for (int i = 0; i < 3; i++) {
            if (!tryLock(lockKey, lockValue)) {
                // 没抢到锁：短暂等待后重试，让抢到锁的线程有时间回填缓存
                sleepQuietly(50L);
                continue;
            }
            try {
                // 双重检查
                String cacheJson = stringRedisTemplate.opsForValue().get(key);
                if (StrUtil.isNotBlank(cacheJson)) {
                    RedisData redisData = JSONUtil.toBean(cacheJson, RedisData.class);
                    return JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
                }
                if (cacheJson != null) {
                    return null;
                }
                Shop shop = getById(id);
                if (shop == null) {
                    // 空值缓存，防止缓存穿透
                    stringRedisTemplate.opsForValue()
                            .set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                    return null;
                }
                saveShop2Redis(id, REBUILD_SECONDS);
                return shop;
            } finally {
                unLock(lockKey, lockValue);
            }
        }

        // 重试耗尽仍抢不到锁：降级直接查库，宁可慢也不返回错
        log.warn("获取店铺缓存重建锁失败，降级直接查库 id={}", id);
        return getById(id);
    }

    /**
     * 预热指定店铺的逻辑过期缓存。
     * <p>
     * 逻辑过期方案依赖缓存常驻，新店铺上线后需要预热；否则首次访问会走互斥加载兜底。
     */
    @Override
    public void saveShop2Redis(Long id, long seconds) {
        Shop shop = getById(id);
        if (shop == null) {
            log.warn("预热失败，店铺不存在 id={}", id);
            return;
        }
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(seconds));
        // 逻辑过期方案的关键：不设置 Redis TTL，让 key 常驻，过期由业务字段判断
        stringRedisTemplate.opsForValue()
                .set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result update(Shop shop) {
        if (shop.getId() == null) {
            return Result.fail("店铺ID不能为空");
        }
        // 1、更新数据库
        updateById(shop);

        // 2、事务提交后再删缓存。
        // 若在事务提交前删除，一旦事务回滚，缓存已被清空，并发读会把旧值重新回填，造成脏缓存。
        Long shopId = shop.getId();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
                    log.info("店铺更新事务已提交，删除缓存 id={}", shopId);
                }
            });
        } else {
            stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
        }
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1、判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要根据坐标查询，根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 2、计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 3、查询redis，根据距离排序分页
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(end));
        // 4、解析出Id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distance = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String shopIdstr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdstr));
            Distance dist = result.getDistance();
            distance.put(shopIdstr, dist);
        });
        // 5、根据Id批量查询店铺，注意顺序
        String idsStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id, " + idsStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distance.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }

    /**
     * 获取锁：value 使用「UUID + 线程ID」，作为释放时的持有者凭证。
     */
    private boolean tryLock(String key, String value) {
        Boolean flag = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, value, LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁：通过 Lua 脚本比较 value 后再删除，保证「判断 + 删除」原子执行。
     */
    private void unLock(String key, String value) {
        Long result = stringRedisTemplate.execute(
                UNLOCK_SCRIPT, Collections.singletonList(key), value);
        if (result == null || result == 0L) {
            log.warn("释放锁未生效（锁已过期或非本线程持有）key={}", key);
        }
    }

    private String buildLockValue() {
        return UUID.randomUUID().toString() + "-" + Thread.currentThread().getId();
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
