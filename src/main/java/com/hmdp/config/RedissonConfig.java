package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 客户端配置。
 * <p>
 * 连接地址与密码全部来自配置，不再硬编码：仓库是公开的，把密码写死在源码里等于把密码公开。
 * application.yml 里已经是 {@code ${REDIS_HOST:...}} / {@code ${REDIS_PORT:...}} / {@code ${REDIS_PASSWORD:...}}
 * 的形式，本地启动时用环境变量注入即可（见 README「快速开始」）。
 */
@Configuration
public class RedissonConfig {

    @Value("${spring.redis.host:127.0.0.1}")
    private String host;

    @Value("${spring.redis.port:6379}")
    private int port;

    @Value("${spring.redis.password:}")
    private String password;

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        SingleServerConfig single = config.useSingleServer()
                .setAddress("redis://" + host + ":" + port);
        // 没配密码就不要调 setPassword("")，Redis 会当成「用空密码 AUTH」直接报错
        if (password != null && !password.trim().isEmpty()) {
            single.setPassword(password);
        }
        // destroyMethod = shutdown：容器关闭时一并关掉 Redisson 的连接与 Netty 线程，
        // 否则会出现「应用已退出、线程还在跑」的刷屏
        return Redisson.create(config);
    }
}
