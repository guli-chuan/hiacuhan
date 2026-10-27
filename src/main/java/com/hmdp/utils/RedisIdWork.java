package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWork {
    private static final long START_STAMP = 1786790367L;
    private final StringRedisTemplate stringRedisTemplate;
    private static final long SEQUENCE_BIT = 32;

    public RedisIdWork(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String key) {
        //1、获取时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - START_STAMP;
        if (timestamp < 0) {
            throw new RuntimeException("时间戳错误");
        }
        //2、获取自增ID
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long increment = stringRedisTemplate.opsForValue().increment("icr:"+key+":"+date);

        //3、拼接并返回
        return timestamp << SEQUENCE_BIT | increment;
    }

}
