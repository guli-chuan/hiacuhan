package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryList() {
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        String shopTypeList = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopTypeList)){
            List<ShopType> shopTypes = JSON.parseArray(shopTypeList, ShopType.class);
            return shopTypes;
        }
        List<ShopType> shopTypes = query().list();
        if (CollectionUtils.isEmpty(shopTypes)){
            //存入空数组字符串，设置过期时间
            stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(new ArrayList<>()), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
            return new ArrayList<>();
        }
        //数据库有数据，写入缓存
        stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(shopTypes), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shopTypes;
    }
}
