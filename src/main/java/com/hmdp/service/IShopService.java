package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IShopService extends IService<Shop> {

    Shop queryById(Long id);

    /**
     * 预热店铺的逻辑过期缓存。
     *
     * @param id      店铺id
     * @param seconds 逻辑过期时长（秒），仅写入业务字段，不设置 Redis TTL
     */
    void saveShop2Redis(Long id, long seconds);

    Result update(Shop shop);

    Result queryShopByType(Integer typeId, Integer current, Double x, Double y);
}
