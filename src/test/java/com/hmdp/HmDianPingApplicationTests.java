package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Autowired
    private IShopService shopService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void loadShopData(){
        //1、查询所有店铺
        List<Shop> shop = shopService.list();
        //2、将店铺分组，按照typeId分组
        Map<Long,List<Shop>> entry = shop.stream().collect(Collectors.groupingBy(Shop :: getTypeId));
        //3、将分组后的数据写入Redis
        for (Map.Entry<Long, List<Shop>> map : entry.entrySet()) {
            //3.1、获取typeId
            Long typeId = map.getKey();
            //3.2、获取店铺列表
            List<Shop> shops = map.getValue();
            //3.3、写入Redis
            String key = "shop:geo:" + typeId;
            for (Shop shop1 : shops) {
                stringRedisTemplate.opsForGeo().add(key,new Point(shop1.getX(), shop1.getY()), shop1.getId().toString());
            }
        }
    }

}
