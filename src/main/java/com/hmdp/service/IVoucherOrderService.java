package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /**
     * 创建秒杀订单（消费端通过事务代理调用）。
     * @return 1=成功，0=重复下单，-1=库存不足
     */
    int createVoucherOrder(VoucherOrder voucherOrder);
}
