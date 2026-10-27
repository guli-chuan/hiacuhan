--1、参数列表
--1.1 优惠券Id
local voucherId = ARGV[1]
--1.2 用户Id
local userId = ARGV[2]
--1.3 订单Id
local orderId = ARGV[3]
--2、数据的key
--2.1 库存key
local stockKey = "seckill:stock:" .. voucherId
--2.2 用户购买记录key（带上券id，一张券一个记录，互不影响）
local userKey = "seckill:user:" .. voucherId .. ":" .. userId
--2.3 秒杀时间窗口key（由应用启动/创建秒杀券时写入）
local beginKey = "seckill:begin:" .. voucherId
local endKey = "seckill:end:" .. voucherId
--3、校验秒杀时间（key不存在时跳过校验，兼容历史数据）
local now = redis.call("time")[1]
local beginTime = redis.call("get", beginKey)
if beginTime and tonumber(beginTime) > tonumber(now) then
    return 3 -- 秒杀尚未开始
end
local endTime = redis.call("get", endKey)
if endTime and tonumber(endTime) < tonumber(now) then
    return 4 -- 秒杀已结束
end
--4、获取库存
local stock = tonumber(redis.call("get", stockKey))
if not stock or stock <= 0 then
    return 0 -- 库存不足
end
--5、判断用户是否已经购买过
if (redis.call("sismember", userKey, userId) == 1) then
    return 2 -- 不能重复下单
end
--6、扣减库存
redis.call("incrby", stockKey, -1)
--7、记录用户购买
redis.call("sadd", userKey, userId)
--8、投递订单消息到stream（字段名必须与VoucherOrder实体字段完全一致：id、userId、voucherId）
redis.call("xadd", "stream.orders", "*", "id", orderId, "userId", userId, "voucherId", voucherId)
return 1 -- 成功
