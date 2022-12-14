-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]

-- 2.数据key
-- 2.1.库存key
-- lua 中拼接字符串不适用加号 使用 ..
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单 key
local orderKey = 'seckill:order' .. voucherId

-- 3.脚本业务
-- 3.1.判断库存是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    -- 3.2.库存不足返回1
    return 1
end
-- 3.2.判断用户是否下过单
if (redis.call('sismember', orderKey, userId) == 1) then
    -- 3.3.存在说明是重复下单
    return 2
end
-- 3.4扣减库存
redis.call('incrby', stockKey, -1)
-- 3.5下单（保存用户）
redis.call('sadd', orderKey, userId)
return 0