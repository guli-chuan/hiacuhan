-- KEYS[1]：博客点赞 ZSet 的 key
-- ARGV[1]：当前用户 ID
-- ARGV[2]：当前时间戳，用作 ZSet score

if redis.call('zscore', KEYS[1], ARGV[1]) then
    redis.call('zrem', KEYS[1], ARGV[1])
    return -1
end

redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])
return 1
