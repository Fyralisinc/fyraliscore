-- services/ingestion/rate_limit/scripts/report_retry_after.lua
-- KEYS[1]   = bucket key
-- ARGV[1]   = now_ms
-- ARGV[2]   = retry_after_ms (from source's Retry-After header)
--
-- Extends a lockout that overrides token math until now + retry_after_ms.
-- Concurrent workers can observe different Retry-After values. Never replace
-- a later shared deadline with an earlier one, or one worker can prematurely
-- reopen the provider bucket for every replica.
local now_ms = tonumber(ARGV[1])
local retry_after_ms = tonumber(ARGV[2])
local requested_until_ms = now_ms + retry_after_ms
local existing_until_ms = tonumber(
    redis.call('HGET', KEYS[1], 'lockout_until_ms')
)
local lockout_until_ms = requested_until_ms
if existing_until_ms and existing_until_ms > lockout_until_ms then
    lockout_until_ms = existing_until_ms
end

redis.call('HMSET', KEYS[1], 'lockout_until_ms', lockout_until_ms)
redis.call(
    'PEXPIRE',
    KEYS[1],
    math.max(86400000, lockout_until_ms - now_ms)
)
return lockout_until_ms
