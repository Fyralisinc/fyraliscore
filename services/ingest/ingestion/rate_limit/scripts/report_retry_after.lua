-- services/ingestion/rate_limit/scripts/report_retry_after.lua
-- KEYS[1]   = bucket key
-- ARGV[1]   = now_ms
-- ARGV[2]   = retry_after_ms (from source's Retry-After header)
--
-- Sets a lockout that overrides token math until now + retry_after_ms.
local now_ms = tonumber(ARGV[1])
local retry_after_ms = tonumber(ARGV[2])
redis.call('HMSET', KEYS[1], 'lockout_until_ms', now_ms + retry_after_ms)
redis.call('PEXPIRE', KEYS[1], 86400000)
return 1
