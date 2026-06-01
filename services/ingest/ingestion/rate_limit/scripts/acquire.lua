-- services/ingestion/rate_limit/scripts/acquire.lua
-- KEYS[1]   = bucket key, e.g. "rate:<tenant>:<source>:<method>"
-- ARGV[1]   = now_ms (integer)
-- ARGV[2]   = capacity (integer, max tokens)
-- ARGV[3]   = refill_per_sec (number, tokens added per second)
-- ARGV[4]   = cost (integer, tokens consumed per acquire)
--
-- Returns table: { granted (0 or 1), tokens_remaining, retry_after_ms }
--
-- retry_after_ms semantics:
--   = 0   on grant.
--   > 0   on deny; milliseconds until the bucket will have enough tokens
--         to serve `cost`, OR remaining lockout window (whichever applies).
--   = -1  SENTINEL: indefinite lockout. Returned ONLY when
--         refill_per_sec == 0 and the bucket has insufficient tokens.
--         With zero refill, the deficit / refill_per_sec * 1000 math
--         is +infinity (math.huge in Lua), which Redis cannot serialise
--         as an integer. The sentinel tells callers "this bucket will
--         never recover on its own; clear it via report_retry_after.lua
--         or wait for a configuration change." Callers must handle -1
--         explicitly — treating it as a millisecond duration would be
--         wrong both in Python (negative sleep) and in semantics.
--
-- Behavior:
--   1. If a lockout is set (set by report_retry_after.lua), and we are
--      within the lockout window, deny with retry_after = remaining lockout.
--   2. Otherwise compute current token level = min(capacity, last_tokens
--      + (now - last_updated) * refill_per_sec / 1000).
--   3. If tokens >= cost, deduct and grant.
--   4. Else if refill_per_sec == 0, return the -1 sentinel.
--   5. Else compute retry_after = ceil((cost - tokens) / refill_per_sec * 1000).

local now_ms = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill_per_sec = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

-- Read current state.
local state = redis.call('HMGET', KEYS[1],
    'tokens', 'updated_at_ms', 'lockout_until_ms')
local tokens = tonumber(state[1])
local updated_at_ms = tonumber(state[2])
local lockout_until_ms = tonumber(state[3])

-- Check lockout (set by report_retry_after.lua when source returns 429).
if lockout_until_ms and lockout_until_ms > now_ms then
    return {0, tokens or capacity, lockout_until_ms - now_ms}
end

-- Compute refilled token count.
if tokens == nil then
    tokens = capacity
    updated_at_ms = now_ms
end
local elapsed_ms = now_ms - updated_at_ms
local refilled = elapsed_ms * refill_per_sec / 1000
tokens = math.min(capacity, tokens + refilled)

if tokens >= cost then
    tokens = tokens - cost
    redis.call('HMSET', KEYS[1],
        'tokens', tokens, 'updated_at_ms', now_ms)
    redis.call('HDEL', KEYS[1], 'lockout_until_ms')
    redis.call('PEXPIRE', KEYS[1], 86400000)  -- 24h
    return {1, tokens, 0}
end

-- Refill==0 + insufficient tokens → indefinite lockout (sentinel -1).
-- Persist state without consuming; the bucket can be cleared only by
-- report_retry_after.lua or by a config change that raises refill.
if refill_per_sec == 0 then
    redis.call('HMSET', KEYS[1],
        'tokens', tokens, 'updated_at_ms', now_ms)
    redis.call('PEXPIRE', KEYS[1], 86400000)
    return {0, tokens, -1}
end

local deficit = cost - tokens
local retry_after_ms = math.ceil(deficit / refill_per_sec * 1000)
-- Persist current state without consuming.
redis.call('HMSET', KEYS[1],
    'tokens', tokens, 'updated_at_ms', now_ms)
redis.call('PEXPIRE', KEYS[1], 86400000)
return {0, tokens, retry_after_ms}
