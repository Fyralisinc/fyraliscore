-- Atomic weighted acquisition across every quota scope for one provider call.
--
-- KEYS       = bucket keys (one per scope)
-- ARGV[1]    = now_ms
-- For key i:
-- ARGV[2 + (i-1)*3] = capacity
-- ARGV[3 + (i-1)*3] = refill_per_second
-- ARGV[4 + (i-1)*3] = cost
--
-- Returns {granted, blocked_key_index, retry_after_ms}.
-- blocked_key_index is 1-based and 0 on grant. A retry of -1 means the
-- selected zero-refill bucket cannot recover without external intervention.
--
-- The script performs a read/compute pass first. No scope is charged unless
-- every scope can admit the operation, avoiding the conservative token loss
-- caused by sequential single-key acquisition.

local now_ms = tonumber(ARGV[1])
local states = {}
local blocked_index = 0
local blocked_retry_ms = 0

for i = 1, #KEYS do
    local offset = 2 + (i - 1) * 3
    local capacity = tonumber(ARGV[offset])
    local refill_per_sec = tonumber(ARGV[offset + 1])
    local cost = tonumber(ARGV[offset + 2])
    local state = redis.call(
        'HMGET',
        KEYS[i],
        'tokens',
        'updated_at_ms',
        'lockout_until_ms'
    )
    local tokens = tonumber(state[1])
    local updated_at_ms = tonumber(state[2])
    local lockout_until_ms = tonumber(state[3])

    if tokens == nil then
        tokens = capacity
        updated_at_ms = now_ms
    end
    local elapsed_ms = math.max(0, now_ms - updated_at_ms)
    tokens = math.min(
        capacity,
        tokens + elapsed_ms * refill_per_sec / 1000
    )

    local retry_ms = 0
    if lockout_until_ms and lockout_until_ms > now_ms then
        retry_ms = lockout_until_ms - now_ms
    elseif tokens < cost then
        if refill_per_sec == 0 then
            retry_ms = -1
        else
            retry_ms = math.ceil(
                (cost - tokens) / refill_per_sec * 1000
            )
        end
    end

    states[i] = {
        tokens = tokens,
        cost = cost,
        lockout_until_ms = lockout_until_ms,
    }
    if retry_ms ~= 0 then
        if blocked_index == 0
            or (retry_ms == -1 and blocked_retry_ms ~= -1)
            or (blocked_retry_ms ~= -1 and retry_ms > blocked_retry_ms)
        then
            blocked_index = i
            blocked_retry_ms = retry_ms
        end
    end
end

for i = 1, #KEYS do
    local state = states[i]
    local tokens = state.tokens
    if blocked_index == 0 then
        tokens = tokens - state.cost
    end
    redis.call(
        'HMSET',
        KEYS[i],
        'tokens',
        tokens,
        'updated_at_ms',
        now_ms
    )
    if blocked_index == 0 then
        redis.call('HDEL', KEYS[i], 'lockout_until_ms')
    end
    local ttl_ms = 86400000
    if state.lockout_until_ms and state.lockout_until_ms > now_ms then
        ttl_ms = math.max(ttl_ms, state.lockout_until_ms - now_ms)
    end
    redis.call('PEXPIRE', KEYS[i], ttl_ms)
end

if blocked_index == 0 then
    return {1, 0, 0}
end
return {0, blocked_index, blocked_retry_ms}
