-- Atomically gate exact quota buckets through their shared circuit state,
-- then charge every weighted quota scope or none of them.
--
-- KEYS[1..N]       = quota bucket keys
-- KEYS[N+1..2N]    = matching circuit-state keys
-- ARGV[1]          = now_ms
-- ARGV[2]          = N
-- ARGV[3]          = half_open_probe_lease_ms
-- ARGV[4]          = circuit_state_retention_ms
-- For quota i:
-- ARGV[5 + (i-1)*3] = capacity
-- ARGV[6 + (i-1)*3] = refill_per_second
-- ARGV[7 + (i-1)*3] = cost
--
-- Returns {granted, blocked_index, retry_after_ms, denial_kind}.
-- denial_kind: 0=grant, 1=quota, 2=circuit_open.

local now_ms = tonumber(ARGV[1])
local count = tonumber(ARGV[2])
local probe_lease_ms = tonumber(ARGV[3])
local circuit_retention_ms = tonumber(ARGV[4])
local probe_indices = {}
local circuit_blocked_index = 0
local circuit_retry_ms = 0

-- Read-only circuit pass. Nothing is claimed until quota can also admit.
for i = 1, count do
    local circuit_key = KEYS[count + i]
    local circuit = redis.call(
        'HMGET',
        circuit_key,
        'state',
        'open_until_ms',
        'probe_until_ms'
    )
    local state = circuit[1] or 'closed'
    local open_until_ms = tonumber(circuit[2]) or 0
    local probe_until_ms = tonumber(circuit[3]) or 0
    local retry_ms = 0

    if state == 'open' then
        if open_until_ms > now_ms then
            retry_ms = open_until_ms - now_ms
        elseif probe_until_ms > now_ms then
            retry_ms = probe_until_ms - now_ms
        else
            table.insert(probe_indices, i)
        end
    elseif state == 'half_open' then
        if probe_until_ms > now_ms then
            retry_ms = probe_until_ms - now_ms
        else
            table.insert(probe_indices, i)
        end
    end

    if retry_ms > circuit_retry_ms then
        circuit_blocked_index = i
        circuit_retry_ms = retry_ms
    end
end

if circuit_blocked_index ~= 0 then
    return {0, circuit_blocked_index, circuit_retry_ms, 2}
end

local quota_states = {}
local quota_blocked_index = 0
local quota_retry_ms = 0

-- Read/compute quota pass.
for i = 1, count do
    local offset = 5 + (i - 1) * 3
    local capacity = tonumber(ARGV[offset])
    local refill_per_sec = tonumber(ARGV[offset + 1])
    local cost = tonumber(ARGV[offset + 2])
    local quota = redis.call(
        'HMGET',
        KEYS[i],
        'tokens',
        'updated_at_ms',
        'lockout_until_ms'
    )
    local tokens = tonumber(quota[1])
    local updated_at_ms = tonumber(quota[2])
    local lockout_until_ms = tonumber(quota[3])

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

    quota_states[i] = {
        tokens = tokens,
        cost = cost,
        lockout_until_ms = lockout_until_ms,
    }
    if retry_ms ~= 0 then
        if quota_blocked_index == 0
            or (retry_ms == -1 and quota_retry_ms ~= -1)
            or (quota_retry_ms ~= -1 and retry_ms > quota_retry_ms)
        then
            quota_blocked_index = i
            quota_retry_ms = retry_ms
        end
    end
end

-- Persist refill state. Charge only if every quota and circuit admitted.
for i = 1, count do
    local quota = quota_states[i]
    local tokens = quota.tokens
    if quota_blocked_index == 0 then
        tokens = tokens - quota.cost
    end
    redis.call(
        'HMSET',
        KEYS[i],
        'tokens',
        tokens,
        'updated_at_ms',
        now_ms
    )
    if quota_blocked_index == 0 then
        redis.call('HDEL', KEYS[i], 'lockout_until_ms')
    end
    local ttl_ms = 86400000
    if quota.lockout_until_ms and quota.lockout_until_ms > now_ms then
        ttl_ms = math.max(ttl_ms, quota.lockout_until_ms - now_ms)
    end
    redis.call('PEXPIRE', KEYS[i], ttl_ms)
end

if quota_blocked_index ~= 0 then
    return {0, quota_blocked_index, quota_retry_ms, 1}
end

-- Quota admitted: atomically claim the single distributed half-open probe.
for _, i in ipairs(probe_indices) do
    local circuit_key = KEYS[count + i]
    redis.call(
        'HMSET',
        circuit_key,
        'state',
        'half_open',
        'probe_until_ms',
        now_ms + probe_lease_ms
    )
    redis.call('PEXPIRE', circuit_key, circuit_retention_ms)
end

return {1, 0, 0, 0}
