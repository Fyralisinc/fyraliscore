-- Record one retryable upstream failure against every concrete circuit key.
--
-- KEYS       = exact quota-bucket-derived circuit keys
-- ARGV[1]    = now_ms
-- ARGV[2]    = consecutive_failure_threshold
-- ARGV[3]    = open_duration_ms
-- ARGV[4]    = circuit_state_retention_ms

local now_ms = tonumber(ARGV[1])
local failure_threshold = tonumber(ARGV[2])
local open_duration_ms = tonumber(ARGV[3])
local retention_ms = tonumber(ARGV[4])

for i = 1, #KEYS do
    local state = redis.call('HGET', KEYS[i], 'state') or 'closed'
    local failures = tonumber(
        redis.call('HGET', KEYS[i], 'consecutive_failures')
    ) or 0

    if state == 'half_open' then
        redis.call(
            'HMSET',
            KEYS[i],
            'state',
            'open',
            'consecutive_failures',
            failure_threshold,
            'open_until_ms',
            now_ms + open_duration_ms,
            'probe_until_ms',
            0
        )
    elseif state == 'open' then
        local existing_until_ms = tonumber(
            redis.call('HGET', KEYS[i], 'open_until_ms')
        ) or 0
        redis.call(
            'HMSET',
            KEYS[i],
            'consecutive_failures',
            math.max(failures, failure_threshold),
            'open_until_ms',
            math.max(existing_until_ms, now_ms + open_duration_ms)
        )
    else
        failures = failures + 1
        if failures >= failure_threshold then
            redis.call(
                'HMSET',
                KEYS[i],
                'state',
                'open',
                'consecutive_failures',
                failures,
                'open_until_ms',
                now_ms + open_duration_ms,
                'probe_until_ms',
                0
            )
        else
            redis.call(
                'HMSET',
                KEYS[i],
                'state',
                'closed',
                'consecutive_failures',
                failures
            )
        end
    end
    redis.call('PEXPIRE', KEYS[i], retention_ms)
end

return #KEYS
