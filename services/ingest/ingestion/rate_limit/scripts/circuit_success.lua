-- Record a provider-reachable outcome against every concrete circuit key.
-- A half-open probe closes its scope. A closed scope resets its consecutive
-- failure count. An already-open scope is left open because the success may
-- belong to a request admitted before another replica opened the circuit.
--
-- KEYS       = exact quota-bucket-derived circuit keys
-- ARGV[1]    = circuit_state_retention_ms

local retention_ms = tonumber(ARGV[1])

for i = 1, #KEYS do
    local state = redis.call('HGET', KEYS[i], 'state') or 'closed'
    if state == 'half_open' or state == 'closed' then
        redis.call(
            'HMSET',
            KEYS[i],
            'state',
            'closed',
            'consecutive_failures',
            0,
            'open_until_ms',
            0,
            'probe_until_ms',
            0
        )
        redis.call('PEXPIRE', KEYS[i], retention_ms)
    end
end

return #KEYS
