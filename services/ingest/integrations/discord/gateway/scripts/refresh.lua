-- services/integrations/discord/gateway/scripts/refresh.lua
--
-- Per ingestion LLD §1.5 + M4.1 work order.
-- Atomic refresh of the Discord Gateway leader lease — check ownership,
-- then extend TTL. If we are not the owner (lease was lost to expiry
-- and re-acquired by another pod, OR was force-deleted), refuse the
-- refresh so the caller treats itself as no longer the leader.
--
-- KEYS[1] = lease key
-- ARGV[1] = expected lease value (this process's UUID)
-- ARGV[2] = ttl_seconds
--
-- Returns:
--   1 on refreshed
--   0 on lease lost (key absent OR value belongs to someone else)
--
-- Atomicity matters: Python-side GET-then-EXPIRE has a race window
-- where another pod could acquire between the two calls and we would
-- extend their lease. Lua bundles the check+extend in a single Redis
-- command turn.
local current = redis.call('GET', KEYS[1])
if not current or current ~= ARGV[1] then
    return 0
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
