-- services/integrations/discord/gateway/scripts/acquire.lua
--
-- Per ingestion LLD §1.5 + §13 + M4.1 work order.
-- Atomic acquire of the Discord Gateway leader lease.
--
-- KEYS[1] = lease key, e.g. "gateway:discord:leader_lock"
-- ARGV[1] = lease value (per-process UUID — used by refresh/release
--           to prove ownership)
-- ARGV[2] = ttl_seconds
--
-- Returns:
--   1 on acquired
--   0 on contention (some other holder already owns the lease)
--
-- Semantics: SET NX EX. The script form (vs. a plain redis.call from
-- the Python side) keeps M4.1 consistent with the rest of the
-- Lua-in-Redis pattern (M1.3 rate limiter, M3.3 backlog bucket) and
-- gives us one shape of script-load/EVALSHA across all three.
local acquired = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if acquired then
    return 1
end
return 0
