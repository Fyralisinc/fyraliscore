-- services/integrations/discord/gateway/scripts/release.lua
--
-- Per ingestion LLD §1.5 + M4.1 work order.
-- Atomic release of the Discord Gateway leader lease — check ownership,
-- then delete. Without the check, a stale shutdown handler could
-- release someone else's lease (the lease expired, was re-acquired,
-- and now we'd delete the new holder's).
--
-- KEYS[1] = lease key
-- ARGV[1] = expected lease value (this process's UUID)
--
-- Returns:
--   1 on released (we owned it and it is now gone)
--   0 on no-op (key absent OR value belongs to someone else)
--
-- Atomicity matters for the same reason as refresh.lua — Python-side
-- GET-then-DEL has a race window. Lua collapses it.
local current = redis.call('GET', KEYS[1])
if not current or current ~= ARGV[1] then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
