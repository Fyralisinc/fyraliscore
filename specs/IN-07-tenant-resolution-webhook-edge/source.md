IN-07 [P0] Tenant resolution at webhook edge (provider_installations)
**Files relevant**
- new: services/webhooks/tenant_resolver.py
- new: db/migrations/0041_provider_installations.sql

**Why it is needed**
A Slack webhook payload doesn't say "this is tenant A." We need a mapping from `(provider, workspace/team/installation_id)` to `tenant_id`. Today there is no such mapping — only the bearer-token path knows the tenant. Without this, webhook events have nowhere to route.

**How can it be done**
1. New table:
```sql
CREATE TABLE provider_installations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    provider TEXT NOT NULL,
    installation_id TEXT NOT NULL,  -- slack team_id, github installation, etc.
    secret_ref TEXT,                -- pointer to secrets manager
    enabled BOOLEAN NOT NULL DEFAULT true,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, installation_id)
);
```
2. Tenant resolver extracts provider-native id from each payload:
   - Slack: `team_id`
   - GitHub: `installation.id`
   - Discord: `guild_id` or `application_id`
   - Linear: `organizationId`
   - Stripe: `account` (from header)
3. Cache lookups in Redis with 5min TTL
4. Unknown installation → 401 (don't leak existence)
5. Add CLI/admin endpoint to install a new (provider, installation_id, tenant_id) tuple

**Acceptance criteria**
- Slack workspace can be linked to a tenant via the installation flow
- Webhook events route to the right tenant
- Unknown teams get 401 (not 404)
- Redis cache hit rate >95% after warmup

**Estimated effort:** 3 days
