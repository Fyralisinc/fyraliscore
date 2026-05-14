# HTTP Contract — `/webhooks/slack/*` (extension)

Extends the existing IN-06 Slack webhook surface. **No new routes.** This contract documents the ingestion-handler-level branching that recognizes installation-lifecycle events.

## Surface

`POST /webhooks/slack/{subpath:path}` — existing route, no signature change.

## What changes inside

After IN-06's signature verification and IN-08-Phase-2's `TenantResolver.resolve(...)` call, the Slack ingestion handler (`services/ingestion/handlers/slack_message.py`) examines `payload.event.type` (or `payload.type` for the outer envelope).

### Branch table

| Inbound `event.type` (or outer `type`) | Routes to | Effect on `provider_installations` | Audit |
|----------------------------------------|-----------|-------------------------------------|-------|
| `message`, `message.channels`, `message.groups`, `message.im`, `message.mpim`, `app_mention` | Existing IN-06 ingestion path (`Observation` produced) | none | none |
| `app_uninstalled` | `services/integrations/slack/uninstall.py::handle_app_uninstalled` | row's `enabled` → false, `secret_ref` cleared, prior secret rows deleted from `encrypted_secrets` | `installation_audit_log` row with `action='uninstall'`, `status='ok'` |
| `tokens_revoked` | `services/integrations/slack/uninstall.py::handle_tokens_revoked` | same as above | same |
| `url_verification` (outer `type`) | Existing IN-06 handshake response | none | none |
| Any other event type | Existing IN-06 ingestion path; treated as a no-op for installation lifecycle | none | none |

### Uninstall handler logic (`handle_app_uninstalled`, `handle_tokens_revoked`)

Both events carry the `team_id` in the outer envelope. The resolver has already mapped `team_id → tenant_id` via `provider_installations`, so:

1. Receive `Resolved(tenant_id, installation_row_id, secret_ref)` from the IN-07 resolver.
2. Look up the installation row to read its `secret_ref` (already in the `Resolved` outcome).
3. `tenant_resolver.disable_installation(installation_row_id)`. (Existing IN-07 admin action.)
4. Collect all `encrypted_secrets` rows for this installation:
   - `SELECT id FROM encrypted_secrets WHERE tenant_id = $tenant_id AND label LIKE 'slack_%token:' || $team_id`
   - This is a tenant-scoped query (defense-in-depth + RLS).
5. For each ref, `secret_store.delete(ref)`. Tolerant of "already deleted" — `DELETE … RETURNING id` returning zero rows is not an error.
6. Best-effort: `provider_installations.secret_ref` is left pointing at the now-deleted bot-token row. This is acceptable because (a) the row is `enabled=false`, so the resolver returns `UnknownInstallation` for subsequent webhooks (next webhook → 401), and (b) re-install path (FR-018) issues a fresh `secret_ref` and the dangling pointer is overwritten.
7. `INSERT INTO installation_audit_log (..., action='uninstall', status='ok', context={"event_type": event_type})`.
8. Invalidate the resolver cache for `(slack, team_id)`.
9. **Return 200** to Slack (Slack expects an ack within ~3 s).

### Edge: uninstall for a `team_id` we never installed

- Resolver returns `UnknownInstallation`. Per IN-07, the router returns 401 BEFORE the ingestion handler runs.
- This is correct behavior: Slack's retry policy on uninstalls is bounded; a 401 is observable in Slack's app-dashboard for the operator without us doing anything special.
- A debug-level metric increments: `slack_uninstall_outcomes_total{outcome="unknown_team"}`.

### Edge: uninstall partial failure (disable succeeds, secret-delete fails)

- The installation row is `enabled=false`. Resolver returns `UnknownInstallation`. Subsequent webhooks reject.
- The dangling secret rows are eventually deleted by a follow-up retry of the same `app_uninstalled` event (Slack retries) OR by a re-install path overwriting `secret_ref`.
- Audit row written with `status='error'`, `context.failure_phase='secret_delete'`. Operator can re-run the delete via an admin endpoint (not in scope for this task — call out as a follow-up if it becomes operationally painful).
- Metric: `slack_uninstall_outcomes_total{outcome="error"}`.

### Race: webhook arrives mid-uninstall

- Both the `disable_installation` UPDATE and the `secret_delete` are in the same transaction (single asyncpg `acquire()` block) so a concurrent reader either sees `enabled=true` (and the secret rows still present) OR sees `enabled=false` (and the secret rows possibly missing).
- The resolver filters `WHERE enabled = TRUE` first; if it sees `enabled=true`, the secret rows are still present (same transaction). If it sees `enabled=false`, it returns `UnknownInstallation` immediately and never tries to read the secret.

### Idempotency

- Repeated `app_uninstalled` events for the same `team_id` are no-ops: `disable_installation` is idempotent (UPDATE sets a boolean), `secret_delete` tolerates missing rows. Each repeat writes a new audit row though — Slack retries can be frequent; the audit table is append-only and we accept the small noise rather than dedup.

## Test plan

| Test | Type | Asserts |
|------|------|---------|
| `test_uninstall_disables_row` | integration | `app_uninstalled` event → 200; `provider_installations.enabled = false`; resolver cache cleared. |
| `test_uninstall_zeros_secrets` | integration | All `encrypted_secrets` rows for this installation are deleted. |
| `test_uninstall_writes_audit` | integration | `installation_audit_log` row with `action='uninstall'`, `status='ok'`. |
| `test_uninstall_next_webhook_returns_401` | integration | A `slack:message` webhook for the same `team_id` after `app_uninstalled` returns 401 `unknown_installation`. |
| `test_uninstall_unknown_team` | integration | `app_uninstalled` for a never-installed `team_id` → 401 from the router (resolver returns `UnknownInstallation`). |
| `test_uninstall_partial_failure_audit` | integration | If `secret_store.delete` raises, the row is still disabled and an `status='error'` audit row exists. |
| `test_reinstall_after_uninstall_reuses_row` | integration | After uninstall, a re-install for the same `team_id` (FR-018) preserves `provider_installations.id`. |
| `test_tokens_revoked_equivalence` | integration | `tokens_revoked` event has identical effect to `app_uninstalled`. |
