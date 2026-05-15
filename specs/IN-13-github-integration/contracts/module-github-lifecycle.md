# Module Contract: `services/integrations/github/lifecycle.py` and `uninstall.py`

Handlers for the `installation` and `installation_repositories` webhook events, plus the shared `_disable_installation_github` chokepoint that both inbound webhooks and the outbound client converge upon.

## `services/integrations/github/lifecycle.py`

### Public surface

```python
async def dispatch(
    *,
    payload: Mapping[str, Any],
    event_type: str,                  # 'installation' | 'installation_repositories'
    tenant_id: UUID,
    installation_row_id: UUID,
    pool: asyncpg.Pool,
    metrics: LifecycleMetrics | None = None,
) -> dict[str, Any]:
    """Dispatch on (event_type, action). Returns the JSON body the
    webhook router will return with HTTP 200.

    Raises:
      ValidationError(reason='unsupported_lifecycle_action')
        — action not in the supported set for the event_type
      LifecycleHandlerError(reason='...')
        — internal failure (DB write failed, etc.)
    """
```

### Dispatch table

| event_type | action | Effect | DB writes | Returns |
|---|---|---|---|---|
| `installation` | `created` | No-op if row exists; raise `unknown_installation` if not | 0 (row was written by OAuth callback) | `{"handled": "installation_created"}` |
| `installation` | `deleted` | `_disable_installation_github(reason='installation_deleted_webhook')` | 1 UPDATE + 1 INSERT audit | `{"handled": "installation_deleted"}` |
| `installation` | `suspend` | Same as `deleted` in effect (`enabled=FALSE`) but audit `action='suspend'` | 1 UPDATE + 1 INSERT audit | `{"handled": "installation_suspend"}` |
| `installation` | `unsuspend` | `UPDATE provider_installations SET enabled=TRUE` + audit `action='unsuspend'` | 1 UPDATE + 1 INSERT audit | `{"handled": "installation_unsuspend"}` |
| `installation_repositories` | `added` | Merge `payload.repositories_added[].full_name` into `selected_repositories` (or seed from `[]` if was NULL and `repository_selection='selected'`) | 1 UPDATE + 1 INSERT audit | `{"handled": "installation_repositories_added", "count": N}` |
| `installation_repositories` | `removed` | Remove `payload.repositories_removed[].full_name` from `selected_repositories` | 1 UPDATE + 1 INSERT audit | `{"handled": "installation_repositories_removed", "count": N}` |

### Idempotency

- `installation.deleted` on an already-disabled row: UPDATE returns 0 rows changed but emits the audit row anyway (lock-free double-fire posture from IN-09). Test `test_lifecycle_dispatch_idempotent`.
- `installation_repositories.added` for a repo already in the list: no-op on the list; audit row written (the customer's intent — re-affirmation — is recorded).
- `installation_repositories.removed` for a repo not in the list: no-op on the list; audit row written.

### Repository selection mode flip

If a delivery's payload root carries `repository_selection='all'` AND the installation row's `selected_repositories` is non-NULL → set the column to NULL.
If `repository_selection='selected'` AND the column is NULL → seed it to `[]` before applying the added/removed merge.

### Error class

```python
class LifecycleHandlerError(CompanyOSError):
    default_code = "github_lifecycle_error"
    # context: {reason, event_type, action}
```

`ValidationError` (existing) is raised for `unsupported_lifecycle_action` so the router maps to 400 the same way other unsupported events are mapped.

### Metrics

- `github_webhook_lifecycle_total{event,action}` (counter) per FR-017.

### Logging

Per FR-016: log line carries `installation_row_id`, `tenant_id`, `event_type`, `action`. NEVER `account.login`, `account.id`, or raw `installation_id`.

## `services/integrations/github/uninstall.py`

### Public surface

```python
async def _disable_installation_github(
    pool: asyncpg.Pool,
    installation_row_id: UUID,
    *,
    reason: str,                       # 'installation_deleted_webhook' | 'installation_suspend_webhook' | 'outbound_401_or_404_chokepoint'
    audit_action: str = 'uninstall',   # caller can pass 'suspend' for the suspend path
    audit_status: str = 'ok',
    installation_token_cache: dict | None = None,  # invalidate the cached token if present
) -> None:
    """Atomic: UPDATE row enabled=FALSE; INSERT audit_log row;
    invalidate cached installation access token (if cache provided
    and the entry exists).

    Idempotent on the row: double-fire from inbound + outbound is a
    designed property (lock-free, no row lock taken).

    Does NOT touch any secret in encrypted_secrets — the App-level
    webhook secret is shared across all tenants and must outlive any
    single uninstall (FR-012, distinct from IN-08/IN-09).

    Raises:
      InstallationNotFoundError — installation_row_id does not exist
                                  (defensive; in practice the caller
                                  already verified existence)
    """
```

### Invariants

- The UPDATE runs without `FOR UPDATE` / `SELECT FOR UPDATE`. Two concurrent invocations both win their UPDATEs (Postgres serializes them) and both write audit rows; double-audit is the documented cost of correctness without locking.
- The audit row's `context JSONB` includes `reason`. Dashboards can `SELECT DISTINCT ON (installation_row_id, audit_action)` to deduplicate for "unique uninstall events" counts.
- Test `test_uninstall_github.py::test_concurrent_uninstall_is_idempotent` runs two `asyncio.gather`-parallel calls and asserts ≤ 2 audit rows, `enabled=FALSE`, no exception.

### Convergence point

Both the inbound webhook path (`lifecycle.dispatch` on `('installation', 'deleted')`) AND the outbound chokepoint (`GithubClient._maybe_disable_on_revocation`) call this same function. The `reason` argument is the audit-context discriminant — it records which path fired.

### Logging

Single INFO log line on entry: `github_uninstall_chokepoint installation_row_id=<uuid> reason=<...>`. No raw `installation_id`, no token material.

## Tests

Co-located in `services/integrations/tests/test_lifecycle_github.py` and `test_uninstall_github.py`.

Test coverage matrix:

| Test | Scenario |
|---|---|
| `test_lifecycle_installation_created_existing_row_noop` | row exists → no-op |
| `test_lifecycle_installation_created_missing_row_raises` | row absent → ValidationError; 401-equivalent return body |
| `test_lifecycle_installation_deleted_disables_row` | enabled flips to FALSE; secret untouched |
| `test_lifecycle_installation_suspend_then_unsuspend_roundtrip` | suspend → FALSE → unsuspend → TRUE |
| `test_lifecycle_installation_repositories_added_seeds_from_null` | NULL → ['org/a'] after `added` with `repository_selection='selected'` |
| `test_lifecycle_installation_repositories_added_idempotent` | re-add same repo → list unchanged, audit row written |
| `test_lifecycle_installation_repositories_removed_drops_repo` | ['org/a','org/b'] → ['org/b'] after remove org/a |
| `test_lifecycle_repository_selection_flip_to_all` | non-NULL → NULL when payload says all |
| `test_uninstall_chokepoint_idempotent_under_race` | concurrent inbound + outbound → enabled=FALSE; 2 audit rows; no exception |
| `test_uninstall_chokepoint_invalidates_token_cache` | with token cache entry → entry removed after chokepoint |
| `test_uninstall_chokepoint_does_not_delete_app_secret` | encrypted_secrets row for github_app:webhook_secret still present after chokepoint |
