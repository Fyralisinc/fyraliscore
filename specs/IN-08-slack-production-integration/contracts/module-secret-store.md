# Module Contract — `lib.shared.secrets`

Python API for the envelope-encrypted secret store. The backing table is `encrypted_secrets` (see `data-model.md` §1).

## Public surface

```python
# lib/shared/secrets/__init__.py

from typing import Protocol
from uuid import UUID

from lib.shared.errors import CompanyOSError


class SecretStore(Protocol):
    """Envelope-encrypted secret store. Backed by `encrypted_secrets`."""

    async def put(
        self,
        plaintext: bytes | str,
        *,
        label: str,
        tenant_id: UUID,
    ) -> str:
        """Persist `plaintext` encrypted-at-rest. Returns an opaque ref
        (stringified UUID) callers persist in their domain rows."""

    async def get(
        self,
        ref: str,
        *,
        tenant_id: UUID,
    ) -> bytes:
        """Resolve `ref` to plaintext. Raises `SecretNotFoundError` if
        the ref is unknown for this tenant. Raises `SecretStoreError`
        on backend failure (DB unavailable, KEK rotation in progress)."""

    async def rotate(
        self,
        ref: str,
        new_plaintext: bytes | str,
        *,
        tenant_id: UUID,
    ) -> None:
        """Replace the ciphertext for `ref`. The ref is stable;
        callers do not need to update their domain rows. Raises
        `SecretNotFoundError` if the ref is unknown for this tenant."""

    async def delete(
        self,
        ref: str,
        *,
        tenant_id: UUID,
    ) -> None:
        """Remove the ciphertext row. Tolerant of "already deleted" —
        a no-op if the ref does not exist (caller may have raced)."""


class SecretStoreError(CompanyOSError):
    """Backend-level failure (DB unavailable, KEK invalid, etc.)."""
    code = "secret_store_unavailable"


class SecretNotFoundError(CompanyOSError):
    """The ref is unknown for this tenant — callers should treat this
    as an authentication failure for webhook signature paths."""
    code = "secret_not_found"


def build_secret_store(
    pool: "asyncpg.Pool",
    master_kek_loader: Callable[[], bytes] | None = None,
) -> SecretStore:
    """Construct a production `FernetSecretStore`.

    - `pool`: gateway's asyncpg pool.
    - `master_kek_loader`: callable returning the 32-byte URL-safe-
      base64-encoded Fernet key. Defaults to reading `MASTER_KEK` env
      var once at construction time. The callable form supports
      lifespan-rotation via `MultiFernet` (next-key first, then prior
      keys) without service restart.
    """
    ...
```

### `FernetSecretStore` (concrete implementation)

```python
# lib/shared/secrets/store.py

class FernetSecretStore:
    """Fernet-backed `SecretStore` implementation.

    Construction:
        store = FernetSecretStore(pool, master_kek=MASTER_KEK_bytes)

    Or with rotation support:
        store = FernetSecretStore(pool, multi_fernet=MultiFernet([new_key, old_key]))
    """

    def __init__(
        self,
        pool: "asyncpg.Pool",
        *,
        master_kek: bytes | None = None,
        multi_fernet: "MultiFernet | None" = None,
    ) -> None: ...

    # async def put / get / rotate / delete — see Protocol above
```

## Behavior contract

| Property | Guarantee |
|----------|-----------|
| **Confidentiality** | Plaintext is encrypted with Fernet (AES-128-CBC + HMAC-SHA256) before reaching the DB. The DB never sees the plaintext. |
| **Authenticity** | Fernet ciphertext carries a HMAC; tampering produces a decryption failure (caller observes `SecretStoreError`). |
| **Tenant scoping** | Every operation requires `tenant_id`. The underlying SQL always carries `WHERE tenant_id = $...` even though RLS would also filter. |
| **ID allocation** | Refs are `uuid7()` strings. Time-orderable for log correlation. |
| **Rotation** | `rotate()` replaces the row's ciphertext and bumps `rotated_at`; the `id` (and thus the ref) does NOT change. Callers do not need to update their pointers. |
| **Delete tolerance** | `delete()` on a missing ref is a no-op. Idempotent uninstall paths rely on this. |
| **Concurrency** | Each operation is a single SQL statement; no read-modify-write windows. `rotate()` is atomic. |
| **No partial state** | Each method does exactly one row touch. If the DB call fails, the in-memory state is unchanged and the caller sees `SecretStoreError`. |

## Error model

| Method | `SecretNotFoundError` | `SecretStoreError` | `ValueError` |
|--------|----------------------|--------------------|--------------|
| `put` | n/a (creates) | DB unavailable, KEK invalid | empty `label`, `tenant_id` is `None` |
| `get` | ref unknown for tenant | DB unavailable, decrypt fails | invalid ref format |
| `rotate` | ref unknown for tenant | DB unavailable, KEK invalid | empty new plaintext |
| `delete` | n/a (idempotent) | DB unavailable | invalid ref format |

`SecretNotFoundError` is **distinct** from `SecretStoreError` because they map to different HTTP shapes upstream:

- `SecretNotFoundError` from `get` during webhook signature verification → 401 `unknown_installation` (same shape IN-07 uses for an absent installation).
- `SecretStoreError` from `get` → 503 `secret_store_unavailable`.

## Migration / wiring

- The `MASTER_KEK` env var holds the Fernet key (URL-safe base64, 32 bytes decoded). At gateway startup:
  - Production (`FYRALIS_ENV=prod`): missing/empty `MASTER_KEK` → fail-startup with structured error.
  - Dev (`FYRALIS_ENV` unset or `dev`): missing `MASTER_KEK` → generate a one-shot in-memory key, log a structured warning, continue.
- `build_secret_store(app.state.pool)` is called from the gateway lifespan AFTER pool wiring; the resulting `SecretStore` is attached to `app.state.secret_store`.
- `services/webhooks/secrets.py::load_secrets` reads `provider_installations.secret_ref` and calls `app.state.secret_store.get(ref, tenant_id=tenant_id)`. The env-var path is gated by `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` and is rejected at startup in prod (per `research.md` R4).

## Test plan

| Test | Type | Asserts |
|------|------|---------|
| `test_put_returns_uuid_ref` | unit | Ref is a valid UUID, `uuid7()` (time-ordered). |
| `test_get_after_put_roundtrip` | integration | `get(put(plaintext))` returns the same plaintext byte-for-byte. |
| `test_get_unknown_ref_raises_not_found` | integration | `SecretNotFoundError` on bogus ref. |
| `test_get_wrong_tenant_raises_not_found` | integration | Ref from tenant A, `get(ref, tenant_id=B)` raises `SecretNotFoundError` (not a leak). |
| `test_rotate_preserves_ref` | integration | After `rotate(ref, new)`, `get(ref)` returns `new`. Ref unchanged. |
| `test_delete_then_get_raises_not_found` | integration | After `delete`, `get` raises `SecretNotFoundError`. |
| `test_delete_unknown_is_noop` | integration | `delete(bogus_ref)` does not raise. |
| `test_decrypt_failure_raises_store_error` | unit | Corrupt the ciphertext directly; `get` raises `SecretStoreError`. |
| `test_db_unavailable_raises_store_error` | integration | Close the pool; `get` raises `SecretStoreError`. |
| `test_rls_isolates_tenants` | integration | With `app.current_tenant=A`, raw `SELECT * FROM encrypted_secrets` cannot see tenant-B rows. |
