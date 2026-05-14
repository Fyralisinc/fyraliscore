"""Integration tests for the IN-08 DB-backed `load_secrets` path.

Covers:
  - FR-001..FR-005 (secret store resolution)
  - SC-002 (no plaintext in env in prod)
  - SC-008 (env-var fallback still reachable in dev)

`load_secrets` becomes async in IN-08; we test the new contract:
  load_secrets(provider, tenant_id, *, app_state=None) -> list[Secret]

When `app_state` is provided AND the (provider, tenant_id) maps to a
`provider_installations` row with a populated `secret_ref`, the secret
plaintext comes from `app_state.secret_store.get(...)`. Otherwise the
function falls through to the legacy env-var path IF and ONLY IF
`WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` is set.

`assert_prod_safety_invariants()` is a separate startup helper that
fails fast when `FYRALIS_ENV=prod` and the fallback flag is also set —
this prevents an accidental prod misconfiguration from silently
allowing tenant secrets to live in environment variables.
"""
from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.webhooks.secrets import (
    assert_prod_safety_invariants,
    load_secrets,
)
from cryptography.fernet import Fernet


pytestmark = pytest.mark.integration


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name, created_at) VALUES ($1, $2, now())",
        tid,
        f"secrets_test_{tid}",
    )
    return tid


async def _seed_installation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    provider: str,
    secret_ref: str | None,
    enabled: bool = True,
) -> UUID:
    row_id = uuid7()
    await pool.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        row_id,
        tenant_id,
        provider,
        f"T_{row_id}",
        secret_ref,
        enabled,
    )
    return row_id


def _make_app_state(pool: asyncpg.Pool) -> SimpleNamespace:
    """Mimics the FastAPI `app.state` namespace for tests that don't
    spin up the gateway. Holds the `pool` and a `secret_store`."""
    return SimpleNamespace(
        pool=pool,
        secret_store=FernetSecretStore(pool, master_kek=Fernet.generate_key()),
    )


async def test_load_secrets_resolves_via_secret_store(
    fresh_db: asyncpg.Pool,
) -> None:
    """FR-001/FR-004: load_secrets reads provider_installations.secret_ref
    and returns the plaintext via the secret store."""
    app_state = _make_app_state(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    plaintext = b"shhh-the-slack-signing-secret"
    ref = await app_state.secret_store.put(
        plaintext, label="slack_signing_secret:app", tenant_id=tenant,
    )
    await _seed_installation(fresh_db, tenant, "slack", secret_ref=ref)

    secrets = await load_secrets("slack", tenant, app_state=app_state)

    assert len(secrets) == 1
    s = secrets[0]
    assert s.provider == "slack"
    assert s.value == plaintext.decode()
    assert s.tenant_id == str(tenant)


async def test_load_secrets_env_fallback_off_returns_empty(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-005: with fallback flag OFF and no DB ref, returns []
    (rather than reading from env)."""
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "fallback-value")
    monkeypatch.delenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", raising=False)
    app_state = _make_app_state(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    # installation row exists but secret_ref is NULL — no DB resolution possible
    await _seed_installation(fresh_db, tenant, "slack", secret_ref=None)

    secrets = await load_secrets("slack", tenant, app_state=app_state)

    assert secrets == []


async def test_load_secrets_env_fallback_on_uses_env(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-005: with fallback flag ON and no DB ref, falls through to
    the legacy env-var path so dev loops are not broken."""
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "fallback-value")
    app_state = _make_app_state(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    # No installation row → only env-var path can supply a secret.

    secrets = await load_secrets("slack", tenant, app_state=app_state)

    assert len(secrets) == 1
    assert secrets[0].value == "fallback-value"


async def test_load_secrets_db_ref_wins_over_env(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When both paths are configured, the DB ref takes precedence
    (the env-var path is a strict fallback, not a primary)."""
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "this-should-not-be-used")
    app_state = _make_app_state(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    ref = await app_state.secret_store.put(
        b"db-wins", label="slack_signing_secret:app", tenant_id=tenant,
    )
    await _seed_installation(fresh_db, tenant, "slack", secret_ref=ref)

    secrets = await load_secrets("slack", tenant, app_state=app_state)

    assert len(secrets) == 1
    assert secrets[0].value == "db-wins"


async def test_load_secrets_no_app_state_uses_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy callers that don't pass app_state still get the env-var
    path. This preserves any callers we haven't migrated yet."""
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "legacy-env")
    tenant = uuid7()

    secrets = await load_secrets("slack", tenant)

    assert len(secrets) == 1
    assert secrets[0].value == "legacy-env"


async def test_load_secrets_disabled_installation_returns_empty(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disabled provider_installations row must NOT yield secrets,
    even if it has a populated secret_ref (matches resolver behavior:
    enabled=false → unknown_installation)."""
    monkeypatch.delenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", raising=False)
    app_state = _make_app_state(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    ref = await app_state.secret_store.put(
        b"stale-but-present", label="slack_signing_secret:app", tenant_id=tenant,
    )
    await _seed_installation(
        fresh_db, tenant, "slack", secret_ref=ref, enabled=False,
    )

    secrets = await load_secrets("slack", tenant, app_state=app_state)

    assert secrets == []


# ---------------------------------------------------------------------
# assert_prod_safety_invariants — startup helper
# ---------------------------------------------------------------------


def test_assert_prod_safety_invariants_passes_when_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    # Should NOT raise.
    assert_prod_safety_invariants()


def test_assert_prod_safety_invariants_raises_when_prod_and_fallback_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")
    with pytest.raises(RuntimeError, match="WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW"):
        assert_prod_safety_invariants()


def test_assert_prod_safety_invariants_allows_in_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In non-prod, the flag is permitted (and indeed expected)."""
    monkeypatch.setenv("FYRALIS_ENV", "dev")
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")
    # Should NOT raise.
    assert_prod_safety_invariants()
