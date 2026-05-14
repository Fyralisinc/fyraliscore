"""Integration tests for FernetSecretStore.

Every test runs against real Postgres on localhost:5433 (Constitution
§IV; no DB mocks). Test plan mirrors
specs/IN-08-slack-production-integration/contracts/module-secret-store.md.
"""
from __future__ import annotations

from uuid import UUID

import asyncpg
import pytest
from cryptography.fernet import Fernet

from lib.shared.errors import SecretNotFoundError, SecretStoreError
from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore, build_secret_store


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------

async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name, created_at) VALUES ($1, $2, now())",
        tid,
        f"secrets_test_{tid}",
    )
    return tid


def _make_store(pool: asyncpg.Pool) -> FernetSecretStore:
    return FernetSecretStore(pool, master_kek=Fernet.generate_key())


# ---------------------------------------------------------------------
# put
# ---------------------------------------------------------------------

async def test_put_returns_uuid_ref(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)

    ref = await store.put(b"secret-1", label="slack_bot_token:T1", tenant_id=tenant)

    parsed = UUID(ref)
    # uuid7 has version nibble 7 — verify time-orderable shape.
    assert parsed.version == 7


async def test_put_rejects_empty_label(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)

    with pytest.raises(ValueError):
        await store.put(b"x", label="", tenant_id=tenant)


# ---------------------------------------------------------------------
# get
# ---------------------------------------------------------------------

async def test_get_after_put_roundtrip(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    plaintext = b"xoxb-secret-bot-token-AAA"

    ref = await store.put(plaintext, label="slack_bot_token:T2", tenant_id=tenant)
    got = await store.get(ref, tenant_id=tenant)

    assert got == plaintext


async def test_get_unknown_ref_raises_not_found(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    fake_ref = str(uuid7())

    with pytest.raises(SecretNotFoundError):
        await store.get(fake_ref, tenant_id=tenant)


async def test_get_wrong_tenant_raises_not_found(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant_a = await _seed_tenant(fresh_db)
    tenant_b = await _seed_tenant(fresh_db)

    ref = await store.put(b"a-secret", label="slack_bot_token:T3", tenant_id=tenant_a)

    # Cross-tenant get returns the same shape as never-existed.
    with pytest.raises(SecretNotFoundError):
        await store.get(ref, tenant_id=tenant_b)


async def test_get_invalid_ref_raises_value_error(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)

    with pytest.raises(ValueError):
        await store.get("not-a-uuid", tenant_id=tenant)


# ---------------------------------------------------------------------
# rotate
# ---------------------------------------------------------------------

async def test_rotate_preserves_ref(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    ref = await store.put(b"old-token", label="slack_bot_token:T4", tenant_id=tenant)

    await store.rotate(ref, b"new-token", tenant_id=tenant)
    got = await store.get(ref, tenant_id=tenant)

    assert got == b"new-token"
    # rotated_at populated.
    row = await fresh_db.fetchrow(
        "SELECT rotated_at FROM encrypted_secrets WHERE id = $1", UUID(ref),
    )
    assert row is not None and row["rotated_at"] is not None


async def test_rotate_unknown_ref_raises_not_found(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)

    with pytest.raises(SecretNotFoundError):
        await store.rotate(str(uuid7()), b"x", tenant_id=tenant)


# ---------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------

async def test_delete_then_get_raises_not_found(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    ref = await store.put(b"will-be-deleted", label="slack_bot_token:T5", tenant_id=tenant)

    await store.delete(ref, tenant_id=tenant)

    with pytest.raises(SecretNotFoundError):
        await store.get(ref, tenant_id=tenant)


async def test_delete_unknown_is_noop(fresh_db: asyncpg.Pool) -> None:
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    # Random uuid that was never inserted — must not raise.
    await store.delete(str(uuid7()), tenant_id=tenant)


# ---------------------------------------------------------------------
# Backend failure modes
# ---------------------------------------------------------------------

async def test_decrypt_failure_raises_store_error(fresh_db: asyncpg.Pool) -> None:
    """Corrupt the ciphertext directly; get must surface SecretStoreError."""
    store = _make_store(fresh_db)
    tenant = await _seed_tenant(fresh_db)
    ref = await store.put(b"original", label="slack_bot_token:T6", tenant_id=tenant)

    await fresh_db.execute(
        "UPDATE encrypted_secrets SET ciphertext = $1 WHERE id = $2",
        b"this is not a valid Fernet token",
        UUID(ref),
    )

    with pytest.raises(SecretStoreError):
        await store.get(ref, tenant_id=tenant)


async def test_invalid_kek_raises_store_error(fresh_db: asyncpg.Pool) -> None:
    """A malformed MASTER_KEK fails-fast at construction."""
    with pytest.raises(SecretStoreError):
        FernetSecretStore(fresh_db, master_kek=b"not-base64-and-wrong-length")


# ---------------------------------------------------------------------
# RLS isolation
# ---------------------------------------------------------------------

async def test_rls_policy_is_registered(fresh_db: asyncpg.Pool) -> None:
    """SC-010: encrypted_secrets has ENABLE+FORCE RLS with the
    `tenant_isolation` policy installed.

    Note: in this dev environment, the connecting role (`company_os`)
    is a Postgres superuser, which bypasses RLS regardless of FORCE.
    The runtime defense-in-depth lives in production where the app
    connects as a non-superuser role. This test verifies that the
    policy declaration is in place; functional isolation is exercised
    by the existing IN-07 RLS conformance suite under a non-superuser
    role.
    """
    flags = await fresh_db.fetchrow(
        "SELECT relrowsecurity, relforcerowsecurity "
        "FROM pg_class WHERE relname = 'encrypted_secrets'",
    )
    assert flags is not None
    assert flags["relrowsecurity"] is True
    assert flags["relforcerowsecurity"] is True

    policies = await fresh_db.fetch(
        "SELECT polname FROM pg_policy p "
        "JOIN pg_class c ON c.oid = p.polrelid "
        "WHERE c.relname = 'encrypted_secrets'",
    )
    policy_names = {r["polname"] for r in policies}
    assert "tenant_isolation" in policy_names


# ---------------------------------------------------------------------
# build_secret_store factory
# ---------------------------------------------------------------------

async def test_build_secret_store_dev_warns_on_missing_kek(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MASTER_KEK", raising=False)
    monkeypatch.delenv("FYRALIS_ENV", raising=False)

    with pytest.warns(UserWarning, match="MASTER_KEK"):
        store = build_secret_store(fresh_db)

    tenant = await _seed_tenant(fresh_db)
    # Generated key still works.
    ref = await store.put(b"x", label="test", tenant_id=tenant)
    assert await store.get(ref, tenant_id=tenant) == b"x"


async def test_build_secret_store_prod_missing_kek_raises(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MASTER_KEK", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "prod")

    with pytest.raises(SecretStoreError):
        build_secret_store(fresh_db)
