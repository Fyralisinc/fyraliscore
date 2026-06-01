"""Integration tests for the admin actions on TenantResolver.

Covers FR-002 (uniqueness), FR-007 (register), FR-008 (disable /
re-enable / update-secret-ref), FR-010 (cache invalidation), FR-014
(structured admin errors), SC-001, SC-006 (consistency window),
Clarifications Q1 (secret_ref updatable).
"""
from __future__ import annotations

import time
from uuid import uuid4

import asyncpg
import pytest

from lib.shared.errors import (
    InstallationConflictError,
    InstallationNotFoundError,
)
from lib.shared.ids import uuid7
from services.webhooks.tenant_resolver import (
    InstallationCache,
    RegisterInstallationRequest,
    Resolved,
    TenantResolverDeps,
    UnknownInstallation,
    build_tenant_resolver,
    noop_metrics,
)


pytestmark = pytest.mark.integration


async def _seed_tenant(pool: asyncpg.Pool) -> "UUID":  # type: ignore[name-defined]  # noqa: F821
    from uuid import UUID  # noqa: F401 — local import for forward-ref type

    tid = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name, created_at) "
        "VALUES ($1, $2, now())",
        tid,
        f"test_tenant_{tid}",
    )
    return tid


def _build_resolver(pool: asyncpg.Pool):
    return build_tenant_resolver(
        TenantResolverDeps(
            pool=pool,
            cache=InstallationCache(),
            clock=time.monotonic,
            metrics=noop_metrics(),
        )
    )


# =====================================================================
# register_installation
# =====================================================================

async def test_register_then_resolve_returns_resolved(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)

    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_REG_1",
            secret_ref="ref-v1",
        )
    )
    assert installation.tenant_id == tenant_id
    assert installation.enabled is True
    assert installation.secret_ref == "ref-v1"

    outcome = await resolver.resolve("slack", {"team_id": "T_REG_1"}, {})
    assert isinstance(outcome, Resolved)
    assert outcome.tenant_id == tenant_id


async def test_register_duplicate_raises_conflict(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_a = await _seed_tenant(fresh_db)
    tenant_b = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_a,
            installation_id="T_DUP",
        )
    )
    # Same (provider, installation_id), different tenant → conflict.
    with pytest.raises(InstallationConflictError) as ei:
        await resolver.register_installation(
            RegisterInstallationRequest(
                provider="slack",
                tenant_id=tenant_b,
                installation_id="T_DUP",
            )
        )
    assert ei.value.code == "installation_conflict"
    assert ei.value.context["provider"] == "slack"
    assert ei.value.context["installation_id"] == "T_DUP"


# =====================================================================
# disable / enable
# =====================================================================

async def test_disable_then_resolve_returns_unknown(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_DIS",
        )
    )
    await resolver.disable_installation(installation.id)

    outcome = await resolver.resolve("slack", {"team_id": "T_DIS"}, {})
    assert isinstance(outcome, UnknownInstallation)


async def test_enable_after_disable_restores_resolve(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_TOG",
        )
    )
    await resolver.disable_installation(installation.id)
    await resolver.enable_installation(installation.id)

    outcome = await resolver.resolve("slack", {"team_id": "T_TOG"}, {})
    assert isinstance(outcome, Resolved)


async def test_disable_nonexistent_raises_not_found(
    fresh_db: asyncpg.Pool,
) -> None:
    resolver = _build_resolver(fresh_db)
    with pytest.raises(InstallationNotFoundError) as ei:
        await resolver.disable_installation(uuid4())
    assert ei.value.code == "installation_not_found"


# =====================================================================
# update_secret_ref (Clarification Q1)
# =====================================================================

async def test_update_secret_ref_changes_resolved_value(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_SEC",
            secret_ref="ref-v1",
        )
    )

    # First resolve populates cache with v1.
    outcome = await resolver.resolve("slack", {"team_id": "T_SEC"}, {})
    assert isinstance(outcome, Resolved)
    assert outcome.secret_ref == "ref-v1"

    # Rotate the pointer.
    await resolver.update_secret_ref(installation.id, "ref-v2")

    # Next resolve must see v2 (cache invalidation in update_secret_ref).
    outcome2 = await resolver.resolve("slack", {"team_id": "T_SEC"}, {})
    assert isinstance(outcome2, Resolved)
    assert outcome2.secret_ref == "ref-v2"


async def test_update_secret_ref_to_null(fresh_db: asyncpg.Pool) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_SEC_NULL",
            secret_ref="ref-v1",
        )
    )
    await resolver.update_secret_ref(installation.id, None)
    outcome = await resolver.resolve("slack", {"team_id": "T_SEC_NULL"}, {})
    assert isinstance(outcome, Resolved)
    assert outcome.secret_ref is None


async def test_update_secret_ref_nonexistent_raises_not_found(
    fresh_db: asyncpg.Pool,
) -> None:
    resolver = _build_resolver(fresh_db)
    with pytest.raises(InstallationNotFoundError):
        await resolver.update_secret_ref(uuid4(), "ref-anything")


# =====================================================================
# SC-006 — consistency window: admin action visible to resolver
# within 5 seconds. (Our invalidation is synchronous and in-process,
# so this is far stricter than 5 s; we still assert the budget.)
# =====================================================================

async def test_admin_to_resolve_consistency_under_5_seconds(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)

    t0 = time.monotonic()
    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_CONSIST",
        )
    )
    outcome = await resolver.resolve("slack", {"team_id": "T_CONSIST"}, {})
    elapsed = time.monotonic() - t0

    assert isinstance(outcome, Resolved)
    assert elapsed < 5.0, f"register→resolve took {elapsed:.2f}s"

    # Also: disable is reflected immediately.
    t1 = time.monotonic()
    await resolver.disable_installation(installation.id)
    outcome2 = await resolver.resolve("slack", {"team_id": "T_CONSIST"}, {})
    elapsed2 = time.monotonic() - t1
    assert isinstance(outcome2, UnknownInstallation)
    assert elapsed2 < 5.0
