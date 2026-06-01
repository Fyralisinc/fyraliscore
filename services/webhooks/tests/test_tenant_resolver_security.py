"""Integration tests for the security-critical properties of the
tenant resolver.

Three concerns:
  1. Disabled and never-registered installations produce externally
     indistinguishable outcomes (SC-003, FR-005).
  2. Log lines never contain the installation_id verbatim (SC-008,
     FR-015).
  3. Row-level security blocks cross-tenant reads of the
     `provider_installations` table (US-6, SC-006-RLS, FR-012).
"""
from __future__ import annotations

import hashlib
import json
import time

import asyncpg
import pytest
from structlog.testing import capture_logs

from lib.shared.ids import uuid7
from services.webhooks.tenant_resolver import (
    InstallationCache,
    RegisterInstallationRequest,
    TenantResolverDeps,
    build_tenant_resolver,
    noop_metrics,
)


pytestmark = pytest.mark.integration


async def _seed_tenant(pool: asyncpg.Pool):
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
# SC-003 — disabled vs never-registered are byte-equal
# =====================================================================

async def test_disabled_and_never_registered_are_byte_equal(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_HASH_DISABLED",
        )
    )
    await resolver.disable_installation(installation.id)

    disabled = await resolver.resolve(
        "slack", {"team_id": "T_HASH_DISABLED"}, {}
    )
    never = await resolver.resolve(
        "slack", {"team_id": "T_HASH_NEVER"}, {}
    )
    # Canonicalize the outcome JSON to hash it; if hashes match, the
    # caller cannot distinguish the two cases byte-for-byte.
    disabled_canon = json.dumps(disabled.model_dump(), sort_keys=True, default=str)
    never_canon = json.dumps(never.model_dump(), sort_keys=True, default=str)
    assert hashlib.sha256(disabled_canon.encode()).digest() == \
           hashlib.sha256(never_canon.encode()).digest()


# =====================================================================
# SC-008 — installation_id never appears in log lines
# =====================================================================

async def test_log_records_never_contain_installation_id(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    secret_id_in = "T_SECRET_PROBE_VALUE_QXYZ"
    await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id=secret_id_in,
        )
    )

    # structlog's capture_logs intercepts log records emitted via the
    # structlog configuration. Run a few resolves to populate.
    with capture_logs() as cap:
        await resolver.resolve("slack", {"team_id": secret_id_in}, {})
        await resolver.resolve("slack", {"team_id": "T_OTHER_PROBE"}, {})
        await resolver.resolve("slack", {}, {})  # payload missing path

    # Scan every captured event dict; reject if our installation_id
    # appears anywhere.
    for event in cap:
        flat = json.dumps(event, default=str)
        assert secret_id_in not in flat, (
            f"installation_id leaked in log: {event!r}"
        )
        assert "T_OTHER_PROBE" not in flat


# =====================================================================
# US-6 / FR-012 — RLS structural correctness
#
# The dev DB user (`company_os`) is a Postgres superuser, which means
# RLS is bypassed at the engine level *regardless* of `FORCE ROW LEVEL
# SECURITY`. This is a pre-existing property of every integration test
# in the repo (see `test_signal_readings_sidecar.py::
# test_rls_blocks_cross_tenant_select` for the same constraint).
#
# Per Constitution §III: "hand-rolled `WHERE tenant_id = $1` remains
# authoritative and required" until the strict-RLS migration flips
# things. Until then, what IS testable for a new tenant-scoped table
# is the *structural correctness* of the RLS attachment — table is
# in the regime, FORCE is set, the policy is the canonical
# `tenant_isolation` shape, and the resolver's own SQL never returns
# cross-tenant rows when a tenant has only its own installations.
# =====================================================================

async def test_rls_structural_correctness(
    fresh_db: asyncpg.Pool,
) -> None:
    """The table participates in the tenant isolation regime."""
    row = await fresh_db.fetchrow(
        """
        SELECT relrowsecurity, relforcerowsecurity
          FROM pg_class
         WHERE relname = 'provider_installations'
        """
    )
    assert row is not None
    assert row["relrowsecurity"] is True, "RLS must be ENABLED on provider_installations"
    assert row["relforcerowsecurity"] is True, "FORCE must be set on provider_installations"

    policy = await fresh_db.fetchrow(
        """
        SELECT policyname, cmd, qual, with_check
          FROM pg_policies
         WHERE schemaname = 'public'
           AND tablename = 'provider_installations'
        """
    )
    assert policy is not None, "tenant_isolation policy missing"
    assert policy["policyname"] == "tenant_isolation"
    # ALL covers both USING (read) and WITH CHECK (write) — the
    # canonical migration-0036 shape.
    assert policy["cmd"] == "ALL"
    # Sanity-check policy expression mentions the right setting key
    # and the tenant_id column. Format-tolerant — the exact SQL
    # rendering can vary by version.
    qual = (policy["qual"] or "").lower()
    with_check = (policy["with_check"] or "").lower()
    assert "app.current_tenant" in qual
    assert "tenant_id" in qual
    assert "app.current_tenant" in with_check
    assert "tenant_id" in with_check


async def test_resolver_select_never_returns_cross_tenant_rows(
    fresh_db: asyncpg.Pool,
) -> None:
    """The resolver's hand-rolled lookup SQL is keyed by (provider,
    installation_id), and the UNIQUE constraint guarantees one row
    per pair. Even though superuser bypasses RLS in the dev env,
    the application code cannot accidentally return tenant_b's row
    when only tenant_a's installation_id is queried.
    """
    tenant_a = await _seed_tenant(fresh_db)
    tenant_b = await _seed_tenant(fresh_db)
    resolver = _build_resolver(fresh_db)
    await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_a,
            installation_id="T_AAA",
        )
    )
    await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_b,
            installation_id="T_BBB",
        )
    )
    # Querying T_AAA must yield tenant_a; querying T_BBB must yield
    # tenant_b. Cross-leakage would require a bug in the SQL itself,
    # which this test catches.
    out_a = await resolver.resolve("slack", {"team_id": "T_AAA"}, {})
    out_b = await resolver.resolve("slack", {"team_id": "T_BBB"}, {})
    assert hasattr(out_a, "tenant_id") and out_a.tenant_id == tenant_a  # type: ignore[union-attr]
    assert hasattr(out_b, "tenant_id") and out_b.tenant_id == tenant_b  # type: ignore[union-attr]
    # Sanity: tenant_a's lookup must NOT have returned tenant_b's id.
    assert out_a.tenant_id != tenant_b  # type: ignore[union-attr]
    assert out_b.tenant_id != tenant_a  # type: ignore[union-attr]
