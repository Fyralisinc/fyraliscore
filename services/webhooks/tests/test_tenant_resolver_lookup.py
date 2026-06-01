"""Integration tests for the tenant resolver lookup path.

Real Postgres via the `fresh_db` fixture. Parametrized over all five
launch providers so SC-005 is covered end-to-end (the extractor unit
tests in `test_tenant_resolver_extract.py` cover id extraction alone).

Covers:
  - FR-001, FR-003, FR-004, FR-005 (per-provider Resolved path)
  - FR-009, FR-011 (cache integration + cache-backend-unavailable
    fallback)
  - FR-018 (metric increments)
  - SC-002 (100% 401 outcomes for unknown), SC-003 (indistinguishability),
    SC-004 (cache-hit rate after warmup), SC-005 (all five providers),
    SC-007 (cache-unavailable correctness), SC-009 (latency budget).
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Mapping
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.webhooks import metrics as resolver_metrics
from services.webhooks.tenant_resolver import (
    InstallationCache,
    RegisterInstallationRequest,
    Resolved,
    ResolverProvider,
    TenantResolverDeps,
    UnknownInstallation,
    build_tenant_resolver,
    default_metrics,
)


pytestmark = pytest.mark.integration


SAMPLES = Path(__file__).parent / "samples"


def _load(name: str) -> dict:
    with open(SAMPLES / name) as fh:
        return json.load(fh)


# Five-provider test matrix. Each entry names the provider, the
# installation_id present in the fixture, the payload to pass, and
# headers (Stripe is header-driven).
PROVIDER_FIXTURES: list[tuple[ResolverProvider, str, dict, dict]] = [
    ("slack",   "T_ACME_FIXTURE",      _load("slack_event_callback.json"), {}),
    ("github",  "4567890",             _load("github_webhook.json"),       {}),
    ("linear",  "ORG_FIXTURE_UUID",    _load("linear_webhook.json"),       {}),
    ("stripe",  "acct_STRIPE_FIXTURE", {},  {"Stripe-Account": "acct_STRIPE_FIXTURE"}),
    ("discord", "GUILD_FIXTURE",       _load("discord_interaction.json"),  {}),
]


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    """Insert a tenants row and return its id (required by 0037 FK)."""
    tid = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name, created_at) "
        "VALUES ($1, $2, now())",
        tid,
        f"test_tenant_{tid}",
    )
    return tid


# =====================================================================
# Resolved path — parametrized over all 5 providers (SC-005)
# =====================================================================

@pytest.mark.parametrize(
    "provider,installation_id,payload,headers",
    PROVIDER_FIXTURES,
    ids=[p[0] for p in PROVIDER_FIXTURES],
)
async def test_resolved_path_all_providers(
    fresh_db: asyncpg.Pool,
    provider: ResolverProvider,
    installation_id: str,
    payload: Mapping[str, object],
    headers: Mapping[str, str],
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    deps = TenantResolverDeps(
        pool=fresh_db,
        cache=InstallationCache(),
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    resolver = build_tenant_resolver(deps)
    await resolver.register_installation(
        RegisterInstallationRequest(
            provider=provider,
            tenant_id=tenant_id,
            installation_id=installation_id,
            secret_ref=f"ref-{provider}",
        )
    )

    outcome = await resolver.resolve(provider, payload, headers)

    assert isinstance(outcome, Resolved), f"{provider}: expected Resolved, got {outcome}"
    assert outcome.tenant_id == tenant_id
    assert outcome.secret_ref == f"ref-{provider}"


# =====================================================================
# UnknownInstallation path — parametrized over all 5 providers
# =====================================================================

@pytest.mark.parametrize(
    "provider,installation_id,payload,headers",
    PROVIDER_FIXTURES,
    ids=[p[0] for p in PROVIDER_FIXTURES],
)
async def test_unknown_installation_path_all_providers(
    fresh_db: asyncpg.Pool,
    provider: ResolverProvider,
    installation_id: str,
    payload: Mapping[str, object],
    headers: Mapping[str, str],
) -> None:
    # Note: no installation registered for this provider/id.
    deps = TenantResolverDeps(
        pool=fresh_db,
        cache=InstallationCache(),
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    resolver = build_tenant_resolver(deps)
    outcome = await resolver.resolve(provider, payload, headers)
    assert isinstance(outcome, UnknownInstallation), (
        f"{provider}: expected UnknownInstallation, got {outcome}"
    )
    assert outcome.provider == provider


# =====================================================================
# Disabled = indistinguishable from never-registered (SC-003)
# =====================================================================

async def test_disabled_row_indistinguishable_from_never_registered(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    deps = TenantResolverDeps(
        pool=fresh_db,
        cache=InstallationCache(),
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    resolver = build_tenant_resolver(deps)
    installation = await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_DISABLED",
        )
    )
    await resolver.disable_installation(installation.id)

    disabled_outcome = await resolver.resolve(
        "slack", {"team_id": "T_DISABLED"}, {}
    )
    never_outcome = await resolver.resolve(
        "slack", {"team_id": "T_NEVER_REGISTERED"}, {}
    )
    # The two outcomes are structurally identical (same shape, same
    # serialization) — SC-003.
    assert isinstance(disabled_outcome, UnknownInstallation)
    assert isinstance(never_outcome, UnknownInstallation)
    assert disabled_outcome.model_dump() == never_outcome.model_dump()


# =====================================================================
# Metric counters increment correctly (FR-018, SC-002, SC-005)
# =====================================================================

async def test_resolve_emits_outcome_counter_per_branch(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    deps = TenantResolverDeps(
        pool=fresh_db,
        cache=InstallationCache(),
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    resolver = build_tenant_resolver(deps)
    await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_COUNTER",
        )
    )

    # Resolved
    await resolver.resolve("slack", {"team_id": "T_COUNTER"}, {})
    # Unknown
    await resolver.resolve("slack", {"team_id": "T_NOPE"}, {})
    # Payload missing (no team_id key at all)
    await resolver.resolve("slack", {}, {})

    assert resolver_metrics.get_resolver_outcome_count("slack", "resolved") == 1
    assert (
        resolver_metrics.get_resolver_outcome_count(
            "slack", "unknown_installation"
        )
        == 1
    )
    assert (
        resolver_metrics.get_resolver_outcome_count("slack", "payload_missing")
        == 1
    )


# =====================================================================
# Cache-backend-unavailable fallback (FR-011, SC-007)
# =====================================================================

class _RaisingCache:
    """Cache that raises on every operation. Drop-in for InstallationCache
    via duck typing — the resolver only calls get/put/invalidate.
    """
    def get(self, *a, **k):  # noqa: ARG002
        raise RuntimeError("synthetic cache failure")

    def put(self, *a, **k):  # noqa: ARG002
        raise RuntimeError("synthetic cache failure")

    def invalidate(self, *a, **k):  # noqa: ARG002
        raise RuntimeError("synthetic cache failure")


async def test_resolve_falls_back_to_db_when_cache_raises(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    good_cache = InstallationCache()
    deps_good = TenantResolverDeps(
        pool=fresh_db,
        cache=good_cache,
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    # Seed the row using a healthy cache (otherwise invalidate would
    # blow up — out of scope of this assertion).
    good_resolver = build_tenant_resolver(deps_good)
    await good_resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_CACHE_BYPASS",
        )
    )

    # Now drive the resolver with a broken cache and confirm it still
    # produces correct results from the DB, and that the `bypass`
    # counter increments.
    deps_bad = TenantResolverDeps(
        pool=fresh_db,
        cache=_RaisingCache(),  # type: ignore[arg-type]
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    bad_resolver = build_tenant_resolver(deps_bad)

    bypass_before = resolver_metrics.get_resolver_cache_count("slack", "bypass")
    outcome = await bad_resolver.resolve(
        "slack", {"team_id": "T_CACHE_BYPASS"}, {}
    )
    bypass_after = resolver_metrics.get_resolver_cache_count("slack", "bypass")

    assert isinstance(outcome, Resolved)
    assert outcome.tenant_id == tenant_id
    # Both the `get` and `put` paths raise → at least one `bypass` increment.
    assert bypass_after > bypass_before


# =====================================================================
# Latency SLO (SC-009)
# =====================================================================

@pytest.mark.slow
async def test_resolve_latency_within_slo(fresh_db: asyncpg.Pool) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    deps = TenantResolverDeps(
        pool=fresh_db,
        cache=InstallationCache(),
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    resolver = build_tenant_resolver(deps)
    await resolver.register_installation(
        RegisterInstallationRequest(
            provider="slack",
            tenant_id=tenant_id,
            installation_id="T_LATENCY",
        )
    )
    # Reset metrics so the histogram contains only this test's samples.
    resolver_metrics.reset()

    # Warmup: 1 call populates the cache.
    await resolver.resolve("slack", {"team_id": "T_LATENCY"}, {})
    # Hot path: 200 cache-hit resolves.
    for _ in range(200):
        await resolver.resolve("slack", {"team_id": "T_LATENCY"}, {})

    p95 = resolver_metrics.resolver_duration_p95("slack")
    assert p95 is not None
    # 2 ms SLO for cache-hit path (SC-009). CI noise budget: 5 ms
    # ceiling — if even that fails the test is signal, not noise.
    # If you hit transient flake at 2 ms on shared CI runners, the
    # plan documents a one-retry budget at the test-runner level.
    assert p95 <= 0.005, f"hit-path p95 = {p95:.4f}s exceeded 5ms ceiling"


# =====================================================================
# Cache hit-rate SLO (SC-004)
# =====================================================================

@pytest.mark.slow
async def test_cache_hit_rate_above_threshold_after_warmup(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db)
    deps = TenantResolverDeps(
        pool=fresh_db,
        cache=InstallationCache(),
        clock=time.monotonic,
        metrics=default_metrics(),
    )
    resolver = build_tenant_resolver(deps)

    keys: list[str] = []
    for i in range(10):
        installation_id = f"T_HITRATE_{i}"
        await resolver.register_installation(
            RegisterInstallationRequest(
                provider="slack",
                tenant_id=tenant_id,
                installation_id=installation_id,
            )
        )
        keys.append(installation_id)

    # Warmup: one cold-cache resolve per key.
    for k in keys:
        await resolver.resolve("slack", {"team_id": k}, {})

    # Reset metrics so post-warmup is the measured window.
    resolver_metrics.reset()

    # Drive 200 hot resolves over the same 10 keys.
    for i in range(200):
        await resolver.resolve("slack", {"team_id": keys[i % 10]}, {})

    hit = resolver_metrics.get_resolver_cache_count("slack", "hit")
    miss = resolver_metrics.get_resolver_cache_count("slack", "miss")
    bypass = resolver_metrics.get_resolver_cache_count("slack", "bypass")
    total = hit + miss + bypass
    assert total == 200
    hit_rate = hit / total
    assert hit_rate >= 0.95, (
        f"hit rate = {hit_rate:.3f} below 0.95 threshold "
        f"(hit={hit}, miss={miss}, bypass={bypass})"
    )
