"""tests/unit/sage/test_discovery_repos.py — Phase 10 discovery repos.

Direct repo tests for `discovery_shortcuts` + `negative_memory` (migration
0052). Lives under tests/unit but touches a real Postgres for the same
reason as test_inquiry_traces_repo.py — these repos are thin wrappers
over SQL and there is no business logic worth mocking. Uses the same
`gateway_pool` fixture (per-test pool + TRUNCATE + auto-tenant-register
trigger) re-exported via services/gateway/tests/conftest.py.

`pytest.mark.integration` keeps them out of any "pure unit" selection
that runs without a database.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.sage.discovery.negative_memory_repo import NegativeMemoryRepo
from services.sage.discovery.shortcuts_repo import (
    FAILURE_DECAY_FACTOR,
    SUCCESS_UTILITY_BUMP,
    DiscoveryShortcutsRepo,
)
from services.sage.discovery.types import (
    DiscoveryShortcut,
    NegativeMemory,
    Signature,
)


# Re-use gateway integration fixtures (per-test pool + fresh DB).
from services.gateway.tests.conftest import (  # noqa: F401
    gateway_pool,
    tenant_id,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _set_tenant(conn: asyncpg.Connection, tenant_id: UUID) -> None:
    """Set RLS tenant scope on the connection."""
    await conn.execute(
        "SELECT set_config('app.current_tenant', $1, true)", str(tenant_id),
    )


def _sig(**kwargs) -> Signature:
    """Convenience constructor for a `Signature`."""
    return Signature(**kwargs)


# =====================================================================
# DiscoveryShortcutsRepo
# =====================================================================


@pytest.mark.asyncio
async def test_shortcuts_find_for_signature_exact_match(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A probe signature that exactly matches the stored signature
    surfaces the shortcut."""
    # NOTE: do NOT call _set_tenant here. `set_config(..., is_local=true)`
    # outside a transaction sets the value to '' (empty string) and that
    # persists across pool release/reacquire, which makes the RLS policy
    # try to cast '' to uuid and crash. RLS is permissive when the
    # setting is NULL, which is the desired state for these unit tests.
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)

    sig = _sig(
        signal_type="enterprise_customer_blocker",
        entities=["customer", "SSO"],
        question_primitive="DEPENDENCY",
    )
    shortcut = await repo.upsert_from_outcome(
        sig, to_affordance="map.region.sso", delta_utility=0.3,
    )
    assert shortcut.utility_score == pytest.approx(0.3)

    found = await repo.find_for_signature(sig)
    assert len(found) == 1
    assert found[0].id == shortcut.id
    assert found[0].to_affordance == "map.region.sso"


@pytest.mark.asyncio
async def test_shortcuts_find_for_signature_partial_match(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A partial probe (only question_primitive) surfaces stored
    shortcuts whose signature is a superset of the probe — the @>
    containment direction is the contract."""
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)

    # Specific shortcut: signature pins all three fields.
    specific = await repo.upsert_from_outcome(
        _sig(
            signal_type="renewal_at_risk",
            entities=["Globex"],
            question_primitive="DEPENDENCY",
        ),
        to_affordance="lookup.commitment",
        delta_utility=0.4,
    )
    # Different question primitive — should NOT surface for DEPENDENCY probe.
    await repo.upsert_from_outcome(
        _sig(
            signal_type="renewal_at_risk",
            entities=["Globex"],
            question_primitive="OWNERSHIP",
        ),
        to_affordance="lookup.actor",
        delta_utility=0.2,
    )

    found = await repo.find_for_signature(
        _sig(question_primitive="DEPENDENCY"),
    )
    found_ids = {row.id for row in found}
    assert specific.id in found_ids
    assert all(
        row.from_signature.get("question_primitive") == "DEPENDENCY"
        for row in found
    )


@pytest.mark.asyncio
async def test_shortcuts_record_success_and_failure_utility(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """record_success additively bumps utility + counter; record_failure
    decays multiplicatively and clamps at the UTILITY_FLOOR."""
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)

    sig = _sig(question_primitive="CAUSE", entities=["latency"])
    shortcut = await repo.upsert_from_outcome(
        sig, to_affordance="trace.latency", delta_utility=0.4,
    )

    # Success path.
    bumped = await repo.record_success(shortcut.id)
    assert bumped is not None
    assert bumped.success_count == 1
    assert bumped.utility_score == pytest.approx(0.4 + SUCCESS_UTILITY_BUMP)
    assert bumped.last_success_at is not None

    # Failure path: decays by FAILURE_DECAY_FACTOR.
    decayed = await repo.record_failure(shortcut.id)
    assert decayed is not None
    assert decayed.failure_count == 1
    expected = (0.4 + SUCCESS_UTILITY_BUMP) * FAILURE_DECAY_FACTOR
    assert decayed.utility_score == pytest.approx(expected)
    assert decayed.last_failure_at is not None


@pytest.mark.asyncio
async def test_shortcuts_upsert_from_outcome_creates_new_row(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A fresh signature/target combo inserts a new row; the same combo
    again hits the bump path rather than inserting a duplicate."""
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)

    sig = _sig(question_primitive="CONSTRAINT", signal_type="policy_drift")

    first = await repo.upsert_from_outcome(
        sig, to_affordance="policy.audit", delta_utility=0.2,
    )
    assert first.utility_score == pytest.approx(0.2)

    second = await repo.upsert_from_outcome(
        sig, to_affordance="policy.audit", delta_utility=0.15,
    )
    # Same row, bumped.
    assert second.id == first.id
    assert second.utility_score == pytest.approx(0.35)

    # Verify only one row exists.
    listed = await repo.find_for_signature(sig)
    target_rows = [r for r in listed if r.to_affordance == "policy.audit"]
    assert len(target_rows) == 1


@pytest.mark.asyncio
async def test_shortcuts_sweep_expired(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """sweep_expired deletes rows whose `expires_at <= now()`, leaves
    others untouched, and returns the delete count."""
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)

    now = datetime.now(timezone.utc)
    past = now - timedelta(hours=1)
    future = now + timedelta(hours=1)

    expired = await repo.upsert_from_outcome(
        _sig(question_primitive="RECURRENCE"),
        to_affordance="ssr.expired",
        delta_utility=0.1,
        expires_at=past,
    )
    fresh = await repo.upsert_from_outcome(
        _sig(question_primitive="RECURRENCE"),
        to_affordance="ssr.fresh",
        delta_utility=0.1,
        expires_at=future,
    )
    permanent = await repo.upsert_from_outcome(
        _sig(question_primitive="RECURRENCE"),
        to_affordance="ssr.permanent",
        delta_utility=0.1,
    )

    deleted = await repo.sweep_expired()
    assert deleted == 1

    # Expired shortcut also drops out of find_for_signature.
    surviving = await repo.find_for_signature(
        _sig(question_primitive="RECURRENCE"),
    )
    surviving_ids = {row.id for row in surviving}
    assert expired.id not in surviving_ids
    assert fresh.id in surviving_ids
    assert permanent.id in surviving_ids


@pytest.mark.asyncio
async def test_shortcuts_multi_target_kinds(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A shortcut may point at a Model, a region, or an affordance —
    the same signature can produce three independent rows, one per
    target kind."""
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)

    sig = _sig(question_primitive="GOAL_IMPACT", entities=["Acme"])
    region_id = uuid7()

    aff_shortcut = await repo.upsert_from_outcome(
        sig, to_affordance="map.region.acme", delta_utility=0.3,
    )
    region_shortcut = await repo.upsert_from_outcome(
        sig, to_region_id=region_id, delta_utility=0.25,
    )

    # The shortcuts pointing at distinct target kinds are distinct rows.
    assert aff_shortcut.id != region_shortcut.id
    assert aff_shortcut.to_affordance == "map.region.acme"
    assert aff_shortcut.to_region_id is None
    assert region_shortcut.to_region_id == region_id
    assert region_shortcut.to_affordance is None

    found = await repo.find_for_signature(sig)
    found_ids = {row.id for row in found}
    assert aff_shortcut.id in found_ids
    assert region_shortcut.id in found_ids


# =====================================================================
# NegativeMemoryRepo
# =====================================================================


def _negmem(
    *,
    tenant_id: UUID,
    memory_type: str = "rejected_hypothesis",
    signature: dict | None = None,
    reason: str = "evidence directly contradicts",
    evidence_snapshot_hash: str | None = None,
    expires_at: datetime | None = None,
) -> NegativeMemory:
    return NegativeMemory(
        id=uuid7(),
        tenant_id=tenant_id,
        memory_type=memory_type,
        signature=signature or {"question_primitive": "DEPENDENCY"},
        rejected_claim="Globex is blocked on SSO",
        rejected_path=None,
        reason=reason,
        evidence_snapshot_hash=evidence_snapshot_hash,
        confidence=0.6,
        expires_at=expires_at or (datetime.now(timezone.utc) + timedelta(days=7)),
    )


@pytest.mark.asyncio
async def test_negative_memory_insert_and_find(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Insert returns a hydrated row with id/created_at, and
    find_for_signature surfaces it via @> containment."""
    repo = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)

    mem = _negmem(
        tenant_id=tenant_id,
        signature={
            "signal_type": "renewal_at_risk",
            "entities": ["Globex"],
            "question_primitive": "DEPENDENCY",
        },
    )
    inserted = await repo.insert(mem)
    assert inserted.id is not None
    assert inserted.tenant_id == tenant_id
    assert inserted.memory_type == "rejected_hypothesis"
    assert inserted.confidence == pytest.approx(0.6)
    assert inserted.created_at is not None

    # Probe with a partial signature — should still surface via @>.
    found = await repo.find_for_signature(
        _sig(question_primitive="DEPENDENCY"),
    )
    assert any(r.id == inserted.id for r in found)


@pytest.mark.asyncio
async def test_negative_memory_find_filters_by_memory_type(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """When memory_type is supplied to find_for_signature, only rows
    of that kind are returned."""
    repo = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)

    rejected = await repo.insert(_negmem(
        tenant_id=tenant_id,
        memory_type="rejected_hypothesis",
        signature={"question_primitive": "CAUSE"},
    ))
    noisy = await repo.insert(_negmem(
        tenant_id=tenant_id,
        memory_type="noisy_path",
        signature={"question_primitive": "CAUSE"},
    ))

    probe = _sig(question_primitive="CAUSE")

    # No filter: both rows surface.
    all_rows = await repo.find_for_signature(probe)
    all_ids = {r.id for r in all_rows}
    assert rejected.id in all_ids
    assert noisy.id in all_ids

    # Filter to rejected_hypothesis only.
    only_rejected = await repo.find_for_signature(
        probe, memory_type="rejected_hypothesis",
    )
    only_rejected_ids = {r.id for r in only_rejected}
    assert rejected.id in only_rejected_ids
    assert noisy.id not in only_rejected_ids


@pytest.mark.asyncio
async def test_negative_memory_sweep_expired(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """sweep_expired drops rows whose `expires_at <= now()` and leaves
    non-expired ones in place."""
    repo = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)

    now = datetime.now(timezone.utc)
    past = now - timedelta(hours=1)
    future = now + timedelta(days=1)

    expired = await repo.insert(_negmem(
        tenant_id=tenant_id,
        signature={"question_primitive": "RECURRENCE"},
        expires_at=past,
    ))
    fresh = await repo.insert(_negmem(
        tenant_id=tenant_id,
        signature={"question_primitive": "RECURRENCE"},
        expires_at=future,
    ))

    deleted = await repo.sweep_expired()
    assert deleted == 1

    surviving = await repo.find_for_signature(
        _sig(question_primitive="RECURRENCE"),
    )
    surviving_ids = {r.id for r in surviving}
    assert expired.id not in surviving_ids
    assert fresh.id in surviving_ids


@pytest.mark.asyncio
async def test_negative_memory_invalidate_by_evidence_change(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """invalidate_by_evidence_change drops rows whose stored evidence
    hash no longer matches; NULL-evidence rows are untouched."""
    repo = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)

    sig_dict = {"question_primitive": "CONSTRAINT", "entities": ["policy_x"]}

    pinned_stale = await repo.insert(_negmem(
        tenant_id=tenant_id,
        signature=sig_dict,
        evidence_snapshot_hash="evidence_hash_v1",
    ))
    pinned_current = await repo.insert(_negmem(
        tenant_id=tenant_id,
        signature=sig_dict,
        evidence_snapshot_hash="evidence_hash_v2",
    ))
    unpinned = await repo.insert(_negmem(
        tenant_id=tenant_id,
        signature=sig_dict,
        evidence_snapshot_hash=None,
    ))

    deleted = await repo.invalidate_by_evidence_change(
        _sig(question_primitive="CONSTRAINT", entities=["policy_x"]),
        new_evidence_hash="evidence_hash_v2",
    )
    # Only the stale-pinned row drops; current-pinned + unpinned survive.
    assert deleted == 1

    surviving = await repo.find_for_signature(
        _sig(question_primitive="CONSTRAINT", entities=["policy_x"]),
    )
    surviving_ids = {r.id for r in surviving}
    assert pinned_stale.id not in surviving_ids
    assert pinned_current.id in surviving_ids
    assert unpinned.id in surviving_ids
