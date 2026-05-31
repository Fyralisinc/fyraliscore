"""tests/unit/sage/test_topology_optimizer.py — Phase 13 Topology Optimizer.

Integration tests for the rule-based topology optimizer (doc §16).
Despite living under tests/unit, they touch a real Postgres for the
same reason as `test_inquiry_traces_repo.py` — the optimizer chains
several Wave-1 repos that are themselves thin SQL wrappers, and the
contract worth testing is the end-to-end effect on the Discovery
Utility Layer tables, not the internal call graph.

Uses the same `gateway_pool` + `tenant_id` fixtures (per-test pool +
fresh DB via TRUNCATE) re-exported through services/gateway/tests/
conftest.py. `pytest.mark.integration` keeps these out of any pure-
unit selection that runs without a database.

Hermetic notes:
  * Each test seeds its own inquiry_sessions + models + affordance
    profiles row(s) — we cannot rely on global state because the
    optimizer's behavior is gated on FK-resolved Model existence.
  * `models` requires `born_from_event_id`, `embedding`, and several
    other NOT NULL columns; we seed via raw SQL with a deterministic
    768-dim zero vector to stay under the migration's CHECK
    constraints. The embedding values are not exercised by these
    tests — only the row identity matters.
  * We never write to `models` from the optimizer itself; the test
    that asserts "canonical merge candidate is produced but not
    applied" verifies this by snapshotting the `models` table row
    count + content before/after the run.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.sage.affordances.repo import AffordanceProfilesRepo
from services.sage.affordances.types import RetrievalAffordanceProfile
from services.sage.discovery.negative_memory_repo import NegativeMemoryRepo
from services.sage.discovery.shortcuts_repo import DiscoveryShortcutsRepo
from services.sage.discovery.types import Signature
from services.sage.inquiry_traces.repo import OutcomeEventsRepo
from services.sage.topology_optimizer import (
    OptimizationRunReport,
    REINFORCE_DELTA,
    TopologyOptimizer,
    optimize_topology,
)


# Re-use gateway integration fixtures (per-test pool + fresh DB).
from services.gateway.tests.conftest import (  # noqa: F401
    gateway_pool,
    tenant_id,
)


pytestmark = pytest.mark.integration


# =====================================================================
# Helpers
# =====================================================================


_ZERO_EMBEDDING = "[" + ",".join(["0"] * 768) + "]"


async def _seed_inquiry_session(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> UUID:
    """Insert a minimal inquiry_sessions row so FK references resolve.

    Mirrors the helper in test_inquiry_traces_repo.py.
    """
    session_id = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO inquiry_sessions (
              id, tenant_id, signal_ref_type, signal_ref_id,
              route, status, stop_status
            ) VALUES (
              $1, $2, 'internal', NULL,
              'DEEP_INQUIRY_PATH', 'running', 'insufficient_continue'
            )
            """,
            session_id,
            tenant_id,
        )
    return session_id


async def _seed_model(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    proposition: dict | None = None,
) -> UUID:
    """Insert a minimal `models` row so affordance / structural FKs resolve.

    Uses raw SQL with a zero embedding — the optimizer never inspects
    embedding contents, so the only constraint that matters is the
    schema-level NOT NULL on `embedding`.
    """
    import json

    model_id = uuid7()
    born_from_event_id = uuid7()
    prop = proposition or {"kind": "belief", "subject": "test"}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id,
              proposition, "natural", embedding,
              scope_temporal,
              confidence, activation
            ) VALUES (
              $1, $2, $3,
              $4::jsonb, $5, $6::vector,
              $7::jsonb,
              0.5, 1.0
            )
            """,
            model_id,
            tenant_id,
            born_from_event_id,
            json.dumps(prop),
            "test model",
            _ZERO_EMBEDDING,
            json.dumps({}),
        )
    return model_id


async def _seed_affordance_profile(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    model_id: UUID,
    utility_score: float = 0.5,
) -> RetrievalAffordanceProfile:
    """Seed a profile row for `model_id` so reinforce/decay can land."""
    repo = AffordanceProfilesRepo(pool, tenant_id=tenant_id)
    return await repo.upsert(
        RetrievalAffordanceProfile(
            model_id=model_id,
            tenant_id=tenant_id,
            answers_question_primitives=["DEPENDENCY"],
            utility_score=utility_score,
        )
    )


# =====================================================================
# Tests
# =====================================================================


@pytest.mark.asyncio
async def test_reinforces_affordance_after_node_used_in_valid_diff(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A `node_used_in_valid_diff` event reinforces the model's
    affordance utility score by REINFORCE_DELTA."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    model_id = await _seed_model(gateway_pool, tenant_id=tenant_id)
    seeded = await _seed_affordance_profile(
        gateway_pool, tenant_id=tenant_id, model_id=model_id,
        utility_score=0.5,
    )
    baseline_utility = seeded.utility_score

    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    await events.append(
        session_id,
        "node_used_in_valid_diff",
        {"model_id": str(model_id)},
    )

    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    report = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="validated_synthesis_diff_applied",
    )

    assert report.affordance_reinforces == 1

    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    updated = await repo.get(model_id)
    assert updated is not None
    assert updated.utility_score == pytest.approx(
        baseline_utility + REINFORCE_DELTA,
    )
    assert updated.last_reinforced_at is not None


@pytest.mark.asyncio
async def test_decays_affordance_after_repeated_retrieved_but_omitted(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A model_id appearing in 2+ `retrieved_evidence_omitted` events
    (and never later requested or in a useful path) decays via
    affordance.decay."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    model_id = await _seed_model(gateway_pool, tenant_id=tenant_id)
    seeded = await _seed_affordance_profile(
        gateway_pool, tenant_id=tenant_id, model_id=model_id,
        utility_score=1.0,
    )
    baseline_utility = seeded.utility_score

    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    # Three omissions of the same model — and crucially NO
    # omitted_evidence_later_requested + NO useful path that uses it.
    for _ in range(3):
        await events.append(
            session_id,
            "retrieved_evidence_omitted",
            {"model_id": str(model_id), "omission_reason": "redundant"},
        )

    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    report = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="background_region_scan_complete",
    )

    assert report.affordance_decays == 1
    assert report.affordance_reinforces == 0

    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    updated = await repo.get(model_id)
    assert updated is not None
    # decay() multiplies utility by DECAY_FACTOR (0.95).
    assert updated.utility_score < baseline_utility
    assert updated.utility_score == pytest.approx(baseline_utility * 0.95)


@pytest.mark.asyncio
async def test_creates_shortcut_after_path_used_in_valid_diff(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A `path_used_in_valid_diff` event with a signature + target
    upserts a discovery shortcut with positive utility."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)

    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    payload = {
        "path_id": str(uuid4()),
        "signal_type": "enterprise_customer_blocker",
        "entities": ["customer", "SSO"],
        "question_primitive": "DEPENDENCY",
        "to_affordance": "map.region.sso",
    }
    await events.append(session_id, "path_used_in_valid_diff", payload)

    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    report = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="validated_synthesis_diff_applied",
    )

    assert report.shortcut_creates_or_bumps == 1

    shortcuts = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)
    found = await shortcuts.find_for_signature(
        Signature(question_primitive="DEPENDENCY"),
    )
    assert len(found) >= 1
    assert any(s.to_affordance == "map.region.sso" for s in found)
    sso_row = next(s for s in found if s.to_affordance == "map.region.sso")
    assert sso_row.utility_score > 0.0


@pytest.mark.asyncio
async def test_records_failure_on_noisy_shortcut(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A `retrieved_evidence_omitted` event whose payload pins a
    `shortcut_id` calls record_failure on that shortcut (decaying its
    utility) — distinct from negative-memory inserts, which always
    happen for noisy paths."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)

    # Seed a shortcut so the optimizer can decay it.
    shortcuts = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)
    sig = Signature(
        signal_type="renewal_at_risk",
        entities=["Globex"],
        question_primitive="CAUSE",
    )
    seeded = await shortcuts.upsert_from_outcome(
        sig, to_affordance="lookup.commitment", delta_utility=0.4,
    )
    baseline_utility = seeded.utility_score
    assert baseline_utility > 0.0

    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    await events.append(
        session_id,
        "retrieved_evidence_omitted",
        {
            "shortcut_id": str(seeded.id),
            "signal_type": "renewal_at_risk",
            "entities": ["Globex"],
            "question_primitive": "CAUSE",
            "omission_reason": "generic_hub",
        },
    )

    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    report = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="background_region_scan_complete",
    )

    assert report.shortcut_decays == 1
    decayed_rows = await shortcuts.find_for_signature(sig)
    decayed = next(r for r in decayed_rows if r.id == seeded.id)
    assert decayed.failure_count == 1
    assert decayed.utility_score < baseline_utility


@pytest.mark.asyncio
async def test_inserts_negative_memory_with_expires_at(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A `rejected_hypothesis` payload field produces a negative_memory
    row with `expires_at` populated (doc §14 mandates expiry)."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)

    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    await events.append(
        session_id,
        "validation_failed_due_to_missing_evidence",
        {
            "rejected_hypothesis": "Globex is blocked on SSO rollout",
            "signal_type": "renewal_at_risk",
            "entities": ["Globex"],
            "question_primitive": "DEPENDENCY",
            "reason": "no evidence of SSO rollout in past 30 days",
        },
    )

    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    report = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="reasoning_diff_failed_validation",
    )

    assert report.negative_memory_inserts >= 1

    nm_repo = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)
    found = nm_repo.find_for_signature
    rows = await found(
        Signature(question_primitive="DEPENDENCY"),
        memory_type="rejected_hypothesis",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.expires_at is not None
    assert row.rejected_claim == "Globex is blocked on SSO rollout"
    assert row.memory_type == "rejected_hypothesis"


@pytest.mark.asyncio
async def test_canonical_merge_candidate_is_produced_but_not_applied(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Two models co-occurring in multiple useful paths with high
    proposition similarity produce a merge candidate dict — but the
    optimizer NEVER writes to `models` / `model_edges` / `observations`.
    """
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    model_a = await _seed_model(gateway_pool, tenant_id=tenant_id)
    model_b = await _seed_model(gateway_pool, tenant_id=tenant_id)

    # Snapshot canonical-truth row counts BEFORE the optimizer runs.
    async with gateway_pool.acquire() as conn:
        before_models = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1", tenant_id,
        )
        before_edges = await conn.fetchval(
            "SELECT count(*) FROM model_edges WHERE tenant_id = $1",
            tenant_id,
        )
        before_obs = await conn.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1",
            tenant_id,
        )

    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    # Two distinct useful paths both linking (a, b) with high
    # proposition similarity → merge candidate threshold met.
    for path_idx in range(2):
        await events.append(
            session_id,
            "path_used_in_valid_diff",
            {
                "path_id": str(uuid4()),
                "from_model_id": str(model_a),
                "to_model_id": str(model_b),
                "proposition_similarity": 0.9,
                "signal_type": "blocker",
                "question_primitive": "DEPENDENCY",
                "to_affordance": f"map.path.{path_idx}",
            },
        )

    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    report = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="validated_synthesis_diff_applied",
    )

    # Candidate produced.
    assert len(report.canonical_merge_candidates) == 1
    cand = report.canonical_merge_candidates[0]
    assert cand["op"] == "merge"
    assert set(cand["source_model_ids"]) == {str(model_a), str(model_b)}
    assert "evidence_session_ids" in cand
    assert str(session_id) in cand["evidence_session_ids"]

    # Canonical truth NOT mutated.
    async with gateway_pool.acquire() as conn:
        after_models = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1", tenant_id,
        )
        after_edges = await conn.fetchval(
            "SELECT count(*) FROM model_edges WHERE tenant_id = $1",
            tenant_id,
        )
        after_obs = await conn.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1",
            tenant_id,
        )
    assert after_models == before_models
    assert after_edges == before_edges
    assert after_obs == before_obs


@pytest.mark.asyncio
async def test_optimization_run_report_metrics_has_all_expected_keys(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """`OptimizationRunReport.metrics` always carries the documented
    keys (useful_nodes, useful_paths, noisy_paths, missing_anchors,
    trigger_recognized) even when the inquiry produced no events."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)

    report = await optimize_topology(
        pool=gateway_pool,
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        trigger_event="validated_synthesis_diff_applied",
    )

    assert isinstance(report, OptimizationRunReport)
    expected_keys = {
        "useful_nodes",
        "useful_paths",
        "noisy_paths",
        "missing_anchors",
        "trigger_recognized",
    }
    assert expected_keys.issubset(report.metrics.keys())
    # All values are floats per the dataclass annotation.
    for k in expected_keys:
        assert isinstance(report.metrics[k], float)
    # An empty session produces zero of everything.
    assert report.metrics["useful_nodes"] == 0.0
    assert report.metrics["useful_paths"] == 0.0
    assert report.affordance_reinforces == 0
    assert report.shortcut_creates_or_bumps == 0
    # Known trigger → recognized=1.
    assert report.metrics["trigger_recognized"] == 1.0


@pytest.mark.asyncio
async def test_unknown_trigger_marks_metrics_not_recognized(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """An unrecognized trigger still runs the optimizer (best-effort)
    but flags `trigger_recognized=0.0` in metrics so callers can spot
    callers passing typos / unsupported triggers."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)

    report = await optimize_topology(
        pool=gateway_pool,
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        trigger_event="not_a_known_trigger",
    )

    assert report.metrics["trigger_recognized"] == 0.0
    # Empty session still returns a well-formed report.
    assert report.inquiry_session_id == session_id
    assert report.canonical_merge_candidates == ()
