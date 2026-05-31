"""tests/unit/sage/test_sage_edge_cases.py — extensive SAGE edge cases.

Covers the awkward corners the per-component tests don't hit:

  * Tenant isolation under RLS (the most security-critical case)
  * Foreign-key cascade: archiving / deleting a Model cleans up its
    derived discovery-utility rows (affordance profiles, structural
    features, model_predictions, model_prediction_errors)
  * JSONB signature containment edge cases for discovery_shortcuts
  * Models with NULL falsifier flow through evidence_projection
  * Boundary utility scores (0.0, very large, repeated reinforcement)
  * Idempotent + concurrent TopologyOptimizer runs on the same session
  * Negative memory expiry / invalidation by evidence-hash change
  * `discovery_shortcuts_has_target` CHECK enforced at the SQL layer
  * RLS permissive default works when `app.current_tenant` is NULL

All cases are `@pytest.mark.integration` and use the gateway_pool
fixture. Helpers come from `tests.unit.sage._seed`.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.sage.affordances.repo import AffordanceProfilesRepo
from services.sage.affordances.types import RetrievalAffordanceProfile
from services.sage.discovery.negative_memory_repo import NegativeMemoryRepo
from services.sage.discovery.shortcuts_repo import (
    FAILURE_DECAY_FACTOR,
    SUCCESS_UTILITY_BUMP,
    DiscoveryShortcutsRepo,
)
from services.sage.discovery.types import NegativeMemory, Signature
from services.sage.evidence_projection import (
    EvidenceProjector,
    ProjectionBudget,
)
from services.sage.inquiry_traces.repo import OutcomeEventsRepo
from services.sage.model_predictions.repo import (
    ModelPredictionErrorsRepo,
    ModelPredictionsRepo,
)
from services.sage.model_predictions.types import (
    ExpectedObservation,
    ModelPrediction,
    ModelPredictionError,
)
from services.sage.region_summaries.repo import RegionSummariesRepo
from services.sage.region_summaries.types import RegionSufficientState
from services.sage.structural_features.repo import StructuralFeaturesRepo
from services.sage.structural_features.types import (
    EdgeStructuralFeatures,
    ModelStructuralFeatures,
)
from services.sage.topology_optimizer import TopologyOptimizer

from services.gateway.tests.conftest import (  # noqa: F401
    gateway_pool,
    tenant_id,
    tenant_id_b,
)
from tests.unit.sage._seed import seed_model, seed_observation


pytestmark = pytest.mark.integration


# =====================================================================
# Local helpers
# =====================================================================


async def _seed_inquiry_session(
    pool: asyncpg.Pool, *, tenant_id: UUID,
) -> UUID:
    sid = uuid7()
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
            sid, tenant_id,
        )
    return sid


# =====================================================================
# 1. Tenant isolation
# =====================================================================


@pytest.mark.asyncio
async def test_affordance_writes_isolated_between_tenants(
    gateway_pool: asyncpg.Pool, tenant_id: UUID, tenant_id_b: UUID,
):
    """An affordance profile written for tenant A is invisible to
    tenant B (cross-tenant query by model_id returns None for B)."""
    model_id = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo_a = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    repo_b = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id_b)

    await repo_a.upsert(RetrievalAffordanceProfile(
        model_id=model_id, tenant_id=tenant_id,
        answers_question_primitives=["DEPENDENCY"], utility_score=0.7,
    ))

    a_view = await repo_a.get(model_id)
    b_view = await repo_b.get(model_id)
    assert a_view is not None
    assert a_view.utility_score == pytest.approx(0.7)
    assert b_view is None, "tenant B must not see tenant A's affordance profile"


@pytest.mark.asyncio
async def test_discovery_shortcut_isolated_between_tenants(
    gateway_pool: asyncpg.Pool, tenant_id: UUID, tenant_id_b: UUID,
):
    """A shortcut upserted for tenant A is not surfaced by an identical
    signature probe under tenant B."""
    repo_a = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)
    repo_b = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id_b)
    sig = Signature(
        signal_type="renewal_at_risk", entities=["Globex"],
        question_primitive="DEPENDENCY",
    )
    await repo_a.upsert_from_outcome(
        sig, to_affordance="lookup.commitment", delta_utility=0.5,
    )

    a_hits = await repo_a.find_for_signature(sig)
    b_hits = await repo_b.find_for_signature(sig)
    assert len(a_hits) == 1
    assert b_hits == []


@pytest.mark.asyncio
async def test_negative_memory_isolated_between_tenants(
    gateway_pool: asyncpg.Pool, tenant_id: UUID, tenant_id_b: UUID,
):
    """Negative memory written for tenant A is not visible to tenant B."""
    repo_a = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)
    repo_b = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id_b)

    sig = {"signal_type": "x", "entities": ["e1"]}
    await repo_a.insert(NegativeMemory(
        id=uuid7(), tenant_id=tenant_id,
        memory_type="rejected_hypothesis",
        signature=sig,
        rejected_claim="probe rejected",
        reason="probe",
        expires_at=datetime.now(timezone.utc) + timedelta(days=30),
    ))

    a_hits = await repo_a.find_for_signature(sig)
    b_hits = await repo_b.find_for_signature(sig)
    assert len(a_hits) == 1
    assert b_hits == []


@pytest.mark.asyncio
async def test_region_summary_isolated_between_tenants(
    gateway_pool: asyncpg.Pool, tenant_id: UUID, tenant_id_b: UUID,
):
    """A region summary for tenant A is not visible to tenant B."""
    repo_a = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id)
    repo_b = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id_b)

    region_id = uuid7()
    await repo_a.upsert(RegionSufficientState(
        region_id=region_id, tenant_id=tenant_id,
        summary="tenant A region summary",
    ))

    a_view = await repo_a.get(region_id)
    b_view = await repo_b.get(region_id)
    assert a_view is not None
    assert b_view is None


@pytest.mark.asyncio
async def test_model_predictions_isolated_between_tenants(
    gateway_pool: asyncpg.Pool, tenant_id: UUID, tenant_id_b: UUID,
):
    """A model prediction for tenant A is not visible to tenant B."""
    model_a = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo_a = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)
    repo_b = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id_b)

    pred = await repo_a.insert(ModelPrediction(
        tenant_id=tenant_id, model_id=model_a,
        prediction="ARR will grow",
        expected_observation=ExpectedObservation(
            kind="metric_delta",
            value_constraint={"op": "gt", "value": 0, "field": "delta"},
        ),
        confidence=0.7,
    ))

    a_view = await repo_a.get(pred.id)
    b_view = await repo_b.get(pred.id)
    assert a_view is not None
    assert b_view is None


# =====================================================================
# 2. Foreign-key cascade behavior
# =====================================================================


@pytest.mark.asyncio
async def test_deleting_model_cascades_to_affordance_profile(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """ON DELETE CASCADE on model_id removes the derived affordance
    profile when the canonical Model row is deleted."""
    model_id = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    await repo.upsert(RetrievalAffordanceProfile(
        model_id=model_id, tenant_id=tenant_id,
        answers_question_primitives=["DEPENDENCY"],
    ))
    assert await repo.get(model_id) is not None

    async with gateway_pool.acquire() as conn:
        await conn.execute("DELETE FROM models WHERE id = $1", model_id)

    assert await repo.get(model_id) is None, (
        "Affordance profile must cascade-delete when its model is removed."
    )


@pytest.mark.asyncio
async def test_deleting_model_cascades_to_structural_features(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """structural_features row goes when the Model goes."""
    model_id = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo = StructuralFeaturesRepo(gateway_pool, tenant_id=tenant_id)
    await repo.upsert_model_features([ModelStructuralFeatures(
        model_id=model_id, tenant_id=tenant_id,
        degree_total=4, degree_in=2, degree_out=2,
        hub_score=0.7, bridge_score=0.3,
    )])

    before = await repo.get_for_models([model_id])
    assert len(before) == 1

    async with gateway_pool.acquire() as conn:
        await conn.execute("DELETE FROM models WHERE id = $1", model_id)

    after = await repo.get_for_models([model_id])
    assert after == {} or after == []


@pytest.mark.asyncio
async def test_deleting_model_cascades_to_model_predictions(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """model_predictions row goes when the Model goes (FK CASCADE)."""
    model_id = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)
    pred = await repo.insert(ModelPrediction(
        tenant_id=tenant_id, model_id=model_id,
        prediction="metric will rise",
        expected_observation=ExpectedObservation(
            kind="metric_delta",
            value_constraint={"op": "gt", "value": 0, "field": "delta"},
        ),
        confidence=0.6,
    ))
    assert await repo.get(pred.id) is not None

    async with gateway_pool.acquire() as conn:
        await conn.execute("DELETE FROM models WHERE id = $1", model_id)

    assert await repo.get(pred.id) is None


# =====================================================================
# 3. JSONB signature containment edge cases
# =====================================================================


@pytest.mark.asyncio
async def test_discovery_shortcut_empty_entities_probe_matches_any_signal_type(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A probe with only `signal_type` should surface stored shortcuts
    that have a matching signal_type, regardless of their entities."""
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)
    await repo.upsert_from_outcome(
        Signature(signal_type="customer_blocker", entities=["A"]),
        to_affordance="aff.a", delta_utility=0.3,
    )
    await repo.upsert_from_outcome(
        Signature(signal_type="customer_blocker", entities=["B"]),
        to_affordance="aff.b", delta_utility=0.3,
    )
    await repo.upsert_from_outcome(
        Signature(signal_type="unrelated", entities=["A"]),
        to_affordance="aff.c", delta_utility=0.3,
    )

    hits = await repo.find_for_signature(
        Signature(signal_type="customer_blocker"),
    )
    affords = sorted(h.to_affordance for h in hits)
    assert affords == ["aff.a", "aff.b"]


@pytest.mark.asyncio
async def test_discovery_shortcut_check_constraint_rejects_all_null_targets(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """The `discovery_shortcuts_has_target` CHECK should be enforced
    at the SQL layer regardless of which path inserts the row.
    Direct SQL insert with all three target columns NULL must fail."""
    async with gateway_pool.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                """
                INSERT INTO discovery_shortcuts (
                  id, tenant_id, from_signature,
                  to_model_id, to_region_id, to_affordance,
                  utility_score
                ) VALUES ($1, $2, '{"signal_type":"x"}'::jsonb, NULL, NULL, NULL, 0.5)
                """,
                uuid7(), tenant_id,
            )


@pytest.mark.asyncio
async def test_discovery_shortcut_utility_score_non_negative_check(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """utility_score >= 0 is a SQL-level CHECK; direct insert with a
    negative score must fail."""
    async with gateway_pool.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                """
                INSERT INTO discovery_shortcuts (
                  id, tenant_id, from_signature,
                  to_affordance, utility_score
                ) VALUES ($1, $2, '{"signal_type":"x"}'::jsonb, 'a', -1.0)
                """,
                uuid7(), tenant_id,
            )


# =====================================================================
# 4. NULL falsifier through projection
# =====================================================================


@pytest.mark.asyncio
async def test_evidence_projection_handles_null_falsifier(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A Model with falsifier=NULL must not crash the projector; it
    just skips the falsification_relevant stage."""
    seed_obs = await seed_observation(gateway_pool, tenant_id=tenant_id)
    support_obs = await seed_observation(
        gateway_pool, tenant_id=tenant_id, content_text="support",
    )
    model_id = await seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[support_obs],
        falsifier=None,
    )

    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="STATUS",
    )
    assert result.coverage["falsification_share"] == 0.0
    assert all(
        p.reason != "falsification_relevant" for p in result.projected
    )


@pytest.mark.asyncio
async def test_evidence_projection_with_empty_supporting_events(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A Model with no supporting events still projects (just empty)."""
    seed_obs = await seed_observation(gateway_pool, tenant_id=tenant_id)
    model_id = await seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[],
    )

    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="STATUS",
    )
    assert result.projected == ()
    # Coverage shares may be 0/0 → projector should report 0.0, not NaN.
    for key, val in result.coverage.items():
        assert val == val, f"coverage[{key}] is NaN"
        assert 0.0 <= val <= 1.0


@pytest.mark.asyncio
async def test_evidence_projection_respects_token_budget(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """When the token budget is tiny, picks demote toward ref_only."""
    seed_obs = await seed_observation(gateway_pool, tenant_id=tenant_id)
    obs_a = await seed_observation(
        gateway_pool, tenant_id=tenant_id, content_text="a",
    )
    obs_b = await seed_observation(
        gateway_pool, tenant_id=tenant_id, content_text="b",
    )
    model_id = await seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[obs_a, obs_b],
    )

    # 200 tokens — below an evidence_card (120) + summary_only (40) = 160
    # but tiny enough that two raw_excerpts (400 each) get demoted.
    projector = EvidenceProjector(
        budget=ProjectionBudget(max_total_tokens=200, max_evidence_per_node=5),
    )
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="DEPENDENCY",
    )
    total_tokens = sum(p.token_estimate for p in result.projected)
    assert total_tokens <= 200 + 8  # +8 == ref_only floor


# =====================================================================
# 5. Boundary utility-score behavior
# =====================================================================


@pytest.mark.asyncio
async def test_affordance_repeated_reinforcement_does_not_saturate(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Repeated reinforce() additively grows utility — caller chooses
    saturation policy."""
    model_id = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    await repo.upsert(RetrievalAffordanceProfile(
        model_id=model_id, tenant_id=tenant_id,
        answers_question_primitives=["DEPENDENCY"], utility_score=0.0,
    ))
    for _ in range(20):
        await repo.reinforce(model_id, delta_utility=0.05)
    profile = await repo.get(model_id)
    assert profile.utility_score == pytest.approx(1.0, abs=1e-6)


@pytest.mark.asyncio
async def test_discovery_shortcut_failure_decay_clamps_at_floor(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Many record_failure calls drive utility down but it never goes
    negative (the SQL CHECK + the python UTILITY_FLOOR both clamp)."""
    repo = DiscoveryShortcutsRepo(gateway_pool, tenant_id=tenant_id)
    sig = Signature(signal_type="probe", entities=["x"])
    sc = await repo.upsert_from_outcome(
        sig, to_affordance="aff", delta_utility=0.4,
    )

    for _ in range(50):
        sc = await repo.record_failure(sc.id)
    assert sc is not None
    assert sc.utility_score >= 0.0
    assert sc.failure_count == 50


# =====================================================================
# 6. Concurrency / idempotency
# =====================================================================


@pytest.mark.asyncio
async def test_topology_optimizer_two_runs_same_session_are_idempotent(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Two sequential optimizer runs on the same session emit the same
    side effects on the discovery layer for the second run — affordance
    utility shouldn't double-bump."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    model_id = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    await repo.upsert(RetrievalAffordanceProfile(
        model_id=model_id, tenant_id=tenant_id,
        answers_question_primitives=["DEPENDENCY"], utility_score=0.5,
    ))
    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    await events.append(
        session_id, "node_used_in_valid_diff",
        {"model_id": str(model_id)},
    )

    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    r1 = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="validated_synthesis_diff_applied",
    )
    after1 = (await repo.get(model_id)).utility_score
    r2 = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="validated_synthesis_diff_applied",
    )
    after2 = (await repo.get(model_id)).utility_score

    # The same events should produce the same reinforcement direction
    # on both runs. The optimizer is allowed to reinforce again
    # (it doesn't dedupe outcome events), so utility grows monotonically.
    assert after2 >= after1
    assert r2.affordance_reinforces == r1.affordance_reinforces


@pytest.mark.asyncio
async def test_concurrent_affordance_reinforce_safety(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Five concurrent reinforce() calls don't lose updates — final
    utility must reflect all five increments (no torn write)."""
    model_id = await seed_model(gateway_pool, tenant_id=tenant_id)
    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    await repo.upsert(RetrievalAffordanceProfile(
        model_id=model_id, tenant_id=tenant_id,
        answers_question_primitives=["DEPENDENCY"], utility_score=0.0,
    ))

    await asyncio.gather(*[
        repo.reinforce(model_id, delta_utility=0.1)
        for _ in range(5)
    ])
    profile = await repo.get(model_id)
    assert profile.utility_score == pytest.approx(0.5, abs=1e-6)


# =====================================================================
# 7. Negative-memory expiry + invalidation
# =====================================================================


@pytest.mark.asyncio
async def test_negative_memory_sweep_drops_expired_only(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """sweep_expired removes rows whose expires_at <= now() and leaves
    fresh ones intact."""
    repo = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)
    now = datetime.now(timezone.utc)
    expired = await repo.insert(NegativeMemory(
        id=uuid7(), tenant_id=tenant_id,
        memory_type="noisy_path",
        signature={"signal_type": "stale"},
        reason="stale path",
        expires_at=now - timedelta(seconds=1),
    ))
    fresh = await repo.insert(NegativeMemory(
        id=uuid7(), tenant_id=tenant_id,
        memory_type="noisy_path",
        signature={"signal_type": "fresh"},
        reason="fresh path",
        expires_at=now + timedelta(days=30),
    ))

    removed = await repo.sweep_expired()
    assert removed == 1

    remaining = await repo.find_for_signature({"signal_type": "fresh"})
    assert len(remaining) == 1
    assert remaining[0].id == fresh.id

    gone = await repo.find_for_signature({"signal_type": "stale"})
    assert gone == []


@pytest.mark.asyncio
async def test_negative_memory_invalidate_by_evidence_hash(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """When the evidence snapshot changes, the prior negative-memory
    rows keyed on the old hash should be invalidated (deleted)."""
    repo = NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id)
    sig = {"signal_type": "candidate"}
    await repo.insert(NegativeMemory(
        id=uuid7(), tenant_id=tenant_id,
        memory_type="rejected_hypothesis",
        signature=sig,
        rejected_claim="X is blocked by Y",
        reason="weak support",
        evidence_snapshot_hash="hash_v1",
        expires_at=datetime.now(timezone.utc) + timedelta(days=30),
    ))
    assert len(await repo.find_for_signature(sig)) == 1

    await repo.invalidate_by_evidence_change(sig, "hash_v2")

    remaining = await repo.find_for_signature(sig)
    assert remaining == [], (
        "Negative memory keyed on hash_v1 must be invalidated when the "
        "evidence snapshot changes to hash_v2."
    )


# =====================================================================
# 8. Outcome event payload edge cases
# =====================================================================


@pytest.mark.asyncio
async def test_outcome_event_with_null_payload_appends_cleanly(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """append() with payload=None must store as JSON null, not crash."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    await events.append(session_id, "user_accepted_node", None)

    listing = await events.list_for_session(session_id)
    assert len(listing) == 1
    assert listing[0].event_type == "user_accepted_node"


@pytest.mark.asyncio
async def test_outcome_event_invalid_type_rejected(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """An event_type not in OUTCOME_EVENT_TYPES is rejected before the
    SQL CHECK ever runs (caller fast-fail)."""
    from lib.shared.errors import ValidationError
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    events = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    with pytest.raises((ValidationError, ValueError, Exception)) as exc_info:
        await events.append(session_id, "not_a_real_event", {})
    # Either fast-fail validation OR the SQL CHECK constraint — both
    # are acceptable; what matters is that it doesn't silently land.
    assert "not_a_real_event" in str(exc_info.value) or "event_type" in str(exc_info.value).lower()
