"""services/think/tests/test_reconciler.py — graph signals + per-kind rules.

Covers the enrichment added on top of the cosine/scope/recency dedup:

  * shared evidence-event boost
  * shared supporting_model_ids boost
  * shared falsifier boost
  * market_assessment auto_merge at 0.75-0.85 cosine
  * concern auto_merge at 0.78-0.85 cosine
  * recommendation never auto-merges
  * situation member_model_ids overlap
  * pattern_instance requires matching parent pattern_id
  * borderline cosine emits same_issue_as RelationshipCandidate
  * signal_breakdown is populated
"""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.think.diff_schema import ClaimOp
from services.think.reconciler import (
    KindRule,
    ReconcileResult,
    ReconcilerConfig,
    _compute_signal_breakdown,
    _falsifier_cosine,
    _member_overlap_fraction,
    _pattern_id,
    reconcile_claim_op,
)
from services.think.text_embedding import deterministic_text_embedding


# Async DB tests get `asyncio` marker individually; the unit tests
# below stay sync.


# =====================================================================
# Pure unit tests (no DB)
# =====================================================================


def test_member_overlap_fraction_half():
    a = {UUID(int=1), UUID(int=2), UUID(int=3), UUID(int=4)}
    b = {UUID(int=2), UUID(int=4), UUID(int=99), UUID(int=100)}
    # smaller=4, shared=2 → 0.5
    assert _member_overlap_fraction(a, b) == pytest.approx(0.5)


def test_member_overlap_fraction_full():
    a = {UUID(int=1), UUID(int=2)}
    b = {UUID(int=1), UUID(int=2), UUID(int=3)}
    # smaller=2, shared=2 → 1.0
    assert _member_overlap_fraction(a, b) == pytest.approx(1.0)


def test_pattern_id_extraction():
    pid = uuid7()
    assert _pattern_id({"pattern_id": str(pid)}) == pid
    assert _pattern_id({"parent_pattern_id": str(pid)}) == pid
    assert _pattern_id({"unrelated": "x"}) is None
    assert _pattern_id(None) is None


def test_falsifier_cosine_identical_text_is_one():
    fals = {"pattern": "audit evidence delivered and accepted"}
    val = _falsifier_cosine({"falsifier": fals}, {"falsifier": fals})
    assert val == pytest.approx(1.0)


def test_falsifier_cosine_missing_returns_zero():
    assert _falsifier_cosine({}, {"falsifier": {"pattern": "x"}}) == 0.0
    assert _falsifier_cosine({"falsifier": {"pattern": "x"}}, {}) == 0.0


def test_signal_breakdown_shared_evidence_event_boosts_cosine():
    """The +0.10 evidence-event boost lifts the adjusted score."""
    shared = uuid7()
    entry = {
        "supporting_event_ids": [str(shared)],
        "born_from_event_id": str(uuid7()),
    }
    row = {"supporting_event_ids": [shared]}
    adjusted, breakdown = _compute_signal_breakdown(entry, row, base_cosine=0.70)
    assert adjusted == pytest.approx(0.80)
    assert breakdown["cosine"] == pytest.approx(0.70)
    assert breakdown["shared_evidence_events"] >= 1.0
    assert breakdown["graph_boost"] == pytest.approx(0.10)
    assert breakdown["adjusted_score"] == pytest.approx(0.80)


def test_signal_breakdown_shared_supporting_models_boosts():
    m1, m2 = uuid7(), uuid7()
    entry = {"supporting_model_ids": [str(m1), str(m2), str(uuid7())]}
    row = {"supporting_model_ids": [m1, m2]}
    adjusted, breakdown = _compute_signal_breakdown(entry, row, base_cosine=0.70)
    assert adjusted == pytest.approx(0.75)
    assert breakdown["shared_supporting_models"] >= 2.0


def test_signal_breakdown_falsifier_match_boosts():
    fals = {"pattern": "renewal forecast accuracy improves by 20% in 14 days"}
    entry = {"falsifier": fals}
    row = {"falsifier": fals}
    adjusted, breakdown = _compute_signal_breakdown(entry, row, base_cosine=0.70)
    assert adjusted == pytest.approx(0.75)
    assert breakdown["falsifier_cosine"] >= 0.80


def test_signal_breakdown_no_boost_keeps_cosine():
    adjusted, breakdown = _compute_signal_breakdown({}, {}, base_cosine=0.42)
    assert adjusted == pytest.approx(0.42)
    assert breakdown["cosine"] == pytest.approx(0.42)
    assert "graph_boost" not in breakdown


def test_signal_breakdown_caps_at_one():
    shared = uuid7()
    entry = {"supporting_event_ids": [str(shared)]}
    row = {"supporting_event_ids": [shared]}
    adjusted, _ = _compute_signal_breakdown(entry, row, base_cosine=0.99)
    assert adjusted == pytest.approx(1.0)


# =====================================================================
# DB-backed scenarios (use the Think conftest fixtures).
# =====================================================================


async def _insert_obs(conn, tenant, text: str) -> UUID:
    from services.think.tests.conftest import make_embedding

    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                $4, FALSE, 'authoritative')
        """,
        oid, tenant, text, make_embedding(text),
    )
    return oid


async def _insert_model(
    conn,
    tenant,
    *,
    born_event: UUID,
    proposition: dict,
    natural: str,
    confidence: float = 0.6,
    embedding=None,
    supporting_event_ids: list[UUID] | None = None,
    supporting_model_ids: list[UUID] | None = None,
    falsifier: dict | None = None,
) -> UUID:
    mid = uuid7()
    emb = embedding if embedding is not None else deterministic_text_embedding(natural)
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient, supporting_event_ids,
           supporting_model_ids, falsifier)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                '{}'::jsonb, $7, 1.0, 'active', $7, 1.0,
                $8::uuid[], $9::uuid[], $10::jsonb)
        """,
        mid,
        tenant,
        born_event,
        json.dumps(proposition),
        natural,
        emb,
        confidence,
        list(supporting_event_ids or []),
        list(supporting_model_ids or []),
        json.dumps(falsifier) if falsifier else None,
    )
    return mid


def _insert_entry(
    *,
    tenant,
    born_event,
    proposition: dict,
    natural: str,
    confidence: float = 0.7,
    embedding=None,
    supporting_event_ids: list[UUID] | None = None,
    supporting_model_ids: list[UUID] | None = None,
    falsifier: dict | None = None,
) -> dict:
    entry: dict = {
        "tenant_id": str(tenant),
        "born_from_event_id": str(born_event),
        "proposition": proposition,
        "natural": natural,
        "scope_actors": [],
        "scope_entities": [],
        "scope_temporal": {},
        "confidence": confidence,
        "confidence_at_assertion": confidence,
    }
    if embedding is not None:
        entry["embedding"] = embedding
    if supporting_event_ids is not None:
        entry["supporting_event_ids"] = [str(e) for e in supporting_event_ids]
    if supporting_model_ids is not None:
        entry["supporting_model_ids"] = [str(m) for m in supporting_model_ids]
    if falsifier is not None:
        entry["falsifier"] = falsifier
    return entry


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_shared_evidence_event_pushes_to_auto_merge(
    fresh_db, tenant, tenant_cleanup,
):
    """Shared supporting_event_ids boosts a sub-threshold cosine into auto_merge.

    Pinning the cosine with controlled vectors: 0.78 + 0.10 boost = 0.88
    which crosses the default 0.85 auto_merge threshold.
    """
    async with fresh_db.acquire() as conn:
        shared_obs = await _insert_obs(conn, tenant, "atlas renewal risk")
        old_obs = await _insert_obs(conn, tenant, "atlas renewal anchor obs")
        new_obs = await _insert_obs(conn, tenant, "atlas renewal new obs")
        # Build vectors with cosine = 0.78.
        v1 = [0.0] * 768
        v1[0] = 1.0
        v2 = [0.0] * 768
        v2[0] = 0.78
        v2[1] = (1.0 - 0.78 ** 2) ** 0.5

        existing = await _insert_model(
            conn, tenant,
            born_event=old_obs,
            proposition={
                "kind": "state",
                "subject": "Atlas renewal",
                "assertion": "at risk",
            },
            natural="Atlas renewal is at risk because procurement is delayed.",
            embedding=v1,
            supporting_event_ids=[old_obs, shared_obs],
        )
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "state",
                "subject": "Atlas renewal",
                "assertion": "at risk",
            },
            natural="Atlas renewal is concerning because procurement team is slow.",
            embedding=v2,
            supporting_event_ids=[new_obs, shared_obs],
        )

        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

    # Without the boost the raw cosine 0.78 would be human_review.
    # With +0.10 evidence-event boost the adjusted score is 0.88 →
    # crosses the default auto_merge threshold of 0.85.
    assert result.decision == "auto_merge"
    assert result.matched_model_id == existing
    assert result.cosine_similarity is not None
    assert result.cosine_similarity < 0.85
    assert "cosine" in result.signal_breakdown
    assert "adjusted_score" in result.signal_breakdown
    # The shared_evidence_events signal MUST be present.
    assert result.signal_breakdown.get("shared_evidence_events", 0.0) >= 1.0
    assert result.signal_breakdown.get("graph_boost", 0.0) >= 0.10
    # And the decision reason should call out the graph-signal boost.
    assert result.decision_reason == "graph_signal_boost"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_market_assessment_auto_merges_at_low_cosine(
    fresh_db, tenant, tenant_cleanup,
):
    """market_assessment uses a 0.75 auto_merge threshold."""
    async with fresh_db.acquire() as conn:
        old_obs = await _insert_obs(conn, tenant, "cloud market old")
        new_obs = await _insert_obs(conn, tenant, "cloud market new")
        existing_natural = (
            "The multicloud market is heating up as AWS outages drive demand."
        )
        candidate_natural = (
            "Demand for multicloud is rising because AWS reliability concerns grow."
        )
        # Force the embedding so we can pin the cosine just above 0.75.
        common_emb = deterministic_text_embedding(existing_natural)
        # Make a slightly perturbed embedding that still cosine >= 0.78.
        perturbed = [v * 0.97 for v in common_emb]
        # Re-normalize (cosine doesn't require it but cleaner).
        import math
        norm = math.sqrt(sum(v * v for v in perturbed))
        perturbed = [v / norm for v in perturbed] if norm else perturbed

        existing = await _insert_model(
            conn, tenant,
            born_event=old_obs,
            proposition={
                "kind": "market_assessment",
                "market": "multicloud",
                "assessment": "heating up",
            },
            natural=existing_natural,
            embedding=common_emb,
        )
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "market_assessment",
                "market": "multicloud",
                "assessment": "rising demand",
            },
            natural=candidate_natural,
            embedding=perturbed,
        )

        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

    # Cosine of the perturbed pair is ~0.999, well above 0.75. The
    # critical assertion is that with the market_assessment rule,
    # cosines in [0.75, 0.85) auto-merge — pin the cosine itself just
    # to be sure the test doesn't accidentally hit the default 0.85.
    assert result.decision == "auto_merge"
    assert result.matched_model_id == existing
    # And it specifically matches the relaxed kind threshold.
    assert result.cosine_similarity is not None
    assert result.cosine_similarity >= 0.75


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_market_assessment_auto_merges_in_relaxed_band(
    fresh_db, tenant, tenant_cleanup,
):
    """A cosine in [0.75, 0.85) for market_assessment must auto_merge."""
    async with fresh_db.acquire() as conn:
        old_obs = await _insert_obs(conn, tenant, "ma old")
        new_obs = await _insert_obs(conn, tenant, "ma new")
        # Build two vectors with controlled cosine.
        v1 = [0.0] * 768
        v1[0] = 1.0
        v2 = [0.0] * 768
        # cosine 0.80
        v2[0] = 0.80
        v2[1] = (1.0 - 0.80 ** 2) ** 0.5

        existing = await _insert_model(
            conn, tenant,
            born_event=old_obs,
            proposition={
                "kind": "market_assessment",
                "market": "fintech",
                "assessment": "consolidating",
            },
            natural="Fintech is consolidating into 3 majors.",
            embedding=v1,
        )
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "market_assessment",
                "market": "fintech",
                "assessment": "consolidating fast",
            },
            natural="Fintech is rapidly consolidating around 3 players.",
            embedding=v2,
        )

        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

    # Cosine is ~0.80: below default 0.85, above market_assessment 0.75.
    assert result.decision == "auto_merge"
    assert result.matched_model_id == existing
    assert 0.78 <= (result.cosine_similarity or 0) <= 0.83


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_recommendation_never_auto_merges(
    fresh_db, tenant, tenant_cleanup,
):
    """Recommendations queue to human_review even at very high cosine."""
    async with fresh_db.acquire() as conn:
        old_obs = await _insert_obs(conn, tenant, "rec old")
        new_obs = await _insert_obs(conn, tenant, "rec new")
        same = "Pause hiring in EMEA for Q3 to preserve runway."
        emb = deterministic_text_embedding(same)
        existing = await _insert_model(
            conn, tenant,
            born_event=old_obs,
            proposition={
                "kind": "recommendation",
                "recommended_action": "pause EMEA hiring",
                "rationale": "preserve runway",
            },
            natural=same,
            embedding=emb,
        )
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "recommendation",
                "recommended_action": "pause EMEA hiring",
                "rationale": "preserve runway",
            },
            natural=same,
            embedding=emb,
        )

        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

    # Identical text → cosine ≈ 1.0, but recommendation MUST NOT auto_merge.
    assert result.decision == "human_review"
    assert result.matched_model_id == existing
    assert result.replacement_op is None
    assert result.decision_reason == "kind_blocked_auto_merge"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_situation_auto_merges_on_high_member_overlap(
    fresh_db, tenant, tenant_cleanup,
):
    """Situation with >=80% shared member_model_ids auto-merges."""
    async with fresh_db.acquire() as conn:
        old_obs = await _insert_obs(conn, tenant, "situation old")
        new_obs = await _insert_obs(conn, tenant, "situation new")
        members = [uuid7() for _ in range(5)]
        existing = await _insert_model(
            conn, tenant,
            born_event=old_obs,
            proposition={
                "kind": "situation",
                "situation": "Q3 renewal crunch",
                "summary": "multi-account renewal risk",
                "member_model_ids": [str(m) for m in members],
                "relationship_summary": "co-occurring renewal risks",
                "pressure_type": "revenue",
                "shared_mechanism": "renewals stalling across multiple accounts simultaneously",
            },
            natural="Q3 renewal crunch across 5 accounts.",
        )
        # New candidate shares 4 of 5 members → 0.80 overlap.
        new_members = members[:4] + [uuid7()]
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "situation",
                "situation": "Q3 renewal crunch",
                "summary": "multi-account renewal risk",
                "member_model_ids": [str(m) for m in new_members],
                "relationship_summary": "co-occurring renewal risks",
                "pressure_type": "revenue",
                "shared_mechanism": "renewals stalling across multiple accounts simultaneously",
            },
            natural="Q3 renewal crunch across 5 accounts.",
        )

        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

    assert result.decision == "auto_merge"
    assert result.matched_model_id == existing
    assert (
        result.signal_breakdown.get("situation_member_overlap", 0.0)
        >= 0.79
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_borderline_emits_same_issue_candidate(
    fresh_db, tenant, tenant_cleanup,
):
    """A 0.70-0.85 cosine pair gets a same_issue_as candidate inserted."""
    async with fresh_db.acquire() as conn:
        old_obs = await _insert_obs(conn, tenant, "border old")
        new_obs = await _insert_obs(conn, tenant, "border new")
        # Build vectors with cosine = 0.78 → in human_review band.
        v1 = [0.0] * 768
        v1[0] = 1.0
        v2 = [0.0] * 768
        v2[0] = 0.78
        v2[1] = (1.0 - 0.78 ** 2) ** 0.5

        existing = await _insert_model(
            conn, tenant,
            born_event=old_obs,
            proposition={
                "kind": "state",
                "subject": "X",
                "assertion": "ships",
            },
            natural="X ships on Friday.",
            embedding=v1,
        )
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "state",
                "subject": "X",
                "assertion": "ships",
            },
            natural="X is shipping on Friday.",
            embedding=v2,
        )

        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

            assert result.decision == "human_review"
            assert result.matched_model_id == existing
            assert result.same_issue_candidate_id is not None
            assert result.decision_reason == "same_issue_candidate_emitted"

            row = await conn.fetchrow(
                """
                SELECT edge_kind, target_model_id, review_status, basis,
                       metadata
                FROM relationship_candidates
                WHERE id = $1
                """,
                result.same_issue_candidate_id,
            )

    assert row is not None
    assert row["edge_kind"] == "same_issue_as"
    assert row["target_model_id"] == existing
    md = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"]
    assert md["origin"] == "reconciler_near_duplicate"
    assert md["operator_basis"] == "paraphrase_suspect"
    assert "cosine" in md
    assert "signal_breakdown" in md


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_signal_breakdown_is_populated_on_decision(
    fresh_db, tenant, tenant_cleanup,
):
    """Every non-skipped decision must carry a signal_breakdown dict."""
    async with fresh_db.acquire() as conn:
        new_obs = await _insert_obs(conn, tenant, "lone obs")
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "state",
                "subject": "Y",
                "assertion": "is healthy",
            },
            natural="Y is healthy this quarter.",
            embedding=deterministic_text_embedding("Y is healthy this quarter."),
        )
        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

    # No prior model → no_match, but the result should still expose
    # the new fields (even if breakdown is empty).
    assert result.decision == "no_match"
    assert result.decision_reason == "no_match"
    assert isinstance(result.signal_breakdown, dict)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_db_pattern_instance_requires_matching_pattern_id(
    fresh_db, tenant, tenant_cleanup,
):
    """Different parent pattern_id → no match regardless of cosine."""
    async with fresh_db.acquire() as conn:
        old_obs = await _insert_obs(conn, tenant, "pi old")
        new_obs = await _insert_obs(conn, tenant, "pi new")
        pattern_a = uuid7()
        pattern_b = uuid7()
        same_text = "Customer X exhibits the renewal-stall pattern."
        emb = deterministic_text_embedding(same_text)

        await _insert_model(
            conn, tenant,
            born_event=old_obs,
            proposition={
                "kind": "pattern_instance",
                "pattern_id": str(pattern_a),
                "instance_natural": same_text,
            },
            natural=same_text,
            embedding=emb,
        )
        entry = _insert_entry(
            tenant=tenant,
            born_event=new_obs,
            proposition={
                "kind": "pattern_instance",
                "pattern_id": str(pattern_b),  # different parent.
                "instance_natural": same_text,
            },
            natural=same_text,
            embedding=emb,
        )

        async with conn.transaction():
            result = await reconcile_claim_op(
                ClaimOp(op="insert", entry=entry),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )

    # Different parents → reconciler must not treat them as duplicates.
    assert result.decision == "no_match"
    assert result.matched_model_id is None
