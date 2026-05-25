from __future__ import annotations

from uuid import uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.relationships import (
    JudgmentScores,
    RelationshipCandidatesRepo,
    adjudicate_candidate_for_trigger,
    candidate_id_from_trigger,
    load_candidate_for_trigger,
    make_edge_candidate,
    make_situation_candidate,
)
from services.retrieval.primary import TriggerContext
from services.think.diff_schema import ClaimOp, EdgeOp, ValidatedDiff


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _diff(
    tenant_id,
    *,
    claim_ops=None,
    edge_ops=None,
    dropped_op_count: int = 0,
) -> ValidatedDiff:
    return ValidatedDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant_id,
        claim_ops=claim_ops or [],
        edge_ops=edge_ops or [],
        dropped_op_count=dropped_op_count,
        dropped_op_errors=["bad op"] if dropped_op_count else [],
    )


def _trigger(tenant_id, candidate_id, members) -> TriggerContext:
    return TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=tenant_id,
        member_model_ids=list(members),
        seed_signature={"relationship_candidate_id": str(candidate_id)},
    )


async def test_adjudication_accepts_candidate_when_edge_applied(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    edge_id = uuid7()
    repo = RelationshipCandidatesRepo()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="blocks",
        basis="topology_suggested",
        explanation="Topology thinks left blocks right.",
        scores=JudgmentScores(impact=0.8, actionability=0.8, confidence=0.8),
        source="latent_topology",
        metadata={
            "mechanism": "Source names the target as a hard dependency.",
            "dependency_basis": "explicit_blocker_target_reference",
        },
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, [left, right]),
            diff=_diff(
                tenant_id,
                edge_ops=[
                    EdgeOp(
                        op="add",
                        source_model_id=left,
                        target_model_id=right,
                        edge_kind="blocks",
                    )
                ],
            ),
            applied={
                "edge_ops": [
                    {
                        "op": "add",
                        "edge_kind": "blocks",
                        "source_model_id": str(left),
                        "target_model_id": str(right),
                        "edge_ids": [str(edge_id)],
                        "review_status": "accepted",
                    }
                ],
            },
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "accepted"
    assert result.accepted_edge_ids == (edge_id,)
    assert row is not None
    assert row["review_status"] == "accepted"
    assert row["accepted_edge_ids"] == [edge_id]
    assert row["metadata"]["latest_adjudication"]["reason"] == (
        "think_promoted_candidate_to_durable_memory"
    )


async def test_adjudication_accepts_candidate_when_situation_inserted(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    members = (uuid7(), uuid7(), uuid7())
    accepted_model_id = uuid7()
    repo = RelationshipCandidatesRepo()
    candidate = make_situation_candidate(
        tenant_id=tenant_id,
        situation="renewal risk pressure",
        summary="Several models describe one renewal risk.",
        relationship_summary="The members interact as one situation.",
        member_model_ids=members,
        basis="topology_suggested",
        scores=JudgmentScores(impact=0.9, actionability=0.7, confidence=0.7),
        source="latent_topology",
    )
    claim = ClaimOp(
        op="insert",
        entry={
            "proposition": {
                "kind": "situation",
                "situation": "renewal risk pressure",
                "summary": "Several models describe one renewal risk.",
                "member_model_ids": [str(m) for m in members],
                "relationship_summary": "The members interact.",
                "status": "forming",
            },
            "natural": "renewal risk pressure",
            "embedding": [1.0] + [0.0] * 767,
            "scope_temporal": {},
            "confidence": 0.7,
            "confidence_at_assertion": 0.7,
        },
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, members),
            diff=_diff(tenant_id, claim_ops=[claim]),
            applied={
                "claim_ops": [
                    {
                        "op": "insert",
                        "model_id": str(accepted_model_id),
                        "proposition_kind": "situation",
                    }
                ],
            },
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "accepted"
    assert result.accepted_model_id == accepted_model_id
    assert row is not None
    assert row["accepted_model_id"] == accepted_model_id


async def test_adjudication_rejects_candidate_on_empty_diff(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    repo = RelationshipCandidatesRepo()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="same_issue_as",
        basis="topology_suggested",
        explanation="Topology thinks these are the same issue.",
        scores=JudgmentScores(impact=0.5, confidence=0.6),
        source="latent_topology",
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, [left, right]),
            diff=_diff(tenant_id),
            applied={},
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "rejected"
    assert row is not None
    assert row["review_status"] == "rejected"


async def test_adjudication_marks_needs_review_for_uncertain_edge(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    repo = RelationshipCandidatesRepo()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="early_warning_for",
        basis="topology_suggested",
        explanation="Topology thinks left is an early warning for right.",
        scores=JudgmentScores(impact=0.7, confidence=0.5),
        source="latent_topology",
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, [left, right]),
            diff=_diff(tenant_id),
            applied={
                "edge_ops": [
                    {
                        "op": "add",
                        "edge_kind": "early_warning_for",
                        "source_model_id": str(left),
                        "target_model_id": str(right),
                        "edge_ids": [],
                        "review_status": "needs_review",
                    }
                ],
            },
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "needs_review"
    assert row is not None
    assert row["review_status"] == "needs_review"
    assert row["decided_at"] is None


async def test_adjudication_marks_needs_review_when_validation_drops_ops(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    repo = RelationshipCandidatesRepo()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="blocks",
        basis="topology_suggested",
        explanation="Topology thinks left blocks right.",
        scores=JudgmentScores(impact=0.7, confidence=0.5),
        source="latent_topology",
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, [left, right]),
            diff=_diff(tenant_id, dropped_op_count=1),
            applied={},
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "needs_review"
    assert row is not None
    assert row["metadata"]["latest_adjudication"]["dropped_op_count"] == 1


async def test_load_candidate_for_trigger_attaches_prompt_shape(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    repo = RelationshipCandidatesRepo()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="blocks",
        basis="topology_suggested",
        explanation="Topology thinks left blocks right.",
        scores=JudgmentScores(impact=0.8, actionability=0.8, confidence=0.8),
        source="latent_topology",
    )
    trigger = _trigger(tenant_id, candidate.id, [])

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        row = await load_candidate_for_trigger(conn, trigger)

    assert row is not None
    assert trigger.member_model_ids == [left, right]
    assert trigger.seed_signature["relationship_candidate"]["edge_kind"] == "blocks"


async def test_irrelevant_trigger_has_no_candidate_id() -> None:
    trigger = TriggerContext(kind="T1", tenant_id=uuid4())
    assert candidate_id_from_trigger(trigger) is None


async def test_adjudication_blocks_without_mechanism_marks_needs_review(
    fresh_db: asyncpg.Pool,
) -> None:
    """Even if Think applies an edge of the requested kind, missing
    structural justification (mechanism / dependency_basis for `blocks`)
    must downgrade the row to `needs_review` instead of `accepted`."""
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    edge_id = uuid7()
    repo = RelationshipCandidatesRepo()
    # Build a `blocks` candidate without a mechanism: pass basis=
    # "topology_suggested" (not causal_*) and no mechanism_summary so
    # the constructor doesn't force one.
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="blocks",
        basis="topology_suggested",
        explanation="Topology guessed blocks without justification.",
        scores=JudgmentScores(impact=0.7, confidence=0.6),
        source="latent_topology",
    )
    assert "mechanism" not in candidate.metadata
    assert "dependency_basis" not in candidate.metadata
    assert "causal" not in candidate.metadata

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, [left, right]),
            diff=_diff(
                tenant_id,
                edge_ops=[
                    EdgeOp(
                        op="add",
                        source_model_id=left,
                        target_model_id=right,
                        edge_kind="blocks",
                    )
                ],
            ),
            applied={
                "edge_ops": [
                    {
                        "op": "add",
                        "edge_kind": "blocks",
                        "source_model_id": str(left),
                        "target_model_id": str(right),
                        "edge_ids": [str(edge_id)],
                        "review_status": "accepted",
                    }
                ],
            },
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "needs_review"
    assert result.decision_reason == "needs_review_missing_mechanism"
    assert row is not None
    assert row["review_status"] == "needs_review"
    md = row["metadata"]["latest_adjudication"]
    assert md["decision_reason"] == "needs_review_missing_mechanism"
    assert "mechanism_or_dependency_basis" in md["missing_fields"]


async def test_adjudication_early_warning_without_lead_time_marks_needs_review(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    edge_id = uuid7()
    repo = RelationshipCandidatesRepo()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="early_warning_for",
        basis="topology_suggested",
        explanation="Topology guess without lead-time evidence.",
        scores=JudgmentScores(impact=0.6, confidence=0.55),
        source="latent_topology",
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, [left, right]),
            diff=_diff(
                tenant_id,
                edge_ops=[
                    EdgeOp(
                        op="add",
                        source_model_id=left,
                        target_model_id=right,
                        edge_kind="early_warning_for",
                    )
                ],
            ),
            applied={
                "edge_ops": [
                    {
                        "op": "add",
                        "edge_kind": "early_warning_for",
                        "source_model_id": str(left),
                        "target_model_id": str(right),
                        "edge_ids": [str(edge_id)],
                        "review_status": "accepted",
                    }
                ],
            },
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "needs_review"
    assert result.decision_reason == "needs_review_missing_mechanism"
    assert row is not None
    assert row["metadata"]["latest_adjudication"]["decision_reason"] == (
        "needs_review_missing_mechanism"
    )


async def test_adjudication_accepted_with_justification_records_decision_reason(
    fresh_db: asyncpg.Pool,
) -> None:
    """The accepted-edge case still works and now records the
    `accepted_with_justification` decision_reason for the dashboard."""
    tenant_id = uuid7()
    left = uuid7()
    right = uuid7()
    edge_id = uuid7()
    repo = RelationshipCandidatesRepo()
    # causal_hypothesis -> mechanism_summary required -> structural ok.
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=left,
        target_model_id=right,
        edge_kind="blocks",
        basis="causal_hypothesis",
        explanation="Topology thinks left blocks right with mechanism.",
        scores=JudgmentScores(impact=0.8, actionability=0.7, confidence=0.7),
        source="latent_topology",
        mechanism_summary="security review is the dependency",
        intervention_surface="remove blocker",
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        result = await adjudicate_candidate_for_trigger(
            conn,
            trigger=_trigger(tenant_id, candidate.id, [left, right]),
            diff=_diff(
                tenant_id,
                edge_ops=[
                    EdgeOp(
                        op="add",
                        source_model_id=left,
                        target_model_id=right,
                        edge_kind="blocks",
                    )
                ],
            ),
            applied={
                "edge_ops": [
                    {
                        "op": "add",
                        "edge_kind": "blocks",
                        "source_model_id": str(left),
                        "target_model_id": str(right),
                        "edge_ids": [str(edge_id)],
                        "review_status": "accepted",
                    }
                ],
            },
        )
        row = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert result is not None
    assert result.review_status == "accepted"
    assert result.decision_reason == "accepted_with_justification"
    assert row is not None
    assert row["metadata"]["latest_adjudication"]["decision_reason"] == (
        "accepted_with_justification"
    )
