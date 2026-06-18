from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.reasoning.think.diff_schema import EdgeOp
from services.reasoning.think.edge_semantics import (
    assess_edge_specificity,
    canonicalize_edge_semantics,
    enforce_edge_specificity,
    normalize_edge_review_status,
)


pytestmark = pytest.mark.asyncio


class _NoRowsConn:
    async def fetch(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []


async def test_edge_semantics_does_not_rewrite_explicit_explains_to_blocks():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="explains",
        explanation=(
            "This explains the composite. It does not block the target and is "
            "not clearly a blocking dependency."
        ),
    )

    out = await canonicalize_edge_semantics(
        op,
        _NoRowsConn(),  # type: ignore[arg-type]
        tenant_id=uuid7(),
    )

    assert out.edge_kind == "explains"
    assert out.metadata is None or "canonicalized_by" not in out.metadata


async def test_edge_semantics_preserves_negated_blocking_language():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="supports",
        explanation=(
            "This does not block the target; it helps explain the customer "
            "risk mechanism."
        ),
    )

    out = await canonicalize_edge_semantics(
        op,
        _NoRowsConn(),  # type: ignore[arg-type]
        tenant_id=uuid7(),
    )

    assert out.edge_kind == "explains"
    assert out.edge_kind != "blocks"


async def test_edge_semantics_does_not_rewrite_analogy_from_endpoint_terms():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="analogous_to",
        explanation=(
            "Both memories are analogous enterprise-review patterns; the edge "
            "does not assert a dependency between them."
        ),
    )

    out = await canonicalize_edge_semantics(
        op,
        _NoRowsConn(),  # type: ignore[arg-type]
        tenant_id=uuid7(),
    )

    assert out.edge_kind == "analogous_to"
    assert out.metadata is None or "canonicalized_by" not in out.metadata


async def test_edge_specificity_downgrades_generic_source_digest_hub_edge():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="supports",
        explanation="These source summaries seem related.",
        review_status="accepted",
    )
    source_model = {
        "proposition": {
            "claim_role": "pattern",
            "retrieval_tags": ["source_digest", "major_source_window"],
        },
        "scope_entities": [],
        "scope_actors": [],
    }
    target_model = {
        "proposition": {
            "claim_role": "pattern",
            "retrieval_tags": ["source_digest"],
        },
        "scope_entities": [],
        "scope_actors": [],
    }

    assessment = assess_edge_specificity(
        op,
        source_model=source_model,
        target_model=target_model,
    )
    guarded = enforce_edge_specificity(
        op,
        source_model=source_model,
        target_model=target_model,
    )

    assert assessment.needs_review is True
    assert "generic_source_digest_endpoint" in assessment.reasons
    assert guarded.review_status == "needs_review"
    assert guarded.metadata["review_status_downgraded_by"] == "edge_specificity_guard"


async def test_edge_specificity_accepts_concrete_evidence_backed_blocker_edge():
    evidence_id = uuid7()
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="blocks",
        explanation=(
            "The security review blocks the launch because approval is required "
            "before production rollout."
        ),
        evidence_event_ids=[evidence_id],
        confidence=0.78,
        review_status="candidate",
    )
    source_model = {"scope_entities": [{"type": "workstream", "id": "security"}]}
    target_model = {"scope_entities": [{"type": "workstream", "id": "launch"}]}

    normalized = normalize_edge_review_status(op, endpoint_models_verified=True)
    guarded = enforce_edge_specificity(
        normalized,
        source_model=source_model,
        target_model=target_model,
    )

    assert normalized.review_status == "accepted"
    assert guarded.review_status == "accepted"
    assert guarded.metadata["edge_specificity_score"] >= 0.8


async def test_edge_specificity_keeps_generic_edge_candidate_when_specificity_is_thin():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="supports",
        explanation="They are related.",
        review_status="accepted",
    )

    guarded = enforce_edge_specificity(op)

    assert guarded.review_status == "needs_review"
    assert "missing_edge_evidence" in guarded.metadata["edge_specificity_reasons"]
