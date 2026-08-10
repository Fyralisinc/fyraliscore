from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError as PydanticValidationError

from lib.shared.ids import uuid7
from services.domain.identity.foundation import EntityMentionRow
from services.domain.identity.resolution import (
    CandidateSeed,
    IdentityConstraintValue,
    IdentityResolutionSnapshot,
    IdentitySnapshotItem,
    decide_resolution,
    rank_candidates,
)


def _mention(*, expected_types: tuple[str, ...] = ("person",)) -> EntityMentionRow:
    now = datetime.now(UTC)
    return EntityMentionRow(
        id=uuid7(),
        tenant_id=uuid7(),
        observation_id=uuid7(),
        observation_occurred_at=now,
        evidence_id=uuid7(),
        mention_kind="text",
        text="Sam",
        expected_types=expected_types,
        context={},
        mention_key="a" * 64,
        status="registered",
        created_at=now,
    )


def test_deterministic_source_candidate_resolves_without_model_judgment() -> None:
    mention = _mention(expected_types=("document",))
    candidate = CandidateSeed(
        candidate_ref={"type": "source_reference", "id": str(uuid7())},
        retrieval_method="deterministic_source_ref",
        features={"direct_source_identity": 1.0, "type_compatibility": 1.0},
        expected_type="document",
    )
    ranked = rank_candidates(
        mention, [candidate], constraints=[], evaluated_at=datetime.now(UTC)
    )
    decision = decide_resolution(mention, ranked)

    assert decision.outcome == "resolved"
    assert decision.selected_ref == candidate.candidate_ref
    assert ranked[0].retrieval_methods == ("deterministic_source_ref",)


def test_equal_high_scoring_names_remain_ambiguous() -> None:
    mention = _mention()
    candidates = [
        CandidateSeed(
            candidate_ref={"type": "actor", "id": str(uuid7())},
            retrieval_method="exact_alias",
            features={
                "exact_alias": 1.0,
                "alias_confidence": 0.9,
                "name_similarity": 1.0,
                "type_compatibility": 1.0,
            },
            expected_type="person",
        )
        for _ in range(2)
    ]
    ranked = rank_candidates(
        mention, candidates, constraints=[], evaluated_at=datetime.now(UTC)
    )
    decision = decide_resolution(mention, ranked)

    assert decision.outcome == "ambiguous"
    assert decision.confidence == 1.0
    assert len(decision.alternatives) == 1
    assert "acceptance_margin_not_met" in decision.reasons


def test_cannot_link_overrides_a_perfect_candidate() -> None:
    mention = _mention()
    ref = {"type": "actor", "id": str(uuid7())}
    constraint = IdentityConstraintValue(
        id=uuid7(),
        kind="cannot_link",
        left_ref={"kind": "mention", "id": str(mention.id)},
        right_ref=ref,
        authority="human",
        valid_from=datetime.now(UTC) - timedelta(minutes=1),
    )
    ranked = rank_candidates(
        mention,
        [
            CandidateSeed(
                candidate_ref=ref,
                retrieval_method="structured_hint",
                features={"provided_reference": 1.0},
                expected_type="person",
            )
        ],
        constraints=[constraint],
        evaluated_at=datetime.now(UTC),
    )

    assert ranked[0].constraint_outcome == "cannot_link"
    assert ranked[0].score == 0
    assert decide_resolution(mention, ranked).outcome == "unresolved"


def test_type_mismatch_rejects_deterministic_reference() -> None:
    mention = _mention(expected_types=("person",))
    ranked = rank_candidates(
        mention,
        [
            CandidateSeed(
                candidate_ref={"type": "work_item", "id": "SEC-42"},
                retrieval_method="deterministic_source_ref",
                features={"direct_source_identity": 1.0},
                expected_type="work_item",
            )
        ],
        constraints=[],
        evaluated_at=datetime.now(UTC),
    )
    assert ranked[0].constraint_outcome == "type_rejected"
    assert decide_resolution(mention, ranked).outcome == "unresolved"


def test_identity_snapshots_are_content_addressed() -> None:
    now = datetime.now(UTC)
    mention = _mention()
    snapshot = IdentityResolutionSnapshot.seal(
        id=uuid7(),
        tenant_id=mention.tenant_id,
        resolver_run_id=uuid7(),
        input_kind="observation",
        observation_id=mention.observation_id,
        observation_occurred_at=mention.observation_occurred_at,
        resolution_status="partial",
        items=(
            IdentitySnapshotItem(
                mention_id=mention.id,
                outcome="unresolved",
                confidence=0,
                reasons=("no_candidates",),
            ),
        ),
        resolver_name="fyralis-identity",
        resolver_version="1.0.0",
        policy_version="source-grounded-v1",
        created_at=now,
    )
    assert len(snapshot.snapshot_hash) == 64

    tampered = snapshot.model_dump(mode="json")
    tampered["resolution_status"] = "complete"
    with pytest.raises(PydanticValidationError, match="hash does not match"):
        IdentityResolutionSnapshot.model_validate(tampered)
