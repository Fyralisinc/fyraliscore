from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from services.platform.execution import evidence_utils, inquiry
from services.platform.execution.types import EvidenceCard


def _card(
    *,
    summary: str = "Launch owner assigned",
    source_type: str = "observation",
    timestamp: datetime | None = None,
) -> EvidenceCard:
    return EvidenceCard(
        evidence_id=uuid4(),
        source_type=source_type,
        source_ref="observation:1",
        source_ref_id=uuid4(),
        summary=summary,
        trust_tier="authoritative",
        timestamp=timestamp,
        retrieval_paths={"semantic", "focused_index"},
        retrieved_for_questions={"Q2", "Q1"},
        supports_hypotheses={"H0"},
        weakens_hypotheses=set(),
        contradicts_hypotheses={"H1"},
        raw_content_ref="observation:1",
        token_estimate=12,
        score=0.123456,
    )


def test_evidence_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._compact is evidence_utils.compact
    assert inquiry._declares_unrelated_to_trigger is (
        evidence_utils.declares_unrelated_to_trigger
    )
    assert inquiry._estimate_tokens is evidence_utils.estimate_tokens
    assert inquiry._evidence_supports_ownership is (
        evidence_utils.evidence_supports_ownership
    )
    assert inquiry._evidence_to_dict is evidence_utils.evidence_to_dict
    assert inquiry._has_material_trigger_overlap is (
        evidence_utils.has_material_trigger_overlap
    )
    assert inquiry._is_counterevidence_for_leading_hypothesis is (
        evidence_utils.is_counterevidence_for_leading_hypothesis
    )
    assert inquiry._is_stale_relative_to_trigger is (
        evidence_utils.is_stale_relative_to_trigger
    )
    assert inquiry._jsonable is evidence_utils.jsonable
    assert inquiry._material_tokens is evidence_utils.material_tokens
    assert inquiry._sensitivity is evidence_utils.sensitivity
    assert inquiry._stable_hash is evidence_utils.stable_hash
    assert inquiry._timestamp_sort_value is evidence_utils.timestamp_sort_value
    assert inquiry._trust_score is evidence_utils.trust_score


def test_json_hash_and_text_helpers_are_stable() -> None:
    now = datetime(2026, 6, 13, 8, 0, tzinfo=timezone.utc)
    tenant_id = uuid4()

    assert evidence_utils.stable_hash({"b": 2, "a": 1}) == evidence_utils.stable_hash(
        {"a": 1, "b": 2}
    )
    assert evidence_utils.jsonable(
        {"id": tenant_id, "at": now, "tags": {"b", "a"}}
    ) == {
        "id": str(tenant_id),
        "at": "2026-06-13T08:00:00+00:00",
        "tags": ["a", "b"],
    }
    assert evidence_utils.compact("  alpha\n beta  gamma  ", 13) == "alpha beta..."
    assert evidence_utils.estimate_tokens("abcd" * 3) == 3
    assert evidence_utils.sensitivity("contains an API key") == "sensitive"
    assert evidence_utils.sensitivity("ordinary update") == "normal"


def test_material_overlap_and_unrelated_detection() -> None:
    assert evidence_utils.material_tokens("Customer launch risk alpha42") == {"alpha42"}
    assert evidence_utils.has_material_trigger_overlap(
        "alpha42 is resolved",
        "customer risk alpha42",
    )
    assert not evidence_utils.has_material_trigger_overlap(
        "beta is resolved",
        "customer risk alpha42",
    )
    assert evidence_utils.has_material_trigger_overlap(
        "anything",
        "customer risk",
    )
    assert evidence_utils.declares_unrelated_to_trigger(
        "this finding is unrelated to the current trigger"
    )


def test_evidence_card_helpers_preserve_packet_shape_and_scoring() -> None:
    now = datetime(2026, 6, 13, 8, 0, tzinfo=timezone.utc)
    card = _card(timestamp=now - timedelta(days=3))

    as_dict = evidence_utils.evidence_to_dict(card)

    assert as_dict["retrieval_paths"] == ["focused_index", "semantic"]
    assert as_dict["retrieved_for_questions"] == ["Q1", "Q2"]
    assert as_dict["supports_hypotheses"] == ["H0"]
    assert as_dict["contradicts_hypotheses"] == ["H1"]
    assert as_dict["score"] == 0.1235
    assert evidence_utils.is_counterevidence_for_leading_hypothesis(card)
    assert evidence_utils.is_stale_relative_to_trigger(
        card,
        trigger_occurred_at=now,
        stale_after_days=1,
    )
    assert not evidence_utils.is_stale_relative_to_trigger(
        card,
        trigger_occurred_at=now,
        stale_after_days=10,
    )
    assert evidence_utils.timestamp_sort_value(card.timestamp) < (
        evidence_utils.timestamp_sort_value(now)
    )
    assert evidence_utils.trust_score("authoritative") == 0.30


def test_evidence_supports_ownership_requires_positive_owner_signal() -> None:
    owner = uuid4()

    assert evidence_utils.evidence_supports_ownership(
        _card(summary=f"launch update owner={owner}")
    )
    assert not evidence_utils.evidence_supports_ownership(
        _card(summary="launch update owner=unassigned")
    )
    assert not evidence_utils.evidence_supports_ownership(
        _card(summary="missing owner for launch")
    )
