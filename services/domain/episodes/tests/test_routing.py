from __future__ import annotations

from datetime import UTC, datetime, timedelta

from lib.shared.ids import uuid7
from services.domain.episodes.routing import RoutingSignal, TopicCandidate, score_membership


NOW = datetime(2026, 8, 5, tzinfo=UTC)


def _signal(anchor, *, text="authentication audit is complete"):
    return RoutingSignal(
        tenant_id=uuid7(), observation_id=uuid7(), evidence_id=uuid7(),
        identity_snapshot_id=uuid7(), knowledge_snapshot_id=uuid7(),
        knowledge_snapshot_hash="a" * 64, claim_set_hash="b" * 64,
        occurred_at=NOW, ingested_at=NOW,
        source="notion", installation_scope="notion:alpen", content_text=text,
        primary_anchor=anchor, anchor_refs=(anchor,),
        lexical_terms=tuple(text.split()), topic_label="security audit",
    )


def _candidate(signal, anchor, *, text="authentication audit remains open"):
    return TopicCandidate(
        topic_id=uuid7(), episode_id=uuid7(), primary_anchor=anchor,
        anchor_refs=(anchor,), claim_predicates=(), lexical_terms=tuple(text.split()),
        last_event_at=signal.occurred_at - timedelta(hours=2),
    )


def test_stable_cross_source_anchor_includes_membership() -> None:
    anchor = {"type": "workstream", "id": "security-audit"}
    signal = _signal(anchor)
    decision = score_membership(signal, _candidate(signal, anchor))
    assert decision.decision == "include"
    assert decision.feature_snapshot["primary_anchor_equal"] is True


def test_lexical_similarity_alone_cannot_merge_conflicting_audits() -> None:
    signal = _signal({"type": "workstream", "id": "security-audit"})
    candidate = _candidate(
        signal, {"type": "workstream", "id": "marketing-content-audit"},
        text="authentication audit is complete",
    )
    decision = score_membership(signal, candidate)
    assert decision.decision == "exclude"
    assert decision.feature_snapshot["conflicting_primary_anchor"] is True
    assert any(reason.code == "hard_negative" for reason in decision.reasons)


def test_weak_lexical_candidate_is_held_or_excluded_not_forced() -> None:
    signal = _signal(
        {"type": "observation_seed", "id": "one"},
        text="audit payments scheduled wednesday",
    )
    candidate = _candidate(
        signal,
        {"type": "observation_seed", "id": "two"},
        text="audit payments scheduled thursday",
    )
    assert score_membership(signal, candidate).decision != "include"
