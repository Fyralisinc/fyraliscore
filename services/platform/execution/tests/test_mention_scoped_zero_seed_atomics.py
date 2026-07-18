from __future__ import annotations

from uuid import UUID, uuid4

from services.platform.execution import context_packet
from services.reasoning.retrieval.primary import TriggerContext


def _assertion(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    text: str,
    surface: str,
    authority: str,
    canonical_ref: str,
    detection_id: str | None,
) -> dict[str, object]:
    start = text.index(surface)
    return {
        "tenant_id": str(tenant_id),
        "observation_id": str(observation_id),
        "source_channel": "slack:message",
        "assertion_text": text,
        "evidence_address": f"observation:{observation_id}:content_text",
        "evidence_field_path": "content_text",
        "evidence_span_start": start,
        "evidence_span_end": start + len(surface),
        "governed_surface": surface,
        "canonical_ref": canonical_ref,
        "coordinate_authority": authority,
        "detection_id": detection_id,
        "uncertainty": [],
    }


def _trigger(
    *, tenant_id: UUID, assertions: list[dict[str, object]], canonical_ref: str,
) -> TriggerContext:
    observation_ids = [UUID(str(row["observation_id"])) for row in assertions]
    return TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_ids=observation_ids,
        seed_signature={
            "governed_learning_episodes": [{
                "episode_id": "mention-scoped-zero-seed-test",
                "tenant_id": str(tenant_id),
                "canonical_ref": canonical_ref,
                "assertions": assertions,
                "uncertainty": [],
            }],
        },
    )


def test_exact_detection_uuid_is_the_only_provisional_scope_coordinate() -> None:
    tenant_id, observation_id, detection_id = uuid4(), uuid4(), uuid4()
    canonical_ref = "provisional:atlas-release"
    trigger = _trigger(
        tenant_id=tenant_id,
        canonical_ref=canonical_ref,
        assertions=[_assertion(
            tenant_id=tenant_id,
            observation_id=observation_id,
            text="Atlas release, update 1: rollout moved to Friday.",
            surface="Atlas release",
            authority="provisional",
            canonical_ref=canonical_ref,
            detection_id=str(detection_id),
        )],
    )

    candidates, material = context_packet._batch_fragment_candidates(trigger)

    assert material is True
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.canonical_scope_ref == f"mention:{detection_id}"
    assert candidate.member_observation_ids == (str(observation_id),)
    assert candidate.source_observation_ids == (str(observation_id),)
    assert candidate.observation_evidence[0]["detection_id"] == str(detection_id)


def test_same_surface_with_distinct_detections_never_collapses_scope() -> None:
    tenant_id = uuid4()
    canonical_ref = "provisional:atlas-release"
    first_observation, second_observation = uuid4(), uuid4()
    first_detection, second_detection = uuid4(), uuid4()
    trigger = _trigger(
        tenant_id=tenant_id,
        canonical_ref=canonical_ref,
        assertions=[
            _assertion(
                tenant_id=tenant_id,
                observation_id=first_observation,
                text="Atlas release, update 1: rollout moved to Friday.",
                surface="Atlas release",
                authority="provisional",
                canonical_ref=canonical_ref,
                detection_id=str(first_detection),
            ),
            _assertion(
                tenant_id=tenant_id,
                observation_id=second_observation,
                text="Atlas release, update 1: checklist gained an owner.",
                surface="Atlas release",
                authority="provisional",
                canonical_ref=canonical_ref,
                detection_id=str(second_detection),
            ),
        ],
    )

    candidates, material = context_packet._batch_fragment_candidates(trigger)

    assert material is True
    assert len(candidates) == 2
    assert {candidate.canonical_scope_ref for candidate in candidates} == {
        f"mention:{first_detection}",
        f"mention:{second_detection}",
    }
    assert len({candidate.candidate_id for candidate in candidates}) == 2
    assert all(len(candidate.member_observation_ids) == 1 for candidate in candidates)


def test_provisional_scope_authorizes_claim_only_not_identity_alias_or_relation() -> None:
    tenant_id, observation_id, detection_id = uuid4(), uuid4(), uuid4()
    canonical_ref = "provisional:atlas-release"
    trigger = _trigger(
        tenant_id=tenant_id,
        canonical_ref=canonical_ref,
        assertions=[_assertion(
            tenant_id=tenant_id,
            observation_id=observation_id,
            text="Atlas release, update 1: rollout moved to Friday.",
            surface="Atlas release",
            authority="provisional",
            canonical_ref=canonical_ref,
            detection_id=str(detection_id),
        )],
    )

    candidates, _ = context_packet._batch_fragment_candidates(trigger)
    candidate = candidates[0]

    assert candidate.op_family == "claim_insert"
    assert candidate.candidate_kind is None
    assert candidate.allowed_operations == ()
    assert candidate.target_model_ids == ()
    assert candidate.relation_evidence_observation_ids == ()
    assert candidate.relation_observation_evidence == ()
    assert candidate.suggested_edge_kinds == ()
    assert candidate.observation_evidence[0]["canonical_ref"] == (
        f"mention:{detection_id}"
    )
    assert not {
        "entity_type", "canonical_entity_id", "alias", "relation_kind",
    } & candidate.observation_evidence[0].keys()
    assert context_packet.synthesis_conclusion_coordinates(trigger) == ()


def test_unresolved_coordinate_without_valid_detection_remains_uncertainty() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    canonical_ref = "unresolved:atlas-release"
    trigger = _trigger(
        tenant_id=tenant_id,
        canonical_ref=canonical_ref,
        assertions=[_assertion(
            tenant_id=tenant_id,
            observation_id=observation_id,
            text="Atlas release, update 1: ownership remains unclear.",
            surface="Atlas release",
            authority="unresolved",
            canonical_ref=canonical_ref,
            detection_id=None,
        )],
    )

    assert context_packet._batch_fragment_candidates(trigger) == ([], False)
    uncertainty = context_packet.batch_fragment_uncertainty_signals(trigger)
    assert len(uncertainty) == 1
    assert uncertainty[0]["observation_id"] == str(observation_id)
    assert uncertainty[0]["kind"] == "missing_governed_entity_coordinate"
    assert uncertainty[0]["routing"] == "entity_resolution"


def test_resolved_canonical_scope_behavior_is_unchanged() -> None:
    tenant_id = uuid4()
    canonical_ref = "workstream:atlas-release"
    first_observation, second_observation = uuid4(), uuid4()
    trigger = _trigger(
        tenant_id=tenant_id,
        canonical_ref=canonical_ref,
        assertions=[
            _assertion(
                tenant_id=tenant_id,
                observation_id=first_observation,
                text="Atlas release, update 1: rollout moved to Friday.",
                surface="Atlas release",
                authority="resolved",
                canonical_ref=canonical_ref,
                detection_id=str(uuid4()),
            ),
            _assertion(
                tenant_id=tenant_id,
                observation_id=second_observation,
                text="Atlas release, update 1: checklist gained an owner.",
                surface="Atlas release",
                authority="resolved",
                canonical_ref=canonical_ref,
                detection_id=str(uuid4()),
            ),
        ],
    )

    candidates, material = context_packet._batch_fragment_candidates(trigger)

    assert material is True
    assert len(candidates) == 2
    assert {candidate.canonical_scope_ref for candidate in candidates} == {
        canonical_ref,
    }
    assert all(
        candidate.observation_evidence[0]["coordinate_authority"] == "resolved"
        for candidate in candidates
    )
    assert all(candidate.op_family == "claim_insert" for candidate in candidates)
    assert context_packet.batch_fragment_uncertainty_signals(trigger) == []
