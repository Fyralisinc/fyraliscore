from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.contracts.perception import EntityCandidateKind
from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    GroundingCandidateInput,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection


NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
TENANT = uuid4()
OBSERVATION = uuid4()
CUSTOMER = {"type": "customer", "id": "customer:nimbus"}


def _build_detected_episode(*, content_text: str | None = None, **kwargs):
    context_command = kwargs.get("prepared_context_command")
    context_outcome = kwargs.get("prepared_context_outcome")
    if context_command is None or context_outcome is None:
        context_command, context_outcome = prepare_context_selection(
            tenant_id=kwargs["tenant_id"],
            observation_id=kwargs["observation_id"],
            phrase=kwargs["phrase"],
            occurred_at=kwargs["occurred_at"],
            source_channel=kwargs["source_channel"],
            source_space=kwargs["source_space"],
            topology_incomplete=kwargs["topology_incomplete"],
            boundary_hypotheses=kwargs["boundary_hypotheses"],
            context_observations=kwargs["context_observations"],
            selection_dependency_refs=kwargs["selection_dependency_refs"],
            now=kwargs["now"],
        )
        kwargs["prepared_context_command"] = context_command
        kwargs["prepared_context_outcome"] = context_outcome
    if "prepared_mention_detection_command" not in kwargs:
        kwargs["prepared_mention_detection_command"] = (
            prepare_entity_mention_detection(
                tenant_id=kwargs["tenant_id"],
                observation_id=kwargs["observation_id"],
                phrase=kwargs["phrase"],
                content_text=(
                    content_text or f"Discussion about {kwargs['phrase']} today"
                ),
                source_channel=kwargs["source_channel"],
                context_command=context_command,
                context_outcome=context_outcome,
                now=kwargs["now"],
            )
        )
    return build_grounding_episode(**kwargs)


def _episode(
    *,
    candidate_id: str | None = None,
    canonical_ref: dict | None = CUSTOMER,
    confidence: float = 0.91,
    context_observations: tuple[ContextObservationInput, ...] = (),
):
    return _build_detected_episode(
        tenant_id=TENANT,
        observation_id=OBSERVATION,
        phrase="NBI",
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=(
            {"kind": "source_topology", "candidate_count": 2},
            {"kind": "same_source_space_temporal", "candidate_count": 5},
        ),
        context_observations=context_observations,
        selection_dependency_refs=("observation:root:v1",),
        candidates=(
            GroundingCandidateInput(
                canonical_ref=CUSTOMER,
                candidate_source="tenant_aliases",
                positive_evidence_refs=("entity-alias:Nimbus Bank",),
                independent_identity_evidence_refs=("manual-alias-adjudication:1",),
            ),
        ),
        model_candidate_id=candidate_id,
        model_canonical_ref=canonical_ref,
        model_confidence=confidence,
        model_reasoning="candidate fits the local phrase",
        high_confidence=0.8,
        review_min=0.5,
        now=NOW + timedelta(minutes=1),
    )


def test_known_closed_set_candidate_is_admitted_without_identity_mutation() -> None:
    episode = _episode(candidate_id=candidate_id_for_ref(CUSTOMER))

    assert episode.current_fate == "resolved_for_consumer"
    assert episode.assessed_canonical_ref == {**CUSTOMER, "version": 1}
    assert episode.admitted_canonical_ref == {**CUSTOMER, "version": 1}
    assert episode.admission.disposition.value == "single_referent"
    assert episode.admission.genuine_source_binding is None
    assert episode.assessment.decisive_evidence_refs == (
        "manual-alias-adjudication:1",
    )
    assert episode.model_output["closed_set_match"] is True


def test_candidate_request_binds_the_durable_detected_mention() -> None:
    episode = _episode()
    detection = episode.mention_detection_command.detection

    assert detection.mention is not None
    assert episode.candidate_set.request.mention_ref == (
        f"mention:{detection.mention.mention_id}:v{detection.detection_version}"
    )
    assert episode.candidate_set.request.mention_version == (
        detection.mention.mention_version
    )
    assert "phrase:" not in episode.candidate_set.request.mention_ref


def test_rejected_mention_cannot_enter_candidate_processing() -> None:
    episode = _episode()
    detected = episode.mention_detection_command
    rejected = prepare_entity_mention_detection(
        tenant_id=TENANT,
        observation_id=OBSERVATION,
        phrase="NBI",
        content_text="No entity phrase occurs here",
        source_channel="slack:message",
        context_command=episode.context_selection_command,
        context_outcome=episode.context_selection_outcome,
        now=NOW + timedelta(minutes=1),
    )

    with pytest.raises(ValueError, match="detected mention fate"):
        _build_detected_episode(
            tenant_id=TENANT,
            observation_id=OBSERVATION,
            phrase="NBI",
            occurred_at=NOW,
            source_channel="slack:message",
            source_space="C-finance",
            topology_incomplete=False,
            boundary_hypotheses=({"kind": "source_topology"},),
            context_observations=(),
            selection_dependency_refs=(),
            candidates=(),
            model_candidate_id=None,
            model_canonical_ref=None,
            model_confidence=0.5,
            model_reasoning="",
            high_confidence=0.8,
            review_min=0.5,
            prepared_context_command=episode.context_selection_command,
            prepared_context_outcome=episode.context_selection_outcome,
            prepared_mention_detection_command=rejected,
            now=NOW + timedelta(minutes=1),
        )

    assert detected.detection.fate.value == "detected"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source_observation_id", uuid4(), "source observation differs"),
        ("candidate_surface", "Nimbus", "phrase differs"),
        ("context_snapshot_digest", "0" * 64, "snapshot digest"),
    ],
)
def test_mention_detection_must_bind_the_exact_grounding_inputs(
    field: str,
    value: object,
    message: str,
) -> None:
    episode = _episode()
    bad_detection = episode.mention_detection_command.detection.model_copy(
        update={field: value}
    )
    bad_command = episode.mention_detection_command.model_copy(
        update={"detection": bad_detection}
    )

    with pytest.raises(ValueError, match=message):
        build_grounding_episode(
            tenant_id=TENANT,
            observation_id=OBSERVATION,
            phrase="NBI",
            occurred_at=NOW,
            source_channel="slack:message",
            source_space="C-finance",
            topology_incomplete=False,
            boundary_hypotheses=({"kind": "source_topology"},),
            context_observations=(),
            selection_dependency_refs=(),
            candidates=(),
            model_candidate_id=None,
            model_canonical_ref=None,
            model_confidence=0.5,
            model_reasoning="",
            high_confidence=0.8,
            review_min=0.5,
            prepared_context_command=episode.context_selection_command,
            prepared_context_outcome=episode.context_selection_outcome,
            prepared_mention_detection_command=bad_command,
            now=NOW + timedelta(minutes=1),
        )


def test_model_confidence_over_navigation_only_alias_cannot_auto_admit() -> None:
    episode = _build_detected_episode(
        tenant_id=TENANT,
        observation_id=OBSERVATION,
        phrase="NBI",
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        candidates=(
            GroundingCandidateInput(
                canonical_ref=CUSTOMER,
                candidate_source="tenant_aliases",
                positive_evidence_refs=("legacy-resolver-alias:1",),
                independent_identity_evidence_refs=(),
            ),
        ),
        model_candidate_id=candidate_id_for_ref(CUSTOMER),
        model_canonical_ref=CUSTOMER,
        model_confidence=0.99,
        model_reasoning="legacy resolver alias repeats the same answer",
        high_confidence=0.8,
        review_min=0.5,
        now=NOW + timedelta(minutes=1),
    )

    assert episode.current_fate == "review"
    assert episode.assessed_canonical_ref == {**CUSTOMER, "version": 1}
    assert episode.admitted_canonical_ref is None
    assert episode.assessment.identity_evidence_refs == ()
    assert episode.assessment.decisive_evidence_refs == ()
    assert episode.admission.selected_referent is None
    assert episode.admission.reason_codes == (
        "independent_identity_evidence_required",
    )


def test_model_invented_referent_is_audited_and_abstained() -> None:
    invented = {"type": "customer", "id": "customer:invented"}
    episode = _episode(
        candidate_id=candidate_id_for_ref(invented),
        canonical_ref=invented,
        confidence=0.99,
    )

    assert episode.current_fate == "abstained"
    assert episode.assessed_canonical_ref is None
    assert episode.admitted_canonical_ref is None
    assert episode.admission.disposition.value == "abstention"
    assert episode.admission.reason_codes == (
        "model_output_outside_authorized_candidate_set",
    )
    assert episode.model_output["canonical_ref"] == invented
    assert episode.model_output["closed_set_match"] is False


@pytest.mark.parametrize(
    ("canonical_ref", "confidence", "fate", "disposition"),
    [
        (CUSTOMER, 0.65, "review", "review"),
        (CUSTOMER, 0.2, "unresolved", "mention_local_only"),
        (None, 0.95, "unresolved", "mention_local_only"),
    ],
)
def test_review_and_unresolved_are_explicit_terminal_fates(
    canonical_ref: dict | None,
    confidence: float,
    fate: str,
    disposition: str,
) -> None:
    episode = _episode(canonical_ref=canonical_ref, confidence=confidence)
    assert episode.current_fate == fate
    assert episode.admission.disposition.value == disposition


def test_candidate_set_is_complete_open_world_and_distribution_is_closed() -> None:
    episode = _episode()
    candidate_set = episode.candidate_set
    kinds = {item.kind for item in candidate_set.candidates}

    assert kinds >= {
        EntityCandidateKind.NONE_OF_THE_ABOVE,
        EntityCandidateKind.NOVEL_REFERENT,
        EntityCandidateKind.UNKNOWN,
    }
    assert {item.lane_id for item in candidate_set.lane_fates} == {
        "source_mentions",
        "tenant_aliases",
    }
    assert set(episode.assessment.candidate_distribution) == {
        item.candidate_id for item in candidate_set.candidates
    }
    assert sum(episode.assessment.candidate_distribution.values()) == pytest.approx(1.0)


def test_future_context_is_excluded_from_as_known_snapshot() -> None:
    past_id = uuid4()
    future_id = uuid4()
    episode = _episode(
        context_observations=(
            ContextObservationInput(
                observation_id=past_id,
                occurred_at=NOW - timedelta(minutes=2),
                source_channel="slack:message",
                source_space="C-finance",
                inclusion_layer="source_topology",
                inclusion_reasons=("same thread",),
            ),
            ContextObservationInput(
                observation_id=future_id,
                occurred_at=NOW + timedelta(seconds=1),
                source_channel="slack:message",
                source_space="C-finance",
                inclusion_layer="temporal_candidate",
                inclusion_reasons=("future event must be excluded",),
            ),
        )
    )
    selected = {
        item.event_revision_id for item in episode.context_snapshot.selected_items
    }
    assert f"observation:{future_id}:v1" not in selected
    assert all(
        item.emitted_at <= NOW
        for item in episode.context_selection_command.candidates
        for item in item.selected_items
    )
    assert f"observation:{past_id}:v1" in {
        item.event_revision_id
        for candidate in episode.context_selection_command.candidates
        for item in candidate.selected_items
    }


def test_context_dependent_slack_phrase_cannot_auto_admit_without_stable_boundary() -> None:
    episode = _build_detected_episode(
        tenant_id=TENANT,
        observation_id=OBSERVATION,
        phrase="the billing thing",
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=(
            {"kind": "same_source_space_temporal", "candidate_count": 1},
        ),
        context_observations=(
            ContextObservationInput(
                observation_id=uuid4(),
                occurred_at=NOW - timedelta(minutes=2),
                source_channel="slack:message",
                source_space="C-finance",
                inclusion_layer="temporal_candidate",
                inclusion_reasons=("same channel before cutoff",),
            ),
        ),
        selection_dependency_refs=(),
        candidates=(
            GroundingCandidateInput(
                canonical_ref=CUSTOMER,
                candidate_source="tenant_aliases",
                positive_evidence_refs=("entity-alias:Nimbus Bank",),
                independent_identity_evidence_refs=("manual-alias-adjudication:1",),
            ),
        ),
        model_candidate_id=candidate_id_for_ref(CUSTOMER),
        model_canonical_ref=CUSTOMER,
        model_confidence=0.99,
        model_reasoning="nearby context suggests the customer",
        high_confidence=0.8,
        review_min=0.5,
        now=NOW + timedelta(minutes=1),
    )

    assert episode.context_selection_outcome.disposition.value == "needs_expansion"
    assert episode.current_fate == "review"
    assert episode.admission.selected_referent is None
    assert episode.admission.reason_codes == (
        "context_not_operationally_sufficient:needs_expansion",
    )


def test_candidate_ids_are_canonical_and_order_independent() -> None:
    same_different_order = {"id": "customer:nimbus", "type": "customer"}
    assert candidate_id_for_ref(CUSTOMER) == candidate_id_for_ref(same_different_order)


def test_naive_as_known_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _build_detected_episode(
            tenant_id=TENANT,
            observation_id=OBSERVATION,
            phrase="NBI",
            occurred_at=datetime(2026, 7, 16, 10, 0),
            source_channel="slack:message",
            source_space="C-finance",
            topology_incomplete=False,
            boundary_hypotheses=(),
            context_observations=(),
            selection_dependency_refs=(),
            candidates=(),
            model_candidate_id=None,
            model_canonical_ref=None,
            model_confidence=0.5,
            model_reasoning="",
            high_confidence=0.8,
            review_min=0.5,
            now=NOW,
        )


def test_every_contract_identifier_is_database_uuid_compatible() -> None:
    episode = _episode()
    values = (
        episode.context_snapshot.snapshot_id,
        episode.candidate_set.request.request_id,
        episode.candidate_set.candidate_set_id,
        episode.assessment.assessment_id,
        episode.admission.decision_id,
    )
    assert all(UUID(value) for value in values)
