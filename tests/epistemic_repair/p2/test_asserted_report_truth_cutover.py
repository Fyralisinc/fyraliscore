from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from lib.contracts.perception import (
    GroundingAdmissionDisposition,
    GroundingContinuity,
    Modality,
    ReferentVersionRef,
    SourceAssertionKind,
    SpeechActKind,
)
from lib.contracts.source_semantics import ProposedBeliefAssertion
from lib.contracts.truth_admission import AdmissionDisposition, TruthCandidateKind
from lib.contracts.truth_evidence import ClaimScopeRole, TruthEvidenceKind
from lib.shared.ids import uuid7
from services.domain.models.epistemic_applier import EpistemicApplier
from services.domain.source_semantics.processor import GroundedBeliefProcessor


NOW = datetime(2026, 7, 17, tzinfo=timezone.utc)


def _proposal(*, tenant_id, model_id) -> ProposedBeliefAssertion:
    interpretation_id = uuid7()
    return ProposedBeliefAssertion(
        proposal_id=uuid7(),
        proposed_model_id=model_id,
        tenant_id=tenant_id,
        interpretation_id=interpretation_id,
        source_assertion_id="assertion-1",
        semantic_frame_id="frame-1",
        speech_act_id="speech-act-1",
        grounding_continuity=GroundingContinuity(
            downstream_object_ref=f"model:{model_id}",
            mention_ref="mention-1",
            mention_version=1,
            resolution_assessment_ref="resolution-assessment:1",
            resolution_assessment_version=1,
            grounding_admission_ref="grounding-admission:1",
            grounding_admission_version=1,
            selected_referent=ReferentVersionRef(
                referent_id="customer:nimbus",
                referent_version=1,
            ),
        ),
        natural="NBI is blocked",
        proposition={
            "kind": "belief",
            "assertion": "NBI is blocked",
            "source_semantic_interpretation_id": str(interpretation_id),
        },
        confidence=0.61,
    )


def test_asserted_report_builds_one_exact_truth_admission_command() -> None:
    tenant_id, model_id, observation_id = uuid7(), uuid7(), uuid7()
    authority = SimpleNamespace(
        tenant_id=tenant_id,
        policy_version="grounding-policy-v4",
        authority_epoch=7,
        decision_time=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
        is_live=lambda at: NOW - timedelta(minutes=1) <= at < NOW + timedelta(hours=1),
    )
    grounding_admission = SimpleNamespace(
        decision_id="grounding-decision-1",
        consumption_authority=authority,
    )
    proposal = _proposal(tenant_id=tenant_id, model_id=model_id)

    command = EpistemicApplier(  # type: ignore[arg-type]
        object(), truth_kernel=object()  # type: ignore[arg-type]
    )._build_command(
        proposal=proposal,
        source_observation_id=observation_id,
        source_actor_id=None,
        occurred_at=NOW - timedelta(minutes=2),
        selected_scope_entity={"type": "customer", "id": "customer:nimbus"},
        grounding_admission=grounding_admission,  # type: ignore[arg-type]
        source_channel="slack:message",
        source_content_text="NBI is blocked",
        admitted_at=NOW,
    )

    assert command.candidate.kind is TruthCandidateKind.ATOMIC_CLAIM
    assert command.decision.disposition is AdmissionDisposition.ACCEPTED
    assert command.version.model_id == model_id
    assert command.version.proposition == proposal.proposition
    assert command.version.natural == proposal.natural
    assert command.version.evidence == command.candidate.proposed_evidence
    assert command.version.scope == command.candidate.proposed_scope
    assert command.version.evidence[0].kind is TruthEvidenceKind.OBSERVATION
    assert command.version.evidence[0].evidence_id == str(observation_id)
    assert command.version.evidence[0].coordinate.span_start == 0
    assert command.version.evidence[0].coordinate.span_end == len("NBI is blocked")
    assert command.version.evidence[0].authority.authority_epoch == 7
    assert {binding.role for binding in command.version.scope} == {
        ClaimScopeRole.SUBJECT
    }


def test_source_semantics_cannot_invert_question_or_nonassertion_into_truth() -> None:
    grounding = SimpleNamespace(
        current_fate="resolved_for_consumer",
        grounding_admission=SimpleNamespace(
            disposition=GroundingAdmissionDisposition.SINGLE_REFERENT,
            selected_referent=object(),
        ),
        selected_scope_entity={"type": "customer", "id": "customer:nimbus"},
    )
    nonasserted = SimpleNamespace(
        source_assertion=SimpleNamespace(kind=SourceAssertionKind.ASKED),
    )
    assert GroundedBeliefProcessor._route_reasons(
        bundle=nonasserted, grounding=grounding
    ) == ("source_assertion_not_asserted",)

    question = SimpleNamespace(
        source_assertion=SimpleNamespace(kind=SourceAssertionKind.ASSERTED),
        speech_act=SimpleNamespace(
            distribution={SpeechActKind.QUESTION: 0.9, SpeechActKind.REPORT: 0.1}
        ),
        semantic_frame=SimpleNamespace(modality=Modality.ACTUAL),
    )
    assert GroundedBeliefProcessor._route_reasons(
        bundle=question, grounding=grounding
    ) == ("speech_act_not_unambiguously_report",)
