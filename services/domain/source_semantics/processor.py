"""Directly callable grounded source-semantics to belief vertical."""

from __future__ import annotations

from datetime import datetime, timezone

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import (
    GroundingAdmissionDisposition,
    Modality,
    SourceAssertionKind,
    SpeechActKind,
)
from lib.contracts.source_semantics import (
    GroundedBeliefApplyResult,
    GroundedSourceSemanticBundle,
    ProposedBeliefAssertion,
    SourceSemanticAdmissionDisposition,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.correction_propagation import CorrectionPropagationService
from services.domain.models.epistemic_applier import EpistemicApplier
from services.domain.models.repo import ModelsRepo
from services.domain.source_semantics.extractor import (
    DeterministicSourceSemanticExtractor,
    primary_mention_is_supported_subject,
)
from services.domain.source_semantics.repo import (
    GroundingTraceContext,
    SourceSemanticRepo,
)


class GroundedBeliefProcessor:
    """Persist source semantics and admit only an asserted/report belief."""

    def __init__(
        self,
        *,
        source_semantic_repo: SourceSemanticRepo | None = None,
        epistemic_applier: EpistemicApplier | None = None,
        models_repo: ModelsRepo | None = None,
        extractor: DeterministicSourceSemanticExtractor | None = None,
        correction_propagation_service: CorrectionPropagationService | None = None,
    ) -> None:
        self._repo = source_semantic_repo or SourceSemanticRepo()
        self._extractor = extractor or DeterministicSourceSemanticExtractor()
        model_repo = models_repo or ModelsRepo(
            pool=None,  # type: ignore[arg-type]
            embedder=None,
            run_topology_on_insert=False,
        )
        if epistemic_applier is not None:
            self._epistemic = epistemic_applier
        else:
            self._epistemic = EpistemicApplier(model_repo)
        self._correction_propagation = (
            correction_propagation_service
            or CorrectionPropagationService(models_repo=model_repo)
        )

    async def process_trace(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id,
        grounding_trace_id,
        embedding: list[float],
        now: datetime | None = None,
    ) -> GroundedBeliefApplyResult:
        """Extract and process one completed grounding trace directly."""

        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")

        async with conn.transaction():
            # Reload and re-extract inside the transaction. Admission therefore
            # has no caller-supplied semantic-bundle path around source truth.
            grounding = await self._repo.load_grounding_trace(
                conn,
                tenant_id=tenant_id,
                grounding_trace_id=grounding_trace_id,
            )
            bundle = self._extractor.extract(grounding)
            duplicate = await self._repo.find_result(
                conn,
                tenant_id=bundle.tenant_id,
                grounding_trace_id=bundle.grounding_trace_id,
                bundle_digest=bundle.bundle_digest,
            )
            if duplicate is not None:
                await self._propagate_correction(
                    conn,
                    grounding=grounding,
                    result=duplicate,
                )
                return duplicate

            self._validate_exact_source_semantics(bundle=bundle, grounding=grounding)
            interpretation_id = uuid7()
            route_reasons = self._route_reasons(bundle=bundle, grounding=grounding)
            admitted = not route_reasons
            model_id = uuid7() if admitted else None
            continuity_admission_id = grounding.grounding_admission_id
            continuity_admission = grounding.grounding_admission
            if admitted:
                (
                    continuity_admission_id,
                    continuity_admission,
                ) = await self._repo.ensure_epistemic_admission(
                    conn,
                    grounding=grounding,
                    now=now,
                )
            continuity = grounding.continuity(
                downstream_object_ref=(
                    f"model:{model_id}"
                    if model_id is not None
                    else f"source-semantic-interpretation:{interpretation_id}"
                ),
                admission_id=continuity_admission_id,
                admission=continuity_admission,
            )
            await self._repo.append_interpretation(
                conn,
                interpretation_id=interpretation_id,
                grounding=grounding,
                bundle=bundle,
                continuity=continuity,
                grounding_admission_id=continuity_admission_id,
                source_content_hash=canonical_sha256(grounding.content_text),
                recorded_at=now,
            )

            proposal: ProposedBeliefAssertion | None = None
            if admitted:
                assert model_id is not None
                assert grounding.selected_scope_entity is not None
                confidence = max(
                    0.05,
                    min(
                        0.69,
                        (1.0 - bundle.source_assertion.uncertainty)
                        * bundle.semantic_frame.confidence
                        * bundle.speech_act.distribution[SpeechActKind.REPORT],
                    ),
                )
                proposition = {
                    "kind": "belief",
                    "claim_role": "fact",
                    "abstraction_level": "atomic",
                    "time_mode": "current",
                    "modality": "observed",
                    "polarity": (
                        "negative" if bundle.semantic_frame.negated else "neutral"
                    ),
                    "assertion": bundle.source_assertion.expressed_content,
                    "source_assertion_ref": bundle.source_assertion.assertion_id,
                    "semantic_frame_ref": bundle.semantic_frame.frame_id,
                    "speech_act_ref": bundle.speech_act.speech_act_id,
                    "grounding_continuity": continuity.model_dump(mode="json"),
                    "source_semantic_interpretation_id": str(interpretation_id),
                    "source_author_ref": grounding.source_author_ref,
                }
                proposal = ProposedBeliefAssertion(
                    proposal_id=uuid7(),
                    proposed_model_id=model_id,
                    tenant_id=bundle.tenant_id,
                    interpretation_id=interpretation_id,
                    source_assertion_id=bundle.source_assertion.assertion_id,
                    semantic_frame_id=bundle.semantic_frame.frame_id,
                    speech_act_id=bundle.speech_act.speech_act_id,
                    grounding_continuity=continuity,
                    natural=bundle.source_assertion.expressed_content,
                    proposition=proposition,
                    confidence=confidence,
                )
                applied_model = await self._epistemic.apply_asserted_report(
                    conn,
                    proposal=proposal,
                    source_observation_id=grounding.source_observation_id,
                    source_actor_id=grounding.source_actor_id,
                    occurred_at=grounding.occurred_at,
                    selected_scope_entity=grounding.selected_scope_entity,
                    embedding=embedding,
                    grounding_admission=continuity_admission,
                    source_channel=grounding.source_channel,
                    source_content_text=grounding.content_text,
                    admitted_at=now,
                )
                if applied_model.id != model_id:
                    raise InvariantViolation(
                        "SOURCE_SEMANTIC_MODEL_ID_MISMATCH",
                        "EpistemicApplier returned a different Model identity",
                    )
                disposition = SourceSemanticAdmissionDisposition.BELIEF_APPLIED
                reason_codes = ("asserted_report_with_single_referent_grounding",)
            else:
                disposition = SourceSemanticAdmissionDisposition.NO_ADMISSION
                reason_codes = route_reasons

            decision_id = uuid7()
            decision_digest = canonical_sha256(
                {
                    "interpretation_id": str(interpretation_id),
                    "disposition": disposition.value,
                    "reason_codes": reason_codes,
                    "proposal_digest": proposal.proposal_digest if proposal else None,
                    "model_id": str(model_id) if model_id else None,
                }
            )
            await self._repo.append_admission(
                conn,
                decision_id=decision_id,
                tenant_id=bundle.tenant_id,
                interpretation_id=interpretation_id,
                disposition=disposition,
                reason_codes=reason_codes,
                proposal=proposal,
                admitted_model_id=model_id,
                decision_digest=decision_digest,
                decided_at=now,
            )
            result = GroundedBeliefApplyResult(
                interpretation_id=interpretation_id,
                admission_decision_id=decision_id,
                disposition=disposition,
                reason_codes=reason_codes,
                model_id=model_id,
            )
            await self._propagate_correction(
                conn,
                grounding=grounding,
                result=result,
            )
            return result

    async def _propagate_correction(
        self,
        conn: asyncpg.Connection,
        *,
        grounding: GroundingTraceContext,
        result: GroundedBeliefApplyResult,
    ) -> None:
        await self._correction_propagation.propagate_direct_correction(
            conn,
            tenant_id=grounding.tenant_id,
            predecessor_grounding_trace_id=(
                grounding.supersedes_grounding_trace_id
            ),
            successor_grounding_trace_id=grounding.trace_id,
            cause_event_id=grounding.source_observation_id,
            corrected_model_id=result.model_id,
        )

    @staticmethod
    def _route_reasons(
        *,
        bundle: GroundedSourceSemanticBundle,
        grounding: GroundingTraceContext,
    ) -> tuple[str, ...]:
        if (
            grounding.current_fate != "resolved_for_consumer"
            or grounding.grounding_admission.disposition
            is not GroundingAdmissionDisposition.SINGLE_REFERENT
            or grounding.grounding_admission.selected_referent is None
            or grounding.selected_scope_entity is None
        ):
            return ("grounding_not_admitted_for_single_referent_use",)
        if bundle.source_assertion.kind is not SourceAssertionKind.ASSERTED:
            return ("source_assertion_not_asserted",)
        top_probability = max(bundle.speech_act.distribution.values())
        report_probability = bundle.speech_act.distribution.get(
            SpeechActKind.REPORT,
            0.0,
        )
        if report_probability != top_probability or sum(
            probability == top_probability
            for probability in bundle.speech_act.distribution.values()
        ) != 1:
            return ("speech_act_not_unambiguously_report",)
        if bundle.semantic_frame.modality is not Modality.ACTUAL:
            return ("semantic_frame_not_actual",)
        if not primary_mention_is_supported_subject(grounding):
            return ("primary_mention_not_supported_report_subject",)
        mention_anchor_ids = {
            grounding.mention.primary_anchor.anchor_id,
            *(
                anchor.anchor_id
                for anchor in grounding.mention.alternate_anchors
            ),
        }
        grounded_anchor_refs = {
            ref
            for argument in bundle.semantic_frame.arguments
            for ref in argument.mention_anchor_refs
        }
        if not mention_anchor_ids & grounded_anchor_refs:
            return ("semantic_frame_missing_grounded_mention_anchor",)
        return ()

    @staticmethod
    def _validate_exact_source_semantics(
        *,
        bundle: GroundedSourceSemanticBundle,
        grounding: GroundingTraceContext,
    ) -> None:
        assertion = bundle.source_assertion
        if assertion.context_snapshot_id != str(grounding.context_snapshot_id):
            raise InvariantViolation(
                "SOURCE_SEMANTIC_CONTEXT_MISMATCH",
                "source assertion does not bind the grounding context snapshot",
            )
        expected_record = f"observation:{grounding.source_observation_id}"
        expected_revision = f"observation:{grounding.source_observation_id}:v1"
        exact = False
        for coordinate in assertion.coordinates:
            if (
                coordinate.evidence_record_id != expected_record
                or coordinate.source_revision != expected_revision
                or coordinate.field_path != "content_text"
                or coordinate.span_start is None
                or coordinate.span_end is None
            ):
                continue
            if (
                grounding.content_text[
                    coordinate.span_start : coordinate.span_end
                ]
                == assertion.expressed_content
            ):
                exact = True
                break
        if not exact:
            raise InvariantViolation(
                "SOURCE_SEMANTIC_COORDINATE_MISMATCH",
                "source assertion does not reconstruct from exact source coordinates",
            )


__all__ = ["GroundedBeliefProcessor"]
