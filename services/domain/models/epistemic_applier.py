"""Narrow asserted-report adapter into the canonical truth kernel.

This adapter is deliberately only a contract translator.  It does not decide
whether source language is an assertion: ``GroundedBeliefProcessor`` makes
that decision from the durable source-semantics objects before calling here.
The caller owns the transaction used by both truth admission and the legacy
read projection.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import GroundingAdmissionDecision
from lib.contracts.source_semantics import ProposedBeliefAssertion
from lib.contracts.truth_admission import (
    AdmissionDecision,
    AdmissionDisposition,
    AdmitModelCommand,
    CandidateReviewState,
    ModelTruthLifecycle,
    ModelVersion,
    TruthCandidate,
    TruthCandidateKind,
)
from lib.contracts.truth_evidence import (
    ClaimScopeBinding,
    ClaimScopeRole,
    EvidenceAuthority,
    ScopeSubjectKind,
    TruthEvidenceCoordinate,
    TruthEvidenceKind,
    TruthEvidenceReference,
    TruthEvidenceRole,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.domain.models.repo import ModelsRepo
from services.domain.truth_kernel import build_default_truth_kernel
from services.domain.truth_kernel.service import TruthKernelService


class EpistemicApplier:
    """Translate one prevalidated asserted/report proposal into truth admission."""

    writer_id = "EpistemicApplier"
    command_version = "asserted-report-truth-command-v1"

    def __init__(
        self,
        models_repo: ModelsRepo,
        *,
        truth_kernel: TruthKernelService | None = None,
    ) -> None:
        self._models_repo = models_repo
        self._truth_kernel = truth_kernel or build_default_truth_kernel()

    async def apply_asserted_report(
        self,
        conn: asyncpg.Connection,
        *,
        proposal: ProposedBeliefAssertion,
        source_observation_id: UUID,
        source_actor_id: UUID | None,
        occurred_at: datetime,
        selected_scope_entity: dict[str, Any],
        embedding: list[float],
        grounding_admission: GroundingAdmissionDecision,
        source_channel: str,
        source_content_text: str,
        admitted_at: datetime,
    ) -> ModelRow:
        """Admit through the kernel and return its legacy read projection.

        ``embedding`` remains in the compatibility API while embeddings are
        migrated to a rebuildable sidecar.  It cannot affect canonical
        admission and is therefore intentionally not placed in the command.
        """

        del embedding
        command = self._build_command(
            proposal=proposal,
            source_observation_id=source_observation_id,
            source_actor_id=source_actor_id,
            occurred_at=occurred_at,
            selected_scope_entity=selected_scope_entity,
            grounding_admission=grounding_admission,
            source_channel=source_channel,
            source_content_text=source_content_text,
            admitted_at=admitted_at,
        )
        receipt = await self._truth_kernel.admit(tx=conn, command=command)
        if receipt.model_id != proposal.proposed_model_id:
            raise InvariantViolation(
                "SOURCE_SEMANTIC_MODEL_ID_MISMATCH",
                "truth kernel admitted a different Model identity",
            )
        projected = await self._models_repo.get_by_id(receipt.model_id, conn=conn)
        if projected is None:
            raise InvariantViolation(
                "TRUTH_COMPATIBILITY_PROJECTION_MISSING",
                "truth admission did not materialize its legacy Model projection",
                model_id=str(receipt.model_id),
            )
        return projected

    def _build_command(
        self,
        *,
        proposal: ProposedBeliefAssertion,
        source_observation_id: UUID,
        source_actor_id: UUID | None,
        occurred_at: datetime,
        selected_scope_entity: dict[str, Any],
        grounding_admission: GroundingAdmissionDecision,
        source_channel: str,
        source_content_text: str,
        admitted_at: datetime,
    ) -> AdmitModelCommand:
        authority = grounding_admission.consumption_authority
        if authority.tenant_id != proposal.tenant_id:
            raise InvariantViolation(
                "TRUTH_ADMISSION_AUTHORITY_TENANT_MISMATCH",
                "grounding authority cannot admit truth for another tenant",
            )
        if not authority.is_live(admitted_at):
            raise InvariantViolation(
                "TRUTH_ADMISSION_AUTHORITY_EXPIRED",
                "grounding authority must be live at truth admission",
            )
        if authority.authority_epoch < 1:
            raise InvariantViolation(
                "TRUTH_ADMISSION_AUTHORITY_EPOCH_INVALID",
                "truth evidence requires an established authority epoch",
            )

        evidence = TruthEvidenceReference(
            reference_id=uuid5(proposal.proposal_id, "source-observation-evidence-v1"),
            tenant_id=proposal.tenant_id,
            kind=TruthEvidenceKind.OBSERVATION,
            evidence_id=str(source_observation_id),
            evidence_version=1,
            evidence_digest=canonical_sha256(source_content_text),
            role=TruthEvidenceRole.SUPPORT,
            coordinate=TruthEvidenceCoordinate(
                source_system=source_channel,
                source_object_id=str(source_observation_id),
                source_revision="1",
                field_path="content_text",
                span_start=0 if source_content_text else None,
                span_end=len(source_content_text) if source_content_text else None,
            ),
            authority=EvidenceAuthority(
                authority_ref=f"grounding-admission:{grounding_admission.decision_id}",
                policy_version=authority.policy_version,
                authority_epoch=authority.authority_epoch,
                decided_at=authority.decision_time,
                expires_at=authority.expires_at,
            ),
            occurred_at=occurred_at,
            recorded_at=admitted_at,
            cutoff_at=admitted_at,
        )
        scope = self._scope(
            proposal=proposal,
            selected_scope_entity=selected_scope_entity,
            source_actor_id=source_actor_id,
            evidence_reference_id=evidence.reference_id,
        )
        candidate = TruthCandidate(
            candidate_id=proposal.proposal_id,
            candidate_version=proposal.proposal_version,
            tenant_id=proposal.tenant_id,
            kind=TruthCandidateKind.ATOMIC_CLAIM,
            review_state=CandidateReviewState.PROPOSED,
            natural=proposal.natural,
            proposition=proposal.proposition,
            proposed_evidence=(evidence,),
            proposed_scope=scope,
            created_at=admitted_at,
        )
        decision_id = uuid7()
        version_id = uuid7()
        decision = AdmissionDecision(
            decision_id=decision_id,
            tenant_id=proposal.tenant_id,
            candidate_id=candidate.candidate_id,
            candidate_version=candidate.candidate_version,
            candidate_digest=candidate.candidate_digest,
            disposition=AdmissionDisposition.ACCEPTED,
            reason_codes=("asserted_report_with_single_referent_grounding",),
            decided_by=self.writer_id,
            decided_at=admitted_at,
            admitted_model_id=proposal.proposed_model_id,
            admitted_version_id=version_id,
        )
        semantic_digest = ModelVersion.compute_semantic_digest(
            proposition=candidate.proposition,
            natural=candidate.natural,
            evidence=candidate.proposed_evidence,
            scope=candidate.proposed_scope,
        )
        version = ModelVersion(
            version_id=version_id,
            model_id=proposal.proposed_model_id,
            version=1,
            tenant_id=proposal.tenant_id,
            admission_decision_id=decision_id,
            source_candidate_id=candidate.candidate_id,
            source_candidate_version=candidate.candidate_version,
            natural=candidate.natural,
            proposition=candidate.proposition,
            evidence=candidate.proposed_evidence,
            scope=candidate.proposed_scope,
            lifecycle=ModelTruthLifecycle.ACTIVE,
            created_at=admitted_at,
            semantic_digest=semantic_digest,
        )
        return AdmitModelCommand(
            command_id=uuid7(),
            idempotency_key=(
                f"{self.command_version}:{proposal.tenant_id}:"
                f"{proposal.proposal_id}:{proposal.proposal_version}"
            ),
            tenant_id=proposal.tenant_id,
            candidate=candidate,
            decision=decision,
            version=version,
            issued_at=admitted_at,
        )

    @staticmethod
    def _scope(
        *,
        proposal: ProposedBeliefAssertion,
        selected_scope_entity: dict[str, Any],
        source_actor_id: UUID | None,
        evidence_reference_id: UUID,
    ) -> tuple[ClaimScopeBinding, ...]:
        if proposal.grounding_continuity.selected_referent is None:
            raise InvariantViolation(
                "TRUTH_ADMISSION_SCOPE_REFERENT_MISSING",
                "asserted/report truth admission requires one selected referent",
            )
        raw_id = str(
            selected_scope_entity.get("id")
            or selected_scope_entity.get("referent_id")
            or proposal.grounding_continuity.selected_referent.referent_id
        )
        try:
            subject_id = UUID(raw_id)
        except ValueError:
            subject_id = uuid5(
                NAMESPACE_URL,
                f"fyralis:{proposal.tenant_id}:canonical-referent:{raw_id}",
            )
        raw_kind = str(selected_scope_entity.get("type") or "other").lower()
        kind_aliases = {"org": "organization", "company": "organization"}
        try:
            subject_kind = ScopeSubjectKind(kind_aliases.get(raw_kind, raw_kind))
        except ValueError:
            subject_kind = ScopeSubjectKind.OTHER
        bindings = [
            ClaimScopeBinding(
                subject_id=subject_id,
                subject_kind=subject_kind,
                role=ClaimScopeRole.SUBJECT,
                claim_local_evidence_refs=(evidence_reference_id,),
            )
        ]
        if source_actor_id is not None and source_actor_id != subject_id:
            bindings.append(
                ClaimScopeBinding(
                    subject_id=source_actor_id,
                    subject_kind=ScopeSubjectKind.PERSON,
                    role=ClaimScopeRole.ACTOR,
                    claim_local_evidence_refs=(evidence_reference_id,),
                )
            )
        return tuple(sorted(bindings, key=lambda item: (str(item.subject_id), item.role.value)))


__all__ = ["EpistemicApplier"]
