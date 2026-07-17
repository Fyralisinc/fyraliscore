"""Governed admission of validated Think claims into canonical Model truth."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.contracts.truth_admission import (
    AdmissionDecision, AdmissionDisposition, AdmitModelCommand,
    CandidateReviewState, ModelTruthLifecycle, ModelVersion, TruthCandidate,
    TruthCandidateKind,
)
from lib.contracts.truth_evidence import (
    ClaimScopeBinding, ClaimScopeRole, EvidenceAuthority, ScopeSubjectKind,
    TruthEvidenceCoordinate, TruthEvidenceKind, TruthEvidenceReference,
    TruthEvidenceRole,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate, ModelRow
from services.domain.models.repo import ModelsRepo
from services.domain.truth_kernel import build_default_truth_kernel


_COMMAND_VERSION = "think-validated-claim-admission-v1"


def _subject_id(tenant_id: UUID, value: Any) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return uuid5(NAMESPACE_URL, f"fyralis:{tenant_id}:think-scope:{value}")


def _subject_kind(value: Any) -> ScopeSubjectKind:
    aliases = {
        "org": "organization", "company": "organization",
        "commitment": "work_item", "decision": "work_item",
        "goal": "work_item", "resource": "work_item",
        "workstream": "project", "workflow": "project",
    }
    normalized = aliases.get(str(value or "other").casefold(),
                             str(value or "other").casefold())
    try:
        return ScopeSubjectKind(normalized)
    except ValueError:
        return ScopeSubjectKind.OTHER


async def build_think_admission_command(
    conn: asyncpg.Connection, *, proposed: ModelCreate, model_id: UUID,
    evidence_observation_ids: tuple[UUID, ...], admitted_at: datetime,
) -> AdmitModelCommand:
    """Compile one already-validated claim into an immutable truth command."""

    if not evidence_observation_ids:
        raise InvariantViolation(
            "THINK_TRUTH_EVIDENCE_MISSING",
            "validated Think claims require claim-local observation evidence",
        )
    rows = await conn.fetch("""
        SELECT id,occurred_at,source_channel,content_text,trust_tier
        FROM observations
        WHERE tenant_id=$1 AND id=ANY($2::uuid[])
        ORDER BY occurred_at,id
    """, proposed.tenant_id, list(evidence_observation_ids))
    found = {row["id"] for row in rows}
    missing = set(evidence_observation_ids) - found
    if missing:
        missing_ids = sorted(map(str, missing))[:8]
        found_ids = sorted(map(str, found))[:8]
        raise InvariantViolation(
            "THINK_TRUTH_EVIDENCE_NOT_FOUND",
            "every claimed evidence observation must exist in the same tenant; "
            f"missing_ids={missing_ids}; found_ids={found_ids}",
            missing=missing_ids,
            found=found_ids,
            claimed_count=len(evidence_observation_ids),
        )
    evidence: list[TruthEvidenceReference] = []
    for row in rows:
        text = str(row["content_text"] or "")
        reference_id = uuid5(
            model_id, f"think-observation-evidence-v1:{row['id']}"
        )
        evidence.append(TruthEvidenceReference(
            reference_id=reference_id, tenant_id=proposed.tenant_id,
            kind=TruthEvidenceKind.OBSERVATION, evidence_id=str(row["id"]),
            evidence_version=1, evidence_digest=canonical_sha256(text),
            role=TruthEvidenceRole.SUPPORT,
            coordinate=TruthEvidenceCoordinate(
                source_system=str(row["source_channel"] or "normalized-signal"),
                source_object_id=str(row["id"]), source_revision="1",
                field_path="content_text", span_start=0 if text else None,
                span_end=len(text) if text else None,
            ),
            authority=EvidenceAuthority(
                authority_ref=f"think-validated-observation:{row['id']}",
                policy_version=_COMMAND_VERSION, authority_epoch=1,
                decided_at=row["occurred_at"],
            ),
            occurred_at=row["occurred_at"], recorded_at=admitted_at,
            cutoff_at=admitted_at,
        ))
    evidence_tuple = tuple(evidence)
    evidence_refs = tuple(sorted((item.reference_id for item in evidence), key=str))
    scopes: dict[tuple[UUID, ScopeSubjectKind, ClaimScopeRole], ClaimScopeBinding] = {}
    for actor in proposed.scope_actors:
        key = (actor, ScopeSubjectKind.PERSON, ClaimScopeRole.ACTOR)
        scopes[key] = ClaimScopeBinding(
            subject_id=actor, subject_kind=ScopeSubjectKind.PERSON,
            role=ClaimScopeRole.ACTOR, claim_local_evidence_refs=evidence_refs,
        )
    for entity in proposed.scope_entities:
        if not isinstance(entity, dict) or not (entity.get("id") or entity.get("referent_id")):
            continue
        subject_id = _subject_id(
            proposed.tenant_id, entity.get("id") or entity.get("referent_id")
        )
        kind = _subject_kind(entity.get("type"))
        key = (subject_id, kind, ClaimScopeRole.SUBJECT)
        scopes[key] = ClaimScopeBinding(
            subject_id=subject_id, subject_kind=kind,
            role=ClaimScopeRole.SUBJECT, claim_local_evidence_refs=evidence_refs,
        )
    scope = tuple(sorted(scopes.values(), key=lambda item: (
        str(item.subject_id), item.subject_kind.value, item.role.value,
    )))
    proposition = dict(proposed.proposition)
    raw_role = str(proposition.get("claim_role") or proposition.get("kind") or "")
    kind = (TruthCandidateKind.SYNTHESIS if raw_role in {
        "situation", "hypothesis", "synthesis", "pattern",
    } else TruthCandidateKind.ATOMIC_CLAIM)
    candidate_id = uuid5(model_id, _COMMAND_VERSION)
    candidate = TruthCandidate(
        candidate_id=candidate_id, candidate_version=1,
        tenant_id=proposed.tenant_id, kind=kind,
        review_state=CandidateReviewState.PROPOSED, natural=proposed.natural,
        proposition=proposition, proposed_evidence=evidence_tuple,
        proposed_scope=scope, created_at=admitted_at,
    )
    decision_id = uuid5(candidate_id, "admission-decision-v1")
    version_id = uuid5(candidate_id, "model-version-v1")
    decision = AdmissionDecision(
        decision_id=decision_id, tenant_id=proposed.tenant_id,
        candidate_id=candidate_id, candidate_version=1,
        candidate_digest=candidate.candidate_digest,
        disposition=AdmissionDisposition.ACCEPTED,
        reason_codes=("validated_think_claim_with_claim_local_evidence",),
        decided_by="ThinkValidatedClaimApplier", decided_at=admitted_at,
        admitted_model_id=model_id, admitted_version_id=version_id,
    )
    version = ModelVersion(
        version_id=version_id, model_id=model_id, version=1,
        tenant_id=proposed.tenant_id, admission_decision_id=decision_id,
        source_candidate_id=candidate_id, source_candidate_version=1,
        natural=proposed.natural, proposition=proposition,
        evidence=evidence_tuple, scope=scope,
        lifecycle=ModelTruthLifecycle.ACTIVE, created_at=admitted_at,
        semantic_digest=ModelVersion.compute_semantic_digest(
            proposition=proposition, natural=proposed.natural,
            evidence=evidence_tuple, scope=scope,
        ),
    )
    return AdmitModelCommand(
        command_id=uuid5(candidate_id, "admit-command-v1"),
        idempotency_key=f"{_COMMAND_VERSION}:{proposed.tenant_id}:{candidate_id}:1",
        tenant_id=proposed.tenant_id, candidate=candidate,
        decision=decision, version=version, issued_at=admitted_at,
    )


async def admit_validated_think_claim(
    conn: asyncpg.Connection, *, proposed: ModelCreate,
    evidence_observation_ids: tuple[UUID, ...], models_repo: ModelsRepo,
) -> ModelRow:
    model_id = proposed.id or uuid7()
    command = await build_think_admission_command(
        conn, proposed=proposed, model_id=model_id,
        evidence_observation_ids=evidence_observation_ids,
        admitted_at=datetime.now(timezone.utc),
    )
    receipt = await build_default_truth_kernel().admit(tx=conn, command=command)
    projected = await models_repo.get_by_id(receipt.model_id, conn=conn)
    if projected is None:
        raise InvariantViolation(
            "THINK_TRUTH_PROJECTION_MISSING",
            "truth admission did not materialize its compatibility projection",
            model_id=str(receipt.model_id),
        )
    return projected


__all__ = ["admit_validated_think_claim", "build_think_admission_command"]
