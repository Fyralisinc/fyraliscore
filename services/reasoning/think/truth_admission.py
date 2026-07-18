"""Governed admission of validated Think claims into canonical Model truth."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.contracts.truth_admission import (
    AdmissionDecision, AdmissionDisposition, AdmitModelCommand,
    AdvanceModelHeadCommand, ModelHeadExpectation, ModelTruthTransition,
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

from .evidence_manifest import authorize_compiler_evidence_manifest


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


def _scope_canonical_ref(entity: dict[str, Any]) -> str | None:
    explicit = entity.get("canonical_ref")
    if isinstance(explicit, dict):
        ref_type = str(explicit.get("type") or "").strip()
        ref_id = str(explicit.get("id") or "").strip()
        if ref_type and ref_id:
            return f"{ref_type}:{ref_id}"
    if explicit:
        return str(explicit).strip() or None
    raw_id = str(entity.get("id") or entity.get("referent_id") or "").strip()
    return raw_id if ":" in raw_id else None


def _scope_display_label(
    entity: dict[str, Any],
    proposition: dict[str, Any],
) -> str | None:
    explicit = entity.get("display_label") or entity.get("label")
    if explicit:
        return str(explicit).strip() or None
    canonical_ref = _scope_canonical_ref(entity)
    if canonical_ref and proposition.get("scope_ref") == canonical_ref:
        label = proposition.get("scope_label")
        return str(label).strip() if label else None
    return None


async def build_think_admission_command(
    conn: asyncpg.Connection, *, proposed: ModelCreate, model_id: UUID,
    evidence_observation_ids: tuple[UUID, ...], admitted_at: datetime,
) -> AdmitModelCommand:
    """Compile one already-validated claim into an immutable truth command."""

    proposition = dict(proposed.proposition)
    compiler_manifest = proposition.pop("evidence_observation_manifest", None)
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
    if compiler_manifest is not None:
        authorize_compiler_evidence_manifest(
            selected_observation_ids=evidence_observation_ids,
            manifest=compiler_manifest,
            persisted_observations=rows,
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
            canonical_ref=_scope_canonical_ref(entity),
            display_label=_scope_display_label(entity, proposition),
            canonical_ref_status=(
                "provisional" if _scope_canonical_ref(entity) else None
            ),
            normalization_version=(1 if _scope_canonical_ref(entity) else None),
        )
    scope = tuple(sorted(scopes.values(), key=lambda item: (
        str(item.subject_id), item.subject_kind.value, item.role.value,
    )))
    raw_role = str(proposition.get("claim_role") or proposition.get("kind") or "")
    kind = (TruthCandidateKind.SYNTHESIS if raw_role in {
        "situation", "hypothesis", "synthesis", "pattern",
    } else TruthCandidateKind.ATOMIC_CLAIM)
    candidate_id = uuid5(model_id, _COMMAND_VERSION)
    candidate = TruthCandidate(
        candidate_id=candidate_id, candidate_version=1,
        tenant_id=proposed.tenant_id, kind=kind,
        review_state=CandidateReviewState.PROPOSED, natural=proposed.natural,
        proposition=proposition, confidence=proposed.confidence,
        falsifier=proposed.falsifier,
        evidential_weight=proposed.evidential_weight,
        supporting_model_ids=tuple(proposed.supporting_model_ids),
        visible_to_subjects=proposed.visible_to_subjects,
        resolution_outcome=None, resolved_at=None,
        temporal_scope=proposed.scope_temporal,
        proposed_evidence=evidence_tuple,
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
        confidence=proposed.confidence,
        semantic_digest_version=2,
        falsifier=proposed.falsifier,
        evidential_weight=proposed.evidential_weight,
        supporting_model_ids=tuple(proposed.supporting_model_ids),
        visible_to_subjects=proposed.visible_to_subjects,
        temporal_scope=proposed.scope_temporal,
        evidence=evidence_tuple, scope=scope,
        lifecycle=ModelTruthLifecycle.ACTIVE, created_at=admitted_at,
        semantic_digest=ModelVersion.compute_semantic_digest(
            proposition=proposition, natural=proposed.natural,
            evidence=evidence_tuple, scope=scope,
            confidence=proposed.confidence,
            falsifier=proposed.falsifier,
            evidential_weight=proposed.evidential_weight,
            supporting_model_ids=tuple(proposed.supporting_model_ids),
            visible_to_subjects=proposed.visible_to_subjects,
            temporal_scope=proposed.scope_temporal,
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


async def _current_truth_version(
    conn: asyncpg.Connection, *, tenant_id: UUID, model_id: UUID,
) -> ModelVersion:
    row = await conn.fetchrow("""
        SELECT v.* FROM model_truth_heads h
        JOIN model_truth_versions v ON v.tenant_id=h.tenant_id AND v.version_id=h.version_id
        WHERE h.tenant_id=$1 AND h.model_id=$2
    """, tenant_id, model_id)
    if row is None:
        raise InvariantViolation("THINK_TRUTH_HEAD_MISSING", "accepted Model has no truth head")
    evidence_rows = await conn.fetch("""
        SELECT * FROM model_truth_evidence_references
        WHERE tenant_id=$1 AND model_version_id=$2 ORDER BY reference_id
    """, tenant_id, row["version_id"])
    evidence = tuple(TruthEvidenceReference(
        reference_id=x["reference_id"], tenant_id=x["tenant_id"],
        kind=TruthEvidenceKind(x["evidence_kind"]), evidence_id=x["evidence_id"],
        evidence_version=x["evidence_version"], evidence_digest=x["evidence_digest"],
        role=TruthEvidenceRole(x["evidence_role"]),
        coordinate=TruthEvidenceCoordinate(
            source_system=x["source_system"], source_object_id=x["source_object_id"],
            source_revision=x["source_revision"], field_path=x["field_path"],
            span_start=x["span_start"], span_end=x["span_end"],
            time_range_start=x["time_range_start"], time_range_end=x["time_range_end"],
        ), authority=EvidenceAuthority(
            authority_ref=x["authority_ref"], policy_version=x["policy_version"],
            authority_epoch=x["authority_epoch"], decided_at=x["authority_decided_at"],
            expires_at=x["authority_expires_at"],
        ), occurred_at=x["occurred_at"], recorded_at=x["recorded_at"], cutoff_at=x["cutoff_at"],
    ) for x in evidence_rows)
    binding_rows = await conn.fetch("""
        SELECT binding_id,subject_id,subject_kind,scope_role,
               canonical_ref,display_label,canonical_ref_status,
               normalization_version
        FROM model_truth_scope_bindings WHERE tenant_id=$1 AND model_version_id=$2
        ORDER BY subject_id,scope_role
    """, tenant_id, row["version_id"])
    scope: list[ClaimScopeBinding] = []
    for binding in binding_rows:
        refs = await conn.fetch("""
            SELECT evidence_reference_id FROM model_truth_scope_evidence
            WHERE tenant_id=$1 AND model_version_id=$2 AND binding_id=$3
            ORDER BY evidence_reference_id
        """, tenant_id, row["version_id"], binding["binding_id"])
        scope.append(ClaimScopeBinding(
            subject_id=binding["subject_id"],
            subject_kind=ScopeSubjectKind(binding["subject_kind"]),
            role=ClaimScopeRole(binding["scope_role"]),
            canonical_ref=binding["canonical_ref"],
            display_label=binding["display_label"],
            canonical_ref_status=binding["canonical_ref_status"],
            normalization_version=binding["normalization_version"],
            claim_local_evidence_refs=tuple(x["evidence_reference_id"] for x in refs),
        ))
    proposition = row["proposition"]
    if isinstance(proposition, str):
        import json
        proposition = json.loads(proposition)
    temporal_scope = row["temporal_scope"] or {}
    if isinstance(temporal_scope, str):
        import json
        temporal_scope = json.loads(temporal_scope)
    return ModelVersion(
        version_id=row["version_id"], model_id=row["model_id"], version=row["version"],
        tenant_id=row["tenant_id"], admission_decision_id=row["admission_decision_id"],
        source_candidate_id=row["source_candidate_id"],
        source_candidate_version=row["source_candidate_version"], natural=row["natural_text"],
        proposition=proposition, confidence=float(row["confidence"]), evidence=evidence,
        semantic_digest_version=int(row["semantic_digest_version"]),
        falsifier=row["falsifier"], evidential_weight=float(row["evidential_weight"]),
        supporting_model_ids=tuple(row["supporting_model_ids"] or ()),
        visible_to_subjects=bool(row["visible_to_subjects"]),
        resolution_outcome=row["resolution_outcome"], resolved_at=row["resolved_at"],
        temporal_scope=temporal_scope,
        scope=tuple(scope), lifecycle=ModelTruthLifecycle(row["lifecycle"]),
        created_at=row["created_at"], semantic_digest=row["semantic_digest"],
    )


async def advance_validated_think_model(
    conn: asyncpg.Connection, *, tenant_id: UUID, model_id: UUID,
    confidence: float, evidence_observation_ids: tuple[UUID, ...],
    proposition: dict[str, Any] | None = None,
    falsifier: dict[str, Any] | None = None,
    evidential_weight: float | None = None,
    supporting_model_ids: tuple[UUID, ...] | None = None,
    visible_to_subjects: bool | None = None,
    resolution_outcome: bool | None = None,
    resolved_at: datetime | None = None,
    scope: tuple[ClaimScopeBinding, ...] | None = None,
    temporal_scope: dict[str, Any] | None = None,
    transition: ModelTruthTransition = ModelTruthTransition.CONFIRM,
    reason_code: str = "validated_think_update",
) -> UUID:
    idempotency_key = f"think-update:{tenant_id}:{model_id}:{reason_code}"
    replay = await conn.fetchval(
        "SELECT command_id FROM truth_command_receipts WHERE tenant_id=$1 AND idempotency_key=$2",
        tenant_id, idempotency_key,
    )
    if replay is not None:
        return replay
    prior = await _current_truth_version(conn, tenant_id=tenant_id, model_id=model_id)
    rows = await conn.fetch("""
        SELECT id,occurred_at,source_channel,content_text FROM observations
        WHERE tenant_id=$1 AND id=ANY($2::uuid[]) ORDER BY occurred_at,id
    """, tenant_id, list(evidence_observation_ids))
    if len(rows) != len(set(evidence_observation_ids)):
        raise InvariantViolation(
            "THINK_TRUTH_UPDATE_EVIDENCE_NOT_FOUND",
            "every update evidence observation must exist in the same tenant",
        )
    at = datetime.now(timezone.utc)
    version_id = uuid5(model_id, f"think-update:{prior.version + 1}:{reason_code}")
    prior_observations = {x.evidence_id for x in prior.evidence if x.kind is TruthEvidenceKind.OBSERVATION}
    additions = tuple(TruthEvidenceReference(
        reference_id=uuid5(version_id, f"observation:{row['id']}"), tenant_id=tenant_id,
        kind=TruthEvidenceKind.OBSERVATION, evidence_id=str(row["id"]), evidence_version=1,
        evidence_digest=canonical_sha256(str(row["content_text"] or "")),
        role=(TruthEvidenceRole.SUPPORT if transition is ModelTruthTransition.CONFIRM else TruthEvidenceRole.COUNTEREVIDENCE),
        coordinate=TruthEvidenceCoordinate(
            source_system=str(row["source_channel"] or "normalized-signal"),
            source_object_id=str(row["id"]), source_revision="1", field_path="content_text",
            span_start=0, span_end=len(str(row["content_text"] or "")),
        ), authority=EvidenceAuthority(
            authority_ref=f"validated-think-update:{model_id}", policy_version=_COMMAND_VERSION,
            authority_epoch=1, decided_at=at,
        ), occurred_at=row["occurred_at"], recorded_at=at, cutoff_at=at,
    ) for row in rows if str(row["id"]) not in prior_observations)
    evidence = (*prior.evidence, *additions)
    next_proposition = proposition if proposition is not None else prior.proposition
    next_falsifier = falsifier if falsifier is not None else prior.falsifier
    next_weight = evidential_weight if evidential_weight is not None else prior.evidential_weight
    next_supporting_models = supporting_model_ids if supporting_model_ids is not None else prior.supporting_model_ids
    next_visible = visible_to_subjects if visible_to_subjects is not None else prior.visible_to_subjects
    next_resolution = resolution_outcome if resolution_outcome is not None else prior.resolution_outcome
    next_resolved_at = resolved_at if resolved_at is not None else prior.resolved_at
    next_scope = scope if scope is not None else prior.scope
    next_temporal_scope = temporal_scope if temporal_scope is not None else prior.temporal_scope
    next_version = prior.model_copy(update={
        "version_id": version_id, "version": prior.version + 1,
        "confidence": confidence, "evidence": evidence,
        "semantic_digest_version": 2,
        "proposition": next_proposition, "falsifier": next_falsifier,
        "evidential_weight": next_weight,
        "supporting_model_ids": next_supporting_models,
        "visible_to_subjects": next_visible,
        "resolution_outcome": next_resolution, "resolved_at": next_resolved_at,
        "scope": next_scope,
        "temporal_scope": next_temporal_scope,
        "lifecycle": transition.resulting_lifecycle, "created_at": at,
        "semantic_digest": ModelVersion.compute_semantic_digest(
            proposition=next_proposition, natural=prior.natural, evidence=evidence,
            scope=next_scope, confidence=confidence,
            falsifier=next_falsifier, evidential_weight=next_weight,
            supporting_model_ids=next_supporting_models,
            visible_to_subjects=next_visible,
            resolution_outcome=next_resolution, resolved_at=next_resolved_at,
            temporal_scope=next_temporal_scope,
        ),
    })
    command_id = uuid5(version_id, "think-update-command")
    await build_default_truth_kernel().advance(tx=conn, command=AdvanceModelHeadCommand(
        command_id=command_id,
        idempotency_key=idempotency_key,
        tenant_id=tenant_id, expectation=ModelHeadExpectation(
            tenant_id=tenant_id, model_id=model_id, expected_version_id=prior.version_id,
            expected_version=prior.version, expected_semantic_digest=prior.semantic_digest,
            expected_lifecycle=prior.lifecycle,
        ), next_version=next_version, transition=transition,
        reason_codes=(reason_code,), issued_at=at,
    ))
    return command_id


__all__ = [
    "admit_validated_think_claim", "advance_validated_think_model",
    "build_think_admission_command",
]
