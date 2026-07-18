from __future__ import annotations

from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest
from pydantic import ValidationError

from lib.contracts.company_learning import (
    AcceptedHeadRef, AcceptedMemorySnapshot, AdmitCompositeRelationCommand,
    CompositeRelationTemplate, EvidenceManifest,
)
from lib.contracts.kernel import canonical_sha256
from lib.contracts.truth_admission import (
    AdmissionDecision, AdmissionDisposition, AdmitModelCommand, CandidateReviewState,
    ModelTruthLifecycle, ModelVersion, TruthCandidate, TruthCandidateKind,
)
from lib.contracts.truth_evidence import (
    ClaimScopeBinding, ClaimScopeRole, EvidenceAuthority, ScopeSubjectKind,
    TruthEvidenceCoordinate, TruthEvidenceKind, TruthEvidenceReference, TruthEvidenceRole,
)


NOW = datetime(2026, 7, 18, tzinfo=timezone.utc)
TENANT = uuid5(NAMESPACE_URL, "tenant")


def uid(value: str) -> UUID:
    return uuid5(NAMESPACE_URL, value)


def evidence(
    name: str, *, kind=TruthEvidenceKind.OBSERVATION,
    role=TruthEvidenceRole.SUPPORT, evidence_id: str | None = None,
):
    return TruthEvidenceReference(
        reference_id=uid(f"ref:{name}"), tenant_id=TENANT, kind=kind,
        evidence_id=evidence_id or str(uid(f"evidence:{name}")), evidence_version=1,
        evidence_digest=canonical_sha256(name), role=role,
        coordinate=TruthEvidenceCoordinate(
            source_system="test", source_object_id=name, source_revision="1",
        ),
        authority=EvidenceAuthority(
            authority_ref="test", policy_version="v1", authority_epoch=1, decided_at=NOW,
        ), occurred_at=NOW, recorded_at=NOW, cutoff_at=NOW,
    )


def head(name: str) -> AcceptedHeadRef:
    return AcceptedHeadRef(
        tenant_id=TENANT, model_id=uid(f"model:{name}"), version_id=uid(f"version:{name}"),
        version=1, semantic_digest=canonical_sha256(name), lifecycle=ModelTruthLifecycle.ACTIVE,
        canonical_scope_refs=("workstream:atlas",),
    )


def composite_admission(direct: TruthEvidenceReference, members: tuple[AcceptedHeadRef, ...]):
    model_id, version_id, candidate_id, decision_id = (
        uid("composite"), uid("composite-version"), uid("candidate"), uid("decision"),
    )
    derivation = tuple(evidence(
        f"derive:{item.model_id}", kind=TruthEvidenceKind.MODEL_VERSION,
        role=TruthEvidenceRole.DERIVATION, evidence_id=str(item.version_id),
    ) for item in members)
    all_evidence = (direct, *derivation)
    scope = (ClaimScopeBinding(
        subject_id=uid("atlas"), subject_kind=ScopeSubjectKind.PROJECT,
        role=ClaimScopeRole.SUBJECT, canonical_ref="workstream:atlas",
        display_label="Atlas", canonical_ref_status="provisional", normalization_version=1,
        claim_local_evidence_refs=tuple(sorted((item.reference_id for item in all_evidence), key=str)),
    ),)
    candidate = TruthCandidate(
        candidate_id=candidate_id, tenant_id=TENANT, kind=TruthCandidateKind.SYNTHESIS,
        review_state=CandidateReviewState.PROPOSED, natural="Atlas is blocked by ownership.",
        proposition={"kind":"situation", "member_model_ids":[str(x.model_id) for x in members]},
        supporting_model_ids=tuple(x.model_id for x in members), proposed_evidence=all_evidence,
        proposed_scope=scope, created_at=NOW,
    )
    decision = AdmissionDecision(
        decision_id=decision_id, tenant_id=TENANT, candidate_id=candidate_id,
        candidate_version=1, candidate_digest=candidate.candidate_digest,
        disposition=AdmissionDisposition.ACCEPTED, reason_codes=("VALID",),
        decided_by="test", decided_at=NOW, admitted_model_id=model_id,
        admitted_version_id=version_id,
    )
    version = ModelVersion(
        version_id=version_id, model_id=model_id, version=1, tenant_id=TENANT,
        admission_decision_id=decision_id, source_candidate_id=candidate_id,
        source_candidate_version=1, natural=candidate.natural, proposition=candidate.proposition,
        supporting_model_ids=candidate.supporting_model_ids, evidence=all_evidence, scope=scope,
        semantic_digest_version=2,
        created_at=NOW, semantic_digest=ModelVersion.compute_semantic_digest(
            proposition=candidate.proposition, natural=candidate.natural, evidence=all_evidence,
            scope=scope, confidence=candidate.confidence, falsifier=None,
            evidential_weight=candidate.evidential_weight,
            supporting_model_ids=candidate.supporting_model_ids,
            visible_to_subjects=True, resolution_outcome=None, resolved_at=None,
            temporal_scope={},
        ),
    )
    return AdmitModelCommand(
        command_id=uid("admit"), idempotency_key="admit:composite", tenant_id=TENANT,
        candidate=candidate, decision=decision, version=version, issued_at=NOW,
    ), derivation


def command() -> AdmitCompositeRelationCommand:
    members = tuple(sorted((head("cause"), head("effect")), key=lambda item: str(item.model_id)))
    direct = evidence("direct")
    relation_evidence = evidence("relation")
    composite, derivation = composite_admission(direct, members)
    manifest = EvidenceManifest(
        tenant_id=TENANT, cutoff_at=NOW, direct=(direct,), model_derivation=tuple(sorted(
            derivation, key=lambda item: str(item.reference_id)
        )), relation=(relation_evidence,),
    )
    relation = CompositeRelationTemplate(
        relation_id=uid("relation"), relation_kind="dependency_constraint",
        source_model_id=members[0].model_id, source_model_version_id=members[0].version_id,
        target_model_id=members[1].model_id, target_model_version_id=members[1].version_id,
        evidence_reference_ids=(relation_evidence.reference_id,), rationale="Ownership blocks release.",
        mechanism="Open ownership blocks rollout readiness.",
    )
    snapshot = AcceptedMemorySnapshot(
        snapshot_id=uid("snapshot"), tenant_id=TENANT, cutoff_at=NOW,
        model_heads=members, retrieval_receipt_ids=(uid("receipt"),),
    )
    provisional = AdmitCompositeRelationCommand.model_construct(
        command_id=uid("bundle"), idempotency_key="pending", tenant_id=TENANT,
        snapshot=snapshot, evidence_manifest=manifest, composite=composite,
        relation=relation, expected_member_heads=members, issued_at=NOW,
    )
    payload = provisional.model_dump()
    payload["idempotency_key"] = provisional.expected_idempotency_key
    return AdmitCompositeRelationCommand(**payload)


def test_snapshot_and_command_digests_are_deterministic() -> None:
    first, second = command(), command()
    assert first.snapshot.snapshot_digest == second.snapshot.snapshot_digest
    assert first.evidence_manifest.manifest_digest == second.evidence_manifest.manifest_digest
    assert first.idempotency_key == second.idempotency_key
    assert first.request_digest == second.request_digest


def test_snapshot_rejects_unsorted_or_terminal_heads() -> None:
    heads = tuple(reversed(sorted((head("z"), head("a")), key=lambda item: str(item.model_id))))
    with pytest.raises(ValidationError, match="deterministically sorted"):
        AcceptedMemorySnapshot(snapshot_id=uid("s"), tenant_id=TENANT, cutoff_at=NOW, model_heads=heads)
    payload = head("a").model_dump()
    payload["lifecycle"] = ModelTruthLifecycle.FALSIFIED
    with pytest.raises(ValidationError, match="must be active"):
        AcceptedHeadRef(**payload)


def test_manifest_rejects_duplicate_partition_and_auxiliary_support() -> None:
    direct = evidence("same")
    with pytest.raises(ValidationError, match="multiple partitions"):
        EvidenceManifest(tenant_id=TENANT, cutoff_at=NOW, direct=(direct,), relation=(direct,))
    with pytest.raises(ValidationError, match="incompatible role"):
        EvidenceManifest(tenant_id=TENANT, cutoff_at=NOW, auxiliary=(direct,))


def test_auxiliary_is_excluded_from_canonical_evidence() -> None:
    direct = evidence("direct")
    auxiliary = evidence("aux", role=TruthEvidenceRole.CONTEXT)
    manifest = EvidenceManifest(
        tenant_id=TENANT, cutoff_at=NOW, direct=(direct,), auxiliary=(auxiliary,),
    )
    assert tuple(item.reference_id for item in manifest.canonical_evidence) == (direct.reference_id,)


def test_command_rejects_stale_endpoint_and_wrong_idempotency_identity() -> None:
    valid = command()
    stale = valid.relation.model_copy(update={"source_model_version_id": uid("stale")})
    with pytest.raises(ValidationError, match="exact expected snapshot versions"):
        AdmitCompositeRelationCommand(**{**valid.model_dump(), "relation": stale})
    with pytest.raises(ValidationError, match="idempotency key"):
        AdmitCompositeRelationCommand(**{**valid.model_dump(), "idempotency_key": "wrong"})
