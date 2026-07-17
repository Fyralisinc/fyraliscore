"""PostgreSQL closure probes for P3 identity, scope, and tenant authority."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any
from uuid import UUID, uuid4

from lib.contracts.truth_admission import (
    AdmissionDecision, AdmissionDisposition, AdmitModelCommand,
    CandidateReviewState, ModelVersion, TruthCandidate, TruthCandidateKind,
)
from lib.contracts.truth_evidence import (
    ClaimScopeBinding, ClaimScopeRole, EvidenceAuthority, ScopeSubjectKind,
    TruthEvidenceCoordinate, TruthEvidenceKind, TruthEvidenceReference,
    TruthEvidenceRole,
)
from lib.evaluation.epistemic_repair.p2_oracles import stable_digest
from services.domain.entity_aliases.repo import insert_alias_with_connection
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.service import TruthKernelService


@dataclass(frozen=True, slots=True)
class P3PostgresProof:
    hg02_conforms: bool
    hg02_applied_count: int
    hg02_replay_extra_effects: int
    hg02_bypass_rejected: bool
    hg06_conforms: bool
    hg06_scope_count: int
    hg14_conforms: bool
    hg14_cross_tenant_incidents: int
    violation_codes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _admission(
    tenant_id: UUID, *, subject_id: UUID, decisive_ref: UUID, ordinal: int
) -> AdmitModelCommand:
    now = datetime(2026, 7, 17, tzinfo=timezone.utc)
    evidence = TruthEvidenceReference(
        reference_id=decisive_ref, tenant_id=tenant_id,
        kind=TruthEvidenceKind.REGISTERED,
        evidence_id=f"grounding-decision:{decisive_ref}", evidence_version=1,
        evidence_digest=stable_digest({"decisive_grounding": str(decisive_ref)}),
        role=TruthEvidenceRole.AUTHORITY,
        coordinate=TruthEvidenceCoordinate(
            source_system="p3-grounding", source_object_id=f"correction-{ordinal}",
            source_revision="1", field_path="selected_referent",
        ),
        authority=EvidenceAuthority(
            authority_ref=f"grounding-adjudication:{decisive_ref}",
            policy_version="p3-v1", authority_epoch=1,
            decided_at=now - timedelta(minutes=2),
        ),
        occurred_at=now - timedelta(minutes=3),
        recorded_at=now - timedelta(minutes=2), cutoff_at=now,
    )
    scope = (ClaimScopeBinding(
        subject_id=subject_id, subject_kind=ScopeSubjectKind.PROJECT,
        role=ClaimScopeRole.SUBJECT,
        claim_local_evidence_refs=(decisive_ref,),
    ),)
    candidate_id, model_id, version_id, decision_id = (
        uuid4(), uuid4(), uuid4(), uuid4()
    )
    proposition = {
        "subject": str(subject_id), "predicate": "grounded_identity",
        "object": f"Project-{ordinal}",
        "decisive_grounding_ref": str(decisive_ref),
    }
    candidate = TruthCandidate(
        candidate_id=candidate_id, tenant_id=tenant_id,
        kind=TruthCandidateKind.ATOMIC_CLAIM,
        review_state=CandidateReviewState.PROPOSED,
        natural=f"Project-{ordinal} is the adjudicated referent.",
        proposition=proposition, proposed_evidence=(evidence,),
        proposed_scope=scope, created_at=now,
    )
    decision = AdmissionDecision(
        decision_id=decision_id, tenant_id=tenant_id,
        candidate_id=candidate_id, candidate_version=1,
        candidate_digest=candidate.candidate_digest,
        disposition=AdmissionDisposition.ACCEPTED,
        reason_codes=("decisive_grounding_provenance",),
        decided_by="p3-postgres-probe", decided_at=now + timedelta(seconds=1),
        admitted_model_id=model_id, admitted_version_id=version_id,
    )
    digest = ModelVersion.compute_semantic_digest(
        proposition=proposition, natural=candidate.natural,
        evidence=(evidence,), scope=scope,
    )
    version = ModelVersion(
        version_id=version_id, model_id=model_id, version=1,
        tenant_id=tenant_id, admission_decision_id=decision_id,
        source_candidate_id=candidate_id, source_candidate_version=1,
        natural=candidate.natural, proposition=proposition,
        evidence=(evidence,), scope=scope, created_at=now + timedelta(seconds=2),
        semantic_digest=digest,
    )
    return AdmitModelCommand(
        command_id=uuid4(), idempotency_key=f"p3-scope:{candidate_id}",
        tenant_id=tenant_id, candidate=candidate, decision=decision,
        version=version, issued_at=now + timedelta(seconds=3),
    )


async def run_p3_postgres_probes(conn: Any) -> P3PostgresProof:
    primary, foreign = uuid4(), uuid4()
    await conn.executemany(
        "INSERT INTO tenants(id,name) VALUES($1,$2)",
        ((primary, "p3-primary"), (foreign, "p3-foreign")),
    )
    subjects: list[UUID] = []
    alias_ids: list[UUID] = []
    for ordinal in range(5):
        subject, provenance = uuid4(), uuid4()
        subjects.append(subject)
        kwargs = dict(
            phrase=f"P3 corrected identity {ordinal}",
            resolved_entity_ref={"type": "project", "id": str(subject), "version": 1},
            source="ingestion", confidence=1.0, tenant_id=primary,
            extra_metadata={
                "identity_basis_class": "source_authoritative",
                "identity_basis_ref": f"p3-correction:{provenance}",
                "authority_ref": f"grounding-adjudication:{provenance}",
                "source_provenance_ref": str(provenance),
            },
        )
        first = await insert_alias_with_connection(conn, **kwargs)
        replay = await insert_alias_with_connection(conn, **kwargs)
        if first.id == replay.id:
            alias_ids.append(first.id)
    alias_rows = await conn.fetch(
        "SELECT id,entity_metadata FROM entity_aliases WHERE tenant_id=$1 AND id=ANY($2::uuid[])",
        primary, alias_ids,
    )
    alias_count = len(alias_rows)
    provenance_ok = all(
        (metadata := (json.loads(row["entity_metadata"]) if isinstance(row["entity_metadata"], str) else row["entity_metadata"]))
        and metadata.get("source") == "ingestion"
        and metadata.get("identity_basis_class") == "source_authoritative"
        and metadata.get("authority_ref")
        and metadata.get("source_provenance_ref")
        for row in alias_rows
    )
    insert_bypass_rejected = False
    try:
        async with conn.transaction():
            await conn.execute(
                """INSERT INTO entity_aliases
                   (id,tenant_id,alias_text,resolved_entity_ref,confidence)
                   VALUES($1,$2,'forbidden bypass','{}'::jsonb,1.0)""",
                uuid4(), primary,
            )
    except Exception:
        insert_bypass_rejected = True
    delete_bypass_rejected = False
    try:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM entity_aliases WHERE tenant_id=$1 AND id=$2",
                primary, alias_ids[0],
            )
    except Exception:
        delete_bypass_rejected = True
    bypass_rejected = insert_bypass_rejected and delete_bypass_rejected

    service = TruthKernelService(storage=AsyncpgTruthKernelStorage())
    commands = [
        _admission(primary, subject_id=subjects[i], decisive_ref=uuid4(), ordinal=i)
        for i in range(5)
    ]
    foreign_command = _admission(
        foreign, subject_id=uuid4(), decisive_ref=uuid4(), ordinal=99
    )
    for command in (*commands, foreign_command):
        await service.admit(tx=conn, command=command)
    scope_rows = await conn.fetch(
        """
        SELECT b.model_version_id, b.subject_id, b.subject_kind, b.scope_role,
               b.evidence_reference_id, e.evidence_role, e.authority_ref
        FROM (
          SELECT binding.tenant_id, binding.model_version_id, binding.subject_id,
                 binding.subject_kind, binding.scope_role,
                 scope_evidence.evidence_reference_id
          FROM model_truth_scope_bindings binding
          JOIN model_truth_scope_evidence scope_evidence
            USING (tenant_id, model_version_id, binding_id)
          WHERE binding.tenant_id=$1
        ) b
        JOIN model_truth_evidence_references e
          ON e.tenant_id=b.tenant_id
         AND e.model_version_id=b.model_version_id
         AND e.reference_id=b.evidence_reference_id
        """,
        primary,
    )
    scope_ok = len(scope_rows) == 5 and all(
        row["subject_kind"] == "project"
        and row["scope_role"] == "subject"
        and row["evidence_role"] == "authority"
        and row["authority_ref"].startswith("grounding-adjudication:")
        for row in scope_rows
    )

    await conn.execute(
        "SELECT set_config('app.current_tenant',$1,true)", str(primary)
    )
    visible_candidates = await conn.fetchval("SELECT count(*) FROM truth_candidates")
    visible_scopes = await conn.fetchval("SELECT count(*) FROM model_truth_scope_bindings")
    visible_aliases = await conn.fetchval("SELECT count(*) FROM entity_aliases WHERE tenant_id<>$1", primary)
    await conn.execute("SELECT set_config('app.current_tenant','',true)")
    tenant_ok = visible_candidates == 5 and visible_scopes == 5 and visible_aliases == 0

    hg02 = len(alias_ids) == 5 and alias_count == 5 and provenance_ok and bypass_rejected
    violations = tuple(code for ok, code in (
        (hg02, "identity_correction_not_governed_idempotent"),
        (scope_ok, "scope_not_bound_to_decisive_grounding"),
        (tenant_ok, "cross_tenant_context_candidate_or_link_visible"),
    ) if not ok)
    return P3PostgresProof(
        hg02, int(alias_count), max(0, int(alias_count) - 5), bypass_rejected,
        scope_ok, len(scope_rows), tenant_ok, 0 if tenant_ok else 1, violations,
    )


__all__ = ["P3PostgresProof", "run_p3_postgres_probes"]
