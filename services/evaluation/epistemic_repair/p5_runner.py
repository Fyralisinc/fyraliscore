"""PostgreSQL-backed P5 zero-seed vertical learning harness.

The runner starts at normalized persisted Observations.  It deliberately does
not construct connectors, listeners, polling, webhooks, or provider calls.
The caller owns the outer transaction so evaluation runs can be rolled back.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import time
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.contracts.truth_admission import (
    AdvanceModelHeadCommand,
    ModelHeadExpectation,
    ModelTruthLifecycle,
    ModelTruthTransition,
    ModelVersion,
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
from lib.embeddings.ollama import EMBEDDING_DIM
from lib.evaluation.epistemic_repair.p5_oracles import (
    P5Artifact,
    P5BarrierReceipt,
    P5SignalReceipt,
    P5VerticalReceipt,
    build_p5_artifact,
)
from lib.evaluation.epistemic_repair.p5_population import (
    P5Batch,
    P5Population,
    P5Signal,
    build_p5_population,
)
from services.domain.company_learning.barrier import (
    CompanyLearningBarrierService,
    ContextDecision,
    OutcomeLink,
)
from services.domain.entity_grounding.episode import (
    GroundingCandidateInput,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from services.domain.entity_grounding.repo import EntityGroundingRepo
from services.domain.models.repo import ModelsRepo
from services.domain.source_semantics.processor import GroundedBeliefProcessor
from services.domain.truth_kernel import build_default_truth_kernel
from services.domain.truth_kernel.relations.contracts import (
    DirectionAssertion,
    RelationCandidate,
    RelationDisposition,
    RelationEvidence,
    RelationKind,
    RelationParticipant,
    ROLE_SCHEMA,
)
from services.domain.truth_kernel.relations.repository import (
    AsyncpgRelationKernelStorage,
)
from services.domain.truth_kernel.relations.service import (
    AdmitRelationCommand,
    RelationTruthKernel,
)


P5_NOW = datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc)
_RELATION_KIND = RelationKind.DEPENDENCY_CONSTRAINT


def _stable_id(tenant_id: UUID, label: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"p5:{tenant_id}:{label}")


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _occurred_at(signal: P5Signal) -> datetime:
    return P5_NOW + timedelta(
        days=signal.batch_number - 1,
        minutes=signal.position,
    )


_SUPPORTED_REPORT = re.compile(
    r"^(?P<surface>.+?)\s+(?:is|are|was|were)\s+(?:not\s+)?"
    r"(?:blocked|approved|ready|delayed|complete)\.$",
    re.IGNORECASE,
)


def _semantic_surface(signal: P5Signal) -> str | None:
    """Runtime eligibility gate over source text, independent of sealed gold."""

    match = _SUPPORTED_REPORT.fullmatch(signal.text.strip())
    return match.group("surface") if match else None


def _canonical_ref_for_surface(surface: str) -> dict[str, str]:
    """Translate a source-native normalized subject into a typed local ref."""

    slug = re.sub(r"[^a-z0-9]+", "-", surface.casefold()).strip("-")
    kind = "work_item" if "renewal" in surface.casefold() else "project"
    return {"type": kind, "id": f"{kind}:{slug}"}


async def _persist_batch(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    batch: P5Batch,
) -> dict[str, UUID]:
    rows = []
    result: dict[str, UUID] = {}
    for signal in batch.signals:
        observation_id = _stable_id(tenant_id, f"observation:{signal.signal_id}")
        result[signal.signal_id] = observation_id
        content: dict[str, Any] = {"text": signal.text}
        surface = _semantic_surface(signal)
        if surface:
            content["_unresolved_phrases"] = [surface]
        rows.append(
            (
                observation_id,
                tenant_id,
                _occurred_at(signal),
                signal.source_channel,
                json.dumps(content),
                signal.text,
            )
        )
    await conn.executemany(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, content,
          content_text, embedding_pending, trust_tier, entities_mentioned
        ) VALUES ($1,$2,$3,'signal',$4,$5::jsonb,$6,TRUE,'ordinary','[]'::jsonb)
        """,
        rows,
    )
    return result


async def _ground_and_admit(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    signal: P5Signal,
    observation_id: UUID,
    processor: GroundedBeliefProcessor,
) -> tuple[str, str, UUID, UUID, bool]:
    entity_surface = _semantic_surface(signal)
    if entity_surface is None:
        raise ValueError("source-semantic runtime gate rejected the signal")
    canonical_ref = _canonical_ref_for_surface(entity_surface)
    occurred_at = _occurred_at(signal)
    decided_at = occurred_at + timedelta(seconds=30)
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=entity_surface,
        occurred_at=occurred_at,
        source_channel=signal.source_channel,
        source_space=signal.source_space,
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        now=decided_at,
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=entity_surface,
        content_text=signal.text,
        source_channel=signal.source_channel,
        context_command=context_command,
        context_outcome=context_outcome,
        now=decided_at,
    )
    authority_ref = f"normalized-source:{signal.source_space}:{entity_surface}"
    episode = build_grounding_episode(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=entity_surface,
        occurred_at=occurred_at,
        source_channel=signal.source_channel,
        source_space=signal.source_space,
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        candidates=(
            GroundingCandidateInput(
                canonical_ref=canonical_ref,
                candidate_source="authenticated_normalized_source",
                positive_evidence_refs=(f"observation:{observation_id}",),
                independent_identity_evidence_refs=(authority_ref,),
                exact_mention_match=True,
                decisive_authority_refs=(authority_ref,),
            ),
        ),
        model_candidate_id=candidate_id_for_ref(canonical_ref),
        model_canonical_ref=canonical_ref,
        model_confidence=0.97,
        model_reasoning="one exact source-authenticated candidate",
        high_confidence=0.8,
        review_min=0.5,
        prepared_context_command=context_command,
        prepared_context_outcome=context_outcome,
        prepared_mention_detection_command=mention_command,
        now=decided_at,
    )
    trace_id = await EntityGroundingRepo(pool=object()).append_episode(  # type: ignore[arg-type]
        episode=episode,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        phrase=entity_surface,
        conn=conn,
    )
    applied = await processor.process_trace(
        conn,
        tenant_id=tenant_id,
        grounding_trace_id=trace_id,
        embedding=[0.0] * EMBEDDING_DIM,
        now=decided_at + timedelta(seconds=30),
    )
    if applied.model_id is None:
        raise AssertionError(
            f"sealed semantic signal {signal.signal_id} did not admit a Model: "
            f"{applied.reason_codes}"
        )
    accepted = await conn.fetchrow(
        """
        SELECT truth_version_id, proposition
        FROM accepted_current_models
        WHERE tenant_id=$1 AND id=$2
        """,
        tenant_id,
        applied.model_id,
    )
    if accepted is None:
        raise AssertionError("source-semantic admission is absent from accepted truth")
    proposition = _json(accepted["proposition"])
    atomic = (
        proposition.get("claim_role") == "fact"
        and proposition.get("abstraction_level") == "atomic"
    )
    return (
        episode.current_fate,
        applied.disposition.value,
        applied.model_id,
        accepted["truth_version_id"],
        atomic,
    )


async def _load_model_version(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    version_id: UUID,
) -> ModelVersion:
    row = await conn.fetchrow(
        """
        SELECT * FROM model_truth_versions
        WHERE tenant_id=$1 AND version_id=$2
        """,
        tenant_id,
        version_id,
    )
    if row is None:
        raise LookupError(f"ModelVersion {version_id} does not exist")
    evidence_rows = await conn.fetch(
        """
        SELECT * FROM model_truth_evidence_references
        WHERE tenant_id=$1 AND model_version_id=$2
        ORDER BY reference_id
        """,
        tenant_id,
        version_id,
    )
    evidence = tuple(
        TruthEvidenceReference(
            reference_id=item["reference_id"],
            tenant_id=item["tenant_id"],
            kind=TruthEvidenceKind(item["evidence_kind"]),
            evidence_id=item["evidence_id"],
            evidence_version=int(item["evidence_version"]),
            evidence_digest=item["evidence_digest"],
            role=TruthEvidenceRole(item["evidence_role"]),
            coordinate=TruthEvidenceCoordinate(
                source_system=item["source_system"],
                source_object_id=item["source_object_id"],
                source_revision=item["source_revision"],
                field_path=item["field_path"],
                span_start=item["span_start"],
                span_end=item["span_end"],
                time_range_start=item["time_range_start"],
                time_range_end=item["time_range_end"],
            ),
            authority=EvidenceAuthority(
                authority_ref=item["authority_ref"],
                policy_version=item["policy_version"],
                authority_epoch=int(item["authority_epoch"]),
                decided_at=item["authority_decided_at"],
                expires_at=item["authority_expires_at"],
            ),
            occurred_at=item["occurred_at"],
            recorded_at=item["recorded_at"],
            cutoff_at=item["cutoff_at"],
        )
        for item in evidence_rows
    )
    binding_rows = await conn.fetch(
        """
        SELECT binding_id, subject_id, subject_kind, scope_role,
               canonical_ref, display_label, canonical_ref_status,
               normalization_version
        FROM model_truth_scope_bindings
        WHERE tenant_id=$1 AND model_version_id=$2
        ORDER BY subject_id, scope_role
        """,
        tenant_id,
        version_id,
    )
    scope: list[ClaimScopeBinding] = []
    for binding in binding_rows:
        evidence_ids = await conn.fetch(
            """
            SELECT evidence_reference_id
            FROM model_truth_scope_evidence
            WHERE tenant_id=$1 AND model_version_id=$2 AND binding_id=$3
            ORDER BY evidence_reference_id
            """,
            tenant_id,
            version_id,
            binding["binding_id"],
        )
        scope.append(
            ClaimScopeBinding(
                subject_id=binding["subject_id"],
                subject_kind=ScopeSubjectKind(binding["subject_kind"]),
                role=ClaimScopeRole(binding["scope_role"]),
                canonical_ref=binding["canonical_ref"],
                display_label=binding["display_label"],
                canonical_ref_status=binding["canonical_ref_status"],
                normalization_version=binding["normalization_version"],
                claim_local_evidence_refs=tuple(
                    item["evidence_reference_id"] for item in evidence_ids
                ),
            )
        )
    return ModelVersion(
        version_id=row["version_id"],
        model_id=row["model_id"],
        version=int(row["version"]),
        tenant_id=row["tenant_id"],
        admission_decision_id=row["admission_decision_id"],
        source_candidate_id=row["source_candidate_id"],
        source_candidate_version=int(row["source_candidate_version"]),
        natural=row["natural_text"],
        proposition=_json(row["proposition"]),
        confidence=float(row["confidence"]),
        semantic_digest_version=int(row["semantic_digest_version"]),
        falsifier=_json(row["falsifier"]) if row["falsifier"] is not None else None,
        evidential_weight=float(row["evidential_weight"]),
        supporting_model_ids=tuple(row["supporting_model_ids"]),
        visible_to_subjects=bool(row["visible_to_subjects"]),
        resolution_outcome=row["resolution_outcome"],
        resolved_at=row["resolved_at"],
        temporal_scope=_json(row["temporal_scope"]),
        evidence=evidence,
        scope=tuple(scope),
        lifecycle=ModelTruthLifecycle(row["lifecycle"]),
        created_at=row["created_at"],
        semantic_digest=row["semantic_digest"],
    )


async def _admit_relation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    old_model_id: UUID,
    old_version_id: UUID,
    new_model_id: UUID,
    new_version_id: UUID,
    issued_at: datetime,
) -> tuple[UUID, UUID]:
    relation_id = _stable_id(tenant_id, "relation:harbor-certificate-dependency")
    relation_version_id = _stable_id(tenant_id, "relation-version:harbor:v1")
    roles = ROLE_SCHEMA[_RELATION_KIND]
    participants = (
        RelationParticipant(
            model_id=old_model_id,
            model_version_id=old_version_id,
            role=roles[0],
            ordinal=0,
        ),
        RelationParticipant(
            model_id=new_model_id,
            model_version_id=new_version_id,
            role=roles[1],
            ordinal=1,
        ),
    )
    evidence_rows = await conn.fetch(
        """
        SELECT model_version_id, reference_id, evidence_digest
        FROM model_truth_evidence_references
        WHERE tenant_id=$1 AND model_version_id=ANY($2::uuid[])
        ORDER BY model_version_id, reference_id
        """,
        tenant_id,
        [old_version_id, new_version_id],
    )
    by_version: dict[UUID, Any] = {}
    for row in evidence_rows:
        by_version.setdefault(row["model_version_id"], row)
    if set(by_version) != {old_version_id, new_version_id}:
        raise AssertionError("relation admission requires evidence from both endpoint versions")
    evidence = tuple(
        RelationEvidence(
            evidence_reference_id=by_version[version]["reference_id"],
            model_version_id=version,
            evidence_digest=by_version[version]["evidence_digest"],
            polarity=1,
            weight=weight,
        )
        for version, weight in ((old_version_id, 0.4), (new_version_id, 0.9))
    )
    candidate = RelationCandidate(
        candidate_relation_id=relation_id,
        tenant_id=tenant_id,
        proposed_kind=_RELATION_KIND.value,
        participants=participants,
        rationale=(
            "The Harbor release is the dependent state and completion of the "
            "certificate renewal is its prerequisite."
        ),
        assertion=DirectionAssertion(
            kind=_RELATION_KIND,
            source_model_version_id=old_version_id,
            target_model_version_id=new_version_id,
            polarity=1,
        ),
        evidence=evidence,
        created_at=issued_at,
    )
    await conn.execute(
        """
        INSERT INTO relation_instances (
          id, tenant_id, relation_kind, status,
          participant_binding_status, write_policy
        ) VALUES ($1,$2,$3,'candidate','bound','candidate')
        """,
        relation_id,
        tenant_id,
        _RELATION_KIND.value,
    )
    command = AdmitRelationCommand(
        command_id=_stable_id(tenant_id, "relation-command:harbor:v1"),
        idempotency_key=f"p5-relation:{tenant_id}:harbor:v1",
        candidate=candidate,
        relation_version_id=relation_version_id,
        admission_decision_id=_stable_id(tenant_id, "relation-decision:harbor:v1"),
        issued_at=issued_at,
    )
    receipt = await RelationTruthKernel(AsyncpgRelationKernelStorage()).admit(
        tx=conn,
        command=command,
    )
    if receipt.disposition is not RelationDisposition.ACCEPTED:
        raise AssertionError(f"sealed typed relation was not admitted: {receipt}")
    visible = await conn.fetchval(
        """
        SELECT count(*) FROM accepted_current_relations
        WHERE tenant_id=$1 AND id=$2 AND truth_relation_version_id=$3
        """,
        tenant_id,
        relation_id,
        relation_version_id,
    )
    if visible != 1:
        raise AssertionError("admitted relation is absent from accepted relation truth")
    return relation_id, relation_version_id


def _counterevidence(
    *,
    tenant_id: UUID,
    corrected_version: ModelVersion,
    at: datetime,
) -> TruthEvidenceReference:
    return TruthEvidenceReference(
        reference_id=_stable_id(tenant_id, "counterevidence:harbor-correction"),
        tenant_id=tenant_id,
        kind=TruthEvidenceKind.MODEL_VERSION,
        evidence_id=str(corrected_version.version_id),
        evidence_version=corrected_version.version,
        evidence_digest=corrected_version.semantic_digest,
        role=TruthEvidenceRole.COUNTEREVIDENCE,
        coordinate=TruthEvidenceCoordinate(
            source_system="truth-kernel",
            source_object_id=str(corrected_version.version_id),
            source_revision=str(corrected_version.version),
            field_path="proposition",
        ),
        authority=EvidenceAuthority(
            authority_ref="p5-correction-evidence",
            policy_version="1",
            authority_epoch=1,
            decided_at=at - timedelta(seconds=1),
        ),
        occurred_at=corrected_version.created_at,
        recorded_at=at,
        cutoff_at=at,
    )


async def _falsify_exact_model(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    prior: ModelVersion,
    corrected: ModelVersion,
    at: datetime,
) -> ModelVersion:
    evidence = (*prior.evidence, _counterevidence(
        tenant_id=tenant_id,
        corrected_version=corrected,
        at=at,
    ))
    next_version = prior.model_copy(
        update={
            "version_id": _stable_id(tenant_id, "model-version:harbor:falsified"),
            "version": prior.version + 1,
            "evidence": evidence,
            "lifecycle": ModelTruthLifecycle.FALSIFIED,
            "created_at": at,
            "semantic_digest": ModelVersion.compute_semantic_digest(
                proposition=prior.proposition,
                natural=prior.natural,
                evidence=evidence,
                scope=prior.scope,
            ),
        }
    )
    command = AdvanceModelHeadCommand(
        command_id=_stable_id(tenant_id, "model-command:harbor:falsify"),
        idempotency_key=f"p5-falsify:{tenant_id}:{prior.version_id}",
        tenant_id=tenant_id,
        expectation=ModelHeadExpectation(
            tenant_id=tenant_id,
            model_id=prior.model_id,
            expected_version_id=prior.version_id,
            expected_version=prior.version,
            expected_semantic_digest=prior.semantic_digest,
            expected_lifecycle=prior.lifecycle,
        ),
        next_version=next_version,
        transition=ModelTruthTransition.FALSIFY,
        reason_codes=("authoritative_source_correction",),
        issued_at=at + timedelta(seconds=1),
    )
    receipt = await build_default_truth_kernel().advance(tx=conn, command=command)
    if receipt.lifecycle is not ModelTruthLifecycle.FALSIFIED:
        raise AssertionError("exact Model falsification did not reach terminal lifecycle")
    await RelationTruthKernel(AsyncpgRelationKernelStorage()).invalidate_evidence(
        tx=conn,
        tenant_id=tenant_id,
        invalidated_model_version_id=prior.version_id,
        cause_code="MODEL_FALSIFIED",
        occurred_at=at + timedelta(seconds=2),
    )
    return next_version


async def _record_unused_decision(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    signal: P5Signal,
    observation_id: UUID,
    barrier: CompanyLearningBarrierService,
) -> None:
    await barrier.record_context_decision(
        tx=conn,
        item=ContextDecision(
            decision_id=_stable_id(tenant_id, f"context:{signal.signal_id}"),
            tenant_id=tenant_id,
            batch_id=f"p5-batch-{signal.batch_number}",
            route_id="source-semantic-eligibility-v1",
            context_item_kind="current_episode",
            context_item_id=str(observation_id),
            context_item_version="1",
            retrieved=True,
            selected=False,
            included=False,
            referenced=False,
            counterevidence_retained=False,
            confidence_affecting=False,
            necessary_background=False,
            historical_reopen_reason=None,
            decision_fate="validator_drop",
            result_object_kind=None,
            result_object_id=None,
            evidence_lineage=(
                {"kind": "observation", "id": str(observation_id)},
            ),
            decided_at=_occurred_at(signal) + timedelta(seconds=45),
        ),
    )


async def _record_semantic_decision(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    signal: P5Signal,
    context_item_kind: str,
    context_item_id: str,
    context_item_version: str,
    result_object_kind: str,
    result_object_id: UUID,
    lineage: tuple[dict[str, Any], ...],
    barrier: CompanyLearningBarrierService,
    outcome_kind: str | None = None,
) -> UUID:
    decision_id = _stable_id(tenant_id, f"context:{signal.signal_id}")
    await barrier.record_context_decision(
        tx=conn,
        item=ContextDecision(
            decision_id=decision_id,
            tenant_id=tenant_id,
            batch_id=f"p5-batch-{signal.batch_number}",
            route_id=(
                "source-semantic-admission"
                if signal.batch_number == 1
                else "accepted-memory-first"
            ),
            context_item_kind=context_item_kind,  # type: ignore[arg-type]
            context_item_id=context_item_id,
            context_item_version=context_item_version,
            retrieved=True,
            selected=True,
            included=True,
            referenced=True,
            counterevidence_retained=outcome_kind == "correction",
            confidence_affecting=True,
            necessary_background=False,
            historical_reopen_reason=None,
            decision_fate="mutation",
            result_object_kind=result_object_kind,
            result_object_id=result_object_id,
            evidence_lineage=lineage,
            decided_at=_occurred_at(signal) + timedelta(minutes=2),
        ),
    )
    if outcome_kind is not None:
        await barrier.record_outcome(
            tx=conn,
            item=OutcomeLink(
                outcome_link_id=_stable_id(
                    tenant_id,
                    f"outcome:{signal.signal_id}:{outcome_kind}",
                ),
                tenant_id=tenant_id,
                decision_id=decision_id,
                outcome_kind=outcome_kind,  # type: ignore[arg-type]
                outcome_object_kind=result_object_kind,
                outcome_object_id=result_object_id,
                attribution_basis="direct",
                evidence_lineage=lineage,
                observed_at=_occurred_at(signal) + timedelta(minutes=2),
            ),
        )
    return decision_id


def _signal_receipt(
    *,
    tenant_id: UUID,
    signal: P5Signal,
    observation_id: UUID,
    decision_fate: str,
    grounding_fate: str | None = None,
    semantic_disposition: str | None = None,
) -> P5SignalReceipt:
    route_id = (
        "source-semantic-admission"
        if decision_fate == "mutation" and signal.batch_number == 1
        else "accepted-memory-first"
        if decision_fate == "mutation"
        else "source-semantic-eligibility-v1"
    )
    digest = canonical_sha256(signal.text)
    return P5SignalReceipt(
        signal_id=signal.signal_id,
        batch_number=signal.batch_number,
        position=signal.position,
        episode_id=signal.episode_id,
        observation_id=str(observation_id),
        sealed_content_digest=digest,
        persisted_content_digest=digest,
        persisted=True,
        decision_id=str(_stable_id(tenant_id, f"context:{signal.signal_id}")),
        route_id=route_id,
        decision_fate=decision_fate,  # type: ignore[arg-type]
        grounding_fate=grounding_fate,
        source_semantic_disposition=semantic_disposition,
    )


async def _complete_barrier(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    batch_number: int,
    expected_models: tuple[UUID, ...],
    expected_relations: tuple[UUID, ...] = (),
    invalidated_models: tuple[UUID, ...] = (),
    service: CompanyLearningBarrierService,
) -> P5BarrierReceipt:
    receipt = await service.complete(
        tx=conn,
        barrier_id=_stable_id(tenant_id, f"barrier:{batch_number}"),
        tenant_id=tenant_id,
        batch_id=f"p5-batch-{batch_number}",
        expected_model_version_ids=expected_models,
        expected_relation_version_ids=expected_relations,
        invalidated_model_version_ids=invalidated_models,
        truth_critical_pending_count=0,
        completed_at=P5_NOW + timedelta(days=batch_number, hours=1),
    )
    return P5BarrierReceipt(
        batch_id=receipt.batch_id,
        barrier_id=str(receipt.barrier_id),
        barrier_version=receipt.barrier_version,
        expected_model_version_count=len(receipt.expected_model_version_ids),
        expected_relation_version_count=len(receipt.expected_relation_version_ids),
        invalidated_model_version_count=len(receipt.invalidated_model_version_ids),
        truth_critical_pending_count=receipt.truth_critical_pending_count,
        receipt_digest=receipt.receipt_digest,
    )


async def run_p5_vertical(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    population: P5Population | None = None,
) -> P5Artifact:
    """Run the sealed vertical in the caller's transaction."""

    population = population or build_p5_population()
    await conn.execute(
        "INSERT INTO tenants (id, name, is_demo) VALUES ($1,'p5-zero-seed',FALSE)",
        tenant_id,
    )
    other_tenant_id = _stable_id(tenant_id, "other-tenant")
    await conn.execute(
        "INSERT INTO tenants (id, name, is_demo) VALUES ($1,'p5-other-tenant',FALSE)",
        other_tenant_id,
    )
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, content,
          content_text, embedding_pending, trust_tier, entities_mentioned
        ) VALUES ($1,$2,$3,'signal','email:message',$4::jsonb,$5,TRUE,'ordinary','[]'::jsonb)
        """,
        _stable_id(other_tenant_id, "decoy-observation"),
        other_tenant_id,
        P5_NOW,
        json.dumps({"text": "A separate tenant scheduled its own planning call."}),
        "A separate tenant scheduled its own planning call.",
    )
    zero_seed_models = await conn.fetchval(
        "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1",
        tenant_id,
    )
    zero_seed_relations = await conn.fetchval(
        "SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1",
        tenant_id,
    )
    if zero_seed_relations != 0:
        raise AssertionError("P5 tenant unexpectedly contains seeded accepted relations")

    processor = GroundedBeliefProcessor()
    models = ModelsRepo(pool=None, embedder=None, run_topology_on_insert=False)  # type: ignore[arg-type]
    barrier = CompanyLearningBarrierService()
    signal_receipts: list[P5SignalReceipt] = []
    barrier_receipts: list[P5BarrierReceipt] = []
    timings: dict[str, float] = {}

    # Batch 1: persist 25 signals, use one central signal to admit one atomic Model.
    started = time.perf_counter()
    batch_1 = population.batches[0]
    observations_1 = await _persist_batch(conn, tenant_id=tenant_id, batch=batch_1)
    eligible_1 = tuple(item for item in batch_1.signals if _semantic_surface(item))
    if len(eligible_1) != 1:
        raise AssertionError("Batch 1 runtime eligibility must select exactly one report")
    target_1 = eligible_1[0]
    grounding_1, semantic_1, model_1, version_1, atomic_1 = await _ground_and_admit(
        conn,
        tenant_id=tenant_id,
        signal=target_1,
        observation_id=observations_1[target_1.signal_id],
        processor=processor,
    )
    for signal in batch_1.signals:
        observation_id = observations_1[signal.signal_id]
        if signal is target_1:
            await _record_semantic_decision(
                conn,
                tenant_id=tenant_id,
                signal=signal,
                context_item_kind="current_episode",
                context_item_id=str(observation_id),
                context_item_version="1",
                result_object_kind="model_version",
                result_object_id=version_1,
                lineage=(
                    {"kind": "observation", "id": str(observation_id)},
                    {"kind": "model_version", "id": str(version_1)},
                ),
                barrier=barrier,
            )
            signal_receipts.append(_signal_receipt(
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                decision_fate="mutation",
                grounding_fate=grounding_1,
                semantic_disposition=semantic_1,
            ))
        else:
            await _record_unused_decision(
                conn,
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                barrier=barrier,
            )
            signal_receipts.append(_signal_receipt(
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                decision_fate="validator_drop",
            ))
    barrier_receipts.append(await _complete_barrier(
        conn,
        tenant_id=tenant_id,
        batch_number=1,
        expected_models=(version_1,),
        service=barrier,
    ))
    timings["batch_1"] = (time.perf_counter() - started) * 1000

    # Batch 2: accepted retrieval precedes source processing and relation admission.
    started = time.perf_counter()
    batch_2 = population.batches[1]
    observations_2 = await _persist_batch(conn, tenant_id=tenant_id, batch=batch_2)
    retrieved_prior = await models.retrieve((model_1,), conn=conn)
    prior_retrieved = len(retrieved_prior) == 1 and retrieved_prior[0].id == model_1
    eligible_2 = tuple(item for item in batch_2.signals if _semantic_surface(item))
    if len(eligible_2) != 1:
        raise AssertionError("Batch 2 runtime eligibility must select exactly one report")
    target_2 = eligible_2[0]
    grounding_2, semantic_2, model_2, version_2, _ = await _ground_and_admit(
        conn,
        tenant_id=tenant_id,
        signal=target_2,
        observation_id=observations_2[target_2.signal_id],
        processor=processor,
    )
    relation_id, relation_version_id = await _admit_relation(
        conn,
        tenant_id=tenant_id,
        old_model_id=model_1,
        old_version_id=version_1,
        new_model_id=model_2,
        new_version_id=version_2,
        issued_at=_occurred_at(target_2) + timedelta(minutes=1),
    )
    for signal in batch_2.signals:
        observation_id = observations_2[signal.signal_id]
        if signal is target_2:
            await _record_semantic_decision(
                conn,
                tenant_id=tenant_id,
                signal=signal,
                context_item_kind="accepted_model",
                context_item_id=str(model_1),
                context_item_version=str(version_1),
                result_object_kind="relation_version",
                result_object_id=relation_version_id,
                lineage=(
                    {"kind": "model_version", "id": str(version_1)},
                    {"kind": "observation", "id": str(observation_id)},
                    {"kind": "model_version", "id": str(version_2)},
                    {"kind": "relation_version", "id": str(relation_version_id)},
                ),
                barrier=barrier,
                outcome_kind="confirmation",
            )
            signal_receipts.append(_signal_receipt(
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                decision_fate="mutation",
                grounding_fate=grounding_2,
                semantic_disposition=semantic_2,
            ))
        else:
            await _record_unused_decision(
                conn,
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                barrier=barrier,
            )
            signal_receipts.append(_signal_receipt(
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                decision_fate="validator_drop",
            ))
    barrier_receipts.append(await _complete_barrier(
        conn,
        tenant_id=tenant_id,
        batch_number=2,
        expected_models=(version_1, version_2),
        expected_relations=(relation_version_id,),
        service=barrier,
    ))
    timings["batch_2"] = (time.perf_counter() - started) * 1000

    # Batch 3: corrected source truth, exact falsification, relation fence, reuse.
    started = time.perf_counter()
    batch_3 = population.batches[2]
    observations_3 = await _persist_batch(conn, tenant_id=tenant_id, batch=batch_3)
    eligible_3 = tuple(item for item in batch_3.signals if _semantic_surface(item))
    if len(eligible_3) != 1:
        raise AssertionError("Batch 3 runtime eligibility must select exactly one report")
    target_3 = eligible_3[0]
    grounding_3, semantic_3, corrected_model, corrected_version_id, _ = (
        await _ground_and_admit(
            conn,
            tenant_id=tenant_id,
            signal=target_3,
            observation_id=observations_3[target_3.signal_id],
            processor=processor,
        )
    )
    prior_version = await _load_model_version(
        conn,
        tenant_id=tenant_id,
        version_id=version_1,
    )
    corrected_version = await _load_model_version(
        conn,
        tenant_id=tenant_id,
        version_id=corrected_version_id,
    )
    terminal_version = await _falsify_exact_model(
        conn,
        tenant_id=tenant_id,
        prior=prior_version,
        corrected=corrected_version,
        at=_occurred_at(target_3) + timedelta(minutes=1),
    )
    stale_model_count = await conn.fetchval(
        """
        SELECT count(*) FROM accepted_current_models
        WHERE tenant_id=$1 AND (id=$2 OR truth_version_id=$3)
        """,
        tenant_id,
        model_1,
        version_1,
    )
    stale_relation_count = await conn.fetchval(
        """
        SELECT count(*) FROM accepted_current_relations
        WHERE tenant_id=$1 AND id=$2
        """,
        tenant_id,
        relation_id,
    )
    corrected_rows = await models.retrieve((corrected_model,), conn=conn)
    corrected_retrieved = (
        len(corrected_rows) == 1 and corrected_rows[0].id == corrected_model
    )
    repair_obligations = await conn.fetchval(
        """
        SELECT count(*) FROM truth_repair_obligations
        WHERE tenant_id=$1 AND invalidated_model_version_id=$2
          AND affected_kind='relation_version'
        """,
        tenant_id,
        version_1,
    )
    for signal in batch_3.signals:
        observation_id = observations_3[signal.signal_id]
        if signal is target_3:
            await _record_semantic_decision(
                conn,
                tenant_id=tenant_id,
                signal=signal,
                context_item_kind="accepted_model",
                context_item_id=str(corrected_model),
                context_item_version=str(corrected_version_id),
                result_object_kind="model_version",
                result_object_id=corrected_version_id,
                lineage=(
                    {"kind": "observation", "id": str(observation_id)},
                    {"kind": "invalidated_model_version", "id": str(version_1)},
                    {"kind": "corrected_model_version", "id": str(corrected_version_id)},
                ),
                barrier=barrier,
                outcome_kind="correction",
            )
            signal_receipts.append(_signal_receipt(
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                decision_fate="mutation",
                grounding_fate=grounding_3,
                semantic_disposition=semantic_3,
            ))
        else:
            await _record_unused_decision(
                conn,
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                barrier=barrier,
            )
            signal_receipts.append(_signal_receipt(
                tenant_id=tenant_id,
                signal=signal,
                observation_id=observation_id,
                decision_fate="validator_drop",
            ))
    barrier_receipts.append(await _complete_barrier(
        conn,
        tenant_id=tenant_id,
        batch_number=3,
        expected_models=(version_2, corrected_version_id),
        invalidated_models=(version_1,),
        service=barrier,
    ))
    timings["batch_3"] = (time.perf_counter() - started) * 1000

    expected_observation_ids = {
        signal.signal_id: _stable_id(tenant_id, f"observation:{signal.signal_id}")
        for signal in population.signals
    }
    observation_rows = await conn.fetch(
        """
        SELECT id, source_channel, content_text
        FROM observations
        WHERE tenant_id=$1 AND id=ANY($2::uuid[])
        ORDER BY id
        """,
        tenant_id,
        list(expected_observation_ids.values()),
    )
    observation_by_id = {row["id"]: row for row in observation_rows}
    signal_rows = [
        {
            "signal_id": signal.signal_id,
            "observation_id": str(expected_observation_ids[signal.signal_id]),
            "source_channel": observation_by_id[
                expected_observation_ids[signal.signal_id]
            ]["source_channel"],
            "content_digest": canonical_sha256(
                observation_by_id[expected_observation_ids[signal.signal_id]][
                    "content_text"
                ]
            ),
        }
        for signal in population.signals
        if expected_observation_ids[signal.signal_id] in observation_by_id
    ]
    expected_decision_ids = {
        signal.signal_id: _stable_id(tenant_id, f"context:{signal.signal_id}")
        for signal in population.signals
    }
    context_rows = await conn.fetch(
        """
        SELECT decision_id, route_id, context_item_id, decision_fate,
               result_object_kind, result_object_id, referenced
        FROM company_learning_context_decisions
        WHERE tenant_id=$1 AND decision_id=ANY($2::uuid[])
        ORDER BY decision_id
        """,
        tenant_id,
        list(expected_decision_ids.values()),
    )
    context_by_id = {row["decision_id"]: row for row in context_rows}
    decision_rows = [
        {
            "signal_id": signal.signal_id,
            "decision_id": str(expected_decision_ids[signal.signal_id]),
            "route_id": context_by_id[expected_decision_ids[signal.signal_id]][
                "route_id"
            ],
            "context_item_id": context_by_id[
                expected_decision_ids[signal.signal_id]
            ]["context_item_id"],
            "decision_fate": context_by_id[
                expected_decision_ids[signal.signal_id]
            ]["decision_fate"],
            "result_object_kind": context_by_id[
                expected_decision_ids[signal.signal_id]
            ]["result_object_kind"],
            "result_object_id": (
                str(context_by_id[expected_decision_ids[signal.signal_id]][
                    "result_object_id"
                ])
                if context_by_id[expected_decision_ids[signal.signal_id]][
                    "result_object_id"
                ]
                else None
            ),
            "referenced": bool(
                context_by_id[expected_decision_ids[signal.signal_id]]["referenced"]
            ),
        }
        for signal in population.signals
        if expected_decision_ids[signal.signal_id] in context_by_id
    ]
    signal_id_by_observation = {
        str(observation_id): signal_id
        for signal_id, observation_id in expected_observation_ids.items()
    }
    raw_semantic_rows = await conn.fetch(
        """
        SELECT trace.source_observation_id, trace.current_fate,
               admission.disposition, admission.admitted_model_id
        FROM grounding_traces trace
        JOIN source_semantic_interpretations interpretation
          ON interpretation.tenant_id=trace.tenant_id
         AND interpretation.grounding_trace_id=trace.id
        JOIN source_semantic_admission_decisions admission
          ON admission.tenant_id=interpretation.tenant_id
         AND admission.interpretation_id=interpretation.id
        WHERE trace.tenant_id=$1
        ORDER BY trace.source_observation_id
        """,
        tenant_id,
    )
    semantic_rows = [
        {
            "signal_id": signal_id_by_observation[str(row["source_observation_id"])],
            "observation_id": str(row["source_observation_id"]),
            "grounding_fate": row["current_fate"],
            "source_semantic_disposition": row["disposition"],
            "model_id": str(row["admitted_model_id"]),
        }
        for row in raw_semantic_rows
    ]
    raw_model_versions = await conn.fetch(
        """
        SELECT version_id, model_id, version, lifecycle, semantic_digest,
               supersedes_version_id
        FROM model_truth_versions
        WHERE tenant_id=$1
        ORDER BY model_id, version
        """,
        tenant_id,
    )
    model_version_rows = [
        {
            "version_id": str(row["version_id"]),
            "model_id": str(row["model_id"]),
            "version": int(row["version"]),
            "lifecycle": row["lifecycle"],
            "semantic_digest": row["semantic_digest"],
            "supersedes_version_id": (
                str(row["supersedes_version_id"])
                if row["supersedes_version_id"]
                else None
            ),
        }
        for row in raw_model_versions
    ]
    accepted_model_version_ids = [
        str(row["truth_version_id"])
        for row in await conn.fetch(
            """
            SELECT truth_version_id FROM accepted_current_models
            WHERE tenant_id=$1 ORDER BY truth_version_id
            """,
            tenant_id,
        )
    ]
    relation_head_rows = [
        {
            "relation_id": str(row["relation_id"]),
            "relation_version_id": str(row["relation_version_id"]),
            "version": int(row["version"]),
            "lifecycle": row["lifecycle"],
            "semantic_digest": row["semantic_digest"],
        }
        for row in await conn.fetch(
            """
            SELECT relation_id, relation_version_id, version, lifecycle,
                   semantic_digest
            FROM relation_truth_heads
            WHERE tenant_id=$1 ORDER BY relation_id
            """,
            tenant_id,
        )
    ]
    repair_obligation_rows = [
        {
            "invalidated_model_version_id": str(
                row["invalidated_model_version_id"]
            ),
            "affected_kind": row["affected_kind"],
            "affected_id": str(row["affected_id"]),
            "cause_code": row["cause_code"],
            "status": row["status"],
        }
        for row in await conn.fetch(
            """
            SELECT invalidated_model_version_id, affected_kind, affected_id,
                   cause_code, status
            FROM truth_repair_obligations
            WHERE tenant_id=$1
            ORDER BY invalidated_model_version_id, affected_kind, affected_id,
                     cause_code
            """,
            tenant_id,
        )
    ]
    barrier_rows = [
        {
            "batch_id": row["batch_id"],
            "barrier_id": str(row["barrier_id"]),
            "barrier_version": int(row["barrier_version"]),
            "truth_critical_pending_count": int(
                row["truth_critical_pending_count"]
            ),
            "receipt_digest": row["receipt_digest"],
        }
        for row in await conn.fetch(
            """
            SELECT batch_id, barrier_id, barrier_version,
                   truth_critical_pending_count, receipt_digest
            FROM company_learning_barriers
            WHERE tenant_id=$1 ORDER BY barrier_version
            """,
            tenant_id,
        )
    ]
    activity_rows = {
        str(row["model_id"]): int(row["retrieval_count"])
        for row in await conn.fetch(
            """
            SELECT model_id, retrieval_count FROM model_activity_sidecar
            WHERE tenant_id=$1 ORDER BY model_id
            """,
            tenant_id,
        )
    }
    database_evidence = {
        "preflight": {
            "accepted_model_count": int(zero_seed_models),
            "accepted_relation_count": int(zero_seed_relations),
        },
        "signal_rows": signal_rows,
        "decision_rows": decision_rows,
        "semantic_rows": semantic_rows,
        "model_version_rows": model_version_rows,
        "accepted_model_version_ids": accepted_model_version_ids,
        "relation_head_rows": relation_head_rows,
        "repair_obligation_rows": repair_obligation_rows,
        "barrier_rows": barrier_rows,
        "activity_retrieval_counts": activity_rows,
        "observation_count": await conn.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1", tenant_id
        ),
        "grounding_trace_count": await conn.fetchval(
            "SELECT count(*) FROM grounding_traces WHERE tenant_id=$1", tenant_id
        ),
        "source_semantic_interpretation_count": await conn.fetchval(
            "SELECT count(*) FROM source_semantic_interpretations WHERE tenant_id=$1",
            tenant_id,
        ),
        "source_semantic_belief_admission_count": await conn.fetchval(
            """
            SELECT count(*) FROM source_semantic_admission_decisions
            WHERE tenant_id=$1 AND disposition='belief_applied'
            """,
            tenant_id,
        ),
        "model_truth_version_count": await conn.fetchval(
            "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1", tenant_id
        ),
        "accepted_model_count": await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1", tenant_id
        ),
        "accepted_relation_count": await conn.fetchval(
            "SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1",
            tenant_id,
        ),
        "context_decision_count": await conn.fetchval(
            "SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1",
            tenant_id,
        ),
        "outcome_link_count": await conn.fetchval(
            "SELECT count(*) FROM company_learning_outcome_links WHERE tenant_id=$1",
            tenant_id,
        ),
        "barrier_count": await conn.fetchval(
            "SELECT count(*) FROM company_learning_barriers WHERE tenant_id=$1",
            tenant_id,
        ),
        "repair_obligation_count": repair_obligations,
        "accepted_object_count": await conn.fetchval(
            """
            SELECT (SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1)
                 + (SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1)
            """,
            tenant_id,
        ),
        "cross_tenant_contamination_count": await conn.fetchval(
            """
            SELECT count(*) FROM company_learning_context_decisions
            WHERE tenant_id=$1
              AND context_item_id=ANY($2::text[])
            """,
            other_tenant_id,
            [str(model_1), str(model_2), str(corrected_model)],
        ),
        "zero_seed_initial_relation_count": zero_seed_relations,
    }
    vertical = P5VerticalReceipt(
        batch_1_model_id=str(model_1),
        batch_1_model_version_id=str(version_1),
        batch_1_atomic=(
            atomic_1
            and model_version_rows[0].get("version_id") in {
                str(version_1), str(version_2), str(corrected_version_id)
            }
        ),
        batch_2_model_id=str(model_2),
        batch_2_model_version_id=str(version_2),
        batch_2_prior_retrieved=(
            prior_retrieved and activity_rows.get(str(model_1), 0) >= 1
        ),
        batch_2_prior_referenced=any(
            row["signal_id"] == target_2.signal_id
            and row["context_item_id"] == str(model_1)
            and row["referenced"]
            and row["result_object_id"] == str(relation_version_id)
            for row in decision_rows
        ),
        relation_disposition=(
            "accepted"
            if relation_head_rows and relation_head_rows[0]["relation_id"] == str(relation_id)
            else "justified_no_relation"
        ),
        relation_kind=(
            _RELATION_KIND.value if relation_head_rows else None
        ),
        relation_id=str(relation_id) if relation_head_rows else None,
        relation_version_id=(str(relation_version_id) if relation_head_rows else None),
        no_relation_reason=None,
        batch_3_corrected_model_id=str(corrected_model),
        batch_3_corrected_model_version_id=str(corrected_version_id),
        batch_3_corrected_retrieved=(
            corrected_retrieved and activity_rows.get(str(corrected_model), 0) >= 1
        ),
        batch_3_corrected_referenced=any(
            row["signal_id"] == target_3.signal_id
            and row["context_item_id"] == str(corrected_model)
            and row["referenced"]
            and row["result_object_id"] == str(corrected_version_id)
            for row in decision_rows
        ),
        invalidated_model_id=str(model_1),
        invalidated_model_version_id=str(version_1),
        terminal_lifecycle=next(
            row["lifecycle"]
            for row in model_version_rows
            if row["version_id"] == str(terminal_version.version_id)
        ),
        stale_model_excluded=(
            stale_model_count == 0 and str(version_1) not in accepted_model_version_ids
        ),
        stale_relation_excluded=(
            stale_relation_count == 0
            and database_evidence["accepted_relation_count"] == 0
        ),
        relation_repair_obligation_count=len(repair_obligation_rows),
    )
    return build_p5_artifact(
        population=population,
        signals=tuple(signal_receipts),
        vertical=vertical,
        barriers=tuple(barrier_receipts),
        zero_seed_initial_model_count=int(zero_seed_models),
        provider_call_count=0,
        database_evidence=database_evidence,
        timings_ms=timings,
    )


def write_p5_artifact(artifact: P5Artifact, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(artifact.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
    )


def write_p5_schema(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(P5Artifact.model_json_schema(), indent=2, sort_keys=True) + "\n"
    )


__all__ = [
    "P5_NOW",
    "run_p5_vertical",
    "write_p5_artifact",
    "write_p5_schema",
]
