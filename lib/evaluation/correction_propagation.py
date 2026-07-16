"""Read-only audit of downstream state after an entity-grounding correction."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.kernel import canonical_sha256


class _AuditModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class CorrectionDependencyKind(StrEnum):
    SEMANTIC_INTERPRETATION = "semantic_interpretation"
    SEMANTIC_ADMISSION = "semantic_admission"
    MODEL = "model"
    MODEL_EDGE = "model_edge"
    RELATION_INSTANCE = "relation_instance"
    MODEL_BELIEF_ADDRESS = "model_belief_address"
    PROJECTION_SNAPSHOT = "projection_snapshot"
    PROJECTION_DEPENDENCY = "projection_dependency"
    PROJECTION_REFRESH_JOB = "projection_refresh_job"


class CorrectionPropagationScope(_AuditModel):
    tenant_id: UUID
    predecessor_grounding_trace_id: UUID
    run_id: str = Field(min_length=1)
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def observed_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value


class CorrectionDependencyRecord(_AuditModel):
    kind: CorrectionDependencyKind
    object_ref: str = Field(min_length=1)
    dependency_basis: tuple[str, ...] = Field(min_length=1)
    lifecycle_state: str = Field(min_length=1)
    repair_required: bool
    read_surface: bool
    fenced: bool = False
    repaired_or_superseded: bool = False
    unsafe_readable: bool = False
    repair_pending: bool = False
    details: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def state_is_coherent(self) -> Self:
        if self.unsafe_readable and not self.read_surface:
            raise ValueError("unsafe-readable dependencies must be read surfaces")
        if self.unsafe_readable and (self.fenced or self.repaired_or_superseded):
            raise ValueError(
                "unsafe-readable dependency cannot also be fenced or repaired"
            )
        if (self.fenced or self.repaired_or_superseded) and not self.repair_required:
            raise ValueError(
                "historical/non-repair records cannot claim repair completion"
            )
        return self


class CorrectionPropagationAudit(_AuditModel):
    scope: CorrectionPropagationScope
    correction_grounding_trace_id: UUID | None
    source_observation_id: UUID | None
    correction_found: bool
    correction_changes_referent: bool | None
    discovered_dependency_count: int = Field(ge=0)
    component_counts: dict[str, int]
    repair_required_dependency_count: int = Field(ge=0)
    fenced_dependency_count: int = Field(ge=0)
    repaired_or_superseded_count: int = Field(ge=0)
    unsafe_readable_count: int = Field(ge=0)
    repair_pending_count: int = Field(ge=0)
    residual_repair_debt_count: int = Field(ge=0)
    convergence_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    safe_containment_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    source_hash_reference_count: int = Field(ge=0)
    source_hash_match_count: int = Field(ge=0)
    source_immutable: bool | None
    audit_read_only: bool
    cross_tenant_reference_count: int = Field(ge=0)
    cross_tenant_change_count: int = Field(ge=0)
    dependencies: tuple[CorrectionDependencyRecord, ...]
    incidents: tuple[str, ...]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def converged(self) -> bool:
        return (
            self.correction_found
            and self.source_immutable is True
            and self.cross_tenant_reference_count == 0
            and self.cross_tenant_change_count == 0
            and self.unsafe_readable_count == 0
            and self.residual_repair_debt_count == 0
        )


async def evaluate_correction_propagation(
    conn: asyncpg.Connection,
    *,
    scope: CorrectionPropagationScope,
    artifact_refs: tuple[str, ...],
) -> CorrectionPropagationAudit:
    """Census exact descendants of one superseded grounding trace."""

    root = await conn.fetchrow(
        """
        SELECT predecessor.*,
               observation.content_text AS observation_content_text,
               detection.source_content_hash AS detection_source_content_hash,
               successor.id AS successor_id,
               successor.source_observation_id AS successor_source_observation_id,
               successor.selected_referent AS successor_selected_referent,
               successor.source_observation_mutated AS successor_source_mutated,
               successor.trace AS successor_trace,
               successor.created_at AS successor_created_at
        FROM grounding_traces predecessor
        JOIN observations observation
          ON observation.tenant_id=predecessor.tenant_id
         AND observation.id=predecessor.source_observation_id
        LEFT JOIN entity_mention_detections detection
          ON detection.tenant_id=predecessor.tenant_id
         AND detection.id=predecessor.entity_mention_detection_id
        LEFT JOIN LATERAL (
          SELECT candidate.*
          FROM grounding_traces candidate
          WHERE candidate.tenant_id=predecessor.tenant_id
            AND candidate.trace ->> 'supersedes_grounding_trace_id'
                = predecessor.id::text
            AND candidate.trace ->> 'correction_kind'
                = 'entity_clarification_adjudication'
          ORDER BY candidate.created_at DESC, candidate.id DESC
          LIMIT 1
        ) successor ON TRUE
        WHERE predecessor.tenant_id=$1 AND predecessor.id=$2
        """,
        scope.tenant_id,
        scope.predecessor_grounding_trace_id,
    )
    if root is None:
        return analyze_correction_propagation_rows(
            scope=scope,
            root=None,
            interpretations=(),
            models=(),
            edges=(),
            relations=(),
            belief_addresses=(),
            projection_snapshots=(),
            projection_dependencies=(),
            projection_refresh_jobs=(),
            cross_tenant_reference_count=0,
            artifact_refs=artifact_refs,
        )

    interpretations = await conn.fetch(
        """
        SELECT interpretation.*,
               admission.id AS admission_id,
               admission.disposition,
               admission.admitted_model_id,
               admission.decided_at AS admission_decided_at
        FROM source_semantic_interpretations interpretation
        LEFT JOIN source_semantic_admission_decisions admission
          ON admission.tenant_id=interpretation.tenant_id
         AND admission.interpretation_id=interpretation.id
        WHERE interpretation.tenant_id=$1
          AND interpretation.grounding_trace_id=$2
        ORDER BY interpretation.recorded_at, interpretation.id
        """,
        scope.tenant_id,
        scope.predecessor_grounding_trace_id,
    )
    model_ids = tuple(
        dict.fromkeys(
            row["admitted_model_id"]
            for row in interpretations
            if row["admitted_model_id"] is not None
        )
    )
    models: Sequence[Mapping[str, Any]] = ()
    edges: Sequence[Mapping[str, Any]] = ()
    relations: Sequence[Mapping[str, Any]] = ()
    belief_addresses: Sequence[Mapping[str, Any]] = ()
    projection_snapshots: Sequence[Mapping[str, Any]] = ()
    projection_dependencies: Sequence[Mapping[str, Any]] = ()
    projection_refresh_jobs: Sequence[Mapping[str, Any]] = ()
    cross_tenant_reference_count = 0
    if model_ids:
        models = await conn.fetch(
            """
            SELECT id, tenant_id, born_from_event_id, status, archived_at,
                   archive_reason, visible_to_subjects, supporting_event_ids,
                   supporting_model_ids, created_at
            FROM models
            WHERE tenant_id=$1 AND id=ANY($2::uuid[])
            ORDER BY created_at, id
            """,
            scope.tenant_id,
            list(model_ids),
        )
        model_events = await conn.fetch(
            """
            SELECT id, model_id, event_type, created_at
            FROM model_events
            WHERE tenant_id=$1 AND model_id=ANY($2::uuid[])
            ORDER BY created_at, id
            """,
            scope.tenant_id,
            list(model_ids),
        )
        model_event_ids = tuple(row["id"] for row in model_events)
        edges = await conn.fetch(
            """
            SELECT *
            FROM model_edges
            WHERE tenant_id=$1
              AND (
                source_model_id=ANY($2::uuid[])
                OR target_model_id=ANY($2::uuid[])
              )
            ORDER BY created_at, id
            """,
            scope.tenant_id,
            list(model_ids),
        )
        relations = await conn.fetch(
            """
            SELECT DISTINCT relation.*
            FROM relation_instances relation
            LEFT JOIN relation_participants participant
              ON participant.tenant_id=relation.tenant_id
             AND participant.relation_id=relation.id
            WHERE relation.tenant_id=$1
              AND (
                relation.evidence_model_ids && $2::uuid[]
                OR participant.model_id=ANY($2::uuid[])
              )
            ORDER BY relation.created_at, relation.id
            """,
            scope.tenant_id,
            list(model_ids),
        )
        belief_addresses = await conn.fetch(
            """
            SELECT address.*, model.status AS model_status,
                   model.visible_to_subjects
            FROM model_belief_addresses address
            JOIN models model
              ON model.tenant_id=address.tenant_id
             AND model.id=address.model_id
            WHERE address.tenant_id=$1
              AND address.model_id=ANY($2::uuid[])
            ORDER BY address.model_id
            """,
            scope.tenant_id,
            list(model_ids),
        )
        projection_snapshots = await conn.fetch(
            """
            SELECT *
            FROM projection_snapshots
            WHERE tenant_id=$1
              AND (
                source_model_ids && $2::uuid[]
                OR (
                  cardinality($3::uuid[]) > 0
                  AND source_event_ids && $3::uuid[]
                )
              )
            ORDER BY projection_name, projection_version, subject_key
            """,
            scope.tenant_id,
            list(model_ids),
            list(model_event_ids),
        )
        projection_dependencies = await conn.fetch(
            """
            SELECT *
            FROM projection_dependencies
            WHERE tenant_id=$1
              AND (
                (ref_kind='model' AND ref_value=ANY($2::text[]))
                OR (
                  ref_kind='model_event'
                  AND ref_value=ANY($3::text[])
                )
              )
            ORDER BY projection_name, projection_version, subject_key,
                     ref_kind, ref_value
            """,
            scope.tenant_id,
            [str(value) for value in model_ids],
            [str(value) for value in model_event_ids],
        )
        projection_keys = {
            (
                str(row["projection_name"]),
                str(row["projection_version"]),
                str(row["subject_key"]),
            )
            for row in (*projection_snapshots, *projection_dependencies)
        }
        all_refresh_jobs = await conn.fetch(
            """
            SELECT *
            FROM projection_refresh_jobs
            WHERE tenant_id=$1
            ORDER BY created_at, id
            """,
            scope.tenant_id,
        )
        projection_refresh_jobs = tuple(
            row
            for row in all_refresh_jobs
            if _refresh_job_matches(
                row,
                model_ids=model_ids,
                model_event_ids=model_event_ids,
                projection_keys=projection_keys,
            )
        )
        cross_tenant_reference_count = await _cross_tenant_reference_count(
            conn,
            tenant_id=scope.tenant_id,
            model_ids=model_ids,
            model_event_ids=model_event_ids,
        )
    return analyze_correction_propagation_rows(
        scope=scope,
        root=root,
        interpretations=interpretations,
        models=models,
        edges=edges,
        relations=relations,
        belief_addresses=belief_addresses,
        projection_snapshots=projection_snapshots,
        projection_dependencies=projection_dependencies,
        projection_refresh_jobs=projection_refresh_jobs,
        cross_tenant_reference_count=cross_tenant_reference_count,
        artifact_refs=artifact_refs,
    )


def analyze_correction_propagation_rows(
    *,
    scope: CorrectionPropagationScope,
    root: Mapping[str, Any] | None,
    interpretations: Sequence[Mapping[str, Any]],
    models: Sequence[Mapping[str, Any]],
    edges: Sequence[Mapping[str, Any]],
    relations: Sequence[Mapping[str, Any]],
    belief_addresses: Sequence[Mapping[str, Any]],
    projection_snapshots: Sequence[Mapping[str, Any]],
    projection_dependencies: Sequence[Mapping[str, Any]],
    projection_refresh_jobs: Sequence[Mapping[str, Any]],
    cross_tenant_reference_count: int,
    artifact_refs: tuple[str, ...],
) -> CorrectionPropagationAudit:
    """Pure analysis seam used by unit tests and the live DB evaluator."""

    if root is None:
        return CorrectionPropagationAudit(
            scope=scope,
            correction_grounding_trace_id=None,
            source_observation_id=None,
            correction_found=False,
            correction_changes_referent=None,
            discovered_dependency_count=0,
            component_counts={},
            repair_required_dependency_count=0,
            fenced_dependency_count=0,
            repaired_or_superseded_count=0,
            unsafe_readable_count=0,
            repair_pending_count=0,
            residual_repair_debt_count=0,
            convergence_ratio=None,
            safe_containment_ratio=None,
            source_hash_reference_count=0,
            source_hash_match_count=0,
            source_immutable=None,
            audit_read_only=True,
            cross_tenant_reference_count=0,
            cross_tenant_change_count=0,
            dependencies=(),
            incidents=("predecessor_grounding_trace_missing",),
            uncertainty=(
                "No correction-propagation census is possible without the exact "
                "tenant-scoped predecessor grounding trace.",
            ),
            artifact_refs=artifact_refs,
        )

    successor_trace = _json_obj(root.get("successor_trace"))
    successor_id = _uuid_or_none(root.get("successor_id"))
    source_observation_id = _uuid_or_none(root.get("source_observation_id"))
    correction_found = bool(
        successor_id
        and successor_trace.get("supersedes_grounding_trace_id")
        == str(scope.predecessor_grounding_trace_id)
        and successor_trace.get("correction_kind")
        == "entity_clarification_adjudication"
        and str(root.get("successor_source_observation_id"))
        == str(source_observation_id)
    )
    correction_changes_referent = (
        _json_obj(root.get("selected_referent"))
        != _json_obj(root.get("successor_selected_referent"))
        if correction_found
        else None
    )
    dependencies: list[CorrectionDependencyRecord] = []
    for row in interpretations:
        interpretation_id = str(row["id"])
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.SEMANTIC_INTERPRETATION,
                object_ref=f"source-semantic-interpretation:{interpretation_id}",
                dependency_basis=(
                    f"grounding-trace:{scope.predecessor_grounding_trace_id}",
                ),
                lifecycle_state="append_only_history",
                repair_required=False,
                read_surface=False,
                details={"source_content_hash": row.get("source_content_hash")},
            )
        )
        if row.get("admission_id") is not None:
            dependencies.append(
                CorrectionDependencyRecord(
                    kind=CorrectionDependencyKind.SEMANTIC_ADMISSION,
                    object_ref=f"source-semantic-admission:{row['admission_id']}",
                    dependency_basis=(
                        f"source-semantic-interpretation:{interpretation_id}",
                    ),
                    lifecycle_state="append_only_history",
                    repair_required=False,
                    read_surface=False,
                    details={
                        "disposition": row.get("disposition"),
                        "admitted_model_id": (
                            str(row["admitted_model_id"])
                            if row.get("admitted_model_id")
                            else None
                        ),
                    },
                )
            )
    for row in models:
        status = str(row.get("status") or "active")
        visible = bool(row.get("visible_to_subjects", True))
        repaired = status in {"archived", "superseded", "contested_false"}
        fenced = not repaired and not visible
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.MODEL,
                object_ref=f"model:{row['id']}",
                dependency_basis=("source-semantic-admission:admitted_model_id",),
                lifecycle_state=status,
                repair_required=True,
                read_surface=True,
                fenced=fenced,
                repaired_or_superseded=repaired,
                unsafe_readable=not repaired and not fenced,
                details={
                    "archive_reason": row.get("archive_reason"),
                    "visible_to_subjects": visible,
                },
            )
        )
    for row in edges:
        status = str(row.get("status") or "active")
        repaired = status != "active"
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.MODEL_EDGE,
                object_ref=f"model-edge:{row['id']}",
                dependency_basis=(
                    f"model:{row['source_model_id']}",
                    f"model:{row['target_model_id']}",
                ),
                lifecycle_state=status,
                repair_required=True,
                read_surface=True,
                repaired_or_superseded=repaired,
                unsafe_readable=not repaired,
                details={"edge_kind": row.get("edge_kind")},
            )
        )
    for row in relations:
        status = str(row.get("status") or "candidate")
        repaired = status in {"rejected", "retired"}
        fenced = status in {"candidate", "needs_review", "disputed"}
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.RELATION_INSTANCE,
                object_ref=f"relation-instance:{row['id']}",
                dependency_basis=("relation:evidence_or_participant_model",),
                lifecycle_state=status,
                repair_required=True,
                read_surface=True,
                fenced=fenced,
                repaired_or_superseded=repaired,
                unsafe_readable=status in {"active", "accepted"},
                details={"relation_kind": row.get("relation_kind")},
            )
        )
    for row in belief_addresses:
        model_status = str(row.get("model_status") or row.get("status") or "active")
        visible = bool(row.get("visible_to_subjects", True))
        repaired = model_status in {"archived", "superseded", "contested_false"}
        fenced = not repaired and not visible
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.MODEL_BELIEF_ADDRESS,
                object_ref=f"model-belief-address:{row['model_id']}",
                dependency_basis=(f"model:{row['model_id']}",),
                lifecycle_state=model_status,
                repair_required=True,
                read_surface=True,
                fenced=fenced,
                repaired_or_superseded=repaired,
                unsafe_readable=not repaired and not fenced,
                details={"fingerprint": row.get("fingerprint")},
            )
        )
    for row in projection_snapshots:
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.PROJECTION_SNAPSHOT,
                object_ref=_projection_ref(row),
                dependency_basis=("projection:source_model_or_event",),
                lifecycle_state="materialized",
                repair_required=True,
                read_surface=True,
                unsafe_readable=True,
                details={
                    "updated_at": _json_safe(row.get("updated_at")),
                    "source_model_ids": [
                        str(item) for item in (row.get("source_model_ids") or ())
                    ],
                },
            )
        )
    for row in projection_dependencies:
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.PROJECTION_DEPENDENCY,
                object_ref=(
                    f"{_projection_ref(row)}:dependency:"
                    f"{row['ref_kind']}:{row['ref_value']}"
                ),
                dependency_basis=(
                    f"{row['ref_kind']}:{row['ref_value']}",
                ),
                lifecycle_state="stale_dependency_ref",
                repair_required=True,
                read_surface=False,
                details={"reason": row.get("reason")},
            )
        )
    for row in projection_refresh_jobs:
        status = str(row.get("status") or "pending")
        dependencies.append(
            CorrectionDependencyRecord(
                kind=CorrectionDependencyKind.PROJECTION_REFRESH_JOB,
                object_ref=f"projection-refresh-job:{row['id']}",
                dependency_basis=(_projection_ref(row),),
                lifecycle_state=status,
                repair_required=False,
                read_surface=False,
                repair_pending=status in {"pending", "leased"},
                details={
                    "attempts": int(row.get("attempts") or 0),
                    "last_error": row.get("last_error"),
                },
            )
        )

    repair_dependencies = [item for item in dependencies if item.repair_required]
    repaired_count = sum(item.repaired_or_superseded for item in repair_dependencies)
    fenced_count = sum(item.fenced for item in repair_dependencies)
    unsafe_count = sum(item.unsafe_readable for item in repair_dependencies)
    repair_pending_count = sum(item.repair_pending for item in dependencies)
    repair_required_count = len(repair_dependencies)
    residual_debt = max(0, repair_required_count - repaired_count)
    current_hash = canonical_sha256(str(root.get("observation_content_text") or ""))
    hash_references = [
        str(value)
        for value in (
            root.get("detection_source_content_hash"),
            *(row.get("source_content_hash") for row in interpretations),
        )
        if value
    ]
    hash_matches = sum(value == current_hash for value in hash_references)
    mutation_flags_clean = not bool(root.get("source_observation_mutated")) and not bool(
        root.get("successor_source_mutated")
    )
    source_immutable = (
        mutation_flags_clean and hash_matches == len(hash_references)
        if hash_references
        else None
    )
    incidents: set[str] = set()
    uncertainty: set[str] = {
        "This is an audit-only dependency census; it does not mutate, fence, "
        "supersede, rebuild or refresh any dependent state."
    }
    if not correction_found:
        incidents.add("adjudicated_grounding_successor_missing")
    elif correction_changes_referent is not True:
        incidents.add("correction_does_not_change_referent")
    if source_immutable is False:
        incidents.add("source_observation_hash_or_mutation_flag_mismatch")
    elif source_immutable is None:
        uncertainty.add(
            "Source immutability is unknown because no durable source-content "
            "hash was available for comparison."
        )
    if cross_tenant_reference_count:
        incidents.add("cross_tenant_dependency_reference")
    if unsafe_count:
        incidents.add("unsafe_readable_corrected_dependency")
    if residual_debt:
        uncertainty.add(
            "Residual repair debt counts every repair-required dependency that "
            "is not yet repaired or superseded; fenced rows remain debt."
        )
    component_counts = Counter(item.kind.value for item in dependencies)
    return CorrectionPropagationAudit(
        scope=scope,
        correction_grounding_trace_id=successor_id,
        source_observation_id=source_observation_id,
        correction_found=correction_found,
        correction_changes_referent=correction_changes_referent,
        discovered_dependency_count=len(dependencies),
        component_counts=dict(sorted(component_counts.items())),
        repair_required_dependency_count=repair_required_count,
        fenced_dependency_count=fenced_count,
        repaired_or_superseded_count=repaired_count,
        unsafe_readable_count=unsafe_count,
        repair_pending_count=repair_pending_count,
        residual_repair_debt_count=residual_debt,
        convergence_ratio=_ratio(repaired_count, repair_required_count),
        safe_containment_ratio=_ratio(
            repaired_count + fenced_count,
            repair_required_count,
        ),
        source_hash_reference_count=len(hash_references),
        source_hash_match_count=hash_matches,
        source_immutable=source_immutable,
        audit_read_only=True,
        cross_tenant_reference_count=cross_tenant_reference_count,
        cross_tenant_change_count=0,
        dependencies=tuple(dependencies),
        incidents=tuple(sorted(incidents)),
        uncertainty=tuple(sorted(uncertainty)),
        artifact_refs=artifact_refs,
    )


def render_correction_propagation_markdown(
    audit: CorrectionPropagationAudit,
) -> str:
    lines = [
        f"# Correction propagation audit: {audit.scope.run_id}",
        "",
        f"- Tenant: `{audit.scope.tenant_id}`",
        (
            "- Grounding correction: "
            f"`{audit.scope.predecessor_grounding_trace_id}` -> "
            f"`{audit.correction_grounding_trace_id or 'missing'}`"
        ),
        f"- Source observation: `{audit.source_observation_id or 'unknown'}`",
        f"- Correction found: **{'yes' if audit.correction_found else 'no'}**",
        f"- Audit is read-only: **{'yes' if audit.audit_read_only else 'no'}**",
        f"- Converged: **{'yes' if audit.converged else 'no'}**",
        "",
        "## Census",
        "",
        f"- Discovered records: **{audit.discovered_dependency_count}**",
        f"- Repair-required dependencies: **{audit.repair_required_dependency_count}**",
        f"- Fenced: **{audit.fenced_dependency_count}**",
        f"- Repaired or superseded: **{audit.repaired_or_superseded_count}**",
        f"- Unsafe-readable: **{audit.unsafe_readable_count}**",
        f"- Repair pending: **{audit.repair_pending_count}**",
        f"- Residual repair debt: **{audit.residual_repair_debt_count}**",
        f"- Convergence ratio: **{_fmt_ratio(audit.convergence_ratio)}**",
        f"- Safe-containment ratio: **{_fmt_ratio(audit.safe_containment_ratio)}**",
        "",
        "## Safety",
        "",
        f"- Source immutable: **{_fmt_bool(audit.source_immutable)}**",
        (
            "- Source hash matches: "
            f"**{audit.source_hash_match_count}/{audit.source_hash_reference_count}**"
        ),
        f"- Cross-tenant dependency references: **{audit.cross_tenant_reference_count}**",
        f"- Cross-tenant changes by this audit: **{audit.cross_tenant_change_count}**",
        "",
        "## Component counts",
        "",
    ]
    if audit.component_counts:
        lines.extend(
            f"- `{name}`: {count}"
            for name, count in audit.component_counts.items()
        )
    else:
        lines.append("- none")
    lines.extend(["", "## Incidents", ""])
    lines.extend(f"- {item}" for item in audit.incidents or ("none",))
    lines.extend(["", "## Uncertainty", ""])
    lines.extend(f"- {item}" for item in audit.uncertainty or ("none",))
    return "\n".join(lines).rstrip() + "\n"


async def _cross_tenant_reference_count(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: tuple[UUID, ...],
    model_event_ids: tuple[UUID, ...],
) -> int:
    return int(
        await conn.fetchval(
            """
            SELECT
              (SELECT count(*) FROM model_edges
               WHERE tenant_id<>$1
                 AND (
                   source_model_id=ANY($2::uuid[])
                   OR target_model_id=ANY($2::uuid[])
                 ))
              + (SELECT count(*) FROM relation_instances
                 WHERE tenant_id<>$1 AND evidence_model_ids && $2::uuid[])
              + (SELECT count(*) FROM relation_participants
                 WHERE tenant_id<>$1 AND model_id=ANY($2::uuid[]))
              + (SELECT count(*) FROM model_belief_addresses
                 WHERE tenant_id<>$1 AND model_id=ANY($2::uuid[]))
              + (SELECT count(*) FROM projection_snapshots
                 WHERE tenant_id<>$1
                   AND (
                     source_model_ids && $2::uuid[]
                     OR (
                       cardinality($3::uuid[]) > 0
                       AND source_event_ids && $3::uuid[]
                     )
                   ))
              + (SELECT count(*) FROM projection_dependencies
                 WHERE tenant_id<>$1
                   AND (
                     (ref_kind='model' AND ref_value=ANY($4::text[]))
                     OR (
                       ref_kind='model_event'
                       AND ref_value=ANY($5::text[])
                     )
                   ))
            """,
            tenant_id,
            list(model_ids),
            list(model_event_ids),
            [str(value) for value in model_ids],
            [str(value) for value in model_event_ids],
        )
        or 0
    )


def _refresh_job_matches(
    row: Mapping[str, Any],
    *,
    model_ids: tuple[UUID, ...],
    model_event_ids: tuple[UUID, ...],
    projection_keys: set[tuple[str, str, str]],
) -> bool:
    key = (
        str(row.get("projection_name") or ""),
        str(row.get("projection_version") or ""),
        str(row.get("subject_key") or ""),
    )
    if key in projection_keys:
        return True
    if set(row.get("event_ids") or ()).intersection(model_event_ids):
        return True
    model_values = {str(value) for value in model_ids}
    event_values = {str(value) for value in model_event_ids}
    return any(
        isinstance(ref, dict)
        and (
            (
                ref.get("ref_kind") == "model"
                and str(ref.get("ref_value")) in model_values
            )
            or (
                ref.get("ref_kind") == "model_event"
                and str(ref.get("ref_value")) in event_values
            )
        )
        for ref in _json_list(row.get("dependency_refs"))
    )


def _projection_ref(row: Mapping[str, Any]) -> str:
    return (
        "projection:"
        f"{row.get('projection_name')}:{row.get('projection_version')}:"
        f"{row.get('subject_key')}"
    )


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _fmt_ratio(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.1%}"


def _fmt_bool(value: bool | None) -> str:
    return "unknown" if value is None else ("yes" if value else "no")


def _uuid_or_none(value: Any) -> UUID | None:
    try:
        return UUID(str(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    return value if isinstance(value, dict) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return value if isinstance(value, list) else []


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    return value


__all__ = [
    "CorrectionDependencyKind",
    "CorrectionDependencyRecord",
    "CorrectionPropagationAudit",
    "CorrectionPropagationScope",
    "analyze_correction_propagation_rows",
    "evaluate_correction_propagation",
    "render_correction_propagation_markdown",
]
