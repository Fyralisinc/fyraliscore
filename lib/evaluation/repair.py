"""Independent evaluation of correction invalidation and repair convergence."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.kernel import canonical_sha256
from lib.contracts.execution import LeaseResolution, WorkObligationState
from lib.contracts.repair import (
    DependencyEdge,
    InvalidationRequestRecord,
    RepairCoverageBasis,
    RepairEpisode,
    RepairEpisodeCommand,
    RepairEpisodeState,
    RepairObligation,
    RepairObligationCommand,
    RepairObligationState,
    RepairReceipt,
    RepairReceiptCommand,
    repair_episode_transition_allowed,
    repair_obligation_transition_allowed,
)
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceTier,
    FateDenominatorRecord,
    IncidentObservation,
    IncidentStatus,
    InvariantRunEvidence,
    MetricObservation,
)


class _RepairEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class RepairEvaluationScope(_RepairEvaluationModel):
    tenant_id: UUID
    start: datetime
    end: datetime
    run_id: str = Field(min_length=1)

    @field_validator("start", "end")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def interval_is_forward(self) -> Self:
        if self.end <= self.start:
            raise ValueError("repair evaluation end must follow start")
        return self


class RepairEvaluationState(_RepairEvaluationModel):
    scope: RepairEvaluationScope
    dependency_edge_count: int = Field(ge=0)
    valid_dependency_edge_count: int = Field(ge=0)
    dependency_edge_validity_rate: float | None = Field(default=None, ge=0, le=1)
    invalidation_request_count: int = Field(ge=0)
    exact_source_bound_request_count: int = Field(ge=0)
    invalidation_source_binding_rate: float | None = Field(
        default=None, ge=0, le=1
    )
    repair_episode_count: int = Field(ge=0)
    episode_state_counts: dict[str, int]
    legal_episode_count: int = Field(ge=0)
    episode_history_integrity_rate: float | None = Field(default=None, ge=0, le=1)
    convergence_claim_count: int = Field(ge=0)
    valid_convergence_claim_count: int = Field(ge=0)
    convergence_validity_rate: float | None = Field(default=None, ge=0, le=1)
    repair_obligation_count: int = Field(ge=0)
    obligation_fate_counts: dict[str, int]
    legal_obligation_count: int = Field(ge=0)
    obligation_history_integrity_rate: float | None = Field(
        default=None, ge=0, le=1
    )
    dependency_bound_lineage_count: int = Field(ge=0)
    dependency_lineage_integrity_rate: float | None = Field(
        default=None, ge=0, le=1
    )
    repair_redrive_generation_count: int = Field(ge=0)
    authorized_repair_redrive_generation_count: int = Field(ge=0)
    repair_redrive_authorization_rate: float | None = Field(
        default=None, ge=0, le=1
    )
    closed_repair_redrive_generation_count: int = Field(ge=0)
    repair_redrive_closure_rate: float | None = Field(default=None, ge=0, le=1)
    child_work_obligation_count: int = Field(ge=0)
    valid_child_work_binding_count: int = Field(ge=0)
    child_work_binding_rate: float | None = Field(default=None, ge=0, le=1)
    terminal_child_work_required_count: int = Field(ge=0)
    closed_child_work_count: int = Field(ge=0)
    child_work_closure_rate: float | None = Field(default=None, ge=0, le=1)
    repair_receipt_count: int = Field(ge=0)
    receipt_required_obligation_count: int = Field(ge=0)
    valid_repair_receipt_count: int = Field(ge=0)
    repair_receipt_closure_rate: float | None = Field(default=None, ge=0, le=1)
    known_material_dependency_count: int = Field(ge=0)
    known_covered_dependency_count: int = Field(ge=0)
    known_dependency_coverage_rate: float | None = Field(default=None, ge=0, le=1)
    oracle_material_dependency_count: int | None = Field(default=None, ge=0)
    oracle_covered_dependency_count: int | None = Field(default=None, ge=0)
    oracle_dependency_coverage_rate: float | None = Field(default=None, ge=0, le=1)
    unresolved_current_obligation_count: int = Field(ge=0)
    immutable_table_count: int = Field(ge=0)
    guarded_immutable_table_count: int = Field(ge=0)
    immutable_storage_guard_rate: float | None = Field(default=None, ge=0, le=1)
    command_count: int = Field(ge=0)
    reconstructable_command_count: int = Field(ge=0)
    command_reconstructability_rate: float | None = Field(default=None, ge=0, le=1)
    command_event_coverage: float | None = Field(default=None, ge=0, le=1)
    command_outbox_coverage: float | None = Field(default=None, ge=0, le=1)
    incident_counts: dict[str, int]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


_IMMUTABLE_REPAIR_TABLES = frozenset(
    {
        "repair_dependency_edges",
        "invalidation_request_records",
        "repair_episode_versions",
        "repair_obligation_specs",
        "repair_obligation_versions",
        "repair_receipts",
    }
)

_RECEIPT_STATES = frozenset(
    {
        RepairObligationState.REPAIRED,
        RepairObligationState.NO_OP,
        RepairObligationState.ADJUDICATED_RESIDUE,
        RepairObligationState.EXHAUSTED,
        RepairObligationState.ESCALATED,
    }
)

_COVERED_STATES = frozenset(
    {
        RepairObligationState.REPAIRED,
        RepairObligationState.NO_OP,
        RepairObligationState.ADJUDICATED_RESIDUE,
    }
)


async def evaluate_repair_state(
    conn: asyncpg.Connection,
    *,
    scope: RepairEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> RepairEvaluationState:
    invalidations = await conn.fetch(
        """
        SELECT r.*,
               s.tenant_id AS source_result_tenant_id,
               s.writer_id AS source_result_writer_id,
               s.object_type AS source_result_object_type,
               s.object_id AS source_result_object_id,
               s.object_version AS source_result_object_version
        FROM invalidation_request_records r
        LEFT JOIN agency_command_results s ON s.id=r.source_command_result_id
        WHERE r.tenant_id=$1 AND r.created_at >= $2 AND r.created_at < $3
        ORDER BY r.created_at, r.request_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    dependencies = await conn.fetch(
        """
        SELECT DISTINCT e.*
        FROM repair_dependency_edges e
        JOIN invalidation_request_records r
          ON r.tenant_id=e.tenant_id
         AND r.source_object_type=e.source_object_type
         AND r.source_object_id=e.source_object_id
         AND r.predecessor_source_version=e.source_object_version
        WHERE r.tenant_id=$1 AND r.created_at >= $2 AND r.created_at < $3
        ORDER BY e.declared_at, e.edge_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    episodes = await conn.fetch(
        """
        SELECT v.*, h.invalidation_request_id AS head_request_id,
               h.invalidation_epoch AS head_epoch,
               h.current_version AS head_version,
               h.current_state AS head_state,
               h.current_episode_digest AS head_digest,
               r.request,
               (SELECT count(*) FROM work_obligation_heads w
                 JOIN repair_obligation_heads ro
                   ON ro.tenant_id=w.tenant_id
                  AND ro.current_child_work_obligation_id=w.obligation_id
                WHERE ro.tenant_id=h.tenant_id
                  AND ro.invalidation_request_id=h.invalidation_request_id
                  AND w.current_state IN (
                    'leased','reconciliation_required','lease_lost',
                    'quarantined','owner_terminalization_pending'
                  )) AS unsafe_child_work_count
        FROM repair_episode_versions v
        JOIN repair_episode_heads h
          ON h.tenant_id=v.tenant_id AND h.episode_id=v.episode_id
        JOIN invalidation_request_records r
          ON r.tenant_id=h.tenant_id AND r.request_id=h.invalidation_request_id
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.episode_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    obligations = await conn.fetch(
        """
        SELECT v.*, s.initial_obligation, s.initial_obligation_digest,
               s.lineage_id AS spec_lineage_id,
               s.generation AS spec_generation,
               s.parent_obligation_id,
               h.lineage_id AS head_lineage_id,
               h.generation AS head_generation,
               h.current_version AS head_version,
               h.current_state AS head_state,
               h.current_obligation_digest AS head_digest,
               h.current_child_work_obligation_id,
               h.current_repair_receipt_id,
               l.current_obligation_id AS lineage_current_obligation_id,
               l.current_generation AS lineage_current_generation,
               r.request,
               cw.current_state AS child_work_state,
               cw.current_version AS child_work_version,
               cw.generation AS child_work_generation,
               cws.target_object_type AS child_target_object_type,
               cws.target_object_id AS child_target_object_id,
               cws.owner_writer_id AS child_owner_writer_id,
               cws.purpose AS child_purpose,
               cws.effect_possible AS child_effect_possible,
               rr.command_result_id AS repair_receipt_command_result_id,
               rr.receipt AS current_repair_receipt,
               cwr.writer_id AS child_result_writer_id,
               cwr.object_type AS child_result_object_type,
               cwr.object_id AS child_result_object_id,
               cwr.object_version AS child_result_object_version,
               cwr.result AS child_result,
               ccv.transition_payload AS child_completion_payload,
               (SELECT count(*) FROM repair_dependency_edges e
                 WHERE e.tenant_id=s.tenant_id
                   AND e.source_object_type=s.source_object_type
                   AND e.source_object_id=s.source_object_id
                   AND e.source_object_version=r.predecessor_source_version
                   AND e.dependent_object_type=s.dependent_object_type
                   AND e.dependent_object_id=s.dependent_object_id
                   AND e.dependent_object_version=s.dependent_object_version
                   AND e.dependency_kind=s.dependency_kind
                   AND e.material) AS dependency_match_count
        FROM repair_obligation_versions v
        JOIN repair_obligation_heads h
          ON h.tenant_id=v.tenant_id AND h.obligation_id=v.obligation_id
        JOIN repair_obligation_specs s
          ON s.tenant_id=h.tenant_id AND s.obligation_id=h.obligation_id
        JOIN repair_obligation_lineage_heads l
          ON l.tenant_id=h.tenant_id AND l.lineage_id=h.lineage_id
        JOIN invalidation_request_records r
          ON r.tenant_id=h.tenant_id AND r.request_id=h.invalidation_request_id
        LEFT JOIN work_obligation_heads cw
          ON cw.tenant_id=h.tenant_id
         AND cw.obligation_id=h.current_child_work_obligation_id
        LEFT JOIN work_obligation_specs cws
          ON cws.tenant_id=cw.tenant_id AND cws.obligation_id=cw.obligation_id
        LEFT JOIN repair_receipts rr
          ON rr.tenant_id=h.tenant_id AND rr.receipt_id=h.current_repair_receipt_id
        LEFT JOIN agency_command_results cwr
          ON cwr.tenant_id=rr.tenant_id
         AND cwr.id=NULLIF(rr.receipt->>'child_work_command_result_id','')::uuid
        LEFT JOIN LATERAL (
          SELECT wv.transition_payload
          FROM work_obligation_versions wv
          WHERE wv.tenant_id=cw.tenant_id
            AND wv.obligation_id=cw.obligation_id
            AND wv.state='completed'
          ORDER BY wv.aggregate_version DESC
          LIMIT 1
        ) ccv ON TRUE
        WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY v.obligation_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    receipts = await conn.fetch(
        """
        SELECT rr.*,
               cr.writer_id AS dependent_result_writer_id,
               cr.command_kind AS dependent_result_command_kind,
               cr.object_type AS dependent_result_object_type,
               cr.object_id AS dependent_result_object_id,
               cr.object_version AS dependent_result_object_version
        FROM repair_receipts rr
        JOIN repair_obligation_heads h
          ON h.tenant_id=rr.tenant_id AND h.obligation_id=rr.repair_obligation_id
        LEFT JOIN agency_command_results cr
          ON cr.tenant_id=rr.tenant_id AND cr.id=rr.dependent_command_result_id
        WHERE rr.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        ORDER BY rr.observed_at, rr.receipt_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    commands = await conn.fetch(
        """
        WITH component_results AS (
          SELECT v.command_result_id FROM repair_episode_versions v
            JOIN repair_episode_heads h USING (tenant_id, episode_id)
           WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION
          SELECT v.command_result_id FROM repair_obligation_versions v
            JOIN repair_obligation_heads h USING (tenant_id, obligation_id)
           WHERE v.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
          UNION
          SELECT rr.command_result_id FROM repair_receipts rr
            JOIN repair_obligation_heads h
              ON h.tenant_id=rr.tenant_id AND h.obligation_id=rr.repair_obligation_id
           WHERE rr.tenant_id=$1 AND h.updated_at >= $2 AND h.updated_at < $3
        )
        SELECT r.*,
               (SELECT count(*) FROM agency_canonical_events e
                 WHERE e.command_result_id=r.id) AS event_count,
               (SELECT count(*) FROM agency_canonical_events e
                 JOIN agency_outbox_records o ON o.event_id=e.id
                 WHERE e.command_result_id=r.id) AS outbox_count
        FROM agency_command_results r
        JOIN component_results c ON c.command_result_id=r.id
        ORDER BY r.created_at, r.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    guarded = await conn.fetch(
        """
        SELECT DISTINCT c.relname AS table_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND NOT t.tgisinternal
          AND t.tgname LIKE 'reject_%_mutation'
        """
    )
    return analyze_repair_rows(
        scope=scope,
        dependencies=dependencies,
        invalidations=invalidations,
        episodes=episodes,
        obligations=obligations,
        receipts=receipts,
        commands=commands,
        guarded_tables={row["table_name"] for row in guarded},
        artifact_refs=artifact_refs,
    )


def analyze_repair_rows(
    *,
    scope: RepairEvaluationScope,
    dependencies: Sequence[Mapping[str, Any]],
    invalidations: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]],
    obligations: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    guarded_tables: set[str],
    artifact_refs: tuple[str, ...],
) -> RepairEvaluationState:
    incidents: Counter[str] = Counter()
    valid_edges = sum(_valid_dependency(row, incidents) for row in dependencies)
    valid_requests = sum(_valid_invalidation(row, incidents) for row in invalidations)
    request_models = _request_models(invalidations)
    edge_counts = Counter(
        (
            row["source_object_type"],
            row["source_object_id"],
            int(row["source_object_version"]),
        )
        for row in dependencies
        if bool(row.get("material"))
    )
    obligation_groups = _group(obligations, "obligation_id")
    episode_groups = _group(episodes, "episode_id")
    obligation_models: dict[UUID, RepairObligation] = {}
    legal_obligations = 0
    bound_lineages = 0
    child_work_count = 0
    valid_child_work_bindings = 0
    terminal_child_work_required = 0
    closed_child_work = 0
    current_tails: dict[UUID, Counter[str]] = defaultdict(Counter)
    historical_counts: Counter[UUID] = Counter()
    for obligation_id, rows in obligation_groups.items():
        valid, obligation = _valid_obligation_history(rows, incidents)
        legal_obligations += int(valid)
        if obligation is not None:
            obligation_models[obligation_id] = obligation
            historical_counts[obligation.invalidation_request_id] += 1
            if rows[0]["lineage_current_obligation_id"] == obligation_id:
                current_tails[obligation.invalidation_request_id][
                    str(rows[-1]["head_state"])
                ] += 1
        lineage_valid = _valid_obligation_lineage(rows[0], obligation, incidents)
        bound_lineages += int(lineage_valid)
        if obligation is not None and obligation.child_work_obligation_id is not None:
            child_work_count += 1
            binding_valid = _valid_child_work_binding(
                rows[-1], obligation=obligation, incidents=incidents
            )
            valid_child_work_bindings += int(binding_valid)
            if obligation.state.terminal:
                terminal_child_work_required += 1
                closed_child_work += int(
                    _valid_child_work_closure(
                        rows[-1],
                        obligation=obligation,
                        binding_valid=binding_valid,
                        incidents=incidents,
                    )
                )

    repair_redrives = tuple(
        obligation
        for obligation in obligation_models.values()
        if obligation.generation > 1
    )
    authorized_repair_redrives = 0
    closed_repair_redrives = 0
    for child in repair_redrives:
        authorized, closed = _valid_repair_redrive_generation(
            child=child,
            obligation_models=obligation_models,
            obligation_groups=obligation_groups,
            incidents=incidents,
        )
        authorized_repair_redrives += int(authorized)
        closed_repair_redrives += int(closed)

    episode_models: dict[UUID, RepairEpisode] = {}
    legal_episodes = 0
    convergence_claims = 0
    valid_convergence = 0
    known_material = 0
    known_covered = 0
    oracle_material_values: list[int] = []
    oracle_covered_values: list[int] = []
    oracle_complete = True
    for episode_id, rows in episode_groups.items():
        history_valid, episode = _valid_episode_history(rows, incidents)
        legal_episodes += int(history_valid)
        if episode is None:
            continue
        episode_models[episode_id] = episode
        request = request_models.get(episode.invalidation_request_id)
        actual_known = (
            edge_counts[
                (
                    request.source_object_type,
                    request.source_object_id,
                    request.predecessor_source_version,
                )
            ]
            if request is not None
            else 0
        )
        tails = current_tails[episode.invalidation_request_id]
        actual_covered = sum(tails[state.value] for state in _COVERED_STATES)
        population_valid = (
            request is not None
            and episode.known_material_dependency_count == actual_known
            and episode.known_covered_dependency_count == actual_covered
            and Counter(episode.current_tail_fate_counts) == tails
            and episode.historical_generation_count
            == historical_counts[episode.invalidation_request_id]
            and episode.adjudicated_residue_count
            == tails[RepairObligationState.ADJUDICATED_RESIDUE.value]
            and int(rows[-1].get("unsafe_child_work_count") or 0) == 0
        )
        if (
            episode.coverage_basis
            is RepairCoverageBasis.INSTRUMENTED_CONTRACT_COMPLETE
            and (
                episode.oracle_material_dependency_count != actual_known
                or episode.oracle_covered_dependency_count != actual_covered
            )
        ):
            population_valid = False
        if not population_valid:
            incidents["repair_episode_population_mismatch"] += 1
        known_material += actual_known
        known_covered += actual_covered
        if episode.oracle_material_dependency_count is None:
            oracle_complete = False
        else:
            oracle_material_values.append(episode.oracle_material_dependency_count)
            oracle_covered_values.append(episode.oracle_covered_dependency_count or 0)
        if episode.state.terminal:
            convergence_claims += 1
            claim_valid = history_valid and population_valid and episode.state in {
                RepairEpisodeState.CONVERGED,
                RepairEpisodeState.CONVERGED_WITH_ADJUDICATED_RESIDUE,
            }
            valid_convergence += int(claim_valid)
            if not claim_valid:
                incidents["invalid_repair_convergence_claim"] += 1

    receipt_required = sum(
        rows[-1]["current_repair_receipt_id"] is not None
        for rows in obligation_groups.values()
    )
    valid_receipts = sum(
        _valid_receipt(
            row,
            obligation_models=obligation_models,
            obligation_groups=obligation_groups,
            episode_models=episode_models,
            incidents=incidents,
        )
        for row in receipts
    )
    if len(receipts) != receipt_required:
        incidents["repair_receipt_cardinality_mismatch"] += abs(
            receipt_required - len(receipts)
        )

    reconstructable = 0
    event_covered = 0
    outbox_covered = 0
    for row in commands:
        command_valid = _command_reconstructable(row)
        reconstructable += int(command_valid)
        if not command_valid:
            incidents["unreconstructable_repair_command"] += 1
        event_count = int(row.get("event_count") or 0)
        outbox_count = int(row.get("outbox_count") or 0)
        event_covered += int(event_count == 1)
        outbox_covered += int(outbox_count == 1)
        if event_count != 1:
            incidents["repair_command_without_exact_event"] += 1
        if outbox_count != 1:
            incidents["repair_command_without_exact_outbox"] += 1

    guarded_count = len(_IMMUTABLE_REPAIR_TABLES & guarded_tables)
    missing_guards = len(_IMMUTABLE_REPAIR_TABLES - guarded_tables)
    if missing_guards:
        incidents["immutable_repair_table_unguarded"] += missing_guards

    episode_states = Counter(
        str(rows[-1]["head_state"]) for rows in episode_groups.values()
    )
    obligation_states = Counter(
        str(rows[-1]["head_state"]) for rows in obligation_groups.values()
    )
    unresolved = sum(
        count
        for state, count in obligation_states.items()
        if not RepairObligationState(state).terminal
    )
    oracle_material = sum(oracle_material_values) if oracle_complete else None
    oracle_covered = sum(oracle_covered_values) if oracle_complete else None
    return RepairEvaluationState(
        scope=scope,
        dependency_edge_count=len(dependencies),
        valid_dependency_edge_count=valid_edges,
        dependency_edge_validity_rate=_ratio(valid_edges, len(dependencies)),
        invalidation_request_count=len(invalidations),
        exact_source_bound_request_count=valid_requests,
        invalidation_source_binding_rate=_ratio(valid_requests, len(invalidations)),
        repair_episode_count=len(episode_groups),
        episode_state_counts=dict(episode_states),
        legal_episode_count=legal_episodes,
        episode_history_integrity_rate=_ratio(legal_episodes, len(episode_groups)),
        convergence_claim_count=convergence_claims,
        valid_convergence_claim_count=valid_convergence,
        convergence_validity_rate=_ratio(valid_convergence, convergence_claims),
        repair_obligation_count=len(obligation_groups),
        obligation_fate_counts=dict(obligation_states),
        legal_obligation_count=legal_obligations,
        obligation_history_integrity_rate=_ratio(
            legal_obligations, len(obligation_groups)
        ),
        dependency_bound_lineage_count=bound_lineages,
        dependency_lineage_integrity_rate=_ratio(
            bound_lineages, len(obligation_groups)
        ),
        repair_redrive_generation_count=len(repair_redrives),
        authorized_repair_redrive_generation_count=authorized_repair_redrives,
        repair_redrive_authorization_rate=_ratio(
            authorized_repair_redrives, len(repair_redrives)
        ),
        closed_repair_redrive_generation_count=closed_repair_redrives,
        repair_redrive_closure_rate=_ratio(
            closed_repair_redrives, len(repair_redrives)
        ),
        child_work_obligation_count=child_work_count,
        valid_child_work_binding_count=valid_child_work_bindings,
        child_work_binding_rate=_ratio(valid_child_work_bindings, child_work_count),
        terminal_child_work_required_count=terminal_child_work_required,
        closed_child_work_count=closed_child_work,
        child_work_closure_rate=_ratio(
            closed_child_work, terminal_child_work_required
        ),
        repair_receipt_count=len(receipts),
        receipt_required_obligation_count=receipt_required,
        valid_repair_receipt_count=valid_receipts,
        repair_receipt_closure_rate=_ratio(valid_receipts, receipt_required),
        known_material_dependency_count=known_material,
        known_covered_dependency_count=known_covered,
        known_dependency_coverage_rate=_ratio(known_covered, known_material),
        oracle_material_dependency_count=oracle_material,
        oracle_covered_dependency_count=oracle_covered,
        oracle_dependency_coverage_rate=(
            _ratio(oracle_covered, oracle_material)
            if oracle_covered is not None and oracle_material is not None
            else None
        ),
        unresolved_current_obligation_count=unresolved,
        immutable_table_count=len(_IMMUTABLE_REPAIR_TABLES),
        guarded_immutable_table_count=guarded_count,
        immutable_storage_guard_rate=_ratio(
            guarded_count, len(_IMMUTABLE_REPAIR_TABLES)
        ),
        command_count=len(commands),
        reconstructable_command_count=reconstructable,
        command_reconstructability_rate=_ratio(reconstructable, len(commands)),
        command_event_coverage=_ratio(event_covered, len(commands)),
        command_outbox_coverage=_ratio(outbox_covered, len(commands)),
        incident_counts=dict(sorted(incidents.items())),
        uncertainty=(
            "This E3 component proof validates committed repair mechanics, not the semantic correctness of a production dependency declaration.",
            "Oracle-complete dependency recall is available only when an instrumented or synthetic world supplies the denominator.",
            "The replay covers a first-generation no-op, a real dependent-writer repair with atomic child Work creation and exact receipt-backed child closure, and an identity-preserving authorized successor after exact reverse child-Work exhaustion; deletion, residue adjudication, crash/reorder recovery and partition rebalance remain unproven.",
            "Historical as-known query preservation and policy/reward/intent correction closure require separate evaluators.",
        ),
        artifact_refs=artifact_refs,
    )


def _valid_dependency(row: Mapping[str, Any], incidents: Counter[str]) -> bool:
    try:
        edge = DependencyEdge.model_validate(_json(row["dependency"]))
        valid = (
            edge.edge_id == row["edge_id"]
            and edge.tenant_id == row["tenant_id"]
            and edge.edge_digest == row["edge_digest"]
            and edge.source_object_type == row["source_object_type"]
            and edge.source_object_id == row["source_object_id"]
            and edge.source_object_version == int(row["source_object_version"])
            and edge.dependent_object_type == row["dependent_object_type"]
            and edge.dependent_object_id == row["dependent_object_id"]
            and edge.dependent_object_version == int(row["dependent_object_version"])
            and edge.material == bool(row["material"])
        )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalid_repair_dependency_edge"] += 1
    return valid


def _valid_invalidation(row: Mapping[str, Any], incidents: Counter[str]) -> bool:
    try:
        request = InvalidationRequestRecord.model_validate(_json(row["request"]))
        valid = (
            request.request_id == row["request_id"]
            and request.tenant_id == row["tenant_id"]
            and request.request_digest == row["request_digest"]
            and request.source_writer_id == row["source_result_writer_id"]
            and request.source_object_type == row["source_result_object_type"]
            and request.source_object_id == row["source_result_object_id"]
            and request.successor_source_version
            == int(row["source_result_object_version"])
            and request.tenant_id == row["source_result_tenant_id"]
        )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalidation_without_exact_source_commit"] += 1
    return valid


def _request_models(
    rows: Sequence[Mapping[str, Any]],
) -> dict[UUID, InvalidationRequestRecord]:
    result: dict[UUID, InvalidationRequestRecord] = {}
    for row in rows:
        try:
            request = InvalidationRequestRecord.model_validate(_json(row["request"]))
            result[request.request_id] = request
        except (KeyError, TypeError, ValueError):
            continue
    return result


def _valid_episode_history(
    rows: Sequence[Mapping[str, Any]], incidents: Counter[str]
) -> tuple[bool, RepairEpisode | None]:
    valid = True
    prior: RepairEpisode | None = None
    for position, row in enumerate(rows, start=1):
        try:
            episode = RepairEpisode.model_validate(_json(row["episode"]))
            row_valid = (
                int(row["aggregate_version"]) == position
                and episode.episode_id == row["episode_id"]
                and episode.state == row["state"]
                and episode.episode_digest == row["episode_digest"]
                and repair_episode_transition_allowed(
                    prior.state if prior else None, episode.state
                )
            )
            if prior is not None:
                row_valid = row_valid and _same_fields(
                    prior,
                    episode,
                    (
                        "episode_id",
                        "tenant_id",
                        "invalidation_request_id",
                        "invalidation_epoch",
                        "kind",
                        "coverage_basis",
                        "created_at",
                    ),
                )
        except (KeyError, TypeError, ValueError):
            row_valid = False
            episode = None
        valid = valid and row_valid
        if episode is not None:
            prior = episode
    last = rows[-1]
    valid = valid and prior is not None and (
        int(last["head_version"]) == int(last["aggregate_version"])
        and str(last["head_state"]) == str(last["state"])
        and last["head_digest"] == prior.episode_digest
        and last["head_request_id"] == prior.invalidation_request_id
        and int(last["head_epoch"]) == prior.invalidation_epoch
    )
    if not valid:
        incidents["invalid_repair_episode_history"] += 1
    return valid, prior


def _valid_obligation_history(
    rows: Sequence[Mapping[str, Any]], incidents: Counter[str]
) -> tuple[bool, RepairObligation | None]:
    valid = True
    prior: RepairObligation | None = None
    for position, row in enumerate(rows, start=1):
        try:
            obligation = RepairObligation.model_validate(_json(row["obligation"]))
            row_valid = (
                int(row["aggregate_version"]) == position
                and obligation.obligation_id == row["obligation_id"]
                and obligation.state == row["state"]
                and obligation.obligation_digest == row["obligation_digest"]
                and repair_obligation_transition_allowed(
                    prior.state if prior else None, obligation.state
                )
            )
            if prior is not None:
                row_valid = row_valid and _same_fields(
                    prior,
                    obligation,
                    (
                        "obligation_id",
                        "lineage_id",
                        "tenant_id",
                        "generation",
                        "parent_obligation_id",
                        "invalidation_request_id",
                        "invalidation_epoch",
                        "source_object_type",
                        "source_object_id",
                        "source_generation",
                        "dependent_object_type",
                        "dependent_object_id",
                        "dependent_object_version",
                        "dependency_kind",
                        "fence_class",
                        "required_dependent_writer_id",
                        "required_dependent_transition",
                        "expected_target_version",
                        "maximum_attempts",
                        "deadline",
                        "residue_policy_ref",
                        "redrive_authorization_ref",
                        "created_at",
                    ),
                )
        except (KeyError, TypeError, ValueError):
            row_valid = False
            obligation = None
        valid = valid and row_valid
        if obligation is not None:
            prior = obligation
    first = rows[0]
    last = rows[-1]
    try:
        initial = RepairObligation.model_validate(_json(first["initial_obligation"]))
        valid = valid and initial.obligation_digest == first["initial_obligation_digest"]
    except (KeyError, TypeError, ValueError):
        valid = False
    valid = valid and prior is not None and (
        int(last["head_version"]) == int(last["aggregate_version"])
        and str(last["head_state"]) == str(last["state"])
        and last["head_digest"] == prior.obligation_digest
    )
    if not valid:
        incidents["invalid_repair_obligation_history"] += 1
    return valid, prior


def _valid_obligation_lineage(
    row: Mapping[str, Any],
    obligation: RepairObligation | None,
    incidents: Counter[str],
) -> bool:
    try:
        valid = obligation is not None and (
            obligation.lineage_id == row["spec_lineage_id"]
            and obligation.lineage_id == row["head_lineage_id"]
            and obligation.generation == int(row["spec_generation"])
            and obligation.generation == int(row["head_generation"])
            and int(row["lineage_current_generation"]) >= obligation.generation
            and int(row["dependency_match_count"]) == 1
            and ((obligation.generation == 1) == (row["parent_obligation_id"] is None))
        )
        if valid and int(row["lineage_current_generation"]) == obligation.generation:
            valid = row["lineage_current_obligation_id"] == obligation.obligation_id
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalid_repair_dependency_or_lineage"] += 1
    return bool(valid)


def _valid_repair_redrive_generation(
    *,
    child: RepairObligation,
    obligation_models: Mapping[UUID, RepairObligation],
    obligation_groups: Mapping[UUID, Sequence[Mapping[str, Any]]],
    incidents: Counter[str],
) -> tuple[bool, bool]:
    parent = obligation_models.get(child.parent_obligation_id)
    parent_rows = obligation_groups.get(child.parent_obligation_id, ())
    child_rows = obligation_groups.get(child.obligation_id, ())
    authorized = parent is not None and len(parent_rows) >= 2 and bool(child_rows)
    if parent is not None:
        identity_fields = (
            "tenant_id",
            "lineage_id",
            "invalidation_request_id",
            "invalidation_epoch",
            "source_object_type",
            "source_object_id",
            "source_generation",
            "dependent_object_type",
            "dependent_object_id",
            "dependent_object_version",
            "dependency_kind",
            "fence_class",
            "required_dependent_writer_id",
            "required_dependent_transition",
            "expected_target_version",
            "residue_policy_ref",
        )
        authorized = authorized and (
            all(getattr(child, name) == getattr(parent, name) for name in identity_fields)
            and child.generation == parent.generation + 1
            and child.parent_obligation_id == parent.obligation_id
            and bool(child.redrive_authorization_ref)
        )
    if len(parent_rows) >= 2 and child_rows:
        try:
            parent_before = RepairObligation.model_validate(
                _json(parent_rows[-2]["obligation"])
            )
            parent_after = RepairObligation.model_validate(
                _json(parent_rows[-1]["obligation"])
            )
            child_initial = RepairObligation.model_validate(
                _json(child_rows[0]["obligation"])
            )
            authorized = authorized and (
                parent_before.state
                in {
                    RepairObligationState.EXHAUSTED,
                    RepairObligationState.ESCALATED,
                }
                and child.created_at > parent_before.updated_at
                and parent_after.state
                is RepairObligationState.SUPERSEDED_BY_NEW_GENERATION
                and parent_after.successor_obligation_id == child.obligation_id
                and parent_rows[-1]["transition_kind"] == "successor_registered"
                and parent_rows[-1]["command_result_id"]
                == child_rows[0]["command_result_id"]
                and child_initial.state is RepairObligationState.OPEN
                and child_initial.attempt == 0
                and child_initial.child_work_obligation_id is None
                and child_initial.dependent_command_result_id is None
                and child_initial.repair_receipt_id is None
                and child_initial.successor_obligation_id is None
            )
        except (KeyError, TypeError, ValueError):
            authorized = False
    if not authorized:
        incidents["unauthorized_or_drifted_repair_redrive"] += 1
    closed = bool(authorized and child.state in _COVERED_STATES)
    if authorized and not closed and child.state.terminal:
        incidents["repair_redrive_without_safe_current_closure"] += 1
    return bool(authorized), closed


def _valid_child_work_binding(
    row: Mapping[str, Any],
    *,
    obligation: RepairObligation,
    incidents: Counter[str],
) -> bool:
    try:
        valid = (
            row["current_child_work_obligation_id"]
            == obligation.child_work_obligation_id
            and int(row["child_work_generation"]) == obligation.attempt
            and row["child_target_object_type"] == "repair_obligation"
            and row["child_target_object_id"] == obligation.obligation_id
            and row["child_owner_writer_id"] == "RepairLedgerApplier"
            and row["child_purpose"] == "execute_repair_obligation"
            and not bool(row["child_effect_possible"])
            and isinstance(
                WorkObligationState(str(row["child_work_state"])),
                WorkObligationState,
            )
        )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalid_repair_child_work_binding"] += 1
    return valid


def _valid_child_work_closure(
    row: Mapping[str, Any],
    *,
    obligation: RepairObligation,
    binding_valid: bool,
    incidents: Counter[str],
) -> bool:
    valid = binding_valid
    try:
        receipt = RepairReceipt.model_validate(_json(row["current_repair_receipt"]))
        if receipt.fate in {
            RepairObligationState.REPAIRED,
            RepairObligationState.NO_OP,
            RepairObligationState.ADJUDICATED_RESIDUE,
        }:
            resolution = LeaseResolution.model_validate(
                _json(row["child_completion_payload"])
            )
            receipt_result_id = row["repair_receipt_command_result_id"]
            valid = valid and (
                WorkObligationState(str(row["child_work_state"]))
                is WorkObligationState.COMPLETED
                and receipt_result_id is not None
                and resolution.obligation_id == obligation.child_work_obligation_id
                and resolution.obligation_generation == obligation.attempt
                and f"agency-command-result:{receipt_result_id}"
                in resolution.result_evidence_refs
                and resolution.to_work_state is WorkObligationState.COMPLETED
            )
        else:
            child_result = _json(row["child_result"])
            child_result_state = str(
                child_result.get("state") or child_result.get("work_state") or ""
            )
            valid = valid and (
                receipt.fate
                in {
                    RepairObligationState.EXHAUSTED,
                    RepairObligationState.ESCALATED,
                }
                and receipt.child_work_command_result_id is not None
                and row["child_result_writer_id"] == "WorkLedgerApplier"
                and row["child_result_object_type"] == "work_obligation"
                and row["child_result_object_id"]
                == obligation.child_work_obligation_id
                and int(row["child_result_object_version"])
                == int(row["child_work_version"])
                and child_result_state == str(row["child_work_state"])
                and WorkObligationState(child_result_state).terminal
                and f"agency-command-result:{receipt.child_work_command_result_id}"
                in receipt.proof_refs
            )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["repair_terminal_fate_without_closed_child_work"] += 1
    return valid


def _valid_receipt(
    row: Mapping[str, Any],
    *,
    obligation_models: Mapping[UUID, RepairObligation],
    obligation_groups: Mapping[UUID, Sequence[Mapping[str, Any]]],
    episode_models: Mapping[UUID, RepairEpisode],
    incidents: Counter[str],
) -> bool:
    try:
        receipt = RepairReceipt.model_validate(_json(row["receipt"]))
        obligation = obligation_models[receipt.repair_obligation_id]
        versions = obligation_groups[receipt.repair_obligation_id]
        receipt_version = next(
            item
            for item in versions
            if int(item["aggregate_version"]) == int(row["obligation_version"])
        )
        receipt_obligation = RepairObligation.model_validate(
            _json(receipt_version["obligation"])
        )
        episode = next(
            item
            for item in episode_models.values()
            if item.invalidation_request_id == receipt.invalidation_request_id
        )
        valid = (
            receipt.receipt_id == row["receipt_id"]
            and receipt.receipt_digest == row["receipt_digest"]
            and receipt.repair_obligation_id == row["repair_obligation_id"]
            and receipt.repair_generation == int(row["repair_generation"])
            and receipt.invalidation_request_id == row["invalidation_request_id"]
            and receipt.invalidation_epoch == int(row["invalidation_epoch"])
            and receipt.fate == row["fate"]
            and receipt.fate == receipt_obligation.state
            and receipt.receipt_id == receipt_obligation.repair_receipt_id
            and receipt.receipt_id == obligation.repair_receipt_id
            and receipt_version["command_result_id"] == row["command_result_id"]
            and receipt.completed_watermark.covers(episode.snapshot_watermark)
        )
        if receipt.fate is RepairObligationState.REPAIRED:
            valid = valid and (
                receipt.dependent_command_result_id is not None
                and row["dependent_result_writer_id"]
                == obligation.required_dependent_writer_id
                and row["dependent_result_command_kind"]
                == obligation.required_dependent_transition
                and row["dependent_result_object_type"]
                == obligation.dependent_object_type
                and row["dependent_result_object_id"] == obligation.dependent_object_id
                and int(row["dependent_result_object_version"])
                == obligation.expected_target_version
                and receipt.resulting_dependent_version
                == obligation.expected_target_version
            )
    except (KeyError, StopIteration, TypeError, ValueError):
        valid = False
    if not valid:
        incidents["invalid_repair_receipt"] += 1
    return valid


def _command_reconstructable(row: Mapping[str, Any]) -> bool:
    try:
        kind = str(row["command_kind"])
        command_type = {
            "apply_repair_episode": RepairEpisodeCommand,
            "apply_repair_obligation": RepairObligationCommand,
            "apply_repair_receipt": RepairReceiptCommand,
        }[kind]
        command = command_type.model_validate(_json(row["command"]))
        if kind == "apply_repair_episode":
            object_id = command.episode.episode_id
            object_type = "repair_episode"
            object_version = command.expected_version + 1
        elif kind == "apply_repair_obligation":
            object_id = command.obligation.obligation_id
            object_type = "repair_obligation"
            object_version = command.expected_version + 1
        else:
            object_id = command.receipt.repair_obligation_id
            object_type = "repair_obligation"
            object_version = command.expected_obligation_version + 1
        return bool(
            canonical_sha256(command.model_dump(mode="json")) == row["request_digest"]
            and command.context.command_id == row["command_id"]
            and command.context.tenant_id == row["tenant_id"]
            and command.context.idempotency_key == row["semantic_idempotency_key"]
            and command.context.processing_authority.fingerprint
            == row["processing_authority_fingerprint"]
            and command.context.writer_scope_epoch.scope_id == row["writer_scope_id"]
            and command.context.writer_scope_epoch.epoch == int(row["writer_epoch"])
            and row["writer_id"] == "RepairLedgerApplier"
            and row["object_type"] == object_type
            and row["object_id"] == object_id
            and int(row["object_version"]) == object_version
        )
    except (KeyError, TypeError, ValueError):
        return False


def build_repair_invariant_evidence(
    state: RepairEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    by_id = {item.invariant_id: item for item in registry.invariants}
    correction_denominator = (
        state.oracle_material_dependency_count
        if state.oracle_material_dependency_count is not None
        else state.known_material_dependency_count
    )
    correction_numerator = (
        state.oracle_covered_dependency_count
        if state.oracle_covered_dependency_count is not None
        else state.known_covered_dependency_count
    )
    repair_exposures = (
        state.repair_obligation_count
        + state.convergence_claim_count
        + state.child_work_obligation_count
        + state.terminal_child_work_required_count
        + (2 * state.repair_redrive_generation_count)
    )
    repair_safe = min(
        state.legal_obligation_count,
        state.dependency_bound_lineage_count,
        state.valid_repair_receipt_count,
    ) + (
        state.valid_convergence_claim_count
        + state.valid_child_work_binding_count
        + state.closed_child_work_count
        + state.authorized_repair_redrive_generation_count
        + state.closed_repair_redrive_generation_count
    )
    definitions = {
        "INV-14": (
            "inv.correction_closure",
            correction_numerator,
            correction_denominator,
            {
                "invalid_repair_dependency_edge",
                "invalidation_without_exact_source_commit",
                "repair_episode_population_mismatch",
                "invalid_repair_convergence_claim",
                "invalid_repair_receipt",
            },
        ),
        "INV-16": (
            "inv.repair_reconstructability",
            state.reconstructable_command_count,
            state.command_count,
            {"unreconstructable_repair_command"},
        ),
        "INV-24": (
            "inv.repair_closure",
            repair_safe,
            repair_exposures,
            {
                "invalid_repair_episode_history",
                "invalid_repair_obligation_history",
                "invalid_repair_dependency_or_lineage",
                "invalid_repair_child_work_binding",
                "repair_terminal_fate_without_closed_child_work",
                "unauthorized_or_drifted_repair_redrive",
                "repair_redrive_without_safe_current_closure",
                "invalid_repair_receipt",
                "repair_receipt_cardinality_mismatch",
                "repair_episode_population_mismatch",
                "invalid_repair_convergence_claim",
            },
        ),
        "INV-29": (
            "inv.repair_atomic_transport",
            min(
                state.reconstructable_command_count,
                _rate_numerator(state.command_event_coverage, state.command_count),
                _rate_numerator(state.command_outbox_coverage, state.command_count),
                _rate_numerator(
                    state.immutable_storage_guard_rate, state.command_count
                ),
            ),
            state.command_count,
            {
                "unreconstructable_repair_command",
                "repair_command_without_exact_event",
                "repair_command_without_exact_outbox",
                "immutable_repair_table_unguarded",
            },
        ),
    }
    rows = []
    for invariant_id, (metric_id, numerator, denominator_value, names) in (
        definitions.items()
    ):
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        violations = sum(state.incident_counts.get(name, 0) for name in names)
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:repair",
            denominator_version="repair-denominator-v1",
            population_definition_version="canonical-repair-component-v1",
            query_or_manifest_hash=canonical_sha256(
                {
                    "scope": state.scope.model_dump(mode="json"),
                    "invariant": invariant_id,
                }
            ),
            source_or_oracle_population=denominator_value,
            production_accepted=denominator_value,
            eligible=denominator_value,
            attempted_or_committed=denominator_value,
            terminal_fates={"covered": min(numerator, denominator_value)},
            nonterminal_fates={
                "uncovered": max(0, denominator_value - numerator)
            },
            report_cutoff=state.scope.end.isoformat(),
            population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
            population_partition_value="correction_invalidation_repair",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5 if "source" in name or "convergence" in name else 4,
                summary=f"Observed {state.incident_counts[name]} scoped {name} incidents.",
                artifact_refs=state.artifact_refs,
            )
            for name in sorted(names)
            if state.incident_counts.get(name, 0)
        )
        rows.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_value,
                observed_trace_facts=frozenset(
                    {
                        "source_command_result_and_invalidation_epoch",
                        "dependency_edges_and_repair_lineage_heads",
                        "repair_receipts_and_watermark_vectors",
                        "repair_child_work_and_receipt_result_closure",
                        "repair_redrive_authorization_and_successor_closure",
                        "object_event_result_and_outbox_ids",
                    }
                ),
                executed_scenario_ids=frozenset(
                    invariant.proof.suite_and_scenario_ids
                )
                & executed_scenario_ids,
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="repair-runtime-v1",
                        raw_numerator=float(numerator),
                        raw_denominator=float(denominator_value),
                        point_estimate=(
                            numerator / denominator_value
                            if denominator_value
                            else None
                        ),
                        violation_count=violations,
                        severity_mass=float(violations),
                        artifact_refs=state.artifact_refs,
                    ),
                ),
                incidents=incidents,
                achieved_evidence_tier=EvidenceTier.E3,
                denominator=denominator,
                uncertainty=state.uncertainty,
                blind_spots=state.uncertainty,
                artifact_refs=state.artifact_refs,
            )
        )
    return tuple(rows)


def render_repair_markdown(state: RepairEvaluationState) -> str:
    lines = [
        f"# Correction and repair evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        _metric_line(
            "Exact source-bound invalidations",
            state.exact_source_bound_request_count,
            state.invalidation_request_count,
            state.invalidation_source_binding_rate,
        ),
        _metric_line(
            "Valid DependencyEdges",
            state.valid_dependency_edge_count,
            state.dependency_edge_count,
            state.dependency_edge_validity_rate,
        ),
        _metric_line(
            "Legal RepairEpisode histories",
            state.legal_episode_count,
            state.repair_episode_count,
            state.episode_history_integrity_rate,
        ),
        _metric_line(
            "Valid convergence claims",
            state.valid_convergence_claim_count,
            state.convergence_claim_count,
            state.convergence_validity_rate,
        ),
        _metric_line(
            "Legal RepairObligation histories",
            state.legal_obligation_count,
            state.repair_obligation_count,
            state.obligation_history_integrity_rate,
        ),
        _metric_line(
            "Dependency-bound lineages",
            state.dependency_bound_lineage_count,
            state.repair_obligation_count,
            state.dependency_lineage_integrity_rate,
        ),
        _metric_line(
            "Authorized repair redrive generations",
            state.authorized_repair_redrive_generation_count,
            state.repair_redrive_generation_count,
            state.repair_redrive_authorization_rate,
        ),
        _metric_line(
            "Safely closed repair redrive generations",
            state.closed_repair_redrive_generation_count,
            state.repair_redrive_generation_count,
            state.repair_redrive_closure_rate,
        ),
        _metric_line(
            "Valid child Work bindings",
            state.valid_child_work_binding_count,
            state.child_work_obligation_count,
            state.child_work_binding_rate,
        ),
        _metric_line(
            "Closed terminal child Work",
            state.closed_child_work_count,
            state.terminal_child_work_required_count,
            state.child_work_closure_rate,
        ),
        _metric_line(
            "Valid repair receipts",
            state.valid_repair_receipt_count,
            state.receipt_required_obligation_count,
            state.repair_receipt_closure_rate,
        ),
        _metric_line(
            "Known material dependency closure",
            state.known_covered_dependency_count,
            state.known_material_dependency_count,
            state.known_dependency_coverage_rate,
        ),
        _metric_line(
            "Command reconstruction",
            state.reconstructable_command_count,
            state.command_count,
            state.command_reconstructability_rate,
        ),
        f"- Unresolved current repair obligations: **{state.unresolved_current_obligation_count}**",
        "",
        "## Repair obligation fates",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.obligation_fate_counts.items())
            if state.obligation_fate_counts
            else ("- no scoped repair obligations",)
        ),
        "",
        "## Incidents",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.incident_counts.items())
            if state.incident_counts
            else ("- none observed in this scope",)
        ),
        "",
        "## Proof limits",
        "",
        *(f"- {item}" for item in state.uncertainty),
        "",
    ]
    return "\n".join(lines)


def _group(
    rows: Sequence[Mapping[str, Any]], key: str
) -> dict[UUID, tuple[Mapping[str, Any], ...]]:
    grouped: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    return {
        item_id: tuple(sorted(items, key=lambda item: int(item["aggregate_version"])))
        for item_id, items in grouped.items()
    }


def _same_fields(left: Any, right: Any, names: tuple[str, ...]) -> bool:
    return all(getattr(left, name) == getattr(right, name) for name in names)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _rate_numerator(value: float | None, denominator: int) -> int:
    return round((value or 0.0) * denominator)


def _metric_line(
    label: str, numerator: int, denominator: int, rate: float | None
) -> str:
    rendered = f"{rate:.1%}" if rate is not None else "n/a"
    return f"- {label}: **{numerator}/{denominator} ({rendered})**"


__all__ = [
    "RepairEvaluationScope",
    "RepairEvaluationState",
    "analyze_repair_rows",
    "build_repair_invariant_evidence",
    "evaluate_repair_state",
    "render_repair_markdown",
]
