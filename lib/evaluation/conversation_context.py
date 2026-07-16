"""Continuous reconstruction of durable conversational context selection state."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.conversation_context import (
    CommitInterpretationContextCommand,
    ContextProbeEnvelope,
    ConversationContextCandidate,
)
from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import (
    InterpretationContextSnapshot,
    SelectionDependency,
    SufficiencyDisposition,
)
from lib.conversation_context_selection import select_context
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


class _ContextEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ConversationContextEvaluationScope(_ContextEvaluationModel):
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
            raise ValueError("context evaluation end must follow start")
        return self


class ConversationContextEvaluationState(_ContextEvaluationModel):
    scope: ConversationContextEvaluationScope
    head_count: int = Field(ge=0)
    valid_head_count: int = Field(ge=0)
    head_integrity_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    selection_count: int = Field(ge=0)
    reconstructable_selection_count: int = Field(ge=0)
    selection_reconstructability_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    replay_equivalent_selection_count: int = Field(ge=0)
    selection_replay_equivalence_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    candidate_count: int = Field(ge=0)
    candidate_with_probe_fate_count: int = Field(ge=0)
    candidate_probe_fate_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    required_probe_surface_count: int = Field(ge=0)
    completed_required_probe_surface_count: int = Field(ge=0)
    required_probe_surface_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    dependency_complete_selection_count: int = Field(ge=0)
    selection_dependency_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    command_event_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    command_outbox_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    immutable_table_count: int = Field(ge=0)
    guarded_immutable_table_count: int = Field(ge=0)
    immutable_storage_guard_rate: float = Field(ge=0.0, le=1.0)
    unsafe_selected_candidate_count: int = Field(ge=0)
    premature_sufficiency_count: int = Field(ge=0)
    disposition_counts: dict[str, int]
    incident_counts: dict[str, int]
    incident_refs: dict[str, tuple[str, ...]]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _dependency_complete(
    *,
    snapshot: InterpretationContextSnapshot,
    dependency: SelectionDependency,
) -> bool:
    selected = {item.event_revision_id for item in snapshot.selected_items}
    hypotheses = {
        hypothesis.content_hash for hypothesis in snapshot.embedded_episode_hypotheses
    }
    return (
        dependency.snapshot_id == snapshot.snapshot_id
        and dependency.snapshot_version == snapshot.snapshot_version
        and set(dependency.selected_event_revision_ids) == selected
        and set(dependency.embedded_hypothesis_hashes) == hypotheses
        and snapshot.request.source_topology_version in dependency.topology_versions
        and all(
            f"event-revision:{event_id}" in dependency.invalidation_keys
            for event_id in selected
        )
    )


def analyze_conversation_context_rows(
    *,
    scope: ConversationContextEvaluationScope,
    heads: Sequence[Mapping[str, Any]],
    snapshots: Sequence[Mapping[str, Any]],
    candidate_records: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    outboxes: Sequence[Mapping[str, Any]],
    immutable_tables: Sequence[str],
    guarded_immutable_tables: Sequence[str],
    artifact_refs: tuple[str, ...],
) -> ConversationContextEvaluationState:
    incidents: Counter[str] = Counter()
    incident_refs: dict[str, set[str]] = defaultdict(set)

    def incident(name: str, ref: str) -> None:
        incidents[name] += 1
        incident_refs[name].add(ref)

    snapshots_by_id = {UUID(str(row["id"])): row for row in snapshots}
    valid_heads = 0
    for head in heads:
        ref = f"context-head:{head['selection_key']}"
        snapshot_row = snapshots_by_id.get(UUID(str(head["current_snapshot_id"])))
        if snapshot_row is None:
            incident("context_head_missing_snapshot", ref)
            continue
        if (
            snapshot_row.get("selection_key") != head["selection_key"]
            or int(snapshot_row.get("aggregate_version") or 0)
            != int(head["current_aggregate_version"])
            or snapshot_row["snapshot_content_hash"]
            != head["current_snapshot_digest"]
            or snapshot_row.get("selection_decision_digest")
            != head["current_decision_digest"]
        ):
            incident("context_head_snapshot_mismatch", ref)
            continue
        valid_heads += 1

    candidate_by_result: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in candidate_records:
        candidate_by_result[UUID(str(row["command_result_id"]))].append(row)
    event_by_result: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in events:
        event_by_result[UUID(str(row["command_result_id"]))].append(row)
    outbox_by_event: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in outboxes:
        outbox_by_event[UUID(str(row["event_id"]))].append(row)

    reconstructable = 0
    replay_equivalent = 0
    candidate_total = 0
    candidate_fates = 0
    required_surfaces = 0
    completed_surfaces = 0
    dependency_complete = 0
    unsafe_selected = 0
    premature_sufficient = 0
    event_covered = 0
    outbox_covered = 0
    dispositions: Counter[str] = Counter()

    for row in commands:
        result_id = UUID(str(row["id"]))
        ref = f"context-command:{result_id}"
        result = _payload(row["result"])
        snapshot_id_raw = result.get("snapshot_id")
        snapshot_row = None
        try:
            snapshot_row = snapshots_by_id.get(UUID(str(snapshot_id_raw)))
        except (TypeError, ValueError):
            pass
        if snapshot_row is None:
            incident("context_command_missing_snapshot", ref)
            continue
        disposition = str(result.get("disposition") or "unknown")
        dispositions[disposition] += 1
        records = candidate_by_result.get(result_id, [])
        event_rows = event_by_result.get(result_id, [])
        if len(event_rows) == 1:
            event_covered += 1
            if len(outbox_by_event.get(UUID(str(event_rows[0]["id"])), [])) == 1:
                outbox_covered += 1
            else:
                incident("context_command_outbox_closure", ref)
        else:
            incident("context_command_event_closure", ref)

        try:
            command = CommitInterpretationContextCommand.model_validate(
                _payload(row["command"])
            )
            snapshot = InterpretationContextSnapshot.model_validate(
                _payload(snapshot_row["snapshot"])
            )
            dependency = SelectionDependency.model_validate(
                _payload(snapshot_row["selection_dependency"])
            )
        except Exception:
            incident("invalid_context_command_or_snapshot", ref)
            continue
        if command.request_digest != row["request_digest"]:
            incident("context_command_digest_mismatch", ref)
            continue
        reconstructable += 1
        expected_candidate_ids = {candidate.candidate_id for candidate in command.candidates}
        candidate_total += len(command.candidates)
        required = set(command.request.required_probe_surfaces)
        required_surfaces += len(required) * len(command.candidates)
        stored_candidate_ids: set[UUID] = set()
        selected_candidate_ids: set[UUID] = set()
        for candidate_row in records:
            candidate_ref = f"{ref}:candidate:{candidate_row['candidate_id']}"
            try:
                candidate = ConversationContextCandidate.model_validate(
                    _payload(candidate_row["candidate"])
                )
                probe = ContextProbeEnvelope.model_validate(
                    _payload(candidate_row["probe"])
                )
            except Exception:
                incident("invalid_context_candidate_or_probe", candidate_ref)
                continue
            stored_candidate_ids.add(candidate.candidate_id)
            candidate_fates += 1
            completed_surfaces += len(required & set(probe.completed_probe_surfaces))
            if bool(candidate_row["selected"]):
                selected_candidate_ids.add(candidate.candidate_id)
                if probe.probe.future_or_authority_incident_refs:
                    unsafe_selected += 1
                    incident("unsafe_context_candidate_selected", candidate_ref)
        if stored_candidate_ids != expected_candidate_ids:
            missing = expected_candidate_ids - stored_candidate_ids
            extra = stored_candidate_ids - expected_candidate_ids
            incident(
                "context_candidate_population_mismatch",
                f"{ref}:missing={sorted(map(str, missing))}:extra={sorted(map(str, extra))}",
            )
        candidate_manifest = canonical_sha256(
            tuple(sorted(candidate.candidate_content_hash for candidate in command.candidates))
        )
        probe_manifest = canonical_sha256(
            tuple(
                probe.model_dump(mode="json")
                for probe in sorted(command.probes, key=lambda item: str(item.candidate_id))
            )
        )
        if (
            candidate_manifest != snapshot_row["candidate_manifest_digest"]
            or probe_manifest != snapshot_row["probe_manifest_digest"]
        ):
            incident("context_candidate_or_probe_manifest_mismatch", ref)
        if _dependency_complete(snapshot=snapshot, dependency=dependency):
            dependency_complete += 1
        else:
            incident("context_selection_dependency_incomplete", ref)
        try:
            replayed = select_context(
                command,
                aggregate_version=int(snapshot_row["aggregate_version"]),
                snapshot_id=UUID(snapshot.snapshot_id),
                dependency_id=UUID(dependency.dependency_id),
                frozen_at=snapshot.frozen_at,
            )
        except Exception:
            incident("context_selection_replay_failed", ref)
            continue
        if (
            replayed.snapshot.snapshot_content_hash
            == snapshot.snapshot_content_hash
            and replayed.decision_digest == snapshot_row["selection_decision_digest"]
            and replayed.dependency.model_dump(mode="json")
            == dependency.model_dump(mode="json")
            and set(replayed.selected_candidate_ids) == selected_candidate_ids
        ):
            replay_equivalent += 1
        else:
            incident("context_selection_replay_diverged", ref)
        if (
            snapshot.sufficiency_verdict.disposition
            in {
                SufficiencyDisposition.OPERATIONALLY_SUFFICIENT,
                SufficiencyDisposition.MULTI_CONTEXT,
            }
            and not replayed.eligible_candidate_ids
        ):
            premature_sufficient += 1
            incident("context_premature_sufficiency", ref)

    guarded = set(guarded_immutable_tables)
    immutable = set(immutable_tables)
    for table in immutable - guarded:
        incident("context_immutable_guard_missing", f"table:{table}")
    command_count = len(commands)
    return ConversationContextEvaluationState(
        scope=scope,
        head_count=len(heads),
        valid_head_count=valid_heads,
        head_integrity_rate=_rate(valid_heads, len(heads)),
        selection_count=command_count,
        reconstructable_selection_count=reconstructable,
        selection_reconstructability_rate=_rate(reconstructable, command_count),
        replay_equivalent_selection_count=replay_equivalent,
        selection_replay_equivalence_rate=_rate(replay_equivalent, command_count),
        candidate_count=candidate_total,
        candidate_with_probe_fate_count=candidate_fates,
        candidate_probe_fate_coverage=_rate(candidate_fates, candidate_total),
        required_probe_surface_count=required_surfaces,
        completed_required_probe_surface_count=completed_surfaces,
        required_probe_surface_coverage=_rate(completed_surfaces, required_surfaces),
        dependency_complete_selection_count=dependency_complete,
        selection_dependency_coverage=_rate(dependency_complete, command_count),
        command_event_coverage=_rate(event_covered, command_count),
        command_outbox_coverage=_rate(outbox_covered, command_count),
        immutable_table_count=len(immutable),
        guarded_immutable_table_count=len(immutable & guarded),
        immutable_storage_guard_rate=(
            len(immutable & guarded) / len(immutable) if immutable else 0.0
        ),
        unsafe_selected_candidate_count=unsafe_selected,
        premature_sufficiency_count=premature_sufficient,
        disposition_counts=dict(sorted(dispositions.items())),
        incident_counts=dict(sorted(incidents.items())),
        incident_refs={
            key: tuple(sorted(values)) for key, values in sorted(incident_refs.items())
        },
        uncertainty=(
            "This E3 state evaluator proves registered context-selection protocol integrity, not that its probes are semantically accurate.",
            "No gold sufficient-set, context-contamination, coreference, edit/delete, long-range recurrence or cross-channel oracle is present in database state alone.",
            "Finite selected dependencies do not prove candidate discovery recall or that every legacy context builder is cut over.",
            "Live process crash/restart, source-tail advance, authority-revocation races and downstream repair convergence remain E4 work.",
        ),
        artifact_refs=artifact_refs,
    )


async def evaluate_conversation_context_state(
    conn: asyncpg.Connection,
    *,
    scope: ConversationContextEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> ConversationContextEvaluationState:
    heads = await conn.fetch(
        "SELECT * FROM interpretation_context_heads WHERE tenant_id=$1",
        scope.tenant_id,
    )
    snapshots = await conn.fetch(
        """
        SELECT * FROM interpretation_context_snapshots
        WHERE tenant_id=$1
          AND contract_version='conversation-context-selection-v1'
        """,
        scope.tenant_id,
    )
    commands = await conn.fetch(
        """
        SELECT * FROM agency_command_results
        WHERE tenant_id=$1 AND writer_id='GroundingAnnotationAppender'
          AND created_at >= $2 AND created_at < $3
        ORDER BY created_at, id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    result_ids = [row["id"] for row in commands]
    if result_ids:
        candidate_records = await conn.fetch(
            """
            SELECT * FROM conversation_context_candidate_records
            WHERE tenant_id=$1 AND command_result_id=ANY($2::uuid[])
            ORDER BY aggregate_version, candidate_id
            """,
            scope.tenant_id,
            result_ids,
        )
        events = await conn.fetch(
            """
            SELECT * FROM agency_canonical_events
            WHERE tenant_id=$1 AND command_result_id=ANY($2::uuid[])
            """,
            scope.tenant_id,
            result_ids,
        )
        event_ids = [row["id"] for row in events]
        outboxes = (
            await conn.fetch(
                """
                SELECT * FROM agency_outbox_records
                WHERE tenant_id=$1 AND event_id=ANY($2::uuid[])
                """,
                scope.tenant_id,
                event_ids,
            )
            if event_ids
            else []
        )
    else:
        candidate_records = []
        events = []
        outboxes = []
    immutable_tables = (
        "interpretation_context_snapshots",
        "conversation_context_candidate_records",
    )
    guarded = await conn.fetch(
        """
        SELECT c.relname AS table_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid=t.tgrelid
        WHERE NOT t.tgisinternal
          AND c.relname=ANY($1::text[])
          AND pg_get_triggerdef(t.oid) LIKE '%reject_consequential_immutable_mutation%'
        """,
        list(immutable_tables),
    )
    return analyze_conversation_context_rows(
        scope=scope,
        heads=heads,
        snapshots=snapshots,
        candidate_records=candidate_records,
        commands=commands,
        events=events,
        outboxes=outboxes,
        immutable_tables=immutable_tables,
        guarded_immutable_tables=tuple(row["table_name"] for row in guarded),
        artifact_refs=artifact_refs,
    )


def build_conversation_context_invariant_evidence(
    state: ConversationContextEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    by_id = {item.invariant_id: item for item in registry.invariants}
    definitions = {
        "INV-16": (
            "inv.reconstructability",
            state.replay_equivalent_selection_count,
            state.selection_count,
            ("complete_dependency_manifest", "object_event_and_result_ids"),
            ("context_selection_replay", "invalid_context_command"),
        ),
        "INV-25": (
            "inv.derived_embedding",
            state.dependency_complete_selection_count,
            state.selection_count,
            (
                "snapshot_and_selected_hypothesis_hash",
                "inherited_authority",
                "writer_scope",
            ),
            ("context_selection_dependency", "context_head"),
        ),
        "INV-27": (
            "inv.time_authority",
            state.selection_count - state.unsafe_selected_candidate_count,
            state.selection_count,
            (
                "temporal_mode_and_cutoff",
                "processing_and_consumption_fingerprints_and_epochs",
            ),
            ("unsafe_context", "future", "authority"),
        ),
        "INV-29": (
            "inv.transport_atomicity",
            min(
                state.selection_count,
                int((state.command_event_coverage or 0.0) * state.selection_count),
                int((state.command_outbox_coverage or 0.0) * state.selection_count),
            ),
            state.selection_count,
            (
                "command_key_hash_scope_and_epoch",
                "aggregate_version",
                "command_result",
                "required_outboxes",
            ),
            ("context_command_event", "context_command_outbox"),
        ),
    }
    rows: list[InvariantRunEvidence] = []
    for invariant_id, (
        metric_id,
        successes,
        denominator_count,
        observed,
        incident_prefixes,
    ) in definitions.items():
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5 if "unsafe" in name or "authority" in name else 4,
                summary=f"Observed {count} scoped {name} violations.",
                artifact_refs=state.artifact_refs,
            )
            for name, count in state.incident_counts.items()
            if any(prefix in name for prefix in incident_prefixes)
        )
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:context-selections",
            denominator_version="conversation-context-selection-denominator-v1",
            population_definition_version="registered-context-selection-commands-v1",
            query_or_manifest_hash=canonical_sha256(
                {
                    "scope": state.scope.model_dump(mode="json"),
                    "invariant_id": invariant_id,
                    "artifacts": state.artifact_refs,
                }
            ),
            source_or_oracle_population=denominator_count,
            production_accepted=denominator_count,
            eligible=denominator_count,
            attempted_or_committed=denominator_count,
            terminal_fates=state.disposition_counts,
            nonterminal_fates={},
            report_cutoff=state.scope.end.isoformat(),
            population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
            population_partition_value="conversation_context_selection",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        violations = max(0, denominator_count - successes)
        rows.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_count,
                observed_trace_facts=frozenset(observed),
                executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids,
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="conversation-context-selection-runtime-v1",
                        raw_numerator=float(successes),
                        raw_denominator=float(denominator_count),
                        point_estimate=(
                            successes / denominator_count
                            if denominator_count
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


def _fmt_rate(value: float | None) -> str:
    return "unknown/not exposed" if value is None else f"{value:.1%}"


def render_conversation_context_markdown(
    state: ConversationContextEvaluationState,
) -> str:
    return "\n".join(
        [
            f"# Conversational context evaluation: {state.scope.run_id}",
            "",
            f"- Tenant: `{state.scope.tenant_id}`",
            f"- Registered selections: **{state.selection_count}**",
            f"- Head integrity: **{_fmt_rate(state.head_integrity_rate)}**",
            f"- Command reconstructability: **{_fmt_rate(state.selection_reconstructability_rate)}**",
            f"- Independent replay equivalence: **{_fmt_rate(state.selection_replay_equivalence_rate)}**",
            f"- Candidate/probe fate coverage: **{_fmt_rate(state.candidate_probe_fate_coverage)}**",
            f"- Required probe-surface coverage: **{_fmt_rate(state.required_probe_surface_coverage)}**",
            f"- SelectionDependency coverage: **{_fmt_rate(state.selection_dependency_coverage)}**",
            "",
            "## Dispositions",
            "",
            *(
                (f"- {name}: {count}" for name, count in state.disposition_counts.items())
                if state.disposition_counts
                else ("- none exposed",)
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
    )


__all__ = [
    "ConversationContextEvaluationScope",
    "ConversationContextEvaluationState",
    "analyze_conversation_context_rows",
    "build_conversation_context_invariant_evidence",
    "evaluate_conversation_context_state",
    "render_conversation_context_markdown",
]
