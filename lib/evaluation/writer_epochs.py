"""Continuous reconstruction of WriterScopeEpoch ownership and cutover state."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Mapping, Sequence
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.kernel import WriterCutoverState, canonical_sha256
from lib.contracts.writer_epochs import (
    ActivateWriterTransferCommand,
    AdvanceWriterScopeCommand,
    FenceWriterTransferCommand,
    MergeWriterScopesCommand,
    RegisterWriterScopeCommand,
    RetireWriterScopeCommand,
    SplitWriterScopeCommand,
    WriterScopeProofKind,
    WriterScopeVersion,
    writer_scope_advance_allowed,
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


class _WriterEpochEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class WriterEpochEvaluationScope(_WriterEpochEvaluationModel):
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
    def interval_is_forward(self):
        if self.end <= self.start:
            raise ValueError("writer-epoch evaluation end must follow start")
        return self


class WriterEpochEvaluationState(_WriterEpochEvaluationModel):
    scope: WriterEpochEvaluationScope
    head_count: int = Field(ge=0)
    valid_head_count: int = Field(ge=0)
    head_integrity_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    version_count: int = Field(ge=0)
    legal_version_count: int = Field(ge=0)
    lifecycle_conformance_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    epoch_transition_count: int = Field(ge=0)
    valid_epoch_transition_count: int = Field(ge=0)
    epoch_monotonicity_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    active_scope_count: int = Field(ge=0)
    claimed_partition_count: int = Field(ge=0)
    valid_partition_claim_count: int = Field(ge=0)
    partition_claim_integrity_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    cutover_command_count: int = Field(ge=0)
    proof_complete_command_count: int = Field(ge=0)
    cutover_proof_coverage_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    fence_version_count: int = Field(ge=0)
    valid_fence_version_count: int = Field(ge=0)
    no_writer_fence_integrity_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    split_command_count: int = Field(ge=0)
    conserved_split_count: int = Field(ge=0)
    split_conservation_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    merge_command_count: int = Field(ge=0)
    conserved_merge_count: int = Field(ge=0)
    merge_conservation_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    command_count: int = Field(ge=0)
    reconstructable_command_count: int = Field(ge=0)
    command_reconstructability_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    command_event_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    command_outbox_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    immutable_table_count: int = Field(ge=0)
    guarded_immutable_table_count: int = Field(ge=0)
    immutable_storage_guard_rate: float = Field(ge=0.0, le=1.0)
    current_state_counts: dict[str, int]
    incident_counts: dict[str, int]
    incident_refs: dict[str, tuple[str, ...]]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


_COMMAND_TYPES = {
    cls.__name__: cls
    for cls in (
        RegisterWriterScopeCommand,
        AdvanceWriterScopeCommand,
        FenceWriterTransferCommand,
        ActivateWriterTransferCommand,
        SplitWriterScopeCommand,
        MergeWriterScopesCommand,
        RetireWriterScopeCommand,
    )
}


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _required_proofs(command_kind: str, command: Mapping[str, Any]) -> set[str]:
    if command_kind == "RegisterWriterScopeCommand":
        return {
            (
                WriterScopeProofKind.BOOTSTRAP_MANIFEST
                if command.get("bootstrap_root")
                else WriterScopeProofKind.PARTITION_COVERAGE
            ).value
        }
    if command_kind == "AdvanceWriterScopeCommand":
        return {
            "adapter_enforced": {WriterScopeProofKind.ADAPTER_COMPATIBILITY.value},
            "backfilling": {WriterScopeProofKind.BACKFILL_MANIFEST.value},
            "catch_up": {WriterScopeProofKind.CATCH_UP_COMPLETE.value},
            "verified": {
                WriterScopeProofKind.SEMANTIC_EQUIVALENCE.value,
                WriterScopeProofKind.AUTHORITY_EQUIVALENCE.value,
            },
            "legacy": {WriterScopeProofKind.ROLLBACK.value},
        }.get(str(command.get("to_state")), set())
    return {
        "FenceWriterTransferCommand": {
            WriterScopeProofKind.CATCH_UP_COMPLETE.value,
            WriterScopeProofKind.SEMANTIC_EQUIVALENCE.value,
            WriterScopeProofKind.AUTHORITY_EQUIVALENCE.value,
            WriterScopeProofKind.REPRESENTABILITY.value,
        },
        "ActivateWriterTransferCommand": {
            WriterScopeProofKind.FENCE_ACKNOWLEDGED.value
        },
        "SplitWriterScopeCommand": {WriterScopeProofKind.PARTITION_COVERAGE.value},
        "MergeWriterScopesCommand": {WriterScopeProofKind.PARTITION_COVERAGE.value},
        "RetireWriterScopeCommand": {
            WriterScopeProofKind.CONSUMER_DRAIN.value,
            WriterScopeProofKind.SEMANTIC_EQUIVALENCE.value,
            WriterScopeProofKind.REPAIR_RESIDUE_CLOSED.value,
        },
    }.get(command_kind, set())


def analyze_writer_epoch_rows(
    *,
    scope: WriterEpochEvaluationScope,
    heads: Sequence[Mapping[str, Any]],
    versions: Sequence[Mapping[str, Any]],
    claims: Sequence[Mapping[str, Any]],
    proofs: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    outboxes: Sequence[Mapping[str, Any]],
    immutable_tables: Sequence[str],
    guarded_immutable_tables: Sequence[str],
    artifact_refs: tuple[str, ...],
) -> WriterEpochEvaluationState:
    incidents: Counter[str] = Counter()
    incident_refs: dict[str, set[str]] = defaultdict(set)

    def incident(name: str, ref: str) -> None:
        incidents[name] += 1
        incident_refs[name].add(ref)

    parsed_versions: list[tuple[Mapping[str, Any], WriterScopeVersion]] = []
    by_scope: dict[UUID, list[tuple[Mapping[str, Any], WriterScopeVersion]]] = defaultdict(list)
    by_result: dict[UUID, list[WriterScopeVersion]] = defaultdict(list)
    for row in versions:
        ref = f"writer-scope:{row['scope_id']}:v{row['aggregate_version']}"
        try:
            version = WriterScopeVersion.model_validate(_payload(row["version"]))
        except Exception:  # evaluator must localize corrupt durable rows
            incident("invalid_writer_scope_version", ref)
            continue
        parsed_versions.append((row, version))
        by_scope[version.scope_id].append((row, version))
        by_result[UUID(str(row["command_result_id"]))].append(version)

    for history in by_scope.values():
        history.sort(key=lambda item: item[1].aggregate_version)

    command_by_id = {UUID(str(row["id"])): row for row in commands}
    event_results = {UUID(str(row["command_result_id"])) for row in events}
    outbox_results = {UUID(str(row["command_result_id"])) for row in outboxes}
    proofs_by_result: dict[UUID, set[str]] = defaultdict(set)
    for row in proofs:
        proofs_by_result[UUID(str(row["command_result_id"]))].add(
            str(row["proof_kind"])
        )

    legal_versions = 0
    epoch_transitions = 0
    valid_epoch_transitions = 0
    fence_versions = 0
    valid_fences = 0
    for scope_id, history in by_scope.items():
        for index, (row, current) in enumerate(history):
            ref = f"writer-scope:{scope_id}:v{current.aggregate_version}"
            valid = True
            if current.aggregate_version != index + 1:
                valid = False
            if index == 0:
                if current.aggregate_version != 1:
                    valid = False
            else:
                previous = history[index - 1][1]
                epoch_transitions += 1
                command_kind = str(
                    command_by_id.get(UUID(str(row["command_result_id"])), {}).get(
                        "command_kind", ""
                    )
                )
                ordinary = writer_scope_advance_allowed(previous.state, current.state)
                fence = (
                    command_kind == "FenceWriterTransferCommand"
                    and previous.state is WriterCutoverState.VERIFIED
                    and current.state is WriterCutoverState.WRITER_FENCED
                    and current.epoch == previous.epoch + 1
                    and current.writer_owner == previous.writer_owner
                )
                activate = (
                    command_kind == "ActivateWriterTransferCommand"
                    and previous.state is WriterCutoverState.WRITER_FENCED
                    and current.state is WriterCutoverState.NEW_CANONICAL
                    and current.epoch == previous.epoch
                    and current.writer_owner == previous.pending_writer_owner
                )
                retire = (
                    command_kind
                    in {
                        "RetireWriterScopeCommand",
                        "SplitWriterScopeCommand",
                        "MergeWriterScopesCommand",
                    }
                    and current.state is WriterCutoverState.RETIRED
                    and current.epoch == previous.epoch + 1
                )
                ordinary_epoch = ordinary and current.epoch == previous.epoch
                epoch_valid = ordinary_epoch or fence or activate or retire
                if epoch_valid:
                    valid_epoch_transitions += 1
                else:
                    incident("illegal_writer_epoch_transition", ref)
                    valid = False
                if (
                    current.source_partitions != previous.source_partitions
                    or current.semantic_responsibility
                    != previous.semantic_responsibility
                ):
                    incident("writer_scope_identity_drift", ref)
                    valid = False
            if current.state is WriterCutoverState.WRITER_FENCED:
                fence_versions += 1
                if current.pending_writer_owner and not current.embedded_epoch(
                    source_partition=current.source_partitions[0]
                ).permits(
                    writer_owner=current.writer_owner,
                    epoch=current.epoch,
                    tenant_id=current.tenant_id,
                    semantic_responsibility=current.semantic_responsibility,
                    source_partition=current.source_partitions[0],
                ):
                    valid_fences += 1
                else:
                    incident("invalid_no_writer_fence", ref)
                    valid = False
            if valid:
                legal_versions += 1
            else:
                incident("illegal_writer_scope_history", ref)

    version_by_key = {
        (version.tenant_id, version.scope_id, version.aggregate_version): version
        for _, version in parsed_versions
    }
    valid_heads = 0
    head_by_scope: dict[UUID, Mapping[str, Any]] = {}
    for head in heads:
        scope_id = UUID(str(head["scope_id"]))
        head_by_scope[scope_id] = head
        key = (scope.tenant_id, scope_id, int(head["current_aggregate_version"]))
        version = version_by_key.get(key)
        if version is not None and (
            version.version_digest == head["current_version_digest"]
            and version.epoch == int(head["current_epoch"])
            and version.state.value == head["current_state"]
            and version.writer_owner == head["writer_owner"]
            and tuple(head["source_partitions"]) == version.source_partitions
        ):
            valid_heads += 1
        else:
            incident("writer_scope_head_version_mismatch", f"writer-scope:{scope_id}")

    claims_by_scope: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for claim in claims:
        claims_by_scope[UUID(str(claim["scope_id"]))].append(claim)
    valid_claims = 0
    for scope_id, head in head_by_scope.items():
        current_claims = claims_by_scope.get(scope_id, [])
        expected_parts = set(head["source_partitions"])
        if head["current_state"] == WriterCutoverState.RETIRED.value:
            if current_claims:
                for claim in current_claims:
                    incident(
                        "retired_scope_retains_partition_claim",
                        f"writer-scope:{scope_id}:{claim['source_partition']}",
                    )
            continue
        for claim in current_claims:
            if (
                claim["source_partition"] in expected_parts
                and claim["semantic_responsibility"]
                == head["semantic_responsibility"]
                and int(claim["scope_epoch"]) == int(head["current_epoch"])
                and int(claim["scope_aggregate_version"])
                == int(head["current_aggregate_version"])
            ):
                valid_claims += 1
            else:
                incident(
                    "invalid_writer_partition_claim",
                    f"writer-scope:{scope_id}:{claim['source_partition']}",
                )
        missing = expected_parts - {
            str(claim["source_partition"]) for claim in current_claims
        }
        for partition in sorted(missing):
            incident(
                "missing_writer_partition_claim",
                f"writer-scope:{scope_id}:{partition}",
            )

    proof_complete = 0
    reconstructable = 0
    for command_id, row in command_by_id.items():
        kind = str(row["command_kind"])
        payload = _payload(row["command"])
        cls = _COMMAND_TYPES.get(kind)
        try:
            parsed = cls.model_validate(payload) if cls else None
            if parsed is None or parsed.request_digest != row["request_digest"]:
                raise ValueError("command digest mismatch")
        except Exception:
            incident("unreconstructable_writer_scope_command", f"command:{command_id}")
        else:
            reconstructable += 1
        required = _required_proofs(kind, payload)
        if required and required <= proofs_by_result.get(command_id, set()):
            proof_complete += 1
        else:
            incident("incomplete_writer_cutover_proof", f"command:{command_id}")
        if command_id not in event_results or command_id not in outbox_results:
            incident("partial_writer_command_bundle", f"command:{command_id}")

    split_count = 0
    conserved_splits = 0
    merge_count = 0
    conserved_merges = 0
    for command_id, row in command_by_id.items():
        kind = str(row["command_kind"])
        payload = _payload(row["command"])
        committed = by_result.get(command_id, [])
        if kind == "SplitWriterScopeCommand":
            split_count += 1
            parent_id = UUID(str(payload["expected_parent"]["scope_id"]))
            parent_terminal = next(
                (v for v in committed if v.scope_id == parent_id and v.state is WriterCutoverState.RETIRED),
                None,
            )
            child_ids = {UUID(str(item["scope_id"])) for item in payload["children"]}
            children = [v for v in committed if v.scope_id in child_ids and v.aggregate_version == 1]
            previous = None
            if parent_terminal:
                previous = version_by_key.get(
                    (
                        scope.tenant_id,
                        parent_id,
                        parent_terminal.aggregate_version - 1,
                    )
                )
            child_parts = [part for child in children for part in child.source_partitions]
            if (
                previous
                and len(children) == len(child_ids)
                and len(child_parts) == len(set(child_parts))
                and set(child_parts) == set(previous.source_partitions)
                and all(child.parent_scope_ids == (parent_id,) for child in children)
            ):
                conserved_splits += 1
            else:
                incident("writer_scope_split_nonconservation", f"command:{command_id}")
        elif kind == "MergeWriterScopesCommand":
            merge_count += 1
            merged_id = UUID(str(payload["merged_scope_id"]))
            merged = next(
                (v for v in committed if v.scope_id == merged_id and v.aggregate_version == 1),
                None,
            )
            parent_ids = {
                UUID(str(item["scope_id"])) for item in payload["expected_parents"]
            }
            parents = []
            for parent_id in parent_ids:
                terminal = next(
                    (
                        v
                        for v in committed
                        if v.scope_id == parent_id and v.state is WriterCutoverState.RETIRED
                    ),
                    None,
                )
                if terminal:
                    previous = version_by_key.get(
                        (
                            scope.tenant_id,
                            parent_id,
                            terminal.aggregate_version - 1,
                        )
                    )
                    if previous:
                        parents.append(previous)
            parent_parts = [part for parent in parents for part in parent.source_partitions]
            if (
                merged
                and len(parents) == len(parent_ids)
                and len(parent_parts) == len(set(parent_parts))
                and set(parent_parts) == set(merged.source_partitions)
                and set(merged.parent_scope_ids) == parent_ids
            ):
                conserved_merges += 1
            else:
                incident("writer_scope_merge_nonconservation", f"command:{command_id}")

    guarded = len(set(immutable_tables) & set(guarded_immutable_tables))
    for table in set(immutable_tables) - set(guarded_immutable_tables):
        incident("mutable_writer_scope_history", table)
    active_scopes = sum(
        1 for head in heads if head["current_state"] != WriterCutoverState.RETIRED.value
    )
    event_covered = sum(1 for command_id in command_by_id if command_id in event_results)
    outbox_covered = sum(1 for command_id in command_by_id if command_id in outbox_results)
    return WriterEpochEvaluationState(
        scope=scope,
        head_count=len(heads),
        valid_head_count=valid_heads,
        head_integrity_rate=_rate(valid_heads, len(heads)),
        version_count=len(versions),
        legal_version_count=legal_versions,
        lifecycle_conformance_rate=_rate(legal_versions, len(versions)),
        epoch_transition_count=epoch_transitions,
        valid_epoch_transition_count=valid_epoch_transitions,
        epoch_monotonicity_rate=_rate(valid_epoch_transitions, epoch_transitions),
        active_scope_count=active_scopes,
        claimed_partition_count=len(claims),
        valid_partition_claim_count=valid_claims,
        partition_claim_integrity_rate=_rate(valid_claims, len(claims)),
        cutover_command_count=len(commands),
        proof_complete_command_count=proof_complete,
        cutover_proof_coverage_rate=_rate(proof_complete, len(commands)),
        fence_version_count=fence_versions,
        valid_fence_version_count=valid_fences,
        no_writer_fence_integrity_rate=_rate(valid_fences, fence_versions),
        split_command_count=split_count,
        conserved_split_count=conserved_splits,
        split_conservation_rate=_rate(conserved_splits, split_count),
        merge_command_count=merge_count,
        conserved_merge_count=conserved_merges,
        merge_conservation_rate=_rate(conserved_merges, merge_count),
        command_count=len(commands),
        reconstructable_command_count=reconstructable,
        command_reconstructability_rate=_rate(reconstructable, len(commands)),
        command_event_coverage=_rate(event_covered, len(commands)),
        command_outbox_coverage=_rate(outbox_covered, len(commands)),
        immutable_table_count=len(immutable_tables),
        guarded_immutable_table_count=guarded,
        immutable_storage_guard_rate=(guarded / len(immutable_tables) if immutable_tables else 1.0),
        current_state_counts=dict(
            sorted(Counter(str(head["current_state"]) for head in heads).items())
        ),
        incident_counts=dict(sorted(incidents.items())),
        incident_refs={
            name: tuple(sorted(refs)) for name, refs in sorted(incident_refs.items())
        },
        uncertainty=(
            "This is E3 canonical-registry and transaction evidence, not E4 live producer cutover or process-crash evidence.",
            "Exact finite partition claims prove registered disjointness; they do not prove every legacy write entrypoint has been inventoried and routed through the registry.",
            "The tenant WriterEpochApplier root is bootstrapped and intentionally cannot transfer or retire without a future external constitutional root.",
            "Existing semantic appliers do not yet all call validate_live_writer_scope inside their write transaction, so global stale-writer rejection remains incomplete.",
            "Watermark objects and typed proof digests are structurally reconstructed; live source replay, rebalance and semantic-equivalence procedures require E4 scenarios.",
        ),
        artifact_refs=artifact_refs,
    )


async def evaluate_writer_epoch_state(
    conn: asyncpg.Connection,
    *,
    scope: WriterEpochEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> WriterEpochEvaluationState:
    heads = await conn.fetch(
        "SELECT * FROM writer_scope_heads WHERE tenant_id=$1 ORDER BY scope_id",
        scope.tenant_id,
    )
    versions = await conn.fetch(
        """
        SELECT * FROM writer_scope_versions
        WHERE tenant_id=$1 AND recorded_at < $2
        ORDER BY scope_id, aggregate_version
        """,
        scope.tenant_id,
        scope.end,
    )
    claims = await conn.fetch(
        """
        SELECT * FROM writer_scope_partition_claims
        WHERE tenant_id=$1 ORDER BY semantic_responsibility, source_partition
        """,
        scope.tenant_id,
    )
    proofs = await conn.fetch(
        """
        SELECT * FROM writer_scope_transition_proofs
        WHERE tenant_id=$1 AND observed_at < $2
        ORDER BY observed_at, proof_id
        """,
        scope.tenant_id,
        scope.end,
    )
    commands = await conn.fetch(
        """
        SELECT DISTINCT r.* FROM agency_command_results r
        JOIN writer_scope_versions v ON v.command_result_id=r.id
        WHERE r.tenant_id=$1 AND r.writer_id='WriterEpochApplier'
          AND v.recorded_at < $2
        ORDER BY r.created_at, r.id
        """,
        scope.tenant_id,
        scope.end,
    )
    events = await conn.fetch(
        """
        SELECT e.command_result_id FROM agency_canonical_events e
        JOIN agency_command_results r ON r.id=e.command_result_id
        WHERE r.tenant_id=$1 AND r.writer_id='WriterEpochApplier'
        """,
        scope.tenant_id,
    )
    outboxes = await conn.fetch(
        """
        SELECT e.command_result_id FROM agency_outbox_records o
        JOIN agency_canonical_events e ON e.id=o.event_id
        JOIN agency_command_results r ON r.id=e.command_result_id
        WHERE r.tenant_id=$1 AND r.writer_id='WriterEpochApplier'
        """,
        scope.tenant_id,
    )
    immutable_tables = ("writer_scope_versions", "writer_scope_transition_proofs")
    trigger_rows = await conn.fetch(
        """
        SELECT c.relname FROM pg_trigger t
        JOIN pg_class c ON c.oid=t.tgrelid
        WHERE NOT t.tgisinternal AND t.tgenabled <> 'D'
          AND c.relname=ANY($1::text[])
        """,
        list(immutable_tables),
    )
    return analyze_writer_epoch_rows(
        scope=scope,
        heads=heads,
        versions=versions,
        claims=claims,
        proofs=proofs,
        commands=commands,
        events=events,
        outboxes=outboxes,
        immutable_tables=immutable_tables,
        guarded_immutable_tables=tuple(row["relname"] for row in trigger_rows),
        artifact_refs=artifact_refs,
    )


def build_writer_epoch_invariant_evidence(
    state: WriterEpochEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    definitions = {
        "INV-08": (
            "inv.axis_conformance",
            state.valid_partition_claim_count,
            state.claimed_partition_count,
            {"invalid_writer_partition_claim", "missing_writer_partition_claim"},
            {"writer_scope_and_epoch", "transition_identity"},
        ),
        "INV-16": (
            "inv.reconstructability",
            state.reconstructable_command_count,
            state.command_count,
            {"unreconstructable_writer_scope_command"},
            {
                "object_event_and_result_ids",
                "authority_context",
                "trace_and_artifact_hashes",
            },
        ),
        "INV-29": (
            "inv.transport_atomicity",
            min(
                state.reconstructable_command_count,
                round((state.command_event_coverage or 0.0) * state.command_count),
                round((state.command_outbox_coverage or 0.0) * state.command_count),
            ),
            state.command_count,
            {"partial_writer_command_bundle", "illegal_writer_epoch_transition"},
            {
                "command_key_hash_scope_and_epoch",
                "aggregate_version",
                "command_result",
                "required_outboxes",
            },
        ),
        "INV-35": (
            "inv.inference_topology",
            state.valid_head_count,
            state.head_count,
            {"writer_scope_head_version_mismatch"},
            {"commit_commands_and_results", "writer_scopes"},
        ),
    }
    by_id = {item.invariant_id: item for item in registry.invariants}
    evidence = []
    for invariant_id, (
        metric_id,
        numerator,
        denominator_value,
        incident_names,
        facts,
    ) in definitions.items():
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        violations = sum(state.incident_counts.get(name, 0) for name in incident_names)
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:writer-scope",
            denominator_version="writer-scope-epoch-denominator-v1",
            population_definition_version="writer-scope-inception-to-cutoff-v1",
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
            population_partition_value="writer_scope_epoch",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5,
                summary=f"Observed {state.incident_counts[name]} scoped {name} incidents.",
                artifact_refs=state.artifact_refs,
            )
            for name in sorted(incident_names)
            if state.incident_counts.get(name, 0)
        )
        evidence.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_value,
                observed_trace_facts=frozenset(facts),
                executed_scenario_ids=(
                    frozenset(invariant.proof.suite_and_scenario_ids)
                    & executed_scenario_ids
                ),
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="writer-scope-epoch-runtime-v1",
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
    return tuple(evidence)


def render_writer_epoch_markdown(state: WriterEpochEvaluationState) -> str:
    def fmt(value: float | None) -> str:
        return f"{value:.1%}" if value is not None else "unknown/not exposed"

    metrics = (
        ("Head/version integrity", state.valid_head_count, state.head_count, state.head_integrity_rate),
        ("Lifecycle conformance", state.legal_version_count, state.version_count, state.lifecycle_conformance_rate),
        ("Epoch monotonicity", state.valid_epoch_transition_count, state.epoch_transition_count, state.epoch_monotonicity_rate),
        ("Partition-claim integrity", state.valid_partition_claim_count, state.claimed_partition_count, state.partition_claim_integrity_rate),
        ("Typed cutover proofs", state.proof_complete_command_count, state.cutover_command_count, state.cutover_proof_coverage_rate),
        ("No-writer fences", state.valid_fence_version_count, state.fence_version_count, state.no_writer_fence_integrity_rate),
        ("Split conservation", state.conserved_split_count, state.split_command_count, state.split_conservation_rate),
        ("Merge conservation", state.conserved_merge_count, state.merge_command_count, state.merge_conservation_rate),
        ("Command reconstruction", state.reconstructable_command_count, state.command_count, state.command_reconstructability_rate),
    )
    lines = [
        f"# Writer-scope epoch evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        f"- Scope: `{state.scope.start.isoformat()}` to `{state.scope.end.isoformat()}`",
        "",
        "## State vector",
        "",
        *(f"- {label}: **{n}/{d} ({fmt(rate)})**" for label, n, d, rate in metrics),
        f"- Command event coverage: **{fmt(state.command_event_coverage)}**",
        f"- Command outbox coverage: **{fmt(state.command_outbox_coverage)}**",
        f"- Immutable guards: **{state.guarded_immutable_table_count}/{state.immutable_table_count} ({state.immutable_storage_guard_rate:.1%})**",
        "",
        "## Current states",
        "",
        *(f"- `{name}`: {count}" for name, count in state.current_state_counts.items()),
        "",
        "## Incidents",
        "",
        *(
            (
                f"- {name}: {count} ({', '.join(state.incident_refs.get(name, ()))})"
                for name, count in state.incident_counts.items()
            )
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


__all__ = [
    "WriterEpochEvaluationScope",
    "WriterEpochEvaluationState",
    "analyze_writer_epoch_rows",
    "build_writer_epoch_invariant_evidence",
    "evaluate_writer_epoch_state",
    "render_writer_epoch_markdown",
]
