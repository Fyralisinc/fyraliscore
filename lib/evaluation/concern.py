"""Continuous evaluation of attention governance and Concern integrity."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.agency import (
    AttentionGovernanceBinding,
    ConcernEvaluationCommand,
    ConcernIdentityCorrectionCommand,
    ConcernSnapshot,
    ConcernState,
    ConcernTransition,
    EffectiveAttentionGovernanceEnvelope,
    compose_attention_governance_bindings,
    derive_concern_id,
    reduce_concern_state,
)
from lib.contracts.kernel import canonical_sha256
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


class _ConcernEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ConcernEvaluationScope(_ConcernEvaluationModel):
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
            raise ValueError("Concern evaluation end must follow start")
        return self


class ConcernEvaluationState(_ConcernEvaluationModel):
    scope: ConcernEvaluationScope
    binding_count: int = Field(ge=0)
    valid_binding_count: int = Field(ge=0)
    binding_contract_validity_rate: float = Field(ge=0.0, le=1.0)
    concern_head_count: int = Field(ge=0)
    concern_state_counts: dict[str, int]
    version_count: int = Field(ge=0)
    valid_snapshot_count: int = Field(ge=0)
    snapshot_contract_validity_rate: float = Field(ge=0.0, le=1.0)
    reducer_conformant_count: int = Field(ge=0)
    reducer_conformance_rate: float = Field(ge=0.0, le=1.0)
    contributor_monotone_count: int = Field(ge=0)
    contributor_monotonicity_rate: float = Field(ge=0.0, le=1.0)
    envelope_conformant_count: int = Field(ge=0)
    binding_envelope_conformance_rate: float = Field(ge=0.0, le=1.0)
    disposition_capability_conformant_count: int = Field(ge=0)
    disposition_capability_conformance_rate: float = Field(ge=0.0, le=1.0)
    transition_conformant_count: int = Field(ge=0)
    transition_conformance_rate: float = Field(ge=0.0, le=1.0)
    command_count: int = Field(ge=0)
    reconstructable_command_count: int = Field(ge=0)
    command_reconstructability_rate: float = Field(ge=0.0, le=1.0)
    command_version_coverage: float = Field(ge=0.0, le=1.0)
    command_event_coverage: float = Field(ge=0.0, le=1.0)
    command_outbox_coverage: float = Field(ge=0.0, le=1.0)
    identity_correction_count: int = Field(ge=0)
    reciprocal_identity_correction_count: int = Field(ge=0)
    identity_correction_reciprocity_rate: float = Field(ge=0.0, le=1.0)
    incident_counts: dict[str, int]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


async def evaluate_concern_state(
    conn: asyncpg.Connection,
    *,
    scope: ConcernEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> ConcernEvaluationState:
    bindings = await conn.fetch(
        """
        SELECT * FROM attention_governance_bindings
        WHERE tenant_id = $1
        ORDER BY created_at, binding_ref
        """,
        scope.tenant_id,
    )
    heads = await conn.fetch(
        """
        SELECT * FROM concern_heads
        WHERE tenant_id = $1
        ORDER BY concern_id
        """,
        scope.tenant_id,
    )
    versions = await conn.fetch(
        """
        SELECT v.*,
               t.transition,
               t.transitioned_at,
               r.command_kind,
               r.command,
               r.request_digest,
               r.processing_authority_fingerprint,
               r.consumption_authority_fingerprint,
               (SELECT count(*) FROM concern_canonical_events e
                 WHERE e.tenant_id = v.tenant_id
                   AND e.concern_id = v.concern_id
                   AND e.aggregate_version = v.aggregate_version) AS event_count,
               (SELECT count(*)
                  FROM concern_canonical_events e
                  JOIN concern_outbox_records o ON o.event_id = e.id
                 WHERE e.tenant_id = v.tenant_id
                   AND e.concern_id = v.concern_id
                   AND e.aggregate_version = v.aggregate_version) AS outbox_count
        FROM concern_versions v
        LEFT JOIN concern_transitions t
          ON t.tenant_id = v.tenant_id
         AND t.concern_id = v.concern_id
         AND t.to_version = v.aggregate_version
        JOIN concern_command_results r ON r.id = v.command_result_id
        WHERE v.tenant_id = $1
          AND v.transaction_at >= $2 AND v.transaction_at < $3
        ORDER BY v.concern_id, v.aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    commands = await conn.fetch(
        """
        SELECT r.*,
               (SELECT count(*) FROM concern_versions v
                 WHERE v.tenant_id = r.tenant_id
                   AND v.command_result_id = r.id) AS version_count,
               (SELECT count(*) FROM concern_canonical_events e
                 WHERE e.tenant_id = r.tenant_id
                   AND e.command_result_id = r.id) AS event_count,
               (SELECT count(*)
                  FROM concern_canonical_events e
                  JOIN concern_outbox_records o ON o.event_id = e.id
                 WHERE e.tenant_id = r.tenant_id
                   AND e.command_result_id = r.id) AS outbox_count
        FROM concern_command_results r
        WHERE r.tenant_id = $1
          AND r.created_at >= $2 AND r.created_at < $3
        ORDER BY r.created_at, r.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    corrections = await conn.fetch(
        """
        SELECT * FROM concern_identity_corrections
        WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
        ORDER BY created_at, id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    return analyze_concern_rows(
        scope=scope,
        bindings=bindings,
        heads=heads,
        versions=versions,
        commands=commands,
        corrections=corrections,
        artifact_refs=artifact_refs,
    )


def analyze_concern_rows(
    *,
    scope: ConcernEvaluationScope,
    bindings: Sequence[Mapping[str, Any]],
    heads: Sequence[Mapping[str, Any]],
    versions: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    corrections: Sequence[Mapping[str, Any]],
    artifact_refs: tuple[str, ...],
) -> ConcernEvaluationState:
    parsed_bindings: dict[str, AttentionGovernanceBinding] = {}
    valid_binding_count = 0
    binding_digest_failures = 0
    for row in bindings:
        try:
            binding = AttentionGovernanceBinding.model_validate(_json(row["binding"]))
            valid = (
                binding.binding_ref == row["binding_ref"]
                and binding.binding_digest == row["binding_digest"]
                and binding.attention_source_ref == row["attention_source_ref"]
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            binding = None
        if valid and binding is not None:
            valid_binding_count += 1
            parsed_bindings[binding.binding_ref] = binding
        else:
            binding_digest_failures += 1

    head_by_id = {row["concern_id"]: row for row in heads}
    state_counts = Counter(str(row["current_state"]) for row in heads)
    dedupe_counts = Counter(str(row["dedupe_key"]) for row in heads)
    dedupe_collisions = sum(count - 1 for count in dedupe_counts.values() if count > 1)
    versions_by_concern: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in versions:
        versions_by_concern[row["concern_id"]].append(row)

    valid_snapshots = 0
    reducer_conformant = 0
    contributor_monotone = 0
    envelope_conformant = 0
    disposition_conformant = 0
    transition_conformant = 0
    reducer_failures = 0
    contributor_losses = 0
    envelope_failures = 0
    disposition_failures = 0
    transition_failures = 0
    protocol_version_failures = 0
    protocol_event_failures = 0
    protocol_outbox_failures = 0
    head_mismatches = 0
    previous_contributors: dict[UUID, frozenset[str]] = {}

    for row in versions:
        try:
            snapshot = ConcernSnapshot.model_validate(_json(row["snapshot"]))
            snapshot_valid = (
                snapshot.concern_id == row["concern_id"]
                and snapshot.aggregate_version == row["aggregate_version"]
                and snapshot.state.value == row["state"]
                and snapshot.concern_id == derive_concern_id(snapshot.identity)
                and canonical_sha256(snapshot.model_dump(mode="json"))
                == row["snapshot_digest"]
            )
        except (TypeError, ValueError, KeyError):
            snapshot_valid = False
            snapshot = None
        valid_snapshots += int(snapshot_valid)
        if not snapshot_valid or snapshot is None:
            reducer_failures += 1
            contributor_losses += 1
            envelope_failures += 1
            disposition_failures += 1
            transition_failures += 1
            continue

        transitioned_at = row.get("transitioned_at") or snapshot.evidence_cutoff
        if snapshot.aggregate_version == 1:
            expected_state = ConcernState.CANDIDATE
        else:
            expected_state = reduce_concern_state(
                criteria=snapshot.criteria,
                at=transitioned_at,
                gap_identity_valid=snapshot.gap_identity_valid,
                validity_deadline=snapshot.validity_deadline,
            )
        reducer_ok = snapshot.state is expected_state
        reducer_conformant += int(reducer_ok)
        reducer_failures += int(not reducer_ok)

        prior_refs = previous_contributors.get(snapshot.concern_id, frozenset())
        contributors_ok = prior_refs <= snapshot.contributing_attention_source_refs
        contributor_monotone += int(contributors_ok)
        contributor_losses += int(not contributors_ok)
        previous_contributors[snapshot.concern_id] = (
            snapshot.contributing_attention_source_refs
        )

        try:
            envelope = EffectiveAttentionGovernanceEnvelope.model_validate(
                _json(row["effective_binding_envelope"])
            )
            envelope_ok = (
                envelope.envelope_digest == row["effective_binding_digest"]
            )
            active = {
                item.attention_binding_ref: parsed_bindings[item.attention_binding_ref]
                for item in snapshot.criteria
                if item.applicable and item.attention_binding_ref in parsed_bindings
            }
            expected_refs = {
                item.attention_binding_ref for item in snapshot.criteria if item.applicable
            }
            if active and set(active) == expected_refs and all(
                item.is_live(transitioned_at) for item in active.values()
            ):
                expected_envelope = compose_attention_governance_bindings(
                    tuple(active.values()), at=transitioned_at
                )
                envelope_ok = envelope_ok and envelope == expected_envelope
        except (TypeError, ValueError, KeyError):
            envelope_ok = False
        envelope_conformant += int(envelope_ok)
        envelope_failures += int(not envelope_ok)

        disposition_ok = True
        for criterion in snapshot.criteria:
            if criterion.disposition is None:
                continue
            binding = parsed_bindings.get(criterion.attention_binding_ref)
            required = (
                binding.disposition_capability_refs.get(criterion.disposition)
                if binding
                else None
            )
            if required != criterion.disposition_capability_ref:
                disposition_ok = False
        disposition_conformant += int(disposition_ok)
        disposition_failures += int(not disposition_ok)

        try:
            transition = ConcernTransition.model_validate(_json(row["transition"]))
            transition_ok = (
                transition.concern_id == snapshot.concern_id
                and transition.to_version == snapshot.aggregate_version
                and transition.to_state is snapshot.state
            )
        except (TypeError, ValueError, KeyError):
            transition_ok = False
        transition_conformant += int(transition_ok)
        transition_failures += int(not transition_ok)
        protocol_version_failures += int(row.get("command_kind") is None)
        protocol_event_failures += int(int(row.get("event_count") or 0) != 1)
        protocol_outbox_failures += int(int(row.get("outbox_count") or 0) != 1)

    for concern_id, scoped_versions in versions_by_concern.items():
        head = head_by_id.get(concern_id)
        if head is None:
            head_mismatches += 1
            continue
        latest = max(scoped_versions, key=lambda item: int(item["aggregate_version"]))
        # Only assert exact head equality when the scoped population contains the
        # current version; a later version outside the interval is legitimate.
        if int(head["current_version"]) == int(latest["aggregate_version"]):
            head_mismatches += int(head["current_state"] != latest["state"])

    reconstructable_commands = 0
    command_version_covered = 0
    command_event_covered = 0
    command_outbox_covered = 0
    command_failures = 0
    for row in commands:
        payload = _json(row.get("command"))
        valid = False
        expected_records = 2 if row.get("command_kind") == "correct_identity" else 1
        try:
            if row.get("command_kind") == "evaluate":
                command = ConcernEvaluationCommand.model_validate(payload)
                valid = (
                    command.request_digest == row["request_digest"]
                    and command.processing_authority.fingerprint
                    == row["processing_authority_fingerprint"]
                    and command.consumption_authority.fingerprint
                    == row["consumption_authority_fingerprint"]
                )
            elif row.get("command_kind") == "correct_identity":
                correction = ConcernIdentityCorrectionCommand.model_validate(payload)
                valid = (
                    correction.request_digest == row["request_digest"]
                    and correction.successor.processing_authority.fingerprint
                    == row["processing_authority_fingerprint"]
                    and correction.successor.consumption_authority.fingerprint
                    == row["consumption_authority_fingerprint"]
                )
        except (TypeError, ValueError, KeyError):
            valid = False
        reconstructable_commands += int(valid)
        command_failures += int(not valid)
        command_version_covered += int(int(row.get("version_count") or 0) == expected_records)
        command_event_covered += int(int(row.get("event_count") or 0) == expected_records)
        command_outbox_covered += int(int(row.get("outbox_count") or 0) == expected_records)

    reciprocal_corrections = 0
    correction_failures = 0
    for row in corrections:
        predecessor = head_by_id.get(row["predecessor_concern_id"])
        successor = head_by_id.get(row["successor_concern_id"])
        reciprocal = bool(
            predecessor
            and successor
            and predecessor["current_state"] == ConcernState.INVALIDATED.value
            and predecessor["successor_concern_id"] == row["successor_concern_id"]
            and successor["predecessor_concern_id"] == row["predecessor_concern_id"]
        )
        reciprocal_corrections += int(reciprocal)
        correction_failures += int(not reciprocal)

    incident_counts = {
        "invalid_attention_binding_contract": binding_digest_failures,
        "concern_dedupe_collision": dedupe_collisions,
        "invalid_concern_snapshot": len(versions) - valid_snapshots,
        "concern_reducer_mismatch": reducer_failures,
        "concern_contributor_lost": contributor_losses,
        "attention_binding_envelope_mismatch": envelope_failures,
        "concern_disposition_capability_mismatch": disposition_failures,
        "invalid_concern_transition": transition_failures,
        "concern_version_without_command": protocol_version_failures,
        "concern_version_without_event": protocol_event_failures,
        "concern_version_without_outbox": protocol_outbox_failures,
        "concern_head_mismatch": head_mismatches,
        "unreconstructable_concern_command": command_failures,
        "concern_command_without_versions": len(commands) - command_version_covered,
        "concern_command_without_events": len(commands) - command_event_covered,
        "concern_command_without_outboxes": len(commands) - command_outbox_covered,
        "concern_identity_correction_not_reciprocal": correction_failures,
    }
    incident_counts = {key: value for key, value in incident_counts.items() if value > 0}
    return ConcernEvaluationState(
        scope=scope,
        binding_count=len(bindings),
        valid_binding_count=valid_binding_count,
        binding_contract_validity_rate=_ratio(valid_binding_count, len(bindings)),
        concern_head_count=len(heads),
        concern_state_counts=dict(sorted(state_counts.items())),
        version_count=len(versions),
        valid_snapshot_count=valid_snapshots,
        snapshot_contract_validity_rate=_ratio(valid_snapshots, len(versions)),
        reducer_conformant_count=reducer_conformant,
        reducer_conformance_rate=_ratio(reducer_conformant, len(versions)),
        contributor_monotone_count=contributor_monotone,
        contributor_monotonicity_rate=_ratio(contributor_monotone, len(versions)),
        envelope_conformant_count=envelope_conformant,
        binding_envelope_conformance_rate=_ratio(envelope_conformant, len(versions)),
        disposition_capability_conformant_count=disposition_conformant,
        disposition_capability_conformance_rate=_ratio(
            disposition_conformant, len(versions)
        ),
        transition_conformant_count=transition_conformant,
        transition_conformance_rate=_ratio(transition_conformant, len(versions)),
        command_count=len(commands),
        reconstructable_command_count=reconstructable_commands,
        command_reconstructability_rate=_ratio(
            reconstructable_commands, len(commands)
        ),
        command_version_coverage=_ratio(command_version_covered, len(commands)),
        command_event_coverage=_ratio(command_event_covered, len(commands)),
        command_outbox_coverage=_ratio(command_outbox_covered, len(commands)),
        identity_correction_count=len(corrections),
        reciprocal_identity_correction_count=reciprocal_corrections,
        identity_correction_reciprocity_rate=_ratio(
            reciprocal_corrections, len(corrections)
        ),
        incident_counts=incident_counts,
        uncertainty=(
            "This E3 slice proves Concern protocol mechanics, not that materiality or impact estimates are correct.",
            "CriteriaProjector source eligibility and passive-source attempts require live integration denominators.",
            "Human interruption value, fatigue, burden and downstream product value require simulation or pilot evidence.",
            "Cross-worker concurrency and repair convergence require E4 fault and replay scenarios.",
        ),
        artifact_refs=artifact_refs,
    )


def build_concern_invariant_evidence(
    state: ConcernEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    by_id = {item.invariant_id: item for item in registry.invariants}
    definitions = {
        "INV-20": (
            "inv.concern_control",
            min(state.reducer_conformant_count, state.envelope_conformant_count),
            state.version_count,
            {"concern_reducer_mismatch", "attention_binding_envelope_mismatch"},
        ),
        "INV-23": (
            "inv.protocol_completion",
            min(
                round(state.command_version_coverage * state.command_count),
                round(state.command_event_coverage * state.command_count),
                round(state.command_outbox_coverage * state.command_count),
            ),
            state.command_count,
            {
                "concern_command_without_versions",
                "concern_command_without_events",
                "concern_command_without_outboxes",
            },
        ),
        "INV-37": (
            "inv.human_attention",
            min(
                state.envelope_conformant_count,
                state.disposition_capability_conformant_count,
            ),
            state.version_count,
            {
                "attention_binding_envelope_mismatch",
                "concern_disposition_capability_mismatch",
            },
        ),
    }
    rows = []
    for invariant_id, (metric_id, numerator, denominator_value, incident_names) in definitions.items():
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        violations = sum(
            count
            for name, count in state.incident_counts.items()
            if name in incident_names
        )
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5 if "capability" in name else 4,
                summary=f"Observed {count} scoped {name} incidents.",
                artifact_refs=state.artifact_refs,
            )
            for name, count in state.incident_counts.items()
            if name in incident_names
        )
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:concern",
            denominator_version="governed-concern-denominator-v1",
            population_definition_version="concern-version-command-union-v1",
            query_or_manifest_hash=canonical_sha256(
                {"scope": state.scope.model_dump(mode="json"), "invariant": invariant_id}
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
            population_partition_value="governed_concern",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        rows.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_value,
                observed_trace_facts=frozenset(
                    {
                        "attention_binding_versions",
                        "concern_snapshot_and_transition",
                        "command_result_event_outbox",
                        "disposition_capability",
                    }
                ),
                executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids,
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="governed-concern-runtime-v1",
                        raw_numerator=float(numerator),
                        raw_denominator=float(denominator_value),
                        point_estimate=(
                            numerator / denominator_value if denominator_value else None
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


def render_concern_markdown(state: ConcernEvaluationState) -> str:
    lines = [
        f"# Attention-governance and Concern evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        f"- Interval: `{state.scope.start.isoformat()}` to `{state.scope.end.isoformat()}`",
        f"- Binding contract validity: **{state.valid_binding_count}/{state.binding_count} ({state.binding_contract_validity_rate:.1%})**",
        f"- Snapshot validity: **{state.valid_snapshot_count}/{state.version_count} ({state.snapshot_contract_validity_rate:.1%})**",
        f"- Reducer conformance: **{state.reducer_conformant_count}/{state.version_count} ({state.reducer_conformance_rate:.1%})**",
        f"- Contributor monotonicity: **{state.contributor_monotone_count}/{state.version_count} ({state.contributor_monotonicity_rate:.1%})**",
        f"- Binding-envelope conformance: **{state.envelope_conformant_count}/{state.version_count} ({state.binding_envelope_conformance_rate:.1%})**",
        f"- Command reconstructability: **{state.reconstructable_command_count}/{state.command_count} ({state.command_reconstructability_rate:.1%})**",
        f"- Identity-correction reciprocity: **{state.reciprocal_identity_correction_count}/{state.identity_correction_count} ({state.identity_correction_reciprocity_rate:.1%})**",
        "",
        "## Current Concern states",
        "",
        *(f"- {name}: {count}" for name, count in state.concern_state_counts.items()),
        "",
        "## Constitutional and structural incidents",
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


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 1.0


__all__ = [
    "ConcernEvaluationScope",
    "ConcernEvaluationState",
    "analyze_concern_rows",
    "build_concern_invariant_evidence",
    "evaluate_concern_state",
    "render_concern_markdown",
]
