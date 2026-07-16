"""Continuous evaluation of proposal, acceptance, and intent-apply integrity."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.agency import (
    ExactProposalAcceptance,
    InterpretedIntentProposal,
    TypedConstitutiveIntentCommand,
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


_TERMINAL_PROPOSAL_FATES = frozenset(
    {"accepted_for_authorization", "rejected", "expired", "superseded"}
)
_DIRECT_THINK_INTENT_OPS = frozenset(
    {
        "create_goal",
        "update_goal",
        "transition_goal",
        "create_commitment",
        "transition_commitment",
        "create_decision",
        "transition_decision",
        "add_edge_contributes_to",
        "add_edge_depends_on",
        "add_edge_constrained_by",
    }
)


class _IntentEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class IntentEvaluationScope(_IntentEvaluationModel):
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
            raise ValueError("intent evaluation end must follow start")
        return self


class IntentEvaluationState(_IntentEvaluationModel):
    scope: IntentEvaluationScope
    proposal_count: int = Field(ge=0)
    proposal_fate_counts: dict[str, int]
    terminal_proposal_count: int = Field(ge=0)
    overdue_open_proposal_count: int = Field(ge=0)
    proposal_contract_valid_count: int = Field(ge=0)
    proposal_contract_validity_rate: float = Field(ge=0.0, le=1.0)
    accepted_proposal_count: int = Field(ge=0)
    exact_acceptance_count: int = Field(ge=0)
    exact_acceptance_coverage: float = Field(ge=0.0, le=1.0)
    acceptance_digest_mismatch_count: int = Field(ge=0)
    accepted_with_applied_command_count: int = Field(ge=0)
    accepted_apply_coverage: float = Field(ge=0.0, le=1.0)
    command_count: int = Field(ge=0)
    complete_authority_capture_count: int = Field(ge=0)
    command_reconstructability_rate: float = Field(ge=0.0, le=1.0)
    command_request_mismatch_count: int = Field(ge=0)
    command_version_coverage: float = Field(ge=0.0, le=1.0)
    command_event_coverage: float = Field(ge=0.0, le=1.0)
    command_outbox_coverage: float = Field(ge=0.0, le=1.0)
    think_intent_opportunity_count: int = Field(ge=0)
    think_proposal_count: int = Field(ge=0)
    think_proposal_coverage: float = Field(ge=0.0, le=1.0)
    think_direct_intent_mutation_summary_count: int = Field(ge=0)
    acted_recommendation_count: int = Field(ge=0)
    governed_recommendation_action_count: int = Field(ge=0)
    governed_recommendation_action_coverage: float = Field(ge=0.0, le=1.0)
    legacy_baseline_count: int = Field(ge=0)
    incident_counts: dict[str, int]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


async def evaluate_intent_state(
    conn: asyncpg.Connection,
    *,
    scope: IntentEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> IntentEvaluationState:
    proposals = await conn.fetch(
        """
        SELECT * FROM intent_proposals
        WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
        ORDER BY created_at, id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    proposal_ids = [row["id"] for row in proposals]
    acceptances = (
        await conn.fetch(
            """
            SELECT * FROM intent_exact_acceptances
            WHERE tenant_id = $1 AND proposal_id = ANY($2::uuid[])
            """,
            scope.tenant_id,
            proposal_ids,
        )
        if proposal_ids
        else []
    )
    commands = await conn.fetch(
        """
        SELECT r.*,
               EXISTS (
                 SELECT 1 FROM intent_versions v
                 WHERE v.tenant_id = r.tenant_id AND v.command_result_id = r.id
               ) AS has_version,
               EXISTS (
                 SELECT 1 FROM intent_canonical_events e
                 WHERE e.tenant_id = r.tenant_id AND e.command_result_id = r.id
               ) AS has_event,
               EXISTS (
                 SELECT 1
                 FROM intent_canonical_events e
                 JOIN intent_outbox_records o ON o.event_id = e.id
                 WHERE e.tenant_id = r.tenant_id AND e.command_result_id = r.id
               ) AS has_outbox
        FROM intent_command_results r
        WHERE r.tenant_id = $1 AND r.created_at >= $2 AND r.created_at < $3
        ORDER BY r.created_at, r.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    think_runs = await conn.fetch(
        """
        SELECT id, ops_applied FROM think_runs
        WHERE tenant_id = $1 AND started_at >= $2 AND started_at < $3
          AND ops_applied IS NOT NULL
        ORDER BY started_at, id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    acted_recommendations = await conn.fetch(
        """
        SELECT id, caused_act_change_id
        FROM models
        WHERE tenant_id = $1
          AND claim_role = 'recommendation'
          AND archive_reason = 'acted_upon'
          AND archived_at >= $2 AND archived_at < $3
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    legacy_baseline_count = await conn.fetchval(
        """
        SELECT count(*) FROM intent_legacy_baselines
        WHERE tenant_id = $1 AND captured_at >= $2 AND captured_at < $3
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    return analyze_intent_rows(
        scope=scope,
        proposals=proposals,
        acceptances=acceptances,
        commands=commands,
        think_runs=think_runs,
        acted_recommendations=acted_recommendations,
        legacy_baseline_count=int(legacy_baseline_count or 0),
        artifact_refs=artifact_refs,
    )


def analyze_intent_rows(
    *,
    scope: IntentEvaluationScope,
    proposals: Sequence[Mapping[str, Any]],
    acceptances: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    think_runs: Sequence[Mapping[str, Any]],
    acted_recommendations: Sequence[Mapping[str, Any]],
    legacy_baseline_count: int,
    artifact_refs: tuple[str, ...],
) -> IntentEvaluationState:
    fate_counts = Counter(str(row["fate"]) for row in proposals)
    acceptance_by_proposal = {row["proposal_id"]: row for row in acceptances}
    commands_by_acceptance = {
        row["proposal_acceptance_id"]: row
        for row in commands
        if row.get("proposal_acceptance_id") is not None
    }
    valid_proposals = 0
    acceptance_digest_mismatches = 0
    accepted_with_command = 0
    accepted_count = fate_counts.get("accepted_for_authorization", 0)
    overdue = 0
    for row in proposals:
        payload = _json(row["proposal"])
        try:
            parsed = InterpretedIntentProposal.model_validate(payload)
            valid = (
                parsed.normalized_payload_digest
                == parsed.normalized_mutation.payload_digest
                and row["proposal_digest"] == canonical_sha256(payload)
                and row["normalized_payload_digest"]
                == parsed.normalized_payload_digest
            )
        except (ValueError, TypeError):
            valid = False
        valid_proposals += int(valid)
        if row["fate"] in {"open", "deferred"} and row["review_due_at"] < scope.end:
            overdue += 1
        acceptance = acceptance_by_proposal.get(row["id"])
        if acceptance is not None:
            acceptance_payload = _json(acceptance["acceptance"])
            try:
                parsed_acceptance = ExactProposalAcceptance.model_validate(
                    acceptance_payload
                )
                exact = (
                    parsed_acceptance.proposal_digest == row["proposal_digest"]
                    and parsed_acceptance.normalized_payload_digest
                    == row["normalized_payload_digest"]
                    and acceptance["proposal_version"] == row["proposal_version"]
                )
            except (ValueError, TypeError):
                exact = False
            acceptance_digest_mismatches += int(not exact)
            command = commands_by_acceptance.get(acceptance["id"])
            accepted_with_command += int(
                row["fate"] == "accepted_for_authorization"
                and command is not None
                and command["status"] in {"applied", "duplicate"}
            )

    complete_commands = 0
    request_mismatches = 0
    version_count = 0
    event_count = 0
    outbox_count = 0
    for row in commands:
        command_payload = _json(row.get("command"))
        valid = False
        if row.get("authority_capture_status") == "complete" and isinstance(
            command_payload, dict
        ):
            try:
                command = TypedConstitutiveIntentCommand.model_validate(command_payload)
                valid = (
                    command.request_digest == row["request_digest"]
                    and command.processing_authority.fingerprint
                    == row["processing_authority_fingerprint"]
                    and command.consumption_authority.fingerprint
                    == row["consumption_authority_fingerprint"]
                )
            except (ValueError, TypeError):
                valid = False
        complete_commands += int(valid)
        request_mismatches += int(not valid)
        version_count += int(bool(row.get("has_version")))
        event_count += int(bool(row.get("has_event")))
        outbox_count += int(bool(row.get("has_outbox")))

    proposal_ids = {str(row["id"]) for row in proposals}
    think_opportunities = 0
    think_proposals = 0
    think_direct = 0
    for row in think_runs:
        ops = _json(row["ops_applied"])
        for item in ops.get("act_ops", ()) if isinstance(ops, dict) else ():
            if not isinstance(item, dict):
                continue
            operation = item.get("op")
            if operation == "propose_intent":
                think_opportunities += 1
                if str(item.get("intent_proposal_id")) in proposal_ids:
                    think_proposals += 1
            elif operation in _DIRECT_THINK_INTENT_OPS:
                think_opportunities += 1
                think_direct += 1

    governed_acted = 0
    command_aggregate_ids = {
        row["aggregate_id"]
        for row in commands
        if row["status"] in {"applied", "duplicate"}
    }
    for row in acted_recommendations:
        governed_acted += int(row["caused_act_change_id"] in command_aggregate_ids)

    incident_counts = {
        "invalid_proposal_contract": len(proposals) - valid_proposals,
        "overdue_open_proposal": overdue,
        "accepted_without_exact_acceptance": max(
            0, accepted_count - len(acceptances)
        ),
        "acceptance_digest_mismatch": acceptance_digest_mismatches,
        "accepted_without_applied_command": max(0, accepted_count - accepted_with_command),
        "unreconstructable_intent_command": request_mismatches,
        "command_without_intent_version": len(commands) - version_count,
        "command_without_canonical_event": len(commands) - event_count,
        "command_without_outbox": len(commands) - outbox_count,
        "think_directly_mutated_intent": think_direct,
        "think_intent_opportunity_without_proposal": max(
            0, think_opportunities - think_proposals - think_direct
        ),
        "recommendation_action_bypassed_intent_protocol": max(
            0, len(acted_recommendations) - governed_acted
        ),
    }
    incident_counts = {key: value for key, value in incident_counts.items() if value > 0}
    return IntentEvaluationState(
        scope=scope,
        proposal_count=len(proposals),
        proposal_fate_counts=dict(sorted(fate_counts.items())),
        terminal_proposal_count=sum(
            count for fate, count in fate_counts.items() if fate in _TERMINAL_PROPOSAL_FATES
        ),
        overdue_open_proposal_count=overdue,
        proposal_contract_valid_count=valid_proposals,
        proposal_contract_validity_rate=_ratio(valid_proposals, len(proposals)),
        accepted_proposal_count=accepted_count,
        exact_acceptance_count=len(acceptances),
        exact_acceptance_coverage=_ratio(len(acceptances), accepted_count),
        acceptance_digest_mismatch_count=acceptance_digest_mismatches,
        accepted_with_applied_command_count=accepted_with_command,
        accepted_apply_coverage=_ratio(accepted_with_command, accepted_count),
        command_count=len(commands),
        complete_authority_capture_count=complete_commands,
        command_reconstructability_rate=_ratio(complete_commands, len(commands)),
        command_request_mismatch_count=request_mismatches,
        command_version_coverage=_ratio(version_count, len(commands)),
        command_event_coverage=_ratio(event_count, len(commands)),
        command_outbox_coverage=_ratio(outbox_count, len(commands)),
        think_intent_opportunity_count=think_opportunities,
        think_proposal_count=think_proposals,
        think_proposal_coverage=_ratio(think_proposals, think_opportunities),
        think_direct_intent_mutation_summary_count=think_direct,
        acted_recommendation_count=len(acted_recommendations),
        governed_recommendation_action_count=governed_acted,
        governed_recommendation_action_coverage=_ratio(
            governed_acted, len(acted_recommendations)
        ),
        legacy_baseline_count=legacy_baseline_count,
        incident_counts=incident_counts,
        uncertainty=(
            "This runtime slice measures protocol integrity, not whether the authorized objective was wise.",
            "Direct structured source-contract and delegated-policy intent paths are not yet live in this scope.",
            "Authority-basis correction and survival repair require longitudinal correction scenarios.",
            "Belief invariance under intent-only perturbation requires the paired E4 metamorphic suite.",
        ),
        artifact_refs=artifact_refs,
    )


def build_intent_invariant_evidence(
    state: IntentEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    by_id = {item.invariant_id: item for item in registry.invariants}
    definitions = {
        "INV-13": (
            "inv.intent_admission",
            state.accepted_with_applied_command_count,
            state.accepted_proposal_count,
            state.acceptance_digest_mismatch_count
            + max(
                0,
                state.accepted_proposal_count
                - state.accepted_with_applied_command_count,
            ),
        ),
        "INV-16": (
            "inv.reconstructability",
            state.complete_authority_capture_count,
            state.command_count,
            state.command_request_mismatch_count,
        ),
        "INV-23": (
            "inv.protocol_completion",
            min(
                round(state.command_version_coverage * state.command_count),
                round(state.command_event_coverage * state.command_count),
                round(state.command_outbox_coverage * state.command_count),
            ),
            state.command_count,
            state.incident_counts.get("command_without_intent_version", 0)
            + state.incident_counts.get("command_without_canonical_event", 0)
            + state.incident_counts.get("command_without_outbox", 0),
        ),
        "INV-33": (
            "inv.intent_acquisition",
            state.think_proposal_count,
            state.think_intent_opportunity_count,
            state.think_direct_intent_mutation_summary_count
            + state.incident_counts.get("think_intent_opportunity_without_proposal", 0),
        ),
    }
    rows = []
    for invariant_id, (metric_id, numerator, denominator_value, violations) in definitions.items():
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:intent",
            denominator_version="governed-intent-denominator-v1",
            population_definition_version="proposal-command-think-opportunity-union-v1",
            query_or_manifest_hash=canonical_sha256(
                {"scope": state.scope.model_dump(mode="json"), "invariant": invariant_id}
            ),
            source_or_oracle_population=denominator_value,
            production_accepted=denominator_value,
            eligible=denominator_value,
            attempted_or_committed=numerator,
            terminal_fates={"covered": numerator},
            nonterminal_fates={
                "uncovered": max(0, denominator_value - numerator)
            },
            report_cutoff=state.scope.end.isoformat(),
            population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
            population_partition_value="governed_intent",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5 if "bypass" in name or "directly" in name else 4,
                summary=f"Observed {count} scoped {name} incidents.",
                artifact_refs=state.artifact_refs,
            )
            for name, count in state.incident_counts.items()
        )
        rows.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_value,
                observed_trace_facts=frozenset(
                    {
                        "proposal_and_payload_digest",
                        "exact_acceptance",
                        "authority_basis",
                        "command_result",
                        "canonical_event_and_outbox",
                    }
                ),
                executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids,
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="governed-intent-runtime-v1",
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


def render_intent_markdown(state: IntentEvaluationState) -> str:
    lines = [
        f"# Governed-intent evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        f"- Interval: `{state.scope.start.isoformat()}` to `{state.scope.end.isoformat()}`",
        f"- Proposal contract validity: **{state.proposal_contract_valid_count}/{state.proposal_count} ({state.proposal_contract_validity_rate:.1%})**",
        f"- Exact acceptance coverage: **{state.exact_acceptance_count}/{state.accepted_proposal_count} ({state.exact_acceptance_coverage:.1%})**",
        f"- Accepted-to-applied coverage: **{state.accepted_with_applied_command_count}/{state.accepted_proposal_count} ({state.accepted_apply_coverage:.1%})**",
        f"- Command reconstructability: **{state.complete_authority_capture_count}/{state.command_count} ({state.command_reconstructability_rate:.1%})**",
        f"- Think proposal coverage: **{state.think_proposal_count}/{state.think_intent_opportunity_count} ({state.think_proposal_coverage:.1%})**",
        f"- Governed recommendation actions: **{state.governed_recommendation_action_count}/{state.acted_recommendation_count} ({state.governed_recommendation_action_coverage:.1%})**",
        "",
        "## Proposal fates",
        "",
        *(f"- {fate}: {count}" for fate, count in state.proposal_fate_counts.items()),
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
    "IntentEvaluationScope",
    "IntentEvaluationState",
    "analyze_intent_rows",
    "build_intent_invariant_evidence",
    "evaluate_intent_state",
    "render_intent_markdown",
]
