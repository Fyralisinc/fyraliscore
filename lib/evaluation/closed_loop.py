"""Cross-plane evaluation for one complete intervention and feedback loop.

Component evaluators prove their own writers and reducers.  This module proves
the missing joins between them without acquiring any semantic write authority.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from typing import Any, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.agency import (
    Attribution,
    ConsequentialProposal,
    EpisodeStageFate,
    InterventionEpisode,
    Outcome,
)
from lib.contracts.kernel import canonical_sha256
from lib.evaluation.agency import AgencyEvaluationScope, evaluate_agency_state
from lib.evaluation.concern import ConcernEvaluationScope, evaluate_concern_state
from lib.evaluation.execution import (
    ExecutionEvaluationScope,
    evaluate_execution_state,
)
from lib.evaluation.intent import IntentEvaluationScope, evaluate_intent_state
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
from lib.evaluation.source_semantics import (
    SourceSemanticEvaluationScope,
    evaluate_source_semantic_state,
)


_REQUIRED_STAGE_WRITERS = {
    "belief": "EpistemicApplier",
    "intent": "IntentApplier",
    "concern": "ConcernApplier",
    "proposal": "ProposalAppender",
    "prediction": "PredictionWriter",
    "authorization": "AuthorizationApplier",
    "workflow": "AgencyStateApplier",
    "task": "AgencyStateApplier",
    "work": "WorkLedgerApplier",
    "effect": "ExecutionLedgerApplier",
    "outcome": "OutcomeRecorder",
    "settlement": "SettlementApplier",
    "attribution": "AttributionApplier",
}

_STAGE_STORAGE = {
    "belief": ("models", "id", "model"),
    "intent": ("intent_aggregate_heads", "aggregate_id", "intent"),
    "concern": ("concern_heads", "concern_id", "concern"),
    "proposal": ("consequential_proposals", "id", "proposal"),
    "prediction": ("consequential_predictions", "id", "prediction"),
    "authorization": (
        "consequential_authorization_decisions",
        "id",
        "authorization",
    ),
    "workflow": ("agency_workflow_run_heads", "workflow_run_id", "workflow"),
    "task": ("agency_task_heads", "task_id", "task"),
    "work": ("work_obligation_heads", "obligation_id", "work"),
    "effect": ("external_effect_attempt_heads", "effect_attempt_id", "effect"),
    "outcome": ("consequential_outcomes", "id", "outcome"),
    "settlement": ("consequential_settlements", "id", "settlement"),
    "attribution": ("consequential_attributions", "id", "attribution"),
}


class _ClosedLoopModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ClosedLoopEvaluationScope(_ClosedLoopModel):
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
            raise ValueError("closed-loop evaluation end must follow start")
        return self


class ClosedLoopEpisodeReport(_ClosedLoopModel):
    episode_id: UUID
    stage_link_validity: dict[str, bool]
    continuity_checks: dict[str, bool]
    missing_or_invalid_stages: tuple[str, ...]
    continuity_breaks: tuple[str, ...]
    completion_score: float = Field(ge=0.0, le=1.0)
    complete: bool


class ClosedLoopEvaluationState(_ClosedLoopModel):
    scope: ClosedLoopEvaluationScope
    episode_count: int = Field(ge=0)
    complete_episode_count: int = Field(ge=0)
    closed_loop_completion_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    stage_coverage_rates: dict[str, float | None]
    continuity_rates: dict[str, float | None]
    component_violation_counts: dict[str, int]
    component_key_rates: dict[str, float | None]
    incident_counts: dict[str, int]
    episode_reports: tuple[ClosedLoopEpisodeReport, ...]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values()) + sum(
            self.component_violation_counts.values()
        )


async def evaluate_closed_loop_state(
    conn: asyncpg.Connection,
    *,
    scope: ClosedLoopEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> ClosedLoopEvaluationState:
    """Evaluate component health and exact cross-plane episode continuity."""

    component_states = await _component_states(
        conn,
        scope=scope,
        artifact_refs=artifact_refs,
    )
    episode_rows = await conn.fetch(
        """
        SELECT h.episode_id, v.episode
        FROM intervention_episode_heads h
        JOIN intervention_episode_versions v
          ON v.tenant_id=h.tenant_id
         AND v.episode_id=h.episode_id
         AND v.aggregate_version=h.current_version
        WHERE h.tenant_id=$1 AND h.created_at < $3 AND h.updated_at >= $2
        ORDER BY h.created_at, h.episode_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    reports = tuple(
        [
            await _evaluate_episode(
                conn,
                tenant_id=scope.tenant_id,
                episode=InterventionEpisode.model_validate(_json(row["episode"])),
            )
            for row in episode_rows
        ]
    )
    stage_coverage_rates = {
        stage: _rate(
            sum(report.stage_link_validity.get(stage, False) for report in reports),
            len(reports),
        )
        for stage in _REQUIRED_STAGE_WRITERS
    }
    continuity_names = sorted(
        {
            name
            for report in reports
            for name in report.continuity_checks
        }
    )
    continuity_rates = {
        name: _rate(
            sum(report.continuity_checks.get(name, False) for report in reports),
            len(reports),
        )
        for name in continuity_names
    }
    incident_counts = Counter(
        issue
        for report in reports
        for issue in (
            *(f"missing_or_invalid_stage:{stage}" for stage in report.missing_or_invalid_stages),
            *(f"continuity_break:{name}" for name in report.continuity_breaks),
        )
    )
    component_violation_counts = {
        name: state.violation_count
        for name, state in component_states.items()
        if state.violation_count > 0
    }
    complete_count = sum(report.complete for report in reports)
    return ClosedLoopEvaluationState(
        scope=scope,
        episode_count=len(reports),
        complete_episode_count=complete_count,
        closed_loop_completion_rate=_rate(complete_count, len(reports)),
        stage_coverage_rates=stage_coverage_rates,
        continuity_rates=continuity_rates,
        component_violation_counts=component_violation_counts,
        component_key_rates=_component_key_rates(component_states),
        incident_counts=dict(sorted(incident_counts.items())),
        episode_reports=reports,
        uncertainty=(
            "This E3 vertical proves one mechanical joined loop, not intervention value across company worlds.",
            "Model-to-Concern and Concern-to-Proposal continuity are currently validated from exact durable references, not database foreign keys.",
            "An explicit withheld-credit attribution is a feedback fate, not evidence that an adaptive policy improved behavior.",
        ),
        artifact_refs=artifact_refs,
    )


async def _component_states(
    conn: asyncpg.Connection,
    *,
    scope: ClosedLoopEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> dict[str, Any]:
    common = {
        "tenant_id": scope.tenant_id,
        "start": scope.start,
        "end": scope.end,
        "run_id": scope.run_id,
    }
    return {
        "source_semantics": await evaluate_source_semantic_state(
            conn,
            scope=SourceSemanticEvaluationScope(**common),
            artifact_refs=artifact_refs,
        ),
        "intent": await evaluate_intent_state(
            conn,
            scope=IntentEvaluationScope(**common),
            artifact_refs=artifact_refs,
        ),
        "concern": await evaluate_concern_state(
            conn,
            scope=ConcernEvaluationScope(**common),
            artifact_refs=artifact_refs,
        ),
        "agency": await evaluate_agency_state(
            conn,
            scope=AgencyEvaluationScope(**common),
            artifact_refs=artifact_refs,
        ),
        "execution": await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(**common),
            artifact_refs=artifact_refs,
        ),
    }


def _component_key_rates(states: dict[str, Any]) -> dict[str, float | None]:
    source = states["source_semantics"]
    intent = states["intent"]
    concern = states["concern"]
    agency = states["agency"]
    execution = states["execution"]
    return {
        "source_model_dependency_closure": source.model_dependency_closure_rate,
        "intent_exact_acceptance": _unknown_on_zero(
            intent.exact_acceptance_coverage,
            intent.accepted_proposal_count,
        ),
        "intent_accepted_apply": _unknown_on_zero(
            intent.accepted_apply_coverage,
            intent.accepted_proposal_count,
        ),
        "concern_reducer_conformance": _unknown_on_zero(
            concern.reducer_conformance_rate,
            concern.version_count,
        ),
        "proposal_spec_atomicity": _unknown_on_zero(
            agency.proposal_spec_atomicity_rate,
            agency.proposal_count,
        ),
        "prediction_preregistration": _unknown_on_zero(
            agency.prediction_preregistration_rate,
            agency.prediction_count,
        ),
        "authorization_exactness": _unknown_on_zero(
            agency.authorization_exactness_rate,
            agency.authorization_count,
        ),
        "outcome_independence": _unknown_on_zero(
            agency.outcome_independence_rate,
            agency.outcome_count,
        ),
        "settlement_comparability": _unknown_on_zero(
            agency.settlement_comparability_rate,
            agency.settlement_count,
        ),
        "conservative_attribution": _unknown_on_zero(
            agency.conservative_attribution_rate,
            agency.attribution_count,
        ),
        "workflow_history_integrity": execution.workflow_history_integrity_rate,
        "task_history_integrity": execution.task_history_integrity_rate,
        "work_history_integrity": execution.work_history_integrity_rate,
        "lease_integrity": execution.lease_integrity_rate,
        "effect_continuity": execution.effect_continuity_rate,
        "execution_receipt_closure": execution.receipt_closure_rate,
    }


async def _evaluate_episode(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    episode: InterventionEpisode,
) -> ClosedLoopEpisodeReport:
    links = {link.stage: link for link in episode.stage_links}
    rows: dict[str, asyncpg.Record | None] = {}
    stage_validity: dict[str, bool] = {}
    for stage, expected_writer in _REQUIRED_STAGE_WRITERS.items():
        link = links.get(stage)
        if (
            link is None
            or link.fate is not EpisodeStageFate.PRESENT
            or link.writer_id != expected_writer
        ):
            stage_validity[stage] = False
            rows[stage] = None
            continue
        object_id = _object_ref_uuid(link.object_ref, expected_prefix=stage)
        if object_id is None:
            stage_validity[stage] = False
            rows[stage] = None
            continue
        row = await _fetch_stage_row(
            conn,
            tenant_id=tenant_id,
            stage=stage,
            object_id=object_id,
        )
        rows[stage] = row
        stage_validity[stage] = row is not None

    continuity = _continuity_checks(episode=episode, links=links, rows=rows)
    missing = tuple(
        stage for stage in _REQUIRED_STAGE_WRITERS if not stage_validity[stage]
    )
    breaks = tuple(name for name, valid in continuity.items() if not valid)
    passed = sum(stage_validity.values()) + sum(continuity.values())
    total = len(stage_validity) + len(continuity)
    return ClosedLoopEpisodeReport(
        episode_id=episode.episode_id,
        stage_link_validity=stage_validity,
        continuity_checks=continuity,
        missing_or_invalid_stages=missing,
        continuity_breaks=breaks,
        completion_score=passed / total,
        complete=not missing and not breaks,
    )


def _continuity_checks(
    *,
    episode: InterventionEpisode,
    links: dict[str, Any],
    rows: dict[str, asyncpg.Record | None],
) -> dict[str, bool]:
    try:
        model_row = _require(rows, "belief")
        intent_row = _require(rows, "intent")
        concern_row = _require(rows, "concern")
        proposal_row = _require(rows, "proposal")
        prediction_row = _require(rows, "prediction")
        authorization_row = _require(rows, "authorization")
        workflow_row = _require(rows, "workflow")
        task_row = _require(rows, "task")
        work_row = _require(rows, "work")
        effect_row = _require(rows, "effect")
        outcome_row = _require(rows, "outcome")
        settlement_row = _require(rows, "settlement")
        attribution_row = _require(rows, "attribution")
    except KeyError:
        return {
            "belief_to_concern": False,
            "intent_to_concern": False,
            "concern_to_proposal": False,
            "intervention_spec_continuity": False,
            "execution_chain_terminal": False,
            "execution_to_outcome_separation": False,
            "outcome_to_settlement": False,
            "settlement_to_attribution": False,
            "outcome_to_concern_closure": False,
            "explicit_feedback_fate": False,
        }

    concern_snapshot = _json(concern_row["current_snapshot"])
    estimate = dict(concern_snapshot.get("current_state_estimate") or {})
    proposal = ConsequentialProposal.model_validate(_json(proposal_row["proposal"]))
    outcome = Outcome.model_validate(_json(outcome_row["outcome"]))
    attribution = Attribution.model_validate(_json(attribution_row["attribution"]))
    model_ref = links["belief"].object_ref
    intent_ref = links["intent"].object_ref
    concern_ref = links["concern"].object_ref
    outcome_ref = links["outcome"].object_ref
    intent_id = intent_row["aggregate_id"]
    spec_digests = {
        value
        for value in (
            episode.intervention_spec_digest,
            proposal_row["intervention_spec_digest"],
            prediction_row["intervention_spec_digest"],
            authorization_row["intervention_spec_digest"],
            workflow_row["intervention_spec_digest"],
            task_row["intervention_spec_digest"],
            effect_row["intervention_spec_digest"],
        )
        if value is not None
    }
    criteria = tuple(concern_snapshot.get("criteria") or ())
    return {
        "belief_to_concern": (
            model_row["id"] is not None
            and model_ref in tuple(estimate.get("belief_model_refs") or ())
        ),
        "intent_to_concern": (
            intent_ref in tuple(estimate.get("intent_refs") or ())
            and f"goal:{intent_id}" in tuple(
                concern_snapshot.get("contributing_attention_source_refs") or ()
            )
        ),
        "concern_to_proposal": concern_ref in proposal.source_refs,
        "intervention_spec_continuity": len(spec_digests) == 1,
        "execution_chain_terminal": (
            workflow_row["current_state"] == "completed"
            and task_row["current_state"] == "completed"
            and work_row["current_state"] == "completed"
            and effect_row["current_state"] == "succeeded"
            and work_row["obligation_id"] == effect_row["work_obligation_id"]
            and task_row["task_id"] == effect_row["task_id"]
            and workflow_row["workflow_run_id"] == task_row["workflow_run_id"]
        ),
        "execution_to_outcome_separation": (
            outcome.episode_id == episode.episode_id
            and outcome.independent_of_execution_claim
            and all(
                not ref.startswith(
                    ("execution-receipt:", "effect:", "task:", "work:")
                )
                for ref in outcome.source_evidence_refs
            )
        ),
        "outcome_to_settlement": (
            settlement_row["prediction_id"] == prediction_row["id"]
            and settlement_row["outcome_id"] == outcome_row["id"]
            and settlement_row["episode_id"] == episode.episode_id
        ),
        "settlement_to_attribution": (
            attribution_row["settlement_id"] == settlement_row["id"]
            and attribution.episode_id == episode.episode_id
            and str(settlement_row["id"]) in attribution.evidence_refs
        ),
        "outcome_to_concern_closure": (
            concern_row["current_state"] == "resolved"
            and outcome_ref in tuple(estimate.get("outcome_refs") or ())
            and bool(criteria)
            and all(item.get("impact") == "satisfied" for item in criteria)
        ),
        "explicit_feedback_fate": (
            attribution.withheld_credit and bool(attribution.withholding_reason)
        ),
    }


def build_closed_loop_invariant_evidence(
    state: ClosedLoopEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    invariant = next(item for item in registry.invariants if item.invariant_id == "INV-22")
    assert invariant.proof is not None
    continuity_incidents = tuple(
        IncidentObservation(
            incident_id=f"{state.scope.run_id}:INV-22:{kind}",
            incident_class=kind,
            status=IncidentStatus.CONFIRMED,
            severity=5 if "effect" in kind or "authorization" in kind else 4,
            summary=f"Observed {count} closed-loop continuity violations.",
            artifact_refs=state.artifact_refs,
        )
        for kind, count in state.incident_counts.items()
    )
    component_incidents = tuple(
        IncidentObservation(
            incident_id=f"{state.scope.run_id}:INV-22:component:{component}",
            incident_class=f"component:{component}",
            status=IncidentStatus.CONFIRMED,
            severity=5 if component in {"source_semantics", "execution"} else 4,
            summary=f"Observed {count} component-plane violations.",
            artifact_refs=state.artifact_refs,
        )
        for component, count in state.component_violation_counts.items()
    )
    incidents = continuity_incidents + component_incidents
    denominator = FateDenominatorRecord(
        denominator_id=f"{state.scope.run_id}:INV-22:closed-loop-episodes",
        denominator_version="closed-loop-denominator-v1",
        population_definition_version="current-intervention-episode-heads-v1",
        query_or_manifest_hash=canonical_sha256(
            {
                "scope": state.scope.model_dump(mode="json"),
                "artifact_refs": state.artifact_refs,
            }
        ),
        source_or_oracle_population=state.episode_count,
        production_accepted=state.episode_count,
        eligible=state.episode_count,
        attempted_or_committed=state.episode_count,
        terminal_fates={
            "complete": state.complete_episode_count,
            "incomplete": state.episode_count - state.complete_episode_count,
        },
        unknown_or_untraced=0,
        report_cutoff=state.scope.end.isoformat(),
        population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
        population_partition_value="closed_loop_episode",
        population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    )
    return (
        InvariantRunEvidence(
            invariant_id="INV-22",
            applicable_exposures=state.episode_count,
            observed_trace_facts=(
                frozenset(
                    {
                        "belief_intent_concern_links",
                        "proposal_spec_authority_work_lease_effect_receipt_and_episode_versions",
                        "independent_outcome",
                        "settlement_and_attribution",
                        "explicit_feedback_fate",
                    }
                )
                if state.episode_count
                else frozenset()
            ),
            executed_scenario_ids=(
                frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids
            ),
            metric_observations=(
                MetricObservation(
                    metric_id="inv.spec_continuity",
                    metric_version="closed-loop-runtime-v1",
                    raw_numerator=float(state.complete_episode_count),
                    raw_denominator=float(state.episode_count),
                    point_estimate=state.closed_loop_completion_rate,
                    violation_count=state.violation_count,
                    severity_mass=float(state.violation_count),
                    artifact_refs=state.artifact_refs,
                ),
            ),
            incidents=incidents,
            achieved_evidence_tier=(
                EvidenceTier.E3 if state.episode_count else EvidenceTier.E0
            ),
            denominator=denominator,
            uncertainty=state.uncertainty,
            blind_spots=state.uncertainty,
            artifact_refs=state.artifact_refs,
        ),
    )


def render_closed_loop_markdown(state: ClosedLoopEvaluationState) -> str:
    lines = [
        "# Closed Intervention And Feedback Loop",
        "",
        f"- Run: `{state.scope.run_id}`",
        f"- Episodes: {state.episode_count}",
        f"- Complete joined episodes: {state.complete_episode_count}",
        f"- Completion rate: {_display_rate(state.closed_loop_completion_rate)}",
        "",
        "## Required Stage Coverage",
        "",
        "| Stage | Coverage |",
        "| --- | ---: |",
    ]
    lines.extend(
        f"| {stage} | {_display_rate(rate)} |"
        for stage, rate in state.stage_coverage_rates.items()
    )
    lines.extend(
        (
            "",
            "## Cross-Plane Continuity",
            "",
            "| Boundary | Rate |",
            "| --- | ---: |",
        )
    )
    lines.extend(
        f"| {name.replace('_', ' ')} | {_display_rate(rate)} |"
        for name, rate in state.continuity_rates.items()
    )
    lines.extend(("", "## Component Key Rates", "", "| Measure | Rate |", "| --- | ---: |"))
    lines.extend(
        f"| {name.replace('_', ' ')} | {_display_rate(rate)} |"
        for name, rate in state.component_key_rates.items()
    )
    lines.extend(("", "## Incidents", ""))
    if state.incident_counts or state.component_violation_counts:
        lines.extend(
            f"- `{kind}`: {count}" for kind, count in state.incident_counts.items()
        )
        lines.extend(
            f"- `component:{name}`: {count}"
            for name, count in state.component_violation_counts.items()
        )
    else:
        lines.append("- None observed in scope.")
    lines.extend(("", "## Uncertainty", ""))
    lines.extend(f"- {item}" for item in state.uncertainty)
    return "\n".join(lines) + "\n"


def _require(
    rows: dict[str, asyncpg.Record | None],
    stage: str,
) -> asyncpg.Record:
    row = rows.get(stage)
    if row is None:
        raise KeyError(stage)
    return row


async def _fetch_stage_row(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    stage: str,
    object_id: UUID,
) -> asyncpg.Record | None:
    if stage == "concern":
        return await conn.fetchrow(
            """
            SELECT h.*, v.snapshot AS current_snapshot
            FROM concern_heads h
            JOIN concern_versions v
              ON v.tenant_id=h.tenant_id
             AND v.concern_id=h.concern_id
             AND v.aggregate_version=h.current_version
            WHERE h.tenant_id=$1 AND h.concern_id=$2
            """,
            tenant_id,
            object_id,
        )
    if stage == "work":
        return await conn.fetchrow(
            """
            SELECT h.*, s.target_object_type, s.target_object_id,
                   s.owner_writer_id, s.obligation
            FROM work_obligation_heads h
            JOIN work_obligation_specs s
              ON s.tenant_id=h.tenant_id
             AND s.obligation_id=h.obligation_id
            WHERE h.tenant_id=$1 AND h.obligation_id=$2
            """,
            tenant_id,
            object_id,
        )
    table, id_column, _ = _STAGE_STORAGE[stage]
    return await conn.fetchrow(
        f"SELECT * FROM {table} WHERE tenant_id=$1 AND {id_column}=$2",
        tenant_id,
        object_id,
    )


def _object_ref_uuid(value: str | None, *, expected_prefix: str) -> UUID | None:
    if not value:
        return None
    prefix, separator, raw = value.partition(":")
    if not separator or prefix != expected_prefix:
        return None
    try:
        return UUID(raw)
    except ValueError:
        return None


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _unknown_on_zero(value: float, denominator: int) -> float | None:
    return value if denominator else None


def _display_rate(value: float | None) -> str:
    return "unknown/not exposed" if value is None else f"{value:.1%}"


__all__ = [
    "ClosedLoopEpisodeReport",
    "ClosedLoopEvaluationScope",
    "ClosedLoopEvaluationState",
    "build_closed_loop_invariant_evidence",
    "evaluate_closed_loop_state",
    "render_closed_loop_markdown",
]
