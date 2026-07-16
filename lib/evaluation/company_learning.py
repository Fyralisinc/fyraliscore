"""Canonical active-slice evaluation for autonomous company learning."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.evaluation.conversation_context import (
    ConversationContextEvaluationScope,
    ConversationContextEvaluationState,
    build_conversation_context_invariant_evidence,
    evaluate_conversation_context_state,
)
from lib.evaluation.entity_grounding import (
    EntityGroundingEvaluationState,
    GroundingEvaluationScope,
    build_entity_grounding_invariant_evidence,
    evaluate_entity_grounding_state,
)
from lib.evaluation.proof import (
    InvariantEvidenceManifest,
    InvariantProofMatrixReport,
    SubstantiationState,
)
from lib.evaluation.source_semantics import (
    SourceSemanticEvaluationScope,
    SourceSemanticEvaluationState,
    build_source_semantic_invariant_evidence,
    evaluate_source_semantic_state,
)

ACTIVE_COMPANY_LEARNING_INVARIANT_IDS = frozenset(
    {
        "INV-04",
        "INV-05",
        "INV-06",
        "INV-07",
        "INV-16",
        "INV-25",
        "INV-26",
        "INV-27",
        "INV-29",
    }
)


class _CompanyLearningModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class CompanyLearningEvaluationScope(_CompanyLearningModel):
    tenant_id: UUID
    observation_ids: tuple[UUID, ...]
    start: datetime
    end: datetime
    run_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def valid_scope(self) -> Self:
        if not self.observation_ids:
            raise ValueError("company-learning evaluation requires observations")
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.end <= self.start:
            raise ValueError("company-learning evaluation end must follow start")
        return self


class CompanyLearningEvaluationState(_CompanyLearningModel):
    schema_version: str = "company-learning-evaluation-v1"
    scope: CompanyLearningEvaluationScope
    created_at: datetime
    status: str
    observed_slice_health: str
    conversation_context: ConversationContextEvaluationState
    entity_grounding: EntityGroundingEvaluationState
    source_semantics: SourceSemanticEvaluationState
    learning_loop: dict[str, int | float | None]
    incident_counts: dict[str, int]
    proof_gaps: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


def assess_company_learning_runtime_state(
    *,
    incident_count: int,
    context_selection_count: int,
    governed_replay_exposure_count: int,
    source_semantic_exposure_count: int,
    critical_rates: tuple[float | None, ...],
) -> tuple[str, str]:
    """Classify measured runtime health without inventing proof substantiation."""

    exposures = (
        context_selection_count,
        governed_replay_exposure_count,
        source_semantic_exposure_count,
    )
    if incident_count:
        return "contradicted", "contradicted"
    if not any(exposures):
        return "not_observed", "not_observed"
    if all(exposures) and all(value == 1.0 for value in critical_rates):
        return "insufficient", "healthy"
    return "insufficient", "incomplete"


async def evaluate_company_learning_state(
    conn: asyncpg.Connection,
    *,
    scope: CompanyLearningEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> CompanyLearningEvaluationState:
    """Evaluate the active company-physics and correction/replay loop once."""

    context_state = await evaluate_conversation_context_state(
        conn,
        scope=ConversationContextEvaluationScope(
            tenant_id=scope.tenant_id,
            start=scope.start,
            end=scope.end,
            run_id=scope.run_id,
            observation_ids=scope.observation_ids,
        ),
        artifact_refs=artifact_refs,
    )
    grounding_state = await evaluate_entity_grounding_state(
        conn,
        scope=GroundingEvaluationScope(
            tenant_id=scope.tenant_id,
            observation_start=scope.start,
            observation_end=scope.end,
            run_id=scope.run_id,
            observation_ids=scope.observation_ids,
        ),
        artifact_refs=artifact_refs,
    )
    semantic_state = await evaluate_source_semantic_state(
        conn,
        scope=SourceSemanticEvaluationScope(
            tenant_id=scope.tenant_id,
            start=scope.start,
            end=scope.end,
            run_id=scope.run_id,
            observation_ids=scope.observation_ids,
        ),
        artifact_refs=artifact_refs,
    )
    incidents: dict[str, int] = {}
    for component, values in (
        ("conversation_context", context_state.incident_counts),
        ("entity_grounding", grounding_state.incident_counts),
        ("source_semantics", semantic_state.incident_counts),
    ):
        for name, count in values.items():
            if count:
                incidents[f"{component}.{name}"] = int(count)

    learning_loop: dict[str, int | float | None] = {
        "answered_entity_clarifications": (
            grounding_state.answered_entity_clarification_count
        ),
        "lineage_valid_entity_clarifications": (
            grounding_state.answered_entity_clarification_lineage_count
        ),
        "adjudicated_aliases": grounding_state.adjudicated_alias_count,
        "observed_future_corrective_reuse": (
            grounding_state.corrective_memory_observed_reuse_count
        ),
        "governed_alias_replay_exposures": (
            grounding_state.alias_replay_exposure_count
        ),
        "governed_alias_replays_resolved": (
            grounding_state.alias_replay_resolved_count
        ),
        "governed_alias_replay_resolution_rate": (
            grounding_state.alias_replay_resolution_rate
        ),
        "governed_alias_replay_llm_calls_avoided": (
            grounding_state.alias_replay_llm_avoided_count
        ),
        "grounding_to_interpretation_coverage": (
            semantic_state.eligible_grounding_interpretation_coverage
        ),
        "one_model_cardinality_rate": semantic_state.one_model_cardinality_rate,
        "non_admitted_no_model_safety_rate": (
            semantic_state.non_admitted_no_model_safety_rate
        ),
    }
    required_exposures = (
        context_state.selection_count,
        grounding_state.alias_replay_exposure_count,
        semantic_state.eligible_grounding_count,
    )
    critical_rates = (
        context_state.selection_reconstructability_rate,
        context_state.selection_replay_equivalence_rate,
        grounding_state.stage_continuity_rate,
        grounding_state.terminal_trace_coverage,
        grounding_state.alias_replay_resolution_rate,
        semantic_state.eligible_grounding_interpretation_coverage,
        semantic_state.grounding_continuity_exactness_rate,
        semantic_state.one_model_cardinality_rate,
        semantic_state.non_admitted_no_model_safety_rate,
    )
    status, observed_slice_health = assess_company_learning_runtime_state(
        incident_count=sum(incidents.values()),
        context_selection_count=required_exposures[0],
        governed_replay_exposure_count=required_exposures[1],
        source_semantic_exposure_count=required_exposures[2],
        critical_rates=critical_rates,
    )
    proof_gaps = sorted(
        {
            *context_state.uncertainty,
            *grounding_state.uncertainty,
            *semantic_state.uncertainty,
            *(
                ("No company-learning exposure was observed.",)
                if not any(required_exposures)
                else ()
            ),
            *(
                (
                    "The active learning slice was only partially exposed; "
                    "context selection, governed correction replay and source "
                    "semantics must all have positive denominators.",
                )
                if any(required_exposures) and not all(required_exposures)
                else ()
            ),
            *(
                (
                    "Runtime state is healthy, but registered scenario and "
                    "evidence-tier proof is required before substantiation.",
                )
                if observed_slice_health == "healthy"
                else ()
            ),
        }
    )
    return CompanyLearningEvaluationState(
        scope=scope,
        created_at=datetime.now(timezone.utc),
        status=status,
        observed_slice_health=observed_slice_health,
        conversation_context=context_state,
        entity_grounding=grounding_state,
        source_semantics=semantic_state,
        learning_loop=learning_loop,
        incident_counts=incidents,
        proof_gaps=tuple(proof_gaps),
        artifact_refs=artifact_refs,
    )


def build_company_learning_evidence_manifest(
    state: CompanyLearningEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    system_version: str,
    experiment_manifest_ref: str,
    executed_scenario_ids: frozenset[str] = frozenset(),
) -> InvariantEvidenceManifest:
    """Build one canonical manifest for the complete active learning slice."""

    evidence = (
        *build_conversation_context_invariant_evidence(
            state.conversation_context,
            registry=registry,
            executed_scenario_ids=executed_scenario_ids,
        ),
        *build_entity_grounding_invariant_evidence(
            state.entity_grounding,
            registry=registry,
            executed_scenario_ids=executed_scenario_ids,
        ),
        *build_source_semantic_invariant_evidence(
            state.source_semantics,
            registry=registry,
            executed_scenario_ids=executed_scenario_ids,
        ),
    )
    return InvariantEvidenceManifest(
        manifest_version="company-learning-evidence-v1",
        run_id=state.scope.run_id,
        architecture_digest=registry.digest,
        system_version=system_version,
        created_at=state.created_at.isoformat(),
        experiment_manifest_ref=experiment_manifest_ref,
        evidence=evidence,
        artifact_refs=state.artifact_refs,
    )


def company_learning_assurance_status(
    state: CompanyLearningEvaluationState,
    proof: InvariantProofMatrixReport,
) -> str:
    """Return proof-backed assurance for the active company-learning invariants."""

    if state.violation_count:
        return "contradicted"
    if state.observed_slice_health == "not_observed":
        return "not_observed"
    if state.observed_slice_health != "healthy":
        return "insufficient"
    records = tuple(
        row
        for row in proof.records
        if row.invariant_id in ACTIVE_COMPANY_LEARNING_INVARIANT_IDS
    )
    if any(
        row.substantiation_state is SubstantiationState.CONTRADICTED
        for row in records
    ):
        return "contradicted"
    if len(records) == len(ACTIVE_COMPANY_LEARNING_INVARIANT_IDS) and all(
        row.substantiation_state is SubstantiationState.SUBSTANTIATED
        for row in records
    ):
        return "substantiated"
    return "insufficient"


def render_company_learning_markdown(
    state: CompanyLearningEvaluationState,
) -> str:
    loop = state.learning_loop
    lines = [
        "# Company Learning Evaluation",
        "",
        f"- Run: `{state.scope.run_id}`",
        f"- Assurance status: **{state.status}**",
        f"- Observed slice health: **{state.observed_slice_health}**",
        (
            "- Governed alias replay: "
            f"{loop['governed_alias_replays_resolved']}/"
            f"{loop['governed_alias_replay_exposures']} "
            f"(rate={_rate(loop['governed_alias_replay_resolution_rate'])})"
        ),
        (
            "- Grounding to interpretation: "
            f"{_rate(loop['grounding_to_interpretation_coverage'])}"
        ),
        f"- Exactly-one-Model rate: {_rate(loop['one_model_cardinality_rate'])}",
        (
            "- Non-admission safety: "
            f"{_rate(loop['non_admitted_no_model_safety_rate'])}"
        ),
        "",
        "## Incidents",
        "",
    ]
    if state.incident_counts:
        lines.extend(
            f"- `{name}`: {count}"
            for name, count in state.incident_counts.items()
        )
    else:
        lines.append("- None observed.")
    lines.extend(["", "## Proof Gaps", ""])
    lines.extend(f"- {gap}" for gap in state.proof_gaps)
    return "\n".join(lines).rstrip() + "\n"


def _rate(value: int | float | None) -> str:
    if value is None:
        return "unknown/not exposed"
    return f"{float(value):.1%}"


__all__ = [
    "ACTIVE_COMPANY_LEARNING_INVARIANT_IDS",
    "CompanyLearningEvaluationScope",
    "CompanyLearningEvaluationState",
    "assess_company_learning_runtime_state",
    "build_company_learning_evidence_manifest",
    "company_learning_assurance_status",
    "evaluate_company_learning_state",
    "render_company_learning_markdown",
]
