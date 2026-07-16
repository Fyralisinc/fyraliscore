"""Continuous evaluation of grounded source semantics and belief admission."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Iterable, Mapping, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
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


_SUPPORTED_REPORT_SUFFIX = re.compile(
    r"^\s+(?:is|are|was|were)\s+(?:not\s+)?"
    r"(?:blocked|approved|ready|delayed|complete)\.?$",
    re.IGNORECASE,
)


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _grounded_referent(value: Any) -> dict[str, Any] | None:
    payload = _json(value)
    if not isinstance(payload, dict):
        return None
    if "referent_id" in payload:
        return payload
    referent_id = payload.get("id")
    if not referent_id:
        return None
    return {
        "referent_id": referent_id,
        "referent_version": int(payload.get("version", 1)),
    }


class _EvaluationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class SourceSemanticEvaluationScope(_EvaluationModel):
    tenant_id: UUID
    start: datetime
    end: datetime
    run_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def valid_window(self) -> Self:
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.end <= self.start:
            raise ValueError("end must be later than start")
        return self


class SourceSemanticEvaluationState(_EvaluationModel):
    scope: SourceSemanticEvaluationScope
    eligible_grounding_count: int = Field(ge=0)
    interpretation_count: int = Field(ge=0)
    decision_count: int = Field(ge=0)
    applied_decision_count: int = Field(ge=0)
    no_admission_decision_count: int = Field(ge=0)
    reconstructable_source_count: int = Field(ge=0)
    structurally_closed_interpretation_count: int = Field(ge=0)
    exact_grounding_continuity_count: int = Field(ge=0)
    expected_supported_report_count: int = Field(ge=0)
    correctly_applied_supported_report_count: int = Field(ge=0)
    epistemic_admission_continuity_count: int = Field(ge=0)
    applied_model_coverage_count: int = Field(ge=0)
    one_model_cardinality_count: int = Field(ge=0)
    model_source_provenance_count: int = Field(ge=0)
    model_scope_referent_count: int = Field(ge=0)
    model_grounding_dependency_count: int = Field(ge=0)
    model_dependency_closure_count: int = Field(ge=0)
    safe_no_admission_count: int = Field(ge=0)
    complete_core_fate_count: int = Field(ge=0)
    decision_fate_counts: dict[str, int]
    incident_counts: dict[str, int]
    incident_trace_refs: dict[str, tuple[str, ...]]
    artifact_refs: tuple[str, ...] = Field(min_length=1)
    uncertainty: tuple[str, ...]

    eligible_grounding_interpretation_coverage: float | None = None
    source_coordinate_reconstructability_rate: float | None = None
    interpretation_structural_closure_rate: float | None = None
    grounding_continuity_exactness_rate: float | None = None
    explicit_admission_fate_coverage: float | None = None
    supported_report_admission_precision: float | None = None
    supported_report_admission_recall: float | None = None
    epistemic_consumer_admission_continuity_rate: float | None = None
    applied_decision_model_coverage: float | None = None
    one_model_cardinality_rate: float | None = None
    model_source_provenance_rate: float | None = None
    model_scope_referent_rate: float | None = None
    model_grounding_dependency_rate: float | None = None
    model_dependency_closure_rate: float | None = None
    non_admitted_no_model_safety_rate: float | None = None


def _supported_report_expected(row: Mapping[str, Any]) -> bool:
    if row.get("current_fate") != "resolved_for_consumer":
        return False
    text = str(row.get("content_text") or "")
    mention = _json(row.get("mention"))
    if not isinstance(mention, dict):
        return False
    anchor = mention.get("primary_anchor")
    if not isinstance(anchor, dict) or anchor.get("kind") != "explicit":
        return False
    coordinate = anchor.get("coordinate")
    if not isinstance(coordinate, dict):
        return False
    start = coordinate.get("span_start")
    end = coordinate.get("span_end")
    surface = anchor.get("surface_form")
    source_ref = f"observation:{row.get('source_observation_id')}"
    if (
        not isinstance(start, int)
        or not isinstance(end, int)
        or not isinstance(surface, str)
        or start < 0
        or end > len(text)
        or text[start:end] != surface
        or text[:start].strip()
        or coordinate.get("field_path") != "content_text"
        or coordinate.get("evidence_record_id") != source_ref
        or coordinate.get("source_object_id") != source_ref
    ):
        return False
    return bool(_SUPPORTED_REPORT_SUFFIX.fullmatch(text[end:]))


def _source_is_reconstructable(row: Mapping[str, Any]) -> bool:
    assertion = _json(row.get("source_assertion"))
    if not isinstance(assertion, dict):
        return False
    text = str(row.get("content_text") or "")
    expected_ref = f"observation:{row.get('source_observation_id')}"
    expected_revision = f"{expected_ref}:v1"
    for coordinate in assertion.get("coordinates") or ():
        if not isinstance(coordinate, dict):
            continue
        start = coordinate.get("span_start")
        end = coordinate.get("span_end")
        if (
            isinstance(start, int)
            and isinstance(end, int)
            and 0 <= start <= end <= len(text)
            and coordinate.get("evidence_record_id") == expected_ref
            and coordinate.get("source_object_id") == expected_ref
            and coordinate.get("source_revision") == expected_revision
            and coordinate.get("field_path") == "content_text"
            and text[start:end] == assertion.get("expressed_content")
        ):
            return True
    return False


def _interpretation_is_structurally_closed(row: Mapping[str, Any]) -> bool:
    assertion = _json(row.get("source_assertion"))
    frame = _json(row.get("semantic_frame"))
    speech_act = _json(row.get("speech_act"))
    if not all(isinstance(item, dict) for item in (assertion, frame, speech_act)):
        return False
    assertion_id = assertion.get("assertion_id")
    if not assertion_id:
        return False
    if frame.get("source_assertion_id") != assertion_id:
        return False
    if speech_act.get("source_assertion_id") != assertion_id:
        return False
    required_assertion = (
        "current_speaker_or_author",
        "kind",
        "expressed_content",
        "uncertainty",
        "extractor_version",
    )
    required_frame = (
        "predicate_or_event_type",
        "arguments",
        "negated",
        "modality",
        "confidence",
    )
    return all(key in assertion for key in required_assertion) and all(
        key in frame for key in required_frame
    ) and bool(speech_act.get("distribution"))


def _continuity_is_exact(row: Mapping[str, Any]) -> bool:
    continuity = _json(row.get("grounding_continuity"))
    if not isinstance(continuity, dict):
        return False
    expected_assessment = f"resolution-assessment:{row.get('resolution_assessment_id')}"
    expected_admission = f"grounding-admission:{row.get('interpretation_grounding_admission_id')}"
    expected_mention = str(row.get("mention_ref") or "")
    selected = _grounded_referent(row.get("selected_referent"))
    if (
        continuity.get("resolution_assessment_ref") != expected_assessment
        or continuity.get("grounding_admission_ref") != expected_admission
        or continuity.get("mention_ref") != expected_mention
    ):
        return False
    if row.get("disposition") == "belief_applied":
        if continuity.get("downstream_object_ref") != (
            f"model:{row.get('admitted_model_id')}"
        ):
            return False
        return continuity.get("selected_referent") == selected
    return continuity.get("downstream_object_ref") == (
        f"source-semantic-interpretation:{row.get('interpretation_id')}"
    )


def _epistemic_admission_is_exact(row: Mapping[str, Any]) -> bool:
    decision = _json(row.get("interpretation_grounding_admission"))
    return (
        isinstance(decision, dict)
        and row.get("interpretation_grounding_consumer") == "epistemic-applier"
        and row.get("interpretation_grounding_purpose") == "belief-admission"
        and row.get("interpretation_grounding_operation")
        == "create-grounded-belief"
        and row.get("interpretation_grounding_assessment_id")
        == row.get("resolution_assessment_id")
        and decision.get("selected_referent")
        == _grounded_referent(row.get("selected_referent"))
    )


def _model_checks(row: Mapping[str, Any]) -> tuple[bool, bool, bool, bool, bool]:
    model_exists = row.get("model_id") is not None
    one_model = int(row.get("interpretation_model_count") or 0) == 1
    proposition = _json(row.get("model_proposition"))
    scope_entities = _json(row.get("model_scope_entities"))
    continuity = _json(row.get("grounding_continuity"))
    provenance = model_exists and row.get("model_born_from_event_id") == row.get(
        "source_observation_id"
    )
    scope = (
        model_exists
        and isinstance(scope_entities, list)
        and _json(row.get("selected_referent")) in scope_entities
    )
    dependency = (
        model_exists
        and isinstance(proposition, dict)
        and proposition.get("source_semantic_interpretation_id")
        == str(row.get("interpretation_id"))
        and proposition.get("grounding_continuity") == continuity
    )
    return model_exists, one_model, provenance, scope, dependency


def analyze_source_semantic_rows(
    *,
    scope: SourceSemanticEvaluationScope,
    rows: Iterable[Mapping[str, Any]],
    artifact_refs: tuple[str, ...],
) -> SourceSemanticEvaluationState:
    materialized = tuple(rows)
    incidents: Counter[str] = Counter()
    incident_refs: defaultdict[str, list[str]] = defaultdict(list)
    fates: Counter[str] = Counter()
    counts: Counter[str] = Counter()

    def incident(kind: str, row: Mapping[str, Any]) -> None:
        incidents[kind] += 1
        incident_refs[kind].append(f"grounding-trace:{row.get('grounding_trace_id')}")

    for row in materialized:
        trace_id = row.get("grounding_trace_id")
        if row.get("interpretation_id") is None:
            incident("eligible_grounding_without_interpretation", row)
            continue
        counts["interpretation"] += 1
        reconstructable = _source_is_reconstructable(row)
        structural = _interpretation_is_structurally_closed(row)
        continuity = _continuity_is_exact(row)
        counts["reconstructable"] += int(reconstructable)
        counts["structural"] += int(structural)
        counts["continuity"] += int(continuity)
        if not reconstructable:
            incident("source_coordinate_not_reconstructable", row)
        if not structural:
            incident("source_semantic_structure_not_closed", row)
        if not continuity:
            incident("grounding_continuity_mismatch", row)

        disposition = row.get("disposition")
        if disposition not in {"belief_applied", "no_admission"}:
            incident("interpretation_without_explicit_admission_fate", row)
            continue
        counts["decision"] += 1
        fates[str(disposition)] += 1
        expected_supported = _supported_report_expected(row)
        counts["expected_supported"] += int(expected_supported)
        applied = disposition == "belief_applied"
        fate_safe = False
        if applied:
            counts["applied"] += 1
            correct_supported = expected_supported
            counts["correct_supported"] += int(correct_supported)
            if not correct_supported:
                incident("unsupported_expression_admitted_as_belief", row)
            epistemic = _epistemic_admission_is_exact(row)
            counts["epistemic"] += int(epistemic)
            if not epistemic:
                incident("epistemic_consumer_admission_discontinuity", row)
            model, one_model, provenance, referent_scope, dependency = _model_checks(row)
            counts["model"] += int(model)
            counts["one_model"] += int(one_model)
            counts["provenance"] += int(provenance)
            counts["referent_scope"] += int(referent_scope)
            counts["model_dependency"] += int(dependency)
            closed = all(
                (model, one_model, provenance, referent_scope, dependency, epistemic)
            )
            counts["model_closure"] += int(closed)
            fate_safe = correct_supported and closed
            if not model:
                incident("applied_decision_without_model", row)
            if not one_model:
                incident("applied_interpretation_model_cardinality", row)
            if not provenance:
                incident("model_source_provenance_discontinuity", row)
            if not referent_scope:
                incident("model_scope_referent_discontinuity", row)
            if not dependency:
                incident("model_grounding_dependency_discontinuity", row)
        else:
            counts["no_admission"] += 1
            safe = (
                row.get("admitted_model_id") is None
                and row.get("model_id") is None
                and int(row.get("interpretation_model_count") or 0) == 0
            )
            counts["safe_no_admission"] += int(safe)
            fate_safe = safe and not expected_supported
            if not safe:
                incident("no_admission_created_model", row)

        if expected_supported and not applied:
            incident("supported_report_not_admitted", row)
        if reconstructable and structural and continuity and fate_safe:
            counts["core_fate"] += 1
        if trace_id is None:
            incident("interpretation_without_grounding_trace", row)

    eligible = len(materialized)
    return SourceSemanticEvaluationState(
        scope=scope,
        eligible_grounding_count=eligible,
        interpretation_count=counts["interpretation"],
        decision_count=counts["decision"],
        applied_decision_count=counts["applied"],
        no_admission_decision_count=counts["no_admission"],
        reconstructable_source_count=counts["reconstructable"],
        structurally_closed_interpretation_count=counts["structural"],
        exact_grounding_continuity_count=counts["continuity"],
        expected_supported_report_count=counts["expected_supported"],
        correctly_applied_supported_report_count=counts["correct_supported"],
        epistemic_admission_continuity_count=counts["epistemic"],
        applied_model_coverage_count=counts["model"],
        one_model_cardinality_count=counts["one_model"],
        model_source_provenance_count=counts["provenance"],
        model_scope_referent_count=counts["referent_scope"],
        model_grounding_dependency_count=counts["model_dependency"],
        model_dependency_closure_count=counts["model_closure"],
        safe_no_admission_count=counts["safe_no_admission"],
        complete_core_fate_count=counts["core_fate"],
        decision_fate_counts=dict(sorted(fates.items())),
        incident_counts=dict(sorted(incidents.items())),
        incident_trace_refs={
            key: tuple(dict.fromkeys(refs))
            for key, refs in sorted(incident_refs.items())
        },
        artifact_refs=artifact_refs,
        uncertainty=(
            "The bootstrap oracle covers only exact mention-prefix copular reports.",
            "No general attribution, quotation, condition, quantity or time oracle is active.",
            "Rows without a completed grounding trace are outside this component denominator.",
        ),
        eligible_grounding_interpretation_coverage=_rate(
            counts["interpretation"], eligible
        ),
        source_coordinate_reconstructability_rate=_rate(
            counts["reconstructable"], counts["interpretation"]
        ),
        interpretation_structural_closure_rate=_rate(
            counts["structural"], counts["interpretation"]
        ),
        grounding_continuity_exactness_rate=_rate(
            counts["continuity"], counts["interpretation"]
        ),
        explicit_admission_fate_coverage=_rate(
            counts["decision"], counts["interpretation"]
        ),
        supported_report_admission_precision=_rate(
            counts["correct_supported"], counts["applied"]
        ),
        supported_report_admission_recall=_rate(
            counts["correct_supported"], counts["expected_supported"]
        ),
        epistemic_consumer_admission_continuity_rate=_rate(
            counts["epistemic"], counts["applied"]
        ),
        applied_decision_model_coverage=_rate(counts["model"], counts["applied"]),
        one_model_cardinality_rate=_rate(
            counts["one_model"], counts["applied"]
        ),
        model_source_provenance_rate=_rate(
            counts["provenance"], counts["applied"]
        ),
        model_scope_referent_rate=_rate(
            counts["referent_scope"], counts["applied"]
        ),
        model_grounding_dependency_rate=_rate(
            counts["model_dependency"], counts["applied"]
        ),
        model_dependency_closure_rate=_rate(
            counts["model_closure"], counts["applied"]
        ),
        non_admitted_no_model_safety_rate=_rate(
            counts["safe_no_admission"], counts["no_admission"]
        ),
    )


async def evaluate_source_semantic_state(
    conn: asyncpg.Connection,
    *,
    scope: SourceSemanticEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> SourceSemanticEvaluationState:
    rows = await conn.fetch(
        """
        SELECT gt.id AS grounding_trace_id,
               gt.source_observation_id, gt.current_fate,
               gt.resolution_assessment_id, gt.selected_referent,
               o.content_text, o.occurred_at,
               req.mention_ref, emd.mention,
               ssi.id AS interpretation_id,
               ssi.grounding_admission_id AS interpretation_grounding_admission_id,
               ssi.source_assertion, ssi.semantic_frame, ssi.speech_act,
               ssi.grounding_continuity,
               ssad.disposition, ssad.admitted_model_id,
               egad.assessment_id AS interpretation_grounding_assessment_id,
               egad.consumer AS interpretation_grounding_consumer,
               egad.purpose AS interpretation_grounding_purpose,
               egad.operation AS interpretation_grounding_operation,
               egad.decision AS interpretation_grounding_admission,
               m.id AS model_id,
               m.born_from_event_id AS model_born_from_event_id,
               m.proposition AS model_proposition,
               m.scope_entities AS model_scope_entities,
               COALESCE((
                 SELECT count(*)
                 FROM models mx
                 WHERE mx.tenant_id=gt.tenant_id
                   AND mx.proposition->>'source_semantic_interpretation_id'
                       = ssi.id::text
               ), 0) AS interpretation_model_count
        FROM grounding_traces gt
        JOIN observations o
          ON o.tenant_id=gt.tenant_id AND o.id=gt.source_observation_id
        JOIN entity_candidate_generation_requests req
          ON req.tenant_id=gt.tenant_id AND req.id=gt.candidate_request_id
        JOIN entity_mention_detections emd
          ON emd.tenant_id=gt.tenant_id
         AND emd.id=gt.entity_mention_detection_id
        LEFT JOIN source_semantic_interpretations ssi
          ON ssi.tenant_id=gt.tenant_id AND ssi.grounding_trace_id=gt.id
        LEFT JOIN source_semantic_admission_decisions ssad
          ON ssad.tenant_id=ssi.tenant_id AND ssad.interpretation_id=ssi.id
        LEFT JOIN grounding_admission_decisions egad
          ON egad.tenant_id=ssi.tenant_id
         AND egad.id=ssi.grounding_admission_id
        LEFT JOIN models m
          ON m.tenant_id=ssad.tenant_id AND m.id=ssad.admitted_model_id
        WHERE gt.tenant_id=$1
          AND o.occurred_at >= $2 AND o.occurred_at < $3
        ORDER BY o.occurred_at, gt.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    return analyze_source_semantic_rows(
        scope=scope,
        rows=rows,
        artifact_refs=artifact_refs,
    )


def build_source_semantic_invariant_evidence(
    state: SourceSemanticEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    invariant = next(item for item in registry.invariants if item.invariant_id == "INV-26")
    assert invariant.proof is not None
    eligible = state.eligible_grounding_count
    terminal = state.decision_fate_counts
    core_successes = state.complete_core_fate_count
    incidents = tuple(
        IncidentObservation(
            incident_id=f"{state.scope.run_id}:INV-26:{kind}",
            incident_class=kind,
            status=IncidentStatus.CONFIRMED,
            severity=5 if "admitted" in kind or "model" in kind else 4,
            summary=f"Observed {count} source-semantic continuity violations.",
            artifact_refs=state.artifact_refs + state.incident_trace_refs.get(kind, ()),
        )
        for kind, count in state.incident_counts.items()
    )
    denominator = FateDenominatorRecord(
        denominator_id=f"{state.scope.run_id}:INV-26:source-semantic-population",
        denominator_version="source-semantic-denominator-v1",
        population_definition_version="completed-grounding-traces-in-scope-v1",
        query_or_manifest_hash=canonical_sha256(
            {
                "scope": state.scope.model_dump(mode="json"),
                "artifact_refs": state.artifact_refs,
            }
        ),
        source_or_oracle_population=eligible,
        production_accepted=eligible,
        eligible=eligible,
        attempted_or_committed=eligible,
        terminal_fates=terminal,
        unknown_or_untraced=0,
        report_cutoff=state.scope.end.isoformat(),
        population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
        population_partition_value="source_semantics",
        population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    )
    return (
        InvariantRunEvidence(
            invariant_id="INV-26",
            applicable_exposures=eligible,
            observed_trace_facts=frozenset(
                {
                    "exact_source_coordinates",
                    "source_and_attributed_speakers",
                    "predicate_and_arguments",
                    "uncertainty",
                    "destination_command_and_result",
                }
            ),
            executed_scenario_ids=(
                frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids
            ),
            metric_observations=(
                MetricObservation(
                    metric_id="inv.source_semantics",
                    metric_version="source-semantic-runtime-v1",
                    raw_numerator=float(core_successes),
                    raw_denominator=float(eligible),
                    point_estimate=_rate(core_successes, eligible),
                    violation_count=max(0, eligible - core_successes),
                    severity_mass=float(sum(state.incident_counts.values())),
                    artifact_refs=state.artifact_refs,
                ),
            ),
            incidents=incidents,
            achieved_evidence_tier=EvidenceTier.E3,
            denominator=denominator,
            uncertainty=state.uncertainty,
            blind_spots=state.uncertainty,
            artifact_refs=state.artifact_refs,
        ),
    )


def _display_rate(value: float | None) -> str:
    return "unknown/not exposed" if value is None else f"{value:.3f}"


def render_source_semantic_markdown(state: SourceSemanticEvaluationState) -> str:
    lines = [
        "# Source-Semantic Admission State",
        "",
        f"- Run: `{state.scope.run_id}`",
        f"- Eligible grounding traces: {state.eligible_grounding_count}",
        f"- Interpretations: {state.interpretation_count}",
        f"- Explicit decisions: {state.decision_count}",
        "",
        "| Measure | Rate |",
        "| --- | ---: |",
    ]
    measures = (
        ("Grounding to interpretation coverage", state.eligible_grounding_interpretation_coverage),
        ("Source coordinate reconstructability", state.source_coordinate_reconstructability_rate),
        ("Interpretation structural closure", state.interpretation_structural_closure_rate),
        ("Grounding continuity exactness", state.grounding_continuity_exactness_rate),
        ("Explicit admission fate coverage", state.explicit_admission_fate_coverage),
        ("Supported-report precision", state.supported_report_admission_precision),
        ("Supported-report recall", state.supported_report_admission_recall),
        ("Epistemic admission continuity", state.epistemic_consumer_admission_continuity_rate),
        ("Applied decision to Model", state.applied_decision_model_coverage),
        ("Exactly one Model", state.one_model_cardinality_rate),
        ("Model source provenance", state.model_source_provenance_rate),
        ("Model scope referent", state.model_scope_referent_rate),
        ("Model grounding dependency", state.model_grounding_dependency_rate),
        ("Complete Model dependency closure", state.model_dependency_closure_rate),
        ("No-admission creates no Model", state.non_admitted_no_model_safety_rate),
    )
    lines.extend(f"| {label} | {_display_rate(value)} |" for label, value in measures)
    lines.extend(("", "## Incidents", ""))
    if state.incident_counts:
        lines.extend(
            f"- `{kind}`: {count} ({', '.join(state.incident_trace_refs[kind])})"
            for kind, count in state.incident_counts.items()
        )
    else:
        lines.append("- None observed in scope.")
    lines.extend(("", "## Uncertainty", ""))
    lines.extend(f"- {item}" for item in state.uncertainty)
    return "\n".join(lines) + "\n"


__all__ = [
    "SourceSemanticEvaluationScope",
    "SourceSemanticEvaluationState",
    "analyze_source_semantic_rows",
    "build_source_semantic_invariant_evidence",
    "evaluate_source_semantic_state",
    "render_source_semantic_markdown",
]
