"""Gold evaluation of the persisted entity grounding pipeline.

This evaluator starts at normalized, persisted observations.  It reads the
durable mention -> candidate -> assessment -> trace chain, but correctness is
defined exclusively by evaluator-owned gold and canonical-label mappings.
Operational fate strings are never treated as evidence that a decision was
correct.
"""

from __future__ import annotations

import json
import math
from typing import Any, Literal, Mapping, Sequence
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class GoldRelationExpectation(_Record):
    """Evaluator-owned graph consequence of one or more grounded mentions.

    ``expected_admission=False`` is a strong no-edge expectation: no active
    relation may originate from the listed mention cases. Endpoint/type fields
    are therefore only legal for expected admissions.
    """

    expectation_id: str = Field(min_length=1)
    expected_admission: bool
    source_model_gold_label: str | None = None
    target_model_gold_label: str | None = None
    relation_type: str | None = None
    source_mention_case_ids: tuple[str, ...] = Field(min_length=1)

    def model_post_init(self, __context: Any) -> None:
        typed = (
            self.source_model_gold_label,
            self.target_model_gold_label,
            self.relation_type,
        )
        if self.expected_admission and not all(typed):
            raise ValueError(
                "admitted relation expectations require source, target, and type"
            )
        if not self.expected_admission and any(value is not None for value in typed):
            raise ValueError("non-admission expectations must not describe an edge")


class GoldEntityPipelineCase(_Record):
    case_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source_observation_id: UUID
    surface: str = Field(min_length=1)
    gold_entity_type: str = Field(min_length=1)
    gold_canonical_label: str | None = None
    expected_detection_fate: Literal["detected", "rejected"] | None = None
    acceptable_terminal_fates: tuple[
        Literal["resolved_for_consumer", "review", "unresolved", "abstained"], ...
    ] = ()
    expected_semantic_disposition: Literal["belief_applied", "no_admission"] | None = None
    expected_relations: tuple[GoldRelationExpectation, ...] = ()


class EntityPipelineMetrics(_Record):
    gold_case_count: int = Field(ge=0)
    detected_case_count: int = Field(ge=0)
    candidate_population_count: int = Field(ge=0)
    assessed_case_count: int = Field(ge=0)
    type_assessed_case_count: int = Field(ge=0)
    terminal_case_count: int = Field(ge=0)
    governed_fate_coverage: float | None = Field(default=None, ge=0, le=1)
    candidate_population_coverage: float | None = Field(default=None, ge=0, le=1)
    resolution_assessment_coverage: float | None = Field(default=None, ge=0, le=1)
    detection_accuracy: float | None = Field(default=None, ge=0, le=1)
    type_assessment_coverage: float | None = Field(default=None, ge=0, le=1)
    type_assessment_accuracy: float | None = Field(default=None, ge=0, le=1)
    mean_gold_type_probability: float | None = Field(default=None, ge=0, le=1)
    type_assessment_brier_score: float | None = Field(default=None, ge=0)
    type_assessment_log_loss: float | None = Field(default=None, ge=0)
    candidate_recall_at_k: dict[int, float | None]
    candidate_recall_hits_at_k: dict[int, int] = Field(default_factory=dict)
    candidate_recall_population_count: int = Field(default=0, ge=0)
    gold_type_present_at_k: dict[int, float | None]
    selected_type_accuracy: float | None = Field(default=None, ge=0, le=1)
    canonical_link_accuracy: float | None = Field(default=None, ge=0, le=1)
    canonical_link_coverage: float | None = Field(default=None, ge=0, le=1)
    canonical_link_correct_count: int = Field(default=0, ge=0)
    canonical_link_admitted_count: int = Field(default=0, ge=0)
    canonical_link_population_count: int = Field(default=0, ge=0)
    abstention_rate: float | None = Field(default=None, ge=0, le=1)
    review_rate: float | None = Field(default=None, ge=0, le=1)
    terminal_fate_accuracy: float | None = Field(default=None, ge=0, le=1)
    safe_decision_rate: float | None = Field(default=None, ge=0, le=1)
    harmful_false_link_rate: float | None = Field(default=None, ge=0, le=1)
    detection_to_terminal_coverage: float | None = Field(default=None, ge=0, le=1)
    lineage_integrity: float | None = Field(default=None, ge=0, le=1)
    rejected_detection_candidate_count: int = Field(ge=0)
    unknown_canonical_ref_count: int = Field(ge=0)
    known_wrong_type_consequential_admission_count: int = Field(default=0, ge=0)
    invalid_type_assessment_count: int = Field(ge=0)
    type_assessment_lineage_integrity: float | None = Field(default=None, ge=0, le=1)
    semantic_expected_case_count: int = Field(ge=0)
    semantic_interpretation_count: int = Field(ge=0)
    semantic_decision_count: int = Field(ge=0)
    belief_applied_count: int = Field(ge=0)
    semantic_interpretation_coverage: float | None = Field(default=None, ge=0, le=1)
    semantic_decision_coverage: float | None = Field(default=None, ge=0, le=1)
    semantic_disposition_accuracy: float | None = Field(default=None, ge=0, le=1)
    semantic_lineage_integrity: float | None = Field(default=None, ge=0, le=1)
    belief_model_materialization_rate: float | None = Field(default=None, ge=0, le=1)
    belief_model_lineage_integrity: float | None = Field(default=None, ge=0, le=1)
    no_admission_no_model_safety_rate: float | None = Field(default=None, ge=0, le=1)
    harmful_semantic_propagation_rate: float | None = Field(default=None, ge=0, le=1)
    safe_no_admission_count: int = Field(default=0, ge=0)
    no_admission_count: int = Field(default=0, ge=0)
    harmful_semantic_propagation_count: int = Field(default=0, ge=0)
    semantic_propagation_count: int = Field(default=0, ge=0)
    relation_expectation_count: int = Field(ge=0)
    expected_relation_admission_count: int = Field(ge=0)
    observed_active_relation_count: int = Field(ge=0)
    relation_admission_accuracy: float | None = Field(default=None, ge=0, le=1)
    expected_relation_recall: float | None = Field(default=None, ge=0, le=1)
    relation_non_admission_safety_rate: float | None = Field(default=None, ge=0, le=1)
    relation_endpoint_accuracy: float | None = Field(default=None, ge=0, le=1)
    relation_type_accuracy: float | None = Field(default=None, ge=0, le=1)
    relation_direction_accuracy: float | None = Field(default=None, ge=0, le=1)
    relation_lineage_coverage: float | None = Field(default=None, ge=0, le=1)
    relation_lineage_integrity: float | None = Field(default=None, ge=0, le=1)
    relation_lineage_correct_count: int = Field(default=0, ge=0)
    exact_admitted_relation_count: int = Field(default=0, ge=0)
    unexpected_relation_rate: float | None = Field(default=None, ge=0, le=1)
    harmful_topology_relation_count: int = Field(ge=0)
    harmful_topology_model_count: int = Field(ge=0)
    harmful_topology_propagation_rate: float | None = Field(default=None, ge=0, le=1)
    unknown_topology_endpoint_count: int = Field(ge=0)
    unlineaged_active_relation_count: int = Field(ge=0)
    unlineaged_active_relation_rate: float | None = Field(default=None, ge=0, le=1)


class GoldEntityPipelineReport(_Record):
    schema_version: str = "gold-entity-pipeline-v4"
    overall: EntityPipelineMetrics
    by_batch: dict[str, EntityPipelineMetrics]
    uncertainties: tuple[str, ...] = ()


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def canonical_ref_key(ref: Mapping[str, Any]) -> str:
    """Stable evaluator key; labels remain sealed outside runtime tables."""

    return f"{ref.get('type', '')}:{ref.get('id', '')}:v{int(ref.get('version', 1))}"


def _candidate_ref(candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    if str(candidate.get("kind")) != "canonical_referent":
        return None
    referent_id = candidate.get("canonical_referent_id")
    if not referent_id:
        return None
    return {
        "type": str(candidate.get("candidate_type") or ""),
        "id": str(referent_id),
        "version": int(candidate.get("canonical_referent_version") or 1),
    }


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _probability_distribution(value: Any) -> dict[str, float] | None:
    if not isinstance(value, dict) or not value:
        return None
    try:
        result = {str(key): float(probability) for key, probability in value.items()}
    except (TypeError, ValueError):
        return None
    if (
        any(
            not math.isfinite(probability) or not 0 <= probability <= 1
            for probability in result.values()
        )
        or not math.isclose(sum(result.values()), 1.0, rel_tol=1e-6, abs_tol=1e-6)
        or "unknown" not in result
    ):
        return None
    return result


def _active_relations(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = _json(row.get("downstream_relations")) or []
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict) or str(item.get("status")) != "active":
            continue
        relation_id = str(item.get("id") or "")
        if not relation_id or relation_id in seen:
            continue
        if not all(item.get(field) for field in (
            "source_model_id", "target_model_id", "edge_kind",
        )):
            continue
        seen.add(relation_id)
        result.append(item)
    return result


def _relation_mention_ids(relation: Mapping[str, Any]) -> set[str]:
    metadata = _json(relation.get("metadata")) or {}
    if not isinstance(metadata, dict):
        return set()
    values: list[Any] = []
    singular = metadata.get("source_entity_mention_id")
    if singular is not None:
        values.append(singular)
    plural = metadata.get("source_entity_mention_ids")
    if isinstance(plural, list):
        values.extend(plural)
    return {str(value) for value in values if value is not None}


def _relation_labels(
    relation: Mapping[str, Any],
    model_gold_labels: Mapping[str, str],
) -> tuple[str | None, str | None]:
    return (
        model_gold_labels.get(str(relation.get("source_model_id"))),
        model_gold_labels.get(str(relation.get("target_model_id"))),
    )


def analyze_entity_pipeline_rows(
    *,
    gold_cases: Sequence[GoldEntityPipelineCase],
    canonical_gold_labels: Mapping[str, str],
    rows: Sequence[Mapping[str, Any]],
    topology_model_gold_labels: Mapping[str, str] | None = None,
    ks: Sequence[int] = (1, 3, 5),
) -> GoldEntityPipelineReport:
    """Score joined DB rows without trusting resolver-declared success fates."""

    if not ks or any(k <= 0 for k in ks):
        raise ValueError("ks must contain positive values")
    topology_model_gold_labels = topology_model_gold_labels or {}
    keys = [(case.source_observation_id, case.surface) for case in gold_cases]
    if len(keys) != len(set(keys)) or len(
        {case.case_id for case in gold_cases}
    ) != len(gold_cases):
        raise ValueError("gold cases must have unique IDs and observation/surface keys")
    case_ids = {case.case_id for case in gold_cases}
    relation_expectations = [
        expectation
        for case in gold_cases
        for expectation in case.expected_relations
    ]
    expectation_ids = [item.expectation_id for item in relation_expectations]
    if len(expectation_ids) != len(set(expectation_ids)):
        raise ValueError("relation expectation IDs must be globally unique")
    if any(
        source_case_id not in case_ids
        for item in relation_expectations
        for source_case_id in item.source_mention_case_ids
    ):
        raise ValueError("relation expectation references an unknown mention case")
    rows_by_key = {
        (UUID(str(row["source_observation_id"])), str(row["candidate_surface"])): row
        for row in rows
    }

    def metrics(cases: Sequence[GoldEntityPipelineCase]) -> EntityPipelineMetrics:
        governed = detected = candidate_population = assessed = type_assessed = terminal = 0
        detection_expected = detection_correct = 0
        type_correct = type_lineage_ok = 0
        gold_type_probability_sum = brier_sum = log_loss_sum = 0.0
        terminal_expected = terminal_correct = 0
        safe_decisions = harmful_links = 0
        selected_type_total = selected_type_correct = 0
        known_link_total = linked = link_correct = 0
        abstained = reviewed = lineage_ok = rejected_with_candidates = unknown_refs = 0
        invalid_type_assessments = 0
        known_wrong_type_consequential_admissions = 0
        semantic_expected = semantic_interpreted = semantic_decided = 0
        semantic_disposition_correct = semantic_lineage_ok = 0
        belief_applied = belief_materialized = belief_model_lineage_ok = 0
        semantic_propagated = 0
        no_admission = safe_no_admission = harmful_semantic_propagations = 0
        relations_by_case: dict[str, list[dict[str, Any]]] = {}
        mention_id_by_case: dict[str, str] = {}
        unsafe_topology_origin: dict[str, bool] = {}
        recall_hits = {k: 0 for k in ks}
        type_hits = {k: 0 for k in ks}
        recall_denominator = type_denominator = 0

        for case in cases:
            row = rows_by_key.get((case.source_observation_id, case.surface))
            if row is None:
                relations_by_case[case.case_id] = []
                unsafe_topology_origin[case.case_id] = True
                detection_expected += int(case.expected_detection_fate is not None)
                terminal_expected += int(bool(case.acceptable_terminal_fates))
                semantic_expected += int(case.expected_semantic_disposition is not None)
                continue
            relations_by_case[case.case_id] = _active_relations(row)
            if row.get("entity_mention_id") is not None:
                mention_id_by_case[case.case_id] = str(row["entity_mention_id"])
            governed += 1
            fate = str(row.get("detection_fate") or "")
            is_detected = fate == "detected"
            detected += int(is_detected)
            if case.expected_detection_fate is not None:
                detection_expected += 1
                detection_correct += int(
                    is_detected == (case.expected_detection_fate == "detected")
                )
            semantic_expected += int(case.expected_semantic_disposition is not None)
            raw_candidates = _json(row.get("candidates")) or []
            candidates = [item for item in raw_candidates if isinstance(item, dict)]
            if not is_detected:
                unsafe_topology_origin[case.case_id] = True
                rejected_with_candidates += int(
                    bool(candidates or row.get("candidate_request_id"))
                )
                # A governed rejection is terminal at the detection stage.
                terminal += int(fate.startswith("rejected_") or fate == "unsupported_implicit")
                lineage_ok += int(not candidates and row.get("candidate_request_id") is None)
                continue

            detection_command = _json(row.get("detection_command")) or {}
            type_assessment = (
                (detection_command.get("detection") or {}).get("entity_type_assessment")
                if isinstance(detection_command, dict)
                else None
            )
            type_distribution = (
                type_assessment.get("type_distribution")
                if isinstance(type_assessment, dict)
                else None
            )
            probabilities = _probability_distribution(type_distribution)
            if probabilities is not None:
                type_assessed += 1
                predicted_type = min(
                    probabilities,
                    key=lambda key: (-probabilities[key], key),
                )
                type_correct += int(predicted_type == case.gold_entity_type)
                gold_probability = probabilities.get(case.gold_entity_type, 0.0)
                gold_type_probability_sum += gold_probability
                brier_sum += sum(
                    (probability - float(entity_type == case.gold_entity_type)) ** 2
                    for entity_type, probability in probabilities.items()
                )
                log_loss_sum += -math.log(max(gold_probability, 1e-15))
                request_payload = _json(row.get("candidate_request")) or {}
                assessment_ref = str(type_assessment.get("assessment_id") or "")
                refs = request_payload.get("entity_type_assessment_refs") or []
                type_lineage_ok += int(
                    bool(assessment_ref)
                    and assessment_ref in {str(ref) for ref in refs}
                )
            elif type_assessment is not None:
                invalid_type_assessments += 1

            has_population = row.get("candidate_set_id") is not None and bool(candidates)
            candidate_population += int(has_population)
            distribution = _json(row.get("candidate_distribution")) or {}
            has_assessment = row.get("assessment_id") is not None and isinstance(
                distribution, dict
            )
            assessed += int(has_assessment)
            selected_id = row.get("selected_candidate_id")
            by_id = {str(item.get("candidate_id")): item for item in candidates}
            ranked = sorted(
                candidates,
                key=lambda item: (
                    -float(distribution.get(str(item.get("candidate_id")), 0.0)),
                    str(item.get("candidate_id")),
                ),
            )
            canonical_ranked = [(item, _candidate_ref(item)) for item in ranked]

            if case.gold_canonical_label is not None:
                recall_denominator += 1
                known_link_total += 1
                for k in ks:
                    labels = {
                        canonical_gold_labels.get(canonical_ref_key(ref))
                        for _, ref in canonical_ranked[:k]
                        if ref is not None
                    }
                    recall_hits[k] += int(case.gold_canonical_label in labels)
            type_denominator += 1
            for k in ks:
                types = {str(item.get("candidate_type") or "") for item, _ in canonical_ranked[:k]}
                type_hits[k] += int(case.gold_entity_type in types)

            selected = by_id.get(str(selected_id)) if selected_id is not None else None
            selected_ref = _candidate_ref(selected) if selected else None
            if selected_ref is not None:
                selected_type_total += 1
                selected_type_correct += int(
                    selected.get("candidate_type") == case.gold_entity_type
                )
            admitted_ref = _json(row.get("selected_referent"))
            resolved_label = None
            if admitted_ref:
                linked += int(case.gold_canonical_label is not None)
                known_wrong_type_consequential_admissions += int(
                    selected is not None
                    and selected.get("candidate_type") != case.gold_entity_type
                )
                resolved_label = canonical_gold_labels.get(canonical_ref_key(admitted_ref))
                unknown_refs += int(resolved_label is None)
                link_correct += int(
                    case.gold_canonical_label is not None
                    and resolved_label == case.gold_canonical_label
                )
                harmful_links += int(
                    case.gold_canonical_label is None
                    or resolved_label != case.gold_canonical_label
                )
            unsafe_topology_origin[case.case_id] = (
                case.gold_canonical_label is None
                or not admitted_ref
                or resolved_label != case.gold_canonical_label
            )
            current_fate = str(row.get("current_fate") or "")
            abstained += int(current_fate in {"abstained", "unresolved"})
            reviewed += int(current_fate == "review")
            is_terminal = current_fate in {
                "resolved_for_consumer", "review", "unresolved", "abstained",
            }
            terminal += int(is_terminal)
            safe_decisions += int(
                (
                    case.gold_canonical_label is not None
                    and resolved_label == case.gold_canonical_label
                )
                if admitted_ref
                else current_fate in {"review", "unresolved", "abstained"}
            )
            if case.acceptable_terminal_fates:
                terminal_expected += 1
                terminal_correct += int(current_fate in case.acceptable_terminal_fates)

            ids = (
                row.get("context_snapshot_id"),
                row.get("detection_id"),
                row.get("candidate_request_id"),
                row.get("candidate_set_id"), row.get("assessment_id"), row.get("trace_id"),
                row.get("admission_id"),
            )
            embedded = _json(row.get("trace")) or {}
            trace_ids = (
                ((embedded.get("context_snapshot") or {}).get("id")),
                ((embedded.get("mention_detection") or {}).get("id")),
                ((embedded.get("candidate_request") or {}).get("id")),
                ((embedded.get("candidate_set") or {}).get("id")),
                ((embedded.get("assessment") or {}).get("id")),
                embedded.get("id") or row.get("trace_id"),
                ((embedded.get("admission") or {}).get("id")),
            )
            lineage_ok += int(
                is_terminal and all(ids)
                and all(str(actual) == str(expected) for actual, expected in zip(ids, trace_ids))
            )

            if case.expected_semantic_disposition is not None:
                interpretation_id = row.get("semantic_interpretation_id")
                semantic_admission_id = row.get("semantic_admission_id")
                semantic_disposition = str(row.get("semantic_disposition") or "")
                has_interpretation = interpretation_id is not None
                has_semantic_decision = semantic_admission_id is not None
                semantic_interpreted += int(has_interpretation)
                semantic_decided += int(has_semantic_decision)
                semantic_disposition_correct += int(
                    has_semantic_decision
                    and semantic_disposition == case.expected_semantic_disposition
                )
                semantic_continuity = (
                    _json(row.get("semantic_grounding_continuity")) or {}
                )
                semantic_grounding_admission_id = row.get(
                    "semantic_grounding_admission_id"
                )
                is_belief_applied = semantic_disposition == "belief_applied"
                is_no_admission = semantic_disposition == "no_admission"
                model_id = row.get("downstream_model_id")
                admitted_model_id = row.get("semantic_admitted_model_id")
                model_materialized = model_id is not None
                shared_semantic_lineage = (
                    has_interpretation
                    and str(row.get("semantic_grounding_trace_id"))
                    == str(row.get("trace_id"))
                    and str(row.get("semantic_source_observation_id"))
                    == str(case.source_observation_id)
                    and str(row.get("semantic_context_snapshot_id"))
                    == str(row.get("context_snapshot_id"))
                    and str(row.get("semantic_entity_mention_id"))
                    == str(row.get("entity_mention_id"))
                    and str(row.get("semantic_resolution_assessment_id"))
                    == str(row.get("assessment_id"))
                    and semantic_grounding_admission_id is not None
                    and str(row.get("semantic_grounding_admission_assessment_id"))
                    == str(row.get("assessment_id"))
                    and semantic_continuity.get("grounding_admission_ref")
                    == f"grounding-admission:{semantic_grounding_admission_id}"
                    and semantic_continuity.get("resolution_assessment_ref")
                    == f"resolution-assessment:{row.get('assessment_id')}"
                    and semantic_continuity.get("mention_ref")
                    == row.get("mention_ref")
                )
                belief_lineage = (
                    is_belief_applied
                    and str(semantic_grounding_admission_id)
                    != str(row.get("admission_id"))
                    and row.get("semantic_grounding_admission_consumer")
                    == "epistemic-applier"
                    and row.get("semantic_grounding_admission_purpose")
                    == "belief-admission"
                    and row.get("semantic_grounding_admission_operation")
                    == "create-grounded-belief"
                    and semantic_continuity.get("downstream_object_ref")
                    == f"model:{admitted_model_id}"
                )
                no_admission_lineage = (
                    is_no_admission
                    and str(semantic_grounding_admission_id)
                    == str(row.get("admission_id"))
                    and semantic_continuity.get("downstream_object_ref")
                    == f"source-semantic-interpretation:{interpretation_id}"
                    and admitted_model_id is None
                    and not model_materialized
                    and int(row.get("semantic_interpretation_model_count") or 0) == 0
                )
                semantic_lineage_ok += int(
                    shared_semantic_lineage
                    and (belief_lineage or no_admission_lineage)
                )
                interpretation_model_count = int(
                    row.get("semantic_interpretation_model_count") or 0
                )
                semantic_propagated += int(interpretation_model_count > 0)
                belief_applied += int(is_belief_applied)
                belief_materialized += int(is_belief_applied and model_materialized)
                proposition = _json(row.get("downstream_model_proposition")) or {}
                belief_model_lineage_ok += int(
                    is_belief_applied
                    and model_materialized
                    and str(model_id) == str(admitted_model_id)
                    and isinstance(proposition, dict)
                    and str(proposition.get("source_semantic_interpretation_id"))
                    == str(interpretation_id)
                )
                no_admission += int(is_no_admission)
                safe_no_admission += int(
                    is_no_admission
                    and admitted_model_id is None
                    and model_id is None
                    and interpretation_model_count == 0
                )
                harmful_semantic_propagations += int(
                    interpretation_model_count > 0
                    and (
                        case.gold_canonical_label is None
                        or not admitted_ref
                        or canonical_gold_labels.get(canonical_ref_key(admitted_ref))
                        != case.gold_canonical_label
                    )
                )

        expectations = [
            expectation
            for case in cases
            for expectation in case.expected_relations
        ]
        expected_admissions = [item for item in expectations if item.expected_admission]
        expected_non_admissions = [
            item for item in expectations if not item.expected_admission
        ]
        unique_relations: dict[str, dict[str, Any]] = {}
        relation_origins: dict[str, set[str]] = {}
        for case_id, relations in relations_by_case.items():
            for relation in relations:
                relation_id = str(relation["id"])
                unique_relations.setdefault(relation_id, relation)
        case_id_by_mention_id = {
            mention_id: case_id for case_id, mention_id in mention_id_by_case.items()
        }
        for relation_id, relation in unique_relations.items():
            relation_origins[relation_id] = {
                case_id_by_mention_id[mention_id]
                for mention_id in _relation_mention_ids(relation)
                if mention_id in case_id_by_mention_id
            }
        unlineaged_relation_ids = {
            relation_id
            for relation_id, origins in relation_origins.items()
            if not origins
        }

        admission_correct = endpoint_correct = relation_type_correct = 0
        direction_correct = exact_admitted = relation_lineage_ok = 0
        non_admission_correct = 0
        matched_expected_relation_ids: set[str] = set()
        for expectation in expected_admissions:
            expected_origin_ids = set(expectation.source_mention_case_ids)
            candidates = {
                relation_id: relation
                for relation_id, relation in unique_relations.items()
                if relation_origins.get(relation_id, set()) & expected_origin_ids
            }
            scored: list[tuple[tuple[int, int, int, str], dict[str, Any]]] = []
            for relation_id, relation in candidates.items():
                source_label, target_label = _relation_labels(
                    relation, topology_model_gold_labels
                )
                endpoints_ok = sorted((source_label or "", target_label or "")) == sorted((
                    expectation.source_model_gold_label or "",
                    expectation.target_model_gold_label or "",
                ))
                direction_ok = (
                    source_label == expectation.source_model_gold_label
                    and target_label == expectation.target_model_gold_label
                )
                type_ok = str(relation.get("edge_kind")) == expectation.relation_type
                scored.append((
                    (
                        int(endpoints_ok) + int(direction_ok) + int(type_ok),
                        int(direction_ok),
                        int(type_ok),
                        relation_id,
                    ),
                    relation,
                ))
            selected_relation = max(scored, default=None, key=lambda item: item[0])
            if selected_relation is None:
                continue
            relation = selected_relation[1]
            source_label, target_label = _relation_labels(
                relation, topology_model_gold_labels
            )
            endpoints_ok = sorted((source_label or "", target_label or "")) == sorted((
                expectation.source_model_gold_label or "",
                expectation.target_model_gold_label or "",
            ))
            direction_ok = (
                source_label == expectation.source_model_gold_label
                and target_label == expectation.target_model_gold_label
            )
            type_ok = str(relation.get("edge_kind")) == expectation.relation_type
            exact = endpoints_ok and direction_ok and type_ok
            endpoint_correct += int(endpoints_ok)
            direction_correct += int(direction_ok)
            relation_type_correct += int(type_ok)
            admission_correct += int(exact)
            exact_admitted += int(exact)
            if exact:
                relation_id = str(relation["id"])
                matched_expected_relation_ids.add(relation_id)
                required_mentions = {
                    mention_id_by_case[source_case_id]
                    for source_case_id in expectation.source_mention_case_ids
                    if source_case_id in mention_id_by_case
                }
                lineage_ok_for_relation = (
                    len(required_mentions) == len(expectation.source_mention_case_ids)
                    and required_mentions <= _relation_mention_ids(relation)
                )
                relation_lineage_ok += int(lineage_ok_for_relation)

        for expectation in expected_non_admissions:
            expected_origin_ids = set(expectation.source_mention_case_ids)
            has_relation = any(
                origins & expected_origin_ids
                for origins in relation_origins.values()
            )
            non_admission_correct += int(not has_relation)
            admission_correct += int(not has_relation)

        unexpected_relation_ids = (
            set(unique_relations) - matched_expected_relation_ids
        )
        harmful_relation_ids = {
            relation_id
            for relation_id in unique_relations
            if relation_id in unexpected_relation_ids
            or any(
                unsafe_topology_origin.get(case_id, True)
                for case_id in relation_origins.get(relation_id, set())
            )
        }
        harmful_models = {
            str(unique_relations[relation_id][field])
            for relation_id in harmful_relation_ids
            for field in ("source_model_id", "target_model_id")
        }
        unknown_topology_endpoints = sum(
            str(relation[field]) not in topology_model_gold_labels
            for relation in unique_relations.values()
            for field in ("source_model_id", "target_model_id")
        )

        return EntityPipelineMetrics(
            gold_case_count=len(cases), detected_case_count=detected,
            candidate_population_count=candidate_population, assessed_case_count=assessed,
            type_assessed_case_count=type_assessed,
            terminal_case_count=terminal,
            governed_fate_coverage=_rate(governed, len(cases)),
            candidate_population_coverage=_rate(candidate_population, detected),
            resolution_assessment_coverage=_rate(assessed, detected),
            detection_accuracy=_rate(detection_correct, detection_expected),
            type_assessment_coverage=_rate(type_assessed, detected),
            type_assessment_accuracy=_rate(type_correct, type_assessed),
            mean_gold_type_probability=(
                gold_type_probability_sum / type_assessed if type_assessed else None
            ),
            type_assessment_brier_score=(brier_sum / type_assessed if type_assessed else None),
            type_assessment_log_loss=(log_loss_sum / type_assessed if type_assessed else None),
            candidate_recall_at_k={k: _rate(recall_hits[k], recall_denominator) for k in ks},
            candidate_recall_hits_at_k=dict(recall_hits),
            candidate_recall_population_count=recall_denominator,
            gold_type_present_at_k={k: _rate(type_hits[k], type_denominator) for k in ks},
            selected_type_accuracy=_rate(selected_type_correct, selected_type_total),
            canonical_link_accuracy=_rate(link_correct, linked),
            canonical_link_coverage=_rate(linked, known_link_total),
            canonical_link_correct_count=link_correct,
            canonical_link_admitted_count=linked,
            canonical_link_population_count=known_link_total,
            abstention_rate=_rate(abstained, detected), review_rate=_rate(reviewed, detected),
            terminal_fate_accuracy=_rate(terminal_correct, terminal_expected),
            safe_decision_rate=_rate(safe_decisions, detected),
            harmful_false_link_rate=_rate(harmful_links, detected),
            detection_to_terminal_coverage=_rate(terminal, len(cases)),
            lineage_integrity=_rate(lineage_ok, len(cases)),
            rejected_detection_candidate_count=rejected_with_candidates,
            unknown_canonical_ref_count=unknown_refs,
            known_wrong_type_consequential_admission_count=(
                known_wrong_type_consequential_admissions
            ),
            invalid_type_assessment_count=invalid_type_assessments,
            type_assessment_lineage_integrity=_rate(type_lineage_ok, type_assessed),
            semantic_expected_case_count=semantic_expected,
            semantic_interpretation_count=semantic_interpreted,
            semantic_decision_count=semantic_decided,
            belief_applied_count=belief_applied,
            semantic_interpretation_coverage=_rate(semantic_interpreted, semantic_expected),
            semantic_decision_coverage=_rate(semantic_decided, semantic_expected),
            semantic_disposition_accuracy=_rate(
                semantic_disposition_correct, semantic_expected
            ),
            semantic_lineage_integrity=_rate(semantic_lineage_ok, semantic_interpreted),
            belief_model_materialization_rate=_rate(
                belief_materialized, belief_applied
            ),
            belief_model_lineage_integrity=_rate(
                belief_model_lineage_ok, belief_applied
            ),
            no_admission_no_model_safety_rate=_rate(safe_no_admission, no_admission),
            harmful_semantic_propagation_rate=_rate(
                harmful_semantic_propagations,
                semantic_propagated,
            ),
            safe_no_admission_count=safe_no_admission,
            no_admission_count=no_admission,
            harmful_semantic_propagation_count=harmful_semantic_propagations,
            semantic_propagation_count=semantic_propagated,
            relation_expectation_count=len(expectations),
            expected_relation_admission_count=len(expected_admissions),
            observed_active_relation_count=len(unique_relations),
            relation_admission_accuracy=_rate(
                admission_correct, len(expectations)
            ),
            expected_relation_recall=_rate(
                exact_admitted, len(expected_admissions)
            ),
            relation_non_admission_safety_rate=_rate(
                non_admission_correct, len(expected_non_admissions)
            ),
            relation_endpoint_accuracy=_rate(
                endpoint_correct, len(expected_admissions)
            ),
            relation_type_accuracy=_rate(
                relation_type_correct, len(expected_admissions)
            ),
            relation_direction_accuracy=_rate(
                direction_correct, len(expected_admissions)
            ),
            relation_lineage_coverage=_rate(
                relation_lineage_ok, len(expected_admissions)
            ),
            relation_lineage_integrity=_rate(
                relation_lineage_ok, exact_admitted
            ),
            relation_lineage_correct_count=relation_lineage_ok,
            exact_admitted_relation_count=exact_admitted,
            unexpected_relation_rate=_rate(
                len(unexpected_relation_ids), len(unique_relations)
            ),
            harmful_topology_relation_count=len(harmful_relation_ids),
            harmful_topology_model_count=len(harmful_models),
            harmful_topology_propagation_rate=_rate(
                len(harmful_relation_ids), len(unique_relations)
            ),
            unknown_topology_endpoint_count=unknown_topology_endpoints,
            unlineaged_active_relation_count=len(unlineaged_relation_ids),
            unlineaged_active_relation_rate=_rate(
                len(unlineaged_relation_ids), len(unique_relations)
            ),
        )

    overall = metrics(gold_cases)
    batches = sorted({case.batch_id for case in gold_cases})
    uncertainties = []
    if not gold_cases:
        uncertainties.append("no_gold_entity_pipeline_cases")
    if any(case.gold_canonical_label is None for case in gold_cases):
        uncertainties.append("canonical_metrics_exclude_open_world_gold_cases")
    if any(case.expected_detection_fate is None for case in gold_cases):
        uncertainties.append("detection_accuracy_excludes_unlabeled_cases")
    if any(not case.acceptable_terminal_fates for case in gold_cases):
        uncertainties.append("terminal_fate_accuracy_excludes_unlabeled_cases")
    if overall.type_assessed_case_count < overall.detected_case_count:
        uncertainties.append("detected_cases_without_valid_type_assessment")
    if any(case.expected_semantic_disposition is None for case in gold_cases):
        uncertainties.append("semantic_impact_metrics_exclude_unlabeled_cases")
    if any(not case.expected_relations for case in gold_cases):
        uncertainties.append("relation_topology_metrics_exclude_unlabeled_cases")
    return GoldEntityPipelineReport(
        overall=overall,
        by_batch={
            batch: metrics([case for case in gold_cases if case.batch_id == batch])
            for batch in batches
        },
        uncertainties=tuple(uncertainties),
    )


async def evaluate_persisted_entity_pipeline(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    gold_cases: Sequence[GoldEntityPipelineCase],
    canonical_gold_labels: Mapping[str, str],
    topology_model_gold_labels: Mapping[str, str] | None = None,
    ks: Sequence[int] = (1, 3, 5),
) -> GoldEntityPipelineReport:
    """Load current persisted heads and evaluate the complete grounding chain."""

    observation_ids = tuple({case.source_observation_id for case in gold_cases})
    rows = await conn.fetch(
        """
        SELECT d.source_observation_id, d.candidate_surface,
               d.id AS detection_id, d.fate AS detection_fate,
               d.mention_id AS entity_mention_id,
               d.context_snapshot_id, acr.command AS detection_command,
               req.id AS candidate_request_id, req.mention_ref,
               cs.id AS candidate_set_id,
               req.request AS candidate_request, cs.candidates, ra.id AS assessment_id,
               ra.candidate_distribution, ra.selected_candidate_id,
               gad.id AS admission_id, gt.id AS trace_id, gt.current_fate,
               gt.selected_referent, gt.trace,
               ssi.id AS semantic_interpretation_id,
               ssi.grounding_trace_id AS semantic_grounding_trace_id,
               ssi.source_observation_id AS semantic_source_observation_id,
               ssi.context_snapshot_id AS semantic_context_snapshot_id,
               ssi.entity_mention_id AS semantic_entity_mention_id,
               ssi.resolution_assessment_id AS semantic_resolution_assessment_id,
               ssi.grounding_admission_id AS semantic_grounding_admission_id,
               ssi.grounding_continuity AS semantic_grounding_continuity,
               semantic_gad.assessment_id
                   AS semantic_grounding_admission_assessment_id,
               semantic_gad.consumer AS semantic_grounding_admission_consumer,
               semantic_gad.purpose AS semantic_grounding_admission_purpose,
               semantic_gad.operation AS semantic_grounding_admission_operation,
               ssad.id AS semantic_admission_id,
               ssad.disposition AS semantic_disposition,
               ssad.admitted_model_id AS semantic_admitted_model_id,
               downstream_model.id AS downstream_model_id,
               downstream_model.proposition AS downstream_model_proposition,
               topology.downstream_relations,
               COALESCE((
                 SELECT count(*)
                 FROM models semantic_model
                 WHERE semantic_model.tenant_id=ssi.tenant_id
                   AND semantic_model.proposition->>'source_semantic_interpretation_id'
                       = ssi.id::text
               ), 0) AS semantic_interpretation_model_count
        FROM entity_mention_detection_heads h
        JOIN entity_mention_detections d
          ON d.tenant_id=h.tenant_id AND d.id=h.current_detection_id
        JOIN agency_command_results acr
          ON acr.tenant_id=d.tenant_id AND acr.id=d.command_result_id
        LEFT JOIN entity_candidate_generation_requests req
          ON req.tenant_id=d.tenant_id AND req.entity_mention_detection_id=d.id
        LEFT JOIN entity_candidate_sets cs
          ON cs.tenant_id=req.tenant_id AND cs.request_id=req.id
        LEFT JOIN resolution_assessments ra
          ON ra.tenant_id=cs.tenant_id AND ra.candidate_set_id=cs.id
        LEFT JOIN grounding_traces gt
          ON gt.tenant_id=ra.tenant_id
         AND gt.entity_mention_detection_id=d.id
         AND gt.candidate_request_id=req.id
         AND gt.candidate_set_id=cs.id
         AND gt.resolution_assessment_id=ra.id
        LEFT JOIN grounding_admission_decisions gad
          ON gad.tenant_id=gt.tenant_id AND gad.id=gt.grounding_admission_id
        LEFT JOIN source_semantic_interpretations ssi
          ON ssi.tenant_id=gt.tenant_id AND ssi.grounding_trace_id=gt.id
        LEFT JOIN grounding_admission_decisions semantic_gad
          ON semantic_gad.tenant_id=ssi.tenant_id
         AND semantic_gad.id=ssi.grounding_admission_id
        LEFT JOIN source_semantic_admission_decisions ssad
          ON ssad.tenant_id=ssi.tenant_id AND ssad.interpretation_id=ssi.id
        LEFT JOIN models downstream_model
          ON downstream_model.tenant_id=ssad.tenant_id
         AND downstream_model.id=ssad.admitted_model_id
        LEFT JOIN LATERAL (
          SELECT jsonb_agg(jsonb_build_object(
                   'id', edge.id,
                   'source_model_id', edge.source_model_id,
                   'target_model_id', edge.target_model_id,
                   'edge_kind', edge.edge_kind,
                   'status', edge.status,
                   'metadata', edge.metadata,
                   'created_by_event_id', edge.created_by_event_id
                 ) ORDER BY edge.created_at, edge.id) AS downstream_relations
          FROM model_edges edge
          WHERE edge.tenant_id=downstream_model.tenant_id
            AND (
              edge.source_model_id=downstream_model.id
              OR edge.target_model_id=downstream_model.id
            )
        ) topology ON true
        WHERE d.tenant_id=$1 AND d.source_observation_id = ANY($2::uuid[])
        ORDER BY d.source_observation_id, d.candidate_surface,
                 ra.assessment_version DESC NULLS LAST, gt.created_at DESC NULLS LAST
        """,
        tenant_id,
        observation_ids,
    )
    # SQL order places current assessment/trace first; one row per sealed gold key.
    deduped: dict[tuple[UUID, str], Mapping[str, Any]] = {}
    for row in rows:
        deduped.setdefault((row["source_observation_id"], row["candidate_surface"]), row)
    return analyze_entity_pipeline_rows(
        gold_cases=gold_cases,
        canonical_gold_labels=canonical_gold_labels,
        topology_model_gold_labels=topology_model_gold_labels,
        rows=tuple(deduped.values()),
        ks=ks,
    )


__all__ = [
    "EntityPipelineMetrics", "GoldEntityPipelineCase", "GoldEntityPipelineReport",
    "GoldRelationExpectation",
    "analyze_entity_pipeline_rows", "canonical_ref_key", "evaluate_persisted_entity_pipeline",
]
