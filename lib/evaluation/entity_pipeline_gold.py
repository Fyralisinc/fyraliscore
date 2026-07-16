"""Gold evaluation of the persisted entity grounding pipeline.

This evaluator starts at normalized, persisted observations.  It reads the
durable mention -> candidate -> assessment -> trace chain, but correctness is
defined exclusively by evaluator-owned gold and canonical-label mappings.
Operational fate strings are never treated as evidence that a decision was
correct.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class GoldEntityPipelineCase(_Record):
    case_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source_observation_id: UUID
    surface: str = Field(min_length=1)
    gold_entity_type: str = Field(min_length=1)
    gold_canonical_label: str | None = None


class EntityPipelineMetrics(_Record):
    gold_case_count: int = Field(ge=0)
    detected_case_count: int = Field(ge=0)
    candidate_population_count: int = Field(ge=0)
    assessed_case_count: int = Field(ge=0)
    terminal_case_count: int = Field(ge=0)
    candidate_recall_at_k: dict[int, float | None]
    gold_type_present_at_k: dict[int, float | None]
    selected_type_accuracy: float | None = Field(default=None, ge=0, le=1)
    canonical_link_accuracy: float | None = Field(default=None, ge=0, le=1)
    canonical_link_coverage: float | None = Field(default=None, ge=0, le=1)
    abstention_rate: float | None = Field(default=None, ge=0, le=1)
    review_rate: float | None = Field(default=None, ge=0, le=1)
    detection_to_terminal_coverage: float | None = Field(default=None, ge=0, le=1)
    lineage_integrity: float | None = Field(default=None, ge=0, le=1)
    rejected_detection_candidate_count: int = Field(ge=0)
    unknown_canonical_ref_count: int = Field(ge=0)


class GoldEntityPipelineReport(_Record):
    schema_version: str = "gold-entity-pipeline-v1"
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


def analyze_entity_pipeline_rows(
    *,
    gold_cases: Sequence[GoldEntityPipelineCase],
    canonical_gold_labels: Mapping[str, str],
    rows: Sequence[Mapping[str, Any]],
    ks: Sequence[int] = (1, 3, 5),
) -> GoldEntityPipelineReport:
    """Score joined DB rows without trusting resolver-declared success fates."""

    if not ks or any(k <= 0 for k in ks):
        raise ValueError("ks must contain positive values")
    keys = [(case.source_observation_id, case.surface) for case in gold_cases]
    if len(keys) != len(set(keys)) or len({case.case_id for case in gold_cases}) != len(gold_cases):
        raise ValueError("gold cases must have unique IDs and observation/surface keys")
    rows_by_key = {
        (UUID(str(row["source_observation_id"])), str(row["candidate_surface"])): row
        for row in rows
    }

    def metrics(cases: Sequence[GoldEntityPipelineCase]) -> EntityPipelineMetrics:
        detected = candidate_population = assessed = terminal = 0
        selected_type_total = selected_type_correct = 0
        known_link_total = linked = link_correct = 0
        abstained = reviewed = lineage_ok = rejected_with_candidates = unknown_refs = 0
        recall_hits = {k: 0 for k in ks}
        type_hits = {k: 0 for k in ks}
        recall_denominator = type_denominator = 0

        for case in cases:
            row = rows_by_key.get((case.source_observation_id, case.surface))
            if row is None:
                continue
            fate = str(row.get("detection_fate") or "")
            is_detected = fate == "detected"
            detected += int(is_detected)
            raw_candidates = _json(row.get("candidates")) or []
            candidates = [item for item in raw_candidates if isinstance(item, dict)]
            if not is_detected:
                rejected_with_candidates += int(bool(candidates or row.get("candidate_request_id")))
                # A governed rejection is terminal at the detection stage.
                terminal += int(fate.startswith("rejected_") or fate == "unsupported_implicit")
                lineage_ok += int(not candidates and row.get("candidate_request_id") is None)
                continue

            has_population = row.get("candidate_set_id") is not None and bool(candidates)
            candidate_population += int(has_population)
            distribution = _json(row.get("candidate_distribution")) or {}
            has_assessment = row.get("assessment_id") is not None and isinstance(distribution, dict)
            assessed += int(has_assessment)
            selected_id = row.get("selected_candidate_id")
            by_id = {str(item.get("candidate_id")): item for item in candidates}
            ranked = sorted(
                candidates,
                key=lambda item: (-float(distribution.get(str(item.get("candidate_id")), 0.0)), str(item.get("candidate_id"))),
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
                selected_type_correct += int(selected.get("candidate_type") == case.gold_entity_type)
            admitted_ref = _json(row.get("selected_referent"))
            if admitted_ref:
                linked += int(case.gold_canonical_label is not None)
                resolved_label = canonical_gold_labels.get(canonical_ref_key(admitted_ref))
                unknown_refs += int(resolved_label is None)
                link_correct += int(
                    case.gold_canonical_label is not None
                    and resolved_label == case.gold_canonical_label
                )
            current_fate = str(row.get("current_fate") or "")
            abstained += int(current_fate in {"abstained", "unresolved"})
            reviewed += int(current_fate == "review")
            is_terminal = current_fate in {"resolved_for_consumer", "review", "unresolved", "abstained"}
            terminal += int(is_terminal)

            ids = (
                row.get("detection_id"), row.get("candidate_request_id"),
                row.get("candidate_set_id"), row.get("assessment_id"), row.get("trace_id"),
            )
            embedded = _json(row.get("trace")) or {}
            trace_ids = (
                ((embedded.get("mention_detection") or {}).get("id")),
                ((embedded.get("candidate_request") or {}).get("id")),
                ((embedded.get("candidate_set") or {}).get("id")),
                ((embedded.get("assessment") or {}).get("id")),
            )
            lineage_ok += int(
                is_terminal and all(ids) and all(str(ids[i]) == str(trace_ids[i]) for i in range(4))
            )

        return EntityPipelineMetrics(
            gold_case_count=len(cases), detected_case_count=detected,
            candidate_population_count=candidate_population, assessed_case_count=assessed,
            terminal_case_count=terminal,
            candidate_recall_at_k={k: _rate(recall_hits[k], recall_denominator) for k in ks},
            gold_type_present_at_k={k: _rate(type_hits[k], type_denominator) for k in ks},
            selected_type_accuracy=_rate(selected_type_correct, selected_type_total),
            canonical_link_accuracy=_rate(link_correct, linked),
            canonical_link_coverage=_rate(linked, known_link_total),
            abstention_rate=_rate(abstained, detected), review_rate=_rate(reviewed, detected),
            detection_to_terminal_coverage=_rate(terminal, len(cases)),
            lineage_integrity=_rate(lineage_ok, len(cases)),
            rejected_detection_candidate_count=rejected_with_candidates,
            unknown_canonical_ref_count=unknown_refs,
        )

    overall = metrics(gold_cases)
    batches = sorted({case.batch_id for case in gold_cases})
    uncertainties = []
    if not gold_cases:
        uncertainties.append("no_gold_entity_pipeline_cases")
    if any(case.gold_canonical_label is None for case in gold_cases):
        uncertainties.append("canonical_metrics_exclude_open_world_gold_cases")
    return GoldEntityPipelineReport(
        overall=overall,
        by_batch={batch: metrics([case for case in gold_cases if case.batch_id == batch]) for batch in batches},
        uncertainties=tuple(uncertainties),
    )


async def evaluate_persisted_entity_pipeline(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    gold_cases: Sequence[GoldEntityPipelineCase],
    canonical_gold_labels: Mapping[str, str],
    ks: Sequence[int] = (1, 3, 5),
) -> GoldEntityPipelineReport:
    """Load current persisted heads and evaluate the complete grounding chain."""

    observation_ids = tuple({case.source_observation_id for case in gold_cases})
    rows = await conn.fetch(
        """
        SELECT d.source_observation_id, d.candidate_surface,
               d.id AS detection_id, d.fate AS detection_fate,
               req.id AS candidate_request_id, cs.id AS candidate_set_id,
               cs.candidates, ra.id AS assessment_id,
               ra.candidate_distribution, ra.selected_candidate_id,
               gt.id AS trace_id, gt.current_fate, gt.selected_referent, gt.trace
        FROM entity_mention_detection_heads h
        JOIN entity_mention_detections d
          ON d.tenant_id=h.tenant_id AND d.id=h.current_detection_id
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
        rows=tuple(deduped.values()),
        ks=ks,
    )


__all__ = [
    "EntityPipelineMetrics", "GoldEntityPipelineCase", "GoldEntityPipelineReport",
    "analyze_entity_pipeline_rows", "canonical_ref_key", "evaluate_persisted_entity_pipeline",
]
