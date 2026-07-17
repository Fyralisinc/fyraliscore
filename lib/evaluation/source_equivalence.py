"""Continuous normalized-source equivalence evaluation.

The evaluator starts after source connectors: each row describes the durable
entity, Model and relation outcomes produced from one persisted signal batch.
Semantic equivalence is scored independently from source authority and source
boundary fidelity so normalization cannot earn quality by erasing provenance.
"""

from __future__ import annotations

from itertools import combinations
from typing import Any, Mapping, Sequence


JsonObject = Mapping[str, Any]
_REQUIRED_SOURCES = frozenset({"slack", "email", "jira", "document_meeting"})


def evaluate_normalized_source_equivalence(
    rows: Sequence[JsonObject], *, require_relation_exposure: bool = False
) -> dict[str, Any]:
    normalized = [_normalize(row) for row in rows]
    by_case: dict[str, list[dict[str, Any]]] = {}
    for row in normalized:
        by_case.setdefault(row["semantic_case_id"], []).append(row)

    case_reports = []
    for case_id, cases in sorted(by_case.items()):
        sources = {row["source_kind"] for row in cases}
        case_reports.append({
            "semantic_case_id": case_id,
            "source_coverage": len(sources & _REQUIRED_SOURCES) / len(_REQUIRED_SOURCES),
            "entity_outcome_similarity": _pairwise_similarity(cases, "entity_refs"),
            "model_outcome_similarity": _pairwise_similarity(cases, "model_signatures"),
            "relation_outcome_similarity": _pairwise_similarity(cases, "relation_signatures"),
            "relation_outcome_exposure": _mean(
                bool(row["relation_signatures"]) for row in cases
            ),
            "source_authority_fidelity": _mean(
                bool(row["expected_authority_ref"])
                and row["authority_ref"] == row["expected_authority_ref"]
                for row in cases
            ),
            "source_coordinate_fidelity": _mean(
                bool(row["expected_source_system"])
                and row["assertion_source_system"] == row["expected_source_system"]
                for row in cases
            ),
            "conversational_boundary_fidelity": _mean(
                bool(row["expected_boundary_refs"])
                and set(row["boundary_refs"]) == set(row["expected_boundary_refs"])
                for row in cases
            ),
            "batch_integrity": _mean(row["batch_signal_count"] >= 2 for row in cases),
            "learning_outcome_lineage": _mean(
                row["entity_lineage_complete"]
                and row["model_lineage_complete"]
                and row["relation_lineage_complete"]
                for row in cases
            ),
        })

    measurements = {
        key: _mean(report[key] for report in case_reports)
        for key in (
            "source_coverage", "entity_outcome_similarity",
            "model_outcome_similarity", "relation_outcome_similarity",
            "relation_outcome_exposure",
            "source_authority_fidelity", "source_coordinate_fidelity",
            "conversational_boundary_fidelity", "batch_integrity",
            "learning_outcome_lineage",
        )
    }
    semantic_equivalence = _mean(
        measurements[key] for key in (
            "entity_outcome_similarity", "model_outcome_similarity",
            "relation_outcome_similarity",
        )
    )
    provenance_fidelity = _mean(
        measurements[key] for key in (
            "source_authority_fidelity", "source_coordinate_fidelity",
            "conversational_boundary_fidelity",
        )
    )
    observed_quality_score = (
        0.50 * semantic_equivalence
        + 0.25 * provenance_fidelity
        + 0.10 * measurements["source_coverage"]
        + 0.05 * measurements["batch_integrity"]
        + 0.10 * measurements["learning_outcome_lineage"]
    )
    semantic_outcome_coverage = _mean((
        1.0,
        1.0,
        measurements["relation_outcome_exposure"] if require_relation_exposure else 1.0,
    ))
    continuous_score = observed_quality_score * semantic_outcome_coverage
    checks = {
        "all_source_families_covered": measurements["source_coverage"] == 1.0,
        "semantic_outcomes_consistent": semantic_equivalence >= 0.90,
        "relation_outcomes_exposed": (
            measurements["relation_outcome_exposure"] == 1.0
            if require_relation_exposure else True
        ),
        "source_authority_preserved": measurements["source_authority_fidelity"] == 1.0,
        "source_coordinates_preserved": measurements["source_coordinate_fidelity"] == 1.0,
        "conversational_boundaries_preserved": (
            measurements["conversational_boundary_fidelity"] == 1.0
        ),
        "signals_processed_as_batches": measurements["batch_integrity"] == 1.0,
        "learning_outcomes_are_lineaged": measurements["learning_outcome_lineage"] == 1.0,
    }
    return {
        "schema_version": "normalized-source-equivalence-evaluation-v1",
        "required_sources": sorted(_REQUIRED_SOURCES),
        "population": {"cases": len(case_reports), "source_batches": len(normalized)},
        "measurements": {**measurements, "semantic_equivalence": semantic_equivalence,
                         "provenance_fidelity": provenance_fidelity,
                         "semantic_outcome_coverage": semantic_outcome_coverage},
        "observed_quality_score": observed_quality_score,
        "continuous_score": continuous_score,
        "checks": checks,
        "verdict": "meets_policy" if all(checks.values()) else "below_policy",
        "proof_gaps": (
            [] if measurements["relation_outcome_exposure"] == 1.0 else [
                "relation_outcome_population_unexposed"
            ]
        ),
        "cases": case_reports,
    }


def _normalize(row: JsonObject) -> dict[str, Any]:
    def strings(key: str) -> tuple[str, ...]:
        value = row.get(key)
        return tuple(sorted({str(item) for item in value})) if isinstance(value, list) else ()

    return {
        "semantic_case_id": str(row.get("semantic_case_id") or ""),
        "source_kind": str(row.get("source_kind") or ""),
        "entity_refs": strings("entity_refs"),
        "model_signatures": strings("model_signatures"),
        "relation_signatures": strings("relation_signatures"),
        "authority_ref": str(row.get("authority_ref") or ""),
        "expected_authority_ref": str(row.get("expected_authority_ref") or ""),
        "assertion_source_system": str(row.get("assertion_source_system") or ""),
        "expected_source_system": str(row.get("expected_source_system") or ""),
        "boundary_refs": strings("boundary_refs"),
        "expected_boundary_refs": strings("expected_boundary_refs"),
        "batch_signal_count": max(0, int(row.get("batch_signal_count") or 0)),
        "entity_lineage_complete": row.get("entity_lineage_complete") is True,
        "model_lineage_complete": row.get("model_lineage_complete") is True,
        "relation_lineage_complete": row.get("relation_lineage_complete") is True,
    }


def _pairwise_similarity(rows: Sequence[dict[str, Any]], key: str) -> float:
    pairs = list(combinations(rows, 2))
    if not pairs:
        return 0.0
    scores = []
    for left, right in pairs:
        a, b = set(left[key]), set(right[key])
        scores.append(1.0 if not a and not b else len(a & b) / len(a | b))
    return _mean(scores)


def _mean(values) -> float:
    items = [float(value) for value in values]
    return 0.0 if not items else sum(items) / len(items)


__all__ = ["evaluate_normalized_source_equivalence"]
