"""Compose independent SHA-bound company-learning evidence without trust inflation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_model_ablation import evaluate_company_model_ablation
from lib.evaluation.feedback_learning_effect import validate_feedback_learning_effect_artifact


COMPONENTS = (
    "retrieval_evolution", "company_model_ablation", "feedback_learning",
    "source_equivalence", "correction_homeostasis",
)


@dataclass(frozen=True)
class BoundArtifact:
    payload: Mapping[str, Any]
    artifact_sha256: str


def compose_objective_company_learning_evidence(
    *, retrieval_evolution: BoundArtifact | None = None,
    company_model_ablation: BoundArtifact | None = None,
    feedback_learning: BoundArtifact | None = None,
    source_equivalence: BoundArtifact | None = None,
    correction_homeostasis: BoundArtifact | None = None,
) -> dict[str, Any]:
    inputs = {
        "retrieval_evolution": retrieval_evolution,
        "company_model_ablation": company_model_ablation,
        "feedback_learning": feedback_learning,
        "source_equivalence": source_equivalence,
        "correction_homeostasis": correction_homeostasis,
    }
    components: dict[str, Any] = {}
    bindings: dict[str, Any] = {}
    blockers: list[str] = []
    gaps: list[str] = []
    for name, bound in inputs.items():
        if bound is None:
            components[name] = {"status": "unknown", "continuous_score": None,
                                "population": None, "verdict": "unknown"}
            bindings[name] = {"status": "unavailable", "artifact_sha256": None}
            gaps.append(f"component_unavailable:{name}")
            continue
        _require_sha(bound.artifact_sha256, name)
        normalized = _normalize_component(name, bound.payload)
        components[name] = normalized
        bindings[name] = {
            "status": "observed", "artifact_sha256": bound.artifact_sha256,
            "schema_version": normalized["schema_version"],
            "internal_digest": normalized.get("internal_digest"),
        }
        blockers.extend(_blockers(name, normalized["report"]))
        gaps.extend(_proof_gaps(name, normalized["report"]))

    observed = [row for row in components.values() if row["status"] == "observed"]
    coverage = len(observed) / len(COMPONENTS)
    observed_score = (
        sum(float(row["continuous_score"]) for row in observed) / len(observed)
        if observed else None
    )
    coverage_adjusted = None if observed_score is None else observed_score * coverage
    below = [name for name, row in components.items()
             if row["status"] == "observed" and row["verdict"] not in {
                 "meets_policy", "meets_preregistered_policy", "observed"
             }]
    if blockers:
        verdict = "not_credible"
    elif coverage < 1.0:
        verdict = "partial_evidence"
    elif below:
        verdict = "below_bounded_policy"
    else:
        verdict = "meets_bounded_policy"
    output = {
        "schema_version": "objective-company-learning-evidence-v1",
        "artifact_bindings": bindings,
        "components": components,
        "exact_populations": {
            name: row["population"] for name, row in components.items()
        },
        "evidence_coverage": coverage,
        "observed_component_score": observed_score,
        "coverage_adjusted_score": coverage_adjusted,
        "below_policy_components": below,
        "noncompensable_blockers": sorted(set(blockers)),
        "proof_gaps": sorted(set(gaps)),
        "verdict": verdict,
        "guarantee_boundary": (
            "The summary composes bounded simulated and database-backed company-learning "
            "evidence. It does not establish open-world customer value, connector "
            "reliability, unbounded scale, or autonomous task execution."
        ),
    }
    output["composition_sha256"] = canonical_sha256(output)
    return output


def _normalize_component(name: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    report: Mapping[str, Any]
    internal_digest = None
    if name == "retrieval_evolution":
        _schema(payload, "retrieval-evolution-evaluation-v1")
        report = payload
        population = {"batches": int(payload.get("batch_count") or 0),
                      "phase_batches": payload.get("phase_batch_counts")}
    elif name == "company_model_ablation":
        _schema(payload, "bounded-company-model-ablation-artifact-v1")
        report = _object(payload.get("evaluation"), "ablation evaluation")
        recomputed = evaluate_company_model_ablation(
            manifest=_object(payload.get("manifest"), "ablation manifest"),
            learned=_object(payload.get("learned_arm"), "learned arm"),
            frozen=_object(payload.get("frozen_arm"), "frozen arm"),
        )
        if dict(report) != recomputed:
            raise ValueError("company-model ablation evaluation does not recompute")
        population = {"batches": report.get("batch_count"),
                      "signals": report.get("signal_count"),
                      "hidden_theses": report.get("hidden_thesis_count"),
                      "calibration_samples_per_arm": {
                          key: value.get("calibration_n")
                          for key, value in _object(report.get("arms"), "arms").items()
                      }}
    elif name == "feedback_learning":
        validated = validate_feedback_learning_effect_artifact(dict(payload))
        report = validated.report.model_dump(mode="json")
        internal_digest = validated.digest
        population = {"matched_pairs": validated.report.matched_pair_count,
                      "useful_pairs": validated.report.useful_pair_count,
                      "safety_pairs": validated.report.safety_pair_count}
    elif name == "source_equivalence":
        _schema(payload, "normalized-source-equivalence-evaluation-v1")
        report = payload
        population = dict(_object(payload.get("population"), "source population"))
    elif name == "correction_homeostasis":
        _schema(payload, "correction-homeostasis-db-objective-v1")
        expected = payload.get("objective_sha256")
        body = dict(payload)
        body.pop("objective_sha256", None)
        if expected != canonical_sha256(body):
            raise ValueError("correction-homeostasis objective digest mismatch")
        internal_digest = str(expected)
        report = _object(payload.get("evaluation"), "homeostasis evaluation")
        population = dict(_object(report.get("population"), "homeostasis population"))
    else:  # pragma: no cover
        raise ValueError(f"unknown component {name}")
    score = report.get("continuous_score")
    if not isinstance(score, (int, float)) or not 0 <= float(score) <= 1:
        raise ValueError(f"{name} continuous score must be in [0,1]")
    return {"status": "observed", "schema_version": str(report["schema_version"]),
            "continuous_score": float(score), "population": population,
            "verdict": str(report.get("verdict") or report.get("status") or "unknown"),
            "internal_digest": internal_digest, "report": dict(report)}


def _blockers(name: str, report: Mapping[str, Any]) -> list[str]:
    checks = report.get("checks") if isinstance(report.get("checks"), Mapping) else {}
    names = {
        "retrieval_evolution": ("late_raw_reopening_is_justified",),
        "company_model_ablation": ("no_learned_safety_incident",),
        "source_equivalence": ("source_authority_preserved", "source_coordinates_preserved",
                               "conversational_boundaries_preserved", "learning_outcomes_are_lineaged"),
        "correction_homeostasis": ("unsafe_reads_contained", "replay_is_idempotent",
                                   "restart_preserves_state", "deep_cascade_is_complete_and_cycle_safe"),
    }.get(name, ())
    blockers = [f"{name}:{key}" for key in names if checks.get(key) is False]
    if name == "feedback_learning":
        if report.get("truth_immutability_rate") != 1.0:
            blockers.append("feedback_learning:canonical_truth_mutated")
        if report.get("status") == "contradicted":
            blockers.append("feedback_learning:matched_effect_contradicted")
    return blockers


def _proof_gaps(name: str, report: Mapping[str, Any]) -> list[str]:
    raw = report.get("proof_boundary")
    if isinstance(raw, str):
        return [f"{name}:{raw}"]
    if isinstance(raw, list):
        return [f"{name}:{item}" for item in raw]
    if name in {"retrieval_evolution", "source_equivalence", "feedback_learning"}:
        return [f"{name}:bounded_component_does_not_establish_open_world_generalization"]
    return []


def _schema(payload: Mapping[str, Any], expected: str) -> None:
    if payload.get("schema_version") != expected:
        raise ValueError(f"expected {expected}")


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _require_sha(value: str, name: str) -> None:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{name} artifact SHA must be lowercase SHA-256")


__all__ = ["BoundArtifact", "compose_objective_company_learning_evidence"]
