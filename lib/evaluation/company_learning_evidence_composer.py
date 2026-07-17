"""Compose independent SHA-bound company-learning evidence without trust inflation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_model_ablation import evaluate_company_model_ablation
from lib.evaluation.feedback_learning_effect import validate_feedback_learning_effect_artifact


COMPONENTS = (
    "retrieval_evolution", "company_model_ablation", "feedback_learning",
    "source_equivalence", "correction_homeostasis", "joined_runtime",
)


@dataclass(frozen=True)
class BoundArtifact:
    payload: Mapping[str, Any]
    artifact_sha256: str


def compose_objective_company_learning_evidence(
    *, retrieval_evolution: BoundArtifact | None = None,
    retrieval_evolution_postfix: BoundArtifact | None = None,
    company_model_ablation: BoundArtifact | None = None,
    company_model_ablation_legacy: BoundArtifact | None = None,
    company_model_ablation_active_failure: BoundArtifact | None = None,
    company_model_ablation_active_predecessor: BoundArtifact | None = None,
    feedback_learning: BoundArtifact | None = None,
    source_equivalence: BoundArtifact | None = None,
    correction_homeostasis: BoundArtifact | None = None,
    joined_runtime: BoundArtifact | None = None,
) -> dict[str, Any]:
    inputs = {
        "feedback_learning": feedback_learning,
        "source_equivalence": source_equivalence,
        "correction_homeostasis": correction_homeostasis,
        "joined_runtime": joined_runtime,
    }
    components: dict[str, Any] = {}
    bindings: dict[str, Any] = {}
    blockers: list[str] = []
    gaps: list[str] = []
    retrieval_component, retrieval_bindings, retrieval_gaps = _compose_retrieval_evidence(
        retrieval_evolution, retrieval_evolution_postfix
    )
    components["retrieval_evolution"] = retrieval_component
    bindings["retrieval_evolution"] = retrieval_bindings
    gaps.extend(retrieval_gaps)
    if retrieval_component["status"] == "observed":
        blockers.extend(_blockers("retrieval_evolution", retrieval_component["report"]))
    ablation_component, ablation_bindings, ablation_gaps = _compose_ablation_evidence(
        legacy=company_model_ablation_legacy,
        active_failure=company_model_ablation_active_failure,
        active_predecessor=company_model_ablation_active_predecessor,
        active=company_model_ablation,
    )
    components["company_model_ablation"] = ablation_component
    bindings["company_model_ablation"] = ablation_bindings
    gaps.extend(ablation_gaps)
    if ablation_component["status"] == "observed":
        blockers.extend(_blockers("company_model_ablation", ablation_component["report"]))
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
                 "meets_policy", "meets_preregistered_policy", "observed",
                 "current_meets_bounded_policy_historical_below_policy",
             }]
    historical_below = [
        "retrieval_evolution"
        for row in (components.get("retrieval_evolution"),)
        if row and row.get("historical_verdict") == "below_policy"
    ]
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
        "historical_below_policy_components": historical_below,
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


def _compose_ablation_evidence(
    *, legacy: BoundArtifact | None, active_failure: BoundArtifact | None,
    active_predecessor: BoundArtifact | None, active: BoundArtifact | None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Preserve ablation history while letting only the current active lane decide."""
    bindings: dict[str, Any] = {}
    gaps: list[str] = []
    legacy_component = None
    failure_report = None
    predecessor_component = None

    # A pre-active artifact passed through the old argument is history, not a
    # current result. This compatibility rule prevents old callers from
    # accidentally restoring the v4 development pass as the governing verdict.
    if (
        active is not None
        and active.payload.get("schema_version") == "bounded-company-model-ablation-artifact-v1"
    ):
        if legacy is not None:
            raise ValueError("legacy company-model ablation supplied twice")
        legacy, active = active, None

    if legacy is not None:
        _require_sha(legacy.artifact_sha256, "legacy company-model ablation")
        legacy_component = _normalize_component("company_model_ablation", legacy.payload)
        bindings["legacy_v4_development"] = {
            "status": "observed", "artifact_sha256": legacy.artifact_sha256,
            "schema_version": legacy_component["schema_version"],
            "verdict": legacy_component["verdict"],
        }
        gaps.append(
            "company_model_ablation:legacy_v4_development_is_not_active_lane_evidence"
        )
    else:
        bindings["legacy_v4_development"] = {
            "status": "unavailable", "artifact_sha256": None,
        }

    if active_failure is not None:
        _require_sha(active_failure.artifact_sha256, "active ablation contract failure")
        _schema(
            active_failure.payload,
            "bounded-company-model-holdout-v5-failure-artifact-v1",
        )
        _verify_objective(active_failure.payload, "active ablation contract failure")
        failure_report = dict(active_failure.payload)
        bindings["active_v5_contract_failure"] = {
            "status": "observed",
            "artifact_sha256": active_failure.artifact_sha256,
            "schema_version": active_failure.payload["schema_version"],
            "internal_digest": active_failure.payload.get("objective_sha256"),
            "verdict": active_failure.payload.get("verdict"),
        }
        gaps.append(
            "company_model_ablation:active_v5_inconclusive_runtime_contract_failure"
        )
    else:
        bindings["active_v5_contract_failure"] = {
            "status": "unavailable", "artifact_sha256": None,
        }

    if active_predecessor is not None:
        _require_sha(
            active_predecessor.artifact_sha256,
            "superseded active company-model ablation",
        )
        predecessor_schema = str(
            active_predecessor.payload.get("schema_version") or ""
        )
        if not (
            predecessor_schema.startswith("bounded-company-model-holdout-v")
            and predecessor_schema.endswith("-artifact-v1")
        ):
            raise ValueError("active ablation predecessor must be a versioned holdout")
        predecessor_component = _normalize_ablation_artifact(
            active_predecessor.payload
        )
        bindings["superseded_active_holdout"] = {
            "status": "observed",
            "artifact_sha256": active_predecessor.artifact_sha256,
            "schema_version": predecessor_schema,
            "verdict": predecessor_component["verdict"],
        }
        gaps.append(
            "company_model_ablation:superseded_active_holdout_is_historical_not_governing"
        )
    else:
        bindings["superseded_active_holdout"] = {
            "status": "unavailable", "artifact_sha256": None,
        }

    if active is None:
        bindings["current_active_holdout"] = {
            "status": "unavailable", "artifact_sha256": None,
        }
        gaps.append("component_unavailable:company_model_ablation.current_active_holdout")
        return ({
            "status": "unknown", "continuous_score": None, "population": None,
            "verdict": "unknown", "active_lane_verdict": "unknown",
            "legacy_v4_development": legacy_component,
            "active_v5_contract_failure": failure_report,
            "superseded_active_holdout": predecessor_component,
        }, bindings, gaps)

    _require_sha(active.artifact_sha256, "current active company-model ablation")
    schema = str(active.payload.get("schema_version") or "")
    if not (
        schema.startswith("bounded-company-model-holdout-v")
        and schema.endswith("-artifact-v1")
    ):
        raise ValueError("current active ablation must be a versioned holdout artifact")
    active_component = _normalize_ablation_artifact(active.payload)
    if schema == "bounded-company-model-holdout-v7-artifact-v1":
        active_component.update({
            "capability_claim": "cross_batch_evidence_accumulation_and_availability",
            "synthesis_claim": "not_established",
            "does_not_prove": (
                "one persisted Model contains the complete cross-batch hidden pattern "
                "with prior-Model lineage"
            ),
        })
        gaps.append(
            "company_model_ablation:v7_collective_facet_union_is_not_single_model_synthesis"
        )
    gaps.extend(_proof_gaps("company_model_ablation", active_component["report"]))
    bindings["current_active_holdout"] = {
        "status": "observed", "artifact_sha256": active.artifact_sha256,
        "schema_version": schema, "verdict": active_component["verdict"],
    }
    return ({
        **active_component,
        "active_lane_verdict": active_component["verdict"],
        "legacy_v4_development": legacy_component,
        "active_v5_contract_failure": failure_report,
        "superseded_active_holdout": predecessor_component,
        "governing_era": schema.removeprefix("bounded-company-model-holdout-").removesuffix(
            "-artifact-v1"
        ),
    }, bindings, gaps)


def _compose_retrieval_evidence(
    historical: BoundArtifact | None,
    postfix: BoundArtifact | None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    gaps: list[str] = []
    bindings: dict[str, Any] = {}
    historical_report = None
    postfix_report = None
    if historical is not None:
        _require_sha(historical.artifact_sha256, "historical retrieval")
        _schema(historical.payload, "retrieval-evolution-evaluation-v1")
        historical_report = dict(historical.payload)
        bindings["immutable_historical"] = {
            "status": "observed", "artifact_sha256": historical.artifact_sha256,
            "schema_version": historical.payload["schema_version"],
        }
    else:
        bindings["immutable_historical"] = {"status": "unavailable", "artifact_sha256": None}
        gaps.append("component_unavailable:retrieval_evolution.immutable_historical")
    if postfix is not None:
        _require_sha(postfix.artifact_sha256, "postfix retrieval")
        _schema(postfix.payload, "bounded-retrieval-evolution-postfix-objective-v1")
        _verify_objective(postfix.payload, "postfix retrieval")
        postfix_report = dict(_object(postfix.payload.get("evaluation"), "postfix retrieval evaluation"))
        bindings["current_bounded_postfix"] = {
            "status": "observed", "artifact_sha256": postfix.artifact_sha256,
            "schema_version": postfix.payload["schema_version"],
            "internal_digest": postfix.payload.get("objective_sha256"),
        }
        gaps.append(f"retrieval_evolution:{postfix.payload.get('proof_boundary')}")
    else:
        bindings["current_bounded_postfix"] = {"status": "unavailable", "artifact_sha256": None}
        gaps.append("component_unavailable:retrieval_evolution.current_bounded_postfix")
    active = postfix_report or historical_report
    if active is None:
        return ({"status": "unknown", "continuous_score": None, "population": None,
                 "verdict": "unknown"}, bindings, gaps)
    score = float(active.get("continuous_score") or 0.0)
    historical_verdict = str(historical_report.get("verdict")) if historical_report else "unknown"
    current_verdict = str(postfix_report.get("verdict")) if postfix_report else "unknown"
    verdict = (
        "current_meets_bounded_policy_historical_below_policy"
        if postfix_report and current_verdict == "meets_preregistered_policy"
        and historical_verdict == "below_policy"
        else current_verdict if postfix_report else historical_verdict
    )
    return ({
        "status": "observed", "schema_version": "retrieval-evolution-two-era-v1",
        "continuous_score": score,
        "population": {
            "immutable_historical_batches": historical_report.get("batch_count")
            if historical_report else None,
            "current_bounded_batches": postfix_report.get("batch_count")
            if postfix_report else None,
        },
        "verdict": verdict, "historical_verdict": historical_verdict,
        "current_bounded_verdict": current_verdict,
        "report": postfix_report or historical_report,
        "immutable_historical": historical_report,
        "current_bounded_postfix": postfix_report,
    }, bindings, gaps)


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
        return _normalize_ablation_artifact(payload)
    elif name == "feedback_learning":
        validated = validate_feedback_learning_effect_artifact(dict(payload))
        report = validated.report.model_dump(mode="json")
        internal_digest = validated.digest
        population = {"matched_pairs": validated.report.matched_pair_count,
                      "useful_pairs": validated.report.useful_pair_count,
                      "safety_pairs": validated.report.safety_pair_count}
    elif name == "source_equivalence":
        is_db_objective = payload.get("schema_version") == "source-equivalence-db-objective-v1"
        if payload.get("schema_version") in {
            "bounded-source-equivalence-objective-v1",
            "source-equivalence-db-objective-v1",
        }:
            _verify_objective(payload, "source equivalence")
            internal_digest = str(payload.get("objective_sha256"))
            report = _object(payload.get("evaluation"), "source equivalence evaluation")
        else:
            _schema(payload, "normalized-source-equivalence-evaluation-v1")
            report = payload
        report = dict(report)
        checks = dict(_object(report.get("checks"), "source equivalence checks"))
        relation_path = payload.get("relation_path")
        population_payload = _object(payload.get("population"), "source population")
        production_relation_path_exercised = bool(
            is_db_objective
            and isinstance(relation_path, Mapping)
            and int(relation_path.get("accepted_edges") or 0)
            == int(population_payload.get("sources") or 0)
            and checks.get("relation_outcomes_exposed") is True
        )
        checks["production_relation_path_exercised"] = production_relation_path_exercised
        report["checks"] = checks
        if not production_relation_path_exercised:
            report["verdict"] = "below_policy"
        if payload.get("proof_boundary") is not None:
            report["proof_boundary"] = payload["proof_boundary"]
        population = dict(population_payload)
    elif name == "correction_homeostasis":
        _schema(payload, "correction-homeostasis-db-objective-v1")
        _verify_objective(payload, "correction-homeostasis")
        expected = payload.get("objective_sha256")
        internal_digest = str(expected)
        report = _object(payload.get("evaluation"), "homeostasis evaluation")
        population = dict(_object(report.get("population"), "homeostasis population"))
    elif name == "joined_runtime":
        _schema(payload, "integrated-company-learning-vertical-v2")
        _verify_objective(payload, "joined company-learning runtime")
        internal_digest = str(payload.get("objective_sha256"))
        checks = dict(_object(payload.get("checks"), "joined runtime checks"))
        if len(checks) != 17:
            raise ValueError("joined runtime must expose exactly 17 objective checks")
        population = dict(_object(payload.get("populations"), "joined runtime populations"))
        report = {key: payload.get(key) for key in (
            "schema_version", "continuous_score", "verdict", "checks",
            "active_populations", "material_use_ablation", "correction",
            "negative_controls", "proof_boundary", "batch_count", "signal_count",
        )}
    else:  # pragma: no cover
        raise ValueError(f"unknown component {name}")
    score = report.get("continuous_score")
    if not isinstance(score, (int, float)) or not 0 <= float(score) <= 1:
        raise ValueError(f"{name} continuous score must be in [0,1]")
    return {"status": "observed", "schema_version": str(report["schema_version"]),
            "continuous_score": float(score), "population": population,
            "verdict": str(report.get("verdict") or report.get("status") or "unknown"),
            "internal_digest": internal_digest, "report": dict(report)}


def _normalize_ablation_artifact(payload: Mapping[str, Any]) -> dict[str, Any]:
    report = _object(payload.get("evaluation"), "ablation evaluation")
    recomputed = evaluate_company_model_ablation(
        manifest=_object(payload.get("manifest"), "ablation manifest"),
        learned=_object(payload.get("learned_arm"), "learned arm"),
        frozen=_object(payload.get("frozen_arm"), "frozen arm"),
    )
    if dict(report) != recomputed:
        raise ValueError("company-model ablation evaluation does not recompute")
    score = report.get("continuous_score")
    if not isinstance(score, (int, float)) or not 0 <= float(score) <= 1:
        raise ValueError("company_model_ablation continuous score must be in [0,1]")
    arms = _object(report.get("arms"), "arms")
    return {
        "status": "observed",
        "schema_version": str(payload["schema_version"]),
        "continuous_score": float(score),
        "population": {
            "batches": report.get("batch_count"),
            "signals": report.get("signal_count"),
            "hidden_theses": report.get("hidden_thesis_count"),
            "calibration_samples_per_arm": {
                key: value.get("calibration_n") for key, value in arms.items()
            },
        },
        "verdict": str(report.get("verdict") or "unknown"),
        "internal_digest": None,
        "report": dict(report),
    }


def _blockers(name: str, report: Mapping[str, Any]) -> list[str]:
    checks = report.get("checks") if isinstance(report.get("checks"), Mapping) else {}
    names = {
        "retrieval_evolution": ("late_raw_reopening_is_justified",),
        "company_model_ablation": ("no_learned_safety_incident",),
        "source_equivalence": (
            "source_authority_preserved", "source_coordinates_preserved",
            "conversational_boundaries_preserved", "learning_outcomes_are_lineaged",
            "relation_outcomes_exposed", "production_relation_path_exercised",
        ),
        "correction_homeostasis": ("unsafe_reads_contained", "replay_is_idempotent",
                                   "restart_preserves_state", "deep_cascade_is_complete_and_cycle_safe"),
        "joined_runtime": tuple(report.get("checks", {}).keys()),
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


def _verify_objective(payload: Mapping[str, Any], label: str) -> None:
    expected = payload.get("objective_sha256")
    body = dict(payload)
    body.pop("objective_sha256", None)
    if expected != canonical_sha256(body):
        raise ValueError(f"{label} objective digest mismatch")


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _require_sha(value: str, name: str) -> None:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{name} artifact SHA must be lowercase SHA-256")


__all__ = ["BoundArtifact", "compose_objective_company_learning_evidence"]
