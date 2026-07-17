"""Aggregate, artifact-backed evaluation for a large company simulation.

This module does not run the simulation.  It joins the storyline benchmark,
Company Vitals, and company-learning assurance reports into one continuous
assessment that preserves proof gaps and non-compensatory safety failures.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


JsonObject = Mapping[str, Any]


@dataclass(frozen=True)
class SimulationProfile:
    name: str
    minimum_signals: int
    minimum_storylines: int
    required_t1_batches: int
    minimum_future_validation_events: int
    minimum_thesis_judgements: int
    require_vitals: bool
    require_assurance: bool


PROFILES = {
    "authoritative-45": SimulationProfile(
        "authoritative-45", 1_125, 8, 45, 10, 6, True, True
    ),
}

DIMENSION_WEIGHTS = {
    "hidden_pattern_recovery": 0.25,
    "temporal_improvement": 0.15,
    "entity_model_quality": 0.20,
    "learning_correction_lift": 0.20,
    "operational_drain": 0.10,
    "proof_completeness": 0.10,
}


def evaluate_large_company_simulation(
    *,
    benchmark: JsonObject,
    run_summary: JsonObject,
    vitals: JsonObject | None,
    assurance: JsonObject | None,
    run_config: JsonObject,
    profile_name: str,
    entity_evidence: JsonObject | None = None,
) -> dict[str, Any]:
    """Return a precise continuous report over saved simulation artifacts."""
    profile = PROFILES[profile_name]
    gaps: list[str] = []
    hard_failures: list[str] = []

    benchmark_failures = _strings(benchmark.get("required_run_failures"))
    hard_failures.extend(f"benchmark: {item}" for item in benchmark_failures)
    if benchmark.get("status") == "failed" and not benchmark_failures:
        hard_failures.append("benchmark reported failed without failure details")

    if vitals is None:
        gaps.append("Company Vitals artifact is missing.")
        if profile.require_vitals:
            hard_failures.append("required Company Vitals artifact is missing")
    else:
        hard_failures.extend(
            f"vitals: {item}" for item in _strings(vitals.get("hard_failures"))
        )

    if assurance is None:
        gaps.append("Company-learning Assurance v7 artifact is missing.")
        if profile.require_assurance:
            hard_failures.append(
                "required company-learning Assurance v7 artifact is missing"
            )
    else:
        version = assurance.get("schema_version")
        if version != "company-learning-assurance-summary-v7":
            hard_failures.append(
                "company-learning assurance is not a v7 artifact"
            )
        hard_failures.extend(
            f"assurance: {item}"
            for item in _strings(assurance.get("blocking_failures"))
        )
        if assurance.get("status") != "working":
            hard_failures.append(
                f"company-learning assurance status is {assurance.get('status')!r}"
            )

    scorecard = _object(benchmark.get("company_intelligence_scorecard"))
    dimensions = _object(scorecard.get("dimensions"))
    storyline_scores = _objects(benchmark.get("storyline_scores"))
    hidden = _hidden_pattern_dimension(benchmark, storyline_scores, gaps)
    temporal = _temporal_dimension(dimensions, gaps)
    entity_model = _entity_model_dimension(
        dimensions, vitals, entity_evidence, gaps
    )
    learning = _learning_dimension(vitals, assurance, gaps)
    operational = _operational_dimension(
        benchmark=benchmark,
        run_summary=run_summary,
        vitals=vitals,
        hard_failures=hard_failures,
    )
    proof = _proof_dimension(
        benchmark=benchmark,
        run_summary=run_summary,
        scorecard=scorecard,
        assurance=assurance,
        run_config=run_config,
        profile=profile,
        gaps=gaps,
        hard_failures=hard_failures,
    )
    retrieval_evolution = _retrieval_evolution(benchmark, gaps)

    dimension_rows = {
        "hidden_pattern_recovery": hidden,
        "temporal_improvement": temporal,
        "entity_model_quality": entity_model,
        "learning_correction_lift": learning,
        "operational_drain": operational,
        "proof_completeness": proof,
    }
    overall = sum(
        DIMENSION_WEIGHTS[name] * float(payload["score"])
        for name, payload in dimension_rows.items()
    )
    evidence_coverage = sum(
        DIMENSION_WEIGHTS[name] * float(payload["coverage"])
        for name, payload in dimension_rows.items()
    )
    all_gaps = _dedupe(
        gaps
        + _strings(scorecard.get("proof_gaps"))
        + _strings(
            _object(scorecard.get("product_value_evals")).get("proof_gaps")
        )
        + (_strings(vitals.get("proof_gaps")) if vitals else [])
    )
    status = _status(
        score=overall,
        coverage=evidence_coverage,
        hard_failures=hard_failures,
    )
    run_id = (
        benchmark.get("run_id")
        or run_summary.get("run_id")
        or (assurance or {}).get("run_id")
    )
    return {
        "schema_version": "large-company-simulation-evaluation-v1",
        "run_id": run_id,
        "profile": profile.name,
        "status": status,
        "overall_score": round(overall, 4),
        "evidence_coverage": round(evidence_coverage, 4),
        "interpretation": _interpretation(status, overall, evidence_coverage),
        "dimensions": dimension_rows,
        "scale": proof["metrics"]["scale"],
        "run_contract": proof["metrics"]["run_contract"],
        "retrieval_evolution": retrieval_evolution,
        "hard_failures": _dedupe(hard_failures),
        "proof_gaps": all_gaps,
        "claims_supported": _claims_supported(
            dimensions=dimension_rows,
            hard_failures=hard_failures,
        ),
        "claims_not_supported": _claims_not_supported(all_gaps),
    }


def _hidden_pattern_dimension(
    benchmark: JsonObject,
    storylines: list[JsonObject],
    gaps: list[str],
) -> dict[str, Any]:
    latent = _object(benchmark.get("latent_pattern_fitness"))
    thesis = _object(benchmark.get("thesis_recovery_judge"))
    avg_latent = _ratio01(latent.get("average_latent_pattern_score"))
    best_coverage = _ratio01(latent.get("average_best_pattern_coverage"))
    total = max(1, len(storylines))
    concrete = _ratio01(
        _number(latent.get("storylines_with_concrete_latent_model")) / total
    )
    judged = int(_number(thesis.get("n")))
    thesis_score = _ratio01(thesis.get("average_score"))
    thesis_accuracy = _ratio01(
        _number(thesis.get("correct_count")) / max(1, judged)
    )
    if judged == 0:
        gaps.append(
            "No independent thesis judgements were recorded; hidden-pattern "
            "quality relies on lexical/structural proxies."
        )
    if not storylines:
        gaps.append("No storyline-level scores were recorded.")
    coverage = _mean(
        [
            1.0 if storylines else 0.0,
            1.0 if latent else 0.0,
            min(1.0, judged / max(1, len(storylines))),
        ]
    )
    # The independent judge is the only measure here that asks whether the
    # recovered Models state the actual operating thesis. Lexical facets,
    # topology and concrete-Model presence are supporting diagnostics, not
    # substitutes for causal correctness.
    score = _weighted(
        [
            (avg_latent, 0.15),
            (best_coverage, 0.10),
            (concrete, 0.15),
            (thesis_score, 0.25),
            (thesis_accuracy, 0.35),
        ]
    )
    weakest = sorted(
        (
            (str(row.get("storyline_id") or row.get("title") or "unknown"),
             _ratio01(row.get("latent_pattern_score")))
            for row in storylines
        ),
        key=lambda item: item[1],
    )[:5]
    return _dimension(
        score,
        coverage,
        {
            "average_latent_pattern_score": avg_latent,
            "average_best_pattern_coverage": best_coverage,
            "concrete_pattern_storyline_ratio": concrete,
            "thesis_judgement_count": judged,
            "thesis_average_score": thesis_score,
            "thesis_accuracy": thesis_accuracy,
            "causal_thesis_miss_rate": (
                round(1.0 - thesis_accuracy, 4) if judged else None
            ),
            "independent_thesis_weight": 0.60,
            "proxy_structure_weight": 0.40,
            "weakest_storylines": [
                {"storyline": name, "latent_pattern_score": score}
                for name, score in weakest
            ],
        },
    )


def _temporal_dimension(
    dimensions: JsonObject,
    gaps: list[str],
) -> dict[str, Any]:
    temporal = _object(dimensions.get("temporal_improvement"))
    metrics = _object(temporal.get("metrics"))
    events = int(_number(metrics.get("future_validation_events")))
    memory_touches = int(
        _number(metrics.get("future_validation_memory_touch_ops"))
    )
    context_use = _ratio01(
        metrics.get("future_validation_model_or_graph_context_use_score")
    )
    if events == 0:
        gaps.append("No future-validation events exercised temporal learning.")
    touch_rate = _ratio01(memory_touches / max(1, events))
    score = _weighted(
        [
            (_ratio01(temporal.get("score")), 0.55),
            (touch_rate, 0.25),
            (context_use, 0.20),
        ]
    )
    return _dimension(
        score,
        1.0 if temporal and events else (0.5 if temporal else 0.0),
        {
            "benchmark_temporal_score": _ratio01(temporal.get("score")),
            "future_validation_events": events,
            "future_validation_memory_touch_ops": memory_touches,
            "future_validation_memory_touch_rate": touch_rate,
            "future_validation_context_use": context_use,
        },
    )


def _entity_model_dimension(
    dimensions: JsonObject,
    vitals: JsonObject | None,
    entity_evidence: JsonObject | None,
    gaps: list[str],
) -> dict[str, Any]:
    memory_truth = _score(dimensions, "memory_truth")
    compression = _score(dimensions, "compression")
    edge = _score(dimensions, "edge_intelligence")
    vital_rows = _object((vitals or {}).get("vitals"))
    coherence = _nested_score(vital_rows, "model_coherence")
    metabolism = _nested_score(vital_rows, "metabolism_yield")
    entity_quality, entity_coverage, entity_metrics = _objective_entity_quality(
        entity_evidence, gaps
    )
    values = [memory_truth, compression, edge, coherence, metabolism, entity_quality]
    observed = [value for value in values if value is not None]
    if entity_quality is None:
        gaps.append(
            "No explicit entity-extraction/resolution quality score was found; "
            "active-surface status is not objective entity-quality evidence."
        )
    return _dimension(
        _mean(observed),
        (
            sum(value is not None for value in values[:-1]) + entity_coverage
        ) / len(values),
        {
            "memory_truth": memory_truth,
            "compression": compression,
            "edge_intelligence": edge,
            "model_coherence": coherence,
            "metabolism_yield": metabolism,
            "entity_identity_quality": entity_quality,
            "entity_identity_evidence_coverage": entity_coverage,
            "entity_identity_metrics": entity_metrics,
        },
    )


def _objective_entity_quality(
    evidence: JsonObject | None,
    gaps: list[str],
) -> tuple[float | None, float, dict[str, Any]]:
    """Score only numeric evaluator output, never a component status label."""

    if not evidence:
        return None, 0.0, {}
    if evidence.get("schema_version") not in {
        "objective-entity-evidence-v1", "objective-entity-evidence-v2"
    }:
        gaps.append("Objective entity evidence has an unsupported schema version.")
        return None, 0.0, {}
    extraction = _object(evidence.get("extraction"))
    pipeline = _object(evidence.get("pipeline"))
    extraction_overall = _object(extraction.get("overall"))
    pipeline_overall = _object(pipeline.get("overall"))
    if extraction.get("schema_version") != "gold-entity-extraction-v1":
        gaps.append("Objective entity evidence lacks a labeled extraction-v1 report.")
        extraction_overall = {}
    if pipeline.get("schema_version") != "gold-entity-pipeline-v4":
        gaps.append("Objective entity evidence lacks a labeled entity-pipeline-v4 report.")
        pipeline_overall = {}

    components = {
        "exact_span_f1": _optional_ratio(extraction_overall.get("span_f1")),
        "type_accuracy": _optional_ratio(extraction_overall.get("type_accuracy")),
        "canonical_link_accuracy": _optional_ratio(
            pipeline_overall.get("canonical_link_accuracy")
        ),
        "canonical_link_coverage": _optional_ratio(
            pipeline_overall.get("canonical_link_coverage")
        ),
        "grounding_lineage_integrity": _optional_ratio(
            pipeline_overall.get("lineage_integrity")
        ),
        "semantic_lineage_integrity": _optional_ratio(
            pipeline_overall.get("semantic_lineage_integrity")
        ),
        "relation_admission_accuracy": _optional_ratio(
            pipeline_overall.get("relation_admission_accuracy")
        ),
        "relation_endpoint_accuracy": _optional_ratio(
            pipeline_overall.get("relation_endpoint_accuracy")
        ),
        "relation_type_accuracy": _optional_ratio(
            pipeline_overall.get("relation_type_accuracy")
        ),
        "relation_direction_accuracy": _optional_ratio(
            pipeline_overall.get("relation_direction_accuracy")
        ),
        "relation_lineage_coverage": _optional_ratio(
            pipeline_overall.get("relation_lineage_coverage")
        ),
        "false_link_safety": _inverse_optional_ratio(
            pipeline_overall.get("harmful_false_link_rate")
        ),
        "topology_propagation_safety": _inverse_optional_ratio(
            pipeline_overall.get("harmful_topology_propagation_rate")
        ),
        "active_relation_lineage_safety": _inverse_optional_ratio(
            pipeline_overall.get("unlineaged_active_relation_rate")
        ),
    }
    if evidence.get("schema_version") == "objective-entity-evidence-v2":
        readiness = _object(evidence.get("readiness"))
        readiness_components = _object(readiness.get("component_scores"))
        components.update({
            "adversarial_topology_safety": _optional_ratio(
                readiness_components.get("adversarial_topology")
            ),
            "correction_propagation_safety": _optional_ratio(
                readiness_components.get("correction_safety")
            ),
            "consequence_tier_safety": _optional_ratio(
                readiness_components.get("consequence_safety")
            ),
            "open_world_abstention_safety": _optional_ratio(
                readiness_components.get("open_world_safety")
            ),
        })
    observed = [value for value in components.values() if value is not None]
    coverage = len(observed) / len(components)
    if components["canonical_link_accuracy"] is None or components[
        "canonical_link_coverage"
    ] is None:
        gaps.append("Objective canonical-link accuracy or coverage is unpopulated.")
    topology_keys = (
        "relation_admission_accuracy", "relation_endpoint_accuracy",
        "relation_type_accuracy", "relation_direction_accuracy",
        "relation_lineage_coverage", "topology_propagation_safety",
        "active_relation_lineage_safety",
    )
    if any(components[key] is None for key in topology_keys):
        gaps.append("Objective relation/topology evidence is incomplete or unpopulated.")
    for source, label in ((extraction, "extraction"), (pipeline, "pipeline")):
        for uncertainty in _strings(source.get("uncertainties")):
            gaps.append(f"Objective entity {label} uncertainty: {uncertainty}")
    gaps.extend(
        f"Objective entity evidence: {item}"
        for item in _strings(evidence.get("proof_gaps"))
    )
    if evidence.get("schema_version") == "objective-entity-evidence-v2":
        adversarial = _object(evidence.get("adversarial_company_physics"))
        population = _object(adversarial.get("population"))
        attempts = population.get("adversarial_relation_attempts")
        gaps.append(
            "Objective adversarial company-physics evidence is bounded to "
            f"{attempts if isinstance(attempts, int) else 'unknown'} relation "
            "attempts and does not establish company-scale generalization."
        )
    component_weights = {
        key: (0.25 if key in {
            "adversarial_topology_safety", "correction_propagation_safety",
            "consequence_tier_safety", "open_world_abstention_safety",
        } else 1.0)
        for key in components
    }
    weighted = [
        (value, component_weights[key])
        for key, value in components.items() if value is not None
    ]
    weighted_score = (
        sum(value * weight for value, weight in weighted)
        / sum(weight for _value, weight in weighted)
        if weighted else None
    )
    return (
        weighted_score,
        coverage,
        {
            "components": components,
            "observed_component_count": len(observed),
            "required_component_count": len(components),
            "coverage": round(coverage, 4),
            "component_weights": component_weights,
            "bounded_adversarial_total_weight": 1.0,
        },
    )


def _learning_dimension(
    vitals: JsonObject | None,
    assurance: JsonObject | None,
    gaps: list[str],
) -> dict[str, Any]:
    vital_rows = _object((vitals or {}).get("vitals"))
    self_improvement = _nested_score(vital_rows, "self_improvement")
    human_loop = _nested_score(vital_rows, "human_loop_closure")
    positive = _assurance_lift(assurance)
    correction = _assurance_component_score(assurance, "correction")
    retention = _assurance_component_score(assurance, "retention")
    safety = _assurance_component_score(assurance, "negative")
    values = [positive, correction, retention, safety, self_improvement, human_loop]
    observed = [value for value in values if value is not None]
    if assurance is None:
        gaps.append("Learning and correction lift lacks Assurance v7 evidence.")
    return _dimension(
        _mean(observed),
        len(observed) / len(values),
        {
            "adaptive_minus_frozen_lift": positive,
            "correction_assurance": correction,
            "retention_assurance": retention,
            "negative_control_assurance": safety,
            "self_improvement": self_improvement,
            "human_loop_closure": human_loop,
        },
    )


def _operational_dimension(
    *,
    benchmark: JsonObject,
    run_summary: JsonObject,
    vitals: JsonObject | None,
    hard_failures: list[str],
) -> dict[str, Any]:
    health = _object(benchmark.get("run_health"))
    pending_triggers = int(
        _number(health.get("pending_triggers", run_summary.get("pending_triggers")))
    )
    pending_post = int(
        _number(
            health.get(
                "pending_post_commit_actions",
                run_summary.get("pending_post_commit_actions"),
            )
        )
    )
    dead = int(
        _number(
            health.get(
                "dead_lettered_post_commit_actions",
                run_summary.get("dead_lettered_post_commit_actions"),
            )
        )
    )
    failed_think = int(
        _number(health.get("think_runs_failed", run_summary.get("think_runs_failed")))
    )
    successful_think = int(
        _number(
            health.get("think_runs_success", run_summary.get("think_runs_success"))
        )
    )
    validation_errors = int(
        _number(
            _object(benchmark.get("run_amplification")).get(
                "validation_error_count"
            )
        )
    )
    total_runs = max(1, successful_think + failed_think)
    success_rate = _ratio01(successful_think / total_runs)
    error_score = 1.0 / (1.0 + failed_think + validation_errors)
    drain_score = 1.0 / (1.0 + pending_triggers + pending_post + dead)
    vitals_control = _nested_score(
        _object((vitals or {}).get("vitals")), "control_plane_health"
    )
    components = [drain_score, success_rate, error_score]
    if vitals_control is not None:
        components.append(vitals_control)
    return _dimension(
        _mean(components),
        1.0 if benchmark and run_summary else 0.5,
        {
            "pending_triggers": pending_triggers,
            "pending_post_commit_actions": pending_post,
            "dead_lettered_post_commit_actions": dead,
            "think_runs_success": successful_think,
            "think_runs_failed": failed_think,
            "think_success_rate": success_rate,
            "validation_errors": validation_errors,
            "control_plane_health": vitals_control,
            "noncompensatory_failure_count": len(hard_failures),
        },
    )


def _proof_dimension(
    *,
    benchmark: JsonObject,
    run_summary: JsonObject,
    scorecard: JsonObject,
    assurance: JsonObject | None,
    run_config: JsonObject,
    profile: SimulationProfile,
    gaps: list[str],
    hard_failures: list[str],
) -> dict[str, Any]:
    waves = _objects(benchmark.get("waves"))
    successful_batches = sum(
        1
        for wave in waves
        if _object(_object(wave.get("t1_batch")).get("run")).get("status")
        == "success"
    )
    if successful_batches == 0:
        successful_batches = int(
            _number(
                _object(benchmark.get("run_amplification")).get(
                    "think_runs_success"
                )
            )
        )
    signals = int(
        _number(benchmark.get("signals", run_summary.get("signal_count")))
    )
    storylines = int(
        _number(benchmark.get("storyline_count"))
        or len(_objects(benchmark.get("storyline_scores")))
    )
    temporal_metrics = _object(
        _object(
            _object(scorecard.get("dimensions")).get("temporal_improvement")
        ).get("metrics")
    )
    future_events = int(_number(temporal_metrics.get("future_validation_events")))
    thesis_n = int(
        _number(_object(benchmark.get("thesis_recovery_judge")).get("n"))
    )
    run_contract = _authoritative_run_contract(
        benchmark=benchmark,
        run_summary=run_summary,
        run_config=run_config,
        required_batches=profile.required_t1_batches,
        hard_failures=hard_failures,
        gaps=gaps,
    )
    scale = {
        "signals": _scale(signals, profile.minimum_signals),
        "storylines": _scale(storylines, profile.minimum_storylines),
        "successful_t1_batches": _scale(
            successful_batches, profile.required_t1_batches
        ),
        "future_validation_events": _scale(
            future_events, profile.minimum_future_validation_events
        ),
        "thesis_judgements": _scale(
            thesis_n, profile.minimum_thesis_judgements
        ),
    }
    for name, payload in scale.items():
        if payload["coverage"] < 1.0:
            gaps.append(
                f"Profile {profile.name} scale is short on "
                f"{name.replace('_', ' ')}: {payload['observed']}/"
                f"{payload['required']}."
            )
    artifact_coverage = _mean(
        [
            1.0 if benchmark else 0.0,
            1.0 if run_summary else 0.0,
            1.0 if assurance else 0.0,
            float(run_contract["coverage"]),
        ]
    )
    scale_coverage = _mean(
        [float(payload["coverage"]) for payload in scale.values()]
    )
    reported_gaps = len(_strings(scorecard.get("proof_gaps")))
    gap_quality = 1.0 / (1.0 + 0.10 * reported_gaps)
    return _dimension(
        _weighted(
            [
                (artifact_coverage, 0.35),
                (scale_coverage, 0.50),
                (gap_quality, 0.15),
            ]
        ),
        _mean([artifact_coverage, scale_coverage]),
        {
            "scale": scale,
            "run_contract": run_contract,
            "artifact_coverage": artifact_coverage,
            "scale_coverage": scale_coverage,
            "benchmark_reported_proof_gap_count": reported_gaps,
        },
    )


def _authoritative_run_contract(
    *,
    benchmark: JsonObject,
    run_summary: JsonObject,
    run_config: JsonObject,
    required_batches: int,
    hard_failures: list[str],
    gaps: list[str],
) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    checks["run_mode"] = run_config.get("mode") == "run"
    checks["exact_target_t1_batches"] = (
        int(_number(run_config.get("target_t1_batches"))) == required_batches
    )
    checks["exact_signal_population"] = int(
        _number(benchmark.get("signals", run_summary.get("signal_count")))
    ) == 1_125
    checks["zero_seeded_models_configured"] = (
        int(_number(run_config.get("seed_models"))) == 0
    )
    checks["fresh_non_append_run"] = not bool(
        run_summary.get("append") or run_config.get("append_to_run_id")
    )
    checks["batching_configured"] = (
        _number(run_config.get("t1_batch_min_size")) > 1
        and _number(run_config.get("t1_batch_window_s")) > 0
    )
    waves = _objects(benchmark.get("waves"))
    t1_batches = [
        _object(wave.get("t1_batch"))
        for wave in waves
        if _object(wave.get("t1_batch"))
    ]
    checks["exact_required_t1_batches_observed"] = (
        len(t1_batches) == required_batches
    )
    checks["every_t1_run_genuinely_batched"] = bool(t1_batches) and all(
        not batch.get("unbatched")
        and int(_number(batch.get("member_count"))) > 1
        for batch in t1_batches
    )
    checks["all_signals_processed_in_batches"] = bool(t1_batches) and all(
        int(_number(batch.get("observation_count"))) == 25
        and int(_number(batch.get("member_count"))) == 25
        for batch in t1_batches
    )

    before = _object(
        run_summary.get("semantic_memory_before_first_wave")
        or benchmark.get("semantic_memory_before_first_wave")
    )
    semantic_keys = (
        "models",
        "model_edges",
        "pattern_candidates",
        "hypotheses",
    )
    checks["pre_first_wave_semantic_snapshot_present"] = all(
        key in before for key in semantic_keys
    )
    checks["pre_first_wave_semantic_memory_zero"] = (
        checks["pre_first_wave_semantic_snapshot_present"]
        and all(int(_number(before.get(key))) == 0 for key in semantic_keys)
    )
    scaffolding = _object(
        run_summary.get("pre_first_wave_scaffolding")
        or benchmark.get("pre_first_wave_scaffolding")
    )
    if not before:
        gaps.append(
            "No pre-first-wave semantic-memory snapshot proves that Models, "
            "edges, pattern candidates, and hypotheses started at zero."
        )
    for name, passed in checks.items():
        if not passed:
            hard_failures.append(
                f"authoritative run contract failed: {name.replace('_', ' ')}"
            )
    return {
        "required_batches": required_batches,
        "checks": checks,
        "checks_satisfied": sum(checks.values()),
        "checks_total": len(checks),
        "coverage": round(sum(checks.values()) / max(1, len(checks)), 4),
        "semantic_memory_before_first_wave": dict(before),
        "pre_first_wave_scaffolding": dict(scaffolding),
        "scaffolding_note": (
            "Tenant, source, actor, and company scaffolding may exist before "
            "wave one. It is not semantic memory and is reported separately."
        ),
    }


def _retrieval_evolution(
    benchmark: JsonObject,
    gaps: list[str],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for index, wave in enumerate(_objects(benchmark.get("waves")), start=1):
        batch = _object(wave.get("t1_batch"))
        run = _object(batch.get("run"))
        if not run:
            continue
        observations = int(_number(run.get("retrieval_observation_count")))
        models = int(_number(run.get("retrieval_model_count")))
        total = observations + models
        ops = _object(run.get("ops_applied"))
        context = _object(ops.get("context_use"))
        rows.append(
            {
                "wave": wave.get("wave", index),
                "sequence": wave.get("sequence"),
                "retrieved_observations": observations,
                "retrieved_models": models,
                "model_share": round(models / total, 4) if total else None,
                "observation_share": (
                    round(observations / total, 4) if total else None
                ),
                "model_references_used": context.get("model_references_used"),
                "observation_references_used": context.get(
                    "observation_references_used"
                ),
                "late_raw_reopening_reasons": context.get(
                    "raw_observation_reopening_reasons"
                ),
            }
        )
    shares = [
        float(row["model_share"])
        for row in rows
        if row["model_share"] is not None
    ]
    if not shares:
        gaps.append(
            "Wave artifacts do not expose retrieval Model/Observation counts."
        )
    used_refs_present = any(
        row["model_references_used"] is not None
        or row["observation_references_used"] is not None
        for row in rows
    )
    if not used_refs_present:
        gaps.append(
            "Wave artifacts expose selected retrieval counts but not whether "
            "Model and Observation references were actually used."
        )
    late_rows = rows[(2 * len(rows)) // 3 :]
    late_raw = [
        row
        for row in late_rows
        if int(row["retrieved_observations"]) > 0
    ]
    unjustified = [
        row for row in late_raw if not row["late_raw_reopening_reasons"]
    ]
    if late_raw and len(unjustified) == len(late_raw):
        gaps.append(
            "Late raw-observation retrieval occurs, but artifacts do not "
            "attribute it to uncertainty, contradiction, correction, or "
            "provenance needs."
        )
    phases = _retrieval_phases(rows)
    slope = _linear_slope(shares)
    transition = "unmeasured"
    if shares:
        if slope > 0.01:
            transition = "increasing_model_reliance"
        elif slope < -0.01:
            transition = "increasing_raw_observation_reliance"
        else:
            transition = "flat_mixed_retrieval"
    return {
        "wave_count": len(rows),
        "waves": rows,
        "phases": phases,
        "model_share_linear_slope_per_wave": round(slope, 6),
        "transition": transition,
        "reference_use_observed": used_refs_present,
        "late_raw_observation_reopening_count": len(late_raw),
        "late_raw_reopening_with_recorded_reason_count": (
            len(late_raw) - len(unjustified)
        ),
    }


def _retrieval_phases(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    cut1 = max(1, len(rows) // 3)
    cut2 = max(cut1 + 1, (2 * len(rows)) // 3)
    groups = {
        "early": rows[:cut1],
        "middle": rows[cut1:cut2],
        "late": rows[cut2:],
    }
    out: dict[str, Any] = {}
    for name, phase in groups.items():
        shares = [
            float(row["model_share"])
            for row in phase
            if row["model_share"] is not None
        ]
        out[name] = {
            "wave_count": len(phase),
            "average_model_share": round(_mean(shares), 4) if shares else None,
            "retrieved_models": sum(
                int(row["retrieved_models"]) for row in phase
            ),
            "retrieved_observations": sum(
                int(row["retrieved_observations"]) for row in phase
            ),
        }
    return out


def _linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    x_mean = (len(values) - 1) / 2
    y_mean = _mean(values)
    numerator = sum(
        (index - x_mean) * (value - y_mean)
        for index, value in enumerate(values)
    )
    denominator = sum(
        (index - x_mean) ** 2 for index in range(len(values))
    )
    return numerator / denominator if denominator else 0.0


def _assurance_lift(assurance: JsonObject | None) -> float | None:
    if not assurance:
        return None
    positive = _object(assurance.get("positive"))
    value = positive.get("adaptive_minus_frozen_correctness")
    return _ratio01(value) if value is not None else None


def _assurance_component_score(
    assurance: JsonObject | None,
    key: str,
) -> float | None:
    if not assurance:
        return None
    component = _object(assurance.get(key))
    if not component:
        return None
    status = str(component.get("status") or "")
    if status in {"working", "observed", "substantiated", "passed"}:
        return 1.0
    if status in {"failed", "unsafe"}:
        return 0.0
    report = _object(component.get("report"))
    support = report.get("support_ratio", report.get("satisfaction_ratio"))
    return _ratio01(support) if support is not None else None


def _score(dimensions: JsonObject, key: str) -> float | None:
    payload = _object(dimensions.get(key))
    return _ratio01(payload.get("score")) if payload else None


def _nested_score(rows: JsonObject, key: str) -> float | None:
    payload = _object(rows.get(key))
    return _ratio01(payload.get("score")) if payload else None


def _dimension(
    score: float,
    coverage: float,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "score": round(_ratio01(score), 4),
        "coverage": round(_ratio01(coverage), 4),
        "metrics": metrics,
    }


def _scale(observed: int, required: int) -> dict[str, Any]:
    coverage = 1.0 if required == 0 else _ratio01(observed / required)
    return {
        "observed": observed,
        "required": required,
        "coverage": round(coverage, 4),
    }


def _status(
    *,
    score: float,
    coverage: float,
    hard_failures: list[str],
) -> str:
    if hard_failures:
        return "not_credible"
    if coverage < 0.80:
        return "insufficient_evidence"
    if score >= 0.85 and coverage >= 0.95:
        return "strong"
    if score >= 0.70:
        return "credible_with_gaps"
    return "weak"


def _interpretation(status: str, score: float, coverage: float) -> str:
    return (
        f"{status.replace('_', ' ')}: measured quality {score:.1%}, "
        f"evidence coverage {coverage:.1%}. Safety and drain failures are "
        "non-compensatory."
    )


def _claims_supported(
    *,
    dimensions: JsonObject,
    hard_failures: list[str],
) -> list[str]:
    if hard_failures:
        return []
    claims: list[str] = []
    labels = {
        "hidden_pattern_recovery": "recovers planted hidden patterns",
        "temporal_improvement": "uses prior memory to improve later waves",
        "entity_model_quality": "forms a coherent entity and Model substrate",
        "learning_correction_lift": "learns from correction with measured lift",
        "operational_drain": "drains the evaluated workload reliably",
    }
    for key, label in labels.items():
        payload = _object(dimensions.get(key))
        if _number(payload.get("score")) >= 0.8 and _number(
            payload.get("coverage")
        ) >= 0.8:
            claims.append(label)
    return claims


def _claims_not_supported(gaps: list[str]) -> list[str]:
    base = [
        "real-customer validity or product-market fit",
        "production connector, webhook, OAuth, or transport durability",
        "open-world accuracy outside the simulated distribution",
        "causal truth from correlation alone",
        "autonomous task execution",
    ]
    if gaps:
        base.append("capabilities named by unresolved proof gaps")
    return base


def _object(value: Any) -> JsonObject:
    return value if isinstance(value, Mapping) else {}


def _objects(value: Any) -> list[JsonObject]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _strings(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item) for item in value if str(item).strip()]


def _number(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _ratio01(value: Any) -> float:
    return max(0.0, min(1.0, _number(value)))


def _optional_ratio(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    numeric = float(value)
    if not math.isfinite(numeric) or not 0.0 <= numeric <= 1.0:
        return None
    return numeric


def _inverse_optional_ratio(value: Any) -> float | None:
    observed = _optional_ratio(value)
    return None if observed is None else 1.0 - observed


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _weighted(values: list[tuple[float, float]]) -> float:
    denominator = sum(weight for _, weight in values)
    return (
        sum(value * weight for value, weight in values) / denominator
        if denominator
        else 0.0
    )


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))
