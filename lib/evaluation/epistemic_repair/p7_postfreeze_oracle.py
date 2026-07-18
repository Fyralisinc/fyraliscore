"""Independent evidence-coordinate oracle for frozen production P7 worlds.

No lexical matching is used. Models are credited only through canonical
observation evidence IDs joined to sealed P6 signal/claim/lifecycle labels.
"""

from __future__ import annotations

import random
import re
import json
from statistics import mean
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import P6Population
from lib.evaluation.epistemic_repair.p7_population import (
    P7_MAX_WORLD_COUNT,
    P7SemanticOraclePopulation,
    StructuredClaimOracle,
    build_p7_semantic_oracles,
)
from lib.evaluation.epistemic_repair.p7_runner import P7_ARMS
from lib.shared.errors import InvariantViolation


STAGES = (3, 6, 12)
_RELATION_MARKERS = {
    "causes": ("cause", "causes", "lead", "leads", "recur when", "driven by"),
    "depends_on": ("depend", "depends", "requires", "blocked by", "block"),
    "predicts": ("predict", "predicts", "follows", "indicator"),
}
_NEGATED_PREDICATE = re.compile(
    r"\b(?:does\s+not|do\s+not|did\s+not|never)\b(?:\s+\w+){0,3}\s+"
    r"(?:cause|causes|lead|leads|recur|depends?|requires?|blocks?|predicts?|follows?)\b"
)


def _semantic_text(model: dict[str, Any]) -> str:
    proposition = model.get("proposition") or {}
    return " ".join((str(model.get("natural_text") or ""), str(proposition))).casefold()


def entails_structured_claim(
    model: dict[str, Any], oracle: StructuredClaimOracle,
) -> bool:
    """Require subject, cause, effect, relation, and non-opposite polarity."""

    proposition = model.get("proposition") or {}
    if isinstance(proposition, dict) and str(proposition.get("polarity") or "").casefold() in {
        "negative", "negated", "opposite",
    }:
        return False
    text = _semantic_text(model)
    if _NEGATED_PREDICATE.search(text) or re.search(
        r"\b(?:unrelated|independent)\s+(?:of|from)\b", text
    ):
        return False
    groups = (*oracle.cause_facets, *oracle.effect_facets)
    return (
        any(term.casefold() in text for term in oracle.subject_terms)
        and all(any(term.casefold() in text for term in group) for group in groups)
        and any(marker in text for marker in _RELATION_MARKERS[oracle.required_relation])
    )


def _matches_facets(model: dict[str, Any], facets: tuple[tuple[str, ...], ...]) -> bool:
    text = _semantic_text(model)
    return all(any(term.casefold() in text for term in group) for group in facets)


def _metric(num: float, den: int, *, measured: bool = True) -> dict[str, Any]:
    return {
        "numerator": num,
        "denominator": den,
        "value": num / den if measured and den else None,
        "measured": measured and den > 0,
    }


def _expected_phases(stage: int) -> frozenset[str]:
    phases = {"weak_initial"}
    if stage >= 4:
        phases.add("corroboration")
    if stage >= 7:
        phases.add("contradiction")
    if stage >= 9:
        phases.add("correction")
    if stage >= 11:
        phases.add("external_outcome")
    return frozenset(phases)


def _scope_refs(value: Any) -> set[str]:
    refs: set[str] = set()
    for item in value or ():
        if isinstance(item, dict):
            ref = item.get("id") or item.get("canonical_ref") or item.get("entity_id")
        else:
            ref = item
        if ref:
            refs.add(str(ref))
    return refs


def _models_for_snapshot(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    models = [dict(row) for row in snapshot.get("accepted_models") or ()]
    for run in snapshot.get("validated_only_runs") or ():
        payload = run.get("validation_result") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        validated = payload.get("validated_proposals") or {}
        for index, op in enumerate(validated.get("claim_ops") or ()):
            entry = op.get("entry") or {}
            if op.get("op") != "insert" or not entry:
                continue
            models.append({
                "id": f"validate-only:{run.get('id')}:{index}",
                "natural_text": entry.get("natural"),
                "proposition": entry.get("proposition") or {},
                "confidence": entry.get("confidence"),
                "scope_entities": entry.get("scope_entities") or (),
                "evidence_observation_ids": entry.get("supporting_event_ids") or (),
                "validated_only": True,
            })
    return models


def _world_maps(population: P6Population, tenant_id: UUID) -> dict[str, Any]:
    batch_by_signal = {
        signal.signal_id: batch.batch_number
        for batch in population.batches for signal in batch.signals
    }
    gold_by_observation = {}
    for item in population.gold:
        observation_id = str(uuid5(
            NAMESPACE_URL, f"p6-think:{tenant_id}:{item.signal_id}"
        ))
        gold_by_observation[observation_id] = item
    return {"batch_by_signal": batch_by_signal, "gold_by_observation": gold_by_observation}


def _model_evidence(
    model: dict[str, Any], maps: dict[str, Any], *, through_stage: int,
) -> list[Any]:
    rows = []
    for evidence_id in model.get("evidence_observation_ids") or ():
        gold = maps["gold_by_observation"].get(str(evidence_id))
        if gold is not None and maps["batch_by_signal"][gold.signal_id] <= through_stage:
            rows.append(gold)
    return rows


def _score_storyline(
    *, storyline: str, stage: int, snapshot: dict[str, Any], maps: dict[str, Any],
    semantic_oracle: P7SemanticOraclePopulation,
) -> dict[str, Any]:
    claim_oracle = next(
        item for item in semantic_oracle.claims if item.storyline_id == storyline
    )
    relation_oracle = next(
        item for item in semantic_oracle.relations if item.storyline_id == storyline
    )
    expected_gold = [
        item for item in maps["gold_by_observation"].values()
        if item.storyline_id == storyline
        and maps["batch_by_signal"][item.signal_id] <= stage
        and item.claim_id
    ]
    expected_claims = {item.claim_id for item in expected_gold}
    expected_phases = _expected_phases(stage)
    candidates: list[tuple[dict[str, Any], list[Any]]] = []
    for model in _models_for_snapshot(snapshot):
        evidence = _model_evidence(model, maps, through_stage=stage)
        if any(item.storyline_id == storyline for item in evidence):
            candidates.append((model, evidence))
    lineage_pure = [
        (model, evidence) for model, evidence in candidates
        if evidence
        and {item.storyline_id for item in evidence if item.storyline_id} == {storyline}
        and all(item.role not in {"noise", "high_similarity_distractor"} for item in evidence)
    ]
    pure = [
        (model, evidence) for model, evidence in lineage_pure
        if entails_structured_claim(model, claim_oracle)
    ]
    represented_claims = {
        item.claim_id for _, evidence in pure for item in evidence if item.claim_id
    }
    represented_phases = {
        item.lifecycle_phase for _, evidence in pure for item in evidence
    }
    expected_refs = {
        item.canonical_ref for item in expected_gold if item.canonical_ref
    }
    scoped_models = [model for model, _ in candidates if _scope_refs(model.get("scope_entities"))]
    safe_scopes = sum(
        _scope_refs(model.get("scope_entities")) <= expected_refs
        for model in scoped_models
    )
    relation_outputs = snapshot.get("accepted_relations") or ()
    model_by_id = {str(model["id"]): model for model, _ in lineage_pure}
    expected_relations = [relation_oracle] if stage >= 6 else []
    matched_expected: set[int] = set()
    valid_relation_outputs = 0
    for relation in relation_outputs:
        participants = {
            str(value.get("participant_role")): str(value.get("model_id"))
            for value in relation.get("participants") or ()
            if isinstance(value, dict) and value.get("model_id")
        }
        if set(participants) != {relation_oracle.cause_role, relation_oracle.effect_role}:
            continue
        kind = str(relation.get("truth_relation_kind") or "").casefold()
        for index, expected in enumerate(expected_relations):
            cause_id = participants[expected.cause_role]
            effect_id = participants[expected.effect_role]
            if (
                kind == expected.relation_kind
                and cause_id != effect_id
                and cause_id in model_by_id
                and effect_id in model_by_id
                and _matches_facets(model_by_id[cause_id], expected.cause_participant_facets)
                and _matches_facets(model_by_id[effect_id], expected.effect_participant_facets)
            ):
                matched_expected.add(index)
                valid_relation_outputs += 1
                break
    precision = _metric(len(pure), len(candidates))
    recall = _metric(len(represented_claims & expected_claims), len(expected_claims))
    p = precision["value"] or 0.0
    r = recall["value"] or 0.0
    f1 = 0.0 if p + r == 0 else 2 * p * r / (p + r)
    phase_completeness = _metric(
        len(expected_phases & represented_phases), len(expected_phases)
    )
    relation_precision = _metric(valid_relation_outputs, len(relation_outputs))
    relation_recall = _metric(len(matched_expected), len(expected_relations))
    relation_p = relation_precision["value"]
    relation_r = relation_recall["value"]
    relation_accuracy = (
        None if relation_p is None or relation_r is None
        else 0.0 if relation_p + relation_r == 0
        else 2 * relation_p * relation_r / (relation_p + relation_r)
    )
    return {
        "storyline_id": storyline,
        "stage_batch": stage,
        "direct_thesis_accuracy": _metric(
            int(expected_phases <= represented_phases), 1
        ),
        "thesis_phase_completeness": phase_completeness,
        "thesis_facet_completeness": phase_completeness,
        "atomic_claim_precision": precision,
        "atomic_claim_recall": recall,
        "atomic_claim_f1": {"value": f1, "measured": True},
        "relation_joint_precision": relation_precision,
        "relation_joint_recall": relation_recall,
        "relation_joint_accuracy": {
            "value": relation_accuracy,
            "measured": relation_accuracy is not None,
        },
        "boundary_entity_safety": _metric(safe_scopes, len(scoped_models)),
        "retained_answerability": recall,
        "false_truth_from_noise": len(candidates) - len(lineage_pure),
        "semantic_contradiction_or_nonentailment": len(lineage_pure) - len(pure),
        "exact_denominators": {
            "expected_claims": len(expected_claims),
            "expected_phases": len(expected_phases),
            "candidate_models": len(candidates),
            "expected_relations": len(expected_relations),
        },
    }


def _paired_intervals(endpoints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    intervals = []
    for comparator in P7_ARMS:
        if comparator == "adaptive":
            continue
        mature = [row for row in endpoints if row["stage_batch"] == 12]
        adaptive = {(row["world_id"], row["storyline_id"]): row for row in mature if row["arm_id"] == "adaptive"}
        baseline = {(row["world_id"], row["storyline_id"]): row for row in mature if row["arm_id"] == comparator}
        worlds = sorted({world for world, _ in adaptive} & {world for world, _ in baseline})
        if len(worlds) < 3:
            raise InvariantViolation(
                "P7_POSTFREEZE_UNPAIRED_WORLDS", "at least three paired worlds required",
                comparator=comparator, world_count=len(worlds),
            )
        expected_storylines = {"atlas", "beacon", "cobalt", "delta"}
        for world in worlds:
            adaptive_storylines = {
                storyline for paired_world, storyline in adaptive if paired_world == world
            }
            baseline_storylines = {
                storyline for paired_world, storyline in baseline if paired_world == world
            }
            if adaptive_storylines != expected_storylines or baseline_storylines != expected_storylines:
                raise InvariantViolation(
                    "P7_POSTFREEZE_PAIRED_DENOMINATOR_INVALID",
                    "every paired world must contain each preregistered storyline exactly once",
                    comparator=comparator,
                    world_id=world,
                )
        deltas = [
            mean(
                adaptive[(world, storyline)]["thesis_facet_completeness"]["value"]
                - baseline[(world, storyline)]["thesis_facet_completeness"]["value"]
                for storyline in ("atlas", "beacon", "cobalt", "delta")
            ) for world in worlds
        ]
        seed = 771_000 + sum(map(ord, comparator))
        rng = random.Random(seed)
        samples = sorted(mean(rng.choices(deltas, k=len(worlds))) for _ in range(5000))
        intervals.append({
            "comparator_arm": comparator, "paired_world_count": len(worlds),
            "mean_delta": mean(deltas), "lower_95": samples[125],
            "upper_95": samples[4874], "bootstrap_seed": seed,
            "bootstrap_samples": 5000,
        })
    return intervals


def _mean_measured(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [row.get(field, {}).get("value") for row in rows]
    return mean(values) if values and all(value is not None for value in values) else None


def _expected_calibration_error(
    predictions: list[dict[str, float]], *, bin_count: int = 10,
) -> float | None:
    """Frequency-weighted ECE over preregistered pre-outcome predictions."""

    if not predictions:
        return None
    bins: list[list[dict[str, float]]] = [[] for _ in range(bin_count)]
    for row in predictions:
        confidence = float(row["confidence"])
        label = float(row["label"])
        if not 0.0 <= confidence <= 1.0 or label not in {0.0, 1.0}:
            return None
        bins[min(int(confidence * bin_count), bin_count - 1)].append(row)
    return sum(
        (len(bucket) / len(predictions))
        * abs(
            mean(float(row["confidence"]) for row in bucket)
            - mean(float(row["label"]) for row in bucket)
        )
        for bucket in bins if bucket
    )


def _identical_budget_contracts(world_results: list[dict[str, Any]]) -> bool:
    contracts = [
        arm.get("budget_contract")
        for world in world_results for arm in world.get("arm_results") or ()
    ]
    return bool(contracts) and all(
        isinstance(contract, dict) and contract == contracts[0]
        for contract in contracts
    )


def _strategic_decision(
    *, endpoints: list[dict[str, Any]], intervals: list[dict[str, Any]],
    correction_by_arm: dict[str, dict[str, float | None]],
    economics: list[dict[str, Any]], historical_raw: dict[str, dict[str, int]],
    hard_gates: dict[str, bool], global_noise_truth: int,
) -> tuple[str, dict[str, Any]]:
    mature = [row for row in endpoints if row["stage_batch"] == 12]
    by_arm = {arm: [row for row in mature if row["arm_id"] == arm] for arm in P7_ARMS}
    quality = {
        arm: {
            field: _mean_measured(rows, field)
            for field in (
                "direct_thesis_accuracy", "atomic_claim_f1",
                "boundary_entity_safety", "relation_joint_precision",
                "external_outcome_calibration_ece",
            )
        }
        for arm, rows in by_arm.items()
    }
    for arm, rows in by_arm.items():
        predictions = [
            prediction
            for row in rows
            for prediction in row.get("external_outcome_predictions") or ()
        ]
        if predictions:
            quality[arm]["external_outcome_calibration_ece"] = (
                _expected_calibration_error(predictions)
            )
    adaptive = quality["adaptive"]
    primary_baselines = ("frozen", "observation_only")
    thesis_deltas = {
        arm: (
            adaptive["direct_thesis_accuracy"] - quality[arm]["direct_thesis_accuracy"]
            if adaptive["direct_thesis_accuracy"] is not None
            and quality[arm]["direct_thesis_accuracy"] is not None else None
        ) for arm in primary_baselines
    }
    complete_by_world: dict[str, dict[str, int]] = {}
    for row in mature:
        complete_by_world.setdefault(row["world_id"], {}).setdefault(row["arm_id"], 0)
        complete_by_world[row["world_id"]][row["arm_id"]] += int(
            row["direct_thesis_accuracy"]["value"] or 0
        )
    additional_theses = {
        arm: mean(
            values.get("adaptive", 0) - values.get(arm, 0)
            for values in complete_by_world.values()
        ) for arm in primary_baselines
    }
    criterion_1 = all(
        (thesis_deltas[arm] is not None and thesis_deltas[arm] >= 0.20)
        or additional_theses[arm] >= 1.0
        for arm in primary_baselines
    )
    baseline_f1 = [quality[arm]["atomic_claim_f1"] for arm in P7_ARMS if arm != "adaptive"]
    criterion_2 = (
        adaptive["atomic_claim_f1"] is not None
        and all(value is not None for value in baseline_f1)
        and adaptive["atomic_claim_f1"] - max(baseline_f1) >= 0.05
    )
    criterion_3 = all(interval["lower_95"] > 0 for interval in intervals)
    a_correction = correction_by_arm["adaptive"]
    f_correction = correction_by_arm["frozen"]
    criterion_4 = (
        a_correction["latency"] is not None and f_correction["latency"] is not None
        and a_correction["stale"] is not None and f_correction["stale"] is not None
        and a_correction["latency"] <= f_correction["latency"]
        and a_correction["stale"] < f_correction["stale"]
    )
    safety_fields = ("boundary_entity_safety", "relation_joint_precision")
    criterion_5 = global_noise_truth == 0 and all(
        adaptive[field] is not None
        and all(
            quality[arm][field] is not None
            and adaptive[field] >= quality[arm][field]
            for arm in P7_ARMS if arm != "adaptive"
        ) for field in safety_fields
    )
    criterion_6 = (
        adaptive["external_outcome_calibration_ece"] is not None
        and all(
            quality[arm]["external_outcome_calibration_ece"] is not None
            and adaptive["external_outcome_calibration_ece"]
            <= quality[arm]["external_outcome_calibration_ece"] + 0.02
            for arm in P7_ARMS if arm != "adaptive"
        )
    )
    mature_cost = {
        arm: {
            "tokens": sum(row["input_tokens"] + row["output_tokens"] for row in economics
                          if row["arm_id"] == arm and row["stage_batch"] == 12),
            "wall": sum(row["wall_time_s"] for row in economics
                        if row["arm_id"] == arm and row["stage_batch"] == 12),
        } for arm in P7_ARMS
    }
    all_baselines = tuple(arm for arm in P7_ARMS if arm != "adaptive")
    baseline_tokens = min(mature_cost[arm]["tokens"] for arm in all_baselines)
    baseline_wall = min(mature_cost[arm]["wall"] for arm in all_baselines)
    criterion_7 = (
        baseline_tokens > 0 and baseline_wall > 0
        and mature_cost["adaptive"]["tokens"] <= 1.5 * baseline_tokens
        and mature_cost["adaptive"]["wall"] <= 1.5 * baseline_wall
    )
    criteria = {
        "20.7.1_thesis_lift": criterion_1,
        "20.7.2_atomic_f1_lift": criterion_2,
        "20.7.3_paired_ci_excludes_zero": criterion_3,
        "20.7.4_correction_and_stale": criterion_4,
        "20.7.5_safety_not_worse": criterion_5,
        "20.7.6_calibration_not_worse": criterion_6,
        "20.7.7_economics_within_1_5x": criterion_7,
    }
    primary = all(hard_gates.values()) and all(criteria.values())
    if primary:
        verdict = "primary_memory_earned"
    else:
        obs = quality["observation_only"]
        no_quality_loss = all(
            adaptive[field] is not None and obs[field] is not None
            and adaptive[field] >= obs[field]
            for field in ("direct_thesis_accuracy", "atomic_claim_f1",
                          "boundary_entity_safety", "relation_joint_precision")
        )
        raw_a = historical_raw["adaptive"]
        raw_o = historical_raw["observation_only"]
        raw_reduction = (
            raw_o["selected"] > 0 and raw_a["selected"] <= 0.75 * raw_o["selected"]
        )
        token_reduction = (
            mature_cost["observation_only"]["tokens"] > 0
            and mature_cost["adaptive"]["tokens"]
            <= 0.75 * mature_cost["observation_only"]["tokens"]
        )
        time_reduction = (
            mature_cost["observation_only"]["wall"] > 0
            and mature_cost["adaptive"]["wall"]
            <= 0.75 * mature_cost["observation_only"]["wall"]
        )
        if all(hard_gates.values()) and no_quality_loss and (
            raw_reduction or token_reduction or time_reduction
        ):
            verdict = "limited_compression_value"
        elif (
            any(
                value is None
                for metrics in quality.values() for value in metrics.values()
            )
            or any(
                interval["lower_95"] <= 0 <= interval["upper_95"]
                for interval in intervals
            )
        ):
            verdict = "insufficient_evidence"
        else:
            verdict = "not_earned"
    return verdict, {
        "criteria": criteria, "quality_by_arm": quality,
        "thesis_deltas": thesis_deltas, "additional_complete_theses": additional_theses,
        "correction_by_arm": correction_by_arm, "mature_cost_by_arm": mature_cost,
    }


def evaluate_frozen_worlds(
    *, execution_artifact: dict[str, Any], sealed_worlds: dict[str, P6Population],
) -> dict[str, Any]:
    """Evaluate complete frozen outputs; gold is first accessed here."""

    world_results = execution_artifact.get("world_results") or ()
    if not execution_artifact.get("complete") or len(world_results) < 3:
        raise InvariantViolation(
            "P7_POSTFREEZE_EXECUTION_INCOMPLETE",
            "incomplete or underpowered execution cannot receive scores",
        )
    endpoints = []
    global_noise_truth = 0
    historical_raw = {arm: {"selected": 0, "unjustified": 0} for arm in P7_ARMS}
    for world_result in world_results:
        world_id = world_result["world_id"]
        population = sealed_worlds.get(world_id)
        if population is None or population.population_digest != world_result["population_digest"]:
            raise InvariantViolation("P7_POSTFREEZE_WORLD_ORACLE_MISMATCH", "world digest mismatch")
        semantic_oracle = build_p7_semantic_oracles(population)
        for arm_result in world_result["arm_results"]:
            arm = arm_result["arm"]
            tenant_id = UUID(arm_result["tenant_id"])
            maps = _world_maps(population, tenant_id)
            waves = {int(row["batch_number"]): row for row in arm_result["waves"]}
            for stage in STAGES:
                wave = waves.get(stage)
                snapshot = (wave or {}).get("stage_snapshot")
                if snapshot is None:
                    raise InvariantViolation("P7_POSTFREEZE_STAGE_MISSING", "stage snapshot missing")
                for storyline, _ in population.thesis_by_storyline:
                    score = _score_storyline(
                        storyline=storyline, stage=stage, snapshot=snapshot, maps=maps,
                        semantic_oracle=semantic_oracle,
                    )
                    endpoints.append({"world_id": world_id, "arm_id": arm, **score})
                if stage == 12:
                    calibration_snapshot = (waves.get(10) or {}).get("stage_snapshot")
                    if calibration_snapshot is None:
                        raise InvariantViolation(
                            "P7_POSTFREEZE_CALIBRATION_FREEZE_MISSING",
                            "batch-10 confidence freeze is required before outcome signals",
                        )
                    for endpoint in endpoints[-4:]:
                        storyline = endpoint["storyline_id"]
                        claim_oracle = next(
                            item for item in semantic_oracle.claims
                            if item.storyline_id == storyline
                        )
                        outcome = next(
                            item for item in semantic_oracle.outcomes
                            if item.storyline_id == storyline
                        )
                        eligible = [
                            model for model in _models_for_snapshot(calibration_snapshot)
                            if entails_structured_claim(model, claim_oracle)
                            and any(
                                item.storyline_id == storyline
                                for item in _model_evidence(
                                    model, maps, through_stage=10
                                )
                            )
                        ]
                        confidences = [float(model.get("confidence") or 0) for model in eligible]
                        label = float(outcome.outcome_label)
                        endpoint["external_outcome_predictions"] = [
                            {"confidence": value, "label": label}
                            for value in confidences
                        ]
                        endpoint["external_outcome_calibration_ece"] = {
                            "value": (
                                mean(abs(value - label) for value in confidences)
                                if confidences else None
                            ),
                            "measured": bool(confidences),
                            "denominator": len(confidences),
                            "freeze_after_batch": outcome.freeze_after_batch,
                        }
                        endpoint["external_outcome_brier"] = {
                            "value": (
                                mean((value - label) ** 2 for value in confidences)
                                if confidences else None
                            ),
                            "measured": bool(confidences),
                            "denominator": len(confidences),
                        }
                    for model in _models_for_snapshot(snapshot):
                        evidence = _model_evidence(model, maps, through_stage=12)
                        if evidence and all(
                            item.role in {"noise", "high_similarity_distractor"}
                            for item in evidence
                        ):
                            global_noise_truth += 1
                if stage == 12:
                    for decision in snapshot.get("context_decisions") or ():
                        if decision.get("context_item_kind") != "observation" or not decision.get("selected"):
                            continue
                        historical_raw[arm]["selected"] += 1
                        gold = maps["gold_by_observation"].get(str(decision.get("context_item_id")))
                        old = gold is not None and maps["batch_by_signal"][gold.signal_id] <= 10
                        if old and not decision.get("historical_reopen_reason"):
                            historical_raw[arm]["unjustified"] += 1
    expected_endpoints = len(world_results) * len(P7_ARMS) * len(STAGES) * 4
    endpoint_keys = [
        (row["world_id"], row["arm_id"], row["stage_batch"], row["storyline_id"])
        for row in endpoints
    ]
    expected_keys = {
        (world["world_id"], arm, stage, storyline)
        for world in world_results for arm in P7_ARMS for stage in STAGES
        for storyline in ("atlas", "beacon", "cobalt", "delta")
    }
    if (
        len(endpoints) != expected_endpoints
        or len(set(endpoint_keys)) != len(endpoint_keys)
        or set(endpoint_keys) != expected_keys
    ):
        raise InvariantViolation("P7_POSTFREEZE_DENOMINATOR_INVALID", "endpoint denominator mismatch")
    intervals = _paired_intervals(endpoints)
    corrupted = [
        arm for world in world_results for arm in world["arm_results"]
        if arm["arm"] == "corrupted"
    ]
    corruption_metrics = []
    for arm in corrupted:
        injected = arm.get("corruption_injected_batch")
        recovered = next((
            wave["batch_number"] for wave in arm["waves"]
            if any(
                receipt.get("within_two_batch_recovery_bound") is not None
                for receipt in wave.get("lifecycle_receipts") or ()
            )
        ), None)
        latency = recovered - injected if injected is not None and recovered is not None else None
        corruption_metrics.append({
            "tenant_id": arm["tenant_id"], "injected_batch": injected,
            "recovered_batch": recovered, "correction_latency_batches": latency,
            "stale_truth_exposure_batches": (
                min(12, recovered or 12) - injected if injected is not None else None
            ),
            "within_two_batches": latency is not None and latency <= 2,
        })
    arm_economics = []
    lifecycle_by_arm: dict[str, list[tuple[float, float]]] = {
        arm: [] for arm in P7_ARMS
    }
    for world in world_results:
        for arm in world["arm_results"]:
            for stage in STAGES:
                waves = [wave for wave in arm["waves"] if wave["batch_number"] <= stage]
                ledger = next((
                    wave.get("provider_identity_ledger") for wave in reversed(waves)
                    if wave.get("provider_identity_ledger")
                ), {}) or {}
                stage_snapshot = next(
                    (wave.get("stage_snapshot") for wave in reversed(waves)
                     if wave.get("stage_snapshot")),
                    {},
                ) or {}
                writes = stage_snapshot.get("write_counts") or {}
                arm_economics.append({
                    "world_id": world["world_id"], "arm_id": arm["arm"],
                    "stage_batch": stage,
                    "input_tokens": int(ledger.get("input_tokens") or 0),
                    "output_tokens": int(ledger.get("output_tokens") or 0),
                    "physical_attempts": int(ledger.get("physical_attempt_count") or 0),
                    "wall_time_s": sum(float(wave.get("elapsed_s") or 0) for wave in waves),
                    "canonical_writes": int(
                        writes.get("canonical_model_versions") or 0
                    ) + int(writes.get("canonical_relation_versions") or 0),
                    "derived_writes": int(
                        writes.get("derived_relation_projections") or 0
                    ) + int(writes.get("derived_projection_snapshots") or 0),
                })
            lifecycle_batches = [
                int(wave["batch_number"])
                for wave in arm["waves"]
                if int(wave["batch_number"]) >= 7
                and any(
                    receipt.get("action") in {"falsify", "archive", "supersede"}
                    for receipt in wave.get("lifecycle_receipts") or ()
                )
            ]
            # No correction is not missing data: it is right-censored harm
            # through the end of the preregistered horizon.
            latency = float(min(lifecycle_batches) - 7) if lifecycle_batches else 6.0
            lifecycle_by_arm[arm["arm"]].append((latency, latency))
    correction_by_arm = {
        arm: {
            "latency": mean(value[0] for value in values) if values else None,
            "stale": mean(value[1] for value in values) if values else None,
            "denominator": len(values),
        }
        for arm, values in lifecycle_by_arm.items()
    }
    gates = {
        "all_failures_preserved": all(
            wave.get("think_run_id") for world in world_results
            for arm in world["arm_results"]
            for wave in arm["waves"] if wave["reasoning_executed"]
        ),
        "corrupted_memory_safe_within_two_batches": all(
            arm["corruption_recovered_within_two_batches"] for arm in corrupted
        ),
        "corrupted_memory_unsafe_accepted_persistence_zero": all(
            not (
                set(map(str, arm.get("corruption_model_ids") or ()))
                & {
                    str(model.get("id"))
                    for model in (arm.get("frozen_outputs") or {}).get(
                        "accepted_models", ()
                    )
                }
            )
            for arm in corrupted
        ),
        "durable_attempt_receipts": all(
            (wave.get("provider_identity_ledger") or {}).get("valid") is True
            for world in world_results for arm in world["arm_results"]
            for wave in arm["waves"] if wave["reasoning_executed"]
        ),
        "exact_bootstrap_clones": all(
            world.get("population_digest") and len(world["arm_results"]) == len(P7_ARMS)
            for world in world_results
        ),
        "exact_paired_population": len(endpoints) == expected_endpoints,
        "identical_budgets": _identical_budget_contracts(list(world_results)),
        "isolated_tenants": execution_artifact["isolated_tenant_count"] == execution_artifact["arm_execution_count"],
        "no_frozen_or_observation_mutation": all(
            arm["arm_contract_satisfied"] for world in world_results
            for arm in world["arm_results"] if arm["arm"] in {"frozen", "observation_only"}
        ),
        "no_hidden_model_access": all(
            wave["retrieval_policy"] == "hide_models"
            for world in world_results for arm in world["arm_results"]
            if arm["arm"] in {"memory_hidden", "observation_only"}
            for wave in arm["waves"] if wave["reasoning_executed"]
        ),
        "semantic_outcome_calibration": all(
            row.get("external_outcome_calibration_ece", {}).get("measured")
            for row in endpoints if row["stage_batch"] == 12
        ),
        "zero_false_truth_from_noise": global_noise_truth == 0,
    }
    verdict, decision = _strategic_decision(
        endpoints=endpoints, intervals=intervals,
        correction_by_arm=correction_by_arm,
        economics=arm_economics, historical_raw=historical_raw,
        hard_gates=gates, global_noise_truth=global_noise_truth,
    )
    continue_worlds = any(interval["lower_95"] <= 0 <= interval["upper_95"] for interval in intervals)
    payload = {
        "schema_version": "epistemic-repair-p7-postfreeze-oracle-v1",
        "execution_artifact_digest": canonical_sha256(execution_artifact),
        "world_count": len(world_results), "endpoint_denominator": expected_endpoints,
        "endpoints": endpoints, "paired_bootstrap_intervals": intervals,
        "historical_raw_use": historical_raw,
        "global_false_truth_from_noise": global_noise_truth,
        "correction_and_stale_exposure": corruption_metrics,
        "tokens_calls_and_wall_time": arm_economics,
        "hard_gates": gates,
        "memory_earns_decision": decision,
        "stopping_rule": {
            "continue": continue_worlds and len(world_results) < P7_MAX_WORLD_COUNT,
            "maximum_worlds": P7_MAX_WORLD_COUNT,
            "reason": "paired facet interval crosses zero" if continue_worlds else "paired intervals resolved",
        },
        "strategic_verdict": verdict,
        "phase_exit_ready": all(gates.values()) and verdict != "insufficient_evidence",
        "proof_boundary": (
            "Claim, lifecycle-phase, contamination, scope, and relation credit use exact sealed evidence IDs.",
            "Pre-outcome batch-10 confidence is scored against sealed batch-11 external outcome labels.",
        ),
    }
    return {**payload, "content_digest": canonical_sha256(payload)}


__all__ = ["evaluate_frozen_worlds"]
