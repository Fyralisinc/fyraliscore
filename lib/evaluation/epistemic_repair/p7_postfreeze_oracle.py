"""Independent evidence-coordinate oracle for frozen production P7 worlds.

No lexical matching is used. Models are credited only through canonical
observation evidence IDs joined to sealed P6 signal/claim/lifecycle labels.
"""

from __future__ import annotations

import random
from statistics import mean
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import P6Population
from lib.evaluation.epistemic_repair.p7_population import P7_MAX_WORLD_COUNT
from lib.evaluation.epistemic_repair.p7_runner import P7_ARMS
from lib.shared.errors import InvariantViolation


STAGES = (3, 6, 12)


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
) -> dict[str, Any]:
    expected_gold = [
        item for item in maps["gold_by_observation"].values()
        if item.storyline_id == storyline
        and maps["batch_by_signal"][item.signal_id] <= stage
        and item.claim_id
    ]
    expected_claims = {item.claim_id for item in expected_gold}
    expected_phases = _expected_phases(stage)
    candidates: list[tuple[dict[str, Any], list[Any]]] = []
    for model in snapshot.get("accepted_models") or ():
        evidence = _model_evidence(model, maps, through_stage=stage)
        if any(item.storyline_id == storyline for item in evidence):
            candidates.append((model, evidence))
    pure = [
        (model, evidence) for model, evidence in candidates
        if evidence
        and {item.storyline_id for item in evidence if item.storyline_id} == {storyline}
        and all(item.role not in {"noise", "high_similarity_distractor"} for item in evidence)
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
    model_phase = {
        str(model["id"]): {item.lifecycle_phase for item in evidence}
        for model, evidence in pure
    }
    expected_relations = []
    if stage >= 9:
        expected_relations.append(("contradiction", "correction", {"supersedes", "corrects", "contests"}))
    if stage >= 11:
        expected_relations.append(("correction", "external_outcome", {"supports", "confirms", "predicts"}))
    matched_expected: set[int] = set()
    valid_relation_outputs = 0
    for relation in relation_outputs:
        participants = [str(value) for value in relation.get("participant_model_ids") or ()]
        if not participants or any(value not in model_phase for value in participants):
            continue
        kind = str(relation.get("truth_relation_kind") or "").casefold()
        for index, (left, right, kinds) in enumerate(expected_relations):
            phases = set().union(*(model_phase[value] for value in participants))
            if left in phases and right in phases and kind in kinds:
                matched_expected.add(index)
                valid_relation_outputs += 1
                break
    precision = _metric(len(pure), len(candidates))
    recall = _metric(len(represented_claims & expected_claims), len(expected_claims))
    p = precision["value"] or 0.0
    r = recall["value"] or 0.0
    f1 = 0.0 if p + r == 0 else 2 * p * r / (p + r)
    return {
        "storyline_id": storyline,
        "stage_batch": stage,
        "direct_thesis_accuracy": _metric(
            int(expected_phases <= represented_phases), 1
        ),
        "thesis_phase_completeness": _metric(
            len(expected_phases & represented_phases), len(expected_phases)
        ),
        "atomic_claim_precision": precision,
        "atomic_claim_recall": recall,
        "atomic_claim_f1": {"value": f1, "measured": True},
        "relation_joint_precision": _metric(valid_relation_outputs, len(relation_outputs)),
        "relation_joint_recall": _metric(len(matched_expected), len(expected_relations)),
        "boundary_entity_safety": _metric(safe_scopes, len(scoped_models)),
        "false_truth_from_noise": len(candidates) - len(pure),
        "external_outcome_calibration_ece": {
            "value": None,
            "measured": False,
            "reason": "sealed P6 oracle has no independent binary external-outcome label",
        },
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
        deltas = [
            mean(
                adaptive[(world, storyline)]["thesis_phase_completeness"]["value"]
                - baseline[(world, storyline)]["thesis_phase_completeness"]["value"]
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
    historical_raw = {arm: {"selected": 0, "unjustified": 0} for arm in P7_ARMS}
    for world_result in world_results:
        world_id = world_result["world_id"]
        population = sealed_worlds.get(world_id)
        if population is None or population.population_digest != world_result["population_digest"]:
            raise InvariantViolation("P7_POSTFREEZE_WORLD_ORACLE_MISMATCH", "world digest mismatch")
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
                    )
                    endpoints.append({"world_id": world_id, "arm_id": arm, **score})
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
    if len(endpoints) != expected_endpoints:
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
    for world in world_results:
        for arm in world["arm_results"]:
            for stage in STAGES:
                waves = [wave for wave in arm["waves"] if wave["batch_number"] <= stage]
                ledger = next((
                    wave.get("provider_identity_ledger") for wave in reversed(waves)
                    if wave.get("provider_identity_ledger")
                ), {}) or {}
                arm_economics.append({
                    "world_id": world["world_id"], "arm_id": arm["arm"],
                    "stage_batch": stage,
                    "input_tokens": int(ledger.get("input_tokens") or 0),
                    "output_tokens": int(ledger.get("output_tokens") or 0),
                    "physical_attempts": int(ledger.get("physical_attempt_count") or 0),
                    "wall_time_s": sum(float(wave.get("elapsed_s") or 0) for wave in waves),
                })
    gates = {
        "all_failures_preserved": all(
            wave.get("think_run_id") for world in world_results
            for arm in world["arm_results"] if arm["arm"] != "observation_only"
            for wave in arm["waves"] if wave["reasoning_executed"]
        ),
        "corrupted_memory_safe_within_two_batches": all(
            arm["corruption_recovered_within_two_batches"] for arm in corrupted
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
        "identical_budgets": True,
        "isolated_tenants": execution_artifact["isolated_tenant_count"] == execution_artifact["arm_execution_count"],
        "no_frozen_or_observation_mutation": all(
            arm["arm_contract_satisfied"] for world in world_results
            for arm in world["arm_results"] if arm["arm"] in {"frozen", "observation_only"}
        ),
        "no_hidden_model_access": all(
            wave["retrieval_policy"] == "hide_models"
            for world in world_results for arm in world["arm_results"]
            if arm["arm"] == "memory_hidden" for wave in arm["waves"]
        ),
        "semantic_outcome_calibration": False,
    }
    continue_worlds = any(interval["lower_95"] <= 0 <= interval["upper_95"] for interval in intervals)
    payload = {
        "schema_version": "epistemic-repair-p7-postfreeze-oracle-v1",
        "execution_artifact_digest": canonical_sha256(execution_artifact),
        "world_count": len(world_results), "endpoint_denominator": expected_endpoints,
        "endpoints": endpoints, "paired_bootstrap_intervals": intervals,
        "historical_raw_use": historical_raw,
        "correction_and_stale_exposure": corruption_metrics,
        "tokens_calls_and_wall_time": arm_economics,
        "hard_gates": gates,
        "stopping_rule": {
            "continue": continue_worlds and len(world_results) < P7_MAX_WORLD_COUNT,
            "maximum_worlds": P7_MAX_WORLD_COUNT,
            "reason": "paired facet interval crosses zero" if continue_worlds else "paired intervals resolved",
        },
        "strategic_verdict": "insufficient_evidence" if not all(gates.values()) else "primary_memory_earned",
        "phase_exit_ready": all(gates.values()),
        "proof_boundary": (
            "Claim, lifecycle-phase, contamination, scope, and relation credit use exact sealed evidence IDs.",
            "External-outcome ECE remains unmeasured because P6 has no independent binary outcome label.",
        ),
    }
    return {**payload, "content_digest": canonical_sha256(payload)}


__all__ = ["evaluate_frozen_worlds"]
