"""Fail-closed evaluation for bounded company-model learning ablations.

The producer and judge artifacts are deliberately separate.  The evaluator
does not infer hidden truth from a model's prose and does not count selected
context as used.  Both arms must have seen the exact same genuine signal
batches; the only permitted difference is whether previously learned company
memory was available to later batches.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Mapping, Sequence


JsonObject = Mapping[str, Any]


@dataclass(frozen=True)
class CompanyModelAblationPolicy:
    schema_version: str = "company-model-ablation-policy-v1"
    minimum_batches: int = 3
    minimum_signals_per_batch: int = 2
    minimum_hidden_theses: int = 3
    minimum_calibration_samples: int = 12
    minimum_recovery_lift: float = 0.15
    minimum_learned_recovery: float = 0.70
    maximum_learned_ece: float = 0.20
    maximum_ece_regression: float = 0.05


def evaluate_company_model_ablation(
    *,
    manifest: JsonObject,
    learned: JsonObject,
    frozen: JsonObject,
    policy: CompanyModelAblationPolicy | None = None,
) -> dict[str, Any]:
    """Compare learned-memory and frozen-memory arms on sealed hidden truth."""
    policy = policy or CompanyModelAblationPolicy()
    _require_schema(manifest, "company-model-hidden-truth-v1")
    _require_schema(learned, "company-model-ablation-arm-v1")
    _require_schema(frozen, "company-model-ablation-arm-v1")
    if learned.get("arm") != "learned_memory":
        raise ValueError("learned arm must be named learned_memory")
    if frozen.get("arm") != "frozen_memory":
        raise ValueError("frozen arm must be named frozen_memory")

    truth = _truth(manifest)
    truth_ids = set(truth)
    manifest_digest = _digest(manifest)
    for arm in (learned, frozen):
        if arm.get("hidden_truth_digest") != manifest_digest:
            raise ValueError("arm is not bound to the exact hidden-truth manifest")
        if arm.get("producer_id") == manifest.get("judge_id"):
            raise ValueError("hidden-truth judge must be independent of arm producer")
        if arm.get("truth_visible_to_producer") is not False:
            raise ValueError("producer must attest hidden truth was not visible")

    learned_batches = _batches(learned)
    frozen_batches = _batches(frozen)
    if learned_batches != frozen_batches:
        raise ValueError("ablation arms must expose the exact same ordered batches")
    flat = [signal for batch in learned_batches for signal in batch]
    if len(flat) != len(set(flat)):
        raise ValueError("a signal may occur in exactly one genuine batch")

    learned_rows = _predictions(learned, truth_ids)
    frozen_rows = _predictions(frozen, truth_ids)
    learned_metrics = _arm_metrics(learned_rows)
    frozen_metrics = _arm_metrics(frozen_rows)
    recovery_lift = learned_metrics["recovery_rate"] - frozen_metrics["recovery_rate"]
    ece_delta = learned_metrics["expected_calibration_error"] - frozen_metrics[
        "expected_calibration_error"
    ]
    checks = {
        "enough_genuine_batches": len(learned_batches) >= policy.minimum_batches,
        "batch_size_is_genuine": all(
            len(batch) >= policy.minimum_signals_per_batch for batch in learned_batches
        ),
        "enough_hidden_theses": len(truth_ids) >= policy.minimum_hidden_theses,
        "complete_thesis_denominator": (
            set(learned_rows) == truth_ids == set(frozen_rows)
        ),
        "enough_calibration_samples": min(
            learned_metrics["calibration_n"], frozen_metrics["calibration_n"]
        )
        >= policy.minimum_calibration_samples,
        "learned_recovery_meets_floor": (
            learned_metrics["recovery_rate"] >= policy.minimum_learned_recovery
        ),
        "learning_has_causal_direction": recovery_lift >= policy.minimum_recovery_lift,
        "learned_calibration_meets_budget": (
            learned_metrics["expected_calibration_error"]
            <= policy.maximum_learned_ece
        ),
        "learning_does_not_buy_accuracy_with_miscalibration": (
            ece_delta <= policy.maximum_ece_regression
        ),
        "no_learned_safety_incident": not list(learned.get("safety_incidents") or []),
    }
    return {
        "schema_version": "company-model-ablation-evaluation-v1",
        "policy": asdict(policy),
        "manifest_digest": manifest_digest,
        "experiment_id": manifest.get("experiment_id"),
        "batch_count": len(learned_batches),
        "signal_count": len(flat),
        "hidden_thesis_count": len(truth_ids),
        "arms": {"learned_memory": learned_metrics, "frozen_memory": frozen_metrics},
        "effects": {
            "recovery_rate_lift": round(recovery_lift, 6),
            "ece_delta": round(ece_delta, 6),
            "direction": (
                "learned_better" if recovery_lift > 0 else
                "frozen_better" if recovery_lift < 0 else "no_difference"
            ),
        },
        "checks": checks,
        "continuous_score": sum(checks.values()) / len(checks),
        "verdict": "meets_policy" if all(checks.values()) else "below_policy",
        "proof_boundary": [
            "bounded simulated E4 company-world evidence",
            "not open-world or customer-value evidence",
            "does not test connector or listener transport",
            "does not authorize autonomous task execution",
        ],
    }


def manifest_digest(manifest: JsonObject) -> str:
    """Public helper used by arm producers before the judge sees predictions."""
    _require_schema(manifest, "company-model-hidden-truth-v1")
    return _digest(manifest)


def evaluate_single_model_synthesis(
    *, manifest: JsonObject, learned: JsonObject, frozen: JsonObject,
) -> dict[str, Any]:
    """Require one complete persisted synthesis Model with prior-Model lineage.

    This is intentionally stricter than collective facet availability: facets
    spread across multiple Models never count as synthesized recovery.
    """

    _require_schema(manifest, "company-model-synthesis-manifest-v1")
    truth = manifest.get("hidden_patterns")
    if not isinstance(truth, list) or not truth:
        raise ValueError("synthesis manifest requires hidden_patterns")
    thesis_ids = {str(row.get("thesis_id")) for row in truth if isinstance(row, Mapping)}
    if len(thesis_ids) != len(truth) or "None" in thesis_ids:
        raise ValueError("synthesis thesis ids must be unique and complete")
    results = {}
    for arm_name, arm in (("learned_memory", learned), ("frozen_memory", frozen)):
        _require_schema(arm, "company-model-synthesis-arm-v1")
        if arm.get("arm") != arm_name:
            raise ValueError(f"synthesis arm must be named {arm_name}")
        prior_ids = {str(value) for value in arm.get("prior_model_ids") or []}
        required_lineage = arm.get("required_lineage_by_thesis") or {}
        if not isinstance(required_lineage, Mapping):
            raise ValueError("required_lineage_by_thesis must be an object")
        models = arm.get("models")
        if not isinstance(models, list):
            raise ValueError("synthesis arm requires models")
        recovered = []
        rows = []
        for pattern in truth:
            thesis_id = str(pattern["thesis_id"])
            required = {str(value) for value in pattern.get("required_facets") or []}
            if not required:
                raise ValueError("each synthesis pattern requires facets")
            eligible = []
            for model in models:
                if not isinstance(model, Mapping) or str(model.get("thesis_id")) != thesis_id:
                    continue
                facets = {str(value) for value in model.get("facets") or []}
                lineage = {str(value) for value in model.get("evidence_model_ids") or []}
                complete = required <= facets
                expected_lineage = {
                    str(value) for value in required_lineage.get(thesis_id) or []
                }
                lineaged = (
                    bool(expected_lineage)
                    and lineage == expected_lineage
                    and lineage <= prior_ids
                )
                persisted = model.get("persisted") is True and bool(model.get("model_id"))
                if complete and lineaged and persisted:
                    eligible.append(model)
            is_recovered = len(eligible) == 1
            recovered.append(is_recovered)
            rows.append({"thesis_id": thesis_id, "recovered": is_recovered,
                "qualifying_model_count": len(eligible)})
        results[arm_name] = {"recovered_count": sum(recovered),
            "thesis_count": len(recovered), "recovery_rate": sum(recovered) / len(recovered),
            "theses": rows}
    learned_rate = results["learned_memory"]["recovery_rate"]
    frozen_rate = results["frozen_memory"]["recovery_rate"]
    checks = {"learned_has_single_complete_lineaged_model": learned_rate == 1.0,
        "frozen_has_no_single_complete_lineaged_model": frozen_rate == 0.0,
        "positive_synthesis_lift": learned_rate > frozen_rate}
    return {"schema_version": "single-model-synthesis-evaluation-v1",
        "capability": "single_persisted_cross_batch_pattern_synthesis",
        "arms": results, "synthesis_lift": learned_rate - frozen_rate,
        "checks": checks, "continuous_score": sum(checks.values()) / len(checks),
        "verdict": "meets_policy" if all(checks.values()) else "below_policy",
        "proof_boundary": "one complete persisted Model per thesis with prior-Model lineage"}


def _require_schema(value: JsonObject, expected: str) -> None:
    if value.get("schema_version") != expected:
        raise ValueError(f"expected {expected}")


def _digest(value: JsonObject) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _truth(manifest: JsonObject) -> dict[str, JsonObject]:
    rows = manifest.get("hidden_theses")
    if not isinstance(rows, list) or not rows:
        raise ValueError("hidden truth manifest requires hidden_theses")
    out: dict[str, JsonObject] = {}
    for row in rows:
        if not isinstance(row, Mapping) or not row.get("thesis_id"):
            raise ValueError("each hidden thesis requires thesis_id")
        key = str(row["thesis_id"])
        if key in out:
            raise ValueError("hidden thesis ids must be unique")
        out[key] = row
    return out


def _batches(arm: JsonObject) -> tuple[tuple[str, ...], ...]:
    rows = arm.get("batches")
    if not isinstance(rows, list) or not rows:
        raise ValueError("arm requires genuine batches")
    out: list[tuple[str, ...]] = []
    for row in rows:
        if not isinstance(row, Mapping) or not row.get("batch_id"):
            raise ValueError("each batch requires batch_id")
        signals = row.get("signal_ids")
        if not isinstance(signals, list) or not signals:
            raise ValueError("each batch requires signal_ids")
        out.append(tuple(str(item) for item in signals))
    return tuple(out)


def _predictions(arm: JsonObject, truth_ids: set[str]) -> dict[str, JsonObject]:
    rows = arm.get("predictions")
    if not isinstance(rows, list):
        raise ValueError("arm requires predictions")
    out: dict[str, JsonObject] = {}
    for row in rows:
        if not isinstance(row, Mapping) or not row.get("thesis_id"):
            raise ValueError("each prediction requires thesis_id")
        key = str(row["thesis_id"])
        if key in out or key not in truth_ids:
            raise ValueError("prediction ids must uniquely match sealed truth")
        confidence = float(row.get("confidence", -1))
        if not 0 <= confidence <= 1:
            raise ValueError("prediction confidence must be in [0, 1]")
        outcomes = row.get("future_outcomes")
        if not isinstance(outcomes, list) or any(value not in (0, 1, False, True) for value in outcomes):
            raise ValueError("future_outcomes must be binary independent judgments")
        out[key] = row
    if set(out) != truth_ids:
        raise ValueError("predictions must cover every sealed hidden thesis")
    return out


def _arm_metrics(rows: Mapping[str, JsonObject]) -> dict[str, Any]:
    recovered = [bool(row.get("recovered")) for row in rows.values()]
    samples = [
        (float(row["confidence"]), int(outcome))
        for row in rows.values()
        for outcome in row["future_outcomes"]
    ]
    ece = _ece(samples)
    brier = sum((confidence - outcome) ** 2 for confidence, outcome in samples) / len(samples)
    return {
        "recovered_count": sum(recovered),
        "thesis_count": len(recovered),
        "recovery_rate": sum(recovered) / len(recovered),
        "calibration_n": len(samples),
        "expected_calibration_error": round(ece, 6),
        "brier_score": round(brier, 6),
    }


def _ece(samples: Sequence[tuple[float, int]], bins: int = 10) -> float:
    if not samples:
        raise ValueError("calibration requires future outcomes")
    total = len(samples)
    error = 0.0
    for index in range(bins):
        low, high = index / bins, (index + 1) / bins
        bucket = [
            item for item in samples
            if low <= item[0] <= high and (index == bins - 1 or item[0] < high)
        ]
        if bucket:
            confidence = sum(item[0] for item in bucket) / len(bucket)
            accuracy = sum(item[1] for item in bucket) / len(bucket)
            error += len(bucket) / total * abs(confidence - accuracy)
    return error
