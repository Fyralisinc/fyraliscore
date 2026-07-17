"""Objective, preregistered proof of retrieval evolution across learning batches."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence


JsonObject = Mapping[str, Any]


@dataclass(frozen=True)
class RetrievalEvolutionPolicy:
    schema_version: str = "retrieval-evolution-policy-v1"
    minimum_batches_per_phase: int = 3
    minimum_early_observation_share: float = 0.55
    minimum_late_model_share: float = 0.60
    minimum_late_model_reference_share: float = 0.60
    minimum_model_share_gain: float = 0.15
    minimum_late_reference_coverage: float = 0.80
    minimum_late_raw_reason_coverage: float = 0.80


def evaluate_retrieval_evolution(
    batches: Sequence[JsonObject],
    *,
    policy: RetrievalEvolutionPolicy | None = None,
) -> dict[str, Any]:
    """Measure whether retrieval moves from raw evidence to learned memory.

    A batch is measured only from persisted retrieval/context-use telemetry. A
    Model merely selected is not treated as used: it must occur in the runtime's
    referenced Model IDs. Late Observation use is allowed when the runtime
    records a reopening reason (uncertainty, contradiction, correction, or
    provenance).
    """
    policy = policy or RetrievalEvolutionPolicy()
    rows = [_normalize_batch(batch, index + 1) for index, batch in enumerate(batches)]
    cut1 = len(rows) // 3
    cut2 = (2 * len(rows)) // 3
    phases = {"early": rows[:cut1], "middle": rows[cut1:cut2], "late": rows[cut2:]}

    early_observation_share = _share(phases["early"], "selected_observations")
    late_model_share = _share(phases["late"], "selected_models")
    model_share_gain = (
        None
        if early_observation_share is None or late_model_share is None
        else round(late_model_share - (1.0 - early_observation_share), 6)
    )
    late_with_selection = [row for row in phases["late"] if row["selected_total"]]
    late_model_reference_share = _reference_share(phases["late"], "referenced_models")
    late_reference_coverage = _ratio(
        sum(1 for row in late_with_selection if row["referenced_total"] > 0),
        len(late_with_selection),
    )
    late_raw = [
        row
        for row in phases["late"]
        if row["selected_historical_observations"] > 0
        and row["referenced_observations"] > 0
    ]
    late_raw_reason_coverage = (
        None
        if not phases["late"]
        else 1.0
        if not late_raw
        else _ratio(
            sum(1 for row in late_raw if row["raw_reopening_reasons"]),
            len(late_raw),
        )
    )

    measurements = {
        "early_observation_selection_share": early_observation_share,
        "late_model_selection_share": late_model_share,
        "late_model_reference_share": late_model_reference_share,
        "model_selection_share_gain": model_share_gain,
        "late_selected_context_reference_coverage": late_reference_coverage,
        "late_raw_observation_reason_coverage": late_raw_reason_coverage,
    }
    checks = {
        "enough_batches_per_phase": all(
            len(phase) >= policy.minimum_batches_per_phase for phase in phases.values()
        ),
        "early_is_observation_heavy": _at_least(
            early_observation_share, policy.minimum_early_observation_share
        ),
        "late_is_model_preferred": _at_least(
            late_model_share, policy.minimum_late_model_share
        ),
        "late_actual_use_is_model_preferred": _at_least(
            late_model_reference_share, policy.minimum_late_model_reference_share
        ),
        "model_reliance_increases": _at_least(
            model_share_gain, policy.minimum_model_share_gain
        ),
        "late_selected_context_is_actually_referenced": _at_least(
            late_reference_coverage, policy.minimum_late_reference_coverage
        ),
        "late_raw_reopening_is_justified": _at_least(
            late_raw_reason_coverage, policy.minimum_late_raw_reason_coverage
        ),
    }
    measured_checks = [value for value in checks.values() if value is not None]
    score = _ratio(sum(bool(value) for value in measured_checks), len(checks))
    return {
        "schema_version": "retrieval-evolution-evaluation-v1",
        "policy": asdict(policy),
        "batch_count": len(rows),
        "phase_batch_counts": {name: len(phase) for name, phase in phases.items()},
        "measurements": measurements,
        "checks": checks,
        "continuous_score": score,
        "verdict": (
            "meets_preregistered_policy"
            if checks and all(checks.values())
            else "below_policy"
        ),
        "rows": rows,
    }


def _normalize_batch(batch: JsonObject, sequence: int) -> dict[str, Any]:
    context = batch.get("context_use")
    if not isinstance(context, Mapping):
        ops = batch.get("ops_applied")
        context = ops.get("context_use") if isinstance(ops, Mapping) else {}
    context = context if isinstance(context, Mapping) else {}
    selected_models = _count(
        context, "selected_model_ids", batch, "retrieval_model_count"
    )
    selected_observations = _count(
        context, "selected_observation_ids", batch, "retrieval_observation_count"
    )
    referenced_models = len(_ids(context.get("referenced_model_ids")))
    referenced_observations = len(_ids(context.get("referenced_observation_ids")))
    selected_historical = max(
        0, int(context.get("selected_historical_observation_count") or 0)
    )
    reasons = context.get("raw_observation_reopening_reasons") or []
    return {
        "sequence": batch.get("sequence", sequence),
        "selected_models": selected_models,
        "selected_observations": selected_observations,
        "selected_total": selected_models + selected_observations,
        "referenced_models": referenced_models,
        "referenced_observations": referenced_observations,
        "referenced_total": referenced_models + referenced_observations,
        "selected_historical_observations": selected_historical,
        "raw_reopening_reasons": (
            [str(reason) for reason in reasons]
            if isinstance(reasons, list)
            else []
        ),
    }


def _count(context: JsonObject, ids_key: str, batch: JsonObject, fallback: str) -> int:
    ids = _ids(context.get(ids_key))
    return len(ids) if ids else max(0, int(batch.get(fallback) or 0))


def _ids(value: Any) -> set[str]:
    return {str(item) for item in value} if isinstance(value, list) else set()


def _share(rows: Sequence[JsonObject], numerator: str) -> float | None:
    total = sum(int(row["selected_total"]) for row in rows)
    return None if not total else sum(int(row[numerator]) for row in rows) / total


def _reference_share(rows: Sequence[JsonObject], numerator: str) -> float | None:
    total = sum(int(row["referenced_total"]) for row in rows)
    return None if not total else sum(int(row[numerator]) for row in rows) / total


def _ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


def _at_least(value: float | None, threshold: float) -> bool | None:
    return None if value is None else value >= threshold
