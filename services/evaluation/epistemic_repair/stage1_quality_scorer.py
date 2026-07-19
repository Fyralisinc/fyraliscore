"""Independent post-execution quality scoring for the Stage 1 memory loop.

The production runner never imports this module.  It consumes only an already
frozen worker artifact and evaluator-owned population/gold objects.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from lib.contracts.kernel import canonical_sha256


SCHEMA_VERSION = "stage1-company-memory-quality-v1"


def _items(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _metric(numerator: int, denominator: int, *, lower_is_better: bool = False) -> dict[str, Any]:
    value = _ratio(numerator, denominator)
    if lower_is_better and value is not None:
        value = 1.0 - value
    return {
        "value": round(value, 6) if value is not None else None,
        "numerator": numerator,
        "denominator": denominator,
        "status": "measured" if value is not None else "unmeasured",
    }


def _evidence_ids(model: Mapping[str, Any]) -> tuple[str, ...]:
    proposition = model.get("proposition")
    if not isinstance(proposition, Mapping):
        return ()
    return tuple(str(value) for value in proposition.get("evidence_event_ids") or ())


def score_stage1_company_memory(
    raw_execution: Mapping[str, Any],
    *,
    signals: Sequence[Any],
    expected_scope_by_signal: Mapping[str, str | None],
    expected_claim_signal_ids: frozenset[str],
    synthesis_signal_id: str | None = None,
    expected_synthesis: str | None = None,
    correction_signal_id: str | None = None,
    expected_correction: str | None = None,
) -> dict[str, Any]:
    """Score a frozen Stage 1 run without consulting runtime state or an LLM."""

    tenant_id = UUID(str(raw_execution["tenant_id"]))
    observed_batches = int(raw_execution.get("completed_batches") or 0)
    observed_signals = [
        signal for signal in signals
        if int(signal.batch_number) <= observed_batches
    ]
    signal_by_observation = {
        str(uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}")): signal
        for signal in observed_signals
    }
    signal_by_id = {signal.signal_id: signal for signal in observed_signals}
    expected_claims = expected_claim_signal_ids & signal_by_id.keys()

    frozen = raw_execution.get("frozen_outputs") or {}
    models = _items(frozen.get("accepted_models") if isinstance(frozen, Mapping) else ())
    exact_claim_counts: Counter[str] = Counter()
    canonical_scope_correct = 0
    canonically_scoped_signals: set[str] = set()
    canonical_scope_predicted = 0
    canonical_scope_expected = sum(
        expected_scope_by_signal.get(signal_id) is not None
        for signal_id in expected_claims
    )
    evidence_bound_models = 0

    for model in models:
        evidence = _evidence_ids(model)
        if len(evidence) != 1 or evidence[0] not in signal_by_observation:
            continue
        signal = signal_by_observation[evidence[0]]
        if signal.signal_id not in expected_claims:
            continue
        evidence_bound_models += 1
        proposition = model.get("proposition") or {}
        if not isinstance(proposition, Mapping):
            continue
        if (
            proposition.get("abstraction_level") == "atomic"
            and str(model.get("natural_text") or "").strip() == signal.text.strip()
        ):
            exact_claim_counts[signal.signal_id] += 1
        expected_scope = expected_scope_by_signal.get(signal.signal_id)
        scope = proposition.get("scope_ref")
        if scope and not str(scope).startswith("mention:"):
            canonical_scope_predicted += 1
            if expected_scope is not None and scope == expected_scope:
                canonical_scope_correct += 1
                canonically_scoped_signals.add(signal.signal_id)

    exact_claim_correct = sum(1 for count in exact_claim_counts.values() if count >= 1)
    exact_claim_predictions = sum(exact_claim_counts.values())
    duplicate_exact_claims = sum(max(0, count - 1) for count in exact_claim_counts.values())

    waves = _items(raw_execution.get("waves"))
    prior_model_used = any(
        bool(
            (((wave.get("execution") or {}).get("run") or {}).get("ops_applied") or {})
            .get("context_use", {}).get("model_context_used")
        )
        for wave in waves[1:]
    )

    def matching_models(text: str | None) -> list[Mapping[str, Any]]:
        if not text:
            return []
        return [model for model in models if model.get("natural_text") == text]

    historical_models = [
        model
        for wave in waves
        for model in _items((wave.get("snapshot") or {}).get("accepted_models"))
    ]
    synthesis_models = [
        model for model in historical_models
        if model.get("natural_text") == expected_synthesis
    ]
    synthesis_observed = synthesis_signal_id in signal_by_id if synthesis_signal_id else False
    synthesis_ok = any(
        (model.get("proposition") or {}).get("abstraction_level") == "composite"
        for model in synthesis_models
    ) if synthesis_observed else None

    correction_models = matching_models(expected_correction)
    correction_observed = correction_signal_id in signal_by_id if correction_signal_id else False
    correction_ok = any(
        int(model.get("truth_version") or 0) > 1
        and (model.get("proposition") or {}).get("abstraction_level") == "composite"
        and (model.get("proposition") or {}).get("lifecycle_phase") == "correction"
        for model in correction_models
    ) if correction_observed else None

    metrics = {
        "exact_claim_precision": _metric(exact_claim_correct, exact_claim_predictions),
        "exact_claim_recall": _metric(exact_claim_correct, len(expected_claims)),
        "evidence_bound_model_precision": _metric(evidence_bound_models, len(models)),
        "canonical_scope_precision": _metric(canonical_scope_correct, canonical_scope_predicted),
        "canonical_scope_recall": _metric(
            len(canonically_scoped_signals), canonical_scope_expected,
        ),
        "duplicate_exact_claim_avoidance": _metric(
            duplicate_exact_claims, max(1, exact_claim_predictions), lower_is_better=True,
        ),
        "prior_model_use": {
            "value": 1.0 if prior_model_used else 0.0,
            "status": "measured" if observed_batches >= 2 else "unmeasured",
        },
        "synthesis_accuracy": {
            "value": (1.0 if synthesis_ok else 0.0) if synthesis_ok is not None else None,
            "status": "measured" if synthesis_ok is not None else "unmeasured",
        },
        "correction_in_place_accuracy": {
            "value": (1.0 if correction_ok else 0.0) if correction_ok is not None else None,
            "status": "measured" if correction_ok is not None else "unmeasured",
        },
    }
    measured = [row["value"] for row in metrics.values() if row["value"] is not None]
    body = {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": str(tenant_id),
        "population_digest": raw_execution.get("population_digest"),
        "completed_batches": observed_batches,
        "model_count": len(models),
        "metrics": metrics,
        "minimum_measured_score": min(measured) if measured else None,
        "all_dimensions_measured": all(row["status"] == "measured" for row in metrics.values()),
    }
    return {**body, "artifact_digest": canonical_sha256(body)}


__all__ = ["SCHEMA_VERSION", "score_stage1_company_memory"]
