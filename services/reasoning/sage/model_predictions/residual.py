"""services.reasoning.sage.model_predictions.residual — pure residual helpers.

No DB calls live in this module. Callers (the residual sweeper, the
observation post-commit hook) fetch the inputs first and then call
into here for the decision + scoring.

Design notes
------------
* `detect_prediction_error` returns `None` when the observation
  satisfies the prediction (or is ambiguous in a way that should NOT
  flip the prediction into `falsified` yet). It returns a populated
  `ModelPredictionError` when the observation clearly contradicts the
  expectation — that result is what the caller hands to
  `ModelPredictionErrorsRepo.insert`.

* `score_residual_severity` measures how badly the observation
  violates the expectation, on [0, 1]. Currently a coarse heuristic
  driven by the structured `value_constraint` block; later phases can
  swap in a learned scorer without touching the call sites.

* `score_residual_impact` measures how much the failing Model
  matters, on [0, 1]. Combines Model centrality / hub-ness (read from
  the row passed in), confidence (high-confidence failures hurt
  more), and dependent count when available. Pure: caller pre-fetches
  the Model row + any structural feature row and merges them into a
  single dict here.
"""
from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

from services.reasoning.sage.model_predictions.types import (
    ExpectedObservation,
    ModelPrediction,
    ModelPredictionError,
)


# ---------------------------------------------------------------------
# Constraint comparators — used by both detect + severity.
# ---------------------------------------------------------------------

_NUMERIC_OPS = {"gt", "gte", "lt", "lte", "eq", "neq", "ne"}
_TEXT_OPS = {"eq", "neq", "ne", "in", "not_in", "contains", "not_contains"}


def _coerce_number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _check_value_constraint(
    constraint: Mapping[str, Any] | None,
    observed_value: Any,
) -> tuple[bool, float]:
    """Return `(satisfied, deviation)` for an observed value vs a
    constraint.

    `deviation` is on [0, 1] — 0 when the constraint is satisfied,
    higher when the violation is larger. For text comparators the
    deviation is binary (1.0 on mismatch).
    """
    if constraint is None:
        # No structured comparator → can't decide from this slot.
        # Treat as satisfied so coarse natural-language fallback is
        # the only signal that can flip the prediction.
        return True, 0.0

    op = str(constraint.get("op") or "").lower()
    target = constraint.get("value")

    if op in _NUMERIC_OPS:
        obs_num = _coerce_number(observed_value)
        tgt_num = _coerce_number(target)
        if obs_num is None or tgt_num is None:
            # Shape mismatch — treat as violation, modest severity so
            # the triage layer can decide.
            return False, 0.5
        if op == "gt":
            satisfied = obs_num > tgt_num
        elif op == "gte":
            satisfied = obs_num >= tgt_num
        elif op == "lt":
            satisfied = obs_num < tgt_num
        elif op == "lte":
            satisfied = obs_num <= tgt_num
        elif op == "eq":
            satisfied = obs_num == tgt_num
        else:  # neq / ne
            satisfied = obs_num != tgt_num
        if satisfied:
            return True, 0.0
        scale = max(abs(tgt_num), 1.0)
        deviation = min(1.0, abs(obs_num - tgt_num) / scale)
        # Clamp to a floor so we don't report a violation as 0
        # severity just because the gap is tiny relative to the scale.
        return False, max(deviation, 0.1)

    if op in _TEXT_OPS:
        if op in {"eq"}:
            return (observed_value == target, 0.0 if observed_value == target else 1.0)
        if op in {"neq", "ne"}:
            return (observed_value != target, 0.0 if observed_value != target else 1.0)
        if op == "in":
            ok = isinstance(target, (list, tuple, set)) and observed_value in target
            return (ok, 0.0 if ok else 1.0)
        if op == "not_in":
            ok = not (isinstance(target, (list, tuple, set)) and observed_value in target)
            return (ok, 0.0 if ok else 1.0)
        if op == "contains":
            try:
                ok = target in observed_value  # type: ignore[operator]
            except TypeError:
                ok = False
            return (ok, 0.0 if ok else 1.0)
        if op == "not_contains":
            try:
                ok = target not in observed_value  # type: ignore[operator]
            except TypeError:
                ok = True
            return (ok, 0.0 if ok else 1.0)

    # Unknown op → can't decide; don't fire a residual on it.
    return True, 0.0


def _observation_in_scope(
    expected: ExpectedObservation,
    observation_row: Mapping[str, Any] | None,
) -> bool:
    """Conservative scope match. If the prediction declares scope
    entities/actors and the observation declares none of them, we
    skip the residual rather than report a false positive.
    """
    if observation_row is None:
        return False
    obs_entities = observation_row.get("scope_entities") or []
    obs_actors = observation_row.get("scope_actors") or []

    if expected.scope_entities:
        expected_ids = {
            str(e.get("id")) for e in expected.scope_entities if isinstance(e, dict)
        }
        obs_ids = {
            str(e.get("id")) for e in obs_entities if isinstance(e, dict)
        }
        if expected_ids and not (expected_ids & obs_ids):
            return False

    if expected.scope_actors:
        expected_actor_ids = {str(a) for a in expected.scope_actors}
        obs_actor_ids = {str(a) for a in obs_actors}
        if expected_actor_ids and not (expected_actor_ids & obs_actor_ids):
            return False

    return True


# =====================================================================
# Public API
# =====================================================================


def detect_prediction_error(
    prediction: ModelPrediction,
    observation_row: Mapping[str, Any] | None,
    expected_observation: Mapping[str, Any] | None = None,
) -> ModelPredictionError | None:
    """Decide whether `observation_row` violates `prediction`.

    Parameters
    ----------
    prediction
        Live Pydantic `ModelPrediction` row. Only inspected — not
        mutated.
    observation_row
        The incoming observation as a flat dict (the same shape the
        observations repo returns). May be None when the caller is
        checking a time-bound prediction that has no observation yet
        (in which case this helper returns None — expiry is a
        repo-level transition, not a residual).
    expected_observation
        Optional override for the prediction's expected_observation
        payload. Lets callers re-test against an updated expectation
        without rebuilding the whole prediction row.

    Returns
    -------
    ModelPredictionError | None
        A populated (but un-inserted) error row when the observation
        contradicts the expectation; None otherwise. The caller is
        responsible for inserting via
        `ModelPredictionErrorsRepo.insert`.
    """
    if observation_row is None:
        return None
    if prediction.status != "active":
        return None

    expected = prediction.expected_observation
    if expected_observation is not None:
        # Build a fresh ExpectedObservation, tolerating extension fields
        # by routing unknowns into `extras`.
        declared_keys = {
            "kind",
            "scope_entities",
            "scope_actors",
            "value_constraint",
            "falsification_rule",
        }
        declared = {
            k: expected_observation[k]
            for k in declared_keys
            if k in expected_observation
        }
        extras = {
            k: v for k, v in expected_observation.items() if k not in declared_keys
        }
        declared.setdefault("kind", expected.kind)
        declared["extras"] = extras
        expected = ExpectedObservation.model_validate(declared)

    if not _observation_in_scope(expected, observation_row):
        return None

    # Pull the observed value the constraint targets. Convention:
    # constraint may name the field via `field`; otherwise we fall back
    # to common observation keys.
    constraint = expected.value_constraint or None
    observed_value: Any = None
    if constraint is not None:
        field_name = constraint.get("field")
        if field_name and field_name in observation_row:
            observed_value = observation_row[field_name]
        else:
            for candidate in ("value", "delta", "state", "outcome", "payload"):
                if candidate in observation_row:
                    observed_value = observation_row[candidate]
                    break

    satisfied, _deviation = _check_value_constraint(constraint, observed_value)
    if satisfied:
        return None

    severity = score_residual_severity(prediction, observation_row)
    return ModelPredictionError(
        tenant_id=prediction.tenant_id,
        model_id=prediction.model_id,
        prediction_id=prediction.id,
        observed_signal_id=_observation_id(observation_row),
        error_summary=_summarize_error(prediction, observation_row, observed_value),
        severity=severity,
        # `impact_score` requires the Model row; callers that didn't
        # supply one get a confidence-derived placeholder. Use
        # `score_residual_impact` to refresh it before insert.
        impact_score=_default_impact_from_confidence(prediction.confidence),
        status="open",
    )


def score_residual_severity(
    prediction: ModelPrediction,
    observation_row: Mapping[str, Any] | None,
) -> float:
    """How badly does this observation violate the expectation?

    Combines:
      * value_constraint deviation (when present),
      * a small boost when the prediction's confidence was high
        (a high-confidence prediction failing is a sharper signal).
    Clamped to [0, 1].
    """
    if observation_row is None:
        return 0.0

    expected = prediction.expected_observation
    constraint = expected.value_constraint or None
    observed_value: Any = None
    if constraint is not None:
        field_name = constraint.get("field")
        if field_name and field_name in observation_row:
            observed_value = observation_row[field_name]
        else:
            for candidate in ("value", "delta", "state", "outcome", "payload"):
                if candidate in observation_row:
                    observed_value = observation_row[candidate]
                    break

    satisfied, deviation = _check_value_constraint(constraint, observed_value)
    if satisfied:
        return 0.0
    base = max(deviation, 0.25)

    # Confidence acts as a multiplier in [0.55, 1.0] so even maxed-out
    # deviations differentiate between low and high confidence predictions
    # without saturating the [0, 1] cap. A high-confidence prediction
    # failing is a strictly sharper signal than the same deviation under
    # a low-confidence one.
    conf = prediction.confidence
    if conf is None:
        sharpness = 0.85  # treat unknown confidence as moderate
    else:
        sharpness = 0.55 + 0.45 * max(0.0, min(1.0, float(conf)))

    return max(0.0, min(1.0, base * sharpness))


def score_residual_impact(
    prediction: ModelPrediction,
    model_row: Mapping[str, Any] | None,
) -> float:
    """How much does this failure matter?

    Inputs the caller pre-fetches:
      * `model_row`: hydrated Model row (any mapping with the columns
        we care about). Optional structural keys we read:
        `hub_score`, `bridge_score`, `dependent_count`,
        `retrieval_count`, `confidence`. Missing keys are ignored.

    Output is a weighted sum, clamped to [0, 1]:
      * 0.40 × hub_score (defaults to 0 when absent),
      * 0.30 × bridge_score,
      * 0.20 × normalized dependent_count (saturates at 10),
      * 0.10 × prediction confidence.

    The weights are a starting heuristic; later phases can swap in a
    learned scorer without touching the call sites.
    """
    if model_row is None:
        return _default_impact_from_confidence(prediction.confidence)

    hub = _clamp01(model_row.get("hub_score"))
    bridge = _clamp01(model_row.get("bridge_score"))
    dependents_raw = model_row.get("dependent_count")
    try:
        dependents = float(dependents_raw) if dependents_raw is not None else 0.0
    except (TypeError, ValueError):
        dependents = 0.0
    dependent_norm = min(1.0, dependents / 10.0)
    conf = _clamp01(prediction.confidence)

    score = (
        0.40 * hub
        + 0.30 * bridge
        + 0.20 * dependent_norm
        + 0.10 * conf
    )
    return max(0.0, min(1.0, score))


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------


def _clamp01(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def _default_impact_from_confidence(confidence: float | None) -> float:
    """Fallback impact when no structural data is available.

    Reflects "the only thing we know about this Model is how confident
    it was". Bounded into [0.05, 0.6] so an un-enriched residual still
    gets some attention but doesn't crowd out properly-scored ones.
    """
    if confidence is None:
        return 0.2
    c = _clamp01(confidence)
    return 0.05 + 0.55 * c


def _observation_id(observation_row: Mapping[str, Any]) -> UUID | None:
    raw = observation_row.get("id")
    if raw is None:
        return None
    if isinstance(raw, UUID):
        return raw
    try:
        return UUID(str(raw))
    except (TypeError, ValueError):
        return None


def _summarize_error(
    prediction: ModelPrediction,
    observation_row: Mapping[str, Any],
    observed_value: Any,
) -> str:
    """One-line debug summary embedded into the residual row."""
    pred_excerpt = (prediction.prediction or "").strip()
    if len(pred_excerpt) > 140:
        pred_excerpt = pred_excerpt[:137] + "..."
    obs_kind = observation_row.get("kind") or observation_row.get("type") or "observation"
    return (
        f"Residual: expected '{pred_excerpt}' but {obs_kind} "
        f"reported value={observed_value!r}"
    )


__all__ = [
    "detect_prediction_error",
    "score_residual_impact",
    "score_residual_severity",
]
