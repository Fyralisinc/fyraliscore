"""Evaluator phase marker — distinguishes monthly-checkpoint evaluators from final-state."""

from __future__ import annotations

from enum import Enum


class EvaluatorPhase(str, Enum):
    """When an evaluator is invoked during a run.

    - ``per_month``: run at each monthly checkpoint.
    - ``final``: run once, at the end of the corpus (Layers 3, 5, 6 typically).
    """

    per_month = "per_month"
    final = "final"


def evaluator_phase(evaluator: object) -> EvaluatorPhase:
    """Introspect an evaluator to decide when it runs.

    Contract: if an evaluator exposes a ``runs_at`` attribute, we honor it.
    Otherwise we fall back to the layer-based convention (L3, L5, L6 run at
    the end; everything else runs per month).
    """
    runs_at = getattr(evaluator, "runs_at", None)
    if isinstance(runs_at, EvaluatorPhase):
        return runs_at
    if isinstance(runs_at, str):
        try:
            return EvaluatorPhase(runs_at)
        except ValueError:
            pass
    layer_id = getattr(evaluator, "layer_id", None)
    if layer_id in (3, 5, 6):
        return EvaluatorPhase.final
    return EvaluatorPhase.per_month
