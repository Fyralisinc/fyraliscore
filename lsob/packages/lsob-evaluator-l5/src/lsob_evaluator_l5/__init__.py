"""LSOB Layer 5 (temporal dynamics) evaluator package."""

from lsob_evaluator_l5.evaluator import LayerFiveEvaluator
from lsob_evaluator_l5.metrics import linear_regression, mean_median_p90
from lsob_evaluator_l5.mock_sut import MockTemporalSUT, TemporalBeliefRecord
from lsob_evaluator_l5.stability import StableWindow, find_stable_windows

__version__ = "0.1.0"

__all__ = [
    "LayerFiveEvaluator",
    "MockTemporalSUT",
    "StableWindow",
    "TemporalBeliefRecord",
    "__version__",
    "find_stable_windows",
    "linear_regression",
    "mean_median_p90",
]
