"""services.sage.model_predictions — Phase 12 model-substrate predictions.

Internal Model-substrate prediction store (NOT the user-facing
Forecasts surface — that lives in `predictions` from migration 0041).
This package backs the SAGE self-evolution loop: Models emit
expectations, observations are compared against them, and residuals
(expectation violations) get prioritized by impact to drive inquiry.

Schema reference: db/migrations/0054_sage_model_predictions.sql.

Re-exports the public surface:
  * `ModelPrediction`, `ModelPredictionError`, `ExpectedObservation`
    — row types
  * `ModelPredictionsRepo`, `ModelPredictionErrorsRepo` — asyncpg repos
  * `detect_prediction_error`, `score_residual_severity`,
    `score_residual_impact` — pure residual-detection helpers
"""

from services.sage.model_predictions.repo import (
    ModelPredictionErrorsRepo,
    ModelPredictionsRepo,
)
from services.sage.model_predictions.residual import (
    detect_prediction_error,
    score_residual_impact,
    score_residual_severity,
)
from services.sage.model_predictions.types import (
    ExpectedObservation,
    ModelPrediction,
    ModelPredictionError,
)

__all__ = [
    "ExpectedObservation",
    "ModelPrediction",
    "ModelPredictionError",
    "ModelPredictionErrorsRepo",
    "ModelPredictionsRepo",
    "detect_prediction_error",
    "score_residual_impact",
    "score_residual_severity",
]
