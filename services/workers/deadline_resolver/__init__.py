"""services/workers/deadline_resolver — Wave 4-A.

Deadline resolver polls for prediction Models whose `evaluate_at` has
passed and enqueues T2 prediction_overdue triggers for Think. It never
writes to `models` directly — Think's deterministic T2 handler
(`services/think/deterministic.py`) owns the confidence deltas,
archival, and `resolution_outcome` update.
"""
from services.workers.deadline_resolver.worker import (  # noqa: F401
    DeadlineResolver,
    ProvisionalOutcome,
    DEFAULT_POLL_INTERVAL_S,
)
from services.workers.deadline_resolver.evaluators import (  # noqa: F401
    evaluate_falsifier,
    EvaluationContext,
)
