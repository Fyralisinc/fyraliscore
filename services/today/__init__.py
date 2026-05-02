"""services/today — aggregator for the Today page.

Reads the existing recommendation list, calibration, and Acts/Resources
tables; derives the severity/kind/tag/stats/evidence/paths shape that
FYRALIS_TODAY_SPEC.md asks the UI to render.

Backend doesn't store severity directly — it's derived from
`expected_impact * confidence` plus the proposition_kind. Same for the
kind label, tag, and suggested-paths shape. This service is the single
mapping layer between the substrate and the Today UI.
"""
from .aggregator import (
    TodayPayload,
    build_today,
)
from .triage import (
    TriageError,
    triage_recommendation,
)

__all__ = [
    "TodayPayload",
    "build_today",
    "TriageError",
    "triage_recommendation",
]
