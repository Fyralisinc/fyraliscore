"""services/history — aggregator for the History page.

Reads observations, models, commitments, and decisions; derives the
events / predictions / arcs / calibration shape that the History UI
(ui/src/pages/History.tsx) renders.
"""
from .aggregator import HistoryPayload, build_history

__all__ = ["HistoryPayload", "build_history"]
