"""Cheap intent and mode classification for Ask Fyralis."""
from __future__ import annotations

from .schemas import AskMode, AskScope


_DEEP_TERMS = (
    "are we sure", "hidden", "contradict", "contradicted", "contradiction",
    "stale", "missing", "assumption", "assumptions", "deeper", "deep review",
)
_GLOBAL_TERMS = (
    "company-wide", "company wide", "across the company", "whole company",
    "most at risk", "recurring", "all teams",
)
_QUICK_TERMS = (
    "why", "changed", "since", "risk", "contradicts", "blocked", "behind",
)


def classify_intent(query: str, scope: AskScope) -> tuple[str, AskMode]:
    q = query.casefold()
    if scope.type == "whole_company" or any(term in q for term in _GLOBAL_TERMS):
        return "background_review", "background_review"
    if any(term in q for term in _DEEP_TERMS):
        return "state_gap_inquiry", "deep_inquiry"
    if any(term in q for term in _QUICK_TERMS):
        return "causal_context", "quick_inquiry"
    return "factual_synthesis_read", "direct_synthesis_read"
