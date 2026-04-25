"""
services/query/strategies — per-category retrieval strategies.

Every strategy exports:
  - `parse(query, conversation_history, card_context) -> ParsedQuery`
      extracts entities, time windows, actors, recipients, etc.
  - `build_trigger(parsed, tenant_id) -> TriggerContext`
      configures the retrieval pathway mix for this category.
  - `async gather(parsed, tenant_id, conn, *, models_repo=None) -> ContextBundle`
      runs primary_retrieve + assemble_context and attaches
      category-specific annotations to the bundle.

The registry `STRATEGIES` maps QueryCategory -> strategy module so
`QueryHandler` can dispatch without a long if/elif ladder.
"""
from __future__ import annotations

from typing import Callable

from services.query.classifier import QueryCategory

from . import arbitrary as _arbitrary
from . import draft as _draft
from . import show_me as _show_me
from . import summary as _summary
from . import what_if as _what_if
from . import why as _why
from .base import ParsedQuery, StrategyContext, StrategyResult, StrategyProtocol


STRATEGIES: dict[QueryCategory, StrategyProtocol] = {
    "why": _why.strategy,
    "show_me": _show_me.strategy,
    "draft": _draft.strategy,
    "what_if": _what_if.strategy,
    "summary": _summary.strategy,
    "arbitrary": _arbitrary.strategy,
}


def get_strategy(category: QueryCategory) -> StrategyProtocol:
    """Return the strategy implementation for a category, falling back
    to 'arbitrary' if the category is unknown (defense in depth — the
    classifier should never emit an unknown label, but defaulting
    keeps the path alive)."""
    return STRATEGIES.get(category, STRATEGIES["arbitrary"])


__all__ = [
    "STRATEGIES",
    "get_strategy",
    "ParsedQuery",
    "StrategyContext",
    "StrategyResult",
    "StrategyProtocol",
]
