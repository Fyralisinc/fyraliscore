"""services.conversations — per-card probe conversation persistence.

DRIFTWOOD_TODAY_CARD_REVISION.md replaces the five static detail
sections with a probe-driven conversation model. This package owns:

  - The persistence layer (card_conversations + card_exchanges, see
    db/migrations/0024_card_conversations.sql).
  - The probe handler that resolves a probe id (phrase, chip, or free-
    form question) into a response, optionally routed through the
    QueryHandler when the substrate has full context.
  - The FastAPI router mounted on the gateway as
    /v1/cards/{card_id}/conversation and /v1/cards/{card_id}/probe.

Resolution strategy for v1:
  - Phrase / chip clicks: deterministic templates that reference the
    underlying recommendation. Cheap, predictable, no LLM call.
  - Free-form Ask: routed through services.query.QueryHandler with the
    card context wired in via inline_card_context.
"""
from .repo import (
    CardConversation,
    CardExchange,
    ConversationRepo,
)
from .handler import ProbeHandler, ProbeRequest, ProbeResponse
from .api import build_router

__all__ = [
    "CardConversation",
    "CardExchange",
    "ConversationRepo",
    "ProbeHandler",
    "ProbeRequest",
    "ProbeResponse",
    "build_router",
]
