"""
services/query/api.py — FastAPI routes for the Ask surface.

Endpoints (CONTRACTS §1.2 + §1.3):

  POST /view/ceo/ask
    Request:  { query: str, context_card_id?: UUID }
    Response: { turn_id, query_echo, response_html, verbs[],
                computed_at, latency_ms }

  POST /view/ceo/turn-action
    Request:  { turn_id, action: save|done|followup, follow_up_query? }
    Response: { ok, new_turn_id? }

Behavioral notes:
  - `/ask` accepts an optional `query_id` (not in the contract; used by
    the UI when it taps a pre-loaded chip — lets us hit prefetch cache).
    If `query_id` maps to a cached prefetch response, we return it
    directly with latency_ms set from the cache read. Otherwise we run
    the full pipeline.
  - `/turn-action` with `action=followup` is the simplest path: the
    UI could call `/ask` instead, but the contract prefers a dedicated
    endpoint to make save/done idempotent. When followup is sent with
    `follow_up_query`, we invoke the handler and return `new_turn_id`.
    For save/done we simply acknowledge (persistence is deferred to
    Agent-GRT's cache; until then we keep an in-memory map of saved
    turn_ids).
  - Conversation history is NOT maintained server-side in V1 — the UI
    passes back the full history on each `/ask` call via a
    `conversation_history` field that is accepted as an optional
    extension to the contract. This avoids a server-side session
    store. If CONTRACTS ever forbids this we drop the extension
    (logged separately).

Tenant resolution is stubbed — single-tenant dogfood. Header
`x-tenant-id` can override for test harnesses. Real auth lands with
Agent-GRT's WS auth pass.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Header, Request
from pydantic import BaseModel, Field

from lib.shared.errors import ValidationError

from .core import (
    AnswerQueryRequest,
    AnswerQueryResponse,
    CardContext,
    QueryHandler,
    Turn,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Pydantic models on the wire
# ---------------------------------------------------------------------


class AskRequestBody(BaseModel):
    query: str
    context_card_id: Optional[UUID] = None
    # Extension: when the UI taps a pre-loaded chip it sends the
    # chip's id so we can try the prefetch cache.
    query_id: Optional[str] = None
    # Extension: UI passes the turn history back on each call.
    conversation_history: list["TurnModel"] = Field(default_factory=list)
    # Extension: UI may pass an inline card context instead of making
    # us look it up. Used by tests and by cases where the UI already
    # has the card info.
    inline_card_context: Optional["CardContextModel"] = None


class TurnModel(BaseModel):
    turn_id: UUID
    query: str
    response_html: str
    category: str
    created_at: datetime
    saved: bool = False
    done: bool = False


class CardContextModel(BaseModel):
    card_id: UUID
    subject: Optional[str] = None
    recipient: Optional[str] = None
    kind: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class VerbModel(BaseModel):
    id: Literal["followup", "save", "done"]
    label: str


class AskResponseBody(BaseModel):
    turn_id: UUID
    query_echo: str
    response_html: str
    verbs: list[VerbModel]
    computed_at: datetime
    latency_ms: int


class TurnActionRequestBody(BaseModel):
    turn_id: UUID
    action: Literal["save", "done", "followup"]
    follow_up_query: Optional[str] = None


class TurnActionResponseBody(BaseModel):
    ok: bool
    new_turn_id: Optional[UUID] = None


# Pydantic v2 forward-ref resolution
AskRequestBody.model_rebuild()


# ---------------------------------------------------------------------
# Turn-action bookkeeping (in-memory until Agent-GRT's store lands)
# ---------------------------------------------------------------------


class _TurnStore:
    def __init__(self) -> None:
        self._saved: set[UUID] = set()
        self._done: set[UUID] = set()

    def mark_saved(self, turn_id: UUID) -> None:
        self._saved.add(turn_id)

    def mark_done(self, turn_id: UUID) -> None:
        self._done.add(turn_id)

    def is_saved(self, turn_id: UUID) -> bool:
        return turn_id in self._saved

    def is_done(self, turn_id: UUID) -> bool:
        return turn_id in self._done

    def clear(self) -> None:
        self._saved.clear()
        self._done.clear()


# ---------------------------------------------------------------------
# Default verb set for a conversation turn
# ---------------------------------------------------------------------


def _default_verbs() -> list[VerbModel]:
    """Every conversation turn gets three verbs, per design doc §11."""
    return [
        VerbModel(id="followup", label="Follow up"),
        VerbModel(id="save", label="Save"),
        VerbModel(id="done", label="Done"),
    ]


# ---------------------------------------------------------------------
# Dependency wiring
# ---------------------------------------------------------------------


def _tenant_id_from_header(
    x_tenant_id: Optional[str] = Header(default=None),
) -> UUID:
    """Pull tenant from `x-tenant-id` header. Dogfood accepts a hard
    default env — wired at app construction time."""
    if x_tenant_id:
        try:
            return UUID(x_tenant_id)
        except ValueError as e:
            raise HTTPException(
                status_code=400, detail="invalid x-tenant-id"
            ) from e
    # The handler factory below binds the dogfood tenant via closure.
    raise HTTPException(
        status_code=400,
        detail="x-tenant-id header required",
    )


# ---------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------


def build_router(
    handler: QueryHandler,
    *,
    default_tenant_id: Optional[UUID] = None,
    turn_store: Optional[_TurnStore] = None,
) -> APIRouter:
    """Build the FastAPI router bound to a specific QueryHandler.

    `default_tenant_id` is used when the request omits the
    `x-tenant-id` header (dogfood single-tenant convenience).
    """
    router = APIRouter()
    store = turn_store or _TurnStore()

    def tenant_dep(
        x_tenant_id: Optional[str] = Header(default=None),
    ) -> UUID:
        if x_tenant_id:
            try:
                return UUID(x_tenant_id)
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail="invalid x-tenant-id"
                ) from e
        if default_tenant_id is not None:
            return default_tenant_id
        raise HTTPException(status_code=400, detail="x-tenant-id header required")

    @router.post("/view/ceo/ask", response_model=AskResponseBody)
    async def ask(
        body: AskRequestBody,
        tenant_id: UUID = Depends(tenant_dep),
    ) -> AskResponseBody:
        # --- Prefetch fast path ---
        if body.query_id:
            cached = await handler.try_serve_from_prefetch(
                tenant_id, body.query_id
            )
            if cached is not None:
                return AskResponseBody(
                    turn_id=cached.turn_id,
                    query_echo=cached.query_echo,
                    response_html=cached.response_html,
                    verbs=_default_verbs(),
                    computed_at=cached.computed_at,
                    latency_ms=cached.latency_ms,
                )

        # --- Build request for handler ---
        card_ctx: Optional[CardContext] = None
        if body.inline_card_context is not None:
            cc = body.inline_card_context
            card_ctx = CardContext(
                card_id=cc.card_id,
                subject=cc.subject,
                recipient=cc.recipient,
                kind=cc.kind,
                raw=dict(cc.raw or {}),
            )
        history = [
            Turn(
                turn_id=t.turn_id,
                query=t.query,
                response_html=t.response_html,
                category=t.category,  # type: ignore[arg-type]
                created_at=t.created_at,
                saved=t.saved,
                done=t.done,
            )
            for t in body.conversation_history
        ]
        req = AnswerQueryRequest(
            tenant_id=tenant_id,
            query=body.query,
            context_card_id=body.context_card_id,
            conversation_history=history,
            inline_card_context=card_ctx,
            query_id=body.query_id,
        )

        try:
            resp = await handler.answer_query(req)
        except ValidationError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:  # noqa: BLE001
            log.exception("ask_handler_failed")
            raise HTTPException(status_code=500, detail="internal_error") from e

        return AskResponseBody(
            turn_id=resp.turn_id,
            query_echo=resp.query_echo,
            response_html=resp.response_html,
            verbs=_default_verbs(),
            computed_at=resp.computed_at,
            latency_ms=resp.latency_ms,
        )

    @router.post(
        "/view/ceo/turn-action", response_model=TurnActionResponseBody
    )
    async def turn_action(
        body: TurnActionRequestBody,
        tenant_id: UUID = Depends(tenant_dep),
    ) -> TurnActionResponseBody:
        if body.action == "save":
            store.mark_saved(body.turn_id)
            return TurnActionResponseBody(ok=True)
        if body.action == "done":
            store.mark_done(body.turn_id)
            return TurnActionResponseBody(ok=True)
        if body.action == "followup":
            if not body.follow_up_query or not body.follow_up_query.strip():
                raise HTTPException(
                    status_code=400,
                    detail="follow_up_query required for action=followup",
                )
            req = AnswerQueryRequest(
                tenant_id=tenant_id,
                query=body.follow_up_query,
                # No automatic history: the UI should re-issue
                # /ask with conversation_history. This endpoint exists
                # for minimalist clients (e.g. raw curl) that want
                # single-shot follow-ups without managing state.
                conversation_history=[],
            )
            try:
                resp = await handler.answer_query(req)
            except ValidationError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e
            except Exception as e:  # noqa: BLE001
                log.exception("followup_handler_failed")
                raise HTTPException(status_code=500, detail="internal_error") from e
            return TurnActionResponseBody(ok=True, new_turn_id=resp.turn_id)
        # Unreachable — Literal enforces the set.
        raise HTTPException(status_code=400, detail="unknown action")

    return router


__all__ = [
    "AskRequestBody",
    "AskResponseBody",
    "TurnActionRequestBody",
    "TurnActionResponseBody",
    "CardContextModel",
    "TurnModel",
    "VerbModel",
    "build_router",
]
