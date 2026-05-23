"""services.conversations.api — FastAPI router for the probe surface.

Endpoints (DRIFTWOOD_TODAY_CARD_REVISION.md §13):

  GET    /v1/cards/{card_id}/conversation   → CardConversation
  POST   /v1/cards/{card_id}/probe          → ProbeResponse
  DELETE /v1/cards/{card_id}/conversation   → {ok: true}

Auth + tenant come from the gateway BearerAuthMiddleware (request.state.auth).
"""
from __future__ import annotations

import logging
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from .handler import ProbeHandler, ProbeRequest as HandlerProbeRequest
from .repo import ConversationRepo


log = logging.getLogger(__name__)


def build_router(
    *, repo: ConversationRepo, handler: ProbeHandler,
) -> APIRouter:
    router = APIRouter()

    def _auth(request: Request):
        auth = getattr(request.state, "auth", None)
        if auth is None:
            raise HTTPException(status_code=401, detail="unauthorized")
        return auth

    @router.get("/v1/cards/{card_id}/conversation")
    async def get_conversation(card_id: str, request: Request):
        auth = _auth(request)
        try:
            cid = UUID(card_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="invalid_card_id")
        conv = await repo.fetch(
            tenant_id=auth.tenant_id, actor_id=auth.actor_id, card_id=cid,
        )
        if conv is None:
            raise HTTPException(status_code=404, detail="no_conversation")
        exchanges = await repo.list_exchanges(conversation_id=conv.id)
        return conv.to_wire(exchanges)

    @router.post("/v1/cards/{card_id}/probe")
    async def post_probe(card_id: str, request: Request):
        auth = _auth(request)
        try:
            cid = UUID(card_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="invalid_card_id")
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="invalid_json")
        kind = body.get("kind")
        if kind not in ("phrase", "chip", "ask"):
            raise HTTPException(status_code=400, detail="invalid_kind")
        probe_id = body.get("probe_id")
        query = body.get("query")
        try:
            resp = await handler.probe(
                HandlerProbeRequest(
                    tenant_id=auth.tenant_id,
                    actor_id=auth.actor_id,
                    card_id=cid,
                    kind=kind,
                    probe_id=probe_id if isinstance(probe_id, str) else None,
                    query=query if isinstance(query, str) else None,
                )
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception:  # noqa: BLE001
            log.exception("probe_failed")
            raise HTTPException(status_code=500, detail="internal_error")
        return {"exchange": resp.exchange.to_wire()}

    @router.delete("/v1/cards/{card_id}/conversation")
    async def delete_conversation(card_id: str, request: Request):
        auth = _auth(request)
        try:
            cid = UUID(card_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="invalid_card_id")
        ok = await repo.clear(
            tenant_id=auth.tenant_id, actor_id=auth.actor_id, card_id=cid,
        )
        return {"ok": ok}

    return router
