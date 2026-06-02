"""services/app/webhooks/google_push.py — Calendar/Drive push webhook ingress.

    POST /webhooks/google_calendar/push
    POST /webhooks/google_drive/push
    X-Goog-Channel-ID:     <the channel id WE minted at watch time>
    X-Goog-Channel-Token:  <the shared secret WE set>
    X-Goog-Resource-State: sync | exists | change | update | …
    (body is empty — a content-less ping)

Unlike Gmail's Pub/Sub (OIDC JWT), Calendar/Drive push directly to this
`web_hook` with `X-Goog-*` headers. Verification is the channel token we set at
watch time (constant-time compared in `resolve_push`). On a verified ping we
drain the delta via the SAME path the poller uses, so a push-driven and a
poll-driven observation are indistinguishable and dedup at `observations.UNIQUE`.

ALWAYS returns 200 on anything we can't act on (unknown channel, token
mismatch, transient drain error) — the live poller is the safety net, and a
non-2xx would only make Google retry. The initial `state=sync` handshake is
acked without work.
"""
from __future__ import annotations

from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.ingest.integrations._google_watch import WatchSpec, drain_push, resolve_push
from services.ingest.integrations.google_calendar.watch import SPEC as _CALENDAR_SPEC
from services.ingest.integrations.google_drive.watch import SPEC as _DRIVE_SPEC


log = structlog.get_logger("webhooks.google_push")


router = APIRouter(prefix="/webhooks", tags=["webhooks", "google"])


def _pool(request: Request) -> asyncpg.Pool | None:
    pool = getattr(request.app.state, "pool", None)
    if pool is not None:
        return pool
    deps = getattr(request.app.state, "deps", None)
    return getattr(deps, "pool", None) if deps else None


async def _handle(request: Request, spec: WatchSpec) -> JSONResponse:
    headers = request.headers
    channel_id = headers.get("X-Goog-Channel-ID")
    token = headers.get("X-Goog-Channel-Token")
    state = (headers.get("X-Goog-Resource-State") or "").lower()

    # Initial handshake Google sends right after watch() — no delta yet.
    if state == "sync":
        return JSONResponse(content={"status": "sync_ack"})

    pool = _pool(request)
    if pool is None:
        log.error(f"{spec.source}.push.no_pool")
        return JSONResponse(content={"status": "skipped", "reason": "no_pool"})

    row = await resolve_push(pool, spec, channel_id=channel_id, token=token)
    if row is None:
        # Unknown channel or token mismatch — drop silently (200, no retry).
        log.warning(f"{spec.source}.push.unresolved", channel_present=bool(channel_id))
        return JSONResponse(content={"status": "skipped", "reason": "unknown_or_unverified"})

    try:
        ingested = await drain_push(pool, spec, row)
    except Exception as exc:  # noqa: BLE001 — never 5xx a webhook (poller covers it)
        log.exception(f"{spec.source}.push.drain_error", error=str(exc)[:200])
        return JSONResponse(content={"status": "error_swallowed"})

    return JSONResponse(content={"status": "ok", "ingested": ingested})


@router.post("/google_calendar/push")
async def google_calendar_push(request: Request) -> JSONResponse:
    return await _handle(request, _CALENDAR_SPEC)


@router.post("/google_drive/push")
async def google_drive_push(request: Request) -> JSONResponse:
    return await _handle(request, _DRIVE_SPEC)


__all__ = ["router"]
