"""Core gateway HTTP routes: health, metrics, auth session, and ingest."""
from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from services.app.gateway.auth import AuthContext, create_session
from services.ingest.ingestion.core import (
    IngestResult,
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    ingest,
)
from services.ingest.ingestion.handlers import HandlerNotFound
from services.ingest.ingestion.handlers.slack import (
    SlackSignatureError,
    verify_slack_signature,
)


class IngestSizeError(Exception):
    """Raised when the ingest body is rejected before parsing."""

    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        super().__init__(payload.get("error", "ingest_size_error"))
        self.status_code = status_code
        self.payload = payload


async def ingest_size_error_handler(
    request: Request, exc: IngestSizeError
) -> JSONResponse:
    return JSONResponse(exc.payload, status_code=exc.status_code)


async def ingest_body_bytes(request: Request) -> bytes:
    """Bounded body reader for POST /ingest/*."""
    te = request.headers.get("transfer-encoding", "").lower()
    if "chunked" in te:
        raise IngestSizeError(
            status.HTTP_413_CONTENT_TOO_LARGE,
            {
                "error": "payload_too_large",
                "reason": "chunked_unsupported",
            },
        )
    cl_raw = request.headers.get("content-length")
    if cl_raw is not None:
        try:
            cl = int(cl_raw)
        except ValueError:
            raise IngestSizeError(
                status.HTTP_400_BAD_REQUEST,
                {"error": "invalid_content_length"},
            )
        if cl < 0 or cl > MAX_PAYLOAD_BYTES:
            raise IngestSizeError(
                status.HTTP_413_CONTENT_TOO_LARGE,
                {
                    "error": "payload_too_large",
                    "max_bytes": MAX_PAYLOAD_BYTES,
                },
            )
    buf = bytearray()
    async for chunk in request.stream():
        if not chunk:
            continue
        buf.extend(chunk)
        if len(buf) > MAX_PAYLOAD_BYTES:
            raise IngestSizeError(
                status.HTTP_413_CONTENT_TOO_LARGE,
                {
                    "error": "payload_too_large",
                    "max_bytes": MAX_PAYLOAD_BYTES,
                },
            )
    return bytes(buf)


def build_core_router() -> APIRouter:
    router = APIRouter(tags=["gateway-core"])

    @router.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/metrics")
    async def metrics() -> Response:
        from services.app.webhooks import metrics as webhook_metrics

        return Response(
            content=webhook_metrics.render_prometheus(),
            media_type="text/plain; version=0.0.4",
        )

    @router.post("/auth/session")
    async def post_session(request: Request) -> JSONResponse:
        deps = _deps(request)
        bootstrap = os.environ.get("AUTH_BOOTSTRAP_SECRET")
        hdr = request.headers.get("X-Bootstrap-Secret", "")
        if bootstrap and hdr != bootstrap:
            return JSONResponse(
                {"error": "bootstrap_secret_mismatch"},
                status_code=status.HTTP_403_FORBIDDEN,
            )
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        try:
            actor_id = UUID(str(body.get("actor_id")))
            tenant_id = UUID(str(body.get("tenant_id")))
        except Exception:
            return JSONResponse(
                {"error": "actor_id and tenant_id required as UUID"},
                status_code=400,
            )
        ttl_s = body.get("ttl_seconds") or 24 * 3600
        try:
            ttl_s = int(ttl_s)
        except Exception:
            return JSONResponse(
                {"error": "ttl_seconds must be int"}, status_code=400
            )

        row = await deps.pool.fetchrow(
            "SELECT tenant_id FROM actors WHERE id = $1", actor_id
        )
        if row is None or row["tenant_id"] != tenant_id:
            return JSONResponse(
                {"error": "actor_not_found_for_tenant"},
                status_code=404,
            )
        token, ctx = await create_session(
            deps.pool,
            actor_id=actor_id,
            tenant_id=tenant_id,
            ttl=timedelta(seconds=ttl_s),
        )
        return JSONResponse(
            {
                "token": token,
                "expires_at": ctx.expires_at.isoformat(),
                "session_id": str(ctx.session_id),
            },
            status_code=201,
        )

    @router.post("/ingest/{channel:path}")
    async def post_ingest(
        channel: str,
        request: Request,
        raw: bytes = Depends(ingest_body_bytes),
    ) -> JSONResponse:
        deps = _deps(request)
        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")

        if channel == "slack:message":
            secret = deps.slack_signing_secret
            ts = request.headers.get("X-Slack-Request-Timestamp", "")
            sig = request.headers.get("X-Slack-Signature", "")
            try:
                verify_slack_signature(raw, ts, sig, secret or "")
            except SlackSignatureError as e:
                return JSONResponse(
                    {"error": "slack_signature", "reason": e.message},
                    status_code=status.HTTP_403_FORBIDDEN,
                )
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            return JSONResponse(
                {"error": "invalid_json", "detail": e.msg},
                status_code=400,
            )
        try:
            result: IngestResult = await ingest(
                channel,
                payload,
                pool=deps.pool,
                tenant_id=auth.tenant_id,
                actor_repo=deps.actor_repo,
                alias_repo=deps.alias_repo,
                embedder=deps.embedder,
                request_headers=dict(request.headers),
            )
        except HandlerNotFound:
            return JSONResponse(
                {"error": "handler_not_found", "channel": channel},
                status_code=404,
            )
        except PayloadTooLarge:
            return JSONResponse({"error": "payload_too_large"}, status_code=413)
        except ValidationError as e:
            return JSONResponse(
                {"error": "validation_error", "detail": e.to_dict()},
                status_code=400,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()},
                status_code=400,
            )
        return JSONResponse(
            {
                "observation_id": str(result.observation.id),
                "deduped": result.deduped,
                "trigger_queue_id": (
                    str(result.trigger_queue_id)
                    if result.trigger_queue_id
                    else None
                ),
            },
            status_code=200 if result.deduped else 201,
        )

    return router


def _auth(request: Request) -> AuthContext | None:
    return getattr(request.state, "auth", None)


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps


def _unauth(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )
