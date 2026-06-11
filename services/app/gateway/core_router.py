"""Core gateway HTTP routes: health, metrics, auth session, and ingest."""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.http_headers import safe_headers
from services.app.gateway.auth import AuthContext, create_session
from services.ingest.ingestion.core import (
    IngestResult,
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    ingest,
)
from services.ingest.ingestion.handlers import HandlerNotFound
from services.app.gateway.state_wiring import probe_integration_runtime_state


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

    @router.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        payload, status_code = await _readiness_payload(request)
        return JSONResponse(payload, status_code=status_code)

    @router.get("/metrics")
    async def metrics() -> Response:
        from services.app.webhooks import metrics as webhook_metrics

        content = webhook_metrics.render_prometheus()
        # Shared lib.observability registry: http_request_*, db_pool_*,
        # ollama_*, kafka_producer_*, plus the per-source integration
        # collector (install/lifecycle counters live in this process).
        try:
            import services.ingest.integrations.metrics_export  # noqa: F401
            from lib.observability.metrics import render_default

            content += render_default()
        except Exception:  # noqa: BLE001 — scrape must not 500
            pass
        return Response(
            content=content,
            media_type="text/plain; version=0.0.4",
        )

    @router.post("/auth/session")
    async def post_session(request: Request) -> JSONResponse:
        deps = _deps(request)
        settings = _settings(request)
        bootstrap = settings.auth_bootstrap_secret
        hdr = request.headers.get("X-Bootstrap-Secret", "")
        if not bootstrap and settings.environment in {"prod", "production"}:
            return JSONResponse(
                {"error": "bootstrap_secret_required"},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
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
                request_headers=safe_headers(request.headers),
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


def _settings(request: Request) -> Any:
    settings = getattr(request.app.state, "gateway_settings", None)
    if settings is None:
        raise RuntimeError(
            "Gateway settings not initialised (construct via build_app)"
        )
    return settings


async def _readiness_payload(request: Request) -> tuple[dict[str, Any], int]:
    app_state = request.app.state
    startup_status = getattr(app_state, "startup_status", None)
    if startup_status is None:
        payload: dict[str, Any] = {
            "ready": False,
            "failed": False,
            "phase": "not_started",
            "components": {},
        }
    else:
        payload = startup_status.as_dict()

    components = dict(payload.get("components", {}))
    ready = bool(payload.get("ready")) and not bool(payload.get("failed"))
    failed = bool(payload.get("failed"))

    def set_component(
        name: str,
        component_status: str,
        *,
        required: bool,
        detail: str | None = None,
        error_type: str | None = None,
    ) -> None:
        nonlocal ready, failed
        component: dict[str, Any] = {
            "status": component_status,
            "required": required,
        }
        if detail:
            component["detail"] = detail
        if error_type:
            component["error_type"] = error_type
        components[name] = component
        if required and component_status != "ok":
            ready = False
            failed = True

    deps = getattr(app_state, "deps", None)
    if deps is None:
        set_component(
            "db",
            "failed",
            required=True,
            detail="gateway_deps_missing",
        )
    else:
        try:
            await deps.pool.fetchval("SELECT 1")
            set_component("db", "ok", required=True)
        except Exception as exc:  # noqa: BLE001
            set_component(
                "db",
                "failed",
                required=True,
                detail=str(exc),
                error_type=type(exc).__name__,
            )

    settings = getattr(app_state, "gateway_settings", None)

    for name in ("secret_store", "tenant_resolver", "tenant_flags"):
        if getattr(app_state, name, None) is None:
            set_component(name, "failed", required=True, detail="missing")
        else:
            set_component(name, "ok", required=True)

    probe_timeout_s = float(
        getattr(settings, "integration_runtime_probe_timeout_s", 5.0)
    )
    for result in await probe_integration_runtime_state(
        app_state,
        timeout_s=probe_timeout_s,
    ):
        if result.ok:
            set_component(result.component, "ok", required=True)
        else:
            set_component(
                result.component,
                "failed",
                required=True,
                detail=result.detail,
                error_type=result.error_type,
            )

    require_realtime = bool(getattr(settings, "require_realtime", False))
    realtime = getattr(app_state, "realtime", None)
    dispatcher = getattr(realtime, "dispatcher", None) if realtime else None
    if dispatcher is None and require_realtime:
        set_component("realtime", "failed", required=True, detail="missing")
    elif dispatcher is None:
        set_component(
            "realtime",
            "degraded",
            required=False,
            detail="not_running",
        )
    else:
        set_component("realtime", "ok", required=require_realtime)

    require_github = bool(
        getattr(settings, "require_github_integration", False)
    )
    github_client = getattr(app_state, "github_client", None)
    github_replay_cache = getattr(app_state, "github_replay_cache", None)
    if github_client is not None and github_replay_cache is not None:
        set_component(
            "github_gateway_state",
            "ok",
            required=require_github,
        )
    elif require_github:
        set_component(
            "github_gateway_state",
            "failed",
            required=True,
            detail="missing",
        )
    else:
        set_component(
            "github_gateway_state",
            "degraded",
            required=False,
            detail="not_wired",
        )

    require_data_plane = bool(
        getattr(settings, "require_ingestion_data_plane", False)
    )
    producer = getattr(app_state, "kafka_producer", None)
    s3_client = getattr(app_state, "s3_raw_client", None)
    if producer is not None and s3_client is not None:
        set_component(
            "ingestion_data_plane",
            "ok",
            required=require_data_plane,
        )
    elif require_data_plane:
        set_component(
            "ingestion_data_plane",
            "failed",
            required=True,
            detail="required_clients_missing",
        )
    else:
        set_component(
            "ingestion_data_plane",
            "disabled",
            required=False,
            detail="not_configured",
        )

    payload["ready"] = ready
    payload["failed"] = failed
    payload["components"] = components
    return payload, status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE


def _unauth(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )
