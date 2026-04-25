"""services/gateway/main.py — FastAPI entry point.

BUILD-PLAN §3 Prompt 2.A. Delivers:

- POST /ingest/{channel}  — routes to services.ingestion.core.ingest
- POST /auth/session      — creates an actor_sessions row
- GET  /observations      — Wave-4 retrieval stubbed with list-by-tenant
- GET  /models            — stubbed
- GET  /commitments       — stubbed
- GET  /goals             — stubbed
- GET  /decisions         — stubbed
- GET  /resources         — stubbed
- WS   /stream            — Wave-5 stub (accepts, hellos, closes)

Middleware:
- BearerAuthMiddleware    — resolves Bearer token → actor / tenant.
- RateLimitMiddleware     — per-(tenant, actor) token bucket.
- RequestContextMiddleware — request_id, structlog bind, access log.

Tenant resolution:
- `X-Tenant-Id` header (primary for Wave 2-A).
- `DEFAULT_TENANT_ID` env var fallback in dev (explicitly documented
  as a deviation). Subdomain-based resolution is DEFERRED to Wave 5.

The dispatcher is built by `build_app()` so tests can override
`pool`, `actor_repo`, `alias_repo`, `embedder`, and the rate limiter.
"""
from __future__ import annotations

import contextlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Awaitable, Callable
from uuid import UUID

import asyncpg
import structlog
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.auth import (
    AuthContext,
    create_session,
    validate_token,
)
from services.gateway.db_bootstrap import (
    _register_codecs,
    close_gateway_pool,
    create_gateway_pool,
)
from services.gateway.logging_config import configure_structlog, get_logger
from services.gateway.rate_limit import RateLimiter, RateTier
from services.ingestion.core import (
    IngestResult,
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    ingest,
)
from services.ingestion.handlers import CHANNEL_TRUST_MAP, HandlerNotFound
from services.ingestion.handlers.slack import (
    SlackSignatureError,
    verify_slack_signature,
)


log = get_logger("gateway")


# ---------------------------------------------------------------------
# App state
# ---------------------------------------------------------------------


class GatewayDeps:
    """Container for Gateway-wide dependencies, attached to `app.state`.

    Tests override individual attributes before constructing an
    `httpx.AsyncClient(app=app, ...)`.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        actor_repo: ActorRepo,
        alias_repo: EntityAliasRepo,
        embedder: OllamaClient | None,
        rate_limiter: RateLimiter,
        slack_signing_secret: str | None,
    ) -> None:
        self.pool = pool
        self.actor_repo = actor_repo
        self.alias_repo = alias_repo
        self.embedder = embedder
        self.rate_limiter = rate_limiter
        self.slack_signing_secret = slack_signing_secret


# ---------------------------------------------------------------------
# Middleware — request context + structured logging
# ---------------------------------------------------------------------


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Binds request_id to structlog context; logs request summary."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = str(uuid7())
        # Tenant header if present — otherwise bind DEFAULT_TENANT_ID
        # for dev. Auth middleware later may override actor_id.
        tenant_header = request.headers.get("X-Tenant-Id")
        request.state.request_id = request_id
        request.state.tenant_id = tenant_header
        bind_vars: dict[str, Any] = {"request_id": request_id}
        if tenant_header:
            bind_vars["tenant_id"] = tenant_header
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(**bind_vars)
        started = time.monotonic()
        try:
            response = await call_next(request)
        except Exception as e:  # pragma: no cover — fallthrough for uncaught
            duration_ms = (time.monotonic() - started) * 1000
            log.error(
                "request_failed",
                method=request.method,
                path=request.url.path,
                duration_ms=round(duration_ms, 2),
                error=type(e).__name__,
            )
            raise
        duration_ms = (time.monotonic() - started) * 1000
        # Auth middleware bound actor_id/tenant_id to contextvars in a
        # downstream task context; Starlette's BaseHTTPMiddleware boundary
        # doesn't propagate those back up, so pull directly from request.state.
        auth_ctx: AuthContext | None = getattr(request.state, "auth", None)
        log_extra: dict[str, Any] = {}
        if auth_ctx is not None:
            log_extra["actor_id"] = str(auth_ctx.actor_id)
            log_extra["tenant_id"] = str(auth_ctx.tenant_id)
        elif tenant_header:
            log_extra["tenant_id"] = tenant_header
        log.info(
            "request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 2),
            **log_extra,
        )
        response.headers["X-Request-Id"] = request_id
        return response


# ---------------------------------------------------------------------
# Middleware — bearer auth
# ---------------------------------------------------------------------

# Paths that do not require authentication (e.g. health checks, the
# session-minting endpoint itself uses a separate actor lookup).
_PUBLIC_PATHS = frozenset({"/healthz", "/auth/session"})

# Path prefixes that bypass the gateway's bearer-session middleware.
# Week-4 integration: the CEO-view sub-routers carry their own token
# auth (`VIEW_CEO_TOKEN` resolved by the stream manager), and the
# internal rendering endpoints are reached only from in-process
# adapters. Exposing them publicly on the single Uvicorn host during
# dogfood is acceptable; real auth lands with Wave-5-adj.
_PUBLIC_PATH_PREFIXES: tuple[str, ...] = (
    "/view/ceo/",
    "/rendering/",
    "/simulation/",
    "/simulation-ui/",
    "/debug/",
)


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Validates `Authorization: Bearer <token>` against actor_sessions.

    Resolves deps from `request.app.state.deps` each dispatch so we are
    tolerant of deps being set AFTER middleware construction (the
    default `build_app()` path wires deps during lifespan startup).
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if (
            request.url.path in _PUBLIC_PATHS
            or request.url.path.startswith("/stream")
            or any(request.url.path.startswith(p) for p in _PUBLIC_PATH_PREFIXES)
        ):
            return await call_next(request)

        authz = request.headers.get("Authorization", "")
        if not authz.startswith("Bearer "):
            return _unauth("missing_bearer")
        token = authz[len("Bearer ") :].strip()
        if not token:
            return _unauth("empty_bearer")
        deps = _deps(request)
        ctx = await validate_token(deps.pool, token)
        if ctx is None:
            return _unauth("invalid_or_expired")
        request.state.auth = ctx
        structlog.contextvars.bind_contextvars(
            actor_id=str(ctx.actor_id),
            tenant_id=str(ctx.tenant_id),
        )
        hdr_tenant = request.headers.get("X-Tenant-Id")
        if hdr_tenant and hdr_tenant != str(ctx.tenant_id):
            return JSONResponse(
                {"error": "tenant_mismatch"},
                status_code=status.HTTP_403_FORBIDDEN,
            )
        return await call_next(request)


def _unauth(reason: str) -> Response:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


# ---------------------------------------------------------------------
# Middleware — rate limiting
# ---------------------------------------------------------------------


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Token-bucket limiter per (tenant, actor). Signal-ingest path
    (POST /ingest/*) gets the higher 1000/min budget; everything else
    uses the 100/min default budget."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if (
            request.url.path in _PUBLIC_PATHS
            or request.url.path.startswith("/stream")
            or any(request.url.path.startswith(p) for p in _PUBLIC_PATH_PREFIXES)
        ):
            return await call_next(request)
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:
            return await call_next(request)
        deps = _deps(request)
        tier = (
            RateTier.SIGNAL_INGEST
            if request.url.path.startswith("/ingest/")
            else RateTier.DEFAULT
        )
        allowed = await deps.rate_limiter.consume(
            (auth.tenant_id, auth.actor_id), tier
        )
        if not allowed:
            return JSONResponse(
                {"error": "rate_limited", "tier": tier.value},
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            )
        return await call_next(request)


# ---------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------


def build_app(
    *,
    pool: asyncpg.Pool | None = None,
    actor_repo: ActorRepo | None = None,
    alias_repo: EntityAliasRepo | None = None,
    embedder: OllamaClient | None = None,
    rate_limiter: RateLimiter | None = None,
    slack_signing_secret: str | None = None,
    configure_logging: bool = True,
) -> FastAPI:
    """Build the FastAPI app. Every dependency is injectable for tests.

    When the Gateway is started normally (via `uvicorn services.gateway:app`),
    `build_app()` is called with all dependencies None — the lifespan
    handler constructs them from env vars.
    """
    if configure_logging:
        configure_structlog(os.environ.get("LOG_LEVEL", "INFO"))

    # Lifespan context-manager per FastAPI >= 0.110 recommended pattern.
    @contextlib.asynccontextmanager
    async def _lifespan(app_: FastAPI) -> AsyncIterator[None]:
        nonlocal pool, actor_repo, alias_repo, embedder, rate_limiter
        if pool is None:
            pool = await create_gateway_pool()
        if actor_repo is None:
            actor_repo = ActorRepo(pool)
        if alias_repo is None:
            alias_repo = EntityAliasRepo(pool)
        if embedder is None and os.environ.get("OLLAMA_URL"):
            embedder = OllamaClient(OllamaConfig.from_env())
        if rate_limiter is None:
            rate_limiter = RateLimiter()
        app_.state.deps = GatewayDeps(
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            rate_limiter=rate_limiter,
            slack_signing_secret=(
                slack_signing_secret
                or os.environ.get("SLACK_SIGNING_SECRET")
            ),
        )
        # Wave 4-D: realtime wiring. Only configure if not already done
        # (tests path pre-wires before lifespan). Lazy import to avoid
        # a services.gateway ↔ services.realtime circular.
        if getattr(app_.state, "realtime", None) is None:
            from services.realtime.main import (
                configure_realtime as _configure_realtime,
            )

            rt_deps = _configure_realtime(
                app_, pool=pool, start=False
            )
            await rt_deps.dispatcher.start()

        # Week-4 Integration: mount CEO-view routers (RND / GRT / QRY /
        # SIM). Env-gated so tests that pre-build the app still see the
        # old behaviour unless they opt in. Each sub-app is mounted on
        # the main gateway so the UI speaks to one host.
        if os.environ.get("GATEWAY_CEO_VIEW_ENABLED", "1") != "0":
            try:
                await _configure_ceo_view(app_, pool=pool)
            except Exception as _ceo_exc:  # noqa: BLE001
                # Never break the gateway startup if CEO wiring fails;
                # log and continue with the core routes.
                log.error(
                    "ceo_view_wiring_failed",
                    error=str(_ceo_exc),
                    error_type=type(_ceo_exc).__name__,
                )
        try:
            yield
        finally:
            # Stop the dispatcher we started here (not the test-owned one).
            rt = getattr(app_.state, "realtime", None)
            if rt is not None:
                try:
                    await rt.dispatcher.stop()
                except Exception:
                    pass
            ceo = getattr(app_.state, "ceo_view", None)
            if ceo is not None:
                scheduler = ceo.get("scheduler")
                if scheduler is not None:
                    try:
                        await scheduler.stop()
                    except Exception:
                        pass
            deps: GatewayDeps = app_.state.deps
            if deps.embedder is not None:
                try:
                    await deps.embedder.close()
                except Exception:
                    pass
            if os.environ.get("GATEWAY_OWNS_POOL", "") == "1":
                await close_gateway_pool(deps.pool)

    app = FastAPI(
        title="Company OS Gateway",
        version="0.1.0",
        lifespan=_lifespan,
    )

    # If caller pre-built every dep, skip the lifespan path and attach
    # immediately so tests can construct the app synchronously and
    # avoid lifespan orchestration.
    if (
        pool is not None
        and actor_repo is not None
        and alias_repo is not None
        and rate_limiter is not None
    ):
        app.state.deps = GatewayDeps(
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            rate_limiter=rate_limiter,
            slack_signing_secret=slack_signing_secret,
        )

    # Middleware order: add last → first to run.
    # Each middleware resolves deps lazily from request.app.state so
    # it tolerates deps being wired in lifespan startup (default path).
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(BearerAuthMiddleware)
    app.add_middleware(RequestContextMiddleware)

    _register_routes(app)

    # Wave 4-D: mount the realtime WS sub-router. When the caller pre-
    # supplied the pool (test path), we configure the Dispatcher
    # immediately without starting it (tests control lifecycle). The
    # production path (lifespan-wired) relies on the lifespan handler
    # above to wire realtime once deps exist — see the lifespan context
    # manager where `app.state.deps` is finalised.
    # Import is deferred to runtime to break the services.gateway ↔
    # services.realtime circular import.
    if pool is not None:
        from services.realtime.main import (  # local import (break cycle)
            configure_realtime as _configure_realtime,
        )

        _configure_realtime(app, pool=pool, start=False)
    return app


# ---------------------------------------------------------------------
# Helpers — deps resolver (for routes + middleware that run late)
# ---------------------------------------------------------------------


def _deps(request_or_app) -> GatewayDeps:  # type: ignore[no-untyped-def]
    """Pull deps off the app state (works for Request or FastAPI)."""
    app = getattr(request_or_app, "app", request_or_app)
    deps = getattr(app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------


def _register_routes(app: FastAPI) -> None:
    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/auth/session")
    async def post_session(request: Request) -> JSONResponse:
        """Mint a session for an actor. Authenticated via:
          - `X-Bootstrap-Secret` env var matching `AUTH_BOOTSTRAP_SECRET`
            (dev-only — production ships a real auth path in Wave 5).
          - Body: {"actor_id": "<uuid>", "tenant_id": "<uuid>",
                   "ttl_seconds": optional int}.
        Returns {"token": "...", "expires_at": "..."}.
        """
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
            return JSONResponse(
                {"error": "invalid_json"}, status_code=400
            )
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
        # Verify the actor exists + matches the tenant.
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

    @app.post("/ingest/{channel:path}")
    async def post_ingest(channel: str, request: Request) -> JSONResponse:
        deps = _deps(request)
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:
            return _unauth("missing_bearer")
        # Enforce payload size (Starlette doesn't enforce a default
        # body limit; we check after reading).
        raw = await request.body()
        if len(raw) > MAX_PAYLOAD_BYTES:
            return JSONResponse(
                {"error": "payload_too_large", "max_bytes": MAX_PAYLOAD_BYTES},
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            )
        # Slack signature check — only for slack:message (the one
        # signature-verified channel in Wave 2-A).
        if channel == "slack:message":
            secret = deps.slack_signing_secret
            ts = request.headers.get("X-Slack-Request-Timestamp", "")
            sig = request.headers.get("X-Slack-Signature", "")
            try:
                verify_slack_signature(
                    raw, ts, sig, secret or ""
                )
            except SlackSignatureError as e:
                return JSONResponse(
                    {"error": "slack_signature", "reason": e.message},
                    status_code=status.HTTP_403_FORBIDDEN,
                )
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return JSONResponse(
                {"error": "invalid_json"}, status_code=400
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
        except HandlerNotFound as e:
            return JSONResponse(
                {"error": "handler_not_found", "channel": channel},
                status_code=404,
            )
        except PayloadTooLarge:
            return JSONResponse(
                {"error": "payload_too_large"},
                status_code=413,
            )
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

    # ---------------- Stub retrieval endpoints (Wave 4) ---------------
    # Minimal list-by-tenant endpoints with limit/offset paging. These
    # are intentionally dumb — Wave 4 retrieval integration replaces
    # them with the real primary-pathway resolver.

    @app.get("/observations")
    async def get_observations(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        auth: AuthContext = request.state.auth
        deps = _deps(request)
        rows = await deps.pool.fetch(
            """
            SELECT id, kind, source_channel, occurred_at, content_text
            FROM observations
            WHERE tenant_id = $1
            ORDER BY occurred_at DESC
            LIMIT $2 OFFSET $3
            """,
            auth.tenant_id,
            _clip(limit, 1, 500),
            max(offset, 0),
        )
        return {
            "items": [
                {
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "source_channel": r["source_channel"],
                    "occurred_at": r["occurred_at"].isoformat(),
                    "content_text": r["content_text"],
                }
                for r in rows
            ],
            "stub": True,
        }

    @app.get("/models")
    async def get_models(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "models",
            ("id", "proposition", "confidence", "status", "created_at"),
            limit,
            offset,
        )

    @app.get("/commitments")
    async def get_commitments(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "commitments",
            ("id", "title", "state", "owner_id", "due_date", "created_at"),
            limit,
            offset,
        )

    @app.get("/goals")
    async def get_goals(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "goals",
            ("id", "title", "state", "altitude", "cached_health", "created_at"),
            limit,
            offset,
        )

    @app.get("/decisions")
    async def get_decisions(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "decisions",
            ("id", "title", "state", "created_at"),
            limit,
            offset,
        )

    @app.get("/resources")
    async def get_resources(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "resources",
            ("id", "kind", "identity", "utilization_state", "created_at"),
            limit,
            offset,
        )

    # ---------------- POST /contest/{model_id} (Wave 4-C) -------------
    @app.post("/contest/{model_id}")
    async def post_contest(model_id: str, request: Request) -> JSONResponse:
        """Wave 4-C contestability endpoint per BUILD-PLAN §5 Prompt 4.C.

        Body:
          {
            "contestation_kind": "belief" | "reading",
            "contestor_actor_id": "<uuid>",  # optional; defaults to auth.actor_id
            "rationale": "<string>",
            "proposed_alternative": {...}   # optional
          }

        Returns 200 with the contestation observation id + new
        confidence. Returns 403 when the actor has no standing on the
        Model (per spec §11). Returns 404 when the Model does not
        exist. Auth + rate-limit middleware already ran — we do NOT
        touch them here.
        """
        from services.contestability import (
            ContestationInput,
            NoStandingError,
            contest_model,
        )

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover — middleware guarantees this
            return _unauth("missing_bearer")
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)

        try:
            target_model = UUID(model_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_model_id"}, status_code=400
            )
        kind = body.get("contestation_kind")
        if kind not in ("belief", "reading"):
            return JSONResponse(
                {"error": "invalid_contestation_kind"}, status_code=400
            )
        rationale = body.get("rationale")
        if not isinstance(rationale, str) or not rationale.strip():
            return JSONResponse(
                {"error": "rationale_required"}, status_code=400
            )
        contestor_raw = body.get("contestor_actor_id")
        if contestor_raw is None:
            contestor_id = auth.actor_id
        else:
            try:
                contestor_id = UUID(str(contestor_raw))
            except (ValueError, TypeError):
                return JSONResponse(
                    {"error": "invalid_contestor_actor_id"},
                    status_code=400,
                )
            # A session holder can only contest on behalf of the
            # actor they authenticated as. Wave 5-A adds delegation.
            if contestor_id != auth.actor_id:
                return JSONResponse(
                    {"error": "cannot_contest_on_behalf_of_others"},
                    status_code=403,
                )

        deps = _deps(request)
        inp = ContestationInput(
            model_id=target_model,
            contestor_actor_id=contestor_id,
            tenant_id=auth.tenant_id,
            contestation_kind=kind,
            rationale=rationale,
            proposed_alternative=body.get("proposed_alternative"),
        )
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    result = await contest_model(conn, inp)
        except NoStandingError as e:
            return JSONResponse(
                {"error": "no_standing", "detail": e.to_dict()},
                status_code=403,
            )
        except ValidationError as e:
            status_code = 404 if "does not exist" in (e.message or "") else 400
            return JSONResponse(
                {"error": "validation_error", "detail": e.to_dict()},
                status_code=status_code,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()},
                status_code=400,
            )
        return JSONResponse(
            {
                "observation_id": str(result.observation_id),
                "trigger_id": str(result.trigger_id) if result.trigger_id else None,
                "previous_confidence": result.previous_confidence,
                "new_confidence": result.new_confidence,
                "standing_basis": result.standing_basis,
                "override_applied": result.override_applied,
            },
            status_code=200,
        )

    # ---------------- Dashboard endpoints (Wave 5-B) ------------------
    # These wrap services/bridge/ for the UI. Each applies tenant
    # isolation via auth.tenant_id; the per-customer endpoint also
    # consults access_control.can_read_by_id on the customer Resource.
    @app.get("/dashboard/revenue-at-risk")
    async def get_dashboard_revenue_at_risk(
        request: Request, horizon_days: int = 90,
    ) -> dict[str, Any]:
        from services.bridge import render_revenue_at_risk
        auth: AuthContext = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_revenue_at_risk(
                auth.tenant_id, horizon_days=int(horizon_days), conn=conn
            )
        return json.loads(result.model_dump_json())

    @app.get("/dashboard/goals")
    async def get_dashboard_goals(request: Request) -> dict[str, Any]:
        from services.bridge import render_goals
        auth: AuthContext = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_goals(auth.tenant_id, conn=conn)
        return json.loads(result.model_dump_json())

    @app.get("/dashboard/capacity")
    async def get_dashboard_capacity(request: Request) -> dict[str, Any]:
        from services.bridge import render_capacity
        auth: AuthContext = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_capacity(auth.tenant_id, conn=conn)
        return json.loads(result.model_dump_json())

    @app.get("/dashboard/customer/{customer_id}")
    async def get_dashboard_customer(
        customer_id: str, request: Request, window_days: int = 30,
    ) -> Any:
        from services.access_control.checks import can_read_by_id
        from services.bridge import render_customer_detail

        auth: AuthContext = request.state.auth
        deps = _deps(request)
        try:
            cid = UUID(customer_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_customer_id"}, status_code=400
            )
        async with deps.pool.acquire() as conn:
            # Access-control check: customer Resource must be visible
            # to the caller. 5-A's decorator isn't applied here because
            # we want to surface a 404 vs 403 distinction cleanly and
            # pass the tenant through explicitly.
            decision = await can_read_by_id(
                auth.actor_id, "resource", cid,
                conn=conn, tenant_id=auth.tenant_id,
            )
            if not decision.allowed:
                status_code = 404 if decision.reason == "entity_not_found" else 403
                return JSONResponse(
                    {"error": "access_denied", "reason": decision.reason},
                    status_code=status_code,
                )
            try:
                result = await render_customer_detail(
                    cid, tenant_id=auth.tenant_id,
                    window_days=int(window_days), conn=conn,
                )
            except ValueError as e:
                return JSONResponse({"error": str(e)}, status_code=404)
        return json.loads(result.model_dump_json())

    # ---------------- WS /stream ------------------------------------
    # Wave 4-D mounts the real realtime router on startup via
    # `services.realtime.configure_realtime(app, pool=pool)`. The
    # previous Wave-5 accept-and-close stub has been removed; when
    # `configure_realtime` has not been called (e.g. legacy tests that
    # construct the app without a realtime wiring), WS /stream will
    # simply 404 — which is correct behavior for an unconfigured app.


async def _generic_list(
    request: Request,
    table: str,
    columns: tuple[str, ...],
    limit: int,
    offset: int,
) -> dict[str, Any]:
    """Reusable list-by-tenant stub for Wave 4 retrieval endpoints."""
    auth: AuthContext = request.state.auth
    deps = _deps(request)
    col_list = ", ".join(columns)
    query = (
        f"SELECT {col_list} FROM {table} "
        "WHERE tenant_id = $1 "
        "ORDER BY created_at DESC "
        "LIMIT $2 OFFSET $3"
    )
    rows = await deps.pool.fetch(
        query, auth.tenant_id, _clip(limit, 1, 500), max(offset, 0)
    )
    items: list[dict[str, Any]] = []
    for r in rows:
        item: dict[str, Any] = {}
        for c in columns:
            v = r[c]
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            elif isinstance(v, UUID):
                v = str(v)
            elif isinstance(v, (dict, list)):
                pass
            elif v is None:
                pass
            else:
                v = v
            item[c] = str(v) if isinstance(v, UUID) else v
        items.append(item)
    return {"items": items, "stub": True}


def _clip(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))


# ---------------------------------------------------------------------
# Week-4 Integration: CEO view wiring
# ---------------------------------------------------------------------


async def _configure_ceo_view(app_: FastAPI, *, pool: asyncpg.Pool) -> None:
    """Wire the four Week-4 routers (RND / GRT / QRY / SIM) onto the
    gateway app. Called from `_lifespan` after deps are initialised.

    Construction order:
      1. Rendering service (module singleton) — from env. RND's FastAPI
         routes are mounted via `services.rendering.api.router`.
      2. GRT scheduler + cache + stream manager. Scheduler gets a
         rendering adapter pointing at the same-process RND router via
         `GRT_RENDERING_BASE_URL` (if set) OR the `MockRenderingAdapter`.
      3. QRY handler + router, bound to the gateway pool and an HTTP
         rendering adapter. Env: `QUERY_RENDERING_BASE_URL` to flip to
         HTTP, `QUERY_CACHE_BACKEND=pg` to flip cache to Postgres.
      4. SIM router (simulation/server.py) is mounted read-only for
         authoring helpers (personas, channels, messages, inject). The
         SIM app owns its own state so we mount it via `app.mount`.

    All app state is stored under `app.state.ceo_view` so the lifespan
    teardown can stop the scheduler cleanly.
    """
    from uuid import UUID as _UUID

    # ---- 1. RND — rendering router ---------------------------------
    from services.rendering.api import (
        get_service as _rnd_get_service,
        router as rnd_router,
    )
    from services.rendering.core import RenderingService

    # Build the rendering service with the gateway pool so cost rows
    # land in `view_render_costs`.
    _rnd_service = RenderingService.from_env(pool=pool)
    app_.include_router(rnd_router)
    app_.dependency_overrides[_rnd_get_service] = lambda: _rnd_service

    # ---- 2. GRT — scheduler + stream + HTTP router -----------------
    from services.greeting.cache import ViewCeoCacheRepo
    from services.greeting.scheduler import GreetingScheduler, SchedulerConfig
    from services.greeting.snapshot import FounderContext
    from services.greeting.stream import (
        StaticTenantTokenMap,
        ViewCeoStreamManager,
        build_ceo_stream_router,
    )
    from services.greeting.api import build_ceo_api_router
    from services.greeting.rendering_adapter import build_rendering_adapter

    cache_repo = ViewCeoCacheRepo(pool)
    rendering_adapter = build_rendering_adapter()
    scheduler = GreetingScheduler(
        pool=pool,
        cache=cache_repo,
        rendering=rendering_adapter,
        config=SchedulerConfig(),
    )

    # Register the dogfood tenant (single-tenant) and token.
    default_tenant = os.environ.get("DEFAULT_TENANT_ID")
    ceo_token = os.environ.get("VIEW_CEO_TOKEN", "ceo-dogfood-token")
    token_map = StaticTenantTokenMap.from_env()
    if default_tenant:
        tid = _UUID(default_tenant)
        founder = FounderContext(
            tenant_id=tid,
            role="ceo",
            display_name=os.environ.get("VIEW_CEO_DISPLAY_NAME", "Rachin"),
            timezone_name=os.environ.get("VIEW_CEO_TIMEZONE", "Asia/Kathmandu"),
            observed_rhythms={},
        )
        scheduler.register_tenant(tid, founder)
        if ceo_token not in token_map.tokens:
            token_map.tokens[ceo_token] = tid
    stream_manager = ViewCeoStreamManager(token_map=token_map)

    # Tie stream → scheduler so cache writes publish to WS clients.
    from dataclasses import dataclass as _dc
    scheduler.set_stream_publisher(
        type("_SP", (), {"publish": staticmethod(stream_manager.publish)})()
    )

    # Only start the background loops if the integration flag is set;
    # tests might not want them running.
    if os.environ.get("GATEWAY_START_GRT_SCHEDULER", "1") != "0":
        await scheduler.start()

    app_.include_router(
        build_ceo_api_router(
            cache=cache_repo,
            scheduler=scheduler,
            stream_manager=stream_manager,
            default_tenant_id=_UUID(default_tenant) if default_tenant else None,
        )
    )
    app_.include_router(build_ceo_stream_router(stream_manager))

    # ---- 3. QRY — handler + router ---------------------------------
    from services.gateway.db_bootstrap import _register_codecs as _codec_hook  # noqa: F401
    from services.query.adapters import (
        build_cache_adapter as _build_qry_cache,
        build_rendering_adapter as _build_qry_rnd,
    )
    from services.query.core import QueryHandler
    from services.query.api import build_router as build_query_router

    qry_handler = QueryHandler(
        conn_provider=pool.acquire,
        rendering_adapter=_build_qry_rnd(),
        cache_adapter=_build_qry_cache(pool=pool),
    )
    default_tenant_uuid = _UUID(default_tenant) if default_tenant else None
    app_.include_router(
        build_query_router(qry_handler, default_tenant_id=default_tenant_uuid),
    )

    # ---- 4. SIM — authoring-side endpoints -------------------------
    # Week 5: `simulation.server.build_sim_router(deps)` returns a plain
    # APIRouter that does NOT own a pool or lifespan. We share the
    # gateway pool and a lazily-constructed embedder; the standalone
    # `simulation.server:app` continues to work via its own app factory.
    #
    # Default ON in dev/test, OFF in prod. Set `GATEWAY_MOUNT_SIM=0` to
    # force off regardless of environment.
    env_name = os.environ.get("COMPANY_OS_ENV", "dev").lower()
    _mount_sim_default = "0" if env_name == "prod" else "1"
    if os.environ.get("GATEWAY_MOUNT_SIM", _mount_sim_default) == "1":
        try:
            from simulation.server import SimDeps, build_sim_router
            from simulation.workers._common import (
                _resolve_run_id, _resolve_tenant_id, ensure_personas_seeded,
            )

            sim_tenant = _resolve_tenant_id(None)
            sim_run = _resolve_run_id(None)
            try:
                await ensure_personas_seeded(pool, sim_tenant)
            except Exception as _seed_exc:  # noqa: BLE001
                log.warning(
                    "sim_persona_seed_failed", error=str(_seed_exc),
                )
            sim_deps = SimDeps(
                pool=pool,
                tenant_id=sim_tenant,
                run_id=sim_run,
                embedder=getattr(app_.state, "deps", None).embedder
                if getattr(app_.state, "deps", None) is not None else None,
                actor_repo=ActorRepo(pool),
                alias_repo=EntityAliasRepo(pool),
            )
            app_.include_router(build_sim_router(sim_deps))
            app_.state.sim_deps = sim_deps
            # Mount slack_ui static files at /simulation/slack_ui so the
            # bundled HTML/JS composer is usable without running the
            # standalone sim app on a second port.
            try:
                import pathlib as _pl
                from fastapi.staticfiles import StaticFiles as _StaticFiles
                _static_dir = (
                    _pl.Path(__file__).resolve().parents[2]
                    / "simulation" / "slack_ui"
                )
                if _static_dir.is_dir() and not any(
                    getattr(r, "name", None) == "slack_ui_static"
                    for r in app_.routes
                ):
                    app_.mount(
                        "/simulation/slack_ui",
                        _StaticFiles(directory=str(_static_dir), html=True),
                        name="slack_ui_static",
                    )
            except Exception as exc:  # noqa: BLE001
                log.warning("sim_static_mount_failed", error=str(exc))
        except Exception as exc:  # noqa: BLE001
            log.warning("sim_mount_failed", error=str(exc))

    # ---- 5. DEBUG — inspector router -------------------------------
    # Read-only endpoints for /debug UI: signals, think runs, models,
    # acts, renders, cache. Gated by COMPANY_OS_ENV so prod doesn't
    # leak raw prompts + substrate.
    if env_name in ("dev", "staging", "test"):
        try:
            from services.gateway.debug_router import build_debug_router
            app_.include_router(build_debug_router())
        except Exception as exc:  # noqa: BLE001
            log.warning("debug_router_mount_failed", error=str(exc))

    # Expose under a common state bag for observability + teardown.
    app_.state.ceo_view = {
        "scheduler": scheduler,
        "cache": cache_repo,
        "stream_manager": stream_manager,
        "rendering_adapter": rendering_adapter,
        "qry_handler": qry_handler,
        "tenant_id": _UUID(default_tenant) if default_tenant else None,
        "token": ceo_token,
    }


# The module-level `app` used by `uvicorn services.gateway:app`. Lazy
# initialised (lifespan handles pool / repo / embedder wiring).
app = build_app()


__all__ = ["app", "build_app", "GatewayDeps"]
