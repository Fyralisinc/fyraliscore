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
    # Demo picker page calls these from an unauthenticated browser; the
    # /sessions/start endpoint mints the auth token for everything else.
    "/v1/demo/companies",
    "/v1/demo/sessions/start",
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
            # Public paths skip auth, BUT if the caller passes a demo
            # bearer token we still resolve it and inject X-Tenant-Id
            # so the CEO-view sub-router's `tenant_dep` (which reads
            # the header) finds the demo tenant. Without this, the
            # bottom AskZone hits /view/ceo/ask with a bearer token
            # only and the dep raises "x-tenant-id header required".
            authz = request.headers.get("Authorization", "")
            if authz.startswith("Bearer "):
                token = authz[len("Bearer ") :].strip()
                if token:
                    deps = _deps(request)
                    ctx = await validate_token(deps.pool, token)
                    if ctx is not None:
                        request.state.auth = ctx
                        hdr_tenant = request.headers.get("X-Tenant-Id")
                        if not hdr_tenant:
                            tenant_str = str(ctx.tenant_id).encode("latin-1")
                            new_headers = [
                                (n, v)
                                for (n, v) in request.scope["headers"]
                                if n.lower() != b"x-tenant-id"
                            ]
                            new_headers.append((b"x-tenant-id", tenant_str))
                            request.scope["headers"] = new_headers
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
        # Inject the bearer-resolved tenant into the request headers so
        # downstream routers that resolve tenant via `X-Tenant-Id`
        # (services.query.api.tenant_dep, services.greeting.api, …) work
        # under demo bearer auth without forcing every client to send
        # the header explicitly. Demo sessions don't expose tenant_id
        # to the browser so the UI can't send it.
        if not hdr_tenant:
            tenant_str = str(ctx.tenant_id).encode("latin-1")
            new_headers = [
                (name, value)
                for (name, value) in request.scope["headers"]
                if name.lower() != b"x-tenant-id"
            ]
            new_headers.append((b"x-tenant-id", tenant_str))
            request.scope["headers"] = new_headers
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

    # Mount the demo router (Session 1 of DEMO-BUILD-PLAN). Adds the
    # picker, session lifecycle, simulator, and SSE recommendation
    # stream under /v1/demo/* (and /v1/recommendations/stream).
    from services.demo.router import demo_router as _demo_router

    app.include_router(_demo_router)
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

    # ---------------- /v1/recommendations (Stage 1 decision support) -
    # Three endpoints: list (ranker), act, dismiss. All require an
    # actor session (BearerAuthMiddleware). Authorization rule for v1:
    # the requesting actor must be the queried/owning actor — Wave 5-A
    # delegation is not yet wired in here. See
    # services/recommendations/{repo,handlers}.py for the read/write
    # surfaces this thin route layer wraps.
    @app.get("/v1/recommendations")
    async def list_recommendations(request: Request) -> JSONResponse:
        from services.recommendations.repo import list_for_actor

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover — middleware guarantees this
            return _unauth("missing_bearer")

        actor_param = request.query_params.get("actor_id")
        if actor_param is None:
            target_actor = auth.actor_id
        else:
            try:
                target_actor = UUID(str(actor_param))
            except (ValueError, TypeError):
                return JSONResponse(
                    {"error": "invalid_actor_id"}, status_code=400
                )
            # v1: only the actor themselves can query their own action
            # list. Delegation lands with Wave 5-A.
            if target_actor != auth.actor_id:
                return JSONResponse(
                    {"error": "forbidden",
                     "reason": "cross_actor_access_not_supported"},
                    status_code=status.HTTP_403_FORBIDDEN,
                )

        limit_raw = request.query_params.get("limit", "15")
        try:
            limit = max(1, min(100, int(limit_raw)))
        except (ValueError, TypeError):
            return JSONResponse({"error": "invalid_limit"}, status_code=400)

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            views = await list_for_actor(
                tenant_id=auth.tenant_id,
                target_actor_id=target_actor,
                limit=limit,
                conn=conn,
            )

        return JSONResponse(
            {
                "items": [_serialize_recommendation(v) for v in views],
                "count": len(views),
            },
            status_code=200,
        )

    @app.post("/v1/recommendations/{recommendation_id}/act")
    async def act_on_recommendation_endpoint(
        recommendation_id: str, request: Request,
    ) -> JSONResponse:
        from services.recommendations.handlers import (
            AlreadyArchivedError,
            act_on_recommendation,
        )

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400
            )

        try:
            body = await request.json() if (await request.body()) else {}
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        notes_raw = body.get("notes") if isinstance(body, dict) else None
        notes = (
            str(notes_raw).strip()
            if isinstance(notes_raw, str) and notes_raw.strip()
            else None
        )

        deps = _deps(request)
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    # v1 access policy: only the recommendation's
                    # target_actor_id can act on it.
                    target_row = await conn.fetchrow(
                        "SELECT target_actor_id FROM models "
                        "WHERE id = $1 AND tenant_id = $2 "
                        "  AND proposition_kind = 'recommendation'",
                        rec_id, auth.tenant_id,
                    )
                    if target_row is None:
                        return JSONResponse(
                            {"error": "not_found"}, status_code=404,
                        )
                    if target_row["target_actor_id"] != auth.actor_id:
                        return JSONResponse(
                            {"error": "forbidden",
                             "reason": "not_target_actor"},
                            status_code=403,
                        )

                    result = await act_on_recommendation(
                        recommendation_id=rec_id,
                        actor_id=auth.actor_id,
                        tenant_id=auth.tenant_id,
                        notes=notes,
                        conn=conn,
                    )
        except AlreadyArchivedError as e:
            return JSONResponse(
                {"error": "already_archived", "detail": e.to_dict()},
                status_code=409,
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
                "recommendation_id": str(result.recommendation_id),
                "target_act_change_kind": result.target_act_change_kind,
                "target_act_change_id": str(result.target_act_change_id),
            },
            status_code=200,
        )

    @app.post("/v1/recommendations/{recommendation_id}/dismiss")
    async def dismiss_recommendation_endpoint(
        recommendation_id: str, request: Request,
    ) -> JSONResponse:
        from services.recommendations.handlers import (
            AlreadyArchivedError,
            dismiss_recommendation,
        )

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400
            )

        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        reason = (body or {}).get("reason")
        if not isinstance(reason, str) or not reason.strip():
            return JSONResponse(
                {"error": "reason_required"}, status_code=400,
            )

        deps = _deps(request)
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    target_row = await conn.fetchrow(
                        "SELECT target_actor_id FROM models "
                        "WHERE id = $1 AND tenant_id = $2 "
                        "  AND proposition_kind = 'recommendation'",
                        rec_id, auth.tenant_id,
                    )
                    if target_row is None:
                        return JSONResponse(
                            {"error": "not_found"}, status_code=404,
                        )
                    if target_row["target_actor_id"] != auth.actor_id:
                        return JSONResponse(
                            {"error": "forbidden",
                             "reason": "not_target_actor"},
                            status_code=403,
                        )

                    await dismiss_recommendation(
                        recommendation_id=rec_id,
                        actor_id=auth.actor_id,
                        tenant_id=auth.tenant_id,
                        reason=reason,
                        conn=conn,
                    )
        except AlreadyArchivedError as e:
            return JSONResponse(
                {"error": "already_archived", "detail": e.to_dict()},
                status_code=409,
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
                "recommendation_id": str(rec_id),
                "archived_with_reason": reason.strip(),
            },
            status_code=200,
        )

    # ---------------- /v1/today (Fyralis Today aggregator) -------
    # The Today UI (ui/src/App.tsx) consumes a single payload that
    # combines recommendations + signal strip + vitals + state line.
    # services.today.aggregator owns the substrate→UI mapping.
    @app.get("/v1/today")
    async def today_endpoint(request: Request) -> JSONResponse:
        from services.today import build_today

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover — middleware enforces
            return _unauth("missing_bearer")

        actor_param = request.query_params.get("actor_id")
        target_actor = auth.actor_id
        if actor_param is not None:
            try:
                target_actor = UUID(str(actor_param))
            except (ValueError, TypeError):
                return JSONResponse(
                    {"error": "invalid_actor_id"}, status_code=400,
                )
            if target_actor != auth.actor_id:
                return JSONResponse(
                    {"error": "forbidden",
                     "reason": "cross_actor_access_not_supported"},
                    status_code=status.HTTP_403_FORBIDDEN,
                )

        deps = _deps(request)
        # Pull the actor's display name for the ask-zone suggestions.
        display_name: str | None = None
        async with deps.pool.acquire() as conn:
            actor_row = await conn.fetchrow(
                "SELECT display_name FROM actors WHERE id = $1 AND tenant_id = $2",
                target_actor, auth.tenant_id,
            )
            if actor_row is not None:
                display_name = actor_row["display_name"]

            tenant_row = await conn.fetchrow(
                "SELECT min(ingested_at) AS first_seen FROM observations "
                "WHERE tenant_id = $1",
                auth.tenant_id,
            )
            days_since = 1
            if tenant_row and tenant_row["first_seen"] is not None:
                from datetime import datetime as _dt, timezone as _tz
                delta = _dt.now(_tz.utc) - tenant_row["first_seen"]
                days_since = max(1, int(delta.days) + 1)

            # Read brand_name override from a per-tenant key/value if
            # the tenant has one; default to "Fyralis" otherwise.
            brand_row = await conn.fetchrow(
                "SELECT current_value FROM resources "
                "WHERE tenant_id = $1 AND kind = 'ip' "
                "  AND identity = 'fyralis.brand_name' "
                "  AND archived_at IS NULL "
                "ORDER BY last_updated_at DESC LIMIT 1",
                auth.tenant_id,
            )
            brand_name = "Fyralis"
            if brand_row is not None:
                cv = brand_row["current_value"] or {}
                if isinstance(cv, str):
                    try:
                        cv = json.loads(cv)
                    except json.JSONDecodeError:
                        cv = {}
                if isinstance(cv, dict) and isinstance(cv.get("name"), str):
                    brand_name = cv["name"]

            payload = await build_today(
                tenant_id=auth.tenant_id,
                actor_id=target_actor,
                actor_display_name=display_name,
                brand_name=brand_name,
                conn=conn,
                days_since_inception=days_since,
            )
        return JSONResponse(payload.to_dict(), status_code=200)

    @app.post("/v1/today/brand")
    async def today_brand_endpoint(request: Request) -> JSONResponse:
        """Persist a per-tenant brand-name override (the user clicks the
        wordmark and renames Fyralis to anything that better fits the
        company's self-perception, per spec §10.5)."""
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        new_name = (body or {}).get("name")
        if not isinstance(new_name, str) or not new_name.strip():
            return JSONResponse({"error": "name_required"}, status_code=400)
        new_name = new_name.strip()[:64]

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT id FROM resources "
                    "WHERE tenant_id = $1 AND kind = 'ip' "
                    "  AND identity = 'fyralis.brand_name' "
                    "  AND archived_at IS NULL",
                    auth.tenant_id,
                )
                if existing is None:
                    await conn.execute(
                        """
                        INSERT INTO resources (
                            id, tenant_id, kind, identity, current_value,
                            created_at, last_updated_at
                        ) VALUES ($1, $2, 'ip', 'fyralis.brand_name',
                                  $3::jsonb, now(), now())
                        """,
                        uuid7(), auth.tenant_id,
                        json.dumps({"name": new_name}),
                    )
                else:
                    await conn.execute(
                        "UPDATE resources SET current_value = $2::jsonb, "
                        "last_updated_at = now() WHERE id = $1",
                        existing["id"], json.dumps({"name": new_name}),
                    )
        return JSONResponse(
            {"ok": True, "name": new_name}, status_code=200,
        )

    @app.post("/v1/recommendations/{recommendation_id}/triage")
    async def triage_recommendation_endpoint(
        recommendation_id: str, request: Request,
    ) -> JSONResponse:
        """Generic triage endpoint covering hold / route / snooze /
        dismiss. `act` keeps its dedicated `/act` endpoint because it
        applies the proposed_change. Everything else archives with
        `archive_reason='manual'` and records the actor's intent in
        the audit-trail Observation."""
        from services.today import (
            TriageError,
            triage_recommendation,
        )
        from services.recommendations.handlers import (
            AlreadyArchivedError,
        )

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400,
            )
        try:
            body = await request.json() if (await request.body()) else {}
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        action_raw = body.get("action")
        if action_raw not in ("hold", "route", "snooze", "dismiss"):
            return JSONResponse(
                {"error": "invalid_action",
                 "reason": "use /act for act; one of hold/route/snooze/dismiss here"},
                status_code=400,
            )

        reason = body.get("reason") if isinstance(body.get("reason"), str) else None
        routed_to = body.get("routed_to") if isinstance(body.get("routed_to"), str) else None
        snooze_until_raw = body.get("snooze_until")
        snooze_until = None
        if isinstance(snooze_until_raw, str) and snooze_until_raw.strip():
            try:
                from datetime import datetime as _dt
                snooze_until = _dt.fromisoformat(snooze_until_raw)
            except ValueError:
                return JSONResponse(
                    {"error": "invalid_snooze_until"}, status_code=400,
                )

        deps = _deps(request)
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    target_row = await conn.fetchrow(
                        "SELECT target_actor_id FROM models "
                        "WHERE id = $1 AND tenant_id = $2 "
                        "  AND proposition_kind = 'recommendation'",
                        rec_id, auth.tenant_id,
                    )
                    if target_row is None:
                        return JSONResponse(
                            {"error": "not_found"}, status_code=404,
                        )
                    if target_row["target_actor_id"] != auth.actor_id:
                        return JSONResponse(
                            {"error": "forbidden",
                             "reason": "not_target_actor"},
                            status_code=403,
                        )
                    result = await triage_recommendation(
                        recommendation_id=rec_id,
                        actor_id=auth.actor_id,
                        tenant_id=auth.tenant_id,
                        action=action_raw,
                        reason=reason,
                        routed_to=routed_to,
                        snooze_until=snooze_until,
                        conn=conn,
                    )
        except AlreadyArchivedError as e:
            return JSONResponse(
                {"error": "already_archived", "detail": e.to_dict()},
                status_code=409,
            )
        except TriageError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()},
                status_code=400,
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
                "ok": True,
                "recommendation_id": str(result.recommendation_id),
                "action": result.action,
            },
            status_code=200,
        )

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


def _serialize_recommendation(view: Any) -> dict[str, Any]:
    """Hand-rolled JSON projection for /v1/recommendations responses.

    Keeps the API stable independently of `RecommendationView` field
    layout. The action-list UI consumes this shape directly.
    """
    target = view.target_entity
    return {
        "id": str(view.id),
        "proposition_text": view.proposition_text,
        "confidence": view.confidence,
        "target_act_ref": view.target_act_ref,
        "proposed_change": view.proposed_change,
        "expected_impact": view.expected_impact,
        "qualitative_impact": view.qualitative_impact,
        "target_actor_id": str(view.target_actor_id),
        "supporting_event_ids": [str(x) for x in view.supporting_event_ids],
        "supporting_model_ids": [str(x) for x in view.supporting_model_ids],
        "created_at": view.created_at.isoformat(),
        "scope_entities": view.scope_entities,
        "rank_score": view.rank_score,
        "target_entity": (
            {
                "type": target.type,
                "id": str(target.id),
                "title": target.title,
                "state": target.state,
                "archived": target.archived,
            }
            if target is not None
            else None
        ),
    }


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

    # Reuse the gateway's shared Ollama embedder so QRY pathway B
    # (semantic) can vectorise the seed text. Without this, retrieval
    # silently skips Pathway B and the LLM gets an empty context, so
    # /view/ceo/ask answers come back with "0 observations / 0 models".
    deps = getattr(app_.state, "deps", None)
    qry_embedder = deps.embedder if deps is not None else None
    qry_handler = QueryHandler(
        conn_provider=pool.acquire,
        rendering_adapter=_build_qry_rnd(),
        cache_adapter=_build_qry_cache(pool=pool),
        embedder=qry_embedder,
    )
    default_tenant_uuid = _UUID(default_tenant) if default_tenant else None
    app_.include_router(
        build_query_router(qry_handler, default_tenant_id=default_tenant_uuid),
    )

    # ---- 3.5 Card conversations (Driftwood revision) ---------------
    from services.conversations import (
        ConversationRepo,
        ProbeHandler,
        build_router as build_conversations_router,
    )

    conv_repo = ConversationRepo(pool)
    probe_handler = ProbeHandler(
        repo=conv_repo, pool=pool, query_handler=qry_handler,
    )
    app_.include_router(
        build_conversations_router(repo=conv_repo, handler=probe_handler)
    )
    app_.state.conversations = {"repo": conv_repo, "handler": probe_handler}

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
