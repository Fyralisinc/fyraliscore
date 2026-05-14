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
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request,
    Response,
    WebSocket,
    status,
)
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
_PUBLIC_PATHS = frozenset({
    "/healthz",
    "/auth/session",
    # IN-08: the OAuth callback is public (state-token-authed inside
    # the handler). The /install route stays Bearer-required, so it is
    # NOT in this allowlist. We deliberately do NOT add "/integrations/"
    # as a prefix entry — single-route, not blanket public.
    "/integrations/slack/callback",
})

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
    "/api/debug/",
    # Demo picker page calls these from an unauthenticated browser; the
    # /sessions/start endpoint mints the auth token for everything else.
    "/v1/demo/companies",
    "/v1/demo/sessions/start",
    # IN-06: webhook ingress. Authentication is the per-provider
    # cryptographic signature check inside services.webhooks.router —
    # NOT a Bearer token. The Bearer middleware MUST skip this prefix
    # or every webhook becomes a 401 with `missing_bearer`.
    "/webhooks/",
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
# Ingest body guard — IN-01
# ---------------------------------------------------------------------


class IngestSizeError(Exception):
    """Raised by `ingest_body_bytes` when a request body is rejected
    before (or during a bounded read of) ingest. The paired handler
    `ingest_size_error_handler` renders `payload` verbatim so callers see
    a flat `{"error": "...", ...}` body, matching pre-IN-01 shape rather
    than FastAPI's `{"detail": {...}}` wrapping.
    """

    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        super().__init__(payload.get("error", "ingest_size_error"))
        self.status_code = status_code
        self.payload = payload


async def ingest_size_error_handler(
    request: Request, exc: IngestSizeError
) -> JSONResponse:
    return JSONResponse(exc.payload, status_code=exc.status_code)


async def ingest_body_bytes(request: Request) -> bytes:
    """FastAPI dependency for POST /ingest/* — enforces payload limits
    before the body hits memory and returns the validated bytes.

    Order matters:
      1. Reject `Transfer-Encoding: chunked` (no streaming-ingest support
         in Wave 2; a chunked sender bypasses Content-Length entirely).
      2. Reject when `Content-Length` exceeds `MAX_PAYLOAD_BYTES` — this
         is the OOM-amplification fix: no body byte is read.
      3. Stream-read with a byte counter that trips at the same limit;
         defense in depth when `Content-Length` is absent or lies.
    """
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


def _wire_in08_state(app_: FastAPI, pool: asyncpg.Pool) -> None:
    """Wire IN-08 dependencies onto `app_.state`.

    Idempotent — checks for existing attributes so repeated calls
    (test path + lifespan path) don't double-construct. Both the
    secret store and the tenant resolver are required by the webhook
    router cutover in IN-08, so they're wired together here.

    Also pins `app_.state.pool` (some legacy code reads it from
    `app_.state.deps.pool` only; the new secret-resolution path reads
    it directly off `app_.state` for simpler call sites) and asserts
    the prod-safety invariants exactly once at startup.

    Defers imports to function scope to avoid pulling lib.shared.secrets
    and services.webhooks.tenant_resolver into the module-load graph
    when tests don't need them.
    """
    import time

    from lib.shared.secrets import build_secret_store
    from services.webhooks.secrets import assert_prod_safety_invariants
    from services.webhooks.tenant_resolver import (
        InstallationCache,
        TenantResolverDeps,
        build_tenant_resolver,
        default_metrics,
    )

    # IN-08 SC-002: refuse to start if prod has the env-var fallback on.
    assert_prod_safety_invariants()

    # Pin pool on app.state for the new code paths.
    if getattr(app_.state, "pool", None) is None:
        app_.state.pool = pool

    if getattr(app_.state, "secret_store", None) is None:
        app_.state.secret_store = build_secret_store(pool)

    if getattr(app_.state, "tenant_resolver", None) is None:
        app_.state.tenant_resolver = build_tenant_resolver(
            TenantResolverDeps(
                pool=pool,
                cache=InstallationCache(),
                clock=time.monotonic,
                metrics=default_metrics(),
            )
        )


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
        # IN-08: wire the envelope-encrypted secret store and the
        # DB-backed TenantResolver. The webhook router and the new
        # integrations router both read these from `request.app.state`.
        _wire_in08_state(app_, pool)

        # IN-08 T031: start the oauth_install_states sweep task. Runs
        # every 5 min in the gateway process, deletes rows older than
        # 1h (whether expired or consumed). Bounded by LIMIT 1000 so
        # a sudden backlog can't lock the table.
        import asyncio as _asyncio

        async def _sweep_oauth_states() -> None:
            while True:
                try:
                    await _asyncio.sleep(300)  # 5 min
                    deleted = await pool.execute(
                        """
                        DELETE FROM oauth_install_states
                         WHERE id IN (
                            SELECT id FROM oauth_install_states
                             WHERE expires_at < now() - INTERVAL '1 hour'
                                OR (consumed_at IS NOT NULL
                                    AND consumed_at < now() - INTERVAL '1 hour')
                             LIMIT 1000
                         )
                        """,
                    )
                    log.info(
                        "oauth_install_states_sweep",
                        deleted_summary=deleted,
                    )
                except _asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.error(
                        "oauth_install_states_sweep_error",
                        error_type=type(exc).__name__,
                    )
                    # Continue looping; transient DB hiccups should
                    # not kill the sweep task.

        app_.state.oauth_sweep_task = _asyncio.create_task(_sweep_oauth_states())
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
            # IN-08: cancel the oauth_install_states sweep task.
            sweep_task = getattr(app_.state, "oauth_sweep_task", None)
            if sweep_task is not None:
                sweep_task.cancel()
                try:
                    await sweep_task
                except (BaseException,):  # noqa: BLE001
                    pass
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
        # IN-08: same wiring as the lifespan path, for tests that
        # pre-build the app synchronously and skip lifespan startup.
        _wire_in08_state(app, pool)

    # Middleware order: add last → first to run.
    # Each middleware resolves deps lazily from request.app.state so
    # it tolerates deps being wired in lifespan startup (default path).
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(BearerAuthMiddleware)
    app.add_middleware(RequestContextMiddleware)

    app.add_exception_handler(IngestSizeError, ingest_size_error_handler)

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

    # IN-06: webhook gateway router. Mounted at /webhooks/{provider}/...
    # Bearer middleware is configured to skip this prefix (see
    # `_PUBLIC_PATH_PREFIXES`). Verification is per-provider signature.
    from services.webhooks.router import build_webhooks_router

    app.include_router(build_webhooks_router())

    # IN-08: integrations router (Slack OAuth install + callback).
    # Mounted at /integrations/{provider}/*. The /install route is
    # Bearer-authed (standard middleware); only /callback is in the
    # public-paths allowlist (exact match, no prefix exposure).
    from services.integrations.router import build_integrations_router

    app.include_router(build_integrations_router())
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
    async def post_ingest(
        channel: str,
        request: Request,
        raw: bytes = Depends(ingest_body_bytes),
    ) -> JSONResponse:
        deps = _deps(request)
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:
            return _unauth("missing_bearer")
        # Slack signature check — only for slack:message (the one
        # signature-verified channel in Wave 2-A). Uses the same bytes
        # the dependency assembled so the HMAC sees the wire payload.
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

    # ---------------- /v1/recommendations/{id}/watch -----------------
    # Per-actor "Watch for revision" subscription on a falsifier
    # predicate. The substrate stores the row; T2 cascade work that
    # detects predicate firing lands later. See
    # services/recommendations/watchers.py + migration 0027.
    @app.post("/v1/recommendations/{recommendation_id}/watch")
    async def watch_recommendation_endpoint(
        recommendation_id: str, request: Request,
    ) -> JSONResponse:
        from services.recommendations.watchers import create_watch

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
        predicate_raw = body.get("predicate")
        if not isinstance(predicate_raw, str) or not predicate_raw.strip():
            return JSONResponse(
                {"error": "predicate_required"}, status_code=400,
            )
        predicate = predicate_raw.strip()

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            watch_id = await create_watch(
                tenant_id=auth.tenant_id,
                recommendation_id=rec_id,
                actor_id=auth.actor_id,
                predicate=predicate,
                conn=conn,
            )
        return JSONResponse(
            {
                "ok": True,
                "watch_id": str(watch_id),
                "recommendation_id": str(rec_id),
            },
            status_code=200,
        )

    @app.delete("/v1/recommendations/{recommendation_id}/watch")
    async def unwatch_recommendation_endpoint(
        recommendation_id: str, request: Request,
    ) -> JSONResponse:
        from services.recommendations.watchers import clear_watch

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400,
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            await clear_watch(
                tenant_id=auth.tenant_id,
                recommendation_id=rec_id,
                actor_id=auth.actor_id,
                conn=conn,
            )
        return JSONResponse({"ok": True}, status_code=200)

    # ---------------- /v1/artifacts/{type}/{id} ----------------------
    # Tenant-scoped read of any artifact referenced from the Today UI.
    # Powers the dotted-underline drawer that opens when the user clicks
    # an artifact mention. Returns a small, type-specific payload.
    @app.get("/v1/artifacts/{artifact_type}/{artifact_id}")
    async def get_artifact_endpoint(
        artifact_type: str, artifact_id: str, request: Request,
    ) -> JSONResponse:
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            aid = UUID(artifact_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_artifact_id"}, status_code=400,
            )
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            payload = await _fetch_artifact(
                artifact_type, aid, auth.tenant_id, conn,
            )
        if payload is None:
            return JSONResponse(
                {"error": "not_found", "type": artifact_type},
                status_code=404,
            )
        return JSONResponse(payload, status_code=200)

    # ---------------- /v1/structure/overlay/{commitment_id} ------
    # Returns a single commitment plus the related goal / customer /
    # owner data needed to overlay it onto the Structure page's
    # in-memory sample graph. Used after a create_commitment
    # recommendation is accepted, so the freshly-created entity can
    # appear in the relational view without a full DB-backed graph.
    @app.get("/v1/structure/overlay/{commitment_id}")
    async def structure_overlay_endpoint(
        commitment_id: str, request: Request,
    ) -> JSONResponse:
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            cid = UUID(commitment_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_commitment_id"}, status_code=400,
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            bundle = await _fetch_commitment_overlay(
                cid, auth.tenant_id, conn,
            )
        if bundle is None:
            return JSONResponse(
                {"error": "not_found"}, status_code=404,
            )
        return JSONResponse(bundle, status_code=200)

    # ---------------- /v1/structure/recent ------------------------
    # Returns commitments created within the last `since_minutes`
    # window, plus their related goal / customer / owner entities.
    # Structure.tsx fetches this on mount so freshly-auto-accepted
    # commitments (from `_maybe_auto_accept`) appear in the relational
    # view without the user knowing the new commitment's UUID.
    @app.get("/v1/structure/recent")
    async def structure_recent_endpoint(
        request: Request, since_minutes: int = 10,
    ) -> JSONResponse:
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")

        # since_minutes=0 (or any non-positive) signals "return all
        # active commitments for this tenant" — used by Structure.tsx
        # on initial load so a freshly-loaded snapshot populates the
        # full graph, not just the live-overlay window.
        try:
            raw_minutes = int(since_minutes)
        except (ValueError, TypeError):
            raw_minutes = 10
        all_active = raw_minutes <= 0
        window_minutes = max(1, min(1440 * 365, raw_minutes))

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            if all_active:
                rows = await conn.fetch(
                    "SELECT id FROM commitments "
                    "WHERE tenant_id = $1 "
                    "  AND terminal_at IS NULL "
                    "ORDER BY last_state_change_at DESC NULLS LAST, "
                    "         created_at DESC "
                    "LIMIT 500",
                    auth.tenant_id,
                )
            else:
                rows = await conn.fetch(
                    "SELECT id FROM commitments "
                    "WHERE tenant_id = $1 "
                    "  AND ( "
                    "    created_at >= now() - ($2 || ' minutes')::interval "
                    "    OR last_state_change_at >= now() - ($2 || ' minutes')::interval "
                    "  ) "
                    "  AND terminal_at IS NULL "
                    "ORDER BY last_state_change_at DESC NULLS LAST, "
                    "         created_at DESC "
                    "LIMIT 500",
                    auth.tenant_id, str(window_minutes),
                )

            commitments_payload: list[dict[str, Any]] = []
            goals_by_id: dict[str, dict[str, Any]] = {}
            people_by_id: dict[str, dict[str, Any]] = {}
            customers_by_id: dict[str, dict[str, Any]] = {}
            decisions_by_id: dict[str, dict[str, Any]] = {}
            resources_by_id: dict[str, dict[str, Any]] = {}

            for r in rows:
                cid = r["id"]
                bundle = await _fetch_commitment_overlay(
                    cid, auth.tenant_id, conn,
                )
                if bundle is None:
                    continue
                commitments_payload.append(bundle["commitment"])
                for g in bundle["goals"]:
                    goals_by_id.setdefault(g["id"], g)
                for p in bundle["people"]:
                    people_by_id.setdefault(p["id"], p)
                for c in bundle["customers"]:
                    customers_by_id.setdefault(c["id"], c)
                for d in bundle.get("decisions", []):
                    decisions_by_id.setdefault(d["id"], d)
                for rs in bundle.get("resources", []):
                    # Strip the per-commitment deployed_quantity here
                    # since the global list represents the resource
                    # itself, not its slice on any one commitment.
                    rid = rs["id"]
                    if rid not in resources_by_id:
                        resources_by_id[rid] = {
                            "id": rid,
                            "label": rs["label"],
                            "kind": rs["kind"],
                            "unit": rs.get("unit"),
                        }

            # Always include the tenant's full active goal tree so
            # strategic parents (which usually have no direct commits)
            # still show up alongside the commit-linked operational
            # goals — needed for the goal hierarchy in the list rail
            # and aggregate graph.
            goal_all_rows = await conn.fetch(
                "SELECT id, title, altitude, parent_goal_id FROM goals "
                "WHERE tenant_id = $1 AND archived_at IS NULL "
                "ORDER BY altitude, title "
                "LIMIT 200",
                auth.tenant_id,
            )
            for gr in goal_all_rows:
                gid = str(gr["id"])
                if gid in goals_by_id:
                    # Already merged via commitment overlay — leave it
                    # untouched (carries the same parent_goal_id).
                    continue
                altitude = (
                    gr["altitude"] if gr["altitude"] in ("strategic", "operational")
                    else "operational"
                )
                goals_by_id[gid] = {
                    "id": gid,
                    "label": gr["title"],
                    "altitude": altitude,
                    "parent_goal_id": (
                        str(gr["parent_goal_id"]) if gr["parent_goal_id"] else None
                    ),
                }

            # Always include the tenant's full active human roster so
            # the Structure Team section reflects real DB actors, not
            # only the people tied to recent commitments.
            actor_rows = await conn.fetch(
                "SELECT id, display_name, metadata FROM actors "
                "WHERE tenant_id = $1 AND status = 'active' "
                "  AND type IN ('human_internal', 'human') "
                "ORDER BY display_name "
                "LIMIT 80",
                auth.tenant_id,
            )
            # Always overwrite role with the actor-metadata-derived
            # canonical role. The commitment-overlay path tags actors as
            # "Owner"/"Contributor" relative to a commitment, but for
            # the team list we want the actor's actual title.
            for ar in actor_rows:
                aid = str(ar["id"])
                md = ar["metadata"]
                if isinstance(md, str):
                    try:
                        md = json.loads(md)
                    except json.JSONDecodeError:
                        md = {}
                elif not isinstance(md, dict):
                    md = {}
                role = md.get("title") or md.get("role") or "Team member"
                people_by_id[aid] = {
                    "id": aid,
                    "label": ar["display_name"],
                    "role": role,
                }

        return JSONResponse(
            {
                "commitments": commitments_payload,
                "goals": list(goals_by_id.values()),
                "people": list(people_by_id.values()),
                "customers": list(customers_by_id.values()),
                "decisions": list(decisions_by_id.values()),
                "resources": list(resources_by_id.values()),
            },
            status_code=200,
        )

    # ---------------- /v1/structure/resources/aggregate ----------
    # Returns the full capacity-resource portfolio for the tenant with
    # derived utilization metrics. Drives the Resources view in
    # Structure.tsx — overall utilization, top consumers per resource,
    # underutilized vs over-allocated callouts.
    @app.get("/v1/structure/resources/aggregate")
    async def structure_resources_aggregate(
        request: Request,
    ) -> JSONResponse:
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            res_rows = await conn.fetch(
                "SELECT id, kind, identity, description, current_value, "
                "       utilization_state, controllability, metadata "
                "FROM resources "
                "WHERE tenant_id = $1 "
                "  AND archived_at IS NULL "
                "  AND kind IN ('human', 'financial', 'technical', 'time') "
                "ORDER BY kind, identity",
                auth.tenant_id,
            )

            resources_payload: list[dict[str, Any]] = []
            for r in res_rows:
                cv = r["current_value"]
                if isinstance(cv, str):
                    try:
                        cv = json.loads(cv)
                    except json.JSONDecodeError:
                        cv = {}
                if not isinstance(cv, dict):
                    cv = {}
                md = r["metadata"]
                if isinstance(md, str):
                    try:
                        md = json.loads(md)
                    except json.JSONDecodeError:
                        md = {}
                if not isinstance(md, dict):
                    md = {}

                capacity = cv.get("capacity")
                unit = cv.get("unit") or ""
                label = cv.get("label") or md.get("label") or r["identity"] or "Resource"

                # Sum deployed quantities across active deployments. The
                # bridge stores `{value: X}` so we extract via JSONB
                # arrow operator and cast.
                deployed_row = await conn.fetchrow(
                    "SELECT COALESCE(SUM((deployed_quantity->>'value')::float), 0) AS total, "
                    "       COUNT(*) AS deployments "
                    "FROM resource_deployments rd "
                    "JOIN commitments c ON c.id = rd.commitment_id "
                    "WHERE rd.resource_id = $1 "
                    "  AND rd.released_at IS NULL "
                    "  AND c.tenant_id = $2 "
                    "  AND c.terminal_at IS NULL",
                    r["id"], auth.tenant_id,
                )
                total_deployed = float(deployed_row["total"] or 0.0)
                deployments_count = int(deployed_row["deployments"] or 0)

                cap = float(capacity) if isinstance(capacity, (int, float)) else 0.0
                util_pct = (total_deployed / cap * 100.0) if cap > 0 else 0.0

                # Top 5 consumers (commit titles) for the per-resource
                # detail panel. Scoped to the same active set.
                top_rows = await conn.fetch(
                    "SELECT c.id, c.title, c.state, c.owner_id, "
                    "       (rd.deployed_quantity->>'value')::float AS qty "
                    "FROM resource_deployments rd "
                    "JOIN commitments c ON c.id = rd.commitment_id "
                    "WHERE rd.resource_id = $1 "
                    "  AND rd.released_at IS NULL "
                    "  AND c.tenant_id = $2 "
                    "  AND c.terminal_at IS NULL "
                    "ORDER BY (rd.deployed_quantity->>'value')::float DESC NULLS LAST "
                    "LIMIT 5",
                    r["id"], auth.tenant_id,
                )
                top_consumers: list[dict[str, Any]] = []
                for tr in top_rows:
                    top_consumers.append({
                        "commitment_id": str(tr["id"]),
                        "label": tr["title"] or "(untitled)",
                        "state": tr["state"],
                        "owner_id": (
                            str(tr["owner_id"]) if tr["owner_id"] else None
                        ),
                        "deployed_quantity": float(tr["qty"] or 0.0),
                    })

                # Health label from utilization band.
                if util_pct >= 100.0:
                    health = "over-allocated"
                elif util_pct >= 85.0:
                    health = "constrained"
                elif util_pct >= 50.0:
                    health = "deployed"
                elif util_pct > 0:
                    health = "under-utilized"
                else:
                    health = "available"

                resources_payload.append({
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "identity": r["identity"],
                    "label": label,
                    "description": r["description"] or "",
                    "capacity": cap,
                    "unit": unit,
                    "deployed": total_deployed,
                    "available": max(0.0, cap - total_deployed),
                    "utilization_pct": util_pct,
                    "deployments_count": deployments_count,
                    "health": health,
                    "category": md.get("category"),
                    "top_consumers": top_consumers,
                })

        return JSONResponse(
            {"resources": resources_payload},
            status_code=200,
        )

    # ---------------- /v1/structure/resources/{rid}/overlay -----
    # Single-resource focus payload — the resource itself + every
    # commitment consuming it (ordered by deployed quantity desc) +
    # owner + customer references so the focus view can render edges
    # to all touch points without a second roundtrip.
    @app.get("/v1/structure/resources/{rid}/overlay")
    async def structure_resource_overlay(
        rid: str, request: Request,
    ) -> JSONResponse:
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover
            return _unauth("missing_bearer")
        try:
            resource_uuid = UUID(rid)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_resource_id"}, status_code=400,
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            r = await conn.fetchrow(
                "SELECT id, kind, identity, description, current_value, "
                "       utilization_state, metadata "
                "FROM resources "
                "WHERE id = $1 AND tenant_id = $2 "
                "  AND archived_at IS NULL",
                resource_uuid, auth.tenant_id,
            )
            if r is None:
                return JSONResponse(
                    {"error": "not_found"}, status_code=404,
                )

            cv = r["current_value"]
            if isinstance(cv, str):
                try:
                    cv = json.loads(cv)
                except json.JSONDecodeError:
                    cv = {}
            if not isinstance(cv, dict):
                cv = {}
            md = r["metadata"]
            if isinstance(md, str):
                try:
                    md = json.loads(md)
                except json.JSONDecodeError:
                    md = {}
            if not isinstance(md, dict):
                md = {}

            consumers = await conn.fetch(
                "SELECT c.id, c.title, c.state, c.owner_id, c.due_date, "
                "       (rd.deployed_quantity->>'value')::float AS qty "
                "FROM resource_deployments rd "
                "JOIN commitments c ON c.id = rd.commitment_id "
                "WHERE rd.resource_id = $1 "
                "  AND rd.released_at IS NULL "
                "  AND c.tenant_id = $2 "
                "  AND c.terminal_at IS NULL "
                "ORDER BY (rd.deployed_quantity->>'value')::float DESC NULLS LAST "
                "LIMIT 80",
                resource_uuid, auth.tenant_id,
            )

            consumers_payload: list[dict[str, Any]] = []
            owner_ids: set[UUID] = set()
            for cr in consumers:
                if cr["owner_id"] is not None:
                    owner_ids.add(cr["owner_id"])
                consumers_payload.append({
                    "id": str(cr["id"]),
                    "label": cr["title"] or "(untitled)",
                    "state": cr["state"],
                    "owner_id": (
                        str(cr["owner_id"]) if cr["owner_id"] else None
                    ),
                    "due_date": (
                        cr["due_date"].date().isoformat()
                        if cr["due_date"] is not None else None
                    ),
                    "deployed_quantity": float(cr["qty"] or 0.0),
                })

            owners_payload: list[dict[str, Any]] = []
            if owner_ids:
                owner_rows = await conn.fetch(
                    "SELECT id, display_name, metadata FROM actors "
                    "WHERE tenant_id = $1 AND id = ANY($2::uuid[])",
                    auth.tenant_id, list(owner_ids),
                )
                for orow in owner_rows:
                    md_o = orow["metadata"]
                    if isinstance(md_o, str):
                        try:
                            md_o = json.loads(md_o)
                        except json.JSONDecodeError:
                            md_o = {}
                    if not isinstance(md_o, dict):
                        md_o = {}
                    role = md_o.get("title") or md_o.get("role") or "Team member"
                    owners_payload.append({
                        "id": str(orow["id"]),
                        "label": orow["display_name"],
                        "role": role,
                    })

            cap = float(cv.get("capacity") or 0.0)
            total_deployed = sum(c["deployed_quantity"] for c in consumers_payload)
            util_pct = (total_deployed / cap * 100.0) if cap > 0 else 0.0

        return JSONResponse(
            {
                "resource": {
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "identity": r["identity"],
                    "label": cv.get("label") or md.get("label") or r["identity"],
                    "description": r["description"] or "",
                    "capacity": cap,
                    "unit": cv.get("unit") or "",
                    "deployed": total_deployed,
                    "utilization_pct": util_pct,
                    "category": md.get("category"),
                },
                "consumers": consumers_payload,
                "owners": owners_payload,
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

    # ---------------- /v1/history (History page aggregator) -------
    # Returns events / predictions / arcs / calibration / layer_counts
    # for the period requested. services.history.aggregator owns the
    # substrate→UI mapping; this handler is just the HTTP shell.
    @app.get("/v1/history")
    async def history_endpoint(request: Request) -> JSONResponse:
        from services.history import build_history

        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:  # pragma: no cover — middleware enforces
            return _unauth("missing_bearer")

        period = request.query_params.get("period") or "90d"
        if period not in ("7d", "30d", "90d", "365d", "all"):
            return JSONResponse(
                {"error": "invalid_period",
                 "reason": "expected one of 7d/30d/90d/365d/all"},
                status_code=400,
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            payload = await build_history(
                tenant_id=auth.tenant_id,
                period=period,
                conn=conn,
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


# ---------------------------------------------------------------------
# Artifact lookup — per-type fetch + relationship queries powering the
# artifact drawer. Each kind composes a few short SELECTs and assembles
# a structured `sections` list (field-grid, narrative, link-list).
# ---------------------------------------------------------------------


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _ago(ts: Any, *, now: datetime | None = None) -> str:
    """Human-friendly relative timestamp ("3 days ago", "2 hr ago")."""
    if ts is None or not hasattr(ts, "tzinfo"):
        return "—"
    now = now or datetime.now(timezone.utc)
    delta = now - ts
    secs = int(delta.total_seconds())
    if secs < 60:
        return "just now"
    if secs < 3600:
        return f"{secs // 60} min ago"
    if secs < 86400:
        return f"{secs // 3600} hr ago"
    days = secs // 86400
    if days == 1:
        return "yesterday"
    if days < 30:
        return f"{days} days ago"
    months = days // 30
    if months < 12:
        return f"{months} mo ago"
    return f"{days // 365} yr ago"


def _trim(s: str | None, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


async def _fetch_commitment_overlay(
    cid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    """Build the Structure-overlay payload for a single commitment:
    the commitment row + its contributing goals + its customer link +
    its owner / contributors. Used by both the focus-by-id endpoint
    and the recent-commitments list endpoint."""
    crow = await conn.fetchrow(
        "SELECT id, title, state, owner_id, due_date, priority, "
        "       is_maintenance "
        "FROM commitments WHERE id = $1 AND tenant_id = $2",
        cid, tenant_id,
    )
    if crow is None:
        return None

    owner_row = None
    if crow["owner_id"] is not None:
        owner_row = await conn.fetchrow(
            "SELECT id, display_name FROM actors "
            "WHERE id = $1 AND tenant_id = $2",
            crow["owner_id"], tenant_id,
        )

    goal_rows = await conn.fetch(
        "SELECT g.id, g.title, g.altitude, g.parent_goal_id FROM goals g "
        "JOIN contributes_to ct ON ct.goal_id = g.id "
        "WHERE ct.commitment_id = $1 AND g.tenant_id = $2",
        cid, tenant_id,
    )

    customer_rows = await conn.fetch(
        "SELECT r.id, r.identity, r.metadata FROM resources r "
        "JOIN customer_commitments cc ON cc.customer_resource_id = r.id "
        "WHERE cc.commitment_id = $1 AND r.tenant_id = $2",
        cid, tenant_id,
    )

    contributor_rows = await conn.fetch(
        "SELECT a.id, a.display_name FROM actors a "
        "JOIN commitment_contributors cc ON cc.actor_id = a.id "
        "WHERE cc.commitment_id = $1 AND a.tenant_id = $2",
        cid, tenant_id,
    )

    # Capacity resources consumed by this commitment. We exclude the
    # `relational` kind so customer rows (also stored in `resources`)
    # don't double-count as capacity resources in the graph.
    consumed_resource_rows = await conn.fetch(
        "SELECT r.id, r.kind, r.identity, r.description, r.current_value, "
        "       r.utilization_state, r.metadata, "
        "       rd.deployed_quantity "
        "FROM resources r "
        "JOIN resource_deployments rd ON rd.resource_id = r.id "
        "WHERE rd.commitment_id = $1 "
        "  AND rd.released_at IS NULL "
        "  AND r.tenant_id = $2 "
        "  AND r.kind IN ('human', 'financial', 'technical', 'time') "
        "ORDER BY r.kind, r.identity",
        cid, tenant_id,
    )

    decision_rows = await conn.fetch(
        "SELECT d.id, d.title, d.decision_text, d.rationale, d.state "
        "FROM decisions d "
        "JOIN constrained_by cb ON cb.decision_id = d.id "
        "WHERE cb.commitment_id = $1 AND d.tenant_id = $2",
        cid, tenant_id,
    )

    # Models scoped to this commitment — surfaced as learned-pattern
    # bundles on the commitment card. Filter by scope_entities @>
    # [{type=commitment, id=cid}] using JSONB containment, then pick
    # top 6 by confidence so the card stays scannable.
    pattern_model_rows = await conn.fetch(
        """
        SELECT id, "natural", proposition, confidence, falsifier,
               proposition_kind AS kind,
               supporting_event_ids, evidential_weight,
               created_at
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND scope_entities @> $2::jsonb
        ORDER BY confidence DESC NULLS LAST, created_at DESC
        LIMIT 6
        """,
        tenant_id,
        json.dumps([{"type": "commitment", "id": str(cid)}]),
    )

    # State-change history: most recent transition + the originating
    # signal that caused it. Used to render "why this is at risk" on
    # the Structure detail card.
    state_change_rows = await conn.fetch(
        """
        SELECT id, occurred_at, cause_id, content
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'state_change'
          AND content->>'entity_kind' = 'commitment'
          AND content->>'entity_id' = $2::text
        ORDER BY occurred_at DESC
        LIMIT 5
        """,
        tenant_id, str(cid),
    )

    activity_payload: list[dict[str, Any]] = []
    substrate_insight: str | None = None
    seen_cause_ids: set[UUID] = set()
    for sc in state_change_rows:
        sc_content = sc["content"]
        if isinstance(sc_content, str):
            try:
                sc_content = json.loads(sc_content)
            except json.JSONDecodeError:
                sc_content = {}
        elif not isinstance(sc_content, dict):
            sc_content = {}
        from_state = sc_content.get("from_state")
        to_state = sc_content.get("to_state")
        sc_date = sc["occurred_at"].date().isoformat()
        if from_state and to_state:
            activity_payload.append({
                "date": sc_date,
                "desc": f"transitioned {from_state} → {to_state}",
            })
        cause_id = sc["cause_id"]
        if cause_id is None or cause_id in seen_cause_ids:
            continue
        seen_cause_ids.add(cause_id)
        cause_row = await conn.fetchrow(
            "SELECT source_channel, content_text, occurred_at, "
            "       actor_id "
            "FROM observations "
            "WHERE id = $1 AND tenant_id = $2",
            cause_id, tenant_id,
        )
        if cause_row is None:
            continue
        text = (cause_row["content_text"] or "").strip()
        if not text:
            continue
        actor_label: str | None = None
        if cause_row["actor_id"] is not None:
            actor_lookup = await conn.fetchrow(
                "SELECT display_name FROM actors "
                "WHERE id = $1 AND tenant_id = $2",
                cause_row["actor_id"], tenant_id,
            )
            if actor_lookup is not None:
                actor_label = actor_lookup["display_name"]
        cause_date = cause_row["occurred_at"].date().isoformat()
        ch = cause_row["source_channel"] or "signal"
        truncated = text if len(text) <= 240 else text[:237] + "…"
        attribution = (
            f"{actor_label} via {ch}" if actor_label else ch
        )
        activity_payload.append({
            "date": cause_date,
            "desc": f"{attribution}: {truncated}",
        })
        # First (most recent) cause becomes the substrate insight —
        # the headline reason this commitment is in its current state.
        if substrate_insight is None and from_state and to_state:
            substrate_insight = (
                f"Moved to {to_state} after {attribution.lower()}: "
                f"\u201c{truncated}\u201d"
            )

    owner_id_str = str(owner_row["id"]) if owner_row else None
    owner_label = owner_row["display_name"] if owner_row else None

    state = crow["state"]
    status_label = "on-track"
    if state == "blocked":
        status_label = "blocked"
    elif state == "paused":
        status_label = "at-risk"

    customer_id_str: str | None = None
    customer_label: str | None = None
    if customer_rows:
        cr = customer_rows[0]
        customer_id_str = str(cr["id"])
        md = cr["metadata"]
        if isinstance(md, str):
            try:
                md = json.loads(md)
            except json.JSONDecodeError:
                md = {}
        elif not isinstance(md, dict):
            md = {}
        customer_label = (
            md.get("display_name") or cr["identity"] or "Customer"
        )

    goals_payload: list[dict[str, Any]] = []
    for g in goal_rows:
        altitude = (
            g["altitude"] if g["altitude"] in ("strategic", "operational")
            else "operational"
        )
        goals_payload.append({
            "id": str(g["id"]),
            "label": g["title"],
            "altitude": altitude,
            "parent_goal_id": (
                str(g["parent_goal_id"]) if g["parent_goal_id"] else None
            ),
        })

    people_payload: list[dict[str, Any]] = []
    seen_actor_ids: set[str] = set()
    if owner_row is not None:
        people_payload.append({
            "id": str(owner_row["id"]),
            "label": owner_row["display_name"],
            "role": "Owner",
        })
        seen_actor_ids.add(str(owner_row["id"]))
    for c in contributor_rows:
        cid_str = str(c["id"])
        if cid_str in seen_actor_ids:
            continue
        seen_actor_ids.add(cid_str)
        people_payload.append({
            "id": cid_str,
            "label": c["display_name"],
            "role": "Contributor",
        })

    customers_payload: list[dict[str, Any]] = []
    if customer_id_str and customer_label:
        customers_payload.append({
            "id": customer_id_str,
            "label": customer_label,
        })

    decisions_payload: list[dict[str, Any]] = []
    for d in decision_rows:
        decisions_payload.append({
            "id": str(d["id"]),
            "label": d["title"],
            "state": d["state"] if d["state"] in (
                "in-force", "drifting", "revisited",
            ) else "in-force",
        })

    # Resources consumed by this commitment — used by the right quadrant
    # of the relational graph and the commitment side-panel "Resources"
    # block. Each entry carries the deployed quantity in the resource's
    # native unit (FTE, USD, engineer-weeks, GPU-hours).
    resources_payload: list[dict[str, Any]] = []
    for rr in consumed_resource_rows:
        cv = rr["current_value"]
        if isinstance(cv, str):
            try:
                cv = json.loads(cv)
            except json.JSONDecodeError:
                cv = {}
        if not isinstance(cv, dict):
            cv = {}
        md = rr["metadata"]
        if isinstance(md, str):
            try:
                md = json.loads(md)
            except json.JSONDecodeError:
                md = {}
        if not isinstance(md, dict):
            md = {}
        dq = rr["deployed_quantity"]
        if isinstance(dq, str):
            try:
                dq = json.loads(dq)
            except json.JSONDecodeError:
                dq = {}
        if not isinstance(dq, dict):
            dq = {}
        resources_payload.append({
            "id": str(rr["id"]),
            "label": cv.get("label") or md.get("label") or rr["identity"] or "Resource",
            "kind": rr["kind"],
            "unit": cv.get("unit"),
            "deployed_quantity": dq.get("value"),
        })

    # Build LearnedPattern bundles from the scoped models. Each model's
    # natural-language statement becomes the pattern statement;
    # supporting_event_ids resolve to short evidence snippets via a
    # bounded observation lookup (cap 3 per model so we don't blow up
    # the response).
    learnings_payload: list[dict[str, Any]] = []
    for m in pattern_model_rows:
        prop = m["proposition"]
        if isinstance(prop, str):
            try:
                prop = json.loads(prop)
            except json.JSONDecodeError:
                prop = {}
        if not isinstance(prop, dict):
            prop = {}
        statement = (m["natural"] or "").strip()
        if not statement:
            continue
        evidence_payload: list[dict[str, Any]] = []
        ev_ids = m["supporting_event_ids"] or []
        # ev_ids is a list of UUIDs (or strings). Cap at 3.
        ev_lookup_ids = [eid for eid in list(ev_ids)[:3]]
        if ev_lookup_ids:
            ev_rows = await conn.fetch(
                "SELECT id, occurred_at, content_text FROM observations "
                "WHERE tenant_id = $1 AND id = ANY($2::uuid[])",
                tenant_id, [
                    UUID(str(x)) if not isinstance(x, UUID) else x
                    for x in ev_lookup_ids
                ],
            )
            for er in ev_rows:
                t = (er["content_text"] or "").strip()
                if not t:
                    continue
                evidence_payload.append({
                    "when": er["occurred_at"].date().isoformat(),
                    "text": t if len(t) <= 180 else t[:177] + "…",
                })
        learnings_payload.append({
            "id": str(m["id"]),
            "statement": statement if len(statement) <= 240 else statement[:237] + "…",
            "strength": float(m["confidence"] or 0.5),
            "evidence": evidence_payload,
        })

    commitment_payload = {
        "id": str(crow["id"]),
        "label": crow["title"],
        "owner": owner_id_str,
        "owner_display": owner_label,
        "due_date": (
            crow["due_date"].date().isoformat()
            if crow["due_date"] is not None else None
        ),
        "status": status_label,
        "priority": (
            "high" if (crow["priority"] or 5) <= 3
            else "low" if (crow["priority"] or 5) >= 8
            else "standard"
        ),
        "customer": customer_id_str,
        "customer_label": customer_label,
        "edges": {
            "contributes_to": [str(g["id"]) for g in goal_rows],
            "constrained_by": [str(d["id"]) for d in decision_rows],
            "consumes": [r["id"] for r in resources_payload],
            "contributors": [str(c["id"]) for c in contributor_rows],
        },
        # Per-commit slice of every consumed resource (label, unit,
        # deployed_quantity in the resource's native unit). Lets the
        # commitment focus view show "Engineering pod · 0.4 FTE"
        # without a second roundtrip to fetch resource metadata.
        "consumed_resources": resources_payload,
        "substrate_insight": substrate_insight,
        "activity": activity_payload,
        "learnings": learnings_payload,
    }

    return {
        "commitment": commitment_payload,
        "goals": goals_payload,
        "people": people_payload,
        "customers": customers_payload,
        "decisions": decisions_payload,
        "resources": resources_payload,
    }


async def _fetch_artifact(
    kind: str, aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    """Dispatch to per-kind builder. Each builder does its own queries
    and returns the assembled drawer payload."""
    builders = {
        "actor": _build_actor_drawer,
        "commitment": _build_commitment_drawer,
        "goal": _build_goal_drawer,
        "decision": _build_decision_drawer,
        "resource": _build_resource_drawer,
        "observation": _build_observation_drawer,
        "model": _build_model_drawer,
    }
    builder = builders.get(kind)
    if builder is None:
        return None
    return await builder(aid, tenant_id, conn)


# ----- actor ---------------------------------------------------------


async def _build_actor_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, display_name, type, email, created_at, last_seen_at "
        "FROM actors WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    owns = await conn.fetch(
        "SELECT id, title, state, last_state_change_at FROM commitments "
        "WHERE tenant_id = $1 AND owner_id = $2 AND terminal_at IS NULL "
        "ORDER BY last_state_change_at DESC LIMIT 5",
        tenant_id, aid,
    )
    owns_count = await conn.fetchval(
        "SELECT count(*) FROM commitments "
        "WHERE tenant_id = $1 AND owner_id = $2 AND terminal_at IS NULL",
        tenant_id, aid,
    ) or 0

    recent = await conn.fetch(
        "SELECT id, source_channel, occurred_at, content_text "
        "FROM observations WHERE tenant_id = $1 AND actor_id = $2 "
        "ORDER BY occurred_at DESC LIMIT 5",
        tenant_id, aid,
    )

    last_seen = row["last_seen_at"] or row["created_at"]
    summary_bits: list[str] = []
    if owns_count:
        summary_bits.append(f"owns {owns_count} active commitment{'s' if owns_count != 1 else ''}")
    if last_seen:
        summary_bits.append(f"last seen {_ago(last_seen)}")
    summary = " · ".join(summary_bits) or None

    sections: list[dict[str, Any]] = [
        {
            "kind": "fields",
            "title": "At a glance",
            "rows": [
                {"label": "Type", "value": row["type"] or "—"},
                {"label": "Email", "value": row["email"] or "—"},
                {"label": "Joined", "value": _ago(row["created_at"])},
                {"label": "Last seen", "value": _ago(row["last_seen_at"])},
            ],
        }
    ]
    if owns:
        sections.append({
            "kind": "links",
            "title": f"Owns ({owns_count})",
            "items": [
                {
                    "type": "commitment", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": f"{c['state']} · updated {_ago(c['last_state_change_at'])}",
                }
                for c in owns
            ],
        })
    if recent:
        sections.append({
            "kind": "links",
            "title": "Recent activity",
            "items": [
                {
                    "type": "observation", "id": str(o["id"]),
                    "primary": _trim(o["content_text"], 120),
                    "secondary": f"{o['source_channel'] or 'signal'} · {_ago(o['occurred_at'])}",
                }
                for o in recent
            ],
        })
    return {
        "type": "actor",
        "id": str(row["id"]),
        "title": row["display_name"],
        "subtitle": f"actor · {row['type'] or 'unknown'}",
        "summary": summary,
        "sections": sections,
    }


# ----- commitment ----------------------------------------------------


async def _build_commitment_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT c.id, c.title, c.state, c.description, c.created_at, "
        "c.last_state_change_at, c.terminal_at, c.due_date, "
        "c.owner_id, a.display_name AS owner_name "
        "FROM commitments c LEFT JOIN actors a ON a.id = c.owner_id "
        "WHERE c.id = $1 AND c.tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    contributors = await conn.fetch(
        "SELECT cc.actor_id, a.display_name, a.type "
        "FROM commitment_contributors cc "
        "JOIN actors a ON a.id = cc.actor_id "
        "WHERE cc.commitment_id = $1 AND a.tenant_id = $2 "
        "ORDER BY a.display_name LIMIT 10",
        aid, tenant_id,
    )

    # Recent state-change observations referencing this commitment
    recent_obs = await conn.fetch(
        """
        SELECT id, source_channel, occurred_at, content_text
        FROM observations
        WHERE tenant_id = $1
          AND entities_mentioned @> jsonb_build_array(
              jsonb_build_object('type','commitment','id',$2::text)
          )
        ORDER BY occurred_at DESC LIMIT 5
        """,
        tenant_id, str(aid),
    )

    # Models that reference this commitment via scope_entities
    related_models = await conn.fetch(
        """
        SELECT id, "natural", confidence, proposition_kind
        FROM models
        WHERE tenant_id = $1 AND status = 'active'
          AND scope_entities @> jsonb_build_array(
              jsonb_build_object('type','commitment','id',$2::text)
          )
        ORDER BY confidence DESC LIMIT 5
        """,
        tenant_id, str(aid),
    )

    state = row["state"] or "unknown"
    days_in_state = 0
    if row["last_state_change_at"]:
        days_in_state = max(
            0,
            int((datetime.now(timezone.utc) - row["last_state_change_at"]).total_seconds() // 86400),
        )
    summary_bits = [f"in <strong>{state}</strong> for {days_in_state}d"]
    if row["owner_name"]:
        summary_bits.append(f"owned by {row['owner_name']}")
    if row["due_date"]:
        summary_bits.append(f"due {_iso(row['due_date'])[:10] if _iso(row['due_date']) else ''}")

    sections: list[dict[str, Any]] = []

    fields_rows: list[dict[str, str]] = [
        {"label": "State", "value": state},
        {"label": "Owner", "value": row["owner_name"] or "—"},
        {"label": "Created", "value": _ago(row["created_at"])},
        {"label": "Last move", "value": _ago(row["last_state_change_at"])},
    ]
    if row["due_date"]:
        fields_rows.append({"label": "Due", "value": _iso(row["due_date"]) or "—"})
    sections.append({"kind": "fields", "title": "At a glance", "rows": fields_rows})

    if row["description"]:
        sections.append({
            "kind": "narrative",
            "title": "Acceptance",
            "body": row["description"],
        })

    if row["owner_id"]:
        # Show owner as a single link so the user can drill into them
        owner_items: list[dict[str, Any]] = [{
            "type": "actor", "id": str(row["owner_id"]),
            "primary": row["owner_name"] or "Owner",
            "secondary": "owner",
        }]
        for c in contributors:
            if c["actor_id"] == row["owner_id"]:
                continue
            owner_items.append({
                "type": "actor", "id": str(c["actor_id"]),
                "primary": c["display_name"], "secondary": "contributor",
            })
        sections.append({
            "kind": "links",
            "title": f"People ({len(owner_items)})",
            "items": owner_items,
        })

    if related_models:
        sections.append({
            "kind": "links",
            "title": "Why it exists",
            "items": [
                {
                    "type": "model", "id": str(m["id"]),
                    "primary": _trim(m["natural"], 140),
                    "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                    "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
                }
                for m in related_models
            ],
        })

    if recent_obs:
        sections.append({
            "kind": "links",
            "title": f"Recent mentions ({len(recent_obs)})",
            "items": [
                {
                    "type": "observation", "id": str(o["id"]),
                    "primary": _trim(o["content_text"], 120),
                    "secondary": f"{o['source_channel'] or 'signal'} · {_ago(o['occurred_at'])}",
                }
                for o in recent_obs
            ],
        })

    return {
        "type": "commitment",
        "id": str(row["id"]),
        "title": row["title"],
        "subtitle": f"commitment · {state}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- goal ----------------------------------------------------------


async def _build_goal_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, title, state, description, altitude, target_date, "
        "parent_goal_id, cached_health, "
        "created_at, last_state_change_at "
        "FROM goals WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    parent_row = None
    if row["parent_goal_id"]:
        parent_row = await conn.fetchrow(
            "SELECT id, title, state FROM goals WHERE id = $1 AND tenant_id = $2",
            row["parent_goal_id"], tenant_id,
        )
    children = await conn.fetch(
        "SELECT id, title, state, cached_health FROM goals "
        "WHERE tenant_id = $1 AND parent_goal_id = $2 AND archived_at IS NULL "
        "ORDER BY created_at LIMIT 8",
        tenant_id, aid,
    )
    contrib = await conn.fetch(
        """
        SELECT c.id, c.title, c.state, c.last_state_change_at
        FROM commitments c
        JOIN contributes_to ct ON ct.commitment_id = c.id
        WHERE ct.goal_id = $1 AND c.tenant_id = $2 AND c.terminal_at IS NULL
        ORDER BY c.last_state_change_at DESC LIMIT 8
        """,
        aid, tenant_id,
    )

    summary_bits: list[str] = []
    if row["altitude"]:
        summary_bits.append(row["altitude"])
    if row["cached_health"]:
        summary_bits.append(row["cached_health"])
    if children:
        summary_bits.append(f"{len(children)} sub-goal{'s' if len(children) != 1 else ''}")
    if contrib:
        summary_bits.append(f"{len(contrib)} contributing commitment{'s' if len(contrib) != 1 else ''}")

    fields_rows = [
        {"label": "State", "value": row["state"] or "—"},
        {"label": "Altitude", "value": row["altitude"] or "—"},
        {"label": "Health", "value": row["cached_health"] or "—"},
    ]
    if row["target_date"]:
        fields_rows.append({"label": "Target", "value": _iso(row["target_date"]) or "—"})

    sections: list[dict[str, Any]] = [
        {"kind": "fields", "title": "At a glance", "rows": fields_rows},
    ]
    if row["description"]:
        sections.append({"kind": "narrative", "title": "Description", "body": row["description"]})
    if parent_row:
        sections.append({
            "kind": "links",
            "title": "Parent goal",
            "items": [{
                "type": "goal", "id": str(parent_row["id"]),
                "primary": parent_row["title"], "secondary": parent_row["state"] or "",
            }],
        })
    if children:
        sections.append({
            "kind": "links",
            "title": f"Sub-goals ({len(children)})",
            "items": [
                {
                    "type": "goal", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": c["state"] or "",
                    "meta": c["cached_health"] or None,
                }
                for c in children
            ],
        })
    if contrib:
        sections.append({
            "kind": "links",
            "title": f"Contributing commitments ({len(contrib)})",
            "items": [
                {
                    "type": "commitment", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": f"{c['state']} · {_ago(c['last_state_change_at'])}",
                }
                for c in contrib
            ],
        })
    return {
        "type": "goal",
        "id": str(row["id"]),
        "title": row["title"],
        "subtitle": f"goal · {row['state'] or 'unknown'}",
        "summary": " · ".join(summary_bits) or None,
        "sections": sections,
    }


# ----- decision -------------------------------------------------------


async def _build_decision_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, title, state, decision_text, rationale, "
        "created_at, last_state_change_at "
        "FROM decisions WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    constrained = await conn.fetch(
        """
        SELECT c.id, c.title, c.state, c.last_state_change_at
        FROM commitments c
        JOIN constrained_by cb ON cb.commitment_id = c.id
        WHERE cb.decision_id = $1 AND c.tenant_id = $2 AND c.terminal_at IS NULL
        ORDER BY c.last_state_change_at DESC LIMIT 8
        """,
        aid, tenant_id,
    )

    related_models = await conn.fetch(
        """
        SELECT id, "natural", confidence, proposition_kind
        FROM models
        WHERE tenant_id = $1 AND status = 'active'
          AND scope_entities @> jsonb_build_array(
              jsonb_build_object('type','decision','id',$2::text)
          )
        ORDER BY confidence DESC LIMIT 5
        """,
        tenant_id, str(aid),
    )

    days_since_change = (
        max(0, int((datetime.now(timezone.utc) - row["last_state_change_at"]).total_seconds() // 86400))
        if row["last_state_change_at"] else None
    )
    summary_bits = [f"<strong>{row['state'] or 'drafted'}</strong>"]
    if days_since_change is not None:
        summary_bits.append(f"unchanged for {days_since_change}d")
    if constrained:
        summary_bits.append(f"constrains {len(constrained)} commitment{'s' if len(constrained) != 1 else ''}")

    sections: list[dict[str, Any]] = [
        {
            "kind": "fields",
            "title": "At a glance",
            "rows": [
                {"label": "State", "value": row["state"] or "—"},
                {"label": "Created", "value": _ago(row["created_at"])},
                {"label": "Last move", "value": _ago(row["last_state_change_at"])},
            ],
        }
    ]
    if row["decision_text"]:
        sections.append({"kind": "narrative", "title": "Decision", "body": row["decision_text"]})
    if row["rationale"]:
        sections.append({"kind": "narrative", "title": "Rationale", "body": row["rationale"]})
    if constrained:
        sections.append({
            "kind": "links",
            "title": f"Constrains ({len(constrained)})",
            "items": [
                {
                    "type": "commitment", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": f"{c['state']} · {_ago(c['last_state_change_at'])}",
                }
                for c in constrained
            ],
        })
    if related_models:
        sections.append({
            "kind": "links",
            "title": "Reasoning that cites this",
            "items": [
                {
                    "type": "model", "id": str(m["id"]),
                    "primary": _trim(m["natural"], 140),
                    "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                    "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
                }
                for m in related_models
            ],
        })
    return {
        "type": "decision",
        "id": str(row["id"]),
        "title": row["title"],
        "subtitle": f"decision · {row['state'] or 'unknown'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- resource -------------------------------------------------------


async def _build_resource_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, kind, identity, description, current_value, "
        "utilization_state, controllability, metadata, "
        "created_at, last_updated_at "
        "FROM resources WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None
    cv = row["current_value"]
    if isinstance(cv, str):
        try:
            cv = json.loads(cv)
        except json.JSONDecodeError:
            cv = None
    if not isinstance(cv, dict):
        cv = {}
    md = row["metadata"]
    if isinstance(md, str):
        try:
            md = json.loads(md)
        except json.JSONDecodeError:
            md = {}
    if not isinstance(md, dict):
        md = {}

    is_capacity_kind = row["kind"] in ("human", "financial", "technical", "time")
    label = cv.get("label") or md.get("label") or row["identity"] or "Resource"
    capacity = cv.get("capacity")
    unit = cv.get("unit") or ""
    legacy_value = cv.get("value")  # customer rows etc.

    # Aggregate deployed quantity if this is a capacity resource.
    total_deployed = 0.0
    deployments_count = 0
    util_pct = 0.0
    if is_capacity_kind:
        agg = await conn.fetchrow(
            "SELECT COALESCE(SUM((deployed_quantity->>'value')::float), 0) AS total, "
            "       COUNT(*) AS n "
            "FROM resource_deployments rd "
            "JOIN commitments c ON c.id = rd.commitment_id "
            "WHERE rd.resource_id = $1 "
            "  AND rd.released_at IS NULL "
            "  AND c.tenant_id = $2 "
            "  AND c.terminal_at IS NULL",
            aid, tenant_id,
        )
        total_deployed = float(agg["total"] or 0.0)
        deployments_count = int(agg["n"] or 0)
        if isinstance(capacity, (int, float)) and capacity > 0:
            util_pct = total_deployed / float(capacity) * 100.0

    if is_capacity_kind and isinstance(capacity, (int, float)):
        capacity_str = f"{_fmt_quantity(capacity, unit)}"
        deployed_str = f"{_fmt_quantity(total_deployed, unit)}"
        util_str = f"{util_pct:.0f}% utilized"
    elif legacy_value is not None:
        capacity_str = f"{legacy_value} {unit}".strip()
        deployed_str = "—"
        util_str = "—"
    else:
        capacity_str = "—"
        deployed_str = "—"
        util_str = "—"

    summary_bits: list[str] = [row["kind"] or "resource"]
    if is_capacity_kind:
        summary_bits.append(util_str)
    elif row["utilization_state"]:
        summary_bits.append(row["utilization_state"])

    fields_rows: list[dict[str, Any]] = [
        {"label": "Kind", "value": row["kind"] or "—"},
    ]
    if is_capacity_kind:
        fields_rows.extend([
            {"label": "Capacity", "value": capacity_str},
            {"label": "Deployed", "value": deployed_str},
            {"label": "Utilization", "value": util_str},
            {"label": "Active commitments", "value": str(deployments_count)},
        ])
    else:
        fields_rows.extend([
            {"label": "Current", "value": capacity_str},
            {"label": "Utilization", "value": row["utilization_state"] or "—"},
        ])
    fields_rows.append({"label": "Control", "value": row["controllability"] or "—"})
    fields_rows.append({"label": "Updated", "value": _ago(row["last_updated_at"])})

    sections: list[dict[str, Any]] = [
        {"kind": "fields", "title": "At a glance", "rows": fields_rows},
    ]
    if row["description"]:
        sections.append({
            "kind": "narrative",
            "title": "Description",
            "body": row["description"],
        })

    if is_capacity_kind:
        consumers = await conn.fetch(
            "SELECT c.id, c.title, c.state, "
            "       (rd.deployed_quantity->>'value')::float AS qty, "
            "       a.display_name AS owner_name "
            "FROM resource_deployments rd "
            "JOIN commitments c ON c.id = rd.commitment_id "
            "LEFT JOIN actors a ON a.id = c.owner_id "
            "WHERE rd.resource_id = $1 "
            "  AND rd.released_at IS NULL "
            "  AND c.tenant_id = $2 "
            "  AND c.terminal_at IS NULL "
            "ORDER BY (rd.deployed_quantity->>'value')::float DESC NULLS LAST "
            "LIMIT 8",
            aid, tenant_id,
        )
        items: list[dict[str, Any]] = []
        for cr in consumers:
            qty = float(cr["qty"] or 0.0)
            secondary = cr["owner_name"] or ""
            meta_str = (
                f"{_fmt_quantity(qty, unit)}" if unit else f"{qty:.2g}"
            )
            if cr["state"]:
                meta_str = f"{meta_str} · {cr['state']}"
            items.append({
                "type": "commitment",
                "id": str(cr["id"]),
                "primary": cr["title"] or "(untitled)",
                "secondary": secondary,
                "meta": meta_str,
            })
        sections.append({
            "kind": "links",
            "title": "Top consumers",
            "items": items,
            "empty_text": "No active commitments are drawing on this resource.",
        })

    return {
        "type": "resource",
        "id": str(row["id"]),
        "title": label,
        "subtitle": f"resource · {row['kind'] or 'unknown'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


def _fmt_quantity(value: float, unit: str) -> str:
    """Pretty-format a quantity in its unit. Cash gets dollar formatting,
    FTE gets one decimal, engineer-weeks/credits/GPU-hours get integer
    rounding."""
    u = (unit or "").lower()
    if "usd" in u:
        if value >= 1_000_000:
            return f"${value / 1_000_000:.1f}M"
        if value >= 1_000:
            return f"${value / 1_000:.0f}k"
        return f"${value:.0f}"
    if "fte" in u:
        return f"{value:.1f} FTE"
    if not unit:
        return f"{value:.2f}"
    return f"{value:.0f} {unit}"


# ----- observation ----------------------------------------------------


async def _build_observation_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, kind, source_channel, occurred_at, content_text, "
        "actor_id, trust_tier, entities_mentioned, source_actor_ref "
        "FROM observations WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    actor_link = None
    if row["actor_id"]:
        a = await conn.fetchrow(
            "SELECT id, display_name, type FROM actors "
            "WHERE id = $1 AND tenant_id = $2",
            row["actor_id"], tenant_id,
        )
        if a:
            actor_link = {
                "type": "actor", "id": str(a["id"]),
                "primary": a["display_name"],
                "secondary": a["type"] or "actor",
            }

    # Models that count this observation among their supporting events.
    using_models = await conn.fetch(
        """
        SELECT id, "natural", confidence, proposition_kind
        FROM models
        WHERE tenant_id = $1 AND status = 'active'
          AND $2 = ANY (supporting_event_ids)
        ORDER BY confidence DESC LIMIT 5
        """,
        tenant_id, aid,
    )

    # entities_mentioned is a jsonb array of {type,id}; resolve ids → titles
    mentioned: list[dict[str, Any]] = []
    em = row["entities_mentioned"]
    if isinstance(em, str):
        try:
            em = json.loads(em)
        except json.JSONDecodeError:
            em = []
    if isinstance(em, list):
        for ent in em[:8]:
            if not isinstance(ent, dict):
                continue
            etype = ent.get("type")
            eid = ent.get("id")
            if not etype or not eid:
                continue
            try:
                e_uuid = UUID(str(eid))
            except (ValueError, TypeError):
                continue
            title = await _resolve_entity_title(etype, e_uuid, tenant_id, conn)
            if title:
                mentioned.append({
                    "type": etype, "id": str(e_uuid),
                    "primary": title, "secondary": etype,
                })

    summary_bits = [
        row["source_channel"] or row["kind"] or "signal",
        _ago(row["occurred_at"]),
    ]
    if row["trust_tier"]:
        summary_bits.append(f"trust: {row['trust_tier']}")

    sections: list[dict[str, Any]] = [
        {
            "kind": "fields",
            "title": "At a glance",
            "rows": [
                {"label": "Channel", "value": row["source_channel"] or "—"},
                {"label": "Kind", "value": row["kind"] or "—"},
                {"label": "Trust", "value": row["trust_tier"] or "—"},
                {"label": "Source", "value": row["source_actor_ref"] or "—"},
                {"label": "Occurred", "value": _ago(row["occurred_at"])},
            ],
        },
        {
            "kind": "narrative",
            "title": "Content",
            "body": row["content_text"] or "",
        },
    ]
    if actor_link:
        sections.append({
            "kind": "links", "title": "From", "items": [actor_link],
        })
    if mentioned:
        sections.append({
            "kind": "links",
            "title": f"Mentions ({len(mentioned)})",
            "items": mentioned,
        })
    if using_models:
        sections.append({
            "kind": "links",
            "title": f"Used in {len(using_models)} model{'s' if len(using_models) != 1 else ''}",
            "items": [
                {
                    "type": "model", "id": str(m["id"]),
                    "primary": _trim(m["natural"], 140),
                    "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                    "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
                }
                for m in using_models
            ],
        })
    return {
        "type": "observation",
        "id": str(row["id"]),
        "title": _trim(row["content_text"], 140),
        "subtitle": f"evidence · {row['source_channel'] or row['kind'] or 'signal'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- model ----------------------------------------------------------


async def _build_model_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        'SELECT id, "natural", proposition_kind, confidence, status, '
        "supporting_event_ids, supporting_model_ids, "
        "scope_actors, scope_entities, falsifier, "
        "confirmed_count, contested_count, "
        "created_at, resolved_at "
        "FROM models WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    pk = (row["proposition_kind"] or "model").replace("_", " ")
    conf = float(row["confidence"] or 0.0)
    conf_pct = int(round(conf * 100))

    # Top supporting observations
    sup_obs: list[dict[str, Any]] = []
    if row["supporting_event_ids"]:
        rows_obs = await conn.fetch(
            "SELECT id, source_channel, occurred_at, content_text "
            "FROM observations WHERE id = ANY($1::uuid[]) AND tenant_id = $2 "
            "ORDER BY occurred_at DESC LIMIT 5",
            list(row["supporting_event_ids"]), tenant_id,
        )
        for o in rows_obs:
            sup_obs.append({
                "type": "observation", "id": str(o["id"]),
                "primary": _trim(o["content_text"], 120),
                "secondary": f"{o['source_channel'] or 'signal'} · {_ago(o['occurred_at'])}",
            })

    # Top supporting models
    sup_models: list[dict[str, Any]] = []
    if row["supporting_model_ids"]:
        rows_m = await conn.fetch(
            'SELECT id, "natural", confidence, proposition_kind '
            "FROM models WHERE id = ANY($1::uuid[]) AND tenant_id = $2 "
            "ORDER BY confidence DESC LIMIT 5",
            list(row["supporting_model_ids"]), tenant_id,
        )
        for m in rows_m:
            sup_models.append({
                "type": "model", "id": str(m["id"]),
                "primary": _trim(m["natural"], 140),
                "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
            })

    # Falsifier as a narrative
    falsifier_body: str | None = None
    fals = row["falsifier"]
    if isinstance(fals, str):
        try:
            fals = json.loads(fals)
        except json.JSONDecodeError:
            fals = None
    if isinstance(fals, dict):
        if fals.get("text"):
            falsifier_body = str(fals["text"])
        elif fals.get("description"):
            falsifier_body = str(fals["description"])

    # Scope actors → links
    actor_links: list[dict[str, Any]] = []
    if row["scope_actors"]:
        rows_a = await conn.fetch(
            "SELECT id, display_name, type FROM actors "
            "WHERE id = ANY($1::uuid[]) AND tenant_id = $2 LIMIT 6",
            list(row["scope_actors"]), tenant_id,
        )
        for a in rows_a:
            actor_links.append({
                "type": "actor", "id": str(a["id"]),
                "primary": a["display_name"],
                "secondary": a["type"] or "actor",
            })

    # Scope entities → links
    entity_links: list[dict[str, Any]] = []
    se = row["scope_entities"]
    if isinstance(se, str):
        try:
            se = json.loads(se)
        except json.JSONDecodeError:
            se = []
    if isinstance(se, list):
        for ent in se[:6]:
            if not isinstance(ent, dict):
                continue
            etype = ent.get("type")
            eid = ent.get("id")
            if not etype or not eid:
                continue
            try:
                e_uuid = UUID(str(eid))
            except (ValueError, TypeError):
                continue
            title = await _resolve_entity_title(etype, e_uuid, tenant_id, conn)
            if title:
                entity_links.append({
                    "type": etype, "id": str(e_uuid),
                    "primary": title, "secondary": etype,
                })

    confirmed = int(row["confirmed_count"] or 0)
    contested = int(row["contested_count"] or 0)
    summary_bits = [
        f"{conf_pct}% confident",
        pk,
        f"{len(sup_obs)} signal{'s' if len(sup_obs) != 1 else ''}",
    ]
    if confirmed or contested:
        summary_bits.append(f"{confirmed}↑ {contested}↓")

    fields_rows = [
        {"label": "Kind", "value": pk},
        {"label": "Confidence", "value": f"{conf_pct}%"},
        {"label": "Status", "value": row["status"] or "—"},
        {"label": "Confirmed", "value": str(confirmed)},
        {"label": "Contested", "value": str(contested)},
        {"label": "Created", "value": _ago(row["created_at"])},
    ]
    if row["resolved_at"]:
        fields_rows.append({"label": "Resolved", "value": _ago(row["resolved_at"])})

    sections: list[dict[str, Any]] = [
        {"kind": "fields", "title": "At a glance", "rows": fields_rows},
        {"kind": "narrative", "title": "What it claims", "body": row["natural"] or ""},
    ]
    if falsifier_body:
        sections.append({
            "kind": "narrative", "title": "What would falsify it",
            "body": falsifier_body,
        })
    if entity_links:
        sections.append({
            "kind": "links",
            "title": f"About ({len(entity_links)})",
            "items": entity_links,
        })
    if actor_links:
        sections.append({
            "kind": "links",
            "title": f"Subjects ({len(actor_links)})",
            "items": actor_links,
        })
    if sup_obs:
        sections.append({
            "kind": "links",
            "title": f"Built from ({len(sup_obs)} signal{'s' if len(sup_obs) != 1 else ''})",
            "items": sup_obs,
        })
    if sup_models:
        sections.append({
            "kind": "links",
            "title": f"Built on ({len(sup_models)} other model{'s' if len(sup_models) != 1 else ''})",
            "items": sup_models,
        })

    return {
        "type": "model",
        "id": str(row["id"]),
        "title": row["natural"] or "(no natural rendering)",
        "subtitle": f"{pk} · {row['status'] or 'unknown'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- entity title resolver -----------------------------------------


_TITLE_SQL_BY_TYPE: dict[str, str] = {
    "actor":      "SELECT display_name AS title FROM actors WHERE id = $1 AND tenant_id = $2",
    "commitment": "SELECT title FROM commitments WHERE id = $1 AND tenant_id = $2",
    "goal":       "SELECT title FROM goals WHERE id = $1 AND tenant_id = $2",
    "decision":   "SELECT title FROM decisions WHERE id = $1 AND tenant_id = $2",
    "resource":   "SELECT identity AS title FROM resources WHERE id = $1 AND tenant_id = $2",
    "observation":"SELECT left(content_text, 100) AS title FROM observations WHERE id = $1 AND tenant_id = $2",
    "model":      'SELECT left("natural", 100) AS title FROM models WHERE id = $1 AND tenant_id = $2',
}


async def _resolve_entity_title(
    kind: str, eid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> str | None:
    sql = _TITLE_SQL_BY_TYPE.get(kind)
    if sql is None:
        return None
    try:
        row = await conn.fetchrow(sql, eid, tenant_id)
    except Exception:
        return None
    return (row["title"] if row else None) or None


# The module-level `app` used by `uvicorn services.gateway:app`. Lazy
# initialised (lifespan handles pool / repo / embedder wiring).
app = build_app()


__all__ = ["app", "build_app", "GatewayDeps"]
