"""services/app/gateway/main.py — FastAPI entry point.

BUILD-PLAN §3 Prompt 2.A. Delivers:

- POST /ingest/{channel}  — routes to services.ingest.ingestion.core.ingest
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
from datetime import timedelta
from typing import Any, AsyncIterator, Awaitable, Callable
from uuid import UUID

import asyncpg
import structlog
from fastapi import (
    Depends,
    FastAPI,
    Request,
    Response,
    status,
)
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.app.gateway.auth import (
    AuthContext,
    create_session,
    validate_token,
)
from services.app.gateway.db_bootstrap import (
    close_gateway_pool,
    create_gateway_pool,
)
from services.app.gateway.logging_config import configure_structlog, get_logger
from services.app.gateway.rate_limit import RateLimiter, RateTier
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
    # Prometheus scrape path for the webhook verification/resolver
    # counters (FR-011). Scrapers carry no Bearer token, and the data is
    # bounded-enum counters with no tenant/installation labels (FR-015).
    "/metrics",
    "/auth/session",
    # IN-08: the OAuth callback is public (state-token-authed inside
    # the handler). The /install route stays Bearer-required, so it is
    # NOT in this allowlist. We deliberately do NOT add "/integrations/"
    # as a prefix entry — single-route, not blanket public.
    "/integrations/slack/callback",
    # IN-09: same posture for Discord. /install stays Bearer-required.
    # /installed and /install-error are the redirect targets the OAuth
    # callback issues. The browser follows the 302 without a Bearer,
    # so these MUST be on the allowlist or the browser sees 401
    # missing_bearer after a successful install.
    "/integrations/discord/callback",
    "/integrations/discord/installed",
    "/integrations/discord/install-error",
    "/integrations/slack/installed",
    "/integrations/slack/install-error",
    # IN-13: GitHub App callback + redirect targets. /install stays
    # Bearer-required like Slack/Discord.
    "/integrations/github/callback",
    "/integrations/github/installed",
    "/integrations/github/install-error",
    # IN-14: Notion OAuth callback + redirect targets. /install stays
    # Bearer-required.
    "/integrations/notion/callback",
    "/integrations/notion/installed",
    "/integrations/notion/install-error",
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
    # cryptographic signature check inside services.app.webhooks.router —
    # NOT a Bearer token. The Bearer middleware MUST skip this prefix
    # or every webhook becomes a 401 with `missing_bearer`.
    "/webhooks/",
    # Finance testing panel (Mercury + QuickBooks). Dev/testing tool scoped by
    # X-Tenant-Id header (no bearer), same posture as /debug. Env-gated at mount.
    "/finance/",
    # Slack DM testing panel (per-user OAuth DM ingestion). Same posture as
    # /finance — X-Tenant-Id header, no bearer, env-gated at mount.
    "/slack/",
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
        # (services.product.query.api.tenant_dep, services.product.greeting.api, …) work
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
    and services.app.webhooks.tenant_resolver into the module-load graph
    when tests don't need them.
    """
    import time

    from lib.shared.secrets import build_secret_store
    from services.app.webhooks.secrets import assert_prod_safety_invariants
    from services.app.webhooks.tenant_resolver import (
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

    # M5.3 cutover: the webhook router reads `ingestion.kafka_path_enabled`
    # off `app.state.tenant_flags` to decide pipeline-vs-inline per tenant.
    # Without this the cutover branch can never activate and every provider
    # stays on inline ingest(). Per-process reader with a 30s TTL cache.
    if getattr(app_.state, "tenant_flags", None) is None:
        from services.ingest.ingestion.feature_flags import TenantFlags
        app_.state.tenant_flags = TenantFlags(pool)

    # IN-13: GitHub App outbound client (single instance per pod;
    # owns the installation-access-token cache) and replay LRU
    # (in-process; FR-014 — defense-in-depth, not correctness gate).
    if getattr(app_.state, "github_client", None) is None:
        from services.ingest.integrations.github.client import GithubClient
        app_.state.github_client = GithubClient(
            pool=pool,
            tenant_resolver=app_.state.tenant_resolver,
        )
    if getattr(app_.state, "github_replay_cache", None) is None:
        from services.ingest.integrations.github.replay_cache import (
            make_replay_cache,
        )
        app_.state.github_replay_cache = make_replay_cache()


async def _wire_ingestion_data_plane(app_: FastAPI) -> None:
    """Wire the ingestion data plane onto ``app.state`` for ALL sources.

    Webhook / Pub/Sub ingress must traverse the real data plane:
    ``shadow_write`` → Kafka ``ingestion.raw`` → normalizer →
    observation_writer (instead of the inline ``ingest()`` against the
    gateway DB). This builds the single Kafka producer + raw-tier S3
    client every ingress path hands to ``shadow_write_raw``.

    CANONICAL NAMES: stored under ``app.state.kafka_producer`` /
    ``app.state.s3_raw_client`` — the names the slack/github M5.3 cutover
    branch (``services/app/webhooks/router.py::_attempt_kafka_path``) and the
    gmail Pub/Sub endpoint (``services/app/webhooks/gmail_pubsub.py``) read.
    Wiring them activates the full pipeline for those providers once their
    tenant's ``ingestion.kafka_path_enabled`` flag is TRUE; until then the
    cutover branch is a no-op and ingress stays inline (graceful
    degradation, never a drop).

    Notion has no inline handler, so it reads the SAME producer + S3
    client via the ``app.state.notion_data_plane`` alias (kept for the
    IN-14 handler's call sites) — one producer/connection per process,
    shared.

    Guarded: when ``KAFKA_BOOTSTRAP_SERVERS`` is unset (unit tests,
    minimal deployments) this is a no-op; the cutover branch then sees
    missing deps and every provider stays inline. A producer/S3 startup
    failure is logged and swallowed so it never blocks gateway startup.
    """
    if getattr(app_.state, "kafka_producer", None) is not None:
        return
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not brokers:
        return
    try:
        from types import SimpleNamespace

        from services.ingest.ingestion.kafka.producer import (
            IdempotentProducer,
            ProducerConfig,
        )
        from services.ingest.ingestion.raw_tier.s3 import S3Client

        producer = IdempotentProducer(
            ProducerConfig(
                bootstrap_servers=brokers,
                client_id="gateway-ingress",
            )
        )
        await producer.start()
        s3_client = S3Client(
            os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        )
        await s3_client.connect()
        # Canonical names — slack/github cutover + gmail Pub/Sub read these.
        app_.state.kafka_producer = producer
        app_.state.s3_raw_client = s3_client
        # IN-14 alias — the Notion webhook handler reads the same instances.
        app_.state.notion_data_plane = SimpleNamespace(
            producer=producer, s3_client=s3_client,
        )
        log.info("ingestion_data_plane_wired", brokers=brokers)
    except Exception as exc:  # noqa: BLE001 — never block startup
        log.error(
            "ingestion_data_plane_wiring_failed",
            error_type=type(exc).__name__,
            error=str(exc),
        )


async def _close_ingestion_data_plane(app_: FastAPI) -> None:
    """Tear down the data plane wired by ``_wire_ingestion_data_plane``.

    The canonical names and the Notion alias point at the SAME producer +
    S3 client; stop/close each exactly once.
    """
    producer = getattr(app_.state, "kafka_producer", None)
    s3_client = getattr(app_.state, "s3_raw_client", None)
    if producer is not None:
        try:
            await producer.stop()
        except Exception:  # noqa: BLE001
            pass
    if s3_client is not None:
        try:
            await s3_client.close()
        except Exception:  # noqa: BLE001
            pass


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

    When the Gateway is started normally (via `uvicorn services.app.gateway:app`),
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
        # Ensure the Pelago demo company is registered. Tests truncate
        # demo_configs between cases (see services/app/gateway/tests/
        # conftest.py), so after running pytest the dev/demo database
        # is left with no demo companies and the UI's AutoDemoSession
        # gets stuck on "can't start demo". Idempotent — the migration
        # uses ON CONFLICT DO UPDATE.
        try:
            await _ensure_demo_seed(pool)
        except Exception:  # noqa: BLE001 — startup must not fail here
            log.exception("demo_seed_warning")
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
        # a services.app.gateway ↔ services.app.realtime circular.
        if getattr(app_.state, "realtime", None) is None:
            from services.app.realtime.main import (
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
        # Wire the ingestion data plane (Kafka producer + S3 raw client)
        # under the canonical names so EVERY source's webhook / Pub/Sub
        # ingress can traverse the full pipeline instead of inline
        # ingest(); Notion reads the same instances via its alias.
        # Guarded + swallow-on-failure (see helper docstring).
        await _wire_ingestion_data_plane(app_)
        try:
            yield
        finally:
            await _close_ingestion_data_plane(app_)
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
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "lifespan_dispatcher_stop_failed",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
            ceo = getattr(app_.state, "ceo_view", None)
            if ceo is not None:
                scheduler = ceo.get("scheduler")
                if scheduler is not None:
                    try:
                        await scheduler.stop()
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "lifespan_scheduler_stop_failed",
                            error=str(exc),
                            error_type=type(exc).__name__,
                        )
            deps: GatewayDeps = app_.state.deps
            if deps.embedder is not None:
                try:
                    await deps.embedder.close()
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "lifespan_embedder_close_failed",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
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
    # Import is deferred to runtime to break the services.app.gateway ↔
    # services.app.realtime circular import.
    if pool is not None:
        from services.app.realtime.main import (  # local import (break cycle)
            configure_realtime as _configure_realtime,
        )

        _configure_realtime(app, pool=pool, start=False)

    # Mount the demo router (Session 1 of DEMO-BUILD-PLAN). Adds the
    # picker, session lifecycle, simulator, and SSE recommendation
    # stream under /v1/demo/* (and /v1/recommendations/stream).
    from services.product.demo.router import demo_router as _demo_router

    app.include_router(_demo_router)

    from services.product.decision_deltas.router import build_router as build_decision_deltas_router
    from services.product.forecasts import build_router as build_forecasts_router
    from services.product.model_trace.router import router as model_trace_router
    from services.product.history.router import router as history_router
    from services.app.webhooks.router import build_webhooks_router

    app.include_router(build_decision_deltas_router())
    app.include_router(build_forecasts_router())
    app.include_router(model_trace_router)
    app.include_router(history_router)
    app.include_router(build_webhooks_router())

    # Spec-aligned product routes (Operating Threads, Decision Deltas
    # spec view, Forecasts spec view, unified Ledger Events). The UI
    # tries these endpoints first and falls back to in-browser fixtures
    # when they 404 — so adding them here is purely additive.
    from services.app.gateway.spec_routes import register_spec_routes

    register_spec_routes(app)

    # Model page v2.
    # Adapter over the existing models / model_edges / model_trace
    # substrate. The UI falls back to fixtures when data is sparse, so
    # this is purely additive.
    from services.app.gateway.model_page_routes import register_model_page_routes

    register_model_page_routes(app)

    # Today page v2.
    # Synthesizes the spec's Proposed Change wire shape from the
    # existing decision_deltas + evidence + topology_events tables.
    # The DB schema is unchanged; status / field divergences are
    # handled in the synth layer.
    from services.app.gateway.today_routes import register_today_routes

    register_today_routes(app)

# IN-08: integrations router (Slack OAuth install + callback).
    # Mounted at /integrations/{provider}/*. The /install route is
    # Bearer-authed (standard middleware); only /callback is in the
    # public-paths allowlist (exact match, no prefix exposure).
    from services.ingest.integrations.router import build_integrations_router

    app.include_router(build_integrations_router())

    # Jira — production install surface (/integrations/jira/connect/*).
    # Bearer-authed admin connect wizard: verify the API token, enumerate
    # projects, store credentials encrypted, then finalize_install + register
    # the webhook edge. Unlike the finance panel below, this is a genuine prod
    # flow (tenant from Bearer auth, real credentials), so it mounts
    # unconditionally. Isolated try so a mount error can't block startup.
    try:
        from services.ingest.integrations.jira.oauth import router as _jira_router

        app.include_router(_jira_router)
        log.info("jira_router_mounted")
    except Exception as exc:  # noqa: BLE001 — never block startup
        log.error("jira_router_mount_failed", error=str(exc))

    # Mercury + QuickBooks — production install surfaces
    # (/integrations/{mercury,quickbooks}/connect/*). Bearer-authed credential
    # wizards that verify the real token/realm, store it encrypted, then
    # finalize_install + register the webhook edge. These are genuine prod
    # surfaces (distinct from the synthetic /finance dev panel below) and so
    # mount unconditionally. Isolated try so a mount error can't block startup.
    try:
        from services.ingest.integrations.mercury.oauth import router as _mercury_router
        from services.ingest.integrations.quickbooks.oauth import router as _qbo_router

        app.include_router(_mercury_router)
        app.include_router(_qbo_router)
        log.info("finance_install_routers_mounted")
    except Exception as exc:  # noqa: BLE001 — never block startup
        log.error("finance_install_routers_mount_failed", error=str(exc))

    # Finance testing control plane (Mercury + QuickBooks): install / backfill /
    # live-emit / status for the UI panel. On by default (the testing
    # deliverable); set FINANCE_PANEL_ENABLED=0 to disable in real prod.
    if os.environ.get("FINANCE_PANEL_ENABLED", "1") != "0":
        try:
            from services.app.gateway.finance_router import build_finance_router
            app.include_router(build_finance_router())
        except Exception as exc:  # noqa: BLE001 — never block startup
            log.error("finance_router_mount_failed", error=str(exc))

    # Slack DM testing control plane (per-user OAuth human↔human DM ingestion):
    # install / backfill / live-emit / status. On by default (the testing
    # deliverable); set SLACK_DM_PANEL_ENABLED=0 to disable in real prod.
    if os.environ.get("SLACK_DM_PANEL_ENABLED", "1") != "0":
        try:
            from services.app.gateway.slack_router import build_slack_router
            app.include_router(build_slack_router())
        except Exception as exc:  # noqa: BLE001 — never block startup
            log.error("slack_router_mount_failed", error=str(exc))

    # GitHub Intelligence Layer — read-only query surface (/github-intel/*).
    # Bearer-authed (standard middleware) + per-tenant repo allowlist in the
    # router; no public-path exposure.
    from services.ingest.github_intel.api import build_github_intel_router

    app.include_router(build_github_intel_router())
    return app


# ---------------------------------------------------------------------
# Helpers — deps resolver (for routes + middleware that run late)
# ---------------------------------------------------------------------


async def _ensure_demo_seed(pool) -> None:  # type: ignore[no-untyped-def]
    """Re-apply the Pelago demo_configs row if it's missing.

    Tests in services/app/gateway/tests/conftest.py TRUNCATE all tables
    between cases, which empties demo_configs and leaves the dev
    database without any demo companies. Without this safeguard,
    starting the gateway after a pytest run produces a broken demo:
    AutoDemoSession in the UI POSTs /api/v1/demo/sessions/start, the
    gateway responds `unknown demo company_id='pelago'`, and the
    page never advances past the "Loading Pelago…" splash.

    The insert uses ON CONFLICT DO UPDATE so it's safe to call on
    every startup. UUID + JSONB literals match
    db/migrations/0028_pelago_demo_config.sql.
    """
    have = await pool.fetchval(
        "SELECT 1 FROM demo_configs WHERE company_id = 'pelago' LIMIT 1"
    )
    if have:
        return
    await pool.execute(
        """
        INSERT INTO demo_configs (
            id, company_id, name, description, tagline, snapshot_uri,
            model_routing, cost_cap_usd_per_session, determinism_seed
        ) VALUES (
            '00000000-0000-7d23-8000-000000000004'::uuid,
            'pelago',
            'Pelago',
            $1,
            'Series A, multi-shock year, founder running on signals',
            'demo/snapshots/pelago-v1.sql.zst',
            $2::jsonb,
            5.00,
            42
        )
        ON CONFLICT (company_id) DO UPDATE
          SET name = EXCLUDED.name,
              description = EXCLUDED.description,
              tagline = EXCLUDED.tagline,
              snapshot_uri = EXCLUDED.snapshot_uri,
              model_routing = EXCLUDED.model_routing,
              cost_cap_usd_per_session = EXCLUDED.cost_cap_usd_per_session,
              determinism_seed = EXCLUDED.determinism_seed
        """,
        (
            "Series A B2B SaaS revenue-intelligence platform. 35 people, "
            "$5.8M ARR, 28 customers. Just closed a $14M Series A. The "
            "company is 9 months in: an anchor design partner has churned, "
            "the VP Eng departed mid-year, and the org has just "
            "reorganized around integration surfaces."
        ),
        '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}',
    )
    log.info("demo_seed_inserted", extra={"company_id": "pelago"})


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

    from services.app.gateway.sage_internal_router import build_sage_internal_router

    app.include_router(build_sage_internal_router())
    from services.app.gateway.recommendations_router import (
        build_recommendations_router,
    )
    from services.app.gateway.structure_router import build_structure_router
    from services.app.gateway.today_core_router import build_today_core_router

    app.include_router(build_recommendations_router())
    app.include_router(build_structure_router())
    app.include_router(build_today_core_router())

    @app.get("/metrics")
    async def metrics() -> Response:
        """Prometheus scrape endpoint for the webhook verification +
        tenant-resolver counters (FR-011 / FR-018). Hand-rolled text
        exposition — no prometheus_client dep. Public (allowlisted in
        `_PUBLIC_PATHS`) because scrapers don't carry a Bearer token and
        the counters are non-sensitive bounded enums."""
        from services.app.webhooks import metrics as webhook_metrics

        return Response(
            content=webhook_metrics.render_prometheus(),
            media_type="text/plain; version=0.0.4",
        )

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
        except HandlerNotFound:
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
        from services.reasoning.contestability import (
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
    # These wrap services/domain/bridge/ for the UI. Each applies tenant
    # isolation via auth.tenant_id; the per-customer endpoint also
    # consults access_control.can_read_by_id on the customer Resource.
    @app.get("/dashboard/revenue-at-risk")
    async def get_dashboard_revenue_at_risk(
        request: Request, horizon_days: int = 90,
    ) -> dict[str, Any]:
        from services.domain.bridge import render_revenue_at_risk
        auth: AuthContext = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_revenue_at_risk(
                auth.tenant_id, horizon_days=int(horizon_days), conn=conn
            )
        return json.loads(result.model_dump_json())

    @app.get("/dashboard/goals")
    async def get_dashboard_goals(request: Request) -> dict[str, Any]:
        from services.domain.bridge import render_goals
        auth: AuthContext = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_goals(auth.tenant_id, conn=conn)
        return json.loads(result.model_dump_json())

    @app.get("/dashboard/capacity")
    async def get_dashboard_capacity(request: Request) -> dict[str, Any]:
        from services.domain.bridge import render_capacity
        auth: AuthContext = request.state.auth
        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            result = await render_capacity(auth.tenant_id, conn=conn)
        return json.loads(result.model_dump_json())

    @app.get("/dashboard/customer/{customer_id}")
    async def get_dashboard_customer(
        customer_id: str, request: Request, window_days: int = 30,
    ) -> Any:
        from services.platform.access_control.checks import can_read_by_id
        from services.domain.bridge import render_customer_detail

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

    # ---------------- /v1/history (History page aggregator) -------
    # Returns events / predictions / arcs / calibration / layer_counts
    # for the period requested. services.product.history.aggregator owns the
    # substrate→UI mapping; this handler is just the HTTP shell.
    @app.get("/v1/history")
    async def history_endpoint(request: Request) -> JSONResponse:
        from services.product.history import build_history

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

        types_raw = request.query_params.get("types")
        types_list = (
            [t for t in types_raw.split(",") if t]
            if types_raw else None
        )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            payload = await build_history(
                tenant_id=auth.tenant_id,
                period=period,
                conn=conn,
                types=types_list,
            )
        return JSONResponse(payload.to_dict(), status_code=200)

    # ---------------- /api/map/* (CEO Map view) ---------------------
    # Wires the four map endpoints onto `app`. Imported lazily so the
    # heavy sklearn dependency only loads when this Gateway boots; the
    # routes themselves only acquire it on first /api/map/* hit.
    from services.app.gateway.map_routes import register_map_routes

    register_map_routes(app)

    # ---------------- WS /stream ------------------------------------
    # Wave 4-D mounts the real realtime router on startup via
    # `services.app.realtime.configure_realtime(app, pool=pool)`. The
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
         routes are mounted via `services.product.rendering.api.router`.
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
    from services.product.rendering.api import (
        get_service as _rnd_get_service,
        router as rnd_router,
    )
    from services.product.rendering.core import RenderingService

    # Build the rendering service with the gateway pool so cost rows
    # land in `view_render_costs`.
    _rnd_service = RenderingService.from_env(pool=pool)
    app_.include_router(rnd_router)
    app_.dependency_overrides[_rnd_get_service] = lambda: _rnd_service

    # ---- 2. GRT — scheduler + stream + HTTP router -----------------
    from services.product.greeting.cache import ViewCeoCacheRepo
    from services.product.greeting.scheduler import GreetingScheduler, SchedulerConfig
    from services.product.greeting.snapshot import FounderContext
    from services.product.greeting.stream import (
        StaticTenantTokenMap,
        ViewCeoStreamManager,
        build_ceo_stream_router,
    )
    from services.product.greeting.api import build_ceo_api_router
    from services.product.greeting.rendering_adapter import build_rendering_adapter
    from services.product.greeting.viewer_state_repo import ViewerStateRepo

    cache_repo = ViewCeoCacheRepo(pool)
    viewer_state_repo = ViewerStateRepo(pool)
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
            viewer_state_repo=viewer_state_repo,
            default_tenant_id=_UUID(default_tenant) if default_tenant else None,
        )
    )
    app_.include_router(build_ceo_stream_router(stream_manager))

    # ---- 3. QRY — handler + router ---------------------------------
    from services.app.gateway.db_bootstrap import _register_codecs as _codec_hook  # noqa: F401
    from services.product.query.adapters import (
        build_cache_adapter as _build_qry_cache,
        build_rendering_adapter as _build_qry_rnd,
    )
    from services.product.query.core import QueryHandler
    from services.product.query.api import build_router as build_query_router

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
    from services.product.conversations import (
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
    # Default ON in dev/test, OFF in prod. In production the sim /
    # authoring endpoints are NEVER mounted, regardless of
    # GATEWAY_MOUNT_SIM, because `/simulation/inject` is an
    # unauthenticated, signature-free, caller-chooses-tenant
    # substrate-injection surface (it lives under the public path
    # allowlist). A stray GATEWAY_MOUNT_SIM=1 in a prod compose must not
    # be able to re-open it.
    from lib.shared.env import env_name as _env_name_fn, is_prod as _is_prod
    env_name = _env_name_fn()
    _prod = _is_prod()
    _sim_requested = (
        os.environ.get("GATEWAY_MOUNT_SIM", "0" if _prod else "1") == "1"
    )
    if _prod and _sim_requested:
        log.error(
            "sim_mount_refused_in_prod",
            reason=(
                "GATEWAY_MOUNT_SIM=1 ignored in production; "
                "/simulation/* is an unauthenticated injection surface"
            ),
        )
    if _sim_requested and not _prod:
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
                    _pl.Path(__file__).resolve().parents[3]
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

    # ---- 4.4 GMAIL Pub/Sub push ingress (always mounted) ----------
    # The webhook ingress mounts UNCONDITIONALLY so Google's pushes always
    # hit a real endpoint (never a silent 404). When the OIDC env isn't set
    # the route returns an explicit 503 `not_configured` rather than 500 —
    # the readiness is observable, not silently skipped. Decoupled from the
    # DWD-credential gate below (which the connect wizards genuinely need).
    try:
        from services.app.webhooks.gmail_pubsub import (
            is_pubsub_configured,
            router as _gmail_pubsub_router,
        )

        app_.include_router(_gmail_pubsub_router)
        if is_pubsub_configured():
            log.info("gmail_pubsub_ingress_mounted", configured=True)
        else:
            # Explicit signal — the ingress exists but can't verify until the
            # OIDC env (GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE + _SA) is set.
            log.warning("gmail_pubsub_ingress_mounted_unconfigured", configured=False)
    except Exception as exc:  # noqa: BLE001
        log.warning("gmail_pubsub_mount_failed", error=str(exc))

    # ---- 4.4b GOOGLE Calendar/Drive push ingress (always mounted) --
    # The native web_hook push endpoints (events.watch / changes.watch land
    # here). Always mounted so Google's pings hit a real endpoint; the channel
    # token is verified per-request in the handler. The watch_scheduler only
    # registers channels when GOOGLE_PUSH_WEBHOOK_BASE is set — otherwise the
    # live poller is the liveness path and these routes simply idle.
    try:
        from services.app.webhooks.google_push import router as _google_push_router

        app_.include_router(_google_push_router)
        log.info("google_push_ingress_mounted")
    except Exception as exc:  # noqa: BLE001
        log.warning("google_push_mount_failed", error=str(exc))

    # ---- 4.5 GMAIL — admin connect wizard --------------------------
    # The DWD connect wizard genuinely needs the service-account JSON, so it
    # stays gated. When absent we log explicitly (no silent skip).
    if (
        os.environ.get("GMAIL_SERVICE_ACCOUNT_JSON_FILE")
        or os.environ.get("GMAIL_SERVICE_ACCOUNT_JSON")
    ):
        try:
            from services.ingest.integrations.gmail.oauth import router as _gmail_oauth_router

            app_.include_router(_gmail_oauth_router)
            log.info("gmail_routers_mounted")
        except Exception as exc:  # noqa: BLE001
            log.warning("gmail_mount_failed", error=str(exc))

        # ---- 4.6 GOOGLE CALENDAR — admin connect wizard -----------
        # Calendar reuses Gmail's DWD service account, so it mounts under
        # the same credential gate. (Live ingestion runs via the
        # google_calendar_live_poller + events.watch push channel; the push
        # ingress is mounted unconditionally above.) Isolated try so a
        # Calendar import error can't unmount Gmail.
        try:
            from services.ingest.integrations.google_calendar.oauth import (
                router as _gcal_oauth_router,
            )

            app_.include_router(_gcal_oauth_router)
            log.info("google_calendar_router_mounted")
        except Exception as exc:  # noqa: BLE001
            log.warning("google_calendar_mount_failed", error=str(exc))

        # ---- 4.7 GOOGLE DRIVE — admin connect wizard --------------
        # Same posture as Calendar: reuses Gmail's DWD service account. Live
        # ingestion runs via the google_drive_live_poller + changes.watch push
        # channel (ingress mounted unconditionally above). Isolated try.
        try:
            from services.ingest.integrations.google_drive.oauth import (
                router as _gdrive_oauth_router,
            )

            app_.include_router(_gdrive_oauth_router)
            log.info("google_drive_router_mounted")
        except Exception as exc:  # noqa: BLE001
            log.warning("google_drive_mount_failed", error=str(exc))

    # ---- 5. DEBUG — inspector router -------------------------------
    # Read-only endpoints for /debug UI: signals, think runs, models,
    # acts, renders, cache. Gated by COMPANY_OS_ENV so prod doesn't
    # leak raw prompts + substrate.
    if env_name in ("dev", "staging", "test"):
        try:
            from services.app.gateway.debug_router import build_debug_router
            app_.include_router(build_debug_router())
        except Exception as exc:  # noqa: BLE001
            log.warning("debug_router_mount_failed", error=str(exc))

    # Expose under a common state bag for observability + teardown.
    app_.state.ceo_view = {
        "scheduler": scheduler,
        "cache": cache_repo,
        "viewer_state_repo": viewer_state_repo,
        "stream_manager": stream_manager,
        "rendering_adapter": rendering_adapter,
        "qry_handler": qry_handler,
        "tenant_id": _UUID(default_tenant) if default_tenant else None,
        "token": ceo_token,
    }




# The module-level `app` used by `uvicorn services.app.gateway:app`. Lazy
# initialised (lifespan handles pool / repo / embedder wiring).
app = build_app()


__all__ = ["app", "build_app", "GatewayDeps"]
