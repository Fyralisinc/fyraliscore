"""Gateway middleware and bearer-auth bypass policy."""
from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

import structlog
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from lib.observability import counter, histogram
from lib.shared.ids import uuid7
from services.app.gateway.auth import AuthContext, validate_token
from services.app.gateway.deps import get_gateway_deps
from services.app.gateway.logging_config import get_logger
from services.app.gateway.rate_limit import RateTier


log = get_logger("gateway")


# Paths that do not require authentication (e.g. health checks, the
# session-minting endpoint itself uses a separate actor lookup).
_PUBLIC_PATHS = frozenset({
    "/healthz",
    "/readyz",
    # Prometheus scrape path for the webhook verification/resolver
    # counters (FR-011). Scrapers carry no Bearer token, and the data is
    # bounded-enum counters with no tenant/installation labels (FR-015).
    "/metrics",
    "/auth/session",
    # IN-08: the OAuth callback is public (state-token-authed inside
    # the handler). The /install route stays Bearer-required, so it is
    # NOT in this allowlist. We deliberately do NOT add "/integrations/"
    # as a prefix entry - single-route, not blanket public.
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
    # WhatsApp (Cloud API) webhook ingress. Authentication is Meta's
    # X-Hub-Signature-256 HMAC (verified inside whatsapp_router), NOT a Bearer
    # token — same posture as the /webhooks/* prefix. Both the GET subscribe
    # handshake and the POST event delivery hit this exact path, so the Bearer
    # middleware MUST skip it or every WhatsApp webhook becomes a 401.
    "/integrations/whatsapp/webhook",
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
    "/debug/",
    "/api/debug/",
    # IN-06: webhook ingress. Authentication is the per-provider
    # cryptographic signature check inside services.app.webhooks.router -
    # NOT a Bearer token. The Bearer middleware MUST skip this prefix
    # or every webhook becomes a 401 with `missing_bearer`.
    "/webhooks/",
)
# Overlay packages (e.g. the demo: /v1/demo/companies, /v1/demo/sessions/start;
# the simulation panel: /simulation/) contribute their own public prefixes via
# the gateway extension seam — core no longer hardcodes them.


def _public_path_prefixes() -> tuple[str, ...]:
    """Core public prefixes plus any contributed by installed extensions."""
    from services.app.gateway.extensions import extension_public_path_prefixes

    return _PUBLIC_PATH_PREFIXES + extension_public_path_prefixes()


# ---------------------------------------------------------------------
# Request metrics. `route` is the matched route TEMPLATE
# (e.g. /v1/forecasts/{prediction_id}) — never the raw path — so UUID
# segments can't explode label cardinality; requests that match no route
# (404 scans) collapse into "unmatched". The duration histogram omits
# `status` to keep series count = methods × routes, not × status too.
# ---------------------------------------------------------------------
_HTTP_REQUESTS = counter(
    "http_requests_total",
    "Gateway requests by method, route template, and status code.",
    ("method", "route", "status"),
)
_HTTP_DURATION = histogram(
    "http_request_duration_seconds",
    "Gateway request latency by method and route template.",
    ("method", "route"),
)


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    template = getattr(route, "path", None)
    return template if isinstance(template, str) else "unmatched"


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Binds request_id to structlog context; logs request summary."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = str(uuid7())
        # Tenant header if present - otherwise bind DEFAULT_TENANT_ID
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
        except Exception as e:  # pragma: no cover - fallthrough for uncaught
            duration_ms = (time.monotonic() - started) * 1000
            route = _route_template(request)
            _HTTP_REQUESTS.inc(method=request.method, route=route, status="500")
            _HTTP_DURATION.observe(
                duration_ms / 1000.0, method=request.method, route=route
            )
            log.error(
                "request_failed",
                method=request.method,
                path=request.url.path,
                duration_ms=round(duration_ms, 2),
                error=type(e).__name__,
            )
            raise
        duration_ms = (time.monotonic() - started) * 1000
        route = _route_template(request)
        _HTTP_REQUESTS.inc(
            method=request.method, route=route, status=str(response.status_code)
        )
        _HTTP_DURATION.observe(
            duration_ms / 1000.0, method=request.method, route=route
        )
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


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Validates `Authorization: Bearer <token>` against actor_sessions."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if (
            request.url.path in _PUBLIC_PATHS
            or request.url.path.startswith("/stream")
            or any(request.url.path.startswith(p) for p in _public_path_prefixes())
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
                    deps = get_gateway_deps(request)
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
        deps = get_gateway_deps(request)
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
        # (services.product.query.api.tenant_dep, services.product.greeting.api, ...)
        # work under demo bearer auth without forcing every client to send
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


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Token-bucket limiter per (tenant, actor)."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if (
            request.url.path in _PUBLIC_PATHS
            or request.url.path.startswith("/stream")
            or any(request.url.path.startswith(p) for p in _public_path_prefixes())
        ):
            return await call_next(request)
        auth: AuthContext | None = getattr(request.state, "auth", None)
        if auth is None:
            return await call_next(request)
        deps = get_gateway_deps(request)
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


def _unauth(reason: str) -> Response:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )
