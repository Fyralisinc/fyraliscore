"""Gateway route access classification.

This module describes the gateway bearer-middleware bypass surface and the
current access boundary for route inventory checks. It is intentionally about
transport-level exposure, not fine-grained substrate authorization.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import Enum

from fastapi import FastAPI
from fastapi.routing import APIRoute, APIWebSocketRoute


class RouteAccess(str, Enum):
    PUBLIC = "public"
    BEARER = "bearer-auth"
    PROVIDER_SIGNED = "provider-signed"
    SELF_AUTHENTICATED = "self-authenticated"
    EXTENSION_AUTH = "extension-auth"
    ADMIN = "admin-only"
    INTERNAL = "internal-only"
    DEBUG = "debug"


@dataclass(frozen=True, slots=True)
class RouteAccessPolicy:
    access: RouteAccess
    reason: str
    gateway_bearer_required: bool


@dataclass(frozen=True, slots=True)
class RouteInventoryEntry:
    methods: tuple[str, ...]
    path: str
    name: str
    tags: tuple[str, ...]
    policy: RouteAccessPolicy


_PUBLIC = RouteAccessPolicy(
    access=RouteAccess.PUBLIC,
    reason="public health/control endpoint with no tenant payload",
    gateway_bearer_required=False,
)
_BOOTSTRAP = RouteAccessPolicy(
    access=RouteAccess.SELF_AUTHENTICATED,
    reason="session bootstrap endpoint authenticated by X-Bootstrap-Secret when configured",
    gateway_bearer_required=False,
)
_OAUTH_CALLBACK = RouteAccessPolicy(
    access=RouteAccess.SELF_AUTHENTICATED,
    reason="OAuth callback/redirect endpoint authenticated by provider state token",
    gateway_bearer_required=False,
)
_PROVIDER_SIGNED = RouteAccessPolicy(
    access=RouteAccess.PROVIDER_SIGNED,
    reason="provider webhook authenticated by provider signature or verification token",
    gateway_bearer_required=False,
)
_VIEW_TOKEN = RouteAccessPolicy(
    access=RouteAccess.SELF_AUTHENTICATED,
    reason="CEO-view surface uses its own view token/session contract",
    gateway_bearer_required=False,
)
_IN_PROCESS = RouteAccessPolicy(
    access=RouteAccess.INTERNAL,
    reason="in-process/internal adapter endpoint; must be protected by deployment boundary",
    gateway_bearer_required=False,
)
_DEBUG = RouteAccessPolicy(
    access=RouteAccess.DEBUG,
    reason="debug/dev endpoint; only mounted when DEBUG_ENDPOINTS_ENABLED is true",
    gateway_bearer_required=False,
)
_EXTENSION = RouteAccessPolicy(
    access=RouteAccess.EXTENSION_AUTH,
    reason="extension API uses extension OAuth bearer plus tenant grant checks",
    gateway_bearer_required=False,
)
_BYOC_CONTROL_PLANE = RouteAccessPolicy(
    access=RouteAccess.SELF_AUTHENTICATED,
    reason=(
        "BYOC data-plane route authenticated by signed enrollment, "
        "signed evidence submissions, signed desired-state updates, "
        "or mTLS-bound agent identity"
    ),
    gateway_bearer_required=False,
)
_PLATFORM_ONBOARDING = RouteAccessPolicy(
    access=RouteAccess.SELF_AUTHENTICATED,
    reason=(
        "customer-facing hosted onboarding entrypoint creates sanitized "
        "commercial/BYOC setup metadata before a tenant session exists"
    ),
    gateway_bearer_required=False,
)
_INTERNAL_BEARER = RouteAccessPolicy(
    access=RouteAccess.INTERNAL,
    reason="internal API currently protected by gateway bearer auth; deployment boundary still required",
    gateway_bearer_required=True,
)
_ADMIN_BEARER = RouteAccessPolicy(
    access=RouteAccess.ADMIN,
    reason="admin/operator API requires gateway bearer auth plus tenant-scoped admin role",
    gateway_bearer_required=True,
)
_BEARER = RouteAccessPolicy(
    access=RouteAccess.BEARER,
    reason="default gateway actor-session bearer auth",
    gateway_bearer_required=True,
)


GATEWAY_BEARER_BYPASS_PATH_POLICIES: dict[str, RouteAccessPolicy] = {
    "/healthz": _PUBLIC,
    "/readyz": _PUBLIC,
    "/metrics": _PUBLIC,
    "/legal/local-test-privacy": _PUBLIC,
    "/auth/session": _BOOTSTRAP,
    "/integrations/slack/callback": _OAUTH_CALLBACK,
    "/integrations/slack/installed": _OAUTH_CALLBACK,
    "/integrations/slack/install-error": _OAUTH_CALLBACK,
    "/integrations/discord/callback": _OAUTH_CALLBACK,
    "/integrations/discord/installed": _OAUTH_CALLBACK,
    "/integrations/discord/install-error": _OAUTH_CALLBACK,
    "/integrations/github/callback": _OAUTH_CALLBACK,
    "/integrations/github/installed": _OAUTH_CALLBACK,
    "/integrations/github/install-error": _OAUTH_CALLBACK,
    "/integrations/notion/callback": _OAUTH_CALLBACK,
    "/integrations/notion/installed": _OAUTH_CALLBACK,
    "/integrations/notion/install-error": _OAUTH_CALLBACK,
    # Figma returns the browser here with a signed, single-use OAuth state.
    # Keep the normal start/status/retry routes bearer-protected; only this
    # provider callback may bypass actor-session authentication.
    "/integrations/figma/oauth/callback": _OAUTH_CALLBACK,
    "/integrations/whatsapp/webhook": _PROVIDER_SIGNED,
    "/integrations/facebook_pages/callback": _OAUTH_CALLBACK,
    "/integrations/facebook_pages/installed": _OAUTH_CALLBACK,
    "/integrations/facebook_pages/install-error": _OAUTH_CALLBACK,
    "/integrations/facebook_pages/webhook": _PROVIDER_SIGNED,
    "/integrations/instagram/callback": _OAUTH_CALLBACK,
    "/integrations/instagram/webhook": _PROVIDER_SIGNED,
}

GATEWAY_BEARER_BYPASS_PATHS = frozenset(GATEWAY_BEARER_BYPASS_PATH_POLICIES)

GATEWAY_BEARER_BYPASS_PREFIX_POLICIES: tuple[
    tuple[str, RouteAccessPolicy],
    ...,
] = (
    ("/view/ceo/", _VIEW_TOKEN),
    ("/rendering/", _IN_PROCESS),
    ("/debug/", _DEBUG),
    ("/api/debug/", _DEBUG),
    ("/webhooks/", _PROVIDER_SIGNED),
    ("/ext/", _EXTENSION),
    ("/byoc/agent/", _BYOC_CONTROL_PLANE),
    ("/byoc/control-plane/", _BYOC_CONTROL_PLANE),
    ("/platform/onboarding/", _PLATFORM_ONBOARDING),
)

GATEWAY_BEARER_BYPASS_PREFIXES = tuple(
    prefix for prefix, _policy in GATEWAY_BEARER_BYPASS_PREFIX_POLICIES
)

# Historical stream bypass. Kept exact to the middleware's previous behavior.
GATEWAY_BEARER_BYPASS_SPECIAL_PREFIXES = ("/stream",)


def classify_gateway_route(path: str) -> RouteAccessPolicy:
    """Classify a FastAPI route path by its current transport access boundary."""
    exact = GATEWAY_BEARER_BYPASS_PATH_POLICIES.get(path)
    if exact is not None:
        return exact
    if path == "/debug" or path.startswith(("/debug/", "/api/debug/")):
        return _DEBUG
    if path.startswith("/api/admin/"):
        return _ADMIN_BEARER
    if path.startswith("/internal/"):
        return _INTERNAL_BEARER
    for prefix, policy in GATEWAY_BEARER_BYPASS_PREFIX_POLICIES:
        if path.startswith(prefix):
            return policy
    if path.startswith(GATEWAY_BEARER_BYPASS_SPECIAL_PREFIXES):
        return RouteAccessPolicy(
            access=RouteAccess.SELF_AUTHENTICATED,
            reason="legacy realtime stream bypass; route must authenticate within the stream protocol",
            gateway_bearer_required=False,
        )
    return _BEARER


def gateway_auth_bypassed(path: str, extension_prefixes: Iterable[str] = ()) -> bool:
    """Return whether the gateway actor-session middleware should skip a path."""
    return (
        path in GATEWAY_BEARER_BYPASS_PATHS
        or path.startswith(GATEWAY_BEARER_BYPASS_SPECIAL_PREFIXES)
        or any(path.startswith(prefix) for prefix in GATEWAY_BEARER_BYPASS_PREFIXES)
        or any(path.startswith(prefix) for prefix in extension_prefixes)
    )


def iter_gateway_route_inventory(app: FastAPI) -> list[RouteInventoryEntry]:
    entries: list[RouteInventoryEntry] = []
    for route, path, inherited_tags in _iter_effective_routes(app.routes):
        if isinstance(route, APIRoute):
            methods = tuple(sorted(route.methods or ()))
            entries.append(
                RouteInventoryEntry(
                    methods=methods,
                    path=path,
                    name=route.name,
                    tags=inherited_tags
                    + tuple(str(tag) for tag in (route.tags or ())),
                    policy=classify_gateway_route(path),
                )
            )
        elif isinstance(route, APIWebSocketRoute):
            entries.append(
                RouteInventoryEntry(
                    methods=("WEBSOCKET",),
                    path=path,
                    name=route.name,
                    tags=inherited_tags,
                    policy=classify_gateway_route(path),
                )
            )
    return sorted(entries, key=lambda entry: (entry.path, entry.methods))


def _iter_effective_routes(
    routes: Iterable[object],
    *,
    prefix: str = "",
    inherited_tags: tuple[str, ...] = (),
) -> Iterator[tuple[APIRoute | APIWebSocketRoute, str, tuple[str, ...]]]:
    """Walk flat and FastAPI 0.139+ lazy included-router inventories."""
    for route in routes:
        if isinstance(route, APIRoute | APIWebSocketRoute):
            yield route, f"{prefix}{route.path}", inherited_tags
            continue

        original_router = getattr(route, "original_router", None)
        include_context = getattr(route, "include_context", None)
        nested_routes = getattr(original_router, "routes", None)
        if include_context is None or nested_routes is None:
            continue
        nested_prefix = f"{prefix}{getattr(include_context, 'prefix', '')}"
        nested_tags = inherited_tags + tuple(
            str(tag) for tag in (getattr(include_context, "tags", ()) or ())
        )
        yield from _iter_effective_routes(
            nested_routes,
            prefix=nested_prefix,
            inherited_tags=nested_tags,
        )


__all__ = [
    "GATEWAY_BEARER_BYPASS_PATHS",
    "GATEWAY_BEARER_BYPASS_PREFIXES",
    "GATEWAY_BEARER_BYPASS_SPECIAL_PREFIXES",
    "RouteAccess",
    "RouteAccessPolicy",
    "RouteInventoryEntry",
    "classify_gateway_route",
    "gateway_auth_bypassed",
    "iter_gateway_route_inventory",
]
