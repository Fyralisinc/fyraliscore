"""Gateway dependency bundle and lookup helper."""
from __future__ import annotations

from typing import Any

import asyncpg

from lib.embeddings.ollama import OllamaClient
from services.app.gateway.rate_limit import RateLimiter
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo


class GatewayDeps:
    """Container for Gateway-wide dependencies, attached to ``app.state``.

    Tests override individual attributes before constructing an
    ``httpx.AsyncClient(app=app, ...)``.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        actor_repo: ActorRepo,
        alias_repo: EntityAliasRepo,
        embedder: OllamaClient | None,
        rate_limiter: RateLimiter,
    ) -> None:
        self.pool = pool
        self.actor_repo = actor_repo
        self.alias_repo = alias_repo
        self.embedder = embedder
        self.rate_limiter = rate_limiter


def attach_gateway_deps(
    request_or_app: Any,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient | None,
    rate_limiter: RateLimiter,
) -> GatewayDeps:
    """Attach the gateway dependency bundle to ``app.state`` and return it."""
    app = getattr(request_or_app, "app", request_or_app)
    deps = GatewayDeps(
        pool=pool,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        rate_limiter=rate_limiter,
    )
    app.state.deps = deps
    return deps


def get_gateway_deps(request_or_app: Any) -> GatewayDeps:
    """Pull gateway deps off app state, accepting either a Request or FastAPI."""
    app = getattr(request_or_app, "app", request_or_app)
    deps = getattr(app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps
