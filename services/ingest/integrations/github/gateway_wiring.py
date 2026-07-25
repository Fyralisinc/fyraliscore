"""Gateway wiring for GitHub-specific integration state."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import asyncpg
from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger
from services.ingest.integrations.github.client import GithubClient
from services.ingest.integrations.github.replay_cache import make_replay_cache
from services.ingest.integrations.provider_transport_runtime import (
    close_provider_transport_runtime,
    get_provider_transport_runtime,
)


log = get_logger("gateway")


@dataclass(frozen=True, slots=True)
class GithubGatewayWiring:
    """Result of wiring GitHub provider state onto the gateway app."""

    wired: bool
    owns_client: bool

    def __bool__(self) -> bool:
        return self.wired


def wire_github_gateway_state(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    tenant_resolver: Any | None,
) -> GithubGatewayWiring:
    """Attach GitHub webhook/OAuth helpers to ``app.state``."""
    owns_client = False
    if getattr(app_.state, "github_client", None) is None:
        provider_runtime = getattr(
            app_.state,
            "provider_transport_runtime",
            None,
        )
        if provider_runtime is None:
            provider_runtime = get_provider_transport_runtime()
            app_.state.provider_transport_runtime = provider_runtime
            app_.state.gateway_owns_provider_transport_runtime = (
                provider_runtime is not None
            )
        app_.state.github_client = GithubClient(
            pool=pool,
            tenant_resolver=tenant_resolver,
            provider_transport=(
                provider_runtime.transport
                if provider_runtime is not None
                else None
            ),
            quota_resolver=(
                provider_runtime.quota_resolver
                if provider_runtime is not None
                else None
            ),
            allow_unlimited_local=provider_runtime is None,
        )
        app_.state.gateway_owns_github_client = True
        owns_client = True
    if getattr(app_.state, "github_replay_cache", None) is None:
        app_.state.github_replay_cache = make_replay_cache()
    return GithubGatewayWiring(wired=True, owns_client=owns_client)


async def close_github_gateway_state(
    app_: FastAPI,
    *,
    client: object | None = None,
) -> None:
    """Close GitHub provider clients owned by gateway startup."""
    target = (
        client if client is not None else getattr(app_.state, "github_client", None)
    )
    target_is_current = getattr(app_.state, "github_client", None) is target
    if target is not None:
        close = getattr(target, "aclose", None)
        if close is not None:
            try:
                await close()
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "github_gateway_client_close_failed",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
    if target_is_current:
        app_.state.github_client = None
    app_.state.github_replay_cache = None
    app_.state.gateway_owns_github_client = False
    if target_is_current and bool(
        getattr(
            app_.state,
            "gateway_owns_provider_transport_runtime",
            False,
        )
    ):
        runtime = getattr(
            app_.state,
            "provider_transport_runtime",
            None,
        )
        try:
            await close_provider_transport_runtime(runtime)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "provider_transport_runtime_close_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
        app_.state.provider_transport_runtime = None
        app_.state.gateway_owns_provider_transport_runtime = False


__all__ = [
    "GithubGatewayWiring",
    "wire_github_gateway_state",
    "close_github_gateway_state",
]
