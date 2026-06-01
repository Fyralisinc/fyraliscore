"""services.integrations.tests.conftest — shared fixtures for IN-08 tests.

The parent project conftest provides `db_pool` / `fresh_db`; this file
adds environment-variable setup for the OAuth flow tests so that
`build_app()` and `build_secret_store()` don't fire the dev-mode
warning under the project's `filterwarnings = error` policy.
"""
from __future__ import annotations

from typing import AsyncIterator

import asyncpg
import httpx
import pytest
import pytest_asyncio


@pytest.fixture(autouse=True)
def _stable_master_kek(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stable test Fernet key + env-fallback flag so `build_secret_store`
    constructs a deterministic envelope and `assert_prod_safety_invariants`
    is happy. Individual tests override `MASTER_KEK` when they need
    distinct keys.
    """
    monkeypatch.setenv(
        "MASTER_KEK", "KuT6Cixjs4991zhixcpj1QAFbiQj3b9N8meZV2AJJyw=",
    )
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")


@pytest_asyncio.fixture
async def gateway_client(
    fresh_db: asyncpg.Pool,
) -> AsyncIterator[httpx.AsyncClient]:
    """Build the real Gateway app against `fresh_db` and yield an
    httpx AsyncClient pointed at it via ASGITransport. Used by the
    GitHub router tests so signature verification, tenant resolution,
    replay cache, repo filter, lifecycle dispatch, and ingestion are
    all exercised end-to-end.
    """
    from services.actors.repo import ActorRepo
    from services.entity_aliases.repo import EntityAliasRepo
    from services.gateway.main import build_app
    from services.gateway.rate_limit import RateLimiter

    app = build_app(
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=transport, base_url="http://t",
        ) as c:
            yield c
