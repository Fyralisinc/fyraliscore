"""Production / spammer backfill read-client builders.

The M6.4–M6.6 fetchers + reconcilers + planners (github / slack / discord)
build their source client here. Historically the fetcher/reconciler openers
`raise RuntimeError` and were satisfied only by the X3 mock monkeypatch; this
module builds the REAL source clients, resolving each source's base URL
through `lib.integrations.endpoints` — so pointing backfill at the local
spammer (or at production) is pure config.

Identity is read from the install row: for github / slack / discord the
`provider_installations.installation_id` column carries the source-native
identity (the X3 harness writes `x3-{slug}-{source}`).

SPAMMER MODE (env `SYNTHETIC_SOURCE_API_BASE` set): the clients skip real
auth and instead carry a spammer-recognized identity token so the spammer
can route the request to the right tenant's fixtures — no GitHub App JWT,
no Slack bot-token secret, no Discord bot token required:
  - github : preseed the installation-token cache with `spam-gh::<inst>`
  - slack  : preset `_bot_token = spam-slack::<team>`
  - discord: preset `_bot_token = spam-bot::<guild>`
The path-keyed endpoints (repo events, history, messages, channels) key on
globally-unique ids, so only the token-scoped endpoints need this.

A process-local asyncpg pool + secret store are created lazily and shared
across shards.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import asyncpg
import httpx


_POOL: asyncpg.Pool | None = None
_POOL_LOCK = asyncio.Lock()
_SECRET_STORE: Any = None
_HTTP: httpx.AsyncClient | None = None
_HTTP_LOCK = asyncio.Lock()


def _spammer_mode() -> bool:
    return bool(os.environ.get("SYNTHETIC_SOURCE_API_BASE"))


async def _get_http() -> httpx.AsyncClient:
    """One process-shared httpx client with keep-alive, reused across all
    shard fetches. Building a fresh client per `_open_*_client` opens new
    TCP connections every fetch — under fan-out backfill that floods the
    single-process spammer with connection churn (it wedges). Keep-alive
    reuse keeps the live-connection count to ~the fetch concurrency."""
    global _HTTP
    if _HTTP is None:
        async with _HTTP_LOCK:
            if _HTTP is None:
                _HTTP = httpx.AsyncClient(
                    timeout=30.0,
                    limits=httpx.Limits(
                        max_connections=64, max_keepalive_connections=32,
                    ),
                )
    return _HTTP


async def _get_pool() -> asyncpg.Pool:
    # Locked lazy-init: without the lock, concurrent first-callers each
    # build a pool (the `global` assignment isn't atomic across awaits),
    # exhausting Postgres connections under fan-out backfill.
    global _POOL
    if _POOL is None:
        async with _POOL_LOCK:
            if _POOL is None:
                from services.ingestion.workflows.runtime import (
                    make_workflow_pool,
                )
                _POOL = await make_workflow_pool(os.environ["DATABASE_URL"])
    return _POOL


async def _effective_pool(
    provided: asyncpg.Pool | None, *, spammer: bool,
) -> asyncpg.Pool | None:
    """Pool for the client to carry. Reuse the caller's pool when given;
    in spammer mode the clients never touch the pool (tokens are preset,
    no secret-store / chokepoint), so don't open one. Only the production
    fetcher/reconciler openers (no pool passed, not spammer) lazily share
    the process-local pool."""
    if provided is not None:
        return provided
    if spammer:
        return None
    return await _get_pool()


async def _get_secret_store() -> Any:
    global _SECRET_STORE
    if _SECRET_STORE is None:
        from lib.shared.secrets import build_secret_store
        _SECRET_STORE = build_secret_store(await _get_pool())
    return _SECRET_STORE


# ---------------------------------------------------------------------
# Client builders (used by both the fetcher/reconciler openers and the
# source_onboarding planner factory).
# ---------------------------------------------------------------------
async def build_github_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    from services.integrations.github.client import (
        CachedInstallationToken,
        GithubClient,
    )

    spammer = _spammer_mode()
    inst = str(install["installation_id"])
    client = GithubClient(
        pool=await _effective_pool(pool, spammer=spammer),
        backfill_installation_id=inst,
        http_client=await _get_http(),
    )
    if spammer:
        # Skip the App-JWT mint: hand the client a ready installation token
        # the spammer recognizes (`spam-gh::<inst>` → repos for that install).
        client._installation_tokens[inst] = CachedInstallationToken(
            token=f"spam-gh::{inst}",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
    return client


async def build_slack_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    from services.integrations.slack.client import SlackClient

    spammer = _spammer_mode()
    team_id = str(install["installation_id"])
    client = SlackClient(
        pool=await _effective_pool(pool, spammer=spammer),
        secret_store=None if spammer else await _get_secret_store(),
        tenant_id=install["tenant_id"],
        installation_row_id=install["id"],
        team_id=team_id,
        http_client=await _get_http(),
    )
    if spammer:
        client._bot_token = f"spam-slack::{team_id}"
    return client


async def build_discord_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    from services.integrations.discord.client import DiscordClient

    spammer = _spammer_mode()
    guild_id = str(install["installation_id"])
    client = DiscordClient(
        pool=await _effective_pool(pool, spammer=spammer),
        secret_store=None if spammer else await _get_secret_store(),
        tenant_id=install["tenant_id"],
        installation_row_id=install["id"],
        guild_id=guild_id,
        http_client=await _get_http(),
    )
    if spammer:
        client._bot_token = f"spam-bot::{guild_id}"
    return client


# ---------------------------------------------------------------------
# Fetcher / reconciler openers — return (client, close).
# ---------------------------------------------------------------------
Opener = tuple[Any, Callable[[], Awaitable[None]]]


async def _noop() -> None:
    # The clients share the process-wide httpx client (_get_http), which
    # must NOT be closed per-fetch — closing it would tear down the
    # keep-alive pool every shard. It lives for the process lifetime.
    return None


async def open_github_client(install: asyncpg.Record) -> Opener:
    return await build_github_client(install), _noop


async def open_slack_client(install: asyncpg.Record) -> Opener:
    return await build_slack_client(install), _noop


async def open_discord_client(install: asyncpg.Record) -> Opener:
    return await build_discord_client(install), _noop


__all__ = [
    "build_github_client", "build_slack_client", "build_discord_client",
    "open_github_client", "open_slack_client", "open_discord_client",
]
