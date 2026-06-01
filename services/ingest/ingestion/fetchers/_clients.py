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
                from services.ingest.ingestion.workflows.runtime import (
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
    from services.ingest.integrations.github.client import (
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
    from services.ingest.integrations.slack.client import SlackClient

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


async def build_slack_user_client(
    *,
    tenant_id: Any,
    team_id: str,
    user_id: str,
    base_url: str | None = None,
    pool: asyncpg.Pool | None = None,
) -> Any:
    """Per-USER Slack read-client (xoxp user token) for DM backfill.

    Unlike the bot client (keyed off a `provider_installations` row), the DM
    grain is per consenting user (`slack_dm_installations`), so this builder
    takes the identity tuple directly rather than an install Record. The user
    token is resolved from the secret store by label
    `slack_user_token:{team_id}:{user_id}` (or preset in spammer mode).

    SPAMMER MODE: preset `_user_token = spam-slack-user::<team>::<user>` so the
    spammer routes `conversations.list(types=im,mpim)` to that user's DM
    fixtures — no real xoxp grant or secret material required.
    """
    from services.ingest.integrations.slack.client import SlackUserClient

    spammer = _spammer_mode()
    client = SlackUserClient(
        pool=await _effective_pool(pool, spammer=spammer),
        secret_store=None if spammer else await _get_secret_store(),
        tenant_id=tenant_id,
        # The user grain has no provider_installations row id; carry the
        # tenant uuid as the (unused-by-_call) installation_row_id slot.
        installation_row_id=tenant_id,
        team_id=team_id,
        user_id=user_id,
        base_url=base_url,
        http_client=await _get_http(),
    )
    if spammer:
        client._user_token = f"spam-slack-user::{team_id}::{user_id}"
    return client


async def build_discord_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    from services.ingest.integrations.discord.client import DiscordClient

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


async def build_notion_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    """Notion read-client. Bot token is long-lived: resolved once from the
    secret store via `install['secret_ref']` (or preset in spammer mode).
    `installation_id` carries the workspace id. The base URL routes through
    the endpoint resolver so backfill can point at the local spammer."""
    from lib.integrations.endpoints import endpoint
    from services.ingest.integrations.notion.client import NotionClient

    spammer = _spammer_mode()
    workspace_id = str(install["installation_id"])
    secret_ref = install["secret_ref"] if "secret_ref" in install else None
    client = NotionClient(
        pool=await _effective_pool(pool, spammer=spammer),
        secret_store=None if spammer else await _get_secret_store(),
        tenant_id=install["tenant_id"],
        secret_ref=secret_ref,
        workspace_id=workspace_id,
        bot_token=(f"spam-notion::{workspace_id}" if spammer else None),
        http_client=await _get_http(),
        api_base_url=(endpoint("notion_api") if not spammer else None),
    )
    return client


async def build_jira_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    """Jira Cloud read-client. API token is long-lived: resolved once from the
    secret store via `install['secret_ref']` (or preset in spammer mode). The
    site base URL is per-install (`install['base_url']`); in spammer mode it is
    overridden via the endpoint resolver so backfill points at the local
    spammer's `/jira` sub-path. `account_email` is the Basic-auth username."""
    from lib.integrations.endpoints import endpoint
    from services.ingest.integrations.jira.client import JiraClient

    spammer = _spammer_mode()
    base_url = str(install["base_url"])
    account_email = str(install["account_email"])
    secret_ref = install["secret_ref"] if "secret_ref" in install else None
    client = JiraClient(
        base_url=base_url,
        account_email=account_email,
        pool=await _effective_pool(pool, spammer=spammer),
        secret_store=None if spammer else await _get_secret_store(),
        tenant_id=install["tenant_id"],
        secret_ref=secret_ref,
        api_token=("spam-jira" if spammer else None),
        http_client=await _get_http(),
        # Spammer routes ALL sites to the one mock host under /jira; prod uses
        # the per-install base_url (api_base_url=None → base_url is used).
        api_base_url=(endpoint("jira_api") if spammer else None),
    )
    return client


async def build_mercury_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    """Mercury read-client. API token is long-lived: resolved once from the
    secret store via `install['secret_ref']` (or preset in spammer mode). The
    base URL routes through the endpoint resolver so backfill can point at the
    local spammer's `/mercury` sub-path."""
    from lib.integrations.endpoints import endpoint
    from services.ingest.integrations.mercury.client import MercuryClient

    spammer = _spammer_mode()
    base_url = str(install["base_url"]) if "base_url" in install else ""
    secret_ref = install["secret_ref"] if "secret_ref" in install else None
    client = MercuryClient(
        base_url=base_url,
        pool=await _effective_pool(pool, spammer=spammer),
        secret_store=None if spammer else await _get_secret_store(),
        tenant_id=install["tenant_id"],
        secret_ref=secret_ref,
        api_token=("spam-mercury" if spammer else None),
        http_client=await _get_http(),
        # Spammer routes to the one mock host under /mercury; prod uses the
        # canonical Mercury API host (api_base_url=None → base_url is used).
        api_base_url=(endpoint("mercury_api") if spammer else None),
    )
    return client


async def build_quickbooks_client(
    install: asyncpg.Record, *, pool: asyncpg.Pool | None = None,
) -> Any:
    """QuickBooks read-client. OAuth access token is resolved once from the
    secret store via `install['secret_ref']` (or preset in spammer mode). The
    realm-scoped base URL is per-install (`install['base_url']`); in spammer
    mode it is overridden via the endpoint resolver so backfill points at the
    local spammer's `/quickbooks` sub-path. `realm_id` scopes every query."""
    from lib.integrations.endpoints import endpoint
    from services.ingest.integrations.quickbooks.client import QuickBooksClient

    spammer = _spammer_mode()
    base_url = str(install["base_url"]) if "base_url" in install else ""
    realm_id = str(install["realm_id"]) if "realm_id" in install else ""
    secret_ref = install["secret_ref"] if "secret_ref" in install else None
    client = QuickBooksClient(
        base_url=base_url,
        realm_id=realm_id,
        pool=await _effective_pool(pool, spammer=spammer),
        secret_store=None if spammer else await _get_secret_store(),
        tenant_id=install["tenant_id"],
        secret_ref=secret_ref,
        access_token=("spam-quickbooks" if spammer else None),
        http_client=await _get_http(),
        api_base_url=(endpoint("quickbooks_api") if spammer else None),
    )
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


async def open_slack_user_client(
    *, tenant_id: Any, team_id: str, user_id: str, base_url: str | None = None,
) -> Opener:
    """Per-user DM read-client opener (fetcher/reconciler seam). Identity comes
    from the `slack_dm_window` shard_identifier (team_id + consenting user_id)
    rather than an install Record. The X3 mock harness monkeypatches the
    per-module `_open_slack_user_client` seam."""
    return (
        await build_slack_user_client(
            tenant_id=tenant_id, team_id=team_id,
            user_id=user_id, base_url=base_url,
        ),
        _noop,
    )


async def open_discord_client(install: asyncpg.Record) -> Opener:
    return await build_discord_client(install), _noop


async def open_notion_client(install: asyncpg.Record) -> Opener:
    return await build_notion_client(install), _noop


async def open_jira_client(install: asyncpg.Record) -> Opener:
    return await build_jira_client(install), _noop


async def open_mercury_client(install: asyncpg.Record) -> Opener:
    return await build_mercury_client(install), _noop


async def open_quickbooks_client(install: asyncpg.Record) -> Opener:
    return await build_quickbooks_client(install), _noop


__all__ = [
    "build_github_client", "build_slack_client", "build_slack_user_client",
    "build_discord_client",
    "build_notion_client", "build_jira_client",
    "build_mercury_client", "build_quickbooks_client",
    "open_github_client", "open_slack_client", "open_slack_user_client",
    "open_discord_client",
    "open_notion_client", "open_jira_client",
    "open_mercury_client", "open_quickbooks_client",
]
