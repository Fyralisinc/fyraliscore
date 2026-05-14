"""services/integrations/discord/gateway/dispatch.py — dispatch router.

The bridge between the Discord Gateway WSS client (which speaks the opcode
protocol) and the ingestion handler (which writes substrate rows).

`handle_dispatch` is called by the client for every op-0 DISPATCH frame.
It routes on `t` (event name) and, for MESSAGE_CREATE, applies the filter
chain documented in research R7:

  1. `author.bot is True` → drop, metric `filtered_bot_total{source=…}`
  2. `webhook_id is not None` → drop, metric `filtered_bot_total{source="webhook"}`
  3. resolve tenant via existing TenantResolver
  4. on UnknownInstallation → drop, metric `dropped_unknown_installation_total`,
     log without raw guild_id
  5. otherwise → call `ingest("discord:message", payload, …)` which
     persists exactly one observation (idempotent on `external_id`)

GUILD_DELETE and GUILD_CREATE bump a dispatch counter and otherwise do
nothing — per Clarifications, IN-09's outbound-401 chokepoint remains the
canonical kick-detection path.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.ingestion.core import ingest
from services.integrations.discord.gateway import metrics
from services.integrations.discord.oauth import short_guild_hash
from services.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)


log = structlog.get_logger("integrations.discord.gateway.dispatch")


@dataclass
class DispatchDeps:
    """Dependencies injected into every dispatch call. Built once at
    worker startup and reused for the process lifetime."""
    pool: asyncpg.Pool
    tenant_resolver: Any  # services.webhooks.tenant_resolver.TenantResolver
    actor_repo: ActorRepo | None
    alias_repo: EntityAliasRepo | None
    embedder: Any  # OllamaClient | None
    application_id: str | None


async def handle_dispatch(frame: dict[str, Any], deps: DispatchDeps) -> None:
    """Top-level dispatch — called for every op-0 DISPATCH frame.

    Routes on `frame["t"]` (event name). Unhandled events bump
    `dispatch_total{event=...}` and return without action.
    """
    event = frame.get("t") or ""
    payload = frame.get("d") or {}

    if event == "MESSAGE_CREATE":
        await handle_message_create(payload, deps)
        return

    if event in ("GUILD_CREATE", "GUILD_DELETE", "READY", "RESUMED"):
        # Metric increment happened in the client's dispatch loop.
        # Nothing else for the worker to do (Clarifications: GUILD_DELETE
        # is metric-only; IN-09's chokepoint is the kick-detection path).
        return

    # Other events (MESSAGE_UPDATE, MESSAGE_DELETE, TYPING_START, …)
    # are not in v1 scope. The dispatch metric was already incremented
    # in the client; nothing else to do.


async def handle_message_create(message: dict[str, Any], deps: DispatchDeps) -> None:
    """The hot path. See module docstring + contracts/module-gateway-dispatch.md."""
    # 1. Author filter (R7: fastest, before any DB hit).
    author = message.get("author") or {}
    if isinstance(author, dict) and author.get("bot") is True:
        source = (
            "self"
            if author.get("id") and author["id"] == deps.application_id
            else "other_bot"
        )
        metrics.inc("discord_gateway_filtered_bot_total", source=source)
        return
    if message.get("webhook_id") is not None:
        metrics.inc("discord_gateway_filtered_bot_total", source="webhook")
        return

    # 2. DM (no guild_id) — out of scope for v1.
    guild_id = message.get("guild_id")
    if not guild_id or not isinstance(guild_id, str):
        metrics.inc("discord_gateway_dispatch_total", event="MESSAGE_CREATE_DM")
        return

    # 3. Tenant resolution (reuse IN-07 substrate). `resolve()` returns
    # a discriminated outcome — never raises for "unknown" / "missing".
    outcome = await deps.tenant_resolver.resolve(
        "discord",
        payload={"guild_id": guild_id},
        headers={},
    )
    if isinstance(outcome, (UnknownInstallation, PayloadMissing)):
        metrics.inc("discord_gateway_dropped_unknown_installation_total")
        log.info(
            "discord_gateway_dropped_unknown_installation",
            short_guild_hash=short_guild_hash(guild_id),
            # NEVER log raw guild_id — SC-006.
        )
        return
    if not isinstance(outcome, Resolved):
        log.error(
            "discord_gateway_bad_tenant_resolver_result",
            short_guild_hash=short_guild_hash(guild_id),
        )
        return
    tenant_id: UUID = outcome.tenant_id

    # 4. Hand to the unified ingest path. The handler registered for
    # `discord:message` shapes the payload; `ingest()` handles dedup
    # via the existing unique index.
    try:
        result = await ingest(
            "discord:message",
            message,
            pool=deps.pool,
            tenant_id=tenant_id,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "discord_gateway_ingest_failed",
            short_guild_hash=short_guild_hash(guild_id),
            message_id=message.get("id"),
        )
        return

    if result.deduped:
        metrics.inc("discord_gateway_messages_dedup_total")
    else:
        metrics.inc("discord_gateway_messages_total")


__all__ = ["DispatchDeps", "handle_dispatch", "handle_message_create"]
