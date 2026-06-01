"""IN-12 US3 + US4: dispatch-time filters.

  US3 — author.bot filter: bot/webhook messages NEVER produce
    observations. This is the structural guard against an IN-13
    outbound reply re-entering ingest (infinite loop).

  US4 — unknown-guild silent drop: MESSAGE_CREATE from a guild with
    no enabled `provider_installations` row drops silently with a
    metric increment and no raw guild_id in logs.

All tests call `handle_message_create` directly. No WSS layer.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import pytest

from services.integrations.discord.gateway import metrics as gateway_metrics
from services.integrations.discord.gateway.dispatch import (
    DispatchDeps, handle_message_create,
)
from services.integrations.discord.gateway.tests.conftest import (
    _APPLICATION_ID, make_message_create,
)


pytestmark = pytest.mark.integration


async def _count_obs(pool: asyncpg.Pool, tenant_id: UUID) -> int:
    return await pool.fetchval(
        "SELECT count(*) FROM observations "
        "WHERE tenant_id=$1 AND source_channel='discord:message'",
        tenant_id,
    )


# ---------------------------------------------------------------------
# US3 — author.bot filter
# ---------------------------------------------------------------------


async def test_author_bot_self_drops_silently(
    fresh_db: asyncpg.Pool, seeded_tenant: UUID, dispatch_deps: DispatchDeps,
) -> None:
    """US3 acceptance scenario 1: author.bot=true AND author.id=APP_ID
    → zero obs, `filtered_bot_total{source="self"}` increments."""
    payload = make_message_create(
        message_id="msg_us3_self_001",
        author_id=_APPLICATION_ID,
        author_bot=True,
        content="this is the bot's own outbound reply",
    )
    await handle_message_create(payload, dispatch_deps)

    assert await _count_obs(fresh_db, seeded_tenant) == 0
    assert gateway_metrics.get(
        "discord_gateway_filtered_bot_total", source="self",
    ) == 1.0
    assert gateway_metrics.get("discord_gateway_messages_total") == 0.0


async def test_author_bot_other_drops_silently(
    fresh_db: asyncpg.Pool, seeded_tenant: UUID, dispatch_deps: DispatchDeps,
) -> None:
    """US3 acceptance scenario 2: another bot in the same channel
    (GitHub bot relaying PR notifications, etc) is filtered."""
    payload = make_message_create(
        message_id="msg_us3_other_002",
        author_id="999999999999999999",  # different from APP_ID
        author_bot=True,
        content="🔔 PR #42 ready for review",
    )
    await handle_message_create(payload, dispatch_deps)

    assert await _count_obs(fresh_db, seeded_tenant) == 0
    assert gateway_metrics.get(
        "discord_gateway_filtered_bot_total", source="other_bot",
    ) == 1.0


async def test_webhook_id_drops_silently(
    fresh_db: asyncpg.Pool, seeded_tenant: UUID, dispatch_deps: DispatchDeps,
) -> None:
    """US3 acceptance scenario 3: Discord webhook-sourced messages
    (e.g., Zapier integrations, channel webhooks) are filtered even
    when `author.bot=false`."""
    payload = make_message_create(
        message_id="msg_us3_webhook_003",
        author_bot=False,
        webhook_id="webhook_123",
        content="from a webhook integration",
    )
    await handle_message_create(payload, dispatch_deps)

    assert await _count_obs(fresh_db, seeded_tenant) == 0
    assert gateway_metrics.get(
        "discord_gateway_filtered_bot_total", source="webhook",
    ) == 1.0


async def test_filter_runs_before_tenant_resolution(
    fresh_db: asyncpg.Pool, dispatch_deps: DispatchDeps,
) -> None:
    """Research R7: the bot filter precedes tenant resolution. A bot
    message from an UNKNOWN guild MUST increment `filtered_bot_total`,
    NOT `dropped_unknown_installation_total` — the filter wins."""
    # NOTE: no seeded_tenant fixture — no install row exists.
    payload = make_message_create(
        message_id="msg_us3_precedence_004",
        author_bot=True,
        guild_id="unknown_guild_xxx",
    )
    await handle_message_create(payload, dispatch_deps)

    assert gateway_metrics.get(
        "discord_gateway_filtered_bot_total", source="other_bot",
    ) == 1.0
    # The unknown-installation metric MUST NOT have been touched.
    assert gateway_metrics.get(
        "discord_gateway_dropped_unknown_installation_total",
    ) == 0.0


# ---------------------------------------------------------------------
# US4 — unknown-guild silent drop
# ---------------------------------------------------------------------


async def test_unknown_guild_drops_silently(
    fresh_db: asyncpg.Pool, dispatch_deps: DispatchDeps,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """US4 acceptance scenario 1: MESSAGE_CREATE for a guild with no
    install row → drop, metric, NO raw guild_id in logs (SC-006)."""
    secret_guild = "secret_guild_id_70999999"
    payload = make_message_create(
        message_id="msg_us4_unknown_001",
        guild_id=secret_guild,
    )
    await handle_message_create(payload, dispatch_deps)

    assert gateway_metrics.get(
        "discord_gateway_dropped_unknown_installation_total",
    ) == 1.0
    # SC-006: the raw guild_id MUST NOT appear in any captured record.
    leaked = [r for r in caplog.records if secret_guild in r.getMessage()]
    assert leaked == [], (
        f"guild_id leaked into logs: {[r.getMessage() for r in leaked]}"
    )


async def test_disabled_install_treated_as_unknown(
    fresh_db: asyncpg.Pool, dispatch_deps: DispatchDeps,
) -> None:
    """US4 acceptance scenario 2: an `enabled=FALSE` install row is
    treated equivalently to a missing row — silent drop, metric."""
    from lib.shared.ids import uuid7
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"discord-us4-disabled-{tid.hex[:8]}",
    )
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, enabled) "
        "VALUES ($1, $2, 'discord', $3, FALSE)",
        uuid7(), tid, "disabled_guild_001",
    )

    payload = make_message_create(
        message_id="msg_us4_disabled_002",
        guild_id="disabled_guild_001",
    )
    await handle_message_create(payload, dispatch_deps)

    assert gateway_metrics.get(
        "discord_gateway_dropped_unknown_installation_total",
    ) == 1.0
    count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations "
        "WHERE tenant_id=$1 AND source_channel='discord:message'",
        tid,
    )
    assert count == 0


async def test_dm_message_drops_silently(
    fresh_db: asyncpg.Pool, dispatch_deps: DispatchDeps,
) -> None:
    """US4 acceptance scenario 4: a MESSAGE_CREATE with no guild_id
    (a DM) drops silently with a distinct metric — not the
    unknown-installation path."""
    payload = make_message_create(
        message_id="msg_us4_dm_003",
        guild_id="",  # falsy
    )
    # The conftest helper always sets guild_id; pop it for the DM case.
    payload.pop("guild_id", None)

    await handle_message_create(payload, dispatch_deps)

    assert gateway_metrics.get(
        "discord_gateway_dispatch_total", event="MESSAGE_CREATE_DM",
    ) == 1.0
    assert gateway_metrics.get(
        "discord_gateway_dropped_unknown_installation_total",
    ) == 0.0
