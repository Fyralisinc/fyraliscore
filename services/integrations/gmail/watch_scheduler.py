"""services/integrations/gmail/watch_scheduler.py — watch renewal worker body.

Renews gmail_mailbox_watches rows whose watch_expiration approaches.
Gmail watches expire every 7 days; we renew anything within 24h of
expiry. The body mirrors the post-commit worker pattern: lease via
FOR UPDATE SKIP LOCKED, exponential backoff on errors.

Run via scripts/run_gmail_watch_scheduler.py. SIGTERM-aware via the
`stop_event` parameter.
"""
from __future__ import annotations

import asyncio
import os
import random
import socket
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.tenant_context import bind_tenant

from services.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
    GoogleHttpClient,
    GoogleRateLimited,
)
from services.integrations.gmail.dwd import get_minter


log = structlog.get_logger("integrations.gmail.watch_scheduler")


SCOPE_ALIAS = {
    "gmail.metadata": GMAIL_METADATA_SCOPE,
    "gmail.readonly": GMAIL_READONLY_SCOPE,
}


_DEFAULT_TICK_S = 15 * 60
_LEASE_BATCH = 25
_BASE_BACKOFF_S = 2.0
_MAX_BACKOFF_S = 300.0


def _worker_name() -> str:
    return f"gmail-watch-scheduler@{socket.gethostname()}:{os.getpid()}"


async def _lease_due_watches(
    conn: asyncpg.Connection, *, limit: int, worker: str,
) -> list[asyncpg.Record]:
    """Lease watches whose expiration is within the renewal window OR
    that are pending/errored.

    Cross-tenant query — caller must bind_tenant before any per-row read.
    """
    return await conn.fetch(
        """
        WITH leased AS (
          SELECT mw.id
            FROM gmail_mailbox_watches mw
           WHERE (
              (mw.state = 'active' AND mw.watch_expiration < now() + interval '24 hours')
           OR (mw.state IN ('pending','errored') AND mw.watch_expiration IS NULL)
              )
             AND (mw.last_poll_at IS NULL OR mw.last_poll_at < now() - interval '60 seconds')
           ORDER BY mw.watch_expiration NULLS FIRST
           LIMIT $1
           FOR UPDATE SKIP LOCKED
        )
        UPDATE gmail_mailbox_watches mw
           SET last_poll_at = now()
          FROM leased
         WHERE mw.id = leased.id
        RETURNING mw.id, mw.tenant_id, mw.gmail_installation_id,
                  mw.email_address, mw.state, mw.history_id,
                  mw.consecutive_poll_failures
        """,
        limit,
    )


async def renew_one(
    pool: asyncpg.Pool, row: asyncpg.Record,
) -> None:
    tenant_id: UUID = row["tenant_id"]
    gmail_installation_id: UUID = row["gmail_installation_id"]
    email = row["email_address"]

    # Fetch install scope + topic name (tenant-bound).
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                meta = await tctx.fetchrow(
                    """
                    SELECT gi.scope, t.topic_name
                      FROM gmail_installations gi
                      JOIN gmail_pubsub_topics t
                        ON t.gmail_installation_id = gi.id
                       AND t.teardown_at IS NULL
                     WHERE gi.id = $1
                     LIMIT 1
                    """,
                    gmail_installation_id,
                )
    if meta is None:
        log.warning(
            "gmail.scheduler.no_install_or_topic",
            gmail_installation_id=str(gmail_installation_id),
        )
        return

    scope_alias = meta["scope"]
    scope_long = SCOPE_ALIAS[scope_alias]
    topic_name = meta["topic_name"]

    minter = get_minter()
    async with GoogleHttpClient(minter) as http:
        gmail = GmailClient(http)
        try:
            result = await gmail.watch(
                user_email=email, scope=scope_long, topic_name=topic_name,
            )
        except GoogleRateLimited as exc:
            await _bump_failure(pool, tenant_id, row["id"], str(exc)[:300])
            return
        except GoogleApiError as exc:
            await _bump_failure(pool, tenant_id, row["id"], str(exc)[:300])
            return

    from services.integrations.gmail.watch import _expiration_to_dt
    history_id = str(result.get("historyId", ""))
    expiration = _expiration_to_dt(result.get("expiration"))

    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    """
                    UPDATE gmail_mailbox_watches
                       SET state = 'active',
                           history_id = $3,
                           watch_expiration = $4,
                           consecutive_poll_failures = 0,
                           last_error = NULL
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    row["id"], tenant_id, history_id, expiration,
                )
    log.info(
        "gmail.scheduler.renewed",
        email=email, expiration=expiration.isoformat() if expiration else None,
    )


async def _bump_failure(
    pool: asyncpg.Pool, tenant_id: UUID, watch_id: UUID, err: str,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    """
                    UPDATE gmail_mailbox_watches
                       SET consecutive_poll_failures = consecutive_poll_failures + 1,
                           last_error = $3,
                           state = CASE
                             WHEN consecutive_poll_failures + 1 >= 5 THEN 'errored'
                             ELSE state
                           END
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    watch_id, tenant_id, err,
                )


async def tick(pool: asyncpg.Pool) -> int:
    """One scheduler pass. Returns the number of watches renewed."""
    worker = _worker_name()
    async with pool.acquire() as conn:
        rows = await _lease_due_watches(conn, limit=_LEASE_BATCH, worker=worker)
    n = 0
    for row in rows:
        try:
            await renew_one(pool, row)
            n += 1
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "gmail.scheduler.tick_error",
                email=row["email_address"], error=str(exc)[:200],
            )
    return n


async def run_forever(
    pool: asyncpg.Pool,
    *,
    stop_event: asyncio.Event | None = None,
    tick_interval_s: float = _DEFAULT_TICK_S,
) -> None:
    """Main loop. Returns when stop_event is set."""
    stop_event = stop_event or asyncio.Event()
    while not stop_event.is_set():
        try:
            await tick(pool)
        except Exception as exc:  # noqa: BLE001
            log.exception("gmail.scheduler.loop_error", error=str(exc)[:200])
        # Jittered sleep so multiple scheduler processes don't sync.
        jitter = random.uniform(0.0, tick_interval_s * 0.1)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=tick_interval_s + jitter)
        except asyncio.TimeoutError:
            pass


__all__ = ["renew_one", "run_forever", "tick"]
