"""services/integrations/gmail/history_poller.py — history.list fallback worker.

Periodically calls users.history.list per mailbox as a safety net for
missed Pub/Sub push notifications (subscription backlog overflow,
watch expiration gaps, retry exhaustion).

Default interval per mailbox: 10 minutes. The poll path and the push
path call the same drain_mailbox_history() helper, so observations
written from either are indistinguishable downstream — and the dedup
at observations.UNIQUE + gmail_thread_members.PK makes overlap safe.
"""
from __future__ import annotations

import asyncio
import os
import random
import socket
from uuid import UUID

import asyncpg
import structlog

from lib.shared.tenant_context import bind_tenant

from services.integrations.gmail.client import (
    GmailClient,
    GoogleApiError,
    GoogleHttpClient,
    GoogleRateLimited,
)
from services.integrations.gmail.dwd import get_minter
from services.integrations.gmail.fetcher import drain_mailbox_history


log = structlog.get_logger("integrations.gmail.history_poller")


_DEFAULT_TICK_S = 60.0           # how often the loop wakes
_POLL_GAP_S = 10 * 60            # min seconds between polls per mailbox
_LEASE_BATCH = 50
_MAX_FAILURES = 5


def _worker_name() -> str:
    return f"gmail-history-poller@{socket.gethostname()}:{os.getpid()}"


async def _lease_due_mailboxes(
    conn: asyncpg.Connection, *, limit: int,
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"""
        WITH leased AS (
          SELECT id FROM gmail_mailbox_watches
           WHERE state = 'active'
             AND history_id IS NOT NULL
             AND (last_poll_at IS NULL OR last_poll_at < now() - interval '{_POLL_GAP_S} seconds')
           ORDER BY last_poll_at NULLS FIRST
           LIMIT $1
           FOR UPDATE SKIP LOCKED
        )
        UPDATE gmail_mailbox_watches mw
           SET last_poll_at = now()
          FROM leased
         WHERE mw.id = leased.id
        RETURNING mw.id, mw.tenant_id, mw.gmail_installation_id,
                  mw.email_address, mw.consecutive_poll_failures
        """,
        limit,
    )


async def poll_one(pool: asyncpg.Pool, row: asyncpg.Record) -> None:
    tenant_id: UUID = row["tenant_id"]
    gmail_installation_id: UUID = row["gmail_installation_id"]
    email = row["email_address"]
    minter = get_minter()
    async with GoogleHttpClient(minter) as http:
        gmail = GmailClient(http)
        try:
            await drain_mailbox_history(
                pool=pool,
                gmail=gmail,
                tenant_id=tenant_id,
                gmail_installation_id=gmail_installation_id,
                email_address=email,
                read_path="poll",
            )
        except GoogleRateLimited as exc:
            await _bump_failure(pool, tenant_id, row["id"], f"rate_limited: {exc}")
        except GoogleApiError as exc:
            await _bump_failure(pool, tenant_id, row["id"], str(exc)[:300])


async def _bump_failure(
    pool: asyncpg.Pool, tenant_id: UUID, watch_id: UUID, err: str,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    f"""
                    UPDATE gmail_mailbox_watches
                       SET consecutive_poll_failures = consecutive_poll_failures + 1,
                           last_error = $3,
                           state = CASE
                             WHEN consecutive_poll_failures + 1 >= {_MAX_FAILURES} THEN 'errored'
                             ELSE state
                           END
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    watch_id, tenant_id, err,
                )


async def tick(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        rows = await _lease_due_mailboxes(conn, limit=_LEASE_BATCH)
    n = 0
    for row in rows:
        try:
            await poll_one(pool, row)
            n += 1
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "gmail.poller.tick_error",
                email=row["email_address"], error=str(exc)[:200],
            )
    return n


async def run_forever(
    pool: asyncpg.Pool,
    *,
    stop_event: asyncio.Event | None = None,
    tick_interval_s: float = _DEFAULT_TICK_S,
) -> None:
    stop_event = stop_event or asyncio.Event()
    while not stop_event.is_set():
        try:
            await tick(pool)
        except Exception as exc:  # noqa: BLE001
            log.exception("gmail.poller.loop_error", error=str(exc)[:200])
        jitter = random.uniform(0.0, tick_interval_s * 0.1)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=tick_interval_s + jitter)
        except asyncio.TimeoutError:
            pass


__all__ = ["poll_one", "run_forever", "tick"]
