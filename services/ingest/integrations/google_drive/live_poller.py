"""services/ingest/integrations/google_drive/live_poller.py — live delta poller.

The near-real-time live-ingestion worker for Google Drive — the analog of
`gmail_history_poller`. On a short cadence it leases active drive targets
(My-Drive + Shared-Drive) whose incremental `start_page_token` is already
seeded and drains the changes delta via the shared `drain_live` helper (same
`changes.list` fetcher + `ingest()` path as backfill, so observations dedup at
`observations.UNIQUE`).

The native push channel (changes.watch — see `watch_scheduler`) is the
low-latency path; this poller is the safety net and guarantees liveness even
with no push at all. Both call `drain_live`.

Leasing uses the `last_live_poll_at` claim slot (migration 0082) with
`FOR UPDATE SKIP LOCKED`; the cross-tenant scan relies on the worker connecting
as the table owner (RLS is `ENABLE`, not `FORCE`); per-tenant writes bind the
tenant.
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
from services.ingest.integrations._google_live import drain_live
from services.ingest.integrations.gmail.client import GoogleApiError, GoogleRateLimited


log = structlog.get_logger("integrations.google_drive.live_poller")


_DEFAULT_TICK_S = 60.0          # how often the loop wakes
_POLL_GAP_S = 120              # min seconds between live polls per target
_LEASE_BATCH = 50
_MAX_FAILURES = 5
_MAX_CONCURRENCY = int(
    os.environ.get("GOOGLE_DRIVE_LIVE_POLLER_MAX_CONCURRENCY", "1")
)

_CHANNEL = "google_drive:file"


def _worker_name() -> str:
    return f"gdrive-live-poller@{socket.gethostname()}:{os.getpid()}"


async def _lease_due_targets(
    conn: asyncpg.Connection, *, limit: int,
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f"""
        WITH leased AS (
          SELECT dt.id
            FROM google_drive_targets dt
            JOIN google_drive_installations gi
              ON gi.id = dt.google_drive_installation_id
             AND gi.tenant_id = dt.tenant_id
           WHERE dt.state = 'active'
             AND dt.start_page_token IS NOT NULL
             AND gi.disabled_at IS NULL
             AND (dt.last_live_poll_at IS NULL
                  OR dt.last_live_poll_at < now() - interval '{_POLL_GAP_S} seconds')
           ORDER BY dt.last_live_poll_at NULLS FIRST
           LIMIT $1
           FOR UPDATE OF dt SKIP LOCKED
        )
        UPDATE google_drive_targets dt
           SET last_live_poll_at = now()
          FROM leased
         WHERE dt.id = leased.id
        RETURNING dt.id, dt.tenant_id, dt.drive_kind, dt.drive_id, dt.owner_email,
                  dt.start_page_token, dt.consecutive_live_failures,
                  dt.google_drive_installation_id AS installation_id,
                  (SELECT scope FROM google_drive_installations
                    WHERE id = dt.google_drive_installation_id
                      AND tenant_id = dt.tenant_id) AS scope
        """,
        limit,
    )


async def poll_one(pool: asyncpg.Pool, row: asyncpg.Record) -> None:
    tenant_id: UUID = row["tenant_id"]
    try:
        ingested, new_token = await drain_live(
            pool=pool,
            tenant_id=tenant_id,
            installation_id=row["installation_id"],
            scope=row["scope"],
            channel=_CHANNEL,
            fetcher=_fetcher(),
            shard_identifier={
                "drive_kind": row["drive_kind"],
                "drive_id": row["drive_id"],
                "owner_email": row["owner_email"],
                "installation_id": str(row["installation_id"]),
                "start_page_token": row["start_page_token"],
            },
            cursor_next_key="next_start_page_token",
            warm_token=row["start_page_token"],
        )
    except (GoogleRateLimited, GoogleApiError) as exc:
        await _bump_failure(pool, tenant_id, row["id"], str(exc)[:300])
        return

    await _persist_success(pool, tenant_id, row["id"], new_token)
    if ingested:
        log.info(
            "gdrive.live.drained",
            drive_id=row["drive_id"], drive_kind=row["drive_kind"], ingested=ingested,
        )


def _fetcher():  # noqa: ANN202 — late import keeps the module import-light
    from services.ingest.ingestion.fetchers.google_drive import fetch_page_google_drive
    return fetch_page_google_drive


async def _persist_success(
    pool: asyncpg.Pool, tenant_id: UUID, target_row_id: UUID, start_page_token: str | None,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    """
                    UPDATE google_drive_targets
                       SET start_page_token = COALESCE($3, start_page_token),
                           last_synced_at = now(),
                           consecutive_live_failures = 0,
                           live_last_error = NULL
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    target_row_id, tenant_id, start_page_token,
                )


async def _bump_failure(
    pool: asyncpg.Pool, tenant_id: UUID, target_row_id: UUID, err: str,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    f"""
                    UPDATE google_drive_targets
                       SET consecutive_live_failures = consecutive_live_failures + 1,
                           live_last_error = $3,
                           state = CASE
                             WHEN consecutive_live_failures + 1 >= {_MAX_FAILURES} THEN 'errored'
                             ELSE state
                           END
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    target_row_id, tenant_id, err,
                )


async def tick(
    pool: asyncpg.Pool, *, max_concurrency: int | None = None,
) -> int:
    async with pool.acquire() as conn:
        rows = await _lease_due_targets(conn, limit=_LEASE_BATCH)
    if not rows:
        return 0

    sem = asyncio.Semaphore(max(1, max_concurrency or _MAX_CONCURRENCY))

    async def _run_one(row: asyncpg.Record) -> int:
        async with sem:
            try:
                await poll_one(pool, row)
                return 1
            except Exception as exc:  # noqa: BLE001
                log.exception(
                    "gdrive.live.tick_error",
                    drive_id=row["drive_id"], error=str(exc)[:200],
                )
                return 0

    results = await asyncio.gather(*(_run_one(row) for row in rows))
    return sum(results)


async def run_forever(
    pool: asyncpg.Pool,
    *,
    stop_event: asyncio.Event | None = None,
    tick_interval_s: float = _DEFAULT_TICK_S,
) -> None:
    stop_event = stop_event or asyncio.Event()
    log.info("gdrive.live_poller.starting", worker=_worker_name())
    while not stop_event.is_set():
        try:
            await tick(pool)
        except Exception as exc:  # noqa: BLE001
            log.exception("gdrive.live.loop_error", error=str(exc)[:200])
        jitter = random.uniform(0.0, tick_interval_s * 0.1)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=tick_interval_s + jitter)
        except asyncio.TimeoutError:
            pass


__all__ = ["poll_one", "run_forever", "tick"]
