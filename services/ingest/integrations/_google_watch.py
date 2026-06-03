"""services/ingest/integrations/_google_watch.py — Google push-channel engine.

Calendar (`events.watch`) and Drive (`changes.watch`) both open a native push
channel the same way: generate a channel id + a shared token, POST `*.watch`
with a `web_hook` address, and persist `{channel_id, resource_id, token,
expiration}` on the per-resource row. Google then pings that address with
`X-Goog-Channel-ID` + `X-Goog-Channel-Token`; the ingress verifies the token
(constant-time) and drains the delta via the SAME `drain_live` the poller uses.

This module owns the engine (register / renew scheduler / push-resolve + drain),
parameterised by a small per-source `WatchSpec`. Table + column names in the
SQL are code-controlled constants from the spec (never request data).
"""
from __future__ import annotations

import asyncio
import hmac
import os
import random
import secrets
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg
import structlog

from lib.shared.tenant_context import bind_tenant
from services.ingest.integrations._google_live import drain_live
from services.ingest.integrations.gmail.client import GoogleApiError, GoogleRateLimited


log = structlog.get_logger("integrations.google_watch")


# Channels are requested with this TTL; the scheduler renews any whose
# expiration falls inside the renewal window. Google caps Calendar/Drive
# channels well above a day, so a 7-day request + 24h renewal window leaves
# ample slack.
_CHANNEL_TTL_S = 7 * 24 * 3600
_RENEW_WINDOW_S = 24 * 3600
_DEFAULT_TICK_S = 900.0
_LEASE_BATCH = 25
_MAX_FAILURES = 5


@dataclass(frozen=True)
class WatchSpec:
    source: str                       # "google_calendar" | "google_drive"
    table: str                        # per-resource table
    install_table: str
    install_fk: str                   # FK column -> install id
    cursor_col: str                   # "sync_token" | "start_page_token"
    cursor_next_key: str              # cursor dict key the fetcher advances
    channel: str                      # ingest channel
    id_cols: tuple[str, ...]          # columns identifying the resource
    push_path: str                    # "/webhooks/google_calendar/push"
    # Per-source callables (close over the source's client + fetcher):
    make_client: Callable[[str], Awaitable[tuple[Any, Callable[[], Awaitable[None]]]]]
    do_watch: Callable[..., Awaitable[dict[str, Any]]]
    do_stop: Callable[..., Awaitable[None]]
    fetcher: Callable[..., Awaitable[Any]]
    build_shard: Callable[[asyncpg.Record | dict], dict[str, Any]]


def push_address_for(spec: WatchSpec) -> str | None:
    """The public `web_hook` URL Google pushes to, derived from
    GOOGLE_PUSH_WEBHOOK_BASE. None when unset (the scheduler then can't
    register channels — the poller remains the liveness guarantee)."""
    base = os.environ.get("GOOGLE_PUSH_WEBHOOK_BASE")
    if not base:
        return None
    return f"{base.rstrip('/')}{spec.push_path}"


def _worker(spec: WatchSpec) -> str:
    return f"{spec.source}-watch@{socket.gethostname()}:{os.getpid()}"


def _ms_to_dt(ms: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------
# Registration + renewal scheduler
# ---------------------------------------------------------------------
async def _lease_due_watches(
    conn: asyncpg.Connection, spec: WatchSpec, *, limit: int,
) -> list[asyncpg.Record]:
    """Lease active, cursor-seeded resources whose channel is inactive or
    nearing expiry. Marks them by bumping watch_expiration's NULL ordering via
    a no-op touch is avoided — instead we rely on SKIP LOCKED + the renewal
    setting expiration, so a concurrent scheduler won't re-lease the same row
    within a tick."""
    cols = ", ".join(f"r.{c}" for c in spec.id_cols)
    return await conn.fetch(
        f"""
        SELECT r.id, r.tenant_id, {cols}, r.{spec.cursor_col} AS cursor_token,
               r.watch_channel_id, r.watch_resource_id,
               (SELECT scope FROM {spec.install_table}
                 WHERE id = r.{spec.install_fk}) AS scope
          FROM {spec.table} r
          JOIN {spec.install_table} gi ON gi.id = r.{spec.install_fk}
         WHERE r.state = 'active'
           AND r.{spec.cursor_col} IS NOT NULL
           AND gi.disabled_at IS NULL
           AND (r.watch_state <> 'active'
                OR r.watch_expiration IS NULL
                OR r.watch_expiration < now() + interval '{_RENEW_WINDOW_S} seconds')
         ORDER BY r.watch_expiration NULLS FIRST
         LIMIT $1
         FOR UPDATE OF r SKIP LOCKED
        """,
        limit,
    )


async def register_watch(
    pool: asyncpg.Pool, spec: WatchSpec, row: asyncpg.Record, *, address: str,
) -> None:
    """Open (or refresh) a push channel for one resource and persist its state.

    A fresh channel id + token are minted each time; the previous channel (if
    any) is best-effort stopped so Google doesn't keep pinging a stale id."""
    tenant_id: UUID = row["tenant_id"]
    channel_id = secrets.token_urlsafe(24)
    token = secrets.token_urlsafe(24)
    scope = row["scope"]

    client, close = await spec.make_client(scope)
    try:
        # Best-effort teardown of the prior channel.
        if row["watch_channel_id"] and row["watch_resource_id"]:
            try:
                await spec.do_stop(client, row=row)
            except (GoogleApiError, GoogleRateLimited) as exc:
                log.info(f"{spec.source}.watch.stop_prior_failed", error=str(exc)[:200])

        try:
            channel = await spec.do_watch(
                client, row=row, channel_id=channel_id,
                address=address, token=token, ttl_seconds=_CHANNEL_TTL_S,
            )
        except (GoogleApiError, GoogleRateLimited) as exc:
            await _bump_watch_failure(pool, spec, tenant_id, row["id"], str(exc)[:300])
            return
    finally:
        await close()

    resource_id = channel.get("resourceId") if isinstance(channel, dict) else None
    expiration = _ms_to_dt(channel.get("expiration")) if isinstance(channel, dict) else None

    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    f"""
                    UPDATE {spec.table}
                       SET watch_channel_id = $3, watch_resource_id = $4,
                           watch_token = $5, watch_expiration = $6,
                           watch_state = 'active'
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    row["id"], tenant_id, channel_id, resource_id, token, expiration,
                )
    log.info(
        f"{spec.source}.watch.registered",
        channel_id=channel_id, expiration=str(expiration),
    )


async def _bump_watch_failure(
    pool: asyncpg.Pool, spec: WatchSpec, tenant_id: UUID, row_id: UUID, err: str,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    f"""
                    UPDATE {spec.table}
                       SET watch_state = 'errored', live_last_error = $3
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    row_id, tenant_id, err,
                )


async def watch_tick(pool: asyncpg.Pool, spec: WatchSpec, *, address: str) -> int:
    async with pool.acquire() as conn:
        rows = await _lease_due_watches(conn, spec, limit=_LEASE_BATCH)
    n = 0
    for row in rows:
        try:
            await register_watch(pool, spec, row, address=address)
            n += 1
        except Exception as exc:  # noqa: BLE001
            log.exception(f"{spec.source}.watch.tick_error", error=str(exc)[:200])
    return n


async def run_watch_scheduler(
    pool: asyncpg.Pool,
    spec: WatchSpec,
    *,
    stop_event: asyncio.Event | None = None,
    tick_interval_s: float = _DEFAULT_TICK_S,
) -> None:
    stop_event = stop_event or asyncio.Event()
    address = push_address_for(spec)
    if not address:
        log.warning(
            f"{spec.source}.watch.disabled_no_address",
            reason="GOOGLE_PUSH_WEBHOOK_BASE unset — live poller is the liveness path",
        )
        # Idle until shutdown rather than busy-loop; the poller covers liveness.
        await stop_event.wait()
        return
    log.info(f"{spec.source}.watch_scheduler.starting", worker=_worker(spec), address=address)
    while not stop_event.is_set():
        try:
            await watch_tick(pool, spec, address=address)
        except Exception as exc:  # noqa: BLE001
            log.exception(f"{spec.source}.watch.loop_error", error=str(exc)[:200])
        jitter = random.uniform(0.0, tick_interval_s * 0.1)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=tick_interval_s + jitter)
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------
# Inbound push: resolve + verify + drain
# ---------------------------------------------------------------------
async def resolve_push(
    pool: asyncpg.Pool, spec: WatchSpec, *, channel_id: str, token: str | None,
) -> asyncpg.Record | None:
    """Look up the watched resource by channel id and constant-time-verify the
    token. Returns the row (with tenant_id, scope, cursor, id_cols) or None when
    unknown / token mismatch. Cross-tenant probe (owner bypasses RLS)."""
    if not channel_id:
        return None
    cols = ", ".join(f"r.{c}" for c in spec.id_cols)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT r.id, r.tenant_id, {cols}, r.{spec.cursor_col} AS cursor_token,
                   r.watch_token,
                   (SELECT scope FROM {spec.install_table}
                     WHERE id = r.{spec.install_fk}) AS scope
              FROM {spec.table} r
             WHERE r.watch_channel_id = $1
            """,
            channel_id,
        )
    if row is None:
        return None
    stored = row["watch_token"]
    if not stored or not token or not hmac.compare_digest(str(stored), str(token)):
        return None
    return row


async def drain_push(
    pool: asyncpg.Pool, spec: WatchSpec, row: asyncpg.Record,
) -> int:
    """Drain the delta for a verified push and advance the cursor + last_push_at.
    Returns the count of newly-ingested observations."""
    tenant_id: UUID = row["tenant_id"]
    warm = row["cursor_token"]
    ingested, new_token = await drain_live(
        pool=pool,
        tenant_id=tenant_id,
        scope=row["scope"],
        channel=spec.channel,
        fetcher=spec.fetcher,
        shard_identifier=spec.build_shard(row),
        cursor_next_key=spec.cursor_next_key,
        warm_token=warm,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                await tctx.execute(
                    f"""
                    UPDATE {spec.table}
                       SET {spec.cursor_col} = COALESCE($3, {spec.cursor_col}),
                           last_synced_at = now(), last_push_at = now()
                     WHERE id = $1 AND tenant_id = $2
                    """,
                    row["id"], tenant_id, new_token,
                )
    return ingested


__all__ = [
    "WatchSpec",
    "drain_push",
    "push_address_for",
    "register_watch",
    "resolve_push",
    "run_watch_scheduler",
    "watch_tick",
]
