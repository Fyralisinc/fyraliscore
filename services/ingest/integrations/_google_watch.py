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
from collections import deque
import hmac
import os
import random
import secrets
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg
import structlog

from lib.shared.tenant_context import bind_tenant
from services.ingest.integrations._google_live import drain_live
from services.ingest.integrations.bounded_renewal import (
    RenewalAttempt,
    RenewalInvocation,
    RenewalInvocationError,
    RenewalManualRepairRequired,
    RenewalOutcome,
    RenewalReauthorizationRequired,
    renewal_next_attempt_at,
    run_bounded_renewal,
)
from services.ingest.integrations.gmail.client import GoogleApiError, GoogleRateLimited
from services.ingest.integrations.gmail.dwd import DwdError
from services.ingest.ingestion.renewal_jobs import (
    RenewalLease,
    mark_renewal_provider_call_started,
)
from services.ingest.source_contract.catalog import source_definition


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
    make_client: Callable[..., Awaitable[tuple[Any, Callable[[], Awaitable[None]]]]]
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


async def _best_effort_stop_channel(
    spec: WatchSpec,
    client: Any,
    *,
    row: asyncpg.Record | dict[str, Any],
    phase: str,
) -> None:
    """Attempt safe channel cleanup without re-entering an unsafe renewal.

    ``events.watch`` and ``changes.watch`` are declared unsafe. Once either
    create call has succeeded, a later stop failure must never escape as a
    ``RetryLater`` to the outer renewal envelope: that would schedule another
    create and duplicate the provider-side channel.  Cleanup remains useful,
    but it is strictly best effort after the create-side outcome is known.
    """

    try:
        await spec.do_stop(client, row=row)
    except Exception as exc:  # noqa: BLE001 - cleanup cannot alter create outcome
        log.info(
            f"{spec.source}.watch.stop_best_effort_failed",
            phase=phase,
            error_type=type(exc).__name__,
        )


# ---------------------------------------------------------------------
# Registration + renewal scheduler
# ---------------------------------------------------------------------
async def _lease_due_watches(
    pool: asyncpg.Pool,
    spec: WatchSpec,
    *,
    limit: int,
) -> list[asyncpg.Record]:
    """Fairly select only resources whose durable renewal state is claimable.

    The source table determines whether a channel needs renewal; the durable
    ``source_renewal_jobs`` row determines whether Fyralis is allowed to
    attempt it now. A future retry/terminal repair row is intentionally not
    returned, so it cannot repeatedly occupy the earliest source-table batch
    while another due installation starves behind it. The actual lease is
    still acquired by ``renew_exact_resource`` immediately before provider
    I/O.
    """

    if limit <= 0:
        raise ValueError("limit must be positive")
    cols = ", ".join(f"r.{c}" for c in spec.id_cols)
    async with pool.acquire() as conn:
        tenant_rows = await conn.fetch(
            f"""
            SELECT DISTINCT r.tenant_id
              FROM {spec.table} r
              JOIN {spec.install_table} gi
                ON gi.id = r.{spec.install_fk}
               AND gi.tenant_id = r.tenant_id
             WHERE r.state = 'active'
               AND r.{spec.cursor_col} IS NOT NULL
               AND gi.disabled_at IS NULL
               AND (r.watch_state <> 'active'
                    OR r.watch_expiration IS NULL
                    OR r.watch_expiration < now() + interval '{_RENEW_WINDOW_S} seconds')
            """,
        )

    selected_by_tenant: dict[UUID, deque[asyncpg.Record]] = {}
    for tenant_row in sorted(tenant_rows, key=lambda row: str(row["tenant_id"])):
        tenant_id: UUID = tenant_row["tenant_id"]
        async with pool.acquire() as conn:
            async with conn.transaction():
                async with bind_tenant(conn, tenant_id) as tctx:
                    rows = await tctx.fetch(
                        f"""
                        SELECT r.id, r.tenant_id, {cols},
                               r.{spec.cursor_col} AS cursor_token,
                               r.watch_channel_id, r.watch_resource_id,
                               r.{spec.install_fk} AS installation_id, gi.scope,
                               j.last_claimed_at AS renewal_last_claimed_at
                          FROM {spec.table} r
                          JOIN {spec.install_table} gi
                            ON gi.id = r.{spec.install_fk}
                           AND gi.tenant_id = r.tenant_id
                          LEFT JOIN source_renewal_jobs j
                            ON j.source_id = $1
                           AND j.tenant_id = r.tenant_id
                           AND j.installation_id = r.{spec.install_fk}
                           AND j.target_key = r.id::text
                         WHERE r.tenant_id = $2
                           AND r.state = 'active'
                           AND r.{spec.cursor_col} IS NOT NULL
                           AND gi.disabled_at IS NULL
                           AND (r.watch_state <> 'active'
                                OR r.watch_expiration IS NULL
                                OR r.watch_expiration < now() + interval '{_RENEW_WINDOW_S} seconds')
                           AND (
                               j.source_id IS NULL
                               OR (
                                   j.state IN ('pending', 'retry_scheduled')
                                   AND j.next_attempt_at <= now()
                               )
                               OR (
                                   j.state = 'leased'
                                   AND j.lease_expires_at <= now()
                               )
                           )
                         ORDER BY j.last_claimed_at NULLS FIRST,
                                  r.watch_expiration NULLS FIRST,
                                  r.id
                         LIMIT $3
                        """,
                        spec.source,
                        tenant_id,
                        limit,
                    )
        if rows:
            selected_by_tenant[tenant_id] = deque(rows)

    tenant_order = tuple(
        sorted(
            selected_by_tenant,
            key=lambda tenant_id: (
                selected_by_tenant[tenant_id][0]["renewal_last_claimed_at"]
                is not None,
                selected_by_tenant[tenant_id][0]["renewal_last_claimed_at"]
                or datetime.min.replace(tzinfo=timezone.utc),
                str(tenant_id),
            ),
        )
    )
    selected: list[asyncpg.Record] = []
    while len(selected) < limit and any(selected_by_tenant.values()):
        for tenant_id in tenant_order:
            queue = selected_by_tenant[tenant_id]
            if queue:
                selected.append(queue.popleft())
                if len(selected) == limit:
                    break
    return selected


async def register_watch(
    pool: asyncpg.Pool,
    spec: WatchSpec,
    row: asyncpg.Record,
    *,
    address: str,
    renewal_lease: RenewalLease | None = None,
    raise_on_failure: bool = False,
    minimum_expiration: datetime | None = None,
    validation_now: datetime | None = None,
) -> datetime | None:
    """Open (or refresh) a push channel for one resource and persist its state.

    A fresh channel id + token are minted each time; the previous channel (if
    any) is best-effort stopped so Google doesn't keep pinging a stale id."""
    tenant_id: UUID = row["tenant_id"]
    channel_id = secrets.token_urlsafe(24)
    token = secrets.token_urlsafe(24)
    scope = row["scope"]

    if renewal_lease is not None:
        await mark_renewal_provider_call_started(pool, renewal_lease)
    client, close = await spec.make_client(
        scope,
        tenant_id=tenant_id,
        installation_id=row["installation_id"],
    )
    try:
        try:
            channel = await spec.do_watch(
                client,
                row=row,
                channel_id=channel_id,
                address=address,
                token=token,
                ttl_seconds=_CHANNEL_TTL_S,
            )
        except (GoogleApiError, GoogleRateLimited) as exc:
            if raise_on_failure:
                raise
            await _bump_watch_failure(
                pool,
                spec,
                tenant_id,
                row["id"],
                (
                    "google_watch_rate_limited"
                    if isinstance(exc, GoogleRateLimited)
                    else "google_watch_api_error"
                ),
            )
            return None

        resource_id = channel.get("resourceId") if isinstance(channel, dict) else None
        expiration = _ms_to_dt(channel.get("expiration")) if isinstance(channel, dict) else None
        now = (
            datetime.now(timezone.utc)
            if validation_now is None
            else validation_now.astimezone(timezone.utc)
        )
        invalid = (
            not isinstance(resource_id, str)
            or not resource_id.strip()
            or expiration is None
            or expiration <= now
            or (
                minimum_expiration is not None
                and expiration <= minimum_expiration.astimezone(timezone.utc)
            )
        )
        if invalid:
            if isinstance(resource_id, str) and resource_id.strip():
                replacement_row = dict(row)
                replacement_row["watch_channel_id"] = channel_id
                replacement_row["watch_resource_id"] = resource_id
                await _best_effort_stop_channel(
                    spec,
                    client,
                    row=replacement_row,
                    phase="invalid_replacement",
                )
            if raise_on_failure:
                raise RenewalManualRepairRequired("watch_response_invalid")
            await _bump_watch_failure(
                pool,
                spec,
                tenant_id,
                row["id"],
                "watch_response_invalid",
            )
            return None

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    async with bind_tenant(conn, tenant_id) as tctx:
                        if renewal_lease is None:
                            await tctx.execute(
                                f"""
                                UPDATE {spec.table}
                                   SET watch_channel_id = $3, watch_resource_id = $4,
                                       watch_token = $5, watch_expiration = $6,
                                       watch_state = 'active'
                                 WHERE id = $1 AND tenant_id = $2
                                """,
                                row["id"],
                                tenant_id,
                                channel_id,
                                resource_id,
                                token,
                                expiration,
                            )
                        else:
                            persisted = await tctx.fetchrow(
                                f"""
                                WITH held_lease AS MATERIALIZED (
                                    SELECT source_id
                                      FROM source_renewal_jobs
                                     WHERE source_id = $7
                                       AND tenant_id = $2
                                       AND installation_id = $8
                                       AND target_key = $11
                                       AND lease_owner = $9
                                       AND lease_version = $10
                                       AND lease_expires_at > now()
                                     FOR UPDATE
                                )
                                UPDATE {spec.table}
                                   SET watch_channel_id = $3,
                                       watch_resource_id = $4,
                                       watch_token = $5,
                                       watch_expiration = $6,
                                       watch_state = 'active'
                                 WHERE id = $1
                                   AND tenant_id = $2
                                   AND {spec.install_fk} = $8
                                   AND $11 = $1::text
                                   AND state = 'active'
                                   AND EXISTS (SELECT 1 FROM held_lease)
                                RETURNING id
                                """,
                                row["id"],
                                tenant_id,
                                channel_id,
                                resource_id,
                                token,
                                expiration,
                                spec.source,
                                row["installation_id"],
                                renewal_lease.owner,
                                renewal_lease.version,
                                renewal_lease.key.target_key,
                            )
                            if persisted is None:
                                raise RenewalManualRepairRequired(
                                    "watch_renewal_lease_lost"
                                )
        except Exception:
            # The provider may have accepted the new channel before the local
            # lease/persistence step failed. Best-effort stop that replacement
            # without touching the pre-existing channel, then fail closed.
            replacement_row = dict(row)
            replacement_row["watch_channel_id"] = channel_id
            replacement_row["watch_resource_id"] = resource_id
            await _best_effort_stop_channel(
                spec,
                client,
                row=replacement_row,
                phase="persistence_failure",
            )
            raise

        # The replacement is durable before we touch the old channel. A stop
        # failure merely leaves an extra provider channel; it cannot remove the
        # newly persisted liveness path.
        if row["watch_channel_id"] and row["watch_resource_id"]:
            await _best_effort_stop_channel(
                spec,
                client,
                row=row,
                phase="prior_channel",
            )
    finally:
        await close()
    log.info(
        f"{spec.source}.watch.registered",
        channel_id=channel_id, expiration=str(expiration),
    )
    return expiration


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


async def _load_exact_watch_resource(
    invocation: RenewalInvocation,
    spec: WatchSpec,
) -> asyncpg.Record | None:
    """Resolve one active resource under its exact installation identity."""

    try:
        resource_id = UUID(invocation.target_key)
    except (TypeError, ValueError) as exc:
        raise RenewalInvocationError(
            "watch renewal target_key must be an exact resource UUID"
        ) from exc
    cols = ", ".join(f"r.{column}" for column in spec.id_cols)
    async with invocation.pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, invocation.tenant_id) as tctx:
                return await tctx.fetchrow(
                    f"""
                    SELECT r.id,
                           r.tenant_id,
                           {cols},
                           r.{spec.cursor_col} AS cursor_token,
                           r.watch_channel_id,
                           r.watch_resource_id,
                           r.watch_expiration,
                           r.{spec.install_fk} AS installation_id,
                           gi.scope
                      FROM {spec.table} r
                      JOIN {spec.install_table} gi
                        ON gi.id = r.{spec.install_fk}
                       AND gi.tenant_id = r.tenant_id
                     WHERE r.id = $1
                       AND r.tenant_id = $2
                       AND r.{spec.install_fk} = $3
                       AND r.state = 'active'
                       AND r.{spec.cursor_col} IS NOT NULL
                       AND gi.disabled_at IS NULL
                    """,
                    resource_id,
                    invocation.tenant_id,
                    invocation.installation_id,
                )


async def renew_exact_resource(
    invocation: RenewalInvocation,
    spec: WatchSpec,
) -> RenewalOutcome:
    """Renew exactly one Google Calendar/Drive resource watch.

    The source wrapper supplies its immutable :class:`WatchSpec`; this helper
    only performs durable lease choreography and exact resource lookups.  It
    does not scan, dispatch by source string, or keep an in-memory retry loop.
    """

    address = invocation.watch_address or push_address_for(spec)
    if not address:
        raise RenewalInvocationError(
            "watch renewal requires an explicit public webhook address"
        )

    async def attempt(
        call: RenewalInvocation,
        lease: RenewalLease,
    ) -> RenewalAttempt:
        row = await _load_exact_watch_resource(call, spec)
        if row is None:
            raise RenewalReauthorizationRequired("watch_resource_unavailable")
        renewal = source_definition(spec.source).renewal
        assert renewal is not None
        now = call.current_time
        expires_at = row["watch_expiration"]
        if (
            expires_at is not None
            and expires_at > now + timedelta(seconds=renewal.renewal_window_seconds)
        ):
            return RenewalAttempt(
                state="not_due",
                next_attempt_at=renewal_next_attempt_at(
                    expires_at,
                    now=now,
                    renewal_window_seconds=renewal.renewal_window_seconds,
                    error_code="watch_expiry_invalid",
                ),
                expires_at=expires_at,
            )
        try:
            renewed_expiry = await register_watch(
                call.pool,
                spec,
                row,
                address=address,
                renewal_lease=lease,
                raise_on_failure=True,
                minimum_expiration=now
                + timedelta(seconds=renewal.renewal_window_seconds),
                validation_now=now,
            )
        except DwdError as exc:
            # A definite DWD authorization rejection happened before the
            # unsafe watch-create request. It is therefore safe to stop this
            # exact resource for administrator repair rather than treating the
            # outcome as an ambiguous provider-side channel creation.
            if exc.status in {400, 401, 403}:
                raise RenewalReauthorizationRequired(
                    "dwd_reauthorization_required"
                ) from exc
            raise
        except GoogleApiError as exc:
            if getattr(exc, "status", None) in {401, 403}:
                raise RenewalReauthorizationRequired(
                    "watch_authorization_required"
                ) from exc
            raise
        if renewed_expiry is None:
            raise RenewalManualRepairRequired("watch_response_invalid")
        next_attempt = renewal_next_attempt_at(
            renewed_expiry,
            now=now,
            renewal_window_seconds=renewal.renewal_window_seconds,
            error_code="watch_expiry_invalid",
        )
        return RenewalAttempt(
            state="renewed",
            next_attempt_at=next_attempt,
            expires_at=renewed_expiry,
        )

    return await run_bounded_renewal(
        invocation,
        source_id=spec.source,
        expected_kind="watch",
        attempt=attempt,
    )


async def watch_tick(pool: asyncpg.Pool, spec: WatchSpec, *, address: str) -> int:
    """Run due watch work through the contract-owned bounded renewal path.

    The legacy due scan is retained only as a candidate selector while this
    migration is in flight. It never performs provider I/O or settles watch
    state itself: each selected resource first wins its exact
    ``source_renewal_jobs`` lease through :func:`renew_exact_resource`.
    """

    rows = await _lease_due_watches(pool, spec, limit=_LEASE_BATCH)
    n = 0
    for row in rows:
        try:
            outcome = await renew_exact_resource(
                RenewalInvocation(
                    pool=pool,
                    tenant_id=row["tenant_id"],
                    installation_id=row["installation_id"],
                    target_key=str(row["id"]),
                    worker_id=_worker(spec),
                    watch_address=address,
                ),
                spec,
            )
            if outcome.state == "renewed":
                n += 1
        except Exception as exc:  # noqa: BLE001
            log.error(
                f"{spec.source}.watch.tick_error",
                error_type=type(exc).__name__,
            )
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
            log.error(
                f"{spec.source}.watch.loop_error",
                error_type=type(exc).__name__,
            )
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
                   r.watch_token, r.{spec.install_fk} AS installation_id,
                   gi.scope
              FROM {spec.table} r
              JOIN {spec.install_table} gi
                ON gi.id = r.{spec.install_fk}
               AND gi.tenant_id = r.tenant_id
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
        installation_id=row["installation_id"],
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
    "renew_exact_resource",
    "register_watch",
    "resolve_push",
    "run_watch_scheduler",
    "watch_tick",
]
