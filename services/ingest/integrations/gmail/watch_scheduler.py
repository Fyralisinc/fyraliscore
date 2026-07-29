"""services/ingest/integrations/gmail/watch_scheduler.py — watch renewal worker body.

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
from collections import deque
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import structlog

from lib.shared.tenant_context import bind_tenant

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
from services.ingest.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
    GoogleRateLimited,
    build_google_http_client,
)
from services.ingest.integrations.gmail.dwd import DwdError, get_minter
from services.ingest.ingestion.renewal_jobs import (
    RenewalLease,
    mark_renewal_provider_call_started,
)
from services.ingest.source_contract.catalog import source_definition


log = structlog.get_logger("integrations.gmail.watch_scheduler")


SCOPE_ALIAS = {
    "gmail.metadata": GMAIL_METADATA_SCOPE,
    "gmail.readonly": GMAIL_READONLY_SCOPE,
}


_DEFAULT_TICK_S = 15 * 60
_LEASE_BATCH = 25
_BASE_BACKOFF_S = 2.0
_MAX_BACKOFF_S = 300.0
_SOURCE_ID = source_definition("gmail").source_id


def _worker_name() -> str:
    return f"gmail-watch-scheduler@{socket.gethostname()}:{os.getpid()}"


def _as_history_int(raw: object) -> int | None:
    """Parse a Gmail historyId (a number serialized as a string) to int.

    Returns None for missing / empty / non-numeric values so callers can
    fall back rather than crash on an unexpected shape.
    """
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return None


def _monotonic_history_id(stored: object, returned: object) -> str | None:
    """Pick the history cursor that never moves backwards.

    Gmail's users.watch() returns a fresh ``historyId`` on every renewal, but
    that value can be LOWER than the cursor the push/poll fetchers have already
    advanced to since the last watch. Blindly overwriting would rewind the
    bookmark and cause history.list to re-fetch or skip. So we keep the GREATER
    of the two ids, compared NUMERICALLY (the ids are numbers stringified —
    lexical compare is wrong: "9" > "10").

    Fallbacks: if one side is missing / non-numeric we keep the other (prefer
    the stored cursor when the returned one is unusable, so we still don't lose
    ground); if both are unusable we return whatever ``returned`` stringifies to
    (or None), matching the old overwrite for the degenerate case.
    """
    s = _as_history_int(stored)
    r = _as_history_int(returned)
    if s is None and r is None:
        return None if returned is None else str(returned)
    if s is None:
        return str(r)
    if r is None:
        return str(s)
    return str(s if s >= r else r)


async def _lease_due_watches(
    pool: asyncpg.Pool,
    *,
    limit: int,
) -> list[asyncpg.Record]:
    """Fairly select Gmail watches whose durable renewal job is claimable.

    The mailbox table tells us a watch needs renewal.  The corresponding
    ``source_renewal_jobs`` row decides whether an attempt is permitted now.
    That prevents a future cooldown or terminal repair from being sampled on
    every tick while later, independently due mailboxes starve.  This function
    intentionally does *not* mutate ``last_poll_at``: the durable job lease is
    the renewal ownership boundary, and poll bookkeeping must not suppress a
    later retry that was never actually claimed.

    Renewal jobs use strict RLS, so the join happens inside one explicitly
    tenant-bound transaction per discovered tenant.  The initial non-secret
    tenant discovery reads only mailbox/install metadata and never exposes a
    credential or durable-job state outside that tenant context.
    """

    if limit <= 0:
        raise ValueError("limit must be positive")

    async with pool.acquire() as conn:
        tenant_rows = await conn.fetch(
            """
            SELECT DISTINCT mw.tenant_id
              FROM gmail_mailbox_watches mw
              JOIN gmail_installations gi
                ON gi.id = mw.gmail_installation_id
               AND gi.tenant_id = mw.tenant_id
             WHERE (
                    (mw.state = 'active'
                     AND mw.watch_expiration < now() + interval '24 hours')
                 OR (mw.state IN ('pending', 'errored')
                     AND mw.watch_expiration IS NULL)
                   )
               AND gi.disabled_at IS NULL
            """,
        )

    selected_by_tenant: dict[UUID, deque[asyncpg.Record]] = {}
    for tenant_row in sorted(tenant_rows, key=lambda row: str(row["tenant_id"])):
        tenant_id: UUID = tenant_row["tenant_id"]
        async with pool.acquire() as conn:
            async with conn.transaction():
                async with bind_tenant(conn, tenant_id) as tctx:
                    rows = await tctx.fetch(
                        """
                        SELECT mw.id,
                               mw.tenant_id,
                               mw.gmail_installation_id,
                               mw.email_address,
                               mw.state,
                               mw.history_id,
                               mw.consecutive_poll_failures,
                               j.last_claimed_at AS renewal_last_claimed_at
                          FROM gmail_mailbox_watches mw
                          JOIN gmail_installations gi
                            ON gi.id = mw.gmail_installation_id
                           AND gi.tenant_id = mw.tenant_id
                          LEFT JOIN source_renewal_jobs j
                            ON j.source_id = $1
                           AND j.tenant_id = mw.tenant_id
                           AND j.installation_id = mw.gmail_installation_id
                           AND j.target_key = mw.id::text
                         WHERE mw.tenant_id = $2
                           AND (
                                  (mw.state = 'active'
                                   AND mw.watch_expiration < now() + interval '24 hours')
                               OR (mw.state IN ('pending', 'errored')
                                   AND mw.watch_expiration IS NULL)
                               )
                           AND gi.disabled_at IS NULL
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
                                  mw.watch_expiration NULLS FIRST,
                                  mw.id
                         LIMIT $3
                        """,
                        _SOURCE_ID,
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


async def renew_one(
    pool: asyncpg.Pool,
    row: asyncpg.Record,
    *,
    renewal_lease: RenewalLease | None = None,
    raise_on_failure: bool = False,
    minimum_expiration: datetime | None = None,
    validation_now: datetime | None = None,
) -> object | None:
    tenant_id: UUID = row["tenant_id"]
    gmail_installation_id: UUID = row["gmail_installation_id"]
    email = row["email_address"]

    # Fetch install scope + topic name (tenant-bound).
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                topics = await tctx.fetch(
                    """
                    SELECT gi.scope, t.topic_name
                      FROM gmail_installations gi
                      JOIN gmail_pubsub_topics t
                        ON t.gmail_installation_id = gi.id
                       AND t.tenant_id = gi.tenant_id
                       AND t.teardown_at IS NULL
                     WHERE gi.id = $1
                       AND gi.tenant_id = $2
                       AND gi.disabled_at IS NULL
                    """,
                    gmail_installation_id,
                    tenant_id,
                )
    if len(topics) != 1:
        if raise_on_failure:
            raise RenewalReauthorizationRequired(
                "gmail_installation_or_topic_unavailable"
            )
        log.warning(
            "gmail.scheduler.invalid_install_or_topic_binding",
            gmail_installation_id=str(gmail_installation_id),
            topic_count=len(topics),
        )
        return None

    meta = topics[0]

    scope_alias = meta["scope"]
    scope_long = SCOPE_ALIAS[scope_alias]
    topic_name = meta["topic_name"]

    minter = get_minter()
    if renewal_lease is not None:
        await mark_renewal_provider_call_started(pool, renewal_lease)
    async with build_google_http_client(
        minter,
        tenant_id=str(tenant_id),
        installation_id=str(gmail_installation_id),
    ) as http:
        gmail = GmailClient(http)
        try:
            result = await gmail.watch(
                user_email=email, scope=scope_long, topic_name=topic_name,
            )
        except GoogleRateLimited:
            if raise_on_failure:
                raise
            await _bump_failure(
                pool,
                tenant_id,
                row["id"],
                "gmail_watch_rate_limited",
            )
            return None
        except GoogleApiError:
            if raise_on_failure:
                raise
            await _bump_failure(
                pool,
                tenant_id,
                row["id"],
                "gmail_watch_api_error",
            )
            return None

    from services.ingest.integrations.gmail.watch import _expiration_to_dt
    history_id = str(result.get("historyId", "")) if isinstance(result, dict) else ""
    expiration = (
        _expiration_to_dt(result.get("expiration"))
        if isinstance(result, dict)
        else None
    )
    now = (
        datetime.now(timezone.utc)
        if validation_now is None
        else validation_now.astimezone(timezone.utc)
    )
    invalid = (
        _as_history_int(history_id) is None
        or expiration is None
        or expiration <= now
        or (
            minimum_expiration is not None
            and expiration <= minimum_expiration.astimezone(timezone.utc)
        )
    )
    if invalid:
        if raise_on_failure:
            raise RenewalManualRepairRequired("gmail_watch_response_invalid")
        await _bump_failure(
            pool,
            tenant_id,
            row["id"],
            "gmail_watch_response_invalid",
        )
        return None

    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                # Monotonic cursor guard: a watch renewal can return a LOWER
                # historyId than the push/poll fetchers have already advanced
                # the stored cursor to. Keep the GREATER id (numeric compare on
                # the stringified values) so the bookmark never rewinds. The
                # comparison happens in SQL so it is also race-safe against a
                # concurrent fetcher advance between our lease read and here.
                # Non-numeric / empty ids fall back to the existing value.
                if renewal_lease is None:
                    await tctx.execute(
                        """
                        UPDATE gmail_mailbox_watches
                           SET state = 'active',
                               history_id = CASE
                                 WHEN $3 !~ '^[0-9]+$' THEN history_id
                                 WHEN history_id IS NULL OR history_id !~ '^[0-9]+$'
                                   THEN $3
                                 WHEN $3::bigint > history_id::bigint THEN $3
                                 ELSE history_id
                               END,
                               watch_expiration = $4,
                               consecutive_poll_failures = 0,
                               last_error = NULL
                         WHERE id = $1 AND tenant_id = $2
                        """,
                        row["id"], tenant_id, history_id, expiration,
                    )
                else:
                    persisted = await tctx.fetchrow(
                        """
                        WITH held_lease AS MATERIALIZED (
                            SELECT source_id
                              FROM source_renewal_jobs
                             WHERE source_id = 'gmail'
                               AND tenant_id = $2
                               AND installation_id = $5
                               AND target_key = $8
                               AND lease_owner = $6
                               AND lease_version = $7
                               AND lease_expires_at > now()
                             FOR UPDATE
                        )
                        UPDATE gmail_mailbox_watches
                           SET state = 'active',
                               history_id = CASE
                                 WHEN $3 !~ '^[0-9]+$' THEN history_id
                                 WHEN history_id IS NULL OR history_id !~ '^[0-9]+$'
                                   THEN $3
                                 WHEN $3::bigint > history_id::bigint THEN $3
                                 ELSE history_id
                               END,
                               watch_expiration = $4,
                               consecutive_poll_failures = 0,
                               last_error = NULL
                         WHERE id = $1
                           AND tenant_id = $2
                           AND gmail_installation_id = $5
                           AND $8 = $1::text
                           AND state IN ('pending', 'active', 'errored')
                           AND EXISTS (SELECT 1 FROM held_lease)
                        RETURNING id
                        """,
                        row["id"],
                        tenant_id,
                        history_id,
                        expiration,
                        gmail_installation_id,
                        renewal_lease.owner,
                        renewal_lease.version,
                        renewal_lease.key.target_key,
                    )
                    if persisted is None:
                        raise RenewalManualRepairRequired(
                            "gmail_watch_renewal_lease_lost"
                        )
    log.info(
        "gmail.scheduler.renewed",
        email=email, expiration=expiration.isoformat() if expiration else None,
    )
    return expiration


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


async def _load_exact_watch(
    invocation: RenewalInvocation,
) -> asyncpg.Record | None:
    """Load one renewable mailbox watch under its exact installation row."""

    try:
        watch_id = UUID(invocation.target_key)
    except (TypeError, ValueError) as exc:
        raise RenewalInvocationError(
            "Gmail watch renewal target_key must be an exact mailbox-watch UUID"
        ) from exc
    async with invocation.pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, invocation.tenant_id) as tctx:
                return await tctx.fetchrow(
                    """
                    SELECT mw.id,
                           mw.tenant_id,
                           mw.gmail_installation_id,
                           mw.email_address,
                           mw.state,
                           mw.history_id,
                           mw.watch_expiration,
                           mw.consecutive_poll_failures
                      FROM gmail_mailbox_watches mw
                      JOIN gmail_installations gi
                        ON gi.id = mw.gmail_installation_id
                       AND gi.tenant_id = mw.tenant_id
                     WHERE mw.id = $1
                       AND mw.tenant_id = $2
                       AND mw.gmail_installation_id = $3
                       AND mw.state IN ('pending', 'active', 'errored')
                       AND gi.disabled_at IS NULL
                    """,
                    watch_id,
                    invocation.tenant_id,
                    invocation.installation_id,
                )


async def renew_exact_installation(
    invocation: RenewalInvocation,
) -> RenewalOutcome:
    """Contract binding for one exact Gmail mailbox watch renewal."""

    source = source_definition("gmail")
    renewal = source.renewal
    if renewal is None or renewal.kind != "watch":  # defensive startup guard
        raise RenewalInvocationError("gmail has no watch renewal contract")

    async def attempt(
        call: RenewalInvocation,
        lease: RenewalLease,
    ) -> RenewalAttempt:
        row = await _load_exact_watch(call)
        if row is None:
            raise RenewalReauthorizationRequired("gmail_watch_unavailable")
        now = call.current_time
        expires_at = row["watch_expiration"]
        if (
            row["state"] == "active"
            and expires_at is not None
            and expires_at > now + timedelta(seconds=renewal.renewal_window_seconds)
        ):
            return RenewalAttempt(
                state="not_due",
                next_attempt_at=renewal_next_attempt_at(
                    expires_at,
                    now=now,
                    renewal_window_seconds=renewal.renewal_window_seconds,
                    error_code="gmail_watch_expiry_invalid",
                ),
                expires_at=expires_at,
            )
        try:
            renewed_expiry = await renew_one(
                call.pool,
                row,
                renewal_lease=lease,
                raise_on_failure=True,
                minimum_expiration=now
                + timedelta(seconds=renewal.renewal_window_seconds),
                validation_now=now,
            )
        except DwdError as exc:
            # The token exchange failed before Gmail's unsafe watch request.
            # A 4xx DWD rejection has an unambiguous repair path; do not let
            # it fall through to generic manual reconciliation.
            if exc.status in {400, 401, 403}:
                raise RenewalReauthorizationRequired(
                    "gmail_dwd_reauthorization_required"
                ) from exc
            raise
        except GoogleApiError as exc:
            if getattr(exc, "status", None) in {401, 403}:
                raise RenewalReauthorizationRequired(
                    "gmail_watch_authorization_required"
                ) from exc
            raise
        if renewed_expiry is None:
            raise RenewalManualRepairRequired("gmail_watch_response_invalid")
        next_attempt = renewal_next_attempt_at(
            renewed_expiry,
            now=now,
            renewal_window_seconds=renewal.renewal_window_seconds,
            error_code="gmail_watch_expiry_invalid",
        )
        return RenewalAttempt(
            state="renewed",
            next_attempt_at=next_attempt,
            expires_at=renewed_expiry,
        )

    return await run_bounded_renewal(
        invocation,
        source_id="gmail",
        expected_kind="watch",
        attempt=attempt,
    )


async def tick(pool: asyncpg.Pool) -> int:
    """Run due Gmail watches through their exact bounded-renewal invoker."""

    worker = _worker_name()
    rows = await _lease_due_watches(pool, limit=_LEASE_BATCH)
    n = 0
    for row in rows:
        try:
            outcome = await renew_exact_installation(
                RenewalInvocation(
                    pool=pool,
                    tenant_id=row["tenant_id"],
                    installation_id=row["gmail_installation_id"],
                    target_key=str(row["id"]),
                    worker_id=worker,
                ),
            )
            if outcome.state == "renewed":
                n += 1
        except Exception as exc:  # noqa: BLE001
            log.error(
                "gmail.scheduler.tick_error",
                error_type=type(exc).__name__,
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
            log.error(
                "gmail.scheduler.loop_error",
                error_type=type(exc).__name__,
            )
        # Jittered sleep so multiple scheduler processes don't sync.
        jitter = random.uniform(0.0, tick_interval_s * 0.1)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=tick_interval_s + jitter)
        except asyncio.TimeoutError:
            pass


__all__ = [
    "_monotonic_history_id",
    "renew_exact_installation",
    "renew_one",
    "run_forever",
    "tick",
]
