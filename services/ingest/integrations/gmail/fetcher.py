"""services/ingest/integrations/gmail/fetcher.py — shared history-drain + dispatch.

Both the push handler and the history poller funnel through here:

    drain_mailbox_history(pool, gmail, tenant_id, install_id, email, read_path)

This module:
  1. Looks up the mailbox's last-known history_id and the install's scope.
  2. Pages users.history.list with historyTypes=['messageAdded'].
  3. For each new messageId: users.messages.get → ingest via the
     `gmail:` handler (which does thread canonicalization + dedup +
     observation write).
  4. Advances history_id and stamps last_push_at / last_poll_at on
     success.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import structlog

from lib.shared.tenant_context import bind_tenant, tenant_transaction

from services.ingest.integrations.gmail.audit import write_read_audit
from services.ingest.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GmailHistoryExpired,
    GmailHistoryRecoveryIncomplete,
    GoogleApiError,
)


log = structlog.get_logger("integrations.gmail.fetcher")


SCOPE_ALIAS = {
    "gmail.metadata": GMAIL_METADATA_SCOPE,
    "gmail.readonly": GMAIL_READONLY_SCOPE,
}

_HISTORY_RECOVERY_COOLDOWN_S = 10 * 60
_HISTORY_RECOVERY_ERROR_PREFIX = "history_recovery:"
_HISTORY_RECOVERY_PAGE_SIZE = 500
_HISTORY_RECOVERY_MAX_LIST_PAGES = max(
    1, int(os.environ.get("GMAIL_HISTORY_RECOVERY_MAX_LIST_PAGES", "2000"))
)
_HISTORY_RECOVERY_MAX_HISTORY_PAGES = max(
    1, int(os.environ.get("GMAIL_HISTORY_RECOVERY_MAX_HISTORY_PAGES", "2000"))
)


@dataclass(frozen=True)
class _GmailDrainContext:
    pool: Any
    gmail: GmailClient
    tenant_id: UUID
    gmail_installation_id: UUID
    email_address: str
    read_path: str
    scope_alias: str
    scope_long: str
    cutover_enabled: bool
    s3_raw_client: Any
    kafka_producer: Any


@dataclass
class _GmailDrainCounters:
    ingested: int = 0
    deduped: int = 0


async def _publish_gmail_message_raw(
    *,
    s3_raw_client: Any,
    kafka_producer: Any,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
    scope_alias: str,
    message_resource: dict[str, Any],
    read_path: str,
) -> bool:
    """Publish one fetched Gmail message to `ingestion.raw` (cutover).

    The raw body is the bare handler-conformant record — byte-shaped
    identically to what the M6.3 backfill fetcher's `_build_record`
    produces — so the normalizer dispatches it through the same `gmail:`
    handler with `headers={}` and derives the SAME external_id
    (`gmail:{install}:{message_id}`). Cross-path dedup therefore collapses
    a backfilled message and its live "poll" twin to one observation.

    Returns True on full publish success, False on any failure (caller
    falls back to inline dispatch). Mirrors the discord gateway cutover
    helper's return-value-signals-failure contract.
    """
    import orjson

    from services.ingest.ingestion.shadow_write import shadow_write_raw

    record = {
        "message_resource": message_resource,
        "mailbox_email": email_address,
        "scope_used": scope_alias,
        "gmail_installation_id": str(gmail_installation_id),
        "read_path": read_path,
    }
    raw_body = orjson.dumps(record)
    try:
        await shadow_write_raw(
            tenant_id=tenant_id,
            source="gmail",
            ingress_kind="poll",
            raw_body=raw_body,
            s3_client=s3_raw_client,
            kafka_producer=kafka_producer,
            ingress_metadata={"read_path": read_path},
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "gmail.fetcher.kafka_path_failed",
            email=email_address,
            error_type=type(exc).__name__,
            error=str(exc)[:200],
        )
        return False


def _validate_read_path(read_path: str) -> None:
    if read_path not in ("push", "poll"):
        raise ValueError(f"read_path must be 'push' or 'poll', got {read_path!r}")


async def _gmail_cutover_enabled(
    *,
    tenant_id: UUID,
    s3_raw_client: Any,
    kafka_producer: Any,
    tenant_flags: Any,
) -> bool:
    if s3_raw_client is None or kafka_producer is None or tenant_flags is None:
        return False
    return bool(await tenant_flags.kafka_path_enabled(tenant_id))


async def _load_mailbox_watch(
    *,
    pool: Any,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
) -> Any | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                return await tctx.fetchrow(
                    """
                    SELECT mw.id, mw.history_id, mw.state,
                           mw.last_poll_at, mw.last_error, gi.scope
                      FROM gmail_mailbox_watches mw
                      JOIN gmail_installations gi
                        ON gi.id = mw.gmail_installation_id
                     WHERE mw.gmail_installation_id = $1
                       AND mw.email_address = $2
                    """,
                    gmail_installation_id,
                    email_address.lower(),
                )


def _skip_result_for_watch(watch_row: Any | None) -> dict[str, Any] | None:
    if watch_row is None:
        return {"status": "skipped", "reason": "no_watch_row"}
    if watch_row["state"] in ("paused", "opted_out"):
        return {
            "status": "skipped",
            "reason": "watch_inactive",
            "state": watch_row["state"],
        }
    if not watch_row["history_id"]:
        return {"status": "skipped", "reason": "no_history_bookmark"}
    return None


def _history_recovery_retry_at(
    watch_row: Any,
    *,
    read_path: str,
) -> datetime | None:
    """Return the durable recovery cooldown deadline for push drains.

    Poll drains already own a ten-minute mailbox lease. Push drains consult
    the same timestamp only while a recovery marker is present, so a normal
    recent poll never delays live mail.
    """
    if read_path != "push":
        return None
    last_error = str(watch_row["last_error"] or "")
    last_attempt = watch_row["last_poll_at"]
    if not last_error.startswith(_HISTORY_RECOVERY_ERROR_PREFIX) or not isinstance(
        last_attempt, datetime
    ):
        return None
    if last_attempt.tzinfo is None:
        last_attempt = last_attempt.replace(tzinfo=timezone.utc)
    retry_at = last_attempt + timedelta(seconds=_HISTORY_RECOVERY_COOLDOWN_S)
    return retry_at if retry_at > datetime.now(timezone.utc) else None


def _retry_later_result(retry_at: datetime | None = None) -> dict[str, Any]:
    if retry_at is None:
        retry_at = datetime.now(timezone.utc) + timedelta(
            seconds=_HISTORY_RECOVERY_COOLDOWN_S
        )
    return {
        "status": "retry_later",
        "reason": "history_recovery_cooldown",
        "not_before": retry_at.isoformat(),
    }


def _is_expired_history_error(exc: GoogleApiError) -> bool:
    if isinstance(exc, GmailHistoryExpired):
        return True
    status = (
        exc.context.get("status")
        or exc.context.get("status_code")
        or exc.context.get("http_status")
    )
    if status is None:
        return False
    try:
        return int(str(status)) == 404
    except (TypeError, ValueError):
        return False


async def _begin_history_recovery(
    ctx: _GmailDrainContext,
    *,
    expired_history_id: str,
) -> bool:
    """Durably claim one recovery attempt.

    The poller has already moved ``last_poll_at`` under a row lease. Push has
    no such lease, so its conditional update supplies the cross-replica claim.
    """
    marker = f"{_HISTORY_RECOVERY_ERROR_PREFIX} expired cursor; full sync pending"
    push_guard = (
        "AND $4::integer >= 0"
        if ctx.read_path == "poll"
        else """
           AND (
                 last_poll_at IS NULL
                 OR last_poll_at <=
                    now() - ($4 * interval '1 second')
               )
        """
    )
    timestamp = "" if ctx.read_path == "poll" else "last_poll_at = now(),"
    async with tenant_transaction(ctx.tenant_id, pool=ctx.pool) as tctx:
        row = await tctx.fetchrow(
            f"""
            UPDATE gmail_mailbox_watches
               SET {timestamp}
                   last_error = $3
             WHERE gmail_installation_id = $1
               AND email_address = $2
               AND history_id = $5
               AND state = 'active'
               {push_guard}
         RETURNING id
            """,
            ctx.gmail_installation_id,
            ctx.email_address.lower(),
            marker,
            _HISTORY_RECOVERY_COOLDOWN_S,
            expired_history_id,
        )
        if row is None and ctx.read_path == "push":
            # A poll or another push owns the recent mailbox slot. Persist the
            # marker as well as returning RetryLater so later pushes skip even
            # the known-dead history.list call until that owner finishes.
            await tctx.execute(
                """
                UPDATE gmail_mailbox_watches
                   SET last_error = $3
                 WHERE gmail_installation_id = $1
                   AND email_address = $2
                   AND history_id = $4
                   AND state = 'active'
                """,
                ctx.gmail_installation_id,
                ctx.email_address.lower(),
                marker,
                expired_history_id,
            )
    return row is not None


async def _collect_history_message_ids(
    *,
    gmail: GmailClient,
    email_address: str,
    scope_long: str,
    start_history_id: str,
    max_pages: int | None = None,
) -> tuple[list[str], str | None]:
    message_ids: list[str] = []
    new_history_id: str | None = start_history_id
    page_token: str | None = None
    seen_page_tokens: set[str] = set()
    pages = 0
    while True:
        if max_pages is not None and pages >= max_pages:
            raise GmailHistoryRecoveryIncomplete(
                "Gmail history recovery exceeded its page bound",
                phase="history_catch_up",
                max_pages=max_pages,
            )
        page = await gmail.history_list(
            user_email=email_address,
            scope=scope_long,
            start_history_id=start_history_id,
            page_token=page_token,
        )
        pages += 1
        message_ids.extend(_message_ids_from_history_page(page))
        latest = page.get("historyId")
        if latest:
            new_history_id = str(latest)
        page_token = page.get("nextPageToken")
        if not page_token:
            return message_ids, new_history_id
        if page_token in seen_page_tokens:
            raise GmailHistoryRecoveryIncomplete(
                "Gmail history recovery repeated a page token",
                phase="history_catch_up",
                page_token=page_token,
            )
        seen_page_tokens.add(page_token)


def _message_ids_from_history_page(page: dict[str, Any]) -> list[str]:
    message_ids: list[str] = []
    for entry in page.get("history") or []:
        for added in entry.get("messagesAdded") or []:
            msg = (added or {}).get("message") or {}
            msg_id = msg.get("id")
            if msg_id:
                message_ids.append(msg_id)
    return message_ids


async def _write_gmail_read_audit(
    *,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
    message_id: str,
    scope_alias: str,
    read_path: str,
) -> None:
    async with tenant_transaction(tenant_id) as tctx:
        await write_read_audit(
            tctx,
            gmail_installation_id=gmail_installation_id,
            email_address=email_address,
            message_id=message_id,
            scope_used=scope_alias,
            read_path=read_path,
        )


async def _dispatch_gmail_resource_inline(
    ctx: _GmailDrainContext,
    message_resource: dict[str, Any],
) -> dict[str, Any] | None:
    # Local import to avoid module-load cycles via the handler registry.
    from services.ingest.ingestion.handlers.gmail import dispatch_gmail_message_resource

    return await dispatch_gmail_message_resource(
        pool=ctx.pool,
        tenant_id=ctx.tenant_id,
        gmail_installation_id=ctx.gmail_installation_id,
        email_address=ctx.email_address,
        scope_alias=ctx.scope_alias,
        message_resource=message_resource,
        read_path=ctx.read_path,
    )


async def _drain_message_ids(
    ctx: _GmailDrainContext,
    message_ids: list[str],
    *,
    required: bool = False,
) -> _GmailDrainCounters:
    counters = _GmailDrainCounters()
    for message_id in message_ids:
        await _drain_message_id(ctx, message_id, counters, required=required)
    return counters


async def _drain_message_id(
    ctx: _GmailDrainContext,
    message_id: str,
    counters: _GmailDrainCounters,
    *,
    required: bool,
) -> None:
    try:
        resource = await ctx.gmail.get_message(
            user_email=ctx.email_address,
            scope=ctx.scope_long,
            message_id=message_id,
        )
    except GoogleApiError as exc:
        if required:
            raise GmailHistoryRecoveryIncomplete(
                "Gmail recovery could not hydrate a required message",
                phase="messages.get",
                message_id=message_id,
                upstream_error=exc.code,
            ) from exc
        log.warning(
            "gmail.fetcher.get_message_failed",
            email=ctx.email_address,
            message_id=message_id,
            error=str(exc)[:200],
        )
        return

    if await _publish_cutover_or_continue_inline(ctx, message_id, resource):
        counters.ingested += 1
        return

    await _dispatch_inline_and_count(
        ctx,
        message_id,
        resource,
        counters,
        required=required,
    )


async def _publish_cutover_or_continue_inline(
    ctx: _GmailDrainContext,
    message_id: str,
    resource: dict[str, Any],
) -> bool:
    if not ctx.cutover_enabled:
        return False
    published = await _publish_gmail_message_raw(
        s3_raw_client=ctx.s3_raw_client,
        kafka_producer=ctx.kafka_producer,
        tenant_id=ctx.tenant_id,
        gmail_installation_id=ctx.gmail_installation_id,
        email_address=ctx.email_address,
        scope_alias=ctx.scope_alias,
        message_resource=resource,
        read_path=ctx.read_path,
    )
    if published:
        await _write_gmail_read_audit(
            tenant_id=ctx.tenant_id,
            gmail_installation_id=ctx.gmail_installation_id,
            email_address=ctx.email_address,
            message_id=message_id,
            scope_alias=ctx.scope_alias,
            read_path=ctx.read_path,
        )
        return True
    log.warning(
        "gmail.fetcher.kafka_path_fallback_to_inline",
        email=ctx.email_address,
        message_id=message_id,
    )
    return False


async def _dispatch_inline_and_count(
    ctx: _GmailDrainContext,
    message_id: str,
    resource: dict[str, Any],
    counters: _GmailDrainCounters,
    *,
    required: bool = False,
) -> None:
    try:
        result = await _dispatch_gmail_resource_inline(ctx, resource)
    except Exception as exc:  # noqa: BLE001 — handler errors should not stop the drain
        if required:
            raise GmailHistoryRecoveryIncomplete(
                "Gmail recovery could not persist a required message",
                phase="message_dispatch",
                message_id=message_id,
                error_type=type(exc).__name__,
            ) from exc
        log.warning(
            "gmail.fetcher.ingest_failed",
            email=ctx.email_address,
            message_id=message_id,
            error=str(exc)[:200],
        )
        return
    if result is None:
        return
    if result.get("deduped"):
        counters.deduped += 1
    else:
        counters.ingested += 1
    await _write_gmail_read_audit(
        tenant_id=ctx.tenant_id,
        gmail_installation_id=ctx.gmail_installation_id,
        email_address=ctx.email_address,
        message_id=message_id,
        scope_alias=ctx.scope_alias,
        read_path=ctx.read_path,
    )


def _merge_counters(
    target: _GmailDrainCounters,
    addition: _GmailDrainCounters,
) -> None:
    target.ingested += addition.ingested
    target.deduped += addition.deduped


def _snapshot_message_ids(
    page: dict[str, Any],
    *,
    seen: set[str],
) -> list[str]:
    result: list[str] = []
    for stub in page.get("messages") or []:
        message_id = stub.get("id") if isinstance(stub, dict) else None
        if not message_id:
            raise GmailHistoryRecoveryIncomplete(
                "Gmail full-sync response omitted a required message id",
                phase="messages.list",
            )
        message_id = str(message_id)
        if message_id not in seen:
            seen.add(message_id)
            result.append(message_id)
    return result


async def _drain_full_mailbox_snapshot(
    ctx: _GmailDrainContext,
) -> tuple[_GmailDrainCounters, set[str]]:
    counters = _GmailDrainCounters()
    seen_message_ids: set[str] = set()
    seen_page_tokens: set[str] = set()
    page_token: str | None = None

    for _page_number in range(_HISTORY_RECOVERY_MAX_LIST_PAGES):
        page = await ctx.gmail.messages_list(
            user_email=ctx.email_address,
            scope=ctx.scope_long,
            page_token=page_token,
            max_results=_HISTORY_RECOVERY_PAGE_SIZE,
        )
        message_ids = _snapshot_message_ids(page, seen=seen_message_ids)
        _merge_counters(
            counters,
            await _drain_message_ids(ctx, message_ids, required=True),
        )
        next_page_token = page.get("nextPageToken")
        if not next_page_token:
            return counters, seen_message_ids
        next_page_token = str(next_page_token)
        if next_page_token in seen_page_tokens:
            raise GmailHistoryRecoveryIncomplete(
                "Gmail full sync repeated a page token",
                phase="messages.list",
                page_token=next_page_token,
            )
        seen_page_tokens.add(next_page_token)
        page_token = next_page_token

    raise GmailHistoryRecoveryIncomplete(
        "Gmail full sync exceeded its page bound",
        phase="messages.list",
        max_pages=_HISTORY_RECOVERY_MAX_LIST_PAGES,
    )


async def _recover_expired_history(
    ctx: _GmailDrainContext,
) -> tuple[_GmailDrainCounters, int, str]:
    """Take a full snapshot, then close its race window with history.list."""
    try:
        profile = await ctx.gmail.get_profile(
            user_email=ctx.email_address,
            scope=ctx.scope_long,
        )
        seed_history_id = profile.get("historyId")
        if seed_history_id is None:
            raise GmailHistoryRecoveryIncomplete(
                "Gmail profile did not provide a recovery historyId",
                phase="profile.get",
            )
        seed_history_id = str(seed_history_id)
        counters, seen = await _drain_full_mailbox_snapshot(ctx)
        catch_up_ids, final_history_id = await _collect_history_message_ids(
            gmail=ctx.gmail,
            email_address=ctx.email_address,
            scope_long=ctx.scope_long,
            start_history_id=seed_history_id,
            max_pages=_HISTORY_RECOVERY_MAX_HISTORY_PAGES,
        )
        required_catch_up = list(
            dict.fromkeys(
                message_id for message_id in catch_up_ids if message_id not in seen
            )
        )
        _merge_counters(
            counters,
            await _drain_message_ids(ctx, required_catch_up, required=True),
        )
        seen.update(required_catch_up)
        return counters, len(seen), str(final_history_id or seed_history_id)
    except GmailHistoryRecoveryIncomplete:
        raise
    except Exception as exc:
        upstream_code = getattr(exc, "code", type(exc).__name__)
        raise GmailHistoryRecoveryIncomplete(
            "Gmail full history recovery did not complete",
            phase="full_sync",
            upstream_error=upstream_code,
        ) from exc


async def _advance_mailbox_bookmark(
    *,
    pool: Any,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
    read_path: str,
    new_history_id: str | None,
    expected_history_id: str | None = None,
) -> bool:
    timestamp_column = "last_push_at" if read_path == "push" else "last_poll_at"
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        row = await tctx.fetchrow(
            f"""
            UPDATE gmail_mailbox_watches
               SET history_id = COALESCE($3, history_id),
                   {timestamp_column} = now(),
                   consecutive_poll_failures = 0,
                   last_error = NULL
             WHERE gmail_installation_id = $1
               AND email_address = $2
               AND ($4::text IS NULL OR history_id = $4)
         RETURNING id
            """,
            gmail_installation_id,
            email_address.lower(),
            new_history_id,
            expected_history_id,
        )
    return row is not None


async def _finish_drain(
    ctx: _GmailDrainContext,
    *,
    counters: _GmailDrainCounters,
    messages_seen: int,
    history_id: str | None,
    status: str,
    expected_history_id: str | None = None,
) -> dict[str, Any]:
    committed = await _advance_mailbox_bookmark(
        pool=ctx.pool,
        tenant_id=ctx.tenant_id,
        gmail_installation_id=ctx.gmail_installation_id,
        email_address=ctx.email_address,
        read_path=ctx.read_path,
        new_history_id=history_id,
        expected_history_id=expected_history_id,
    )
    return {
        "status": status if committed else "superseded",
        "ingested": counters.ingested,
        "deduped": counters.deduped,
        "messages_seen": messages_seen,
        "history_id": history_id,
    }


async def _drain_from_watch(
    ctx: _GmailDrainContext,
    watch_row: Any,
) -> dict[str, Any]:
    retry_at = _history_recovery_retry_at(
        watch_row,
        read_path=ctx.read_path,
    )
    if retry_at is not None:
        return _retry_later_result(retry_at)

    try:
        message_ids, history_id = await _collect_history_message_ids(
            gmail=ctx.gmail,
            email_address=ctx.email_address,
            scope_long=ctx.scope_long,
            start_history_id=watch_row["history_id"],
        )
    except GoogleApiError as exc:
        if not _is_expired_history_error(exc):
            raise
        if not await _begin_history_recovery(
            ctx,
            expired_history_id=str(watch_row["history_id"]),
        ):
            return _retry_later_result()
        counters, messages_seen, history_id = await _recover_expired_history(ctx)
        return await _finish_drain(
            ctx,
            counters=counters,
            messages_seen=messages_seen,
            history_id=history_id,
            status="recovered",
            expected_history_id=str(watch_row["history_id"]),
        )

    counters = await _drain_message_ids(ctx, message_ids)
    return await _finish_drain(
        ctx,
        counters=counters,
        messages_seen=len(message_ids),
        history_id=history_id,
        status="ok",
    )


async def drain_mailbox_history(
    *,
    pool: Any,
    gmail: GmailClient,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
    read_path: str,
    s3_raw_client: Any = None,
    kafka_producer: Any = None,
    tenant_flags: Any = None,
) -> dict[str, Any]:
    """Drain new history for one mailbox. Returns a small counters dict.

    NOTE: a single drain may issue many API calls. Caller is expected
    to scope concurrency per (install, email) — typically by leasing
    via FOR UPDATE SKIP LOCKED in the poller, or by serializing pushes
    per subscription.

    Live-via-Kafka cutover (parallel to the slack/github webhook-router
    cutover + the discord gateway cutover): when the shadow deps
    (`s3_raw_client` + `kafka_producer` + `tenant_flags`) are wired, each
    fetched message resource is published to `ingestion.raw`
    (ingress_kind="poll") instead of ingested inline, and the writer pool
    produces the observation via the full-mode path. Inverted default
    (kafka-first): cutover is on UNLESS an operator / circuit-breaker
    explicitly set `ingestion.kafka_path_enabled=FALSE` for the tenant (the
    kill-switch) — resolved through the shared `kafka_path_enabled()` helper.
    On a per-message publish failure, that message falls back to inline
    dispatch (never dropped).
    """
    _validate_read_path(read_path)
    cutover_enabled = await _gmail_cutover_enabled(
        tenant_id=tenant_id,
        s3_raw_client=s3_raw_client,
        kafka_producer=kafka_producer,
        tenant_flags=tenant_flags,
    )
    watch_row = await _load_mailbox_watch(
        pool=pool,
        tenant_id=tenant_id,
        gmail_installation_id=gmail_installation_id,
        email_address=email_address,
    )
    if skip_result := _skip_result_for_watch(watch_row):
        return skip_result
    assert watch_row is not None

    scope_alias = watch_row["scope"]
    scope_long = SCOPE_ALIAS[scope_alias]
    return await _drain_from_watch(
        _GmailDrainContext(
            pool=pool,
            gmail=gmail,
            tenant_id=tenant_id,
            gmail_installation_id=gmail_installation_id,
            email_address=email_address,
            read_path=read_path,
            scope_alias=scope_alias,
            scope_long=scope_long,
            cutover_enabled=cutover_enabled,
            s3_raw_client=s3_raw_client,
            kafka_producer=kafka_producer,
        ),
        watch_row,
    )


__all__ = ["drain_mailbox_history"]
