"""services/integrations/gmail/watch.py — per-mailbox users.watch lifecycle.

A `watch` registers a Gmail mailbox to publish change-notifications onto
the per-tenant Pub/Sub topic. Properties:

- One row in gmail_mailbox_watches per active mailbox.
- watch.expiration is set ~7d in the future by Gmail; the scheduler in
  services/integrations/gmail/watch_scheduler.py renews before expiry.
- history_id is the bookmark for users.history.list — first watch
  returns the starting historyId; subsequent watches return the latest.
- users.stop drops the watch; the row transitions to 'paused' or
  'opted_out' depending on caller intent.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import TenantContext

from services.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
)


log = structlog.get_logger("integrations.gmail.watch")


def _expiration_to_dt(raw: str | int | None) -> datetime | None:
    """Gmail returns expiration as a string of ms-epoch."""
    if raw is None:
        return None
    try:
        ms = int(raw)
    except (ValueError, TypeError):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


async def upsert_pending_watch(
    tctx: TenantContext,
    *,
    gmail_installation_id: UUID,
    email_address: str,
) -> UUID:
    """Idempotent insert of a `pending` watch row. Returns the row id."""
    row = await tctx.fetchrow(
        """
        INSERT INTO gmail_mailbox_watches (
          id, tenant_id, gmail_installation_id, email_address, state
        ) VALUES ($1, $2, $3, $4, 'pending')
        ON CONFLICT (gmail_installation_id, email_address) DO UPDATE
          SET state = CASE
            WHEN gmail_mailbox_watches.state IN ('opted_out') THEN gmail_mailbox_watches.state
            ELSE EXCLUDED.state
          END
        RETURNING id
        """,
        uuid7(), tctx.tenant_id, gmail_installation_id, email_address.lower(),
    )
    if row is None:
        raise RuntimeError("upsert returned no row — invariant broken")
    return row["id"]


async def activate_watch(
    tctx: TenantContext,
    gmail: GmailClient,
    *,
    gmail_installation_id: UUID,
    email_address: str,
    scope: str,
    topic_name: str,
) -> None:
    """Call Gmail users.watch and persist the resulting historyId + expiration.

    On error, sets state='errored' and stamps last_error; the scheduler
    retries with backoff.
    """
    if scope not in (GMAIL_METADATA_SCOPE, GMAIL_READONLY_SCOPE):
        raise ValueError(f"unsupported gmail scope: {scope!r}")

    try:
        result = await gmail.watch(
            user_email=email_address, scope=scope, topic_name=topic_name,
        )
    except GoogleApiError as exc:
        await tctx.execute(
            """
            UPDATE gmail_mailbox_watches
               SET state = 'errored', last_error = $3
             WHERE gmail_installation_id = $1 AND email_address = $2
            """,
            gmail_installation_id, email_address.lower(), str(exc)[:500],
        )
        raise

    history_id = str(result.get("historyId", ""))
    expiration = _expiration_to_dt(result.get("expiration"))

    await tctx.execute(
        """
        UPDATE gmail_mailbox_watches
           SET state = 'active',
               history_id = $3,
               watch_expiration = $4,
               last_error = NULL,
               consecutive_poll_failures = 0
         WHERE gmail_installation_id = $1 AND email_address = $2
        """,
        gmail_installation_id, email_address.lower(), history_id, expiration,
    )
    log.info(
        "gmail.watch.activated",
        gmail_installation_id=str(gmail_installation_id),
        email=email_address,
        expiration=expiration.isoformat() if expiration else None,
    )


async def renew_watch(
    tctx: TenantContext,
    gmail: GmailClient,
    *,
    gmail_installation_id: UUID,
    email_address: str,
    scope: str,
    topic_name: str,
) -> None:
    """Re-issue users.watch for a mailbox approaching expiration.

    Behaves the same as activate_watch — Gmail accepts repeated watch
    calls and returns a fresh expiration. The historyId on a successful
    renewal is typically unchanged unless a quiet mailbox has advanced.
    """
    await activate_watch(
        tctx, gmail,
        gmail_installation_id=gmail_installation_id,
        email_address=email_address,
        scope=scope,
        topic_name=topic_name,
    )


async def stop_watch(
    tctx: TenantContext,
    gmail: GmailClient,
    *,
    gmail_installation_id: UUID,
    email_address: str,
    scope: str,
    new_state: str = "paused",
) -> None:
    """Call users.stop and transition the row to `new_state`.

    Tolerates 404 from Google (already stopped). Errors on the Gmail
    call are swallowed for opt-out flows — the local row must reflect
    the user's intent regardless of upstream state.
    """
    if new_state not in ("paused", "opted_out", "errored"):
        raise ValueError(f"invalid new_state: {new_state!r}")
    try:
        await gmail.stop(user_email=email_address, scope=scope)
    except GoogleApiError as exc:
        log.warning(
            "gmail.watch.stop_failed",
            email=email_address,
            error=str(exc)[:200],
        )
    await tctx.execute(
        """
        UPDATE gmail_mailbox_watches
           SET state = $3,
               watch_expiration = NULL
         WHERE gmail_installation_id = $1 AND email_address = $2
        """,
        gmail_installation_id, email_address.lower(), new_state,
    )


__all__ = ["activate_watch", "renew_watch", "stop_watch", "upsert_pending_watch"]
