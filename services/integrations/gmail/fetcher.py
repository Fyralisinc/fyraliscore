"""services/integrations/gmail/fetcher.py — shared history-drain + dispatch.

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

from typing import Any
from uuid import UUID

import structlog

from lib.shared.tenant_context import bind_tenant, tenant_transaction

from services.integrations.gmail.audit import write_read_audit
from services.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
)


log = structlog.get_logger("integrations.gmail.fetcher")


SCOPE_ALIAS = {
    "gmail.metadata": GMAIL_METADATA_SCOPE,
    "gmail.readonly": GMAIL_READONLY_SCOPE,
}


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

    from services.ingestion.shadow_write import shadow_write_raw

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
    (`s3_raw_client` + `kafka_producer` + `tenant_flags`) are wired AND
    `ingestion.kafka_path_enabled=TRUE` for the tenant, each fetched
    message resource is published to `ingestion.raw` (ingress_kind="poll")
    instead of ingested inline. The writer pool then produces the
    observation via M5.2's full-mode path. default=False keeps unflagged
    tenants on the inline path (the N1 invariant). On a per-message publish
    failure, that message falls back to inline dispatch (never dropped).
    """
    if read_path not in ("push", "poll"):
        raise ValueError(f"read_path must be 'push' or 'poll', got {read_path!r}")

    # Resolve cutover mode once per drain (the flag read is cached).
    cutover_enabled = False
    if (
        s3_raw_client is not None
        and kafka_producer is not None
        and tenant_flags is not None
    ):
        from services.ingestion.feature_flags import KAFKA_PATH_ENABLED
        cutover_enabled = await tenant_flags.get_bool(
            tenant_id, KAFKA_PATH_ENABLED, default=False,
        )

    # --- step 1: load watch row + install scope (single tenant txn).
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                watch_row = await tctx.fetchrow(
                    """
                    SELECT mw.id, mw.history_id, mw.state, gi.scope
                      FROM gmail_mailbox_watches mw
                      JOIN gmail_installations gi
                        ON gi.id = mw.gmail_installation_id
                     WHERE mw.gmail_installation_id = $1
                       AND mw.email_address = $2
                    """,
                    gmail_installation_id, email_address.lower(),
                )
    if watch_row is None:
        return {"status": "skipped", "reason": "no_watch_row"}
    if watch_row["state"] in ("paused", "opted_out"):
        return {"status": "skipped", "reason": "watch_inactive", "state": watch_row["state"]}
    if not watch_row["history_id"]:
        return {"status": "skipped", "reason": "no_history_bookmark"}

    scope_alias = watch_row["scope"]
    scope_long = SCOPE_ALIAS[scope_alias]

    # --- step 2: page history.list, collecting new messageIds.
    new_message_ids: list[str] = []
    new_history_id: str | None = watch_row["history_id"]
    page_token: str | None = None
    while True:
        page = await gmail.history_list(
            user_email=email_address,
            scope=scope_long,
            start_history_id=watch_row["history_id"],
            page_token=page_token,
        )
        for entry in page.get("history") or []:
            for added in entry.get("messagesAdded") or []:
                msg = (added or {}).get("message") or {}
                msg_id = msg.get("id")
                if msg_id:
                    new_message_ids.append(msg_id)
        # Gmail's historyId on the response is the canonical "you are
        # caught up through this point" bookmark.
        latest = page.get("historyId")
        if latest:
            new_history_id = str(latest)
        page_token = page.get("nextPageToken")
        if not page_token:
            break

    # --- step 3: for each new message: get + ingest.
    ingested = 0
    deduped = 0
    if new_message_ids:
        # Local import to avoid module-load cycles via the handler registry.
        from services.ingestion.handlers.gmail import dispatch_gmail_message_resource

        for msg_id in new_message_ids:
            try:
                resource = await gmail.get_message(
                    user_email=email_address, scope=scope_long, message_id=msg_id,
                )
            except GoogleApiError as exc:
                log.warning(
                    "gmail.fetcher.get_message_failed",
                    email=email_address, message_id=msg_id, error=str(exc)[:200],
                )
                continue

            # ---- Cutover branch: publish to ingestion.raw, skip inline ----
            if cutover_enabled:
                published = await _publish_gmail_message_raw(
                    s3_raw_client=s3_raw_client,
                    kafka_producer=kafka_producer,
                    tenant_id=tenant_id,
                    gmail_installation_id=gmail_installation_id,
                    email_address=email_address,
                    scope_alias=scope_alias,
                    message_resource=resource,
                    read_path=read_path,
                )
                if published:
                    # The observation is produced downstream by the writer;
                    # count it as ingested for the drain's return shape.
                    ingested += 1
                    # Still record the read audit below (we DID read it).
                    async with tenant_transaction(tenant_id) as tctx:
                        await write_read_audit(
                            tctx,
                            gmail_installation_id=gmail_installation_id,
                            email_address=email_address,
                            message_id=msg_id,
                            scope_used=scope_alias,
                            read_path=read_path,
                        )
                    continue
                # Publish failed → graceful fallback to inline dispatch
                # (the message must not be dropped). NOT gate-relaxation.
                log.warning(
                    "gmail.fetcher.kafka_path_fallback_to_inline",
                    email=email_address, message_id=msg_id,
                )

            try:
                result = await dispatch_gmail_message_resource(
                    pool=pool,
                    tenant_id=tenant_id,
                    gmail_installation_id=gmail_installation_id,
                    email_address=email_address,
                    scope_alias=scope_alias,
                    message_resource=resource,
                    read_path=read_path,
                )
            except Exception as exc:  # noqa: BLE001 — handler errors should not stop the drain
                log.warning(
                    "gmail.fetcher.ingest_failed",
                    email=email_address, message_id=msg_id, error=str(exc)[:200],
                )
                continue

            if result is None:
                continue
            if result.get("deduped"):
                deduped += 1
            else:
                ingested += 1

            # Append the per-message read audit (inside its own short txn).
            async with tenant_transaction(tenant_id) as tctx:
                await write_read_audit(
                    tctx,
                    gmail_installation_id=gmail_installation_id,
                    email_address=email_address,
                    message_id=msg_id,
                    scope_used=scope_alias,
                    read_path=read_path,
                )

    # --- step 4: advance bookmark + timestamp.
    async with tenant_transaction(tenant_id) as tctx:
        if read_path == "push":
            await tctx.execute(
                """
                UPDATE gmail_mailbox_watches
                   SET history_id = COALESCE($3, history_id),
                       last_push_at = now(),
                       consecutive_poll_failures = 0,
                       last_error = NULL
                 WHERE gmail_installation_id = $1
                   AND email_address = $2
                """,
                gmail_installation_id, email_address.lower(), new_history_id,
            )
        else:
            await tctx.execute(
                """
                UPDATE gmail_mailbox_watches
                   SET history_id = COALESCE($3, history_id),
                       last_poll_at = now(),
                       consecutive_poll_failures = 0,
                       last_error = NULL
                 WHERE gmail_installation_id = $1
                   AND email_address = $2
                """,
                gmail_installation_id, email_address.lower(), new_history_id,
            )

    return {
        "status": "ok",
        "ingested": ingested,
        "deduped": deduped,
        "messages_seen": len(new_message_ids),
        "history_id": new_history_id,
    }


__all__ = ["drain_mailbox_history"]
