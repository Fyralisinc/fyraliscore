"""services/integrations/gmail/push_handler.py — Pub/Sub push business logic.

Called by the gateway webhook at /webhooks/gmail/pubsub AFTER the OIDC
token has been verified upstream. This function is responsible for:

  1. Mapping subscription_name → (tenant_id, gmail_installation_id).
  2. Inside a tenant-bound transaction, finding the matching
     gmail_mailbox_watches row, calling users.history.list to fetch
     deltas, and dispatching each new message through the Gmail ingest
     handler.
  3. Advancing the mailbox's history_id and stamping last_push_at.

Idempotency: a re-delivered push (Google retries) is safe — the ingest
path dedups on observations.UNIQUE and on gmail_thread_members.PK.
"""
from __future__ import annotations

import base64
import json
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import CompanyOSError
from lib.shared.tenant_context import tenant_transaction

from services.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
    GoogleHttpClient,
    GoogleRateLimited,
)
from services.integrations.gmail.dwd import get_minter


log = structlog.get_logger("integrations.gmail.push_handler")


class GmailPushError(CompanyOSError):
    default_code = "gmail_push_error"


def decode_pubsub_message(envelope: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Parse a Pub/Sub push envelope. Returns (subscription_name, decoded_data)."""
    if not isinstance(envelope, dict):
        raise GmailPushError("push envelope must be JSON object")
    subscription_name = envelope.get("subscription") or (
        (envelope.get("message") or {}).get("attributes", {}) or {}
    ).get("subscription")
    if not subscription_name:
        raise GmailPushError("push envelope missing subscription field")
    msg = envelope.get("message") or {}
    raw_b64 = msg.get("data")
    if not raw_b64:
        return subscription_name, {}
    try:
        raw = base64.b64decode(raw_b64)
        decoded = json.loads(raw)
    except (ValueError, json.JSONDecodeError) as exc:
        raise GmailPushError(f"invalid push data: {exc}") from exc
    if not isinstance(decoded, dict):
        raise GmailPushError("push data must be JSON object")
    return subscription_name, decoded


async def handle_push(
    *,
    pool: Any,  # asyncpg.Pool — typed loose to avoid import cycle
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Top-level entry. Returns a small status dict for the webhook to
    return 200 with. Errors are swallowed for non-2xx-worthy reasons
    (transient API failures); only programmer errors raise."""
    subscription_name, decoded = decode_pubsub_message(envelope)
    email_address = decoded.get("emailAddress")
    history_id_seen = decoded.get("historyId")
    if not email_address or history_id_seen is None:
        # Empty/initial notifications sometimes carry no fields; ack and skip.
        return {"status": "skipped", "reason": "empty_notification"}

    # Look up the tenant the subscription belongs to. This query is
    # intentionally cross-tenant — we have no bound tenant yet.
    async with pool.acquire() as conn:
        topic_row = await conn.fetchrow(
            """
            SELECT tenant_id, gmail_installation_id
              FROM gmail_pubsub_topics
             WHERE subscription_name = $1 AND teardown_at IS NULL
            """,
            subscription_name,
        )
    if topic_row is None:
        log.warning(
            "gmail.push.unknown_subscription",
            subscription_name=subscription_name,
        )
        return {"status": "skipped", "reason": "unknown_subscription"}

    tenant_id: UUID = topic_row["tenant_id"]
    gmail_installation_id: UUID = topic_row["gmail_installation_id"]

    # Per M2.2 work order: the Gmail Pub/Sub endpoint needs the
    # resolved tenant_id to fire the shadow write of the *notification
    # payload* (not the fetched messages — that's M6's scope). The
    # subscription→tenant lookup above is the cheapest place to
    # surface it; we attach it to every post-resolved return path
    # so the endpoint can do `result.get("tenant_id")` without a
    # second DB query.
    try:
        inner = await _drain_history(
            pool=pool,
            tenant_id=tenant_id,
            gmail_installation_id=gmail_installation_id,
            email_address=email_address.lower(),
        )
        # _drain_history returns its own status dict; augment with
        # tenant_id without overwriting any existing key.
        return {**inner, "tenant_id": str(tenant_id)}
    except GoogleRateLimited as exc:
        # Return 200 — Pub/Sub will redeliver eventually; the poller is
        # the safety net. Avoid a retry storm.
        log.info(
            "gmail.push.rate_limited",
            email=email_address,
            retry_after_s=getattr(exc, "kwargs", {}).get("retry_after_s"),
        )
        return {"status": "rate_limited", "tenant_id": str(tenant_id)}
    except GoogleApiError as exc:
        log.warning("gmail.push.google_error", email=email_address, error=str(exc)[:200])
        return {"status": "google_error", "tenant_id": str(tenant_id)}


async def _drain_history(
    *,
    pool: Any,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
) -> dict[str, Any]:
    # Local import to avoid an import cycle (handlers/gmail.py imports
    # threading which imports tenant_context; push_handler is also
    # referenced indirectly during handler registration).
    from services.integrations.gmail.fetcher import drain_mailbox_history

    minter = get_minter()
    async with GoogleHttpClient(minter) as http:
        gmail = GmailClient(http)
        result = await drain_mailbox_history(
            pool=pool,
            gmail=gmail,
            tenant_id=tenant_id,
            gmail_installation_id=gmail_installation_id,
            email_address=email_address,
            read_path="push",
        )
    return result


__all__ = ["GmailPushError", "decode_pubsub_message", "handle_push"]
