"""services/ingestion/handlers/gmail.py — Gmail message → ObservationDraft.

Channel: `gmail:` (note: namespace-prefixed; not the same as
`email:inbound` which handles Postmark/SendGrid webhooks).

Two surfaces in this module:

1. The classic handler registered via @register("gmail:"). Used by
   any caller that hands us a pre-shaped raw_payload through the
   standard ingest() entry point.

2. dispatch_gmail_message_resource(...) — the path used by the push
   handler and the history poller. Wraps:
       (a) thread canonicalization (RFC 5322)
       (b) ingest() through the registered handler
       (c) a post-insert UPDATE to stamp observations.thread_canonical_id
   The post-insert UPDATE is non-atomic with the insert but observation
   rows are useful even before the thread linkage column is written.

raw_payload shape (canonical envelope) — what callers MUST supply:

    {
        "message_resource": <Gmail API message resource>,
        "mailbox_email": "alice@acme.com",
        "scope_used": "gmail.metadata" | "gmail.readonly",
        "read_path": "push" | "poll",
        "gmail_installation_id": "...uuid...",
        "thread_canonical_id": "...uuid...",   # pre-resolved by dispatcher
    }
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import ValidationError

from services.ingestion.handlers import ObservationDraft, register

# Note: the following modules are DB-touching and used ONLY by
# `dispatch_gmail_message_resource` (the inline-path dispatcher used
# by push_handler / history_poller):
#   - `services.integrations.gmail.threading` (imports tenant_context)
#   - `lib.shared.tenant_context` (imports asyncpg)
#   - `services.ingestion.core` (imports asyncpg)
# They are deliberately NOT imported at module level — see M2.3
# Path B: the normalizer worker imports the handler registry, which
# imports every handler module. A top-level DB import here would
# propagate into the normalizer's process and break the static "no
# asyncpg in the worker's import graph" proof. Imported lazily
# inside the dispatcher function below.


log = structlog.get_logger("ingestion.handlers.gmail")


CHANNEL = "gmail:"
# Trust tier — promise-bearing content via DWD-authorized read. Treat
# similarly to slack:message which is also `attested_agent` per the
# CHANNEL_TRUST_MAP table at services/ingestion/handlers/__init__.py:41.
TRUST_TIER = "attested_agent"


# =====================================================================
# Handler — pure function from raw_payload → ObservationDraft.
# =====================================================================


def _headers_map(message_resource: dict[str, Any]) -> dict[str, str]:
    payload = message_resource.get("payload") or {}
    out: dict[str, str] = {}
    for h in payload.get("headers") or []:
        name = h.get("name")
        if isinstance(name, str):
            out[name.lower()] = h.get("value", "")
    return out


def _split_addrs(value: str | None) -> list[str]:
    if not value:
        return []
    # Cheap split: addresses are comma-separated. Strip names.
    out: list[str] = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        # "Alice <alice@x>" → "alice@x"
        if "<" in token and ">" in token:
            inner = token[token.index("<") + 1 : token.index(">")].strip()
            if inner:
                out.append(inner.lower())
                continue
        if "@" in token:
            out.append(token.lower())
    return out


def _split_refs(value: str | None) -> list[str]:
    if not value:
        return []
    return [
        token.strip().strip("<>").strip()
        for token in value.replace(",", " ").split()
        if token.strip()
    ]


def _internal_date_to_dt(value: Any) -> datetime:
    try:
        ms = int(value)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _extract_body(message_resource: dict[str, Any]) -> str | None:
    """Best-effort plain-text extraction. Only called for gmail.readonly."""
    import base64

    def _walk(part: dict[str, Any]) -> str | None:
        mime = part.get("mimeType", "")
        body = part.get("body") or {}
        data = body.get("data")
        if data and mime.startswith("text/"):
            try:
                raw = base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
                return raw.decode("utf-8", errors="replace")
            except (ValueError, UnicodeDecodeError):
                return None
        for sub in part.get("parts") or []:
            text = _walk(sub)
            if text:
                return text
        return None

    payload = message_resource.get("payload") or {}
    return _walk(payload)


def _content_text(payload: dict[str, Any]) -> str:
    """Human-legible representation. content_text is what the
    LLM/embedding sees; we include subject + snippet + (if available)
    a truncated body excerpt."""
    bits: list[str] = []
    subj = payload.get("subject")
    if subj:
        bits.append(f"Subject: {subj}")
    snippet = payload.get("snippet")
    if snippet:
        bits.append(snippet)
    body = payload.get("body")
    if body:
        bits.append(body[:4000])
    if not bits:
        bits.append("(empty gmail message)")
    return "\n".join(bits)


@register(CHANNEL)
async def handle_gmail(
    payload: dict[str, Any],
    request_headers: dict[str, str],
) -> ObservationDraft:
    """Convert a Gmail envelope into an ObservationDraft."""
    if not isinstance(payload, dict):
        raise ValidationError("gmail: payload must be object")

    message_resource = payload.get("message_resource")
    mailbox_email = payload.get("mailbox_email")
    scope_used = payload.get("scope_used")
    read_path = payload.get("read_path")
    gmail_installation_id = payload.get("gmail_installation_id")
    thread_canonical_id = payload.get("thread_canonical_id")

    if not isinstance(message_resource, dict):
        raise ValidationError("gmail: message_resource is required")
    if not mailbox_email or "@" not in mailbox_email:
        raise ValidationError("gmail: mailbox_email is required")
    if scope_used not in ("gmail.metadata", "gmail.readonly"):
        raise ValidationError(f"gmail: invalid scope_used: {scope_used!r}")
    if read_path not in ("push", "poll"):
        raise ValidationError(f"gmail: invalid read_path: {read_path!r}")
    if not gmail_installation_id:
        raise ValidationError("gmail: gmail_installation_id is required")

    headers = _headers_map(message_resource)
    message_id = headers.get("message-id") or ""
    message_id = message_id.strip().strip("<>").strip()
    if not message_id:
        raise ValidationError("gmail: Message-ID header is required")

    from_addr = (headers.get("from") or "").lower()
    # Parse just the address part of From for source_actor_ref.
    from_email = None
    if "<" in from_addr and ">" in from_addr:
        from_email = from_addr[from_addr.index("<") + 1 : from_addr.index(">")].strip()
    elif "@" in from_addr:
        from_email = from_addr.strip()

    body_text = (
        _extract_body(message_resource) if scope_used == "gmail.readonly" else None
    )

    content: dict[str, Any] = {
        "message_id": message_id,
        "thread_id_gmail": message_resource.get("threadId"),
        "from": headers.get("from"),
        "to": _split_addrs(headers.get("to")),
        "cc": _split_addrs(headers.get("cc")),
        "subject": headers.get("subject"),
        "date": headers.get("date"),
        "label_ids": message_resource.get("labelIds", []),
        "internal_date_ms": int(message_resource.get("internalDate") or 0),
        "size_estimate": message_resource.get("sizeEstimate"),
        "snippet": message_resource.get("snippet"),
        "body": body_text,
        "mailbox_email": mailbox_email,
        "scope_used": scope_used,
        "read_path": read_path,
        "gmail_installation_id": str(gmail_installation_id),
        # Surfaced in content so an inspector reading observations.content
        # can see the thread linkage even before the column UPDATE lands.
        "_gmail_thread_canonical_id": (
            str(thread_canonical_id) if thread_canonical_id else None
        ),
    }

    # entity hints: To/Cc emails and any URLs in the body.
    entities: list[dict[str, Any]] = []
    for addr in (content["to"] or []) + (content["cc"] or []):
        entities.append({"kind": "email", "value": addr})
    if from_email:
        entities.append({"kind": "email", "value": from_email})

    return ObservationDraft(
        source_channel=CHANNEL,
        content_text=_content_text(content),
        content=content,
        # external_id is namespaced by install so the same message_id
        # observed by two tenants stays distinct.
        external_id=f"gmail:{gmail_installation_id}:{message_id}",
        occurred_at=_internal_date_to_dt(message_resource.get("internalDate")),
        source_actor_ref=f"email:{from_email}" if from_email else None,
        entities_hint=entities,
        trust_tier=TRUST_TIER,  # type: ignore[arg-type]
        raw_payload={"gmail_message_id": message_id},
    )


# =====================================================================
# Dispatcher — called by push_handler / history_poller.
# =====================================================================


async def dispatch_gmail_message_resource(
    *,
    pool: Any,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
    scope_alias: str,
    message_resource: dict[str, Any],
    read_path: str,
) -> dict[str, Any] | None:
    """End-to-end: canonicalize thread → ingest → stamp thread column.

    Returns {"deduped": bool, "observation_id": str, "thread_canonical_id": str}
    or None if the message lacks a Message-ID and is dropped.
    """
    headers = _headers_map(message_resource)
    message_id = (headers.get("message-id") or "").strip().strip("<>").strip()
    if not message_id:
        log.info(
            "gmail.dispatch.missing_message_id",
            email=email_address,
        )
        return None

    in_reply_to = (headers.get("in-reply-to") or "").strip() or None
    references = _split_refs(headers.get("references"))
    subject = headers.get("subject")
    participants = (
        _split_addrs(headers.get("from"))
        + _split_addrs(headers.get("to"))
        + _split_addrs(headers.get("cc"))
    )

    # Lazy import — DB-touching modules MUST stay out of this module's
    # top-level import graph (see header note + M2.3 Path B proof).
    from lib.shared.tenant_context import tenant_transaction
    from services.ingestion.core import ingest as _ingest_core
    from services.integrations.gmail.threading import canonicalize_thread

    # --- step 1: canonicalize (its own tenant txn).
    async with tenant_transaction(tenant_id) as tctx:
        canon = await canonicalize_thread(
            tctx,
            gmail_installation_id=gmail_installation_id,
            message_id=message_id,
            in_reply_to=in_reply_to,
            references=references,
            subject=subject,
            participants=participants,
        )

    # --- step 2: ingest through the standard pipeline.
    raw_payload = {
        "message_resource": message_resource,
        "mailbox_email": email_address,
        "scope_used": scope_alias,
        "read_path": read_path,
        "gmail_installation_id": str(gmail_installation_id),
        "thread_canonical_id": str(canon.canonical_id),
    }
    result = await _ingest_core(
        CHANNEL,
        raw_payload,
        pool=pool,
        tenant_id=tenant_id,
        enqueue_trigger=True,
    )

    # --- step 3: stamp the column. Idempotent UPDATE.
    async with tenant_transaction(tenant_id) as tctx:
        await tctx.execute(
            """
            UPDATE observations
               SET thread_canonical_id = $2
             WHERE id = $1 AND tenant_id = $3
               AND (thread_canonical_id IS NULL OR thread_canonical_id = $2)
            """,
            result.observation.id, canon.canonical_id, tenant_id,
        )

    return {
        "deduped": result.deduped,
        "observation_id": str(result.observation.id),
        "thread_canonical_id": str(canon.canonical_id),
        "is_new_thread": canon.is_new_thread,
        "orphan": canon.orphan,
    }


__all__ = ["CHANNEL", "TRUST_TIER", "dispatch_gmail_message_resource", "handle_gmail"]
