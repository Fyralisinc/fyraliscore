"""Canonical records for Instagram Messaging.

The webhook payload does not include a Graph Conversation ID. A webhook item is
therefore keyed by a stable local thread key (business account + Instagram-
scoped participant), while history records add ``provider_conversation_id``
after the Conversations API discovers it. Every ingress path emits this shape
before the shared handler runs.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from lib.shared.errors import ValidationError


CHANNEL = "instagram:message"
MESSAGE_RECORD_TYPE = "message"
STATUS_RECORD_TYPE = "status"
EVENT_RECORD_TYPE = "event"


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _obj(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _epoch_to_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    if raw > 10_000_000_000:
        raw /= 1000.0
    try:
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    epoch = _epoch_to_dt(value)
    if epoch is not None:
        return epoch
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _stable_fingerprint(*parts: Any) -> str:
    encoded = json.dumps(parts, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.blake2b(encoded.encode("utf-8"), digest_size=12).hexdigest()


def _message_id(message: dict[str, Any]) -> str | None:
    return _str_or_none(message.get("mid") or message.get("id"))


def _attachments(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        value = value.get("data")
    return [item for item in _list(value) if isinstance(item, dict)]


def business_endpoint_ids(*values: str | None) -> frozenset[str]:
    """Return every provider identifier that represents the business endpoint.

    Meta can use a delivery-scoped Instagram id in webhook and Conversation
    participants while account discovery yields the canonical professional
    account id. These identifiers all refer to the tenant's business, never a
    customer, but observations continue to use the canonical id.
    """
    return frozenset(value for raw in values if (value := _str_or_none(raw)))


def _business_sender(
    sender_id: str | None,
    *,
    ig_business_account_id: str,
    page_id: str | None,
    entry_id: str | None,
) -> bool:
    return sender_id is not None and sender_id in business_endpoint_ids(
        ig_business_account_id,
        page_id,
        entry_id,
    )


def _thread_key(*, ig_business_account_id: str, customer_id: str | None) -> str:
    customer_id = customer_id or "unknown"
    return f"{ig_business_account_id}:{customer_id}"


def _event_text(event_type: str, event: dict[str, Any]) -> str:
    message = _obj(event.get("message"))
    text = _str_or_none(message.get("text") or message.get("message"))
    if text:
        return text
    attachments = _attachments(message.get("attachments"))
    if attachments:
        types = [str(item.get("type") or "attachment") for item in attachments]
        return "[" + ", ".join(types) + "]"
    postback = _obj(event.get("postback"))
    if postback:
        return _str_or_none(postback.get("title") or postback.get("payload")) or "[postback]"
    reaction = _obj(event.get("reaction"))
    if reaction:
        return f"[reaction] {_str_or_none(reaction.get('emoji') or reaction.get('reaction')) or ''}".strip()
    edit = _obj(event.get("message_edit"))
    if edit:
        return _str_or_none(edit.get("text")) or "[message edited]"
    return f"[{event_type}]"


def _event_record(
    *,
    event_type: str,
    ig_business_account_id: str,
    page_id: str | None,
    entry_id: str | None,
    sender_id: str | None,
    recipient_id: str | None,
    timestamp: Any,
    event: dict[str, Any],
    event_id: str,
    event_version: str,
    state_change: bool,
    provider_conversation_id: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    occurred_at = _parse_time(timestamp) or datetime.now(tz=timezone.utc)
    business_sender = _business_sender(
        sender_id,
        ig_business_account_id=ig_business_account_id,
        page_id=page_id,
        entry_id=entry_id,
    )
    customer_id = recipient_id if business_sender else sender_id
    direction = "outbound" if business_sender else "inbound"
    record = {
        "_fyralis_record_type": STATUS_RECORD_TYPE if state_change else EVENT_RECORD_TYPE,
        "event_type": event_type,
        "ig_business_account_id": ig_business_account_id,
        "page_id": page_id,
        "entry_id": entry_id,
        "thread_key": _thread_key(
            ig_business_account_id=ig_business_account_id,
            customer_id=customer_id,
        ),
        "provider_conversation_id": provider_conversation_id,
        "conversation_id": _thread_key(
            ig_business_account_id=ig_business_account_id,
            customer_id=customer_id,
        ),
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "customer_id": customer_id,
        "direction": direction,
        "message_id": event_id,
        "event_id": event_id,
        "event_version": event_version,
        "state": event_type if state_change else None,
        "watermark": event_version if state_change else None,
        "timestamp": timestamp,
        "occurred_at": occurred_at.isoformat(),
        "text": _event_text(event_type, event),
    }
    if extra:
        record.update(extra)
    # The S3 raw tier receives this canonical record. Retain the unmodified
    # provider event there for forensic replay without teaching downstream
    # observations to depend on webhook-shaped fields.
    record["provider_event"] = event
    return record


def build_webhook_record(
    event: dict[str, Any],
    *,
    ig_business_account_id: str | None = None,
    page_id: str | None = None,
    entry_id: str | None = None,
) -> dict[str, Any] | None:
    """Create one canonical record from a single Meta messaging event."""
    if not isinstance(event, dict):
        return None
    sender_id = _str_or_none(_obj(event.get("sender")).get("id"))
    recipient_id = _str_or_none(_obj(event.get("recipient")).get("id"))
    business_id = _str_or_none(ig_business_account_id) or _str_or_none(entry_id)
    if not business_id:
        return None

    timestamp = event.get("timestamp")
    message = _obj(event.get("message"))
    postback = _obj(event.get("postback"))
    reaction = _obj(event.get("reaction"))
    read = _obj(event.get("read"))
    delivery = _obj(event.get("delivery"))
    edit = _obj(event.get("message_edit"))
    provider_conversation_id = _str_or_none(
        _obj(event.get("conversation")).get("id") or event.get("conversation_id")
    )

    if read or delivery:
        status = read or delivery
        mids = _list(status.get("mids"))
        message_id = _str_or_none(status.get("mid") or (mids[0] if mids else None))
        event_type = "read" if read else "delivery"
        version = _str_or_none(status.get("watermark") or timestamp)
        if not message_id:
            message_id = f"unknown:{_stable_fingerprint(event_type, business_id, sender_id, recipient_id, timestamp)}"
        if not version:
            version = str(int((_parse_time(timestamp) or datetime.now(timezone.utc)).timestamp() * 1000))
        return _event_record(
            event_type=event_type,
            ig_business_account_id=business_id,
            page_id=page_id,
            entry_id=entry_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            timestamp=timestamp,
            event=event,
            event_id=message_id,
            event_version=version,
            state_change=True,
            provider_conversation_id=provider_conversation_id,
            extra={"status": status},
        )

    if edit:
        message_id = _str_or_none(edit.get("mid"))
        if not message_id:
            return None
        version = _str_or_none(edit.get("num_edit") or timestamp)
        if not version:
            version = _stable_fingerprint("edit", message_id, edit)
        return _event_record(
            event_type="message_edit",
            ig_business_account_id=business_id,
            page_id=page_id,
            entry_id=entry_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            timestamp=timestamp,
            event=event,
            event_id=message_id,
            event_version=version,
            state_change=True,
            provider_conversation_id=provider_conversation_id,
            extra={"message_edit": edit},
        )

    if reaction:
        target_mid = _str_or_none(reaction.get("mid"))
        if not target_mid:
            return None
        action = _str_or_none(reaction.get("action")) or "react"
        reaction_value = _str_or_none(reaction.get("reaction") or reaction.get("emoji")) or "unknown"
        version = _str_or_none(timestamp) or _stable_fingerprint(
            "reaction", target_mid, sender_id, action, reaction_value, reaction,
        )
        record = _event_record(
            event_type="reaction",
            ig_business_account_id=business_id,
            page_id=page_id,
            entry_id=entry_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            timestamp=timestamp,
            event=event,
            event_id=target_mid,
            event_version=version,
            state_change=True,
            provider_conversation_id=provider_conversation_id,
            extra={"reaction": reaction, "target_message_id": target_mid},
        )
        # Keep the external-id contract anchored on Meta's message id while
        # retaining who/action/value in the status state for distinct reactions.
        record["state"] = f"reaction:{sender_id or 'unknown'}:{action}:{reaction_value}"
        return record

    if postback:
        message_id = _str_or_none(postback.get("mid"))
        if not message_id:
            message_id = f"postback:{_stable_fingerprint(business_id, sender_id, timestamp, postback)}"
        return _event_record(
            event_type="postback",
            ig_business_account_id=business_id,
            page_id=page_id,
            entry_id=entry_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            timestamp=timestamp,
            event=event,
            event_id=message_id,
            event_version=_str_or_none(timestamp) or message_id,
            state_change=False,
            provider_conversation_id=provider_conversation_id,
            extra={"postback": postback},
        )

    if not message:
        return None

    message_id = _message_id(message)
    if not message_id:
        return None
    if bool(message.get("is_deleted")):
        version = _str_or_none(timestamp) or _stable_fingerprint("deleted", message_id, message)
        return _event_record(
            event_type="deleted",
            ig_business_account_id=business_id,
            page_id=page_id,
            entry_id=entry_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            timestamp=timestamp,
            event=event,
            event_id=message_id,
            event_version=version,
            state_change=True,
            provider_conversation_id=provider_conversation_id,
            extra={"message": message},
        )

    occurred_at = _parse_time(timestamp) or datetime.now(tz=timezone.utc)
    business_sender = _business_sender(
        sender_id,
        ig_business_account_id=business_id,
        page_id=page_id,
        entry_id=entry_id,
    )
    direction = "outbound" if bool(message.get("is_echo")) or business_sender else "inbound"
    customer_id = recipient_id if direction == "outbound" else sender_id
    reply_to = _obj(message.get("reply_to"))
    story = _obj(message.get("story")) or _obj(reply_to.get("story"))
    referral = _obj(event.get("referral")) or _obj(message.get("referral"))
    return {
        "_fyralis_record_type": MESSAGE_RECORD_TYPE,
        "event_type": "message",
        "ig_business_account_id": business_id,
        "page_id": page_id,
        "entry_id": entry_id,
        "thread_key": _thread_key(
            ig_business_account_id=business_id,
            customer_id=customer_id,
        ),
        "provider_conversation_id": provider_conversation_id,
        "conversation_id": _thread_key(
            ig_business_account_id=business_id,
            customer_id=customer_id,
        ),
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "customer_id": customer_id,
        "direction": direction,
        "message_id": message_id,
        "timestamp": timestamp,
        "occurred_at": occurred_at.isoformat(),
        "text": _event_text("message", event),
        "message": message,
        "attachments": _attachments(message.get("attachments")),
        "quick_reply": _obj(message.get("quick_reply")) or None,
        "reply_to": reply_to or None,
        "story": story or None,
        "referral": referral or None,
        "provider_event": event,
    }


def iter_webhook_records(
    payload: dict[str, Any],
    *,
    default_ig_business_account_id: str | None = None,
    page_id: str | None = None,
) -> list[dict[str, Any]]:
    """Fan out a Meta webhook delivery into canonical per-event records."""
    records: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return records
    for entry in _list(payload.get("entry")):
        if not isinstance(entry, dict):
            continue
        entry_id = _str_or_none(entry.get("id"))
        business_id = default_ig_business_account_id or entry_id
        for event in _iter_entry_events(entry):
            record = build_webhook_record(
                event,
                ig_business_account_id=business_id,
                page_id=page_id,
                entry_id=entry_id,
            )
            if record is not None:
                records.append(record)
    return records


def build_history_record(
    message: dict[str, Any],
    *,
    ig_business_account_id: str,
    page_id: str | None,
    conversation_id: str,
    webhook_delivery_account_id: str | None = None,
    participant_id: str | None = None,
    participant_username: str | None = None,
    participant_display_name: str | None = None,
) -> dict[str, Any]:
    """Build the same immutable message record from Conversations API history."""
    sender = _obj(message.get("from"))
    recipients = _list(_obj(message.get("to")).get("data"))
    sender_id = _str_or_none(sender.get("id"))
    business_ids = business_endpoint_ids(
        ig_business_account_id,
        page_id,
        webhook_delivery_account_id,
    )
    recipient_ids = [
        candidate
        for item in recipients
        if isinstance(item, dict)
        for candidate in (_str_or_none(item.get("id")),)
        if candidate
    ]
    recipient_id = next(iter(recipient_ids), None)
    direction = "outbound" if sender_id in business_ids else "inbound"
    customer_id = (
        next((candidate for candidate in recipient_ids if candidate not in business_ids), None)
        if direction == "outbound"
        else sender_id
    )
    customer_id = customer_id or participant_id
    message_id = _message_id(message)
    if not message_id:
        raise ValidationError("instagram history message missing id", channel=CHANNEL)
    occurred_at = _parse_time(message.get("created_time"))
    if occurred_at is None:
        raise ValidationError("instagram history message has no timestamp", channel=CHANNEL)
    thread_key = f"{ig_business_account_id}:{customer_id or 'unknown'}"
    text = _str_or_none(message.get("message") or message.get("text"))
    attachments = _attachments(message.get("attachments"))
    if not text:
        text = "[" + ", ".join(str(item.get("type") or "attachment") for item in attachments) + "]" if attachments else "[message]"
    return {
        "_fyralis_record_type": MESSAGE_RECORD_TYPE,
        "event_type": "message",
        "ig_business_account_id": ig_business_account_id,
        "page_id": page_id,
        "thread_key": thread_key,
        "provider_conversation_id": conversation_id,
        "conversation_id": thread_key,
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "customer_id": customer_id,
        "participant_username": participant_username,
        "participant_display_name": participant_display_name,
        "direction": direction,
        "message_id": message_id,
        "timestamp": message.get("created_time"),
        "occurred_at": occurred_at.isoformat(),
        "text": text,
        "message": message,
        "attachments": attachments,
        "provider_message": message,
    }


@dataclass(frozen=True)
class ParsedInstagramRecord:
    record_type: str
    event_type: str
    ig_business_account_id: str
    page_id: str | None
    conversation_id: str
    thread_key: str
    provider_conversation_id: str | None
    message_id: str
    occurred_at: datetime
    text: str
    direction: str | None
    sender_id: str | None
    recipient_id: str | None
    customer_id: str | None
    participant_username: str | None
    participant_display_name: str | None
    state: str | None
    watermark: str | None
    external_id: str


def parse_record(record: dict[str, Any]) -> ParsedInstagramRecord:
    if not isinstance(record, dict):
        raise ValidationError("instagram record must be an object", channel=CHANNEL)
    record_type = _str_or_none(record.get("_fyralis_record_type")) or MESSAGE_RECORD_TYPE
    if record_type not in {MESSAGE_RECORD_TYPE, STATUS_RECORD_TYPE, EVENT_RECORD_TYPE}:
        raise ValidationError(f"instagram record has unknown type {record_type!r}", channel=CHANNEL)
    business_id = _str_or_none(record.get("ig_business_account_id"))
    message_id = _str_or_none(record.get("message_id") or record.get("event_id"))
    occurred_at = _parse_time(record.get("occurred_at") or record.get("timestamp"))
    if not business_id or not message_id or occurred_at is None:
        raise ValidationError("instagram record missing identity or timestamp", channel=CHANNEL)
    event_type = _str_or_none(record.get("event_type")) or record_type
    thread_key = _str_or_none(record.get("thread_key") or record.get("conversation_id"))
    if not thread_key:
        thread_key = f"{business_id}:{_str_or_none(record.get('customer_id')) or 'unknown'}"
    state = _str_or_none(record.get("state"))
    watermark = _str_or_none(record.get("watermark"))

    # Lazy import avoids importing the handler registry when this canonical
    # module is used by a standalone fetcher or diagnostic tool.
    from services.ingest.ingestion import idempotency

    if record_type == MESSAGE_RECORD_TYPE:
        external_id = idempotency.instagram_message(business_id, message_id)
    elif record_type == STATUS_RECORD_TYPE:
        state = state or event_type
        watermark = watermark or str(int(occurred_at.timestamp() * 1000))
        external_id = idempotency.instagram_status(business_id, message_id, state, watermark)
    else:
        version = _str_or_none(record.get("event_version")) or str(int(occurred_at.timestamp() * 1000))
        external_id = idempotency.instagram_event(business_id, event_type, message_id, version)

    return ParsedInstagramRecord(
        record_type=record_type,
        event_type=event_type,
        ig_business_account_id=business_id,
        page_id=_str_or_none(record.get("page_id")),
        conversation_id=thread_key,
        thread_key=thread_key,
        provider_conversation_id=_str_or_none(record.get("provider_conversation_id")),
        message_id=message_id,
        occurred_at=occurred_at,
        text=_str_or_none(record.get("text")) or f"[{event_type}]",
        direction=_str_or_none(record.get("direction")),
        sender_id=_str_or_none(record.get("sender_id")),
        recipient_id=_str_or_none(record.get("recipient_id")),
        customer_id=_str_or_none(record.get("customer_id")),
        participant_username=_str_or_none(record.get("participant_username")),
        participant_display_name=_str_or_none(record.get("participant_display_name")),
        state=state,
        watermark=watermark,
        external_id=external_id,
    )


def first_business_account_id(payload: dict[str, Any]) -> str | None:
    """Extract Meta's entry.id route key before signature verification."""
    if not isinstance(payload, dict):
        return None
    for entry in _list(payload.get("entry")):
        if not isinstance(entry, dict):
            continue
        entry_id = _str_or_none(entry.get("id"))
        if entry_id:
            return entry_id
        for event in _iter_entry_events(entry):
            recipient_id = _str_or_none(_obj(event.get("recipient")).get("id"))
            if recipient_id:
                return recipient_id
    return None


def _iter_entry_events(entry: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for event in _list(entry.get("messaging")):
        if isinstance(event, dict):
            yield event
    for change in _list(entry.get("changes")):
        if not isinstance(change, dict):
            continue
        value = _obj(change.get("value"))
        for event in _list(value.get("messaging")):
            if isinstance(event, dict):
                yield event
        if _obj(value.get("sender")) and _obj(value.get("recipient")):
            yield value


__all__ = [
    "CHANNEL",
    "EVENT_RECORD_TYPE",
    "MESSAGE_RECORD_TYPE",
    "STATUS_RECORD_TYPE",
    "ParsedInstagramRecord",
    "build_history_record",
    "build_webhook_record",
    "business_endpoint_ids",
    "first_business_account_id",
    "iter_webhook_records",
    "parse_record",
]
