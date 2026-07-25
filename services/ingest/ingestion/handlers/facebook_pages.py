"""Facebook Page / Messenger message handler."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError
from services.ingest.ingestion import idempotency
from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
)


_CHANNEL = "facebook_pages:message"
_TRUST = CHANNEL_TRUST_MAP[_CHANNEL]


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
    if isinstance(value, str) and value:
        if value.isdigit():
            raw = int(value)
            return datetime.fromtimestamp(
                raw / 1000.0 if raw > 10_000_000_000 else raw,
                tz=timezone.utc,
            )
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
            except ValueError as exc:
                raise ValidationError(
                    f"facebook_pages timestamp invalid: {value!r}",
                    field="timestamp",
                ) from exc
    raise ValidationError("facebook_pages timestamp is required", field="timestamp")


def _nested_id(value: Any) -> str | None:
    if isinstance(value, dict):
        raw = value.get("id")
        return raw if isinstance(raw, str) and raw else None
    return None


def _message_obj(payload: dict[str, Any]) -> dict[str, Any]:
    msg = payload.get("message")
    if isinstance(msg, dict):
        return msg
    messaging = payload.get("messaging")
    if isinstance(messaging, dict) and isinstance(messaging.get("message"), dict):
        return messaging["message"]
    if isinstance(payload.get("postback"), dict):
        return {}
    if isinstance(messaging, dict) and isinstance(messaging.get("postback"), dict):
        return {}
    if isinstance(payload.get("id"), str):
        return payload
    raise ValidationError("facebook_pages message object missing", channel=_CHANNEL)


def _message_id(payload: dict[str, Any], msg: dict[str, Any]) -> str:
    for key in ("mid", "id", "message_id"):
        value = msg.get(key) or payload.get(key)
        if isinstance(value, str) and value:
            return value
    postback = payload.get("postback")
    messaging = payload.get("messaging")
    if not isinstance(postback, dict) and isinstance(messaging, dict):
        candidate = messaging.get("postback")
        postback = candidate if isinstance(candidate, dict) else None
    if isinstance(postback, dict):
        payload_value = postback.get("payload") or postback.get("title") or "postback"
        sender = _nested_id(payload.get("sender"))
        if sender is None and isinstance(messaging, dict):
            sender = _nested_id(messaging.get("sender"))
        timestamp = payload.get("timestamp")
        if timestamp is None and isinstance(messaging, dict):
            timestamp = messaging.get("timestamp")
        seed = f"{sender}:{timestamp}:{payload_value}"
        return "postback:" + hashlib.blake2b(seed.encode("utf-8"), digest_size=12).hexdigest()
    raise ValidationError("facebook_pages message missing id", channel=_CHANNEL)


def _sender_id(payload: dict[str, Any], msg: dict[str, Any]) -> str | None:
    sender = _nested_id(payload.get("sender"))
    if sender:
        return sender
    messaging = payload.get("messaging")
    if isinstance(messaging, dict):
        sender = _nested_id(messaging.get("sender"))
        if sender:
            return sender
    from_obj = msg.get("from")
    if isinstance(from_obj, dict):
        return _nested_id(from_obj)
    return None


def _recipient_ids(payload: dict[str, Any], msg: dict[str, Any]) -> list[str]:
    recipient = _nested_id(payload.get("recipient"))
    if recipient:
        return [recipient]
    messaging = payload.get("messaging")
    if isinstance(messaging, dict):
        recipient = _nested_id(messaging.get("recipient"))
        if recipient:
            return [recipient]
    to = msg.get("to")
    data = to.get("data") if isinstance(to, dict) else to
    ids: list[str] = []
    for item in data if isinstance(data, list) else []:
        rid = _nested_id(item)
        if rid:
            ids.append(rid)
    return ids


def _message_text(payload: dict[str, Any], msg: dict[str, Any]) -> str:
    for key in ("text", "message"):
        value = msg.get(key)
        if isinstance(value, str) and value:
            return value
    postback = payload.get("postback")
    messaging = payload.get("messaging")
    if not isinstance(postback, dict) and isinstance(messaging, dict):
        candidate = messaging.get("postback")
        postback = candidate if isinstance(candidate, dict) else None
    if isinstance(postback, dict):
        title = postback.get("title")
        postback_payload = postback.get("payload")
        if title and postback_payload:
            return f"{title}: {postback_payload}"
        if title or postback_payload:
            return str(title or postback_payload)
    attachments = msg.get("attachments")
    if isinstance(attachments, dict):
        data = attachments.get("data")
        if isinstance(data, list) and data:
            return f"[attachment] {len(data)} item(s)"
    elif isinstance(attachments, list) and attachments:
        return f"[attachment] {len(attachments)} item(s)"
    return "[facebook page message]"


def _occurred_at(payload: dict[str, Any], msg: dict[str, Any]) -> datetime:
    messaging = payload.get("messaging")
    value = payload.get("timestamp")
    if value is None and isinstance(messaging, dict):
        value = messaging.get("timestamp")
    value = value or msg.get("created_time") or payload.get("created_time")
    return _parse_ts(value)


async def handle_facebook_pages(
    payload: dict[str, Any],
    headers: dict[str, str],
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("facebook_pages payload must be object", channel=_CHANNEL)
    page_id = payload.get("page_id") or headers.get("x-facebook-page-id")
    if not isinstance(page_id, str) or not page_id:
        raise ValidationError("facebook_pages page_id is required", channel=_CHANNEL)

    msg = _message_obj(payload)
    message_id = _message_id(payload, msg)
    sender_id = _sender_id(payload, msg)
    recipients = _recipient_ids(payload, msg)
    occurred_at = _occurred_at(payload, msg)
    is_echo = bool(msg.get("is_echo")) or sender_id == page_id
    actor_ref = page_id if is_echo else sender_id
    conversation_id = payload.get("conversation_id")
    if not isinstance(conversation_id, str) or not conversation_id:
        conversation_id = sender_id if sender_id and sender_id != page_id else message_id
    text = _message_text(payload, msg)

    content = {
        "page_id": page_id,
        "page_name": payload.get("page_name"),
        "conversation_id": conversation_id,
        "message_id": message_id,
        "from_id": sender_id,
        "to_ids": recipients,
        "is_echo": is_echo,
        "source": payload.get("source") or "graph",
        "message": msg,
    }
    postback = payload.get("postback")
    if not isinstance(postback, dict) and isinstance(payload.get("messaging"), dict):
        candidate = payload["messaging"].get("postback")
        postback = candidate if isinstance(candidate, dict) else None
    if isinstance(postback, dict):
        content["postback"] = postback

    entities = [{"type": "facebook_page", "id": page_id}]
    if conversation_id:
        entities.append({"type": "facebook_conversation", "id": conversation_id})
    if sender_id and sender_id != page_id:
        entities.append({"type": "facebook_psid", "id": sender_id})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=text,
        content=content,
        occurred_at=occurred_at,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=actor_ref,
        external_id=idempotency.facebook_page_message(page_id, message_id),
        entities_hint=entities,
        raw_payload=payload,
    )


__all__ = ["handle_facebook_pages"]
