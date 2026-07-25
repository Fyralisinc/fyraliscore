"""services/ingest/ingestion/handlers/whatsapp.py — WhatsApp Cloud API handler.

ONE channel, `whatsapp:message`, like github:webhook / notion:object — the
handler branches on the item shape (a one-channel/many-event-types source). This
is required by the Kafka data plane: the normalizer maps a raw envelope by
(source, ingress_kind) → a SINGLE channel (channel_mapping.resolve_channel), and
WhatsApp messages AND statuses both arrive as ingress_kind="webhook".

The dedicated WhatsApp router fans a webhook delivery out into one item per
message/status and passes a flat dict to this handler (inline) or shadow-writes
one raw envelope per item (Kafka); either way the handler sees one item:

    inbound message:  {"message": <msg>, "metadata": <md>, "contacts": [...]}
    delivery status:  {"status":  <st>,  "metadata": <md>}

Branches:
  * message → kind="signal",       trust="attested_agent" (customer-authored,
              Meta-signed webhook); external_id = whatsapp:{pid}:{wamid}.
  * status  → kind="state_change", trust OVERRIDDEN to "authoritative"
              (Meta-asserted fact); external_id = whatsapp:{pid}:status:{wamid}:{status}.

Both return source_channel="whatsapp:message" (the single registered channel);
messages vs statuses are distinguished by `kind`. external_id parity across the
inline-fallback and Kafka paths (same handler, same key) collapses twins to one
observation.

Signature verification happens in the router BEFORE dispatch (handlers receive
pre-verified payloads — same convention as the Slack handler).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion import idempotency
from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
)


_CHANNEL = "whatsapp:message"
_MESSAGE_TRUST = CHANNEL_TRUST_MAP[_CHANNEL]  # "attested_agent"
_STATUS_TRUST = "authoritative"  # handler override for delivery-status callbacks


def _parse_ts(ts: Any) -> datetime:
    """WhatsApp timestamps are unix-epoch SECONDS as strings."""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except (TypeError, ValueError) as e:
        raise ValidationError(
            f"whatsapp timestamp not an int-string: {ts!r}", field="timestamp"
        ) from e


def _contact_name(contacts: Any, wa_id: str | None) -> str | None:
    """Pull the sender's profile name from the webhook's `contacts[]`."""
    if not isinstance(contacts, list):
        return None
    for c in contacts:
        if not isinstance(c, dict):
            continue
        if wa_id is None or c.get("wa_id") == wa_id:
            profile = c.get("profile")
            if isinstance(profile, dict):
                name = profile.get("name")
                if isinstance(name, str) and name:
                    return name
    return None


def _message_text(msg: dict[str, Any]) -> str:
    """Render a human-legible `content_text` for any inbound message type.

    `observations.content_text` is NOT NULL, so every branch returns a
    non-empty string; media/structured types fall back to a bracketed label.
    """
    mtype = msg.get("type")

    if mtype == "text":
        body = (msg.get("text") or {}).get("body")
        return body if isinstance(body, str) and body else "[empty text]"

    if mtype in ("image", "video", "audio", "document", "sticker", "voice"):
        media = msg.get(mtype) or {}
        caption = media.get("caption") if isinstance(media, dict) else None
        filename = media.get("filename") if isinstance(media, dict) else None
        label = f"[{mtype}]"
        if filename:
            label += f" {filename}"
        if caption:
            return f"{label}: {caption}"
        return label

    if mtype == "location":
        loc = msg.get("location") or {}
        name = loc.get("name") or loc.get("address")
        coords = f"{loc.get('latitude')},{loc.get('longitude')}"
        return f"[location] {name or coords}"

    if mtype == "contacts":
        contacts = msg.get("contacts") or []
        names = []
        for c in contacts if isinstance(contacts, list) else []:
            nm = (c.get("name") or {}).get("formatted_name") if isinstance(c, dict) else None
            if nm:
                names.append(nm)
        return "[contact card] " + (", ".join(names) if names else "shared a contact")

    if mtype == "interactive":
        inter = msg.get("interactive") or {}
        itype = inter.get("type")
        if itype == "button_reply":
            br = inter.get("button_reply") or {}
            return br.get("title") or br.get("id") or "[button reply]"
        if itype == "list_reply":
            lr = inter.get("list_reply") or {}
            return lr.get("title") or lr.get("id") or "[list reply]"
        if itype == "nfm_reply":
            # WhatsApp Flows submission (response_json carries the form data).
            return "[flow reply] " + str((inter.get("nfm_reply") or {}).get("name") or "submitted")
        return f"[interactive:{itype}]"

    if mtype == "button":
        return (msg.get("button") or {}).get("text") or "[button]"

    if mtype == "reaction":
        rx = msg.get("reaction") or {}
        emoji = rx.get("emoji") or ""
        return f"[reaction] {emoji} to {rx.get('message_id')}".strip()

    if mtype == "order":
        order = msg.get("order") or {}
        items = order.get("product_items") or []
        return f"[order] {len(items)} item(s), catalog {order.get('catalog_id')}"

    if mtype == "system":
        return "[system] " + str((msg.get("system") or {}).get("body") or "system update")

    if mtype == "unsupported":
        return "[unsupported message type]"

    return f"[{mtype or 'unknown'}]"


def _draft_message(payload: dict[str, Any]) -> ObservationDraft:
    msg = payload["message"]
    if not isinstance(msg, dict):
        raise ValidationError("whatsapp 'message' must be an object", channel=_CHANNEL)

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    phone_number_id = metadata.get("phone_number_id")

    wamid = msg.get("id")
    if not isinstance(wamid, str) or not wamid:
        raise ValidationError("whatsapp message missing 'id' (wamid)", channel=_CHANNEL)
    from_ = msg.get("from")
    if not isinstance(from_, str) or not from_:
        raise ValidationError("whatsapp message missing 'from'", channel=_CHANNEL)

    occurred_at = _parse_ts(msg.get("timestamp"))
    text = _message_text(msg)
    contact_name = _contact_name(payload.get("contacts"), from_)

    entities: list[dict[str, Any]] = [{"type": "whatsapp_user", "id": from_}]
    if contact_name:
        entities.append({"type": "person_name", "id": contact_name})

    content: dict[str, Any] = {
        "wamid": wamid,
        "from": from_,
        "type": msg.get("type"),
        "phone_number_id": phone_number_id,
        "display_phone_number": metadata.get("display_phone_number"),
        "contact_name": contact_name,
        "timestamp": msg.get("timestamp"),
    }
    mtype = msg.get("type")
    if isinstance(mtype, str) and isinstance(msg.get(mtype), (dict, list)):
        content[mtype] = msg.get(mtype)
    if isinstance(msg.get("context"), dict):
        content["context"] = msg["context"]
    if isinstance(msg.get("referral"), dict):
        content["referral"] = msg["referral"]

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=text,
        content=content,
        occurred_at=occurred_at,
        trust_tier=_MESSAGE_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=f"whatsapp:{from_}",
        external_id=idempotency.whatsapp_message(phone_number_id, wamid),
        entities_hint=entities,
        raw_payload=payload,
    )


def _draft_status(payload: dict[str, Any]) -> ObservationDraft:
    st = payload["status"]
    if not isinstance(st, dict):
        raise ValidationError("whatsapp 'status' must be an object", channel=_CHANNEL)

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    phone_number_id = metadata.get("phone_number_id")

    wamid = st.get("id")
    status = st.get("status")
    if not isinstance(wamid, str) or not wamid:
        raise ValidationError("whatsapp status missing 'id' (wamid)", channel=_CHANNEL)
    if not isinstance(status, str) or not status:
        raise ValidationError("whatsapp status missing 'status'", channel=_CHANNEL)

    recipient = st.get("recipient_id")
    occurred_at = _parse_ts(st.get("timestamp"))

    content: dict[str, Any] = {
        "wamid": wamid,
        "status": status,
        "recipient_id": recipient,
        "phone_number_id": phone_number_id,
        "timestamp": st.get("timestamp"),
        "_whatsapp_kind": "status",
    }
    for opt in ("conversation", "pricing", "errors"):
        if st.get(opt) is not None:
            content[opt] = st[opt]

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=f"WhatsApp message {wamid} to {recipient}: {status}",
        content=content,
        occurred_at=occurred_at,
        trust_tier=_STATUS_TRUST,  # type: ignore[arg-type]  — authoritative override
        kind="state_change",
        source_actor_ref=f"whatsapp:{recipient}" if recipient else None,
        external_id=idempotency.whatsapp_status(phone_number_id, wamid, status),
        entities_hint=[],
        raw_payload=payload,
    )


async def handle_whatsapp(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """Parse one WhatsApp item (message OR status) into an ObservationDraft.

    Branches on which key is present — the router/normalizer always pass a
    single-item dict carrying exactly one of `message` / `status`.
    """
    if not isinstance(payload, dict):
        raise ValidationError("whatsapp payload must be a JSON object", channel=_CHANNEL)
    if isinstance(payload.get("status"), dict):
        return _draft_status(payload)
    if isinstance(payload.get("message"), dict):
        return _draft_message(payload)
    raise ValidationError(
        "whatsapp payload missing 'message' or 'status' object", channel=_CHANNEL
    )


__all__ = ["handle_whatsapp"]
