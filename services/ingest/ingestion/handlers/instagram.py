"""Instagram Messaging handler.

One channel (`instagram:message`) handles customer messages, page/bot replies,
postbacks/reactions, and read/delivery status records. Webhook, backfill, and
poll producers all feed the canonical record shape from
`services.ingest.integrations.instagram.records`.
"""
from __future__ import annotations

from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)
from services.ingest.integrations.instagram.records import (
    CHANNEL,
    STATUS_RECORD_TYPE,
    parse_record,
)


_MESSAGE_TRUST = CHANNEL_TRUST_MAP[CHANNEL]
_STATUS_TRUST = "authoritative"


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "..."


def _actor_ref(parsed: Any) -> str | None:
    if parsed.record_type == STATUS_RECORD_TYPE:
        return None
    if parsed.direction == "outbound":
        return f"instagram:business:{parsed.ig_business_account_id}"
    actor_id = parsed.customer_id or parsed.sender_id
    return (
        f"instagram:{parsed.ig_business_account_id}:user:{actor_id}"
        if actor_id
        else None
    )


def _content_text(parsed: Any) -> str:
    if parsed.record_type == STATUS_RECORD_TYPE:
        target = parsed.customer_id or parsed.recipient_id or parsed.message_id
        return f"Instagram message {parsed.message_id} for {target}: {parsed.state}"
    if parsed.direction == "outbound":
        speaker = "Instagram business"
    else:
        speaker = (
            parsed.participant_display_name
            or parsed.participant_username
            or parsed.customer_id
            or parsed.sender_id
            or "Instagram user"
        )
    return f"Instagram DM {parsed.conversation_id} - {speaker}: {_truncate(parsed.text)}"


def _entities(parsed: Any) -> list[dict[str, Any]]:
    entities: list[dict[str, Any]] = [
        {
            "type": "instagram_business_account",
            "id": parsed.ig_business_account_id,
            "role": "business",
        },
        {
            "type": "instagram_conversation",
            "id": parsed.thread_key,
            "role": "thread",
        },
    ]
    if parsed.page_id:
        entities.append({"type": "facebook_page", "id": parsed.page_id})
    customer_id = parsed.customer_id or (
        parsed.sender_id if parsed.direction != "outbound" else parsed.recipient_id
    )
    if customer_id:
        hint: dict[str, Any] = {
            "type": "instagram_customer",
            "id": f"{parsed.ig_business_account_id}:{customer_id}",
            "instagram_scoped_user_id": customer_id,
            "role": "customer",
        }
        if parsed.participant_username:
            hint["username"] = parsed.participant_username
        if parsed.participant_display_name:
            hint["display_name"] = parsed.participant_display_name
            entities.append({
                "type": "person_name",
                "id": parsed.participant_display_name,
                "role": "customer_name",
            })
        entities.append(hint)
    return entities


@register(CHANNEL)
async def handle_instagram(
    payload: dict[str, Any], headers: dict[str, str],
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "instagram payload must be a JSON object", channel=CHANNEL,
        )

    parsed = parse_record(payload)
    content: dict[str, Any] = {
        "object_type": parsed.record_type,
        "event_type": parsed.event_type,
        "ig_business_account_id": parsed.ig_business_account_id,
        "page_id": parsed.page_id,
        "conversation_id": parsed.conversation_id,
        "thread_key": parsed.thread_key,
        "provider_conversation_id": parsed.provider_conversation_id,
        "message_id": parsed.message_id,
        "direction": parsed.direction,
        "sender_id": parsed.sender_id,
        "recipient_id": parsed.recipient_id,
        "customer_id": parsed.customer_id,
        "participant_username": parsed.participant_username,
        "participant_display_name": parsed.participant_display_name,
        "text": parsed.text,
    }
    if parsed.record_type == STATUS_RECORD_TYPE:
        content["_instagram_kind"] = "status"
        content["state"] = parsed.state
        content["watermark"] = parsed.watermark
    for key in (
        "message",
        "attachments",
        "quick_reply",
        "reply_to",
        "story",
        "postback",
        "reaction",
        "referral",
        "status",
        "message_edit",
        "target_message_id",
    ):
        if payload.get(key) is not None:
            content[key] = payload[key]

    return ObservationDraft(
        source_channel=CHANNEL,
        content_text=_content_text(parsed),
        content=content,
        occurred_at=parsed.occurred_at,
        trust_tier=(
            _STATUS_TRUST if parsed.record_type == STATUS_RECORD_TYPE else _MESSAGE_TRUST
        ),  # type: ignore[arg-type]
        kind="state_change" if parsed.record_type == STATUS_RECORD_TYPE else "signal",
        source_actor_ref=_actor_ref(parsed),
        external_id=parsed.external_id,
        entities_hint=_entities(parsed),
        raw_payload=payload,
    )


__all__ = ["handle_instagram"]
