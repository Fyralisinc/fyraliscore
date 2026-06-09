"""services/ingest/ingestion/handlers/telegram.py — Telegram message handler (IN-TELEGRAM).

ONE channel `telegram:message` (mirrors discord:message's one-channel shape). The
handler is a pure function (no DB / network): it parses the canonical message
record (built identically by the backfill fetcher and the live gateway worker via
`integrations/telegram/records.build_message_record`) into exactly ONE observation.

Because both ingress paths produce the same record and the external_id is derived
through the central `idempotency.telegram_message` constructor, a backfilled
message and its live `updateNewMessage` gateway twin collapse to one observation
(the cross-path dedup invariant — `test_backfill_external_id_parity`).

  external_id: telegram:{installation_id}:{dialog_id}:{message_id}:{edit_date|none}
    — install-namespaced (the global observations UNIQUE has no tenant_id) and
    VERSIONED by edit_date so an edit re-observes while an identical re-fetch
    dedups.

Trust posture: Telegram is a human conversational channel like Slack/Discord →
`attested_agent`.
"""
from __future__ import annotations

from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)
from services.ingest.integrations.telegram.records import (
    CHANNEL,
    parse_message_record,
)


_TRUST = "attested_agent"


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


@register(CHANNEL)
async def handle_telegram(
    payload: dict[str, Any], headers: dict[str, str],
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "telegram payload must be a JSON object", channel=CHANNEL,
        )

    parsed = parse_message_record(payload)

    sender_ref = (
        f"telegram:user:{parsed.sender_id}" if parsed.sender_id is not None else None
    )

    dialog_label = parsed.dialog_title or f"dialog {parsed.dialog_id}"
    who = parsed.sender_username or sender_ref or "someone"
    body = parsed.text or "(no text)"
    content_text = f"[{dialog_label}] {who}: {_truncate(body)}"

    entities: list[dict[str, Any]] = [
        {"type": "telegram_dialog", "id": str(parsed.dialog_id), "role": "channel"},
    ]
    if parsed.sender_id is not None:
        sender_hint: dict[str, Any] = {
            "type": "telegram_user", "id": str(parsed.sender_id), "role": "actor",
        }
        if parsed.sender_username:
            sender_hint["display_name"] = parsed.sender_username
        entities.append(sender_hint)

    content: dict[str, Any] = {
        "object_type": "message",
        "installation_id": parsed.installation_id,
        "dialog_id": parsed.dialog_id,
        "dialog_kind": parsed.dialog_kind,
        "dialog_title": parsed.dialog_title,
        "message_id": parsed.message_id,
        "text": parsed.text,
        "sender": sender_ref,
        "sender_username": parsed.sender_username,
        "edit_date": parsed.edit_date,
        "edited": parsed.edit_date is not None,
    }

    return ObservationDraft(
        source_channel=CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=parsed.occurred_at,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=sender_ref,
        external_id=parsed.external_id,
        entities_hint=entities,
        raw_payload=payload,
    )


CHANNEL_TRUST_MAP.setdefault(CHANNEL, _TRUST)


__all__ = ["handle_telegram"]
