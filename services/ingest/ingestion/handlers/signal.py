"""services/ingest/ingestion/handlers/signal.py — Signal message handler (IN-SIGNAL).

ONE channel `signal:message` (mirrors telegram:message's one-channel shape). The
handler is a pure function (no DB / network): it parses the canonical message
record (built identically by the backfill fetcher and the live gateway worker via
`integrations/signal/records.build_message_record`) into exactly ONE observation.

Because both ingress paths produce the same record and the external_id is derived
through the central `idempotency.signal_message` constructor, a backfilled
message and its live gateway twin collapse to one observation (the cross-path
dedup invariant).

  external_id: signal:{installation_id}:{thread_id}:{message_id}:{edit_ts|none}
    — install-namespaced (the global observations UNIQUE has no tenant_id) and
    VERSIONED by edit slot. Edits are unsupported in v1, so the edit slot is
    always 'none'.

Trust posture: Signal is a human conversational channel like Telegram/Slack →
`attested_agent` (same tier as the telegram archetype).
"""
from __future__ import annotations

from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    ObservationDraft,
)
from services.ingest.integrations.signal.records import (
    CHANNEL,
    parse_message_record,
)


_TRUST = "attested_agent"


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


async def handle_signal(
    payload: dict[str, Any], headers: dict[str, str],
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "signal payload must be a JSON object", channel=CHANNEL,
        )

    parsed = parse_message_record(payload)

    sender_ref = (
        f"signal:user:{parsed.sender_id}" if parsed.sender_id is not None else None
    )

    thread_label = parsed.thread_title or f"thread {parsed.thread_id}"
    who = parsed.sender_username or sender_ref or "someone"
    body = parsed.text or "(no text)"
    content_text = f"[{thread_label}] {who}: {_truncate(body)}"

    entities: list[dict[str, Any]] = [
        {"type": "signal_thread", "id": str(parsed.thread_id), "role": "channel"},
    ]
    if parsed.sender_id is not None:
        sender_hint: dict[str, Any] = {
            "type": "signal_user", "id": str(parsed.sender_id), "role": "actor",
        }
        if parsed.sender_username:
            sender_hint["display_name"] = parsed.sender_username
        entities.append(sender_hint)

    content: dict[str, Any] = {
        "object_type": "message",
        "installation_id": parsed.installation_id,
        "thread_id": parsed.thread_id,
        "thread_kind": parsed.thread_kind,
        "thread_title": parsed.thread_title,
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




__all__ = ["handle_signal"]
