"""services/ingest/integrations/signal/records.py — canonical message record.

The ONE place that defines the shape of a Signal message record as it flows
through the pipeline. Both ingress paths build a record here so they cannot
drift:

  - BACKFILL: `fetchers/signal.py` calls `build_message_record` for each raw
    message returned by `SignalClient.get_history`.
  - LIVE: the gateway worker (`integrations/signal/gateway/`) calls it for each
    new message it receives over the persistent linked-device session.

and the `handlers/signal.py` handler calls `parse_message_record` to turn one
record into an `ObservationDraft`. Because both ingress paths produce the same
record and the handler derives the external_id from it via the central
`idempotency.signal_message` constructor, a backfilled message and its live
twin collapse to one observation (the cross-path dedup invariant).

This is a PURE leaf module: stdlib + the idempotency constructor only. No I/O,
no Signal transport import (the raw message dict is plain data, produced by the
client's transport→dict conversion or by a synthetic generator).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion import idempotency


CHANNEL = "signal:message"
MESSAGE_RECORD_TYPE = "message"

# Thread kinds mirror the signal_threads.thread_kind CHECK.
THREAD_KINDS = ("direct", "group")


def build_message_record(
    message: dict[str, Any],
    *,
    installation_id: object,
    thread_id: int,
    thread_kind: str,
    thread_title: str | None,
) -> dict[str, Any]:
    """Compose the canonical record from a raw Signal message dict + the thread
    context (which lives on the shard / watch row, not the message).

    `message` is the raw-ish message dict (real client→dict, or a synthetic
    generator's mint):
        {"id", "date" (epoch s), "edit_date" (epoch s|None), "message" (text),
         "out" (bool), "from_id": {"user_id": int}|None, "sender_username"?}

    The thread context is injected under `_fyralis_*` keys (mirroring telegram's
    dialog context / jira's `_fyralis_site`) so the pure handler can derive the
    install-namespaced external_id without any DB lookup.
    """
    record = dict(message)
    record["_fyralis_record_type"] = MESSAGE_RECORD_TYPE
    record["_fyralis_installation_id"] = str(installation_id)
    record["_fyralis_thread_id"] = int(thread_id)
    record["_fyralis_thread_kind"] = thread_kind
    record["_fyralis_thread_title"] = thread_title
    return record


@dataclass(frozen=True)
class ParsedMessage:
    installation_id: str
    thread_id: int
    thread_kind: str
    thread_title: str | None
    message_id: int
    occurred_at: datetime
    edit_date: int | None
    text: str
    sender_id: int | None
    sender_username: str | None
    external_id: str


def _epoch_to_dt(value: Any) -> datetime | None:
    if not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _sender_id(message: dict[str, Any]) -> int | None:
    from_id = message.get("from_id")
    if isinstance(from_id, dict):
        uid = from_id.get("user_id")
        if isinstance(uid, int):
            return uid
    # A self-sent / group-system message may have no from_id.
    return None


def parse_message_record(record: dict[str, Any]) -> ParsedMessage:
    """Validate + normalize a canonical record into the fields the handler needs.

    Raises `ValidationError` (→ DLQ, not a crash) on a malformed record — a
    missing message id, an unparseable date, or a missing thread id.

    Edits are unsupported in v1 → the external_id edit slot is always 'none'.
    """
    if not isinstance(record, dict):
        raise ValidationError("signal record must be an object", channel=CHANNEL)

    message_id = record.get("id")
    if not isinstance(message_id, int):
        raise ValidationError("signal message missing integer id", channel=CHANNEL)

    thread_id = record.get("_fyralis_thread_id")
    if not isinstance(thread_id, int):
        raise ValidationError(
            "signal record missing _fyralis_thread_id", channel=CHANNEL,
        )

    occurred = _epoch_to_dt(record.get("date"))
    if occurred is None:
        raise ValidationError(
            "signal message has no parseable date", channel=CHANNEL,
        )

    installation_id = str(record.get("_fyralis_installation_id") or "")
    edit_date = record.get("edit_date")
    edit_date = int(edit_date) if isinstance(edit_date, (int, float)) else None
    text = record.get("message")
    text = text if isinstance(text, str) else ""
    sender_id = _sender_id(record)
    sender_username = record.get("sender_username")
    sender_username = sender_username if isinstance(sender_username, str) else None

    # v1 does not support edits — the edit slot is always 'none'.
    external_id = idempotency.signal_message(
        installation_id, thread_id, message_id, edit_date,
    )

    return ParsedMessage(
        installation_id=installation_id,
        thread_id=thread_id,
        thread_kind=str(record.get("_fyralis_thread_kind") or "direct"),
        thread_title=record.get("_fyralis_thread_title"),
        message_id=message_id,
        occurred_at=occurred,
        edit_date=edit_date,
        text=text,
        sender_id=sender_id,
        sender_username=sender_username,
        external_id=external_id,
    )


__all__ = [
    "CHANNEL",
    "MESSAGE_RECORD_TYPE",
    "THREAD_KINDS",
    "ParsedMessage",
    "build_message_record",
    "parse_message_record",
]
