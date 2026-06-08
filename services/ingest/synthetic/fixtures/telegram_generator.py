"""Telegram dialogs/messages fixture generator (IN-TELEGRAM).

`make_telegram(dialogs=N, messages_per_dialog=M, ...)` produces a deterministic
Telegram install fixture shaped to feed `MockTelegramClient`. It mirrors the
mercury/jira generators: every field is derived via `hashlib` (stable across
runs), timestamps land in 2026-01, and the shape is exactly what the mock client
pages over.

Fixture shape (consumed by `MockTelegramClient(fixture=...)` AND by the harness's
telegram install-seed branch, which INSERTs one telegram_dialogs row per dialog):
    {
      "dialogs": {
        "<dialog_id>": {                       # key is str(dialog_id) (JSON)
          "dialog_id": <int>,
          "dialog_kind": "channel"|"chat"|"user",
          "access_hash": <int>,
          "title": "...",
          # messages sorted by id ASCENDING (oldest..newest); the mock pages
          # them BACKWARD on offset_id, the way messages.getHistory does.
          "messages": [ {id, date, edit_date, message, out, from_id}, ... ],
        },
        ...
      },
      "dialog_order": [<int>, ...],            # planner shard order
      "page_size": 100,
    }

The fetcher (services/ingest/ingestion/fetchers/telegram.py) emits ONE record per
message, so the observation count per dialog is exactly `messages_per_dialog`.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


# Dialog kinds cycled across dialogs (mirrors telegram_dialogs.dialog_kind).
_DIALOG_KINDS = ("channel", "chat", "user")


def make_telegram(
    *,
    dialogs: int = 1,
    messages_per_dialog: int = 5,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
    seed: str = "",
) -> dict[str, Any]:
    """Build a deterministic Telegram install fixture.

    Args:
      dialogs: Number of dialogs (one shard each in the planner).
      messages_per_dialog: Messages on each dialog's history.
      base_iso: Anchor timestamp (2026-01); messages are spaced forward from it
        so id-ascending order matches time-ascending order.
      page_size: The mock client's per-page cap for `get_history`.
      seed: Optional namespacing salt mixed into the synthetic `dialog_id`.
        Telegram's external_id is already install-namespaced
        (`telegram:{installation_id}:{dialog_id}:…`, and each tenant has a
        distinct telegram_installations id), so cross-tenant collision on the
        global observations UNIQUE index cannot happen even with identical
        dialog ids. The seed is belt-and-suspenders: a per-tenant value (the
        tenant slug) also makes the dialog ids themselves tenant-distinct,
        matching production where each account sees distinct peers.

    Returns:
      Fixture dict consumable by `MockTelegramClient(fixture=...)`.
    """
    base_epoch = int(
        datetime.fromisoformat(base_iso.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .timestamp()
    )

    dialogs_map: dict[str, dict[str, Any]] = {}
    dialog_order: list[int] = []
    for d in range(dialogs):
        parts = ("telegram-dialog", d) if not seed else ("telegram-dialog", seed, d)
        # Positive 48-bit int — fits BIGINT and Telegram's peer-id range.
        dialog_id = int(_digest(*parts)[:12], 16)
        # Guard against a (vanishingly unlikely) zero / collision within a run.
        if dialog_id == 0 or dialog_id in dialog_order:
            dialog_id += d + 1
        dialog_order.append(dialog_id)
        kind = _DIALOG_KINDS[d % len(_DIALOG_KINDS)]
        access_hash = int(_digest("access", dialog_id)[:15], 16)
        messages = [
            _message(dialog_id, idx, base_epoch)
            for idx in range(messages_per_dialog)
        ]
        dialogs_map[str(dialog_id)] = {
            "dialog_id": dialog_id,
            "dialog_kind": kind,
            "access_hash": access_hash,
            "title": f"Dialog {d + 1}" + (f" ({seed})" if seed else ""),
            "messages": messages,
        }

    return {
        "dialogs": dialogs_map,
        "dialog_order": dialog_order,
        "page_size": page_size,
    }


def _message(dialog_id: int, idx: int, base_epoch: int) -> dict[str, Any]:
    """One deterministic Telegram message. id is 1-based and monotonic; date is
    spaced one hour apart forward from base so id-asc == time-asc (all 2026-01)."""
    message_id = idx + 1
    date = base_epoch + idx * 3600
    # A small rotating cast of senders per dialog.
    sender_id = int(_digest("sender", dialog_id, idx % 3)[:10], 16)
    return {
        "id": message_id,
        "date": date,
        "edit_date": None,
        "message": f"message {message_id} in dialog {dialog_id}",
        "out": False,
        "from_id": {"user_id": sender_id},
        "sender_username": f"user{idx % 3}_{_digest('u', dialog_id)[:5]}",
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_telegram"]
