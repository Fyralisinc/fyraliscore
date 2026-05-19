"""Gmail mailbox fixture generator.

`make_gmail_mailbox(email=..., messages=N, history_events=M, ...)`
produces a deterministic mailbox shaped to feed `MockGmailClient`.

Output shape:
    {
      "email": "alice@x.com",
      "messages": [{"id": "msg-001", "threadId": "thr-001",
                    "snippet": "...", "internalDate": "...",
                    "payload": {...}, "historyId": "1001"}, ...],
      "history_events": [{"id": "1001", "messages": [...]}, ...],
      "starting_history_id": "1000",
      "current_history_id": "1015",
      "page_size": 5,
    }
"""
from __future__ import annotations

import hashlib
from typing import Any


def make_gmail_mailbox(
    *,
    email: str,
    messages: int = 10,
    history_events: int = 0,
    message_size_kb: int = 2,
    starting_history_id: int = 1000,
    page_size: int = 5,
) -> dict[str, Any]:
    """Build a Gmail mailbox fixture.

    Args:
      email: Mailbox owner.
      messages: Number of messages to seed in the mailbox.
      history_events: Number of history events past
        `starting_history_id` (simulates new messages arriving between
        the planner's read and the fetcher's read → reconciler gap).
      message_size_kb: Per-message synthetic body size (drives snippet
        length so total fixture payload scales predictably).
      starting_history_id: Initial historyId before any events.
      page_size: How many messages `MockGmailClient.messages_list`
        returns per page.

    Returns:
      Fixture dict consumable by `MockGmailClient(fixture=...)`.
    """
    msgs: list[dict[str, Any]] = []
    snippet_pad = "x" * (message_size_kb * 1024)
    for i in range(messages):
        mid = f"msg-{_digest(email, i, 'msg')[:12]}"
        thread_id = f"thr-{_digest(email, i // 3, 'thr')[:12]}"
        history_id = str(starting_history_id - messages + i + 1)
        msgs.append({
            "id": mid,
            "threadId": thread_id,
            "snippet": snippet_pad[:140],
            "internalDate": str(1_700_000_000_000 + i * 60_000),
            "historyId": history_id,
            "payload": {
                "headers": [
                    {"name": "From", "value": f"sender-{i}@example.com"},
                    {"name": "To", "value": email},
                    {"name": "Subject", "value": f"Subject {i}"},
                ],
                "body": {"size": message_size_kb * 1024},
            },
        })

    current_history_id = starting_history_id + history_events
    events: list[dict[str, Any]] = []
    for k in range(history_events):
        new_msg_id = f"msg-gap-{_digest(email, k, 'gap')[:10]}"
        events.append({
            "id": str(starting_history_id + k + 1),
            "messages": [{"id": new_msg_id}],
            "messagesAdded": [
                {"message": {"id": new_msg_id,
                             "threadId": f"thr-gap-{k}"}},
            ],
        })

    return {
        "email": email,
        "messages": msgs,
        "history_events": events,
        "starting_history_id": str(starting_history_id),
        "current_history_id": str(current_history_id),
        "page_size": page_size,
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()
