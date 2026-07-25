"""Deterministic Meta Page/Messenger history fixture."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def make_facebook_pages(
    *,
    page_id: str = "PAGE1",
    page_name: str = "Synthetic Page",
    conversations: int = 2,
    messages_per_conversation: int = 3,
    page_size: int = 100,
) -> dict[str, Any]:
    """Build state consumed directly by Provider Lab's Graph adapter."""

    if conversations < 0 or messages_per_conversation < 0:
        raise ValueError("fixture counts must be non-negative")
    if page_size <= 0:
        raise ValueError("page_size must be positive")

    page_token = f"lab-facebook-pages::{page_id}"
    page = {
        "id": page_id,
        "name": page_name,
        "access_token": page_token,
        "tasks": ["MESSAGING", "MANAGE", "ANALYZE"],
    }
    conversation_rows: list[dict[str, Any]] = []
    message_rows: dict[str, list[dict[str, Any]]] = {}
    anchor = datetime(2026, 1, 1, tzinfo=timezone.utc)

    for conversation_index in range(conversations):
        conversation_id = f"{page_id}-conversation-{conversation_index + 1}"
        participant_id = f"{page_id}-participant-{conversation_index + 1}"
        conversation_rows.append(
            {
                "id": conversation_id,
                "updated_time": _timestamp(
                    anchor + timedelta(days=conversation_index + 1)
                ),
                "participants": {
                    "data": [
                        {"id": participant_id, "name": f"Person {conversation_index + 1}"},
                        {"id": page_id, "name": page_name},
                    ]
                },
            }
        )
        message_rows[conversation_id] = [
            {
                "id": f"{conversation_id}-message-{message_index + 1}",
                "created_time": _timestamp(
                    anchor
                    + timedelta(
                        days=conversation_index,
                        minutes=message_index,
                    )
                ),
                "message": (
                    "Synthetic Messenger message "
                    f"{conversation_index + 1}.{message_index + 1}"
                ),
                "from": {"id": participant_id, "name": f"Person {conversation_index + 1}"},
                "to": {"data": [{"id": page_id, "name": page_name}]},
            }
            for message_index in range(messages_per_conversation)
        ]

    return {
        "pages": {page_id: page},
        "user_pages": {"provider-lab-user-token": [page]},
        "conversations": {page_id: conversation_rows},
        "messages": message_rows,
        "verify_tokens": ["provider-lab-verify"],
        "app_secrets": {"global": "provider-lab-secret"},
        "installations": {page_id: {"enabled": True}},
        "page_size": page_size,
    }


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


__all__ = ["make_facebook_pages"]
