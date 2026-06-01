"""Tests for the Gmail handler that converts a Gmail API message
resource into an ObservationDraft (pure-function, no DB)."""
from __future__ import annotations

import base64
from typing import Any

import pytest

from services.ingestion.handlers.gmail import (
    CHANNEL,
    TRUST_TIER,
    handle_gmail,
)


def _resource(
    *,
    message_id: str = "<abc@mail>",
    in_reply_to: str | None = None,
    references: str | None = None,
    from_: str = "Alice <alice@x.com>",
    to: str = "bob@y.com",
    cc: str | None = None,
    subject: str = "hello",
    snippet: str = "hi there",
    body_text: str | None = None,
    internal_date_ms: int = 1700000000000,
    label_ids: list[str] | None = None,
) -> dict[str, Any]:
    headers = [
        {"name": "Message-ID", "value": message_id},
        {"name": "From", "value": from_},
        {"name": "To", "value": to},
        {"name": "Subject", "value": subject},
    ]
    if cc:
        headers.append({"name": "Cc", "value": cc})
    if in_reply_to:
        headers.append({"name": "In-Reply-To", "value": in_reply_to})
    if references:
        headers.append({"name": "References", "value": references})
    payload: dict[str, Any] = {"headers": headers}
    if body_text is not None:
        encoded = base64.urlsafe_b64encode(body_text.encode("utf-8")).rstrip(b"=").decode()
        payload["mimeType"] = "text/plain"
        payload["body"] = {"data": encoded}
    return {
        "id": "msg-1",
        "threadId": "thr-1",
        "labelIds": label_ids or ["INBOX"],
        "snippet": snippet,
        "internalDate": str(internal_date_ms),
        "payload": payload,
        "sizeEstimate": 1024,
    }


def _payload(
    *,
    scope: str = "gmail.metadata",
    read_path: str = "push",
    thread_canonical_id: str = "00000000-0000-0000-0000-000000000001",
    install_id: str = "00000000-0000-0000-0000-000000000002",
    **kwargs: Any,
) -> dict[str, Any]:
    return {
        "message_resource": _resource(**kwargs),
        "mailbox_email": "alice@x.com",
        "scope_used": scope,
        "read_path": read_path,
        "gmail_installation_id": install_id,
        "thread_canonical_id": thread_canonical_id,
    }


@pytest.mark.asyncio
class TestHandleGmail:
    async def test_minimal_metadata_message(self) -> None:
        draft = await handle_gmail(_payload(), {})
        assert draft.source_channel == CHANNEL
        assert draft.trust_tier == TRUST_TIER
        assert draft.content["message_id"] == "abc@mail"
        assert draft.content["from"] == "Alice <alice@x.com>"
        assert draft.content["to"] == ["bob@y.com"]
        assert draft.content["scope_used"] == "gmail.metadata"
        assert draft.content["body"] is None
        assert draft.external_id == (
            "gmail:00000000-0000-0000-0000-000000000002:abc@mail"
        )
        assert draft.source_actor_ref == "email:alice@x.com"

    async def test_readonly_extracts_body(self) -> None:
        draft = await handle_gmail(
            _payload(scope="gmail.readonly", body_text="hello world"), {},
        )
        assert draft.content["body"] == "hello world"
        assert "hello world" in draft.content_text

    async def test_thread_canonical_id_stamped_in_content(self) -> None:
        draft = await handle_gmail(_payload(), {})
        assert draft.content["_gmail_thread_canonical_id"] == (
            "00000000-0000-0000-0000-000000000001"
        )

    async def test_entity_hints_include_recipients(self) -> None:
        draft = await handle_gmail(_payload(to="bob@y.com, carol@z.com"), {})
        emails = {e["value"] for e in draft.entities_hint if e["kind"] == "email"}
        assert "bob@y.com" in emails
        assert "carol@z.com" in emails
        assert "alice@x.com" in emails  # from

    async def test_missing_message_id_rejected(self) -> None:
        payload = _payload()
        # Strip the Message-ID header.
        payload["message_resource"]["payload"]["headers"] = [
            h
            for h in payload["message_resource"]["payload"]["headers"]
            if h["name"].lower() != "message-id"
        ]
        with pytest.raises(Exception):
            await handle_gmail(payload, {})

    async def test_invalid_scope_rejected(self) -> None:
        payload = _payload()
        payload["scope_used"] = "gmail.modify"
        with pytest.raises(Exception):
            await handle_gmail(payload, {})

    async def test_invalid_read_path_rejected(self) -> None:
        payload = _payload()
        payload["read_path"] = "backfill"
        with pytest.raises(Exception):
            await handle_gmail(payload, {})
