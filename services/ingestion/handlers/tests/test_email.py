"""Tests for services/ingestion/handlers/email.py."""
from __future__ import annotations

import hashlib
import hmac

import pytest

from services.ingestion.handlers.email import (
    EmailSignatureError,
    handle_email_webhook,
    verify_email_signature,
)


# =====================================================================
# Signature tests
# =====================================================================

def _sig(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_email_signature_happy_path():
    body = b'{"from":"a@b"}'
    verify_email_signature(body, _sig(body, "k"), "k")


def test_email_signature_tampered_raises():
    body = b'{"from":"a@b"}'
    sig = _sig(body, "k")
    with pytest.raises(EmailSignatureError):
        verify_email_signature(body + b"X", sig, "k")


def test_email_signature_missing_secret_raises():
    with pytest.raises(EmailSignatureError):
        verify_email_signature(b"{}", "abc", "")


def test_email_signature_missing_header_raises():
    with pytest.raises(EmailSignatureError):
        verify_email_signature(b"{}", None, "k")


# =====================================================================
# From-address resolution
# =====================================================================

class _FakeActorResolver:
    """Test double for services.actors.repo.ActorRepo."""

    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping
        self.calls: list[str] = []

    async def resolve_by_source_actor_ref(self, ref: str):
        self.calls.append(ref)
        return self._mapping.get(ref)


async def test_email_from_resolves_to_actor_id():
    actor_id = "11111111-1111-1111-1111-111111111111"
    resolver = _FakeActorResolver({"email:alice@company.com": actor_id})
    payload = {
        "from": "Alice <alice@company.com>",
        "to": ["bob@company.com"],
        "subject": "hi",
        "body": "hello bob",
        "message_id": "<msg1@server>",
    }
    draft = await handle_email_webhook(
        payload, {}, tenant_id="t1", actor_resolver=resolver
    )
    assert draft.source_actor_ref == "email:alice@company.com"
    # first entity hint is the sender's actor id.
    assert draft.entities_hint[0] == {"type": "actor", "id": actor_id}


async def test_email_from_unknown_stays_email_address():
    resolver = _FakeActorResolver({})
    payload = {
        "from": "unknown@example.net",
        "to": ["bob@company.com"],
        "subject": "inquiry",
        "body": "please reply",
        "message_id": "<msg2@server>",
    }
    draft = await handle_email_webhook(
        payload, {}, tenant_id="t1", actor_resolver=resolver
    )
    # resolver consulted but returned None.
    assert resolver.calls == ["email:unknown@example.net"]
    assert draft.entities_hint[0] == {
        "type": "email_address", "id": "unknown@example.net"
    }


async def test_email_external_id_is_message_id():
    payload = {
        "from": "a@b",
        "to": ["c@d"],
        "subject": "x",
        "body": "y",
        "message_id": "<abc123@mail.local>",
    }
    draft = await handle_email_webhook(payload, {})
    assert draft.external_id == "<abc123@mail.local>"


async def test_email_postmark_shape_accepted():
    payload = {
        "From": "alice@company.com",
        "FromFull": {"Email": "alice@company.com", "Name": "Alice"},
        "To": "bob@company.com",
        "ToFull": [{"Email": "bob@company.com"}],
        "Subject": "re: billing",
        "MessageID": "<pm1@pm>",
        "TextBody": "See https://example.com for details. charlie@c.com cc'd.",
        "Headers": [
            {"Name": "References", "Value": "<prev@mail>"},
        ],
        "Date": "Mon, 21 Apr 2026 10:00:00 +0000",
    }
    draft = await handle_email_webhook(payload, {})
    assert draft.external_id == "<pm1@pm>"
    assert draft.content["references"] == ["<prev@mail>"]
    # URL + additional email address should appear.
    types = {e["type"] for e in draft.entities_hint}
    assert "url" in types
    assert "email_address" in types


async def test_email_malformed_shape_raises():
    from lib.shared.errors import ValidationError

    with pytest.raises(ValidationError):
        await handle_email_webhook({"weird": "shape"}, {})


async def test_email_empty_body_no_hints():
    payload = {
        "from": "a@b.com",
        "to": ["c@d.com"],
        "subject": "",
        "body": "",
        "message_id": "<m@x>",
    }
    draft = await handle_email_webhook(payload, {})
    # Only the sender is a hint; no URL / alias hits.
    assert len(draft.entities_hint) == 1
    assert draft.entities_hint[0]["type"] == "email_address"
