"""Tests for services/ingestion/handlers/linear.py."""
from __future__ import annotations

import hashlib
import hmac

import pytest

from services.ingestion.handlers.linear import (
    LinearSignatureError,
    handle_linear_webhook,
    verify_linear_signature,
)


# =====================================================================
# Signature verification
# =====================================================================

def _sign(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_linear_signature_happy_path():
    body = b'{"type":"Issue"}'
    verify_linear_signature(body, _sign(body, "s"), "s")


def test_linear_signature_tampered_raises():
    body = b'{"type":"Issue"}'
    sig = _sign(body, "s")
    with pytest.raises(LinearSignatureError) as exc:
        verify_linear_signature(body + b"X", sig, "s")
    assert exc.value.context.get("reason") == "mismatch"


def test_linear_signature_missing_header_raises():
    with pytest.raises(LinearSignatureError) as exc:
        verify_linear_signature(b"{}", None, "s")
    assert exc.value.context.get("reason") == "missing_signature"


def test_linear_signature_missing_secret_raises():
    with pytest.raises(LinearSignatureError) as exc:
        verify_linear_signature(b"{}", "abc", "")
    assert exc.value.context.get("reason") == "missing_secret"


# =====================================================================
# Issue state change — readable sentence
# =====================================================================

async def test_linear_issue_state_change_sentence():
    payload = {
        "action": "update",
        "type": "Issue",
        "data": {
            "id": "issue-uuid",
            "identifier": "ENG-123",
            "title": "Fix billing webhook",
            "state": {"name": "In Review"},
            "team": {"id": "team-uuid"},
            "project": {"id": "project-uuid"},
            "assignee": {"name": "Alice", "id": "user-alice"},
            "updatedBy": {"id": "user-alice", "name": "Alice"},
            "updatedAt": "2026-04-21T10:00:00Z",
        },
        "updatedFrom": {"state": {"name": "In Progress"}},
        "createdAt": "2026-04-21T10:00:00Z",
    }
    draft = await handle_linear_webhook(payload, {})
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "state_change"
    assert "moved ENG-123" in draft.content_text
    assert "from In Progress to In Review" in draft.content_text
    assert draft.external_id == "issue-uuid"
    types = {e["type"] for e in draft.entities_hint}
    assert {"linear_issue", "linear_project", "linear_team"} <= types
    assert draft.source_actor_ref == "linear:user-alice"


async def test_linear_issue_create_is_authoritative_signal():
    payload = {
        "action": "create",
        "type": "Issue",
        "data": {
            "id": "i1",
            "identifier": "ENG-1",
            "title": "new issue",
            "team": {"id": "t1"},
            "creator": {"id": "u1", "name": "Carol"},
            "createdAt": "2026-04-21T10:00:00Z",
        },
    }
    draft = await handle_linear_webhook(payload, {})
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert "created ENG-1" in draft.content_text


async def test_linear_comment_is_inferential():
    payload = {
        "action": "create",
        "type": "Comment",
        "data": {
            "id": "comment-uuid",
            "body": "let's ship next week",
            "issue": {"id": "i1", "identifier": "ENG-2"},
            "user": {"id": "u2", "name": "Dan"},
            "createdAt": "2026-04-21T10:00:00Z",
        },
    }
    draft = await handle_linear_webhook(payload, {})
    assert draft.trust_tier == "inferential"
    assert "Dan commented on ENG-2" in draft.content_text
    assert draft.external_id == "comment-uuid"


async def test_linear_project_update_is_state_change():
    payload = {
        "action": "update",
        "type": "Project",
        "data": {
            "id": "p1",
            "name": "Launchpad",
            "state": "started",
            "updatedAt": "2026-04-21T10:00:00Z",
        },
        "updatedFrom": {"state": "planned"},
    }
    draft = await handle_linear_webhook(payload, {})
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "state_change"
    assert "project 'Launchpad'" in draft.content_text


async def test_linear_unsupported_type_raises():
    from lib.shared.errors import ValidationError

    with pytest.raises(ValidationError):
        await handle_linear_webhook(
            {"type": "IssueLabel", "data": {}}, {}
        )


async def test_linear_missing_type_raises():
    from lib.shared.errors import ValidationError

    with pytest.raises(ValidationError):
        await handle_linear_webhook({"data": {}}, {})
