"""Tests for services/ingestion/handlers/github.py."""
from __future__ import annotations

import hashlib
import hmac

import pytest

from services.ingestion.handlers import ObservationDraft
from services.ingestion.handlers.github import (
    GithubSignatureError,
    handle_github_webhook,
    verify_github_signature,
)


# =====================================================================
# Signature verification — happy / tamper / missing-secret / missing-sig
# =====================================================================

def _sign(body: bytes, secret: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    return "sha256=" + mac.hexdigest()


def test_github_signature_happy_path():
    body = b'{"action":"opened"}'
    sig = _sign(body, "shh")
    verify_github_signature(body, sig, "shh")   # no exception


def test_github_signature_tampered_raises():
    body = b'{"action":"opened"}'
    sig = _sign(body, "shh")
    tampered = body + b"X"
    with pytest.raises(GithubSignatureError) as exc:
        verify_github_signature(tampered, sig, "shh")
    assert exc.value.context.get("reason") == "mismatch"


def test_github_signature_missing_header_raises():
    with pytest.raises(GithubSignatureError) as exc:
        verify_github_signature(b"{}", None, "shh")
    assert exc.value.context.get("reason") == "missing_signature"


def test_github_signature_missing_secret_raises():
    with pytest.raises(GithubSignatureError) as exc:
        verify_github_signature(b"{}", "sha256=abc", "")
    assert exc.value.context.get("reason") == "missing_secret"


def test_github_signature_malformed_prefix_raises():
    with pytest.raises(GithubSignatureError) as exc:
        verify_github_signature(b"{}", "md5=abc", "shh")
    assert exc.value.context.get("reason") == "malformed"


# =====================================================================
# PR merge payload — the canonical test from Prompt 2.B
# =====================================================================

async def test_github_pr_merge_happy_path():
    payload = {
        "action": "closed",
        "pull_request": {
            "number": 123,
            "title": "Add rate limiter",
            "node_id": "PR_kwDOABC",
            "merged": True,
            "base": {"ref": "main"},
            "updated_at": "2026-04-21T10:00:00Z",
            "created_at": "2026-04-20T10:00:00Z",
        },
        "repository": {"full_name": "acme/webapp"},
        "sender": {"login": "alice"},
    }
    draft = await handle_github_webhook(
        payload, {"X-GitHub-Event": "pull_request"}
    )
    assert isinstance(draft, ObservationDraft)
    assert draft.source_channel == "github:webhook"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "state_change"
    assert "alice merged PR #123 'Add rate limiter' into main" in draft.content_text
    assert draft.external_id == "PR_kwDOABC"
    types = {e["type"] for e in draft.entities_hint}
    assert {"github_pr", "github_repo", "github_branch"} <= types
    assert draft.source_actor_ref == "github:alice"


async def test_github_pr_closed_no_merge_is_inferential():
    payload = {
        "action": "closed",
        "pull_request": {
            "number": 5, "title": "wip", "node_id": "PR_abc",
            "merged": False, "base": {"ref": "main"},
        },
        "repository": {"full_name": "acme/w"},
        "sender": {"login": "bob"},
    }
    draft = await handle_github_webhook(
        payload, {"X-GitHub-Event": "pull_request"}
    )
    assert draft.trust_tier == "inferential"
    assert "closed PR #5" in draft.content_text


async def test_github_issue_comment_trust_tier_inferential():
    payload = {
        "action": "created",
        "issue": {"number": 10, "node_id": "I_a"},
        "comment": {
            "body": "nudging this",
            "node_id": "IC_a",
            "created_at": "2026-04-21T10:00:00Z",
        },
        "repository": {"full_name": "acme/w"},
        "sender": {"login": "carol"},
    }
    draft = await handle_github_webhook(
        payload, {"X-GitHub-Event": "issue_comment"}
    )
    assert draft.trust_tier == "inferential"
    assert draft.external_id == "IC_a"


async def test_github_check_run_trust_tier_authoritative():
    payload = {
        "action": "completed",
        "check_run": {
            "name": "ci/tests", "status": "completed",
            "conclusion": "success",
            "node_id": "CR_a",
            "head_sha": "deadbeef",
            "completed_at": "2026-04-21T10:00:00Z",
        },
        "repository": {"full_name": "acme/w"},
        "sender": {"login": "ci-bot"},
    }
    draft = await handle_github_webhook(
        payload, {"X-GitHub-Event": "check_run"}
    )
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "state_change"
    assert draft.external_id == "CR_a"


async def test_github_unknown_event_type_raises():
    from lib.shared.errors import ValidationError

    with pytest.raises(ValidationError):
        await handle_github_webhook({}, {"X-GitHub-Event": "star"})


async def test_github_missing_event_header_raises():
    from lib.shared.errors import ValidationError

    with pytest.raises(ValidationError):
        await handle_github_webhook({"action": "opened"}, {})


async def test_github_push_entities_hint_includes_repo_and_branch():
    payload = {
        "ref": "refs/heads/release/2026-04",
        "after": "cafef00d",
        "commits": [{"id": "c1"}, {"id": "c2"}],
        "repository": {"full_name": "acme/repo"},
        "sender": {"login": "alice"},
    }
    draft = await handle_github_webhook(
        payload, {"X-GitHub-Event": "push"}
    )
    types = {e["type"] for e in draft.entities_hint}
    assert "github_repo" in types
    assert "github_branch" in types
    assert draft.external_id == "acme/repo@cafef00d"


async def test_github_pr_review_approved_is_authoritative():
    payload = {
        "action": "submitted",
        "review": {
            "state": "approved", "body": "LGTM",
            "node_id": "PRR_a",
            "submitted_at": "2026-04-21T10:00:00Z",
        },
        "pull_request": {"number": 9, "node_id": "PR_x"},
        "repository": {"full_name": "acme/r"},
        "sender": {"login": "bob"},
    }
    draft = await handle_github_webhook(
        payload, {"X-GitHub-Event": "pull_request_review"}
    )
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "state_change"


async def test_github_pr_review_comment_is_inferential():
    payload = {
        "action": "submitted",
        "review": {
            "state": "commented", "body": "nit: rename",
            "node_id": "PRR_b",
        },
        "pull_request": {"number": 9, "node_id": "PR_y"},
        "repository": {"full_name": "acme/r"},
        "sender": {"login": "bob"},
    }
    draft = await handle_github_webhook(
        payload, {"X-GitHub-Event": "pull_request_review"}
    )
    assert draft.trust_tier == "inferential"
    assert draft.kind == "signal"
