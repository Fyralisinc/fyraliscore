"""Tests for services/ingestion/handlers/jira.py (IN-17)."""
from __future__ import annotations

import pytest

from services.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingestion.handlers.jira import handle_jira_issue


pytestmark = pytest.mark.asyncio


_SITE = "acme.atlassian.net"


def _issue(**over):
    fields = {
        "summary": "Atlas API returns 500 on cold start",
        "issuetype": {"name": "Bug"},
        "status": {"name": "In Progress"},
        "priority": {"name": "High"},
        "assignee": {"accountId": "5b1", "emailAddress": "bob@acme.com", "displayName": "Bob"},
        "reporter": {"accountId": "5a0", "emailAddress": "alice@acme.com", "displayName": "Alice"},
        "project": {"key": "ENG"},
        "labels": ["backend"],
        "created": "2026-05-01T09:00:00.000+0000",
        "updated": "2026-05-20T12:30:00.000+0000",
        "customfield_10016": 5,
    }
    fields.update(over.pop("fields", {}))
    base = {
        "id": "10001",
        "key": "ENG-42",
        "self": f"https://{_SITE}/rest/api/2/issue/10001",
        "fields": fields,
        "_fyralis_record_type": "issue",
        "_fyralis_site": _SITE,
    }
    base.update(over)
    return base


async def test_handler_registered():
    assert get_handler("jira:issue") is handle_jira_issue
    assert CHANNEL_TRUST_MAP["jira:issue"] == "authoritative"


async def test_issue_record_is_signal_with_versioned_external_id():
    draft = await handle_jira_issue(_issue(), {})
    assert draft.source_channel == "jira:issue"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    # external_id versioned by `updated` so a re-edit lands as a new observation.
    assert draft.external_id == f"jira:{_SITE}:issue:10001:2026-05-20T12:30:00.000+0000"
    assert draft.content["object_type"] == "issue"
    assert draft.content["issue_key"] == "ENG-42"
    assert draft.content["status"] == "In Progress"
    assert draft.content["story_points"] == 5
    assert draft.source_actor_ref == "email:alice@acme.com"
    assert draft.occurred_at.isoformat().startswith("2026-05-20T12:30:00")


async def test_issue_reedit_produces_distinct_external_id():
    """Mutable-source dedup lesson: a later edit must NOT collapse onto the
    earlier observation."""
    d1 = await handle_jira_issue(_issue(), {})
    d2 = await handle_jira_issue(
        _issue(fields={"updated": "2026-05-21T08:00:00.000+0000"}), {},
    )
    assert d1.external_id != d2.external_id


async def test_status_transition_record_is_state_change():
    rec = {
        "_fyralis_record_type": "transition",
        "_fyralis_site": _SITE,
        "_fyralis_issue_id": "10001",
        "_fyralis_issue_key": "ENG-42",
        "history": {
            "id": "90210",
            "created": "2026-05-20T12:30:00.000+0000",
            "author": {"emailAddress": "bob@acme.com", "displayName": "Bob"},
            "items": [
                {"field": "status", "fromString": "To Do", "toString": "In Progress"},
            ],
        },
    }
    draft = await handle_jira_issue(rec, {})
    assert draft.kind == "state_change"
    assert draft.external_id == f"jira:{_SITE}:transition:10001:90210"
    assert draft.content["object_type"] == "transition"
    assert "status" in draft.content["changed_fields"]
    assert "In Progress" in draft.content_text


async def test_non_status_transition_is_signal():
    rec = {
        "_fyralis_record_type": "transition",
        "_fyralis_site": _SITE,
        "_fyralis_issue_id": "10001",
        "_fyralis_issue_key": "ENG-42",
        "history": {
            "id": "90211",
            "created": "2026-05-20T13:00:00.000+0000",
            "items": [{"field": "labels", "fromString": "", "toString": "urgent"}],
        },
    }
    draft = await handle_jira_issue(rec, {})
    assert draft.kind == "signal"


async def test_comment_record_extracts_adf_body():
    rec = {
        "_fyralis_record_type": "comment",
        "_fyralis_site": _SITE,
        "_fyralis_issue_id": "10001",
        "_fyralis_issue_key": "ENG-42",
        "comment": {
            "id": "55",
            "created": "2026-05-20T14:00:00.000+0000",
            "updated": "2026-05-20T14:05:00.000+0000",
            "author": {"emailAddress": "carol@acme.com", "displayName": "Carol"},
            "body": {
                "type": "doc",
                "content": [
                    {"type": "paragraph", "content": [
                        {"type": "text", "text": "Blocked on the infra ticket."},
                    ]},
                ],
            },
        },
    }
    draft = await handle_jira_issue(rec, {})
    assert draft.kind == "signal"
    assert draft.content["object_type"] == "comment"
    assert draft.external_id == f"jira:{_SITE}:comment:55:2026-05-20T14:05:00.000+0000"
    assert draft.content["body"] == "Blocked on the infra ticket."


# --- live webhook path -----------------------------------------------------

async def test_webhook_issue_updated_without_changelog_is_issue():
    payload = {
        "webhookEvent": "jira:issue_updated",
        "issue": {
            "id": "10001",
            "key": "ENG-42",
            "self": f"https://{_SITE}/rest/api/2/issue/10001",
            "fields": {
                "summary": "x", "updated": "2026-05-20T12:30:00.000+0000",
                "status": {"name": "Done"}, "project": {"key": "ENG"},
            },
        },
    }
    draft = await handle_jira_issue(payload, {})
    assert draft.content["object_type"] == "issue"
    # Site derived from issue.self when _fyralis_site is absent.
    assert draft.external_id == f"jira:{_SITE}:issue:10001:2026-05-20T12:30:00.000+0000"


async def test_webhook_issue_updated_with_status_change_is_state_change():
    payload = {
        "webhookEvent": "jira:issue_updated",
        "user": {"emailAddress": "bob@acme.com", "displayName": "Bob"},
        "issue": {
            "id": "10001", "key": "ENG-42",
            "self": f"https://{_SITE}/rest/api/2/issue/10001",
            "fields": {"updated": "2026-05-20T12:30:00.000+0000"},
        },
        "changelog": {
            "id": "90210",
            "items": [{"field": "status", "fromString": "To Do", "toString": "Done"}],
        },
    }
    draft = await handle_jira_issue(payload, {})
    assert draft.kind == "state_change"
    # external_id parity with the backfilled transition record.
    assert draft.external_id == f"jira:{_SITE}:transition:10001:90210"


async def test_webhook_comment_created():
    payload = {
        "webhookEvent": "comment_created",
        "issue": {"id": "10001", "key": "ENG-42", "self": f"https://{_SITE}/x/10001"},
        "comment": {
            "id": "55", "updated": "2026-05-20T14:05:00.000+0000",
            "author": {"emailAddress": "carol@acme.com"},
            "body": "looks good",
        },
    }
    draft = await handle_jira_issue(payload, {})
    assert draft.content["object_type"] == "comment"
    assert draft.external_id == f"jira:{_SITE}:comment:55:2026-05-20T14:05:00.000+0000"


async def test_backfill_and_webhook_issue_dedup_to_same_external_id():
    """A backfilled issue and its live-webhook twin (same id + updated)
    collapse to one observation."""
    backfill = await handle_jira_issue(_issue(), {})
    webhook = await handle_jira_issue({
        "webhookEvent": "jira:issue_updated",
        "issue": {
            "id": "10001", "key": "ENG-42",
            "self": f"https://{_SITE}/rest/api/2/issue/10001",
            "fields": {"summary": "x", "updated": "2026-05-20T12:30:00.000+0000"},
        },
    }, {})
    assert backfill.external_id == webhook.external_id
