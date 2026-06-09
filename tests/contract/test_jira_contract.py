"""Contract test: the Jira webhook path emits an observation for a REAL
`jira:issue_updated` delivery whose changelog is a NON-status field change.

Guards the Phase-3 drift fix (finding #32): real Jira Cloud `jira:issue_updated`
webhooks carry a `changelog` for ANY field edit (summary, assignee, priority,
labels, …), not just status/resolution. The handler previously routed only
status/resolution changelogs to the transition builder and dropped the rest onto
the issue snapshot — losing the per-field before/after and, when a webhook omits
`issue.fields`, dropping the change entirely. The fix routes a non-status
changelog through the SAME transition builder, downgraded to kind="signal", and
captures the changed field. Verified against developer.atlassian.com/cloud/jira.
"""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers.jira import handle_jira_issue
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_SITE = "acme.atlassian.net"


def _fixture():
    return load_fixture("jira", "webhook", "issue_updated_nonstatus")


async def test_fixture_is_a_nonstatus_issue_updated():
    body = _fixture().body
    # The real delivery is an issue_updated event whose changelog carries no
    # status/resolution item — only a non-status field change.
    assert body["webhookEvent"] == "jira:issue_updated"
    fields = {i["field"] for i in body["changelog"]["items"]}
    assert "status" not in fields and "resolution" not in fields
    assert "summary" in fields


async def test_handler_emits_observation_capturing_changed_field():
    body = _fixture().body
    draft = await handle_jira_issue(body, {})

    # An observation IS produced (the change is no longer dropped).
    assert draft is not None
    assert draft.source_channel == "jira:issue"
    assert draft.trust_tier == "authoritative"

    # A non-status changelog is a plain signal, NOT a state_change (that path
    # is reserved for status/resolution transitions and is unchanged).
    assert draft.kind == "signal"

    # The changed field is captured (field name + the per-field before/after).
    assert draft.content["object_type"] == "transition"
    assert "summary" in draft.content["changed_fields"]
    changed = {i["field"]: i for i in draft.content["items"]}
    assert changed["summary"]["toString"] == "New title"
    assert changed["summary"]["fromString"] == "Old title"
    assert "summary" in draft.content_text


async def test_external_id_is_stable_site_issue_changelog_namespaced():
    body = _fixture().body
    draft = await handle_jira_issue(body, {})

    issue_id = body["issue"]["id"]
    changelog_id = body["changelog"]["id"]
    # Site host derived from issue.self; versioned by the (immutable) changelog
    # id so the field edit dedups with its backfilled transition twin.
    assert draft.external_id == f"jira:{_SITE}:transition:{issue_id}:{changelog_id}"

    # Idempotent: re-delivering the same webhook yields the same external_id.
    again = await handle_jira_issue(_fixture().body, {})
    assert again.external_id == draft.external_id
