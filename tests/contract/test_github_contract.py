"""Contract test: GitHub PR lifecycle deliveries don't collapse onto one
observation (finding #1).

A PR's `node_id` is a stable GraphQL global id that GitHub reuses BYTE-FOR-BYTE
across the PR's `opened` → `closed`/`merged` deliveries. The pre-fix handler
adopted `node_id` verbatim as the external_id, so the close/merge state-change
deduped onto the open (dedup is on `(source_channel, external_id)` ignoring
occurred_at) and was silently lost. The fix encodes the action:
`external_id = {node_id}:{action}`. This test pins that against two doc-sourced
fixtures sharing one node_id.

Verified against docs.github.com pull_request webhook payloads.
"""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers.github import handle_github_webhook
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_HEADERS = {"X-GitHub-Event": "pull_request"}


def _opened():
    return load_fixture("github", "webhook", "pull_request_opened").body


def _closed():
    return load_fixture("github", "webhook", "pull_request_closed").body


async def test_same_node_id_across_lifecycle():
    """The two fixtures are the SAME PR — identical node_id — at different
    lifecycle points. (If this drifts, the rest of the test is meaningless.)"""
    assert _opened()["pull_request"]["node_id"] == _closed()["pull_request"]["node_id"]


async def test_opened_and_closed_are_distinct_observations():
    opened = await handle_github_webhook(_opened(), _HEADERS)
    closed = await handle_github_webhook(_closed(), _HEADERS)

    node_id = _opened()["pull_request"]["node_id"]
    # external_id encodes the action → the merge is NOT lost to dedup.
    assert opened.external_id == f"{node_id}:opened"
    assert closed.external_id == f"{node_id}:closed"
    assert opened.external_id != closed.external_id
    # the merge is the system-of-record state change
    assert closed.kind == "state_change"
    assert closed.trust_tier == "authoritative"


async def test_redelivery_of_same_action_dedups():
    """A GitHub redelivery of the SAME action (at-least-once webhooks) must
    still produce the SAME external_id so the observation layer dedups it."""
    a = await handle_github_webhook(_opened(), _HEADERS)
    b = await handle_github_webhook(_opened(), _HEADERS)
    assert a.external_id == b.external_id
