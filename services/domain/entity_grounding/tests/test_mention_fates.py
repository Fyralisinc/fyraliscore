from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from services.domain.entity_grounding.mention_fates import (
    _persisted_mention_opportunities,
    _slack_batch_context,
)


def test_persisted_batch_recovers_exact_jira_and_email_surfaces_without_hints() -> None:
    jira = _persisted_mention_opportunities(
        content={"issue_id": "ENG-241"},
        content_text="ENG-241 blocks the Northstar Migration.",
        source_channel="jira:issue",
        has_structural_context=False,
    )
    email = _persisted_mention_opportunities(
        content={"conversation_id": "renewal-thread"},
        content_text="Acme Holdings approved the renewal.",
        source_channel="email:message",
        has_structural_context=False,
    )

    assert jira == ("ENG-241", "Northstar Migration")
    assert email == ("Acme Holdings", "the renewal")


def test_persisted_batch_preserves_hints_and_fills_incomplete_discovery() -> None:
    opportunities = _persisted_mention_opportunities(
        content={"_unresolved_phrases": ["NBI", "NBI"]},
        content_text="NBI depends on Atlas Service.",
        source_channel="jira:issue",
        has_structural_context=False,
    )

    assert opportunities == ("NBI", "Atlas Service")


def test_slack_batch_uses_projected_thread_context_before_admitting_pronoun() -> None:
    tenant_id = uuid4()
    root_id = uuid4()
    reply_id = uuid4()
    unrelated_id = uuid4()
    now = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
    rows = [
        {
            "id": root_id,
            "occurred_at": now,
            "source_channel": "slack:message",
            "content": {
                "channel": "C-DELIVERY",
                "ts": "100.000",
                "thread_ts": "100.000",
                "user": "U-1",
            },
            "content_text": "Northstar Migration is blocked.",
        },
        {
            "id": unrelated_id,
            "occurred_at": now + timedelta(seconds=30),
            "source_channel": "slack:message",
            "content": {
                "channel": "C-RANDOM",
                "ts": "101.000",
                "user": "U-3",
            },
            "content_text": "Atlas Service shipped.",
        },
        {
            "id": reply_id,
            "occurred_at": now + timedelta(minutes=1),
            "source_channel": "slack:message",
            "content": {
                "channel": "C-DELIVERY",
                "ts": "102.000",
                "thread_ts": "100.000",
                "user": "U-2",
            },
            "content_text": "It still needs legal approval.",
        },
    ]

    contexts = _slack_batch_context(rows, tenant_id=tenant_id)
    reply_context = contexts[reply_id]
    opportunities = _persisted_mention_opportunities(
        content=rows[2]["content"],
        content_text=rows[2]["content_text"],
        source_channel="slack:message",
        has_structural_context=any(
            item.inclusion_layer == "source_topology" for item in reply_context
        ),
    )

    assert [item.observation_id for item in reply_context] == [root_id]
    assert reply_context[0].inclusion_layer == "source_topology"
    assert reply_context[0].topology_edge_ids == (
        "slack-thread:100.000->102.000",
    )
    assert opportunities == ("It",)


def test_slack_batch_does_not_promote_unbounded_context_pronoun() -> None:
    opportunities = _persisted_mention_opportunities(
        content={"channel": "C-DELIVERY", "ts": "200.000"},
        content_text="It still needs approval.",
        source_channel="slack:message",
        has_structural_context=False,
    )

    assert opportunities == ()
