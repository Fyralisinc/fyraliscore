"""Shared test fixtures: Acme-Tuesday SubstrateSnapshot + helpers.

Matches the scenario depicted in company-os-design.md §10.2 (active
Tuesday morning). Used by the core/service tests and the Phase-5
sample captures.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from services.rendering.contracts import (
    AnomalyRef,
    CommitmentRef,
    ConversationContext,
    FounderContext,
    ModelRef,
    QueryGridItemSpec,
    ResourceRef,
    StateChange,
    SubstrateSnapshot,
)


TENANT_ID = UUID("00000000-0000-0000-0000-0000000000a1")


def acme_tuesday_snapshot() -> SubstrateSnapshot:
    """Tuesday 06:42 morning. Acme renewal turned unsafe over the weekend."""
    now = datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc)
    sun_0312 = datetime(2026, 4, 19, 3, 12, tzinfo=timezone.utc)
    sat_1903 = datetime(2026, 4, 18, 19, 3, tzinfo=timezone.utc)
    sat_2241 = datetime(2026, 4, 18, 22, 41, tzinfo=timezone.utc)
    thu_end = datetime(2026, 4, 24, 17, 0, tzinfo=timezone.utc)

    top_models = [
        ModelRef(
            id="m-2841",
            claim="Acme renews Q3",
            confidence=0.54,
            prior_confidence=0.81,
            state_changed_at=sun_0312,
            falsifier="two or more contracted deliverables slip past 15 April",
        ),
    ]
    active_commitments = [
        CommitmentRef(
            id="c-187",
            label="ship Acme rate-limiter SLA",
            owner_name="Alice",
            state="Blocked",
            pressure="high",
        ),
        CommitmentRef(
            id="c-203",
            label="Acme dashboard handoff",
            owner_name="Alice",
            state="InProgress",
            pressure="high",
        ),
        CommitmentRef(
            id="decide-acme-path",
            label="choose path A (re-scope) vs path B (extend window)",
            owner_name="rachin",
            state="Open",
            due_at=thu_end,
            pressure="high",
        ),
    ]
    customer_resources = [
        ResourceRef(
            id="r-cust-acme",
            kind="customer",
            name="Acme",
            health="warning",
            revenue_at_risk="$487K",
        ),
        ResourceRef(
            id="r-cust-northwind",
            kind="customer",
            name="Northwind",
            health="healthy",
        ),
    ]
    recent_state_changes = [
        StateChange(
            subject_id="c-187",
            subject_kind="commitment",
            from_state="InProgress",
            to_state="Blocked",
            at=sat_1903,
            reason="linear webhook",
        ),
        StateChange(
            subject_id="c-203",
            subject_kind="commitment",
            from_state="2d",
            to_state="~10d",
            at=sat_2241,
            reason="Alice re-estimate in slack",
        ),
        StateChange(
            subject_id="m-2841",
            subject_kind="model",
            from_state="0.81",
            to_state="0.54",
            at=sun_0312,
            reason="falsifier fired",
        ),
    ]
    anomalies = [
        AnomalyRef(
            id="anom-silence-revenue",
            kind="silence",
            description="0 mentions of Acme slip on #revenue-channel vs 11 on #eng since Fri",
            severity="high",
        ),
    ]
    return SubstrateSnapshot(
        tenant_id=TENANT_ID,
        captured_at=now,
        top_models=top_models,
        active_commitments=active_commitments,
        customer_resources=customer_resources,
        recent_state_changes=recent_state_changes,
        anomalies=anomalies,
        conversation_context=ConversationContext(
            was_here_recently=False, last_visit_at=None, last_queries=[]
        ),
        time_of_day_bucket="morning",
        signals_watched_count=14206,
    )


def quiet_day_snapshot() -> SubstrateSnapshot:
    now = datetime(2026, 4, 23, 7, 15, tzinfo=timezone.utc)
    return SubstrateSnapshot(
        tenant_id=TENANT_ID,
        captured_at=now,
        top_models=[],
        active_commitments=[],
        customer_resources=[
            ResourceRef(id="r-cust-acme", kind="customer", name="Acme", health="healthy")
        ],
        recent_state_changes=[],
        anomalies=[],
        conversation_context=ConversationContext(),
        time_of_day_bucket="morning",
        signals_watched_count=2011,
    )


def founder_rachin() -> FounderContext:
    return FounderContext(
        display_name="Rachin",
        role="ceo",
        observed_rhythms=["reads at 06:42 most weekdays", "checks late on Sunday"],
        recent_interactions=["asked about Acme renewal last Wed"],
    )


def acme_query_grid_specs() -> list[QueryGridItemSpec]:
    return [
        QueryGridItemSpec(
            id="acme-why", icon="why", hot=True, tag="urgent",
            intent="understand why Acme flipped to unsafe",
            query_template="Show me why Acme became unsafe.",
        ),
        QueryGridItemSpec(
            id="acme-board", icon="brief", hot=True, tag="relevant",
            intent="what Acme situation means for Thursday's board update",
            query_template="What this means for Thursday's board update.",
        ),
        QueryGridItemSpec(
            id="monica-brief", icon="draft", hot=False, tag="2min",
            intent="draft a brief for Monica, head of sales, about the Acme slip",
            query_template="Draft a brief for Monica.",
        ),
        QueryGridItemSpec(
            id="miss-yesterday", icon="timeline", hot=False, tag=None,
            intent="retrospective — what did I miss yesterday?",
            query_template="What did I miss yesterday?",
        ),
        QueryGridItemSpec(
            id="beliefs-least", icon="calibration", hot=False, tag=None,
            intent="which founder beliefs are least supported by the substrate right now?",
            query_template="Which of my beliefs are least supported?",
        ),
        QueryGridItemSpec(
            id="silence", icon="observation", hot=False, tag=None,
            intent="where is the company silent where it should be speaking?",
            query_template="Where is the company silent where it shouldn't be?",
        ),
    ]


def acme_card_focus_observation() -> dict:
    return {
        "focus_model_id": "m-2841",
        "focus_resource_id": "r-cust-acme",
        "engineering_mentions_since_friday": 11,
        "revenue_mentions_since_friday": 0,
    }


def acme_card_focus_decision() -> dict:
    return {
        "options": "path_a=re-scope Acme deliverable; path_b=extend renewal window 30 days",
        "deadline": "Thu 24 Apr",
        "at_stake": "$487K",
        "preference": "none",
        "note": "Northwind expansion Model would absorb -0.06 on path B",
    }


def nepal_card_focus_question() -> dict:
    return {
        "standing_days": 41,
        "subject": "goal g-42 (DePIN Nepal)",
        "pattern": "inspection-only: every Observation in last 6 weeks is an inspection event; no Commitments, no Model movement, no Resources deployed",
        "founder_signal": '12 Feb Atlas journal: "the one thing I\'d feel proudest of."',
        "asymmetry": "present intention, absent action, strong emotional anchor",
    }


__all__ = [
    "TENANT_ID",
    "acme_card_focus_decision",
    "acme_card_focus_observation",
    "acme_query_grid_specs",
    "acme_tuesday_snapshot",
    "founder_rachin",
    "nepal_card_focus_question",
    "quiet_day_snapshot",
]
