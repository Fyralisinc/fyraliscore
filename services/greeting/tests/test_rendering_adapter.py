"""Unit tests for MockRenderingAdapter. No DB required.

The mock is deterministic and synchronous-under-await; we lock its
output shape so the scheduler has stable contracts while Agent-RND is
under development.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


from services.greeting.rendering_adapter import MockRenderingAdapter
from services.greeting.snapshot import (
    AnomalyRef,
    CommitmentRef,
    ConversationContext,
    FounderContext,
    QueryGridSnapshot,
    StateChange,
    SubstrateSnapshot,
)


def _empty_snapshot() -> SubstrateSnapshot:
    return SubstrateSnapshot(
        tenant_id=uuid4(),
        captured_at=datetime(2026, 4, 22, 6, 30, tzinfo=timezone.utc),
        top_models=[],
        active_commitments=[],
        customer_resources=[],
        recent_state_changes=[],
        anomalies=[],
        conversation_context=ConversationContext(),
        time_of_day_bucket="early_morning",
    )


def _founder() -> FounderContext:
    return FounderContext(
        tenant_id=uuid4(),
        role="ceo",
        display_name="Test CEO",
        timezone_name="Asia/Kathmandu",
    )


async def test_mock_greeting_quiet():
    adapter = MockRenderingAdapter()
    snap = _empty_snapshot()
    r = await adapter.render_greeting(snap, _founder())
    assert r.body_html
    assert "Good morning" in r.body_html
    assert "normal metabolism" in r.body_html
    assert r.signals_watched_count == 0


async def test_mock_greeting_with_activity():
    adapter = MockRenderingAdapter()
    snap = _empty_snapshot()
    snap_with_anomaly = SubstrateSnapshot(
        tenant_id=snap.tenant_id,
        captured_at=snap.captured_at,
        top_models=[],
        active_commitments=[
            CommitmentRef(
                id=uuid4(), title="blocked thing", state="blocked",
                owner_id=None, due_date=None, priority=3,
                is_critical_path=True, days_to_due=1,
                last_state_change_at=snap.captured_at,
            )
        ],
        customer_resources=[],
        recent_state_changes=[],
        anomalies=[
            AnomalyRef(
                id=uuid4(), kind="customer_health_degraded",
                region={}, significance=0.8,
                published_at=snap.captured_at,
            )
        ],
        conversation_context=ConversationContext(),
        time_of_day_bucket="early_morning",
    )
    r = await adapter.render_greeting(snap_with_anomaly, _founder())
    assert "customer health degraded" in r.body_html
    assert "blocked" in r.body_html
    assert r.signals_watched_count >= 2


async def test_mock_render_card_observation():
    adapter = MockRenderingAdapter()
    snap = SubstrateSnapshot(
        tenant_id=uuid4(),
        captured_at=datetime(2026, 4, 22, tzinfo=timezone.utc),
        top_models=[],
        active_commitments=[],
        customer_resources=[],
        recent_state_changes=[
            StateChange(
                observation_id=uuid4(), entity_id=uuid4(),
                entity_kind="model", kind="insert_model",
                occurred_at=datetime(2026, 4, 22, tzinfo=timezone.utc),
            )
        ],
        anomalies=[],
        conversation_context=ConversationContext(
            recent_queries=[{"card_candidate": {
                "kind": "anomaly", "id": str(uuid4()),
                "subject_kind": "resource_over_deployed",
                "significance": 0.7,
            }}]
        ),
        time_of_day_bucket="early_morning",
    )
    card = await adapter.render_card(snap, _founder(), "observation")
    assert card.kind == "observation"
    assert card.tag_color == "hot"
    assert "observation" in card.id.lower()
    assert card.body_html
    assert any(v["id"] == "why" for v in card.verbs)


async def test_mock_render_query_grid():
    adapter = MockRenderingAdapter()
    grid = QueryGridSnapshot(
        tenant_id=uuid4(),
        captured_at=datetime(2026, 4, 22, tzinfo=timezone.utc),
        situation_queries=[
            {
                "id": "s1",
                "icon": "why",
                "label": "why X",
                "tag": "urgent",
                "hot": True,
            }
        ],
        evergreen_queries=[
            {
                "id": "e1",
                "icon": "timeline",
                "label": "what changed",
                "tag": "evergreen",
                "hot": False,
            }
        ],
        time_of_day_bucket="morning",
    )
    r = await adapter.render_query_grid(grid, _founder())
    assert len(r.queries) == 2
    assert r.queries[0]["hot"] is True
    assert r.queries[1]["hot"] is False


async def test_mock_render_close_line():
    adapter = MockRenderingAdapter()
    snap = _empty_snapshot()
    cl = await adapter.render_close_line(snap, _founder())
    assert cl.body
    assert isinstance(cl.signal_count, int)
    assert isinstance(cl.calibration_pct, int)
