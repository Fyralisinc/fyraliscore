"""Unit tests for MockRenderingAdapter. No DB required.

The mock is deterministic and synchronous-under-await; we lock its
output shape so the scheduler has stable contracts while Agent-RND is
under development.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4


import httpx
import pytest

from lib.shared.circuit_breaker import AsyncCircuitBreaker, CircuitState
from lib.shared.errors import DependencyUnavailableError
from lib.shared.http_retry import HttpRetryConfig, is_retryable_httpx_error
from services.product.greeting.rendering_adapter import (
    HttpRenderingAdapter,
    MockRenderingAdapter,
    build_rendering_adapter,
)
from services.product.greeting.scheduler import GreetingScheduler
from services.product.greeting.snapshot import (
    AnomalyRef,
    CommitmentRef,
    ConversationContext,
    FounderContext,
    QueryGridSnapshot,
    StateChange,
    SubstrateSnapshot,
)


def _rendering_breaker(name: str, *, min_samples: int = 2) -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker(
        name=name,
        failure_threshold=0.5,
        min_samples=min_samples,
        open_duration=30.0,
        record_exception=lambda exc: not isinstance(exc, httpx.HTTPStatusError)
        or is_retryable_httpx_error(exc),
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


def test_build_rendering_adapter_warns_before_dev_mock_fallback(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    monkeypatch.delenv("GRT_RENDERING_BASE_URL", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "dev")
    monkeypatch.setenv("COMPANY_OS_ENV", "dev")

    with caplog.at_level(logging.WARNING):
        adapter = build_rendering_adapter()

    assert isinstance(adapter, MockRenderingAdapter)
    assert "GRT_RENDERING_BASE_URL is unset" in caplog.text


def test_build_rendering_adapter_fails_closed_in_prod(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("GRT_RENDERING_BASE_URL", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    with pytest.raises(RuntimeError, match="GRT_RENDERING_BASE_URL is unset"):
        build_rendering_adapter()


def test_build_rendering_adapter_uses_http_when_configured(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GRT_RENDERING_BASE_URL", "http://rendering:8000")
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    assert isinstance(build_rendering_adapter(), HttpRenderingAdapter)


def test_scheduler_default_renderer_uses_prod_guard(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("GRT_RENDERING_BASE_URL", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    with pytest.raises(RuntimeError, match="GRT_RENDERING_BASE_URL is unset"):
        GreetingScheduler(object())


async def test_http_rendering_adapter_retries_transient_status() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"error": "busy"})
        return httpx.Response(
            200,
            json={
                "body_html": "<p>good morning</p>",
                "meta": {"signals_watched_count": 3},
                "rendering_model_used": "rnd",
                "cost_usd": 0.02,
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sleep = AsyncMock()
    adapter = HttpRenderingAdapter(
        "http://rendering",
        client=client,
        retry_config=HttpRetryConfig(
            max_attempts=2,
            initial_backoff_s=0.01,
            max_backoff_s=0.01,
            jitter_ratio=0,
        ),
        sleep=sleep,
    )
    try:
        rendered = await adapter.render_greeting(_empty_snapshot(), _founder())
    finally:
        await client.aclose()

    assert calls == 2
    sleep.assert_awaited_once_with(0.01)
    assert rendered.body_html == "<p>good morning</p>"
    assert rendered.signals_watched_count == 3


async def test_http_rendering_adapter_reuses_owned_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "body_html": "<p>pooled</p>",
                "meta": {"signals_watched_count": 2},
                "rendering_model_used": "rnd",
                "cost_usd": 0.01,
            }

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.posts = []
            self.closed = False
            created.append(self)

        async def post(self, url: str, json: dict):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        "services.product.greeting.rendering_adapter.httpx.AsyncClient",
        FakeAsyncClient,
    )
    adapter = HttpRenderingAdapter("http://rendering", timeout_s=7.5)

    await adapter.render_greeting(_empty_snapshot(), _founder())
    await adapter.render_greeting(_empty_snapshot(), _founder())

    assert len(created) == 1
    assert created[0].kwargs["timeout"] == 7.5
    assert len(created[0].posts) == 2
    assert created[0].closed is False

    await adapter.aclose()
    assert created[0].closed is True


async def test_http_rendering_adapter_raises_structured_unavailable_after_retries() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "busy"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sleep = AsyncMock()
    adapter = HttpRenderingAdapter(
        "http://rendering",
        client=client,
        retry_config=HttpRetryConfig(
            max_attempts=2,
            initial_backoff_s=0.01,
            max_backoff_s=0.01,
            jitter_ratio=0,
        ),
        sleep=sleep,
    )
    try:
        with pytest.raises(DependencyUnavailableError) as exc_info:
            await adapter.render_greeting(_empty_snapshot(), _founder())
    finally:
        await client.aclose()

    err = exc_info.value
    assert calls == 2
    assert err.code == "dependency_unavailable"
    assert err.recoverable is True
    assert err.context["dependency"] == "rendering"
    assert err.context["operation"] == "rendering/greeting"
    assert err.context["attempts"] == 2
    assert err.context["status_code"] == 503


async def test_http_rendering_adapter_fast_fails_when_breaker_is_open() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "busy"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sleep = AsyncMock()
    breaker = _rendering_breaker("greeting_rendering_open_test", min_samples=2)
    adapter = HttpRenderingAdapter(
        "http://rendering",
        client=client,
        retry_config=HttpRetryConfig(
            max_attempts=1,
            initial_backoff_s=0.01,
            max_backoff_s=0.01,
            jitter_ratio=0,
        ),
        breaker=breaker,
        sleep=sleep,
    )
    try:
        for _ in range(2):
            with pytest.raises(DependencyUnavailableError):
                await adapter.render_greeting(_empty_snapshot(), _founder())

        assert breaker.state == CircuitState.OPEN
        with pytest.raises(DependencyUnavailableError) as exc_info:
            await adapter.render_greeting(_empty_snapshot(), _founder())
    finally:
        await client.aclose()

    err = exc_info.value
    assert calls == 2
    assert err.context["circuit_open"] is True
    assert err.context["attempts"] == 0
    assert err.context["breaker"] == "greeting_rendering_open_test"


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
    assert cl.calibration_pct == 0
    assert "calibration warming" in cl.body


async def test_mock_render_close_line_uses_snapshot_calibration():
    adapter = MockRenderingAdapter()
    base = _empty_snapshot()
    snap = SubstrateSnapshot(
        tenant_id=base.tenant_id,
        captured_at=base.captured_at,
        top_models=base.top_models,
        active_commitments=base.active_commitments,
        customer_resources=base.customer_resources,
        recent_state_changes=base.recent_state_changes,
        anomalies=base.anomalies,
        conversation_context=base.conversation_context,
        time_of_day_bucket=base.time_of_day_bucket,
        calibration_pct=82,
        calibration_sample_count=5,
    )

    cl = await adapter.render_close_line(snap, _founder())

    assert cl.calibration_pct == 82
    assert "calibration 82%" in cl.body
    assert "74" not in cl.body


async def test_http_render_close_line_short_circuits_without_calibration_samples():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"body": "should not be used"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = HttpRenderingAdapter("http://rendering", client=client)
    try:
        cl = await adapter.render_close_line(_empty_snapshot(), _founder())
    finally:
        await client.aclose()

    assert calls == 0
    assert cl.rendering_model_used == "local-close-line/1"
    assert cl.calibration_pct == 0
    assert "calibration warming" in cl.body
