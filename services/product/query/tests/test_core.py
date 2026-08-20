"""Tests for services/product/query/core.QueryHandler.

Retrieval is replaced with FakeStrategy via monkeypatching the
strategies registry. Rendering is replaced with FakeRenderingAdapter.
Classification is replaced with ScriptedClassifier.

This keeps tests fast and hermetic — no DB, no LLM, no network.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from services.platform.access_control.authority import Principal, authority_fingerprint
from services.product.query import strategies as strat_pkg
from services.product.query.adapters import InMemoryCacheAdapter
from services.product.query.core import (
    AnswerQueryRequest,
    CardContext,
    QueryHandler,
    Turn,
)
from services.product.query.tests._helpers import (
    FakeRenderingAdapter,
    FakeStrategy,
    ScriptedClassifier,
    fake_conn_provider,
)


TENANT = uuid4()
VIEWER = uuid4()


def _auth_key(viewer_id=VIEWER) -> str:
    return authority_fingerprint(
        Principal(tenant_id=TENANT, actor_id=viewer_id),
        "ask",
    ).cache_key


@pytest.fixture
def fake_strategies(monkeypatch):
    """Replace every strategy in the registry with FakeStrategy."""
    replacements = {
        cat: FakeStrategy(category=cat)
        for cat in strat_pkg.STRATEGIES.keys()
    }
    monkeypatch.setattr(strat_pkg, "STRATEGIES", replacements, raising=True)
    # Also patch the import in strategies package used by core.get_strategy.
    from services.product.query import strategies as strategies_mod
    monkeypatch.setattr(strategies_mod, "STRATEGIES", replacements, raising=True)
    yield replacements


@pytest.fixture
def handler(fake_strategies):
    return QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="arbitrary"),
        rendering_adapter=FakeRenderingAdapter(),
        cache_adapter=InMemoryCacheAdapter(),
    )


# --------------------------------------------------------------- happy path


async def test_handler_returns_response(handler):
    req = AnswerQueryRequest(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        query="tell me about Acme",
    )
    resp = await handler.answer_query(req)
    assert isinstance(resp.turn_id, UUID)
    assert resp.query_echo == "tell me about Acme"
    assert resp.response_html  # non-empty
    assert resp.category == "arbitrary"
    assert resp.latency_ms >= 0
    assert resp.rendering_cost_usd == Decimal("0.001")


async def test_handler_empty_query_raises(handler):
    req = AnswerQueryRequest(tenant_id=TENANT, viewer_id=VIEWER, query="   ")
    with pytest.raises(Exception):
        await handler.answer_query(req)


async def test_handler_close_closes_rendering_adapter(fake_strategies):
    class ClosableRenderingAdapter(FakeRenderingAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    renderer = ClosableRenderingAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="arbitrary"),
        rendering_adapter=renderer,
        cache_adapter=InMemoryCacheAdapter(),
    )

    await handler.aclose()

    assert renderer.closed is True


async def test_handler_passes_history_to_rendering(fake_strategies):
    renderer = FakeRenderingAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="arbitrary"),
        rendering_adapter=renderer,
    )
    history = [
        Turn(
            turn_id=uuid4(),
            query="why is Acme at risk?",
            response_html="<p>previous</p>",
            category="why",
            created_at=datetime.now(timezone.utc),
        ),
    ]
    req = AnswerQueryRequest(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        query="follow-up: what next?",
        conversation_history=history,
    )
    await handler.answer_query(req)
    assert len(renderer.calls) == 1
    assert len(renderer.calls[0].conversation_history) == 1
    assert renderer.calls[0].conversation_history[0]["query"] == "why is Acme at risk?"


async def test_handler_three_turn_followup_maintains_context(fake_strategies):
    """Integration test: three-turn conversation; each turn sees the
    accumulated history."""
    renderer = FakeRenderingAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="arbitrary"),
        rendering_adapter=renderer,
    )
    history: list[Turn] = []
    queries = ["what about Acme?", "why is it at risk?", "what should I do?"]
    for q in queries:
        req = AnswerQueryRequest(
            tenant_id=TENANT,
            viewer_id=VIEWER,
            query=q,
            conversation_history=list(history),
        )
        resp = await handler.answer_query(req)
        history.append(Turn(
            turn_id=resp.turn_id,
            query=q,
            response_html=resp.response_html,
            category=resp.category,
            created_at=resp.computed_at,
        ))

    # Final render request saw the full 2-turn history.
    assert len(renderer.calls) == 3
    assert len(renderer.calls[-1].conversation_history) == 2
    assert renderer.calls[-1].conversation_history[0]["query"] == "what about Acme?"
    assert renderer.calls[-1].conversation_history[1]["query"] == "why is it at risk?"


# --------------------------------------------------------------- card context


async def test_handler_uses_inline_card_context(fake_strategies):
    renderer = FakeRenderingAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="draft"),
        rendering_adapter=renderer,
    )
    card = CardContext(
        card_id=uuid4(),
        subject="Acme renewal",
        recipient="monica",
        kind="observation",
    )
    req = AnswerQueryRequest(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        query="draft an update",
        context_card_id=card.card_id,
        inline_card_context=card,
    )
    resp = await handler.answer_query(req)
    assert resp.category == "draft"
    assert len(renderer.calls) == 1
    rc = renderer.calls[0].card_context
    assert rc is not None
    assert rc["subject"] == "Acme renewal"
    assert rc["recipient"] == "monica"


async def test_handler_resolves_card_context_via_resolver(fake_strategies):
    card_id = uuid4()
    call_args: list[tuple[UUID, UUID]] = []

    async def resolver(tenant_id: UUID, cid: UUID):
        call_args.append((tenant_id, cid))
        return CardContext(
            card_id=cid,
            subject="customer health",
            kind="observation",
        )

    renderer = FakeRenderingAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="why"),
        rendering_adapter=renderer,
        card_resolver=resolver,
    )
    req = AnswerQueryRequest(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        query="why?",
        context_card_id=card_id,
    )
    await handler.answer_query(req)
    assert call_args == [(TENANT, card_id)]
    rc = renderer.calls[0].card_context
    assert rc["subject"] == "customer health"


# --------------------------------------------------------------- prefetch cache


async def test_handler_caches_under_query_id_when_provided(fake_strategies):
    cache = InMemoryCacheAdapter()
    renderer = FakeRenderingAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="arbitrary"),
        rendering_adapter=renderer,
        cache_adapter=cache,
    )
    req = AnswerQueryRequest(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        query="why is Acme at risk?",
        query_id="chip_123",
    )
    await handler.answer_query(req)
    row = await cache.get(TENANT, f"query_prefetch:{_auth_key()}:chip_123")
    assert row is not None
    assert row["content"]["query_echo"] == "why is Acme at risk?"


async def test_try_serve_from_prefetch_roundtrip(fake_strategies):
    cache = InMemoryCacheAdapter()
    renderer = FakeRenderingAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="arbitrary"),
        rendering_adapter=renderer,
        cache_adapter=cache,
    )
    # Warm the cache.
    await handler.answer_query(AnswerQueryRequest(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        query="q1",
        query_id="chip_A",
    ))
    assert len(renderer.calls) == 1

    # Hit the cache — no additional render call.
    cached = await handler.try_serve_from_prefetch(TENANT, VIEWER, "chip_A")
    assert cached is not None
    assert cached.query_echo == "q1"
    assert len(renderer.calls) == 1  # no new render call

    # Another actor with the same query_id cannot use this viewer's cache.
    other_viewer = uuid4()
    actor_miss = await handler.try_serve_from_prefetch(
        TENANT, other_viewer, "chip_A"
    )
    assert actor_miss is None

    # Miss.
    miss = await handler.try_serve_from_prefetch(TENANT, VIEWER, "chip_missing")
    assert miss is None


# --------------------------------------------------------------- latency


async def test_handler_records_latency_breakdown(fake_strategies):
    renderer = FakeRenderingAdapter(latency_ms=5)
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier(category="arbitrary"),
        rendering_adapter=renderer,
    )
    resp = await handler.answer_query(AnswerQueryRequest(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        query="test",
    ))
    assert resp.retrieval_trace.latency_ms_render >= 0
    assert resp.retrieval_trace.latency_ms_total >= resp.retrieval_trace.latency_ms_render
    # Non-prefetched path should be under the 5s budget (in ms).
    assert resp.latency_ms < 5000
