"""Week-5 integration: render cost attribution lands non-zero rows.

Item 2 from the Week-5 Stabilization brief. In Week-4 the live DeepSeek
run produced rows for `greeting / query_grid / close_line` in
`view_render_costs` with `$0.00` tokens + cost, while `card_observation`
rows were non-zero. Root cause was that the provider-instance usage
aggregator (`LLMProvider._usage_aggregator`) is shared mutable state;
under concurrent render fan-out one call's `finally: set_usage_aggregator(None)`
cleared the aggregator another sibling was depending on, silently dropping
usage. Week-5 fixes this by threading the aggregator via a ContextVar
(`lib.llm.provider.using_usage_aggregator`), making it task-local.

These tests use a ScriptedProvider that emits a fake usage record, mount
a real asyncpg pool, and assert `view_render_costs` rows land with
non-zero tokens + cost — including under concurrent invocation, which is
the original failure mode.

Skipped automatically when DATABASE_URL is absent (see conftest.py).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest


pytestmark = pytest.mark.integration


def _now() -> datetime:
    return datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_render_greeting_writes_nonzero_cost_row(fresh_db):
    """Single greeting render → `view_render_costs` row with non-zero
    tokens + cost. Exercises the non-concurrent path end-to-end through
    the DB write."""
    from services.rendering.contracts import RenderGreetingRequest
    from services.rendering.core import RenderingService
    from services.rendering.tests.fixtures import (
        TENANT_ID, acme_tuesday_snapshot, founder_rachin,
    )
    from services.rendering.tests.scripted import ScriptedProvider

    greeting_html = (
        "Good morning. One thing is worth your attention before the day "
        "starts \u2014 Acme's renewal is <span class=\"serif\">structurally "
        "unsafe</span> as of Sunday, and revenue hasn't caught it yet. One "
        "decision is on you by Thursday. Everything else is handled."
    )

    provider = ScriptedProvider([greeting_html])
    svc = RenderingService(provider=provider, pool=fresh_db)
    req = RenderGreetingRequest(
        tenant_id=TENANT_ID,
        timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        founder_context=founder_rachin(),
    )
    resp = await svc.render_greeting(req)
    assert resp.cost_usd > Decimal("0")
    assert resp.flagged is False

    row = await _wait_for_cost_row(
        fresh_db, tenant_id=TENANT_ID, render_kind="greeting",
    )
    assert row is not None, "view_render_costs row for greeting was not persisted"
    assert row["llm_input_tokens_total"] > 0, "greeting input tokens were zero"
    assert row["llm_output_tokens_total"] > 0, "greeting output tokens were zero"
    assert row["llm_cost_usd"] > 0, "greeting cost_usd landed as $0.00"


@pytest.mark.asyncio
async def test_concurrent_renders_all_land_nonzero_cost_rows(fresh_db):
    """The original Week-4 failure: under concurrent render fan-out on
    a shared provider, some siblings' cost rows landed as `$0.00`.

    Fan out 3 renders (greeting + card_observation + close_line) on a
    single shared RenderingService / provider, then assert every
    `view_render_costs` row has non-zero tokens + cost.
    """
    from services.rendering.contracts import (
        RenderCardRequest, RenderCloseLineRequest, RenderGreetingRequest,
    )
    from services.rendering.core import RenderingService
    from services.rendering.tests.fixtures import (
        TENANT_ID, acme_card_focus_observation, acme_tuesday_snapshot,
        founder_rachin,
    )
    from services.rendering.tests.scripted import ScriptedProvider

    greeting_html = (
        "Good morning. One thing is worth your attention before the day "
        "starts \u2014 Acme's renewal is <span class=\"serif\">structurally "
        "unsafe</span> as of Sunday, and revenue hasn't caught it yet. One "
        "decision is on you by Thursday. Everything else is handled."
    )
    obs_html = (
        "Acme's renewal is <span class=\"serif-hot\">structurally unsafe</span>. "
        "Confidence dropped <span class=\"n\">0.81 \u2192 0.54</span> after two "
        "contracted deliverables slipped. Engineering has discussed this "
        "<span class=\"n\">11 times</span> since Friday; the revenue channel "
        "has <span class=\"hl\">zero mentions</span>. Revenue at risk: "
        "<span class=\"n\">$487K</span>."
    )
    close_html = "That's the signal. You can go."

    # Single shared provider — matches the production path where the
    # gateway builds one RenderingService._service_singleton.
    provider = ScriptedProvider([greeting_html, obs_html, close_html])
    svc = RenderingService(provider=provider, pool=fresh_db)

    greeting_req = RenderGreetingRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        founder_context=founder_rachin(),
    )
    card_req = RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=_now(), kind="observation",
        substrate_state=acme_tuesday_snapshot(),
        card_focus=acme_card_focus_observation(),
        founder_context=founder_rachin(),
    )
    close_req = RenderCloseLineRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        signals_watched_count=14206, external_moves=3, calibration_pct=73,
    )

    results = await asyncio.gather(
        svc.render_greeting(greeting_req),
        svc.render_card_observation(card_req),
        svc.render_close_line(close_req),
    )
    for r in results:
        assert r.cost_usd > Decimal("0"), f"cost was zero for {type(r).__name__}"

    # Every DB row has positive tokens + cost.
    for kind in ("greeting", "card_observation", "close_line"):
        row = await _wait_for_cost_row(
            fresh_db, tenant_id=TENANT_ID, render_kind=kind,
        )
        assert row is not None, f"view_render_costs row missing for {kind}"
        assert row["llm_input_tokens_total"] > 0, f"{kind} input tokens were zero"
        assert row["llm_output_tokens_total"] > 0, f"{kind} output tokens were zero"
        assert row["llm_cost_usd"] > 0, f"{kind} cost_usd landed as $0.00"


async def _wait_for_cost_row(pool, *, tenant_id: UUID, render_kind: str):
    """`_record_cost` fires-and-forgets the INSERT via `loop.create_task`.
    Yield the loop and retry a handful of times so the background task
    can land before we assert."""
    for _ in range(40):
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT render_kind, llm_input_tokens_total,
                       llm_output_tokens_total, llm_cost_usd
                FROM view_render_costs
                WHERE tenant_id = $1 AND render_kind = $2
                ORDER BY computed_at DESC
                LIMIT 1
                """,
                tenant_id,
                render_kind,
            )
        if row is not None:
            return row
        await asyncio.sleep(0.05)
    return None
