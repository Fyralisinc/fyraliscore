"""FU-3 integration test — Think worker cost tracking end-to-end.

AUDIT-FIXES-IMPLEMENTATION-PLAN FU-3: confirms that the Think
pipeline (reason.think) installs an `LLMUsageAggregator` on the
provider for the run, records token usage across the trigger, and
writes a `think_run_costs` row via `record_think_run_cost`.

Strategy:
  * Subclass ScriptedProvider to call `_record_usage(inp, outp)` from
    `_raw_call` so the aggregator accumulates (real providers do the
    same from their SDK response metadata — see provider.py
    `_extract_anthropic_usage` / `_extract_openai_usage`).
  * Drive a real T1 trigger through `think()`. Assert:
      - outcome.llm_calls_count > 0
      - think_run_costs row exists for trigger_id with
        llm_calls_count > 0 and correct outcome.
      - cost row survives commit (i.e. row is still there after the
        trigger's transaction closes).
  * Test cleans up its own think_run_costs row at the end.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from lib.llm.provider import LLMConfig
from lib.shared.ids import uuid7

from services.retrieval.primary import TriggerContext
from services.think.reason import think
from services.think.tests.conftest import ScriptedProvider, make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class UsageEmittingScriptedProvider(ScriptedProvider):
    """ScriptedProvider variant that also records token usage into the
    installed aggregator — mimicking real provider `_raw_call` which
    calls `_record_usage(inp, outp)` from SDK response metadata."""

    def __init__(
        self,
        responses=None,
        cfg=None,
        *,
        input_tokens_per_call: int = 1234,
        output_tokens_per_call: int = 567,
    ):
        super().__init__(responses=responses, cfg=cfg)
        self.input_tokens_per_call = input_tokens_per_call
        self.output_tokens_per_call = output_tokens_per_call

    async def _raw_call(
        self, *, system, user, temperature, max_tokens, schema_hint,
    ):
        raw = await super()._raw_call(
            system=system, user=user, temperature=temperature,
            max_tokens=max_tokens, schema_hint=schema_hint,
        )
        # Bridge usage like a real provider's SDK-response extractor.
        self._record_usage(
            self.input_tokens_per_call,
            self.output_tokens_per_call,
        )
        return raw


async def _seed_observation(
    pool, tenant: UUID,
    *, content_text: str = "event", external_id: str = "fu3-1",
) -> UUID:
    aid = uuid7()
    oid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'Alice', 'active')",
            aid, tenant,
        )
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending,
               trust_tier, external_id)
            VALUES ($1, $2, now(), 'signal', 'fu3', $3,
                    '{}'::jsonb, $4, $5, FALSE, 'authoritative', $6)
            """,
            oid, tenant, aid, content_text,
            make_embedding(content_text), external_id,
        )
    return oid


def _scripted_empty_diff(trigger_id: UUID, tenant: UUID) -> str:
    return json.dumps({
        "trigger_ref": str(trigger_id),
        "tenant_id": str(tenant),
        "claim_ops": [],
        "act_ops": [],
        "resource_ops": [],
        "new_predictions": [],
        "reasoning_trace": "fu3 test diff",
    })


async def test_fu3_think_records_cost_row_on_success(
    fresh_db, tenant, tenant_cleanup,
):
    """Happy-path T1 → think_run_costs row appears with nonzero call
    count + tokens + cost."""
    trigger_id = uuid7()
    obs = await _seed_observation(fresh_db, tenant)
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_entity_ids=[],
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[],
        seed_signature={"trigger_id": str(trigger_id)},
    )
    provider = UsageEmittingScriptedProvider(
        responses=[_scripted_empty_diff(trigger_id, tenant)],
        cfg=LLMConfig(
            provider="deepseek",
            api_key="test",
            model="deepseek-reasoner",
        ),
        input_tokens_per_call=2000,
        output_tokens_per_call=800,
    )

    outcome = await think(
        trigger, fresh_db, llm_provider=provider,
        triggering_content="PR merged",
        reason_for_trigger="fu3",
    )
    assert outcome.status == "success", outcome.error
    # Outcome carries the aggregator totals.
    assert outcome.llm_calls_count == 1
    assert outcome.llm_input_tokens == 2000
    assert outcome.llm_output_tokens == 800
    assert outcome.llm_cost_usd > 0.0
    assert outcome.llm_model_name == "deepseek-reasoner"

    # DB row lands (committed — the cost record is post-commit, best-effort).
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT trigger_kind, llm_calls_count,
                   llm_input_tokens_total, llm_output_tokens_total,
                   llm_cost_usd, outcome, model_name
            FROM think_run_costs
            WHERE trigger_id = $1
            """,
            outcome.trigger_id,
        )
    assert row is not None, "think_run_costs row missing after success"
    assert row["llm_calls_count"] == 1
    assert row["llm_input_tokens_total"] == 2000
    assert row["llm_output_tokens_total"] == 800
    assert float(row["llm_cost_usd"]) > 0.0
    assert row["outcome"] == "success"
    assert row["model_name"] == "deepseek-reasoner"

    # Cleanup — remove the cost row this test created.
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "DELETE FROM think_run_costs WHERE trigger_id = $1",
            outcome.trigger_id,
        )


async def test_fu3_think_records_cost_row_on_failure(
    fresh_db, tenant, tenant_cleanup,
):
    """Cost record persists even when the Think run fails. Observability
    must survive the rollback — the row is best-effort post-commit."""
    trigger_id = uuid7()
    obs = await _seed_observation(fresh_db, tenant, external_id="fu3-2")
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_entity_ids=[],
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[],
        seed_signature={"trigger_id": str(trigger_id)},
    )
    # Provider raises so think() fails. We still expect a cost row for
    # forensics (zero call count since provider raised before tokens
    # were recorded — but the row itself must exist).
    provider = UsageEmittingScriptedProvider(
        responses=[RuntimeError("simulated provider outage")],
        cfg=LLMConfig(
            provider="anthropic",
            api_key="test",
            model="claude-opus-4-7",
        ),
    )

    outcome = await think(
        trigger, fresh_db, llm_provider=provider,
        triggering_content="PR merged",
        reason_for_trigger="fu3 failure",
    )
    assert outcome.status == "failed", outcome.status

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT outcome, llm_calls_count, model_name
            FROM think_run_costs
            WHERE trigger_id = $1
            """,
            outcome.trigger_id,
        )
    assert row is not None, "cost row should persist for failed runs"
    assert row["outcome"] in ("failed", "reasoning_exhausted")
    # Provider raised before _record_usage ran → 0 calls counted.
    assert row["llm_calls_count"] == 0
    assert row["model_name"] == "claude-opus-4-7"

    # Cleanup.
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "DELETE FROM think_run_costs WHERE trigger_id = $1",
            outcome.trigger_id,
        )


async def test_fu3_aggregator_cleared_between_runs(
    fresh_db, tenant, tenant_cleanup,
):
    """After think() returns, the provider's aggregator is detached.
    Subsequent calls with the same provider start from zero (no leak
    across runs)."""
    trigger_id = uuid7()
    obs = await _seed_observation(fresh_db, tenant, external_id="fu3-3")
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_entity_ids=[],
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[],
        seed_signature={"trigger_id": str(trigger_id)},
    )
    provider = UsageEmittingScriptedProvider(
        responses=[_scripted_empty_diff(trigger_id, tenant)],
        cfg=LLMConfig(
            provider="deepseek", api_key="test", model="deepseek-chat",
        ),
        input_tokens_per_call=100,
        output_tokens_per_call=50,
    )

    outcome = await think(
        trigger, fresh_db, llm_provider=provider,
        triggering_content="x",
        reason_for_trigger="y",
    )
    assert outcome.status == "success", outcome.error
    # After the run, the provider has no aggregator installed.
    assert provider._usage_aggregator is None

    # Cleanup.
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "DELETE FROM think_run_costs WHERE trigger_id = $1",
            outcome.trigger_id,
        )
