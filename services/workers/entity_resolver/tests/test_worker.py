"""Tests for services/workers/entity_resolver/worker.py.

Conventions:
- Uses a `ScriptedProvider` (same pattern as lib/llm/tests/test_provider.py)
  to feed canned LLM outputs. Never calls a live LLM.
- Real Postgres via `resolver_db` fixture.
- Each test uses a fresh tenant_id for hermetic isolation.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.llm.provider import LLMConfig, LLMProvider, LLMParseError
from lib.shared.ids import uuid7
from services.entity_aliases.repo import EntityAliasRepo
from services.workers.entity_resolver.worker import (
    EntityResolution,
    EntityResolverWorker,
    LLMRateLimitError,
    LLMTimeoutError,
    ResolverLLMBudget,
)


pytestmark = pytest.mark.integration


# =====================================================================
# Test doubles
# =====================================================================

class ScriptedProvider(LLMProvider):
    """Pops scripted responses (strings or exceptions) in FIFO order."""

    def __init__(self, responses: list):
        super().__init__(
            LLMConfig(provider="anthropic", api_key="k", model="m")
        )
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        self.calls.append(
            {"system": system, "user": user, "schema_hint": schema_hint}
        )
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _resolution_json(
    *,
    type: str | None = "commitment",
    id: str = "commitment-uuid",
    confidence: float = 0.9,
    reasoning: str = "matches payments context",
) -> str:
    ref = None if type is None else {"type": type, "id": id}
    return json.dumps({
        "canonical_ref": ref,
        "confidence": confidence,
        "reasoning": reasoning,
    })


# =====================================================================
# Fixtures
# =====================================================================

async def _seed_observation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    content_text: str,
    unresolved_phrases: list[str],
    source_channel: str = "slack:message",
    occurred_at: datetime | None = None,
) -> UUID:
    obs_id = uuid7()
    occurred_at = occurred_at or datetime.now(timezone.utc)
    content = {
        "text": content_text,
        "metadata": {"_unresolved_phrases": unresolved_phrases},
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                content, content_text, trust_tier
            ) VALUES (
                $1, $2, $3, 'signal', $4, $5::jsonb, $6, 'attested_agent'
            )
            """,
            obs_id,
            tenant_id,
            occurred_at,
            source_channel,
            json.dumps(content),
            content_text,
        )
    return obs_id


async def _fetch_obs_entities(
    pool: asyncpg.Pool, obs_id: UUID
) -> list[dict]:
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT entities_mentioned FROM observations WHERE id = $1",
            obs_id,
        )
    if val is None:
        return []
    if isinstance(val, str):
        return json.loads(val)
    return list(val)


async def _count_review_rows(
    pool: asyncpg.Pool, tenant_id: UUID
) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM entity_review_queue WHERE tenant_id = $1",
            tenant_id,
        ) or 0


async def _count_trigger_rows(
    pool: asyncpg.Pool, tenant_id: UUID
) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant_id,
        ) or 0


async def _count_state_change_obs(
    pool: asyncpg.Pool, tenant_id: UUID
) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """
            SELECT COUNT(*) FROM observations
            WHERE tenant_id = $1 AND kind = 'state_change'
            """,
            tenant_id,
        ) or 0


# =====================================================================
# Resolved-path tests
# =====================================================================

async def test_phrase_in_payment_context_resolves_to_commitment(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="the billing thing is down",
        unresolved_phrases=["the billing thing"],
    )
    provider = ScriptedProvider([
        _resolution_json(
            type="commitment",
            id="payments_service_v2",
            confidence=0.91,
        )
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("the billing thing", "resolved")]

    # Alias row exists.
    ref = await EntityAliasRepo(resolver_db).fast_path_resolve(
        "the billing thing", tenant_id
    )
    assert ref == {"type": "commitment", "id": "payments_service_v2"}

    # Observation has the entity appended.
    ents = await _fetch_obs_entities(resolver_db, obs_id)
    assert {"type": "commitment", "id": "payments_service_v2"} in ents

    # state_change observation emitted.
    assert await _count_state_change_obs(resolver_db, tenant_id) == 1


async def test_resolved_customer_enqueues_think_trigger(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="we just lost the big one",
        unresolved_phrases=["the big one"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-acme", confidence=0.95)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    await worker.process_observation(obs_id, tenant_id)
    # think_trigger_queue row for this tenant.
    assert await _count_trigger_rows(resolver_db, tenant_id) == 1


async def test_resolved_non_material_type_does_not_enqueue_trigger(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="a link to the wiki",
        unresolved_phrases=["the wiki"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="url", id="https://wiki", confidence=0.9)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    await worker.process_observation(obs_id, tenant_id)
    assert await _count_trigger_rows(resolver_db, tenant_id) == 0


# =====================================================================
# Review-queue path
# =====================================================================

async def test_ambiguous_confidence_goes_to_review_queue(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="the project", unresolved_phrases=["the project"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="goal", id="g-1", confidence=0.6)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("the project", "review")]

    # No alias written; review row exists.
    assert await _count_review_rows(resolver_db, tenant_id) == 1


# =====================================================================
# Dropped path
# =====================================================================

async def test_low_confidence_is_dropped(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="we just said hi",
        unresolved_phrases=["just said hi"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="signal", id="x", confidence=0.1)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("just said hi", "dropped")]

    # No alias, no review, no trigger.
    ref = await EntityAliasRepo(resolver_db).fast_path_resolve(
        "just said hi", tenant_id
    )
    assert ref is None
    assert await _count_review_rows(resolver_db, tenant_id) == 0


async def test_null_canonical_ref_is_dropped(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="???",
        unresolved_phrases=["not an entity"],
    )
    provider = ScriptedProvider([
        _resolution_json(type=None, id="", confidence=0.9)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("not an entity", "dropped")]


# =====================================================================
# Failure-mode paths: LLM timeout, rate-limit, malformed response.
# =====================================================================

async def test_llm_timeout_is_requeued(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="x", unresolved_phrases=["a"],
    )
    provider = ScriptedProvider([asyncio.TimeoutError()])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("a", "rate_limited")]  # requeue semantics
    assert worker.requeue_delay_s(obs_id) > 0


async def test_llm_rate_limit_is_requeued(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    class RateLimit(Exception):
        """Provider raises something class-named 'RateLimit...'."""

    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="x", unresolved_phrases=["a"],
    )
    provider = ScriptedProvider([
        type("RateLimitError", (Exception,), {})("slow down")
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("a", "rate_limited")]


async def test_llm_malformed_response_exhausts_retries_and_drops(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="x", unresolved_phrases=["a"],
    )
    # 3 consecutive unparseable responses (default max_retries=2 →
    # 3 total attempts).
    provider = ScriptedProvider(["not json", "still junk", "nope"])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("a", "dropped")]


# =====================================================================
# Budget / rate limiter
# =====================================================================

async def test_per_tenant_budget_skips_call_when_exhausted(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="x", unresolved_phrases=["a", "b"],
    )
    # Budget = 1 per minute — first phrase consumes, second is rate-limited.
    provider = ScriptedProvider([_resolution_json(confidence=0.95)])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
        budget=ResolverLLMBudget(per_minute=1),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    # First phrase resolved, second skipped without an LLM call.
    assert decisions[0][1] == "resolved"
    assert decisions[1][1] == "rate_limited"
    assert len(provider.calls) == 1


# =====================================================================
# No unresolved phrases → no LLM calls
# =====================================================================

async def test_no_unresolved_phrases_is_noop(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="nothing", unresolved_phrases=[],
    )
    provider = ScriptedProvider([])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == []
    assert len(provider.calls) == 0


# =====================================================================
# Idempotency: same phrase + same obs → alias inserted once
# =====================================================================

async def test_alias_is_idempotent_on_rerun(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="ship it now",
        unresolved_phrases=["ship it"],
    )
    responses = [
        _resolution_json(type="commitment", id="c1", confidence=0.9)
        for _ in range(2)
    ]
    provider = ScriptedProvider(responses)
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    await worker.process_observation(obs_id, tenant_id)
    await worker.process_observation(obs_id, tenant_id)

    # Only one alias row per (tenant, phrase, actor_id=NULL).
    async with resolver_db.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM entity_aliases
            WHERE tenant_id = $1 AND alias_text = 'ship it'
            """,
            tenant_id,
        )
    assert n == 1

    # entities_mentioned still contains exactly one copy.
    ents = await _fetch_obs_entities(resolver_db, obs_id)
    ids = [e.get("id") for e in ents]
    assert ids.count("c1") == 1


# =====================================================================
# End-to-end fixture: 50 mixed events → correct count & no dupes
# =====================================================================

async def test_end_to_end_50_mixed_events(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    """Replay a 50-observation fixture spanning Slack/GitHub/Linear.

    10 of those have an unresolved phrase; the resolver should produce
    10 resolved aliases, append 10 entities to their parent
    observations, emit 10 state_change Observations, and never
    duplicate.
    """
    base = datetime.now(timezone.utc)
    channels = ["slack:message", "github:webhook", "linear:webhook"]
    obs_ids: list[UUID] = []
    for i in range(50):
        channel = channels[i % 3]
        phrases = [f"phrase_{i}"] if i % 5 == 0 else []
        obs = await _seed_observation(
            resolver_db, tenant_id,
            content_text=f"event {i}",
            unresolved_phrases=phrases,
            source_channel=channel,
            occurred_at=base.replace(microsecond=i * 1000),
        )
        obs_ids.append(obs)

    # Script a resolve for every unresolved phrase.
    n_phrases = sum(1 for i in range(50) if i % 5 == 0)
    provider = ScriptedProvider([
        _resolution_json(
            type="commitment", id=f"c{i}", confidence=0.9
        )
        for i in range(n_phrases)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
        # Relax budget for the e2e test.
        budget=ResolverLLMBudget(per_minute=1000),
    )

    decisions: list[tuple[str, str]] = []
    for obs_id in obs_ids:
        decisions.extend(await worker.process_observation(obs_id, tenant_id))

    resolved = [d for d in decisions if d[1] == "resolved"]
    assert len(resolved) == n_phrases
    # Verify no duplicate aliases.
    async with resolver_db.acquire() as conn:
        alias_rows = await conn.fetch(
            """
            SELECT alias_text, COUNT(*) c
            FROM entity_aliases
            WHERE tenant_id = $1
            GROUP BY alias_text
            """,
            tenant_id,
        )
    assert all(r["c"] == 1 for r in alias_rows)
    assert len(alias_rows) == n_phrases

    # state_change observation count equals n_phrases.
    assert await _count_state_change_obs(resolver_db, tenant_id) == n_phrases


# =====================================================================
# Review-queue visibility (tenant isolation)
# =====================================================================

async def test_review_queue_tenant_isolated(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    other_tenant = uuid7()
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="ambi", unresolved_phrases=["ambi"],
    )
    other_obs = await _seed_observation(
        resolver_db, other_tenant,
        content_text="ambi2", unresolved_phrases=["ambi2"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="commitment", id="c1", confidence=0.6),
        _resolution_json(type="commitment", id="c2", confidence=0.6),
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    await worker.process_observation(obs_id, tenant_id)
    await worker.process_observation(other_obs, other_tenant)
    # Each tenant has its own row; neither sees the other.
    assert await _count_review_rows(resolver_db, tenant_id) == 1
    assert await _count_review_rows(resolver_db, other_tenant) == 1


# =====================================================================
# Budget instance method — direct unit test (no DB)
# =====================================================================

def test_budget_refills_tokens_over_time(monkeypatch):
    budget = ResolverLLMBudget(per_minute=60)  # 1 per sec
    t = UUID("11111111-1111-1111-1111-111111111111")
    # Monkey-patch monotonic clock.
    fake_now = [0.0]

    def _mono():
        return fake_now[0]

    import time

    monkeypatch.setattr(time, "monotonic", _mono)

    # Consume 60 tokens immediately.
    for _ in range(60):
        assert budget.check_and_consume(t)
    assert not budget.check_and_consume(t)
    # Advance 1 second → 1 token refilled.
    fake_now[0] = 1.0
    assert budget.check_and_consume(t)
