"""Real-LLM ingestion tests: signal -> Observation -> Think trigger -> Models."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.synthetic.core import SyntheticSignal, inject
from tests.real_llm.infrastructure.assertion_helpers import (
    assert_at_least_one_model_matching,
    assert_model_count_in_range,
)
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import (
    Scenario,
    inject_sequence,
)
from tests.real_llm.infrastructure.think_drain import (
    load_active_models,
    wait_for_think_to_drain,
)


@pytest.mark.asyncio
@real_llm_test(attempts=1)
async def test_ingestion_preserves_signal_through_to_observation(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    sequence = scenario_02.get_sequence("alice_ships_refund_flow")
    expected_count = len(sequence)
    assert expected_count >= 1, "alice_ships_refund_flow sequence should be non-empty"

    obs_ids = await inject_sequence(
        scenario_02,
        "alice_ships_refund_flow",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )

    assert len(obs_ids) == expected_count, (
        f"expected {expected_count} observations, got {len(obs_ids)}"
    )

    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, kind, content, external_id
            FROM observations
            WHERE id = ANY($1::uuid[])
            ORDER BY sequence_num ASC
            """,
            obs_ids,
        )

    assert len(rows) == expected_count, (
        f"expected {expected_count} rows fetched, got {len(rows)}"
    )

    seen_external_ids: set[str] = set()
    for row in rows:
        assert row["tenant_id"] == scenario_02.tenant_id, (
            f"obs {row['id']} tenant {row['tenant_id']} != scenario tenant "
            f"{scenario_02.tenant_id}"
        )
        assert row["kind"] == "signal", (
            f"obs {row['id']} kind {row['kind']!r} != 'signal'"
        )
        content = row["content"]
        assert isinstance(content, dict), (
            f"obs {row['id']} content not dict-typed: {type(content).__name__}"
        )
        assert content.get("synthetic") is True, (
            f"obs {row['id']} content missing synthetic=true marker: {content}"
        )
        ext_id = row["external_id"]
        assert ext_id is not None, f"obs {row['id']} has NULL external_id"
        assert ext_id not in seen_external_ids, (
            f"obs {row['id']} external_id {ext_id!r} not unique within sequence"
        )
        seen_external_ids.add(ext_id)


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=900)
async def test_ingestion_triggers_think_which_produces_models(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_02,
        "alice_ships_refund_flow",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )

    await wait_for_think_to_drain(
        scenario_02.tenant_id,
        fresh_db,
        timeout_seconds=600,
    )

    models = await load_active_models(scenario_02.tenant_id, fresh_db)
    assert_model_count_in_range(
        models,
        low=2,
        high=15,
        context="alice_ships_refund_flow should produce a tractable Model set",
    )
    assert_at_least_one_model_matching(
        models,
        scope_actor_id=scenario_02.actor_id("Alice Chen"),
        context="At least one Model should be scoped to Alice Chen",
    )


@pytest.mark.asyncio
@real_llm_test(attempts=1)
async def test_ingestion_handles_external_actor_signals(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    sequence = scenario_02.get_sequence("customer_churn_signal")
    assert sequence, "customer_churn_signal sequence should be non-empty"
    first = sequence[0]
    assert isinstance(first.get("actor"), str) and first["actor"].startswith("external:"), (
        f"customer_churn_signal[0] actor must be external:* — got {first.get('actor')!r}"
    )

    obs_ids = await inject_sequence(
        scenario_02,
        "customer_churn_signal",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )

    external_obs_id = obs_ids[0]
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, actor_id, source_actor_ref, content
            FROM observations
            WHERE id = $1
            """,
            external_obs_id,
        )

    assert row is not None, (
        f"external-actor observation {external_obs_id} did not land in DB"
    )
    assert row["tenant_id"] == scenario_02.tenant_id, (
        f"obs tenant {row['tenant_id']} != scenario tenant {scenario_02.tenant_id}"
    )
    assert row["actor_id"] is None, (
        f"external-actor observation should have NULL actor_id, got {row['actor_id']}"
    )
    assert row["source_actor_ref"] == first["actor"], (
        f"source_actor_ref {row['source_actor_ref']!r} != "
        f"signal actor {first['actor']!r}"
    )


@pytest.mark.asyncio
@real_llm_test(attempts=1)
async def test_ingestion_dedups_repeated_external_id(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    sequence = scenario_02.get_sequence("alice_ships_refund_flow")
    assert sequence, "alice_ships_refund_flow should be non-empty"
    first = sequence[0]

    occurred_at = scenario_02.base_time or datetime.now(timezone.utc)
    shared_external_id = f"{scenario_02.scenario_id}:dedup-test:{uuid4()}"
    content_text = first.get("content") or first.get("text") or ""

    def _build_signal() -> SyntheticSignal:
        return SyntheticSignal(
            source_channel=first["channel"],
            content_text=content_text,
            content={"text": content_text},
            occurred_at=occurred_at,
            source_actor_ref=None,
            external_id=shared_external_id,
            entities_hint=[],
            kind="signal",
            scenario_id=scenario_02.scenario_id,
        )

    first_result = await inject(
        _build_signal(),
        scenario_02.tenant_id,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
    )
    assert first_result.deduped is False, (
        "first injection of fresh external_id should not be deduped"
    )

    second_result = await inject(
        _build_signal(),
        scenario_02.tenant_id,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
    )
    assert second_result.deduped is True, (
        "second injection with same (source_channel, external_id) should dedup"
    )
    assert second_result.observation.id == first_result.observation.id, (
        f"deduped observation id {second_result.observation.id} != "
        f"original {first_result.observation.id}"
    )

    async with fresh_db.acquire() as conn:
        row_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE source_channel = $1
              AND external_id = $2
            """,
            first["channel"],
            shared_external_id,
        )
    assert int(row_count or 0) == 1, (
        f"expected exactly 1 observation row for ({first['channel']!r}, "
        f"{shared_external_id!r}), got {row_count}"
    )
