"""Full-signal durability tests for deep synthetic-company corpora.

These are intentionally expensive and opt-in. Each test injects every signal in
the corpus through production ingestion, resolves actual-content aliases with a
real LLM, drains Think with DeepSeek, and verifies Models plus Bridge surfaces.
"""
from __future__ import annotations

import os

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from lib.llm.provider import LLMProvider
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from tests.real_llm.infrastructure.durability_flow import (
    FullSignalSummary,
    assert_customer_bridge_surface,
    collect_full_signal_summary,
    flatten_observation_ids,
    inject_all_sequences,
    observation_id_for_signal_text,
    resolve_alias_phrase_from_observation,
    run_think_until_drain,
)
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import Scenario


RUN_DURABILITY_E2E = os.environ.get("RUN_DURABILITY_E2E") == "1"


def _scenario_enabled(scenario_id: str) -> bool:
    raw = os.environ.get("DURABILITY_SCENARIOS")
    if not raw:
        return True
    enabled = {part.strip() for part in raw.split(",") if part.strip()}
    return scenario_id in enabled


@pytest.mark.asyncio
@pytest.mark.skipif(
    not RUN_DURABILITY_E2E,
    reason="set RUN_DURABILITY_E2E=1 to run full-signal durability E2E tests",
)
@pytest.mark.skipif(
    not _scenario_enabled("industrial_ops"),
    reason="industrial_ops not listed in DURABILITY_SCENARIOS",
)
@real_llm_test(attempts=1, pass_threshold=1, timeout_seconds=7200)
async def test_scenario_05_every_signal_reaches_think_and_bridge(
    scenario_05: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    provider: LLMProvider,
) -> None:
    assert scenario_05.tenant_id is not None
    tenant_id = scenario_05.tenant_id
    run_id = "industrial-ops-full-signal-durability"

    ids_by_sequence = await inject_all_sequences(
        scenario_05,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        run_id=run_id,
    )
    observation_ids = flatten_observation_ids(ids_by_sequence)

    await _resolve_and_assert_alias(
        scenario_05,
        ids_by_sequence,
        "titan_furnace_outage",
        "TFI means Titan Foundry Inc",
        "TFI",
        "Titan Foundry Inc",
        tenant_id=tenant_id,
        pool=fresh_db,
        alias_repo=alias_repo,
        provider=provider,
    )
    await _resolve_and_assert_alias(
        scenario_05,
        ids_by_sequence,
        "metrorail_safety_identity",
        "MRA is MetroRail Authority",
        "MRA",
        "MetroRail Authority",
        tenant_id=tenant_id,
        pool=fresh_db,
        alias_repo=alias_repo,
        provider=provider,
    )

    await run_think_until_drain(
        tenant_id,
        pool=fresh_db,
        provider=provider,
        timeout_seconds=5400,
    )
    summary = await collect_full_signal_summary(
        scenario_05,
        run_id=run_id,
        pool=fresh_db,
        original_observation_ids=observation_ids,
    )
    _assert_full_signal_summary_is_healthy(summary)
    await assert_customer_bridge_surface(
        scenario_05,
        pool=fresh_db,
        customer_name="Titan Foundry Inc",
        required_commitments={
            "Stabilize Titan furnace telemetry ingestion",
            "Deliver Titan penalty-risk explanation export",
        },
    )


@pytest.mark.asyncio
@pytest.mark.skipif(
    not RUN_DURABILITY_E2E,
    reason="set RUN_DURABILITY_E2E=1 to run full-signal durability E2E tests",
)
@pytest.mark.skipif(
    not _scenario_enabled("fintech_risk"),
    reason="fintech_risk not listed in DURABILITY_SCENARIOS",
)
@real_llm_test(attempts=1, pass_threshold=1, timeout_seconds=7200)
async def test_scenario_06_every_signal_reaches_think_and_bridge(
    scenario_06: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    provider: LLMProvider,
) -> None:
    assert scenario_06.tenant_id is not None
    tenant_id = scenario_06.tenant_id
    run_id = "fintech-risk-full-signal-durability"

    ids_by_sequence = await inject_all_sequences(
        scenario_06,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        run_id=run_id,
    )
    observation_ids = flatten_observation_ids(ids_by_sequence)

    await _resolve_and_assert_alias(
        scenario_06,
        ids_by_sequence,
        "atlas_network_and_ledger_incident",
        "ACS means Atlas Card Services",
        "ACS",
        "Atlas Card Services",
        tenant_id=tenant_id,
        pool=fresh_db,
        alias_repo=alias_repo,
        provider=provider,
    )
    await _resolve_and_assert_alias(
        scenario_06,
        ids_by_sequence,
        "blueriver_kyc_drift_exam",
        "BRCU is BlueRiver Credit Union",
        "BRCU",
        "BlueRiver Credit Union",
        tenant_id=tenant_id,
        pool=fresh_db,
        alias_repo=alias_repo,
        provider=provider,
    )

    await run_think_until_drain(
        tenant_id,
        pool=fresh_db,
        provider=provider,
        timeout_seconds=5400,
    )
    summary = await collect_full_signal_summary(
        scenario_06,
        run_id=run_id,
        pool=fresh_db,
        original_observation_ids=observation_ids,
    )
    _assert_full_signal_summary_is_healthy(summary)
    await assert_customer_bridge_surface(
        scenario_06,
        pool=fresh_db,
        customer_name="Atlas Card Services",
        required_commitments={
            "Resolve Atlas card-network incident memory",
            "Ship Atlas ledger reconciliation evidence",
        },
    )


async def _resolve_and_assert_alias(
    scenario: Scenario,
    ids_by_sequence: dict[str, list],
    sequence_name: str,
    text_needle: str,
    phrase: str,
    customer_name: str,
    *,
    tenant_id,
    pool: asyncpg.Pool,
    alias_repo: EntityAliasRepo,
    provider: LLMProvider,
) -> None:
    observation_id = observation_id_for_signal_text(
        scenario,
        ids_by_sequence,
        sequence_name=sequence_name,
        text_needle=text_needle,
    )
    await resolve_alias_phrase_from_observation(
        observation_id,
        phrase,
        tenant_id=tenant_id,
        pool=pool,
        alias_repo=alias_repo,
        provider=provider,
    )
    assert await alias_repo.fast_path_resolve(phrase, tenant_id) == {
        "type": "customer",
        "id": str(scenario.customer_id(customer_name)),
    }


def _assert_full_signal_summary_is_healthy(summary: FullSignalSummary) -> None:
    assert summary.observation_count == summary.expected_signal_count, summary
    assert summary.unique_observation_count == summary.expected_signal_count, summary
    assert summary.trigger_count >= summary.expected_signal_count, summary
    assert summary.pending_triggers == 0, summary
    assert summary.failed_runs == 0, summary
    assert (
        summary.successful_runs + summary.skipped_runs
        >= summary.expected_signal_count
    ), summary
    assert summary.context_use_reports >= max(1, summary.expected_signal_count // 10), (
        summary
    )
    assert summary.downstream_state_changes >= 1, summary
    assert summary.active_models >= 5, summary
    assert summary.distinct_channels >= 7, summary
    assert summary.observations_with_entities >= max(
        10,
        summary.expected_signal_count // 3,
    ), summary
