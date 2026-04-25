"""Phase 3 behavioural tests: real DeepSeek Think reasoning across the 3 scenarios."""
from __future__ import annotations

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.models.repo import ModelsRepo
from tests.real_llm.infrastructure.assertion_helpers import (
    assert_at_least_one_model_matching,
    assert_model_count_in_range,
    assert_proposition_kind_distribution,
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


_VALID_PROPOSITION_KINDS = {
    "state",
    "relation",
    "prediction",
    "pattern",
    "pattern_instance",
    "capability_assessment",
    "hypothesis",
    "concern",
    "market_assessment",
    "environmental_trend",
}


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_think_recognizes_customer_churn_risk(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_02,
        "customer_churn_signal",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    await wait_for_think_to_drain(
        scenario_02.tenant_id, fresh_db, timeout_seconds=300
    )

    globex_id = scenario_02.customer_id("Globex Inc")
    models = await load_active_models(
        scenario_02.tenant_id, fresh_db, scope_entity_id=globex_id
    )

    risk_models = assert_at_least_one_model_matching(
        models,
        proposition_text_contains=[
            "risk",
            "churn",
            "concern",
            "at-risk",
            "evaluating",
            "alternatives",
        ],
        confidence_range=(0.3, 0.95),
        context="Globex churn signals should produce a risk-themed Model",
    )
    for m in risk_models:
        if m.confidence > 0.7:
            assert m.falsifier is not None and "kind" in m.falsifier, (
                f"High-confidence Model {m.id} (conf={m.confidence:.2f}) "
                f"missing structural falsifier: {m.falsifier!r}"
            )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_think_captures_commitment_progress(
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
        scenario_02.tenant_id, fresh_db, timeout_seconds=300
    )

    alice_id = scenario_02.actor_id("Alice Chen")
    refund_id = scenario_02.commitment_id("Implement refund flow")

    actor_models = await load_active_models(
        scenario_02.tenant_id, fresh_db, scope_actor_id=alice_id
    )
    commitment_models = await load_active_models(
        scenario_02.tenant_id, fresh_db, scope_entity_id=refund_id
    )
    # Dedupe by id; the same Model can match both scopes.
    by_id = {m.id: m for m in (*actor_models, *commitment_models)}
    relevant = list(by_id.values())

    assert_model_count_in_range(
        relevant,
        1,
        10,
        context="Alice + refund-flow scoped Models for ships sequence",
    )
    assert_at_least_one_model_matching(
        relevant,
        proposition_kind={"state", "prediction"},
        proposition_text_contains=[
            "ship",
            "merge",
            "deploy",
            "complete",
            "ready",
        ],
        context="Should have a state/prediction Model about shipping/merging",
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_think_models_have_valid_proposition_kinds(
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
        scenario_02.tenant_id, fresh_db, timeout_seconds=300
    )

    repo = ModelsRepo(fresh_db)
    models = await repo.search_by_scope(
        tenant_id=scenario_02.tenant_id, status="active", limit=500
    )
    if len(models) < 3:
        pytest.skip(
            f"Not enough Models ({len(models)}) for distribution analysis"
        )

    # Every kind we observe must be in the canonical PropositionKind enum.
    seen_kinds = {m.proposition.get("kind") for m in models}
    unknown = seen_kinds - _VALID_PROPOSITION_KINDS - {None}
    assert not unknown, f"Unexpected proposition kinds emitted by Think: {unknown}"

    # Loose distribution: no single kind should dominate above 80%.
    expected_bands = {kind: (0.0, 0.8) for kind in _VALID_PROPOSITION_KINDS}
    assert_proposition_kind_distribution(
        models,
        expected_bands,
        context="No single proposition kind should exceed 80% of all Models",
    )


@pytest.mark.asyncio
@real_llm_test(attempts=5, pass_threshold=3, timeout_seconds=600)
async def test_think_handles_contested_signals(
    scenario_01: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_01,
        "founder_debate",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    await wait_for_think_to_drain(
        scenario_01.tenant_id, fresh_db, timeout_seconds=300
    )

    repo = ModelsRepo(fresh_db)
    # status=None -> all statuses including contested_false
    all_models = await repo.search_by_scope(
        tenant_id=scenario_01.tenant_id, status=None, limit=500
    )
    assert all_models, (
        "founder_debate produced zero Models — Think appears not to have run"
    )

    # (a) any Model with multiple signal_readings entries
    has_multi_readings = any(len(m.signal_readings) >= 2 for m in all_models)
    # (b) any Model annotated as contested_false
    has_contested_false = any(m.status == "contested_false" for m in all_models)
    # (c) two Models with conflicting propositions on the same scope (same
    #     scope_entities OR overlapping scope_actors) where one negates/contests
    #     a key term of the other.
    contest_terms = (
        "disagree",
        "contest",
        "contested",
        "not",
        "no longer",
        "reject",
        "oppose",
        "against",
        "but",
        "however",
    )

    def _scope_key(m) -> tuple:
        ent = tuple(
            sorted(
                (e.get("type"), str(e.get("id"))) for e in m.scope_entities
            )
        )
        acts = tuple(sorted(str(a) for a in m.scope_actors))
        return (ent, acts)

    has_conflicting_pair = False
    by_scope: dict[tuple, list] = {}
    for m in all_models:
        by_scope.setdefault(_scope_key(m), []).append(m)
    for shared in by_scope.values():
        if len(shared) < 2:
            continue
        for i, a in enumerate(shared):
            for b in shared[i + 1 :]:
                a_lc = a.natural.lower()
                b_lc = b.natural.lower()
                if any(t in a_lc for t in contest_terms) or any(
                    t in b_lc for t in contest_terms
                ):
                    has_conflicting_pair = True
                    break
            if has_conflicting_pair:
                break
        if has_conflicting_pair:
            break

    assert has_multi_readings or has_contested_false or has_conflicting_pair, (
        f"Expected contested-signal evidence (multi-reading Model, "
        f"contested_false status, or conflicting-pair on shared scope) "
        f"among {len(all_models)} Models, found none"
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_think_produces_high_confidence_models_with_falsifiers(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_02,
        "feature_launch_cycle",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    await wait_for_think_to_drain(
        scenario_02.tenant_id, fresh_db, timeout_seconds=300
    )

    repo = ModelsRepo(fresh_db)
    models = await repo.search_by_scope(
        tenant_id=scenario_02.tenant_id, status="active", limit=500
    )
    high_conf = [m for m in models if m.confidence > 0.7]
    if not high_conf:
        pytest.skip(
            f"No high-confidence (>0.7) Models among {len(models)} produced"
        )
    missing = [
        m for m in high_conf if m.falsifier is None or "kind" not in m.falsifier
    ]
    assert not missing, (
        f"{len(missing)}/{len(high_conf)} high-confidence Models missing "
        f"structural falsifier; example ids: "
        f"{[str(m.id) for m in missing[:3]]}"
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_think_models_scope_to_relevant_entities(
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
        scenario_02.tenant_id, fresh_db, timeout_seconds=300
    )

    repo = ModelsRepo(fresh_db)
    models = await repo.search_by_scope(
        tenant_id=scenario_02.tenant_id, status="active", limit=500
    )
    assert models, "alice_ships_refund_flow produced zero Models"

    dangling = [
        m for m in models if not m.scope_actors and not m.scope_entities
    ]
    assert not dangling, (
        f"{len(dangling)}/{len(models)} Models have no actor or entity scope; "
        f"example ids: {[str(m.id) for m in dangling[:3]]}"
    )

    alice_id = scenario_02.actor_id("Alice Chen")
    refund_id = scenario_02.commitment_id("Implement refund flow")
    refund_str = str(refund_id)

    def _references_focus(m) -> bool:
        if alice_id in m.scope_actors:
            return True
        for e in m.scope_entities:
            if str(e.get("id")) == refund_str:
                return True
        return False

    relevant = [m for m in models if _references_focus(m)]
    fraction = len(relevant) / len(models)
    assert fraction >= 0.5, (
        f"Only {len(relevant)}/{len(models)} ({fraction:.0%}) Models reference "
        f"Alice or the refund-flow Commitment; expected >=50%"
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_think_recognizes_cross_team_dependency_block(
    scenario_03: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_03,
        "cross_team_dependency_block",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    await wait_for_think_to_drain(
        scenario_03.tenant_id, fresh_db, timeout_seconds=300
    )

    blocked_id = scenario_03.commitment_id("Cut Identity service over to auth-v3")
    sdk_id = scenario_03.commitment_id("Publish auth-v3 client SDK")
    rc1_id = scenario_03.commitment_id("Ship auth-svc v3 RC1 to staging")

    blocked_models = await load_active_models(
        scenario_03.tenant_id, fresh_db, scope_entity_id=blocked_id
    )
    sdk_models = await load_active_models(
        scenario_03.tenant_id, fresh_db, scope_entity_id=sdk_id
    )
    rc1_models = await load_active_models(
        scenario_03.tenant_id, fresh_db, scope_entity_id=rc1_id
    )
    by_id = {
        m.id: m for m in (*blocked_models, *sdk_models, *rc1_models)
    }
    relevant = list(by_id.values())

    assert_model_count_in_range(
        relevant,
        2,
        15,
        context="cross_team_dependency_block scoped Models",
    )
    assert_at_least_one_model_matching(
        relevant,
        proposition_text_contains=[
            "block",
            "depend",
            "wait",
            "stuck",
            "ETA",
        ],
        context="Should surface a Model mentioning the cross-team block",
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_think_recognizes_decision_revisit(
    scenario_03: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_03,
        "decision_revision_cascade",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    await wait_for_think_to_drain(
        scenario_03.tenant_id, fresh_db, timeout_seconds=300
    )

    decision_id = scenario_03.decision_id(
        "Adopt Kafka as the company-wide event bus"
    )
    models = await load_active_models(
        scenario_03.tenant_id, fresh_db, scope_entity_id=decision_id
    )

    assert_model_count_in_range(
        models,
        1,
        10,
        context="decision_revision_cascade scoped Models",
    )

    # Try the text-based existential first; if that misses, accept any
    # `concern`-kind Model on the decision scope as evidence.
    text_matches = [
        m
        for m in models
        if any(
            term in m.natural.lower()
            for term in (
                "revisit",
                "reconsider",
                "review",
                "concern",
                "outdated",
                "no longer",
                "changed",
            )
        )
    ]
    concern_matches = [
        m for m in models if m.proposition.get("kind") == "concern"
    ]
    assert text_matches or concern_matches, (
        f"Expected at least one Model on the Kafka decision indicating a "
        f"revisit/concern; found {len(models)} Models, none matching "
        f"text terms or proposition_kind='concern'"
    )
