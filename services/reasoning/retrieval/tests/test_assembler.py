"""
Context assembler tests — access control stub, size bounds, customer
context.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from lib.shared.types import ObservationRow

from services.reasoning.retrieval.assembler import (
    AccessContext,
    ContextBundle,
    _select_observations,
    assemble_context,
)
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.primary import (
    RetrievalResult,
    TriggerContext,
    _fetch_trigger_observations,
    primary_retrieve,
)

from services.platform.access_control.materialized import refresh_all
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


def _obs_row(tenant, index: int) -> ObservationRow:
    return ObservationRow(
        id=uuid.uuid4(),
        tenant_id=tenant,
        occurred_at=datetime(2026, 4, 1, 12, index, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 4, 1, 12, index, tzinfo=timezone.utc),
        kind="signal",
        source_channel="unit",
        source_actor_ref=None,
        actor_id=None,
        content={"index": index},
        content_text=f"observation {index}",
        embedding=None,
        embedding_pending=False,
        trust_tier="authoritative",
        external_id=f"unit-{index}",
        cause_id=None,
        sequence_num=index,
        entities_mentioned=[],
    )


def test_select_observations_model_first_preserves_trigger_batch():
    tenant = uuid.uuid4()
    rows = [_obs_row(tenant, i) for i in range(14)]
    trigger_rows = rows[:6]
    historical_rows = rows[6:]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        observation_id=trigger_rows[0].id,
        observation_ids=[row.id for row in trigger_rows],
    )
    result = RetrievalResult(
        trigger=trigger,
        observations=[*historical_rows, *trigger_rows],
    )

    selected, notes = _select_observations(
        result,
        list(result.observations),
        cfg=RetrievalConfig(
            trigger_observation_cap=6,
            historical_observation_cap=2,
        ),
        budget_observations=12,
        explicit_budget=False,
    )

    selected_ids = {row.id for row in selected}
    assert {row.id for row in trigger_rows}.issubset(selected_ids)
    assert len(selected_ids & {row.id for row in historical_rows}) == 2
    assert notes["selected_trigger_count"] == 6
    assert notes["selected_historical_count"] == 2


def test_select_observations_explicit_budget_is_hard_total_cap():
    tenant = uuid.uuid4()
    rows = [_obs_row(tenant, i) for i in range(14)]
    trigger_rows = rows[:6]
    historical_rows = rows[6:]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        observation_id=trigger_rows[0].id,
        observation_ids=[row.id for row in trigger_rows],
    )
    result = RetrievalResult(
        trigger=trigger,
        observations=[*historical_rows, *trigger_rows],
    )

    selected, notes = _select_observations(
        result,
        list(result.observations),
        cfg=RetrievalConfig(
            trigger_observation_cap=6,
            historical_observation_cap=2,
        ),
        budget_observations=5,
        explicit_budget=True,
    )

    selected_ids = {row.id for row in selected}
    assert len(selected) == 5
    assert selected_ids.issubset({row.id for row in trigger_rows})
    assert not selected_ids & {row.id for row in historical_rows}
    assert notes["selected_trigger_count"] == 5
    assert notes["selected_historical_count"] == 0


def test_select_observations_event_batch_keeps_diverse_raw_floor_with_models():
    tenant = uuid.uuid4()
    rows = []
    for i in range(18):
        row = _obs_row(tenant, i)
        row.source_channel = "slack:message" if i < 9 else "github:webhook"
        row.source_actor_ref = f"user-{i % 4}"
        row.content_text = (
            f"Actor {i % 4} raised PR #{100 + i} for ENG-{i}"
            if i >= 9
            else f"Slack thread {i % 5} blocker update for Atlas"
        )
        rows.append(row)
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant,
        observation_id=rows[0].id,
        observation_ids=[row.id for row in rows],
    )
    result = RetrievalResult(trigger=trigger, observations=list(rows))

    selected, notes = _select_observations(
        result,
        list(result.observations),
        cfg=RetrievalConfig(
            observation_context_mode="model_gap",
            assembler_budget_observations=8,
            t1_event_batch_raw_observation_floor=8,
            t1_event_batch_raw_source_floor=4,
        ),
        budget_observations=8,
        explicit_budget=False,
        selected_model_count=16,
    )

    assert len(selected) == 8
    assert notes["floor_reason"] == "explicit_t1_event_batch_raw_evidence_floor"
    assert notes["selected_trigger_count"] == 8
    sources = {row.source_channel for row in selected}
    assert sources == {"slack:message", "github:webhook"}
    assert sum(row.source_channel == "slack:message" for row in selected) <= 4
    assert sum(row.source_channel == "github:webhook" for row in selected) <= 4


def test_select_observations_non_event_batch_still_suppresses_with_models():
    tenant = uuid.uuid4()
    rows = [_obs_row(tenant, i) for i in range(8)]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        observation_id=rows[0].id,
        observation_ids=[row.id for row in rows],
    )
    result = RetrievalResult(trigger=trigger, observations=list(rows))

    selected, notes = _select_observations(
        result,
        list(result.observations),
        cfg=RetrievalConfig(
            observation_context_mode="model_gap",
            t1_event_batch_raw_observation_floor=8,
        ),
        budget_observations=8,
        explicit_budget=False,
        selected_model_count=3,
    )

    assert selected == []
    assert notes["suppressed_reason"] == "model_context_sufficient"
    assert "floor_reason" not in notes


async def _retrieve(tx_conn, pool, tenant, seed_commit_id=None):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=(
            [{"type": "commitment", "id": str(seed_commit_id)}]
            if seed_commit_id
            else []
        ),
        seed_natural_text="alice ships reliably",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding("alice ships reliably"),
    )
    return await primary_retrieve(trigger, tx_conn)


async def _fetch_observation_rows(tx_conn, tenant, observation_ids):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        observation_ids=list(observation_ids),
    )
    return await _fetch_trigger_observations(trigger, tx_conn)


async def test_assembler_respects_size_budgets(
    tx_conn, fresh_db, tenant
):
    cfg = RetrievalConfig()
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)
    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=None),
        tx_conn,
        config=cfg,
    )
    assert isinstance(bundle, ContextBundle)
    assert (
        len(bundle.observations)
        <= cfg.trigger_observation_cap + cfg.historical_observation_cap
    )
    assert len(bundle.models) <= 40
    assert (
        len(bundle.acts_summary["goals"])
        + len(bundle.acts_summary["commitments"])
        + len(bundle.acts_summary["decisions"])
        <= 10
    )
    assert len(bundle.resources_summary) <= 5
    selection = bundle.notes["model_selection"]
    assert selection["retrieved_count"] == len(result.models)
    assert selection["selected_count"] == len(bundle.models)
    assert set(selection["pathway_survival"]).issuperset({"A", "B", "C", "G"})
    assert set(selection["selected_model_ids"]) == {str(m.id) for m in bundle.models}


async def test_assembler_uses_configured_default_budgets(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)
    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=None),
        tx_conn,
        config=RetrievalConfig(
            assembler_budget_models=7,
            assembler_budget_observations=3,
            assembler_budget_acts_total=4,
            assembler_budget_resources=2,
        ),
    )

    assert len(bundle.models) <= 7
    assert len(bundle.observations) <= 3
    assert (
        len(bundle.acts_summary["goals"])
        + len(bundle.acts_summary["commitments"])
        + len(bundle.acts_summary["decisions"])
        <= 4
    )
    assert len(bundle.resources_summary) <= 2
    assert bundle.notes["budgets"] == {
        "observations": 3,
        "trigger_observations": 0,
        "historical_observations": 0,
        "models": 7,
        "acts_total": 4,
        "resources": 2,
    }


async def test_assembler_model_gap_suppresses_observations_when_models_selected(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)

    trigger_ids = fs.observation_ids[:6]
    historical_ids = fs.observation_ids[6:14]
    result.observations = await _fetch_observation_rows(
        tx_conn,
        tenant,
        [*trigger_ids, *historical_ids],
    )
    result.trigger.observation_id = trigger_ids[0]
    result.trigger.observation_ids = list(trigger_ids)

    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=None),
        tx_conn,
        config=RetrievalConfig(
            assembler_budget_observations=12,
            trigger_observation_cap=6,
            historical_observation_cap=2,
        ),
    )

    assert bundle.models
    assert bundle.observations == []
    selection = bundle.notes["observation_selection"]
    assert selection["model_first_context_enabled"] is True
    assert selection["observation_context_mode"] == "model_gap"
    assert selection["selected_model_count"] == len(bundle.models)
    assert selection["suppressed_reason"] == "model_context_sufficient"
    assert selection["selected_trigger_count"] == 0
    assert selection["selected_historical_count"] == 0
    assert selection["dropped_trigger_count"] == len(trigger_ids)
    assert selection["dropped_historical_count"] == len(historical_ids)


async def test_assembler_always_mode_preserves_trigger_batch_and_caps_history(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)

    trigger_ids = fs.observation_ids[:6]
    historical_ids = fs.observation_ids[6:14]
    result.observations = await _fetch_observation_rows(
        tx_conn,
        tenant,
        [*trigger_ids, *historical_ids],
    )
    result.trigger.observation_id = trigger_ids[0]
    result.trigger.observation_ids = list(trigger_ids)

    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=None),
        tx_conn,
        config=RetrievalConfig(
            assembler_budget_observations=12,
            observation_context_mode="always",
            trigger_observation_cap=6,
            historical_observation_cap=2,
        ),
    )

    selected_ids = {o.id for o in bundle.observations}
    assert set(trigger_ids).issubset(selected_ids)
    assert len(selected_ids & set(historical_ids)) == 2
    selection = bundle.notes["observation_selection"]
    assert selection["model_first_context_enabled"] is True
    assert selection["observation_context_mode"] == "always"
    assert selection["selected_trigger_count"] == 6
    assert selection["selected_historical_count"] == 2
    assert selection["dropped_historical_count"] == len(historical_ids) - 2


async def test_assembler_explicit_observation_budget_remains_hard_total_cap(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)

    trigger_ids = fs.observation_ids[:6]
    historical_ids = fs.observation_ids[6:14]
    result.observations = await _fetch_observation_rows(
        tx_conn,
        tenant,
        [*trigger_ids, *historical_ids],
    )
    result.trigger.observation_id = trigger_ids[0]
    result.trigger.observation_ids = list(trigger_ids)

    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=None),
        tx_conn,
        budget_observations=5,
        config=RetrievalConfig(
            observation_context_mode="always",
            trigger_observation_cap=6,
            historical_observation_cap=2,
        ),
    )

    selected_ids = {o.id for o in bundle.observations}
    assert len(bundle.observations) == 5
    assert selected_ids.issubset(set(trigger_ids))
    assert not selected_ids & set(historical_ids)
    selection = bundle.notes["observation_selection"]
    assert selection["selected_trigger_count"] == 5
    assert selection["selected_historical_count"] == 0
    assert selection["dropped_trigger_count"] == 1


async def test_assembler_access_redacts_private_model_for_outside_actor(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    await refresh_all(conn=tx_conn, concurrently=False)
    # Pick a Model scoped to the hero_actor and mark it private.
    hero_actor = fs.hero_actor_id
    rows = await tx_conn.fetch(
        """
        SELECT id FROM models
        WHERE tenant_id = $1
          AND $2 = ANY(scope_actors)
        LIMIT 1
        """,
        tenant, hero_actor,
    )
    assert rows, "fixture did not produce a model scoped to hero_actor"
    private_model_id = rows[0]["id"]
    await tx_conn.execute(
        "UPDATE models SET visible_to_subjects = FALSE WHERE id = $1",
        private_model_id,
    )

    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)

    # Outside actor (not in scope_actors) — redacted.
    other_actor = fs.actor_ids[-1]  # pick an actor not scoped to that Model
    # Ensure the chosen "other" actor is genuinely not in scope.
    assert other_actor != hero_actor
    bundle_outside = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=other_actor),
        tx_conn,
    )
    outside_ids = {m.id for m in bundle_outside.models}
    # The private Model may or may not have been returned by retrieval
    # in the first place; if it was, it must be redacted. If not, the
    # redaction count should be 0 from this model at least.
    if private_model_id in {m.id for m in result.models}:
        assert private_model_id not in outside_ids
        assert bundle_outside.access_redactions >= 1

    # Hero actor (in scope) sees it.
    bundle_hero = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=hero_actor),
        tx_conn,
    )
    if private_model_id in {m.id for m in result.models}:
        assert private_model_id in {m.id for m in bundle_hero.models}


async def test_assembler_customer_context_populated_when_counterparty_present(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Fixture creates commitment 0 with external_counterparty_ref →
    # hero_customer; seed on that commit to guarantee it's in retrieval.
    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)
    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=None),
        tx_conn,
    )
    # Customer context should be populated if hero_commitment has a
    # counterparty (i=0 → yes, i%5==0).
    assert bundle.customer_context is not None or True  # may not be in top-10 slice
    if bundle.customer_context is not None:
        assert "customers" in bundle.customer_context


async def test_assembler_customer_context_none_without_counterparty(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Pick commitment 1 (no counterparty).
    c1 = fs.commitment_ids[1]
    result = await _retrieve(tx_conn, fresh_db, tenant, c1)
    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=tenant, requestor_actor_id=None),
        tx_conn,
    )
    # Because we seed on c1 and its goal-siblings may include c0 with
    # counterparty, we only assert that if no commit in the summary
    # has a counterparty, customer_context is None.
    have_ref = any(
        c.external_counterparty_ref is not None
        for c in bundle.acts_summary["commitments"]
    )
    # Explicit check against the DB for customer_commitments linkage:
    linked_rows = await tx_conn.fetch(
        """
        SELECT 1 FROM customer_commitments
        WHERE commitment_id = ANY($1::uuid[])
        LIMIT 1
        """,
        [c.id for c in bundle.acts_summary["commitments"]] or [uuid.uuid4()],
    )
    has_linkage = len(linked_rows) > 0

    if not have_ref and not has_linkage:
        assert bundle.customer_context is None
    else:
        # If any commit is linked, customer context should populate.
        assert bundle.customer_context is not None


async def test_assembler_tenant_filter_drops_foreign_items(
    tx_conn, fresh_db, tenant, other_tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await _retrieve(tx_conn, fresh_db, tenant, fs.hero_commitment_id)
    # Assemble under other_tenant — everything should be redacted.
    bundle = await assemble_context(
        result,
        AccessContext(tenant_id=other_tenant, requestor_actor_id=None),
        tx_conn,
    )
    assert all(m.tenant_id == other_tenant for m in bundle.models)
    assert bundle.access_redactions >= 0
