"""
RA-5 — RetrievalConfig + pathway C entity mentions + HNSW tuning.

Source: RETRIEVAL-DESIGN-AUDIT §11 item 5, §4 arg 2, §3 arg 4.

Verification (AUDIT-FIXES-IMPLEMENTATION-PLAN §2 RA-5):
  1. Config loads from environment variables when set.
  2. Changing semantic_k from 20 to 40 via config produces different
     retrieval results.
  3. Pathway C fix: observation mentioning an actor is returned even
     when the actor isn't author.
  4. All existing retrieval tests pass with default config (covered
     by the global suite run).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.retrieval.config import (
    RetrievalConfig,
    reload_config,
)
from services.retrieval.pathways import pathway_c_temporal
from services.retrieval.primary import TriggerContext, primary_retrieve

from services.retrieval.tests._fixtures import build_fixture, make_embedding


# ---------------------------------------------------------------------
# Unit: env loading
# ---------------------------------------------------------------------


def test_ra5_config_defaults_match_spec():
    cfg = RetrievalConfig()
    assert cfg.structural_k_per_entity == 5
    assert cfg.semantic_k == 20
    assert cfg.semantic_hnsw_ef_search == 80
    assert cfg.temporal_window_minutes == 60
    assert cfg.temporal_include_entity_mentions is True
    assert cfg.context_budget_tokens == 100_000
    assert cfg.mmr_lambda_diversity == 0.5
    assert cfg.second_pass_sparse_threshold == 5
    assert cfg.second_pass_bridge_confidence_threshold == 0.7


def test_ra5_config_env_overrides_int(monkeypatch):
    monkeypatch.setenv("RETRIEVAL_SEMANTIC_K", "40")
    monkeypatch.setenv("RETRIEVAL_SEMANTIC_HNSW_EF_SEARCH", "200")
    cfg = RetrievalConfig.from_env()
    assert cfg.semantic_k == 40
    assert cfg.semantic_hnsw_ef_search == 200


def test_ra5_config_env_overrides_bool(monkeypatch):
    monkeypatch.setenv("RETRIEVAL_TEMPORAL_INCLUDE_ENTITY_MENTIONS", "false")
    cfg = RetrievalConfig.from_env()
    assert cfg.temporal_include_entity_mentions is False
    monkeypatch.setenv("RETRIEVAL_TEMPORAL_INCLUDE_ENTITY_MENTIONS", "1")
    cfg = RetrievalConfig.from_env()
    assert cfg.temporal_include_entity_mentions is True


def test_ra5_config_env_overrides_float(monkeypatch):
    monkeypatch.setenv("RETRIEVAL_MMR_LAMBDA_DIVERSITY", "0.7")
    monkeypatch.setenv("RETRIEVAL_SECOND_PASS_BRIDGE_CONFIDENCE_THRESHOLD", "0.85")
    cfg = RetrievalConfig.from_env()
    assert abs(cfg.mmr_lambda_diversity - 0.7) < 1e-9
    assert abs(cfg.second_pass_bridge_confidence_threshold - 0.85) < 1e-9


def test_ra5_config_invalid_env_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("RETRIEVAL_SEMANTIC_K", "not_a_number")
    cfg = RetrievalConfig.from_env()
    assert cfg.semantic_k == 20  # default


def test_ra5_config_reload_updates_singleton(monkeypatch):
    monkeypatch.setenv("RETRIEVAL_SEMANTIC_K", "33")
    new = reload_config()
    assert new.semantic_k == 33
    # Restore via separate reload after monkeypatch unsets.
    monkeypatch.delenv("RETRIEVAL_SEMANTIC_K", raising=False)
    restored = reload_config()
    assert restored.semantic_k == 20


# ---------------------------------------------------------------------
# Integration: changing semantic_k via config alters retrieval results
# ---------------------------------------------------------------------


@pytest.mark.integration
async def test_ra5_semantic_k_change_alters_retrieval_results(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    seeds = [{"type": "commitment", "id": str(fs.hero_commitment_id)}]
    vec = make_embedding("alice ships reliably")
    base_trigger_kwargs = dict(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=seeds,
        seed_natural_text="alice ships reliably",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=vec,
        # Force semantic_k to the SDK default 40 so config can override
        # only when the trigger keeps that value. We set an explicit
        # field below.
    )

    # Config k=20.
    cfg_low = RetrievalConfig(semantic_k=20)
    trigger_default = TriggerContext(**base_trigger_kwargs)
    r_low = await primary_retrieve(trigger_default, tx_conn, config=cfg_low)

    # Config k=80 (much larger).
    cfg_high = RetrievalConfig(semantic_k=80)
    r_high = await primary_retrieve(
        TriggerContext(**base_trigger_kwargs), tx_conn, config=cfg_high,
    )

    # k=80 should pick up at least as many B-pathway models as k=20.
    pr_b_low = next((p for p in r_low.pathway_results if p.source_pathway == "B"), None)
    pr_b_high = next((p for p in r_high.pathway_results if p.source_pathway == "B"), None)
    assert pr_b_low is not None and pr_b_high is not None
    # Larger k → strictly >= count.
    assert len(pr_b_high.models) >= len(pr_b_low.models)
    # In the test fixture (100 Models), k=20 should saturate at 20 and
    # k=80 should saturate higher — they should differ.
    assert len(pr_b_high.models) > len(pr_b_low.models), (
        f"semantic_k change had no effect: {len(pr_b_low.models)} vs {len(pr_b_high.models)}"
    )


# ---------------------------------------------------------------------
# Integration: pathway C entity-mentions returns obs the actor only
# appears in entities_mentioned.
# ---------------------------------------------------------------------


async def _insert_obs_with_mention(
    conn, tenant: UUID, *, occurred_at: datetime,
    actor_id: UUID | None,
    mentions: list[dict],
) -> UUID:
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            source_actor_ref, actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, 'signal', 'ra5:test', 'ra5:test', $4,
            '{}'::jsonb, 'ra5 obs', $5,
            FALSE, 'authoritative', $6, $7::jsonb
        )
        """,
        oid, tenant, occurred_at, actor_id,
        make_embedding(f"ra5-obs-{oid}"),
        f"ra5-obs-{oid}",
        json.dumps(mentions),
    )
    return oid


@pytest.mark.integration
async def test_ra5_pathway_c_includes_entity_mentions_when_enabled(
    tx_conn, fresh_db, tenant
):
    """An observation where Alice is in entities_mentioned but NOT
    author_id should surface when temporal_include_entity_mentions is
    True (the new default)."""
    seed = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    other_actor = uuid7()
    alice = uuid7()
    # Insert two distinct actors (FK requirement-free since FKs to
    # actors aren't enforced from observations.actor_id at the DB level
    # in this schema; if needed we'd insert an actor row first).
    for aid, name in ((other_actor, "other"), (alice, "alice")):
        await tx_conn.execute(
            """
            INSERT INTO actors (
                id, tenant_id, type, display_name, email, status,
                metadata, created_at, last_seen_at
            ) VALUES (
                $1, $2, 'human_internal', $3, NULL, 'active',
                '{}'::jsonb, now(), NULL
            )
            """,
            aid, tenant, f"ra5-{name}-{aid}",
        )

    # Obs A: alice as author.
    obsA = await _insert_obs_with_mention(
        tx_conn, tenant,
        occurred_at=seed,
        actor_id=alice,
        mentions=[],
    )
    # Obs B: other actor as author, but alice in entities_mentioned.
    obsB = await _insert_obs_with_mention(
        tx_conn, tenant,
        occurred_at=seed + timedelta(minutes=5),
        actor_id=other_actor,
        mentions=[{"type": "actor", "id": str(alice)}],
    )
    # Obs C: unrelated.
    obsC = await _insert_obs_with_mention(
        tx_conn, tenant,
        occurred_at=seed + timedelta(minutes=10),
        actor_id=other_actor,
        mentions=[{"type": "actor", "id": str(uuid7())}],
    )

    # With include_entity_mentions=True (default), obs A and B both
    # surface for actor=alice; C does not.
    r_inc = await pathway_c_temporal(
        seed + timedelta(minutes=5),
        timedelta(minutes=30),
        tenant,
        tx_conn,
        scope_actors=[alice],
        include_entity_mentions=True,
    )
    inc_ids = {o.id for o in r_inc.observations}
    assert obsA in inc_ids, "author_id-matched obs missing"
    assert obsB in inc_ids, "entity-mention obs missing (the RA-5 fix)"
    assert obsC not in inc_ids

    # With include_entity_mentions=False (legacy), only A surfaces.
    r_excl = await pathway_c_temporal(
        seed + timedelta(minutes=5),
        timedelta(minutes=30),
        tenant,
        tx_conn,
        scope_actors=[alice],
        include_entity_mentions=False,
    )
    excl_ids = {o.id for o in r_excl.observations}
    assert obsA in excl_ids
    assert obsB not in excl_ids


@pytest.mark.integration
async def test_ra5_primary_retrieve_threads_config_through(
    tx_conn, fresh_db, tenant
):
    """primary_retrieve should report the active config in notes."""
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    cfg = RetrievalConfig(semantic_k=11, semantic_hnsw_ef_search=99)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="hello",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding("hello"),
    )
    r = await primary_retrieve(trigger, tx_conn, config=cfg)
    cs = r.notes["config_summary"]
    assert cs["semantic_k"] == 11
    assert cs["semantic_hnsw_ef_search"] == 99
    assert cs["temporal_include_entity_mentions"] is True
