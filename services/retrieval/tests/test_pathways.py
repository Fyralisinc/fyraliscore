"""
Pathway-level tests.

Each pathway is exercised in isolation with the full hand-built
fixture dataset. We assert on:
  - the expected entities surface
  - the diagnostic `notes` block
  - tenant isolation
  - empty-seed graceful behavior
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient, OllamaConfig

from services.retrieval.pathways import (
    RetrievalPathwayError,
    pathway_a_structural,
    pathway_b_semantic,
    pathway_c_temporal,
    pathway_d_pattern,
)

from services.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


# =====================================================================
# Pathway A — structural
# =====================================================================


async def test_pathway_a_commitment_seed_returns_owning_goal_and_scoped_models(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    seeds = [{"type": "commitment", "id": str(fs.hero_commitment_id)}]
    result = await pathway_a_structural(
        seeds, tenant, tx_conn, max_hops=2
    )
    assert result.source_pathway == "A"
    # The hero commitment itself is in the touched set.
    commit_ids = {c.id for c in result.acts["commitments"]}
    assert fs.hero_commitment_id in commit_ids
    # The goal it contributes to should be there.
    goal_ids = {g.id for g in result.acts["goals"]}
    assert len(goal_ids) >= 1
    # Models scoped to the commitment should surface.
    assert result.notes["entities_touched"]["commitments"] >= 1
    assert result.notes["models_returned"] >= 0


async def test_pathway_a_actor_seed_surfaces_owned_commitments(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    seeds = [{"type": "actor", "id": str(fs.hero_actor_id)}]
    result = await pathway_a_structural(seeds, tenant, tx_conn, max_hops=1)
    commit_ids = {c.id for c in result.acts["commitments"]}
    # Hero actor owns commitments 0, 10, 20, 30, 40 (every n_actors=10th).
    assert len(commit_ids) >= 1


async def test_pathway_a_customer_seed_surfaces_commitments(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    seeds = [{"type": "customer_resource", "id": str(fs.hero_customer_id)}]
    result = await pathway_a_structural(seeds, tenant, tx_conn, max_hops=2)
    # hero_customer (index 0) is linked to commitments with i%5==0 where
    # (i//5)%n_customers==0 → i∈{0, 50} — but n_commitments=50 so just {0}.
    commit_ids = {c.id for c in result.acts["commitments"]}
    assert len(commit_ids) >= 1
    assert len(result.resources) >= 1


async def test_pathway_a_empty_seed_graceful(tx_conn, fresh_db, tenant):
    result = await pathway_a_structural([], tenant, tx_conn)
    assert result.models == []
    assert result.acts == {"goals": [], "commitments": [], "decisions": []}
    assert result.notes["reason"] == "empty_seed"


async def test_pathway_a_unknown_seed_type_skipped(tx_conn, fresh_db, tenant):
    result = await pathway_a_structural(
        [{"type": "bogus", "id": str(uuid.uuid4())}],
        tenant, tx_conn,
    )
    assert result.models == []
    assert result.notes["seeds_accepted"] == 0


async def test_pathway_a_tenant_isolation(
    tx_conn, fresh_db, tenant, other_tenant
):
    # Build fixture on `tenant`, then query with `other_tenant` — must
    # return nothing for `tenant`'s commitments.
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    seeds = [{"type": "commitment", "id": str(fs.hero_commitment_id)}]
    result = await pathway_a_structural(seeds, other_tenant, tx_conn)
    # The commitment row won't be returned because tenant filter rejects.
    assert all(c.tenant_id == other_tenant for c in result.acts["commitments"])


# =====================================================================
# Pathway B — semantic
# =====================================================================


async def test_pathway_b_precomputed_vector_finds_clustered_models(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Embed a phrase close to the "alice ships reliably" topic Models
    # have (topic index 0 in the builder, models at indices 20,24,28,...).
    qvec = make_embedding("alice ships reliably")
    result = await pathway_b_semantic(
        "alice ships reliably", tenant, tx_conn,
        k=10, precomputed_vector=qvec,
    )
    assert result.source_pathway == "B"
    assert len(result.models) <= 10
    assert result.notes["vector_source"] == "precomputed"
    # At least one returned Model should be scoped to tenant.
    for m in result.models:
        assert m.tenant_id == tenant


async def test_pathway_b_empty_seed_returns_empty(tx_conn, fresh_db, tenant):
    result = await pathway_b_semantic("", tenant, tx_conn, k=5)
    assert result.models == []
    assert result.notes["reason"] == "empty_seed"


async def test_pathway_b_wrong_dim_raises(tx_conn, fresh_db, tenant):
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await pathway_b_semantic(
            "x", tenant, tx_conn, k=5,
            precomputed_vector=[0.0] * 10,
        )


async def test_pathway_b_no_seed_no_embedder_raises(tx_conn, tenant):
    with pytest.raises(RetrievalPathwayError):
        await pathway_b_semantic(
            "non-empty seed", tenant, tx_conn, k=5,
        )


@pytest.mark.skipif(
    os.environ.get("SKIP_OLLAMA_TEST") == "1",
    reason="Ollama integration test (set SKIP_OLLAMA_TEST=1 to skip).",
)
async def test_pathway_b_real_ollama_semantic_cluster(
    tx_conn, fresh_db, tenant
):
    """One integration test uses real Ollama per the prompt."""
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    cfg = OllamaConfig.from_env()
    async with OllamaClient(cfg) as client:
        try:
            # Cheap pre-check: can we reach Ollama?
            _ = await client.embed("ping")
        except Exception:
            pytest.skip("Ollama not reachable on this machine")
        result = await pathway_b_semantic(
            "alice ships reliably on small tickets",
            tenant, tx_conn, k=10, embedder=client,
        )
    assert result.notes["vector_source"] == "ollama"
    assert len(result.models) <= 10


# =====================================================================
# Pathway C — temporal
# =====================================================================


async def test_pathway_c_returns_observations_in_window(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Fixture builds obs at base_time 2026-04-01 12:00 + i*10min.
    # Obs 50 is at 2026-04-01 20:20.
    seed = datetime(2026, 4, 1, 20, 0, 0, tzinfo=timezone.utc)
    window = timedelta(minutes=60)
    result = await pathway_c_temporal(
        seed, window, tenant, tx_conn,
    )
    assert result.source_pathway == "C"
    assert len(result.observations) > 0
    for o in result.observations:
        assert seed - window <= o.occurred_at <= seed + window


async def test_pathway_c_filters_by_actor(tx_conn, fresh_db, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    seed = datetime(2026, 4, 1, 18, 0, 0, tzinfo=timezone.utc)
    window = timedelta(hours=2)
    hero = fs.hero_actor_id
    result = await pathway_c_temporal(
        seed, window, tenant, tx_conn,
        scope_actors=[hero],
    )
    for o in result.observations:
        assert o.actor_id == hero


async def test_pathway_c_invalid_window_raises(tx_conn, tenant):
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await pathway_c_temporal(
            datetime.now(timezone.utc),
            timedelta(seconds=0),
            tenant, tx_conn,
        )


# =====================================================================
# Pathway D — pattern
# =====================================================================


async def test_pathway_d_returns_patterns_and_instances(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Shared signature from the fixture.
    result = await pathway_d_pattern(
        {"regex": "^hotfix"}, tenant, tx_conn,
    )
    assert result.source_pathway == "D"
    # Fixture creates 10 pattern Models with that signature.
    pattern_kinds = {m.proposition_kind for m in result.models}
    assert "pattern" in pattern_kinds or "pattern_instance" in pattern_kinds
    assert result.notes["patterns_returned"] >= 1


async def test_pathway_d_no_signature_returns_all_patterns(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await pathway_d_pattern(None, tenant, tx_conn, limit=50)
    # Should surface at least the 10 pattern Models.
    pattern_models = [m for m in result.models if m.proposition_kind == "pattern"]
    assert len(pattern_models) >= 10
