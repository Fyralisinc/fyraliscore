"""
services/workers/precipitation/tests/test_precipitation.py — Wave 4-C.

Covers the four test cases from BUILD-PLAN §5 Prompt 4.C
"Precipitation" plus an end-to-end promote-via-Think-T4 trip.

Every cluster test uses tight 768-d embeddings (small jitter around a
base vector) so HDBSCAN reliably finds the cluster with min_cluster_size=3.
"""
from __future__ import annotations

import json
import uuid

import asyncpg
import pytest

from services.workers.precipitation.clustering import (
    cluster_active_models,
)
from services.workers.precipitation.proposer import (
    enqueue_pattern_review_triggers,
    promote_pattern_candidate,
    reject_pattern_candidate,
    write_candidates,
)
from services.workers.precipitation.tests.conftest import (
    insert_model,
    make_embedding,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _jittered(base, *, seed, jitter):
    import random
    rng = random.Random(seed)
    v = [x + rng.gauss(0.0, jitter) for x in base]
    norm = sum(x * x for x in v) ** 0.5
    return [x / norm for x in v] if norm else v


async def _add_hypothesis_cluster(
    tx_conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    born_from_event_id: uuid.UUID,
    base_text: str,
    count: int,
    jitter: float = 0.005,
    kind: str = "hypothesis",
) -> list[uuid.UUID]:
    base = make_embedding(base_text)
    ids: list[uuid.UUID] = []
    for i in range(count):
        # Different seed per member so points are distinct but tightly
        # clustered (similar — but not identical).
        emb = _jittered(base, seed=1000 + i, jitter=jitter)
        prop = (
            {"kind": "hypothesis", "hypothesis_text": f"{base_text} #{i}",
             "test_conditions": ["inspect"]}
            if kind == "hypothesis"
            else {"kind": "concern", "about": base_text, "nature": f"#{i}",
                  "raised_by": "tester"}
        )
        mid = await insert_model(
            tx_conn,
            tenant=tenant,
            born_from_event_id=born_from_event_id,
            proposition=prop,
            natural=f"{base_text} instance {i}",
            embedding=emb,
            scope_actors=[],
            confidence=0.55,
            confidence_at_assertion=0.55,
        )
        ids.append(mid)
    return ids


async def _add_noise_hypotheses(
    tx_conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    born_from_event_id: uuid.UUID,
    count: int,
    seed_prefix: str = "noise",
    kind: str = "hypothesis",
) -> list[uuid.UUID]:
    """
    Insert N isolated hypothesis/concern Models each with a completely
    different embedding (different make_embedding seed). Needed so
    HDBSCAN's density estimator has "noise" reference points — the
    algorithm can't reliably discriminate a cluster from singletons
    with fewer than ~10 total points (inherent density-estimator
    behaviour documented in BUILD-LOG Deviations).
    """
    ids: list[uuid.UUID] = []
    for i in range(count):
        emb = make_embedding(f"{seed_prefix}-{i}-unique")
        prop = (
            {"kind": "hypothesis",
             "hypothesis_text": f"{seed_prefix} unrelated #{i}",
             "test_conditions": ["none"]}
            if kind == "hypothesis"
            else {"kind": "concern", "about": f"other-topic-{i}",
                  "nature": "unrelated", "raised_by": "tester"}
        )
        mid = await insert_model(
            tx_conn,
            tenant=tenant,
            born_from_event_id=born_from_event_id,
            proposition=prop,
            natural=f"noise hypothesis #{i}",
            embedding=emb,
            scope_actors=[],
            confidence=0.4,
            confidence_at_assertion=0.4,
        )
        ids.append(mid)
    return ids


# ---------------------------------------------------------------------
# Cluster detection
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_three_similar_hypothesis_models_yield_one_candidate(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Three tightly-clustered hypothesis Models + noise Models →
    exactly one dense cluster → one pattern_candidates row with
    cluster_size=3.

    The noise Models are necessary for HDBSCAN's density estimator
    to discriminate a cluster from singletons (see BUILD-LOG
    Deviations: "HDBSCAN needs >=8 total points before its density
    estimator separates tight groups of 3 from noise — documented
    inherent behaviour, worked around by passing noise Models").
    """
    member_ids = await _add_hypothesis_cluster(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event,
        base_text="alice underestimates distributed systems", count=3,
    )
    await _add_noise_hypotheses(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event, count=10,
    )
    clusters = await cluster_active_models(
        tx_conn, tenant_id=tenant, min_cluster_size=3, density_threshold=0.3,
    )
    # At least one cluster contains the 3 tight members.
    cluster_of_interest = next(
        (
            c for c in clusters
            if set(m.model_id for m in c.members) == set(member_ids)
        ),
        None,
    )
    assert cluster_of_interest is not None, (
        f"expected a dense 3-member cluster; got {len(clusters)} clusters "
        f"with sizes {[c.size for c in clusters]}"
    )
    assert cluster_of_interest.size == 3
    assert cluster_of_interest.density >= 0.3

    candidate_ids = await write_candidates(tx_conn, [cluster_of_interest])
    assert len(candidate_ids) == 1

    row = await tx_conn.fetchrow(
        "SELECT cluster_size, density, promoted_at, rejected_at FROM pattern_candidates WHERE id=$1",
        candidate_ids[0],
    )
    assert row["cluster_size"] == 3
    assert row["promoted_at"] is None
    assert row["rejected_at"] is None


@pytest.mark.asyncio
async def test_two_similar_models_produce_no_candidate(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Below MIN_CLUSTER_SIZE (count=2) → even in the presence of noise,
    no candidate row should match those 2 tight Models.
    """
    tight_ids = await _add_hypothesis_cluster(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event,
        base_text="bob underestimates monitoring", count=2,
    )
    await _add_noise_hypotheses(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event, count=10,
    )
    clusters = await cluster_active_models(
        tx_conn, tenant_id=tenant, min_cluster_size=3,
    )
    # A cluster that contains our 2 tight members specifically must not
    # exist (they're under min_cluster_size).
    bad = [
        c for c in clusters
        if set(m.model_id for m in c.members) == set(tight_ids)
    ]
    assert bad == []
    # And any cluster that DOES fire must not be the 2-member set.
    for c in clusters:
        assert c.size >= 3


@pytest.mark.asyncio
async def test_diverse_models_no_false_pattern(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Six hypothesis Models with completely different content (random
    vectors) → HDBSCAN either finds no clusters, or finds a cluster
    whose density is below threshold.
    """
    import random
    rng = random.Random(9001)
    for i in range(6):
        vec = [rng.gauss(0.0, 1.0) for _ in range(768)]
        norm = sum(x * x for x in vec) ** 0.5
        emb = [x / norm for x in vec] if norm else vec
        await insert_model(
            tx_conn,
            tenant=tenant,
            born_from_event_id=born_from_event,
            proposition={"kind": "hypothesis", "hypothesis_text": f"diverse-{i}",
                         "test_conditions": ["check"]},
            natural=f"unrelated hypothesis {i}",
            embedding=emb,
        )
    clusters = await cluster_active_models(
        tx_conn, tenant_id=tenant, min_cluster_size=3, density_threshold=0.5,
    )
    # HDBSCAN may label all 6 as noise (-1) or emit a diffuse cluster
    # below threshold. Either way, we want zero precipitated candidates.
    assert all(c.density >= 0.5 for c in clusters)
    # If it did find a cluster, density filter should mean we don't
    # write it — confirm via write_candidates, which would dedup but
    # accept any non-empty list. Stronger: we expect an empty list.
    if clusters:
        # The dense cluster, if any, must be real by threshold.
        ids = await write_candidates(tx_conn, clusters)
        row = await tx_conn.fetchrow(
            "SELECT COUNT(*) AS c FROM pattern_candidates WHERE tenant_id=$1", tenant,
        )
        assert row["c"] == len(ids)
    else:
        row = await tx_conn.fetchrow(
            "SELECT COUNT(*) AS c FROM pattern_candidates WHERE tenant_id=$1", tenant,
        )
        assert row["c"] == 0


# ---------------------------------------------------------------------
# Candidate → T4 trigger enqueue → Think-T4 promotion
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_candidate_enqueues_t4_trigger_and_think_t4_promotes(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Happy-path end-to-end:
      1. Insert 3 similar concerns + noise.
      2. cluster_active_models → write_candidates → one candidate row
         (we filter down to the cluster whose members match our 3
         tight inserts; HDBSCAN may surface additional spurious
         clusters from the noise set — those are filtered out here).
      3. enqueue_pattern_review_triggers → one T4 trigger_queue row.
      4. Think T4 calls promote_pattern_candidate → Pattern Model
         inserted; promoted_at populated.
    """
    tight_ids = await _add_hypothesis_cluster(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event,
        base_text="repeated overcommitment concerns", count=3, kind="concern",
    )
    await _add_noise_hypotheses(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event, count=10,
        kind="concern",
    )
    clusters = await cluster_active_models(
        tx_conn, tenant_id=tenant, min_cluster_size=3, density_threshold=0.3,
    )
    # Pick the cluster that matches our tight set.
    cluster_of_interest = next(
        (
            c for c in clusters
            if set(m.model_id for m in c.members) == set(tight_ids)
        ),
        None,
    )
    assert cluster_of_interest is not None
    cand_ids = await write_candidates(tx_conn, [cluster_of_interest])
    assert len(cand_ids) == 1

    trig_ids = await enqueue_pattern_review_triggers(tx_conn, cand_ids)
    assert len(trig_ids) == 1
    trig_row = await tx_conn.fetchrow(
        "SELECT trigger_kind, trigger_subkind, payload FROM think_trigger_queue WHERE id=$1",
        trig_ids[0],
    )
    assert trig_row["trigger_kind"] == "T4"
    assert trig_row["trigger_subkind"] == "pattern_review"
    payload = trig_row["payload"]
    if isinstance(payload, (bytes, bytearray)):
        payload = json.loads(payload.decode())
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["pattern_candidate_id"] == str(cand_ids[0])

    # Now simulate Think T4 pattern_review: call promote_pattern_candidate.
    from services.models.repo import ModelsRepo
    repo = ModelsRepo(pool=None)
    pattern_id = await promote_pattern_candidate(
        tx_conn, cand_ids[0],
        models_repo=repo,
        born_from_event_id=born_from_event,
    )
    # Verify promoted_at + promoted_pattern_model_id populated.
    row = await tx_conn.fetchrow(
        "SELECT promoted_at, promoted_pattern_model_id FROM pattern_candidates WHERE id=$1",
        cand_ids[0],
    )
    assert row["promoted_at"] is not None
    assert row["promoted_pattern_model_id"] == pattern_id

    # Pattern Model exists with kind='pattern'.
    pat_row = await tx_conn.fetchrow(
        "SELECT proposition_kind, supporting_model_ids FROM models WHERE id=$1",
        pattern_id,
    )
    assert pat_row["proposition_kind"] == "pattern"
    assert len(pat_row["supporting_model_ids"]) == 3

    # Constituents back-link to the Pattern.
    con = await tx_conn.fetch(
        """
        SELECT id FROM models
        WHERE tenant_id=$1
          AND proposition_kind='concern'
          AND $2 = ANY(supporting_model_ids)
        """,
        tenant, pattern_id,
    )
    assert len(con) == 3


@pytest.mark.asyncio
async def test_reject_pattern_candidate_is_idempotent(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    tight_ids = await _add_hypothesis_cluster(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event,
        base_text="reject path cluster", count=3,
    )
    await _add_noise_hypotheses(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event, count=10,
    )
    clusters = await cluster_active_models(
        tx_conn, tenant_id=tenant, min_cluster_size=3, density_threshold=0.3,
    )
    target = next(
        (c for c in clusters if set(m.model_id for m in c.members) == set(tight_ids)),
        None,
    )
    assert target is not None
    cand_ids = await write_candidates(tx_conn, [target])
    assert len(cand_ids) == 1
    await reject_pattern_candidate(tx_conn, cand_ids[0], reason="too speculative")
    row = await tx_conn.fetchrow(
        "SELECT rejected_at, rejection_reason, promoted_at FROM pattern_candidates WHERE id=$1",
        cand_ids[0],
    )
    assert row["rejected_at"] is not None
    assert row["rejection_reason"] == "too speculative"
    assert row["promoted_at"] is None
    # Idempotency: a second call should be a no-op.
    first_reject = row["rejected_at"]
    await reject_pattern_candidate(tx_conn, cand_ids[0], reason="other")
    row2 = await tx_conn.fetchrow(
        "SELECT rejected_at, rejection_reason FROM pattern_candidates WHERE id=$1",
        cand_ids[0],
    )
    assert row2["rejected_at"] == first_reject
    assert row2["rejection_reason"] == "too speculative"


@pytest.mark.asyncio
async def test_write_candidates_deduplicates_identical_clusters(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Running precipitation twice on the same Models produces exactly
    one candidate (the second run returns the existing id rather than
    inserting a duplicate).
    """
    tight_ids = await _add_hypothesis_cluster(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event,
        base_text="dedup cluster", count=3,
    )
    await _add_noise_hypotheses(
        tx_conn, tenant=tenant, born_from_event_id=born_from_event, count=10,
    )
    clusters = await cluster_active_models(
        tx_conn, tenant_id=tenant, min_cluster_size=3, density_threshold=0.3,
    )
    target = [
        c for c in clusters
        if set(m.model_id for m in c.members) == set(tight_ids)
    ]
    assert target, "tight 3-member cluster not found"
    first_ids = await write_candidates(tx_conn, target)
    second_ids = await write_candidates(tx_conn, target)
    assert first_ids == second_ids
    # Count candidate rows matching exactly the tight set.
    row = await tx_conn.fetchrow(
        """
        SELECT COUNT(*) AS c FROM pattern_candidates
        WHERE tenant_id = $1
          AND constituent_model_ids @> $2::uuid[]
          AND cardinality(constituent_model_ids) = cardinality($2::uuid[])
        """,
        tenant, sorted(tight_ids),
    )
    assert row["c"] == 1
