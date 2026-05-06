"""services/workers/anomaly_processor/tests/test_anomaly_processor.py.

Wave 4-B test matrix per BUILD-PLAN Prompt 4.B and the agent brief.

Test breakdown (22 tests, 20+ minimum):

Per-detector positive/negative (12):
 1. contestation_cluster fires
 2. contestation_cluster outside window doesn't fire
 3. silent_disagreement fires
 4. silent_disagreement no rise doesn't fire
 5. activation_decay_anomaly fires
 6. activation_decay_anomaly uniform cohort doesn't fire
 7. external_signal_anomaly fires
 8. external_signal_anomaly single observation doesn't fire
 9. commitment_drift fires
10. commitment_drift single event doesn't fire
11. resource_overcommit fires
12. resource_overcommit normal util doesn't fire

Significance modulators (3):
13. critical-path modulator (×1.5)
14. customer modulator (×1.3)
15. trust-tier weighting

Debounce + memory fabric (3):
16. debounce suppresses duplicate within window
17. sub-threshold candidate → Memory Fabric
18. Memory Fabric promotion after 6 sub-threshold signals

End-to-end + property + false-positive (4):
19. False-positive resistance (100 normal messages → 0 enqueues)
20. Tenant isolation (tenant A doesn't affect tenant B)
21. End-to-end: T3 enqueue payload contains region_spec AND
    seed_entity_ids AND is rehydrated cleanly by _populate_seed_fields.
22. Rate limit per tenant: 30 detections → 20 enqueues + 10 fabric.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7

from services.workers.anomaly_processor import (
    AnomalyProcessor,
    AnomalyProcessorConfig,
)
from services.workers.anomaly_processor.debounce import (
    compute_region_hash,
)
from services.workers.anomaly_processor.detectors import (
    AnomalyCandidate,
    detect_activation_decay_anomaly,
    detect_commitment_drift,
    detect_contestation_cluster,
    detect_external_signal_anomaly,
    detect_resource_overcommit,
    detect_silent_disagreement,
)
from services.workers.anomaly_processor.memory_fabric import (
    promote_if_accumulated,
    record_subthreshold_signal,
)
from services.workers.anomaly_processor.significance import (
    compute_significance,
)

from .conftest import (
    insert_actor,
    insert_commitment,
    insert_contributes_to,
    insert_goal,
    insert_minimal_model,
    insert_observation,
    insert_resource,
)


pytestmark = pytest.mark.integration


# =====================================================================
# 1. contestation_cluster — fires
# =====================================================================


async def test_contestation_cluster_fires_within_window(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant, "Alice")
        seed_obs = await insert_observation(conn, tenant, actor_id=actor)
        model = await insert_minimal_model(
            conn, tenant, born_from_event_id=seed_obs,
            scope_actors=[actor],
        )
        now = datetime.now(timezone.utc)
        # 5 contestations within 30 minutes
        obs_ids = []
        for i in range(5):
            oid = await insert_observation(
                conn, tenant,
                actor_id=actor,
                kind="contestation",
                content={"contested_model_id": str(model), "i": i},
                content_text=f"contest {i}",
                occurred_at=now - timedelta(minutes=i * 2),
                source_channel="test",
                trust_tier="attested_agent",
                external_id=f"contest-{i}",
            )
            obs_ids.append(oid)

        candidates = await detect_contestation_cluster(
            tenant, timedelta(minutes=30), conn,
        )
    assert len(candidates) == 1
    c = candidates[0]
    assert c.kind == "contestation_cluster"
    assert c.entity_id == model
    assert set(c.triggering_observation_ids) == set(obs_ids)
    assert c.significance > 0.4


# =====================================================================
# 2. contestation_cluster — outside window, doesn't fire
# =====================================================================


async def test_contestation_cluster_does_not_fire_over_10_days(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant, "Alice")
        seed_obs = await insert_observation(conn, tenant, actor_id=actor)
        model = await insert_minimal_model(
            conn, tenant, born_from_event_id=seed_obs,
        )
        now = datetime.now(timezone.utc)
        # 5 contestations spread over 10 days
        for i in range(5):
            await insert_observation(
                conn, tenant,
                actor_id=actor,
                kind="contestation",
                content={"contested_model_id": str(model)},
                content_text="contest",
                occurred_at=now - timedelta(days=i * 2),
                external_id=f"contest-{i}",
            )
        # Window = 30 min
        candidates = await detect_contestation_cluster(
            tenant, timedelta(minutes=30), conn,
        )
    assert candidates == []


# =====================================================================
# 3. silent_disagreement — fires
# =====================================================================


async def test_silent_disagreement_fires(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        alice = await insert_actor(conn, tenant, "Alice")
        seed_obs = await insert_observation(conn, tenant, actor_id=alice)
        # 4 Slack messages from Alice, NONE in supporting_event_ids.
        for i in range(4):
            await insert_observation(
                conn, tenant, actor_id=alice,
                kind="signal", source_channel="slack:x",
                content_text=f"msg {i}", external_id=f"m-{i}",
            )
        # Model about Alice, confidence rose from 0.4 → 0.7, no support from her.
        model = await insert_minimal_model(
            conn, tenant, born_from_event_id=seed_obs,
            scope_actors=[alice],
            confidence=0.7, confidence_at_assertion=0.4,
            supporting_event_ids=[],  # empty on purpose
        )
        cands = await detect_silent_disagreement(
            tenant, timedelta(days=7), conn,
        )
    assert any(c.entity_id == model for c in cands)


# =====================================================================
# 4. silent_disagreement — doesn't fire when actor is supporting
# =====================================================================


async def test_silent_disagreement_no_rise_does_not_fire(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        alice = await insert_actor(conn, tenant, "Alice")
        seed_obs = await insert_observation(conn, tenant, actor_id=alice)
        for i in range(4):
            await insert_observation(
                conn, tenant, actor_id=alice,
                source_channel="slack:x",
                content_text=f"msg {i}", external_id=f"m-{i}",
            )
        # Confidence DIDN'T rise (prior == current) → no silent disagreement.
        await insert_minimal_model(
            conn, tenant, born_from_event_id=seed_obs,
            scope_actors=[alice],
            confidence=0.5, confidence_at_assertion=0.5,
        )
        cands = await detect_silent_disagreement(
            tenant, timedelta(days=7), conn,
        )
    assert cands == []


# =====================================================================
# 5. activation_decay_anomaly — fires
# =====================================================================


async def test_activation_decay_anomaly_fires(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        seed_obs = await insert_observation(conn, tenant)
        # Cohort of 4 'belief' models at activation 0.3, one at 0.95.
        for i in range(4):
            mid = await insert_minimal_model(
                conn, tenant, born_from_event_id=seed_obs,
                natural=f"cold model {i}",
                activation=0.3,
            )
        hot = await insert_minimal_model(
            conn, tenant, born_from_event_id=seed_obs,
            natural="HOT model",
            activation=0.95,
        )
        cands = await detect_activation_decay_anomaly(tenant, conn)
    hot_cands = [c for c in cands if c.entity_id == hot]
    assert len(hot_cands) == 1
    assert hot_cands[0].significance > 0.4


# =====================================================================
# 6. activation_decay_anomaly — uniform cohort, doesn't fire
# =====================================================================


async def test_activation_decay_anomaly_uniform_cohort_no_fire(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        seed_obs = await insert_observation(conn, tenant)
        for i in range(5):
            await insert_minimal_model(
                conn, tenant, born_from_event_id=seed_obs,
                natural=f"model {i}",
                activation=0.3 + 0.02 * i,  # tight cluster around 0.3-0.38
            )
        cands = await detect_activation_decay_anomaly(tenant, conn)
    assert cands == []


# =====================================================================
# 7. external_signal_anomaly — fires
# =====================================================================


async def test_external_signal_anomaly_fires(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        customer_ref = {"type": "customer", "id": str(uuid7())}
        now = datetime.now(timezone.utc)
        for i in range(4):
            await insert_observation(
                conn, tenant,
                content_text=f"news {i}",
                trust_tier="authoritative_external",
                entities_mentioned=[customer_ref],
                occurred_at=now - timedelta(minutes=i * 5),
                external_id=f"news-{i}",
            )
        cands = await detect_external_signal_anomaly(
            tenant, timedelta(hours=1), conn,
        )
    assert len(cands) >= 1
    found = [c for c in cands if c.payload.get("entity_ref") == customer_ref["id"]]
    assert len(found) == 1
    assert found[0].significance > 0.4


# =====================================================================
# 8. external_signal_anomaly — single observation doesn't fire
# =====================================================================


async def test_external_signal_anomaly_single_no_fire(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        customer_ref = {"type": "customer", "id": str(uuid7())}
        await insert_observation(
            conn, tenant,
            trust_tier="authoritative_external",
            entities_mentioned=[customer_ref],
        )
        cands = await detect_external_signal_anomaly(
            tenant, timedelta(hours=1), conn,
        )
    assert cands == []


# =====================================================================
# 9. commitment_drift — fires
# =====================================================================


async def test_commitment_drift_fires(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        alice = await insert_actor(conn, tenant)
        seed_obs = await insert_observation(conn, tenant)
        commit = await insert_commitment(
            conn, tenant, owner_id=alice, created_by_event_id=seed_obs,
            due_date=datetime.now(timezone.utc) + timedelta(days=7),
        )
        for i in range(3):
            await insert_observation(
                conn, tenant,
                kind="state_change",
                source_channel="internal:state_change",
                content={
                    "entity_id": str(commit),
                    "entity_kind": "commitment",
                    "state_change_kind": "due_date_extended",
                    "metadata": {"from": "2026-05-01", "to": f"2026-06-0{i+1}"},
                },
                content_text=f"extend {i}",
                trust_tier="authoritative",
                external_id=f"drift-{i}",
            )
        cands = await detect_commitment_drift(
            tenant, timedelta(days=28), conn,
        )
    assert len(cands) == 1
    assert cands[0].entity_id == commit
    assert cands[0].payload["drift_event_count"] == 3


# =====================================================================
# 10. commitment_drift — single event, doesn't fire
# =====================================================================


async def test_commitment_drift_single_event_no_fire(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        alice = await insert_actor(conn, tenant)
        seed_obs = await insert_observation(conn, tenant)
        commit = await insert_commitment(
            conn, tenant, owner_id=alice, created_by_event_id=seed_obs,
            due_date=datetime.now(timezone.utc) + timedelta(days=30),
        )
        await insert_observation(
            conn, tenant,
            kind="state_change",
            source_channel="internal:state_change",
            content={
                "entity_id": str(commit),
                "entity_kind": "commitment",
                "state_change_kind": "due_date_extended",
            },
            content_text="extend",
            trust_tier="authoritative",
            external_id="drift-1",
        )
        cands = await detect_commitment_drift(
            tenant, timedelta(days=28), conn,
        )
    assert cands == []


# =====================================================================
# 11. resource_overcommit — fires
# =====================================================================


async def test_resource_overcommit_fires(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        r = await insert_resource(
            conn, tenant,
            kind="capacity",
            current_value={
                "total_units": 100.0,
                "deployed_units": 98.0,
                "available_units": 2.0,
            },
        )
        cands = await detect_resource_overcommit(tenant, conn)
    matched = [c for c in cands if c.entity_id == r]
    assert len(matched) == 1
    assert matched[0].significance == 0.8


# =====================================================================
# 12. resource_overcommit — normal util, no fire
# =====================================================================


async def test_resource_overcommit_normal_util_no_fire(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        await insert_resource(
            conn, tenant,
            kind="capacity",
            current_value={
                "total_units": 100.0,
                "deployed_units": 50.0,
                "available_units": 50.0,
            },
        )
        cands = await detect_resource_overcommit(tenant, conn)
    assert cands == []


# =====================================================================
# 13. Significance — critical-path modulator (×1.5)
# =====================================================================


async def test_significance_critical_path_modulator(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    """Same candidate shape — one is on a critical-path goal, one isn't."""
    async with fresh_db.acquire() as conn:
        alice = await insert_actor(conn, tenant)
        seed_obs = await insert_observation(conn, tenant)
        goal = await insert_goal(conn, tenant, created_by_event_id=seed_obs)
        commit_cp = await insert_commitment(
            conn, tenant, owner_id=alice, created_by_event_id=seed_obs,
            due_date=datetime.now(timezone.utc) + timedelta(days=7),
        )
        commit_nocp = await insert_commitment(
            conn, tenant, owner_id=alice, created_by_event_id=seed_obs,
            due_date=datetime.now(timezone.utc) + timedelta(days=7),
        )
        await insert_contributes_to(
            conn, commitment_id=commit_cp, goal_id=goal,
            is_critical_path=True,
        )
        await insert_contributes_to(
            conn, commitment_id=commit_nocp, goal_id=goal,
            is_critical_path=False,
        )
        # Build candidates with the same base=0.4.
        cand_cp = AnomalyCandidate(
            kind="commitment_drift",
            entity_type="commitment",
            entity_id=commit_cp,
            tenant_id=tenant,
            region_entity_ids=[{"entity_kind": "commitment", "entity_id": str(commit_cp)}],
            significance=0.4,
        )
        cand_nocp = AnomalyCandidate(
            kind="commitment_drift",
            entity_type="commitment",
            entity_id=commit_nocp,
            tenant_id=tenant,
            region_entity_ids=[{"entity_kind": "commitment", "entity_id": str(commit_nocp)}],
            significance=0.4,
        )
        sig_cp = await compute_significance(cand_cp, conn)
        sig_nocp = await compute_significance(cand_nocp, conn)

    assert sig_cp == pytest.approx(0.4 * 1.5, rel=0.01)
    assert sig_nocp == pytest.approx(0.4, rel=0.01)
    assert sig_cp > sig_nocp


# =====================================================================
# 14. Significance — customer modulator (×1.3)
# =====================================================================


async def test_significance_customer_modulator(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        alice = await insert_actor(conn, tenant)
        seed_obs = await insert_observation(conn, tenant)
        # Commitment with external_counterparty_ref (i.e., customer-linked).
        commit_customer = await insert_commitment(
            conn, tenant, owner_id=alice, created_by_event_id=seed_obs,
            due_date=datetime.now(timezone.utc) + timedelta(days=7),
            external_counterparty_ref={"type": "customer", "id": "acme"},
        )
        commit_plain = await insert_commitment(
            conn, tenant, owner_id=alice, created_by_event_id=seed_obs,
            due_date=datetime.now(timezone.utc) + timedelta(days=7),
        )
        cand_customer = AnomalyCandidate(
            kind="commitment_drift",
            entity_type="commitment",
            entity_id=commit_customer,
            tenant_id=tenant,
            region_entity_ids=[{"entity_kind": "commitment", "entity_id": str(commit_customer)}],
            significance=0.5,
        )
        cand_plain = AnomalyCandidate(
            kind="commitment_drift",
            entity_type="commitment",
            entity_id=commit_plain,
            tenant_id=tenant,
            region_entity_ids=[{"entity_kind": "commitment", "entity_id": str(commit_plain)}],
            significance=0.5,
        )
        sig_cust = await compute_significance(cand_customer, conn)
        sig_plain = await compute_significance(cand_plain, conn)
    assert sig_cust == pytest.approx(0.5 * 1.3, rel=0.01)
    assert sig_plain == pytest.approx(0.5, rel=0.01)


# =====================================================================
# 15. Significance — trust-tier weighting
# =====================================================================


async def test_significance_trust_tier_weighting(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        cand_high = AnomalyCandidate(
            kind="external_signal_anomaly",
            entity_type="customer",
            entity_id=uuid7(),
            tenant_id=tenant,
            region_entity_ids=[{"entity_kind": "customer", "entity_id": "x"}],
            significance=0.4,
            trust_tiers=["authoritative_external"],
        )
        cand_low = AnomalyCandidate(
            kind="external_signal_anomaly",
            entity_type="customer",
            entity_id=uuid7(),
            tenant_id=tenant,
            region_entity_ids=[{"entity_kind": "customer", "entity_id": "y"}],
            significance=0.4,
            trust_tiers=["inferential"],
        )
        sig_high = await compute_significance(cand_high, conn)
        sig_low = await compute_significance(cand_low, conn)
    assert sig_high > sig_low
    # multiplier 1.15 for authoritative_external, 1.0 for inferential
    assert sig_high == pytest.approx(0.4 * 1.15, rel=0.02)
    assert sig_low == pytest.approx(0.4, rel=0.02)


# =====================================================================
# 16. Debounce — same kind/region within 30 min suppressed
# =====================================================================


async def test_debounce_suppresses_duplicate(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        seed = await insert_observation(conn, tenant)
        r = await insert_resource(
            conn, tenant, kind="capacity",
            current_value={"total_units": 10, "deployed_units": 10, "available_units": 0},
        )
        processor = AnomalyProcessor(
            fresh_db,
            config=AnomalyProcessorConfig(
                poll_interval_s=1.0,
                debounce_window_minutes=30,
                promote_every_n_cycles=0,
                t3_budget_per_tenant_per_min=100,
            ),
        )
        # First pass — should enqueue.
        c1 = await processor.process_once([tenant])
        # Second pass — same anomaly, same region → debounced.
        c2 = await processor.process_once([tenant])

    assert c1["enqueued_t3"] >= 1
    assert c2["debounced"] >= 1
    assert c2["enqueued_t3"] == 0


# =====================================================================
# 17. Sub-threshold → Memory Fabric
# =====================================================================


async def test_subthreshold_signal_writes_to_fabric(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        cand = AnomalyCandidate(
            kind="commitment_drift",
            entity_type="commitment",
            entity_id=uuid7(),
            tenant_id=tenant,
            region_entity_ids=[{"entity_kind": "commitment", "entity_id": "c1"}],
            significance=0.1,  # definitely sub-threshold
        )
        region_hash = compute_region_hash(tenant, cand.kind, cand.region_entity_ids)
        row_id = await record_subthreshold_signal(cand, region_hash, 0.1, conn)
        cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM signal_memory_fabric WHERE id = $1",
            row_id,
        )
        # And trigger queue empty for this tenant.
        q_cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant,
        )
    assert cnt == 1
    assert q_cnt == 0


# =====================================================================
# 18. Memory Fabric promotion after 6 signals in 7 days
# =====================================================================


async def test_memory_fabric_promotion(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    """6 sub-threshold rows in window → promote returns a candidate, and
    `promoted_at` is stamped on all of them."""
    async with fresh_db.acquire() as conn:
        # Build 6 identical sub-threshold candidates for the same region.
        entity = uuid7()
        region = [{"entity_kind": "commitment", "entity_id": str(entity)}]
        region_hash = compute_region_hash(tenant, "commitment_drift", region)
        for i in range(6):
            cand = AnomalyCandidate(
                kind="commitment_drift",
                entity_type="commitment",
                entity_id=entity,
                tenant_id=tenant,
                region_entity_ids=region,
                significance=0.2,
            )
            await record_subthreshold_signal(cand, region_hash, 0.2, conn)
        promoted = await promote_if_accumulated(tenant, region_hash, conn)
    assert promoted is not None
    assert promoted.kind == "commitment_drift"
    assert promoted.significance >= 0.2

    async with fresh_db.acquire() as conn:
        unpromoted = await conn.fetchval(
            """
            SELECT COUNT(*) FROM signal_memory_fabric
            WHERE tenant_id = $1 AND region_hash = $2 AND promoted_at IS NULL
            """,
            tenant, region_hash,
        )
    assert unpromoted == 0


# =====================================================================
# 19. False positives — 100 normal Slack messages
# =====================================================================


async def test_false_positive_resistance_normal_traffic(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        now = datetime.now(timezone.utc)
        for i in range(100):
            # Normal 'signal' observations, diverse entities, reputable tier.
            await insert_observation(
                conn, tenant,
                actor_id=actor,
                source_channel="slack:general",
                content_text=f"message {i}",
                trust_tier="inferential",
                occurred_at=now - timedelta(minutes=i),
                external_id=f"msg-{i}",
                entities_mentioned=[{"type": "topic", "id": f"t-{i}"}],
            )
        processor = AnomalyProcessor(
            fresh_db,
            config=AnomalyProcessorConfig(
                promote_every_n_cycles=0,
                t3_budget_per_tenant_per_min=100,
            ),
        )
        counters = await processor.process_once([tenant])
    assert counters["enqueued_t3"] == 0


# =====================================================================
# 20. Tenant isolation
# =====================================================================


async def test_tenant_isolation(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    other_tenant: UUID,
    two_tenant_cleanup,
):
    """An anomaly in tenant A never enqueues for tenant B."""
    async with fresh_db.acquire() as conn:
        # Over-committed resource in tenant A.
        await insert_resource(
            conn, tenant, kind="capacity",
            current_value={"total_units": 10, "deployed_units": 10, "available_units": 0},
        )
        # Normal resource in tenant B.
        await insert_resource(
            conn, other_tenant, kind="capacity",
            current_value={"total_units": 10, "deployed_units": 1, "available_units": 9},
        )
        processor = AnomalyProcessor(
            fresh_db,
            config=AnomalyProcessorConfig(
                promote_every_n_cycles=0,
                t3_budget_per_tenant_per_min=100,
            ),
        )
        counters = await processor.process_once([tenant, other_tenant])

        a_rows = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant,
        )
        b_rows = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            other_tenant,
        )
    assert a_rows >= 1
    assert b_rows == 0
    assert counters["enqueued_t3"] == a_rows


# =====================================================================
# 21. End-to-end: T3 payload picked up cleanly by _populate_seed_fields
# =====================================================================


async def test_t3_payload_populates_trigger_context(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    """Verify our T3 enqueue payload is consumed correctly by the
    Wave 3 worker's `_populate_seed_fields` (bug #1 patch)."""
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext

    async with fresh_db.acquire() as conn:
        r = await insert_resource(
            conn, tenant, kind="capacity",
            current_value={"total_units": 10, "deployed_units": 10, "available_units": 0},
        )
        processor = AnomalyProcessor(
            fresh_db,
            config=AnomalyProcessorConfig(
                promote_every_n_cycles=0,
                t3_budget_per_tenant_per_min=100,
            ),
        )
        await processor.process_once([tenant])

        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, trigger_kind, payload
            FROM think_trigger_queue
            WHERE tenant_id = $1
            ORDER BY enqueued_at DESC
            LIMIT 1
            """,
            tenant,
        )
    assert row is not None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert "region_spec" in payload
    assert "entity_ids" in payload["region_spec"]
    assert payload["seed_entity_ids"] == payload["region_spec"]["entity_ids"]
    # Confirm at least the resource UUID is in the entity_ids.
    entity_ids = payload["region_spec"]["entity_ids"]
    assert any(e.get("entity_id") == str(r) for e in entity_ids)

    # Now rehydrate via the bug #1 patch.
    trig = TriggerContext(kind="T3", tenant_id=tenant)
    _populate_seed_fields(trig, payload)
    # seed_entity_ids must be the list we enqueued.
    assert trig.seed_entity_ids == entity_ids
    # region_spec is the dict the worker consumes.
    assert trig.region_spec == payload["region_spec"]


# =====================================================================
# 22. Rate limit — 30 detections → 20 enqueues + 10 fabric
# =====================================================================


async def test_rate_limit_per_tenant(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    """30 distinct anomalies within a minute for one tenant → 20 land in
    think_trigger_queue and 10 overflow into signal_memory_fabric."""
    async with fresh_db.acquire() as conn:
        # 30 over-committed capacity resources (30 distinct regions → 30 candidates).
        resources = []
        for i in range(30):
            rid = await insert_resource(
                conn, tenant, kind="capacity",
                identity=f"r-{i}",
                current_value={"total_units": 10, "deployed_units": 10, "available_units": 0},
            )
            resources.append(rid)

        processor = AnomalyProcessor(
            fresh_db,
            config=AnomalyProcessorConfig(
                promote_every_n_cycles=0,
                t3_budget_per_tenant_per_min=20,  # cap = 20
            ),
        )
        counters = await processor.process_once([tenant])

        trig_cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant,
        )
        fabric_cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM signal_memory_fabric WHERE tenant_id = $1",
            tenant,
        )

    assert counters["detected"] >= 30
    assert trig_cnt == 20
    assert fabric_cnt == 10
    assert counters["rate_limited"] == 10
