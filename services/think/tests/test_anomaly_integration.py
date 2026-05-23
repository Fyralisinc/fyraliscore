"""services/think/tests/test_anomaly_integration.py — anomaly detectors.

Covers spec §7 anomaly checking + Wave 3-B Outstanding #6:

Four detectors:
  * confidence_drop       (drop > 0.25)
  * critical_path_blocked (critical-path Commitment → Blocked/Paused)
  * resource_over_deployed (utilization > 0.95)
  * customer_health_degraded (customer Resource update with degraded /
    critical / warning `health`)

Also covers publish_anomalies writing to think_anomalies_raw.
"""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.think.anomaly_integration import (
    Anomaly, check_anomalies, publish_anomalies,
)
from services.think.diff_schema import (
    ActOp, ClaimOp, ResourceOp, ValidatedDiff,
)
from services.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_model_with_conf_at_assertion(
    conn, tenant_id: UUID, *, confidence_at_assertion: float, confidence: float,
) -> UUID:
    aid = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, status) "
        "VALUES ($1, $2, 'human_internal', 'x', 'active')",
        aid, tenant_id,
    )
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', $3,
                '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
        """,
        oid, tenant_id, aid, make_embedding("x"),
    )
    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                $9::jsonb, $10, 0.5, 'active', $11, 1.0)
        """,
        mid, tenant_id, oid,
        json.dumps({"kind": "state", "text": "claim"}),
        "claim", make_embedding("x"), [], "[]", "{}",
        float(confidence), float(confidence_at_assertion),
    )
    return mid


# =====================================================================
# confidence_drop detector
# =====================================================================


async def test_confidence_drop_above_threshold_flags(
    fresh_db, tenant, tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        mid = await _insert_model_with_conf_at_assertion(
            conn, tenant, confidence_at_assertion=0.90, confidence=0.90,
        )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        claim_ops=[
            ClaimOp(op="update", model_id=mid, changes={"confidence": 0.30}),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert any(a.kind == "confidence_drop" for a in found)
    anom = next(a for a in found if a.kind == "confidence_drop")
    assert anom.region["model_id"] == str(mid)
    # Significance grows with delta.
    assert anom.significance > 0.5


async def test_confidence_drop_below_threshold_not_flagged(
    fresh_db, tenant, tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        mid = await _insert_model_with_conf_at_assertion(
            conn, tenant, confidence_at_assertion=0.80, confidence=0.80,
        )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        claim_ops=[
            # Drop of 0.20 — below 0.25 threshold.
            ClaimOp(op="update", model_id=mid, changes={"confidence": 0.60}),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert not any(a.kind == "confidence_drop" for a in found)


async def test_confidence_drop_missing_model_skipped(
    fresh_db, tenant, tenant_cleanup,
):
    """Update op on a model that doesn't exist should not crash the
    detector (the applier would reject it earlier, but we guard)."""
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        claim_ops=[
            ClaimOp(op="update", model_id=uuid7(), changes={"confidence": 0.1}),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert not any(a.kind == "confidence_drop" for a in found)


# =====================================================================
# critical_path_blocked detector
# =====================================================================


async def test_critical_path_commitment_blocked_flagged(
    fresh_db, tenant, tenant_cleanup,
):
    from services.acts import commitments as commitments_svc
    from services.acts import goals as goals_svc
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'A', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship", owner_id=aid,
                contributes_to_goal_ids=[(g.id, True)],  # critical path
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )

    # The diff proposes a transition_commitment to blocked; the detector
    # looks up the contributes_to edge and finds is_critical_path=TRUE.
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        act_ops=[
            ActOp(
                op="transition_commitment",
                entity={"id": str(c.id), "new_state": "blocked"},
            ),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert any(a.kind == "critical_path_blocked" for a in found)
    anom = next(a for a in found if a.kind == "critical_path_blocked")
    assert anom.significance >= 0.75


async def test_non_critical_path_commitment_blocked_not_flagged(
    fresh_db, tenant, tenant_cleanup,
):
    from services.acts import commitments as commitments_svc
    from services.acts import goals as goals_svc
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'A', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship", owner_id=aid,
                contributes_to_goal_ids=[(g.id, False)],  # NOT critical
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )

    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        act_ops=[
            ActOp(
                op="transition_commitment",
                entity={"id": str(c.id), "new_state": "blocked"},
            ),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert not any(a.kind == "critical_path_blocked" for a in found)


async def test_critical_path_commitment_paused_also_flagged(
    fresh_db, tenant, tenant_cleanup,
):
    """`paused` is also a critical-path-affecting state."""
    from services.acts import commitments as commitments_svc
    from services.acts import goals as goals_svc
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'A', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship", owner_id=aid,
                contributes_to_goal_ids=[(g.id, True)],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        act_ops=[
            ActOp(
                op="transition_commitment",
                entity={"id": str(c.id), "new_state": "paused"},
            ),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert any(a.kind == "critical_path_blocked" for a in found)


# =====================================================================
# resource_over_deployed detector
# =====================================================================


async def _insert_capacity_resource(
    conn, tenant_id: UUID,
    *, total: float, deployed: float, kind: str = "capacity",
) -> UUID:
    aid = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, status) "
        "VALUES ($1, $2, 'human_internal', 'x', 'active')",
        aid, tenant_id,
    )
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', $3,
                '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
        """,
        oid, tenant_id, aid, make_embedding("x"),
    )
    rid = uuid7()
    await conn.execute(
        """
        INSERT INTO resources
          (id, tenant_id, kind, identity, description, current_value,
           utilization_state, controllability, temporal_character,
           valuation_confidence, metadata, last_updated_by_event_id)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb,
                'available', 'owned', 'permanent', 1.0,
                '{}'::jsonb, $7)
        """,
        rid, tenant_id, kind,
        "eng_capacity",
        "engineering capacity",
        json.dumps({
            "total_units": total,
            "deployed_units": deployed,
        }),
        oid,
    )
    return rid


async def test_resource_over_deployed_flagged(fresh_db, tenant, tenant_cleanup):
    async with fresh_db.acquire() as conn:
        rid = await _insert_capacity_resource(
            conn, tenant, total=100.0, deployed=98.0,
        )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        resource_ops=[
            ResourceOp(
                op="deploy", resource_id=rid,
                commitment_id=uuid7(),
                quantity={"units": 2},
            ),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert any(a.kind == "resource_over_deployed" for a in found)
    anom = next(a for a in found if a.kind == "resource_over_deployed")
    assert anom.triggering_op["utilization"] > 0.95


async def test_resource_not_over_deployed_not_flagged(
    fresh_db, tenant, tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        rid = await _insert_capacity_resource(
            conn, tenant, total=100.0, deployed=50.0,
        )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        resource_ops=[
            ResourceOp(
                op="deploy", resource_id=rid,
                commitment_id=uuid7(),
                quantity={"units": 10},
            ),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert not any(a.kind == "resource_over_deployed" for a in found)


# =====================================================================
# customer_health_degraded detector
# =====================================================================


async def test_customer_health_degraded_flagged(
    fresh_db, tenant, tenant_cleanup,
):
    """Any ResourceOp update with patch.health in (warning, degraded, critical)."""
    rid = uuid7()
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        resource_ops=[
            ResourceOp(op="update", resource_id=rid, patch={"health": "degraded"}),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert any(a.kind == "customer_health_degraded" for a in found)
    anom = next(a for a in found if a.kind == "customer_health_degraded")
    assert anom.significance >= 0.6


async def test_customer_health_critical_has_highest_significance(
    fresh_db, tenant, tenant_cleanup,
):
    diffs = []
    for h in ("warning", "degraded", "critical"):
        diffs.append(ValidatedDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            resource_ops=[
                ResourceOp(op="update", resource_id=uuid7(), patch={"health": h}),
            ],
        ))
    severities = {}
    async with fresh_db.acquire() as conn:
        for d in diffs:
            found = await check_anomalies(d, conn)
            anom = next(a for a in found if a.kind == "customer_health_degraded")
            severities[d.resource_ops[0].patch["health"]] = anom.significance
    assert severities["critical"] > severities["degraded"] > severities["warning"]


async def test_customer_health_healthy_not_flagged(
    fresh_db, tenant, tenant_cleanup,
):
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        resource_ops=[
            ResourceOp(op="update", resource_id=uuid7(), patch={"health": "healthy"}),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    assert not any(a.kind == "customer_health_degraded" for a in found)


# =====================================================================
# publish_anomalies — durable write
# =====================================================================


async def test_publish_anomalies_writes_to_table(fresh_db, tenant, tenant_cleanup):
    run_id = uuid7()
    anomalies = [
        Anomaly(
            kind="confidence_drop",
            region={"model_id": str(uuid7())},
            significance=0.8,
            triggering_op={"op": "update", "prior": 0.9, "new": 0.5},
        ),
        Anomaly(
            kind="critical_path_blocked",
            region={"commitment_id": str(uuid7())},
            significance=0.75,
        ),
    ]
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            n = await publish_anomalies(anomalies, run_id, tenant, conn)
        assert n == 2
        rows = await conn.fetch(
            "SELECT * FROM think_anomalies_raw WHERE think_run_id = $1",
            run_id,
        )
    assert len(rows) == 2
    kinds = {r["kind"] for r in rows}
    assert kinds == {"confidence_drop", "critical_path_blocked"}
    for r in rows:
        assert r["consumed_at"] is None
        assert r["published_at"] is not None
        assert r["significance"] > 0.0


async def test_publish_anomalies_empty_noop(fresh_db, tenant, tenant_cleanup):
    run_id = uuid7()
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            n = await publish_anomalies([], run_id, tenant, conn)
    assert n == 0


async def test_anomaly_detectors_ignore_non_capacity_resource(
    fresh_db, tenant, tenant_cleanup,
):
    """Only resources of kind='capacity' are checked for over-deployment."""
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        rid = uuid7()
        await conn.execute(
            """
            INSERT INTO resources
              (id, tenant_id, kind, identity, description, current_value,
               utilization_state, controllability, temporal_character,
               valuation_confidence, metadata, last_updated_by_event_id)
            VALUES ($1, $2, 'financial', $3, $4, $5::jsonb,
                    'available', 'owned', 'permanent', 1.0,
                    '{}'::jsonb, $6)
            """,
            rid, tenant,
            "treasury",
            "cash",
            json.dumps({"total_units": 100, "deployed_units": 99}),
            oid,
        )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=tenant,
        resource_ops=[
            ResourceOp(
                op="deploy", resource_id=rid,
                commitment_id=uuid7(),
                quantity={"units": 1},
            ),
        ],
    )
    async with fresh_db.acquire() as conn:
        found = await check_anomalies(diff, conn)
    # Financial, not capacity — detector should skip.
    assert not any(a.kind == "resource_over_deployed" for a in found)
