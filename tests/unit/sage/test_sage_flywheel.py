"""tests/unit/sage/test_sage_flywheel.py — end-to-end SAGE feedback loop.

Closes the reader-writer feedback loop from the spec:

  Phase-1 emission  ->  Outcome Evaluator  ->  Topology Optimizer
       (events)              (events)              (discovery updates)

The Wave 1/2 integration tests cover each stage in isolation. This file
asserts the *composition* works: events produced by the evaluator are
shape-compatible with what the optimizer consumes, the discovery utility
layer gets updated as expected, and the canonical truth layer (models,
model_edges, observations) is untouched.

Reads as a single tight scenario rather than many parametrised cases:
a synthetic Think run with two diff'd model_ids and one omitted-evidence
candidate, then both Phase 13 stages, then assertions on the flywheel
state.

Per-test fresh DB via the gateway_pool fixture; integration-marked.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.sage.affordances.repo import AffordanceProfilesRepo
from services.sage.affordances.types import RetrievalAffordanceProfile
from services.sage.inquiry_traces import OutcomeEventsRepo
from services.sage.outcome_evaluator import OutcomeEvaluator
from services.sage.topology_optimizer import TopologyOptimizer


from services.gateway.tests.conftest import (  # noqa: F401
    gateway_pool,
    tenant_id,
)


pytestmark = pytest.mark.integration


_ZERO_EMBEDDING = "[" + ",".join(["0"] * 768) + "]"


async def _seed_model(pool: asyncpg.Pool, *, tenant_id: UUID) -> UUID:
    model_id = uuid7()
    born = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id,
              proposition, "natural", embedding,
              scope_temporal, confidence, activation
            ) VALUES (
              $1, $2, $3,
              $4::jsonb, $5, $6::vector,
              $7::jsonb, 0.5, 1.0
            )
            """,
            model_id, tenant_id, born,
            json.dumps({"kind": "belief", "subject": "flywheel"}),
            "flywheel test model", _ZERO_EMBEDDING,
            json.dumps({}),
        )
    return model_id


async def _seed_inquiry_session(
    pool: asyncpg.Pool, *, tenant_id: UUID, think_run_id: UUID, packet: dict,
) -> UUID:
    sid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO inquiry_sessions (
              id, tenant_id, signal_ref_type, signal_ref_id,
              route, status, stop_status,
              context_packet, think_run_id
            ) VALUES (
              $1, $2, 'internal', NULL,
              'DEEP_INQUIRY_PATH', 'completed', 'sufficient_for_reasoning',
              $3::jsonb, $4
            )
            """,
            sid, tenant_id, json.dumps(packet, default=str), think_run_id,
        )
    return sid


async def _seed_evidence(
    pool: asyncpg.Pool, *,
    tenant_id: UUID, session_id: UUID, source_ref: str,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO inquiry_evidence_items (
              id, session_id, tenant_id, source_type, source_ref,
              summary, token_estimate
            ) VALUES ($1, $2, $3, 'observation', $4, 'seed', 1)
            """,
            uuid7(), session_id, tenant_id, source_ref,
        )


async def _seed_think_run(
    pool: asyncpg.Pool, *,
    tenant_id: UUID, model_ids: list[UUID],
) -> UUID:
    run_id = uuid7()
    trig = uuid7()
    ops = {
        "claim_ops": [{"op": "update", "model_id": str(m)} for m in model_ids],
        "edge_ops": [],
        "act_ops": [],
        "resource_ops": [],
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO applied_triggers (
              trigger_id, tenant_id, diff_hash, trigger_kind, outcome
            ) VALUES ($1, $2, 'flywheel-h', 'T1', 'success')
            """,
            trig, tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO think_runs (
              id, tenant_id, trigger_id, trigger_kind,
              status, ops_applied
            ) VALUES ($1, $2, $3, 'T1', 'success', $4::jsonb)
            """,
            run_id, tenant_id, trig, json.dumps(ops, default=str),
        )
    return run_id


async def _snapshot_canonical_counts(
    pool: asyncpg.Pool, *, tenant_id: UUID,
) -> dict[str, int]:
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant', $1, true)",
            str(tenant_id),
        )
        models = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1", tenant_id,
        )
        edges = await conn.fetchval(
            "SELECT count(*) FROM model_edges WHERE tenant_id = $1", tenant_id,
        )
        obs = await conn.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant_id,
        )
    return {"models": models, "model_edges": edges, "observations": obs}


@pytest.mark.asyncio
async def test_sage_flywheel_closes_reader_to_writer(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """End-to-end: a Think run's outputs are read by OutcomeEvaluator
    into typed events, then TopologyOptimizer consumes those events to
    reinforce the affordance profiles of the models that landed in the
    valid diff — all without touching canonical truth tables."""

    # --- Arrange: two diff'd models (will get reinforced), one omitted
    #     observation. Seed affordance profiles so reinforce can land.
    model_used_a = await _seed_model(gateway_pool, tenant_id=tenant_id)
    model_used_b = await _seed_model(gateway_pool, tenant_id=tenant_id)

    aff_repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    seed_a = await aff_repo.upsert(RetrievalAffordanceProfile(
        model_id=model_used_a, tenant_id=tenant_id,
        answers_question_primitives=["DEPENDENCY"], utility_score=0.40,
    ))
    seed_b = await aff_repo.upsert(RetrievalAffordanceProfile(
        model_id=model_used_b, tenant_id=tenant_id,
        answers_question_primitives=["CONSTRAINT"], utility_score=0.55,
    ))

    think_run_id = await _seed_think_run(
        gateway_pool, tenant_id=tenant_id,
        model_ids=[model_used_a, model_used_b],
    )

    # Packet references one observation; we also seed a second evidence
    # item NOT in the packet so the evaluator emits an omission event.
    packet = {
        "tiers": {"supporting_evidence_groups": [
            {"source_ref": "obs:used-in-packet"},
        ]},
        "budget": {"estimated_tokens_used": 1200},
    }
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
        think_run_id=think_run_id, packet=packet,
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id,
        session_id=session_id, source_ref="obs:used-in-packet",
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id,
        session_id=session_id, source_ref="obs:omitted-from-packet",
    )

    canonical_before = await _snapshot_canonical_counts(
        gateway_pool, tenant_id=tenant_id,
    )

    # --- Act 1: evaluator analyses the session, emits typed events.
    evaluator = OutcomeEvaluator(pool=gateway_pool, tenant_id=tenant_id)
    summary = await evaluator.evaluate(inquiry_session_id=session_id)

    # Sanity: the evaluator did produce events of the types the
    # optimizer cares about.
    assert summary.events_emitted >= 3, summary.events_by_type
    assert summary.events_by_type.get("node_used_in_valid_diff", 0) == 2
    assert summary.events_by_type.get("retrieved_evidence_used_in_packet", 0) >= 1
    assert summary.events_by_type.get("retrieved_evidence_omitted", 0) >= 1

    # --- Act 2: optimizer consumes those events, updates discovery
    #     utility layer. Canonical layer must not move.
    optimizer = TopologyOptimizer(pool=gateway_pool, tenant_id=tenant_id)
    report = await optimizer.optimize(
        inquiry_session_id=session_id,
        trigger_event="validated_synthesis_diff_applied",
    )

    # --- Assert: flywheel closure.
    # 1. Both used models had their affordance utility reinforced.
    assert report.affordance_reinforces == 2

    updated_a = await aff_repo.get(model_used_a)
    updated_b = await aff_repo.get(model_used_b)
    assert updated_a is not None and updated_b is not None
    assert updated_a.utility_score > seed_a.utility_score
    assert updated_b.utility_score > seed_b.utility_score
    assert updated_a.last_reinforced_at is not None
    assert updated_b.last_reinforced_at is not None

    # 2. Optimizer reported the metrics surface expected by spec §16.
    assert "useful_nodes" in report.metrics
    assert report.metrics["useful_nodes"] >= 2.0

    # 3. Canonical truth tables untouched — discovery layer is isolated
    #    from the truth layer per spec §22.1.
    canonical_after = await _snapshot_canonical_counts(
        gateway_pool, tenant_id=tenant_id,
    )
    assert canonical_after == canonical_before

    # 4. Idempotency: a second evaluator pass on the same session emits
    #    zero new events (deduped by (event_type, payload-key)).
    rerun = await evaluator.evaluate(inquiry_session_id=session_id)
    assert rerun.events_emitted == 0

    # 5. The events table contents survived both passes — the optimizer
    #    is read-only on the events ledger.
    events_repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    final_events = await events_repo.list_for_session(session_id)
    assert len(final_events) == summary.events_emitted
