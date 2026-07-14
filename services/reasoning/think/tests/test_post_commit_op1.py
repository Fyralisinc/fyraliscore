"""services/reasoning/think/tests/test_post_commit_op1.py — OP-1 tests.

THINK-DESIGN-AUDIT §8.1, §10 arg 1. Verifies:
  * enqueue_post_commit_actions writes expected rows inside a tx
  * dedup collapses duplicate enqueues on the same (tenant, trigger, kind)
  * post_commit_worker processes pending rows
  * a failing handler increments attempts + reschedules with backoff
  * after MAX_ATTEMPTS the row is moved to dead-letter
  * mid-dispatch crash (simulated via a handler that raises) leaves the
    row pending for retry — it is NOT marked processed
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.models.repo import ModelsRepo
from services.reasoning.edge_intelligence import (
    EdgeIntelligenceRepo,
    PairEvidenceObservation,
)
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.diff_schema import ClaimOp, OpenQuestionOp, ValidatedDiff
from services.reasoning.think.post_commit import (
    BACKOFF_BASE_SECONDS,
    MAX_ATTEMPTS,
    VIEW_CEO_REFRESH_CHANNEL,
    enqueue_post_commit_actions,
    fetch_pending_actions,
    process_batch,
    register_handler,
    reset_handlers,
    _compute_backoff,
    _projection_names_for_apply_summary,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _make_diff(
    *,
    tenant_id: UUID,
    trigger_ref: UUID,
    with_predictions: bool = True,
    with_entities: bool = True,
) -> ValidatedDiff:
    """Build a ValidatedDiff that produces enqueues for all four kinds
    (anomalies is passed separately). Predictions + entities are
    included by default so the non-empty gates don't filter us out."""
    predictions: list[ClaimOp] = []
    if with_predictions:
        predictions.append(
            ClaimOp(
                op="insert",
                entry={
                    "confidence": 0.5,
                    "evaluate_at": datetime.now(timezone.utc).isoformat(),
                    "scope_actors": [],
                    "scope_entities": [],
                    "falsifier": "deadline passes without completion",
                    "proposition": {"kind": "prediction"},
                },
            )
        )
    claim_ops: list[ClaimOp] = []
    if with_entities:
        claim_ops.append(
            ClaimOp(
                op="insert",
                entry={
                    "confidence": 0.5,
                    "scope_entities": [
                        {"type": "commitment", "id": str(uuid.uuid4())},
                    ],
                },
            )
        )
    return ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant_id,
        claim_ops=claim_ops,
        act_ops=[],
        resource_ops=[],
        new_predictions=predictions,
    )


@pytest_asyncio.fixture
async def clean_queue(db_pool: asyncpg.Pool, tenant):
    """Ensure the queue starts empty for this tenant and clean up after."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM pending_post_commit_actions WHERE tenant_id = $1",
            tenant,
        )
    yield
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM pending_post_commit_actions WHERE tenant_id = $1",
            tenant,
        )


@pytest.fixture(autouse=True)
def _reset_handlers_each_test():
    reset_handlers()
    yield
    reset_handlers()


# ---------------------------------------------------------------------
# _compute_backoff — pure unit test
# ---------------------------------------------------------------------


def test_compute_backoff_exponential():
    assert _compute_backoff(0) == 0
    assert _compute_backoff(1) == BACKOFF_BASE_SECONDS
    assert _compute_backoff(2) == BACKOFF_BASE_SECONDS * 2
    assert _compute_backoff(3) == BACKOFF_BASE_SECONDS * 4
    assert _compute_backoff(5) == BACKOFF_BASE_SECONDS * 16
    # Cap at 300s regardless of exponent blowing up.
    assert _compute_backoff(20) == 300


def test_projection_names_include_decision_surfaces_for_decision_pressure_summary():
    names = _projection_names_for_apply_summary(
        {
            "claim_ops": [
                {
                    "op": "insert",
                    "model_id": str(uuid.uuid4()),
                    "claim_role": "recommendation",
                    "domain_tags": ["customers", "execution"],
                }
            ]
        }
    )

    assert names == ["constraints", "customers", "decision_surfaces"]


def test_projection_names_include_decision_surfaces_for_decision_act_summary():
    names = _projection_names_for_apply_summary(
        {
            "act_ops": [
                {
                    "op": "create_decision",
                    "decision_id": str(uuid.uuid4()),
                }
            ]
        }
    )

    assert names == ["constraints", "decision_surfaces", "decisions"]


def test_projection_names_include_entity_first_families_for_scoped_summary():
    commitment_id = uuid.uuid4()
    customer_id = uuid.uuid4()
    goal_id = uuid.uuid4()

    names = _projection_names_for_apply_summary(
        {
            "claim_ops": [
                {
                    "op": "insert",
                    "model_id": str(uuid.uuid4()),
                    "claim_role": "situation",
                    "domain_tags": ["commitment", "customer", "goal"],
                    "scope_entities": [
                        {"type": "commitment", "id": str(commitment_id)},
                        {"type": "customer_resource", "id": str(customer_id)},
                        {"type": "goal", "id": str(goal_id)},
                    ],
                }
            ]
        }
    )

    assert names == [
        "commitments",
        "constraints",
        "customers",
        "goals",
    ]


def test_projection_names_include_employee_profiles_only_for_people_signal():
    actor_id = uuid.uuid4()

    names = _projection_names_for_apply_summary(
        {
            "claim_ops": [
                {
                    "op": "insert",
                    "model_id": str(uuid.uuid4()),
                    "claim_role": "recommendation",
                    "domain_tags": ["workload"],
                    "scope_actors": [str(actor_id)],
                }
            ]
        }
    )

    assert names == ["constraints", "employee_profiles"]


# ---------------------------------------------------------------------
# Enqueue tests
# ---------------------------------------------------------------------


async def test_enqueue_creates_rows_per_action_kind(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """Enqueueing a diff with content in every action-kind creates four
    rows: publish_anomalies, schedule_predictions, broadcast_realtime,
    invalidate_metrics."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)
    anomalies = [
        {"kind": "confidence_drop", "region": {"model_id": "abc"}, "significance": 0.6}
    ]

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            inserted = await enqueue_post_commit_actions(
                trigger=None,  # unused
                validated_diff=diff,
                conn=conn,
                anomalies=anomalies,
            )

    assert len(inserted) == 4, f"expected 4 rows, got {len(inserted)}"

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT action_kind FROM pending_post_commit_actions
            WHERE trigger_id = $1
            ORDER BY action_kind
            """,
            trigger_ref,
        )
    kinds = sorted(r["action_kind"] for r in rows)
    assert kinds == [
        "broadcast_realtime",
        "invalidate_metrics",
        "publish_anomalies",
        "schedule_predictions",
    ]


async def test_enqueue_skips_empty_payloads(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """A diff with no anomalies / predictions / entity mutations should
    only enqueue broadcast_realtime (always-on heartbeat)."""
    trigger_ref = uuid.uuid4()
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant,
        claim_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
    )
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            inserted = await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[],
            )

    # Only broadcast_realtime is unconditional.
    assert len(inserted) == 1
    async with db_pool.acquire() as conn:
        kinds = [
            r["action_kind"]
            for r in await conn.fetch(
                "SELECT action_kind FROM pending_post_commit_actions "
                "WHERE trigger_id = $1",
                trigger_ref,
            )
        ]
    assert kinds == ["broadcast_realtime"]


async def test_enqueue_discovers_edges_for_applied_models(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    model_a = uuid.uuid4()
    model_b = uuid.uuid4()
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant,
        claim_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
    )

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            inserted = await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[],
                applied_model_ids=[model_a, model_b, model_a],
            )

    assert len(inserted) == 3
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT action_kind, action_payload
            FROM pending_post_commit_actions
            WHERE trigger_id = $1
            ORDER BY action_kind
            """,
            trigger_ref,
        )

    assert [r["action_kind"] for r in rows] == [
        "broadcast_realtime",
        "discover_model_edges",
        "materialize_projections",
    ]
    payload_text = await db_pool.fetchval(
        """
        SELECT action_payload->>'model_ids'
        FROM pending_post_commit_actions
        WHERE trigger_id = $1 AND action_kind = 'discover_model_edges'
        """,
        trigger_ref,
    )
    assert json.loads(payload_text) == [str(model_a), str(model_b)]
    projection_payload = await db_pool.fetchval(
        """
        SELECT action_payload
        FROM pending_post_commit_actions
        WHERE trigger_id = $1 AND action_kind = 'materialize_projections'
        """,
        trigger_ref,
    )
    projection_payload = json.loads(projection_payload)
    assert projection_payload["model_ids"] == [str(model_a), str(model_b)]
    assert projection_payload["projection_names"] == ["all"]
    assert projection_payload["limit"] == 24


async def test_enqueue_edge_discovery_uses_insert_summary_and_suppresses_downstream_think(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    inserted_model = uuid.uuid4()
    updated_model = uuid.uuid4()
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant,
        claim_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
    )

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=TriggerContext(
                    kind="T4",
                    tenant_id=tenant,
                    subkind="latent_relationship_candidate",
                ),
                validated_diff=diff,
                conn=conn,
                anomalies=[],
                applied_model_ids=[inserted_model, updated_model],
                applied_ops_summary={
                    "claim_ops": [
                        {"op": "insert", "model_id": str(inserted_model)},
                        {"op": "update", "model_id": str(updated_model)},
                    ]
                },
            )

    payload = await db_pool.fetchval(
        """
        SELECT action_payload
        FROM pending_post_commit_actions
        WHERE trigger_id = $1 AND action_kind = 'discover_model_edges'
        """,
        trigger_ref,
    )
    payload = json.loads(payload)
    assert payload["model_ids"] == [str(inserted_model)]
    assert payload["selector"] == "claim_insert_models"
    assert payload["source_trigger_kind"] == "T4"
    assert payload["source_trigger_subkind"] == "latent_relationship_candidate"
    assert payload["enqueue_think"] is False
    assert payload["think_enqueue_budget"] == 0

    projection_payload = await db_pool.fetchval(
        """
        SELECT action_payload
        FROM pending_post_commit_actions
        WHERE trigger_id = $1 AND action_kind = 'materialize_projections'
        """,
        trigger_ref,
    )
    projection_payload = json.loads(projection_payload)
    assert projection_payload["model_ids"] == [
        str(inserted_model),
        str(updated_model),
    ]
    assert projection_payload["selector"] == "all_applied_models"
    assert projection_payload["projection_names"] == ["constraints"]


async def test_enqueue_t1_edge_discovery_caps_immediate_topology_think_budget(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    models = [uuid.uuid4() for _ in range(4)]
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant,
        claim_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
    )

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=TriggerContext(kind="T1", tenant_id=tenant),
                validated_diff=diff,
                conn=conn,
                anomalies=[],
                applied_model_ids=models,
                applied_ops_summary={
                    "claim_ops": [
                        {"op": "insert", "model_id": str(model_id)}
                        for model_id in models
                    ]
                },
            )

    payload = await db_pool.fetchval(
        """
        SELECT action_payload
        FROM pending_post_commit_actions
        WHERE trigger_id = $1 AND action_kind = 'discover_model_edges'
        """,
        trigger_ref,
    )
    payload = json.loads(payload)
    assert payload["model_ids"] == [str(model_id) for model_id in models]
    assert payload["enqueue_think"] is True
    assert payload["think_enqueue_budget"] == 2


async def test_enqueue_searches_applied_open_questions(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    model_id = uuid.uuid4()
    question_id = uuid.uuid4()
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant,
        open_question_ops=[
            OpenQuestionOp(
                op="insert",
                model_id=model_id,
                question="Which signal resolves the cash runway boundary?",
                question_type="constraint_boundary",
            ),
        ],
    )

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[],
                applied_model_ids=[model_id],
                applied_open_question_ids=[question_id],
            )

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT action_payload
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND trigger_id = $2
              AND action_kind = 'search_open_questions'
            """,
            tenant,
            trigger_ref,
        )

    assert row is not None
    payload = row["action_payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["model_ids"] == [str(model_id)]
    assert payload["open_question_ids"] == [str(question_id)]
    assert payload["limit"] >= 20


async def test_materialize_projections_handler_consumes_model_events(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    born_from_event = uuid7()
    repo = ModelsRepo(db_pool, embedder=None, run_topology_on_insert=False)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel,
              source_actor_ref, content, content_text, embedding_pending,
              trust_tier, external_id, entities_mentioned
            ) VALUES (
              $1, $2, now(), 'signal', 'test', 'test:actor',
              '{}'::jsonb, 'cash runway pressure', TRUE,
              'authoritative', $3, '[]'::jsonb
            )
            """,
            born_from_event,
            tenant,
            f"post-commit-projection-{born_from_event}",
        )
        model = await repo.insert(
            ModelCreate(
                tenant_id=tenant,
                born_from_event_id=born_from_event,
                proposition={
                    "kind": "belief",
                    "claim_role": "concern",
                    "assertion": "Cash runway is the active planning constraint.",
                    "domain_tags": ["runway", "financial_capacity", "constraint"],
                },
                natural="Cash runway is the active planning constraint.",
                embedding=[0.0] * 768,
                scope_actors=[],
                scope_entities=[{"type": "company", "id": str(tenant)}],
                scope_temporal={"type": "current"},
                confidence=0.68,
                confidence_at_assertion=0.68,
                domain_tags=["runway", "financial_capacity", "constraint"],
            ),
            conn=conn,
        )
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions (
              tenant_id, trigger_id, action_kind, action_payload
            ) VALUES ($1, $2, 'materialize_projections', $3::jsonb)
            """,
            tenant,
            trigger_ref,
            json.dumps({"model_ids": [str(model.id)], "limit": 100}),
        )

    stats = await process_batch(db_pool, limit=5, tenant_id=tenant)

    async with db_pool.acquire() as conn:
        processed = await conn.fetchval(
            """
            SELECT processed_at
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND trigger_id = $2
              AND action_kind = 'materialize_projections'
            """,
            tenant,
            trigger_ref,
        )
        constraint_snapshot = await conn.fetchrow(
            """
            SELECT payload, source_model_ids
            FROM projection_snapshots
            WHERE tenant_id = $1
              AND projection_name = 'constraints'
              AND projection_version = 'v1'
              AND subject_key = 'company:runway'
            """,
            tenant,
        )
        resource_snapshot = await conn.fetchrow(
            """
            SELECT payload, source_model_ids
            FROM projection_snapshots
            WHERE tenant_id = $1
              AND projection_name = 'resources'
              AND projection_version = 'v1'
              AND subject_key = 'company:financial'
            """,
            tenant,
        )
        decision_surface_snapshot = await conn.fetchrow(
            """
            SELECT payload, source_model_ids
            FROM projection_snapshots
            WHERE tenant_id = $1
              AND projection_name = 'decision_surfaces'
              AND projection_version = 'v1'
              AND subject_key = $2
            """,
            tenant,
            f"company:{tenant}:decision_surface",
        )

    assert stats.processed == 1
    assert stats.failed == 0
    assert processed is not None
    assert constraint_snapshot is not None
    assert resource_snapshot is not None
    assert decision_surface_snapshot is not None
    assert str(model.id) in {
        str(mid) for mid in constraint_snapshot["source_model_ids"]
    }
    assert str(model.id) in {str(mid) for mid in resource_snapshot["source_model_ids"]}
    assert str(model.id) in {
        str(mid) for mid in decision_surface_snapshot["source_model_ids"]
    }


async def test_discover_model_edges_promotes_scoped_pair_evidence(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    repo = EdgeIntelligenceRepo()
    trigger_ref = uuid.uuid4()
    changed_source_id = uuid.uuid4()
    changed_target_id = uuid.uuid4()
    unrelated_source_id = uuid.uuid4()
    unrelated_target_id = uuid.uuid4()

    async with db_pool.acquire() as conn:
        for source_model_id, target_model_id in (
            (changed_source_id, changed_target_id),
            (unrelated_source_id, unrelated_target_id),
        ):
            await repo.record_pair_observation(
                conn,
                PairEvidenceObservation(
                    tenant_id=tenant,
                    left_model_id=source_model_id,
                    right_model_id=target_model_id,
                    primitive="DEPENDENCY",
                    co_used_valid_diff_delta=1,
                    explicit_relation_delta=1,
                    think_edge_op_delta=1,
                    directed_source_model_id=source_model_id,
                    directed_target_model_id=target_model_id,
                    edge_kind_hint="blocks",
                ),
            )
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions (
              tenant_id, trigger_id, action_kind, action_payload
            )
            VALUES ($1, $2, 'discover_model_edges', $3::jsonb)
            """,
            tenant,
            trigger_ref,
            json.dumps({"model_ids": [str(changed_source_id)]}),
        )

    stats = await process_batch(db_pool, limit=5, tenant_id=tenant)

    async with db_pool.acquire() as conn:
        processed = await conn.fetchval(
            """
            SELECT processed_at
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND trigger_id = $2
              AND action_kind = 'discover_model_edges'
            """,
            tenant,
            trigger_ref,
        )
        candidates = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind, source
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND source = 'edge_intelligence_kernel'
            ORDER BY created_at ASC
            """,
            tenant,
        )

    assert stats.processed == 1
    assert stats.failed == 0
    assert processed is not None
    assert len(candidates) == 1
    assert candidates[0]["source_model_id"] == changed_source_id
    assert candidates[0]["target_model_id"] == changed_target_id
    assert candidates[0]["edge_kind"] == "blocks"


async def test_enqueue_dedup_collapses_duplicates(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """Calling enqueue_post_commit_actions twice with the same trigger
    produces one set of rows, not two."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            first = await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[
                    {"kind": "confidence_drop", "region": {}, "significance": 0.5}
                ],
            )
        async with conn.transaction():
            second = await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[
                    {"kind": "confidence_drop", "region": {}, "significance": 0.5}
                ],
            )

    # Second call returns empty (all dedupped by NULLS NOT DISTINCT unique).
    assert len(first) == 4
    assert len(second) == 0

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(
            "SELECT count(*) FROM pending_post_commit_actions " "WHERE trigger_id = $1",
            trigger_ref,
        )
    assert total == 4


async def test_enqueue_dedup_suppresses_processed_historical_rows(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """A processed action row still owns its trigger/action idempotency key."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            first = await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[
                    {"kind": "confidence_drop", "region": {}, "significance": 0.5}
                ],
            )
        await conn.execute(
            """
            UPDATE pending_post_commit_actions
            SET processed_at = now()
            WHERE trigger_id = $1
            """,
            trigger_ref,
        )
        async with conn.transaction():
            second = await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[
                    {"kind": "confidence_drop", "region": {}, "significance": 0.5}
                ],
            )

    assert len(first) == 4
    assert len(second) == 0

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(
            """
            SELECT count(*)
            FROM pending_post_commit_actions
            WHERE trigger_id = $1
            """,
            trigger_ref,
        )
    assert total == 4


async def test_enqueue_rolls_back_with_outer_tx(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """If the outer transaction rolls back, the enqueued rows are rolled
    back with it — this is the entire point of enqueuing inside the
    apply tx."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)

    class _IntentionalRollback(Exception):
        pass

    with pytest.raises(_IntentionalRollback):
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await enqueue_post_commit_actions(
                    trigger=None,
                    validated_diff=diff,
                    conn=conn,
                    anomalies=[{"kind": "x", "region": {}, "significance": 0.5}],
                )
                raise _IntentionalRollback("simulated apply failure")

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(
            "SELECT count(*) FROM pending_post_commit_actions " "WHERE trigger_id = $1",
            trigger_ref,
        )
    assert total == 0, "enqueued rows should roll back with the outer tx"


# ---------------------------------------------------------------------
# Worker dispatch tests
# ---------------------------------------------------------------------


async def test_worker_processes_pending_rows(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """Worker picks up a pending row, dispatches to the registered
    handler, and marks processed_at."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)

    dispatched: list[str] = []

    async def _capturing(payload, tid, trid):
        dispatched.append(f"{tid}:{trid}")

    register_handler("broadcast_realtime", _capturing)
    register_handler("publish_anomalies", _capturing)
    register_handler("schedule_predictions", _capturing)
    register_handler("invalidate_metrics", _capturing)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[
                    {"kind": "confidence_drop", "region": {}, "significance": 0.5}
                ],
            )

    stats = await process_batch(db_pool, limit=50, tenant_id=tenant)
    assert stats.processed == 4
    assert stats.failed == 0
    assert stats.dead_lettered == 0
    assert len(dispatched) == 4

    async with db_pool.acquire() as conn:
        pending = await conn.fetchval(
            """
            SELECT count(*) FROM pending_post_commit_actions
            WHERE trigger_id = $1 AND processed_at IS NULL
            """,
            trigger_ref,
        )
    assert pending == 0


async def test_default_broadcast_realtime_notifies_ceo_refresh(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    diff = _make_diff(
        tenant_id=tenant,
        trigger_ref=trigger_ref,
        with_predictions=False,
        with_entities=False,
    )
    received: asyncio.Queue[str] = asyncio.Queue()

    def _listener(
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        del connection, pid, channel
        received.put_nowait(payload)

    async with db_pool.acquire() as listen_conn:
        await listen_conn.add_listener(VIEW_CEO_REFRESH_CHANNEL, _listener)
        try:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    await enqueue_post_commit_actions(
                        trigger=None,
                        validated_diff=diff,
                        conn=conn,
                        anomalies=[],
                    )

            stats = await process_batch(db_pool, limit=10, tenant_id=tenant)
            assert stats.processed == 1

            raw = await asyncio.wait_for(received.get(), timeout=5.0)
        finally:
            await listen_conn.remove_listener(VIEW_CEO_REFRESH_CHANNEL, _listener)

    payload = json.loads(raw)
    assert payload["tenant_id"] == str(tenant)
    assert payload["trigger_id"] == str(trigger_ref)
    assert payload["reason"] == "substrate_changed"


async def test_default_schedule_predictions_notifies_ceo_refresh(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    received: asyncio.Queue[str] = asyncio.Queue()

    def _listener(
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        del connection, pid, channel
        received.put_nowait(payload)

    async with db_pool.acquire() as listen_conn:
        await listen_conn.add_listener(VIEW_CEO_REFRESH_CHANNEL, _listener)
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO pending_post_commit_actions (
                      tenant_id, trigger_id, action_kind, action_payload
                    )
                    VALUES ($1, $2, 'schedule_predictions', $3::jsonb)
                    """,
                    tenant,
                    trigger_ref,
                    json.dumps(
                        {
                            "predictions": [
                                {
                                    "evaluate_at": datetime.now(
                                        timezone.utc
                                    ).isoformat(),
                                }
                            ]
                        }
                    ),
                )

            stats = await process_batch(db_pool, limit=10, tenant_id=tenant)
            assert stats.processed == 1

            raw = await asyncio.wait_for(received.get(), timeout=5.0)
        finally:
            await listen_conn.remove_listener(VIEW_CEO_REFRESH_CHANNEL, _listener)

    payload = json.loads(raw)
    assert payload["tenant_id"] == str(tenant)
    assert payload["trigger_id"] == str(trigger_ref)
    assert payload["reason"] == "prediction_scheduled"


async def test_schedule_predictions_missing_evaluate_at_retries(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    trigger_ref = uuid.uuid4()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions (
              tenant_id, trigger_id, action_kind, action_payload
            )
            VALUES ($1, $2, 'schedule_predictions', $3::jsonb)
            """,
            tenant,
            trigger_ref,
            json.dumps({"predictions": [{"entry": {"claim_role": "prediction"}}]}),
        )

    stats = await process_batch(db_pool, limit=10, tenant_id=tenant)

    assert stats.processed == 0
    assert stats.failed == 1
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT attempts, processed_at, last_error
            FROM pending_post_commit_actions
            WHERE trigger_id = $1
            """,
            trigger_ref,
        )
    assert row["attempts"] == 1
    assert row["processed_at"] is None
    assert "missing evaluate_at" in (row["last_error"] or "")


async def test_concurrent_workers_process_each_action_once(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """Two post-commit replicas must split the queue without duplicate
    dispatch. The row-level guarantee is `FOR UPDATE SKIP LOCKED`; the
    barrier keeps both handlers inside their batch transaction long enough
    to exercise the lock handoff instead of accidentally serializing.
    """
    trigger_refs = [uuid.uuid4(), uuid.uuid4()]
    started = 0
    release = asyncio.Event()
    dispatched: list[UUID] = []
    dispatch_lock = asyncio.Lock()

    async def _blocking_handler(payload, tid, trid):
        nonlocal started
        async with dispatch_lock:
            started += 1
            if started == 2:
                release.set()
        await release.wait()
        dispatched.append(trid)

    register_handler("broadcast_realtime", _blocking_handler)

    async with db_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO pending_post_commit_actions (
              tenant_id, trigger_id, action_kind, action_payload
            )
            VALUES ($1, $2, 'broadcast_realtime', '{}'::jsonb)
            """,
            [(tenant, trigger_ref) for trigger_ref in trigger_refs],
        )

    stats_a, stats_b = await asyncio.wait_for(
        asyncio.gather(
            process_batch(db_pool, limit=1, tenant_id=tenant),
            process_batch(db_pool, limit=1, tenant_id=tenant),
        ),
        timeout=5.0,
    )

    assert stats_a.processed + stats_b.processed == 2
    assert stats_a.failed == stats_b.failed == 0
    assert sorted(dispatched) == sorted(trigger_refs)
    assert len(dispatched) == len(set(dispatched))

    async with db_pool.acquire() as conn:
        processed_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM pending_post_commit_actions
            WHERE tenant_id = $1 AND processed_at IS NOT NULL
            """,
            tenant,
        )
    assert processed_count == 2


async def test_worker_retries_on_handler_failure(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """A handler that raises causes attempts to increment and the row
    to be rescheduled (scheduled_at > now)."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(
        tenant_id=tenant,
        trigger_ref=trigger_ref,
        with_predictions=False,
        with_entities=False,
    )

    call_count = {"n": 0}

    async def _failing(payload, tid, trid):
        call_count["n"] += 1
        raise RuntimeError("boom")

    register_handler("broadcast_realtime", _failing)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[],
            )

    stats = await process_batch(db_pool, limit=10, tenant_id=tenant)
    assert stats.processed == 0
    assert stats.failed == 1
    assert stats.dead_lettered == 0
    assert call_count["n"] == 1

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT attempts, processed_at, dead_lettered_at, last_error,
                   scheduled_at
            FROM pending_post_commit_actions WHERE trigger_id = $1
            """,
            trigger_ref,
        )
    assert row["attempts"] == 1
    assert row["processed_at"] is None
    assert row["dead_lettered_at"] is None
    assert "boom" in (row["last_error"] or "")
    # scheduled_at should be in the future using the shared queue backoff.
    now = await _db_now(db_pool)
    assert row["scheduled_at"] > now


async def test_process_batch_commits_fast_action_when_later_action_times_out(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    fast_trigger_ref = uuid.uuid4()
    slow_trigger_ref = uuid.uuid4()
    calls: list[str] = []

    async def _fast(payload, tid, trid):
        calls.append("fast")

    async def _slow(payload, tid, trid):
        calls.append("slow")
        await asyncio.sleep(0.2)

    register_handler("broadcast_realtime", _fast)
    register_handler("publish_anomalies", _slow)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions (
              tenant_id, trigger_id, action_kind, action_payload, scheduled_at
            ) VALUES
              ($1, $2, 'broadcast_realtime', '{}'::jsonb, now() - interval '2 seconds'),
              ($1, $3, 'publish_anomalies', '{}'::jsonb, now() - interval '1 second')
            """,
            tenant,
            fast_trigger_ref,
            slow_trigger_ref,
        )

    stats = await process_batch(
        db_pool,
        limit=2,
        tenant_id=tenant,
        action_timeout_seconds=0.01,
    )

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT trigger_id, action_kind, attempts, processed_at, last_error
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND trigger_id = ANY($2::uuid[])
            ORDER BY action_kind
            """,
            tenant,
            [fast_trigger_ref, slow_trigger_ref],
        )

    by_kind = {row["action_kind"]: row for row in rows}
    assert calls == ["fast", "slow"]
    assert stats.processed == 1
    assert stats.failed == 1
    assert by_kind["broadcast_realtime"]["processed_at"] is not None
    assert by_kind["publish_anomalies"]["processed_at"] is None
    assert by_kind["publish_anomalies"]["attempts"] == 1
    assert "TimeoutError" in (by_kind["publish_anomalies"]["last_error"] or "")


async def test_worker_dead_letters_after_max_attempts(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """5 consecutive failures → row is moved to dead-letter (the partial
    index excludes it from the pending poll)."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(
        tenant_id=tenant,
        trigger_ref=trigger_ref,
        with_predictions=False,
        with_entities=False,
    )

    async def _always_failing(payload, tid, trid):
        raise RuntimeError("permanent failure")

    register_handler("broadcast_realtime", _always_failing)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[],
            )

    # Drive N failures. Because scheduled_at is advanced into the future
    # each time, we manually reset it back to now() so the next poll
    # picks up the same row.
    for i in range(MAX_ATTEMPTS):
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pending_post_commit_actions
                SET scheduled_at = now() - interval '1 second'
                WHERE trigger_id = $1
                """,
                trigger_ref,
            )
        await process_batch(db_pool, limit=10, tenant_id=tenant)

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT attempts, processed_at, dead_lettered_at, last_error
            FROM pending_post_commit_actions WHERE trigger_id = $1
            """,
            trigger_ref,
        )
    assert row["attempts"] == MAX_ATTEMPTS
    assert row["dead_lettered_at"] is not None
    assert row["processed_at"] is None
    # Row is excluded from the pending poll now.
    async with db_pool.acquire() as conn:
        pending = await fetch_pending_actions(conn, limit=10, tenant_id=tenant)
    assert all(a.trigger_id != trigger_ref for a in pending)


async def test_worker_fetch_respects_scheduled_at(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """A row scheduled in the future is not returned by fetch_pending."""
    trigger_ref = uuid.uuid4()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions
              (tenant_id, trigger_id, action_kind, action_payload,
               scheduled_at)
            VALUES ($1, $2, 'broadcast_realtime', '{}'::jsonb,
                    now() + interval '1 hour')
            """,
            tenant,
            trigger_ref,
        )
        pending = await fetch_pending_actions(conn, limit=10, tenant_id=tenant)
    assert all(a.trigger_id != trigger_ref for a in pending)


# ---------------------------------------------------------------------
# Worker loop integration — start + stop
# ---------------------------------------------------------------------


async def test_worker_loop_processes_and_stops(
    db_pool: asyncpg.Pool,
    tenant,
    clean_queue,
):
    """Run `post_commit_worker` with a stop_event; enqueue a row;
    verify it gets processed within one poll cycle."""
    from services.reasoning.think.post_commit import post_commit_worker

    trigger_ref = uuid.uuid4()
    diff = _make_diff(
        tenant_id=tenant,
        trigger_ref=trigger_ref,
        with_predictions=False,
        with_entities=False,
    )

    seen = asyncio.Event()

    async def _handler(payload, tid, trid):
        seen.set()

    register_handler("broadcast_realtime", _handler)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None,
                validated_diff=diff,
                conn=conn,
                anomalies=[],
            )

    stop = asyncio.Event()
    task = asyncio.create_task(
        post_commit_worker(
            db_pool,
            poll_interval=0.1,
            stop_event=stop,
            tenant_id=tenant,
        )
    )
    try:
        await asyncio.wait_for(seen.wait(), timeout=5.0)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    async with db_pool.acquire() as conn:
        processed = await conn.fetchval(
            "SELECT processed_at FROM pending_post_commit_actions "
            "WHERE trigger_id = $1",
            trigger_ref,
        )
    assert processed is not None


# ---------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------


async def _db_now(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT now()")
