"""services/reasoning/think/tests/test_applier.py — applier behavior + idempotency.

Unit-ish tests over apply_diff. Many Think end-to-end concerns (region
lock, cascade, anomalies) live in test_end_to_end.py.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from lib.shared.ids import uuid7

from services.domain.models.repo import ModelsRepo
from services.reasoning.think.applier import (
    AlreadyAppliedError, apply_diff, hash_diff,
)
from services.reasoning.think.diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    MemoryLifecycleOp,
    OntologyGapOp,
    RawDiff,
    RelationClaimOp,
    RelationFrameOp,
    RelationFrameParticipantOp,
    ValidatedDiff,
)
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.sage.inquiry_traces import (
    OutcomeEventsRepo,
    TraceContext,
    reset_trace_context,
    set_trace_context,
)
from services.reasoning.sage.topology_optimizer.optimizer import TopologyOptimizer
from services.reasoning.sage.reader import SynthesisReader
from services.reasoning.think.capability_probes import maybe_inject_capability_probe_ops
from services.reasoning.think.text_embedding import deterministic_text_embedding
from services.reasoning.think.validator import validate


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_applier_model(conn, tenant, observation_id, natural: str):
    from services.reasoning.think.tests.conftest import make_embedding

    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                '{}'::jsonb, 0.6, 1.0, 'active', 0.6, 1.0)
        """,
        mid,
        tenant,
        observation_id,
        json.dumps({"kind": "state", "subject": natural, "assertion": "true"}),
        natural,
        make_embedding(natural),
    )
    return mid


async def _insert_inquiry_session(conn, tenant) -> UUID:
    session_id = uuid7()
    await conn.execute(
        """
        INSERT INTO inquiry_sessions (
          id, tenant_id, signal_ref_type, signal_ref_id,
          route, status, stop_status
        ) VALUES (
          $1, $2, 'internal', NULL,
          'DEEP_INQUIRY_PATH', 'running', 'insufficient_continue'
        )
        """,
        session_id,
        tenant,
    )
    return session_id


async def test_apply_diff_acquires_tenant_model_write_lock(
    fresh_db,
    tenant,
    tenant_cleanup,
    monkeypatch,
):
    """Apply serializes the short model-write phase per tenant."""
    from services.reasoning.think import region_locks

    calls = []
    original = region_locks.acquire_region_lock

    async def spy_acquire_region_lock(conn, tenant_id, entity_ids):
        calls.append(list(entity_ids))
        return await original(conn, tenant_id, entity_ids)

    monkeypatch.setattr(
        region_locks,
        "acquire_region_lock",
        spy_acquire_region_lock,
    )

    async with fresh_db.acquire() as conn:
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
        )
        async with conn.transaction():
            await apply_diff(diff, conn, trigger_kind="T1")

    assert calls[0] == [("tenant_model_write", str(tenant))]


async def test_reconciler_db_error_does_not_poison_apply_transaction(
    fresh_db,
    tenant,
    tenant_cleanup,
    monkeypatch,
):
    """Reconciler is best-effort, so its DB failures use a savepoint."""
    from services.reasoning.think import reconciler

    async def failing_inner(*args, **kwargs):
        conn = args[1]
        await conn.execute("SELECT 1 / 0")

    monkeypatch.setattr(reconciler, "_reconcile_inner", failing_inner)

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            result = await reconciler.reconcile_claim_op(
                ClaimOp(op="insert", entry={"natural": "duplicate-ish claim"}),
                conn,
                tenant_id=tenant,
                trigger_id=uuid7(),
                think_run_id=uuid7(),
            )
            assert result.decision == "skipped"
            assert await conn.fetchval("SELECT 1") == 1


async def test_apply_single_claim_insert(fresh_db, tenant, tenant_cleanup):
    """Happy path: a single claim_op insert creates a Model + state_change."""
    from services.reasoning.think.tests.conftest import make_embedding
    async with fresh_db.acquire() as conn:
        # Seed an observation for born_from_event_id.
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid, tenant, make_embedding("x"),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {"kind": "state", "subject": "x", "assertion": "ships"},
                    "natural": "x ships",
                    "embedding": make_embedding("x ships"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                }),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )
        assert len(result["claim_ops"]) == 1
        assert result["applied_model_ids"]
        # applied_triggers row present.
        outcome = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            diff.trigger_ref,
        )
        assert outcome == "success"


async def test_source_digest_pattern_insert_stays_single_pattern_model(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Source digest claims are already compressed patterns, not compounds."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'aws:event', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            oid,
            tenant,
            "[aws] lambda:createfunction<num>",
            make_embedding("[aws] lambda:createfunction<num>"),
        )
        natural = (
            "The aws:event source is showing a source cadence: 10 recent "
            "observations form a major source window. This should be "
            "represented as a compact source-pattern baseline, not left as "
            "independent low-level events."
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "pattern",
                            "abstraction_level": "pattern",
                            "time_mode": "recurring",
                            "modality": "observed",
                            "polarity": "neutral",
                            "signature": "aws:event recurring source pattern",
                            "observed_tendency": (
                                "10 recent observations form a major source window."
                            ),
                            "domain_tags": [
                                "source_digest",
                                "discovered_pattern",
                                "major_source_window",
                            ],
                        },
                        "natural": natural,
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.66,
                        "confidence_at_assertion": 0.66,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": (
                                "The aws:event stream no longer contributes a "
                                "major recurring source window."
                            ),
                            "within_window": "P7D",
                        },
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        rows = await conn.fetch(
            """
            SELECT claim_role, abstraction_level, proposition, domain_tags
            FROM models
            WHERE tenant_id = $1
            ORDER BY created_at
            """,
            tenant,
        )

    assert result["split_summary"]["compound_inputs"] == 0
    assert len(rows) == 1
    proposition = rows[0]["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    assert rows[0]["claim_role"] == "pattern"
    assert rows[0]["abstraction_level"] == "pattern"
    assert proposition["claim_role"] == "pattern"
    assert {"source_digest", "major_source_window"} <= set(rows[0]["domain_tags"])


async def test_curiosity_hypothesis_insert_survives_model_apply(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Open-question hypotheses should persist as searchable company memory."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'slack:message', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            oid,
            tenant,
            "Atlas launch blocker discussion",
            make_embedding("Atlas launch blocker discussion"),
        )
        natural = (
            "Open operating questions remain for Atlas launch: who owns the "
            "next action and whether the blocker is on the critical path."
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "hypothesis",
                            "abstraction_level": "atomic",
                            "time_mode": "current",
                            "modality": "inferred",
                            "polarity": "neutral",
                            "hypothesis_text": natural,
                            "test_conditions": (
                                "Resolve by finding owner and critical path evidence."
                            ),
                            "important_unknowns": [
                                "responsible owner",
                                "whether the blocker is on the critical path",
                            ],
                            "coverage_roles": [
                                "curiosity",
                                "epistemic",
                                "intervention",
                            ],
                            "retrieval_tags": [
                                "open_question",
                                "unresolved_unknown",
                                "success_driver",
                                "coverage_curiosity",
                                "manager_question",
                                "operator_question",
                            ],
                            "domain_tags": [
                                "open_question",
                                "coverage_curiosity",
                                "manager_question",
                                "operator_question",
                            ],
                        },
                        "natural": natural,
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.58,
                        "confidence_at_assertion": 0.58,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": "The owner and critical path status are resolved.",
                            "within_window": "P14D",
                        },
                        "domain_tags": ["open_question", "coverage_curiosity"],
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        row = await conn.fetchrow(
            """
            SELECT claim_role, proposition, domain_tags
            FROM models
            WHERE tenant_id = $1
            """,
            tenant,
        )

    assert result["memory_aggregation"]["model_inserts"] == 1
    assert row is not None
    assert row["claim_role"] == "hypothesis"
    proposition = row["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    assert "curiosity" in set(proposition["coverage_roles"])
    assert "open_question" in set(proposition["retrieval_tags"])
    assert "coverage_curiosity" in set(row["domain_tags"])


async def test_apply_batch_insert_threads_all_supporting_events(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Batched T1 writes should preserve every triggering observation id."""
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        first = await _insert_observation(
            conn,
            tenant,
            content_text="Northstar before state",
        )
        second = await _insert_observation(
            conn,
            tenant,
            content_text="Northstar after state",
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "fact",
                            "abstraction_level": "atomic",
                            "assertion": "Northstar has a pricing transition.",
                        },
                        "natural": "Northstar has a pricing transition.",
                        "confidence": 0.6,
                    },
                ),
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1:event_batch",
                trigger_cause_event_id=first,
                trigger_supporting_event_ids=[first, second],
            )
        model_id = result["applied_model_ids"][0]
        row = await conn.fetchrow(
            """
            SELECT born_from_event_id, supporting_event_ids
            FROM models
            WHERE id = $1
            """,
            model_id,
        )

    assert row["born_from_event_id"] == first
    assert row["supporting_event_ids"] == [first, second]


async def test_apply_batch_update_merges_supporting_events(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Batch evidence should attach to updated Models without replacing history."""
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        first = await _insert_observation(conn, tenant, content_text="initial")
        second = await _insert_observation(conn, tenant, content_text="follow-up")
        third = await _insert_observation(conn, tenant, content_text="batch")
        model_id = await _insert_applier_model(conn, tenant, first, "memory")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="update",
                    model_id=model_id,
                    changes={"confidence": 0.68},
                ),
            ],
        )

        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1:event_batch",
                trigger_cause_event_id=second,
                trigger_supporting_event_ids=[second, third],
            )
        row = await conn.fetchrow(
            """
            SELECT supporting_event_ids, confidence
            FROM models
            WHERE id = $1
            """,
            model_id,
        )

    assert result["claim_ops"][0]["changed"] == [
        "confidence",
        "supporting_event_ids",
    ]
    assert row["supporting_event_ids"] == [second, third]
    assert float(row["confidence"]) == 0.68


async def test_apply_claim_update_writes_semantic_terms_sidecar(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="semantic terms")
        mid = await _insert_applier_model(conn, tenant, oid, "refund replay model")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="update",
                    model_id=mid,
                    changes={
                        "semantic_terms": [
                            "refund replay drift",
                            "idempotency key collision",
                        ],
                    },
                ),
            ],
        )

        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        terms = await conn.fetchval(
            """
            SELECT semantic_terms
            FROM model_semantic_terms
            WHERE model_id = $1
            """,
            mid,
        )

    assert result["claim_ops"][0]["op"] == "update"
    assert "semantic_terms" in result["claim_ops"][0]["changed"]
    assert terms == ["refund replay drift", "idempotency key collision"]


async def test_apply_claim_update_coerces_iso_timestamp_fields(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """LLM JSON timestamp strings should not reach asyncpg as raw strings."""
    from services.reasoning.think.tests.conftest import _insert_observation

    resolved_at = "2026-06-11T10:27:30.575358+00:00"
    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="resolved")
        mid = await _insert_applier_model(conn, tenant, oid, "timestamp model")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="update",
                    model_id=mid,
                    changes={
                        "resolved_at": resolved_at,
                        "resolution_outcome": True,
                        "last_confirmed_at": resolved_at,
                    },
                )
            ],
        )

        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        row = await conn.fetchrow(
            """
            SELECT resolved_at, resolution_outcome, last_confirmed_at
            FROM models
            WHERE id = $1
            """,
            mid,
        )

    assert result["claim_ops"][0]["op"] == "update"
    assert row["resolved_at"].isoformat() == resolved_at
    assert row["resolution_outcome"] is True
    assert row["last_confirmed_at"].isoformat() == resolved_at


async def test_apply_prediction_insert_materializes_internal_prediction(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Prediction Models should immediately enter the lifecycle ledger."""
    from services.reasoning.think.tests.conftest import _insert_observation, make_embedding

    evaluate_at = "2026-06-25T10:00:00+00:00"
    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="forecast")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {
                        "kind": "prediction",
                        "expected": "Atlas renewal probability will recover",
                        "resolution": {
                            "kind": "metric_delta",
                            "check_after": evaluate_at,
                            "value_constraint": {
                                "field": "delta",
                                "op": "gt",
                                "value": 0,
                            },
                        },
                    },
                    "natural": "Atlas renewal probability should recover.",
                    "embedding": make_embedding("Atlas renewal probability should recover."),
                    "scope_actors": [],
                    "scope_entities": [{"type": "customer", "id": str(uuid7())}],
                    "scope_temporal": {
                        "valid_from": "2026-06-11T00:00:00+00:00",
                        "valid_until": evaluate_at,
                    },
                    "confidence": 0.72,
                    "confidence_at_assertion": 0.72,
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "Atlas renewal probability declines",
                        "within_window": "P14D",
                    },
                }),
            ],
        )

        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        model_id = result["applied_model_ids"][0]
        model_row = await conn.fetchrow(
            "SELECT evaluate_at, resolution_criteria FROM models WHERE id = $1",
            model_id,
        )
        prediction_row = await conn.fetchrow(
            """
            SELECT model_id, prediction, expected_observation, check_after,
                   status, confidence
            FROM model_predictions
            WHERE tenant_id = $1 AND model_id = $2
            """,
            tenant,
            model_id,
        )

    assert result["claim_ops"][0]["model_prediction_id"]
    resolution_criteria = (
        json.loads(model_row["resolution_criteria"])
        if isinstance(model_row["resolution_criteria"], str)
        else model_row["resolution_criteria"]
    )
    expected_observation = (
        json.loads(prediction_row["expected_observation"])
        if isinstance(prediction_row["expected_observation"], str)
        else prediction_row["expected_observation"]
    )
    assert model_row["evaluate_at"].isoformat() == evaluate_at
    assert resolution_criteria["source"] == "think_prediction_lifecycle"
    assert prediction_row["model_id"] == model_id
    assert "Atlas renewal probability" in prediction_row["prediction"]
    assert "recover" in prediction_row["prediction"]
    assert expected_observation["kind"] in {"metric_delta", "observation_pattern"}
    assert expected_observation["falsification_rule"]
    assert prediction_row["check_after"].isoformat() == evaluate_at
    assert prediction_row["status"] == "active"
    assert prediction_row["confidence"] == 0.72


async def test_apply_prediction_insert_normalizes_text_resolution_criteria(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Live LLM prediction criteria may arrive as text; apply must canonicalize it."""
    from services.reasoning.think.tests.conftest import _insert_observation, make_embedding

    evaluate_at = "2026-06-25T10:00:00+00:00"
    text_criteria = "Check launch/decision evidence after the stated Friday deadline."
    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="forecast")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {
                        "kind": "prediction",
                        "expected": "Enterprise-control launch will move by Friday",
                        "resolution": (
                            "Later launch evidence shows the decision advanced, "
                            "moved to Friday, or was delayed by capacity."
                        ),
                    },
                    "natural": "Enterprise-control launch will move by Friday.",
                    "embedding": make_embedding(
                        "Enterprise-control launch will move by Friday."
                    ),
                    "scope_actors": [],
                    "scope_entities": [{"type": "customer", "id": str(uuid7())}],
                    "scope_temporal": {
                        "valid_from": "2026-06-11T00:00:00+00:00",
                        "valid_until": evaluate_at,
                    },
                    "evaluate_at": evaluate_at,
                    "resolution_criteria": text_criteria,
                    "confidence": 0.68,
                    "confidence_at_assertion": 0.68,
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "Launch decision evidence contradicts the forecast.",
                        "within_window": "P4D",
                    },
                }),
            ],
        )

        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        model_id = result["applied_model_ids"][0]
        model_row = await conn.fetchrow(
            "SELECT resolution_criteria FROM models WHERE id = $1",
            model_id,
        )
        prediction_row = await conn.fetchrow(
            """
            SELECT expected_observation
            FROM model_predictions
            WHERE tenant_id = $1 AND model_id = $2
            """,
            tenant,
            model_id,
        )

    resolution_criteria = (
        json.loads(model_row["resolution_criteria"])
        if isinstance(model_row["resolution_criteria"], str)
        else model_row["resolution_criteria"]
    )
    expected_observation = (
        json.loads(prediction_row["expected_observation"])
        if isinstance(prediction_row["expected_observation"], str)
        else prediction_row["expected_observation"]
    )
    assert result["claim_ops"][0]["model_prediction_id"]
    assert resolution_criteria["source"] == "think_prediction_lifecycle"
    assert resolution_criteria["natural_language_criteria"] == text_criteria
    assert resolution_criteria["falsification_rule"] == (
        "Launch decision evidence contradicts the forecast."
    )
    assert expected_observation["falsification_rule"] == (
        "Launch decision evidence contradicts the forecast."
    )


async def test_apply_prediction_resolution_syncs_internal_prediction_status(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Resolving a prediction Model should close internal expectations too."""
    from services.reasoning.think.tests.conftest import _insert_observation, make_embedding

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="forecast")
        trigger = uuid7()
        insert_diff = ValidatedDiff(
            trigger_ref=trigger,
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {
                        "kind": "prediction",
                        "expected": "Cobalt SAML packet will unblock review",
                        "resolution": "Review is unblocked by the due date.",
                    },
                    "natural": "Cobalt SAML packet will unblock review.",
                    "embedding": make_embedding("Cobalt SAML packet will unblock review."),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.7,
                    "confidence_at_assertion": 0.7,
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "Review remains blocked",
                        "within_window": "P7D",
                    },
                }),
            ],
        )
        async with conn.transaction():
            insert_result = await apply_diff(
                insert_diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        model_id = insert_result["applied_model_ids"][0]
        resolved_at = datetime(2026, 6, 18, 10, 0, tzinfo=timezone.utc)
        update_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="update",
                    model_id=model_id,
                    changes={
                        "resolved_at": resolved_at,
                        "resolution_outcome": False,
                    },
                )
            ],
        )
        async with conn.transaction():
            update_result = await apply_diff(
                update_diff,
                conn,
                trigger_kind="T2",
                trigger_cause_event_id=oid,
            )
        prediction_status = await conn.fetchval(
            """
            SELECT status
            FROM model_predictions
            WHERE tenant_id = $1 AND model_id = $2
            """,
            tenant,
            model_id,
        )

    assert "model_predictions" in update_result["claim_ops"][0]["changed"]
    assert prediction_status == "falsified"


async def test_apply_memory_lifecycle_confirm_resolves_prediction_model(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Lifecycle reconcile should close prediction Models through the model ledger."""
    from services.reasoning.think.tests.conftest import _insert_observation, make_embedding

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="forecast")
        insert_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {
                        "kind": "prediction",
                        "expected": "Atlas launch will complete by Friday",
                        "resolution": "Launch completion is observed by Friday.",
                    },
                    "natural": "Atlas launch will complete by Friday.",
                    "embedding": make_embedding("Atlas launch will complete by Friday."),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.66,
                    "confidence_at_assertion": 0.66,
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "Atlas launch remains incomplete after Friday",
                        "within_window": "P7D",
                    },
                }),
            ],
        )
        async with conn.transaction():
            insert_result = await apply_diff(
                insert_diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        model_id = insert_result["applied_model_ids"][0]

        evidence_id = await _insert_observation(
            conn,
            tenant,
            content_text="Atlas launch completed by Friday",
        )
        lifecycle_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            memory_lifecycle_ops=[
                MemoryLifecycleOp(
                    model_id=model_id,
                    action="confirm",
                    evidence_event_ids=[evidence_id],
                    rationale="The observed launch completion confirms the forecast.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                lifecycle_diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=evidence_id,
            )
        model_row = await conn.fetchrow(
            """
            SELECT confidence, confirmed_count, last_confirmed_at,
                   resolved_at, resolution_outcome, supporting_event_ids
            FROM models
            WHERE id = $1
            """,
            model_id,
        )
        prediction_status = await conn.fetchval(
            """
            SELECT status
            FROM model_predictions
            WHERE tenant_id = $1 AND model_id = $2
            """,
            tenant,
            model_id,
        )

    assert result["memory_lifecycle_ops"][0]["action"] == "confirm"
    assert result["memory_lifecycle_ops"][0]["compiled_op"] == "update"
    assert result["memory_aggregation"]["memory_lifecycle_ops"] == 1
    assert float(model_row["confidence"]) == pytest.approx(0.71)
    assert model_row["confirmed_count"] == 1
    assert model_row["last_confirmed_at"] is not None
    assert model_row["resolved_at"] is not None
    assert model_row["resolution_outcome"] is True
    assert evidence_id in model_row["supporting_event_ids"]
    assert prediction_status == "confirmed"


async def test_apply_drops_act_op_with_unresolved_confidence_basis(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """A bad act confidence basis should not abort the whole apply tx."""
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="basis missing")
        missing_basis = uuid7()
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="create_commitment",
                    confidence_basis=missing_basis,
                    entity={
                        "title": "Should be skipped before domain insert",
                        "initial_state": "proposed",
                        "priority": 3,
                        "created_by_event_id": str(oid),
                        "contributes_to_goal_ids": [],
                        "estimated_capacity": {"maintenance": True},
                    },
                )
            ],
        )

        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        commitment_count = await conn.fetchval(
            "SELECT count(*) FROM commitments WHERE tenant_id = $1",
            tenant,
        )

    assert result["act_ops"][0]["op"] == "skip"
    assert result["act_ops"][0]["reason"] == "missing_confidence_basis"
    assert result["apply_dropped_op_count"] == 1
    assert commitment_count == 0


async def test_apply_claim_update_drops_unpaired_resolution_timestamp(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """A partial resolution update should be skipped instead of failing apply."""
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text="resolved")
        mid = await _insert_applier_model(conn, tenant, oid, "partial resolution")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="update",
                    model_id=mid,
                    changes={"resolved_at": "2026-06-11T10:27:30.575358+00:00"},
                )
            ],
        )

        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        row = await conn.fetchrow(
            "SELECT resolved_at, resolution_outcome FROM models WHERE id = $1",
            mid,
        )

    assert result["claim_ops"][0]["op"] == "skip"
    assert result["claim_ops"][0]["reason"] == "inconsistent_resolution_update"
    assert row["resolved_at"] is None
    assert row["resolution_outcome"] is None


async def test_apply_dedupes_split_situation_members_after_reconcile(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Duplicate atomic splits can reconcile to the same Model twice."""
    from services.reasoning.think.tests.conftest import _insert_observation

    natural = (
        "Atlas renewal is at risk and Atlas renewal is at risk because "
        "audit evidence is missing"
    )
    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(conn, tenant, content_text=natural)
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "proposition": {
                            "kind": "concern",
                            "about": "Atlas renewal",
                            "nature": natural,
                            "raised_by": "test",
                        },
                        "natural": natural,
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                        "supporting_event_ids": [str(oid)],
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        row = await conn.fetchrow(
            """
            SELECT proposition
            FROM models
            WHERE tenant_id = $1
              AND claim_role = 'situation'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
        )

    assert result["split_summary"]["compound_inputs"] == 1
    assert row is not None
    proposition = row["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    member_ids = proposition["member_model_ids"]
    assert len(member_ids) == len(set(member_ids))
    assert len(member_ids) >= 2


async def test_apply_adds_required_situation_compositional_defaults(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Live/provider or splitter situations missing DB-required fields
    should be normalized before insert."""
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="A composite execution pressure emerged.",
        )
        member_a = await _insert_applier_model(
            conn,
            tenant,
            oid,
            "Atlas operating pressure is visible.",
        )
        member_b = await _insert_applier_model(
            conn,
            tenant,
            oid,
            "Atlas delivery pressure is visible.",
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "proposition": {
                            "kind": "situation",
                            "situation": "Atlas operating pressure",
                            "summary": "Atlas has linked operating pressure.",
                            "member_model_ids": [str(member_a), str(member_b)],
                            "relationship_summary": (
                                "Operating signals are linked."
                            ),
                            "status": "forming",
                        },
                        "natural": "Atlas operating pressure is forming.",
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
            )
        row = await conn.fetchrow(
            """
            SELECT proposition
            FROM models
            WHERE tenant_id = $1
              AND claim_role = 'situation'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
        )

    assert result["claim_ops"][0]["op"] == "insert"
    assert row is not None
    proposition = row["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    assert proposition["pressure_type"] == "execution"
    assert proposition["shared_mechanism"]
    assert proposition["judgment_change"]
    assert proposition["open_falsifier"]


async def test_apply_claim_insert_strips_llm_invented_model_id(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Live providers sometimes put a generated model_id inside insert.entry."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid,
            tenant,
            make_embedding("x"),
        )
        invented = uuid4()
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "model_id": str(invented),
                        "proposition": {
                            "kind": "state",
                            "subject": "x",
                            "assertion": "ships",
                        },
                        "natural": "x ships",
                        "embedding": make_embedding("x ships"),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                    },
                ),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )

        inserted_model_id = UUID(result["claim_ops"][0]["model_id"])
        assert inserted_model_id != invented


async def test_apply_resolves_same_diff_invented_model_id_for_edges(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """If the LLM uses entry.model_id as a same-diff placeholder, resolve it."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid,
            tenant,
            make_embedding("x"),
        )
        existing = await _insert_applier_model(conn, tenant, oid, "existing")
        invented = uuid4()
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(oid),
                        "model_id": str(invented),
                        "proposition": {
                            "kind": "state",
                            "subject": "x",
                            "assertion": "ships",
                        },
                        "natural": "x ships",
                        "embedding": make_embedding("x ships"),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.6,
                        "confidence_at_assertion": 0.6,
                    },
                ),
            ],
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=invented,
                    target_model_id=existing,
                    edge_kind="supports",
                    weight=0.5,
                ),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )

        inserted_model_id = UUID(result["claim_ops"][0]["model_id"])
        row = await conn.fetchrow(
            """
            SELECT source_model_id, target_model_id
            FROM model_edges
            WHERE tenant_id = $1
              AND edge_kind = 'supports'
            """,
            tenant,
        )
        pair_row = await conn.fetchrow(
            """
            SELECT explicit_relation_count, edge_kind_votes, direction_votes
            FROM model_pair_evidence
            WHERE tenant_id = $1
              AND (
                (model_a_id = $2 AND model_b_id = $3)
                OR (model_a_id = $3 AND model_b_id = $2)
              )
            """,
            tenant,
            inserted_model_id,
            existing,
        )
        placeholder_pair_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM model_pair_evidence
            WHERE tenant_id = $1
              AND (model_a_id = $2 OR model_b_id = $2)
            """,
            tenant,
            invented,
        )
        assert row is not None
        assert row["source_model_id"] == inserted_model_id
        assert row["target_model_id"] == existing
        assert pair_row is not None
        edge_kind_votes = (
            json.loads(pair_row["edge_kind_votes"])
            if isinstance(pair_row["edge_kind_votes"], str)
            else pair_row["edge_kind_votes"]
        )
        assert pair_row["explicit_relation_count"] == 1
        assert edge_kind_votes["supports"] == 1
        assert placeholder_pair_count == 0


async def test_apply_resolves_same_diff_insert_refs_for_edges_and_acts(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        new_event = uuid7()
        existing_event = uuid7()
        await conn.executemany(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            [
                (new_event, tenant, "new done event", make_embedding("new done event")),
                (
                    existing_event,
                    tenant,
                    "existing state",
                    make_embedding("existing state"),
                ),
            ],
        )
        existing_model = await _insert_applier_model(
            conn,
            tenant,
            existing_event,
            "existing state",
        )
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id,
            tenant,
        )
        commitment_id = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at, is_maintenance)
            VALUES ($1, $2, 'x', 'active', $3, $4, now(), TRUE)
            """,
            commitment_id,
            tenant,
            actor_id,
            existing_event,
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "state",
                            "subject": "x",
                            "assertion": "done",
                        },
                        "natural": "x is done",
                        "embedding": make_embedding("x is done"),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.82,
                        "confidence_at_assertion": 0.82,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": "x is reopened or marked incomplete",
                            "within_window": "P14D",
                        },
                    },
                )
            ],
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=new_event,
                    target_model_id=existing_model,
                    edge_kind="superseded_by",
                    confidence=0.8,
                    explanation="The new done state supersedes the older state.",
                )
            ],
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=new_event,
                    entity={
                        "id": str(commitment_id),
                        "new_state": "doneunverified",
                    },
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
                models_repo=repo,
            )
        inserted_model = UUID(result["claim_ops"][0]["model_id"])
        edge = await conn.fetchrow(
            """
            SELECT source_model_id, target_model_id FROM model_edges
            WHERE tenant_id=$1 AND edge_kind='superseded_by'
            """,
            tenant,
        )
        commitment = await conn.fetchrow(
            "SELECT state, last_confidence_basis FROM commitments WHERE id=$1",
            commitment_id,
        )

    assert edge["source_model_id"] == existing_model
    assert edge["target_model_id"] == inserted_model
    assert commitment["state"] == "doneunverified"
    assert commitment["last_confidence_basis"] == inserted_model


async def test_reconciler_folds_llm_insert_without_embedding_into_existing_model(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Strict live-LLM inserts omit embeddings; reconciliation still runs."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        old_event = uuid7()
        new_event = uuid7()
        await conn.executemany(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            [
                (
                    old_event,
                    tenant,
                    "renewal risk is rising",
                    make_embedding("renewal risk is rising"),
                ),
                (
                    new_event,
                    tenant,
                    "renewal risk is rising",
                    make_embedding("renewal risk is rising"),
                ),
            ],
        )
        natural = "Atlas renewal risk is rising because audit evidence is late."
        existing_model = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                    '{}'::jsonb, 0.6, 1.0, 'active', 0.6, 1.0)
            """,
            existing_model,
            tenant,
            old_event,
            json.dumps({
                "kind": "concern",
                "about": "Atlas renewal",
                "nature": "audit evidence is late",
                "raised_by": "customer",
            }),
            natural,
            deterministic_text_embedding(natural),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "concern",
                            "about": "Atlas renewal",
                            "nature": "audit evidence is late",
                            "raised_by": "customer",
                            "domain_tags": ["source_digest", "major_source_window"],
                            "retrieval_tags": [
                                "source_digest",
                                "coverage_discovered_pattern",
                            ],
                            "coverage_roles": ["source", "discovered_pattern"],
                        },
                        "natural": natural,
                        "domain_tags": ["source_digest"],
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.78,
                        "confidence_at_assertion": 0.78,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": "Audit evidence is delivered and accepted by procurement",
                            "within_window": "P14D",
                        },
                    },
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
                models_repo=repo,
            )

        assert result["claim_ops"][0]["op"] == "update"
        assert result["claim_ops"][0]["model_id"] == str(existing_model)
        assert result["claim_ops"][0]["reconcile_decision"] == "auto_merge"
        assert result["reconcile_summary"]["auto_merge"] == 1
        model_count = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        row = await conn.fetchrow(
            """
            SELECT confidence, supporting_event_ids, signal_readings,
                   confirmed_count, last_confirmed_at, domain_tags, proposition
            FROM models WHERE id = $1
            """,
            existing_model,
        )

    assert model_count == 1
    assert float(row["confidence"]) == 0.78
    assert new_event in row["supporting_event_ids"]
    assert row["confirmed_count"] == 1
    assert row["last_confirmed_at"] is not None
    assert "source_digest" in set(row["domain_tags"])
    assert "major_source_window" in set(row["domain_tags"])
    proposition = row["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    assert "source_digest" in set(proposition["retrieval_tags"])
    assert "discovered_pattern" in set(proposition["coverage_roles"])
    readings = row["signal_readings"]
    if isinstance(readings, str):
        readings = json.loads(readings)
    assert readings[-1]["kind"] == "confirm"
    assert readings[-1]["source_event_id"] == str(new_event)


async def test_outcome_events_filter_pending_relation_placeholders(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        old_event = uuid7()
        new_event = uuid7()
        trigger_ref = uuid7()
        session_id = await _insert_inquiry_session(conn, tenant)
        target_model = await _insert_applier_model(
            conn,
            tenant,
            old_event,
            "HubSpot import depends on DPA approval.",
        )
        diff = ValidatedDiff(
            trigger_ref=trigger_ref,
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "concern",
                            "subject": "DPA approval",
                            "assertion": "DPA approval is missing.",
                        },
                        "natural": "DPA approval is missing.",
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.78,
                        "confidence_at_assertion": 0.78,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": "DPA approval is delivered.",
                            "within_window": "P14D",
                        },
                    },
                )
            ],
            relation_claim_ops=[
                RelationClaimOp(
                    source_model_id=new_event,
                    target_model_id=target_model,
                    subject_ref={"kind": "pending_model", "born_from_event_id": str(new_event)},
                    object_ref={"kind": "model", "model_id": str(target_model)},
                    predicate="blocks",
                    edge_kind="blocks",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.78,
                    evidence_event_ids=[new_event],
                    evidence_model_ids=[new_event, target_model],
                    explanation="DPA approval blocks the import.",
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        ctx = TraceContext(
            tenant_id=tenant,
            inquiry_session_id=session_id,
            pool=fresh_db,
            conn=conn,
            metadata={"question_primitives": ["CONSTRAINT"], "trigger_kind": "T1"},
        )
        token = set_trace_context(ctx)
        try:
            async with conn.transaction():
                result = await apply_diff(
                    diff,
                    conn,
                    trigger_kind="T1",
                    trigger_cause_event_id=new_event,
                    models_repo=repo,
                )
        finally:
            reset_trace_context(token)

        applied_model_ids = {
            UUID(str(model_id)) for model_id in result["applied_model_ids"]
        }
        assert len(applied_model_ids) == 1
        created_model = next(iter(applied_model_ids))
        events = await OutcomeEventsRepo(
            fresh_db,
            tenant_id=tenant,
        ).list_for_session(session_id)
        emitted_model_ids = {
            UUID(row.payload["model_id"])
            for row in events
            if row.event_type == "node_used_in_valid_diff"
        }

    assert created_model in emitted_model_ids
    assert target_model in emitted_model_ids
    assert new_event not in emitted_model_ids


async def test_question_policy_probe_feedback_reaches_policy_stats(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        event_id = uuid7()
        session_id = await _insert_inquiry_session(conn, tenant)
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(event_id),
                        "supporting_event_ids": [str(event_id)],
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "capability",
                            "abstraction_level": "atomic",
                            "capability_id": (
                                "question_policy_missing_context_precision"
                            ),
                            "subject": "question policy",
                            "assessment": (
                                "Question-policy probe: asking for the missing "
                                "approval owner before writing a strong launch "
                                "relation would have improved precision."
                            ),
                        },
                        "natural": (
                            "Question-policy probe: asking for the missing "
                            "approval owner before writing a strong launch "
                            "relation would have improved precision."
                        ),
                        "scope_actors": [],
                        "scope_entities": [
                            {"type": "customer", "id": str(uuid7())}
                        ],
                        "scope_temporal": {},
                        "confidence": 0.72,
                        "confidence_at_assertion": 0.72,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": (
                                "Future similar probes show the extra question "
                                "has no precision benefit."
                            ),
                            "within_window": "P30D",
                        },
                        "domain_tags": [
                            "question_policy",
                            "learning",
                            "capability_probe",
                        ],
                    },
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        ctx = TraceContext(
            tenant_id=tenant,
            inquiry_session_id=session_id,
            pool=fresh_db,
            conn=conn,
            metadata={
                "question_primitives": ["DEPENDENCY"],
                "signal_type": "T1",
                "trigger_kind": "T1:event_batch",
                "entities": ["customer:enterprise-control"],
            },
        )
        token = set_trace_context(ctx)
        try:
            async with conn.transaction():
                result = await apply_diff(
                    diff,
                    conn,
                    trigger_kind="T1:event_batch",
                    trigger_cause_event_id=event_id,
                    models_repo=repo,
                )
        finally:
            reset_trace_context(token)

        model_ids = [UUID(str(model_id)) for model_id in result["applied_model_ids"]]
        assert len(model_ids) == 1
        model_id = model_ids[0]
        attribution = await conn.fetchrow(
            """
            SELECT question_primitive, signal_type, selected, activation_score
            FROM sage_reader_decision_attributions
            WHERE tenant_id = $1
              AND inquiry_session_id = $2
              AND model_id = $3
            """,
            tenant,
            session_id,
            model_id,
        )
        events = await OutcomeEventsRepo(
            fresh_db,
            tenant_id=tenant,
        ).list_for_session(session_id, conn=conn)
        credit_events = [
            event
            for event in events
            if event.event_type == "reader_decision_used_in_valid_diff"
        ]

        report = await TopologyOptimizer(
            pool=fresh_db,
            tenant_id=tenant,
        ).optimize(
            inquiry_session_id=session_id,
            trigger_event="validated_synthesis_diff_applied",
            conn=conn,
        )
        stats = await conn.fetchrow(
            """
            SELECT attempts, successes, total_credit, utility_score
            FROM sage_question_policy_stats
            WHERE tenant_id = $1
              AND signal_type = 'T1'
              AND question_primitive = 'DEPENDENCY'
            """,
            tenant,
        )

    assert attribution is not None
    assert attribution["question_primitive"] == "DEPENDENCY"
    assert attribution["signal_type"] == "T1"
    assert attribution["selected"] is True
    assert attribution["activation_score"] == 1.0
    assert len(credit_events) == 1
    assert credit_events[0].payload["model_id"] == str(model_id)
    assert credit_events[0].payload["question_primitive"] == "DEPENDENCY"
    assert report.question_policy_updates >= 1
    assert stats is not None
    assert stats["attempts"] >= 1
    assert stats["successes"] >= 1
    assert stats["total_credit"] > 0
    assert stats["utility_score"] > 0


async def test_capability_probe_wave_survives_validate_apply_and_feedback(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    event_time = datetime(2026, 6, 16, tzinfo=timezone.utc)
    fragment_text = (
        "Capability probe. capability_probe=true "
        "capability_probe_kinds=prediction,resource,ontology_gap,archive,"
        "evidence_attachment,question_policy. resource_ops ontology_gap_ops "
        "evidence attachment question_policy evaluate_at archive lifecycle."
    )
    async with fresh_db.acquire() as conn:
        event_id = await _insert_observation(
            conn,
            tenant,
            content_text=fragment_text,
            occurred_at=event_time,
            external_id=f"capability-probe-{uuid7()}",
        )
        session_id = await _insert_inquiry_session(conn, tenant)
        scope_entity = {"type": "customer", "id": str(uuid7())}
        source_model = await _insert_applier_model(
            conn,
            tenant,
            event_id,
            "Enterprise-control launch needs security review.",
        )
        target_model = await _insert_applier_model(
            conn,
            tenant,
            event_id,
            "Security exception approval is still pending.",
        )
        stale_model = await _insert_applier_model(
            conn,
            tenant,
            event_id,
            "Older launch assumption is stale.",
        )
        await conn.execute(
            "UPDATE models SET scope_entities = $2::jsonb WHERE id = ANY($1::uuid[])",
            [source_model, target_model],
            json.dumps([scope_entity]),
        )
        trigger = TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=tenant,
            observation_id=event_id,
            observation_ids=[event_id],
            seed_occurred_at=event_time,
            seed_signature={
                "batch_signal_fragments": [
                    {"observation_id": str(event_id), "text": fragment_text}
                ]
            },
        )
        bundle = ContextBundle(
            models=[
                SimpleNamespace(
                    id=source_model,
                    status="active",
                    confidence=0.9,
                    natural="Enterprise-control launch needs security review.",
                    scope_actors=[],
                    scope_entities=[scope_entity],
                ),
                SimpleNamespace(
                    id=target_model,
                    status="active",
                    confidence=0.8,
                    natural="Security exception approval is still pending.",
                    scope_actors=[],
                    scope_entities=[scope_entity],
                ),
                SimpleNamespace(
                    id=stale_model,
                    status="active",
                    confidence=0.4,
                    natural="Older launch assumption is stale.",
                    scope_actors=[],
                    scope_entities=[],
                ),
            ]
        )
        raw = maybe_inject_capability_probe_ops(
            RawDiff(trigger_ref=uuid7(), tenant_id=tenant),
            trigger,
            bundle,
        )
        retrieval_result = RetrievalResult(
            trigger=trigger,
            models=[],
            observations=[],
            acts={"goals": [], "commitments": [], "decisions": []},
            resources=[],
            pathway_results=[],
            notes={},
            model_scores={},
        )
        validated = await validate(
            raw,
            retrieval_result,
            conn,
            allowed_region=None,
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        ctx = TraceContext(
            tenant_id=tenant,
            inquiry_session_id=session_id,
            pool=fresh_db,
            conn=conn,
            metadata={
                "question_primitives": ["DEPENDENCY"],
                "signal_type": "T1",
                "trigger_kind": "T1:event_batch",
                "entities": ["customer:enterprise-control"],
            },
        )
        token = set_trace_context(ctx)
        try:
            async with conn.transaction():
                result = await apply_diff(
                    validated,
                    conn,
                    trigger_kind="T1:event_batch",
                    trigger_cause_event_id=event_id,
                    models_repo=repo,
                )
        finally:
            reset_trace_context(token)

        report = await TopologyOptimizer(
            pool=fresh_db,
            tenant_id=tenant,
        ).optimize(
            inquiry_session_id=session_id,
            trigger_event="validated_synthesis_diff_applied",
            conn=conn,
        )
        stats = await conn.fetchrow(
            """
            SELECT attempts, successes, total_credit, utility_score
            FROM sage_question_policy_stats
            WHERE tenant_id = $1
              AND signal_type = 'T1'
              AND question_primitive = 'DEPENDENCY'
            """,
            tenant,
        )
        stale_status = await conn.fetchval(
            "SELECT status FROM models WHERE id = $1",
            stale_model,
        )
        prediction_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_predictions
            WHERE tenant_id = $1
            """,
            tenant,
        )

    aggregation = result["memory_aggregation"]
    claim_summaries = result["claim_ops"]
    assert validated.dropped_op_count == 0
    assert len(result["resource_ops"]) == 1
    assert result["resource_ops"][0]["op"] == "create_resource"
    assert len(result["ontology_gap_ops"]) == 1
    assert result["ontology_gap_ops"][0]["op"] == "propose_edge_type"
    assert aggregation["model_archives"] == 1
    assert aggregation["evidence_attachments"] == 1
    assert prediction_count == 1
    assert stale_status == "archived"
    assert any(summary.get("model_prediction_id") for summary in claim_summaries)
    assert any(
        {"question_policy", "capability_probe"}
        <= {str(tag) for tag in (summary.get("domain_tags") or [])}
        for summary in claim_summaries
    )
    assert report.question_policy_updates >= 1
    assert stats is not None
    assert stats["attempts"] >= 1
    assert stats["successes"] >= 1
    assert stats["total_credit"] > 0
    assert stats["utility_score"] > 0


async def test_quality_downgrade_attaches_observe_reading_without_new_model(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    scope_entity = {"type": "customer", "id": str(uuid7())}
    anchor_natural = "Acme renewal call felt rough after the customer review."
    signal_natural = "Yesterday's call with Acme felt rough."
    async with fresh_db.acquire() as conn:
        old_event = await _insert_observation(
            conn, tenant, content_text=anchor_natural,
        )
        new_event = await _insert_observation(
            conn, tenant, content_text=signal_natural,
        )
        anchor_model = uuid7()
        await conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, activation, status, confidence_at_assertion,
                activation_coefficient
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                '{}'::uuid[], $7::jsonb, '{}'::jsonb,
                0.6, 1.0, 'active', 0.6, 1.0
            )
            """,
            anchor_model,
            tenant,
            old_event,
            json.dumps({
                "kind": "belief",
                "claim_role": "fact",
                "subject": "Acme",
                "assertion": anchor_natural,
            }),
            anchor_natural,
            deterministic_text_embedding(anchor_natural),
            json.dumps([scope_entity]),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "fact",
                            "subject": "Acme",
                            "assertion": signal_natural,
                        },
                        "natural": signal_natural,
                        "scope_actors": [],
                        "scope_entities": [scope_entity],
                        "scope_temporal": {},
                        "confidence": 0.5,
                        "confidence_at_assertion": 0.5,
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
            )

        model_count = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        row = await conn.fetchrow(
            """
            SELECT signal_readings, supporting_event_ids, evidential_weight
            FROM models WHERE id = $1
            """,
            anchor_model,
        )
        sidecar_count = await conn.fetchval(
            """
            SELECT count(*) FROM model_signal_readings
            WHERE model_id = $1 AND source_event_id = $2
            """,
            anchor_model,
            new_event,
        )

    assert model_count == 1
    assert result["quality_summary"]["downgrade_to_evidence"] == 1
    assert result["memory_aggregation"]["evidence_attachments"] == 1
    assert result["memory_aggregation"]["model_inserts"] == 0
    assert result["claim_ops"][0]["op"] == "downgrade_to_evidence"
    assert result["claim_ops"][0]["decision"] == "attached_to_existing_model"
    assert result["claim_ops"][0]["model_id"] == str(anchor_model)
    readings = row["signal_readings"]
    if isinstance(readings, str):
        readings = json.loads(readings)
    assert readings[-1]["kind"] == "observe"
    assert readings[-1]["source_event_id"] == str(new_event)
    assert new_event in row["supporting_event_ids"]
    assert float(row["evidential_weight"]) > 0.5
    assert sidecar_count == 1


async def test_scoped_atomic_near_duplicate_absorbs_without_new_model(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Borderline scoped atomics should reinforce existing memory, not clone."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        old_event = uuid7()
        new_event = uuid7()
        await conn.executemany(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            [
                (
                    old_event,
                    tenant,
                    "Atlas evidence old",
                    make_embedding("Atlas evidence old"),
                ),
                (
                    new_event,
                    tenant,
                    "Atlas evidence new",
                    make_embedding("Atlas evidence new"),
                ),
            ],
        )
        customer_id = uuid7()
        scope_entity = {"type": "customer", "id": str(customer_id)}
        v1 = [0.0] * 768
        v1[0] = 1.0
        v2 = [0.0] * 768
        v2[0] = 0.78
        v2[1] = (1.0 - 0.78 ** 2) ** 0.5

        existing_model = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient, supporting_event_ids, domain_tags)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], $7::jsonb,
                    '{}'::jsonb, 0.61, 1.0, 'active', 0.61, 1.0,
                    ARRAY[$3]::uuid[], ARRAY['customers','execution']::text[])
            """,
            existing_model,
            tenant,
            old_event,
            json.dumps({
                "kind": "belief",
                "claim_role": "fact",
                "abstraction_level": "atomic",
                "subject": "Atlas renewal evidence",
                "assertion": "Atlas renewal evidence is delayed",
            }),
            "Atlas renewal evidence is delayed.",
            v1,
            json.dumps([scope_entity]),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "belief",
                            "claim_role": "fact",
                            "abstraction_level": "atomic",
                            "subject": "Atlas renewal evidence",
                            "assertion": (
                                "Atlas renewal evidence remains delayed"
                            ),
                        },
                        "natural": "Atlas renewal evidence remains delayed.",
                        "embedding": v2,
                        "scope_actors": [],
                        "scope_entities": [scope_entity],
                        "scope_temporal": {},
                        "confidence": 0.64,
                        "confidence_at_assertion": 0.64,
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
            )
        model_count = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        row = await conn.fetchrow(
            """
            SELECT signal_readings, supporting_event_ids, evidential_weight,
                   confirmed_count, last_confirmed_at
            FROM models WHERE id = $1
            """,
            existing_model,
        )
        sidecar_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_signal_readings
            WHERE model_id = $1 AND source_event_id = $2
              AND reading_kind = 'confirm'
            """,
            existing_model,
            new_event,
        )

    assert model_count == 1
    assert result["reconcile_summary"]["human_review"] == 1
    assert result["memory_aggregation"]["model_inserts"] == 0
    assert result["memory_aggregation"]["near_duplicate_absorptions"] == 1
    assert result["memory_aggregation"]["absorption_ratio"] == pytest.approx(1.0)
    assert result["claim_ops"][0]["op"] == "absorb_near_duplicate"
    assert result["claim_ops"][0]["decision"] == "attached_to_matched_model"
    assert result["claim_ops"][0]["model_id"] == str(existing_model)
    readings = row["signal_readings"]
    if isinstance(readings, str):
        readings = json.loads(readings)
    assert readings[-1]["kind"] == "confirm"
    assert readings[-1]["source_event_id"] == str(new_event)
    assert new_event in row["supporting_event_ids"]
    assert float(row["evidential_weight"]) > 0.5
    assert row["confirmed_count"] == 1
    assert row["last_confirmed_at"] is not None
    assert sidecar_count == 1


async def test_situation_auto_merge_expands_existing_composite(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Overlapping situations should evolve one composite instead of cloning."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        old_event = uuid7()
        new_event = uuid7()
        await conn.executemany(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            [
                (
                    old_event,
                    tenant,
                    "old renewal pressure",
                    make_embedding("old renewal pressure"),
                ),
                (
                    new_event,
                    tenant,
                    "new renewal pressure",
                    make_embedding("new renewal pressure"),
                ),
            ],
        )
        members = [uuid7() for _ in range(5)]
        new_member = uuid7()
        existing_prop = {
            "kind": "belief",
            "claim_role": "situation",
            "abstraction_level": "composite",
            "time_mode": "current",
            "modality": "inferred",
            "polarity": "mixed",
            "situation": "Atlas renewal delivery pressure",
            "summary": "Atlas renewal, delivery, and audit readiness are linked.",
            "member_model_ids": [str(m) for m in members],
            "relationship_summary": "The members reinforce one renewal risk.",
            "status": "forming",
            "pressure_type": "revenue",
            "shared_mechanism": "The same delivery gap affects renewal readiness.",
            "judgment_change": "The risk is cross-functional.",
            "evidence_event_ids": [str(old_event)],
            "open_falsifier": "Atlas renewal closes and delivery risk clears.",
        }
        existing_model = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient, domain_tags)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                    '{}'::jsonb, 0.62, 1.0, 'active', 0.62, 1.0,
                    ARRAY['customers','execution']::text[])
            """,
            existing_model,
            tenant,
            old_event,
            json.dumps(existing_prop),
            "Atlas renewal delivery pressure is forming.",
            deterministic_text_embedding("Atlas renewal delivery pressure"),
        )
        candidate_prop = {
            **existing_prop,
            "summary": (
                "Atlas renewal, delivery, audit readiness, and support capacity "
                "are linked."
            ),
            "member_model_ids": [str(m) for m in members[:4]] + [str(new_member)],
            "affected_customers": ["Atlas"],
            "evidence_event_ids": [str(new_event)],
        }
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(new_event),
                        "proposition": candidate_prop,
                        "natural": "Atlas renewal delivery pressure is widening.",
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.72,
                        "confidence_at_assertion": 0.72,
                        "domain_tags": ["customers", "execution", "revenue"],
                    },
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
            )

        model_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM models
            WHERE tenant_id = $1 AND claim_role = 'situation'
            """,
            tenant,
        )
        row = await conn.fetchrow(
            """
            SELECT proposition, domain_tags, signal_readings, supporting_event_ids
            FROM models
            WHERE id = $1
            """,
            existing_model,
        )
        member_rows = await conn.fetch(
            """
            SELECT member_model_id, source, evidence_event_ids
            FROM model_composition_members
            WHERE tenant_id = $1 AND composite_model_id = $2
            ORDER BY member_model_id::text
            """,
            tenant,
            existing_model,
        )
        sidecar_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_signal_readings
            WHERE model_id = $1 AND source_event_id = $2
              AND reading_kind = 'confirm'
            """,
            existing_model,
            new_event,
        )

    assert model_count == 1
    assert result["reconcile_summary"]["auto_merge"] == 1
    assert result["memory_aggregation"]["model_inserts"] == 0
    assert result["memory_aggregation"]["situation_model_updates"] == 1
    assert result["memory_aggregation"]["situation_member_additions"] == 1
    assert result["claim_ops"][0]["op"] == "update"
    assert result["claim_ops"][0]["internal_situation_merge"] is True
    assert result["claim_ops"][0]["situation_members_added"] == 1

    proposition = row["proposition"]
    if isinstance(proposition, str):
        proposition = json.loads(proposition)
    assert set(proposition["member_model_ids"]) == {
        str(m) for m in members + [new_member]
    }
    assert {r["member_model_id"] for r in member_rows} == {*members, new_member}
    assert all(r["source"] == "reconciliation_merge" for r in member_rows)
    assert all(new_event in r["evidence_event_ids"] for r in member_rows)
    assert set(row["domain_tags"]) >= {"customers", "execution", "revenue"}
    assert new_event in row["supporting_event_ids"]
    readings = row["signal_readings"]
    if isinstance(readings, str):
        readings = json.loads(readings)
    assert readings[-1]["kind"] == "confirm"
    assert readings[-1]["source_event_id"] == str(new_event)
    assert sidecar_count == 1


async def test_same_event_situations_coalesce_into_one_composite(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Direct and synthesized same-event situations should share one anchor."""
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        event_id = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                    $4, FALSE, 'authoritative')
            """,
            event_id,
            tenant,
            "Atlas renewal and delivery pressure",
            make_embedding("Atlas renewal and delivery pressure"),
        )
        first_members = [uuid7(), uuid7()]
        second_members = [uuid7(), uuid7()]

        def situation_prop(title: str, members: list[UUID]) -> dict:
            return {
                "kind": "belief",
                "claim_role": "situation",
                "abstraction_level": "composite",
                "time_mode": "current",
                "modality": "inferred",
                "polarity": "mixed",
                "domain_tags": ["customers", "execution", "revenue"],
                "situation": title,
                "summary": f"{title} is visible for Atlas.",
                "member_model_ids": [str(m) for m in members],
                "relationship_summary": "The members reinforce one Atlas risk.",
                "status": "forming",
                "pressure_type": "revenue",
                "shared_mechanism": (
                    "The same delivery readiness gap affects Atlas renewal."
                ),
                "judgment_change": (
                    "Together the claims justify one composite situation."
                ),
                "evidence_event_ids": [str(event_id)],
                "open_falsifier": "Atlas renewal risk clears.",
            }

        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(event_id),
                        "proposition": situation_prop(
                            "Atlas renewal pressure",
                            first_members,
                        ),
                        "natural": "Atlas renewal pressure is forming.",
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.68,
                        "confidence_at_assertion": 0.68,
                        "domain_tags": ["customers", "execution", "revenue"],
                    },
                ),
                ClaimOp(
                    op="insert",
                    entry={
                        "tenant_id": str(tenant),
                        "born_from_event_id": str(event_id),
                        "proposition": situation_prop(
                            "Atlas delivery readiness pressure",
                            second_members,
                        ),
                        "natural": (
                            "Atlas delivery readiness pressure is part of the "
                            "same renewal risk."
                        ),
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.66,
                        "confidence_at_assertion": 0.66,
                        "domain_tags": ["customers", "execution", "revenue"],
                    },
                ),
            ],
        )
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=event_id,
            )
        rows = await conn.fetch(
            """
            SELECT id, proposition, supporting_event_ids
            FROM models
            WHERE tenant_id = $1 AND claim_role = 'situation'
            """,
            tenant,
        )
        sidecar_members = await conn.fetch(
            """
            SELECT member_model_id, source
            FROM model_composition_members
            WHERE tenant_id = $1
            """,
            tenant,
        )

    assert len(rows) == 1
    assert result["memory_aggregation"]["model_inserts"] == 1
    assert result["memory_aggregation"]["situation_model_updates"] == 1
    assert result["memory_aggregation"]["situation_member_additions"] == 2
    assert result["claim_ops"][1]["decision"] == "same_event_situation_coalesced"
    prop = rows[0]["proposition"]
    if isinstance(prop, str):
        prop = json.loads(prop)
    assert set(prop["member_model_ids"]) == {
        str(m) for m in first_members + second_members
    }
    assert set(r["member_model_id"] for r in sidecar_members) == {
        *first_members,
        *second_members,
    }
    assert all(
        r["source"] == "same_event_situation_coalesce"
        for r in sidecar_members
    )
    assert event_id in rows[0]["supporting_event_ids"]


async def test_apply_idempotency_second_apply_raises(fresh_db, tenant, tenant_cleanup):
    from services.reasoning.think.tests.conftest import make_embedding
    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid, tenant, make_embedding("x"),
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
                    "natural": "x",
                    "embedding": make_embedding("x"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.5,
                    "confidence_at_assertion": 0.5,
                }),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            await apply_diff(diff, conn, "T1", oid, models_repo=repo)

        # Second apply same trigger.
        async with conn.transaction():
            with pytest.raises(AlreadyAppliedError):
                await apply_diff(diff, conn, "T1", oid, models_repo=repo)


async def test_apply_partial_failure_rolls_back_all_ops(fresh_db, tenant, tenant_cleanup):
    """
    An op mid-apply raising rolls back the whole transaction — no
    partial state, and the applied_triggers row is rolled back with it.
    """
    from services.reasoning.think.tests.conftest import make_embedding
    async with fresh_db.acquire() as conn:
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid, tenant, make_embedding("x"),
        )
        trigger_ref = uuid7()
        diff = ValidatedDiff(
            trigger_ref=trigger_ref,
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
                    "natural": "x",
                    "embedding": make_embedding("x"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.5,
                    "confidence_at_assertion": 0.5,
                }),
                # Second op with invalid archive target → will fail at apply.
                ClaimOp(op="archive", model_id=uuid4(), reason="decay"),
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        with pytest.raises(Exception):
            async with conn.transaction():
                await apply_diff(
                    diff, conn, "T1", oid, models_repo=repo
                )
        # After rollback: no applied_triggers row.
        existing = await conn.fetchval(
            "SELECT COUNT(*) FROM applied_triggers WHERE trigger_id = $1",
            trigger_ref,
        )
        assert existing == 0
        # No models inserted either.
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
    assert n == 0


async def test_apply_drops_domain_invalid_act_op_without_rolling_back_claims(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """
    Late-discovered domain-invalid act_ops are dropped like validator
    partial failures. Valid claim_ops from the same signal still commit.
    """
    from services.reasoning.think.tests.conftest import make_embedding

    async with fresh_db.acquire() as conn:
        oid = uuid7()
        actor_id = uuid7()
        commitment_id = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, 'x',
                    $3, FALSE, 'authoritative')
            """,
            oid, tenant, make_embedding("x"),
        )
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'owner', 'active')",
            actor_id,
            tenant,
        )
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at, is_maintenance)
            VALUES ($1, $2, 'ship reliability work', 'active', $3, $4, now(), TRUE)
            """,
            commitment_id,
            tenant,
            actor_id,
            oid,
        )
        trigger_ref = uuid7()
        diff = ValidatedDiff(
            trigger_ref=trigger_ref,
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(oid),
                    "proposition": {
                        "kind": "state",
                        "subject": "reliability",
                        "assertion": "new risk surfaced",
                    },
                    "natural": "A new reliability risk surfaced.",
                    "embedding": make_embedding("reliability risk"),
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.7,
                    "confidence_at_assertion": 0.7,
                }),
            ],
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=oid,
                    entity={
                        "id": str(commitment_id),
                        "new_state": "blocked",
                    },
                )
            ],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff,
                conn,
                "T1",
                oid,
                models_repo=repo,
            )

        model_count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id = $1",
            tenant,
        )
        commitment_state = await conn.fetchval(
            "SELECT state FROM commitments WHERE id = $1",
            commitment_id,
        )
        trigger_outcome = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            trigger_ref,
        )

    assert model_count == 1
    assert commitment_state == "active"
    assert trigger_outcome == "success"
    assert result["apply_dropped_op_count"] == 1
    assert result["act_ops"][0]["op"] == "skip"
    assert result["act_ops"][0]["reason"] == "illegal_transition"


async def test_hash_diff_is_stable():
    a = ValidatedDiff(
        trigger_ref=uuid7(),
        tenant_id=uuid7(),
        claim_ops=[
            ClaimOp(op="archive", model_id=uuid7(), reason="decay"),
        ],
    )
    h1 = hash_diff(a)
    h2 = hash_diff(a)
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex


async def test_hash_diff_differs_by_content():
    m1 = uuid7()
    m2 = uuid7()
    a = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=uuid7(),
        claim_ops=[ClaimOp(op="archive", model_id=m1, reason="decay")],
    )
    b = ValidatedDiff(
        trigger_ref=a.trigger_ref, tenant_id=a.tenant_id,
        claim_ops=[ClaimOp(op="archive", model_id=m2, reason="decay")],
    )
    assert hash_diff(a) != hash_diff(b)


async def test_apply_edge_ops_add_and_retire(fresh_db, tenant, tenant_cleanup):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="A contradicts B in the operating review",
            external_id=f"edge-op-add-retire-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "A")
        b = await _insert_applier_model(conn, tenant, oid, "B")
        add_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="contradicts",
                    weight=0.8,
                    confidence=0.9,
                    evidence_event_ids=[oid],
                    explanation="A and B cannot both be true.",
                )
            ],
        )
        async with conn.transaction():
            add_result = await apply_diff(add_diff, conn, "T1", oid)

        assert len(add_result["edge_ops"]) == 1
        rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind, confidence,
                   evidence_event_ids, status, review_status
            FROM model_edges
            WHERE tenant_id = $1 AND edge_kind = 'contradicts'
            ORDER BY source_model_id::text
            """,
            tenant,
        )
        assert len(rows) == 2
        assert {
            (r["source_model_id"], r["target_model_id"]) for r in rows
        } == {(a, b), (b, a)}
        assert {r["confidence"] for r in rows} == {0.9}
        assert all(oid in r["evidence_event_ids"] for r in rows)

        retire_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="retire",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="contradicts",
                    reason="operator resolved the contradiction",
                )
            ],
        )
        async with conn.transaction():
            retire_result = await apply_diff(retire_diff, conn, "T1", oid)

        assert retire_result["edge_ops"][0]["retired_edges"] == 2
        statuses = await conn.fetch(
            """
            SELECT status, review_status, status_reason
            FROM model_edges
            WHERE tenant_id = $1 AND edge_kind = 'contradicts'
            """,
            tenant,
        )
        assert {(r["status"], r["review_status"]) for r in statuses} == {
            ("inert", "retired")
        }
        assert {r["status_reason"] for r in statuses} == {
            "operator resolved the contradiction"
        }


async def test_apply_edge_op_attaches_trigger_event_as_edge_evidence(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="The DPA approval blocks the HubSpot import",
            external_id=f"edge-op-cause-evidence-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "DPA approval is pending")
        b = await _insert_applier_model(conn, tenant, oid, "HubSpot import is blocked")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="blocks",
                    weight=0.75,
                    confidence=0.86,
                    explanation="The DPA approval blocks the HubSpot import.",
                )
            ],
        )
        async with conn.transaction():
            await apply_diff(diff, conn, "T1", oid)
        row = await conn.fetchrow(
            """
            SELECT evidence_event_ids, created_by_event_id
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'blocks'
            """,
            tenant,
            a,
            b,
        )

    assert row is not None
    assert row["created_by_event_id"] == oid
    assert row["evidence_event_ids"] == [oid]


async def test_apply_relation_claim_op_persists_claim_and_creates_edge(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="The DPA approval blocks the HubSpot import",
            external_id=f"relation-claim-op-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "DPA approval is pending")
        b = await _insert_applier_model(conn, tenant, oid, "HubSpot import is blocked")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="blocks",
                    edge_kind="blocks",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.86,
                    binding_confidence=0.92,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="The DPA approval blocks the HubSpot import.",
                    explanation="The pending DPA approval gates the import.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)
        claim = await conn.fetchrow(
            """
            SELECT id, source_model_id, target_model_id, edge_kind, write_policy,
                   status, accepted_edge_ids
            FROM relation_claims
            WHERE tenant_id = $1
            """,
            tenant,
        )
        edge = await conn.fetchrow(
            """
            SELECT source_model_id, target_model_id, edge_kind, review_status,
                   evidence_event_ids, metadata
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'blocks'
            """,
            tenant,
            a,
            b,
        )

    assert len(result["relation_claim_ops"]) == 1
    assert result["relation_claim_ops"][0]["status"] == "accepted"
    assert len(result["edge_ops"]) == 1
    assert result["edge_ops"][0]["source"] == "relation_claim_op"
    assert claim is not None
    assert claim["source_model_id"] == a
    assert claim["target_model_id"] == b
    assert claim["write_policy"] == "accepted_edge"
    assert claim["status"] == "accepted"
    assert claim["accepted_edge_ids"]
    assert edge is not None
    assert edge["review_status"] == "accepted"
    assert edge["evidence_event_ids"] == [oid]
    edge_metadata = (
        json.loads(edge["metadata"])
        if isinstance(edge["metadata"], str)
        else edge["metadata"]
    )
    assert edge_metadata["relation_claim_id"] == str(claim["id"])


async def test_apply_retired_relation_claim_retires_projected_edge(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="Future evidence retired the blocker relation",
            external_id=f"relation-claim-retire-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "DPA approval is pending")
        b = await _insert_applier_model(conn, tenant, oid, "HubSpot import is blocked")
        add_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="blocks",
                    edge_kind="blocks",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.86,
                    binding_confidence=0.92,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="The DPA approval blocks the HubSpot import.",
                    explanation="The pending DPA approval gates the import.",
                )
            ],
        )
        async with conn.transaction():
            await apply_diff(add_diff, conn, "T1", oid)

        retire_diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="blocks",
                    edge_kind="blocks",
                    endpoint_binding_status="bound",
                    write_policy="no_edge",
                    status="retired",
                    confidence=0.74,
                    binding_confidence=0.92,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="Future evidence shows the import is no longer blocked.",
                    explanation="Future validation retired the blocker relation.",
                )
            ],
        )
        async with conn.transaction():
            retire_result = await apply_diff(retire_diff, conn, "T1", oid)

        edge = await conn.fetchrow(
            """
            SELECT status, review_status, status_reason
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'blocks'
            """,
            tenant,
            a,
            b,
        )
        retired_claim = await conn.fetchrow(
            """
            SELECT status, write_policy
            FROM relation_claims
            WHERE tenant_id = $1 AND status = 'retired'
            """,
            tenant,
        )

    assert retire_result["relation_claim_ops"][0]["status"] == "retired"
    assert retire_result["edge_ops"][0]["op"] == "retire"
    assert retire_result["edge_ops"][0]["source"] == "relation_claim_op"
    assert retire_result["edge_ops"][0]["retired_edges"] == 1
    assert edge is not None
    assert edge["status"] == "inert"
    assert edge["review_status"] == "retired"
    assert edge["status_reason"] == "Future validation retired the blocker relation."
    assert retired_claim is not None
    assert retired_claim["write_policy"] == "no_edge"


async def test_apply_weighted_relation_claim_creates_weighted_edge(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="Fresh telemetry weakens the launch readiness claim",
            external_id=f"relation-claim-weight-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "Fresh telemetry is bad")
        b = await _insert_applier_model(conn, tenant, oid, "Launch is ready")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="weakens",
                    edge_kind="weakens",
                    weight=0.72,
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.72,
                    binding_confidence=0.92,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="Fresh telemetry weakens launch readiness.",
                    explanation="The new telemetry is counterevidence.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)
        edge = await conn.fetchrow(
            """
            SELECT edge_kind, weight, review_status
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'weakens'
            """,
            tenant,
            a,
            b,
        )

    assert len(result["edge_ops"]) == 1
    assert edge is not None
    assert edge["review_status"] == "accepted"
    assert float(edge["weight"]) == 0.72


async def test_apply_relation_claim_supersedes_generated_support_edge(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.domain.models.repo import _set_model_relations
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="Fresh telemetry weakens the launch readiness claim",
            external_id=f"relation-claim-supersede-support-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "Fresh telemetry is bad")
        b = await _insert_applier_model(conn, tenant, oid, "Launch is ready")
        async with conn.transaction():
            await _set_model_relations(
                conn,
                model_id=b,
                tenant_id=tenant,
                detected_by="think_edge_op",
                supports=[a],
                created_by_event_id=oid,
            )

        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="weakens",
                    edge_kind="weakens",
                    weight=0.72,
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.72,
                    binding_confidence=0.92,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="Fresh telemetry weakens launch readiness.",
                    explanation="The new telemetry is counterevidence.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)

        support = await conn.fetchrow(
            """
            SELECT status, review_status, status_reason
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'supports'
            """,
            tenant,
            a,
            b,
        )
        weakens = await conn.fetchrow(
            """
            SELECT status, review_status, weight, metadata
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'weakens'
            """,
            tenant,
            a,
            b,
        )
        target_arrays = await conn.fetchrow(
            "SELECT supporting_model_ids FROM models WHERE id = $1",
            b,
        )

    assert result["apply_dropped_op_count"] == 0
    assert [edge["op"] for edge in result["edge_ops"]] == ["retire", "add"]
    assert result["edge_ops"][0]["edge_kind"] == "supports"
    assert result["edge_ops"][0]["superseded_by_edge_kind"] == "weakens"
    assert result["relation_claim_ops"][0]["superseded_edge_count"] == 1
    assert support is not None
    assert support["status"] == "inert"
    assert support["review_status"] == "retired"
    assert "superseded_by_relation_claim:weakens" in support["status_reason"]
    assert weakens is not None
    assert weakens["status"] == "active"
    assert weakens["review_status"] == "accepted"
    assert float(weakens["weight"]) == 0.72
    assert a not in list(target_arrays["supporting_model_ids"] or [])


async def test_apply_relation_claim_does_not_supersede_manual_support_edge(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.domain.models.repo import _set_model_relations
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="Fresh telemetry weakens a manually asserted launch claim",
            external_id=f"relation-claim-manual-support-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "Fresh telemetry is bad")
        b = await _insert_applier_model(conn, tenant, oid, "Launch is ready")
        async with conn.transaction():
            await _set_model_relations(
                conn,
                model_id=b,
                tenant_id=tenant,
                detected_by="manual",
                supports=[a],
                created_by_event_id=oid,
            )

        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="weakens",
                    edge_kind="weakens",
                    weight=0.72,
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.72,
                    binding_confidence=0.92,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="Fresh telemetry weakens launch readiness.",
                    explanation="The new telemetry is counterevidence.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)

        support = await conn.fetchrow(
            """
            SELECT status, review_status
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'supports'
            """,
            tenant,
            a,
            b,
        )
        weakens_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'weakens'
            """,
            tenant,
            a,
            b,
        )
        target_arrays = await conn.fetchrow(
            "SELECT supporting_model_ids FROM models WHERE id = $1",
            b,
        )

    assert result["apply_dropped_op_count"] == 1
    assert result["relation_claim_ops"][0]["reason"] == "mutually_exclusive_edge"
    assert support is not None
    assert support["status"] == "active"
    assert support["review_status"] == "accepted"
    assert weakens_count == 0
    assert a in list(target_arrays["supporting_model_ids"] or [])


async def test_apply_symmetric_relation_claim_supersedes_reverse_support(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.domain.models.repo import _set_model_relations
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="The two launch claims cannot both be true",
            external_id=f"relation-claim-contradicts-supports-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "Launch is ready")
        b = await _insert_applier_model(conn, tenant, oid, "Launch cannot proceed")
        async with conn.transaction():
            await _set_model_relations(
                conn,
                model_id=a,
                tenant_id=tenant,
                detected_by="think_edge_op",
                supports=[b],
                created_by_event_id=oid,
            )

        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="contradicts",
                    edge_kind="contradicts",
                    weight=0.81,
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.81,
                    binding_confidence=0.93,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="The two launch claims cannot both be true.",
                    explanation="The assertions are mutually exclusive.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)

        support_rows = await conn.fetch(
            """
            SELECT status, review_status
            FROM model_edges
            WHERE tenant_id = $1
              AND edge_kind = 'supports'
              AND ((source_model_id = $2 AND target_model_id = $3)
                OR (source_model_id = $3 AND target_model_id = $2))
            ORDER BY source_model_id
            """,
            tenant,
            a,
            b,
        )
        contradict_rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, status, review_status, weight
            FROM model_edges
            WHERE tenant_id = $1
              AND edge_kind = 'contradicts'
              AND ((source_model_id = $2 AND target_model_id = $3)
                OR (source_model_id = $3 AND target_model_id = $2))
            """,
            tenant,
            a,
            b,
        )
        arrays = await conn.fetch(
            """
            SELECT id, supporting_model_ids
            FROM models
            WHERE id = ANY($1::uuid[])
            """,
            [a, b],
        )

    assert result["apply_dropped_op_count"] == 0
    assert [edge["op"] for edge in result["edge_ops"]] == ["retire", "add"]
    assert result["relation_claim_ops"][0]["superseded_edge_count"] == 1
    assert len(support_rows) == 1
    assert {row["status"] for row in support_rows} == {"inert"}
    assert {row["review_status"] for row in support_rows} == {"retired"}
    assert len(contradict_rows) == 2
    assert {row["status"] for row in contradict_rows} == {"active"}
    assert {float(row["weight"]) for row in contradict_rows} == {0.81}
    arrays_by_id = {row["id"]: list(row["supporting_model_ids"] or []) for row in arrays}
    assert b not in arrays_by_id[a]


async def test_apply_relation_claim_keeps_support_when_other_conflict_blocks_retry(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.domain.models.edges_repo import EdgesRepo
    from services.domain.models.repo import _set_model_relations
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="A blocks B, but an accepted enables edge already exists",
            external_id=f"relation-claim-enables-conflict-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "Decision is pending")
        b = await _insert_applier_model(conn, tenant, oid, "Import can proceed")
        async with conn.transaction():
            await _set_model_relations(
                conn,
                model_id=b,
                tenant_id=tenant,
                detected_by="think_edge_op",
                supports=[a],
                created_by_event_id=oid,
            )
            await EdgesRepo().link(
                conn,
                source=a,
                target=b,
                kind="enables",
                tenant_id=tenant,
                detected_by="think_edge_op",
                confidence=0.75,
            )

        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="blocks",
                    edge_kind="blocks",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.79,
                    binding_confidence=0.9,
                    evidence_event_ids=[oid],
                    evidence_model_ids=[a, b],
                    evidence_text="The pending decision blocks the import.",
                    explanation="The blocker conflicts with an existing enablement.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)

        rows = await conn.fetch(
            """
            SELECT edge_kind, status, review_status
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
            ORDER BY edge_kind
            """,
            tenant,
            a,
            b,
        )

    assert result["apply_dropped_op_count"] == 1
    assert result["relation_claim_ops"][0]["reason"] == "mutually_exclusive_edge"
    by_kind = {row["edge_kind"]: row for row in rows}
    assert set(by_kind) == {"enables", "supports"}
    assert by_kind["supports"]["status"] == "active"
    assert by_kind["enables"]["status"] == "active"


async def test_apply_relation_frame_op_persists_participants_and_projects_edges(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text=(
                "DPA approval blocks HubSpot import; Priya owns the blocker; "
                "Friday launch may slip; security packet can resolve it."
            ),
            external_id=f"relation-frame-op-{uuid7()}",
        )
        blocker = await _insert_applier_model(conn, tenant, oid, "DPA approval")
        work = await _insert_applier_model(conn, tenant, oid, "HubSpot import")
        owner = await _insert_applier_model(conn, tenant, oid, "Priya/legal owner")
        risk = await _insert_applier_model(conn, tenant, oid, "Friday launch slip")
        resolution = await _insert_applier_model(
            conn,
            tenant,
            oid,
            "Security packet approval",
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_frame_ops=[
                RelationFrameOp(
                    relation_kind="blocked_workstream",
                    status="accepted",
                    participant_binding_status="bound",
                    write_policy="project_edges",
                    confidence=0.86,
                    participants=[
                        RelationFrameParticipantOp(
                            model_id=blocker,
                            role="blocker",
                            binding_confidence=0.9,
                        ),
                        RelationFrameParticipantOp(
                            model_id=work,
                            role="blocked_work",
                            binding_confidence=0.9,
                        ),
                        RelationFrameParticipantOp(
                            model_id=owner,
                            role="owner",
                            binding_confidence=0.8,
                        ),
                        RelationFrameParticipantOp(
                            model_id=risk,
                            role="downstream_risk",
                            binding_confidence=0.82,
                        ),
                        RelationFrameParticipantOp(
                            model_id=resolution,
                            role="possible_resolution",
                            binding_confidence=0.78,
                        ),
                    ],
                    evidence_event_ids=[oid],
                    evidence_model_ids=[blocker, work, owner, risk, resolution],
                    evidence_text="DPA approval blocks HubSpot import.",
                    explanation="This is one blocked workstream frame.",
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)

        frame = await conn.fetchrow(
            """
            SELECT id, relation_kind, status, write_policy
            FROM relation_instances
            WHERE tenant_id = $1
            """,
            tenant,
        )
        participants = await conn.fetch(
            """
            SELECT role, model_id
            FROM relation_participants
            WHERE tenant_id = $1
            ORDER BY role
            """,
            tenant,
        )
        projections = await conn.fetch(
            """
            SELECT projection_rule, edge_kind, source_model_id, target_model_id
            FROM relation_edge_projections
            WHERE tenant_id = $1
            ORDER BY projection_rule
            """,
            tenant,
        )
        edges = await conn.fetch(
            """
            SELECT edge_kind, source_model_id, target_model_id
            FROM model_edges
            WHERE tenant_id = $1
            ORDER BY edge_kind
            """,
            tenant,
        )

    assert result["apply_dropped_op_count"] == 0
    assert result["relation_frame_ops"][0]["projected_edge_count"] == 3
    assert len(result["edge_ops"]) == 3
    assert frame is not None
    assert frame["relation_kind"] == "blocked_workstream"
    assert frame["status"] == "accepted"
    assert frame["write_policy"] == "project_edges"
    assert {row["role"] for row in participants} == {
        "blocked_work",
        "blocker",
        "downstream_risk",
        "owner",
        "possible_resolution",
    }
    projection_tuples = {
        (
            row["source_model_id"],
            row["target_model_id"],
            row["edge_kind"],
            row["projection_rule"],
        )
        for row in projections
    }
    assert (
        blocker,
        work,
        "blocks",
        "blocker_blocks_work",
    ) in projection_tuples
    assert (
        work,
        risk,
        "early_warning_for",
        "blocked_work_warns_downstream_risk",
    ) in projection_tuples
    assert (
        resolution,
        blocker,
        "contributes_to_resolution",
        "resolution_contributes_to_blocker_resolution",
    ) in projection_tuples
    assert all(owner not in {row["source_model_id"], row["target_model_id"]} for row in edges)


async def test_apply_ontology_gap_op_persists_candidate_and_feeds_sage(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="Beacon launch is waiting on executive approval.",
            external_id=f"ontology-gap-op-{uuid7()}",
        )
        blocker = await _insert_applier_model(
            conn,
            tenant,
            oid,
            "Beacon launch is blocked by security exception approval",
        )
        decision = await _insert_applier_model(
            conn,
            tenant,
            oid,
            "Executive sign off decision for the security exception is waiting",
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            ontology_gap_ops=[
                OntologyGapOp(
                    source_model_id=blocker,
                    target_model_id=decision,
                    proposed_edge_kind="gated_by_decision",
                    description="Progress depends on an explicit approval decision.",
                    relationship_summary=(
                        "Beacon launch cannot progress until executive sign off happens."
                    ),
                    parent_kind="blocks",
                    nearest_existing_kind="blocks",
                    directionality="directed",
                    dropped_dimensions=[
                        "authority surface",
                        "approval state",
                    ],
                    evidence_event_ids=[oid],
                    confidence=0.8,
                    impact=0.9,
                    actionability=0.8,
                    authority_required=0.9,
                )
            ],
        )
        async with conn.transaction():
            apply_result = await apply_diff(diff, conn, "T1", oid)

        assert len(apply_result["ontology_gap_ops"]) == 1
        candidate_id = apply_result["ontology_gap_ops"][0][
            "relationship_candidate_id"
        ]
        row = await conn.fetchrow(
            """
            SELECT candidate_kind, basis, member_model_ids, proposed_proposition,
                   metadata, source
            FROM relationship_candidates
            WHERE id = $1
              AND tenant_id = $2
            """,
            UUID(candidate_id),
            tenant,
        )
        assert row is not None
        proposed = (
            json.loads(row["proposed_proposition"])
            if isinstance(row["proposed_proposition"], str)
            else row["proposed_proposition"]
        )
        metadata = (
            json.loads(row["metadata"])
            if isinstance(row["metadata"], str)
            else row["metadata"]
        )
        assert row["candidate_kind"] == "edge_type"
        assert row["basis"] == "ontology_gap"
        assert row["member_model_ids"] == [blocker, decision]
        assert proposed["proposed_edge_kind"] == "gated_by_decision"
        assert metadata["ontology_gap"]["retrieval_fallback_kind"] == (
            "blocks"
        )
        assert row["source"] == "think_ontology_gap_op"

        evidence_row = await conn.fetchrow(
            """
            SELECT predicate, edge_kind_hint, direction, extraction_method,
                   source_model_id, target_model_id, metadata
            FROM relation_evidence
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
            """,
            tenant,
            blocker,
            decision,
        )
        assert evidence_row is not None
        assert evidence_row["predicate"] == "gated_by_decision"
        assert evidence_row["edge_kind_hint"] == "gated_by_decision"
        assert evidence_row["direction"] == "source_to_target"
        assert evidence_row["extraction_method"] == "ontology_gap_op"
        evidence_metadata = (
            json.loads(evidence_row["metadata"])
            if isinstance(evidence_row["metadata"], str)
            else evidence_row["metadata"]
        )
        assert evidence_metadata["proposed_edge_kind"] == "gated_by_decision"

        pair_row = await conn.fetchrow(
            """
            SELECT explicit_relation_count, edge_kind_votes, direction_votes,
                   metadata
            FROM model_pair_evidence
            WHERE tenant_id = $1
              AND (
                (model_a_id = $2 AND model_b_id = $3)
                OR (model_a_id = $3 AND model_b_id = $2)
              )
            """,
            tenant,
            blocker,
            decision,
        )
        assert pair_row is not None
        assert pair_row["explicit_relation_count"] == 1
        edge_kind_votes = (
            json.loads(pair_row["edge_kind_votes"])
            if isinstance(pair_row["edge_kind_votes"], str)
            else pair_row["edge_kind_votes"]
        )
        direction_votes = (
            json.loads(pair_row["direction_votes"])
            if isinstance(pair_row["direction_votes"], str)
            else pair_row["direction_votes"]
        )
        pair_metadata = (
            json.loads(pair_row["metadata"])
            if isinstance(pair_row["metadata"], str)
            else pair_row["metadata"]
        )
        assert edge_kind_votes["gated_by_decision"] == 1
        assert sum(direction_votes.values()) == 1
        assert pair_metadata["ontology_gap"]["proposed_edge_kind"] == (
            "gated_by_decision"
        )

        result = await SynthesisReader().read(
            conn=conn,
            tenant_id=tenant,
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                observation_id=oid,
                seed_natural_text="What is blocking Beacon launch?",
                member_model_ids=[blocker],
                precomputed_seed_vector=[0.0] * 768,
            ),
            question_id="Q_DEPENDENCY",
            question="What is blocking Beacon launch?",
            question_primitive="DEPENDENCY",
            hypotheses=(),
        )

    assert decision in {model.id for model in result.models}
    trace = next(trace for trace in result.activations if trace.model_id == decision)
    assert any("propagated:blocks" in reason for reason in trace.activation_reasons)


async def test_apply_diverse_ontology_gap_matrix_persists_and_feeds_sage(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    from services.reasoning.think.tests.conftest import _insert_observation

    shapes = [
        {
            "kind": "gated_by_decision",
            "fallback": "blocks",
            "primitive": "DEPENDENCY",
            "question": "What is blocking Atlas launch?",
            "source": "Atlas launch is blocked by a pricing exception gate",
            "target": "Finance approval decision for the Atlas exception is waiting",
            "description": "Progress depends on a specific approval decision.",
            "summary": "Atlas launch cannot progress until finance approval happens.",
            "dropped": ["authority surface", "approval state"],
        },
        {
            "kind": "depends_on_assumption",
            "fallback": "supports",
            "primitive": "DEPENDENCY",
            "question": "Which assumptions does the Atlas plan depend on?",
            "source": "Atlas rollout plan assumes partner onboarding will finish by Friday",
            "target": "Partner onboarding completion by Friday is still unproven",
            "description": "The plan rests on an assumption that may later fail.",
            "summary": "The target assumption underpins whether the source plan remains valid.",
            "dropped": ["assumption dependency", "future fragility"],
        },
        {
            "kind": "transfers_risk_to",
            "fallback": "early_warning_for",
            "primitive": "CONSTRAINT",
            "question": "Where does the Atlas mitigation move risk?",
            "source": "Atlas support defers migration work to reduce release risk",
            "target": "Deferred migration creates renewal risk for enterprise accounts",
            "description": "One mitigation reduces local risk by moving it elsewhere.",
            "summary": "The source action creates an early warning on the target risk surface.",
            "dropped": ["risk recipient", "second order consequence"],
        },
        {
            "kind": "competes_for_priority_with",
            "fallback": "blocks",
            "primitive": "CONSTRAINT",
            "question": "Which work competes with the Atlas priority?",
            "source": "Atlas security review needs the same architecture review slot",
            "target": "Helios reliability review is already using the review slot",
            "description": "Two initiatives draw from the same finite decision capacity.",
            "summary": "The source can delay the target because both compete for priority.",
            "dropped": ["shared priority budget", "capacity conflict"],
        },
        {
            "kind": "accountable_for",
            "fallback": "explains",
            "primitive": "OWNERSHIP",
            "question": "Who is accountable for the Atlas handoff?",
            "source": "Atlas customer handoff outcome lacks an accountable owner",
            "target": "Mira owns the Atlas customer handoff outcome this week",
            "description": "One model names the owner accountable for another outcome.",
            "summary": "The target explains who is accountable for the source outcome.",
            "dropped": ["ownership", "accountability surface"],
        },
        {
            "kind": "proxy_for",
            "fallback": "predicts",
            "primitive": "PATTERN",
            "question": "Which signal is a proxy for Atlas customer confidence?",
            "source": "Atlas weekly admin-login drop is a measurable confidence signal",
            "target": "Atlas customer confidence is weakening before renewal",
            "description": "A measurable signal stands in for a harder-to-measure state.",
            "summary": "Movement in the source signal predicts the target latent state.",
            "dropped": ["latent variable", "measurement proxy"],
        },
    ]

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="Atlas ontology-gap matrix evidence.",
            external_id=f"ontology-gap-matrix-{uuid7()}",
        )
        pairs = []
        for shape in shapes:
            source = await _insert_applier_model(
                conn,
                tenant,
                oid,
                shape["source"],
            )
            target = await _insert_applier_model(
                conn,
                tenant,
                oid,
                shape["target"],
            )
            pairs.append((source, target))

        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            ontology_gap_ops=[
                OntologyGapOp(
                    source_model_id=source,
                    target_model_id=target,
                    proposed_edge_kind=shape["kind"],
                    description=shape["description"],
                    relationship_summary=shape["summary"],
                    parent_kind=shape["fallback"],
                    nearest_existing_kind=shape["fallback"],
                    directionality="directed",
                    dropped_dimensions=shape["dropped"],
                    evidence_event_ids=[oid],
                    confidence=0.78,
                    impact=0.86,
                    actionability=0.73,
                    urgency=0.64,
                    uncertainty=0.58,
                    authority_required=0.42,
                    novelty=0.92,
                )
                for shape, (source, target) in zip(shapes, pairs, strict=True)
            ],
        )
        async with conn.transaction():
            apply_result = await apply_diff(diff, conn, "T1", oid)

        assert len(apply_result["ontology_gap_ops"]) == len(shapes)
        rows = await conn.fetch(
            """
            SELECT id, candidate_kind, basis, member_model_ids,
                   proposed_proposition, metadata, source
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            """,
            tenant,
            [
                UUID(summary["relationship_candidate_id"])
                for summary in apply_result["ontology_gap_ops"]
            ],
        )
        rows_by_kind = {}
        for row in rows:
            proposed = (
                json.loads(row["proposed_proposition"])
                if isinstance(row["proposed_proposition"], str)
                else row["proposed_proposition"]
            )
            rows_by_kind[proposed["proposed_edge_kind"]] = row

        for shape, (source, target) in zip(shapes, pairs, strict=True):
            row = rows_by_kind[shape["kind"]]
            proposed = (
                json.loads(row["proposed_proposition"])
                if isinstance(row["proposed_proposition"], str)
                else row["proposed_proposition"]
            )
            metadata = (
                json.loads(row["metadata"])
                if isinstance(row["metadata"], str)
                else row["metadata"]
            )
            assert row["candidate_kind"] == "edge_type"
            assert row["basis"] == "ontology_gap"
            assert row["source"] == "think_ontology_gap_op"
            assert row["member_model_ids"] == [source, target]
            assert proposed["proposed_edge_kind"] == shape["kind"]
            assert proposed["parent_kind"] == shape["fallback"]
            assert metadata["ontology_gap"]["retrieval_fallback_kind"] == (
                shape["fallback"]
            )

            result = await SynthesisReader().read(
                conn=conn,
                tenant_id=tenant,
                trigger=TriggerContext(
                    kind="T1",
                    tenant_id=tenant,
                    observation_id=oid,
                    seed_natural_text=shape["question"],
                    member_model_ids=[source],
                    precomputed_seed_vector=[0.0] * 768,
                ),
                question_id=f"Q_{shape['kind'].upper()}",
                question=shape["question"],
                question_primitive=shape["primitive"],
                hypotheses=(),
            )
            assert target in {model.id for model in result.models}
            trace = next(
                trace for trace in result.activations if trace.model_id == target
            )
            assert trace.selected is True
            assert any(
                f"propagated:{shape['fallback']}" in reason
                for reason in trace.activation_reasons
            )


async def test_apply_edge_cycle_is_dropped_not_transaction_fatal(
    fresh_db, tenant, tenant_cleanup,
):
    from services.domain.models.edges_repo import EdgesRepo
    from services.reasoning.think.tests.conftest import _insert_observation

    async with fresh_db.acquire() as conn:
        oid = await _insert_observation(
            conn,
            tenant,
            content_text="A supports B, but reverse support is invalid",
            external_id=f"edge-cycle-drop-{uuid7()}",
        )
        a = await _insert_applier_model(conn, tenant, oid, "A")
        b = await _insert_applier_model(conn, tenant, oid, "B")
        repo = EdgesRepo()
        async with conn.transaction():
            await repo.link(
                conn,
                source=a,
                target=b,
                kind="supports",
                tenant_id=tenant,
                detected_by="think_edge_op",
                confidence=0.8,
            )

        trigger_ref = uuid7()
        diff = ValidatedDiff(
            trigger_ref=trigger_ref,
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=b,
                    target_model_id=a,
                    edge_kind="supports",
                    confidence=0.8,
                )
            ],
        )
        async with conn.transaction():
            result = await apply_diff(diff, conn, "T1", oid)

        outcome = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            trigger_ref,
        )
        edge_count = await conn.fetchval(
            "SELECT COUNT(*) FROM model_edges WHERE tenant_id = $1",
            tenant,
        )

    assert outcome == "success"
    assert edge_count == 1
    assert result["apply_dropped_op_count"] == 1
    assert result["edge_ops"][0]["op"] == "skip"
    assert result["edge_ops"][0]["reason"] == "cycle_prevention"


# ---------------------------------------------------------------------
# _coerce_update_value — LLM-provided update values reach asyncpg typed.
# ---------------------------------------------------------------------


async def test_coerce_update_value_parses_iso_timestamp_strings() -> None:
    from datetime import datetime, timezone

    from services.reasoning.think.applier import _coerce_update_value

    got = _coerce_update_value("resolved_at", "2026-06-11T05:20:02.460042+00:00")
    assert got == datetime(2026, 6, 11, 5, 20, 2, 460042, tzinfo=timezone.utc)
    # Zulu suffix and existing datetimes also pass through correctly.
    got_z = _coerce_update_value("last_confirmed_at", "2026-06-11T05:20:02Z")
    assert got_z.tzinfo is not None
    assert _coerce_update_value("resolved_at", got) is got
    assert _coerce_update_value("resolved_at", None) is None


async def test_coerce_update_value_coerces_bools_ints_floats() -> None:
    from services.reasoning.think.applier import _coerce_update_value

    assert _coerce_update_value("resolution_outcome", "true") is True
    assert _coerce_update_value("reading_contestable", False) is False
    assert _coerce_update_value("confirmed_count", "3") == 3
    assert _coerce_update_value("evidential_weight", "0.7") == 0.7


async def test_coerce_update_value_rejects_garbage_with_contract_error() -> None:
    from lib.shared.errors import ValidationError as ContractError

    from services.reasoning.think.applier import _coerce_update_value

    with pytest.raises(ContractError):
        _coerce_update_value("resolved_at", "yesterday-ish")
    with pytest.raises(ContractError):
        _coerce_update_value("resolution_outcome", "maybe")
    with pytest.raises(ContractError):
        _coerce_update_value("confirmed_count", "many")
