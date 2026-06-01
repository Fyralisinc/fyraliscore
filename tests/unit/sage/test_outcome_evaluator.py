"""tests/unit/sage/test_outcome_evaluator.py — Phase 13 Outcome Evaluator.

Direct tests for `services.sage.outcome_evaluator.OutcomeEvaluator`.
The evaluator only writes to `inquiry_outcome_events`; everything else
is read-only. Tests seed real rows in `inquiry_sessions`,
`inquiry_evidence_items`, `think_runs`, `applied_triggers`, and
`model_edges`, then assert the typed events emitted match the spec
§15.1 vocabulary and the §17.1 reward features are well-formed.

Re-uses the gateway integration fixtures (per-test pool + fresh DB)
exactly like `tests/unit/sage/test_inquiry_traces_repo.py`.

Idempotency contract (documented here so future readers don't have to
spelunk the impl): a second `evaluate()` call appends ZERO new events
because the evaluator dedupes against
`(event_type, key-from-payload)` before append. The reward features
are recomputed fresh, so they may differ between calls if upstream
state changes between calls — but on a frozen session they match.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.sage.outcome_evaluator import (
    InquiryOutcomeSummary,
    OutcomeEvaluator,
)
from services.sage.inquiry_traces.repo import OutcomeEventsRepo

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------


async def _seed_session(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    context_packet: dict | None = None,
    think_run_id: UUID | None = None,
) -> UUID:
    session_id = uuid7()
    packet = context_packet or {}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO inquiry_sessions (
              id, tenant_id, signal_ref_type, signal_ref_id,
              route, status, stop_status, context_packet, think_run_id
            ) VALUES (
              $1, $2, 'internal', NULL,
              'DEEP_INQUIRY_PATH', 'completed', 'sufficient_for_reasoning',
              $3::jsonb, $4
            )
            """,
            session_id,
            tenant_id,
            json.dumps(packet, default=str),
            think_run_id,
        )
    return session_id


async def _seed_evidence(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    session_id: UUID,
    source_ref: str,
    source_type: str = "observation",
    contradicts: list[str] | None = None,
    weakens: list[str] | None = None,
) -> UUID:
    eid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO inquiry_evidence_items (
              id, session_id, tenant_id, source_type, source_ref,
              summary, token_estimate,
              contradicts_hypotheses, weakens_hypotheses
            ) VALUES (
              $1, $2, $3, $4, $5,
              'seeded', 1,
              $6::jsonb, $7::jsonb
            )
            """,
            eid,
            session_id,
            tenant_id,
            source_type,
            source_ref,
            json.dumps(contradicts or []),
            json.dumps(weakens or []),
        )
    return eid


async def _seed_think_run(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    status: str = "success",
    error: str | None = None,
    ops_applied: dict | None = None,
) -> UUID:
    run_id = uuid7()
    trigger_id = uuid7()
    async with pool.acquire() as conn:
        # applied_triggers row first so the FK-less link is visible.
        await conn.execute(
            """
            INSERT INTO applied_triggers (
              trigger_id, tenant_id, diff_hash, trigger_kind, outcome
            ) VALUES ($1, $2, 'h', 'T1', 'success')
            """,
            trigger_id,
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO think_runs (
              id, tenant_id, trigger_id, trigger_kind,
              status, error, ops_applied
            ) VALUES (
              $1, $2, $3, 'T1',
              $4, $5, $6::jsonb
            )
            """,
            run_id,
            tenant_id,
            trigger_id,
            status,
            error,
            json.dumps(ops_applied or {}, default=str),
        )
    return run_id


async def _seed_edge(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str = "supports",
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO model_edges (
              id, tenant_id, source_model_id, target_model_id,
              edge_kind, status, detected_by
            ) VALUES (
              $1, $2, $3, $4, $5, 'active', 'test_seed'
            )
            """,
            uuid7(),
            tenant_id,
            source_model_id,
            target_model_id,
            edge_kind,
        )


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emits_used_in_packet_for_items_in_context_packet(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Evidence whose source_ref is referenced inside `context_packet`
    must produce a `retrieved_evidence_used_in_packet` event."""
    packet = {
        "tiers": {
            "supporting_evidence_groups": [
                {"source_ref": "obs:included-1"},
                {"source_ref": "obs:included-2"},
            ],
        },
        "budget": {"estimated_tokens_used": 300},
    }
    session_id = await _seed_session(
        gateway_pool, tenant_id=tenant_id, context_packet=packet,
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:included-1",
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:included-2",
    )

    evaluator = OutcomeEvaluator(pool=gateway_pool, tenant_id=tenant_id)
    summary = await evaluator.evaluate(inquiry_session_id=session_id)

    assert isinstance(summary, InquiryOutcomeSummary)
    assert summary.events_by_type.get("retrieved_evidence_used_in_packet") == 2
    assert summary.events_by_type.get("retrieved_evidence_omitted", 0) == 0


@pytest.mark.asyncio
async def test_emits_omitted_for_items_not_in_packet(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Evidence retrieved but absent from the packet emits
    `retrieved_evidence_omitted`."""
    packet = {"tiers": {"supporting_evidence_groups": [
        {"source_ref": "obs:kept"},
    ]}}
    session_id = await _seed_session(
        gateway_pool, tenant_id=tenant_id, context_packet=packet,
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:kept",
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:dropped-A",
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:dropped-B",
    )

    evaluator = OutcomeEvaluator(pool=gateway_pool, tenant_id=tenant_id)
    summary = await evaluator.evaluate(inquiry_session_id=session_id)

    assert summary.events_by_type.get("retrieved_evidence_used_in_packet") == 1
    assert summary.events_by_type.get("retrieved_evidence_omitted") == 2
    # Coverage = used / retrieved = 1/3.
    assert summary.reward_features["evidence_coverage"] == pytest.approx(1 / 3)


@pytest.mark.asyncio
async def test_emits_node_used_in_valid_diff_for_each_diff_model_id(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A successful think_run with claim_ops + edge_ops should emit
    one `node_used_in_valid_diff` per distinct model_id and one
    `path_used_in_valid_diff` per traversed active edge."""
    model_a = uuid7()
    model_b = uuid7()
    ops_applied = {
        "claim_ops": [{"op": "update", "model_id": str(model_a)}],
        "edge_ops": [{
            "op": "add",
            "source_model_id": str(model_a),
            "target_model_id": str(model_b),
            "edge_kind": "supports",
        }],
        "act_ops": [],
        "resource_ops": [],
    }
    run_id = await _seed_think_run(
        gateway_pool, tenant_id=tenant_id,
        status="success", ops_applied=ops_applied,
    )
    await _seed_edge(
        gateway_pool, tenant_id=tenant_id,
        source_model_id=model_a, target_model_id=model_b,
        edge_kind="supports",
    )
    session_id = await _seed_session(
        gateway_pool,
        tenant_id=tenant_id,
        context_packet={
            "source_metadata": {"trigger_kind": "T1"},
            "resolved_entities": [
                {"type": "customer", "id": "Acme"},
                {"type": "system", "id": "SSO"},
            ],
            "question_path": [
                {"question_id": "Q_DEPENDENCY", "primitive": "DEPENDENCY"}
            ],
        },
        think_run_id=run_id,
    )

    evaluator = OutcomeEvaluator(pool=gateway_pool, tenant_id=tenant_id)
    summary = await evaluator.evaluate(inquiry_session_id=session_id)

    assert summary.events_by_type.get("node_used_in_valid_diff") == 2
    assert summary.events_by_type.get("path_used_in_valid_diff") == 1
    assert set(summary.useful_node_ids) == {model_a, model_b}
    events = await OutcomeEventsRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    path_event = next(
        ev for ev in events if ev.event_type == "path_used_in_valid_diff"
    )
    assert path_event.payload["signature"] == {
        "signal_type": "T1",
        "entities": ["Acme", "SSO"],
        "question_primitive": "DEPENDENCY",
    }


@pytest.mark.asyncio
async def test_emits_missing_evidence_on_failed_run_with_missing_evidence_error(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A failed think_run whose error text says 'missing evidence' must
    emit `validation_failed_due_to_missing_evidence`."""
    run_id = await _seed_think_run(
        gateway_pool, tenant_id=tenant_id,
        status="failed",
        error="Validation rejected: missing evidence for claim_op[0]",
        ops_applied={},
    )
    session_id = await _seed_session(
        gateway_pool, tenant_id=tenant_id,
        context_packet={}, think_run_id=run_id,
    )

    evaluator = OutcomeEvaluator(pool=gateway_pool, tenant_id=tenant_id)
    summary = await evaluator.evaluate(inquiry_session_id=session_id)

    assert (
        summary.events_by_type.get("validation_failed_due_to_missing_evidence")
        == 1
    )
    assert summary.missing_anchor_signatures
    # Should NOT also fire bad-reference (different keyword set).
    assert (
        summary.events_by_type.get("validation_failed_due_to_bad_reference", 0)
        == 0
    )


@pytest.mark.asyncio
async def test_reward_features_contain_all_expected_keys_within_range(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Reward features must include the §17.1 keys and every value
    must sit in the [0.0, 2.0] band (the evaluator clamps before
    returning)."""
    packet = {
        "tiers": {"supporting_evidence_groups": [{"source_ref": "obs:1"}]},
        "budget": {"estimated_tokens_used": 9000},
    }
    run_id = await _seed_think_run(
        gateway_pool, tenant_id=tenant_id,
        status="success",
        ops_applied={
            "claim_ops": [{"op": "insert", "entry": {"id": str(uuid7())}}],
            "act_ops": [{"op": "create_goal", "entity": {}}],
            "edge_ops": [],
            "resource_ops": [],
            "dropped_op_count": 0,
        },
    )
    session_id = await _seed_session(
        gateway_pool, tenant_id=tenant_id,
        context_packet=packet, think_run_id=run_id,
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:1",
    )

    evaluator = OutcomeEvaluator(pool=gateway_pool, tenant_id=tenant_id)
    summary = await evaluator.evaluate(inquiry_session_id=session_id)

    expected_keys = {
        "evidence_coverage",
        "diff_deducibility",
        "compression_gain",
        "prediction_falsification_value",
        "action_value",
        "counterevidence_preservation",
        "graph_bloat",
        "redundancy",
        "noise_introduced",
        "token_cost",
        "permission_risk",
    }
    assert set(summary.reward_features.keys()) == expected_keys
    for k, v in summary.reward_features.items():
        # graph_bloat is signed (added - merged); the rest are clamped.
        if k == "graph_bloat":
            continue
        assert 0.0 <= v <= 2.0, f"{k}={v} outside [0, 2]"

    assert summary.reward_features["diff_deducibility"] == 1.0
    assert summary.reward_features["evidence_coverage"] == 1.0
    assert summary.reward_features["token_cost"] == pytest.approx(9000 / 30000)


@pytest.mark.asyncio
async def test_evaluate_is_idempotent_no_double_emit(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Documented contract: a second `evaluate()` for the same session
    appends ZERO new events because the evaluator dedupes against the
    already-stored events (per (event_type, payload-key) tuple).
    Per-type counts therefore stay constant across calls."""
    model_a = uuid7()
    ops_applied = {
        "claim_ops": [{"op": "update", "model_id": str(model_a)}],
        "edge_ops": [], "act_ops": [], "resource_ops": [],
    }
    run_id = await _seed_think_run(
        gateway_pool, tenant_id=tenant_id,
        status="success", ops_applied=ops_applied,
    )
    packet = {"tiers": {"supporting_evidence_groups": [
        {"source_ref": "obs:1"},
    ]}}
    session_id = await _seed_session(
        gateway_pool, tenant_id=tenant_id,
        context_packet=packet, think_run_id=run_id,
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:1",
    )
    await _seed_evidence(
        gateway_pool, tenant_id=tenant_id, session_id=session_id,
        source_ref="obs:dropped",
    )

    evaluator = OutcomeEvaluator(pool=gateway_pool, tenant_id=tenant_id)
    first = await evaluator.evaluate(inquiry_session_id=session_id)
    second = await evaluator.evaluate(inquiry_session_id=session_id)

    # First call emits; second emits zero new events.
    assert first.events_emitted > 0
    assert second.events_emitted == 0
    # Aggregate counts are identical across the two calls.
    assert first.events_by_type == second.events_by_type
    # Spot-check expected per-type counts on the second call.
    assert second.events_by_type.get("retrieved_evidence_used_in_packet") == 1
    assert second.events_by_type.get("retrieved_evidence_omitted") == 1
    assert second.events_by_type.get("node_used_in_valid_diff") == 1
