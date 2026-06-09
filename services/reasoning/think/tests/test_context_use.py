from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow
from services.domain.models.edges_repo import EdgesRepo
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding
from services.reasoning.think.context_use import summarize_context_use
from services.reasoning.think.diff_schema import ActOp, ClaimOp, EdgeOp, RawDiff
from services.reasoning.think.observability import METRICS
from services.reasoning.think.reason import think
from services.reasoning.think.tests.conftest import ScriptedProvider


pytestmark = pytest.mark.integration


def _obs(obs_id):
    now = datetime.now(timezone.utc)
    return ObservationRow(
        id=obs_id,
        tenant_id=uuid4(),
        occurred_at=now,
        ingested_at=now,
        kind="signal",
        source_channel="test",
        source_actor_ref=None,
        actor_id=None,
        content={"text": "context evidence"},
        content_text="context evidence",
        embedding=None,
        embedding_pending=False,
        trust_tier="derived",
        external_id=None,
        cause_id=None,
        sequence_num=1,
        entities_mentioned=[],
    )


def _bundle_with_selection(
    *,
    selected,
    graph_selected=(),
    observations=(),
) -> ContextBundle:
    return ContextBundle(
        observations=list(observations),
        notes={
            "model_selection": {
                "selected_model_ids": [str(mid) for mid in selected],
                "pathway_survival": {
                    "G": {
                        "selected_model_ids": [
                            str(mid) for mid in graph_selected
                        ]
                    }
                },
            }
        }
    )


def test_context_use_counts_edge_ops_between_selected_graph_models():
    tenant_id = uuid4()
    trigger_id = uuid4()
    a = uuid4()
    b = uuid4()
    c = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(selected=[a, b, c], graph_selected=[a, b])
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        edge_ops=[
            EdgeOp(
                op="add",
                source_model_id=a,
                target_model_id=b,
                edge_kind="early_warning_for",
                confidence=0.86,
                evidence_event_ids=[obs_id],
                evidence_model_ids=[c],
                explanation="The selected graph models form an early warning.",
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["selected_model_reference_count"] == 3
    assert report["selected_model_reference_ratio"] == 1.0
    assert report["graph_selected_reference_count"] == 2
    assert report["graph_selected_reference_ratio"] == 1.0
    assert report["edge_ops_between_selected_models"] == 1
    assert report["edge_ops_touching_graph_models"] == 1
    assert report["context_use_grade"] == "graph_context_used"
    assert report["selected_context_used"] is True
    assert report["referenced_observation_ids"] == [str(obs_id)]


def test_context_use_reports_unused_selected_models_for_claim_only_diff():
    tenant_id = uuid4()
    trigger_id = uuid4()
    a = uuid4()
    b = uuid4()
    obs_id = uuid4()
    unused_obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[a, b],
        graph_selected=[a, b],
        observations=[_obs(obs_id), _obs(unused_obs_id)],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "natural": "A new fact from the trigger.",
                },
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["selected_model_reference_count"] == 0
    assert report["selected_model_reference_ratio"] == 0.0
    assert report["graph_selected_reference_count"] == 0
    assert report["unused_selected_model_ids"] == sorted([str(a), str(b)])
    assert report["unused_graph_model_ids"] == sorted([str(a), str(b)])
    assert report["selected_observation_count"] == 2
    assert report["selected_observation_reference_count"] == 1
    assert report["referenced_observation_ids"] == [str(obs_id)]
    assert report["unused_selected_observation_ids"] == [str(unused_obs_id)]
    assert report["context_use_grade"] == "observation_context_used"


def test_context_use_counts_act_confidence_basis_as_model_reference():
    tenant_id = uuid4()
    trigger_id = uuid4()
    basis = uuid4()
    other = uuid4()
    bundle = _bundle_with_selection(selected=[basis, other])
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        act_ops=[
            ActOp(
                op="transition_commitment",
                confidence_basis=basis,
                entity={"id": str(uuid4()), "new_state": "blocked"},
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["selected_model_reference_count"] == 1
    assert report["referenced_model_ids"] == [str(basis)]
    assert report["unused_selected_model_ids"] == [str(other)]


def test_context_use_counts_noop_reasoning_trace_references():
    tenant_id = uuid4()
    trigger_id = uuid4()
    graph_model = uuid4()
    other = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[graph_model, other],
        graph_selected=[graph_model],
        observations=[_obs(obs_id)],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        reasoning_trace=(
            f"Model {graph_model} already captures this signal and "
            f"observation {obs_id} adds no new state transition."
        ),
    )

    report = summarize_context_use(bundle, diff)

    assert report["context_use_grade"] == "justified_noop_context_used"
    assert report["reasoning_trace_context_used"] is True
    assert report["selected_context_used"] is True
    assert report["graph_context_used"] is True
    assert report["selected_model_reference_count"] == 1
    assert report["selected_observation_reference_count"] == 1
    assert report["trace_referenced_model_ids"] == [str(graph_model)]
    assert report["trace_referenced_observation_ids"] == [str(obs_id)]
    assert report["unused_selected_model_ids"] == [str(other)]


@pytest.mark.asyncio
async def test_think_persists_context_use_and_applies_context_edge(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    METRICS.reset()
    async with fresh_db.acquire() as conn:
        fs = await build_fixture(
            conn,
            tenant,
            pool=fresh_db,
            rng_seed=777,
            n_actors=8,
            n_observations=80,
            n_models=90,
            n_commitments=18,
            n_goals=8,
            n_customers=4,
            n_decisions=4,
        )
        seed_id = fs.hero_model_id
        bridge_id = fs.model_ids[70]
        target_id = fs.model_ids[82]
        await EdgesRepo().link(
            conn,
            source=seed_id,
            target=bridge_id,
            kind="same_issue_as",
            tenant_id=tenant,
            detected_by="manual",
            confidence=0.9,
            explanation="Seed and bridge describe the same operating issue.",
        )
        await EdgesRepo().link(
            conn,
            source=bridge_id,
            target=target_id,
            kind="same_issue_as",
            tenant_id=tenant,
            detected_by="manual",
            confidence=0.86,
            explanation="Connecting evidence and target share the same hidden issue.",
        )

    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T3",
        tenant_id=tenant,
        subkind="anomaly",
        model_id=seed_id,
        seed_signature={"trigger_id": str(trigger_id)},
        seed_natural_text="context-use end-to-end hidden warning",
        precomputed_seed_vector=make_embedding(
            "context-use end-to-end hidden warning"
        ),
    )
    response = json.dumps(
        {
            "trigger_ref": str(trigger_id),
            "tenant_id": str(tenant),
            "claim_ops": [],
            "edge_ops": [
                {
                    "op": "add",
                    "source_model_id": str(bridge_id),
                    "target_model_id": str(target_id),
                    "edge_kind": "early_warning_for",
                    "weight": None,
                    "confidence": 0.87,
                    "evidence_event_ids": [],
                    "evidence_model_ids": [str(seed_id)],
                    "explanation": (
                        "The selected bridge Model is an early warning for "
                        "the selected target Model in this operating issue."
                    ),
                    "metadata": {},
                    "review_status": "accepted",
                    "reason": None,
                }
            ],
            "act_ops": [],
            "resource_ops": [],
            "new_predictions": [],
            "reasoning_trace": "scripted context-use edge from selected memory",
        }
    )
    provider = ScriptedProvider(responses=[response])

    outcome = await think(
        trigger,
        fresh_db,
        llm_provider=provider,
        triggering_content="Evaluate the selected graph context.",
        reason_for_trigger="context-use persistence regression",
    )

    assert outcome.status == "success", outcome.error
    async with fresh_db.acquire() as conn:
        run_row = await conn.fetchrow(
            """
            SELECT ops_applied
            FROM think_runs
            WHERE id = $1
            """,
            outcome.run_id,
        )
        edge_row = await conn.fetchrow(
            """
            SELECT edge_kind, detected_by, confidence, evidence_model_ids
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'early_warning_for'
              AND status = 'active'
            """,
            tenant,
            bridge_id,
            target_id,
        )

    assert edge_row is not None
    assert edge_row["detected_by"] == "think_edge_op"
    ops_applied = run_row["ops_applied"]
    if isinstance(ops_applied, str):
        ops_applied = json.loads(ops_applied)
    context_use = ops_applied["context_use"]
    assert context_use["context_use_grade"] == "graph_context_used"
    assert context_use["edge_ops_touching_graph_models"] >= 1
    assert context_use["graph_selected_reference_count"] >= 2
    assert str(bridge_id) in context_use["referenced_model_ids"]
    assert str(target_id) in context_use["referenced_model_ids"]
    metrics = METRICS.snapshot()
    assert metrics["context_use_grades_total"]["T3|graph_context_used"] == 1
    assert metrics["context_use_selected_ratios"]["T3"]
