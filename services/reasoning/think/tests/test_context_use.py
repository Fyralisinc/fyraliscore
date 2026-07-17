from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow
from services.domain.models.edges_repo import EdgesRepo
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.config import CONFIG as RETRIEVAL_CONFIG
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding
from services.reasoning.think.context_planner import _retrieval_config_for_trigger
from services.reasoning.think.context_use import summarize_context_use
from services.reasoning.think.diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    MemoryLifecycleOp,
    RawDiff,
    RelationFrameOp,
    RelationFrameParticipantOp,
)
from services.reasoning.think.observability import METRICS
from services.reasoning.think.reason import think
from services.reasoning.think.tests.conftest import ScriptedProvider, _insert_observation


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


async def _insert_context_use_model(conn, tenant_id, observation_id, natural):
    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                '{}'::jsonb, 0.72, 1.0, 'active', 0.72, 1.0)
        """,
        mid,
        tenant_id,
        observation_id,
        json.dumps({"kind": "state", "subject": natural, "assertion": "true"}),
        natural,
        make_embedding(natural),
    )
    return mid


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


def test_t1_event_batch_uses_compact_historical_context_budget(monkeypatch):
    monkeypatch.delenv("THINK_BATCH_CONTEXT_MODEL_BUDGET", raising=False)
    monkeypatch.delenv("THINK_BATCH_CONTEXT_OBSERVATION_BUDGET", raising=False)
    monkeypatch.delenv("THINK_BATCH_CONTEXT_RAW_OBSERVATION_FLOOR", raising=False)
    monkeypatch.delenv("THINK_BATCH_HISTORICAL_OBSERVATION_CAP", raising=False)
    tenant_id = uuid4()
    first_obs = uuid4()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=first_obs,
        observation_ids=[first_obs, uuid4()],
        seed_signature={"batch": True},
    )

    cfg = _retrieval_config_for_trigger(trigger)

    assert cfg.assembler_budget_models == min(
        RETRIEVAL_CONFIG.assembler_budget_models,
        9,
    )
    assert cfg.historical_observation_cap == min(
        RETRIEVAL_CONFIG.historical_observation_cap,
        2,
    )
    assert cfg.assembler_budget_observations == max(
        RETRIEVAL_CONFIG.assembler_budget_observations,
        20,
    )
    assert cfg.t1_event_batch_raw_observation_floor == min(
        cfg.assembler_budget_observations,
        max(RETRIEVAL_CONFIG.t1_event_batch_raw_observation_floor, 20),
    )
    assert cfg.trigger_observation_cap == RETRIEVAL_CONFIG.trigger_observation_cap
    assert cfg.observation_context_mode == RETRIEVAL_CONFIG.observation_context_mode


def test_large_t1_event_batch_uses_configured_context_cap(monkeypatch):
    monkeypatch.delenv("THINK_BATCH_CONTEXT_MODEL_BUDGET", raising=False)
    monkeypatch.delenv("THINK_BATCH_CONTEXT_OBSERVATION_BUDGET", raising=False)
    monkeypatch.delenv("THINK_BATCH_CONTEXT_RAW_OBSERVATION_FLOOR", raising=False)
    tenant_id = uuid4()
    observation_ids = [uuid4() for _ in range(25)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observation_ids[0],
        observation_ids=observation_ids,
        seed_signature={"batch": True},
    )

    cfg = _retrieval_config_for_trigger(trigger)

    assert cfg.assembler_budget_models == min(
        RETRIEVAL_CONFIG.assembler_budget_models,
        20,
    )
    assert cfg.assembler_budget_observations == max(
        RETRIEVAL_CONFIG.assembler_budget_observations,
        20,
    )


def test_single_t1_keeps_default_context_budget(monkeypatch):
    monkeypatch.delenv("THINK_BATCH_CONTEXT_MODEL_BUDGET", raising=False)
    monkeypatch.delenv("THINK_BATCH_CONTEXT_OBSERVATION_BUDGET", raising=False)
    monkeypatch.delenv("THINK_BATCH_CONTEXT_RAW_OBSERVATION_FLOOR", raising=False)
    monkeypatch.delenv("THINK_BATCH_HISTORICAL_OBSERVATION_CAP", raising=False)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        observation_id=uuid4(),
    )

    assert _retrieval_config_for_trigger(trigger) is RETRIEVAL_CONFIG


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


def test_context_use_counts_relation_frames_as_graph_relation_work():
    tenant_id = uuid4()
    trigger_id = uuid4()
    blocker = uuid4()
    work = uuid4()
    owner = uuid4()
    risk = uuid4()
    resolution = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[blocker, work, owner, risk, resolution],
        graph_selected=[blocker, work],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        relation_frame_ops=[
            RelationFrameOp(
                relation_kind="blocked_workstream",
                participants=[
                    RelationFrameParticipantOp(
                        model_id=blocker,
                        role="blocker",
                        binding_confidence=0.9,
                    ),
                    RelationFrameParticipantOp(
                        model_id=work,
                        role="blocked_work",
                        binding_confidence=0.92,
                    ),
                    RelationFrameParticipantOp(
                        model_id=owner,
                        role="owner",
                        binding_confidence=0.78,
                    ),
                    RelationFrameParticipantOp(
                        model_id=risk,
                        role="downstream_risk",
                        binding_confidence=0.8,
                    ),
                    RelationFrameParticipantOp(
                        model_id=resolution,
                        role="possible_resolution",
                        binding_confidence=0.76,
                    ),
                ],
                participant_binding_status="bound",
                write_policy="project_edges",
                status="accepted",
                confidence=0.84,
                evidence_event_ids=[obs_id],
                evidence_model_ids=[blocker, work],
                explanation="A blocker, workstream, owner, risk, and resolution are linked.",
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["context_use_grade"] == "graph_context_used"
    assert report["relation_frame_ops_count"] == 1
    assert report["relation_frame_ops_between_selected_models"] == 1
    assert report["relation_frame_ops_touching_graph_models"] == 1
    assert report["graph_relation_op_count"] == 1
    assert report["graph_relation_contract_satisfied"] is True
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
    assert report["graph_selected_without_relation_ops"] is True
    assert report["graph_relation_contract_satisfied"] is False
    assert report["selected_observation_count"] == 2
    assert report["selected_observation_reference_count"] == 1
    assert report["referenced_observation_ids"] == [str(obs_id)]
    assert report["unused_selected_observation_ids"] == [str(unused_obs_id)]
    assert report["context_use_grade"] == "observation_context_used"


def test_context_use_counts_situation_member_models_as_graph_context():
    tenant_id = uuid4()
    trigger_id = uuid4()
    blocker = uuid4()
    risk = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[blocker, risk],
        graph_selected=[blocker, risk],
        observations=[_obs(obs_id)],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "situation",
                        "abstraction_level": "composite",
                        "situation": "Renewal pressure has a blocker and risk",
                        "summary": "The selected Models form one pressure.",
                        "member_model_ids": [str(blocker), str(risk)],
                        "evidence_model_ids": [str(blocker)],
                        "relationship_summary": "The blocker drives risk.",
                        "pressure_type": "revenue",
                    },
                    "natural": (
                        "Renewal pressure has a blocker and downstream risk."
                    ),
                    "confidence": 0.72,
                },
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["context_use_grade"] == "graph_context_used"
    assert report["selected_model_reference_count"] == 2
    assert report["graph_selected_reference_count"] == 2
    assert report["referenced_model_ids"] == sorted([str(blocker), str(risk)])
    assert report["graph_claim_op_reference_count"] == 1
    assert report["graph_non_relation_op_count"] == 1
    assert report["graph_relation_contract_satisfied"] is True
    assert report["graph_relation_contract_basis"] == "model_or_act_mutation"


def test_context_use_counts_claim_evidence_model_ids():
    tenant_id = uuid4()
    trigger_id = uuid4()
    selected_model = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[selected_model],
        observations=[_obs(obs_id)],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "fact",
                        "subject": "renewal",
                        "assertion": "The selected Model is confirmed.",
                    },
                    "evidence_model_ids": [str(selected_model)],
                    "natural": "The selected Model is confirmed.",
                    "confidence": 0.7,
                },
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["context_use_grade"] == "model_context_used"
    assert report["selected_model_reference_count"] == 1
    assert report["referenced_model_ids"] == [str(selected_model)]


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


def test_context_use_satisfies_graph_contract_for_graph_claim_update():
    tenant_id = uuid4()
    trigger_id = uuid4()
    graph_model = uuid4()
    other = uuid4()
    bundle = _bundle_with_selection(
        selected=[graph_model, other],
        graph_selected=[graph_model, other],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="update",
                model_id=graph_model,
                changes={"confidence": 0.82},
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["context_use_grade"] == "graph_context_used"
    assert report["graph_selected_without_relation_ops"] is True
    assert report["graph_relation_op_count"] == 0
    assert report["graph_non_relation_op_count"] == 1
    assert report["graph_claim_op_reference_count"] == 1
    assert report["graph_relation_contract_satisfied"] is True
    assert report["graph_relation_contract_basis"] == "model_or_act_mutation"


def test_context_use_satisfies_graph_contract_for_memory_lifecycle_update():
    tenant_id = uuid4()
    trigger_id = uuid4()
    graph_model = uuid4()
    other = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[graph_model, other],
        graph_selected=[graph_model, other],
        observations=[_obs(obs_id)],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        memory_lifecycle_ops=[
            MemoryLifecycleOp(
                model_id=graph_model,
                action="confirm",
                evidence_event_ids=[obs_id],
                rationale="The signal confirms this selected graph memory.",
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["context_use_grade"] == "graph_context_used"
    assert report["memory_lifecycle_ops_count"] == 1
    assert report["graph_non_relation_op_count"] == 1
    assert report["graph_memory_lifecycle_op_reference_count"] == 1
    assert report["graph_relation_contract_satisfied"] is True
    assert report["referenced_model_ids"] == [str(graph_model)]
    assert report["referenced_observation_ids"] == [str(obs_id)]


def test_context_use_satisfies_graph_contract_for_graph_act_basis():
    tenant_id = uuid4()
    trigger_id = uuid4()
    graph_model = uuid4()
    bundle = _bundle_with_selection(
        selected=[graph_model],
        graph_selected=[graph_model],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        act_ops=[
            ActOp(
                op="transition_commitment",
                confidence_basis=graph_model,
                entity={"id": str(uuid4()), "new_state": "blocked"},
            )
        ],
    )

    report = summarize_context_use(bundle, diff)

    assert report["graph_selected_without_relation_ops"] is True
    assert report["graph_non_relation_op_count"] == 1
    assert report["graph_act_op_reference_count"] == 1
    assert report["graph_relation_contract_satisfied"] is True
    assert report["graph_relation_contract_basis"] == "model_or_act_mutation"


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
    assert report["graph_trace_reference_count"] == 1
    assert report["graph_relation_contract_satisfied"] is True
    assert report["graph_relation_contract_basis"] == "noop_trace_accounted"


def test_context_use_accounts_for_selected_context_in_non_empty_diff():
    tenant_id = uuid4()
    trigger_id = uuid4()
    selected_model = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[selected_model],
        observations=[_obs(obs_id)],
    )
    bundle.notes["observation_selection"] = {
        "selected_trigger_count": 1,
        "selected_historical_count": 0,
        "historical_cap": 4,
        "raw_evidence_reopening": {
            "opened": True,
            "reason_codes": ["fresh_trigger_verification_sample"],
            "maturity": "mature",
        },
    }
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(uuid4()),
                    "natural": "A new fact from the trigger.",
                },
            )
        ],
        reasoning_trace=(
            f"Selected Model {selected_model} was checked but it describes "
            "a different customer and does not warrant an edge."
        ),
    )

    report = summarize_context_use(bundle, diff)

    assert report["context_use_grade"] == "model_context_used"
    assert report["selected_context_used"] is True
    assert report["selected_context_accounted_for"] is True
    assert report["model_context_used"] is True
    assert report["selected_model_reference_count"] == 1
    assert report["reasoning_trace_context_used"] is True
    assert report["reasoning_trace_context_accounted"] is True
    assert report["reasoning_trace_context_decision_used"] is True
    assert report["selected_trigger_observation_count"] == 1
    assert report["selected_historical_observation_count"] == 0
    assert report["raw_observation_reopening_reasons"] == [
        "fresh_trigger_verification_sample"
    ]
    assert report["raw_observation_reopening"]["maturity"] == "mature"


def test_context_use_accepts_explicit_no_edge_rationale_for_graph_context():
    tenant_id = uuid4()
    trigger_id = uuid4()
    graph_model = uuid4()
    obs_id = uuid4()
    bundle = _bundle_with_selection(
        selected=[graph_model],
        graph_selected=[graph_model],
        observations=[_obs(obs_id)],
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
        reasoning_trace=(
            f"no edge: selected graph Model {graph_model} describes a "
            "different mechanism, so the new fact should remain separate."
        ),
    )

    report = summarize_context_use(bundle, diff)

    assert report["graph_selected_without_relation_ops"] is True
    assert report["graph_no_edge_rationale_present"] is True
    assert report["graph_context_used"] is True
    assert report["model_context_used"] is True
    assert report["reasoning_trace_context_decision_used"] is True
    assert report["graph_selected_reference_count"] == 1
    assert report["graph_relation_contract_satisfied"] is True
    assert report["graph_relation_contract_basis"] == "no_edge_rationale"


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
    assert context_use["relation_claim_ops_touching_graph_models"] >= 1
    assert context_use["graph_relation_op_count"] >= 1
    assert context_use["graph_selected_reference_count"] >= 2
    assert str(bridge_id) in context_use["referenced_model_ids"]
    assert str(target_id) in context_use["referenced_model_ids"]
    metrics = METRICS.snapshot()
    assert metrics["context_use_grades_total"]["T3|graph_context_used"] == 1
    assert metrics["context_use_selected_ratios"]["T3"]


@pytest.mark.asyncio
async def test_think_applies_ontology_gap_op_end_to_end(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        obs_id = await _insert_observation(
            conn,
            tenant,
            content_text=(
                "Beacon launch is waiting on executive security exception approval."
            ),
            external_id=f"think-ontology-gap-{uuid7()}",
        )
        source_model = await _insert_context_use_model(
            conn,
            tenant,
            obs_id,
            "Beacon launch is blocked by security exception approval",
        )
        target_model = await _insert_context_use_model(
            conn,
            tenant,
            obs_id,
            "Executive sign off decision for Beacon security exception is waiting",
        )

    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T2",
        tenant_id=tenant,
        subkind="ontology_gap_probe",
        model_id=source_model,
        member_model_ids=[target_model],
        seed_signature={"trigger_id": str(trigger_id)},
        seed_natural_text="Beacon launch decision-gate ontology gap",
        precomputed_seed_vector=make_embedding(
            "Beacon launch decision-gate ontology gap"
        ),
    )
    response = json.dumps(
        {
            "trigger_ref": str(trigger_id),
            "tenant_id": str(tenant),
            "claim_ops": [],
            "edge_ops": [],
            "ontology_gap_ops": [
                {
                    "op": "propose_edge_type",
                    "source_model_id": str(source_model),
                    "target_model_id": str(target_model),
                    "proposed_edge_kind": "gated_by_decision",
                    "description": (
                        "Progress depends on an explicit approval decision."
                    ),
                    "relationship_summary": (
                        "Beacon launch cannot progress until executive sign off happens."
                    ),
                    "parent_kind": "blocks",
                    "nearest_existing_kind": "blocks",
                    "directionality": "directed",
                    "dropped_dimensions": [
                        "authority surface",
                        "approval state",
                    ],
                    "evidence_event_ids": [str(obs_id)],
                    "evidence_model_ids": [],
                    "confidence": 0.8,
                    "impact": 0.9,
                    "actionability": 0.8,
                    "urgency": 0.62,
                    "uncertainty": 0.48,
                    "authority_required": 0.9,
                    "novelty": 0.95,
                }
            ],
            "act_ops": [],
            "resource_ops": [],
            "new_predictions": [],
            "reasoning_trace": (
                f"Models {source_model} and {target_model} need a decision-gate "
                "relationship that the registered edge ontology cannot express."
            ),
        }
    )
    provider = ScriptedProvider(responses=[response])

    outcome = await think(
        trigger,
        fresh_db,
        llm_provider=provider,
        triggering_content="Evaluate the decision-gate relationship.",
        reason_for_trigger="ontology-gap write path regression",
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
        candidate = await conn.fetchrow(
            """
            SELECT id, candidate_kind, basis, member_model_ids,
                   evidence_model_ids, evidence_event_ids,
                   proposed_proposition, metadata, source, review_status
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND candidate_kind = 'edge_type'
              AND source = 'think_ontology_gap_op'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
        )
        proposal = await conn.fetchrow(
            """
            SELECT proposed_edge_kind, status, example_count, example_candidate_ids
            FROM relationship_ontology_proposals
            WHERE tenant_id = $1
              AND proposed_edge_kind = 'gated_by_decision'
            """,
            tenant,
        )
        validation_artifact = await conn.fetchrow(
            """
            SELECT payload
            FROM think_run_artifacts
            WHERE run_id = $1
              AND tenant_id = $2
              AND stage = 'validation'
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            outcome.run_id,
            tenant,
        )

    ops_applied = run_row["ops_applied"]
    if isinstance(ops_applied, str):
        ops_applied = json.loads(ops_applied)
    assert len(ops_applied["ontology_gap_ops"]) == 1
    gap_summary = ops_applied["ontology_gap_ops"][0]
    assert gap_summary["proposed_edge_kind"] == "gated_by_decision"
    assert gap_summary["retrieval_fallback_kind"] == "blocks"
    assert gap_summary["ontology_proposals_upserted"] >= 1
    context_use = ops_applied["context_use"]
    assert context_use["context_use_grade"] == "graph_context_used"
    assert context_use["ontology_gap_ops_count"] == 1
    assert context_use["ontology_gap_ops_touching_graph_models"] == 1
    assert context_use["graph_relation_contract_satisfied"] is True
    assert context_use["graph_relation_contract_basis"] == "relation_op"

    assert candidate is not None
    proposed = candidate["proposed_proposition"]
    metadata = candidate["metadata"]
    if isinstance(proposed, str):
        proposed = json.loads(proposed)
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert candidate["candidate_kind"] == "edge_type"
    assert candidate["basis"] == "ontology_gap"
    assert candidate["member_model_ids"] == [source_model, target_model]
    assert candidate["evidence_model_ids"] == [source_model, target_model]
    assert obs_id in candidate["evidence_event_ids"]
    assert proposed["proposed_edge_kind"] == "gated_by_decision"
    assert proposed["dropped_dimensions"] == [
        "authority surface",
        "approval state",
    ]
    assert metadata["ontology_gap"]["retrieval_fallback_kind"] == "blocks"
    assert metadata["think"]["cause_event_id"] is None
    assert candidate["source"] == "think_ontology_gap_op"
    assert candidate["review_status"] == "needs_review"

    assert proposal is not None
    assert proposal["status"] == "draft"
    assert proposal["example_count"] == 1
    assert candidate["id"] in proposal["example_candidate_ids"]

    artifact_payload = validation_artifact["payload"]
    if isinstance(artifact_payload, str):
        artifact_payload = json.loads(artifact_payload)
    assert len(artifact_payload["ontology_gap_ops"]) == 1


@pytest.mark.asyncio
async def test_think_archives_stale_model_end_to_end(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        obs_id = await _insert_observation(
            conn,
            tenant,
            content_text="Beacon launch blocker is stale after the decision closed.",
            external_id=f"think-archive-stale-{uuid7()}",
        )
        stale_model = await _insert_context_use_model(
            conn,
            tenant,
            obs_id,
            "Beacon launch is blocked by an old security exception decision",
        )
        await conn.execute(
            """
            UPDATE models
            SET activation = 0.01,
                last_retrieved_at = now() - interval '60 days'
            WHERE id = $1
            """,
            stale_model,
        )

    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T2",
        tenant_id=tenant,
        subkind="stale_memory_cleanup",
        model_id=stale_model,
        seed_signature={"trigger_id": str(trigger_id)},
        seed_natural_text="Stale low-activation blocker cleanup",
        precomputed_seed_vector=make_embedding(
            "Stale low-activation blocker cleanup"
        ),
    )
    response = json.dumps(
        {
            "trigger_ref": str(trigger_id),
            "tenant_id": str(tenant),
            "claim_ops": [
                {
                    "op": "archive",
                    "model_id": str(stale_model),
                    "reason": "decay",
                }
            ],
            "edge_ops": [],
            "ontology_gap_ops": [],
            "act_ops": [],
            "resource_ops": [],
            "new_predictions": [],
            "reasoning_trace": (
                f"Model {stale_model} is stale, low activation, and no longer "
                "describes the current blocker state, so archival is correct."
            ),
        }
    )
    provider = ScriptedProvider(responses=[response])

    outcome = await think(
        trigger,
        fresh_db,
        llm_provider=provider,
        triggering_content="Archive stale memory if warranted.",
        reason_for_trigger="model archival/staleness regression",
    )

    assert outcome.status == "success", outcome.error
    async with fresh_db.acquire() as conn:
        model_row = await conn.fetchrow(
            """
            SELECT status, archive_reason, archived_at
            FROM models
            WHERE id = $1
              AND tenant_id = $2
            """,
            stale_model,
            tenant,
        )
        state_change = await conn.fetchrow(
            """
            SELECT content, source_channel, trust_tier
            FROM observations
            WHERE tenant_id = $1
              AND kind = 'state_change'
              AND content->>'state_change_kind' = 'archive_model'
              AND content->>'entity_id' = $2
            ORDER BY occurred_at DESC
            LIMIT 1
            """,
            tenant,
            str(stale_model),
        )
        run_row = await conn.fetchrow(
            """
            SELECT ops_applied
            FROM think_runs
            WHERE id = $1
            """,
            outcome.run_id,
        )

    assert model_row["status"] == "archived"
    assert model_row["archive_reason"] == "decay"
    assert model_row["archived_at"] is not None
    assert state_change is not None
    assert state_change["source_channel"] == "internal:state_change"
    assert state_change["trust_tier"] == "authoritative"
    content = state_change["content"]
    if isinstance(content, str):
        content = json.loads(content)
    assert content["metadata"]["archive_reason"] == "decay"

    ops_applied = run_row["ops_applied"]
    if isinstance(ops_applied, str):
        ops_applied = json.loads(ops_applied)
    assert ops_applied["claim_ops"] == [
        {
            "op": "archive",
            "model_id": str(stale_model),
            "reason": "decay",
        }
    ]
    context_use = ops_applied["context_use"]
    assert context_use["context_use_grade"] == "graph_context_used"
    assert context_use["graph_non_relation_op_count"] == 1
    assert context_use["graph_relation_contract_satisfied"] is True
    assert context_use["graph_relation_contract_basis"] == "model_or_act_mutation"


@pytest.mark.asyncio
async def test_think_attaches_low_durability_signal_as_evidence_end_to_end(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    customer_id = uuid7()
    scope_entity = {"type": "customer", "id": str(customer_id)}
    anchor_natural = "Acme renewal call felt rough after the customer review."
    signal_natural = "Yesterday's call with Acme felt rough."
    async with fresh_db.acquire() as conn:
        old_event = await _insert_observation(
            conn,
            tenant,
            content_text=anchor_natural,
            entities_mentioned=[scope_entity],
            external_id=f"think-evidence-anchor-{uuid7()}",
        )
        new_event = await _insert_observation(
            conn,
            tenant,
            content_text=signal_natural,
            entities_mentioned=[scope_entity],
            external_id=f"think-evidence-signal-{uuid7()}",
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
            make_embedding(anchor_natural),
            json.dumps([scope_entity]),
        )

    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=new_event,
        seed_signature={"trigger_id": str(trigger_id)},
        seed_natural_text=signal_natural,
        precomputed_seed_vector=make_embedding(signal_natural),
    )
    question_plan_response = json.dumps(
        {
            "rationale": "No extra inquiry is needed for this repeated signal.",
            "belief_deltas": [],
            "questions": [],
        }
    )
    response = json.dumps(
        {
            "trigger_ref": str(trigger_id),
            "tenant_id": str(tenant),
            "claim_ops": [
                {
                    "op": "insert",
                    "entry": {
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
                        "falsifier": None,
                    },
                }
            ],
            "edge_ops": [],
            "ontology_gap_ops": [],
            "act_ops": [],
            "resource_ops": [],
            "new_predictions": [],
            "reasoning_trace": (
                f"Observation {new_event} repeats the low-durability call-feel "
                f"signal already anchored by Model {anchor_model}."
            ),
        }
    )
    provider = ScriptedProvider(responses=[question_plan_response, response])

    outcome = await think(
        trigger,
        fresh_db,
        llm_provider=provider,
        triggering_content=signal_natural,
        reason_for_trigger="evidence attachment regression",
    )

    assert outcome.status == "success", outcome.error
    async with fresh_db.acquire() as conn:
        model_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM models
            WHERE tenant_id = $1
            """,
            tenant,
        )
        anchor_row = await conn.fetchrow(
            """
            SELECT signal_readings, supporting_event_ids, evidential_weight
            FROM models
            WHERE id = $1
              AND tenant_id = $2
            """,
            anchor_model,
            tenant,
        )
        sidecar_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_signal_readings
            WHERE model_id = $1
              AND tenant_id = $2
              AND source_event_id = $3
            """,
            anchor_model,
            tenant,
            new_event,
        )
        run_row = await conn.fetchrow(
            """
            SELECT ops_applied
            FROM think_runs
            WHERE id = $1
            """,
            outcome.run_id,
        )

    assert model_count == 1
    readings = anchor_row["signal_readings"]
    if isinstance(readings, str):
        readings = json.loads(readings)
    assert readings[-1]["kind"] == "observe"
    assert readings[-1]["source_event_id"] == str(new_event)
    assert new_event in anchor_row["supporting_event_ids"]
    assert float(anchor_row["evidential_weight"]) > 0.5
    assert sidecar_count == 1

    ops_applied = run_row["ops_applied"]
    if isinstance(ops_applied, str):
        ops_applied = json.loads(ops_applied)
    assert ops_applied["quality_summary"]["downgrade_to_evidence"] == 1
    assert ops_applied["memory_aggregation"]["evidence_attachments"] == 1
    assert ops_applied["memory_aggregation"]["model_inserts"] == 0
    assert ops_applied["claim_ops"][0]["op"] == "downgrade_to_evidence"
    assert ops_applied["claim_ops"][0]["decision"] == "attached_to_existing_model"
    assert ops_applied["claim_ops"][0]["model_id"] == str(anchor_model)
