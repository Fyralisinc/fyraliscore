"""Integration tests for the wired SAGE Synthesis Reader."""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import pytest

from services.platform.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.affordances.repo import AffordanceProfilesRepo
from services.reasoning.sage.affordances.types import RetrievalAffordanceProfile
from services.reasoning.sage.reader import SynthesisReader
from tests.unit.sage._seed import ZERO_EMBEDDING, seed_model, seed_observation


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_synthesis_reader_activates_affordance_model(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    obs_id = await seed_observation(
        gateway_pool,
        tenant_id=tenant_id,
        content_text="Acme launch notes: SSO remains on the critical path.",
    )
    model_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Security review capacity blocks enterprise login for Acme SSO",
        supporting_event_ids=[obs_id],
        signal_readings=[{"kind": "observe", "event_id": str(obs_id), "weight": 0.9}],
    )
    await AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id).upsert(
        RetrievalAffordanceProfile(
            model_id=model_id,
            tenant_id=tenant_id,
            answers_question_primitives=["DEPENDENCY"],
            supports_hypothesis_types=["delivery_risk"],
            action_affordances=["map.critical_path"],
            activation_signatures={"entities": ["Acme", "SSO"]},
            utility_score=1.2,
        )
    )

    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text="Acme SSO launch is blocked on the critical path.",
        seed_entity_ids=[
            {"type": "customer", "id": "Acme"},
            {"type": "system", "id": "SSO"},
        ],
        precomputed_seed_vector=ZERO_EMBEDDING,
    )
    async with gateway_pool.acquire() as conn:
        result = await SynthesisReader().read(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            question_id="Q_DEPENDENCY",
            question="Is SSO actually on Acme's critical path?",
            question_primitive="DEPENDENCY",
            hypotheses=(),
        )

    assert model_id in {m.id for m in result.models}
    trace = next(t for t in result.activations if t.model_id == model_id)
    assert trace.selected is True
    assert trace.activation_score > 0
    assert any("affordance:DEPENDENCY" in r for r in trace.activation_reasons)
    assert result.projected_evidence


@pytest.mark.asyncio
async def test_inquiry_persists_sage_reader_activation_traces(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    obs_id = await seed_observation(
        gateway_pool,
        tenant_id=tenant_id,
        content_text="Globex enterprise rollout is blocked by SSO dependency.",
    )
    model_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Globex SSO dependency blocks enterprise rollout",
        supporting_event_ids=[obs_id],
        signal_readings=[{"kind": "observe", "event_id": str(obs_id), "weight": 1.0}],
    )
    await AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id).upsert(
        RetrievalAffordanceProfile(
            model_id=model_id,
            tenant_id=tenant_id,
            answers_question_primitives=["DEPENDENCY", "COUNTEREVIDENCE"],
            supports_hypothesis_types=["delivery_risk"],
            action_affordances=["map.enterprise_rollout"],
            utility_score=0.9,
        )
    )

    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text="Globex rollout is blocked by SSO.",
        seed_entity_ids=[
            {"type": "customer", "id": "Globex"},
            {"type": "system", "id": "SSO"},
        ],
        seed_occurred_at=None,
        precomputed_seed_vector=ZERO_EMBEDDING,
    )
    async with gateway_pool.acquire() as conn:
        result = await run_inquiry_retrieval(
            trigger,
            conn,
            embedder=None,
            llm_provider=None,
            mode="deep",
            top_n=16,
            config=InquiryConfig(
                max_rounds=1,
                questions_per_round=3,
                llm_question_planning_enabled=False,
                sage_reader_enabled=True,
                persist=True,
                candidate_model_limit=32,
                result_model_limit=16,
            ),
        )
        rows = await conn.fetch(
            """
            SELECT question_id, model_id, activation_score,
                   activation_reasons, selected
            FROM sage_reader_activations
            WHERE inquiry_session_id = $1
              AND tenant_id = $2
            """,
            result.session_id,
            tenant_id,
        )

    assert rows
    assert model_id in {row["model_id"] for row in rows}
    selected_rows = [row for row in rows if row["model_id"] == model_id]
    assert any(row["selected"] for row in selected_rows)
    reasons = []
    for row in selected_rows:
        value = row["activation_reasons"]
        reasons.extend(json.loads(value) if isinstance(value, str) else value)
    assert any("affordance:" in reason for reason in reasons)
