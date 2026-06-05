"""Stress and edge-case coverage for the SAGE Synthesis Reader."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.platform.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.affordances.repo import AffordanceProfilesRepo
from services.reasoning.sage.affordances.types import RetrievalAffordanceProfile
from services.reasoning.sage.discovery.negative_memory_repo import NegativeMemoryRepo
from services.reasoning.sage.discovery.types import NegativeMemory
from services.reasoning.sage.reader import ReaderBudget, SynthesisReader
from ._seed import (
    ZERO_EMBEDDING,
    seed_model,
    seed_observation,
)


pytestmark = pytest.mark.integration


ALL_PRIMITIVES = [
    "DEPENDENCY",
    "CONSTRAINT",
    "COUNTEREVIDENCE",
    "OWNERSHIP",
    "GOAL_IMPACT",
    "RECURRENCE",
]


@dataclass(frozen=True, slots=True)
class LargeScenario:
    name: str
    customer: str
    system: str
    primitive: str
    signal: str
    target_claim: str
    expected_token: str


LARGE_SCENARIOS = (
    LargeScenario(
        name="enterprise_sso_dependency",
        customer="AcmeAtlas",
        system="SsoRelay",
        primitive="DEPENDENCY",
        signal="AcmeAtlas SsoRelay launch is blocked by security review.",
        target_claim="AcmeAtlas SsoRelay launch depends on security review capacity",
        expected_token="security review capacity",
    ),
    LargeScenario(
        name="renewal_counterevidence",
        customer="NorthstarBank",
        system="RenewalDesk",
        primitive="COUNTEREVIDENCE",
        signal="NorthstarBank RenewalDesk risk may be overstated by stale churn notes.",
        target_claim="NorthstarBank RenewalDesk churn risk is contradicted by signed expansion",
        expected_token="signed expansion",
    ),
    LargeScenario(
        name="ownership_handoff",
        customer="HelioWorks",
        system="DataBridge",
        primitive="OWNERSHIP",
        signal="HelioWorks DataBridge handoff lacks a clear owner.",
        target_claim="HelioWorks DataBridge ownership sits with platform enablement",
        expected_token="platform enablement",
    ),
    LargeScenario(
        name="recurring_pattern",
        customer="VelaRetail",
        system="ImportFlow",
        primitive="RECURRENCE",
        signal="VelaRetail ImportFlow stalls are recurring every month-end close.",
        target_claim="VelaRetail ImportFlow month-end stalls recur when catalog imports spike",
        expected_token="catalog imports spike",
    ),
    LargeScenario(
        name="goal_resource_constraint",
        customer="OrionHealth",
        system="PatientSync",
        primitive="CONSTRAINT",
        signal="OrionHealth PatientSync goal is constrained by sandbox quota.",
        target_claim="OrionHealth PatientSync goal is constrained by sandbox quota exhaustion",
        expected_token="sandbox quota exhaustion",
    ),
)


@pytest.mark.asyncio
async def test_reader_handles_empty_corpus_and_noisy_unicode_signal(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=None,
        seed_natural_text="   ???  Δ unknown    ",
        seed_entity_ids=[],
        precomputed_seed_vector=ZERO_EMBEDDING,
    )

    async with gateway_pool.acquire() as conn:
        result = await SynthesisReader().read(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            question_id="Q_EMPTY",
            question="???",
            question_primitive="DEPENDENCY",
        )

    assert result.models == ()
    assert result.observations == ()
    assert result.activations == ()
    assert result.debug["selector"]["selected_nodes"] == []


@pytest.mark.asyncio
async def test_reader_negative_memory_suppresses_known_low_value_node(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    obs_id = await seed_observation(
        gateway_pool,
        tenant_id=tenant_id,
        content_text="A dependency review found the actual launch blocker.",
    )
    target_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Launch depends on the actual security reviewer capacity",
        supporting_event_ids=[obs_id],
        signal_readings=[{"kind": "observe", "event_id": str(obs_id), "weight": 1.0}],
    )
    decoy_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Launch dependency generic noisy dashboard repeats old alert",
        supporting_event_ids=[obs_id],
    )
    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    await repo.upsert(
        RetrievalAffordanceProfile(
            model_id=target_id,
            tenant_id=tenant_id,
            answers_question_primitives=["DEPENDENCY"],
            action_affordances=["map.critical_path"],
            utility_score=0.4,
        )
    )
    await repo.upsert(
        RetrievalAffordanceProfile(
            model_id=decoy_id,
            tenant_id=tenant_id,
            answers_question_primitives=["DEPENDENCY"],
            action_affordances=["map.critical_path"],
            utility_score=5.0,
        )
    )
    await NegativeMemoryRepo(gateway_pool, tenant_id=tenant_id).insert(
        NegativeMemory(
            id=uuid7(),
            tenant_id=tenant_id,
            memory_type="low_value_node",
            signature={"signal_type": "T1", "question_primitive": "DEPENDENCY"},
            rejected_path={"model_id": str(decoy_id)},
            reason="Repeatedly selected but never used in valid diffs.",
            confidence=0.95,
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text="launch dependency actual security reviewer",
        seed_entity_ids=[],
        precomputed_seed_vector=ZERO_EMBEDDING,
    )

    async with gateway_pool.acquire() as conn:
        result = await SynthesisReader(
            budget=ReaderBudget(max_nodes=1, max_edges=4)
        ).read(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            question_id="Q_DEP",
            question="What dependency blocks launch?",
            question_primitive="DEPENDENCY",
        )

    assert target_id in {model.id for model in result.models}
    assert decoy_id not in {model.id for model in result.models}
    decoy_trace = next(trace for trace in result.activations if trace.model_id == decoy_id)
    assert decoy_trace.activation_score <= 0.12
    assert any("negative_memory:low_value_node" in r for r in decoy_trace.activation_reasons)


@pytest.mark.asyncio
async def test_reader_preserves_bridge_node_and_summarizes_generic_hub(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    obs_id = await seed_observation(
        gateway_pool,
        tenant_id=tenant_id,
        content_text="Acme SSO depends on a bridge between security and launch scope.",
    )
    target_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Acme SSO launch depends on security review",
        supporting_event_ids=[obs_id],
    )
    bridge_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Bridge evidence links SSO security review to launch readiness",
        supporting_event_ids=[obs_id],
    )
    hub_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Generic operational dashboard hub mentions SSO launch",
        supporting_event_ids=[obs_id],
    )
    repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    for model_id, utility in ((target_id, 2.0), (bridge_id, 1.2), (hub_id, 3.0)):
        await repo.upsert(
            RetrievalAffordanceProfile(
                model_id=model_id,
                tenant_id=tenant_id,
                answers_question_primitives=["DEPENDENCY"],
                action_affordances=["map.critical_path"],
                utility_score=utility,
            )
        )
    async with gateway_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO model_structural_features (
              model_id, tenant_id, degree_total, degree_in, degree_out,
              bridge_score, hub_score
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            [
                (target_id, tenant_id, 2, 1, 1, 0.10, 0.10),
                (bridge_id, tenant_id, 3, 1, 2, 0.92, 0.20),
                (hub_id, tenant_id, 80, 40, 40, 0.05, 0.96),
            ],
        )
        trigger = TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            observation_id=obs_id,
            seed_natural_text="Acme SSO launch dependency",
            seed_entity_ids=[],
            precomputed_seed_vector=ZERO_EMBEDDING,
        )
        result = await SynthesisReader(
            budget=ReaderBudget(max_nodes=2, max_edges=4)
        ).read(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            question_id="Q_BRIDGE",
            question="What connects Acme SSO review to launch readiness?",
            question_primitive="DEPENDENCY",
        )

    assert bridge_id in result.selection.bridge_nodes
    assert bridge_id in result.selection.selected_nodes
    assert hub_id in result.selection.summarized_hubs
    assert hub_id not in result.selection.selected_nodes


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", LARGE_SCENARIOS, ids=lambda s: s.name)
async def test_large_model_e2e_sage_reader_retrieves_target_under_load(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    scenario: LargeScenario,
):
    async with gateway_pool.acquire() as conn:
        seeded = await _seed_large_corpus(
            conn,
            tenant_id=tenant_id,
            scenario=scenario,
            n_noise_models=900,
        )
        trigger = TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            observation_id=seeded["target_observation_id"],
            seed_natural_text=scenario.signal,
            seed_entity_ids=[
                {"type": "customer", "id": scenario.customer},
                {"type": "system", "id": scenario.system},
            ],
            precomputed_seed_vector=ZERO_EMBEDDING,
        )
        started = time.perf_counter()
        result = await run_inquiry_retrieval(
            trigger,
            conn,
            embedder=None,
            llm_provider=None,
            mode="deep",
            top_n=48,
            config=InquiryConfig(
                max_rounds=1,
                questions_per_round=4,
                llm_question_planning_enabled=False,
                sage_reader_enabled=True,
                persist=True,
                candidate_model_limit=180,
                result_model_limit=48,
                evidence_reservoir_limit=180,
            ),
        )
        elapsed = time.perf_counter() - started
        activation_rows = await conn.fetch(
            """
            SELECT model_id, selected, activation_score, activation_reasons
            FROM sage_reader_activations
            WHERE tenant_id = $1
              AND inquiry_session_id = $2
            """,
            tenant_id,
            result.session_id,
        )
        attribution_rows = await conn.fetch(
            """
            SELECT model_id, question_primitive, selected,
                   activation_score, source_breakdown,
                   retrieval_actions, projected_evidence_refs
            FROM sage_reader_decision_attributions
            WHERE tenant_id = $1
              AND inquiry_session_id = $2
            """,
            tenant_id,
            result.session_id,
        )

    retrieved_model_ids = {model.id for model in result.retrieval_result.models}
    selected_activation_ids = {
        row["model_id"] for row in activation_rows if row["selected"]
    }
    assert seeded["target_model_id"] in retrieved_model_ids | selected_activation_ids
    assert seeded["bridge_model_id"] in retrieved_model_ids | selected_activation_ids
    assert attribution_rows
    assert seeded["target_model_id"] in {row["model_id"] for row in attribution_rows}
    assert any(row["retrieval_actions"] for row in attribution_rows)
    assert result.notes["sage_reader"]["activation_trace_count"] > 0
    assert result.notes["sage_reader"]["projected_evidence_count"] > 0
    assert len(result.retrieval_result.models) <= 48
    assert any(
        scenario.expected_token in card.summary.casefold()
        for card in result.evidence_cards
    )
    assert elapsed < 8.0


async def _seed_large_corpus(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    scenario: LargeScenario,
    n_noise_models: int,
) -> dict[str, UUID]:
    now = datetime.now(timezone.utc)
    target_observation_id = uuid7()
    noise_observation_id = uuid7()
    await conn.executemany(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind,
            source_channel, source_actor_ref, actor_id,
            content, content_text,
            embedding, embedding_pending,
            trust_tier, external_id, cause_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, $3, 'signal',
            'sage-large-e2e', NULL, NULL,
            $4::jsonb, $5,
            NULL, TRUE,
            'authoritative', $6, NULL, $7::jsonb
        )
        """,
        [
            (
                target_observation_id,
                tenant_id,
                now,
                json.dumps({"content_text": scenario.target_claim}),
                scenario.target_claim,
                f"target-{target_observation_id}",
                json.dumps([
                    {"type": "customer", "id": scenario.customer},
                    {"type": "system", "id": scenario.system},
                ]),
            ),
            (
                noise_observation_id,
                tenant_id,
                now,
                json.dumps({"content_text": f"Background corpus for {scenario.name}"}),
                f"Background corpus for {scenario.name}",
                f"noise-{noise_observation_id}",
                json.dumps([]),
            ),
        ],
    )

    target_model_id = uuid7()
    bridge_model_id = uuid7()
    hub_model_id = uuid7()
    scope_entities = json.dumps([
        {"type": "customer", "id": scenario.customer},
        {"type": "system", "id": scenario.system},
    ])
    scope_temporal = json.dumps({"valid_from": now.isoformat(), "valid_until": None})
    model_rows = [
        _model_row(
            target_model_id,
            tenant_id,
            target_observation_id,
            scenario.target_claim,
            scope_entities,
            scope_temporal,
            [target_observation_id],
            confidence=0.91,
        ),
        _model_row(
            bridge_model_id,
            tenant_id,
            target_observation_id,
            f"{scenario.customer} {scenario.system} bridge links {scenario.expected_token} to execution risk",
            scope_entities,
            scope_temporal,
            [target_observation_id],
            confidence=0.82,
        ),
        _model_row(
            hub_model_id,
            tenant_id,
            noise_observation_id,
            f"Generic dashboard hub for {scenario.system} with broad operational status",
            scope_entities,
            scope_temporal,
            [noise_observation_id],
            confidence=0.55,
        ),
    ]
    for idx in range(n_noise_models):
        noise_id = uuid7()
        if idx % 37 == 0:
            natural = (
                f"{scenario.customer} historical note {idx} mentions "
                f"{scenario.system} but not {scenario.expected_token}"
            )
            entities = scope_entities
        else:
            natural = (
                f"Background model {idx} for unrelated tenant workflow "
                f"queue health inventory billing support"
            )
            entities = json.dumps([
                {"type": "customer", "id": f"NoiseCustomer{idx % 29}"},
                {"type": "system", "id": f"NoiseSystem{idx % 31}"},
            ])
        model_rows.append(
            _model_row(
                noise_id,
                tenant_id,
                noise_observation_id,
                natural,
                entities,
                scope_temporal,
                [noise_observation_id],
                confidence=0.35 + (idx % 30) / 100,
            )
        )
    await conn.executemany(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, confidence_at_assertion, activation,
            falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            status
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            '{}'::uuid[], $7::jsonb, $8::jsonb,
            $9, $9, 1.0,
            NULL, $10::jsonb,
            $11::uuid[], '{}'::uuid[],
            'active'
        )
        """,
        model_rows,
    )
    await _insert_edge(conn, tenant_id, target_model_id, bridge_model_id, "supports", 0.92)
    await _insert_edge(conn, tenant_id, bridge_model_id, target_model_id, "depends_on", 0.88)
    await conn.executemany(
        """
        INSERT INTO model_structural_features (
          model_id, tenant_id, degree_total, degree_in, degree_out,
          bridge_score, hub_score
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        [
            (target_model_id, tenant_id, 3, 1, 2, 0.25, 0.18),
            (bridge_model_id, tenant_id, 4, 2, 2, 0.88, 0.22),
            (hub_model_id, tenant_id, 120, 60, 60, 0.05, 0.97),
        ],
    )
    profile_repo = AffordanceProfilesRepo(None, tenant_id=tenant_id)
    for model_id, utility, actions in (
        (target_model_id, 3.5, ["map.critical_path", "project.evidence"]),
        (bridge_model_id, 2.2, ["connect.regions", "project.evidence"]),
        (hub_model_id, 0.1, ["rollup.generic_status"]),
    ):
        await profile_repo.upsert(
            RetrievalAffordanceProfile(
                model_id=model_id,
                tenant_id=tenant_id,
                answers_question_primitives=ALL_PRIMITIVES,
                supports_hypothesis_types=["delivery_risk", "customer_risk"],
                action_affordances=actions,
                activation_signatures={
                    "entities": [scenario.customer, scenario.system],
                    "scenario": scenario.name,
                },
                utility_score=utility,
            ),
            conn=conn,
        )
    return {
        "target_observation_id": target_observation_id,
        "target_model_id": target_model_id,
        "bridge_model_id": bridge_model_id,
        "hub_model_id": hub_model_id,
    }


def _model_row(
    model_id: UUID,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
    scope_entities: str,
    scope_temporal: str,
    supporting_event_ids: list[UUID],
    *,
    confidence: float,
) -> tuple:
    return (
        model_id,
        tenant_id,
        born_from_event_id,
        json.dumps({"kind": "belief", "subject": natural}),
        natural,
        ZERO_EMBEDDING,
        scope_entities,
        scope_temporal,
        float(confidence),
        json.dumps([
            {
                "kind": "observe",
                "event_id": str(supporting_event_ids[0]),
                "weight": 1.0,
            }
        ]),
        supporting_event_ids,
    )


async def _insert_edge(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str,
    weight: float,
) -> UUID:
    edge_id = uuid7()
    await conn.execute(
        """
        INSERT INTO model_edges (
            id, tenant_id, source_model_id, target_model_id,
            edge_kind, weight, status, detected_by
        ) VALUES ($1, $2, $3, $4, $5, $6, 'active', 'sage-large-e2e')
        """,
        edge_id,
        tenant_id,
        source_model_id,
        target_model_id,
        edge_kind,
        weight,
    )
    return edge_id
