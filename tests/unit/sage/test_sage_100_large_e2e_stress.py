"""Opt-in 100-case large-corpus E2E stress harness for SAGE retrieval."""
from __future__ import annotations

import json
import os
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.retrieval.primary import TriggerContext
from tests.unit.sage._seed import ZERO_EMBEDDING
from tests.unit.sage.test_synthesis_reader_extensive import (
    ALL_PRIMITIVES,
    _insert_edge,
    _model_row,
)


pytestmark = pytest.mark.integration


@dataclass(frozen=True, slots=True)
class StressCase:
    name: str
    customer: str
    system: str
    primitive: str
    signal: str
    target_claim: str
    expected_token: str


@pytest.mark.asyncio
@pytest.mark.timeout(1800)
@pytest.mark.skipif(
    os.environ.get("SAGE_RUN_100_LARGE_E2E") != "1",
    reason="set SAGE_RUN_100_LARGE_E2E=1 to run the 100-case stress harness",
)
async def test_sage_100_large_model_e2e_stress(
    gateway_pool: asyncpg.Pool,
):
    """100 end-to-end cases, each with >5,000 tenant-scoped Models.

    The corpus intentionally includes many irrelevant high-utility
    affordances to catch low-value learned-utility leakage.
    """
    metrics: list[dict[str, float]] = []
    async with gateway_pool.acquire() as conn:
        case_count = int(os.environ.get("SAGE_LARGE_E2E_CASES", "100"))
        assert case_count >= 1
        for idx in range(case_count):
            case = _stress_case(idx)
            tenant_id = uuid7()
            seeded = await _seed_stress_corpus(
                conn,
                tenant_id=tenant_id,
                case=case,
                n_noise_models=5200,
                n_irrelevant_affordances=360,
            )
            trigger = TriggerContext(
                kind="T1",
                tenant_id=tenant_id,
                observation_id=seeded["target_observation_id"],
                seed_natural_text=case.signal,
                seed_entity_ids=[
                    {"type": "customer", "id": case.customer},
                    {"type": "system", "id": case.system},
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
                top_n=64,
                config=InquiryConfig(
                    max_rounds=1,
                    questions_per_round=3,
                    llm_question_planning_enabled=False,
                    sage_reader_enabled=True,
                    persist=True,
                    candidate_model_limit=220,
                    result_model_limit=48,
                    evidence_reservoir_limit=220,
                    semantic_budget=24,
                ),
            )
            elapsed = time.perf_counter() - started

            activation_rows = await conn.fetch(
                """
                SELECT model_id, selected
                FROM sage_reader_activations
                WHERE tenant_id = $1
                  AND inquiry_session_id = $2
                """,
                tenant_id,
                result.session_id,
            )
            attribution_rows = await conn.fetch(
                """
                SELECT model_id, selected, question_primitive
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
            selected_attr_ids = {
                row["model_id"] for row in attribution_rows if row["selected"]
            }
            expected_ids = {
                seeded["target_model_id"],
                seeded["bridge_model_id"],
                seeded["hub_model_id"],
            }
            irrelevant_selected = selected_attr_ids - expected_ids
            counter_cards = [
                card for card in result.evidence_cards
                if card.contradicts_hypotheses or card.weakens_hypotheses
            ]

            assert seeded["target_model_id"] in (
                retrieved_model_ids | selected_activation_ids
            ), case.name
            assert seeded["bridge_model_id"] in (
                retrieved_model_ids | selected_activation_ids
            ), case.name
            assert attribution_rows, case.name
            assert any(
                case.expected_token in card.summary.casefold()
                for card in result.evidence_cards
            ), case.name
            assert counter_cards, f"{case.name}: no counterevidence survived"
            assert len(result.retrieval_result.models) <= 48
            assert len(irrelevant_selected) <= 12, (
                case.name,
                len(irrelevant_selected),
            )
            metrics.append({
                "elapsed": elapsed,
                "activations": float(len(activation_rows)),
                "attributions": float(len(attribution_rows)),
                "irrelevant_selected": float(len(irrelevant_selected)),
                "evidence_cards": float(len(result.evidence_cards)),
                "counterevidence_cards": float(len(counter_cards)),
            })

    assert len(metrics) == case_count
    summary = {
        "cases": case_count,
        "models_per_case": 5203,
        "elapsed_p50": statistics.median([m["elapsed"] for m in metrics]),
        "elapsed_p95": _p95([m["elapsed"] for m in metrics]),
        "irrelevant_selected_p95": _p95([
            m["irrelevant_selected"] for m in metrics
        ]),
        "activation_p95": _p95([m["activations"] for m in metrics]),
        "attribution_p95": _p95([m["attributions"] for m in metrics]),
        "evidence_cards_p50": statistics.median([
            m["evidence_cards"] for m in metrics
        ]),
        "counterevidence_cards_p50": statistics.median([
            m["counterevidence_cards"] for m in metrics
        ]),
    }
    print("SAGE_100_LARGE_E2E_METRICS", json.dumps(summary, sort_keys=True))
    assert summary["irrelevant_selected_p95"] <= 8
    assert summary["evidence_cards_p50"] <= 16
    assert summary["counterevidence_cards_p50"] >= 1
    assert summary["elapsed_p95"] < 15.0


def _stress_case(idx: int) -> StressCase:
    primitive = [
        "DEPENDENCY",
        "CONSTRAINT",
        "OWNERSHIP",
        "RECURRENCE",
        "COUNTEREVIDENCE",
        "GOAL_IMPACT",
    ][idx % 6]
    customer = f"StressCustomer{idx:03d}"
    system = f"StressSystem{idx % 17:02d}"
    token_by_primitive = {
        "DEPENDENCY": "review capacity",
        "CONSTRAINT": "sandbox quota",
        "OWNERSHIP": "platform owner",
        "RECURRENCE": "month-end recurrence",
        "COUNTEREVIDENCE": "signed expansion",
        "GOAL_IMPACT": "renewal exposure",
    }
    expected = token_by_primitive[primitive]
    signal_by_primitive = {
        "DEPENDENCY": f"{customer} {system} launch is blocked by review capacity.",
        "CONSTRAINT": f"{customer} {system} is constrained by sandbox quota.",
        "OWNERSHIP": f"{customer} {system} has no clear platform owner.",
        "RECURRENCE": f"{customer} {system} stalls recur every month-end close.",
        "COUNTEREVIDENCE": (
            f"{customer} {system} risk may be overstated by stale churn notes."
        ),
        "GOAL_IMPACT": f"{customer} {system} blocker threatens renewal exposure.",
    }
    return StressCase(
        name=f"{primitive.lower()}_{idx:03d}",
        customer=customer,
        system=system,
        primitive=primitive,
        signal=signal_by_primitive[primitive],
        target_claim=f"{customer} {system} evidence resolves {expected}",
        expected_token=expected,
    )


async def _seed_stress_corpus(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    case: StressCase,
    n_noise_models: int,
    n_irrelevant_affordances: int,
) -> dict[str, UUID]:
    now = datetime.now(timezone.utc)
    target_observation_id = uuid7()
    falsifier_observation_id = uuid7()
    noise_observation_id = uuid7()
    falsifier_claim = _stress_falsifier_claim(case)
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
            'sage-100-large-e2e', NULL, NULL,
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
                json.dumps({"content_text": case.target_claim}),
                case.target_claim,
                f"target-{target_observation_id}",
                json.dumps([
                    {"type": "customer", "id": case.customer},
                    {"type": "system", "id": case.system},
                ]),
            ),
            (
                falsifier_observation_id,
                tenant_id,
                now,
                json.dumps({"content_text": falsifier_claim}),
                falsifier_claim,
                f"falsifier-{falsifier_observation_id}",
                json.dumps([
                    {"type": "customer", "id": case.customer},
                    {"type": "system", "id": case.system},
                ]),
            ),
            (
                noise_observation_id,
                tenant_id,
                now,
                json.dumps({"content_text": f"stress noise for {case.name}"}),
                f"stress noise for {case.name}",
                f"noise-{noise_observation_id}",
                json.dumps([]),
            ),
        ],
    )

    target_model_id = uuid7()
    bridge_model_id = uuid7()
    hub_model_id = uuid7()
    scope_entities = json.dumps([
        {"type": "customer", "id": case.customer},
        {"type": "system", "id": case.system},
    ])
    scope_temporal = json.dumps({"valid_from": now.isoformat(), "valid_until": None})
    noise_model_ids: list[UUID] = []
    model_rows = [
        _model_row(
            target_model_id,
            tenant_id,
            target_observation_id,
            case.target_claim,
            scope_entities,
            scope_temporal,
            [target_observation_id, falsifier_observation_id],
            confidence=0.93,
            signal_readings=[
                {
                    "kind": "observe",
                    "event_id": str(target_observation_id),
                    "weight": 1.0,
                },
                {
                    "kind": "contradiction",
                    "event_id": str(falsifier_observation_id),
                    "weight": -0.95,
                    "reason": "fresh adversarial falsifier",
                },
            ],
            falsifier={
                "kind": "observation_pattern",
                "pattern": "not blocked",
            },
        ),
        _model_row(
            bridge_model_id,
            tenant_id,
            target_observation_id,
            f"{case.customer} {case.system} bridge connects {case.expected_token}",
            scope_entities,
            scope_temporal,
            [target_observation_id],
            confidence=0.84,
        ),
        _model_row(
            hub_model_id,
            tenant_id,
            noise_observation_id,
            f"Generic status hub for {case.system}",
            scope_entities,
            scope_temporal,
            [noise_observation_id],
            confidence=0.52,
        ),
    ]
    for noise_idx in range(n_noise_models):
        model_id = uuid7()
        noise_model_ids.append(model_id)
        if noise_idx % 41 == 0:
            natural = (
                f"{case.customer} {case.system} distractor {noise_idx} "
                f"mentions not {case.expected_token}"
            )
            entities = scope_entities
        else:
            natural = (
                f"Unrelated model {noise_idx} queue billing telemetry "
                f"for NoiseCustomer{noise_idx % 113}"
            )
            entities = json.dumps([
                {"type": "customer", "id": f"NoiseCustomer{noise_idx % 113}"},
                {"type": "system", "id": f"NoiseSystem{noise_idx % 97}"},
            ])
        model_rows.append(
            _model_row(
                model_id,
                tenant_id,
                noise_observation_id,
                natural,
                entities,
                scope_temporal,
                [noise_observation_id],
                confidence=0.30 + (noise_idx % 40) / 100,
            )
        )
    for offset in range(0, len(model_rows), 1000):
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
                $12::jsonb, $10::jsonb,
                $11::uuid[], '{}'::uuid[],
                'active'
            )
            """,
            model_rows[offset: offset + 1000],
        )

    await _insert_edge(conn, tenant_id, target_model_id, bridge_model_id, "supports", 0.91)
    await _insert_edge(conn, tenant_id, bridge_model_id, target_model_id, "depends_on", 0.87)
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
            (hub_model_id, tenant_id, 140, 70, 70, 0.05, 0.97),
        ],
    )
    profile_rows = [
        _profile_row(
            target_model_id,
            tenant_id,
            case,
            utility=1.80,
            entities=[case.customer, case.system],
        ),
        _profile_row(
            bridge_model_id,
            tenant_id,
            case,
            utility=1.55,
            entities=[case.customer, case.system],
        ),
        _profile_row(
            hub_model_id,
            tenant_id,
            case,
            utility=0.10,
            entities=[case.customer, case.system],
        ),
    ]
    for idx, model_id in enumerate(noise_model_ids[:n_irrelevant_affordances]):
        profile_rows.append(
            _profile_row(
                model_id,
                tenant_id,
                case,
                utility=5.0 - (idx % 50) / 100,
                entities=[f"IrrelevantCustomer{idx % 29}", f"IrrelevantSystem{idx % 31}"],
            )
        )
    await conn.executemany(
        """
        INSERT INTO retrieval_affordance_profiles (
          model_id, tenant_id,
          answers_question_primitives, supports_hypothesis_types,
          weakens_hypothesis_types, common_composition_types,
          action_affordances, activation_signatures, projection_policy,
          utility_score, last_updated_at
        ) VALUES (
          $1, $2,
          $3::text[], $4::text[],
          '{}'::text[], '{}'::text[],
          $5::text[], $6::jsonb, '{}'::jsonb,
          $7, now()
        )
        """,
        profile_rows,
    )
    return {
        "target_observation_id": target_observation_id,
        "falsifier_observation_id": falsifier_observation_id,
        "target_model_id": target_model_id,
        "bridge_model_id": bridge_model_id,
        "hub_model_id": hub_model_id,
    }


def _stress_falsifier_claim(case: StressCase) -> str:
    return (
        f"{case.customer} {case.system} is not blocked; "
        f"the prior risk is resolved by {case.expected_token}."
    )


def _profile_row(
    model_id: UUID,
    tenant_id: UUID,
    case: StressCase,
    *,
    utility: float,
    entities: list[str],
) -> tuple:
    return (
        model_id,
        tenant_id,
        ALL_PRIMITIVES,
        ["delivery_risk", "customer_risk"],
        ["stress.evidence"],
        json.dumps({"entities": entities, "scenario": case.name}),
        float(utility),
    )


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=20, method="inclusive")[18]
