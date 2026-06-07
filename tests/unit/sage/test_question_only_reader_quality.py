"""Question-only retrieval quality coverage for the SAGE reader.

These tests deliberately model Ask Fyralis-style reads: the input is a
question, not an incoming signal/observation. The integration matrix is
therefore seeded with active Synthesis state and no observation trigger.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.retrieval.primary import TriggerContext
from services.sage.cue_extractor import StructuredCues
from services.sage.reader import (
    ReaderBudget,
    SynthesisReader,
    _candidate_terms,
    _lexical_activation,
    _operational_facet_activation,
)
from services.synthesis.operational_facets import enrich_operational_model_proposition
from tests.unit.sage._seed import ZERO_EMBEDDING, seed_model, seed_observation


@dataclass(frozen=True, slots=True)
class QuestionOnlyScenario:
    name: str
    primitive: str
    question: str
    target_claim: str
    decoy_claims: tuple[str, ...]
    expected_role_reason: str | None = None


QUESTION_ONLY_SCENARIOS = (
    QuestionOnlyScenario(
        name="dependency_acronym_compact_match",
        primitive="DEPENDENCY",
        question="What blocks Acme SSO Relay launch?",
        target_claim="Acme SsoRelay launch is blocked by security review capacity",
        decoy_claims=(
            "Acme SsoRelay dashboard is green but unrelated to launch readiness",
            "Security review capacity is improving for an unrelated customer",
        ),
        expected_role_reason="role:blocker",
    ),
    QuestionOnlyScenario(
        name="ownership",
        primitive="OWNERSHIP",
        question="Who owns HelioWorks DataBridge handoff?",
        target_claim="HelioWorks DataBridge handoff owner is platform enablement",
        decoy_claims=(
            "HelioWorks DataBridge handoff mentions support but no owner",
            "Platform enablement owns an unrelated catalog migration",
        ),
        expected_role_reason="role:owner",
    ),
    QuestionOnlyScenario(
        name="counterevidence",
        primitive="COUNTEREVIDENCE",
        question="What contradicts Northstar churn risk?",
        target_claim="Northstar churn risk is contradicted by a signed expansion order",
        decoy_claims=(
            "Northstar churn risk appears in an old renewal dashboard",
            "Signed expansion order exists for a different account",
        ),
        expected_role_reason="role:counterevidence",
    ),
    QuestionOnlyScenario(
        name="recurrence",
        primitive="RECURRENCE",
        question="Has Vela ImportFlow failed repeatedly?",
        target_claim="Vela ImportFlow month-end stalls recur when catalog imports spike",
        decoy_claims=(
            "Vela ImportFlow had one isolated retry during onboarding",
            "Catalog imports spike for a separate analytics batch",
        ),
        expected_role_reason="role:pattern",
    ),
    QuestionOnlyScenario(
        name="constraint",
        primitive="CONSTRAINT",
        question="Which resource bottleneck constrains Orion PatientSync?",
        target_claim="Orion PatientSync is constrained by sandbox quota exhaustion",
        decoy_claims=(
            "Orion PatientSync roadmap is mentioned in a generic planning note",
            "Sandbox quota was discussed for a retired internal demo",
        ),
        expected_role_reason="role:blocker",
    ),
    QuestionOnlyScenario(
        name="action",
        primitive="ACTION",
        question="What should we do next for Meridian Billing cutover?",
        target_claim="Next action for Meridian Billing cutover is assigning billing ops owner",
        decoy_claims=(
            "Meridian Billing cutover has a historical migration checklist",
            "Billing ops owner is unavailable for a different launch",
        ),
    ),
    QuestionOnlyScenario(
        name="goal_impact",
        primitive="GOAL_IMPACT",
        question="Which goal is affected by Atlas revenue slip?",
        target_claim="Atlas revenue slip puts the retention expansion goal at risk",
        decoy_claims=(
            "Atlas revenue dashboard has stale finance commentary",
            "Retention expansion goal was mentioned in a company all-hands",
        ),
    ),
)


def _cues(
    *,
    relationship_clues: tuple[str, ...] = (),
    aliases: tuple[str, ...] = (),
) -> StructuredCues:
    return StructuredCues(
        explicit_entities=(),
        aliases=aliases,
        actor_mentions=(),
        team_mentions=(),
        customer_mentions=(),
        system_mentions=(),
        goal_mentions=(),
        commitment_mentions=(),
        relationship_clues=relationship_clues,
        time_constraints={},
        status_constraints={},
        source_constraints={},
        access_constraints={},
        expected_synthesis_decision_type=(),
    )


def _model(natural: str) -> ModelRow:
    now = datetime.now(timezone.utc)
    return ModelRow(
        id=uuid7(),
        tenant_id=uuid7(),
        born_from_event_id=uuid7(),
        proposition={"kind": "belief", "subject": natural},
        natural=natural,
        embedding=ZERO_EMBEDDING,
        scope_actors=[],
        scope_entities=[],
        scope_temporal={"valid_from": now.isoformat(), "valid_until": None},
        confidence=0.8,
        activation=1.0,
        falsifier=None,
        signal_readings=[],
        reading_contestable=True,
        supporting_event_ids=[],
        supporting_model_ids=[],
        evidential_weight=0.7,
        status="active",
        archived_at=None,
        archive_reason=None,
        created_at=now,
        last_retrieved_at=None,
        retrieval_count=0,
        evaluate_at=None,
        resolution_criteria=None,
        contributing_models=[],
        visible_to_subjects=True,
        proposition_kind=None,
        claim_role=None,
        abstraction_level=None,
        time_mode=None,
        modality=None,
        polarity=None,
        domain_tags=[],
        memory_grammar_version="v1",
        confirmed_count=0,
        contested_count=0,
        last_confirmed_at=None,
        confidence_at_assertion=0.8,
        resolved_at=None,
        resolution_outcome=None,
        activation_coefficient=1.0,
        target_actor_id=None,
        caused_act_change_id=None,
    )


def test_question_terms_include_phrases_compact_acronym_and_scope_signature():
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid7(),
        seed_signature={"entities": ["Acme", "SsoRelay"]},
    )
    terms = _candidate_terms(
        "What blocks Acme SSO Relay launch?",
        trigger,
        _cues(),
    )

    assert "acme sso relay" in terms
    assert "ssorelay" in terms
    assert "acmessorelay" in terms


def test_question_terms_prioritize_explicit_alternatives():
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    terms = _candidate_terms(
        """Which catalog page has the largest checkbox count?

A. Standard Laptop
B. Developer Laptop (Mac)
C. Sales Laptop
""",
        trigger,
        _cues(),
    )

    assert terms[:4] == [
        "standard laptop",
        "standardlaptop",
        "standard",
        "laptop",
    ]
    assert "developer laptop mac" in terms
    assert "developerlaptopmac" in terms


def test_lexical_activation_bridges_acronym_spacing_and_marks_blocker_role():
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    score, reasons = _lexical_activation(
        _model("Acme SsoRelay launch is blocked by security review capacity"),
        "What blocks Acme SSO Relay launch?",
        trigger,
        _cues(relationship_clues=("blocks",)),
    )

    assert score >= 0.30
    assert any(reason.startswith("lexical_compact:") for reason in reasons)
    assert "role:blocker" in reasons


def test_lexical_activation_marks_explicit_alternative_matches():
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    score, reasons = _lexical_activation(
        _model("Developer Laptop Mac order page has checkbox choice count = 7"),
        """Which page has the largest optional software checkbox count?

A. Standard Laptop
B. Developer Laptop (Mac)
C. Sales Laptop
""",
        trigger,
        _cues(),
    )

    assert score >= 0.20
    assert any(reason.startswith("alternative:") for reason in reasons)


def test_operational_facet_activation_matches_query_shape_not_benchmark_domain():
    proposition = enrich_operational_model_proposition(
        {"kind": "observation", "summary": "Observed request form controls."},
        natural="radio 500 GB [add $300.00] checked=false",
    )

    score, reasons = _operational_facet_activation(
        proposition,
        "What is the extra dollar amount for the largest option?",
    )

    assert score > 0.0
    assert any(reason.startswith("operational_role:") for reason in reasons)
    assert "delta" in reasons[0]
    assert "value" in reasons[0]


def test_lexical_activation_marks_owner_and_counterevidence_roles():
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())

    owner_score, owner_reasons = _lexical_activation(
        _model("HelioWorks DataBridge owner is platform enablement"),
        "Who owns HelioWorks DataBridge?",
        trigger,
        _cues(relationship_clues=("owns",)),
    )
    counter_score, counter_reasons = _lexical_activation(
        _model("Northstar churn risk is contradicted by signed expansion"),
        "What contradicts Northstar churn risk?",
        trigger,
        _cues(relationship_clues=("contradicts",)),
    )

    assert owner_score >= 0.25
    assert "role:owner" in owner_reasons
    assert counter_score >= 0.25
    assert "role:counterevidence" in counter_reasons


@pytest.mark.integration
@pytest.mark.parametrize("scenario", QUESTION_ONLY_SCENARIOS, ids=lambda s: s.name)
async def test_question_only_reader_retrieves_target_across_diverse_questions(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    scenario: QuestionOnlyScenario,
):
    target_id, hub_id = await _seed_question_only_scenario(
        gateway_pool,
        tenant_id=tenant_id,
        scenario=scenario,
        n_noise=140,
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=None,
        seed_natural_text=None,
        seed_entity_ids=[],
        precomputed_seed_vector=ZERO_EMBEDDING,
    )

    async with gateway_pool.acquire() as conn:
        result = await SynthesisReader(
            budget=ReaderBudget(
                max_nodes=10,
                max_edges=18,
                lexical_candidates=44,
            )
        ).read(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            question_id=f"Q_ONLY_{scenario.name}",
            question=scenario.question,
            question_primitive=scenario.primitive,
        )

    selected_ids = {model.id for model in result.models}
    assert target_id in selected_ids
    assert hub_id not in selected_ids
    target_trace = next(trace for trace in result.activations if trace.model_id == target_id)
    assert target_trace.selected is True
    assert target_trace.activation_score >= 0.30
    if scenario.expected_role_reason:
        assert scenario.expected_role_reason in target_trace.activation_reasons


async def _seed_question_only_scenario(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    scenario: QuestionOnlyScenario,
    n_noise: int,
) -> tuple[UUID, UUID]:
    target_obs = await seed_observation(
        pool,
        tenant_id=tenant_id,
        content_text=scenario.target_claim,
    )
    target_id = await seed_model(
        pool,
        tenant_id=tenant_id,
        born_from_event_id=target_obs,
        natural=scenario.target_claim,
        confidence=0.88,
        supporting_event_ids=[target_obs],
        signal_readings=[{"kind": "observe", "event_id": str(target_obs), "weight": 1.0}],
    )
    hub_obs = await seed_observation(
        pool,
        tenant_id=tenant_id,
        content_text=f"Generic hub for {scenario.question}",
    )
    hub_id = await seed_model(
        pool,
        tenant_id=tenant_id,
        born_from_event_id=hub_obs,
        natural=f"Generic dashboard hub repeats {scenario.question} without actionable synthesis",
        confidence=0.62,
        supporting_event_ids=[hub_obs],
    )
    for claim in scenario.decoy_claims:
        obs_id = await seed_observation(pool, tenant_id=tenant_id, content_text=claim)
        await seed_model(
            pool,
            tenant_id=tenant_id,
            born_from_event_id=obs_id,
            natural=claim,
            confidence=0.64,
            supporting_event_ids=[obs_id],
        )
    for idx in range(n_noise):
        noise_claim = (
            f"Background workflow note {idx} for unrelated tenant operations "
            f"inventory support billing queue health"
        )
        if idx % 19 == 0:
            noise_claim = (
                f"Background note {idx}: {scenario.question} appears in a noisy "
                f"dashboard summary without the target answer"
            )
        obs_id = await seed_observation(pool, tenant_id=tenant_id, content_text=noise_claim)
        await seed_model(
            pool,
            tenant_id=tenant_id,
            born_from_event_id=obs_id,
            natural=noise_claim,
            confidence=0.35 + (idx % 20) / 100,
            supporting_event_ids=[obs_id],
        )
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO model_structural_features (
              model_id, tenant_id, degree_total, degree_in, degree_out,
              bridge_score, hub_score
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            [
                (target_id, tenant_id, 3, 1, 2, 0.15, 0.10),
                (hub_id, tenant_id, 180, 90, 90, 0.02, 0.98),
            ],
        )
    return target_id, hub_id
