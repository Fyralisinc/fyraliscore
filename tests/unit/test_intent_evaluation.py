from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.intent import (
    IntentEvaluationScope,
    analyze_intent_rows,
    build_intent_invariant_evidence,
)
from lib.evaluation.proof import EvidenceTier


ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _scope() -> IntentEvaluationScope:
    return IntentEvaluationScope(
        tenant_id=uuid4(),
        start=NOW - timedelta(hours=1),
        end=NOW,
        run_id="intent-eval-unit",
    )


def test_empty_intent_population_is_unknown_coverage_not_an_incident() -> None:
    state = analyze_intent_rows(
        scope=_scope(),
        proposals=(),
        acceptances=(),
        commands=(),
        think_runs=(),
        acted_recommendations=(),
        legacy_baseline_count=0,
        artifact_refs=("pytest:intent-empty",),
    )
    assert state.proposal_count == 0
    assert state.incident_counts == {}
    assert state.command_reconstructability_rate == 1.0
    registry = load_architecture_registry(ROOT / "architecture/registry.yaml")
    evidence = build_intent_invariant_evidence(
        state,
        registry=registry,
        executed_scenario_ids=frozenset(),
    )
    assert {item.invariant_id for item in evidence} == {
        "INV-13",
        "INV-16",
        "INV-23",
        "INV-33",
    }
    assert all(item.achieved_evidence_tier is EvidenceTier.E3 for item in evidence)


def test_intent_evaluator_localizes_protocol_bypass_and_missing_trace() -> None:
    proposal_id = uuid4()
    state = analyze_intent_rows(
        scope=_scope(),
        proposals=(
            {
                "id": proposal_id,
                "proposal_version": 1,
                "proposal": {},
                "proposal_digest": "0" * 64,
                "normalized_payload_digest": "1" * 64,
                "fate": "accepted_for_authorization",
                "review_due_at": NOW - timedelta(minutes=1),
            },
        ),
        acceptances=(),
        commands=(),
        think_runs=(
            {
                "id": uuid4(),
                "ops_applied": {
                    "act_ops": [
                        {"op": "transition_commitment", "commitment_id": str(uuid4())}
                    ]
                },
            },
        ),
        acted_recommendations=(
            {"id": uuid4(), "caused_act_change_id": uuid4()},
        ),
        legacy_baseline_count=0,
        artifact_refs=("pytest:intent-bypass",),
    )
    assert state.incident_counts["invalid_proposal_contract"] == 1
    assert state.incident_counts["accepted_without_exact_acceptance"] == 1
    assert state.incident_counts["accepted_without_applied_command"] == 1
    assert state.incident_counts["think_directly_mutated_intent"] == 1
    assert state.incident_counts[
        "recommendation_action_bypassed_intent_protocol"
    ] == 1
