"""Adversarial integrity tests for the independently owned P5 oracle."""

from __future__ import annotations

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p5_oracles import (
    P5BarrierReceipt,
    P5SignalReceipt,
    P5VerticalReceipt,
    build_p5_artifact,
)
from lib.evaluation.epistemic_repair.p5_population import build_p5_population

_TARGET_POSITION = {1: 13, 2: 10, 3: 16}


def _fabricated_success_inputs():
    population = build_p5_population()
    signals = tuple(
        P5SignalReceipt(
            signal_id=signal.signal_id,
            batch_number=signal.batch_number,
            position=signal.position,
            episode_id=signal.episode_id,
            observation_id=f"fabricated-observation:{signal.signal_id}",
            sealed_content_digest=canonical_sha256(signal.text),
            persisted_content_digest=canonical_sha256(signal.text),
            persisted=True,
            decision_id=f"fabricated-decision:{signal.signal_id}",
            route_id=f"fabricated-route:{signal.signal_id}",
            decision_fate="mutation"
            if signal.position == _TARGET_POSITION[signal.batch_number]
            else "validator_drop",
            grounding_fate="resolved_for_consumer"
            if signal.position == _TARGET_POSITION[signal.batch_number]
            else None,
            source_semantic_disposition="belief_applied"
            if signal.position == _TARGET_POSITION[signal.batch_number]
            else None,
        )
        for signal in population.signals
    )
    vertical = P5VerticalReceipt(
        batch_1_model_id="fabricated-model-1",
        batch_1_model_version_id="fabricated-version-1",
        batch_1_atomic=True,
        batch_2_model_id="fabricated-model-2",
        batch_2_model_version_id="fabricated-version-2",
        batch_2_prior_retrieved=True,
        batch_2_prior_referenced=True,
        relation_disposition="accepted",
        relation_kind="dependency_constraint",
        relation_id="fabricated-relation",
        relation_version_id="fabricated-relation-version",
        no_relation_reason=None,
        batch_3_corrected_model_id="fabricated-model-3",
        batch_3_corrected_model_version_id="fabricated-version-3",
        batch_3_corrected_retrieved=True,
        batch_3_corrected_referenced=True,
        invalidated_model_id="fabricated-model-1",
        invalidated_model_version_id="fabricated-version-1",
        terminal_lifecycle="falsified",
        stale_model_excluded=True,
        stale_relation_excluded=True,
        relation_repair_obligation_count=1,
    )
    barriers = tuple(
        P5BarrierReceipt(
            batch_id=f"p5-batch-{number}",
            barrier_id=f"fabricated-barrier-{number}",
            barrier_version=number,
            expected_model_version_count=1,
            expected_relation_version_count=1 if number == 2 else 0,
            invalidated_model_version_count=1 if number == 3 else 0,
            truth_critical_pending_count=0,
            receipt_digest="0" * 64,
        )
        for number in range(1, 4)
    )
    database_evidence = {
        "accepted_object_count": 3,
        "cross_tenant_contamination_count": 0,
        "database_receipt_digest": "a" * 64,
        "preflight": {"accepted_model_count": 0, "accepted_relation_count": 0},
        "signal_rows": [
            {
                "signal_id": signal.signal_id,
                "observation_id": signal.observation_id,
                "content_digest": signal.persisted_content_digest,
            }
            for signal in signals
        ],
        "decision_rows": [
            {
                "signal_id": signal.signal_id,
                "decision_id": signal.decision_id,
                "decision_fate": signal.decision_fate,
                "route_id": signal.route_id,
                "context_item_id": signal.observation_id,
            }
            for signal in signals
        ],
        "semantic_rows": [
            {
                "signal_id": signal.signal_id,
                "grounding_fate": "resolved_for_consumer",
                "source_semantic_disposition": "belief_applied",
            }
            for signal in signals
            if signal.decision_fate == "mutation"
        ],
        "model_version_rows": [
            {"version_id": vertical.batch_1_model_version_id},
            {"version_id": vertical.batch_2_model_version_id},
            {"version_id": vertical.batch_3_corrected_model_version_id},
        ],
        "accepted_model_version_ids": [
            vertical.batch_2_model_version_id,
            vertical.batch_3_corrected_model_version_id,
        ],
        "relation_head_rows": [
            {"relation_id": vertical.relation_id, "lifecycle": "disputed"}
        ],
        "repair_obligation_rows": [
            {
                "invalidated_model_version_id": vertical.invalidated_model_version_id,
                "affected_id": vertical.relation_version_id,
            }
        ],
    }
    return population, signals, vertical, barriers, database_evidence


def test_artifact_builder_canonical_digest_round_trips() -> None:
    """Builder output must validate under its own canonical representation."""

    population, signals, vertical, barriers, database_evidence = (
        _fabricated_success_inputs()
    )
    artifact = build_p5_artifact(
        population=population,
        signals=signals,
        vertical=vertical,
        barriers=barriers,
        zero_seed_initial_model_count=0,
        provider_call_count=0,
        database_evidence=database_evidence,
        timings_ms={},
    )
    assert len(artifact.content_digest) == 64


def test_fabricated_success_cannot_pass_without_database_receipt_binding() -> None:
    """Caller booleans and fake IDs are not independent database evidence."""

    population, signals, vertical, barriers, _ = _fabricated_success_inputs()
    with pytest.raises(
        ValueError,
        match=r"(?i)(database.*receipt|receipt.*database|evidence.*(binding|identity))",
    ):
        build_p5_artifact(
            population=population,
            signals=signals,
            vertical=vertical,
            barriers=barriers,
            zero_seed_initial_model_count=0,
            provider_call_count=0,
            database_evidence={
                "accepted_object_count": 1,
                "cross_tenant_contamination_count": 0,
            },
            timings_ms={},
        )
