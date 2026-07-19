from __future__ import annotations

import asyncio
import json

import pytest

from lib.contracts.kernel import canonical_sha256
from services.evaluation.epistemic_repair.think_ti3_experiment import (
    ArmPolicy,
    ProviderAttempt,
    default_arm_policies,
    run_ti3_experiment,
    select_cheapest_within_tolerance,
    verify_experiment_artifact,
)


def _decision(payload: dict) -> dict:
    dossier_id = payload["dossier_id"]
    digest = canonical_sha256(payload)
    if dossier_id == "DOS_TI3_NULL_V1":
        decision = {"kind": "abstain", "reason_code": "insufficient_evidence",
                    "explanation": "No authorized record explains the schedule change.",
                    "missing_evidence": ["Authorized change record giving the reason."],
                    "relevant_handles": ["O2", "O3"], "confidence": .9}
    else:
        atlas = dossier_id == "DOS_TI3_ATLAS_V1"
        causes = ["M1", "M2", "O1"] if atlas else ["O1", "M1"]
        support = [row["object_handle"] for row in payload["supporting_evidence"]]
        decision = {"kind": "synthesis",
            "thesis": ("Atlas release ownership mechanism." if atlas
                       else "Cobalt renewal procurement mechanism."),
            "mechanism": ("Certificate ownership handoff caused the release delay."
                          if atlas else
                          "Missing customer procurement approval blocked the renewal signature."),
            "cause_condition_handles": causes, "effect_handles": ["O3"],
            "supporting_evidence_handles": support,
            "counterevidence": [{"handle": "O2", "bearing": "weakens",
                                  "explanation": "Optimistic status conflicts."}],
            "strongest_alternative": {"thesis": "Status noise explains timing.",
                "mechanism": "A status artifact could correlate with timing.",
                "supporting_handles": [], "why_weaker": "Authoritative records disagree."},
            "novelty": {"classification": "novel", "relative_to_model_handles": [],
                        "explanation": "No accepted composite states this mechanism."},
            "confidence": .82, "falsifying_evidence": ["An earlier completed approval."],
            "relation": {"relation_kind": "causes" if atlas else "blocks",
                         "source_handles": ["M1"], "target": "synthesis_output",
                         "direction": "source_to_target", "explanation": "Causal sequence."}}
    return {"schema_version": "think-synthesis-decision-v1", "dossier_id": dossier_id,
            "dossier_digest": digest, "decision": decision}


@pytest.mark.asyncio
async def test_preregistered_attempt_counts_concurrency_artifacts_and_selection(tmp_path) -> None:
    active = 0
    maximum = 0
    calls = []

    async def provider(spec, payload):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        calls.append(spec.attempt_id)
        await asyncio.sleep(.001)
        active -= 1
        return ProviderAttempt(raw_decision=_decision(dict(payload)), input_tokens=900,
                               output_tokens=100, latency_ms=10, cost_usd=.01)

    policies = tuple(
        ArmPolicy(arm=row.arm, interface=row.interface, policy=row.policy,
                  estimated_cost_per_thousand_tokens={"A": .01, "B": .005, "C": .02}[row.arm])
        for row in default_arm_policies()
    )
    artifact = await run_ti3_experiment(
        output_root=tmp_path, run_id="provider-free-r1", provider=provider,
        commit="test-commit",
        arm_policies=policies, quality_tolerance=.03, max_concurrency=3,
    )
    assert artifact["screening_attempt_count"] == 9
    assert artifact["confirmation_attempt_count"] == 12
    assert len(calls) == len(set(calls)) == 21
    assert maximum <= 3
    assert artifact["confirmation_arms"] == ["A", "B"]
    assert artifact["selected_arm"] == "B"
    assert len(list((tmp_path / "ti3/provider-free-r1/attempts").iterdir())) == 21
    assert verify_experiment_artifact(tmp_path, artifact)


@pytest.mark.asyncio
async def test_arm_a_can_ingest_frozen_baseline_without_provider_call(tmp_path) -> None:
    called = 0

    async def provider(_spec, payload):
        nonlocal called
        called += 1
        return ProviderAttempt(raw_decision=_decision(dict(payload)), input_tokens=1,
                               output_tokens=1, latency_ms=1, cost_usd=0)

    from lib.evaluation.epistemic_repair.ti3_frozen_dossiers import build_frozen_dossier_cases
    baselines = {case.case_id: ProviderAttempt(
        raw_decision={"decision": _decision(dict(case.provider_payload))["decision"],
                      "legacy_diagnostic": True}, input_tokens=10,
        output_tokens=2, latency_ms=1, cost_usd=0, source="frozen_baseline",
        compiler_artifact={"accepted": True, "legacy_compiler_receipt": True},
    ) for case in build_frozen_dossier_cases()}
    await run_ti3_experiment(output_root=tmp_path, run_id="baseline-r1", provider=provider,
                             commit="test-commit",
                             arm_a_baselines=baselines)
    assert called == 18


def test_artifact_tampering_and_policy_gate_fail_closed(tmp_path) -> None:
    policies = default_arm_policies()
    with pytest.raises(ValueError, match="no arm satisfies"):
        select_cheapest_within_tolerance(
            {"A": {"hard_gate_pass_rate": 0, "quality_score": 1}},
            policies=policies, quality_tolerance=.1,
        )


@pytest.mark.asyncio
async def test_byte_tamper_invalidates_experiment(tmp_path) -> None:
    async def provider(_spec, payload):
        return ProviderAttempt(raw_decision=_decision(dict(payload)), input_tokens=1,
                               output_tokens=1, latency_ms=1, cost_usd=0)

    artifact = await run_ti3_experiment(
        output_root=tmp_path, run_id="tamper-r1", provider=provider,
        commit="test-commit",
    )
    target = next((tmp_path / "ti3/tamper-r1/attempts").glob("*/raw-response.json"))
    value = json.loads(target.read_text())
    value["tampered"] = True
    target.write_text(json.dumps(value))
    assert not verify_experiment_artifact(tmp_path, artifact)
