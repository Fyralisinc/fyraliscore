from __future__ import annotations

import asyncio
import json

import pytest

from lib.contracts.kernel import canonical_sha256
from services.evaluation.epistemic_repair.think_ti3_experiment import (
    ArmPolicy,
    HistoricalBaselineBinding,
    ProviderAttempt,
    _capture_request,
    _legacy_request,
    _spec,
    _validate_capture_match,
    default_arm_policies,
    run_ti3_experiment,
    select_cheapest_within_tolerance,
    verify_experiment_artifact,
)
from lib.evaluation.epistemic_repair.ti3_frozen_dossiers import build_frozen_dossier_cases
from uuid import NAMESPACE_URL, uuid5


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


def _legacy_decision(case_id: str) -> dict:
    if case_id == "null_adversarial_v1":
        return {"decisions": [{"candidate_id": f"MDC_TI3_{case_id}",
            "decision": "reject", "operation": "no_op", "confidence": .9,
            "reason": "No authorized record explains the schedule change."}],
            "prior_memory_effects": []}
    atlas = case_id == "atlas_positive_v1"
    def model(handle: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"ti3:{case_id}:{handle}"))
    return {"decisions": [{"candidate_id": f"MDC_TI3_{case_id}",
        "decision": "accept", "operation": "situation_and_edge", "confidence": .8,
        "claim_role": "situation",
        "claim_text": ("Atlas release ownership mechanism." if atlas
                       else "Cobalt renewal procurement mechanism."),
        "situation_member_model_ids": [model("M1"), model("M2")],
        "source_model_id": model("M1"), "target_model_id": model("M2"),
        "edge_kind": "causes" if atlas else "blocks",
        "reason": ("Certificate ownership handoff caused the release delay."
                   if atlas else
                   "Missing customer procurement approval blocked the renewal signature.")
        }], "prior_memory_effects": []}


def _attempt(capture) -> ProviderAttempt:
    cases = {case.case_id: case for case in build_frozen_dossier_cases()}
    raw = (_legacy_decision(capture.case_id) if capture.interface == "legacy_isolated"
           else _decision(dict(cases[capture.case_id].provider_payload)))
    return ProviderAttempt(raw_decision=raw, input_tokens=900, output_tokens=100,
        latency_ms=10, cost_usd=.01, attempt_id=capture.attempt_id,
        apply_facts={"applied": False, "reason": "experiment_compile_only",
                     "partial_write_count": 0, "validator_applier_failure_count": 0},
        model=capture.model, effort=capture.effort,
        prompt_digest=capture.prompt_digest, schema_digest=capture.schema_digest)


@pytest.mark.asyncio
async def test_preregistered_attempt_counts_concurrency_artifacts_and_selection(tmp_path) -> None:
    active = 0
    maximum = 0
    calls = []

    captures = []
    async def provider(capture):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        calls.append(capture.attempt_id)
        captures.append(capture)
        await asyncio.sleep(.001)
        active -= 1
        return _attempt(capture)

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
    assert artifact["confirmation_arms"] == ["B", "C"]
    assert artifact["selected_arm"] == "B"
    assert len(list((tmp_path / "ti3/provider-free-r1/attempts").iterdir())) == 21
    assert verify_experiment_artifact(tmp_path, artifact)
    arm_a = next(row for row in captures if row.arm == "A")
    arm_b = next(row for row in captures if row.arm == "B")
    arm_c = next(row for row in captures if row.arm == "C")
    assert arm_a.schema_name == "BatchMemoryDecisionSet"
    assert arm_b.schema_name == "SynthesisDecisionEnvelope"
    assert arm_a.prompt_digest != arm_b.prompt_digest
    assert arm_a.schema_digest != arm_b.schema_digest
    assert arm_a.model == arm_b.model == arm_c.model == "gpt-5.3-codex-spark"
    assert arm_a.effort == arm_b.effort == "medium"
    assert arm_c.effort == "high"
    for capture in captures:
        prompt = capture.system_prompt + capture.user_prompt
        assert "required_mechanism_facets" not in prompt
        assert "expected_decision" not in prompt
        assert "forbidden_claims" not in prompt


@pytest.mark.asyncio
async def test_arm_a_can_ingest_frozen_baseline_without_provider_call(tmp_path) -> None:
    called = 0

    async def provider(capture):
        nonlocal called
        called += 1
        return _attempt(capture)

    atlas = build_frozen_dossier_cases()[0]
    policy = default_arm_policies()[0]
    capture = _capture_request(
        _spec("baseline-r1", "screening", "A", atlas.case_id, 0),
        case=atlas, policy=policy, legacy_request=_legacy_request(atlas),
    )
    raw = _legacy_decision(atlas.case_id)
    evidence = {"compiled_raw_diff": {"claim_ops": [], "relation_claim_ops": []},
                "apply_facts": {"applied": True, "partial_write_count": 0,
                                "validator_applier_failure_count": 0}}
    report = {"run_provenance": {"git_commit": "43dcb197abcdef"},
              "expected_llm_configuration": {"model": "gpt-5.3-codex-spark",
                                              "effort": "medium"},
              "database_trace": {"database": "historical", "tenant": "sealed",
                                 "model": "gpt-5.3-codex-spark", "effort": "medium"},
              "capture_receipt": {"prompt_digest": capture.prompt_digest,
                                  "schema_digest": capture.schema_digest},
              "usage": {"input_tokens": 10, "output_tokens": 2}}
    paths = {}
    for name, value in (("raw", raw), ("evidence", evidence), ("report", report)):
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(value))
        paths[name] = path
    baseline = HistoricalBaselineBinding(
        raw_path=paths["raw"], evidence_path=paths["evidence"], report_path=paths["report"],
        raw_digest=canonical_sha256(raw), evidence_digest=canonical_sha256(evidence),
        report_digest=canonical_sha256(report),
    )
    artifact = await run_ti3_experiment(output_root=tmp_path, run_id="baseline-r1", provider=provider,
                             commit="test-commit",
                             historical_atlas_baseline=baseline)
    assert called == 20
    assert artifact["planned_outcome_count"] == 21
    assert artifact["physical_call_count"] == 20
    assert artifact["historical_substitution_count"] == 1
    historical_manifest = next(
        json.loads(path.read_text()) for path in
        (tmp_path / "ti3/baseline-r1/attempts").glob("*/manifest.json")
        if json.loads(path.read_text())["execution_source"] == "historical_substitution"
    )
    historical_dir = next(
        path.parent for path in (tmp_path / "ti3/baseline-r1/attempts").glob("*/manifest.json")
        if json.loads(path.read_text())["content_digest"] == historical_manifest["content_digest"]
    )
    assert json.loads((historical_dir / "raw-response.json").read_text()) == raw
    assert json.loads(paths["raw"].read_text()) == raw


def test_capture_receipt_rejects_model_effort_and_prompt_mismatch() -> None:
    case = build_frozen_dossier_cases()[0]
    policy = default_arm_policies()[1]
    capture = _capture_request(
        _spec("r", "screening", "B", case.case_id, 0),
        case=case, policy=policy, legacy_request=None,
    )
    attempt = _attempt(capture).model_copy(update={"effort": "high"})
    with pytest.raises(ValueError, match="model or effort mismatch"):
        _validate_capture_match(capture, attempt, historical=False)
    attempt = _attempt(capture).model_copy(update={"prompt_digest": "0" * 64})
    with pytest.raises(ValueError, match="prompt or schema mismatch"):
        _validate_capture_match(capture, attempt, historical=False)


def test_artifact_tampering_and_policy_gate_fail_closed(tmp_path) -> None:
    policies = default_arm_policies()
    with pytest.raises(ValueError, match="no arm satisfies"):
        select_cheapest_within_tolerance(
            {"A": {"hard_gate_pass_rate": 0, "quality_score": 1}},
            policies=policies, quality_tolerance=.1,
        )


@pytest.mark.asyncio
async def test_byte_tamper_invalidates_experiment(tmp_path) -> None:
    async def provider(capture):
        return _attempt(capture)

    artifact = await run_ti3_experiment(
        output_root=tmp_path, run_id="tamper-r1", provider=provider,
        commit="test-commit",
    )
    target = next((tmp_path / "ti3/tamper-r1/attempts").glob("*/raw-response.json"))
    value = json.loads(target.read_text())
    value["tampered"] = True
    target.write_text(json.dumps(value))
    assert not verify_experiment_artifact(tmp_path, artifact)
