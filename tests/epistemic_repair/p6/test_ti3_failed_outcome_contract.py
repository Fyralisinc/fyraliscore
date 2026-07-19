from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.ti3_frozen_dossiers import (
    build_fixture_manifest,
    build_frozen_dossier_cases,
)
from services.evaluation.epistemic_repair.think_ti3_experiment import (
    ProviderAttempt,
    _capture_request,
    _evaluate_attempt,
    _legacy_candidate_id,
    _legacy_request,
    _scorer_case,
    _spec,
    default_arm_policies,
)


def _invalid_no_op_attempt(capture, case) -> tuple[ProviderAttempt, str, dict]:
    raw = {
        "decisions": [
            {
                "candidate_id": _legacy_candidate_id(case),
                # This is intentionally invalid: no_op is an operation only.
                "decision": "no_op",
                "operation": "no_op",
                "confidence": 0.2,
                "reason": "No mutation is warranted.",
            }
        ],
        "prior_memory_effects": [],
    }
    raw_text = json.dumps(raw, indent=2, sort_keys=False)
    cognition_payload = {
        "structured_text": raw_text,
        "raw_digest": canonical_sha256(raw_text),
        "parse_outcome": "accepted",
    }
    attempt = ProviderAttempt(
        raw_decision=raw,
        input_tokens=100,
        output_tokens=20,
        latency_ms=5,
        cost_usd=0.01,
        source="provider",
        validation_status="not_run",
        apply_status="not_run",
        partial_write_count=None,
        validator_applier_failure_count=None,
        attempt_id=capture.attempt_id,
        model=capture.model,
        effort=capture.effort,
        prompt_digest=capture.prompt_digest,
        schema_digest=capture.schema_digest,
        physical_attempt_ids=[f"physical:{capture.attempt_id}"],
        physical_attempt_count=1,
        physical_outcomes=["parse_failure"],
        logical_outcome_id=capture.attempt_id,
        logical_outcome_count=1,
        logical_outcome="exhausted",
        parse_outcome="parse_failure",
        cognition_event_digest=canonical_sha256(cognition_payload),
        cognition_event_payload=cognition_payload,
        cognition_raw_text_digest=canonical_sha256(raw_text),
        accepted_raw_digest=canonical_sha256(raw),
        usage_exactness="reported",
        provider="codex",
        provider_config_effort_digest=canonical_sha256(
            {"provider": "codex", "model": capture.model, "effort": capture.effort}
        ),
    )
    return attempt, raw_text, raw


def test_invalid_decision_no_op_is_preserved_and_scored_red_schema_binding(
    tmp_path, monkeypatch
) -> None:
    case = build_frozen_dossier_cases()[0]
    policy = default_arm_policies()[0]
    spec = _spec("failed-outcome-audit", "screening", "A", case.case_id, 0)
    legacy_request = _legacy_request(case)
    capture = _capture_request(
        spec, case=case, policy=policy, legacy_request=legacy_request
    )
    attempt, raw_text, raw = _invalid_no_op_attempt(capture, case)

    frozen_before = {
        "system_prompt": capture.system_prompt,
        "user_prompt": capture.user_prompt,
        "prompt_digest": capture.prompt_digest,
        "schema": capture.json_schema,
        "schema_digest": capture.schema_digest,
        "model": capture.model,
        "effort": capture.effort,
        "policy": policy.model_dump(mode="json"),
        "gold": _scorer_case(case).model_dump(mode="json"),
        "gold_digest": _scorer_case(case).content_digest,
        "fixture_manifest": build_fixture_manifest(),
    }

    def forbidden_historical_read(*_args, **_kwargs):
        raise AssertionError("failed original run must never be read or substituted")

    monkeypatch.setattr(
        "services.evaluation.epistemic_repair.think_ti3_experiment."
        "load_historical_atlas_baseline",
        forbidden_historical_read,
    )
    outcome = _evaluate_attempt(
        spec,
        case=case,
        policy=policy,
        attempt=attempt,
        output_root=tmp_path,
        commit="audit-commit",
        legacy_request=legacy_request,
        historical_substitution=False,
        capture=capture,
    )

    assert outcome.execution_source == "physical_call"
    assert outcome.result.verdict == "red"
    assert outcome.result.failure_class == "schema_binding"
    assert outcome.result.hard_gates.handles_resolved is False

    attempt_dir = next((tmp_path / "ti3/failed-outcome-audit/attempts").iterdir())
    persisted_raw = json.loads((attempt_dir / "raw-response.json").read_text())
    compiler = json.loads((attempt_dir / "compiler.json").read_text())
    receipt = json.loads((attempt_dir / "capture-receipt.json").read_text())
    persisted_prompt = json.loads((attempt_dir / "prompt.json").read_text())

    assert persisted_raw == raw
    assert persisted_raw["decisions"][0]["decision"] == "no_op"
    assert compiler["accepted"] is False
    assert compiler["error_type"] == "ProviderSchemaParseFailure"
    assert receipt["raw_digest"] == canonical_sha256(raw)
    assert attempt.accepted_raw_digest == canonical_sha256(raw)
    assert receipt["cognition_raw_text_digest"] == canonical_sha256(raw_text)
    assert receipt["cognition_event_payload"]["structured_text"] == raw_text
    assert receipt["cognition_event_payload"]["raw_digest"] == canonical_sha256(
        raw_text
    )
    assert persisted_prompt == {
        "system": capture.system_prompt,
        "user": capture.user_prompt,
        "schema_name": capture.schema_name,
        "json_schema": capture.json_schema,
    }

    frozen_after = {
        "system_prompt": capture.system_prompt,
        "user_prompt": capture.user_prompt,
        "prompt_digest": capture.prompt_digest,
        "schema": capture.json_schema,
        "schema_digest": capture.schema_digest,
        "model": capture.model,
        "effort": capture.effort,
        "policy": policy.model_dump(mode="json"),
        "gold": _scorer_case(case).model_dump(mode="json"),
        "gold_digest": _scorer_case(case).content_digest,
        "fixture_manifest": build_fixture_manifest(),
    }
    assert frozen_after == frozen_before


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {
                "physical_attempt_count": 2,
                "physical_attempt_ids": ["physical:one", "physical:retry"],
            },
            "exactly one physical attempt",
        ),
        ({"physical_outcomes": ["cache_hit"]}, "outcomes disagree"),
        (
            {
                "logical_outcome_count": 0,
                "logical_outcome_id": "missing",
            },
            "exactly one joined logical receipt",
        ),
        ({"cognition_event_payload": {}}, "content digest mismatch"),
        ({"accepted_raw_digest": "0" * 64}, "raw digest mismatch"),
    ],
)
def test_invalid_outcome_retries_cache_missing_receipts_and_tamper_fail_closed(
    changes, message
) -> None:
    case = build_frozen_dossier_cases()[0]
    policy = default_arm_policies()[0]
    spec = _spec("failed-outcome-audit", "screening", "A", case.case_id, 0)
    capture = _capture_request(
        spec, case=case, policy=policy, legacy_request=_legacy_request(case)
    )
    attempt, _, _ = _invalid_no_op_attempt(capture, case)

    with pytest.raises(ValidationError, match=message):
        ProviderAttempt.model_validate({**attempt.model_dump(), **changes})
