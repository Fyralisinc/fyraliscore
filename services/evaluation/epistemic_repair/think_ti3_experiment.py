"""Bounded, provider-injected TI3 three-dossier experiment orchestration."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.ti3_frozen_dossiers import (
    FrozenDossierCase,
    build_fixture_manifest,
    build_frozen_dossier_cases,
)
from services.reasoning.think.synthesis_contract import (
    HandleBinding,
    SynthesisCompileContext,
    SynthesisDecisionEnvelope,
    compile_synthesis_decision,
)
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryDecisionSet,
    CompiledBatchMemoryDecisionRequest,
    build_compiled_batch_memory_decision_request,
)

from .think_policy_receipts import PolicyIdentity, build_evaluation_receipt
from .think_semantic_scorer import (
    ExecutionEvidence,
    SemanticScorerCase,
    SemanticScorerResult,
    score_legacy_compiled_decision,
    score_semantic_decision,
)


ArmName = Literal["A", "B", "C"]
Phase = Literal["screening", "confirmation"]
AttemptProvider = Callable[["CaptureRequest"], Awaitable["ProviderAttempt"]]


class ArmPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    arm: ArmName
    interface: Literal["legacy_isolated", "synthesis_decision_v1"]
    policy: PolicyIdentity
    estimated_cost_per_thousand_tokens: float = Field(ge=0)


class ProviderAttempt(BaseModel):
    model_config = ConfigDict(extra="forbid")
    raw_decision: dict[str, Any]
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    latency_ms: float = Field(ge=0)
    cost_usd: float = Field(ge=0)
    source: Literal["provider", "frozen_baseline"] = "provider"
    compiler_artifact: dict[str, Any] | None = None
    validation_status: Literal["success", "failure", "not_run"]
    apply_status: Literal["success", "failure", "not_run"]
    partial_write_count: int | None = Field(default=None, ge=0)
    validator_applier_failure_count: int | None = Field(default=None, ge=0)
    attempt_id: str
    model: str
    effort: Literal["medium", "high"]
    prompt_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    physical_attempt_ids: list[str]
    physical_attempt_count: int = Field(ge=0)
    physical_outcomes: list[str]
    logical_outcome_id: str
    logical_outcome_count: int = Field(ge=0)
    logical_outcome: Literal["success"]
    parse_outcome: Literal["accepted"]
    cognition_event_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    cognition_event_payload: dict[str, Any]
    cognition_raw_text_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    accepted_raw_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    usage_exactness: Literal["reported"]
    provider: str
    provider_config_effort_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def one_physical_receipt(self) -> "ProviderAttempt":
        if self.physical_attempt_count != 1 or len(self.physical_attempt_ids) != 1:
            raise ValueError("TI3 outcome requires exactly one physical attempt")
        if self.physical_outcomes != ["success"]:
            raise ValueError("TI3 physical attempt must succeed without retry")
        if self.logical_outcome_count != 1:
            raise ValueError("TI3 outcome requires exactly one joined logical receipt")
        if self.source == "provider" and self.logical_outcome_id != self.attempt_id:
            raise ValueError("TI3 provider attempt/logical receipt mismatch")
        if self.accepted_raw_digest != canonical_sha256(self.raw_decision):
            raise ValueError("TI3 accepted raw digest mismatch")
        if canonical_sha256(self.cognition_event_payload) != self.cognition_event_digest:
            raise ValueError("TI3 cognition event content digest mismatch")
        if self.cognition_event_payload.get("raw_digest") != self.cognition_raw_text_digest:
            raise ValueError("TI3 cognition raw text digest mismatch")
        expected = canonical_sha256({"provider": self.provider, "model": self.model,
                                     "effort": self.effort})
        if self.provider_config_effort_digest != expected:
            raise ValueError("TI3 provider/model/effort digest mismatch")
        return self


class CaptureRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    attempt_id: str
    case_id: str
    phase: Phase
    sample_index: int
    arm: ArmName
    interface: Literal["legacy_isolated", "synthesis_decision_v1"]
    model: str
    effort: Literal["medium", "high"]
    system_prompt: str
    user_prompt: str
    schema_name: Literal["BatchMemoryDecisionSet", "SynthesisDecisionEnvelope"]
    json_schema: dict[str, Any]
    prompt_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class HistoricalBaselineBinding:
    raw_path: Path
    evidence_path: Path
    report_path: Path
    raw_digest: str
    evidence_digest: str
    report_digest: str
    commit: str = "43dcb197"
    model: str = "gpt-5.3-codex-spark"
    effort: Literal["medium"] = "medium"


@dataclass(frozen=True, slots=True)
class AttemptSpec:
    run_id: str
    phase: Phase
    arm: ArmName
    case_id: str
    sample_index: int
    attempt_id: str


class AttemptOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")
    spec: dict[str, Any]
    arm_policy_digest: str
    result: SemanticScorerResult
    receipt_digest: str
    artifact_manifest_digest: str
    execution_source: Literal["physical_call", "historical_substitution"]
    physical_attempt_count: int
    logical_outcome_count: int


def default_arm_policies() -> tuple[ArmPolicy, ...]:
    common = dict(provider_schema_version="think-synthesis-decision-v1",
                  compiler_version="ti2-v1", routing_policy_version="ti3-v1")
    return (
        ArmPolicy(arm="A", interface="legacy_isolated",
                  policy=PolicyIdentity(prompt_policy_version="legacy-compiled-v1",
                    model="gpt-5.3-codex-spark", effort="medium", **common),
                  estimated_cost_per_thousand_tokens=0.006),
        ArmPolicy(arm="B", interface="synthesis_decision_v1",
                  policy=PolicyIdentity(prompt_policy_version="dossier-schema-v1",
                    model="gpt-5.3-codex-spark", effort="medium", **common),
                  estimated_cost_per_thousand_tokens=0.006),
        ArmPolicy(arm="C", interface="synthesis_decision_v1",
                  policy=PolicyIdentity(prompt_policy_version="dossier-schema-v1",
                    model="gpt-5.3-codex-spark", effort="high", **common),
                  estimated_cost_per_thousand_tokens=0.012),
    )


async def run_ti3_experiment(
    *, output_root: Path, run_id: str, provider: AttemptProvider,
    commit: str,
    arm_policies: tuple[ArmPolicy, ...] | None = None,
    quality_tolerance: float = 0.03, max_concurrency: int = 3,
    historical_atlas_baseline: HistoricalBaselineBinding | None = None,
) -> dict[str, Any]:
    """Run preregistered 9-call screening then 12-call confirmation."""
    if not 0 <= quality_tolerance <= 1:
        raise ValueError("quality tolerance must be within [0,1]")
    if not 1 <= max_concurrency <= 3:
        raise ValueError("TI3 concurrency must be between one and three")
    run_directory = output_root / "ti3" / run_id
    if run_directory.exists():
        raise FileExistsError(f"TI3 run directory already exists: {run_id}")
    policies = arm_policies or default_arm_policies()
    if {row.arm for row in policies} != {"A", "B", "C"}:
        raise ValueError("TI3 requires exactly arms A, B, and C")
    cases = build_frozen_dossier_cases()
    semaphore = asyncio.Semaphore(max_concurrency)

    screening_specs = tuple(
        _spec(run_id, "screening", policy.arm, case.case_id, 0)
        for case in cases for policy in policies
    )
    screening = await _run_specs(
        screening_specs, cases=cases, policies=policies, provider=provider,
        output_root=output_root, commit=commit, semaphore=semaphore,
        historical_atlas_baseline=historical_atlas_baseline,
    )
    screening_summary = _summarize(screening, policies)
    best_two = _best_arms(screening_summary, count=2)
    confirmation_specs = tuple(
        _spec(run_id, "confirmation", arm, case.case_id, sample)
        for case in cases for arm in best_two for sample in (1, 2)
    )
    confirmation = await _run_specs(
        confirmation_specs, cases=cases, policies=policies, provider=provider,
        output_root=output_root, commit=commit, semaphore=semaphore,
        historical_atlas_baseline=None,
    )
    confirmation_summary = _summarize(confirmation, policies)
    selected = select_cheapest_within_tolerance(
        confirmation_summary, policies=policies, quality_tolerance=quality_tolerance,
    )
    physical_count = sum(row.physical_attempt_count for row in (*screening, *confirmation))
    logical_count = sum(row.logical_outcome_count for row in (*screening, *confirmation))
    if physical_count != 21 or logical_count != 21:
        raise ValueError("TI3 requires exactly 21 physical attempts and logical outcomes")
    body = {
        "schema_version": "think-ti3-experiment-v1", "run_id": run_id,
        "commit": commit,
        "contract_digest": "b1e234eee1cdfaf279a431efda4abe39bb7aff5896d1f1d2de1f0b5fbcb48717",
        "fixture_manifest_digest": build_fixture_manifest()["manifest_digest"],
        "quality_tolerance": quality_tolerance, "max_concurrency": max_concurrency,
        "screening_attempt_count": len(screening), "confirmation_attempt_count": len(confirmation),
        "planned_outcome_count": len(screening) + len(confirmation),
        "physical_call_count": physical_count,
        "current_run_physical_call_count": sum(
            row.physical_attempt_count for row in (*screening, *confirmation)
            if row.execution_source == "physical_call"
        ),
        "logical_outcome_count": logical_count,
        "historical_substitution_count": sum(
            row.execution_source == "historical_substitution"
            for row in (*screening, *confirmation)
        ),
        "screening_summary": screening_summary, "confirmation_summary": confirmation_summary,
        "confirmation_arms": list(best_two), "selected_arm": selected,
        "attempt_manifest_digests": [
            row.artifact_manifest_digest for row in (*screening, *confirmation)
        ],
    }
    artifact = {**body, "content_digest": canonical_sha256(body)}
    _atomic_json(output_root / "ti3" / run_id / "manifest.json", artifact)
    return artifact


async def _run_specs(
    specs: tuple[AttemptSpec, ...], *, cases: tuple[FrozenDossierCase, ...],
    policies: tuple[ArmPolicy, ...], provider: AttemptProvider, output_root: Path,
    commit: str, semaphore: asyncio.Semaphore,
    historical_atlas_baseline: HistoricalBaselineBinding | None,
) -> tuple[AttemptOutcome, ...]:
    case_by_id = {row.case_id: row for row in cases}
    policy_by_arm = {row.arm: row for row in policies}

    async def one(spec: AttemptSpec) -> AttemptOutcome:
        async with semaphore:
            case = case_by_id[spec.case_id]
            policy = policy_by_arm[spec.arm]
            legacy_request = _legacy_request(case) if policy.interface == "legacy_isolated" else None
            capture = _capture_request(spec, case=case, policy=policy,
                                       legacy_request=legacy_request)
            historical = (
                historical_atlas_baseline is not None
                and spec.phase == "screening" and spec.arm == "A"
                and spec.case_id == "atlas_positive_v1"
            )
            attempt = (
                load_historical_atlas_baseline(historical_atlas_baseline, capture=capture)
                if historical else await provider(capture)
            )
            _validate_capture_match(capture, attempt, historical=historical)
            return _evaluate_attempt(
                spec, case=case, policy=policy, attempt=attempt, output_root=output_root,
                commit=commit, legacy_request=legacy_request,
                historical_substitution=historical,
                capture=capture,
            )

    return tuple(await asyncio.gather(*(one(spec) for spec in specs)))


def _evaluate_attempt(
    spec: AttemptSpec, *, case: FrozenDossierCase, policy: ArmPolicy,
    attempt: ProviderAttempt, output_root: Path, commit: str,
    legacy_request: CompiledBatchMemoryDecisionRequest | None,
    historical_substitution: bool,
    capture: CaptureRequest,
) -> AttemptOutcome:
    raw = dict(attempt.raw_decision)
    raw_digest = canonical_sha256(raw)
    compiler_digest: str | None = None
    compiler_accepted = False
    if historical_substitution and attempt.compiler_artifact is not None:
        compiler_artifact = dict(attempt.compiler_artifact)
        compiler_accepted = compiler_artifact.get("accepted", True) is True
        compiler_digest = canonical_sha256(compiler_artifact) if compiler_accepted else None
    elif policy.interface == "legacy_isolated":
        try:
            if legacy_request is None:
                raise ValueError("legacy request missing")
            decisions = BatchMemoryDecisionSet.model_validate(raw)
            trigger = _legacy_trigger(case)
            compiled = legacy_request.to_raw_diff(
                decisions, trigger=trigger,
                trigger_ref=UUID(str(trigger.seed_signature["trigger_id"])),
            )
            compiler_artifact = compiled.model_dump(mode="json")
            compiler_digest = canonical_sha256(compiler_artifact)
            compiler_accepted = True
        except Exception as exc:
            compiler_artifact = {"accepted": False, "error_type": type(exc).__name__,
                                 "error": str(exc)}
    else:
        try:
            envelope = SynthesisDecisionEnvelope.model_validate(raw)
            compiled = compile_synthesis_decision(envelope, context=_compile_context(case))
            compiler_artifact = compiled.model_dump(mode="json")
            compiler_digest = canonical_sha256(compiler_artifact)
            compiler_accepted = True
        except Exception as exc:  # captured as evaluation evidence, never hidden
            compiler_artifact = {"accepted": False, "error_type": type(exc).__name__,
                                 "error": str(exc)}
    scorer_case = _scorer_case(case)
    execution = ExecutionEvidence(
        schema_valid=bool(raw), handles_resolved=compiler_accepted,
        evidence_complete=compiler_accepted, scope_clean=True,
        compiler_accepted=compiler_accepted, unsupported_canonical_relation_count=0,
        validation_status=attempt.validation_status, apply_status=attempt.apply_status,
        partial_write_count=attempt.partial_write_count,
        validator_applier_failure_count=attempt.validator_applier_failure_count,
        compiler_receipt_digest=compiler_digest,
        tokens=attempt.input_tokens + attempt.output_tokens, latency_ms=attempt.latency_ms,
        cost_usd=attempt.cost_usd, consistency=1,
    )
    result = (
        score_legacy_compiled_decision(
            scorer_case, raw, compiled_artifact=compiler_artifact,
            decision_artifact_digest=raw_digest, execution=execution,
            model_handle_by_id=_model_handle_by_id(case),
        ) if policy.interface == "legacy_isolated" else
        score_semantic_decision(
            scorer_case, raw, decision_artifact_digest=raw_digest, execution=execution,
        )
    )
    receipt = build_evaluation_receipt(
        attempt_id=spec.attempt_id, dossier_digest=case.dossier_digest,
        policy=policy.policy, raw_decision_digest=raw_digest, scorer_result=result,
        compiler_receipt_digest=compiler_digest,
    )
    directory = output_root / "ti3" / spec.run_id / "attempts" / spec.attempt_id
    if directory.exists():
        raise FileExistsError(f"attempt directory already exists: {spec.attempt_id}")
    capture_receipt = _capture_receipt(attempt)
    files = {
        "prompt.json": {"system": capture.system_prompt, "user": capture.user_prompt,
                        "schema_name": capture.schema_name,
                        "json_schema": capture.json_schema},
        "raw-response.json": raw,
        "compiler.json": compiler_artifact,
        "applied-result.json": {"validation_status": attempt.validation_status,
            "apply_status": attempt.apply_status,
            "partial_write_count": attempt.partial_write_count,
            "validator_applier_failure_count": attempt.validator_applier_failure_count},
        "score.json": result.model_dump(mode="json"),
        "evaluation-receipt.json": receipt.model_dump(mode="json"),
        "capture-receipt.json": capture_receipt,
    }
    entries = []
    for name, value in files.items():
        path = directory / name
        encoded = _atomic_json(path, value)
        entries.append({"path": name, "content_digest": canonical_sha256(value),
                        "byte_sha256": hashlib.sha256(encoded).hexdigest(),
                        "sensitivity": "evaluation"})
    manifest_body = {
        "schema_version": "think-cognition-artifact-manifest-v1",
        "commit": commit,
        "contract_digest": "b1e234eee1cdfaf279a431efda4abe39bb7aff5896d1f1d2de1f0b5fbcb48717",
        "attempt_id": spec.attempt_id, "case_id": spec.case_id, "arm": spec.arm,
        "phase": spec.phase, "sample_index": spec.sample_index,
        "logical_call_id": spec.attempt_id, "trace_id": spec.attempt_id,
        "created_at": receipt.evaluated_at.isoformat(),
        "execution_source": ("historical_substitution" if historical_substitution
                             else "physical_call"),
        "capture_request_digest": canonical_sha256(capture_receipt),
        "policy_digest": policy.policy.content_digest, "files": entries,
    }
    manifest = {**manifest_body, "content_digest": canonical_sha256(manifest_body)}
    _atomic_json(directory / "manifest.json", manifest)
    return AttemptOutcome(
        spec=_spec_dict(spec), arm_policy_digest=policy.policy.content_digest,
        result=result, receipt_digest=receipt.content_digest,
        artifact_manifest_digest=manifest["content_digest"],
        execution_source=("historical_substitution" if historical_substitution
                          else "physical_call"),
        physical_attempt_count=attempt.physical_attempt_count,
        logical_outcome_count=attempt.logical_outcome_count,
    )


def select_cheapest_within_tolerance(
    summary: Mapping[str, Mapping[str, Any]], *, policies: tuple[ArmPolicy, ...],
    quality_tolerance: float,
) -> ArmName:
    eligible = {arm: row for arm, row in summary.items()
                if float(row["isolated_ti3_gate_pass_rate"]) == 1.0}
    if not eligible:
        raise ValueError("no arm satisfies noncompensatory hard gates")
    best = max(float(row["quality_score"]) for row in eligible.values())
    within = {arm for arm, row in eligible.items()
              if float(row["quality_score"]) >= best - quality_tolerance}
    costs = {row.arm: row.estimated_cost_per_thousand_tokens for row in policies}
    return min(within, key=lambda arm: (costs[arm], arm))  # type: ignore[return-value]


def verify_experiment_artifact(output_root: Path, artifact: Mapping[str, Any]) -> bool:
    body = dict(artifact)
    digest = body.pop("content_digest", None)
    if digest != canonical_sha256(body):
        return False
    base = output_root / "ti3" / str(artifact.get("run_id")) / "attempts"
    for attempt_digest in artifact.get("attempt_manifest_digests") or ():
        matches = list(base.glob("*/manifest.json"))
        found = False
        for path in matches:
            manifest = json.loads(path.read_text(encoding="utf-8"))
            manifest_body = dict(manifest)
            embedded = manifest_body.pop("content_digest", None)
            if embedded != attempt_digest or embedded != canonical_sha256(manifest_body):
                continue
            if all(_verify_file(path.parent, entry) for entry in manifest["files"]):
                found = True
                break
        if not found:
            return False
    return True


def load_historical_atlas_baseline(
    binding: HistoricalBaselineBinding | None, *, capture: CaptureRequest,
) -> ProviderAttempt:
    """Load the sealed Atlas diagnostic as-is and verify all three bound artifacts."""
    if binding is None:
        raise ValueError("historical Atlas baseline binding is missing")
    raw = _read_bound_json(binding.raw_path, binding.raw_digest, "raw")
    evidence = _read_bound_json(binding.evidence_path, binding.evidence_digest, "evidence")
    report = _read_bound_json(binding.report_path, binding.report_digest, "report")
    provenance = dict(report.get("run_provenance") or {})
    configuration = dict(report.get("expected_llm_configuration") or
                         report.get("llm_configuration") or {})
    trace = dict(report.get("database_trace") or report.get("db_trace") or {})
    sealed = dict(report.get("capture_receipt") or {})
    if not str(provenance.get("git_commit") or "").startswith(binding.commit):
        raise ValueError("historical baseline commit mismatch")
    if configuration.get("model") != binding.model or configuration.get("effort") != binding.effort:
        raise ValueError("historical baseline model or effort mismatch")
    if not trace:
        raise ValueError("historical baseline lacks database trace")
    if trace.get("model") != binding.model or trace.get("effort") != binding.effort:
        raise ValueError("historical database trace model or effort mismatch")
    if (sealed.get("prompt_digest") != capture.prompt_digest
            or sealed.get("schema_digest") != capture.schema_digest):
        raise ValueError("historical baseline prompt or schema mismatch")
    compiled = evidence.get("compiled_raw_diff") or report.get("compiled_raw_diff")
    if not isinstance(compiled, Mapping):
        raise ValueError("historical baseline lacks native compiled RawDiff evidence")
    apply_facts = evidence.get("apply_facts") or report.get("apply_facts")
    if not isinstance(apply_facts, Mapping):
        raise ValueError("historical baseline lacks apply facts")
    native_raw = raw.get("raw_response", raw)
    if not isinstance(native_raw, Mapping):
        raise ValueError("historical baseline raw response is not an object")
    usage = dict(report.get("usage") or {})
    physical = list(report.get("physical_attempt_receipts") or ())
    logical = list(report.get("logical_call_receipts") or ())
    cognition = dict(report.get("accepted_cognition_event") or {})
    cognition_payload = dict(cognition.get("payload") or {})
    if len(physical) != 1 or len(logical) != 1:
        raise ValueError("historical baseline requires one physical and logical receipt")
    historical_logical_id = str(logical[0].get("logical_call_id") or "")
    if (not historical_logical_id
            or str(physical[0].get("logical_call_id") or "") != historical_logical_id
            or str(cognition.get("logical_call_id") or "") != historical_logical_id
            or str(cognition.get("physical_attempt_id") or "")
               != str(physical[0].get("physical_attempt_id") or "")):
        raise ValueError("historical physical/logical/cognition receipt join mismatch")
    receipt_model = str(physical[0].get("model") or "")
    if receipt_model != binding.model:
        raise ValueError("historical physical receipt model mismatch")
    structured_text = cognition_payload.get("structured_text")
    if not isinstance(structured_text, str):
        raise ValueError("historical cognition event lacks structured raw text")
    if cognition_payload.get("raw_digest") != canonical_sha256(structured_text):
        raise ValueError("historical cognition raw text digest mismatch")
    if json.loads(structured_text) != dict(native_raw):
        raise ValueError("historical cognition text/accepted object mismatch")
    return ProviderAttempt(
        raw_decision=dict(native_raw), compiler_artifact=dict(compiled),
        validation_status=str(apply_facts.get("validation_status") or "not_run"),
        apply_status=str(apply_facts.get("apply_status") or "not_run"),
        partial_write_count=apply_facts.get("partial_write_count"),
        validator_applier_failure_count=apply_facts.get("validator_applier_failure_count"),
        input_tokens=int(usage.get("input_tokens") or 0),
        output_tokens=int(usage.get("output_tokens") or 0),
        latency_ms=float(usage.get("latency_ms") or 0),
        cost_usd=float(usage.get("cost_usd") or 0), source="frozen_baseline",
        attempt_id=historical_logical_id, model=receipt_model, effort=binding.effort,
        prompt_digest=capture.prompt_digest, schema_digest=capture.schema_digest,
        physical_attempt_ids=[str(physical[0].get("physical_attempt_id"))],
        physical_attempt_count=1, physical_outcomes=[str(physical[0].get("outcome"))],
        logical_outcome_id=historical_logical_id, logical_outcome_count=1,
        logical_outcome=str(logical[0].get("outcome")), parse_outcome="accepted",
        cognition_event_digest=str(cognition.get("content_digest")),
        cognition_event_payload=cognition_payload,
        cognition_raw_text_digest=str(cognition_payload.get("raw_digest")),
        accepted_raw_digest=canonical_sha256(dict(native_raw)),
        usage_exactness=str(physical[0].get("usage_exactness")),
        provider=str(physical[0].get("provider")),
        provider_config_effort_digest=canonical_sha256({
            "provider": str(physical[0].get("provider")), "model": binding.model,
            "effort": binding.effort,
        }),
    )


def _read_bound_json(path: Path, expected: str, label: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or canonical_sha256(value) != expected:
        raise ValueError(f"historical baseline {label} digest mismatch")
    return value


def _capture_request(
    spec: AttemptSpec, *, case: FrozenDossierCase, policy: ArmPolicy,
    legacy_request: CompiledBatchMemoryDecisionRequest | None,
) -> CaptureRequest:
    if policy.interface == "legacy_isolated":
        if legacy_request is None:
            raise ValueError("legacy request builder returned no request")
        system, user = legacy_request.system, legacy_request.user
        schema_name = "BatchMemoryDecisionSet"
        schema = BatchMemoryDecisionSet.model_json_schema()
    else:
        system = (
            "Analyze one closed company-learning dossier. Return exactly one "
            "SynthesisProposal or AbstentionDecision using only local handles."
        )
        user = json.dumps(case.provider_payload, sort_keys=True, separators=(",", ":"))
        schema_name = "SynthesisDecisionEnvelope"
        schema = SynthesisDecisionEnvelope.model_json_schema()
    prompt_digest = canonical_sha256({"system": system, "user": user})
    schema_digest = canonical_sha256(schema)
    return CaptureRequest(
        attempt_id=spec.attempt_id, arm=spec.arm, interface=policy.interface,
        case_id=spec.case_id,
        phase=spec.phase, sample_index=spec.sample_index,
        model=policy.policy.model, effort=policy.policy.effort,
        system_prompt=system, user_prompt=user, schema_name=schema_name,
        json_schema=schema, prompt_digest=prompt_digest, schema_digest=schema_digest,
    )


def _validate_capture_match(
    capture: CaptureRequest, attempt: ProviderAttempt, *, historical: bool,
) -> None:
    if attempt.model != capture.model or attempt.effort != capture.effort:
        raise ValueError("provider receipt model or effort mismatch")
    if attempt.prompt_digest != capture.prompt_digest or attempt.schema_digest != capture.schema_digest:
        raise ValueError("provider receipt prompt or schema mismatch")
    if not historical and attempt.attempt_id != capture.attempt_id:
        raise ValueError("provider receipt attempt mismatch")
    if historical != (attempt.source == "frozen_baseline"):
        raise ValueError("provider receipt execution source mismatch")


def _capture_receipt(attempt: ProviderAttempt) -> dict[str, Any]:
    return {"attempt_id": attempt.attempt_id, "model": attempt.model,
            "effort": attempt.effort, "prompt_digest": attempt.prompt_digest,
            "schema_digest": attempt.schema_digest, "source": attempt.source,
            "raw_digest": canonical_sha256(attempt.raw_decision),
            "physical_attempt_ids": attempt.physical_attempt_ids,
            "physical_attempt_count": attempt.physical_attempt_count,
            "physical_outcomes": attempt.physical_outcomes,
            "logical_outcome_id": attempt.logical_outcome_id,
            "logical_outcome_count": attempt.logical_outcome_count,
            "logical_outcome": attempt.logical_outcome,
            "parse_outcome": attempt.parse_outcome,
            "cognition_event_digest": attempt.cognition_event_digest,
            "cognition_event_payload": attempt.cognition_event_payload,
            "cognition_raw_text_digest": attempt.cognition_raw_text_digest,
            "usage_exactness": attempt.usage_exactness, "provider": attempt.provider,
            "provider_config_effort_digest": attempt.provider_config_effort_digest,
            "usage": {"input_tokens": attempt.input_tokens,
                      "output_tokens": attempt.output_tokens,
                      "latency_ms": attempt.latency_ms, "cost_usd": attempt.cost_usd}}


def _legacy_trigger(case: FrozenDossierCase) -> TriggerContext:
    context = _compile_context(case)
    observations = sorted(context.trigger_observation_ids, key=str)
    trigger_id = uuid5(NAMESPACE_URL, f"ti3:{case.case_id}:legacy-trigger")
    return TriggerContext(
        kind="T1", tenant_id=context.tenant_id, observation_id=observations[0],
        observation_ids=observations,
        seed_natural_text=str(case.provider_payload["scope"]["display_label"]),
        seed_signature={"trigger_id": str(trigger_id)},
    )


def _legacy_request(case: FrozenDossierCase) -> CompiledBatchMemoryDecisionRequest:
    context = _compile_context(case)
    models = [row for row in context.bindings if row.object_kind == "accepted_model_head"]
    observations = [row for row in context.bindings if row.object_kind == "observation"]
    text_by_handle = {
        str(row["handle"]): str(dict(row.get("semantic_content") or {}).get("text") or "")
        for row in case.provider_payload["handles"]
    }
    candidate = {
        "candidate_id": _legacy_candidate_id(case), "candidate_kind": "synthesis",
        "allowed_operations": ["situation_and_edge", "no_op"],
        "op_family": "claim_insert",
        "proposed_text": f"Evaluate the mechanism in {case.provider_payload['scope']['display_label']}.",
        "semantic_scope": [case.provider_payload["scope"]["display_label"]],
        "canonical_scope_ref": context.canonical_scope_ref,
        "member_observation_ids": [str(row.canonical_id) for row in observations],
        "relation_evidence_observation_ids": [str(row.canonical_id) for row in observations],
        "evidence_model_ids": [str(row.canonical_id) for row in models],
        "endpoint_model_versions": {
            str(row.canonical_id): str(row.exact_version_id) for row in models
        },
        "observation_evidence": [
            {"observation_id": str(row.canonical_id), "body": text_by_handle[row.handle],
             "source_channel": "evaluator:frozen_dossier"}
            for row in observations
        ],
        "confidence": .5,
    }
    packet = {
        "signal_summary": "Scope-local frozen evaluator dossier.",
        "memory_decision_candidates": [candidate],
        "synthesis_scope_hydration": {
            "endpoint_model_versions": candidate["endpoint_model_versions"],
            "endpoint_model_cards": {
                str(row.canonical_id): {"id": str(row.canonical_id),
                    "version_id": str(row.exact_version_id),
                    "natural": text_by_handle[row.handle], "proposition": {"kind": "belief"},
                    "canonical_scope": {"label": case.provider_payload["scope"]["display_label"],
                                        "ref": context.canonical_scope_ref}}
                for row in models
            },
        },
    }
    request = build_compiled_batch_memory_decision_request(
        _legacy_trigger(case), ContextBundle(notes={"inquiry_context_packet": packet}),
    )
    if request is None:
        raise ValueError("current compiled builder rejected evaluator dossier")
    return request


def _legacy_candidate_id(case: FrozenDossierCase) -> str:
    """Opaque stable identity derived only from the sealed provider payload."""
    return f"MDC_{case.dossier_digest[:24]}"


def _model_handle_by_id(case: FrozenDossierCase) -> dict[str, str]:
    return {str(row.canonical_id): row.handle for row in _compile_context(case).bindings
            if row.object_kind == "accepted_model_head"}


def _verify_file(directory: Path, entry: Mapping[str, Any]) -> bool:
    path = directory / str(entry["path"])
    if not path.is_file():
        return False
    data = path.read_bytes()
    try:
        value = json.loads(data)
    except json.JSONDecodeError:
        return False
    return (hashlib.sha256(data).hexdigest() == entry["byte_sha256"]
            and canonical_sha256(value) == entry["content_digest"])


def _compile_context(case: FrozenDossierCase) -> SynthesisCompileContext:
    tenant = uuid5(NAMESPACE_URL, f"ti3:{case.case_id}:tenant")
    scope_ref = f"evaluation:{case.dossier_digest[:24]}"
    slots = case.provider_payload["candidate_mechanism_slots"]
    supporting = {row["object_handle"] for row in case.provider_payload["supporting_evidence"]}
    contradictory = {row["object_handle"] for row in case.provider_payload["contradictory_evidence"]}
    bindings = []
    observations = set()
    for item in case.provider_payload["handles"]:
        handle = str(item["handle"])
        if not handle.startswith(("M", "O")):
            continue
        roles = set()
        if handle in set(slots["causes"]) | set(slots["conditions"]):
            roles.add("cause")
        if handle in slots["outcomes"]:
            roles.add("effect")
        if handle in supporting:
            roles.add("support")
        if handle in contradictory:
            roles.add("counterevidence")
        if handle.startswith("M"):
            roles.update(("support", "novelty_reference"))
            kind = "accepted_model_head"
            version = uuid5(NAMESPACE_URL, f"ti3:{case.case_id}:{handle}:version")
        else:
            kind, version = "observation", None
        canonical = uuid5(NAMESPACE_URL, f"ti3:{case.case_id}:{handle}")
        if kind == "observation":
            observations.add(canonical)
        bindings.append(HandleBinding(handle, kind, canonical, version, tenant, scope_ref,
                                      str(item.get("authority_tier") or "unknown"),
                                      frozenset(roles)))
    return SynthesisCompileContext(
        str(case.provider_payload["dossier_id"]), case.dossier_digest, tenant, scope_ref,
        uuid5(NAMESPACE_URL, f"ti3:{case.case_id}:trigger"), frozenset(observations),
        tuple(bindings),
    )


def _scorer_case(case: FrozenDossierCase) -> SemanticScorerCase:
    gold = case.gold
    if gold.expected_decision == "abstain":
        payload = {"schema_version": "think-semantic-case-v1", "case_id": case.case_id,
            "dossier_digest": case.dossier_digest, "case_kind": "null", "null_gold": {
                "allowed_decisions": ["abstain"],
                "allowed_reason_codes": list(gold.acceptable_abstention_reasons),
                "required_missing_evidence_facets": [["record", "evidence", "reason"]],
                "forbidden_handles": [], "maximum_synthesis_confidence": .5}}
    else:
        direction = str(gold.required_direction or "")
        relation = "blocks" if "blocked" in direction else "causes"
        cause_handles = list(dict.fromkeys((*gold.allowed_cause_handles,
                                            *gold.allowed_condition_handles)))
        payload = {"schema_version": "think-semantic-case-v1", "case_id": case.case_id,
            "dossier_digest": case.dossier_digest, "case_kind": "positive", "positive_gold": {
                "required_thesis_facets": [[value] for value in gold.required_scope_facets],
                "required_mechanism_facets": [[value] for value in gold.required_mechanism_facets],
                "allowed_relation_kinds": [relation], "expected_direction": "source_to_synthesis",
                "allowed_cause_handle_sets": [cause_handles],
                "required_support_handles": list(gold.required_support_handles),
                "required_counterevidence_handles": list(gold.required_counterevidence_handles),
                "required_alternative_facets": [], "expected_novelty": "novel",
                "forbidden_handles": [], "confidence_band": {"minimum": 0, "maximum": 1}}}
    return SemanticScorerCase.model_validate(payload)


def _summarize(outcomes: tuple[AttemptOutcome, ...], policies: tuple[ArmPolicy, ...]) -> dict:
    result = {}
    for policy in policies:
        rows = [row for row in outcomes if row.spec["arm"] == policy.arm]
        if not rows:
            continue
        quality = [
            (row.result.continuous_metrics.mechanism_correctness
             + row.result.continuous_metrics.thesis_completeness
             + row.result.continuous_metrics.causal_direction_correctness
             + row.result.continuous_metrics.evidence_coverage
             + row.result.continuous_metrics.counterevidence_recognition
             + row.result.continuous_metrics.abstention_appropriateness) / 6
            for row in rows
        ]
        result[policy.arm] = {
            "attempt_count": len(rows),
            "hard_gate_pass_rate": sum(row.result.verdict == "green" for row in rows) / len(rows),
            "isolated_ti3_gate_pass_rate": sum(
                _isolated_ti3_gates_pass(row.result) for row in rows
            ) / len(rows),
            "quality_score": sum(quality) / len(quality),
            "total_tokens": sum(row.result.continuous_metrics.tokens for row in rows),
            "total_cost_usd": sum(row.result.continuous_metrics.cost_usd for row in rows),
        }
    return result


def _best_arms(summary: Mapping[str, Mapping[str, Any]], *, count: int) -> tuple[ArmName, ...]:
    ordered = sorted(summary, key=lambda arm: (
        -float(summary[arm]["isolated_ti3_gate_pass_rate"]),
        -float(summary[arm]["quality_score"]), arm,
    ))
    return tuple(ordered[:count])  # type: ignore[return-value]


def _isolated_ti3_gates_pass(result: SemanticScorerResult) -> bool:
    gates = result.hard_gates.model_dump()
    return all(
        value is True for key, value in gates.items()
        if key not in {"partial_writes_zero", "validator_applier_failures_zero"}
    )


def _spec(run_id: str, phase: Phase, arm: ArmName, case_id: str, sample: int) -> AttemptSpec:
    key = f"{run_id}:{phase}:{arm}:{case_id}:{sample}"
    return AttemptSpec(run_id, phase, arm, case_id, sample, str(uuid5(NAMESPACE_URL, key)))


def _spec_dict(spec: AttemptSpec) -> dict[str, Any]:
    return {"run_id": spec.run_id, "phase": spec.phase, "arm": spec.arm,
            "case_id": spec.case_id, "sample_index": spec.sample_index,
            "attempt_id": spec.attempt_id}


def _atomic_json(path: Path, value: Any) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(value, sort_keys=True, indent=2, default=str) + "\n").encode()
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    return encoded


__all__ = [
    "ArmPolicy", "AttemptSpec", "ProviderAttempt", "default_arm_policies",
    "run_ti3_experiment", "select_cheapest_within_tolerance",
    "verify_experiment_artifact",
]
