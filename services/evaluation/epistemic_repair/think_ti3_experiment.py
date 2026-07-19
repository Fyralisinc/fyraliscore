"""Bounded, provider-injected TI3 three-dossier experiment orchestration."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal, Mapping
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field

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

from .think_policy_receipts import PolicyIdentity, build_evaluation_receipt
from .think_semantic_scorer import (
    ExecutionEvidence,
    SemanticScorerCase,
    SemanticScorerResult,
    score_semantic_decision,
)


ArmName = Literal["A", "B", "C"]
Phase = Literal["screening", "confirmation"]
AttemptProvider = Callable[["AttemptSpec", Mapping[str, Any]], Awaitable["ProviderAttempt"]]


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


def default_arm_policies() -> tuple[ArmPolicy, ...]:
    common = dict(provider_schema_version="think-synthesis-decision-v1",
                  compiler_version="ti2-v1", routing_policy_version="ti3-v1")
    return (
        ArmPolicy(arm="A", interface="legacy_isolated",
                  policy=PolicyIdentity(prompt_policy_version="legacy-compiled-v1",
                    model="current", effort="medium", **common),
                  estimated_cost_per_thousand_tokens=0.006),
        ArmPolicy(arm="B", interface="synthesis_decision_v1",
                  policy=PolicyIdentity(prompt_policy_version="dossier-schema-v1",
                    model="current", effort="medium", **common),
                  estimated_cost_per_thousand_tokens=0.006),
        ArmPolicy(arm="C", interface="synthesis_decision_v1",
                  policy=PolicyIdentity(prompt_policy_version="dossier-schema-v1",
                    model="stronger", effort="high", **common),
                  estimated_cost_per_thousand_tokens=0.012),
    )


async def run_ti3_experiment(
    *, output_root: Path, run_id: str, provider: AttemptProvider,
    commit: str,
    arm_policies: tuple[ArmPolicy, ...] | None = None,
    quality_tolerance: float = 0.03, max_concurrency: int = 3,
    arm_a_baselines: Mapping[str, ProviderAttempt] | None = None,
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
        baselines=arm_a_baselines or {},
    )
    screening_summary = _summarize(screening, policies)
    best_two = _best_arms(screening_summary, count=2)
    confirmation_specs = tuple(
        _spec(run_id, "confirmation", arm, case.case_id, sample)
        for case in cases for arm in best_two for sample in (1, 2)
    )
    confirmation = await _run_specs(
        confirmation_specs, cases=cases, policies=policies, provider=provider,
        output_root=output_root, commit=commit, semaphore=semaphore, baselines={},
    )
    confirmation_summary = _summarize(confirmation, policies)
    selected = select_cheapest_within_tolerance(
        confirmation_summary, policies=policies, quality_tolerance=quality_tolerance,
    )
    body = {
        "schema_version": "think-ti3-experiment-v1", "run_id": run_id,
        "commit": commit,
        "contract_digest": "b1e234eee1cdfaf279a431efda4abe39bb7aff5896d1f1d2de1f0b5fbcb48717",
        "fixture_manifest_digest": build_fixture_manifest()["manifest_digest"],
        "quality_tolerance": quality_tolerance, "max_concurrency": max_concurrency,
        "screening_attempt_count": len(screening), "confirmation_attempt_count": len(confirmation),
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
    baselines: Mapping[str, ProviderAttempt],
) -> tuple[AttemptOutcome, ...]:
    case_by_id = {row.case_id: row for row in cases}
    policy_by_arm = {row.arm: row for row in policies}

    async def one(spec: AttemptSpec) -> AttemptOutcome:
        async with semaphore:
            case = case_by_id[spec.case_id]
            policy = policy_by_arm[spec.arm]
            attempt = (
                baselines[spec.case_id]
                if spec.phase == "screening" and spec.arm == "A" and spec.case_id in baselines
                else await provider(spec, case.provider_payload)
            )
            return _evaluate_attempt(
                spec, case=case, policy=policy, attempt=attempt, output_root=output_root,
                commit=commit,
            )

    return tuple(await asyncio.gather(*(one(spec) for spec in specs)))


def _evaluate_attempt(
    spec: AttemptSpec, *, case: FrozenDossierCase, policy: ArmPolicy,
    attempt: ProviderAttempt, output_root: Path, commit: str,
) -> AttemptOutcome:
    raw = dict(attempt.raw_decision)
    raw_digest = canonical_sha256(raw)
    compiler_digest: str | None = None
    compiler_accepted = False
    partial_writes = 0
    validator_failures = 0
    if attempt.source == "frozen_baseline" and attempt.compiler_artifact is not None:
        compiler_artifact = dict(attempt.compiler_artifact)
        compiler_accepted = compiler_artifact.get("accepted") is True
        compiler_digest = canonical_sha256(compiler_artifact) if compiler_accepted else None
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
        partial_write_count=partial_writes, validator_applier_failure_count=validator_failures,
        compiler_receipt_digest=compiler_digest,
        tokens=attempt.input_tokens + attempt.output_tokens, latency_ms=attempt.latency_ms,
        cost_usd=attempt.cost_usd, consistency=1,
    )
    result = score_semantic_decision(
        scorer_case, raw, decision_artifact_digest=raw_digest, execution=execution,
    )
    receipt = build_evaluation_receipt(
        attempt_id=spec.attempt_id, dossier_digest=case.dossier_digest,
        policy=policy.policy, raw_decision_digest=raw_digest, scorer_result=result,
        compiler_receipt_digest=compiler_digest,
    )
    directory = output_root / "ti3" / spec.run_id / "attempts" / spec.attempt_id
    if directory.exists():
        raise FileExistsError(f"attempt directory already exists: {spec.attempt_id}")
    files = {
        "raw-response.json": raw,
        "compiler.json": compiler_artifact,
        "score.json": result.model_dump(mode="json"),
        "evaluation-receipt.json": receipt.model_dump(mode="json"),
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
        "policy_digest": policy.policy.content_digest, "files": entries,
    }
    manifest = {**manifest_body, "content_digest": canonical_sha256(manifest_body)}
    _atomic_json(directory / "manifest.json", manifest)
    return AttemptOutcome(
        spec=_spec_dict(spec), arm_policy_digest=policy.policy.content_digest,
        result=result, receipt_digest=receipt.content_digest,
        artifact_manifest_digest=manifest["content_digest"],
    )


def select_cheapest_within_tolerance(
    summary: Mapping[str, Mapping[str, Any]], *, policies: tuple[ArmPolicy, ...],
    quality_tolerance: float,
) -> ArmName:
    eligible = {arm: row for arm, row in summary.items()
                if float(row["hard_gate_pass_rate"]) == 1.0}
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
    scope_ref = f"evaluation:{case.case_id}"
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
            "quality_score": sum(quality) / len(quality),
            "total_tokens": sum(row.result.continuous_metrics.tokens for row in rows),
            "total_cost_usd": sum(row.result.continuous_metrics.cost_usd for row in rows),
        }
    return result


def _best_arms(summary: Mapping[str, Mapping[str, Any]], *, count: int) -> tuple[ArmName, ...]:
    ordered = sorted(summary, key=lambda arm: (
        -float(summary[arm]["hard_gate_pass_rate"]),
        -float(summary[arm]["quality_score"]), arm,
    ))
    return tuple(ordered[:count])  # type: ignore[return-value]


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
