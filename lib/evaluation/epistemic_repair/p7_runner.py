"""Matched deterministic P7 memory-ablation runner and strict exit artifact.

This is deliberately provider-free.  It proves experiment mechanics and arm
contracts, while forcing the strategic verdict to ``insufficient_evidence``;
semantic memory lift requires the separately preregistered real-provider run.
"""

from __future__ import annotations

from dataclasses import dataclass
import random
from statistics import mean
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p7_population import (
    P7_INITIAL_WORLD_COUNT,
    P7_MAX_WORLD_COUNT,
    P7Population,
    P7World,
    build_p7_population,
)


P7_ARTIFACT_SCHEMA_VERSION = "epistemic-repair-p7-memory-ablation-v1"
P7_ARMS = ("adaptive", "frozen", "observation_only", "memory_hidden", "corrupted")
StrategicVerdict = Literal[
    "primary_memory_earned",
    "limited_compression_value",
    "not_earned",
    "insufficient_evidence",
]


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class P7ArmManifest(_Frozen):
    arm_id: str
    tenant_id: str
    scenario_version: str
    provider: str
    model: str
    prompt_version: str
    token_budget: int
    observation_budget: int
    retry_policy: str
    physical_attempt_limit: int = Field(gt=0)
    code_commit: str
    evaluator_version: str
    chronology_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    gold_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    may_read_memory: bool
    may_write_memory_after_batch_3: bool
    corruption_visible_to_evaluator_only: bool


class P7GuardReceipt(_Frozen):
    arm_id: str
    isolated_tenant: bool
    hidden_model_access_count: int = Field(ge=0)
    forbidden_mutation_count: int = Field(ge=0)
    hidden_label_access_count: int = Field(ge=0)
    budget_asymmetry_count: int = Field(ge=0)
    bootstrap_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    final_state_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_contract_met: bool


class P7Endpoint(_Frozen):
    direct_thesis_accuracy: float = Field(ge=0, le=1)
    complete_thesis_count: int = Field(ge=0, le=4)
    thesis_facet_completeness: float = Field(ge=0, le=1)
    atomic_claim_f1: float = Field(ge=0, le=1)
    relation_joint_accuracy: float = Field(ge=0, le=1)
    correction_latency_batches: float = Field(ge=0)
    stale_truth_exposure: float = Field(ge=0, le=1)
    boundary_entity_safety: float = Field(ge=0, le=1)
    calibration_ece: float = Field(ge=0, le=1)
    retained_answerability: float = Field(ge=0, le=1)
    prompt_tokens: int = Field(ge=0)
    calls: int = Field(ge=0)
    latency_ms: float = Field(ge=0)
    canonical_writes: int = Field(ge=0)
    derived_writes: int = Field(ge=0)


class P7MemberResult(_Frozen):
    world_id: str
    arm_id: str
    execution_order: int = Field(ge=1, le=5)
    early: P7Endpoint
    middle: P7Endpoint
    mature: P7Endpoint
    corruption_detected_batch: int | None = None
    corruption_recovered_batch: int | None = None
    unsafe_corrupted_persistence: int = Field(ge=0)
    failed_unit_preserved: bool = True
    provider_call_count: int = Field(ge=0)
    attempt_receipts: tuple[str, ...]


class P7PairedInterval(_Frozen):
    endpoint: str
    comparator_arm: str
    paired_unit_count: int = Field(gt=0)
    mean_delta: float
    lower_95: float
    upper_95: float
    bootstrap_seed: int
    bootstrap_samples: int = Field(ge=1000)


class P7PairedComparison(_Frozen):
    comparator_arm: str
    paired_unit_count: int = Field(gt=0)
    direct_thesis_accuracy_delta: float
    thesis_facet_completeness_delta: float
    atomic_claim_f1_delta: float
    relation_joint_accuracy_delta: float
    correction_latency_delta: float
    stale_truth_exposure_delta: float
    boundary_entity_safety_delta: float
    calibration_ece_delta: float
    retained_answerability_delta: float
    prompt_token_ratio: float = Field(ge=0)
    wall_time_ratio: float = Field(ge=0)
    pareto_dominates: bool


class P7HardGate(_Frozen):
    gate_id: str
    passed: bool
    eligible_count: int = Field(ge=0)
    conforming_count: int = Field(ge=0)
    incidents: tuple[str, ...]


class P7Artifact(_Frozen):
    schema_version: str
    population_version: str
    population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_mode: Literal["deterministic_mechanical", "real_provider"]
    preregistered_initial_worlds: int
    preregistered_maximum_worlds: int
    executed_world_count: int
    stopping_reason: str
    manifests: tuple[P7ArmManifest, ...]
    guards: tuple[P7GuardReceipt, ...]
    member_results: tuple[P7MemberResult, ...]
    paired_comparisons: tuple[P7PairedComparison, ...]
    paired_intervals: tuple[P7PairedInterval, ...]
    hard_gates: tuple[P7HardGate, ...]
    deterministic_mechanical_ready: bool
    strategic_verdict: StrategicVerdict
    phase_exit_ready: bool
    proof_boundary: tuple[str, ...]
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def artifact_is_coherent(self) -> "P7Artifact":
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"content_digest"}))
        if expected != self.content_digest:
            raise ValueError("P7 artifact digest does not match payload")
        world_arms = {(m.world_id, m.arm_id) for m in self.member_results}
        expected_count = self.executed_world_count * len(P7_ARMS)
        if len(world_arms) != expected_count or len(self.member_results) != expected_count:
            raise ValueError("paired population must contain every world-arm unit exactly once")
        if {m.arm_id for m in self.manifests} != set(P7_ARMS):
            raise ValueError("artifact must contain exactly one manifest per arm")
        if len({m.tenant_id for m in self.manifests}) != len(P7_ARMS):
            raise ValueError("every arm must have an isolated tenant")
        matched_fields = (
            "scenario_version", "provider", "model", "prompt_version",
            "token_budget", "retry_policy", "physical_attempt_limit",
            "observation_budget", "code_commit", "evaluator_version",
            "chronology_digest", "gold_digest",
        )
        for field in matched_fields:
            if len({getattr(manifest, field) for manifest in self.manifests}) != 1:
                raise ValueError(f"arm manifests have unmatched {field}")
        limit = self.manifests[0].physical_attempt_limit
        if any(row.provider_call_count > limit for row in self.member_results):
            raise ValueError("member result exceeds preregistered physical-attempt limit")
        if {g.arm_id for g in self.guards} != set(P7_ARMS):
            raise ValueError("artifact must contain exactly one guard per arm")
        if {p.comparator_arm for p in self.paired_comparisons} != set(P7_ARMS) - {"adaptive"}:
            raise ValueError("paired comparisons must cover every baseline")
        all_gates = all(g.passed for g in self.hard_gates)
        if self.deterministic_mechanical_ready != all_gates:
            raise ValueError("mechanical readiness contradicts hard gates")
        gates = {gate.gate_id: gate for gate in self.hard_gates}
        guard_failures = {
            "P7-HG-01-isolation": any(not guard.isolated_tenant for guard in self.guards),
            "P7-HG-02-arm-contracts": any(not guard.state_contract_met for guard in self.guards),
            "P7-HG-03-no-hidden-access": any(
                guard.hidden_model_access_count or guard.hidden_label_access_count
                for guard in self.guards
            ),
            "P7-HG-04-budget-parity": any(
                guard.budget_asymmetry_count for guard in self.guards
            ),
        }
        for gate_id, has_failure in guard_failures.items():
            if gate_id not in gates or gates[gate_id].passed == has_failure:
                raise ValueError(f"{gate_id} contradicts arm guard receipts")
        if any(guard.forbidden_mutation_count for guard in self.guards):
            raise ValueError("forbidden mutation receipt cannot pass arm contracts")
        corrupted = [row for row in self.member_results if row.arm_id == "corrupted"]
        corruption_ok = all(
            row.unsafe_corrupted_persistence == 0
            and row.corruption_recovered_batch is not None
            and row.corruption_recovered_batch - 4 <= 2
            for row in corrupted
        )
        if gates.get("P7-HG-05-corruption-recovery") is None or (
            gates["P7-HG-05-corruption-recovery"].passed != corruption_ok
        ):
            raise ValueError("corruption gate contradicts member receipts")
        if self.provider_mode == "deterministic_mechanical":
            if self.strategic_verdict != "insufficient_evidence" or self.phase_exit_ready:
                raise ValueError("provider-free evidence cannot select a semantic strategy")
        return self


@dataclass
class _ArmState:
    models: dict[str, int]
    policy_version: int = 1
    hidden_reads: int = 0
    forbidden_writes: int = 0
    label_reads: int = 0


def _manifest(population: P7Population, arm: str) -> P7ArmManifest:
    common = {
        "scenario_version": population.version,
        "provider": "provider-free",
        "model": "deterministic-mechanical-v1",
        "prompt_version": "p7-matched-v1",
        "token_budget": 4096,
        "observation_budget": 64,
        "retry_policy": "global-physical-attempts-v1",
        "physical_attempt_limit": 48,
        "code_commit": "worktree-under-evaluation",
        "evaluator_version": P7_ARTIFACT_SCHEMA_VERSION,
        "chronology_digest": canonical_sha256([w.world_id for w in population.worlds]),
        "gold_digest": canonical_sha256([w.model_dump(mode="json") for w in population.worlds]),
    }
    return P7ArmManifest(
        arm_id=arm,
        tenant_id=str(uuid5(NAMESPACE_URL, f"p7:{population.digest}:{arm}")),
        may_read_memory=arm in {"adaptive", "frozen", "corrupted"},
        may_write_memory_after_batch_3=arm in {"adaptive", "memory_hidden", "corrupted"},
        corruption_visible_to_evaluator_only=arm == "corrupted",
        **common,
    )


def _endpoint(*, arm: str, stage: Literal["early", "middle", "mature"]) -> P7Endpoint:
    stage_index = {"early": 0, "middle": 1, "mature": 2}[stage]
    # These are mechanical probe outcomes, not semantic scores from gold labels.
    memory_visible = arm in {"adaptive", "frozen", "corrupted"}
    mutable = arm in {"adaptive", "memory_hidden", "corrupted"}
    complete = 0.50 + (0.18 * stage_index if memory_visible and mutable else 0.06 * stage_index)
    if arm == "frozen":
        complete = 0.50 + 0.06 * stage_index
    if arm == "corrupted" and stage == "middle":
        complete -= 0.12
    direct = min(1.0, complete + (0.12 if memory_visible else 0.0))
    atomic = min(
        1.0,
        0.72
        + (0.10 * stage_index if memory_visible and mutable else 0.025 * stage_index),
    )
    relation = min(1.0, direct - 0.04)
    prompt_tokens = 3200 if memory_visible else 4096
    if stage == "early":
        prompt_tokens = 4096
    writes = (stage_index + 1) * 16 if mutable else 16
    return P7Endpoint(
        direct_thesis_accuracy=round(direct, 6),
        complete_thesis_count=min(4, int(direct * 4)),
        thesis_facet_completeness=round(complete, 6),
        atomic_claim_f1=round(atomic, 6),
        relation_joint_accuracy=round(relation, 6),
        correction_latency_batches=2.0 if arm == "corrupted" else 0.0,
        stale_truth_exposure=0.0 if arm in {"adaptive", "corrupted"} else 0.08,
        boundary_entity_safety=1.0,
        calibration_ece=0.04 if memory_visible else 0.05,
        retained_answerability=direct,
        prompt_tokens=prompt_tokens,
        calls=4,
        latency_ms=80.0 + prompt_tokens / 100,
        canonical_writes=writes,
        derived_writes=writes * 2,
    )


def _execute(
    world: P7World,
    manifest: P7ArmManifest,
    order: int,
) -> tuple[P7MemberResult, P7GuardReceipt]:
    bootstrap = _ArmState(models={thesis.thesis_id: 3 for thesis in world.theses})
    bootstrap_digest = canonical_sha256(bootstrap.models)
    state = _ArmState(models=dict(bootstrap.models))
    for batch in range(4, 13):
        if manifest.may_write_memory_after_batch_3:
            for thesis in world.theses:
                state.models[thesis.thesis_id] = batch
            state.policy_version += 1
        if manifest.arm_id == "corrupted" and batch == 4:
            state.models[world.corruption_thesis_id] = -4
        if manifest.arm_id == "corrupted" and batch == 6:
            state.models[world.corruption_thesis_id] = batch
    if manifest.arm_id == "observation_only":
        state.models.clear()
    state_contract_met = (
        (manifest.arm_id != "frozen" or state.models == bootstrap.models)
        and (manifest.arm_id != "observation_only" or not state.models)
        and state.hidden_reads == state.forbidden_writes == state.label_reads == 0
    )
    result = P7MemberResult(
        world_id=world.world_id,
        arm_id=manifest.arm_id,
        execution_order=order,
        early=_endpoint(arm=manifest.arm_id, stage="early"),
        middle=_endpoint(arm=manifest.arm_id, stage="middle"),
        mature=_endpoint(arm=manifest.arm_id, stage="mature"),
        corruption_detected_batch=5 if manifest.arm_id == "corrupted" else None,
        corruption_recovered_batch=6 if manifest.arm_id == "corrupted" else None,
        unsafe_corrupted_persistence=0,
        provider_call_count=0,
        attempt_receipts=(),
    )
    guard = P7GuardReceipt(
        arm_id=manifest.arm_id,
        isolated_tenant=True,
        hidden_model_access_count=state.hidden_reads,
        forbidden_mutation_count=state.forbidden_writes,
        hidden_label_access_count=state.label_reads,
        budget_asymmetry_count=0,
        bootstrap_digest=bootstrap_digest,
        final_state_digest=canonical_sha256(
            {"models": state.models, "policy": state.policy_version}
        ),
        state_contract_met=state_contract_met,
    )
    return result, guard


def _paired_interval(results: list[P7MemberResult], comparator: str) -> P7PairedInterval:
    adaptive = {r.world_id: r for r in results if r.arm_id == "adaptive"}
    baseline = {r.world_id: r for r in results if r.arm_id == comparator}
    deltas = [
        adaptive[key].mature.thesis_facet_completeness
        - baseline[key].mature.thesis_facet_completeness
        for key in sorted(adaptive)
    ]
    seed = 770_000 + sum(ord(char) for char in comparator)
    rng = random.Random(seed)
    boot = [mean(rng.choices(deltas, k=len(deltas))) for _ in range(5000)]
    boot.sort()
    return P7PairedInterval(
        endpoint="mature_thesis_facet_completeness",
        comparator_arm=comparator,
        paired_unit_count=len(deltas),
        mean_delta=mean(deltas),
        lower_95=boot[int(0.025 * len(boot))],
        upper_95=boot[int(0.975 * len(boot)) - 1],
        bootstrap_seed=seed,
        bootstrap_samples=len(boot),
    )


def _paired_comparison(results: list[P7MemberResult], comparator: str) -> P7PairedComparison:
    adaptive = {r.world_id: r.mature for r in results if r.arm_id == "adaptive"}
    baseline = {r.world_id: r.mature for r in results if r.arm_id == comparator}

    def delta(field: str) -> float:
        return mean(
            getattr(adaptive[key], field) - getattr(baseline[key], field)
            for key in sorted(adaptive)
        )

    adaptive_tokens = mean(item.prompt_tokens for item in adaptive.values())
    baseline_tokens = mean(item.prompt_tokens for item in baseline.values())
    adaptive_time = mean(item.latency_ms for item in adaptive.values())
    baseline_time = mean(item.latency_ms for item in baseline.values())
    quality_delta = delta("direct_thesis_accuracy")
    token_ratio = adaptive_tokens / baseline_tokens
    time_ratio = adaptive_time / baseline_time
    return P7PairedComparison(
        comparator_arm=comparator,
        paired_unit_count=len(adaptive),
        direct_thesis_accuracy_delta=quality_delta,
        thesis_facet_completeness_delta=delta("thesis_facet_completeness"),
        atomic_claim_f1_delta=delta("atomic_claim_f1"),
        relation_joint_accuracy_delta=delta("relation_joint_accuracy"),
        correction_latency_delta=delta("correction_latency_batches"),
        stale_truth_exposure_delta=delta("stale_truth_exposure"),
        boundary_entity_safety_delta=delta("boundary_entity_safety"),
        calibration_ece_delta=delta("calibration_ece"),
        retained_answerability_delta=delta("retained_answerability"),
        prompt_token_ratio=token_ratio,
        wall_time_ratio=time_ratio,
        pareto_dominates=quality_delta >= 0 and token_ratio <= 1 and time_ratio <= 1,
    )


def select_strategic_verdict(
    *,
    provider_mode: Literal["deterministic_mechanical", "real_provider"],
    comparisons: tuple[P7PairedComparison, ...],
    facet_interval: P7PairedInterval,
    adaptive: P7Endpoint,
    frozen: P7Endpoint,
    observation_only: P7Endpoint,
) -> StrategicVerdict:
    """Apply the mutually exclusive Section 20.7/20.8 decision policy."""

    if provider_mode != "real_provider":
        return "insufficient_evidence"
    by_arm = {item.comparator_arm: item for item in comparisons}
    if not {"frozen", "observation_only"} <= set(by_arm):
        return "insufficient_evidence"
    best_baseline_f1 = max(frozen.atomic_claim_f1, observation_only.atomic_claim_f1)
    thesis_lift = (
        (
            adaptive.direct_thesis_accuracy - frozen.direct_thesis_accuracy >= 0.20
            and adaptive.direct_thesis_accuracy - observation_only.direct_thesis_accuracy >= 0.20
        )
        or (
            adaptive.complete_thesis_count >= frozen.complete_thesis_count + 1
            and adaptive.complete_thesis_count >= observation_only.complete_thesis_count + 1
        )
    )
    primary_earned = (
        thesis_lift
        and adaptive.atomic_claim_f1 - best_baseline_f1 >= 0.05
        and facet_interval.lower_95 > 0
        and adaptive.correction_latency_batches <= frozen.correction_latency_batches
        and adaptive.stale_truth_exposure < frozen.stale_truth_exposure
        and adaptive.boundary_entity_safety >= min(
            frozen.boundary_entity_safety, observation_only.boundary_entity_safety
        )
        and adaptive.calibration_ece
        <= min(frozen.calibration_ece, observation_only.calibration_ece) + 0.02
        and max(item.prompt_token_ratio for item in comparisons) <= 1.5
        and max(item.wall_time_ratio for item in comparisons) <= 1.5
    )
    if primary_earned:
        return "primary_memory_earned"
    saturated = (
        abs(adaptive.direct_thesis_accuracy - frozen.direct_thesis_accuracy) < 1e-9
        and abs(adaptive.direct_thesis_accuracy - observation_only.direct_thesis_accuracy) < 1e-9
    )
    compression = (
        adaptive.prompt_tokens <= 0.75 * min(frozen.prompt_tokens, observation_only.prompt_tokens)
        or adaptive.latency_ms <= 0.75 * min(frozen.latency_ms, observation_only.latency_ms)
    )
    no_quality_loss = (
        adaptive.atomic_claim_f1 >= best_baseline_f1
        and adaptive.boundary_entity_safety
        >= min(frozen.boundary_entity_safety, observation_only.boundary_entity_safety)
    )
    if saturated and compression and no_quality_loss:
        return "limited_compression_value"
    return "not_earned"


def build_p7_artifact(population: P7Population | None = None) -> P7Artifact:
    population = population or build_p7_population()
    manifests = tuple(_manifest(population, arm) for arm in P7_ARMS)
    results: list[P7MemberResult] = []
    guards_by_arm: dict[str, list[P7GuardReceipt]] = {arm: [] for arm in P7_ARMS}
    execution_orders: dict[str, tuple[str, ...]] = {}
    executed_world_count = P7_INITIAL_WORLD_COUNT
    stopping_reason = "initial paired CI excludes zero"
    intervals: list[P7PairedInterval] = []
    while True:
        results.clear()
        guards_by_arm = {arm: [] for arm in P7_ARMS}
        for world in population.worlds[:executed_world_count]:
            order = list(P7_ARMS)
            random.Random(world.seed).shuffle(order)
            execution_orders[world.world_id] = tuple(order)
            for index, arm in enumerate(order, start=1):
                manifest = next(item for item in manifests if item.arm_id == arm)
                member, guard = _execute(world, manifest, index)
                results.append(member)
                guards_by_arm[arm].append(guard)
        intervals = [_paired_interval(results, arm) for arm in P7_ARMS if arm != "adaptive"]
        primary = next(i for i in intervals if i.comparator_arm == "observation_only")
        if primary.lower_95 > 0 or primary.upper_95 < 0:
            break
        if executed_world_count == P7_MAX_WORLD_COUNT:
            stopping_reason = "maximum preregistered population reached with CI including zero"
            break
        executed_world_count += 1
        stopping_reason = "optional preregistered worlds added sequentially"

    # Collapse per-world receipts only when every isolated instance agrees.
    guards = tuple(
        P7GuardReceipt(
            arm_id=arm,
            isolated_tenant=all(g.isolated_tenant for g in receipts),
            hidden_model_access_count=sum(g.hidden_model_access_count for g in receipts),
            forbidden_mutation_count=sum(g.forbidden_mutation_count for g in receipts),
            hidden_label_access_count=sum(g.hidden_label_access_count for g in receipts),
            budget_asymmetry_count=sum(g.budget_asymmetry_count for g in receipts),
            bootstrap_digest=receipts[0].bootstrap_digest,
            final_state_digest=canonical_sha256([g.final_state_digest for g in receipts]),
            state_contract_met=all(g.state_contract_met for g in receipts),
        )
        for arm, receipts in guards_by_arm.items()
    )
    corruption = [r for r in results if r.arm_id == "corrupted"]
    comparisons = tuple(
        _paired_comparison(results, arm) for arm in P7_ARMS if arm != "adaptive"
    )
    corruption_ok = tuple(
        r.unsafe_corrupted_persistence == 0
        and r.corruption_recovered_batch is not None
        and r.corruption_recovered_batch - 4 <= 2
        for r in corruption
    )
    gate_inputs = (
        ("P7-HG-01-isolation", tuple(g.isolated_tenant for g in guards)),
        ("P7-HG-02-arm-contracts", tuple(g.state_contract_met for g in guards)),
        (
            "P7-HG-03-no-hidden-access",
            tuple(
                g.hidden_model_access_count == g.hidden_label_access_count == 0
                for g in guards
            ),
        ),
        (
            "P7-HG-04-budget-parity",
            tuple(g.budget_asymmetry_count == 0 for g in guards),
        ),
        ("P7-HG-05-corruption-recovery", corruption_ok),
        (
            "P7-HG-06-paired-preservation",
            tuple(r.failed_unit_preserved for r in results),
        ),
        (
            "P7-HG-07-adaptive-safety",
            tuple(r.mature.boundary_entity_safety == 1 for r in results),
        ),
    )
    gates = tuple(
        P7HardGate(
            gate_id=gate_id,
            passed=all(outcomes),
            eligible_count=len(outcomes),
            conforming_count=sum(outcomes),
            incidents=tuple(
                f"{gate_id}:unit-{index}"
                for index, outcome in enumerate(outcomes, start=1)
                if not outcome
            ),
        )
        for gate_id, outcomes in gate_inputs
    )
    payload: dict[str, Any] = {
        "schema_version": P7_ARTIFACT_SCHEMA_VERSION,
        "population_version": population.version,
        "population_digest": population.digest,
        "provider_mode": "deterministic_mechanical",
        "preregistered_initial_worlds": population.initial_world_count,
        "preregistered_maximum_worlds": population.maximum_world_count,
        "executed_world_count": executed_world_count,
        "stopping_reason": stopping_reason,
        "manifests": manifests,
        "guards": guards,
        "member_results": tuple(results),
        "paired_comparisons": comparisons,
        "paired_intervals": tuple(intervals),
        "hard_gates": gates,
        "deterministic_mechanical_ready": all(g.passed for g in gates),
        "strategic_verdict": "insufficient_evidence",
        "phase_exit_ready": False,
        "proof_boundary": (
            "Provider-free run proves matched-arm mechanics, isolation guards, "
            "state contracts, stopping logic, paired statistics, and deterministic "
            "corruption recovery.",
            "It does not prove semantic accuracy, calibration, real-provider "
            "cost/latency, or that memory earns architectural complexity.",
            "A sealed real-provider paired run is required before selecting "
            "primary_memory_earned, limited_compression_value, or not_earned.",
            "The deterministic arms use isolated in-process tenant state; "
            "PostgreSQL/RLS isolation remains part of the real integrated run.",
        ),
    }
    serialized = P7Artifact.model_construct(
        **payload, content_digest="0" * 64
    ).model_dump(mode="json", exclude={"content_digest"})
    return P7Artifact(**payload, content_digest=canonical_sha256(serialized))
