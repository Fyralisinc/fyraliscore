#!/usr/bin/env python3
"""Run the sealed held-out exact-alias company-learning population."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Literal, Self
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    ArmLineageRefs,
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    CorrectiveMemoryArmResult,
    CorrectiveMemoryExperimentReport,
    CorrectiveMemoryExperimentSpec,
    PairedRecurrenceResult,
    RecurrenceCaseKind,
    SealedArmExpectation,
    SealedRecurrenceCase,
    evaluate_corrective_memory_experiment,
)
from lib.evaluation.company_learning_population import (
    HeldOutExactAliasCase,
    HeldOutExactAliasPopulation,
    HeldOutPairObservation,
    HeldOutPopulationReport,
    evaluate_heldout_population,
)
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from scripts.company_learning_recurrence_runtime import (
    NegativeControlAssignment,
    NegativeControlCaseDefinition,
)
from scripts.run_company_learning_negative_controls_db import (
    RuntimeEntityTarget,
    _prepare_negative_arm,
    _recurrence_rows,
)
from scripts.run_company_learning_pair_harness import (
    _consumer_fate,
    _ingest_slack,
    _json,
    _observation_snapshot,
)
from services.app.gateway.db_bootstrap import _register_codecs


DEFAULT_POPULATION = (
    ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_exact_alias_population_v1.jsonl"
)
ARTIFACT_NAME = "company_learning_population_evidence.json"
_RUNTIME_TARGETS = {
    "customer": RuntimeEntityTarget(
        canonical_ref_type="customer",
        logical_entity_type="customer",
        semantic_kind="customer",
    ),
    "project": RuntimeEntityTarget(
        canonical_ref_type="resource",
        logical_entity_type="project",
        semantic_kind="workstream",
    ),
    "team": RuntimeEntityTarget(
        canonical_ref_type="actor",
        logical_entity_type="team",
        semantic_kind="team",
        actor_type="group",
    ),
    "system": RuntimeEntityTarget(
        canonical_ref_type="resource",
        logical_entity_type="system",
        semantic_kind="system",
    ),
}


class _EvidenceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class PopulationCaseAssignment(_EvidenceModel):
    case_id: str = Field(min_length=1)
    logical_entity_type: str = Field(min_length=1)
    runtime_entity_type: str | None = None
    adaptive_tenant_id: UUID
    frozen_tenant_id: UUID
    adaptive_target_id: UUID
    frozen_target_id: UUID
    unsupported_reason: str | None = None


class CompanyLearningPopulationEvidence(_EvidenceModel):
    schema_version: str = "company-learning-population-evidence-v1"
    created_at: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    execution_mode: Literal["smoke", "full"]
    selection_policy: Literal[
        "deterministic_registry_prefix_smoke",
        "full_registry_once_no_selective_reruns",
    ]
    registry_path: str = Field(min_length=1)
    registry_population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_population: HeldOutExactAliasPopulation
    selected_case_ids: tuple[str, ...] = Field(min_length=1)
    assignments: tuple[PopulationCaseAssignment, ...] = Field(min_length=1)
    raw_pairs: tuple[PairedRecurrenceResult, ...]
    observations: tuple[HeldOutPairObservation, ...] = Field(min_length=1)
    experiment_report: CorrectiveMemoryExperimentReport
    population_report: HeldOutPopulationReport
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def exact_registry_accounting(self) -> Self:
        selected = list(self.selected_case_ids)
        execution_ids = [
            case.case_id for case in self.execution_population.cases
        ]
        if len(selected) != len(set(selected)) or selected != execution_ids:
            raise ValueError("selection must preserve exact unique registry order")
        if self.execution_mode == "full" and len(selected) != 60:
            raise ValueError("full population execution requires all 60 cases")
        if {row.case_id for row in self.assignments} != set(selected):
            raise ValueError("assignments must exactly cover selected cases")
        if {row.case_id for row in self.observations} != set(selected):
            raise ValueError("observations must exactly cover selected cases")
        observed_ids = {
            row.case_id
            for row in self.observations
            if row.execution_status == "observed"
        }
        if {pair.case_id for pair in self.raw_pairs} != observed_ids:
            raise ValueError("raw pairs must exactly cover observed cases")
        tenant_ids = [
            tenant_id
            for row in self.assignments
            for tenant_id in (
                row.adaptive_tenant_id,
                row.frozen_tenant_id,
            )
        ]
        if len(tenant_ids) != len(set(tenant_ids)):
            raise ValueError("every held-out arm requires a fresh tenant")
        if self.experiment_report.pairs != self.raw_pairs:
            raise ValueError("experiment report must retain every raw pair")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def load_committed_population(
    path: Path = DEFAULT_POPULATION,
) -> HeldOutExactAliasPopulation:
    return HeldOutExactAliasPopulation(
        cases=tuple(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    )


async def run_population_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
    case_limit: int | None = None,
    bootstrap_samples: int = 2000,
    llm_call_cost_usd: float = 0.001,
    population_path: Path = DEFAULT_POPULATION,
) -> CompanyLearningPopulationEvidence:
    registry = load_committed_population(population_path)
    if case_limit is not None and not 1 <= case_limit <= len(registry.cases):
        raise ValueError("case_limit must select a non-empty registry prefix")
    selected = (
        registry.cases[:case_limit] if case_limit is not None else registry.cases
    )
    population = HeldOutExactAliasPopulation(cases=selected)
    assignments = tuple(_assignment(case) for case in selected)
    await _assert_fresh_tenants(pool, assignments)
    created_at = datetime.now(timezone.utc)
    sealed_cases: list[SealedRecurrenceCase] = []
    raw_pairs: list[PairedRecurrenceResult] = []
    unsupported: dict[str, str] = {}

    for case, assignment in zip(selected, assignments, strict=True):
        if assignment.unsupported_reason:
            await _materialize_tenants(pool, assignment)
            unsupported[case.case_id] = assignment.unsupported_reason
            continue
        sealed = _sealed_case(case, assignment)
        sealed_cases.append(sealed)
        definition = _runtime_definition(case)
        runtime_assignment = _runtime_assignment(
            case_id=case.case_id,
            assignment=assignment,
        )
        training_at = created_at
        adaptive = await _prepare_negative_arm(
            pool=pool,
            definition=definition,
            assignment=runtime_assignment,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            training_at=training_at,
            runtime_target=_runtime_target(case),
        )
        frozen = await _prepare_negative_arm(
            pool=pool,
            definition=definition,
            assignment=runtime_assignment,
            arm=CorrectiveMemoryArm.FROZEN,
            training_at=training_at,
            runtime_target=_runtime_target(case),
        )
        recurrence_at = training_at + _distance(case.recurrence_distance)
        adaptive_result = await _run_recurrence(
            pool=pool,
            case=case,
            sealed=sealed,
            foundation=adaptive,
            occurred_at=recurrence_at,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        frozen_result = await _run_recurrence(
            pool=pool,
            case=case,
            sealed=sealed,
            foundation=frozen,
            occurred_at=recurrence_at,
            llm_call_cost_usd=llm_call_cost_usd,
        )
        await _assert_pair_isolation(pool, adaptive_result, frozen_result)
        raw_pairs.append(
            PairedRecurrenceResult(
                case_id=case.case_id,
                adaptive=adaptive_result,
                frozen=frozen_result,
                artifact_refs=(f"population-pair:{case.case_id}",),
            )
        )

    if not raw_pairs:
        raise RuntimeError("selection contains no runtime-supported case")
    spec = CorrectiveMemoryExperimentSpec(
        experiment_id=f"corrective-memory-heldout:{run_id}",
        run_id=run_id,
        system_version=system_version,
        created_at=created_at.isoformat(),
        scenario_ids=("ENTITY-CORRECTIVE-MEMORY-HELDOUT-POPULATION",),
        company_foundation_digest=canonical_sha256(
            [row.model_dump(mode="json") for row in assignments]
        ),
        provider_behavior_digest=canonical_sha256(
            {"training_confidence": 0.99, "recurrence_confidence": 0.40}
        ),
        cases=tuple(sealed_cases),
        artifact_refs=(f"population:{population_path.resolve()}",),
    )
    experiment_report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=tuple(raw_pairs),
        artifact_refs=(f"report-directory:{output_dir.resolve()}",),
    )
    assessment = {
        (row.case_id, row.arm): row
        for row in experiment_report.assessments
    }
    pair_by_case = {pair.case_id: pair for pair in raw_pairs}
    observations = tuple(
        (
            HeldOutPairObservation(
                case_id=case.case_id,
                execution_status="unsupported",
                unsupported_reason=unsupported[case.case_id],
            )
            if case.case_id in unsupported
            else _pair_observation(
                pair_by_case[case.case_id],
                assessment=assessment,
            )
        )
        for case in selected
    )
    population_report = evaluate_heldout_population(
        population=population,
        observations=observations,
        bootstrap_samples=bootstrap_samples,
    )
    mode: Literal["smoke", "full"] = (
        "smoke" if case_limit is not None else "full"
    )
    evidence = CompanyLearningPopulationEvidence(
        created_at=created_at.isoformat(),
        run_id=run_id,
        system_version=system_version,
        execution_mode=mode,
        selection_policy=(
            "deterministic_registry_prefix_smoke"
            if mode == "smoke"
            else "full_registry_once_no_selective_reruns"
        ),
        registry_path=str(population_path.resolve()),
        registry_population_digest=registry.digest,
        execution_population=population,
        selected_case_ids=tuple(case.case_id for case in selected),
        assignments=assignments,
        raw_pairs=tuple(raw_pairs),
        observations=observations,
        experiment_report=experiment_report,
        population_report=population_report,
        artifact_refs=(f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(
            {
                **evidence.model_dump(mode="json"),
                "evidence_digest": evidence.digest,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return evidence


def _assignment(case: HeldOutExactAliasCase) -> PopulationCaseAssignment:
    runtime_target = _RUNTIME_TARGETS.get(case.entity_type)
    return PopulationCaseAssignment(
        case_id=case.case_id,
        logical_entity_type=case.entity_type,
        runtime_entity_type=(
            runtime_target.canonical_ref_type if runtime_target else None
        ),
        adaptive_tenant_id=uuid7(),
        frozen_tenant_id=uuid7(),
        adaptive_target_id=uuid7(),
        frozen_target_id=uuid7(),
        unsupported_reason=(
            None
            if runtime_target
            else (
                "current corrective-memory runtime has no canonical target "
                f"support for sealed entity type: {case.entity_type}"
            )
        ),
    )


def _runtime_definition(
    case: HeldOutExactAliasCase,
) -> NegativeControlCaseDefinition:
    return NegativeControlCaseDefinition(
        case_id=case.case_id,
        kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
        entity_type=_runtime_target(case).canonical_ref_type,
        slack_context=case.slack_context,
        wording_variant=case.wording_variant,
        consequence=case.consequence,
        recurrence_distance=1,
        alias_surface=case.alias_surface,
        training_text=case.training_text,
        training_phrase=case.alias_surface,
        candidate_alias=f"Sealed {case.entity_type} {case.case_id}",
        recurrence_text=case.recurrence_text,
        recurrence_phrase=case.alias_surface,
        channel=f"C-{case.slack_context}-{case.case_id}",
        resolution_scope="tenant_global_exact",
        inject_conflicting_source_hint=False,
        recurrence_response="target_low",
        expected_model_count=0,
    )


def _runtime_assignment(
    *,
    case_id: str,
    assignment: PopulationCaseAssignment,
) -> NegativeControlAssignment:
    return NegativeControlAssignment(
        case_id=case_id,
        adaptive_tenant_id=assignment.adaptive_tenant_id,
        frozen_tenant_id=assignment.frozen_tenant_id,
        adaptive_target_id=assignment.adaptive_target_id,
        frozen_target_id=assignment.frozen_target_id,
        adaptive_conflicting_id=uuid7(),
        frozen_conflicting_id=uuid7(),
    )


async def _run_recurrence(
    *,
    pool: asyncpg.Pool,
    case: HeldOutExactAliasCase,
    sealed: SealedRecurrenceCase,
    foundation,
    occurred_at: datetime,
    llm_call_cost_usd: float,
) -> CorrectiveMemoryArmResult:
    observation_id = await _ingest_slack(
        pool=pool,
        tenant_id=foundation.tenant_id,
        alias_repo=foundation.alias_repo,
        text=case.recurrence_text,
        channel=f"C-{case.slack_context}-{case.case_id}",
        occurred_at=occurred_at,
        corrective_memory_reuse_enabled=(
            foundation.arm is CorrectiveMemoryArm.ADAPTIVE
        ),
    )
    before = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    calls_before = len(foundation.provider.calls)
    started = perf_counter()
    await foundation.worker.process_observation(
        observation_id,
        foundation.tenant_id,
    )
    latency = (perf_counter() - started) * 1000.0
    after = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    rows = await _recurrence_rows(
        pool=pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
        recurrence_phrase=case.alias_surface,
    )
    trace = rows["trace"]
    selected = _json(trace["selected_referent"]) if trace else None
    resolved = (
        CanonicalEntityRef.model_validate(selected)
        if isinstance(selected, dict) and selected.get("type") and selected.get("id")
        else None
    )
    model_output = _json(trace["model_output"]) if trace else {}
    incidents = set()
    if before != after:
        from lib.evaluation.company_learning_experiment import (
            HardSafetyIncidentClass,
        )

        incidents.add(HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED)
    return CorrectiveMemoryArmResult(
        case_id=case.case_id,
        arm=foundation.arm,
        tenant_id=foundation.tenant_id,
        consumer_fate=(
            _consumer_fate(str(trace["current_fate"] or ""))
            if trace
            else ConsumerTerminalFate.INCOMPLETE
        ),
        resolved_entity_ref=resolved,
        decision_source=(
            str(model_output.get("decision_source"))
            if model_output.get("decision_source")
            else None
        ),
        llm_call_count=len(foundation.provider.calls) - calls_before,
        latency_ms=latency,
        estimated_cost_usd=(
            (len(foundation.provider.calls) - calls_before)
            * llm_call_cost_usd
        ),
        source_semantic_admitted=False,
        lineage=ArmLineageRefs(
            training_observation_id=foundation.training_observation_id,
            recurrence_observation_id=observation_id,
            clarification_request_id=foundation.clarification_request_id,
            clarification_answer_digest=(
                foundation.clarification_answer_digest
            ),
            adjudicated_alias_id=foundation.adjudicated_alias_id,
            grounding_trace_id=(
                trace["grounding_trace_id"] if trace else None
            ),
            model_ids=(),
            artifact_refs=(f"observation:{observation_id}",),
        ),
        observed_safety_incidents=frozenset(incidents),
    )


def _sealed_case(
    case: HeldOutExactAliasCase,
    assignment: PopulationCaseAssignment,
) -> SealedRecurrenceCase:
    return SealedRecurrenceCase(
        case_id=case.case_id,
        case_version=case.case_version,
        kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
        alias_surface=case.alias_surface,
        source_text_digest=canonical_sha256(case.recurrence_text),
        context_digest=canonical_sha256(case.model_dump(mode="json")),
        adaptive_expectation=SealedArmExpectation(
            tenant_id=assignment.adaptive_tenant_id,
            allowed_consumer_fates=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            ),
            expected_entity_ref=CanonicalEntityRef(
                type=_runtime_target(case).canonical_ref_type,
                id=str(assignment.adaptive_target_id),
            ),
            expected_model_count=0,
            autonomous_resolution_permitted=True,
        ),
        frozen_expectation=SealedArmExpectation(
            tenant_id=assignment.frozen_tenant_id,
            allowed_consumer_fates=(
                ConsumerTerminalFate.REVIEW,
                ConsumerTerminalFate.ABSTAINED,
            ),
            expected_entity_ref=CanonicalEntityRef(
                type=_runtime_target(case).canonical_ref_type,
                id=str(assignment.frozen_target_id),
            ),
            expected_model_count=0,
            autonomous_resolution_permitted=False,
        ),
        artifact_refs=(f"population-case:{case.case_id}",),
    )


def _runtime_target(case: HeldOutExactAliasCase) -> RuntimeEntityTarget:
    try:
        return _RUNTIME_TARGETS[case.entity_type]
    except KeyError as exc:
        raise ValueError(
            f"unsupported held-out logical entity type: {case.entity_type}"
        ) from exc


def _pair_observation(
    pair: PairedRecurrenceResult,
    *,
    assessment,
) -> HeldOutPairObservation:
    adaptive = assessment[(pair.case_id, CorrectiveMemoryArm.ADAPTIVE)]
    frozen = assessment[(pair.case_id, CorrectiveMemoryArm.FROZEN)]
    return HeldOutPairObservation(
        case_id=pair.case_id,
        adaptive_correct=adaptive.correct,
        frozen_correct=frozen.correct,
        adaptive_unsafe=bool(adaptive.incident_classes),
        frozen_unsafe=bool(frozen.incident_classes),
        adaptive_llm_calls=pair.adaptive.llm_call_count,
        frozen_llm_calls=pair.frozen.llm_call_count,
        adaptive_latency_ms=pair.adaptive.latency_ms,
        frozen_latency_ms=pair.frozen.latency_ms,
    )


async def _assert_fresh_tenants(
    pool: asyncpg.Pool,
    assignments: tuple[PopulationCaseAssignment, ...],
) -> None:
    tenant_ids = [
        tenant_id
        for row in assignments
        for tenant_id in (row.adaptive_tenant_id, row.frozen_tenant_id)
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise RuntimeError("population tenant assignments are not unique")
    async with pool.acquire() as conn:
        existing = await conn.fetch(
            "SELECT id FROM tenants WHERE id=ANY($1::uuid[])",
            tenant_ids,
        )
    if existing:
        raise RuntimeError("population execution requires fresh tenants")


async def _materialize_tenants(
    pool: asyncpg.Pool,
    assignment: PopulationCaseAssignment,
) -> None:
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO tenants (id) VALUES ($1)",
            (
                (assignment.adaptive_tenant_id,),
                (assignment.frozen_tenant_id,),
            ),
        )


async def _assert_pair_isolation(
    pool: asyncpg.Pool,
    adaptive: CorrectiveMemoryArmResult,
    frozen: CorrectiveMemoryArmResult,
) -> None:
    async with pool.acquire() as conn:
        cross = await conn.fetchval(
            """
            SELECT
              (SELECT count(*) FROM observations
               WHERE tenant_id=$1 AND id=$4)
              + (SELECT count(*) FROM observations
                 WHERE tenant_id=$2 AND id=$3)
            """,
            adaptive.tenant_id,
            frozen.tenant_id,
            adaptive.lineage.recurrence_observation_id,
            frozen.lineage.recurrence_observation_id,
        )
    if cross:
        raise RuntimeError("population pair tenants influenced each other")


def _distance(value: str) -> timedelta:
    return {
        "same_day": timedelta(hours=1),
        "one_week": timedelta(days=7),
        "one_month": timedelta(days=30),
        "one_quarter": timedelta(days=90),
    }[value]


async def _run(args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL or --dsn is required")
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=6,
        init=_register_codecs,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, ROOT / "db" / "migrations")
        evidence = await run_population_experiment(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
            case_limit=args.case_limit,
            bootstrap_samples=args.bootstrap_samples,
            llm_call_cost_usd=args.llm_call_cost_usd,
            population_path=args.population,
        )
    finally:
        await pool.close()
    report = evidence.population_report
    print(f"artifact={args.output_dir / ARTIFACT_NAME}")
    print(
        "mode={mode} registry={registry} observed={observed} "
        "unsupported={unsupported} adaptive={adaptive} frozen={frozen} "
        "lift={lift}".format(
            mode=evidence.execution_mode,
            registry=report.pair_count,
            observed=report.observed_pair_count,
            unsupported=report.unsupported_case_count,
            adaptive=report.adaptive_correctness.point_estimate,
            frozen=report.frozen_correctness.point_estimate,
            lift=report.adaptive_minus_frozen_correctness.point_estimate,
        )
    )
    return 2 if evidence.experiment_report.incidents else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    parser.add_argument("--population", type=Path, default=DEFAULT_POPULATION)
    parser.add_argument("--case-limit", type=int, default=None)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    parser.add_argument("--llm-call-cost-usd", type=float, default=0.001)
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
