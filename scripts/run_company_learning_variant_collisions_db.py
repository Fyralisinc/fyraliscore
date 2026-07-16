#!/usr/bin/env python3
"""Run the sealed governed-variant collision population on real Postgres."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Self
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    HardSafetyIncidentClass,
    RecurrenceCaseKind,
)
from lib.evaluation.company_learning_variant_collisions import (
    HeldOutVariantCollisionCase,
    HeldOutVariantCollisionPopulation,
    VariantCollisionArmObservation,
    VariantCollisionDecisionBasis,
    VariantCollisionFamily,
    VariantCollisionPairObservation,
    VariantCollisionPopulationReport,
    VariantCollisionTargetRole,
    evaluate_variant_collision_population,
    load_variant_collision_population,
)
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from scripts.company_learning_recurrence_runtime import (
    NegativeControlAssignment,
    NegativeControlCaseDefinition,
)
from scripts.run_company_learning_negative_controls_db import (
    _NegativeArmFoundation,
    _prepare_negative_arm,
    _set_observation_grounding_inputs,
)
from scripts.run_company_learning_pair_harness import (
    _consumer_fate,
    _ingest_slack,
    _json,
    _observation_snapshot,
)
from scripts.run_company_learning_population_harness import _RUNTIME_TARGETS
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.entity_aliases.repo import normalize_phrase


DEFAULT_COLLISION_POPULATION = (
    ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_variant_collision_population_v1.jsonl"
)
ARTIFACT_NAME = "company_learning_variant_collision_evidence.json"
_SOURCE_ID_UNSUPPORTED = (
    "runtime lacks authenticated SourceIdentityBinding evidence"
)


@dataclass(frozen=True)
class _CollisionRuntimeArm:
    tenant_id: UUID
    observation_id: UUID
    observation: VariantCollisionArmObservation


class _EvidenceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class VariantCollisionRuntimeAssignment(_EvidenceModel):
    case_id: str = Field(min_length=1)
    adaptive_tenant_id: UUID
    frozen_tenant_id: UUID
    adaptive_target_id: UUID
    frozen_target_id: UUID
    adaptive_conflicting_id: UUID
    frozen_conflicting_id: UUID


class CompanyLearningVariantCollisionEvidence(_EvidenceModel):
    schema_version: Literal[
        "company-learning-variant-collision-evidence-v1"
    ] = "company-learning-variant-collision-evidence-v1"
    created_at: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    registry_path: str = Field(min_length=1)
    registry_population: HeldOutVariantCollisionPopulation
    registry_population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    assignments: tuple[VariantCollisionRuntimeAssignment, ...]
    observations: tuple[VariantCollisionPairObservation, ...]
    report: VariantCollisionPopulationReport
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def exact_runtime_accounting(self) -> Self:
        registry_ids = tuple(
            case.case_id for case in self.registry_population.cases
        )
        if self.registry_population_digest != self.registry_population.digest:
            raise ValueError("collision registry digest mismatch")
        if tuple(row.case_id for row in self.assignments) != registry_ids:
            raise ValueError("collision assignments changed registry order")
        if tuple(row.case_id for row in self.observations) != registry_ids:
            raise ValueError("collision observations changed registry order")
        observed = sum(
            row.execution_status == "observed"
            for row in self.observations
        )
        unsupported = len(self.observations) - observed
        if observed != 14 or unsupported != 2:
            raise ValueError(
                "collision runtime must retain 14 observed and 2 unsupported"
            )
        if (
            self.report.population_digest
            != self.registry_population_digest
            or self.report.observed_pair_count != observed
            or self.report.unsupported_case_count != unsupported
        ):
            raise ValueError("collision report does not match runtime envelope")
        tenant_ids = [
            tenant_id
            for row in self.assignments
            for tenant_id in (
                row.adaptive_tenant_id,
                row.frozen_tenant_id,
            )
        ]
        if len(tenant_ids) != len(set(tenant_ids)):
            raise ValueError("collision runtime tenants must be unique")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(mode="json"),
            "evidence_digest": self.digest,
        }


async def run_variant_collision_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
    population_path: Path = DEFAULT_COLLISION_POPULATION,
) -> CompanyLearningVariantCollisionEvidence:
    """Execute supported collisions and retain unsupported source-ID cases."""

    registry = load_variant_collision_population(population_path)
    assignments = tuple(_assignment(case) for case in registry.cases)
    await _assert_fresh_tenants(pool=pool, assignments=assignments)
    created_at = datetime.now(timezone.utc)
    observations: list[VariantCollisionPairObservation] = []

    for case, assignment in zip(
        registry.cases,
        assignments,
        strict=True,
    ):
        if (
            case.collision_family
            is VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
        ):
            observations.append(
                VariantCollisionPairObservation(
                    case_id=case.case_id,
                    execution_status="unsupported",
                    unsupported_reason=_SOURCE_ID_UNSUPPORTED,
                )
            )
            continue
        definition = _runtime_definition(case)
        runtime_assignment = _runtime_assignment(assignment)
        adaptive_foundation = await _prepare_collision_arm(
            pool=pool,
            case=case,
            definition=definition,
            assignment=runtime_assignment,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            training_at=created_at,
        )
        frozen_foundation = await _prepare_collision_arm(
            pool=pool,
            case=case,
            definition=definition,
            assignment=runtime_assignment,
            arm=CorrectiveMemoryArm.FROZEN,
            training_at=created_at,
        )
        adaptive = await _run_collision_recurrence(
            pool=pool,
            case=case,
            foundation=adaptive_foundation,
            occurred_at=created_at,
        )
        frozen = await _run_collision_recurrence(
            pool=pool,
            case=case,
            foundation=frozen_foundation,
            occurred_at=created_at,
        )
        await _assert_runtime_pair_isolation(
            pool=pool,
            adaptive=adaptive,
            frozen=frozen,
        )
        observations.append(
            VariantCollisionPairObservation(
                case_id=case.case_id,
                adaptive=adaptive.observation,
                frozen=frozen.observation,
            )
        )

    typed_observations = tuple(observations)
    report = evaluate_variant_collision_population(
        population=registry,
        observations=typed_observations,
    )
    evidence = CompanyLearningVariantCollisionEvidence(
        created_at=created_at.isoformat(),
        run_id=run_id,
        system_version=system_version,
        registry_path=str(population_path.resolve()),
        registry_population=registry,
        registry_population_digest=registry.digest,
        assignments=assignments,
        observations=typed_observations,
        report=report,
        artifact_refs=(
            f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",
        ),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(evidence.artifact_payload(), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return evidence


def _assignment(
    case: HeldOutVariantCollisionCase,
) -> VariantCollisionRuntimeAssignment:
    return VariantCollisionRuntimeAssignment(
        case_id=case.case_id,
        adaptive_tenant_id=uuid7(),
        frozen_tenant_id=uuid7(),
        adaptive_target_id=uuid7(),
        frozen_target_id=uuid7(),
        adaptive_conflicting_id=uuid7(),
        frozen_conflicting_id=uuid7(),
    )


def _runtime_assignment(
    assignment: VariantCollisionRuntimeAssignment,
) -> NegativeControlAssignment:
    return NegativeControlAssignment(
        case_id=assignment.case_id,
        adaptive_tenant_id=assignment.adaptive_tenant_id,
        frozen_tenant_id=assignment.frozen_tenant_id,
        adaptive_target_id=assignment.adaptive_target_id,
        frozen_target_id=assignment.frozen_target_id,
        adaptive_conflicting_id=assignment.adaptive_conflicting_id,
        frozen_conflicting_id=assignment.frozen_conflicting_id,
    )


def _runtime_definition(
    case: HeldOutVariantCollisionCase,
) -> NegativeControlCaseDefinition:
    return NegativeControlCaseDefinition(
        case_id=case.case_id,
        kind=RecurrenceCaseKind.HOMONYM_LOCAL_ASSOCIATION,
        entity_type=(
            _RUNTIME_TARGETS[case.learned_entity_type].canonical_ref_type
        ),
        slack_context="cross_thread_recurrence",
        wording_variant="risk_report",
        consequence="high",
        recurrence_distance=1,
        alias_surface=case.collision_surface,
        training_text=case.training_text,
        training_phrase=case.learned_surface,
        candidate_alias=case.learned_entity_label,
        recurrence_text=case.recurrence_text,
        recurrence_phrase=case.collision_surface,
        channel=case.collision_channel,
        resolution_scope="tenant_global_exact",
        inject_conflicting_source_hint=False,
        recurrence_response="target_low",
        expected_model_count=0,
    )


async def _prepare_collision_arm(
    *,
    pool: asyncpg.Pool,
    case: HeldOutVariantCollisionCase,
    definition: NegativeControlCaseDefinition,
    assignment: NegativeControlAssignment,
    arm: CorrectiveMemoryArm,
    training_at: datetime,
) -> _NegativeArmFoundation:
    foundation = await _prepare_negative_arm(
        pool=pool,
        definition=definition,
        assignment=assignment,
        arm=arm,
        training_at=training_at,
        runtime_target=_RUNTIME_TARGETS[case.learned_entity_type],
        conflicting_runtime_target=(
            _RUNTIME_TARGETS[case.conflicting_entity_type]
        ),
        conflicting_target_label=case.conflicting_entity_label,
        training_channel=case.learned_channel,
        training_phrases=(case.learned_surface,),
        recurrence_confidence=0.99,
    )
    if case.learned_lifecycle.value != "active":
        await _retire_learned_target(
            pool=pool,
            case=case,
            foundation=foundation,
        )
    return foundation


async def _retire_learned_target(
    *,
    pool: asyncpg.Pool,
    case: HeldOutVariantCollisionCase,
    foundation: _NegativeArmFoundation,
) -> None:
    target = _RUNTIME_TARGETS[case.learned_entity_type]
    async with pool.acquire() as conn:
        if target.canonical_ref_type == "actor":
            await conn.execute(
                """
                UPDATE actors SET status='inactive'
                WHERE tenant_id=$1 AND id=$2
                """,
                foundation.tenant_id,
                foundation.target_id,
            )
        else:
            await conn.execute(
                """
                UPDATE resources SET archived_at=now()
                WHERE tenant_id=$1 AND id=$2
                """,
                foundation.tenant_id,
                foundation.target_id,
            )


async def _run_collision_recurrence(
    *,
    pool: asyncpg.Pool,
    case: HeldOutVariantCollisionCase,
    foundation: _NegativeArmFoundation,
    occurred_at: datetime,
) -> _CollisionRuntimeArm:
    observation_id = await _ingest_slack(
        pool=pool,
        tenant_id=foundation.tenant_id,
        alias_repo=foundation.alias_repo,
        text=case.recurrence_text,
        channel=case.collision_channel,
        occurred_at=occurred_at,
        corrective_memory_reuse_enabled=(
            foundation.arm is CorrectiveMemoryArm.ADAPTIVE
        ),
    )
    learned_ref, conflicting_ref = _collision_refs(
        case=case,
        foundation=foundation,
    )
    await _set_observation_grounding_inputs(
        pool=pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
        phrases=(case.collision_surface,),
        entities=(
            learned_ref.model_dump(mode="json"),
            conflicting_ref.model_dump(mode="json"),
        ),
    )
    before = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    await foundation.worker.process_observation(
        observation_id,
        foundation.tenant_id,
    )
    after = await _observation_snapshot(
        pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
    )
    rows = await _collision_recurrence_rows(
        pool=pool,
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
        phrase=case.collision_surface,
    )
    trace = rows["trace"]
    candidates = _json(trace["candidates"]) if trace else []
    if not isinstance(candidates, list):
        candidates = []
    visible_refs = tuple(
        dict.fromkeys(
            ref
            for candidate in candidates
            if (ref := _candidate_ref(candidate)) is not None
        )
    )
    selected = _json(trace["selected_referent"]) if trace else None
    resolved = _canonical_ref(selected)
    current_fate = (
        _consumer_fate(str(trace["current_fate"] or ""))
        if trace
        else ConsumerTerminalFate.ABSTAINED
    )
    resolved_role = _resolved_role(
        resolved=resolved,
        learned_ref=learned_ref,
        conflicting_ref=conflicting_ref,
    )
    decision_basis = (
        VariantCollisionDecisionBasis.UNRESOLVED_COLLISION
        if resolved is None
        else VariantCollisionDecisionBasis.LEARNED_AMBIGUOUS_VARIANT
        if resolved_role is VariantCollisionTargetRole.LEARNED
        else VariantCollisionDecisionBasis.OTHER_UNSEALED_EVIDENCE
    )
    incidents = set()
    if before != after:
        incidents.add(HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED)
    if rows["self_authored"]:
        incidents.add(HardSafetyIncidentClass.SELF_AUTHORITATIVE_EVIDENCE)
    promoted = bool(rows["promoted_alias_count"])
    canonical_candidates = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("kind") == "canonical_referent"
    ]
    observation = VariantCollisionArmObservation(
        arm=foundation.arm,
        consumer_fate=current_fate,
        resolved_entity_ref=resolved,
        decision_basis=decision_basis,
        resolved_target_role=resolved_role,
        learned_alias_promoted=promoted,
        candidate_set_digest=canonical_sha256(
            [ref.model_dump(mode="json") for ref in visible_refs]
        ),
        candidate_set_size=len(visible_refs),
        visible_candidate_refs=visible_refs,
        learned_candidate_ref=learned_ref,
        conflicting_candidate_ref=conflicting_ref,
        both_colliding_candidates_visible=(
            learned_ref in visible_refs and conflicting_ref in visible_refs
        ),
        none_of_above_available=any(
            isinstance(candidate, dict)
            and candidate.get("kind") == "none_of_the_above"
            for candidate in candidates
        ),
        wrong_model_count=len(rows["model_ids"]),
        source_observation_immutable=before == after,
        observed_safety_incidents=frozenset(incidents),
        artifact_refs=(
            f"observation:{observation_id}",
            f"candidate-set-size:{len(canonical_candidates)}",
            f"grounding-trace:{trace['grounding_trace_id'] if trace else 'missing'}",
        ),
    )
    return _CollisionRuntimeArm(
        tenant_id=foundation.tenant_id,
        observation_id=observation_id,
        observation=observation,
    )


def _collision_refs(
    *,
    case: HeldOutVariantCollisionCase,
    foundation: _NegativeArmFoundation,
) -> tuple[CanonicalEntityRef, CanonicalEntityRef]:
    return (
        CanonicalEntityRef(
            type=(
                _RUNTIME_TARGETS[
                    case.learned_entity_type
                ].canonical_ref_type
            ),
            id=str(foundation.target_id),
        ),
        CanonicalEntityRef(
            type=(
                _RUNTIME_TARGETS[
                    case.conflicting_entity_type
                ].canonical_ref_type
            ),
            id=str(foundation.conflicting_id),
        ),
    )


def _resolved_role(
    *,
    resolved: CanonicalEntityRef | None,
    learned_ref: CanonicalEntityRef,
    conflicting_ref: CanonicalEntityRef,
) -> VariantCollisionTargetRole | None:
    if resolved is None:
        return None
    if resolved == learned_ref:
        return VariantCollisionTargetRole.LEARNED
    if resolved == conflicting_ref:
        return VariantCollisionTargetRole.CONFLICTING
    return VariantCollisionTargetRole.OTHER


def _canonical_ref(value: Any) -> CanonicalEntityRef | None:
    payload = _json(value)
    if not isinstance(payload, dict):
        return None
    if not payload.get("type") or not payload.get("id"):
        return None
    return CanonicalEntityRef.model_validate(payload)


def _candidate_ref(candidate: Any) -> CanonicalEntityRef | None:
    if not isinstance(candidate, dict):
        return None
    if candidate.get("kind") != "canonical_referent":
        return None
    if not candidate.get("candidate_type") or not candidate.get(
        "canonical_referent_id"
    ):
        return None
    return CanonicalEntityRef(
        type=str(candidate["candidate_type"]),
        id=str(candidate["canonical_referent_id"]),
        version=int(candidate.get("canonical_referent_version") or 1),
    )


async def _collision_recurrence_rows(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        trace = await conn.fetchrow(
            """
            SELECT trace.id AS grounding_trace_id,
                   trace.current_fate,
                   trace.selected_referent,
                   candidate_set.candidates
            FROM grounding_traces trace
            LEFT JOIN entity_candidate_sets candidate_set
              ON candidate_set.tenant_id=trace.tenant_id
             AND candidate_set.id=trace.candidate_set_id
            WHERE trace.tenant_id=$1
              AND trace.source_observation_id=$2
              AND regexp_replace(lower(trace.phrase), '\\s+', ' ', 'g')=$3
            ORDER BY trace.created_at DESC, trace.id DESC
            LIMIT 1
            """,
            tenant_id,
            observation_id,
            normalize_phrase(phrase),
        )
        model_ids = tuple(
            row["id"]
            for row in await conn.fetch(
                """
                SELECT id FROM models
                WHERE tenant_id=$1 AND born_from_event_id=$2
                ORDER BY id
                """,
                tenant_id,
                observation_id,
            )
        )
        promoted_alias_count = await conn.fetchval(
            """
            SELECT count(*) FROM entity_aliases
            WHERE tenant_id=$1
              AND source_event_id=$2
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')=$3
            """,
            tenant_id,
            observation_id,
            normalize_phrase(phrase),
        )
        self_authored = await conn.fetchval(
            """
            SELECT count(*) FROM observations
            WHERE tenant_id=$1
              AND source_channel='internal:state_change'
              AND content ->> 'source_observation_id'=$2
            """,
            tenant_id,
            str(observation_id),
        )
    return {
        "trace": trace,
        "model_ids": model_ids,
        "promoted_alias_count": int(promoted_alias_count or 0),
        "self_authored": int(self_authored or 0),
    }


async def _assert_fresh_tenants(
    *,
    pool: asyncpg.Pool,
    assignments: tuple[VariantCollisionRuntimeAssignment, ...],
) -> None:
    tenant_ids = [
        tenant_id
        for row in assignments
        for tenant_id in (row.adaptive_tenant_id, row.frozen_tenant_id)
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise RuntimeError("collision runtime tenant assignments repeat")
    async with pool.acquire() as conn:
        existing = await conn.fetch(
            "SELECT id FROM tenants WHERE id=ANY($1::uuid[])",
            tenant_ids,
        )
    if existing:
        raise RuntimeError("collision runtime requires fresh tenants")


async def _assert_runtime_pair_isolation(
    *,
    pool: asyncpg.Pool,
    adaptive: _CollisionRuntimeArm,
    frozen: _CollisionRuntimeArm,
) -> None:
    if adaptive.tenant_id == frozen.tenant_id:
        raise RuntimeError("collision arms reused one tenant")
    async with pool.acquire() as conn:
        cross_count = await conn.fetchval(
            """
            SELECT
              (SELECT count(*) FROM observations
               WHERE tenant_id=$1 AND id=$4)
              + (SELECT count(*) FROM observations
                 WHERE tenant_id=$2 AND id=$3)
              + (SELECT count(*) FROM grounding_traces
                 WHERE tenant_id=$1 AND source_observation_id=$4)
              + (SELECT count(*) FROM grounding_traces
                 WHERE tenant_id=$2 AND source_observation_id=$3)
            """,
            adaptive.tenant_id,
            frozen.tenant_id,
            adaptive.observation_id,
            frozen.observation_id,
        )
    if cross_count:
        raise RuntimeError("collision arm tenants influenced each other")


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
        evidence = await run_variant_collision_experiment(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
            population_path=args.population,
        )
    finally:
        await pool.close()
    print(json.dumps(evidence.report.model_dump(mode="json"), indent=2))
    return 2 if evidence.report.status == "contradicted" else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="Postgres DSN; defaults to DATABASE_URL")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    parser.add_argument(
        "--population",
        type=Path,
        default=DEFAULT_COLLISION_POPULATION,
    )
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
