#!/usr/bin/env python3
"""Run the sealed customer-identity lifecycle proof on real Postgres."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
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
from lib.evaluation.company_learning_customer_lifecycle import (
    CustomerAliasIntervalEvidence,
    CustomerLifecycleCase,
    CustomerLifecycleObservation,
    CustomerLifecyclePopulation,
    CustomerLifecycleReport,
    CustomerRef,
    CustomerResolutionProbe,
    ResolutionProbeCategory,
    ResolutionProbeRole,
    evaluate_customer_lifecycle_population,
    load_customer_lifecycle_population,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.resources import repo as resources_repo


DEFAULT_LIFECYCLE_POPULATION = (
    ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "held_out_customer_lifecycle_population_v1.jsonl"
)
ARTIFACT_NAME = "company_learning_customer_lifecycle_evidence.json"


class _EvidenceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class CustomerLifecycleRuntimeAssignment(_EvidenceModel):
    case_id: str = Field(min_length=1)
    tenant_id: UUID
    isolation_tenant_id: UUID


class CompanyLearningCustomerLifecycleEvidence(_EvidenceModel):
    schema_version: Literal["company-learning-customer-lifecycle-evidence-v1"] = (
        "company-learning-customer-lifecycle-evidence-v1"
    )
    created_at: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    registry_path: str = Field(min_length=1)
    registry_population: CustomerLifecyclePopulation
    registry_population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    assignments: tuple[CustomerLifecycleRuntimeAssignment, ...]
    observations: tuple[CustomerLifecycleObservation, ...]
    report: CustomerLifecycleReport
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def exact_runtime_accounting(self) -> Self:
        registry_ids = tuple(case.case_id for case in self.registry_population.cases)
        if self.registry_population_digest != self.registry_population.digest:
            raise ValueError("customer lifecycle registry digest mismatch")
        if tuple(row.case_id for row in self.assignments) != registry_ids:
            raise ValueError("lifecycle assignments changed registry order")
        if tuple(row.case_id for row in self.observations) != registry_ids:
            raise ValueError("lifecycle observations changed registry order")
        if any(row.execution_status != "observed" for row in self.observations):
            raise ValueError("lifecycle runtime must execute all sealed cases")
        tenant_ids = [
            tenant_id
            for row in self.assignments
            for tenant_id in (row.tenant_id, row.isolation_tenant_id)
        ]
        if len(tenant_ids) != len(set(tenant_ids)):
            raise ValueError("lifecycle runtime tenants must be unique")
        if (
            self.report.population_digest != self.registry_population_digest
            or self.report.observed_case_count != len(self.observations)
            or self.report.unsupported_case_count
        ):
            raise ValueError("lifecycle report does not match runtime evidence")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(mode="json"),
            "evidence_digest": self.digest,
        }


async def run_customer_lifecycle_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
    population_path: Path = DEFAULT_LIFECYCLE_POPULATION,
) -> CompanyLearningCustomerLifecycleEvidence:
    registry = load_customer_lifecycle_population(population_path)
    assignments = tuple(
        CustomerLifecycleRuntimeAssignment(
            case_id=case.case_id,
            tenant_id=uuid7(),
            isolation_tenant_id=uuid7(),
        )
        for case in registry.cases
    )
    await _assert_fresh_tenants(pool=pool, assignments=assignments)
    observations = []
    for case, assignment in zip(
        registry.cases,
        assignments,
        strict=True,
    ):
        observations.append(
            await _run_lifecycle_case(
                pool=pool,
                case=case,
                assignment=assignment,
            )
        )
    typed_observations = tuple(observations)
    report = evaluate_customer_lifecycle_population(
        population=registry,
        observations=typed_observations,
    )
    created_at = datetime.now(timezone.utc)
    evidence = CompanyLearningCustomerLifecycleEvidence(
        created_at=created_at.isoformat(),
        run_id=run_id,
        system_version=system_version,
        registry_path=str(population_path.resolve()),
        registry_population=registry,
        registry_population_digest=registry.digest,
        assignments=assignments,
        observations=typed_observations,
        report=report,
        artifact_refs=(f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(evidence.artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return evidence


async def _run_lifecycle_case(
    *,
    pool: asyncpg.Pool,
    case: CustomerLifecycleCase,
    assignment: CustomerLifecycleRuntimeAssignment,
) -> CustomerLifecycleObservation:
    await _insert_tenant(pool, assignment.tenant_id)
    await _insert_tenant(pool, assignment.isolation_tenant_id)
    create_event_id = await _insert_observation(pool, assignment.tenant_id)
    isolation_event_id = await _insert_observation(
        pool,
        assignment.isolation_tenant_id,
    )
    async with pool.acquire() as conn, conn.transaction():
        customer = await resources_repo.create(
            kind="relational",
            identity=case.initial_identity,
            description="Lifecycle proof customer",
            current_value={"arr_usd": 100_000},
            metadata={"semantic_kind": "customer"},
            tenant_id=assignment.tenant_id,
            created_by_event_id=create_event_id,
            conn=conn,
        )
        isolation_customer = await resources_repo.create(
            kind="relational",
            identity=case.initial_identity,
            description="Tenant-isolation control customer",
            current_value={"arr_usd": 1},
            metadata={"semantic_kind": "customer"},
            tenant_id=assignment.isolation_tenant_id,
            created_by_event_id=isolation_event_id,
            conn=conn,
        )
    canonical_ref = CustomerRef(id=str(customer.id))
    isolation_ref = CustomerRef(id=str(isolation_customer.id))
    evidence_event_id = await _insert_observation(
        pool,
        assignment.tenant_id,
        entities=(canonical_ref,),
    )
    model_id = await _insert_model(
        pool,
        tenant_id=assignment.tenant_id,
        event_id=evidence_event_id,
        customer_ref=canonical_ref,
    )
    old_observation_before = await _observation_refs(
        pool,
        evidence_event_id,
    )
    old_model_before = await _model_refs(pool, model_id)

    rename_event_id = await _insert_observation(pool, assignment.tenant_id)
    async with pool.acquire() as conn, conn.transaction():
        renamed = await resources_repo.rename_customer(
            customer.id,
            new_identity=case.renamed_identity,
            cause_event_id=rename_event_id,
            conn=conn,
        )
    aliases = EntityAliasRepo(pool)
    old_history_after_rename = await aliases.list_history(
        case.initial_identity,
        assignment.tenant_id,
    )
    rename_at = old_history_after_rename[0]["valid_until"]
    if rename_at is None:
        raise RuntimeError("customer rename did not close the old name")
    before_rename = customer.created_at + (rename_at - customer.created_at) / 2
    post_rename_stale = await aliases.fast_path_resolve(
        case.initial_identity,
        assignment.tenant_id,
    )
    current_renamed = await aliases.fast_path_resolve(
        case.renamed_identity,
        assignment.tenant_id,
    )

    rename_aliases_before = await _alias_count(
        pool,
        assignment.tenant_id,
        canonical_ref,
    )
    rename_events_before = await _state_event_count(
        pool,
        assignment.tenant_id,
        customer.id,
        "customer_renamed",
    )
    rename_replay_event_id = await _insert_observation(
        pool,
        assignment.tenant_id,
    )
    async with pool.acquire() as conn, conn.transaction():
        rename_replay = await resources_repo.rename_customer(
            customer.id,
            new_identity=case.renamed_identity,
            cause_event_id=rename_replay_event_id,
            conn=conn,
        )
    rename_aliases_after = await _alias_count(
        pool,
        assignment.tenant_id,
        canonical_ref,
    )
    rename_events_after = await _state_event_count(
        pool,
        assignment.tenant_id,
        customer.id,
        "customer_renamed",
    )

    archive_event_id = await _insert_observation(pool, assignment.tenant_id)
    async with pool.acquire() as conn, conn.transaction():
        archived = await resources_repo.archive(
            customer.id,
            reason="customer relationship ended",
            cause_event_id=archive_event_id,
            conn=conn,
        )
    if archived.archived_at is None:
        raise RuntimeError("customer archive did not set archived_at")
    before_archive = rename_at + (archived.archived_at - rename_at) / 2
    delayed_renamed = await aliases.fast_path_resolve(
        case.renamed_identity,
        assignment.tenant_id,
        as_of=before_archive,
    )
    archived_current = await aliases.fast_path_resolve(
        case.renamed_identity,
        assignment.tenant_id,
    )
    archived_at_boundary = await aliases.fast_path_resolve(
        case.renamed_identity,
        assignment.tenant_id,
        as_of=archived.archived_at,
    )

    archive_aliases_before = await _alias_count(
        pool,
        assignment.tenant_id,
        canonical_ref,
    )
    archive_events_before = await _state_event_count(
        pool,
        assignment.tenant_id,
        customer.id,
        "resource_archived",
    )
    archive_replay_event_id = await _insert_observation(
        pool,
        assignment.tenant_id,
    )
    async with pool.acquire() as conn, conn.transaction():
        archive_replay = await resources_repo.archive(
            customer.id,
            reason="replayed archive",
            cause_event_id=archive_replay_event_id,
            conn=conn,
        )
    archive_aliases_after = await _alias_count(
        pool,
        assignment.tenant_id,
        canonical_ref,
    )
    archive_events_after = await _state_event_count(
        pool,
        assignment.tenant_id,
        customer.id,
        "resource_archived",
    )

    post_archive_rename_rejected = False
    rejected_rename_event_id = await _insert_observation(
        pool,
        assignment.tenant_id,
    )
    try:
        async with pool.acquire() as conn, conn.transaction():
            await resources_repo.rename_customer(
                customer.id,
                new_identity=f"{case.renamed_identity} Again",
                cause_event_id=rejected_rename_event_id,
                conn=conn,
            )
    except InvariantViolation:
        post_archive_rename_rejected = True

    probes = [
        _probe(
            role=ResolutionProbeRole.PRE_RENAME_OLD_NAME,
            phrase=case.initial_identity,
            as_of=before_rename,
            expected=canonical_ref,
            observed=await aliases.fast_path_resolve(
                case.initial_identity,
                assignment.tenant_id,
                as_of=before_rename,
            ),
        ),
        _probe(
            role=ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME,
            phrase=case.initial_identity,
            as_of=rename_at,
            expected=None,
            observed=post_rename_stale,
        ),
        _probe(
            role=ResolutionProbeRole.CURRENT_RENAMED_NAME,
            phrase=case.renamed_identity,
            as_of=None,
            expected=canonical_ref,
            observed=current_renamed,
        ),
        _probe(
            role=ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME,
            phrase=case.renamed_identity,
            as_of=before_archive,
            expected=canonical_ref,
            observed=delayed_renamed,
        ),
        _probe(
            role=ResolutionProbeRole.POST_ARCHIVE_REJECTION,
            phrase=case.renamed_identity,
            as_of=archived.archived_at,
            expected=None,
            observed=archived_at_boundary or archived_current,
        ),
        _probe(
            role=ResolutionProbeRole.TENANT_ISOLATION,
            phrase=case.initial_identity,
            as_of=None,
            expected=isolation_ref,
            observed=await aliases.fast_path_resolve(
                case.initial_identity,
                assignment.isolation_tenant_id,
            ),
        ),
    ]
    if case.reuse_initial_identity:
        reuse_event_id = await _insert_observation(pool, assignment.tenant_id)
        async with pool.acquire() as conn, conn.transaction():
            reused = await resources_repo.create(
                kind="relational",
                identity=case.initial_identity,
                description="Later customer reusing a historical name",
                current_value={"arr_usd": 25_000},
                metadata={"semantic_kind": "customer"},
                tenant_id=assignment.tenant_id,
                created_by_event_id=reuse_event_id,
                conn=conn,
            )
        reused_ref = CustomerRef(id=str(reused.id))
        probes.extend(
            (
                _probe(
                    role=ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
                    phrase=case.initial_identity,
                    as_of=before_rename,
                    expected=canonical_ref,
                    observed=await aliases.fast_path_resolve(
                        case.initial_identity,
                        assignment.tenant_id,
                        as_of=before_rename,
                    ),
                ),
                _probe(
                    role=ResolutionProbeRole.CURRENT_REUSED_OLD_NAME,
                    phrase=case.initial_identity,
                    as_of=None,
                    expected=reused_ref,
                    observed=await aliases.fast_path_resolve(
                        case.initial_identity,
                        assignment.tenant_id,
                    ),
                ),
            )
        )

    old_observation_after = await _observation_refs(pool, evidence_event_id)
    old_model_after = await _model_refs(pool, model_id)
    intervals = await _alias_intervals(
        aliases=aliases,
        case=case,
        tenant_id=assignment.tenant_id,
    )
    return CustomerLifecycleObservation(
        case_id=case.case_id,
        canonical_ref_before=canonical_ref,
        canonical_ref_after_rename=CustomerRef(id=str(renamed.id)),
        canonical_ref_after_archive=CustomerRef(id=str(archived.id)),
        resolution_probes=tuple(probes),
        alias_intervals=intervals,
        old_observation_before=old_observation_before,
        old_observation_after=old_observation_after,
        old_model_before=old_model_before,
        old_model_after=old_model_after,
        rename_replay_alias_count_before=rename_aliases_before,
        rename_replay_alias_count_after=rename_aliases_after,
        rename_replay_event_count_before=rename_events_before,
        rename_replay_event_count_after=rename_events_after,
        archive_replay_alias_count_before=archive_aliases_before,
        archive_replay_alias_count_after=archive_aliases_after,
        archive_replay_event_count_before=archive_events_before,
        archive_replay_event_count_after=archive_events_after,
        post_archive_rename_rejected=post_archive_rename_rejected,
        artifact_refs=(
            f"postgres:tenant:{assignment.tenant_id}",
            f"postgres:customer:{customer.id}",
            f"postgres:observation:{evidence_event_id}",
            f"postgres:model:{model_id}",
            f"postgres:rename-replay:{rename_replay.id}",
            f"postgres:archive-replay:{archive_replay.id}",
        ),
    )


def _probe(
    *,
    role: ResolutionProbeRole,
    phrase: str,
    as_of: datetime | None,
    expected: CustomerRef | None,
    observed: dict[str, Any] | CustomerRef | None,
) -> CustomerResolutionProbe:
    observed_ref = (
        observed
        if isinstance(observed, CustomerRef)
        else CustomerRef.model_validate(observed)
        if observed is not None
        else None
    )
    categories = {
        ResolutionProbeRole.PRE_RENAME_OLD_NAME: (ResolutionProbeCategory.VALID_TIME,),
        ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME: (
            ResolutionProbeCategory.VALID_TIME,
            ResolutionProbeCategory.STALE_ALIAS_REJECTION,
        ),
        ResolutionProbeRole.CURRENT_RENAMED_NAME: (
            ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
        ),
        ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME: (
            ResolutionProbeCategory.VALID_TIME,
            ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
        ),
        ResolutionProbeRole.POST_ARCHIVE_REJECTION: (
            ResolutionProbeCategory.VALID_TIME,
            ResolutionProbeCategory.ARCHIVE_REJECTION,
        ),
        ResolutionProbeRole.TENANT_ISOLATION: (
            ResolutionProbeCategory.TENANT_ISOLATION,
        ),
        ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME: (
            ResolutionProbeCategory.VALID_TIME,
            ResolutionProbeCategory.HISTORICAL_NAME_REUSE,
        ),
        ResolutionProbeRole.CURRENT_REUSED_OLD_NAME: (
            ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
            ResolutionProbeCategory.HISTORICAL_NAME_REUSE,
        ),
    }[role]
    return CustomerResolutionProbe(
        probe_id=role.value,
        role=role,
        phrase=phrase,
        as_of=as_of,
        categories=categories,
        expected_ref=expected,
        observed_ref=observed_ref,
    )


async def _insert_tenant(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    await pool.execute(
        "INSERT INTO tenants (id) VALUES ($1)",
        tenant_id,
    )


async def _insert_observation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    entities: tuple[CustomerRef, ...] = (),
) -> UUID:
    observation_id = uuid7()
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, trust_tier, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'test:customer-lifecycle',
            '{}'::jsonb, 'customer lifecycle proof', 'authoritative',
            $3::jsonb
        )
        """,
        observation_id,
        tenant_id,
        json.dumps([entity.model_dump(mode="json") for entity in entities]),
    )
    return observation_id


async def _insert_model(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    event_id: UUID,
    customer_ref: CustomerRef,
) -> UUID:
    model_id = uuid7()
    await pool.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            contributing_models, status, confidence_at_assertion
        ) VALUES (
            $1, $2, $3,
            '{"kind":"state","subject":"customer","assertion":"active"}'::jsonb,
            'Customer is active', array_fill(0.0::real, ARRAY[768])::vector,
            '{}'::uuid[], $4::jsonb,
            '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
            0.6, NULL, '[]'::jsonb,
            ARRAY[$3]::uuid[], '{}'::uuid[],
            '{}'::uuid[], 'active', 0.6
        )
        """,
        model_id,
        tenant_id,
        event_id,
        json.dumps([customer_ref.model_dump(mode="json")]),
    )
    return model_id


async def _observation_refs(
    pool: asyncpg.Pool,
    observation_id: UUID,
) -> tuple[CustomerRef, ...]:
    value = await pool.fetchval(
        "SELECT entities_mentioned FROM observations WHERE id=$1",
        observation_id,
    )
    return tuple(CustomerRef.model_validate(item) for item in value)


async def _model_refs(
    pool: asyncpg.Pool,
    model_id: UUID,
) -> tuple[CustomerRef, ...]:
    value = await pool.fetchval(
        "SELECT scope_entities FROM models WHERE id=$1",
        model_id,
    )
    return tuple(CustomerRef.model_validate(item) for item in value)


async def _alias_count(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    customer_ref: CustomerRef,
) -> int:
    value = await pool.fetchval(
        """
        SELECT count(*) FROM entity_aliases
        WHERE tenant_id=$1
          AND resolved_entity_ref @> $2::jsonb
          AND resolved_entity_ref <@ $2::jsonb
        """,
        tenant_id,
        json.dumps(customer_ref.model_dump(mode="json")),
    )
    return int(value or 0)


async def _state_event_count(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    customer_id: UUID,
    kind: str,
) -> int:
    value = await pool.fetchval(
        """
        SELECT count(*) FROM observations
        WHERE tenant_id=$1
          AND source_channel='internal:state_change'
          AND content ->> 'entity_id'=$2
          AND content ->> 'state_change_kind'=$3
        """,
        tenant_id,
        str(customer_id),
        kind,
    )
    return int(value or 0)


async def _alias_intervals(
    *,
    aliases: EntityAliasRepo,
    case: CustomerLifecycleCase,
    tenant_id: UUID,
) -> tuple[CustomerAliasIntervalEvidence, ...]:
    rows = [
        *await aliases.list_history(case.initial_identity, tenant_id),
        *await aliases.list_history(case.renamed_identity, tenant_id),
    ]
    return tuple(
        CustomerAliasIntervalEvidence(
            phrase=row["alias_text"],
            resolved_ref=CustomerRef.model_validate(row["resolved_entity_ref"]),
            valid_from=row["valid_from"],
            valid_until=row["valid_until"],
            validity_reason=row["validity_reason"],
        )
        for row in rows
    )


async def _assert_fresh_tenants(
    *,
    pool: asyncpg.Pool,
    assignments: tuple[CustomerLifecycleRuntimeAssignment, ...],
) -> None:
    tenant_ids = [
        tenant_id
        for row in assignments
        for tenant_id in (row.tenant_id, row.isolation_tenant_id)
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise RuntimeError("lifecycle runtime tenant assignments repeat")
    existing = await pool.fetch(
        "SELECT id FROM tenants WHERE id=ANY($1::uuid[])",
        tenant_ids,
    )
    if existing:
        raise RuntimeError("lifecycle runtime requires fresh tenants")


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
        evidence = await run_customer_lifecycle_experiment(
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
        default=DEFAULT_LIFECYCLE_POPULATION,
    )
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
