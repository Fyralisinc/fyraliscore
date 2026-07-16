#!/usr/bin/env python3
"""Run sealed source-identity binding lifecycle proof on Postgres."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable
from uuid import UUID, uuid5

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg

from lib.evaluation.source_identity_binding_lifecycle import (
    BindingLifecycleProofCell,
    SourceIdentityBindingLifecycleEvidence,
    SourceIdentityBindingLifecycleObservation,
    evaluate_source_identity_binding_lifecycle,
    validate_source_identity_binding_lifecycle_artifact,
)
from lib.shared.migrations import apply_migrations_dir
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.source_identity_bindings import SourceIdentityBindingRepo


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_NAME = "source_identity_binding_lifecycle_evidence.json"
CASE_ID = "source-identity-binding-lifecycle-v1"
_UUID_NAMESPACE = UUID("29d09a2d-7778-5d29-97dc-185963bf138d")
VALID_FROM = datetime(2026, 1, 1, tzinfo=timezone.utc)
EFFECTIVE_AT = datetime(2026, 3, 1, tzinfo=timezone.utc)
OLD_EVENT_AT = datetime(2026, 2, 1, tzinfo=timezone.utc)
NEW_EVENT_AT = datetime(2026, 4, 1, tzinfo=timezone.utc)
SURFACE = "ENG"
SOURCE_SYSTEM = "jira"
SOURCE_IDENTIFIER = "jira:system:eng"


class _RollbackProbe(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _ScenarioIds:
    tenant_id: UUID
    foreign_tenant_id: UUID
    old_resource_id: UUID
    new_resource_id: UUID
    original_observation_id: UUID
    delayed_observation_id: UUID

    @property
    def tenant_ids(self) -> tuple[UUID, UUID]:
        return (self.tenant_id, self.foreign_tenant_id)


def _scenario_ids(*, run_id: str, system_version: str) -> _ScenarioIds:
    prefix = f"{run_id}\x1f{system_version}\x1f{CASE_ID}"

    def stable(label: str) -> UUID:
        return uuid5(_UUID_NAMESPACE, f"{prefix}\x1f{label}")

    return _ScenarioIds(
        tenant_id=stable("tenant"),
        foreign_tenant_id=stable("foreign-tenant"),
        old_resource_id=stable("old-resource"),
        new_resource_id=stable("new-resource"),
        original_observation_id=stable("original-observation"),
        delayed_observation_id=stable("delayed-observation"),
    )


async def run_source_identity_binding_lifecycle_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
) -> SourceIdentityBindingLifecycleEvidence:
    """Exercise every sealed binding lifecycle obligation on production repos."""

    run_id = run_id.strip()
    system_version = system_version.strip()
    if not run_id or not system_version:
        raise ValueError("run_id and system_version are required")
    ids = _scenario_ids(run_id=run_id, system_version=system_version)
    await _require_utf8(pool)
    await _require_fresh_tenants(pool, ids.tenant_ids)
    await _seed_foundation(pool, ids=ids)
    repo = SourceIdentityBindingRepo(pool)
    original = await repo.bind(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
        source_identity_authority_ref="jira-system-contract-v1",
        canonical_ref={
            "type": "resource",
            "id": str(ids.old_resource_id),
            "version": 1,
        },
        evidence_refs=("jira:system:eng:v1",),
        valid_from=VALID_FROM,
        transaction_from=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    before_observation = await _observation_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.original_observation_id,
    )
    await repo.attach_to_observation(
        tenant_id=ids.tenant_id,
        observation_id=ids.original_observation_id,
        binding=original,
        source_surface=SURFACE,
        attachment_authority_ref="jira-envelope-v1",
    )
    transition = await repo.supersede(
        tenant_id=ids.tenant_id,
        binding_lineage_id=original.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=EFFECTIVE_AT,
        operation_ref="supersede:jira-system-eng",
        reason="Canonical system resource was replaced.",
        evidence_refs=("review:system-replacement",),
        new_canonical_ref={
            "type": "resource",
            "id": str(ids.new_resource_id),
            "version": 1,
        },
        new_source_identity_authority_ref="jira-system-contract-v2",
        new_evidence_refs=("jira:system:eng:v2",),
    )
    replay = await repo.supersede(
        tenant_id=ids.tenant_id,
        binding_lineage_id=original.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=EFFECTIVE_AT,
        operation_ref="supersede:jira-system-eng",
        reason="Canonical system resource was replaced.",
        evidence_refs=("review:system-replacement",),
        new_canonical_ref={
            "type": "resource",
            "id": str(ids.new_resource_id),
            "version": 1,
        },
        new_source_identity_authority_ref="jira-system-contract-v2",
        new_evidence_refs=("jira:system:eng:v2",),
    )
    closure, successor = transition.result_bindings
    known_before = transition.transaction_at - timedelta(microseconds=1)
    known_after = transition.transaction_at + timedelta(microseconds=1)
    old_known_before = await repo.find_as_of_binding(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
        valid_at=OLD_EVENT_AT,
        known_at=known_before,
    )
    old_known_after = await repo.find_as_of_binding(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
        valid_at=OLD_EVENT_AT,
        known_at=known_after,
    )
    new_known_after = await repo.find_as_of_binding(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
        valid_at=NEW_EVENT_AT,
        known_at=known_after,
    )
    current = await repo.find_current_binding(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
    )
    stale_rejected = await _raises(
        repo.close(
            tenant_id=ids.tenant_id,
            binding_lineage_id=original.binding_lineage_id or "",
            expected_binding_version=1,
            effective_at=NEW_EVENT_AT,
            operation_ref="close:stale-jira-system-eng",
            reason="Stale caller must be rejected.",
            evidence_refs=("pytest:stale-version",),
        ),
        match="stale binding version",
    )
    attachment = await pool.fetchrow(
        """
        SELECT binding_id, binding_version, source_surface,
               attachment_authority_ref
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        ids.tenant_id,
        ids.original_observation_id,
    )
    conflicting_attachment_rejected = await _raises(
        repo.attach_to_observation(
            tenant_id=ids.tenant_id,
            observation_id=ids.original_observation_id,
            binding=closure,
            source_surface=SURFACE,
            attachment_authority_ref="jira-envelope-v1",
        ),
        match="different binding version",
    )
    await repo.attach_to_observation(
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
        binding=closure,
        source_surface=SURFACE,
        attachment_authority_ref="jira-envelope-v1",
    )
    delayed = await repo.resolve_observation_source(
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
        phrase=SURFACE,
        valid_at=OLD_EVENT_AT,
        known_at=datetime.now(timezone.utc),
    )
    close_correct, close_replay = await _terminal_operation_proof(
        repo=repo,
        pool=pool,
        tenant_id=ids.tenant_id,
        operation_kind="close",
        native_id="jira:system:close",
        resource_id=ids.old_resource_id,
    )
    revoke_correct, revoke_replay = await _terminal_operation_proof(
        repo=repo,
        pool=pool,
        tenant_id=ids.tenant_id,
        operation_kind="revoke",
        native_id="jira:system:revoke",
        resource_id=ids.old_resource_id,
    )
    overlap_prevented = await _overlap_proof(
        repo=repo,
        tenant_id=ids.tenant_id,
        old_resource_id=ids.old_resource_id,
        new_resource_id=ids.new_resource_id,
        native_id="jira:system:scheduled",
    )
    foreign_tenant_isolated = await _foreign_tenant_proof(
        repo=repo,
        ids=ids,
        original=original,
    )
    transaction_atomic = await _transaction_atomicity_proof(
        repo=repo,
        pool=pool,
        ids=ids,
    )
    after_observation = await _observation_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.original_observation_id,
    )
    supersession_correct = bool(
        transition.applied
        and closure.binding_version == 2
        and successor.binding_version == 3
        and closure.binding_lineage_id == original.binding_lineage_id
        and successor.binding_lineage_id == original.binding_lineage_id
        and closure.canonical_referent_id == str(ids.old_resource_id)
        and successor.canonical_referent_id == str(ids.new_resource_id)
    )
    current_resolution_correct = bool(
        current == successor
        and current.canonical_referent_id == str(ids.new_resource_id)
    )
    asof_resolution_correct = bool(
        old_known_before is not None
        and old_known_before.binding_id == original.binding_id
        and old_known_before.binding_version == 1
        and old_known_after == closure
        and new_known_after == successor
        and delayed is not None
        and delayed.binding.binding_id == closure.binding_id
        and delayed.binding.binding_version == 2
        and delayed.canonical_ref["id"] == str(ids.old_resource_id)
    )
    exact_attachment_preserved = bool(
        attachment is not None
        and str(attachment["binding_id"]) == original.binding_id
        and int(attachment["binding_version"]) == 1
        and attachment["source_surface"] == SURFACE
        and attachment["attachment_authority_ref"] == "jira-envelope-v1"
        and conflicting_attachment_rejected
    )
    source_immutable = before_observation == after_observation
    replay_idempotent = bool(
        not replay.applied
        and replay.result_bindings == transition.result_bindings
        and close_replay
        and revoke_replay
    )
    observation = SourceIdentityBindingLifecycleObservation(
        tenant_id=ids.tenant_id,
        binding_lineage_id=UUID(original.binding_lineage_id or ""),
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
        source_surface=SURFACE,
        original_binding_version=1,
        closure_binding_version=2,
        successor_binding_version=3,
        original_valid_from=VALID_FROM,
        transition_effective_at=EFFECTIVE_AT,
        transaction_at=transition.transaction_at,
        as_of_valid_at=OLD_EVENT_AT,
        as_of_known_at=known_after,
        source_observation_ref=f"observation:{ids.original_observation_id}",
        current_resolution_correct=_cell(
            current_resolution_correct,
            f"binding:{successor.binding_id}:3",
        ),
        asof_resolution_correct=_cell(
            asof_resolution_correct,
            f"binding:{closure.binding_id}:2",
        ),
        exact_attachment_preserved=_cell(
            exact_attachment_preserved,
            f"attachment:{ids.original_observation_id}:{original.binding_id}:1",
        ),
        closure_correct=_cell(
            close_correct,
            "operation:close:jira:system:close",
        ),
        revocation_correct=_cell(
            revoke_correct,
            "operation:revoke:jira:system:revoke",
        ),
        supersession_correct=_cell(
            supersession_correct,
            "operation:supersede:jira-system-eng",
        ),
        overlap_prevented=_cell(
            overlap_prevented,
            "overlap:jira:system:scheduled",
        ),
        stale_version_rejected=_cell(
            stale_rejected,
            "rejection:stale-binding-version",
        ),
        replay_idempotent=_cell(
            replay_idempotent,
            "replay:all-lifecycle-operations",
        ),
        foreign_tenant_isolated=_cell(
            foreign_tenant_isolated,
            f"tenant:{ids.foreign_tenant_id}",
        ),
        source_immutable=_cell(
            source_immutable,
            f"observation:{ids.original_observation_id}:snapshot",
        ),
        transaction_atomic=_cell(
            transaction_atomic,
            "transaction:rollback-probe",
        ),
        artifact_refs=(
            f"binding-lineage:{original.binding_lineage_id}",
            f"observation:{ids.original_observation_id}",
            f"observation:{ids.delayed_observation_id}",
        ),
    )
    report = evaluate_source_identity_binding_lifecycle(observation)
    evidence = SourceIdentityBindingLifecycleEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at=datetime.now(timezone.utc).isoformat(),
        observation=observation,
        report=report,
        artifact_refs=(f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(evidence.artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    reopened = validate_source_identity_binding_lifecycle_artifact(
        json.loads((output_dir / ARTIFACT_NAME).read_text(encoding="utf-8"))
    )
    if reopened != evidence:
        raise RuntimeError(
            "source identity lifecycle artifact failed reopen validation"
        )
    return evidence


async def _require_utf8(pool: asyncpg.Pool) -> None:
    server_encoding = str(await pool.fetchval("SHOW server_encoding")).upper()
    if server_encoding != "UTF8":
        raise RuntimeError(
            "source identity lifecycle proof requires UTF8 PostgreSQL; "
            f"got {server_encoding}"
        )


async def _require_fresh_tenants(
    pool: asyncpg.Pool,
    tenant_ids: tuple[UUID, UUID],
) -> None:
    rows = await pool.fetch(
        "SELECT id FROM tenants WHERE id=ANY($1::uuid[])",
        list(tenant_ids),
    )
    if rows:
        raise RuntimeError(
            "source identity lifecycle proof requires fresh deterministic " "tenants"
        )


async def _seed_foundation(
    pool: asyncpg.Pool,
    *,
    ids: _ScenarioIds,
) -> None:
    await pool.executemany(
        "INSERT INTO tenants (id) VALUES ($1)",
        ((ids.tenant_id,), (ids.foreign_tenant_id,)),
    )
    for resource_id, identity in (
        (ids.old_resource_id, "Legacy billing system"),
        (ids.new_resource_id, "Billing platform"),
    ):
        await pool.execute(
            """
            INSERT INTO resources (
              id, tenant_id, kind, identity, current_value, metadata
            ) VALUES (
              $1, $2, 'infrastructure', $3,
              jsonb_build_object('name', $3::text),
              '{"semantic_kind":"system"}'::jsonb
            )
            """,
            resource_id,
            ids.tenant_id,
            identity,
        )
    await _seed_observation(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.original_observation_id,
        occurred_at=OLD_EVENT_AT,
        external_id="jira-lifecycle-original",
    )
    await _seed_observation(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
        occurred_at=OLD_EVENT_AT,
        external_id="jira-lifecycle-delayed",
    )


async def _seed_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    occurred_at: datetime,
    external_id: str,
) -> None:
    await pool.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, trust_tier, external_id,
          entities_mentioned
        ) VALUES (
          $1, $2, $3, 'signal', 'jira:webhook',
          '{"fixture":"source-binding-lifecycle"}'::jsonb,
          'ENG lifecycle event', 'authoritative', $4, '[]'::jsonb
        )
        """,
        observation_id,
        tenant_id,
        occurred_at,
        external_id,
    )


async def _terminal_operation_proof(
    *,
    repo: SourceIdentityBindingRepo,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    operation_kind: str,
    native_id: str,
    resource_id: UUID,
) -> tuple[bool, bool]:
    binding = await repo.bind(
        tenant_id=tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=native_id,
        source_identity_authority_ref=f"{operation_kind}-contract-v1",
        canonical_ref={
            "type": "resource",
            "id": str(resource_id),
            "version": 1,
        },
        evidence_refs=(f"{native_id}:v1",),
        valid_from=VALID_FROM,
    )
    operation = getattr(repo, operation_kind)
    kwargs = {
        "tenant_id": tenant_id,
        "binding_lineage_id": binding.binding_lineage_id or "",
        "expected_binding_version": 1,
        "effective_at": EFFECTIVE_AT,
        "operation_ref": f"{operation_kind}:{native_id}",
        "reason": f"{operation_kind} source identity",
        "evidence_refs": (f"review:{operation_kind}",),
    }
    first = await operation(**kwargs)
    replay = await operation(**kwargs)
    current = await repo.find_current_binding(
        tenant_id=tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=native_id,
    )
    before = await repo.find_as_of_binding(
        tenant_id=tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=native_id,
        valid_at=OLD_EVENT_AT,
        known_at=first.transaction_at + timedelta(microseconds=1),
    )
    after = await repo.find_as_of_binding(
        tenant_id=tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=native_id,
        valid_at=NEW_EVENT_AT,
        known_at=first.transaction_at + timedelta(microseconds=1),
    )
    operation_count = await pool.fetchval(
        """
        SELECT count(*) FROM source_identity_binding_operations
        WHERE tenant_id=$1 AND operation_ref=$2
        """,
        tenant_id,
        kwargs["operation_ref"],
    )
    correct = bool(
        first.applied
        and len(first.result_bindings) == 1
        and first.result_bindings[0].binding_version == 2
        and current is None
        and before == first.result_bindings[0]
        and after is None
        and operation_count == 1
    )
    replayed = bool(
        not replay.applied and replay.result_bindings == first.result_bindings
    )
    return correct, replayed


async def _overlap_proof(
    *,
    repo: SourceIdentityBindingRepo,
    tenant_id: UUID,
    old_resource_id: UUID,
    new_resource_id: UUID,
    native_id: str,
) -> bool:
    binding = await repo.bind(
        tenant_id=tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=native_id,
        source_identity_authority_ref="scheduled-contract-v1",
        canonical_ref={
            "type": "resource",
            "id": str(old_resource_id),
            "version": 1,
        },
        evidence_refs=(f"{native_id}:v1",),
        valid_from=VALID_FROM,
    )
    boundary = datetime(2026, 12, 1, tzinfo=timezone.utc)
    await repo.close(
        tenant_id=tenant_id,
        binding_lineage_id=binding.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=boundary,
        operation_ref="close:jira:system:scheduled",
        reason="Scheduled source identity closure.",
        evidence_refs=("review:scheduled-close",),
    )
    return await _raises(
        repo.bind(
            tenant_id=tenant_id,
            source_system=SOURCE_SYSTEM,
            source_native_identifier=native_id,
            source_identity_authority_ref="scheduled-contract-v2",
            canonical_ref={
                "type": "resource",
                "id": str(new_resource_id),
                "version": 1,
            },
            evidence_refs=(f"{native_id}:v2",),
            valid_from=datetime(2026, 8, 1, tzinfo=timezone.utc),
        ),
        match="valid-time interval overlaps",
    )


async def _foreign_tenant_proof(
    *,
    repo: SourceIdentityBindingRepo,
    ids: _ScenarioIds,
    original: Any,
) -> bool:
    foreign_current = await repo.find_current_binding(
        tenant_id=ids.foreign_tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
    )
    foreign_resolution = await repo.resolve_observation_source(
        tenant_id=ids.foreign_tenant_id,
        observation_id=ids.original_observation_id,
        phrase=SURFACE,
        valid_at=OLD_EVENT_AT,
        known_at=datetime.now(timezone.utc),
    )
    foreign_operation_rejected = await _raises(
        repo.close(
            tenant_id=ids.foreign_tenant_id,
            binding_lineage_id=original.binding_lineage_id or "",
            expected_binding_version=1,
            effective_at=NEW_EVENT_AT,
            operation_ref="foreign:close:jira-system-eng",
            reason="Foreign tenant must not mutate binding.",
            evidence_refs=("pytest:foreign-tenant",),
        ),
        match="no current binding",
    )
    return bool(
        foreign_current is None
        and foreign_resolution is None
        and foreign_operation_rejected
    )


async def _transaction_atomicity_proof(
    *,
    repo: SourceIdentityBindingRepo,
    pool: asyncpg.Pool,
    ids: _ScenarioIds,
) -> bool:
    native_id = "jira:system:atomic"
    binding = await repo.bind(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=native_id,
        source_identity_authority_ref="atomic-contract-v1",
        canonical_ref={
            "type": "resource",
            "id": str(ids.old_resource_id),
            "version": 1,
        },
        evidence_refs=(f"{native_id}:v1",),
        valid_from=VALID_FROM,
    )
    before_count = await pool.fetchval(
        """
        SELECT count(*) FROM source_identity_bindings
        WHERE tenant_id=$1 AND lineage_id=$2
        """,
        ids.tenant_id,
        UUID(binding.binding_lineage_id or ""),
    )
    try:
        async with pool.acquire() as conn, conn.transaction():
            await repo.supersede(
                tenant_id=ids.tenant_id,
                binding_lineage_id=binding.binding_lineage_id or "",
                expected_binding_version=1,
                effective_at=EFFECTIVE_AT,
                operation_ref="atomic:supersede:jira-system",
                reason="Rollback probe.",
                evidence_refs=("pytest:rollback-probe",),
                new_canonical_ref={
                    "type": "resource",
                    "id": str(ids.new_resource_id),
                    "version": 1,
                },
                new_source_identity_authority_ref="atomic-contract-v2",
                new_evidence_refs=(f"{native_id}:v2",),
                conn=conn,
            )
            raise _RollbackProbe
    except _RollbackProbe:
        pass
    after_count = await pool.fetchval(
        """
        SELECT count(*) FROM source_identity_bindings
        WHERE tenant_id=$1 AND lineage_id=$2
        """,
        ids.tenant_id,
        UUID(binding.binding_lineage_id or ""),
    )
    operation_count = await pool.fetchval(
        """
        SELECT count(*) FROM source_identity_binding_operations
        WHERE tenant_id=$1 AND operation_ref='atomic:supersede:jira-system'
        """,
        ids.tenant_id,
    )
    current = await repo.find_current_binding(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=native_id,
    )
    return bool(
        before_count == after_count == 1 and operation_count == 0 and current == binding
    )


async def _observation_snapshot(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_id: UUID,
) -> Any:
    return await pool.fetchrow(
        """
        SELECT occurred_at, source_channel, content, content_text,
               entities_mentioned, external_id
        FROM observations
        WHERE tenant_id=$1 AND id=$2
        """,
        tenant_id,
        observation_id,
    )


async def _raises(awaitable: Awaitable[Any], *, match: str) -> bool:
    try:
        await awaitable
    except (ValueError, RuntimeError) as exc:
        return match in str(exc)
    return False


def _cell(satisfied: bool, artifact_ref: str) -> BindingLifecycleProofCell:
    return BindingLifecycleProofCell(
        status="observed",
        satisfied=satisfied,
        artifact_refs=(artifact_ref,),
    )


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
        evidence = await run_source_identity_binding_lifecycle_experiment(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
        )
    finally:
        await pool.close()
    print(json.dumps(evidence.report.model_dump(mode="json"), indent=2, sort_keys=True))
    return 0 if evidence.report.full_scope_complete else 2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="Postgres DSN; defaults to DATABASE_URL")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARTIFACT_NAME",
    "run_source_identity_binding_lifecycle_experiment",
]
