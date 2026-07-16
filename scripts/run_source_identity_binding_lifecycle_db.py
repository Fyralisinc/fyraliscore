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

from lib.contracts.kernel import canonical_sha256
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
QUERY_MANIFEST_NAME = "source_identity_binding_lifecycle_query_manifest.json"
CASE_ID = "source-identity-binding-lifecycle-v1"
_UUID_NAMESPACE = UUID("29d09a2d-7778-5d29-97dc-185963bf138d")
VALID_FROM = datetime(2026, 1, 1, tzinfo=timezone.utc)
EFFECTIVE_AT = datetime(2026, 3, 1, tzinfo=timezone.utc)
OLD_EVENT_AT = datetime(2026, 2, 1, tzinfo=timezone.utc)
NEW_EVENT_AT = datetime(2026, 4, 1, tzinfo=timezone.utc)
SURFACE = "ENG"
SOURCE_SYSTEM = "jira"
SOURCE_IDENTIFIER = "jira:system:eng"
_OBSERVATION_SNAPSHOT_SQL = """
SELECT *
FROM observations
WHERE tenant_id=$1 AND id=$2
ORDER BY occurred_at
"""
_ATTACHMENT_SNAPSHOT_SQL = """
SELECT *
FROM observation_source_identity_bindings
WHERE tenant_id=$1 AND observation_id=$2
ORDER BY observation_occurred_at, binding_version, normalized_source_surface
"""
_COLLIDING_CURRENT_BINDINGS_SQL = """
SELECT *
FROM source_identity_bindings
WHERE tenant_id=ANY($1::uuid[])
  AND source_system=$2
  AND source_native_identifier=$3
  AND valid_to IS NULL
  AND transaction_to IS NULL
ORDER BY tenant_id, binding_version
"""
_COLLIDING_BINDING_LINEAGES_SQL = """
SELECT *
FROM source_identity_bindings
WHERE tenant_id=ANY($1::uuid[])
  AND source_system=$2
  AND source_native_identifier=$3
ORDER BY tenant_id, binding_version
"""
_COLLIDING_ATTACHMENTS_SQL = """
SELECT *
FROM observation_source_identity_bindings
WHERE tenant_id=ANY($1::uuid[])
ORDER BY tenant_id, observation_id, binding_version, normalized_source_surface
"""
_DIRECT_OVERLAP_INSERT_SQL = """
INSERT INTO source_identity_bindings (
  id, tenant_id, lineage_id, binding_version, source_system,
  source_native_identifier, source_identity_authority_ref,
  canonical_referent, valid_from, transaction_from, evidence_refs,
  lifecycle_operation_kind, lifecycle_operation_ref
) VALUES (
  $1, $2, $1, 1, $3, $4, $5, $6::jsonb, $7, $8, $9, 'bind', $10
)
"""


class _RollbackProbe(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _ScenarioIds:
    tenant_id: UUID
    foreign_tenant_id: UUID
    old_resource_id: UUID
    new_resource_id: UUID
    foreign_old_resource_id: UUID
    foreign_new_resource_id: UUID
    original_observation_id: UUID
    delayed_observation_id: UUID
    foreign_observation_id: UUID
    direct_overlap_binding_id: UUID

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
        foreign_old_resource_id=stable("foreign-old-resource"),
        foreign_new_resource_id=stable("foreign-new-resource"),
        original_observation_id=stable("original-observation"),
        delayed_observation_id=stable("delayed-observation"),
        foreign_observation_id=stable("foreign-observation"),
        direct_overlap_binding_id=stable("direct-overlap-binding"),
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
    output_dir.mkdir(parents=True, exist_ok=True)
    query_entries: list[dict[str, Any]] = []
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
    foreign_original = await repo.bind(
        tenant_id=ids.foreign_tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
        source_identity_authority_ref="foreign-jira-system-contract-v1",
        canonical_ref={
            "type": "resource",
            "id": str(ids.foreign_old_resource_id),
            "version": 1,
        },
        evidence_refs=("foreign:jira:system:eng:v1",),
        valid_from=VALID_FROM,
        transaction_from=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    before_observation = await _capture_single_row(
        pool,
        query_entries=query_entries,
        name="primary_observation_before",
        sql=_OBSERVATION_SNAPSHOT_SQL,
        parameters=(ids.tenant_id, ids.original_observation_id),
    )
    await repo.attach_to_observation(
        tenant_id=ids.tenant_id,
        observation_id=ids.original_observation_id,
        binding=original,
        source_surface=SURFACE,
        attachment_authority_ref="jira-envelope-v1",
    )
    before_attachment = await _capture_single_row(
        pool,
        query_entries=query_entries,
        name="primary_attachment_before",
        sql=_ATTACHMENT_SNAPSHOT_SQL,
        parameters=(ids.tenant_id, ids.original_observation_id),
    )
    await repo.attach_to_observation(
        tenant_id=ids.foreign_tenant_id,
        observation_id=ids.foreign_observation_id,
        binding=foreign_original,
        source_surface=SURFACE,
        attachment_authority_ref="foreign-jira-envelope-v1",
    )
    foreign_transition = await repo.supersede(
        tenant_id=ids.foreign_tenant_id,
        binding_lineage_id=foreign_original.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=EFFECTIVE_AT,
        operation_ref="foreign:supersede:jira-system-eng",
        reason="Foreign tenant canonical system resource was replaced.",
        evidence_refs=("foreign:review:system-replacement",),
        new_canonical_ref={
            "type": "resource",
            "id": str(ids.foreign_new_resource_id),
            "version": 1,
        },
        new_source_identity_authority_ref="foreign-jira-system-contract-v2",
        new_evidence_refs=("foreign:jira:system:eng:v2",),
    )
    _, foreign_successor = foreign_transition.result_bindings
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
        pool=pool,
        query_entries=query_entries,
        tenant_id=ids.tenant_id,
        old_resource_id=ids.old_resource_id,
        new_resource_id=ids.new_resource_id,
        native_id="jira:system:scheduled",
        direct_overlap_binding_id=ids.direct_overlap_binding_id,
    )
    foreign_tenant_isolated = await _foreign_tenant_proof(
        repo=repo,
        ids=ids,
        original=original,
        successor=successor,
        foreign_original=foreign_original,
        foreign_successor=foreign_successor,
        foreign_transaction_at=foreign_transition.transaction_at,
    )
    transaction_atomic = await _transaction_atomicity_proof(
        repo=repo,
        pool=pool,
        ids=ids,
    )
    after_observation = await _capture_single_row(
        pool,
        query_entries=query_entries,
        name="primary_observation_after",
        sql=_OBSERVATION_SNAPSHOT_SQL,
        parameters=(ids.tenant_id, ids.original_observation_id),
    )
    after_attachment = await _capture_single_row(
        pool,
        query_entries=query_entries,
        name="primary_attachment_after",
        sql=_ATTACHMENT_SNAPSHOT_SQL,
        parameters=(ids.tenant_id, ids.original_observation_id),
    )
    await _capture_rows(
        pool,
        query_entries=query_entries,
        name="colliding_tenant_current_bindings",
        sql=_COLLIDING_CURRENT_BINDINGS_SQL,
        parameters=(list(ids.tenant_ids), SOURCE_SYSTEM, SOURCE_IDENTIFIER),
    )
    await _capture_rows(
        pool,
        query_entries=query_entries,
        name="colliding_tenant_binding_lineages",
        sql=_COLLIDING_BINDING_LINEAGES_SQL,
        parameters=(list(ids.tenant_ids), SOURCE_SYSTEM, SOURCE_IDENTIFIER),
    )
    await _capture_rows(
        pool,
        query_entries=query_entries,
        name="colliding_tenant_attachments",
        sql=_COLLIDING_ATTACHMENTS_SQL,
        parameters=(list(ids.tenant_ids),),
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
        str(after_attachment["binding_id"]) == original.binding_id
        and int(after_attachment["binding_version"]) == 1
        and after_attachment["source_surface"] == SURFACE
        and after_attachment["attachment_authority_ref"] == "jira-envelope-v1"
        and conflicting_attachment_rejected
    )
    source_immutable = bool(
        before_observation == after_observation
        and before_attachment == after_attachment
    )
    replay_idempotent = bool(
        not replay.applied
        and replay.result_bindings == transition.result_bindings
        and close_replay
        and revoke_replay
    )
    query_manifest = _write_query_manifest(
        output_dir=output_dir,
        run_id=run_id,
        system_version=system_version,
        query_entries=query_entries,
    )
    query_manifest_path = (output_dir / QUERY_MANIFEST_NAME).resolve()
    query_manifest_ref = (
        f"artifact:{query_manifest_path}"
        f"#sha256:{query_manifest['manifest_digest']}"
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
            query_manifest_ref,
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
            query_manifest_ref,
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
            query_manifest_ref,
        ),
        source_immutable=_cell(
            source_immutable,
            query_manifest_ref,
        ),
        transaction_atomic=_cell(
            transaction_atomic,
            "transaction:rollback-probe",
        ),
        artifact_refs=(
            f"binding-lineage:{original.binding_lineage_id}",
            f"observation:{ids.original_observation_id}",
            f"observation:{ids.delayed_observation_id}",
            query_manifest_ref,
        ),
    )
    report = evaluate_source_identity_binding_lifecycle(observation)
    evidence = SourceIdentityBindingLifecycleEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at=datetime.now(timezone.utc).isoformat(),
        observation=observation,
        report=report,
        artifact_refs=(
            f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",
            query_manifest_ref,
        ),
    )
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
    for tenant_id, resource_id, identity in (
        (ids.tenant_id, ids.old_resource_id, "Legacy billing system"),
        (ids.tenant_id, ids.new_resource_id, "Billing platform"),
        (
            ids.foreign_tenant_id,
            ids.foreign_old_resource_id,
            "Foreign legacy billing system",
        ),
        (
            ids.foreign_tenant_id,
            ids.foreign_new_resource_id,
            "Foreign billing platform",
        ),
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
            tenant_id,
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
    await _seed_observation(
        pool,
        tenant_id=ids.foreign_tenant_id,
        observation_id=ids.foreign_observation_id,
        occurred_at=OLD_EVENT_AT,
        external_id="jira-lifecycle-foreign",
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
    pool: asyncpg.Pool,
    query_entries: list[dict[str, Any]],
    tenant_id: UUID,
    old_resource_id: UUID,
    new_resource_id: UUID,
    native_id: str,
    direct_overlap_binding_id: UUID,
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
    repository_overlap_prevented = await _raises(
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
    direct_valid_from = datetime(2026, 8, 1, tzinfo=timezone.utc)
    direct_transaction_from = datetime.now(timezone.utc)
    direct_parameters = (
        direct_overlap_binding_id,
        tenant_id,
        SOURCE_SYSTEM,
        native_id,
        "direct-sql-overlap-contract",
        {
            "type": "resource",
            "id": str(new_resource_id),
            "version": 1,
        },
        direct_valid_from,
        direct_transaction_from,
        [f"{native_id}:direct-sql-overlap"],
        f"bind:{direct_overlap_binding_id}:1",
    )
    direct_overlap_prevented = False
    error: dict[str, Any] | None = None
    try:
        await pool.execute(_DIRECT_OVERLAP_INSERT_SQL, *direct_parameters)
    except asyncpg.ExclusionViolationError as exc:
        error = {
            "class": type(exc).__name__,
            "sqlstate": exc.sqlstate,
            "constraint_name": exc.constraint_name,
        }
        direct_overlap_prevented = bool(
            exc.constraint_name
            == "source_identity_bindings_no_valid_time_overlap"
        )
    query_entries.append(
        _rejected_write_entry(
            name="direct_sql_overlap_rejection",
            sql=_DIRECT_OVERLAP_INSERT_SQL,
            parameters=direct_parameters,
            error=error,
        )
    )
    return repository_overlap_prevented and direct_overlap_prevented


async def _foreign_tenant_proof(
    *,
    repo: SourceIdentityBindingRepo,
    ids: _ScenarioIds,
    original: Any,
    successor: Any,
    foreign_original: Any,
    foreign_successor: Any,
    foreign_transaction_at: datetime,
) -> bool:
    primary_current = await repo.find_current_binding(
        tenant_id=ids.tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
    )
    foreign_current = await repo.find_current_binding(
        tenant_id=ids.foreign_tenant_id,
        source_system=SOURCE_SYSTEM,
        source_native_identifier=SOURCE_IDENTIFIER,
    )
    foreign_resolution = await repo.resolve_observation_source(
        tenant_id=ids.foreign_tenant_id,
        observation_id=ids.foreign_observation_id,
        phrase=SURFACE,
        valid_at=OLD_EVENT_AT,
        known_at=foreign_transaction_at - timedelta(microseconds=1),
    )
    cross_tenant_observation_resolution = await repo.resolve_observation_source(
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
        primary_current == successor
        and foreign_current == foreign_successor
        and foreign_current.canonical_referent_id
        == str(ids.foreign_new_resource_id)
        and foreign_current.canonical_referent_id
        != successor.canonical_referent_id
        and foreign_resolution is not None
        and foreign_resolution.binding.binding_id
        == foreign_original.binding_id
        and foreign_resolution.canonical_ref["id"]
        == str(ids.foreign_old_resource_id)
        and cross_tenant_observation_resolution is None
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


async def _capture_rows(
    pool: asyncpg.Pool,
    *,
    query_entries: list[dict[str, Any]],
    name: str,
    sql: str,
    parameters: tuple[Any, ...],
) -> list[dict[str, Any]]:
    rows = [
        _json_value(dict(row))
        for row in await pool.fetch(sql, *parameters)
    ]
    normalized_parameters = _json_value(parameters)
    query_entries.append(
        {
            "name": name,
            "operation": "select",
            "sql": sql.strip(),
            "parameters": normalized_parameters,
            "query_digest": canonical_sha256(
                {
                    "sql": sql.strip(),
                    "parameters": normalized_parameters,
                }
            ),
            "row_count": len(rows),
            "rows": rows,
            "row_digest": canonical_sha256(rows),
        }
    )
    return rows


async def _capture_single_row(
    pool: asyncpg.Pool,
    *,
    query_entries: list[dict[str, Any]],
    name: str,
    sql: str,
    parameters: tuple[Any, ...],
) -> dict[str, Any]:
    rows = await _capture_rows(
        pool,
        query_entries=query_entries,
        name=name,
        sql=sql,
        parameters=parameters,
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"source identity lifecycle query {name!r} returned "
            f"{len(rows)} rows, expected 1"
        )
    return rows[0]


def _rejected_write_entry(
    *,
    name: str,
    sql: str,
    parameters: tuple[Any, ...],
    error: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_parameters = _json_value(parameters)
    normalized_error = _json_value(error)
    return {
        "name": name,
        "operation": "rejected_write",
        "sql": sql.strip(),
        "parameters": normalized_parameters,
        "query_digest": canonical_sha256(
            {
                "sql": sql.strip(),
                "parameters": normalized_parameters,
            }
        ),
        "outcome": "rejected" if error is not None else "accepted",
        "error": normalized_error,
        "error_digest": canonical_sha256(normalized_error),
    }


def _write_query_manifest(
    *,
    output_dir: Path,
    run_id: str,
    system_version: str,
    query_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema_version": "source-identity-binding-query-manifest-v1",
        "run_id": run_id,
        "system_version": system_version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "queries": query_entries,
    }
    manifest = {
        **payload,
        "manifest_digest": canonical_sha256(payload),
    }
    path = output_dir / QUERY_MANIFEST_NAME
    path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    reopened = validate_source_identity_binding_query_manifest(
        json.loads(path.read_text(encoding="utf-8"))
    )
    if reopened != manifest:
        raise RuntimeError(
            "source identity lifecycle query manifest failed reopen validation"
        )
    return manifest


def validate_source_identity_binding_query_manifest(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Recompute every query/result digest in the raw audit manifest."""

    supplied_digest = str(payload.get("manifest_digest") or "")
    body = {
        key: value
        for key, value in payload.items()
        if key != "manifest_digest"
    }
    if body.get("schema_version") != (
        "source-identity-binding-query-manifest-v1"
    ):
        raise ValueError("source identity query manifest schema mismatch")
    if supplied_digest != canonical_sha256(body):
        raise ValueError("source identity query manifest digest mismatch")
    queries = body.get("queries")
    if not isinstance(queries, list) or not queries:
        raise ValueError("source identity query manifest requires queries")
    names: set[str] = set()
    for entry in queries:
        if not isinstance(entry, dict):
            raise ValueError("source identity query manifest entry is invalid")
        name = str(entry.get("name") or "")
        if not name or name in names:
            raise ValueError("source identity query manifest names must be unique")
        names.add(name)
        expected_query_digest = canonical_sha256(
            {
                "sql": entry.get("sql"),
                "parameters": entry.get("parameters"),
            }
        )
        if entry.get("query_digest") != expected_query_digest:
            raise ValueError(
                f"source identity query digest mismatch for {name}"
            )
        if entry.get("operation") == "select":
            rows = entry.get("rows")
            if not isinstance(rows, list):
                raise ValueError(
                    f"source identity query rows missing for {name}"
                )
            if entry.get("row_count") != len(rows):
                raise ValueError(
                    f"source identity query row count mismatch for {name}"
                )
            if entry.get("row_digest") != canonical_sha256(rows):
                raise ValueError(
                    f"source identity query row digest mismatch for {name}"
                )
        elif entry.get("operation") == "rejected_write":
            if entry.get("error_digest") != canonical_sha256(
                entry.get("error")
            ):
                raise ValueError(
                    f"source identity query error digest mismatch for {name}"
                )
        else:
            raise ValueError(
                f"source identity query operation is invalid for {name}"
            )
    return payload


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


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
    "QUERY_MANIFEST_NAME",
    "run_source_identity_binding_lifecycle_experiment",
    "validate_source_identity_binding_query_manifest",
]
