#!/usr/bin/env python3
"""Run the sealed canonical resource-replacement proof on PostgreSQL."""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.evaluation.canonical_referent_replacement import (
    CanonicalResourceReplacementEvidence,
    CanonicalResourceReplacementObservation,
    ReplacementProofCell,
    evaluate_canonical_resource_replacement,
    validate_canonical_resource_replacement_artifact,
)
from lib.evaluation.company_learning_experiment import CanonicalEntityRef
from lib.shared.errors import InvariantViolation
from lib.shared.migrations import apply_migrations_dir
from services.domain.canonical_referents.replacement import (
    CanonicalResourceReplacementOrchestrator,
)
from services.domain.canonical_referents.service import (
    CanonicalReferentRegistryService,
)
from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentVersionRef,
)
from services.domain.entity_aliases.repo import (
    EntityAliasRepo,
    insert_alias_with_connection,
)
from services.domain.source_identity_bindings.repo import (
    SourceIdentityBindingRepo,
)


ARTIFACT_NAME = "canonical_resource_replacement_evidence.json"
CASE_ID = "canonical-system-resource-replacement-v1"
REPLACEMENT_REASON = (
    "The governed billing platform replaced the legacy system resource."
)
_UUID_NAMESPACE = UUID("7c946c95-a7ca-5ed5-9195-b56b566739d8")


async def _register_codecs(conn: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await conn.set_type_codec(
            type_name,
            encoder=lambda value: (
                value if isinstance(value, str) else json.dumps(value)
            ),
            decoder=json.loads,
            schema="pg_catalog",
        )
    try:
        from pgvector.asyncpg import register_vector

        await register_vector(conn)
    except Exception:
        pass


@dataclass(frozen=True, slots=True)
class _ScenarioIds:
    tenant_id: UUID
    isolation_tenant_id: UUID
    atomicity_tenant_id: UUID
    predecessor_id: UUID
    successor_id: UUID
    alternative_successor_id: UUID
    missing_successor_id: UUID
    cause_event_id: UUID
    delayed_observation_id: UUID
    model_id: UUID
    atomicity_predecessor_id: UUID
    atomicity_successor_id: UUID
    atomicity_cause_event_id: UUID

    @property
    def tenant_ids(self) -> tuple[UUID, ...]:
        return (
            self.tenant_id,
            self.isolation_tenant_id,
            self.atomicity_tenant_id,
        )


def scenario_ids(*, run_id: str, system_version: str) -> _ScenarioIds:
    """Return deterministic identities for one requested proof run."""

    prefix = f"{run_id}\x1f{system_version}\x1f{CASE_ID}"

    def stable(label: str) -> UUID:
        return uuid5(_UUID_NAMESPACE, f"{prefix}\x1f{label}")

    return _ScenarioIds(
        tenant_id=stable("tenant"),
        isolation_tenant_id=stable("isolation-tenant"),
        atomicity_tenant_id=stable("atomicity-tenant"),
        predecessor_id=stable("predecessor"),
        successor_id=stable("successor"),
        alternative_successor_id=stable("alternative-successor"),
        missing_successor_id=stable("missing-successor"),
        cause_event_id=stable("cause-event"),
        delayed_observation_id=stable("delayed-observation"),
        model_id=stable("model"),
        atomicity_predecessor_id=stable("atomicity-predecessor"),
        atomicity_successor_id=stable("atomicity-successor"),
        atomicity_cause_event_id=stable("atomicity-cause-event"),
    )


async def run_canonical_resource_replacement_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
) -> CanonicalResourceReplacementEvidence:
    """Execute, reopen, evaluate, write, and revalidate one replacement proof."""

    run_id = run_id.strip()
    system_version = system_version.strip()
    if not run_id or not system_version:
        raise ValueError("run_id and system_version are required")
    async with pool.acquire() as conn:
        await _register_codecs(conn)
    ids = scenario_ids(run_id=run_id, system_version=system_version)
    await _require_utf8(pool)
    await _require_fresh_tenants(pool, ids.tenant_ids)

    database_now = await pool.fetchval("SELECT clock_timestamp()")
    created_at = database_now - timedelta(days=3)
    effective_at = database_now - timedelta(minutes=5)
    delayed_event_at = effective_at - timedelta(hours=1)
    atomicity_effective_at = database_now - timedelta(minutes=2)
    source_repo = SourceIdentityBindingRepo(pool)

    async with pool.acquire() as conn, conn.transaction():
        for tenant_id in ids.tenant_ids:
            await _seed_tenant(conn, tenant_id)
        await _seed_observation(
            conn,
            tenant_id=ids.tenant_id,
            observation_id=ids.cause_event_id,
            occurred_at=database_now - timedelta(minutes=1),
            source_channel="review:canonical-replacement",
            content_text="canonical replacement authority",
        )
        await _seed_observation(
            conn,
            tenant_id=ids.tenant_id,
            observation_id=ids.delayed_observation_id,
            occurred_at=delayed_event_at,
            source_channel="jira:project",
            content_text="Legacy Billing remains referenced by this old event.",
        )
        await _seed_observation(
            conn,
            tenant_id=ids.atomicity_tenant_id,
            observation_id=ids.atomicity_cause_event_id,
            occurred_at=database_now - timedelta(minutes=1),
            source_channel="review:canonical-replacement",
            content_text="canonical replacement rollback authority",
        )
        await _seed_resource(
            conn,
            tenant_id=ids.tenant_id,
            resource_id=ids.predecessor_id,
            identity="Legacy Billing",
            created_at=created_at,
        )
        await _seed_resource(
            conn,
            tenant_id=ids.tenant_id,
            resource_id=ids.successor_id,
            identity="Billing Platform",
            created_at=created_at + timedelta(hours=1),
        )
        await _seed_resource(
            conn,
            tenant_id=ids.tenant_id,
            resource_id=ids.alternative_successor_id,
            identity="Alternative Billing Platform",
            created_at=created_at + timedelta(hours=2),
        )
        await _seed_resource(
            conn,
            tenant_id=ids.atomicity_tenant_id,
            resource_id=ids.atomicity_predecessor_id,
            identity="Rollback Legacy Billing",
            created_at=created_at,
        )
        await _seed_resource(
            conn,
            tenant_id=ids.atomicity_tenant_id,
            resource_id=ids.atomicity_successor_id,
            identity="Rollback Billing Platform",
            created_at=created_at + timedelta(hours=1),
        )
        await insert_alias_with_connection(
            conn,
            phrase="legacy billing",
            resolved_entity_ref=_ref(ids.predecessor_id).model_dump(mode="json"),
            source="resource_lifecycle",
            confidence=1.0,
            tenant_id=ids.tenant_id,
            is_canonical=True,
            valid_from=created_at,
            source_event_id=ids.cause_event_id,
        )
        await insert_alias_with_connection(
            conn,
            phrase="billing platform",
            resolved_entity_ref=_ref(ids.successor_id).model_dump(mode="json"),
            source="resource_lifecycle",
            confidence=1.0,
            tenant_id=ids.tenant_id,
            is_canonical=True,
            valid_from=created_at + timedelta(hours=1),
            source_event_id=ids.cause_event_id,
        )
        await insert_alias_with_connection(
            conn,
            phrase="rollback legacy billing",
            resolved_entity_ref=_ref(
                ids.atomicity_predecessor_id
            ).model_dump(mode="json"),
            source="resource_lifecycle",
            confidence=1.0,
            tenant_id=ids.atomicity_tenant_id,
            is_canonical=True,
            valid_from=created_at,
            source_event_id=ids.atomicity_cause_event_id,
        )
        binding = await source_repo.bind(
            tenant_id=ids.tenant_id,
            source_system="jira",
            source_native_identifier="jira:project:10042",
            source_identity_authority_ref="jira:installation:replacement-proof",
            canonical_ref=_ref(ids.predecessor_id).model_dump(mode="json"),
            evidence_refs=("jira:project:10042",),
            valid_from=created_at,
            transaction_from=created_at,
            conn=conn,
        )
        await source_repo.attach_to_observation(
            tenant_id=ids.tenant_id,
            observation_id=ids.delayed_observation_id,
            binding=binding,
            source_surface="Legacy Billing",
            attachment_authority_ref="jira:installation:replacement-proof",
            conn=conn,
        )
        await _seed_model_and_projection(
            conn,
            tenant_id=ids.tenant_id,
            model_id=ids.model_id,
            observation_id=ids.cause_event_id,
            predecessor_id=ids.predecessor_id,
        )

    attachment_before = await _attachment_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
    )
    observation_before = await _observation_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
    )
    model_before = await _model_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        model_id=ids.model_id,
    )
    command = _command(
        tenant_id=ids.tenant_id,
        operation_ref=f"{run_id}:replace:legacy-billing:v1",
        predecessor_id=ids.predecessor_id,
        successor_id=ids.successor_id,
        effective_at=effective_at,
        cause_event_id=ids.cause_event_id,
    )
    orchestrator = CanonicalResourceReplacementOrchestrator(pool)
    applied = await orchestrator.apply(command)
    known_after = datetime.now(timezone.utc)

    aliases = EntityAliasRepo(pool)
    current_old_alias = await aliases.fast_path_resolve(
        "legacy billing",
        ids.tenant_id,
    )
    current_successor_alias = await aliases.fast_path_resolve(
        "billing platform",
        ids.tenant_id,
    )
    historical_alias = await aliases.fast_path_resolve(
        "legacy billing",
        ids.tenant_id,
        as_of=delayed_event_at,
    )
    current_binding = await source_repo.find_current_binding(
        tenant_id=ids.tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:10042",
    )
    historical_binding = await source_repo.find_visible_binding(
        tenant_id=ids.tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:10042",
        valid_at=delayed_event_at,
        known_at=known_after,
    )
    boundary_binding = await source_repo.find_visible_binding(
        tenant_id=ids.tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:10042",
        valid_at=effective_at,
        known_at=known_after,
    )
    delayed_attachment_resolution = await source_repo.resolve_observation_source(
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
        phrase="Legacy Billing",
        valid_at=delayed_event_at,
        known_at=known_after,
    )
    registry = CanonicalReferentRegistryService(pool)
    lineage_before = await registry.lineage_at(
        tenant_id=ids.tenant_id,
        referent=_ref(ids.predecessor_id),
        valid_at=effective_at - timedelta(microseconds=1),
        known_at=known_after,
    )
    lineage_at_boundary = await registry.lineage_at(
        tenant_id=ids.tenant_id,
        referent=_ref(ids.predecessor_id),
        valid_at=effective_at,
        known_at=known_after,
    )

    attachment_after = await _attachment_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
    )
    observation_after = await _observation_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        observation_id=ids.delayed_observation_id,
    )
    model_after = await _model_snapshot(
        pool,
        tenant_id=ids.tenant_id,
        model_id=ids.model_id,
    )
    database_state = await _replacement_database_state(
        pool,
        tenant_id=ids.tenant_id,
        predecessor_id=ids.predecessor_id,
        successor_id=ids.successor_id,
        subject_key=f"resource:{ids.predecessor_id}",
        operation_ref=command.operation_ref,
    )

    replay = await orchestrator.apply(command)
    replay_state = await _replacement_database_state(
        pool,
        tenant_id=ids.tenant_id,
        predecessor_id=ids.predecessor_id,
        successor_id=ids.successor_id,
        subject_key=f"resource:{ids.predecessor_id}",
        operation_ref=command.operation_ref,
    )
    conflict_rejected = await _replacement_rejected_with(
        orchestrator,
        _command(
            tenant_id=ids.tenant_id,
            operation_ref=command.operation_ref,
            predecessor_id=ids.predecessor_id,
            successor_id=ids.alternative_successor_id,
            effective_at=effective_at,
            cause_event_id=ids.cause_event_id,
        ),
        invariant="CANONICAL_REFERENT_OPERATION_CONFLICT",
    )
    stale_head_rejected = await _replacement_rejected_with(
        orchestrator,
        _command(
            tenant_id=ids.tenant_id,
            operation_ref=f"{run_id}:replace:stale-head:v1",
            predecessor_id=ids.predecessor_id,
            successor_id=ids.alternative_successor_id,
            effective_at=effective_at + timedelta(microseconds=1),
            cause_event_id=ids.cause_event_id,
        ),
        invariant="CANONICAL_REFERENT_STALE_HEAD",
    )
    foreign_rejected = await _replacement_rejected_with(
        orchestrator,
        _command(
            tenant_id=ids.isolation_tenant_id,
            operation_ref=f"{run_id}:replace:foreign-tenant:v1",
            predecessor_id=ids.predecessor_id,
            successor_id=ids.successor_id,
            effective_at=effective_at,
            cause_event_id=ids.cause_event_id,
        ),
        invariant="CANONICAL_REPLACEMENT_ENDPOINT_MISSING",
    )
    foreign_transition_count = await pool.fetchval(
        """
        SELECT count(*)
        FROM canonical_referent_transitions
        WHERE tenant_id=$1
        """,
        ids.isolation_tenant_id,
    )
    hard_dependency_rejected = await _prove_hard_dependency_rejection(
        pool=pool,
        orchestrator=orchestrator,
        ids=ids,
        effective_at=effective_at + timedelta(microseconds=2),
        run_id=run_id,
    )
    atomicity_proven = await _prove_transaction_atomicity(
        pool=pool,
        ids=ids,
        effective_at=atomicity_effective_at,
        run_id=run_id,
    )

    transition_ref = (
        f"postgres:canonical_referent_transitions:{ids.tenant_id}:"
        f"{applied.transition.transition_id}"
    )
    resource_ref = f"postgres:resources:{ids.tenant_id}"
    alias_ref = f"postgres:entity_aliases:{ids.tenant_id}"
    binding_ref = (
        f"postgres:source_identity_bindings:{ids.tenant_id}:"
        f"{binding.binding_lineage_id}"
    )
    attachment_ref = (
        f"postgres:observation_source_identity_bindings:{ids.tenant_id}:"
        f"{ids.delayed_observation_id}"
    )
    observation_ref = (
        f"postgres:observations:{ids.tenant_id}:{ids.delayed_observation_id}"
    )
    model_ref = f"postgres:models:{ids.tenant_id}:{ids.model_id}"
    projection_ref = (
        f"postgres:projection_snapshots:{ids.tenant_id}:"
        f"resource:{ids.predecessor_id}"
    )
    refresh_ref = (
        f"postgres:projection_refresh_jobs:{ids.tenant_id}:"
        f"resource:{ids.predecessor_id}"
    )
    atomicity_ref = (
        f"postgres:canonical_referent_transitions:{ids.atomicity_tenant_id}:"
        f"{run_id}:replace:rollback:v1"
    )
    hard_dependency_ref = (
        f"postgres:canonical_referent_transitions:{ids.tenant_id}:"
        f"{run_id}:replace:missing-successor:v1"
    )
    predecessor = CanonicalEntityRef(
        type="resource",
        id=str(ids.predecessor_id),
        version=1,
    )
    successor = CanonicalEntityRef(
        type="resource",
        id=str(ids.successor_id),
        version=1,
    )
    expected_ref = _ref_dict(ids.predecessor_id)
    successor_ref = _ref_dict(ids.successor_id)
    refresh_payload = _json_obj(database_state["refresh_payload"])
    expected_projection_ref = {
        "type": "resource",
        "id": str(ids.predecessor_id),
    }
    proof_cells = {
        "transition_applied": _observed(
            applied.transition.applied
            and database_state["transition_count"] == 1,
            transition_ref,
        ),
        "operation_replay_idempotent": _observed(
            not replay.transition.applied
            and not replay.state_changed
            and replay_state["transition_count"] == 1
            and replay_state["refresh_job_count"] == 1,
            transition_ref,
            refresh_ref,
        ),
        "operation_conflict_rejected": _observed(
            conflict_rejected
            and replay_state["transition_count"] == 1,
            transition_ref,
        ),
        "stale_head_rejected": _observed(
            stale_head_rejected
            and replay_state["transition_count"] == 1,
            transition_ref,
        ),
        "tenant_isolated": _observed(
            foreign_rejected and foreign_transition_count == 0,
            transition_ref,
            f"postgres:tenants:{ids.isolation_tenant_id}",
        ),
        "predecessor_retired": _observed(
            applied.predecessor_retired
            and database_state["predecessor_archived_at"] == effective_at,
            f"{resource_ref}:{ids.predecessor_id}",
        ),
        "successor_active": _observed(
            database_state["successor_archived_at"] is None,
            f"{resource_ref}:{ids.successor_id}",
        ),
        "alias_current_successor_safe": _observed(
            current_old_alias is None
            and current_successor_alias == successor_ref,
            alias_ref,
        ),
        "alias_asof_predecessor_safe": _observed(
            historical_alias == expected_ref,
            alias_ref,
        ),
        "exact_source_binding_boundary_safe": _observed(
            _binding_ref(current_binding) == successor_ref
            and _binding_ref(boundary_binding) == successor_ref
            and _binding_ref(historical_binding) == expected_ref,
            binding_ref,
        ),
        "delayed_event_asof_safe": _observed(
            _binding_ref(historical_binding) == expected_ref
            and delayed_attachment_resolution is None,
            binding_ref,
            attachment_ref,
        ),
        "old_attachment_immutable": _observed(
            attachment_before == attachment_after,
            attachment_ref,
        ),
        "source_observation_immutable": _observed(
            observation_before == observation_after,
            observation_ref,
        ),
        "model_scope_immutable": _observed(
            model_before == model_after
            and model_after["status"] == "active"
            and model_after["sidecar_entity_id"] == ids.predecessor_id,
            model_ref,
        ),
        "projection_invalidated": _observed(
            len(applied.projection_fence.invalidated_subjects) == 1
            and database_state["snapshot_count"] == 0
            and database_state["dependency_count"] == 0,
            projection_ref,
        ),
        "projection_single_refresh": _observed(
            database_state["refresh_job_count"] == 1
            and replay_state["refresh_job_count"] == 1
            and refresh_payload.get("correction_kind")
            == "canonical_referent_replaced"
            and refresh_payload.get("canonical_referent")
            == expected_projection_ref
            and refresh_payload.get("scoped_model_ids")
            == [str(ids.model_id)],
            refresh_ref,
        ),
        "lineage_reason_correct": _observed(
            database_state["transition_reason"] == REPLACEMENT_REASON
            and database_state["transition_authority_ref"]
            == "review:canonical-resource-replacement:1"
            and tuple(database_state["transition_evidence_refs"])
            == ("observation:replacement", "review:approved"),
            transition_ref,
        ),
        "lineage_time_boundary_safe": _observed(
            lineage_before.members == (_ref(ids.predecessor_id),)
            and lineage_at_boundary.members
            == (_ref(ids.predecessor_id), _ref(ids.successor_id))
            and lineage_at_boundary.head == _ref(ids.successor_id),
            transition_ref,
        ),
        "hard_dependency_rejected": _observed(
            hard_dependency_rejected,
            hard_dependency_ref,
            (
                f"postgres:resources:{ids.tenant_id}:"
                f"{ids.alternative_successor_id}"
            ),
            (
                f"postgres:resources:{ids.tenant_id}:"
                f"{ids.missing_successor_id}:absent"
            ),
        ),
        "transaction_atomic": _observed(
            atomicity_proven,
            atomicity_ref,
        ),
    }
    artifact_path = (output_dir / ARTIFACT_NAME).resolve()
    observation = CanonicalResourceReplacementObservation(
        predecessor=predecessor,
        successor=successor,
        effective_at=effective_at,
        transaction_at=applied.transition.transaction_at,
        delayed_event_occurred_at=delayed_event_at,
        replacement_reason=REPLACEMENT_REASON,
        **proof_cells,
        artifact_refs=(
            f"artifact:{artifact_path}",
            transition_ref,
            resource_ref,
            alias_ref,
            binding_ref,
            attachment_ref,
            observation_ref,
            model_ref,
            projection_ref,
            refresh_ref,
            hard_dependency_ref,
            atomicity_ref,
        ),
    )
    evidence = CanonicalResourceReplacementEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at=datetime.now(timezone.utc).isoformat(),
        observation=observation,
        report=evaluate_canonical_resource_replacement(observation),
        artifact_refs=(f"artifact:{artifact_path}",),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(evidence.artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    reopened = validate_canonical_resource_replacement_artifact(
        json.loads(artifact_path.read_text(encoding="utf-8"))
    )
    if reopened != evidence:
        raise RuntimeError("canonical replacement artifact failed reopen validation")
    return evidence


async def _require_utf8(pool: asyncpg.Pool) -> None:
    server_encoding = str(await pool.fetchval("SHOW server_encoding")).upper()
    if server_encoding != "UTF8":
        raise RuntimeError(
            "canonical replacement proof requires UTF8 PostgreSQL; "
            f"got {server_encoding}"
        )


async def _require_fresh_tenants(
    pool: asyncpg.Pool,
    tenant_ids: tuple[UUID, ...],
) -> None:
    rows = await pool.fetch(
        "SELECT id FROM tenants WHERE id=ANY($1::uuid[])",
        list(tenant_ids),
    )
    if rows:
        raise RuntimeError(
            "canonical replacement proof requires fresh deterministic tenants"
        )


def _ref(resource_id: UUID) -> CanonicalReferentVersionRef:
    return CanonicalReferentVersionRef(
        type="resource",
        id=str(resource_id),
        version=1,
    )


def _ref_dict(resource_id: UUID) -> dict[str, Any]:
    return _ref(resource_id).model_dump(mode="json")


def _command(
    *,
    tenant_id: UUID,
    operation_ref: str,
    predecessor_id: UUID,
    successor_id: UUID,
    effective_at: datetime,
    cause_event_id: UUID,
) -> CanonicalReferentReplacementCommand:
    return CanonicalReferentReplacementCommand(
        tenant_id=tenant_id,
        operation_ref=operation_ref,
        predecessor=_ref(predecessor_id),
        successor=_ref(successor_id),
        expected_predecessor_version=1,
        effective_at=effective_at,
        authority_ref="review:canonical-resource-replacement:1",
        reason=REPLACEMENT_REASON,
        evidence_refs=("observation:replacement", "review:approved"),
        cause_event_id=cause_event_id,
    )


async def _replacement_rejected_with(
    orchestrator: CanonicalResourceReplacementOrchestrator,
    command: CanonicalReferentReplacementCommand,
    *,
    invariant: str,
) -> bool:
    try:
        await orchestrator.apply(command)
    except InvariantViolation as exc:
        return exc.invariant == invariant
    return False


async def _prove_transaction_atomicity(
    *,
    pool: asyncpg.Pool,
    ids: _ScenarioIds,
    effective_at: datetime,
    run_id: str,
) -> bool:
    class _FailingProjectionAdapter:
        async def invalidate_for_canonical_referent(self, *_args, **_kwargs):
            raise RuntimeError("forced projection failure after lifecycle repairs")

    orchestrator = CanonicalResourceReplacementOrchestrator(
        pool,
        projection_adapter=_FailingProjectionAdapter(),  # type: ignore[arg-type]
    )
    operation_ref = f"{run_id}:replace:rollback:v1"
    failed = False
    try:
        await orchestrator.apply(
            _command(
                tenant_id=ids.atomicity_tenant_id,
                operation_ref=operation_ref,
                predecessor_id=ids.atomicity_predecessor_id,
                successor_id=ids.atomicity_successor_id,
                effective_at=effective_at,
                cause_event_id=ids.atomicity_cause_event_id,
            )
        )
    except RuntimeError as exc:
        failed = str(exc) == "forced projection failure after lifecycle repairs"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM canonical_referent_transitions
               WHERE tenant_id=$1 AND operation_ref=$2) AS transition_count,
              (SELECT archived_at FROM resources
               WHERE tenant_id=$1 AND id=$3) AS archived_at,
              (SELECT valid_until FROM entity_aliases
               WHERE tenant_id=$1
                 AND alias_text='rollback legacy billing') AS alias_valid_until,
              (SELECT count(*) FROM observations
               WHERE tenant_id=$1
                 AND kind='state_change'
                 AND cause_id=$4) AS state_change_count
            """,
            ids.atomicity_tenant_id,
            operation_ref,
            ids.atomicity_predecessor_id,
            ids.atomicity_cause_event_id,
        )
    return bool(
        failed
        and row is not None
        and row["transition_count"] == 0
        and row["archived_at"] is None
        and row["alias_valid_until"] is None
        and row["state_change_count"] == 0
    )


async def _prove_hard_dependency_rejection(
    *,
    pool: asyncpg.Pool,
    orchestrator: CanonicalResourceReplacementOrchestrator,
    ids: _ScenarioIds,
    effective_at: datetime,
    run_id: str,
) -> bool:
    operation_ref = f"{run_id}:replace:missing-successor:v1"
    rejected = await _replacement_rejected_with(
        orchestrator,
        _command(
            tenant_id=ids.tenant_id,
            operation_ref=operation_ref,
            predecessor_id=ids.alternative_successor_id,
            successor_id=ids.missing_successor_id,
            effective_at=effective_at,
            cause_event_id=ids.cause_event_id,
        ),
        invariant="CANONICAL_REPLACEMENT_ENDPOINT_MISSING",
    )
    row = await pool.fetchrow(
        """
        SELECT
          (SELECT count(*) FROM canonical_referent_transitions
           WHERE tenant_id=$1 AND operation_ref=$2) AS transition_count,
          (SELECT archived_at FROM resources
           WHERE tenant_id=$1 AND id=$3) AS predecessor_archived_at,
          (SELECT count(*) FROM resources
           WHERE tenant_id=$1 AND id=$4) AS successor_count
        """,
        ids.tenant_id,
        operation_ref,
        ids.alternative_successor_id,
        ids.missing_successor_id,
    )
    return bool(
        rejected
        and row is not None
        and row["transition_count"] == 0
        and row["predecessor_archived_at"] is None
        and row["successor_count"] == 0
    )


async def _seed_tenant(conn: asyncpg.Connection, tenant_id: UUID) -> None:
    await conn.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, $2, FALSE)
        """,
        tenant_id,
        f"Canonical replacement proof {str(tenant_id)[:8]}",
    )


async def _seed_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    occurred_at: datetime,
    source_channel: str,
    content_text: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, trust_tier
        ) VALUES (
          $1, $2, $3, 'signal', $4,
          jsonb_build_object('proof_case', $5::text),
          $5, 'authoritative'
        )
        """,
        observation_id,
        tenant_id,
        occurred_at,
        source_channel,
        content_text,
    )


async def _seed_resource(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    resource_id: UUID,
    identity: str,
    created_at: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, description, current_value,
          valuation_confidence, utilization_state, controllability,
          temporal_character, metadata, created_at, last_updated_at
        ) VALUES (
          $1, $2, 'infrastructure', $3, $4, '{}'::jsonb,
          1.0, 'available', 'owned', 'permanent',
          jsonb_build_object('semantic_kind', 'system'),
          $5, $5
        )
        """,
        resource_id,
        tenant_id,
        identity,
        f"{identity} system resource",
        created_at,
    )


async def _seed_model_and_projection(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    observation_id: UUID,
    predecessor_id: UUID,
) -> None:
    proposition = {
        "kind": "belief",
        "claim_role": "fact",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "observed",
        "polarity": "positive",
        "subject": f"resource:{predecessor_id}",
        "predicate": "is_operational",
        "object": True,
    }
    scope_entities = [{"type": "resource", "id": str(predecessor_id)}]
    await conn.execute(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_entities, scope_temporal, confidence,
          confidence_at_assertion
        ) VALUES (
          $1, $2, $3, $4::jsonb,
          'The legacy billing system is operational.',
          $5::vector, $6::jsonb, '{}'::jsonb, 0.6, 0.6
        )
        """,
        model_id,
        tenant_id,
        observation_id,
        proposition,
        [0.0] * 768,
        scope_entities,
    )
    await conn.execute(
        """
        INSERT INTO model_scope_entities (
          model_id, tenant_id, entity_type, entity_id, source, confidence
        ) VALUES ($1, $2, 'resource', $3, 'replacement_proof', 1.0)
        ON CONFLICT DO NOTHING
        """,
        model_id,
        tenant_id,
        predecessor_id,
    )
    subject_key = f"resource:{predecessor_id}"
    await conn.execute(
        """
        INSERT INTO projection_snapshots (
          tenant_id, projection_name, projection_version, subject_key,
          payload, confidence, source_model_ids, source_event_ids
        ) VALUES (
          $1, 'resources', 'v1', $2, '{}'::jsonb, 0.8, $3::uuid[], $4::uuid[]
        )
        """,
        tenant_id,
        subject_key,
        [model_id],
        [observation_id],
    )
    await conn.execute(
        """
        INSERT INTO projection_dependencies (
          tenant_id, projection_name, projection_version, subject_key,
          ref_kind, ref_value, reason
        ) VALUES (
          $1, 'resources', 'v1', $2, 'model', $3, 'replacement_proof'
        )
        """,
        tenant_id,
        subject_key,
        str(model_id),
    )


async def _attachment_snapshot(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_id: UUID,
) -> dict[str, Any]:
    row = await pool.fetchrow(
        """
        SELECT binding_id, binding_version, binding_lineage_id,
               source_surface, normalized_source_surface,
               attachment_authority_ref, observation_occurred_at
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        observation_id,
    )
    return dict(row) if row is not None else {}


async def _observation_snapshot(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_id: UUID,
) -> dict[str, Any]:
    row = await pool.fetchrow(
        """
        SELECT occurred_at, kind, source_channel, content, content_text,
               trust_tier, entities_mentioned
        FROM observations
        WHERE tenant_id=$1 AND id=$2
        """,
        tenant_id,
        observation_id,
    )
    return dict(row) if row is not None else {}


async def _model_snapshot(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    model_id: UUID,
) -> dict[str, Any]:
    row = await pool.fetchrow(
        """
        SELECT model.status, model."natural", model.proposition,
               model.scope_entities, scope.entity_type,
               scope.entity_id AS sidecar_entity_id
        FROM models model
        LEFT JOIN model_scope_entities scope
          ON scope.tenant_id=model.tenant_id
         AND scope.model_id=model.id
         AND scope.entity_type='resource'
        WHERE model.tenant_id=$1 AND model.id=$2
        """,
        tenant_id,
        model_id,
    )
    return dict(row) if row is not None else {}


async def _replacement_database_state(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    predecessor_id: UUID,
    successor_id: UUID,
    subject_key: str,
    operation_ref: str,
) -> dict[str, Any]:
    row = await pool.fetchrow(
        """
        SELECT
          (SELECT archived_at FROM resources
           WHERE tenant_id=$1 AND id=$2) AS predecessor_archived_at,
          (SELECT archived_at FROM resources
           WHERE tenant_id=$1 AND id=$3) AS successor_archived_at,
          (SELECT count(*) FROM projection_snapshots
           WHERE tenant_id=$1 AND subject_key=$4) AS snapshot_count,
          (SELECT count(*) FROM projection_dependencies
           WHERE tenant_id=$1 AND subject_key=$4) AS dependency_count,
          (SELECT count(*) FROM projection_refresh_jobs
           WHERE tenant_id=$1 AND subject_key=$4) AS refresh_job_count,
          (SELECT payload FROM projection_refresh_jobs
           WHERE tenant_id=$1 AND subject_key=$4
           ORDER BY created_at LIMIT 1) AS refresh_payload,
          (SELECT count(*) FROM canonical_referent_transitions
           WHERE tenant_id=$1 AND operation_ref=$5) AS transition_count,
          (SELECT reason FROM canonical_referent_transitions
           WHERE tenant_id=$1 AND operation_ref=$5) AS transition_reason,
          (SELECT authority_ref FROM canonical_referent_transitions
           WHERE tenant_id=$1 AND operation_ref=$5) AS transition_authority_ref,
          (SELECT evidence_refs FROM canonical_referent_transitions
           WHERE tenant_id=$1 AND operation_ref=$5) AS transition_evidence_refs
        """,
        tenant_id,
        predecessor_id,
        successor_id,
        subject_key,
        operation_ref,
    )
    return dict(row) if row is not None else {}


def _binding_ref(binding: Any) -> dict[str, Any] | None:
    if binding is None:
        return None
    return {
        "type": binding.canonical_referent_type,
        "id": binding.canonical_referent_id,
        "version": binding.canonical_referent_version,
    }


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _observed(
    satisfied: bool,
    *artifact_refs: str,
) -> ReplacementProofCell:
    return ReplacementProofCell(
        status="observed",
        satisfied=bool(satisfied),
        artifact_refs=tuple(artifact_refs),
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
        evidence = await run_canonical_resource_replacement_experiment(
            pool=pool,
            output_dir=args.output_dir,
            run_id=args.run_id,
            system_version=args.system_version,
        )
    finally:
        await pool.close()
    print(
        json.dumps(
            {
                "artifact_path": str(
                    (args.output_dir / ARTIFACT_NAME).resolve()
                ),
                "evidence_digest": evidence.digest,
                "report": evidence.report.model_dump(mode="json"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 2 if evidence.report.status == "contradicted" else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="Postgres DSN; defaults to DATABASE_URL")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--system-version", required=True)
    return asyncio.run(_run(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
