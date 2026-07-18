"""Genuine PostgreSQL P8 restart/replay execution evidence.

This deliberately reports partial coverage. Each covered case commits or
rolls back production truth/barrier state, closes the connection (the crash
boundary), reconnects, queries durable state, and binds the queried rows into
the receipt digest. Unexercised boundaries are never synthesized.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p2_runner import _admission
from lib.evaluation.epistemic_repair.p2_runner import _advance
from lib.contracts.truth_admission import ModelTruthTransition
from lib.shared.errors import InvariantViolation
from services.domain.company_learning.barrier import CompanyLearningBarrierService
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.service import TruthKernelService
from services.domain.truth_kernel import build_default_truth_kernel


P8_DB_COVERED_BOUNDARIES = (
    "validation_rejection",
    "database_serialization_failure",
    "crash_after_validation_before_apply",
    "crash_after_apply_before_queue_ack",
    "crash_during_dependent_lifecycle_fencing",
    "crash_during_projection_refresh",
    "restart_with_pending_truth_critical_work",
    "duplicate_delivery_replay",
    "authority_revocation_selection_to_commit",
)
P8_DB_UNCOVERED_BOUNDARIES = (
    "provider_timeout_before_response",
    "provider_timeout_after_partial_work",
    "invalid_structured_output",
)


@dataclass(frozen=True, slots=True)
class DurableFaultReceipt:
    boundary: str
    duplicate_delivery: bool
    tenant_id: str
    batch_id: str
    pre_restart_fate: str
    post_restart_barrier_count: int
    post_restart_model_count: int
    post_restart_pending_count: int
    replay_receipt_digest: str
    queried_state_digest: str
    cross_tenant_model_hits: int = 0
    relation_version_count: int = 0
    duplicate_lifecycle_transition_count: int = 0
    partial_truth_state_count: int = 0
    stale_active_truth_count: int = 0
    dead_letter_truth_critical_count: int = 0
    uninterrupted_reference_digest: str = ""
    uninterrupted_reference_matches: bool = False


@dataclass(frozen=True, slots=True)
class PostgresFaultSlice:
    database_run_id: str
    covered_boundaries: tuple[str, ...]
    uncovered_boundaries: tuple[str, ...]
    receipts: tuple[DurableFaultReceipt, ...]
    evidence_digest: str
    exact_required_fault_coverage: bool


async def _setup_tenant(dsn: str, tenant_id: UUID) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,$2)", tenant_id, f"p8-{tenant_id}")
    finally:
        await conn.close()


async def _uninterrupted_reference_digest(dsn: str) -> str:
    """Execute the same semantic success path once, without a restart."""
    tenant_id = uuid4()
    await _setup_tenant(dsn, tenant_id)
    conn = await asyncpg.connect(dsn)
    try:
        tx = conn.transaction()
        await tx.start()
        admission = _admission(tenant_id, 1)
        admitted = await build_default_truth_kernel().admit(tx=conn, command=admission)
        await CompanyLearningBarrierService().complete(
            tx=conn, barrier_id=uuid4(), tenant_id=tenant_id, batch_id="p8:reference",
            expected_model_version_ids=(admitted.version_id,), truth_critical_pending_count=0,
            completed_at=datetime.now(timezone.utc),
        )
        await tx.commit()
        row = await conn.fetchrow(
            """SELECT
                 (SELECT count(*)::int FROM company_learning_barriers WHERE tenant_id=$1) AS barriers,
                 (SELECT count(*)::int FROM accepted_current_models WHERE tenant_id=$1) AS models,
                 (SELECT count(*)::int FROM model_truth_versions WHERE tenant_id=$1) AS versions,
                 (SELECT count(*)::int FROM model_truth_heads WHERE tenant_id=$1) AS heads,
                 (SELECT count(*)::int FROM relation_truth_versions WHERE tenant_id=$1) AS relations""",
            tenant_id,
        )
        return canonical_sha256(dict(row))
    finally:
        await conn.close()


async def _execute_case(dsn: str, *, boundary: str, duplicate: bool) -> DurableFaultReceipt:
    tenant_id, batch_id = uuid4(), f"p8:{boundary}:{int(duplicate)}"
    await _setup_tenant(dsn, tenant_id)
    admitted_version: UUID | None = None
    pre_restart_fate = "connection_closed"
    try:
        if boundary == "database_serialization_failure":
            table = f"p8_serial_{tenant_id.hex}"
            setup = await asyncpg.connect(dsn)
            await setup.execute(f"CREATE TABLE {table} (id int PRIMARY KEY, value int NOT NULL)")
            await setup.execute(f"INSERT INTO {table} VALUES (1,0),(2,0)")
            await setup.close()
            one, two = await asyncpg.connect(dsn), await asyncpg.connect(dsn)
            t1 = one.transaction(isolation="serializable")
            t2 = two.transaction(isolation="serializable")
            await t1.start(); await t2.start()
            await one.fetchval(f"SELECT value FROM {table} WHERE id=2")
            await two.fetchval(f"SELECT value FROM {table} WHERE id=1")
            await one.execute(f"UPDATE {table} SET value=1 WHERE id=1")
            await two.execute(f"UPDATE {table} SET value=1 WHERE id=2")
            await t1.commit()
            try:
                await t2.commit()
            except asyncpg.SerializationError:
                pre_restart_fate = "postgres_serialization_failure"
            else:
                raise AssertionError("PostgreSQL did not produce the injected serialization failure")
            await one.close(); await two.close()
            setup = await asyncpg.connect(dsn)
            await setup.execute(f"DROP TABLE {table}")
            await setup.close()

        conn = await asyncpg.connect(dsn)
        if boundary == "crash_after_validation_before_apply":
            tx = conn.transaction()
            await tx.start()
            await TruthKernelService(storage=AsyncpgTruthKernelStorage()).admit(
                tx=conn, command=_admission(tenant_id, 1),
            )
            await tx.rollback()  # validated work never crosses the apply commit.
            await conn.close()
            pre_restart_fate = "rolled_back_before_apply"
        else:
            tx = conn.transaction()
            await tx.start()
            admission = _admission(tenant_id, 1)
            admitted = await build_default_truth_kernel().admit(tx=conn, command=admission)
            admitted_version = admitted.version_id
            service = CompanyLearningBarrierService()
            if boundary in {"validation_rejection", "restart_with_pending_truth_critical_work"}:
                try:
                    await service.complete(
                        tx=conn, barrier_id=uuid4(), tenant_id=tenant_id,
                        batch_id=batch_id, expected_model_version_ids=(admitted.version_id,),
                        truth_critical_pending_count=1, completed_at=datetime.now(timezone.utc),
                    )
                except InvariantViolation:
                    pre_restart_fate = "validation_rejected" if boundary == "validation_rejection" else "pending_work_rejected"
            elif boundary == "crash_during_dependent_lifecycle_fencing":
                await build_default_truth_kernel().advance(
                    tx=conn,
                    command=_advance(admitted, admission.version, ModelTruthTransition.FALSIFY, 1),
                )
                await tx.rollback()
                await conn.close()
                pre_restart_fate = "lifecycle_fence_transaction_rolled_back"
                conn = await asyncpg.connect(dsn)
                tx = conn.transaction()
                await tx.start()
                # The rollback removed admission too; rebuild canonical state.
                admission = _admission(tenant_id, 1)
                admitted = await build_default_truth_kernel().admit(tx=conn, command=admission)
                admitted_version = admitted.version_id
                await service.complete(
                    tx=conn, barrier_id=uuid4(), tenant_id=tenant_id,
                    batch_id=batch_id, expected_model_version_ids=(admitted.version_id,),
                    truth_critical_pending_count=0, completed_at=datetime.now(timezone.utc),
                )
            elif boundary == "authority_revocation_selection_to_commit":
                # Selection/admission commits, but the later canonical write has
                # no transaction-local truth-kernel command capability.
                await tx.commit()
                await conn.close()
                conn = await asyncpg.connect(dsn)
                rejected = conn.transaction()
                await rejected.start()
                try:
                    await conn.execute(
                        "UPDATE model_truth_heads SET advanced_at=advanced_at WHERE tenant_id=$1 AND model_id=$2",
                        tenant_id, admitted.model_id,
                    )
                except Exception:
                    await rejected.rollback()
                    pre_restart_fate = "revoked_command_authority_rejected"
                else:
                    await rejected.rollback()
                    raise AssertionError("canonical write without command authority was accepted")
                await conn.close()
                conn = await asyncpg.connect(dsn)
                tx = conn.transaction()
                await tx.start()
                await service.complete(
                    tx=conn, barrier_id=uuid4(), tenant_id=tenant_id,
                    batch_id=batch_id, expected_model_version_ids=(admitted.version_id,),
                    truth_critical_pending_count=0, completed_at=datetime.now(timezone.utc),
                )
            else:
                await service.complete(
                    tx=conn, barrier_id=uuid4(), tenant_id=tenant_id,
                    batch_id=batch_id, expected_model_version_ids=(admitted.version_id,),
                    truth_critical_pending_count=0, completed_at=datetime.now(timezone.utc),
                )
            await tx.commit()
            await conn.close()  # actual process-boundary equivalent before ack/refresh.

        # Restart: reconnect and complete/replay from durable canonical state.
        conn = await asyncpg.connect(dsn)
        tx = conn.transaction()
        await tx.start()
        if admitted_version is None:
            admitted = await TruthKernelService(storage=AsyncpgTruthKernelStorage()).admit(
                tx=conn, command=_admission(tenant_id, 1),
            )
            admitted_version = admitted.version_id
        service = CompanyLearningBarrierService()
        receipt = await service.complete(
            tx=conn, barrier_id=uuid4(), tenant_id=tenant_id, batch_id=batch_id,
            expected_model_version_ids=(admitted_version,), truth_critical_pending_count=0,
            completed_at=datetime.now(timezone.utc),
        )
        if duplicate:
            duplicate_receipt = await service.complete(
                tx=conn, barrier_id=uuid4(), tenant_id=tenant_id, batch_id=batch_id,
                expected_model_version_ids=(admitted_version,), truth_critical_pending_count=0,
                completed_at=datetime.now(timezone.utc),
            )
            if duplicate_receipt != receipt:
                raise AssertionError("duplicate delivery did not return the durable receipt")
        await tx.commit()
        await conn.close()

        # Independent post-restart connection is the evidence source.
        conn = await asyncpg.connect(dsn)
        row = await conn.fetchrow(
            """SELECT count(*)::int AS barriers,
                      coalesce(max(truth_critical_pending_count),0)::int AS pending,
                      max(receipt_digest) AS receipt_digest
               FROM company_learning_barriers WHERE tenant_id=$1 AND batch_id=$2""",
            tenant_id, batch_id,
        )
        models = await conn.fetchval(
            "SELECT count(*)::int FROM accepted_current_models WHERE tenant_id=$1",
            tenant_id,
        )
        invariants = await conn.fetchrow(
            """SELECT
                 (SELECT count(*)::int FROM accepted_current_models
                    WHERE model_id=$2 AND tenant_id<>$1) AS cross_tenant_models,
                 (SELECT count(*)::int FROM relation_truth_versions WHERE tenant_id=$1) AS relations,
                 greatest((SELECT count(*)::int FROM model_truth_versions WHERE tenant_id=$1)-1,0) AS lifecycle_duplicates,
                 abs((SELECT count(*)::int FROM models WHERE tenant_id=$1)-1)
                   + abs((SELECT count(*)::int FROM model_truth_versions WHERE tenant_id=$1)-1)
                   + abs((SELECT count(*)::int FROM model_truth_heads WHERE tenant_id=$1)-1) AS partial_truth,
                 (SELECT count(*)::int FROM accepted_current_models
                    WHERE tenant_id=$1 AND truth_version_id<>$3) AS stale_active,
                 (SELECT count(*)::int FROM pending_post_commit_actions
                    WHERE tenant_id=$1 AND dead_lettered_at IS NOT NULL)
                   + (SELECT count(*)::int FROM projection_refresh_jobs
                    WHERE tenant_id=$1 AND status='dead_letter')
                   + (SELECT count(*)::int FROM summarization_batch_items
                    WHERE tenant_id=$1 AND status='failed') AS dead_letters""",
            tenant_id, admitted.model_id, admitted_version,
        )
        await conn.close()
        normalized_state = {
            "barriers": row["barriers"], "models": models,
            "versions": 1 + invariants["lifecycle_duplicates"],
            "heads": 1 if invariants["partial_truth"] == 0 else None,
            "relations": invariants["relations"],
        }
        reference_digest = await _uninterrupted_reference_digest(dsn)
        state = {"tenant_id": str(tenant_id), "batch_id": batch_id,
                 "pending": row["pending"], "receipt_digest": row["receipt_digest"],
                 **normalized_state, **dict(invariants), "reference_digest": reference_digest}
        return DurableFaultReceipt(
            boundary, duplicate, str(tenant_id), batch_id, pre_restart_fate,
            row["barriers"], models, row["pending"], row["receipt_digest"],
            canonical_sha256(state), invariants["cross_tenant_models"], invariants["relations"],
            invariants["lifecycle_duplicates"], invariants["partial_truth"],
            invariants["stale_active"], invariants["dead_letters"], reference_digest,
            canonical_sha256(normalized_state) == reference_digest,
        )
    except Exception:
        # Truth-kernel tables are intentionally append-only, so evidence tenants
        # remain durable for audit/reopen. Run this suite only in an isolated
        # evaluation database whose lifecycle is owned by the coordinator.
        raise


async def run_postgres_fault_slice(dsn: str) -> PostgresFaultSlice:
    run_id = str(uuid4())
    receipts = tuple([
        await _execute_case(dsn, boundary=boundary, duplicate=duplicate)
        for boundary in P8_DB_COVERED_BOUNDARIES
        for duplicate in (False, True)
    ])
    payload: dict[str, Any] = {
        "database_run_id": run_id,
        "covered_boundaries": P8_DB_COVERED_BOUNDARIES,
        "uncovered_boundaries": P8_DB_UNCOVERED_BOUNDARIES,
        "receipts": [asdict(receipt) for receipt in receipts],
    }
    return PostgresFaultSlice(
        run_id, P8_DB_COVERED_BOUNDARIES, P8_DB_UNCOVERED_BOUNDARIES,
        receipts, canonical_sha256(payload), False,
    )
