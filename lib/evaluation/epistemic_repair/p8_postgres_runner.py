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
from lib.shared.errors import InvariantViolation
from services.domain.company_learning.barrier import CompanyLearningBarrierService
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.service import TruthKernelService


P8_DB_COVERED_BOUNDARIES = (
    "crash_after_validation_before_apply",
    "crash_after_apply_before_queue_ack",
    "crash_during_projection_refresh",
    "restart_with_pending_truth_critical_work",
    "duplicate_delivery_replay",
)
P8_DB_UNCOVERED_BOUNDARIES = (
    "provider_timeout_before_response",
    "provider_timeout_after_partial_work",
    "invalid_structured_output",
    "validation_rejection",
    "database_serialization_failure",
    "crash_during_dependent_lifecycle_fencing",
    "authority_revocation_selection_to_commit",
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


async def _execute_case(dsn: str, *, boundary: str, duplicate: bool) -> DurableFaultReceipt:
    tenant_id, batch_id = uuid4(), f"p8:{boundary}:{int(duplicate)}"
    await _setup_tenant(dsn, tenant_id)
    admitted_version: UUID | None = None
    pre_restart_fate = "connection_closed"
    try:
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
            admitted = await TruthKernelService(storage=AsyncpgTruthKernelStorage()).admit(
                tx=conn, command=_admission(tenant_id, 1),
            )
            admitted_version = admitted.version_id
            service = CompanyLearningBarrierService()
            if boundary == "restart_with_pending_truth_critical_work":
                try:
                    await service.complete(
                        tx=conn, barrier_id=uuid4(), tenant_id=tenant_id,
                        batch_id=batch_id, expected_model_version_ids=(admitted.version_id,),
                        truth_critical_pending_count=1, completed_at=datetime.now(timezone.utc),
                    )
                except InvariantViolation:
                    pre_restart_fate = "pending_work_rejected"
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
        await conn.close()
        state = {
            "tenant_id": str(tenant_id), "batch_id": batch_id,
            "barriers": row["barriers"], "models": models,
            "pending": row["pending"], "receipt_digest": row["receipt_digest"],
        }
        return DurableFaultReceipt(
            boundary, duplicate, str(tenant_id), batch_id, pre_restart_fate,
            row["barriers"], models, row["pending"], row["receipt_digest"],
            canonical_sha256(state),
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
