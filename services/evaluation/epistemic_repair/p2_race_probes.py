"""Production-shaped PostgreSQL probes for P2 lifecycle atomicity and races."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid5

from lib.contracts.truth_admission import AdvanceModelHeadCommand
from services.domain.truth_kernel.repository import (
    AsyncpgTruthKernelStorage,
    render_model_head_cas_sql,
)
from services.domain.truth_kernel.service import FenceContext, TruthKernelService


_PROBE_NAMESPACE = UUID("7e81cb5a-f03e-4ae1-90f8-b9f0ee0f7d25")


class InjectedFenceFailure(RuntimeError):
    """Intentional failure used to prove caller-transaction rollback."""


@dataclass(frozen=True, slots=True)
class FaultRetryProbeResult:
    rollback_conforms: bool
    retry_conforms: bool
    rollback_lifecycle_event_count: int
    rollback_repair_obligation_count: int
    lifecycle_event_count: int
    repair_obligation_count: int
    violation_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConcurrentTransitionProbeResult:
    conforms: bool
    winner_count: int
    lifecycle_event_count: int
    final_version: int
    final_lifecycle: str
    violation_codes: tuple[str, ...] = ()


class FiveProjectionFence:
    """Synchronously fence five deterministic dependent projection identities."""

    name = "p2_five_projection_fence"

    def __init__(self, *, fail_after: int | None = None) -> None:
        self.fail_after = fail_after

    async def apply(self, *, tx: Any, context: FenceContext) -> None:
        if not context.next_version.lifecycle.terminal:
            return
        for ordinal in range(1, 6):
            projection_id = uuid5(
                _PROBE_NAMESPACE,
                f"{context.tenant_id}:{context.prior_head.version_id}:projection:{ordinal}",
            )
            await tx.execute(
                """
                INSERT INTO truth_repair_obligations (
                  obligation_id, tenant_id, invalidated_model_version_id,
                  affected_kind, affected_id, cause_code, status, created_at
                ) VALUES (gen_random_uuid(),$1,$2,'projection',$3,
                          'p2_projection_fence','pending',$4)
                ON CONFLICT (
                  tenant_id, invalidated_model_version_id, affected_kind,
                  affected_id, cause_code
                ) DO NOTHING
                """,
                context.tenant_id,
                context.prior_head.version_id,
                projection_id,
                context.next_version.created_at,
            )
            if self.fail_after == ordinal:
                raise InjectedFenceFailure(f"injected failure after projection fence {ordinal}")


async def probe_fault_rollback_and_retry(
    conn: Any,
    *,
    command: AdvanceModelHeadCommand,
) -> FaultRetryProbeResult:
    """Prove partial fence work rolls back and an identical retry is singular."""

    storage = AsyncpgTruthKernelStorage()
    failing = TruthKernelService(
        storage=storage, fences=(FiveProjectionFence(fail_after=3),)
    )
    complete = TruthKernelService(storage=storage, fences=(FiveProjectionFence(),))
    tenant_id = command.tenant_id
    model_id = command.expectation.model_id
    old_version_id = command.expectation.expected_version_id

    failure_observed = False
    try:
        # This nested transaction is a real PostgreSQL savepoint beneath the
        # evaluator's rollback transaction.
        async with conn.transaction():
            await failing.advance(tx=conn, command=command)
    except InjectedFenceFailure:
        failure_observed = True

    head_after_failure = await conn.fetchrow(
        "SELECT version_id, version, lifecycle FROM model_truth_heads WHERE tenant_id=$1 AND model_id=$2",
        tenant_id, model_id,
    )
    partial_versions = await conn.fetchval(
        "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1 AND version_id=$2",
        tenant_id, command.next_version.version_id,
    )
    partial_events = await conn.fetchval(
        "SELECT count(*) FROM model_truth_lifecycle_events WHERE tenant_id=$1 AND command_id=$2",
        tenant_id, command.command_id,
    )
    partial_obligations = await conn.fetchval(
        """SELECT count(*) FROM truth_repair_obligations
           WHERE tenant_id=$1 AND invalidated_model_version_id=$2
             AND cause_code='p2_projection_fence'""",
        tenant_id, old_version_id,
    )
    rollback_conforms = bool(
        failure_observed
        and head_after_failure
        and head_after_failure["version_id"] == old_version_id
        and partial_versions == 0
        and partial_events == 0
        and partial_obligations == 0
    )

    first = await complete.advance(tx=conn, command=command)
    replay = await complete.advance(tx=conn, command=command)
    final_head = await conn.fetchrow(
        "SELECT version_id, version, lifecycle FROM model_truth_heads WHERE tenant_id=$1 AND model_id=$2",
        tenant_id, model_id,
    )
    event_count = await conn.fetchval(
        "SELECT count(*) FROM model_truth_lifecycle_events WHERE tenant_id=$1 AND command_id=$2",
        tenant_id, command.command_id,
    )
    obligation_count = await conn.fetchval(
        """SELECT count(*) FROM truth_repair_obligations
           WHERE tenant_id=$1 AND invalidated_model_version_id=$2
             AND cause_code='p2_projection_fence'""",
        tenant_id, old_version_id,
    )
    retry_conforms = bool(
        first == replay
        and final_head
        and final_head["version_id"] == command.next_version.version_id
        and final_head["lifecycle"] == command.next_version.lifecycle.value
        and event_count == 1
        and obligation_count == 5
    )
    violations = tuple(
        code
        for condition, code in (
            (rollback_conforms, "partial_fence_work_survived_rollback"),
            (retry_conforms, "retry_not_wholly_fenced_or_not_singular"),
        )
        if not condition
    )
    return FaultRetryProbeResult(
        rollback_conforms, retry_conforms, partial_events, partial_obligations,
        event_count, obligation_count, violations
    )


async def probe_concurrent_transitions(
    dsn: str,
    *,
    tenant_id: UUID,
    model_id: UUID,
    transitions: tuple[str, str] = ("active", "falsified"),
) -> ConcurrentTransitionProbeResult:
    """Run the production head-CAS predicate on two real DB transactions.

    Immutable production truth rows intentionally cannot be deleted, so a
    disposable schema mirrors the production head predicate and event write.
    This proves PostgreSQL contention/atomicity without polluting canonical
    truth. The production service's command validation is covered separately.
    """

    import asyncpg

    schema = f"p2_race_{tenant_id.hex}"
    admin = await asyncpg.connect(dsn)
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        await admin.execute(
            f'''CREATE TABLE "{schema}".head (
                  tenant_id uuid NOT NULL, model_id uuid NOT NULL,
                  version_id uuid NOT NULL, version integer NOT NULL,
                  semantic_digest text NOT NULL, lifecycle text NOT NULL,
                  advanced_at timestamptz NOT NULL,
                  PRIMARY KEY (tenant_id, model_id)
                );
                CREATE TABLE "{schema}".events (
                  event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                  tenant_id uuid NOT NULL, model_id uuid NOT NULL,
                  transition text NOT NULL
                )'''
        )
        await admin.execute(
            f'INSERT INTO "{schema}".head VALUES ($1,$2,$3,1,$4,\'active\',$5)',
            tenant_id, model_id, uuid5(_PROBE_NAMESPACE, f"{model_id}:v1"),
            "a" * 64, datetime.now(timezone.utc),
        )
    finally:
        await admin.close()

    ready = asyncio.Event()
    ready_count = 0
    ready_lock = asyncio.Lock()

    async def contend(lifecycle: str) -> tuple[str, str]:
        nonlocal ready_count
        conn = await asyncpg.connect(dsn)
        try:
            async with conn.transaction():
                async with ready_lock:
                    ready_count += 1
                    if ready_count == 2:
                        ready.set()
                await ready.wait()
                result = await conn.execute(
                    render_model_head_cas_sql(f"{schema}.head"),
                    tenant_id, model_id,
                    uuid5(_PROBE_NAMESPACE, f"{model_id}:{lifecycle}:v2"),
                    2, ("b" if lifecycle == "active" else "c") * 64,
                    lifecycle, datetime.now(timezone.utc),
                    uuid5(_PROBE_NAMESPACE, f"{model_id}:v1"),
                    1, "a" * 64, "active",
                )
                if result != "UPDATE 1":
                    return "lost", "truth_head_cas"
                await conn.execute(
                    f'INSERT INTO "{schema}".events (tenant_id,model_id,transition) VALUES ($1,$2,$3)',
                    tenant_id, model_id, lifecycle,
                )
                return "won", lifecycle
        finally:
            await conn.close()

    outcomes = await asyncio.gather(*(contend(item) for item in transitions))
    observer = await asyncpg.connect(dsn)
    try:
        head = await observer.fetchrow(
            f'SELECT version, lifecycle FROM "{schema}".head WHERE tenant_id=$1 AND model_id=$2',
            tenant_id, model_id,
        )
        events = await observer.fetchval(
            f'SELECT count(*) FROM "{schema}".events WHERE tenant_id=$1 AND model_id=$2',
            tenant_id, model_id,
        )
    finally:
        await observer.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await observer.close()
    winners = sum(status == "won" for status, _ in outcomes)
    conforms = bool(
        head
        and winners == 1
        and events == 1
        and head["version"] == 2
        and head["lifecycle"] in {"active", "falsified"}
    )
    return ConcurrentTransitionProbeResult(
        conforms=conforms,
        winner_count=winners,
        lifecycle_event_count=events,
        final_version=int(head["version"]) if head else 0,
        final_lifecycle=head["lifecycle"] if head else "missing",
        violation_codes=() if conforms else ("concurrent_transition_not_single_winner",),
    )


async def cleanup_concurrency_probe(conn: Any, *, tenant_id: UUID) -> None:
    """Remove every committed row owned by the isolated concurrency tenant."""

    for table in (
        "truth_command_receipts", "model_truth_lifecycle_events",
        "model_truth_heads", "model_truth_scope_evidence",
        "model_truth_scope_bindings", "model_truth_evidence_references",
        "model_truth_versions", "truth_admission_decisions", "truth_candidates",
        "models", "tenants",
    ):
        await conn.execute(f"DELETE FROM {table} WHERE tenant_id=$1" if table != "tenants" else "DELETE FROM tenants WHERE id=$1", tenant_id)


__all__ = [
    "ConcurrentTransitionProbeResult", "FaultRetryProbeResult",
    "FiveProjectionFence", "InjectedFenceFailure", "cleanup_concurrency_probe",
    "probe_concurrent_transitions", "probe_fault_rollback_and_retry",
]
