from __future__ import annotations

from datetime import timedelta

import pytest

from lib.contracts.truth_admission import ModelTruthLifecycle
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.service import TruthKernelService
from services.domain.truth_kernel.tests import test_service as fixtures
from services.domain.truth_kernel.tests.test_service import NOW, admission, advance


class RecordingTransaction:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = []

    async def execute(self, sql: str, *args):
        self.statements.append((" ".join(sql.split()), args))
        if sql.lstrip().startswith("UPDATE model_truth_heads"):
            return "UPDATE 1"
        return "INSERT 0 1"

    async def executemany(self, sql: str, args):
        for values in args:
            self.statements.append((" ".join(sql.split()), tuple(values)))

    async def fetchrow(self, sql: str, *args):
        self.statements.append((" ".join(sql.split()), args))
        return None


@pytest.mark.asyncio
async def test_admission_adapter_persists_version_bound_evidence_and_scope() -> None:
    tx = RecordingTransaction()
    command = admission()
    storage = AsyncpgTruthKernelStorage()
    await storage.insert_admission_bundle(tx=tx, command=command)

    sql = "\n".join(statement for statement, _ in tx.statements)
    assert "INSERT INTO truth_candidates" in sql
    assert "INSERT INTO truth_admission_decisions" in sql
    assert "INSERT INTO model_truth_versions" in sql
    assert "INSERT INTO model_truth_evidence_references" in sql
    assert "INSERT INTO model_truth_scope_bindings" in sql
    assert "INSERT INTO model_truth_scope_evidence" in sql
    assert "INSERT INTO models" in sql
    assert "array_fill(0.0::real, ARRAY[768])::vector" in sql
    assert "ON CONFLICT (id) DO NOTHING" in sql
    evidence_args = next(
        args
        for statement, args in tx.statements
        if "INSERT INTO model_truth_evidence_references" in statement
    )
    assert evidence_args[2] == command.version.version_id
    assert evidence_args[24] == command.version.evidence[0].reference_digest
    scope_args = next(
        args
        for statement, args in tx.statements
        if "INSERT INTO model_truth_scope_bindings" in statement
    )
    assert scope_args[6] == command.version.scope[0].canonical_ref
    assert scope_args[7] == command.version.scope[0].display_label
    assert scope_args[9] == command.version.scope[0].canonical_ref_status
    assert scope_args[10] == command.version.scope[0].normalization_version


@pytest.mark.asyncio
async def test_lifecycle_adapter_binds_cas_event_and_receipt_to_exact_versions(
) -> None:
    # Use the memory port only to construct a coherent successor contract.
    initial = admission()
    fixtures.STORE = fixtures.MemoryStorage()
    fixtures.STORE.versions[initial.version.version_id] = initial.version
    head = TruthKernelService._head_from_version(
        initial.version, advanced_at=initial.issued_at
    )
    command = advance(head, ModelTruthLifecycle.FALSIFIED)
    tx = RecordingTransaction()
    storage = AsyncpgTruthKernelStorage()
    successor = TruthKernelService._head_from_version(
        command.next_version, advanced_at=command.issued_at
    )

    await storage.insert_version(tx=tx, version=command.next_version, prior_head=head)
    assert await storage.compare_and_swap_head(
        tx=tx, expected=head, successor=successor
    )
    from services.domain.truth_kernel.service import TruthCommandReceipt

    receipt = TruthCommandReceipt(
        command_id=command.command_id,
        tenant_id=command.tenant_id,
        idempotency_key=command.idempotency_key,
        request_digest=command.request_digest,
        operation="advance",
        model_id=successor.model_id,
        version_id=successor.version_id,
        version=successor.version,
        semantic_digest=successor.semantic_digest,
        lifecycle=successor.lifecycle,
        applied_at=NOW + timedelta(minutes=1),
    )
    await storage.append_event(
        tx=tx,
        operation="advance",
        prior_head=head,
        successor=successor,
        command_id=command.command_id,
        request_digest=command.request_digest,
        transition=command.transition,
        reason_codes=command.reason_codes,
    )
    await storage.insert_receipt(tx=tx, receipt=receipt)

    version_args = next(
        args
        for statement, args in tx.statements
        if "INSERT INTO model_truth_versions" in statement
    )
    event_args = next(
        args
        for statement, args in tx.statements
        if "INSERT INTO model_truth_lifecycle_events" in statement
    )
    receipt_args = next(
        args
        for statement, args in tx.statements
        if "INSERT INTO truth_command_receipts" in statement
    )
    assert version_args[20] == head.version_id
    assert event_args[3:6] == (head.version_id, successor.version_id, "falsify")
    assert receipt_args[0] == event_args[2] == command.command_id
    assert receipt_args[5] == "applied"
    assert receipt_args[6] == successor.version_id
