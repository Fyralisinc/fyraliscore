"""Transaction-scoped admission and lifecycle compiler.

The caller owns the database transaction.  This service deliberately has no
commit, rollback, or background-queue escape hatch: candidate, decision,
version, head, event, and every truth-critical fence either become visible
together or not at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal, Protocol, Sequence
from uuid import UUID

from lib.contracts.kernel import canonical_sha256
from lib.contracts.truth_admission import (
    AdmitModelCommand,
    AdvanceModelHeadCommand,
    ModelHead,
    ModelTruthLifecycle,
    ModelTruthTransition,
    ModelVersion,
)
from lib.shared.errors import InvariantViolation


@dataclass(frozen=True, slots=True)
class TruthCommandReceipt:
    command_id: UUID
    tenant_id: UUID
    idempotency_key: str
    request_digest: str
    operation: str
    model_id: UUID
    version_id: UUID
    version: int
    semantic_digest: str
    lifecycle: ModelTruthLifecycle
    applied_at: datetime
    outcome: Literal["applied", "absorbed_duplicate"] = "applied"

    @property
    def result_digest(self) -> str:
        return canonical_sha256(
            {
                "command_id": str(self.command_id),
                "tenant_id": str(self.tenant_id),
                "idempotency_key": self.idempotency_key,
                "request_digest": self.request_digest,
                "operation": self.operation,
                "model_id": str(self.model_id),
                "version_id": str(self.version_id),
                "version": self.version,
                "semantic_digest": self.semantic_digest,
                "lifecycle": self.lifecycle.value,
                "applied_at": self.applied_at.isoformat(),
                "outcome": self.outcome,
            }
        )


@dataclass(frozen=True, slots=True)
class FenceContext:
    tenant_id: UUID
    model_id: UUID
    prior_head: ModelHead
    next_version: ModelVersion
    command_id: UUID
    cause_digest: str


class TruthFence(Protocol):
    """One synchronous, transaction-local dependent-read fence."""

    name: str

    async def apply(self, *, tx: Any, context: FenceContext) -> None: ...


class TruthKernelStorage(Protocol):
    """Persistence port. Implementations must use only the supplied transaction."""

    async def find_receipt(
        self, *, tx: Any, tenant_id: UUID, idempotency_key: str
    ) -> TruthCommandReceipt | None: ...

    async def lock_semantic_admission(
        self, *, tx: Any, tenant_id: UUID, semantic_digest: str
    ) -> None: ...

    async def find_active_semantic_head(
        self, *, tx: Any, tenant_id: UUID, semantic_digest: str
    ) -> ModelHead | None: ...

    async def insert_admission_bundle(
        self, *, tx: Any, command: AdmitModelCommand
    ) -> None: ...

    async def lock_head(
        self, *, tx: Any, tenant_id: UUID, model_id: UUID
    ) -> ModelHead | None: ...

    async def insert_version(
        self, *, tx: Any, version: ModelVersion, prior_head: ModelHead
    ) -> None: ...

    async def insert_initial_head(self, *, tx: Any, head: ModelHead) -> None: ...

    async def compare_and_swap_head(
        self, *, tx: Any, expected: ModelHead, successor: ModelHead
    ) -> bool: ...

    async def append_event(
        self,
        *,
        tx: Any,
        operation: str,
        prior_head: ModelHead | None,
        successor: ModelHead,
        command_id: UUID,
        request_digest: str,
        transition: ModelTruthTransition | None,
        reason_codes: tuple[str, ...],
    ) -> None: ...

    async def insert_receipt(
        self, *, tx: Any, receipt: TruthCommandReceipt
    ) -> None: ...

    async def insert_semantic_absorption(
        self,
        *,
        tx: Any,
        command: AdmitModelCommand,
        receipt: TruthCommandReceipt,
    ) -> None: ...


class TruthKernelService:
    """The sole semantic command owner for Model admission and head movement."""

    def __init__(
        self,
        *,
        storage: TruthKernelStorage,
        fences: Sequence[TruthFence] = (),
    ) -> None:
        names = [fence.name for fence in fences]
        if len(names) != len(set(names)):
            raise ValueError("truth fence names must be unique")
        self._storage = storage
        self._fences = tuple(fences)

    async def admit(
        self, *, tx: Any, command: AdmitModelCommand
    ) -> TruthCommandReceipt:
        async with self._command_authority(tx=tx, command_id=command.command_id):
            return await self._admit_authorized(tx=tx, command=command)

    async def _admit_authorized(
        self, *, tx: Any, command: AdmitModelCommand
    ) -> TruthCommandReceipt:
        replay = await self._idempotent_replay(tx=tx, command=command)
        if replay is not None:
            return replay

        # The semantic digest binds proposition, representation, evidence and
        # typed scope. Serializing on that complete identity absorbs exact
        # duplicates without conflating textually similar but distinct claims.
        await self._storage.lock_semantic_admission(
            tx=tx,
            tenant_id=command.tenant_id,
            semantic_digest=command.version.semantic_digest,
        )
        duplicate = await self._storage.find_active_semantic_head(
            tx=tx,
            tenant_id=command.tenant_id,
            semantic_digest=command.version.semantic_digest,
        )
        if duplicate is not None:
            receipt = TruthCommandReceipt(
                command_id=command.command_id,
                tenant_id=command.tenant_id,
                idempotency_key=command.idempotency_key,
                request_digest=command.request_digest,
                operation="admit",
                model_id=duplicate.model_id,
                version_id=duplicate.version_id,
                version=duplicate.version,
                semantic_digest=duplicate.semantic_digest,
                lifecycle=duplicate.lifecycle,
                applied_at=command.issued_at,
                outcome="absorbed_duplicate",
            )
            await self._storage.insert_receipt(tx=tx, receipt=receipt)
            await self._storage.insert_semantic_absorption(
                tx=tx,
                command=command,
                receipt=receipt,
            )
            return receipt

        existing = await self._storage.lock_head(
            tx=tx, tenant_id=command.tenant_id, model_id=command.version.model_id
        )
        if existing is not None:
            raise InvariantViolation(
                "TRUTH_ADMISSION_UNIQUE_HEAD",
                "initial admission cannot replace an existing Model head",
                model_id=str(command.version.model_id),
                current_version=existing.version,
            )

        # Storage persists the immutable candidate, decision, version evidence,
        # and typed scope. The contract has already bound every digest/reference.
        await self._storage.insert_admission_bundle(tx=tx, command=command)
        head = self._head_from_version(command.version, advanced_at=command.issued_at)
        await self._storage.insert_initial_head(tx=tx, head=head)
        await self._storage.append_event(
            tx=tx,
            operation="admit",
            prior_head=None,
            successor=head,
            command_id=command.command_id,
            request_digest=command.request_digest,
            transition=None,
            reason_codes=command.decision.reason_codes,
        )
        receipt = self._receipt(
            command=command,
            operation="admit",
            version=command.version,
            applied_at=command.issued_at,
        )
        await self._storage.insert_receipt(tx=tx, receipt=receipt)
        return receipt

    async def advance(
        self, *, tx: Any, command: AdvanceModelHeadCommand
    ) -> TruthCommandReceipt:
        async with self._command_authority(tx=tx, command_id=command.command_id):
            return await self._advance_authorized(tx=tx, command=command)

    async def _advance_authorized(
        self, *, tx: Any, command: AdvanceModelHeadCommand
    ) -> TruthCommandReceipt:
        replay = await self._idempotent_replay(tx=tx, command=command)
        if replay is not None:
            return replay

        current = await self._storage.lock_head(
            tx=tx,
            tenant_id=command.tenant_id,
            model_id=command.expectation.model_id,
        )
        if current is None:
            raise InvariantViolation(
                "TRUTH_HEAD_MISSING", "cannot advance a Model without an admitted head"
            )
        if current.lifecycle.terminal:
            raise InvariantViolation(
                "TRUTH_TERMINAL_RESURRECTION",
                "terminal Model truth cannot be advanced or reactivated",
                lifecycle=current.lifecycle.value,
            )
        self._assert_expected_head(current=current, command=command)

        successor = self._head_from_version(
            command.next_version, advanced_at=command.issued_at
        )
        await self._storage.insert_version(
            tx=tx, version=command.next_version, prior_head=current
        )
        fence_context = FenceContext(
            tenant_id=command.tenant_id,
            model_id=current.model_id,
            prior_head=current,
            next_version=command.next_version,
            command_id=command.command_id,
            cause_digest=command.request_digest,
        )
        for fence in self._fences:
            await fence.apply(tx=tx, context=fence_context)
        won = await self._storage.compare_and_swap_head(
            tx=tx, expected=current, successor=successor
        )
        if not won:
            raise InvariantViolation(
                "TRUTH_HEAD_CAS",
                "Model head changed during lifecycle transition",
                expected_version=current.version,
            )
        await self._storage.append_event(
            tx=tx,
            operation="advance",
            prior_head=current,
            successor=successor,
            command_id=command.command_id,
            request_digest=command.request_digest,
            transition=command.transition,
            reason_codes=command.reason_codes,
        )
        receipt = self._receipt(
            command=command,
            operation="advance",
            version=command.next_version,
            applied_at=command.issued_at,
        )
        await self._storage.insert_receipt(tx=tx, receipt=receipt)
        return receipt

    @staticmethod
    @asynccontextmanager
    async def _command_authority(*, tx: Any, command_id: UUID):
        """Scope the one DB capability accepted by canonical Model triggers.

        In-memory ports deliberately have no SQL executor. PostgreSQL callers
        receive a transaction-local capability which is cleared even when a
        fence, CAS, or constraint rejects the command.
        """
        execute = getattr(tx, "execute", None)
        if execute is None:
            yield
            return
        await execute(
            "SELECT set_config('app.truth_kernel_command', $1, true)",
            f"model:{command_id}",
        )
        try:
            yield
        except BaseException:
            # A PostgreSQL statement error leaves the transaction aborted;
            # rollback clears every LOCAL setting, so preserve the real error.
            try:
                await execute(
                    "SELECT set_config('app.truth_kernel_command', '', true)"
                )
            except Exception:
                pass
            raise
        else:
            await execute(
                "SELECT set_config('app.truth_kernel_command', '', true)"
            )

    async def _idempotent_replay(
        self, *, tx: Any, command: Any
    ) -> TruthCommandReceipt | None:
        prior = await self._storage.find_receipt(
            tx=tx,
            tenant_id=command.tenant_id,
            idempotency_key=command.idempotency_key,
        )
        if prior is None:
            return None
        if prior.request_digest != command.request_digest:
            raise InvariantViolation(
                "TRUTH_IDEMPOTENCY_CONFLICT",
                "idempotency key was already used for a different truth command",
                idempotency_key=command.idempotency_key,
                prior_request_digest=prior.request_digest,
                request_digest=command.request_digest,
            )
        return prior

    @staticmethod
    def _assert_expected_head(
        *, current: ModelHead, command: AdvanceModelHeadCommand
    ) -> None:
        expected = command.expectation
        actual = (
            current.tenant_id,
            current.model_id,
            current.version_id,
            current.version,
            current.semantic_digest,
            current.lifecycle,
        )
        wanted = (
            expected.tenant_id,
            expected.model_id,
            expected.expected_version_id,
            expected.expected_version,
            expected.expected_semantic_digest,
            expected.expected_lifecycle,
        )
        if actual != wanted:
            raise InvariantViolation(
                "TRUTH_HEAD_EXPECTATION",
                "lifecycle command does not bind the exact current Model head",
                expected_version=expected.expected_version,
                actual_version=current.version,
            )

    @staticmethod
    def _head_from_version(
        version: ModelVersion, *, advanced_at: datetime
    ) -> ModelHead:
        return ModelHead(
            tenant_id=version.tenant_id,
            model_id=version.model_id,
            version_id=version.version_id,
            version=version.version,
            semantic_digest=version.semantic_digest,
            lifecycle=version.lifecycle,
            advanced_at=advanced_at,
        )

    @staticmethod
    def _receipt(
        *, command: Any, operation: str, version: ModelVersion, applied_at: datetime
    ) -> TruthCommandReceipt:
        return TruthCommandReceipt(
            command_id=command.command_id,
            tenant_id=command.tenant_id,
            idempotency_key=command.idempotency_key,
            request_digest=command.request_digest,
            operation=operation,
            model_id=version.model_id,
            version_id=version.version_id,
            version=version.version,
            semantic_digest=version.semantic_digest,
            lifecycle=version.lifecycle,
            applied_at=applied_at,
        )


__all__ = [
    "FenceContext",
    "TruthCommandReceipt",
    "TruthFence",
    "TruthKernelService",
    "TruthKernelStorage",
]
