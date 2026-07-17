"""Transaction-local relation admission, versioning, and invalidation kernel."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation

from .contracts import (
    RelationCandidate,
    RelationDisposition,
    RelationEvidence,
    RelationKind,
    RelationLifecycle,
    RelationParticipant,
    RelationVersion,
    validate_admissible_relation,
)


def evidence_confidence(evidence: tuple[RelationEvidence, ...]) -> float:
    """Deterministic confidence projection over unique signed evidence.

    It is intentionally free of counters and insertion order. Counterevidence
    increases the denominator, so confidence can decrease on a later version.
    """

    unique = {item.evidence_reference_id: item for item in evidence}
    support = sum(item.weight for item in unique.values() if item.polarity == 1)
    counter = sum(item.weight for item in unique.values() if item.polarity == -1)
    total = support + counter
    return support / total if total else 0.0


@dataclass(frozen=True, slots=True)
class RelationHead:
    tenant_id: UUID
    relation_id: UUID
    relation_version_id: UUID
    version: int
    semantic_digest: str
    lifecycle: RelationLifecycle
    advanced_at: datetime


@dataclass(frozen=True, slots=True)
class RelationCommandReceipt:
    command_id: UUID
    tenant_id: UUID
    idempotency_key: str
    request_digest: str
    outcome: str
    disposition: RelationDisposition
    relation_version_id: UUID | None
    rejection_code: str | None
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class AdmitRelationCommand:
    command_id: UUID
    idempotency_key: str
    candidate: RelationCandidate
    relation_version_id: UUID
    admission_decision_id: UUID
    issued_at: datetime

    @property
    def request_digest(self) -> str:
        return canonical_sha256(
            {
                "command_id": str(self.command_id),
                "idempotency_key": self.idempotency_key,
                "candidate_digest": self.candidate.candidate_digest,
                "relation_version_id": str(self.relation_version_id),
                "admission_decision_id": str(self.admission_decision_id),
                "issued_at": self.issued_at.isoformat(),
            }
        )


@dataclass(frozen=True, slots=True)
class AdvanceRelationCommand:
    command_id: UUID
    tenant_id: UUID
    idempotency_key: str
    expected_head: RelationHead
    next_version: RelationVersion
    issued_at: datetime

    @property
    def request_digest(self) -> str:
        return canonical_sha256(
            {
                "command_id": str(self.command_id),
                "tenant_id": str(self.tenant_id),
                "idempotency_key": self.idempotency_key,
                "expected": self.expected_head,
                "next": self.next_version.model_dump(mode="json"),
                "issued_at": self.issued_at.isoformat(),
            }
        )


class RelationKernelStorage(Protocol):
    async def find_receipt(self, *, tx: Any, tenant_id: UUID, idempotency_key: str) -> RelationCommandReceipt | None: ...
    async def record_candidate_decision(self, *, tx: Any, command: AdmitRelationCommand, disposition: RelationDisposition, reason_codes: tuple[str, ...], version: RelationVersion | None) -> None: ...
    async def insert_initial_head(self, *, tx: Any, head: RelationHead) -> None: ...
    async def lock_head(self, *, tx: Any, tenant_id: UUID, relation_id: UUID) -> RelationHead | None: ...
    async def load_version(self, *, tx: Any, tenant_id: UUID, relation_version_id: UUID) -> RelationVersion: ...
    async def validate_active_participants(self, *, tx: Any, tenant_id: UUID, participants: tuple[RelationParticipant, ...]) -> tuple[str, ...]: ...
    async def insert_version(self, *, tx: Any, version: RelationVersion) -> None: ...
    async def compare_and_swap_head(self, *, tx: Any, expected: RelationHead, successor: RelationHead) -> bool: ...
    async def insert_receipt(self, *, tx: Any, receipt: RelationCommandReceipt) -> None: ...
    async def dispute_for_invalidated_evidence(self, *, tx: Any, tenant_id: UUID, invalidated_model_version_id: UUID, cause_code: str, occurred_at: datetime) -> tuple[UUID, ...]: ...


class RelationTruthKernel:
    def __init__(self, storage: RelationKernelStorage) -> None:
        self._storage = storage

    async def admit(self, *, tx: Any, command: AdmitRelationCommand) -> RelationCommandReceipt:
        replay = await self._replay(tx=tx, tenant_id=command.candidate.tenant_id, idempotency_key=command.idempotency_key, request_digest=command.request_digest)
        if replay:
            return replay
        candidate = command.candidate
        kind = candidate.known_kind
        if kind is None:
            return await self._nonaccepted(tx=tx, command=command, disposition=RelationDisposition.NEEDS_REVIEW, code="RELATION_KIND_UNKNOWN")
        try:
            if candidate.assertion is None:
                raise ValueError("relation rationale requires a direction assertion")
            validate_admissible_relation(kind=kind, participants=candidate.participants, assertion=candidate.assertion, evidence=candidate.evidence)
            support = sum(item.weight for item in candidate.evidence if item.polarity == 1)
            counter = sum(item.weight for item in candidate.evidence if item.polarity == -1)
            if support <= counter:
                raise ValueError("counterevidence does not support active admission")
        except ValueError as error:
            return await self._nonaccepted(tx=tx, command=command, disposition=RelationDisposition.REJECTED, code=f"RELATION_INVALID:{error}")
        endpoint_errors = await self._storage.validate_active_participants(
            tx=tx, tenant_id=candidate.tenant_id, participants=candidate.participants
        )
        if endpoint_errors:
            return await self._nonaccepted(
                tx=tx,
                command=command,
                disposition=RelationDisposition.REJECTED,
                code="RELATION_ENDPOINT_INVALID:" + ",".join(sorted(endpoint_errors)),
            )
        existing = await self._storage.lock_head(
            tx=tx,
            tenant_id=candidate.tenant_id,
            relation_id=candidate.candidate_relation_id,
        )
        if existing is not None:
            raise InvariantViolation(
                "RELATION_ADMISSION_UNIQUE_HEAD",
                "initial admission cannot replace an existing relation head",
                relation_id=str(candidate.candidate_relation_id),
            )

        semantic_digest = RelationVersion.compute_semantic_digest(
            kind=kind,
            participants=candidate.participants,
            rationale=candidate.rationale,
            assertion=candidate.assertion,
            evidence=candidate.evidence,
        )
        version = RelationVersion(
            relation_version_id=command.relation_version_id,
            relation_id=candidate.candidate_relation_id,
            tenant_id=candidate.tenant_id,
            version=1,
            admission_decision_id=command.admission_decision_id,
            kind=kind,
            participants=candidate.participants,
            rationale=candidate.rationale,
            assertion=candidate.assertion,
            evidence=candidate.evidence,
            created_at=command.issued_at,
            semantic_digest=semantic_digest,
        )
        await self._storage.record_candidate_decision(tx=tx, command=command, disposition=RelationDisposition.ACCEPTED, reason_codes=("RELATION_CONTRACT_VALID",), version=version)
        head = self._head(version, command.issued_at)
        await self._storage.insert_initial_head(tx=tx, head=head)
        receipt = self._receipt(command=command, disposition=RelationDisposition.ACCEPTED, relation_version_id=version.relation_version_id, code=None)
        await self._storage.insert_receipt(tx=tx, receipt=receipt)
        return receipt

    async def advance(self, *, tx: Any, command: AdvanceRelationCommand) -> RelationCommandReceipt:
        replay = await self._replay(tx=tx, tenant_id=command.tenant_id, idempotency_key=command.idempotency_key, request_digest=command.request_digest)
        if replay:
            return replay
        current = await self._storage.lock_head(tx=tx, tenant_id=command.tenant_id, relation_id=command.expected_head.relation_id)
        if current != command.expected_head:
            raise InvariantViolation("RELATION_HEAD_CONFLICT", "relation head compare-and-swap expectation is stale")
        nxt = command.next_version
        if nxt.tenant_id != command.tenant_id or nxt.relation_id != current.relation_id:
            raise InvariantViolation("RELATION_ENDPOINT_REBIND", "relation revision cannot change relation identity")
        if nxt.version != current.version + 1 or nxt.supersedes_relation_version_id != current.relation_version_id:
            raise InvariantViolation("RELATION_VERSION_LINEAGE", "relation revision must bind the current immutable version")
        endpoint_errors = await self._storage.validate_active_participants(
            tx=tx, tenant_id=command.tenant_id, participants=nxt.participants
        )
        if endpoint_errors:
            raise InvariantViolation(
                "RELATION_ENDPOINT_INVALID",
                "relation revision has inactive or mismatched endpoints",
                errors=tuple(sorted(endpoint_errors)),
            )
        # Endpoint Model identities are stable. A new ModelVersion binding is
        # allowed only because it is explicit in this admitted revision; the
        # kernel never performs replacement-driven automatic rebinding.
        current_version = await self._version_for_head(tx=tx, head=current)
        if {p.role: p.model_id for p in nxt.participants} != {p.role: p.model_id for p in current_version.participants}:
            raise InvariantViolation("RELATION_ENDPOINT_REBIND", "relation revision cannot silently replace endpoint Models")
        await self._storage.insert_version(tx=tx, version=nxt)
        successor = self._head(nxt, command.issued_at)
        if not await self._storage.compare_and_swap_head(tx=tx, expected=current, successor=successor):
            raise InvariantViolation("RELATION_HEAD_CONFLICT", "relation head lost compare-and-swap race")
        receipt = RelationCommandReceipt(command.command_id, command.tenant_id, command.idempotency_key, command.request_digest, "applied", RelationDisposition.ACCEPTED, nxt.relation_version_id, None, command.issued_at)
        await self._storage.insert_receipt(tx=tx, receipt=receipt)
        return receipt

    async def invalidate_evidence(self, *, tx: Any, tenant_id: UUID, invalidated_model_version_id: UUID, cause_code: str, occurred_at: datetime) -> tuple[UUID, ...]:
        """Fence affected heads and create one version-bound obligation each.

        The storage operation is deliberately indivisible: implementations
        update heads to disputed and insert uniqueness-protected obligations in
        the caller's transaction.
        """
        return await self._storage.dispute_for_invalidated_evidence(tx=tx, tenant_id=tenant_id, invalidated_model_version_id=invalidated_model_version_id, cause_code=cause_code, occurred_at=occurred_at)

    async def _version_for_head(self, *, tx: Any, head: RelationHead) -> RelationVersion:
        return await self._storage.load_version(tx=tx, tenant_id=head.tenant_id, relation_version_id=head.relation_version_id)

    async def _replay(self, *, tx: Any, tenant_id: UUID, idempotency_key: str, request_digest: str) -> RelationCommandReceipt | None:
        receipt = await self._storage.find_receipt(tx=tx, tenant_id=tenant_id, idempotency_key=idempotency_key)
        if receipt and receipt.request_digest != request_digest:
            raise InvariantViolation("RELATION_IDEMPOTENCY_CONFLICT", "idempotency key was reused with a different request")
        return receipt

    async def _nonaccepted(self, *, tx: Any, command: AdmitRelationCommand, disposition: RelationDisposition, code: str) -> RelationCommandReceipt:
        await self._storage.record_candidate_decision(tx=tx, command=command, disposition=disposition, reason_codes=(code,), version=None)
        receipt = self._receipt(command=command, disposition=disposition, relation_version_id=None, code=code)
        await self._storage.insert_receipt(tx=tx, receipt=receipt)
        return receipt

    @staticmethod
    def _head(version: RelationVersion, at: datetime) -> RelationHead:
        return RelationHead(version.tenant_id, version.relation_id, version.relation_version_id, version.version, version.semantic_digest, version.lifecycle, at)

    @staticmethod
    def _receipt(*, command: AdmitRelationCommand, disposition: RelationDisposition, relation_version_id: UUID | None, code: str | None) -> RelationCommandReceipt:
        return RelationCommandReceipt(command.command_id, command.candidate.tenant_id, command.idempotency_key, command.request_digest, "applied" if disposition is RelationDisposition.ACCEPTED else "rejected", disposition, relation_version_id, code, command.issued_at)


__all__ = ["AdmitRelationCommand", "AdvanceRelationCommand", "RelationCommandReceipt", "RelationHead", "RelationKernelStorage", "RelationTruthKernel", "evidence_confidence"]
