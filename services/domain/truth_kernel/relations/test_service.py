from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from lib.shared.errors import InvariantViolation
from services.domain.truth_kernel.relations.contracts import (
    DirectionAssertion,
    RelationCandidate,
    RelationDisposition,
    RelationEvidence,
    RelationKind,
    RelationLifecycle,
    RelationParticipant,
    RelationVersion,
)
from services.domain.truth_kernel.relations.service import (
    AdmitRelationCommand,
    AdvanceRelationCommand,
    RelationHead,
    RelationTruthKernel,
    evidence_confidence,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
TENANT = uuid4()
LEFT_MODEL, RIGHT_MODEL = uuid4(), uuid4()
LEFT_VERSION, RIGHT_VERSION = uuid4(), uuid4()


def participant(role: str, model_id: UUID, version_id: UUID) -> RelationParticipant:
    return RelationParticipant(model_id=model_id, model_version_id=version_id, role=role)


def evidence(*, polarity: int = 1, weight: float = 0.8, reference_id: UUID | None = None) -> RelationEvidence:
    return RelationEvidence(
        evidence_reference_id=reference_id or uuid4(),
        model_version_id=LEFT_VERSION,
        evidence_digest="a" * 64,
        polarity=polarity,
        weight=weight,
    )


def candidate(
    *,
    proposed_kind: str = "causal_influence",
    polarity: int = 1,
    reverse: bool = False,
    roles: tuple[str, str] = ("cause", "effect"),
) -> RelationCandidate:
    source, target = (RIGHT_VERSION, LEFT_VERSION) if reverse else (LEFT_VERSION, RIGHT_VERSION)
    assertion = None
    if proposed_kind in RelationKind._value2member_map_:
        assertion = DirectionAssertion(
            kind=RelationKind(proposed_kind),
            source_model_version_id=source,
            target_model_version_id=target,
            polarity=polarity,
        )
    return RelationCandidate(
        candidate_relation_id=uuid4(),
        tenant_id=TENANT,
        proposed_kind=proposed_kind,
        participants=(
            participant(roles[0], LEFT_MODEL, LEFT_VERSION),
            participant(roles[1], RIGHT_MODEL, RIGHT_VERSION),
        ),
        rationale="The first endpoint has the declared influence on the second.",
        assertion=assertion,
        evidence=(evidence(),),
        created_at=NOW,
    )


def command(item: RelationCandidate) -> AdmitRelationCommand:
    return AdmitRelationCommand(uuid4(), f"admit:{item.candidate_relation_id}", item, uuid4(), uuid4(), NOW)


class MemoryStorage:
    def __init__(self) -> None:
        self.receipts = {}
        self.decisions = []
        self.heads: dict[tuple[UUID, UUID], RelationHead] = {}
        self.versions: dict[UUID, RelationVersion] = {}
        self.obligations: set[tuple[UUID, UUID, str]] = set()

    async def find_receipt(self, *, tx, tenant_id, idempotency_key):
        return self.receipts.get((tenant_id, idempotency_key))

    async def record_candidate_decision(self, *, tx, command, disposition, reason_codes, version):
        self.decisions.append((command.candidate.candidate_digest, disposition, reason_codes, version))
        if version:
            self.versions[version.relation_version_id] = version

    async def insert_initial_head(self, *, tx, head):
        key = (head.tenant_id, head.relation_id)
        if key in self.heads:
            raise AssertionError("duplicate head")
        self.heads[key] = head

    async def lock_head(self, *, tx, tenant_id, relation_id):
        return self.heads.get((tenant_id, relation_id))

    async def load_version(self, *, tx, tenant_id, relation_version_id):
        version = self.versions[relation_version_id]
        assert version.tenant_id == tenant_id
        return version

    async def validate_active_participants(self, *, tx, tenant_id, participants):
        return ()

    async def insert_version(self, *, tx, version):
        assert version.relation_version_id not in self.versions
        self.versions[version.relation_version_id] = version

    async def compare_and_swap_head(self, *, tx, expected, successor):
        key = (expected.tenant_id, expected.relation_id)
        if self.heads.get(key) != expected:
            return False
        self.heads[key] = successor
        return True

    async def insert_receipt(self, *, tx, receipt):
        self.receipts[(receipt.tenant_id, receipt.idempotency_key)] = receipt

    async def dispute_for_invalidated_evidence(self, *, tx, tenant_id, invalidated_model_version_id, cause_code, occurred_at):
        affected = []
        for version in self.versions.values():
            if version.tenant_id != tenant_id or not any(item.model_version_id == invalidated_model_version_id for item in version.evidence):
                continue
            head = self.heads[(tenant_id, version.relation_id)]
            self.heads[(tenant_id, version.relation_id)] = RelationHead(head.tenant_id, head.relation_id, head.relation_version_id, head.version, head.semantic_digest, RelationLifecycle.DISPUTED, occurred_at)
            self.obligations.add((invalidated_model_version_id, version.relation_version_id, cause_code))
            affected.append(version.relation_version_id)
        return tuple(affected)


@pytest.mark.asyncio
async def test_valid_relation_is_admitted_as_immutable_version_and_head():
    storage = MemoryStorage()
    item = candidate()
    receipt = await RelationTruthKernel(storage).admit(tx=object(), command=command(item))
    assert receipt.disposition is RelationDisposition.ACCEPTED
    version = storage.decisions[0][3]
    assert version.participants == item.participants
    assert storage.heads[(TENANT, item.candidate_relation_id)].relation_version_id == version.relation_version_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "roles"),
    [
        (RelationKind.CAUSAL_INFLUENCE, ("cause", "effect")),
        (RelationKind.DEPENDENCY_CONSTRAINT, ("dependent", "prerequisite")),
        (RelationKind.ENABLEMENT, ("enabler", "enabled")),
        (RelationKind.PREDICTIVE_INDICATOR, ("indicator", "outcome")),
    ],
)
async def test_initial_governed_vocabulary_has_exact_typed_roles(kind, roles):
    storage = MemoryStorage()
    item = candidate(proposed_kind=kind.value, roles=roles)
    receipt = await RelationTruthKernel(storage).admit(tx=object(), command=command(item))
    assert receipt.disposition is RelationDisposition.ACCEPTED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("item", "code"),
    [
        (candidate(polarity=-1), "RELATION_INVALID:self-negating"),
        (candidate(reverse=True), "RELATION_INVALID:rationale direction"),
        (candidate(roles=("effect", "cause")), "RELATION_INVALID:rationale direction"),
    ],
)
async def test_invalid_semantics_are_rejected_without_a_head(item, code):
    storage = MemoryStorage()
    receipt = await RelationTruthKernel(storage).admit(tx=object(), command=command(item))
    assert receipt.disposition is RelationDisposition.REJECTED
    assert code in receipt.rejection_code
    assert storage.heads == {}


@pytest.mark.asyncio
async def test_unknown_kind_remains_a_noncanonical_candidate():
    storage = MemoryStorage()
    item = candidate(proposed_kind="co_occurs")
    receipt = await RelationTruthKernel(storage).admit(tx=object(), command=command(item))
    assert receipt.disposition is RelationDisposition.NEEDS_REVIEW
    assert storage.decisions[0][3] is None
    assert not storage.heads


@pytest.mark.asyncio
async def test_inactive_or_mismatched_endpoint_is_rejected():
    storage = MemoryStorage()

    async def invalid(**kwargs):
        return ("MODEL_VERSION_NOT_CURRENT",)

    storage.validate_active_participants = invalid
    receipt = await RelationTruthKernel(storage).admit(tx=object(), command=command(candidate()))
    assert receipt.disposition is RelationDisposition.REJECTED
    assert receipt.rejection_code == "RELATION_ENDPOINT_INVALID:MODEL_VERSION_NOT_CURRENT"
    assert not storage.heads


def test_confidence_is_unique_signed_and_can_decrease():
    support_id = uuid4()
    initial = (evidence(reference_id=support_id, weight=0.8),)
    disputed = initial + (evidence(polarity=-1, weight=0.8),)
    assert evidence_confidence(initial) == 1.0
    assert evidence_confidence(disputed) == 0.5
    assert evidence_confidence(initial + initial) == evidence_confidence(initial)


@pytest.mark.asyncio
async def test_receipt_replay_is_idempotent_and_digest_conflict_is_rejected():
    storage = MemoryStorage()
    kernel = RelationTruthKernel(storage)
    first_command = command(candidate())
    first = await kernel.admit(tx=object(), command=first_command)
    assert await kernel.admit(tx=object(), command=first_command) is first
    conflicting = AdmitRelationCommand(uuid4(), first_command.idempotency_key, candidate(), uuid4(), uuid4(), NOW)
    with pytest.raises(InvariantViolation, match="idempotency key"):
        await kernel.admit(tx=object(), command=conflicting)


@pytest.mark.asyncio
async def test_falsified_relation_evidence_disputes_head_and_opens_one_obligation():
    storage = MemoryStorage()
    kernel = RelationTruthKernel(storage)
    item = candidate()
    await kernel.admit(tx=object(), command=command(item))
    first = await kernel.invalidate_evidence(tx=object(), tenant_id=TENANT, invalidated_model_version_id=LEFT_VERSION, cause_code="MODEL_FALSIFIED", occurred_at=NOW)
    second = await kernel.invalidate_evidence(tx=object(), tenant_id=TENANT, invalidated_model_version_id=LEFT_VERSION, cause_code="MODEL_FALSIFIED", occurred_at=NOW)
    assert first == second
    assert storage.heads[(TENANT, item.candidate_relation_id)].lifecycle is RelationLifecycle.DISPUTED
    assert len(storage.obligations) == 1


@pytest.mark.asyncio
async def test_revision_has_cas_and_does_not_auto_rebind_endpoint_identity():
    storage = MemoryStorage()
    kernel = RelationTruthKernel(storage)
    item = candidate()
    await kernel.admit(tx=object(), command=command(item))
    current = storage.heads[(TENANT, item.candidate_relation_id)]
    prior = storage.versions[current.relation_version_id]
    replacement_model = uuid4()
    participants = (
        participant("cause", replacement_model, uuid4()),
        prior.participants[1],
    )
    assertion = DirectionAssertion(kind=prior.kind, source_model_version_id=participants[0].model_version_id, target_model_version_id=participants[1].model_version_id, polarity=1)
    digest = RelationVersion.compute_semantic_digest(kind=prior.kind, participants=participants, rationale="Explicit revision", assertion=assertion, evidence=prior.evidence)
    nxt = RelationVersion(relation_version_id=uuid4(), relation_id=prior.relation_id, tenant_id=TENANT, version=2, admission_decision_id=prior.admission_decision_id, kind=prior.kind, participants=participants, rationale="Explicit revision", assertion=assertion, evidence=prior.evidence, supersedes_relation_version_id=prior.relation_version_id, created_at=NOW, semantic_digest=digest)
    advance = AdvanceRelationCommand(uuid4(), TENANT, "advance:1", current, nxt, NOW)
    with pytest.raises(InvariantViolation, match="silently replace endpoint"):
        await kernel.advance(tx=object(), command=advance)
    assert storage.heads[(TENANT, item.candidate_relation_id)] == current
