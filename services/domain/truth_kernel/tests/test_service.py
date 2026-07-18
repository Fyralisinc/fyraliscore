from __future__ import annotations

import copy
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.contracts.truth_admission import (
    AdmissionDecision,
    AdmissionDisposition,
    AdmitModelCommand,
    AdvanceModelHeadCommand,
    CandidateReviewState,
    ModelHead,
    ModelHeadExpectation,
    ModelTruthLifecycle,
    ModelTruthTransition,
    ModelVersion,
    TruthCandidate,
    TruthCandidateKind,
)
from lib.contracts.truth_evidence import (
    ClaimScopeBinding,
    ClaimScopeRole,
    EvidenceAuthority,
    ScopeSubjectKind,
    TruthEvidenceCoordinate,
    TruthEvidenceKind,
    TruthEvidenceReference,
    TruthEvidenceRole,
)
from lib.shared.errors import InvariantViolation
from services.domain.truth_kernel.service import (
    FenceContext,
    TruthCommandReceipt,
    TruthKernelService,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
TENANT = UUID("00000000-0000-0000-0000-000000000101")


def evidence() -> TruthEvidenceReference:
    return TruthEvidenceReference(
        reference_id=uuid4(),
        tenant_id=TENANT,
        kind=TruthEvidenceKind.OBSERVATION,
        evidence_id="observation-1",
        evidence_version=1,
        evidence_digest="a" * 64,
        role=TruthEvidenceRole.SUPPORT,
        coordinate=TruthEvidenceCoordinate(
            source_system="slack",
            source_object_id="channel-1/message-4",
            source_revision="1",
            span_start=0,
            span_end=12,
        ),
        authority=EvidenceAuthority(
            authority_ref="grant-1",
            policy_version="1",
            authority_epoch=1,
            decided_at=NOW - timedelta(days=1),
        ),
        occurred_at=NOW - timedelta(hours=2),
        recorded_at=NOW - timedelta(hours=1),
        cutoff_at=NOW,
    )


def admission() -> AdmitModelCommand:
    item = evidence()
    model_id, version_id, candidate_id, decision_id = uuid4(), uuid4(), uuid4(), uuid4()
    scope = (
        ClaimScopeBinding(
            subject_id=uuid4(),
            subject_kind=ScopeSubjectKind.PROJECT,
            role=ClaimScopeRole.SUBJECT,
            claim_local_evidence_refs=(item.reference_id,),
        ),
    )
    candidate = TruthCandidate(
        candidate_id=candidate_id,
        tenant_id=TENANT,
        kind=TruthCandidateKind.ATOMIC_CLAIM,
        review_state=CandidateReviewState.PROPOSED,
        natural="Project Ember is blocked by legal review.",
        proposition={
            "subject": "Project Ember",
            "predicate": "blocked_by",
            "object": "legal review",
        },
        proposed_evidence=(item,),
        proposed_scope=scope,
        created_at=NOW,
    )
    decision = AdmissionDecision(
        decision_id=decision_id,
        tenant_id=TENANT,
        candidate_id=candidate_id,
        candidate_version=1,
        candidate_digest=candidate.candidate_digest,
        disposition=AdmissionDisposition.ACCEPTED,
        reason_codes=("claim_local_evidence",),
        decided_by="test-policy-v1",
        decided_at=NOW + timedelta(seconds=1),
        admitted_model_id=model_id,
        admitted_version_id=version_id,
    )
    digest = ModelVersion.compute_semantic_digest(
        proposition=candidate.proposition,
        natural=candidate.natural,
        evidence=candidate.proposed_evidence,
        scope=candidate.proposed_scope,
    )
    version = ModelVersion(
        version_id=version_id,
        model_id=model_id,
        version=1,
        tenant_id=TENANT,
        admission_decision_id=decision_id,
        source_candidate_id=candidate_id,
        source_candidate_version=1,
        natural=candidate.natural,
        proposition=candidate.proposition,
        evidence=candidate.proposed_evidence,
        scope=candidate.proposed_scope,
        created_at=NOW + timedelta(seconds=2),
        semantic_digest=digest,
    )
    return AdmitModelCommand(
        command_id=uuid4(),
        idempotency_key=f"admit:{candidate_id}",
        tenant_id=TENANT,
        candidate=candidate,
        decision=decision,
        version=version,
        issued_at=NOW + timedelta(seconds=3),
    )


def advance(head: ModelHead, lifecycle: ModelTruthLifecycle) -> AdvanceModelHeadCommand:
    prior = STORE.versions[head.version_id]
    version_id = uuid4()
    semantic_digest = ModelVersion.compute_semantic_digest(
        proposition=prior.proposition,
        natural=prior.natural,
        evidence=prior.evidence,
        scope=prior.scope,
    )
    successor = ModelVersion(
        version_id=version_id,
        model_id=head.model_id,
        version=head.version + 1,
        tenant_id=head.tenant_id,
        admission_decision_id=prior.admission_decision_id,
        source_candidate_id=prior.source_candidate_id,
        source_candidate_version=prior.source_candidate_version,
        natural=prior.natural,
        proposition=prior.proposition,
        evidence=prior.evidence,
        scope=prior.scope,
        lifecycle=lifecycle,
        created_at=NOW + timedelta(minutes=head.version),
        semantic_digest=semantic_digest,
    )
    return AdvanceModelHeadCommand(
        command_id=uuid4(),
        idempotency_key=f"advance:{head.model_id}:{head.version + 1}:{version_id}",
        tenant_id=head.tenant_id,
        expectation=ModelHeadExpectation(
            tenant_id=head.tenant_id,
            model_id=head.model_id,
            expected_version_id=head.version_id,
            expected_version=head.version,
            expected_semantic_digest=head.semantic_digest,
            expected_lifecycle=head.lifecycle,
        ),
        next_version=successor,
        transition={
            ModelTruthLifecycle.ACTIVE: ModelTruthTransition.CONFIRM,
            ModelTruthLifecycle.DISPUTED: ModelTruthTransition.CONTEST,
            ModelTruthLifecycle.FALSIFIED: ModelTruthTransition.FALSIFY,
            ModelTruthLifecycle.SUPERSEDED: ModelTruthTransition.SUPERSEDE,
            ModelTruthLifecycle.ARCHIVED: ModelTruthTransition.ARCHIVE,
        }[lifecycle],
        reason_codes=("test_evidence_review",),
        issued_at=NOW + timedelta(minutes=head.version, seconds=1),
    )


class MemoryStorage:
    def __init__(self) -> None:
        self.candidates = {}
        self.decisions = {}
        self.versions = {}
        self.heads = {}
        self.events = []
        self.receipts: dict[tuple[UUID, str], TruthCommandReceipt] = {}
        self.absorptions = []

    @asynccontextmanager
    async def transaction(self):
        before = copy.deepcopy(self.__dict__)
        try:
            yield self
        except Exception:
            self.__dict__.clear()
            self.__dict__.update(before)
            raise

    async def find_receipt(self, *, tx, tenant_id, idempotency_key):
        return self.receipts.get((tenant_id, idempotency_key))

    async def lock_semantic_admission(self, *, tx, tenant_id, semantic_digest):
        return None

    async def find_active_semantic_head(self, *, tx, tenant_id, semantic_digest):
        return next(
            (
                head
                for head in self.heads.values()
                if head.tenant_id == tenant_id
                and head.semantic_digest == semantic_digest
                and head.lifecycle is ModelTruthLifecycle.ACTIVE
            ),
            None,
        )

    async def insert_admission_bundle(self, *, tx, command):
        self.candidates[command.candidate.candidate_id] = command.candidate
        self.decisions[command.decision.decision_id] = command.decision
        self.versions[command.version.version_id] = command.version

    async def lock_head(self, *, tx, tenant_id, model_id):
        return self.heads.get((tenant_id, model_id))

    async def insert_version(self, *, tx, version, prior_head):
        self.versions[version.version_id] = version

    async def insert_initial_head(self, *, tx, head):
        assert (head.tenant_id, head.model_id) not in self.heads
        self.heads[(head.tenant_id, head.model_id)] = head

    async def compare_and_swap_head(self, *, tx, expected, successor):
        key = (expected.tenant_id, expected.model_id)
        if self.heads.get(key) != expected:
            return False
        self.heads[key] = successor
        return True

    async def append_event(self, **values):
        self.events.append(values)

    async def insert_receipt(self, *, tx, receipt):
        self.receipts[(receipt.tenant_id, receipt.idempotency_key)] = receipt

    async def insert_semantic_absorption(self, *, tx, command, receipt):
        self.absorptions.append((command, receipt))


class RecordingFence:
    def __init__(self, name: str, calls: list[str], *, fail: bool = False) -> None:
        self.name, self.calls, self.fail = name, calls, fail

    async def apply(self, *, tx, context: FenceContext) -> None:
        self.calls.append(self.name)
        if self.fail:
            raise RuntimeError(f"injected fence failure: {self.name}")


STORE: MemoryStorage


@pytest.fixture(autouse=True)
def store() -> None:
    global STORE
    STORE = MemoryStorage()


@pytest.mark.asyncio
async def test_admission_persists_exact_version_head_event_and_idempotent_receipt(
) -> None:
    command = admission()
    service = TruthKernelService(storage=STORE)
    async with STORE.transaction() as tx:
        first = await service.admit(tx=tx, command=command)
    async with STORE.transaction() as tx:
        replay = await service.admit(tx=tx, command=command)

    assert replay == first
    assert len(STORE.candidates) == len(STORE.decisions) == len(STORE.versions) == 1
    assert len(STORE.heads) == len(STORE.events) == len(STORE.receipts) == 1
    assert first.semantic_digest == command.version.semantic_digest


@pytest.mark.asyncio
async def test_admission_rejects_batch_wrapper_even_if_quality_gate_is_bypassed(
) -> None:
    command = admission()
    proposition = {
        "kind": "belief", "about": "batch", "abstraction_level": "atomic",
        "claim_role": "hypothesis", "domain_tags": ["generic_curiosity"],
    }
    natural = "The event batch may contain a broader recurring pattern."
    candidate = command.candidate.model_copy(update={
        "proposition": proposition, "natural": natural,
    })
    decision = command.decision.model_copy(update={
        "candidate_digest": candidate.candidate_digest,
    })
    version = command.version.model_copy(update={
        "proposition": proposition, "natural": natural,
        "semantic_digest": ModelVersion.compute_semantic_digest(
            proposition=proposition, natural=natural,
            evidence=command.version.evidence, scope=command.version.scope,
        ),
    })
    forged = command.model_copy(update={
        "candidate": candidate, "decision": decision, "version": version,
    })
    service = TruthKernelService(storage=STORE)
    with pytest.raises(InvariantViolation, match="coherent business scope"):
        async with STORE.transaction() as tx:
            await service.admit(tx=tx, command=forged)
    assert STORE.versions == {}


@pytest.mark.asyncio
async def test_independently_keyed_exact_semantic_duplicate_is_absorbed() -> None:
    command = admission()
    service = TruthKernelService(storage=STORE)
    async with STORE.transaction() as tx:
        first = await service.admit(tx=tx, command=command)

    receipts = []
    for ordinal in range(10):
        duplicate = command.model_copy(
            update={
                "command_id": uuid4(),
                "idempotency_key": f"semantic-duplicate:{ordinal}",
                "issued_at": command.issued_at + timedelta(seconds=ordinal + 1),
            }
        )
        async with STORE.transaction() as tx:
            receipts.append(await service.admit(tx=tx, command=duplicate))

    assert all(item.outcome == "absorbed_duplicate" for item in receipts)
    assert all(item.version_id == first.version_id for item in receipts)
    assert len(STORE.versions) == len(STORE.heads) == 1
    assert len(STORE.receipts) == 11
    assert len(STORE.absorptions) == 10
    assert all(
        command.request_digest == receipt.request_digest
        and command.version.semantic_digest == receipt.semantic_digest
        for command, receipt in STORE.absorptions
    )


@pytest.mark.asyncio
async def test_same_text_with_distinct_evidence_is_not_absorbed() -> None:
    service = TruthKernelService(storage=STORE)
    first_command = admission()
    second_command = admission()
    assert first_command.version.natural == second_command.version.natural
    assert first_command.version.semantic_digest != second_command.version.semantic_digest

    async with STORE.transaction() as tx:
        first = await service.admit(tx=tx, command=first_command)
        second = await service.admit(tx=tx, command=second_command)

    assert first.outcome == second.outcome == "applied"
    assert first.version_id != second.version_id
    assert len(STORE.versions) == len(STORE.heads) == 2


def test_semantic_identity_distinguishes_evidence_and_scope() -> None:
    command = admission()
    version = command.version
    changed_evidence = (
        version.evidence[0].model_copy(update={"evidence_id": "different-signal"}),
    )
    changed_scope = (
        version.scope[0].model_copy(update={"subject_id": uuid4()}),
    )
    evidence_digest = ModelVersion.compute_semantic_digest(
        proposition=version.proposition,
        natural=version.natural,
        evidence=changed_evidence,
        scope=version.scope,
    )
    scope_digest = ModelVersion.compute_semantic_digest(
        proposition=version.proposition,
        natural=version.natural,
        evidence=version.evidence,
        scope=changed_scope,
    )
    assert evidence_digest != version.semantic_digest
    assert scope_digest != version.semantic_digest


@pytest.mark.asyncio
async def test_same_idempotency_key_with_changed_request_is_rejected() -> None:
    command = admission()
    service = TruthKernelService(storage=STORE)
    async with STORE.transaction() as tx:
        await service.admit(tx=tx, command=command)
    changed = command.model_copy(
        update={
            "command_id": uuid4(),
            "issued_at": command.issued_at + timedelta(seconds=1),
        }
    )
    with pytest.raises(InvariantViolation, match="different truth command"):
        async with STORE.transaction() as tx:
            await service.admit(tx=tx, command=changed)


@pytest.mark.asyncio
async def test_lifecycle_uses_exact_head_cas_and_one_winner() -> None:
    service = TruthKernelService(storage=STORE)
    initial = admission()
    async with STORE.transaction() as tx:
        await service.admit(tx=tx, command=initial)
    head = next(iter(STORE.heads.values()))
    winner = advance(head, ModelTruthLifecycle.DISPUTED)
    loser = advance(head, ModelTruthLifecycle.FALSIFIED)
    async with STORE.transaction() as tx:
        await service.advance(tx=tx, command=winner)
    with pytest.raises(InvariantViolation, match="exact current Model head"):
        async with STORE.transaction() as tx:
            await service.advance(tx=tx, command=loser)
    assert next(iter(STORE.heads.values())).lifecycle is ModelTruthLifecycle.DISPUTED
    assert len(STORE.events) == 2


@pytest.mark.asyncio
async def test_terminal_head_cannot_be_resurrected_even_with_forged_active_expectation(
) -> None:
    service = TruthKernelService(storage=STORE)
    initial = admission()
    async with STORE.transaction() as tx:
        await service.admit(tx=tx, command=initial)
    head = next(iter(STORE.heads.values()))
    terminal = advance(head, ModelTruthLifecycle.FALSIFIED)
    async with STORE.transaction() as tx:
        await service.advance(tx=tx, command=terminal)
    terminal_head = next(iter(STORE.heads.values()))
    # The public contract refuses a terminal expectation, so use a stale active
    # expectation to prove storage truth, not caller assertions, is authoritative.
    forged = advance(head, ModelTruthLifecycle.ACTIVE)
    with pytest.raises(InvariantViolation, match="terminal Model truth"):
        async with STORE.transaction() as tx:
            await service.advance(tx=tx, command=forged)
    assert next(iter(STORE.heads.values())) == terminal_head


@pytest.mark.asyncio
async def test_failure_in_third_fence_rolls_back_version_fences_head_event_and_receipt(
) -> None:
    calls: list[str] = []
    fences = [RecordingFence(f"fence-{i}", calls, fail=i == 3) for i in range(1, 6)]
    service = TruthKernelService(storage=STORE, fences=fences)
    initial = admission()
    async with STORE.transaction() as tx:
        await service.admit(tx=tx, command=initial)
    before = copy.deepcopy(STORE.__dict__)
    head = next(iter(STORE.heads.values()))

    with pytest.raises(RuntimeError, match="fence-3"):
        async with STORE.transaction() as tx:
            await service.advance(
                tx=tx, command=advance(head, ModelTruthLifecycle.SUPERSEDED)
            )

    assert calls == ["fence-1", "fence-2", "fence-3"]
    assert STORE.candidates == before["candidates"]
    assert STORE.decisions == before["decisions"]
    assert STORE.versions == before["versions"]
    assert STORE.heads == before["heads"]
    assert len(STORE.events) == len(before["events"])
    assert STORE.receipts == before["receipts"]


@pytest.mark.asyncio
async def test_all_fences_finish_before_head_becomes_visible() -> None:
    calls: list[str] = []
    fences = [RecordingFence(f"fence-{i}", calls) for i in range(1, 6)]
    service = TruthKernelService(storage=STORE, fences=fences)
    initial = admission()
    async with STORE.transaction() as tx:
        await service.admit(tx=tx, command=initial)
    head = next(iter(STORE.heads.values()))
    async with STORE.transaction() as tx:
        receipt = await service.advance(
            tx=tx, command=advance(head, ModelTruthLifecycle.ARCHIVED)
        )
    assert calls == [f"fence-{i}" for i in range(1, 6)]
    assert receipt.lifecycle is ModelTruthLifecycle.ARCHIVED
    assert next(iter(STORE.heads.values())).version == 2
