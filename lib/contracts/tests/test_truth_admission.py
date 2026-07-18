from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.contracts.truth_admission import (
    AdmissionDecision,
    AdmissionDisposition,
    AdmitModelCommand,
    AdvanceModelHeadCommand,
    CandidateReviewState,
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


NOW = datetime(2026, 7, 17, 8, 0, tzinfo=timezone.utc)
TENANT = uuid4()
MODEL_ID = uuid4()
VERSION_ID = uuid4()
EVIDENCE_ID = uuid4()
SUBJECT_ID = uuid4()


def _updated(model, **updates):
    payload = model.model_dump(mode="python")
    payload.update(updates)
    return type(model).model_validate(payload)


def _evidence(
    *, tenant_id: UUID = TENANT, reference_id: UUID = EVIDENCE_ID
) -> TruthEvidenceReference:
    return TruthEvidenceReference(
        reference_id=reference_id,
        tenant_id=tenant_id,
        kind=TruthEvidenceKind.OBSERVATION,
        evidence_id="observation:42",
        evidence_version=2,
        evidence_digest="a" * 64,
        role=TruthEvidenceRole.SUPPORT,
        coordinate=TruthEvidenceCoordinate(
            source_system="slack",
            source_object_id="channel:1:message:42",
            source_revision="revision:2",
            field_path="text",
            span_start=0,
            span_end=18,
        ),
        authority=EvidenceAuthority(
            authority_ref="connector-grant:slack-public",
            policy_version="evidence-policy-v1",
            authority_epoch=3,
            decided_at=NOW - timedelta(days=1),
            expires_at=NOW + timedelta(days=1),
        ),
        occurred_at=NOW - timedelta(hours=2),
        recorded_at=NOW - timedelta(hours=1),
        cutoff_at=NOW,
    )


def _scope(*, evidence_id: UUID = EVIDENCE_ID) -> ClaimScopeBinding:
    return ClaimScopeBinding(
        subject_id=SUBJECT_ID,
        subject_kind=ScopeSubjectKind.PROJECT,
        role=ClaimScopeRole.SUBJECT,
        claim_local_evidence_refs=(evidence_id,),
    )


def _candidate(
    *,
    tenant_id: UUID = TENANT,
    kind: TruthCandidateKind = TruthCandidateKind.ATOMIC_CLAIM,
) -> TruthCandidate:
    return TruthCandidate(
        candidate_id=uuid4(),
        tenant_id=tenant_id,
        kind=kind,
        review_state=CandidateReviewState.PROPOSED,
        natural="Project Atlas is blocked by vendor approval.",
        proposition={"predicate": "blocked_by", "subject": str(SUBJECT_ID)},
        proposed_evidence=(_evidence(tenant_id=tenant_id),),
        proposed_scope=(_scope(),),
        created_at=NOW,
    )


def _decision(candidate: TruthCandidate) -> AdmissionDecision:
    return AdmissionDecision(
        decision_id=uuid4(),
        tenant_id=candidate.tenant_id,
        candidate_id=candidate.candidate_id,
        candidate_version=candidate.candidate_version,
        candidate_digest=candidate.candidate_digest,
        disposition=AdmissionDisposition.ACCEPTED,
        reason_codes=("grounded_atomic_claim",),
        decided_by="TruthAdmissionApplier",
        decided_at=NOW + timedelta(seconds=1),
        admitted_model_id=MODEL_ID,
        admitted_version_id=VERSION_ID,
    )


def _version(
    candidate: TruthCandidate,
    decision: AdmissionDecision,
    *,
    version_id: UUID = VERSION_ID,
    version: int = 1,
    lifecycle: ModelTruthLifecycle = ModelTruthLifecycle.ACTIVE,
) -> ModelVersion:
    evidence = candidate.proposed_evidence
    scope = candidate.proposed_scope
    natural = candidate.natural
    proposition = candidate.proposition
    return ModelVersion(
        version_id=version_id,
        model_id=MODEL_ID,
        version=version,
        tenant_id=candidate.tenant_id,
        admission_decision_id=decision.decision_id,
        source_candidate_id=candidate.candidate_id,
        source_candidate_version=candidate.candidate_version,
        natural=natural,
        proposition=proposition,
        evidence=evidence,
        scope=scope,
        lifecycle=lifecycle,
        created_at=NOW + timedelta(seconds=2),
        semantic_digest=ModelVersion.compute_semantic_digest(
            proposition=proposition,
            natural=natural,
            evidence=evidence,
            scope=scope,
        ),
    )


def _command(
    *, kind: TruthCandidateKind = TruthCandidateKind.ATOMIC_CLAIM
) -> AdmitModelCommand:
    candidate = _candidate(kind=kind)
    decision = _decision(candidate)
    return AdmitModelCommand(
        command_id=uuid4(),
        idempotency_key="admit:atlas:v1",
        tenant_id=TENANT,
        candidate=candidate,
        decision=decision,
        version=_version(candidate, decision),
        issued_at=NOW + timedelta(seconds=3),
    )


def test_contracts_are_frozen_and_forbid_unknown_fields() -> None:
    candidate = _candidate()
    with pytest.raises(ValidationError):
        candidate.natural = "changed"
    with pytest.raises(ValidationError, match="extra_forbidden"):
        TruthCandidate.model_validate(
            {**candidate.model_dump(mode="python"), "retrieval_count": 1}
        )


@pytest.mark.parametrize(
    "kind",
    [TruthCandidateKind.ATOMIC_CLAIM, TruthCandidateKind.SYNTHESIS],
)
def test_admissible_candidate_kinds_create_bound_version(kind) -> None:
    command = _command(kind=kind)
    assert command.version.source_candidate_id == command.candidate.candidate_id
    assert command.version.admission_decision_id == command.decision.decision_id
    assert len(command.request_digest) == 64


@pytest.mark.parametrize(
    "kind",
    [
        TruthCandidateKind.BATCH_ENVELOPE,
        TruthCandidateKind.CONTROL_LANGUAGE,
        TruthCandidateKind.PROCESSING_WRAPPER,
    ],
)
def test_wrapper_and_control_candidates_cannot_be_admitted(kind) -> None:
    with pytest.raises(ValidationError, match="cannot become truth"):
        _command(kind=kind)


@pytest.mark.parametrize(
    "disposition",
    [AdmissionDisposition.REJECTED, AdmissionDisposition.NEEDS_REVIEW],
)
def test_nonaccepted_decision_cannot_name_canonical_truth(disposition) -> None:
    candidate = _candidate()
    with pytest.raises(ValidationError, match="nonaccepted"):
        AdmissionDecision(
            **{
                **_decision(candidate).model_dump(mode="python"),
                "disposition": disposition,
            }
        )


def test_accepted_decision_requires_both_model_and_version_ids() -> None:
    candidate = _candidate()
    payload = _decision(candidate).model_dump(mode="python")
    payload["admitted_version_id"] = None
    with pytest.raises(ValidationError, match="must identify"):
        AdmissionDecision(**payload)


@pytest.mark.parametrize(
    "mutator, message",
    [
        (lambda c, d, v: (c, _updated(d, candidate_digest="b" * 64), v), "exact candidate"),
        (lambda c, d, v: (c, d, _updated(v, source_candidate_id=uuid4())), "admission decision"),
        (lambda c, d, v: (c, d, _updated(v, admission_decision_id=uuid4())), "admission decision"),
    ],
)
def test_admission_bundle_rejects_cross_object_mismatches(mutator, message) -> None:
    valid = _command()
    candidate, decision, version = mutator(
        valid.candidate, valid.decision, valid.version
    )
    with pytest.raises(ValidationError, match=message):
        _updated(valid, candidate=candidate, decision=decision, version=version)


def test_admission_bundle_rejects_cross_tenant_evidence() -> None:
    candidate = _candidate()
    foreign = _evidence(tenant_id=uuid4())
    payload = candidate.model_dump(mode="python")
    payload["proposed_evidence"] = (foreign,)
    payload["proposed_scope"] = ()
    with pytest.raises(ValidationError, match="evidence tenant"):
        TruthCandidate(**payload)


def test_admission_cannot_change_candidate_semantics_while_creating_truth() -> None:
    valid = _command()
    changed_natural = "Project Atlas is not blocked."
    changed = ModelVersion(
        **{
            **valid.version.model_dump(mode="python"),
            "natural": changed_natural,
            "semantic_digest": ModelVersion.compute_semantic_digest(
                proposition=valid.version.proposition,
                natural=changed_natural,
                evidence=valid.version.evidence,
                scope=valid.version.scope,
            ),
        }
    )
    with pytest.raises(ValidationError, match="exact candidate"):
        _updated(valid, version=changed)


def test_admission_chronology_is_monotone() -> None:
    valid = _command()
    with pytest.raises(ValidationError, match="precede its candidate"):
        _updated(
            valid,
            decision=_updated(
                valid.decision, decided_at=valid.candidate.created_at - timedelta(seconds=1)
            ),
        )


def test_semantic_digest_is_deterministic_and_binds_all_semantics() -> None:
    command = _command()
    version = command.version
    assert version.semantic_digest == ModelVersion.compute_semantic_digest(
        proposition=dict(reversed(list(version.proposition.items()))),
        natural=version.natural,
        evidence=version.evidence,
        scope=version.scope,
    )
    for change in (
        {"natural": "Project Atlas is delayed."},
        {"proposition": {"predicate": "delayed", "subject": str(SUBJECT_ID)}},
        {
            "evidence": (
                _updated(
                    version.evidence[0], role=TruthEvidenceRole.COUNTEREVIDENCE
                ),
            )
        },
        {"scope": ()},
    ):
        payload = version.model_dump(mode="python")
        payload.update(change)
        with pytest.raises(ValidationError, match="semantic digest"):
            ModelVersion(**payload)


def test_legacy_scoped_digest_reconstructs_without_optional_provenance() -> None:
    version = _command().version
    legacy_scope = tuple(
        item.model_copy(
            update={
                "canonical_ref": None,
                "display_label": None,
                "canonical_ref_status": None,
                "normalization_version": None,
            }
        )
        for item in version.scope
    )
    legacy_digest = canonical_sha256(
        {
            "proposition": version.proposition,
            "natural": version.natural,
            "evidence": [
                item.model_dump(mode="json") for item in version.evidence
            ],
            "scope": [
                {
                    "subject_id": str(item.subject_id),
                    "subject_kind": item.subject_kind.value,
                    "role": item.role.value,
                    "claim_local_evidence_refs": [
                        str(ref) for ref in item.claim_local_evidence_refs
                    ],
                }
                for item in legacy_scope
            ],
        }
    )

    reconstructed = ModelVersion(
        **{
            **version.model_dump(mode="python"),
            "scope": legacy_scope,
            "semantic_digest": legacy_digest,
        }
    )
    assert reconstructed.semantic_digest == legacy_digest


def test_initial_admission_requires_active_version_one() -> None:
    valid = _command()
    for change, message in (
        ({"version": 2}, "version 1"),
        ({"lifecycle": ModelTruthLifecycle.DISPUTED}, "start active"),
    ):
        version = _version(
            valid.candidate,
            valid.decision,
            version=change.get("version", 1),
            lifecycle=change.get("lifecycle", ModelTruthLifecycle.ACTIVE),
        )
        with pytest.raises(ValidationError, match=message):
            _updated(valid, version=version)


def test_head_command_is_exact_cas_and_one_version_advance() -> None:
    admitted = _command()
    next_version = _version(
        admitted.candidate,
        admitted.decision,
        version_id=uuid4(),
        version=2,
        lifecycle=ModelTruthLifecycle.DISPUTED,
    )
    expectation = ModelHeadExpectation(
        tenant_id=TENANT,
        model_id=MODEL_ID,
        expected_version_id=VERSION_ID,
        expected_version=1,
        expected_semantic_digest=admitted.version.semantic_digest,
        expected_lifecycle=ModelTruthLifecycle.ACTIVE,
    )
    command = AdvanceModelHeadCommand(
        command_id=uuid4(),
        idempotency_key="contest:atlas:v1",
        tenant_id=TENANT,
        expectation=expectation,
        next_version=next_version,
        transition=ModelTruthTransition.CONTEST,
        reason_codes=("counterevidence",),
        issued_at=NOW + timedelta(minutes=1),
    )
    assert command.next_version.version == 2
    assert len(command.request_digest) == 64


def test_head_command_binds_transition_and_unique_reasons_to_lifecycle() -> None:
    admitted = _command()
    expectation = ModelHeadExpectation(
        tenant_id=TENANT,
        model_id=MODEL_ID,
        expected_version_id=VERSION_ID,
        expected_version=1,
        expected_semantic_digest=admitted.version.semantic_digest,
        expected_lifecycle=ModelTruthLifecycle.ACTIVE,
    )
    disputed = _version(
        admitted.candidate,
        admitted.decision,
        version_id=uuid4(),
        version=2,
        lifecycle=ModelTruthLifecycle.DISPUTED,
    )
    base = {
        "command_id": uuid4(),
        "idempotency_key": "contest:reasons",
        "tenant_id": TENANT,
        "expectation": expectation,
        "next_version": disputed,
        "issued_at": NOW + timedelta(minutes=1),
    }
    with pytest.raises(ValidationError, match="does not match"):
        AdvanceModelHeadCommand(
            **base,
            transition=ModelTruthTransition.FALSIFY,
            reason_codes=("counterevidence",),
        )
    with pytest.raises(ValidationError, match="at least 1"):
        AdvanceModelHeadCommand(
            **base,
            transition=ModelTruthTransition.CONTEST,
            reason_codes=(),
        )
    with pytest.raises(ValidationError, match="must be unique"):
        AdvanceModelHeadCommand(
            **base,
            transition=ModelTruthTransition.CONTEST,
            reason_codes=("counterevidence", "counterevidence"),
        )


@pytest.mark.parametrize(
    "expected_lifecycle",
    [
        ModelTruthLifecycle.FALSIFIED,
        ModelTruthLifecycle.SUPERSEDED,
        ModelTruthLifecycle.ARCHIVED,
    ],
)
def test_terminal_head_cannot_advance_or_resurrect(expected_lifecycle) -> None:
    valid = _command()
    next_version = _version(
        valid.candidate, valid.decision, version_id=uuid4(), version=2
    )
    expectation = ModelHeadExpectation(
        tenant_id=TENANT,
        model_id=MODEL_ID,
        expected_version_id=VERSION_ID,
        expected_version=1,
        expected_semantic_digest=valid.version.semantic_digest,
        expected_lifecycle=expected_lifecycle,
    )
    with pytest.raises(ValidationError, match="terminal"):
        AdvanceModelHeadCommand(
            command_id=uuid4(),
            idempotency_key="illegal-resurrection",
            tenant_id=TENANT,
            expectation=expectation,
            next_version=next_version,
            transition=ModelTruthTransition.CONFIRM,
            reason_codes=("illegal_resurrection",),
            issued_at=NOW,
        )


def test_head_command_rejects_skipped_version_and_cross_tenant() -> None:
    valid = _command()
    expectation = ModelHeadExpectation(
        tenant_id=TENANT,
        model_id=MODEL_ID,
        expected_version_id=VERSION_ID,
        expected_version=1,
        expected_semantic_digest=valid.version.semantic_digest,
        expected_lifecycle=ModelTruthLifecycle.ACTIVE,
    )
    skipped = _version(
        valid.candidate, valid.decision, version_id=uuid4(), version=3
    )
    with pytest.raises(ValidationError, match="exactly one"):
        AdvanceModelHeadCommand(
            command_id=uuid4(),
            idempotency_key="skip",
            tenant_id=TENANT,
            expectation=expectation,
            next_version=skipped,
            transition=ModelTruthTransition.CONFIRM,
            reason_codes=("skip",),
            issued_at=NOW,
        )
    with pytest.raises(ValidationError, match="tenant"):
        AdvanceModelHeadCommand(
            command_id=uuid4(),
            idempotency_key="cross-tenant",
            tenant_id=uuid4(),
            expectation=expectation,
            next_version=_version(
                valid.candidate, valid.decision, version_id=uuid4(), version=2
            ),
            transition=ModelTruthTransition.CONFIRM,
            reason_codes=("cross_tenant",),
            issued_at=NOW,
        )
