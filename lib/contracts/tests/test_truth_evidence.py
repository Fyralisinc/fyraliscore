from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.truth_evidence import (
    ClaimScopeBinding,
    ClaimScopeRole,
    EvidenceAuthority,
    ScopeSubjectKind,
    TruthEvidenceCoordinate,
    TruthEvidenceKind,
    TruthEvidenceReference,
    TruthEvidenceRole,
    validate_claim_local_scope,
)


NOW = datetime(2026, 7, 17, 8, 0, tzinfo=timezone.utc)
TENANT = uuid4()


def _authority(**updates) -> EvidenceAuthority:
    values = {
        "authority_ref": "grant:1",
        "policy_version": "policy:1",
        "authority_epoch": 1,
        "decided_at": NOW - timedelta(days=1),
        "expires_at": NOW + timedelta(days=1),
    }
    values.update(updates)
    return EvidenceAuthority(**values)


def _reference(**updates) -> TruthEvidenceReference:
    values = {
        "reference_id": uuid4(),
        "tenant_id": TENANT,
        "kind": TruthEvidenceKind.OBSERVATION,
        "evidence_id": "observation:1",
        "evidence_version": 1,
        "evidence_digest": "a" * 64,
        "role": TruthEvidenceRole.SUPPORT,
        "coordinate": TruthEvidenceCoordinate(
            source_system="jira",
            source_object_id="PROJ-42",
            source_revision="revision:7",
            field_path="description",
            span_start=4,
            span_end=11,
        ),
        "authority": _authority(),
        "occurred_at": NOW - timedelta(hours=2),
        "recorded_at": NOW - timedelta(hours=1),
        "cutoff_at": NOW,
    }
    values.update(updates)
    return TruthEvidenceReference(**values)


@pytest.mark.parametrize("kind", list(TruthEvidenceKind))
@pytest.mark.parametrize("role", list(TruthEvidenceRole))
def test_every_evidence_kind_and_role_is_typed(kind, role) -> None:
    reference = _reference(kind=kind, role=role)
    assert reference.kind is kind
    assert reference.role is role
    assert len(reference.reference_digest) == 64


def test_reference_is_frozen_and_requires_exact_digest() -> None:
    reference = _reference()
    with pytest.raises(ValidationError):
        reference.role = TruthEvidenceRole.CONTEXT
    with pytest.raises(ValidationError, match="string_pattern_mismatch"):
        _reference(evidence_digest="not-a-digest")


@pytest.mark.parametrize(
    "updates, message",
    [
        ({"occurred_at": NOW + timedelta(seconds=1)}, "before it occurred"),
        ({"recorded_at": NOW + timedelta(seconds=1)}, "future evidence"),
        (
            {
                "authority": _authority(
                    expires_at=NOW - timedelta(microseconds=1)
                )
            },
            "not live",
        ),
    ],
)
def test_future_or_unauthorized_evidence_is_rejected(updates, message) -> None:
    with pytest.raises(ValidationError, match=message):
        _reference(**updates)


@pytest.mark.parametrize("field", ["occurred_at", "recorded_at", "cutoff_at"])
def test_reference_times_require_timezone(field) -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _reference(**{field: NOW.replace(tzinfo=None)})


def test_coordinate_requires_complete_valid_span_and_time_range() -> None:
    base = {
        "source_system": "slack",
        "source_object_id": "message:1",
        "source_revision": "revision:1",
    }
    with pytest.raises(ValidationError, match="both span"):
        TruthEvidenceCoordinate(**base, span_start=0)
    with pytest.raises(ValidationError, match="after span_start"):
        TruthEvidenceCoordinate(**base, span_start=4, span_end=4)
    with pytest.raises(ValidationError, match="both time"):
        TruthEvidenceCoordinate(**base, time_range_start=NOW)
    with pytest.raises(ValidationError, match="follow start"):
        TruthEvidenceCoordinate(
            **base, time_range_start=NOW, time_range_end=NOW
        )


def test_scope_requires_sorted_unique_claim_local_evidence() -> None:
    first, second = sorted((uuid4(), uuid4()), key=str)
    binding = ClaimScopeBinding(
        subject_id=uuid4(),
        subject_kind=ScopeSubjectKind.PROJECT,
        role=ClaimScopeRole.SUBJECT,
        claim_local_evidence_refs=(first, second),
    )
    assert binding.claim_local_evidence_refs == (first, second)
    with pytest.raises(ValidationError, match="sorted"):
        ClaimScopeBinding(
            **{
                **binding.model_dump(mode="python"),
                "claim_local_evidence_refs": (second, first),
            }
        )
    with pytest.raises(ValidationError, match="unique"):
        ClaimScopeBinding(
            **{
                **binding.model_dump(mode="python"),
                "claim_local_evidence_refs": (first, first),
            }
        )


def test_scope_must_cite_evidence_on_the_exact_claim() -> None:
    evidence = _reference()
    binding = ClaimScopeBinding(
        subject_id=uuid4(),
        subject_kind=ScopeSubjectKind.PROJECT,
        role=ClaimScopeRole.SUBJECT,
        claim_local_evidence_refs=(uuid4(),),
    )
    with pytest.raises(ValueError, match="outside this claim"):
        validate_claim_local_scope(
            evidence=(evidence,), scope=(binding,), tenant_id=TENANT
        )


def test_same_subject_cannot_have_conflicting_entity_types() -> None:
    evidence = _reference()
    subject = uuid4()
    bindings = (
        ClaimScopeBinding(
            subject_id=subject,
            subject_kind=ScopeSubjectKind.PROJECT,
            role=ClaimScopeRole.SUBJECT,
            claim_local_evidence_refs=(evidence.reference_id,),
        ),
        ClaimScopeBinding(
            subject_id=subject,
            subject_kind=ScopeSubjectKind.PERSON,
            role=ClaimScopeRole.ACTOR,
            claim_local_evidence_refs=(evidence.reference_id,),
        ),
    )
    with pytest.raises(ValueError, match="conflicting entity types"):
        validate_claim_local_scope(
            evidence=(evidence,), scope=bindings, tenant_id=TENANT
        )


def test_duplicate_subject_role_and_evidence_ids_are_rejected() -> None:
    evidence = _reference()
    binding = ClaimScopeBinding(
        subject_id=uuid4(),
        subject_kind=ScopeSubjectKind.PROJECT,
        role=ClaimScopeRole.SUBJECT,
        claim_local_evidence_refs=(evidence.reference_id,),
    )
    with pytest.raises(ValueError, match="duplicate subject-role"):
        validate_claim_local_scope(
            evidence=(evidence,), scope=(binding, binding), tenant_id=TENANT
        )
    with pytest.raises(ValueError, match="IDs must be unique"):
        validate_claim_local_scope(
            evidence=(evidence, evidence), scope=(), tenant_id=TENANT
        )


def test_cross_tenant_evidence_is_rejected() -> None:
    with pytest.raises(ValueError, match="tenant"):
        validate_claim_local_scope(
            evidence=(_reference(),), scope=(), tenant_id=uuid4()
        )
