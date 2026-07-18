from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.truth_evidence import (
    ClaimScopeBinding,
    ClaimScopeRole,
    ScopeSubjectKind,
)


def _binding(*, kind: ScopeSubjectKind, canonical_ref: str) -> ClaimScopeBinding:
    return ClaimScopeBinding(
        subject_id=uuid4(),
        subject_kind=kind,
        role=ClaimScopeRole.SUBJECT,
        canonical_ref=canonical_ref,
        claim_local_evidence_refs=(uuid4(),),
    )


def test_unresolved_typed_scope_coordinate_is_valid_provenance() -> None:
    binding = _binding(
        kind=ScopeSubjectKind.WORK_ITEM,
        canonical_ref="commitment:cobalt-renewal",
    )
    assert binding.canonical_ref == "commitment:cobalt-renewal"


@pytest.mark.parametrize(
    "canonical_ref",
    ["batch", "batch:mixed-signals", "commitment"],
)
def test_batch_or_untyped_scope_coordinate_is_rejected(canonical_ref: str) -> None:
    with pytest.raises(ValidationError):
        _binding(kind=ScopeSubjectKind.WORK_ITEM, canonical_ref=canonical_ref)


def test_scope_coordinate_type_must_match_subject_kind() -> None:
    with pytest.raises(ValidationError, match="agree with subject_kind"):
        _binding(
            kind=ScopeSubjectKind.PROJECT,
            canonical_ref="commitment:cobalt-renewal",
        )
