from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentReplacementResult,
    CanonicalReferentVersionRef,
)


TENANT_ID = UUID("11111111-1111-4111-8111-111111111111")
EFFECTIVE_AT = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def _ref(
    referent_type: str,
    referent_id: str,
    *,
    version: int = 1,
) -> CanonicalReferentVersionRef:
    return CanonicalReferentVersionRef(
        type=referent_type,
        id=referent_id,
        version=version,
    )


def _command(**overrides) -> CanonicalReferentReplacementCommand:
    values = {
        "tenant_id": TENANT_ID,
        "operation_ref": "replace:project-northstar",
        "predecessor": _ref("project", "project:north-star", version=2),
        "successor": _ref("project", "project:northstar", version=1),
        "expected_predecessor_version": 2,
        "effective_at": EFFECTIVE_AT,
        "authority_ref": "authority:entity-review:42",
        "reason": "The source-native project key identifies one successor.",
        "evidence_refs": (
            "observation:slack:1",
            "resource:jira-project:Northstar",
        ),
    }
    values.update(overrides)
    return CanonicalReferentReplacementCommand(**values)


def test_replacement_command_has_a_stable_semantic_fingerprint() -> None:
    command = _command()
    reordered_evidence = _command(evidence_refs=tuple(reversed(command.evidence_refs)))

    assert len(command.request_fingerprint) == 64
    assert command.request_fingerprint == reordered_evidence.request_fingerprint


def test_operation_ref_does_not_change_the_semantic_fingerprint() -> None:
    first = _command(operation_ref="replace:one")
    replay_key_alias = _command(operation_ref="replace:two")

    assert first.request_fingerprint == replay_key_alias.request_fingerprint


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"effective_at": datetime(2026, 7, 17, 12, 0)},
            "effective_at must be timezone-aware",
        ),
        (
            {
                "successor": _ref(
                    "project",
                    "project:north-star",
                    version=2,
                )
            },
            "successor must differ",
        ),
        (
            {"expected_predecessor_version": 1},
            "must equal predecessor.version",
        ),
        (
            {
                "predecessor": _ref("team", "team:northstar", version=3),
                "expected_predecessor_version": 3,
            },
            "must have the same type",
        ),
        (
            {"evidence_refs": ("observation:1", "observation:1")},
            "cannot contain duplicates",
        ),
        (
            {"evidence_refs": ("observation:1", " ")},
            "cannot contain blank",
        ),
    ],
)
def test_replacement_command_rejects_ambiguous_requests(
    overrides,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        _command(**overrides)


def test_contracts_are_frozen_and_forbid_unknown_fields() -> None:
    command = _command()

    with pytest.raises(ValidationError):
        command.operation_ref = "changed"
    with pytest.raises(ValidationError):
        CanonicalReferentVersionRef(
            type="project",
            id="project:northstar",
            version=1,
            physical_id="rewritten",
        )


def test_result_represents_applied_and_replayed_outcomes() -> None:
    command = _command()
    base = {
        "transition_id": uuid4(),
        "tenant_id": command.tenant_id,
        "operation_ref": command.operation_ref,
        "request_fingerprint": command.request_fingerprint,
        "predecessor": command.predecessor,
        "successor": command.successor,
        "effective_at": command.effective_at,
        "transaction_at": datetime.now(timezone.utc),
    }

    applied = CanonicalReferentReplacementResult(**base, applied=True)
    replayed = CanonicalReferentReplacementResult(**base, applied=False)

    assert applied.applied is True
    assert replayed.applied is False


def test_result_rejects_invalid_digest_and_naive_transaction_time() -> None:
    command = _command()
    base = {
        "transition_id": uuid4(),
        "tenant_id": command.tenant_id,
        "operation_ref": command.operation_ref,
        "request_fingerprint": command.request_fingerprint,
        "predecessor": command.predecessor,
        "successor": command.successor,
        "effective_at": command.effective_at,
        "transaction_at": datetime.now(timezone.utc),
        "applied": True,
    }

    with pytest.raises(ValidationError):
        CanonicalReferentReplacementResult(
            **{**base, "request_fingerprint": "not-a-digest"}
        )
    with pytest.raises(ValidationError, match="transaction_at must be timezone-aware"):
        CanonicalReferentReplacementResult(
            **{**base, "transaction_at": datetime(2026, 7, 17, 12, 0)}
        )
