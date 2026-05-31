import pytest

from lib.shared.claim_role_registry import (
    CLAIM_ROLE_REGISTRY,
    contract_for_claim_role,
    validate_claim_role_contract,
)
from lib.shared.errors import ValidationError
from lib.shared.memory_grammar import ClaimRole


def test_registry_covers_every_claim_role_literal() -> None:
    assert set(CLAIM_ROLE_REGISTRY) == set(  # type: ignore[attr-defined]
        ClaimRole.__args__
    )


def test_relation_contract_requires_relationship_shape() -> None:
    contract = validate_claim_role_contract(
        {
            "kind": "belief",
            "claim_role": "relation",
            "abstraction_level": "relationship",
            "subject": "Alice",
            "relation": "owns",
            "object": "release readiness",
        }
    )

    assert contract.role == "relation"


def test_role_contract_rejects_stance_mismatch() -> None:
    with pytest.raises(ValidationError) as exc:
        validate_claim_role_contract(
            {
                "kind": "prediction",
                "claim_role": "fact",
                "expected": "Beacon renews",
                "resolution": "renewal state",
            }
        )

    assert "not valid" in exc.value.message


def test_situation_contract_requires_multiple_members() -> None:
    with pytest.raises(ValidationError) as exc:
        validate_claim_role_contract(
            {
                "kind": "belief",
                "claim_role": "situation",
                "abstraction_level": "composite",
                "situation": "Single-model pseudo situation",
                "summary": "Not actually composite.",
                "member_model_ids": ["00000000-0000-0000-0000-000000000001"],
                "relationship_summary": "Only one member is present.",
            }
        )

    assert "member_model_ids" in exc.value.message


def test_pattern_contract_accepts_pattern_instance_shape() -> None:
    contract = validate_claim_role_contract(
        {
            "kind": "belief",
            "claim_role": "pattern",
            "abstraction_level": "atomic",
            "time_mode": "past",
            "matched_context": {"channel": "github", "pr": 847},
        }
    )

    assert contract == contract_for_claim_role("pattern")
