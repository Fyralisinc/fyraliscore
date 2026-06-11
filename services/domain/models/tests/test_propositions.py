"""
services/domain/models/tests/test_propositions.py — Pydantic discriminated-union
tests over proposition kinds.

These are unit tests (no DB) so they don't need the `integration`
marker; they run offline and in <100ms.
"""
from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from lib.shared.errors import ValidationError
from services.domain.models.propositions import (
    LEGAL_KINDS,
    SituationProposition,
    StateProposition,
    PredictionProposition,
    canonicalize_proposition,
    validate_proposition,
)

from .conftest import every_kind_proposition


def test_all_base_proposition_kinds_validate_and_round_trip() -> None:
    """Every spec kind in `every_kind_proposition()` must validate
    and round-trip its discriminator. The Stage-1 `recommendation`
    kind is exercised separately in test_recommendations.py because
    its shape requires a target_act_ref + proposed_change pair."""
    seen: set[str] = set()
    for raw in every_kind_proposition():
        parsed = validate_proposition(raw)
        canonical = canonicalize_proposition(raw)
        assert parsed.kind == canonical["kind"]
        dumped = parsed.model_dump()
        assert dumped["kind"] == canonical["kind"]
        if raw["kind"] != canonical["kind"]:
            assert dumped["legacy_kind"] == raw["kind"]
        seen.add(raw["kind"])
    # The base kinds covered exactly once; recommendation lives
    # in a dedicated test file because of its DB-backed validators.
    assert seen == {
        "state",
        "relation",
        "prediction",
        "pattern",
        "pattern_instance",
        "capability_assessment",
        "hypothesis",
        "concern",
        "market_assessment",
        "environmental_trend",
        "situation",
    }


def test_legal_kinds_matches_spec() -> None:
    """The base discriminator is the four-stance ontology."""
    assert LEGAL_KINDS == frozenset(
        {"observation", "belief", "prediction", "norm"}
    )


def test_unknown_kind_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        validate_proposition({"kind": "superstate", "subject": "x", "assertion": "y"})
    assert "unknown" in exc.value.message.lower() or "proposition.kind" in str(
        exc.value.context
    )


def test_missing_kind_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        validate_proposition({"subject": "x", "assertion": "y"})
    assert "kind" in exc.value.message


def test_non_dict_rejected() -> None:
    with pytest.raises(ValidationError):
        validate_proposition("not a dict")  # type: ignore[arg-type]


def test_missing_required_field_for_kind_rejected() -> None:
    """Spec §2: state proposition needs subject + assertion."""
    with pytest.raises(ValidationError) as exc:
        validate_proposition({"kind": "state", "subject": "alice"})
    # Pydantic collected the error under context['errors']
    errs = exc.value.context.get("errors", [])
    assert errs


def test_claim_role_contract_rejects_kind_role_mismatch() -> None:
    with pytest.raises(ValidationError) as exc:
        validate_proposition(
            {
                "kind": "prediction",
                "claim_role": "fact",
                "expected": "Beacon renews",
                "resolution": "renewal state",
            }
        )

    assert "claim_role" in str(exc.value.context)


def test_claim_role_contract_rejects_malformed_relation() -> None:
    with pytest.raises(ValidationError) as exc:
        validate_proposition(
            {
                "kind": "belief",
                "claim_role": "relation",
                "abstraction_level": "relationship",
                "subject": "Alice",
                "relation": "blocks",
            }
        )

    assert "requires one of" in exc.value.message


def test_hypothesis_role_accepts_assertion_alias() -> None:
    raw = {
        "kind": "belief",
        "claim_role": "hypothesis",
        "abstraction_level": "atomic",
        "assertion": "An off-sensor approval transition likely occurred.",
    }

    canonical = canonicalize_proposition(raw)
    parsed = validate_proposition(raw)

    assert canonical["hypothesis_text"] == raw["assertion"]
    assert parsed.hypothesis_text == raw["assertion"]


def test_hypothesis_can_represent_second_order_belief_without_schema_migration() -> None:
    parsed = validate_proposition({
        "kind": "belief",
        "claim_role": "hypothesis",
        "about_model_id": "018f0000-0000-7000-8000-000000000001",
        "belief_holder_actor_id": "018f0000-0000-7000-8000-000000000002",
        "second_order_attitude": "doubts",
        "belief_target_text": "Maya doubts the Atlas deadline is still credible.",
    })

    assert parsed.kind == "belief"
    assert parsed.claim_role == "hypothesis"
    assert parsed.about_model_id == "018f0000-0000-7000-8000-000000000001"
    assert parsed.second_order_attitude == "doubts"


def test_state_proposition_accepts_dict_subject() -> None:
    raw = {
        "kind": "state",
        "subject": {"type": "actor", "id": "alice"},
        "assertion": "is reliable",
    }
    parsed = validate_proposition(raw)
    assert isinstance(parsed, StateProposition)


def test_prediction_proposition_round_trip() -> None:
    raw = {
        "kind": "prediction",
        "expected": "c-187 doneverified",
        "resolution": "commitment c-187 state",
    }
    parsed = validate_proposition(raw)
    assert isinstance(parsed, PredictionProposition)
    back = parsed.model_dump()
    assert back["kind"] == "prediction"
    assert back["expected"] == raw["expected"]


def test_situation_proposition_compositional_fields_round_trip() -> None:
    """Extended SituationProposition must validate and round-trip the
    eight new compositional fields (pressure_type, shared_mechanism,
    judgment_change, affected_decisions, affected_customers,
    affected_teams, evidence_event_ids, open_falsifier)."""
    member_ids = [str(uuid7()), str(uuid7()), str(uuid7())]
    evidence_ids = [str(uuid7()), str(uuid7())]
    raw = {
        "kind": "situation",
        "situation": "Capacity squeeze on platform pod is hitting Globex",
        "summary": (
            "Two senior engineers on PTO while Globex onboarding ramps; "
            "the renewal-eve sprint is at risk."
        ),
        "member_model_ids": member_ids,
        "relationship_summary": (
            "PTO overlap (M1), onboarding ramp (M2), and renewal date (M3) "
            "interact to compress headroom in the same week."
        ),
        "status": "forming",
        "pressure_type": "capacity",
        "shared_mechanism": (
            "All three claims compete for the same two reviewers in a "
            "single week."
        ),
        "judgment_change": (
            "Read together, the team is one ticket away from missing the "
            "renewal demo even though no individual claim is alarming."
        ),
        "affected_decisions": ["postpone non-Globex hotfixes"],
        "affected_customers": ["Globex Inc"],
        "affected_teams": ["platform"],
        "evidence_event_ids": evidence_ids,
        "open_falsifier": (
            "A senior reviewer returns early or the Globex demo is moved by "
            "more than five business days."
        ),
    }
    parsed = validate_proposition(raw)
    assert isinstance(parsed, SituationProposition)
    assert parsed.kind == "belief"
    assert parsed.legacy_kind == "situation"
    assert parsed.claim_role == "situation"
    assert parsed.pressure_type == "capacity"
    assert parsed.shared_mechanism.startswith("All three claims")
    assert parsed.affected_customers == ["Globex Inc"]
    assert parsed.evidence_event_ids == evidence_ids

    back = parsed.model_dump()
    assert back["kind"] == "belief"
    assert back["legacy_kind"] == "situation"
    for key, expected in raw.items():
        if key == "kind":
            continue
        assert back[key] == expected, f"round-trip lost field {key!r}"


def test_situation_proposition_legacy_shape_still_validates() -> None:
    """Backward compat: the original five-field SituationProposition shape
    must still parse so older Models written before this migration keep
    loading."""
    raw = {
        "kind": "situation",
        "situation": "Legacy situation",
        "summary": "Old shape captured before compositional fields existed.",
        "member_model_ids": [str(uuid7()), str(uuid7())],
        "relationship_summary": "Members co-occur in one operational region.",
        "status": "active",
    }
    parsed = validate_proposition(raw)
    assert isinstance(parsed, SituationProposition)
    assert parsed.pressure_type is None
    assert parsed.shared_mechanism is None
    assert parsed.open_falsifier is None


def test_situation_proposition_rejects_unknown_pressure_type() -> None:
    raw = {
        "kind": "situation",
        "situation": "X",
        "summary": "y",
        "member_model_ids": [str(uuid7())],
        "relationship_summary": "z",
        "status": None,
        "pressure_type": "vibes",  # not in the eight-category enum
        "shared_mechanism": "n/a",
    }
    with pytest.raises(ValidationError):
        validate_proposition(raw)


def test_situation_proposition_rejects_empty_optional_strings() -> None:
    raw = {
        "kind": "situation",
        "situation": "X",
        "summary": "y",
        "member_model_ids": [str(uuid7())],
        "relationship_summary": "z",
        "status": None,
        "shared_mechanism": "   ",  # whitespace-only
    }
    with pytest.raises(ValidationError):
        validate_proposition(raw)
