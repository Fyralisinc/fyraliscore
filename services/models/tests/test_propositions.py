"""
services/models/tests/test_propositions.py — Pydantic discriminated-union
tests over all 10 proposition kinds.

These are unit tests (no DB) so they don't need the `integration`
marker; they run offline and in <100ms.
"""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from services.models.propositions import (
    LEGAL_KINDS,
    StateProposition,
    PredictionProposition,
    validate_proposition,
)

from .conftest import every_kind_proposition


def test_all_ten_proposition_kinds_validate_and_round_trip() -> None:
    """Every spec kind in `every_kind_proposition()` must validate
    and round-trip its discriminator. The Stage-1 `recommendation`
    kind is exercised separately in test_recommendations.py because
    its shape requires a target_act_ref + proposed_change pair."""
    seen: set[str] = set()
    for raw in every_kind_proposition():
        parsed = validate_proposition(raw)
        assert parsed.kind == raw["kind"]
        dumped = parsed.model_dump()
        assert dumped["kind"] == raw["kind"]
        seen.add(raw["kind"])
    # The 10 base kinds covered exactly once; recommendation lives
    # in a dedicated test file because of its DB-backed validators.
    assert seen == LEGAL_KINDS - {"recommendation"}


def test_legal_kinds_matches_spec() -> None:
    """Original Wave-0 set, plus the Stage-1 recommendation kind.
    Changing this set requires a SCHEMA-LOCK amendment + migration."""
    assert LEGAL_KINDS == frozenset(
        {
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
            "recommendation",
        }
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
    assert any("assertion" in str(e.get("loc", "")) for e in errs)


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
