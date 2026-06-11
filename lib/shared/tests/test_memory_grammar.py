from lib.shared.memory_grammar import (
    derive_memory_grammar,
    sanitize_explicit_grammar_axes,
)


def test_prediction_derives_future_expected_atomic_role() -> None:
    grammar = derive_memory_grammar(
        {"kind": "prediction", "expected": "Beacon will renew", "resolution": "renewal"},
        natural="Beacon renewal is expected by quarter end.",
        scope_entities=[{"type": "customer", "id": "not-a-uuid-needed-here"}],
    )

    assert grammar.claim_role == "prediction"
    assert grammar.abstraction_level == "atomic"
    assert grammar.time_mode == "future"
    assert grammar.modality == "expected"
    assert grammar.domain_tags == ("customers",)


def test_situation_derives_composite_mixed_role_with_domains() -> None:
    grammar = derive_memory_grammar(
        {"kind": "situation"},
        natural=(
            "The customer renewal risk is blocked by platform capacity "
            "and budget constraints."
        ),
        scope_entities=[{"type": "commitment", "id": "x"}],
    )

    assert grammar.claim_role == "situation"
    assert grammar.abstraction_level == "composite"
    assert grammar.modality == "inferred"
    assert grammar.polarity == "mixed"
    assert grammar.domain_tags == (
        "execution",
        "customers",
        "finance",
        "people",
        "systems",
        "risk",
    )


def test_unknown_kind_falls_back_to_atomic_inferred_fact() -> None:
    grammar = derive_memory_grammar(
        {"kind": "future_unicorn"},
        natural="Unrecognized model payload.",
    )

    assert grammar.claim_role == "fact"
    assert grammar.abstraction_level == "atomic"
    assert grammar.time_mode == "unspecified"
    assert grammar.modality == "inferred"
    assert grammar.polarity == "neutral"


def test_sanitize_drops_off_enum_explicit_axis_values() -> None:
    prop = {
        "kind": "belief",
        "subject": "brex_account:acct-1",
        "time_mode": "point_in_time",
        "modality": "actual",
        "polarity": "neutral",
    }

    cleaned = sanitize_explicit_grammar_axes(prop)

    assert "time_mode" not in cleaned
    assert "modality" not in cleaned
    assert cleaned["polarity"] == "neutral"
    assert cleaned["kind"] == "belief"
    # The input proposition is not mutated.
    assert prop["time_mode"] == "point_in_time"


def test_sanitize_returns_same_object_when_all_axes_valid() -> None:
    prop = {"kind": "belief", "time_mode": "current", "modality": "inferred"}

    assert sanitize_explicit_grammar_axes(prop) is prop


def test_sanitize_drops_non_string_axis_values() -> None:
    prop = {"kind": "belief", "claim_role": 7, "abstraction_level": ["atomic"]}

    cleaned = sanitize_explicit_grammar_axes(prop)

    assert "claim_role" not in cleaned
    assert "abstraction_level" not in cleaned
