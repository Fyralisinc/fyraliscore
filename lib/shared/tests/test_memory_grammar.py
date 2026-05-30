from lib.shared.memory_grammar import derive_memory_grammar


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
