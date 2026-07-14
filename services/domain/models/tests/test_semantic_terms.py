from __future__ import annotations

from services.domain.models.semantic_terms import (
    derive_query_semantic_terms,
    derive_semantic_terms,
)


def test_derive_semantic_terms_keeps_specific_grounded_phrases() -> None:
    terms = derive_semantic_terms(
        natural=(
            "Slack thread says the partial refund edge case causes duplicate "
            "invoice reversal when idempotency keys collide for ACME customer."
        ),
        proposition={
            "kind": "belief",
            "claim_role": "concern",
            "assertion": (
                "partial refund edge case triggers duplicate invoice reversal "
                "after idempotency key collision"
            ),
            "domain_tags": ["payments"],
        },
        falsifier={
            "open_falsifier": "Refund replay no longer creates duplicate invoice reversal",
        },
        resolution_criteria={"expected": "idempotency key collision is prevented"},
        scope_entities=[{"type": "customer", "id": "acme", "name": "ACME customer"}],
        domain_tags=["payments"],
        suggested_terms=[
            "partial refund edge case",
            "idempotency key collision",
            "ACME customer",
            "payments",
            "Slack thread",
            "PR #123",
        ],
    )

    assert "partial refund edge case" in terms
    assert "idempotency key collision" in terms
    assert "duplicate invoice reversal" in terms
    assert "acme customer" not in terms
    assert "payments" not in terms
    assert "slack thread" not in terms
    assert not any("belief" in term or "concern" in term for term in terms)
    assert "pr 123" not in terms
    assert not any("#123" in term for term in terms)


def test_derive_query_semantic_terms_matches_model_term_shape() -> None:
    terms = derive_query_semantic_terms(
        "refund replay hits idempotency key collision",
        seed_signature={"signature": "duplicate invoice reversal"},
    )

    assert "idempotency key collision" in terms
    assert "duplicate invoice reversal" in terms
    assert "refund replay" in terms


def test_derive_query_semantic_terms_ignores_batch_wrapper_language() -> None:
    terms = derive_query_semantic_terms(
        (
            "Evidence window containing 20 source signals. The window wrapper "
            "is not itself a business fact; derive durable claims only from "
            "individual signals. Atlas Retail Group procurement packet waits "
            "for SOC2 audit export evidence."
        )
    )

    text = " ".join(terms)
    assert "atlas retail group" in terms
    assert "procurement packet waits" in terms
    assert "soc2 audit export" in terms
    for wrapper in ("window", "wrapper", "source signals", "durable claims"):
        assert wrapper not in text
