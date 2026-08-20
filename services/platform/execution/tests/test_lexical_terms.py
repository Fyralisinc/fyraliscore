from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.platform.execution import inquiry, lexical_terms
from services.reasoning.retrieval.primary import TriggerContext


def _trigger(text: str = "Does SOC2-RISK-77 block the Acme launch?") -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[],
        scope_actors=[],
        seed_natural_text=text,
        seed_occurred_at=datetime(2026, 6, 13, 8, 0, tzinfo=timezone.utc),
    )


def test_lexical_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._SPARSE_STRONG_SINGLE_MATCH_MAX_DF == (
        lexical_terms.SPARSE_STRONG_SINGLE_MATCH_MAX_DF
    )
    assert inquiry._focused_index_lookup_groups is (
        lexical_terms.focused_index_lookup_groups
    )
    assert inquiry._focused_index_terms is lexical_terms.focused_index_terms
    assert inquiry._focused_material_tokens is lexical_terms.focused_material_tokens
    assert inquiry._hybrid_lexical_terms is lexical_terms.hybrid_lexical_terms
    assert inquiry._hybrid_lookup_terms is lexical_terms.hybrid_lookup_terms
    assert inquiry._hybrid_sparse_lookup_groups is (
        lexical_terms.hybrid_sparse_lookup_groups
    )
    assert inquiry._hybrid_sparse_lookup_terms is (
        lexical_terms.hybrid_sparse_lookup_terms
    )
    assert inquiry._hybrid_sparse_strong_single_match_terms is (
        lexical_terms.hybrid_sparse_strong_single_match_terms
    )
    assert inquiry._is_focused_strong_token is lexical_terms.is_focused_strong_token
    assert inquiry._like_patterns_for_terms is lexical_terms.like_patterns_for_terms
    assert inquiry._relevance_tokens is lexical_terms.relevance_tokens


def test_focused_index_terms_extract_strong_scoped_terms() -> None:
    terms = lexical_terms.focused_index_terms(
        "Which dependency mentions SOC2-RISK-77 for the Acme launch?",
        _trigger(),
        max_terms=8,
    )

    assert any("soc2-risk-77" in term for term in terms)
    assert "which" not in terms
    assert len(terms) <= 8
    assert lexical_terms.focused_index_lookup_groups(terms)


def test_hybrid_terms_and_sparse_lookup_stay_bounded() -> None:
    terms = lexical_terms.hybrid_lexical_terms(
        "Which dependency mentions SOC2-RISK-77?",
        _trigger(),
        max_terms=4,
    )
    sparse_terms = lexical_terms.hybrid_sparse_lookup_terms(
        ["alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo"]
    )

    assert any("soc2-risk-77" in term for term in terms)
    assert sparse_terms == [
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
    ]
    assert lexical_terms.hybrid_sparse_strong_single_match_terms(
        ["alpha", "soc2-risk-77", "risk_42"]
    ) == ["soc2-risk-77", "risk_42"]


def test_hybrid_lookup_terms_drop_generic_fallback_words() -> None:
    terms = lexical_terms.hybrid_lookup_terms(
        [
            "owner responsible assigned dependency evidence blocker customer launch",
            "customer-95 Borealis Bank renewal timeline",
        ]
    )

    assert "owner" not in terms
    assert "dependency" not in terms
    assert "launch" not in terms
    assert "customer-95" in terms
    assert "borealis" in terms
    assert "renewal" in terms


def test_lexical_terms_drop_batch_wrapper_words_but_keep_company_anchors() -> None:
    focused = lexical_terms.focused_index_terms(
        (
            "What evidence would weaken the interpretation that Evidence window "
            "containing 20 source signals. The window wrapper is not itself a "
            "business fact; derive durable claims only from individual signals. "
            "Atlas Retail Group needs Publish SOC2 evidence room."
        ),
        _trigger("Atlas Retail Group procurement packet is waiting on SOC2."),
        max_terms=8,
    )
    sparse = lexical_terms.hybrid_lookup_terms(
        [
            "Evidence window containing source signals wrapper derive durable claims",
            "Atlas Retail Group Publish SOC2 evidence room",
        ]
    )
    focused_text = " ".join(focused)

    assert "atlas retail group" in focused
    assert "publish soc2" in focused_text
    for generic in ("window", "wrapper", "source", "signals", "claims"):
        assert generic not in focused_text
        assert generic not in sparse
    assert "atlas" in sparse
    assert "retail" in sparse
    assert "soc2" in sparse


def test_sparse_groups_patterns_and_relevance_tokens_are_stable() -> None:
    assert lexical_terms.hybrid_sparse_lookup_groups(
        ["Alpha Bravo", "SOC2-RISK-77", "tiny"]
    ) == [["alpha", "bravo"], ["soc2-risk-77"]]
    assert lexical_terms.like_patterns_for_terms(["A_B%", "A_B%"]) == ["%a!_b!%%"]
    assert lexical_terms.relevance_tokens("About Alpha 123 risk without context") == {
        "alpha",
        "risk",
    }
