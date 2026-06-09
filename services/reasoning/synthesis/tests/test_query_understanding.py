from __future__ import annotations

from services.reasoning.synthesis.query_understanding import (
    alternative_terms,
    compact_alternative_key,
    extract_query_alternatives,
)


def test_extract_query_alternatives_from_multiple_choice_lines() -> None:
    query = """Which page has the largest count?

A. Standard Laptop
B. Developer Laptop (Mac)
C. Sales Laptop
"""

    assert extract_query_alternatives(query) == (
        "Standard Laptop",
        "Developer Laptop (Mac)",
        "Sales Laptop",
    )


def test_extract_query_alternatives_from_quotes_and_between_phrase() -> None:
    query = (
        "Compare `SsoRelay` with \"DataBridge\" between Platform Launch "
        "and Security Review."
    )

    alternatives = extract_query_alternatives(query)

    assert "SsoRelay" in alternatives
    assert "DataBridge" in alternatives
    assert "Platform Launch" in alternatives
    assert "Security Review" in alternatives


def test_extract_query_alternatives_ignores_quoted_ui_labels_without_comparison_frame() -> None:
    query = (
        'When I open the "Filters" dropdown, excluding "Edit personal filters" '
        'and "-- None --", which labels contain Incident?'
    )

    assert extract_query_alternatives(query) == ()
    assert "Filters" in extract_query_alternatives(query, include_quoted=True)


def test_alternative_terms_bridge_spacing_and_punctuation() -> None:
    terms = alternative_terms("Developer Laptop (Mac)")

    assert "developer laptop mac" in terms
    assert "developerlaptopmac" in terms
    assert compact_alternative_key("Developer Laptop (Mac)") == "developerlaptopmac"
