"""Pure-function tests for RFC 5322 header normalization.

The DB-touching parts of canonicalize_thread are covered by an
integration test in tests/integration/ (out of scope here — requires
a live Postgres).
"""
from __future__ import annotations

from services.integrations.gmail.threading import (
    normalize_message_id,
    normalize_participants,
    normalize_subject,
    split_references,
)


class TestNormalizeMessageId:
    def test_strips_angle_brackets(self) -> None:
        assert normalize_message_id("<abc@mail>") == "abc@mail"

    def test_strips_whitespace(self) -> None:
        assert normalize_message_id("  <abc@mail>  ") == "abc@mail"

    def test_returns_none_for_empty(self) -> None:
        assert normalize_message_id("") is None
        assert normalize_message_id(None) is None
        assert normalize_message_id("   ") is None

    def test_preserves_local_part_case(self) -> None:
        # Message-ID local-part is case-sensitive per RFC 5322 §3.6.4.
        assert normalize_message_id("<ABC123@mail>") == "ABC123@mail"

    def test_no_brackets_passthrough(self) -> None:
        assert normalize_message_id("abc@mail") == "abc@mail"


class TestSplitReferences:
    def test_space_separated(self) -> None:
        result = split_references("<a@x> <b@y> <c@z>")
        assert result == ["a@x", "b@y", "c@z"]

    def test_comma_separated(self) -> None:
        result = split_references("<a@x>, <b@y>, <c@z>")
        assert result == ["a@x", "b@y", "c@z"]

    def test_empty(self) -> None:
        assert split_references("") == []
        assert split_references(None) == []

    def test_mixed_separators(self) -> None:
        result = split_references("<a@x>, <b@y> <c@z>")
        assert result == ["a@x", "b@y", "c@z"]


class TestNormalizeSubject:
    def test_strips_re_prefix(self) -> None:
        assert normalize_subject("Re: hello") == "hello"
        assert normalize_subject("RE: hello") == "hello"
        assert normalize_subject("re: hello") == "hello"

    def test_strips_fwd_prefix(self) -> None:
        assert normalize_subject("Fwd: hello") == "hello"
        assert normalize_subject("Fw: hello") == "hello"

    def test_strips_nested_prefixes(self) -> None:
        assert normalize_subject("Re: Re: Re: hello") == "hello"
        assert normalize_subject("Re: Fwd: Re: foo") == "foo"

    def test_collapses_whitespace(self) -> None:
        assert normalize_subject("hello   world") == "hello world"

    def test_empty_returns_none(self) -> None:
        assert normalize_subject("") is None
        assert normalize_subject("Re: ") is None


class TestNormalizeParticipants:
    def test_lowercases_and_dedupes(self) -> None:
        result = normalize_participants(["Alice@X", "alice@x", "Bob@y"])
        assert result == ["alice@x", "bob@y"]

    def test_preserves_order(self) -> None:
        result = normalize_participants(["c@x", "a@x", "b@x"])
        assert result == ["c@x", "a@x", "b@x"]

    def test_filters_empties(self) -> None:
        result = normalize_participants(["", "  ", "alice@x", None])  # type: ignore[list-item]
        assert result == ["alice@x"]
