from __future__ import annotations

import math

import pytest

from benchmarks.adapters.toy_adapter import ToyMemoryAdapter
from benchmarks.fyralis_eval.fyralis_db import (
    EMBEDDING_DIM,
    FyralisDBReader,
    hashed_token_vector,
)


def test_hashed_token_vector_is_stable_and_normalized() -> None:
    first = hashed_token_vector("Aisha ships the ledger page")
    second = hashed_token_vector("Aisha ships the ledger page")

    assert first == second
    assert len(first) == EMBEDDING_DIM
    assert math.isclose(math.sqrt(sum(value * value for value in first)), 1.0)


def test_hashed_token_vector_reflects_token_overlap() -> None:
    query = hashed_token_vector("ledger page owner")
    matching = hashed_token_vector("Aisha owns the ledger page implementation")
    unrelated = hashed_token_vector("Marco prepared invoice reconciliation")

    assert _dot(query, matching) > _dot(query, unrelated)


def test_fyralis_db_reader_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    adapter = ToyMemoryAdapter()
    observations = list(adapter.iter_observations())

    with pytest.raises(ValueError, match="requires DATABASE_URL"):
        FyralisDBReader(observations, top_k=3)


def test_embedding_text_clipper_preserves_head_and_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://example.invalid/db")
    monkeypatch.setenv("BENCHMARK_EMBED_MAX_CHARS", "20")
    reader = FyralisDBReader.__new__(FyralisDBReader)
    reader.embedding_max_chars = 20

    clipped = reader._embedding_text("abcdefghijklmnopqrstuvwxyz")

    assert clipped.startswith("abcdefghij")
    assert clipped.endswith("qrstuvwxyz")
    assert "..." in clipped


def _dot(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))
