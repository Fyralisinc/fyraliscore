"""Unit tests for shared utilities in lsob_baselines.common."""

from __future__ import annotations

import numpy as np
import pytest

from lsob_baselines.common import (
    chunk_text,
    cosine_similarity,
    extract_entities,
    hash_embedding,
    top_k,
)


def test_hash_embedding_is_deterministic():
    a = hash_embedding("hello world", dim=64)
    b = hash_embedding("hello world", dim=64)
    assert a.shape == (64,)
    assert np.allclose(a, b)


def test_hash_embedding_is_unit_norm():
    v = hash_embedding("an arbitrary sentence of reasonable length", dim=128)
    assert abs(float(np.linalg.norm(v)) - 1.0) < 1e-9


def test_hash_embedding_empty_text_is_safe():
    v = hash_embedding("", dim=32)
    assert abs(float(np.linalg.norm(v)) - 1.0) < 1e-9


def test_hash_embedding_rejects_bad_dim():
    with pytest.raises(ValueError):
        hash_embedding("x", dim=0)


def test_cosine_similarity_bounds():
    rng = np.random.default_rng(0)
    for _ in range(50):
        a = rng.standard_normal(32)
        b = rng.standard_normal(32)
        sim = cosine_similarity(a, b)
        assert -1.0 <= sim <= 1.0


def test_cosine_similarity_identity():
    v = hash_embedding("identical text", dim=64)
    assert abs(cosine_similarity(v, v) - 1.0) < 1e-9


def test_cosine_similarity_zero_vector():
    z = np.zeros(8)
    v = hash_embedding("anything", dim=8)
    assert cosine_similarity(z, v) == 0.0


def test_chunk_text_splits_at_sentence_boundaries():
    text = "First sentence. Second sentence! Third one? Fourth."
    chunks = chunk_text(text, max_chars=25)
    assert len(chunks) >= 2
    assert all(len(c) > 0 for c in chunks)


def test_extract_entities_picks_up_ids_and_names():
    ents = extract_entities("Alice merged PR #412 for C-ingest against Acme.")
    assert "Alice" in ents
    assert "Acme" in ents
    assert "C-ingest" in ents
    assert "#412" in ents


def test_top_k_orders_descending():
    items = [(0.1, "a"), (0.9, "b"), (0.5, "c")]
    assert [v for _, v in top_k(items, 2)] == ["b", "c"]
