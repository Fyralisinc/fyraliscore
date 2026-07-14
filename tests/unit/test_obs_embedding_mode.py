"""Unit coverage for the OBS_EMBEDDING_MODE staged rollout flag and the
shadow/lazy seed helpers added for the observation-embedding decommission.

Pure logic only (no DB / no Ollama) — the behavioural integration (ingest skips
the embed; T1 retrieval re-embeds on demand) is exercised by the DB-backed
ingest/retrieval suites once the flag is set.
"""
from __future__ import annotations

import pytest

from lib.embeddings.mode import (
    ObsEmbeddingMode,
    obs_embedding_mode,
    seed_from_stored_obs_vector,
    shadow_compare_seed,
    write_obs_embeddings,
)
from services.reasoning.retrieval.primary import (
    _cosine_similarity,
    _shadow_compare_seed_vector,
)


@pytest.mark.parametrize(
    "raw,mode,write,seed_stored,shadow",
    [
        ("eager", ObsEmbeddingMode.EAGER, True, True, False),
        ("shadow", ObsEmbeddingMode.SHADOW, True, True, True),
        ("cutover", ObsEmbeddingMode.CUTOVER, False, False, False),
        ("SHADOW", ObsEmbeddingMode.SHADOW, True, True, True),  # case-insensitive
        ("garbage", ObsEmbeddingMode.EAGER, True, True, False),  # unknown -> eager
        ("", ObsEmbeddingMode.EAGER, True, True, False),  # unset-ish -> eager
    ],
)
def test_mode_truth_table(monkeypatch, raw, mode, write, seed_stored, shadow):
    monkeypatch.setenv("OBS_EMBEDDING_MODE", raw)
    assert obs_embedding_mode() is mode
    assert write_obs_embeddings() is write
    assert seed_from_stored_obs_vector() is seed_stored
    assert shadow_compare_seed() is shadow


def test_mode_defaults_to_eager_when_unset(monkeypatch):
    monkeypatch.delenv("OBS_EMBEDDING_MODE", raising=False)
    assert obs_embedding_mode() is ObsEmbeddingMode.EAGER
    assert write_obs_embeddings() is True
    assert seed_from_stored_obs_vector() is True
    assert shadow_compare_seed() is False


def test_cutover_is_the_only_lazy_mode(monkeypatch):
    # Invariant the whole rollout relies on: writes stop exactly when the read
    # stops seeding from the stored vector, so there is never a window where new
    # rows are unembedded but retrieval still expects a stored seed.
    for raw in ("eager", "shadow", "cutover"):
        monkeypatch.setenv("OBS_EMBEDDING_MODE", raw)
        assert write_obs_embeddings() == seed_from_stored_obs_vector()


def test_cosine_similarity_bounds():
    assert _cosine_similarity([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert _cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)
    assert _cosine_similarity([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0)
    # zero vector -> defined as 0.0 (no division by zero)
    assert _cosine_similarity([0.0, 0.0], [1.0, 1.0]) == 0.0
    # scale invariance
    assert _cosine_similarity([3.0, 4.0], [6.0, 8.0]) == pytest.approx(1.0)


class _FakeEmbedder:
    def __init__(self, vector=None, exc=None):
        self._vector = vector
        self._exc = exc
        self.calls = 0

    async def embed(self, text):
        self.calls += 1
        if self._exc is not None:
            raise self._exc
        return list(self._vector)


async def test_shadow_compare_records_cosine_on_match():
    stored = [1.0, 0.0, 0.0]
    embedder = _FakeEmbedder(vector=[1.0, 0.0, 0.0])
    notes: dict = {}
    await _shadow_compare_seed_vector(
        embedder=embedder, stored=stored, seed_text="hello", notes=notes
    )
    assert embedder.calls == 1
    assert notes["shadow_seed_cosine"] == pytest.approx(1.0)
    assert "shadow_seed_error" not in notes


async def test_shadow_compare_flags_dim_mismatch():
    embedder = _FakeEmbedder(vector=[1.0, 0.0])  # wrong dim
    notes: dict = {}
    await _shadow_compare_seed_vector(
        embedder=embedder, stored=[1.0, 0.0, 0.0], seed_text="x", notes=notes
    )
    assert notes["shadow_seed_dim_mismatch"] == [3, 2]
    assert "shadow_seed_cosine" not in notes


async def test_shadow_compare_swallows_embedder_error():
    from lib.embeddings.ollama import OllamaError

    embedder = _FakeEmbedder(exc=OllamaError("down"))
    notes: dict = {}
    # Must not raise — shadow measurement is best-effort and never breaks retrieval.
    await _shadow_compare_seed_vector(
        embedder=embedder, stored=[1.0, 0.0], seed_text="x", notes=notes
    )
    assert "shadow_seed_error" in notes
    assert "shadow_seed_cosine" not in notes
