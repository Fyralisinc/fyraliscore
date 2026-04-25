"""Shared utilities for LSOB baselines.

Provides deterministic hashing-based embeddings (no model required),
cosine similarity, and simple chunking. Used by the in-memory fallback
paths so that tests are hermetic and reproducible.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import Iterable

import numpy as np


def hash_embedding(text: str, dim: int = 128) -> np.ndarray:
    """Produce a deterministic, unit-norm pseudo-embedding for ``text``.

    This is a hashing-trick embedding intended for tests and the in-memory
    fallback paths of baselines. In production configurations (see the
    ``heavy`` optional dependency group and the ``docker/`` services), you
    should swap this out for ``nomic-embed-text`` via Ollama.
    """

    tokens = _tokenize(text)
    if dim <= 0:
        raise ValueError("dim must be positive")
    vec = np.zeros(dim, dtype=np.float64)
    if not tokens:
        vec[0] = 1.0
        return vec / np.linalg.norm(vec)

    for tok in tokens:
        digest = hashlib.blake2b(tok.encode("utf-8"), digest_size=8).digest()
        idx = int.from_bytes(digest[:4], "little") % dim
        sign = 1.0 if (digest[4] & 1) == 0 else -1.0
        vec[idx] += sign

    norm = np.linalg.norm(vec)
    if norm < 1e-12:
        vec[0] = 1.0
        norm = 1.0
    return vec / norm


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity in ``[-1, 1]``; returns 0.0 for zero-vectors."""

    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na < 1e-12 or nb < 1e-12:
        return 0.0
    val = float(np.dot(a, b) / (na * nb))
    # Guard against floating-point drift outside [-1, 1].
    if math.isnan(val):
        return 0.0
    return max(-1.0, min(1.0, val))


_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+")


def chunk_text(text: str, max_chars: int = 512) -> list[str]:
    """Split ``text`` into sentence-ish chunks bounded by ``max_chars``.

    The baseline spec calls for ~512 tokens per chunk in production; for the
    in-memory path we use character-count as a cheap proxy.
    """

    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    sentences = [s.strip() for s in _SENT_SPLIT.split(text.strip()) if s.strip()]
    if not sentences:
        return []

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for sent in sentences:
        extra = len(sent) + (1 if current else 0)
        if current and current_len + extra > max_chars:
            chunks.append(" ".join(current))
            current = [sent]
            current_len = len(sent)
        else:
            current.append(sent)
            current_len += extra
    if current:
        chunks.append(" ".join(current))
    return chunks


_WORD = re.compile(r"[A-Za-z0-9_]+")


def _tokenize(text: str) -> list[str]:
    return [m.group(0).lower() for m in _WORD.finditer(text or "")]


def extract_entities(text: str) -> list[str]:
    """Very small regex-based entity extractor used by the KG / GraphRAG paths.

    Picks up tokens that look like IDs (``C-ingest``, ``PR #412``) and
    capitalised words (``Acme``, ``Alice``). Cheap, deterministic, and
    good-enough for the in-memory fallback.
    """

    ents: list[str] = []
    # Capitalised words / names
    for m in re.finditer(r"\b[A-Z][a-zA-Z0-9]{2,}\b", text or ""):
        ents.append(m.group(0))
    # IDs of the shape ``C-ingest`` / ``P-pattern-1``
    for m in re.finditer(r"\b[A-Z]-[a-zA-Z0-9_\-]+\b", text or ""):
        ents.append(m.group(0))
    # PR numbers
    for m in re.finditer(r"#\d+", text or ""):
        ents.append(m.group(0))
    seen: set[str] = set()
    out: list[str] = []
    for e in ents:
        if e in seen:
            continue
        seen.add(e)
        out.append(e)
    return out


def top_k(
    scored: Iterable[tuple[float, object]], k: int
) -> list[tuple[float, object]]:
    """Return the top-``k`` highest-scoring items."""

    items = list(scored)
    items.sort(key=lambda t: t[0], reverse=True)
    return items[: max(0, k)]
