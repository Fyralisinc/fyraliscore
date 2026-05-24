"""Small deterministic text embeddings for Think fallback paths.

Production semantic quality still comes from the configured embedder. This
module only prevents active Models and reconciliation candidates from entering
the memory graph as all-zero vectors when an LLM diff omits embeddings.
"""
from __future__ import annotations

import hashlib
import math
import re
from typing import Any


def deterministic_text_embedding(text: str, dim: int = 768) -> list[float]:
    """Return a cheap non-zero lexical vector for fallback anchoring."""
    tokens = re.findall(r"[a-z0-9_#.-]+", (text or "").lower())
    if not tokens:
        tokens = ["empty"]
    vec = [0.0] * dim
    for token in tokens:
        digest = hashlib.sha256(token.encode()).digest()
        idx = int.from_bytes(digest[:4], "big") % dim
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vec[idx] += sign
    norm = math.sqrt(sum(x * x for x in vec))
    if norm <= 0.0:
        vec[0] = 1.0
        return vec
    return [x / norm for x in vec]


def is_zero_embedding(value: Any) -> bool:
    if not isinstance(value, (list, tuple)) or not value:
        return False
    try:
        return all(float(x) == 0.0 for x in value)
    except (TypeError, ValueError):
        return False


__all__ = ["deterministic_text_embedding", "is_zero_embedding"]
