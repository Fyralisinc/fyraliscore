"""
services/reasoning/topology/tests/test_anchor.py — unit tests for the pure
content -> topo projection.

These tests are pure-Python (no DB). The DB-integration test that
asserts `models.topo_embedding` is populated on insert lives in
`services/domain/models/tests/test_repo.py::test_insert_initializes_topo_embedding`.
"""
from __future__ import annotations

import math
import random

import pytest

from lib.shared.types import TOPO_EMBEDDING_DIM
from services.reasoning.topology.anchor import content_anchor


def _random_unit_768(seed: int) -> list[float]:
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(768)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v] if n else v


def test_content_anchor_returns_topo_dim_vector():
    v = _random_unit_768(seed=1)
    out = content_anchor(v)
    assert len(out) == TOPO_EMBEDDING_DIM == 128


def test_content_anchor_is_l2_normalized():
    v = _random_unit_768(seed=2)
    out = content_anchor(v)
    norm = math.sqrt(sum(x * x for x in out))
    assert norm == pytest.approx(1.0, abs=1e-9)


def test_content_anchor_is_deterministic():
    v = _random_unit_768(seed=3)
    a = content_anchor(v)
    b = content_anchor(v)
    assert a == b


def test_content_anchor_different_inputs_different_outputs():
    a = content_anchor(_random_unit_768(seed=4))
    b = content_anchor(_random_unit_768(seed=5))
    # Should differ in essentially every coordinate for distinct
    # random inputs.
    assert a != b


def test_content_anchor_rejects_wrong_dim():
    with pytest.raises(ValueError, match="768"):
        content_anchor([0.0] * 256)
    with pytest.raises(ValueError, match="768"):
        content_anchor([])
