"""Pure-Python tests for lib.topology.relocate primitives."""
from __future__ import annotations

import math
from uuid import UUID, uuid4

import pytest

from lib.shared.errors import ValidationError
from lib.shared.types import TOPO_EMBEDDING_DIM
from lib.topology.relocate import (
    RELOCATE_CASCADE_MAX_FANOUT,
    RelocateTarget,
    blend_topo,
    damped_magnitude,
    parse_relocate_target,
    select_bounded_neighbors,
)


def _unit_vec(seed: float) -> list[float]:
    """Build a deterministic L2-normalized 128-d vector for tests."""
    v = [math.sin(seed + i * 0.1) for i in range(TOPO_EMBEDDING_DIM)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


# ---------------------------------------------------------------------
# parse_relocate_target
# ---------------------------------------------------------------------


def test_parse_target_kind_vector_happy_path():
    vec = _unit_vec(1.0)
    target = parse_relocate_target(
        {"kind": "vector", "value": vec, "alpha": 0.5},
    )
    assert target.kind == "vector"
    assert target.alpha == 0.5
    assert isinstance(target.value, list)
    assert len(target.value) == TOPO_EMBEDDING_DIM


def test_parse_target_kind_model_id_happy_path():
    mid = uuid4()
    target = parse_relocate_target({"kind": "model_id", "value": str(mid)})
    assert target.kind == "model_id"
    assert target.value == mid
    assert target.alpha == 1.0


def test_parse_target_kind_neighborhood_id_happy_path():
    nid = uuid4()
    target = parse_relocate_target(
        {"kind": "neighborhood_id", "value": str(nid), "alpha": 0.7}
    )
    assert target.kind == "neighborhood_id"
    assert target.value == nid


def test_parse_target_rejects_unknown_kind():
    with pytest.raises(ValidationError, match="kind"):
        parse_relocate_target({"kind": "garbage", "value": "irrelevant"})


def test_parse_target_rejects_missing_value():
    with pytest.raises(ValidationError, match="value"):
        parse_relocate_target({"kind": "vector"})


def test_parse_target_rejects_wrong_vector_dim():
    with pytest.raises(ValidationError, match="dim"):
        parse_relocate_target({"kind": "vector", "value": [0.0, 1.0]})


def test_parse_target_rejects_alpha_zero_or_negative():
    with pytest.raises(ValidationError, match="alpha"):
        parse_relocate_target(
            {"kind": "model_id", "value": str(uuid4()), "alpha": 0.0}
        )
    with pytest.raises(ValidationError, match="alpha"):
        parse_relocate_target(
            {"kind": "model_id", "value": str(uuid4()), "alpha": -0.1}
        )


def test_parse_target_rejects_alpha_above_one():
    with pytest.raises(ValidationError, match="alpha"):
        parse_relocate_target(
            {"kind": "model_id", "value": str(uuid4()), "alpha": 1.5}
        )


def test_parse_target_rejects_invalid_uuid():
    with pytest.raises(ValidationError, match="UUID"):
        parse_relocate_target({"kind": "model_id", "value": "not-a-uuid"})


def test_parse_target_accepts_uuid_object():
    mid = uuid4()
    target = parse_relocate_target({"kind": "model_id", "value": mid})
    assert target.value == mid


# ---------------------------------------------------------------------
# blend_topo
# ---------------------------------------------------------------------


def test_blend_alpha_one_snaps_to_target():
    cur = _unit_vec(1.0)
    tgt = _unit_vec(2.0)
    out = blend_topo(cur, tgt, alpha=1.0)
    # Result should be (essentially) the target after L2-normalize.
    assert all(abs(out[i] - tgt[i]) < 1e-6 for i in range(len(out)))


def test_blend_alpha_half_is_midpoint_normalized():
    cur = _unit_vec(0.0)
    tgt = _unit_vec(1.0)
    out = blend_topo(cur, tgt, alpha=0.5)
    # Output should be unit-norm.
    norm = math.sqrt(sum(x * x for x in out))
    assert abs(norm - 1.0) < 1e-6


def test_blend_alpha_close_to_zero_stays_close_to_current():
    cur = _unit_vec(0.0)
    tgt = _unit_vec(2.0)
    out = blend_topo(cur, tgt, alpha=0.01)
    # Should be close to current.
    assert sum((out[i] - cur[i]) ** 2 for i in range(len(out))) < 1e-2


def test_blend_rejects_dim_mismatch():
    with pytest.raises(ValidationError):
        blend_topo([0.0, 1.0], _unit_vec(0.0), 0.5)


def test_blend_rejects_alpha_out_of_range():
    cur = _unit_vec(0.0)
    tgt = _unit_vec(1.0)
    with pytest.raises(ValidationError):
        blend_topo(cur, tgt, alpha=0.0)
    with pytest.raises(ValidationError):
        blend_topo(cur, tgt, alpha=1.5)


# ---------------------------------------------------------------------
# select_bounded_neighbors
# ---------------------------------------------------------------------


def test_select_returns_top_k_by_centrality():
    a, b, c, d = (uuid4() for _ in range(4))
    candidates = [(a, 0.1), (b, 0.9), (c, 0.5), (d, 0.7)]
    selected = select_bounded_neighbors(
        candidates, next_hop_depth=1, max_fanout=2,
    )
    ids = [s.model_id for s in selected]
    assert ids == [b, d]
    assert all(s.hop_depth == 1 for s in selected)


def test_select_treats_none_centrality_as_zero():
    a, b = uuid4(), uuid4()
    candidates = [(a, None), (b, 0.1)]
    selected = select_bounded_neighbors(
        candidates, next_hop_depth=2, max_fanout=2,
    )
    assert selected[0].model_id == b


def test_select_max_fanout_zero_returns_empty():
    a = uuid4()
    selected = select_bounded_neighbors(
        [(a, 0.5)], next_hop_depth=1, max_fanout=0,
    )
    assert selected == []


def test_select_empty_candidates_returns_empty():
    selected = select_bounded_neighbors(
        [], next_hop_depth=1, max_fanout=10,
    )
    assert selected == []


def test_select_caps_at_max_fanout_default():
    candidates = [(uuid4(), 0.5) for _ in range(50)]
    selected = select_bounded_neighbors(
        candidates, next_hop_depth=1,
    )
    assert len(selected) == RELOCATE_CASCADE_MAX_FANOUT


# ---------------------------------------------------------------------
# damped_magnitude
# ---------------------------------------------------------------------


def test_damped_magnitude_at_depth_zero_is_base():
    assert damped_magnitude(1.0, hop_depth=0) == 1.0


def test_damped_magnitude_depth_n_is_gamma_to_n():
    assert damped_magnitude(1.0, hop_depth=2, gamma=0.5) == pytest.approx(0.25)
    assert damped_magnitude(2.0, hop_depth=3, gamma=0.5) == pytest.approx(0.25)


def test_damped_magnitude_rejects_negative_depth():
    with pytest.raises(ValidationError):
        damped_magnitude(1.0, hop_depth=-1)


def test_damped_magnitude_rejects_invalid_gamma():
    with pytest.raises(ValidationError):
        damped_magnitude(1.0, hop_depth=1, gamma=0.0)
    with pytest.raises(ValidationError):
        damped_magnitude(1.0, hop_depth=1, gamma=1.5)
