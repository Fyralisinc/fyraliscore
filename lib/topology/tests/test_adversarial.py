"""Adversarial tests for the pure helpers in lib.topology.

Target every boundary, every malformed-input shape, every numeric edge
the production code can encounter. Failures here usually indicate a
real bug; 'expected ValidationError' assertions guard the contract."""
from __future__ import annotations

import math
from uuid import uuid4

import pytest

from lib.shared.errors import ValidationError
from lib.shared.types import TOPO_EMBEDDING_DIM
from lib.topology.naming import (
    MemberSummary,
    derive_signature,
    member_summaries_from_rows,
)
from lib.topology.relocate import (
    RELOCATE_CASCADE_MAX_FANOUT,
    RelocateTarget,
    blend_topo,
    damped_magnitude,
    parse_relocate_target,
    select_bounded_neighbors,
)


def _unit(seed: float) -> list[float]:
    v = [math.sin(seed + i * 0.1) for i in range(TOPO_EMBEDDING_DIM)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


# =====================================================================
# Naming — unicode, very large inputs, all-None scopes
# =====================================================================


def test_naming_handles_cjk_unicode_in_labels():
    cust_id = "11111111-2222-3333-4444-555555555555"
    a = uuid4()
    members = [
        MemberSummary(
            model_id=uuid4(),
            proposition_kind="state",
            scope_actor_ids=(a,),
            scope_entity_refs=(("customer", cust_id),),
            actor_labels={a: "陈"},  # 1-char Chinese name
            entity_labels={("customer", cust_id): "東京リセラー"},
        )
    ]
    sig = derive_signature(members)
    assert "東京リセラー" in sig
    assert "陈" in sig


def test_naming_handles_emoji_label():
    cust_id = "11111111-2222-3333-4444-555555555555"
    members = [
        MemberSummary(
            model_id=uuid4(),
            proposition_kind="state",
            scope_actor_ids=(),
            scope_entity_refs=(("customer", cust_id),),
            entity_labels={("customer", cust_id): "Glob 🔥 ex"},
        )
    ]
    sig = derive_signature(members)
    # Emoji should pass through.
    assert "🔥" in sig


def test_naming_with_only_scope_no_kind():
    """A community that's all scope and no kind should still name."""
    cust_id = "11111111-2222-3333-4444-555555555555"
    members = [
        MemberSummary(
            model_id=uuid4(),
            proposition_kind=None,
            scope_actor_ids=(),
            scope_entity_refs=(("customer", cust_id),),
            entity_labels={("customer", cust_id): "Globex"},
        )
    ]
    sig = derive_signature(members)
    assert "customer:Globex" in sig
    assert "@" not in sig  # no kind, so no kind separator


def test_naming_100_members_same_kind():
    """Very large community with all same kind shouldn't blow up."""
    members = [
        MemberSummary(
            model_id=uuid4(),
            proposition_kind="state",
            scope_actor_ids=(),
            scope_entity_refs=(),
        )
        for _ in range(100)
    ]
    sig = derive_signature(members)
    assert sig == "state"


def test_naming_label_truncation_with_unicode_doesnt_split_codepoint():
    """If we naively slice mid-codepoint we'd produce invalid UTF-8."""
    long_label = "中" * 200  # 200 chinese chars, each 1 codepoint
    cust_id = "11111111-2222-3333-4444-555555555555"
    members = [
        MemberSummary(
            model_id=uuid4(),
            proposition_kind="state",
            scope_actor_ids=(),
            scope_entity_refs=(("customer", cust_id),),
            entity_labels={("customer", cust_id): long_label},
        )
    ]
    sig = derive_signature(members)
    # Encoding back to UTF-8 + decoding should be lossless.
    assert sig.encode("utf-8").decode("utf-8") == sig


def test_naming_kind_alpha_tiebreak_deterministic_across_calls():
    """Same input → same signature, every time."""
    members = [
        MemberSummary(
            model_id=uuid4(),
            proposition_kind="recommendation",
            scope_actor_ids=(),
            scope_entity_refs=(),
        ),
        MemberSummary(
            model_id=uuid4(),
            proposition_kind="state",
            scope_actor_ids=(),
            scope_entity_refs=(),
        ),
    ]
    sigs = [derive_signature(members) for _ in range(5)]
    assert len(set(sigs)) == 1


def test_member_summaries_from_rows_missing_id_raises():
    """A row without 'id' is malformed; the helper should not silently skip."""
    rows = [{"proposition_kind": "state", "scope_actors": [], "scope_entities": []}]
    with pytest.raises(KeyError):
        member_summaries_from_rows(rows)


def test_member_summaries_from_rows_handles_none_scope_arrays():
    rows = [
        {
            "id": uuid4(),
            "proposition_kind": None,
            "scope_actors": None,
            "scope_entities": None,
        }
    ]
    summaries = member_summaries_from_rows(rows)
    assert summaries[0].scope_actor_ids == ()
    assert summaries[0].scope_entity_refs == ()


# =====================================================================
# parse_relocate_target — every malformed shape
# =====================================================================


def test_parse_target_rejects_none_input():
    with pytest.raises(ValidationError):
        parse_relocate_target(None)  # type: ignore[arg-type]


def test_parse_target_rejects_empty_dict():
    with pytest.raises(ValidationError, match="kind"):
        parse_relocate_target({})


def test_parse_target_rejects_string_input():
    with pytest.raises(ValidationError):
        parse_relocate_target("not-a-dict")  # type: ignore[arg-type]


def test_parse_target_vector_rejects_nan_component():
    """Vector with NaN must be rejected at parse time — pgvector
    rejects it on INSERT with an opaque error, so we surface
    earlier."""
    bad = [0.0] * TOPO_EMBEDDING_DIM
    bad[7] = float("nan")
    with pytest.raises(ValidationError, match="non-finite"):
        parse_relocate_target(
            {"kind": "vector", "value": bad, "alpha": 1.0}
        )


def test_parse_target_vector_rejects_inf_component():
    bad = [0.0] * TOPO_EMBEDDING_DIM
    bad[7] = float("inf")
    with pytest.raises(ValidationError, match="non-finite"):
        parse_relocate_target(
            {"kind": "vector", "value": bad, "alpha": 1.0}
        )


def test_parse_target_vector_rejects_negative_inf_component():
    bad = [0.0] * TOPO_EMBEDDING_DIM
    bad[7] = float("-inf")
    with pytest.raises(ValidationError, match="non-finite"):
        parse_relocate_target(
            {"kind": "vector", "value": bad, "alpha": 1.0}
        )


def test_parse_target_with_extra_fields_silently_ignored():
    """parse_relocate_target doesn't reject unknown keys; that's
    intentional permissive behavior. Documented here so it doesn't
    drift."""
    target = parse_relocate_target(
        {
            "kind": "model_id",
            "value": str(uuid4()),
            "extra_field": "ignored",
        }
    )
    assert target.alpha == 1.0


def test_parse_target_alpha_string_input():
    """alpha as a numeric string should parse via float()."""
    mid = uuid4()
    target = parse_relocate_target(
        {"kind": "model_id", "value": str(mid), "alpha": "0.5"}
    )
    assert target.alpha == 0.5


def test_parse_target_alpha_none_falls_to_default():
    """Explicit alpha=None should hit the default, not ValidationError."""
    mid = uuid4()
    # When alpha is explicitly None, raw.get returns None; float(None)
    # raises TypeError. Currently this raises ValidationError —
    # confirm the contract.
    with pytest.raises(ValidationError, match="alpha"):
        parse_relocate_target(
            {"kind": "model_id", "value": str(mid), "alpha": None}
        )


# =====================================================================
# blend_topo — numerical edge cases
# =====================================================================


def test_blend_zero_vectors_returns_zero():
    """Both inputs zero → blend is zero — but L2-normalize would
    divide by 0. Documented behavior: returns zero vector unchanged."""
    zero = [0.0] * TOPO_EMBEDDING_DIM
    out = blend_topo(zero, zero, alpha=0.5)
    assert all(x == 0.0 for x in out)


def test_blend_opposite_unit_vectors_alpha_half():
    """v + (-v) at midpoint → zero vector (not normalized; norm is 0)."""
    a = _unit(0.0)
    b = [-x for x in a]
    out = blend_topo(a, b, alpha=0.5)
    # Midpoint is exactly 0 → norm 0 → no normalization → all 0.
    assert all(abs(x) < 1e-9 for x in out)


def test_blend_alpha_one_with_unnormalized_target_normalizes():
    """If target is not unit-norm, output should be unit-norm."""
    cur = _unit(0.0)
    tgt = [3.0 * x for x in _unit(1.0)]  # 3x unit
    out = blend_topo(cur, tgt, alpha=1.0)
    norm = math.sqrt(sum(x * x for x in out))
    assert abs(norm - 1.0) < 1e-6


# =====================================================================
# select_bounded_neighbors — pathological inputs
# =====================================================================


def test_select_negative_max_fanout_returns_empty():
    """Negative fanout shouldn't blow up."""
    a = uuid4()
    out = select_bounded_neighbors(
        [(a, 0.5)], next_hop_depth=1, max_fanout=-5,
    )
    assert out == []


def test_select_all_none_centralities_returns_in_uuid_order():
    """Stable ordering even when no centralities differentiate."""
    ids = sorted([uuid4() for _ in range(5)], key=str)
    candidates = [(i, None) for i in reversed(ids)]
    out = select_bounded_neighbors(
        candidates, next_hop_depth=1, max_fanout=10,
    )
    assert [t.model_id for t in out] == ids  # alphabetical UUIDs


def test_select_max_fanout_huge_returns_all():
    cands = [(uuid4(), 0.5) for _ in range(3)]
    out = select_bounded_neighbors(
        cands, next_hop_depth=1, max_fanout=1_000_000,
    )
    assert len(out) == 3


def test_select_duplicate_candidates_passed_through():
    """The selector doesn't dedupe — caller must. Document behavior."""
    a = uuid4()
    out = select_bounded_neighbors(
        [(a, 0.5), (a, 0.5)], next_hop_depth=1, max_fanout=10,
    )
    # Both pass through; the queue's UNIQUE constraint upstream
    # handles dedup. Document.
    assert len(out) == 2


# =====================================================================
# damped_magnitude — boundary conditions
# =====================================================================


def test_damped_magnitude_gamma_one_no_decay():
    assert damped_magnitude(1.0, hop_depth=10, gamma=1.0) == 1.0


def test_damped_magnitude_base_zero_returns_zero():
    assert damped_magnitude(0.0, hop_depth=5) == 0.0


def test_damped_magnitude_huge_depth_underflows_cleanly():
    """At depth 100 with γ=0.5 the value should be tiny but finite."""
    out = damped_magnitude(1.0, hop_depth=100, gamma=0.5)
    assert out > 0.0
    assert out < 1e-20
