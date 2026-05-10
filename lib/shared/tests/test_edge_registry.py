"""
lib/shared/tests/test_edge_registry.py — pure-Python unit tests for
the edge_registry. No DB; the registry is a declarative module.
"""
from __future__ import annotations

import pytest

from lib.shared.edge_registry import (
    EDGE_REGISTRY,
    EdgeRegistryError,
    assert_writable,
    cycle_scope_for,
    get_spec,
    is_symmetric,
    legacy_supports_cause_kind,
    validate_weight,
    writable_kinds,
)


def test_registry_has_expected_kinds():
    """v1 must register exactly six edge kinds: four enabled
    producers + two reserved names (`contradicts`, `weakens`)."""
    assert set(EDGE_REGISTRY.keys()) == {
        "supports",
        "contributes_to_resolution",
        "instance_of",
        "superseded_by",
        "contradicts",
        "weakens",
    }


def test_writable_kinds_excludes_reserved():
    """Reserved names live in the registry but the repo refuses to
    write them in v1 (no producer)."""
    assert writable_kinds() == {
        "supports",
        "contributes_to_resolution",
        "instance_of",
        "superseded_by",
    }


def test_get_spec_unknown_raises():
    with pytest.raises(EdgeRegistryError) as exc:
        get_spec("nonexistent_kind")
    assert "unknown edge_kind" in str(exc.value)


def test_assert_writable_rejects_reserved():
    """contradicts is reserved → repo must reject INSERTs of it."""
    with pytest.raises(EdgeRegistryError) as exc:
        assert_writable("contradicts")
    assert "reserved" in str(exc.value)


def test_assert_writable_accepts_supports():
    spec = assert_writable("supports")
    assert spec.name == "supports"
    assert spec.enabled_for_writes is True


def test_supports_is_directed_dag():
    spec = get_spec("supports")
    assert spec.is_directed is True
    assert spec.cycle_scope == frozenset({"supports", "instance_of"})
    assert spec.on_source_archive is not None
    assert spec.on_target_archive is None


def test_contributes_to_resolution_dag_isolated():
    """contributes_to_resolution cycles only against itself; mixing
    with `supports` would produce spurious rejections (predictions
    can be supported by their own resolvers)."""
    spec = get_spec("contributes_to_resolution")
    assert spec.cycle_scope == frozenset({"contributes_to_resolution"})


def test_instance_of_cycle_scope_shared_with_supports():
    """A Model cannot transitively support its own pattern via
    either edge."""
    spec = get_spec("instance_of")
    assert spec.cycle_scope == frozenset({"supports", "instance_of"})
    assert spec.on_source_archive is not None
    assert spec.on_target_archive is not None


def test_superseded_by_chain_only():
    spec = get_spec("superseded_by")
    assert spec.cycle_scope == frozenset({"superseded_by"})
    # Supersession has no cascade — supports cascade handles
    # dependents already.
    assert spec.on_source_archive is None
    assert spec.on_target_archive is None


def test_contradicts_is_symmetric_no_dag():
    spec = get_spec("contradicts")
    assert spec.is_directed is False  # symmetric → 2 rows
    assert spec.cycle_scope is None
    assert spec.weight_required is True


def test_is_symmetric_helper():
    assert is_symmetric("contradicts") is True
    assert is_symmetric("supports") is False
    assert is_symmetric("instance_of") is False


def test_cycle_scope_helper():
    assert cycle_scope_for("supports") == frozenset(
        {"supports", "instance_of"}
    )
    assert cycle_scope_for("contradicts") is None


def test_validate_weight_supports_optional():
    """supports allows weight but doesn't require it."""
    validate_weight("supports", None)  # OK
    validate_weight("supports", 0.5)   # OK
    with pytest.raises(EdgeRegistryError):
        validate_weight("supports", 1.5)  # out of range


def test_validate_weight_contradicts_required():
    """contradicts requires a weight (none of the dual-weighted
    `tension_with` vs `directly_inconsistent` ambiguity)."""
    with pytest.raises(EdgeRegistryError) as exc:
        validate_weight("contradicts", None)
    assert "requires a weight" in str(exc.value)


def test_validate_weight_superseded_forbids():
    """superseded_by is binary; a weight on it would be meaningless."""
    validate_weight("superseded_by", None)  # OK
    with pytest.raises(EdgeRegistryError) as exc:
        validate_weight("superseded_by", 0.5)
    assert "forbids weight" in str(exc.value)


def test_validate_weight_negative_rejected():
    with pytest.raises(EdgeRegistryError):
        validate_weight("supports", -0.1)


def test_legacy_supports_cause_kind_pre_s1_taxonomy():
    """The five pre-S1 cause_kinds map deterministically to their
    archive_reason. Default for unknown reasons preserves the
    pre-S1 behavior of 'supporting_archived'."""
    assert legacy_supports_cause_kind("deprecated") == "supporting_deprecated"
    assert legacy_supports_cause_kind("superseded") == "supporting_superseded"
    assert (
        legacy_supports_cause_kind("falsifier_triggered")
        == "falsifier_triggered_upstream"
    )
    assert (
        legacy_supports_cause_kind("contested_incorrect")
        == "contested_cluster"
    )
    assert (
        legacy_supports_cause_kind("contested_reading_incorrect")
        == "contested_cluster"
    )
    # Default fallback.
    assert legacy_supports_cause_kind("manual") == "supporting_archived"
    assert (
        legacy_supports_cause_kind("totally_unknown")
        == "supporting_archived"
    )


def test_specs_are_frozen():
    """EdgeKindSpec is frozen; can't be mutated post-import."""
    spec = get_spec("supports")
    with pytest.raises(Exception):
        spec.weight_required = True  # type: ignore[misc]
