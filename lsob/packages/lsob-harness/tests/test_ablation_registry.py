"""Tests for the harness ablation registry."""

from __future__ import annotations

import pytest

from lsob_contracts import AblationConfig

from lsob_harness.ablation import (
    REGISTRY,
    AblationRegistry,
)


EXPECTED_NAMES = {
    "none",
    "no-bridge",
    "no-calibration",
    "no-second-pass",
    "no-activation",
    "no-entity-resolver",
    "no-pattern-precipitation",
    "no-model-composition",
    "all-off",
}


def test_registry_contains_all_named_ablations() -> None:
    names = set(REGISTRY.list_names())
    assert EXPECTED_NAMES <= names


def test_registry_get_canonical_dash() -> None:
    cfg = REGISTRY.get("no-bridge")
    assert cfg.name == "no-bridge"
    assert cfg.disable_bridge is True


def test_registry_get_accepts_underscore_alias() -> None:
    cfg_dash = REGISTRY.get("no-calibration")
    cfg_under = REGISTRY.get("no_calibration")
    assert cfg_dash.disable_calibration is True
    assert cfg_under.disable_calibration is True
    assert cfg_dash.name == cfg_under.name == "no-calibration"


def test_registry_all_off_has_every_flag_set() -> None:
    cfg = REGISTRY.get("all-off")
    assert cfg.disable_bridge
    assert cfg.disable_calibration
    assert cfg.disable_second_pass
    assert cfg.disable_activation
    assert cfg.disable_entity_resolver
    assert cfg.disable_pattern_precipitation
    assert cfg.disable_model_composition
    assert cfg.any_disabled()


def test_registry_none_has_no_flags() -> None:
    cfg = REGISTRY.get("none")
    assert not cfg.any_disabled()


def test_registry_unknown_name_raises() -> None:
    with pytest.raises(KeyError):
        REGISTRY.get("no-such-ablation")


def test_registry_register_and_override() -> None:
    reg = AblationRegistry()
    reg.register("custom", AblationConfig(name="custom", disable_bridge=True))
    assert "custom" in reg
    assert reg.get("custom").disable_bridge is True
    # Re-registration replaces the entry.
    reg.register("custom", AblationConfig(name="custom", disable_calibration=True))
    refreshed = reg.get("custom")
    assert refreshed.disable_calibration is True
    assert refreshed.disable_bridge is False


def test_registry_list_names_is_sorted() -> None:
    names = REGISTRY.list_names()
    assert names == sorted(names)
