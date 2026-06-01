"""Unit tests for live scenario dataclasses + presets."""
from __future__ import annotations

from services.synthetic.scenarios import (
    BURSTY_PUBSUB,
    MIXED_PUBSUB,
    STEADY_STATE_PUBSUB,
    LivePubSubScenario,
    PerTenantBurst,
)


def test_steady_state_preset_has_ten_iterations() -> None:
    assert isinstance(STEADY_STATE_PUBSUB, LivePubSubScenario)
    assert len(STEADY_STATE_PUBSUB.tenants) == 1
    assert len(STEADY_STATE_PUBSUB.tenants[0].burst_pattern) == 10


def test_bursty_preset_shape() -> None:
    assert BURSTY_PUBSUB.tenants[0].burst_pattern[0] == (0, 50)


def test_mixed_preset_has_five_tenants() -> None:
    assert len(MIXED_PUBSUB.tenants) == 5
    slugs = {t.tenant_slug for t in MIXED_PUBSUB.tenants}
    assert slugs == {f"mixed-{i}" for i in range(5)}


def test_per_tenant_burst_is_frozen() -> None:
    t = PerTenantBurst(
        tenant_slug="x", mailbox_email="x@y.com",
        burst_pattern=[(0, 1)],
    )
    import pytest
    with pytest.raises(Exception):
        t.tenant_slug = "y"  # type: ignore[misc]


def test_live_pubsub_scenario_defaults() -> None:
    s = LivePubSubScenario(tenants=[
        PerTenantBurst(
            tenant_slug="t", mailbox_email="t@y.com",
            burst_pattern=[(0, 1)],
        ),
    ])
    assert s.replay_probability == 0.0
