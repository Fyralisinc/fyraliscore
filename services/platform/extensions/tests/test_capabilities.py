"""Pure unit tests for the extension capability model (no DB).

Covers: the vocabulary stays in lock-step with `can_read`'s discriminators,
`Capabilities` parse/validate/intersect, the INV-1 guard, and `extension_can_read`.
"""
from __future__ import annotations

import typing
from uuid import uuid4

import pytest

from lib.extensions.host_api.v1 import (
    ALL_CHANNELS,
    Capabilities,
    CapabilityError,
    RESOURCE_KINDS,
    SUBSTRATE_KINDS,
)
from services.platform.access_control.checks import EntityKind, _RESOURCE_KIND_ROLES
from services.platform.access_control.extension_caps import extension_can_read


def test_substrate_kinds_match_can_read_entity_kinds():
    assert SUBSTRATE_KINDS == set(typing.get_args(EntityKind))


def test_resource_kinds_match_can_read_resource_roles():
    assert RESOURCE_KINDS == set(_RESOURCE_KIND_ROLES.keys())


def test_capabilities_parse_and_query():
    c = Capabilities.from_dict(
        {
            "read_channels": ["github:webhook"],
            "substrate_read": ["observation", "model"],
            "resource_kinds": ["financial"],
            "mutate_reasoning": "augment_only",
        }
    )
    assert c.allows_channel("github:webhook")
    assert not c.allows_channel("slack:message")
    assert c.allows_read_kind("observation") and c.allows_read_kind("model")
    assert c.allows_resource_kind("financial")
    assert not c.may_write_reasoning


def test_capabilities_all_channels():
    c = Capabilities.from_dict({"read_channels": ALL_CHANNELS, "substrate_read": ["observation"]})
    assert c.allows_channel("anything:at:all")


def test_capabilities_reject_unknown_kind():
    with pytest.raises(CapabilityError):
        Capabilities.from_dict({"substrate_read": ["nonsense"]})


def test_capabilities_reject_unknown_resource_kind():
    with pytest.raises(CapabilityError):
        Capabilities.from_dict({"resource_kinds": ["nope"]})


def test_capabilities_reject_bad_mutate_reasoning():
    with pytest.raises(CapabilityError):
        Capabilities.from_dict({"mutate_reasoning": "yolo"})


def test_intersection_never_exceeds_declared():
    declared = Capabilities.from_dict(
        {"read_channels": ["github:webhook", "slack:message"], "substrate_read": ["observation", "model"]}
    )
    approved = Capabilities.from_dict(
        {"read_channels": ["github:webhook"], "substrate_read": ["observation"]}
    )
    eff = declared.intersect(approved)
    assert eff.read_channels == ("github:webhook",)
    assert eff.substrate_read == frozenset({"observation"})


def test_intersection_weakens_mutate_reasoning():
    a = Capabilities.from_dict({"mutate_reasoning": "contribute_diff"})
    b = Capabilities.from_dict({"mutate_reasoning": "augment_only"})
    assert a.intersect(b).mutate_reasoning == "augment_only"


# ---- extension_can_read (structural layers only) --------------------
def test_extension_can_read_allows_granted_channel():
    t = uuid4()
    caps = Capabilities.from_dict({"read_channels": ["github:webhook"], "substrate_read": ["observation"]})
    d = extension_can_read(
        caps, {"kind": "observation", "tenant_id": t, "source_channel": "github:webhook"}, tenant_id=t
    )
    assert bool(d) and d.reason == "ext_capability_grant"


def test_extension_can_read_denies_ungranted_channel():
    t = uuid4()
    caps = Capabilities.from_dict({"read_channels": ["github:webhook"], "substrate_read": ["observation"]})
    d = extension_can_read(
        caps, {"kind": "observation", "tenant_id": t, "source_channel": "slack:message"}, tenant_id=t
    )
    assert not bool(d)


def test_extension_can_read_denies_ungranted_kind():
    t = uuid4()
    caps = Capabilities.from_dict({"substrate_read": ["observation"]})
    d = extension_can_read(caps, {"kind": "model", "tenant_id": t}, tenant_id=t)
    assert not bool(d)


def test_extension_can_read_denies_cross_tenant():
    t, other = uuid4(), uuid4()
    caps = Capabilities.from_dict({"read_channels": ALL_CHANNELS, "substrate_read": ["observation"]})
    d = extension_can_read(
        caps, {"kind": "observation", "tenant_id": other, "source_channel": "github:webhook"}, tenant_id=t
    )
    assert not bool(d) and d.reason == "ext_tenant_mismatch"


def test_extension_can_read_resource_kind_gate():
    t = uuid4()
    caps = Capabilities.from_dict({"substrate_read": ["resource"], "resource_kinds": ["financial"]})
    ok = extension_can_read(caps, {"kind": "resource", "tenant_id": t, "resource_kind": "financial"}, tenant_id=t)
    no = extension_can_read(caps, {"kind": "resource", "tenant_id": t, "resource_kind": "ip"}, tenant_id=t)
    assert bool(ok) and not bool(no)
