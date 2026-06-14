"""Pure unit tests for the marketplace gate + signing (M8 / E4). No DB."""
from __future__ import annotations

from services.platform.extensions.marketplace import signing
from services.platform.extensions.marketplace.review import automated_gate

GOOD = {
    "id": "acme", "version": "1.0.0", "publisher": "Acme Inc", "trust_tier": "third_party",
    "engines_fyralis_host_api": ">=1.0,<2.0", "contributes": ["draft-enricher:github:webhook"],
    "capabilities": {"read_channels": ["github:webhook"], "substrate_read": ["observation"]},
}


def test_gate_passes_good_manifest():
    r = automated_gate(GOOD)
    assert r.passed and r.problems == []
    assert r.checks["publisher"] and r.checks["contributes"]


def test_gate_flags_missing_publisher_and_contributes():
    bad = {**GOOD, "publisher": "unknown", "contributes": []}
    r = automated_gate(bad)
    assert not r.passed
    assert any("publisher" in p for p in r.problems)
    assert any("contributes" in p for p in r.problems)


def test_gate_enforces_inv1_for_third_party():
    bad = {**GOOD, "capabilities": {"mutate_reasoning": "contribute_diff"}}
    r = automated_gate(bad)
    assert not r.passed and any("first-party-only" in p for p in r.problems)
    # first-party may
    ok = automated_gate({**bad, "trust_tier": "first_party"})
    assert ok.passed


def test_gate_public_callback_rules():
    writes = {**GOOD, "capabilities": {**GOOD["capabilities"], "write_observations": True}}
    # public writer with no callback -> fail
    assert not automated_gate(writes, visibility="public").passed
    # public writer with http callback -> fail (must be https)
    assert not automated_gate({**writes, "callback_url": "http://x.com/h"}, visibility="public").passed
    # public writer with localhost https -> fail (not a public domain)
    assert not automated_gate({**writes, "callback_url": "https://localhost/h"}, visibility="public").passed
    # public writer with a real https callback -> pass
    assert automated_gate({**writes, "callback_url": "https://ext.acme.com/h"}, visibility="public").passed


def test_signing_roundtrip_and_tamper():
    sig = signing.sign(GOOD)
    assert sig.startswith("v1=")
    assert signing.verify(GOOD, sig) is True
    assert signing.verify({**GOOD, "version": "9.9.9"}, sig) is False
    assert signing.verify(GOOD, None) is False
    # canonical form is key-order independent
    assert signing.canonical({"a": 1, "b": 2}) == signing.canonical({"b": 2, "a": 1})
