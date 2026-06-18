"""Pure unit tests for governance helpers (M5) — consent screen + provenance mapping.
No DB.
"""
from __future__ import annotations

from lib.extensions.manifest import ExtensionManifest
from services.platform.extensions.consent import consent_screen
from services.platform.extensions.provenance import source_identity_for_channel


def test_consent_screen_renders_requested_scopes():
    m = ExtensionManifest(
        id="acme", version="1.2.0", publisher="Acme", trust_tier="third_party",
        capabilities={"read_channels": ["github:webhook"], "substrate_read": ["observation"],
                      "write_observations": True, "mutate_reasoning": "none"},
    )
    screen = consent_screen(m)
    assert screen["extension_id"] == "acme" and screen["publisher"] == "Acme"
    assert screen["requests"]["read_channels"] == ["github:webhook"]
    assert screen["requests"]["substrate_read"] == ["observation"]
    assert screen["requests"]["write_observations"] is True
    assert "warning" in screen


def test_provenance_source_identity_mapping():
    assert source_identity_for_channel("ext:github_intel:risk") == ("extension:github_intel", True)
    assert source_identity_for_channel("ext:acme:a:b") == ("extension:acme", True)
    ident, third = source_identity_for_channel("github:webhook")
    assert ident == "channel:github:webhook" and third is False
