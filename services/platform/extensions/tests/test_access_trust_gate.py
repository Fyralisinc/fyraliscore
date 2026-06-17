"""The first-party fast-path is gated on a HOST-owned allowlist, not the
self-declared manifest trust_tier (pure; no DB).

Covers the trust_tier-spoof fix: an installed package that simply declares
``trust_tier="first_party"`` (+ no feature_flag) must NOT be auto-enabled — only
ids the operator allowlists via FYRALIS_FIRST_PARTY_EXTENSION_IDS get the
no-grant / implicit-enablement fast path.
"""
from __future__ import annotations

from uuid import uuid4

from lib.extensions.manifest import ExtensionManifest
from services.platform.extensions.access import _is_trusted_first_party, is_enabled


def _fp(ext_id: str = "spoofy") -> ExtensionManifest:
    return ExtensionManifest(id=ext_id, trust_tier="first_party", feature_flag=None)


def test_self_declared_first_party_not_trusted_without_allowlist(monkeypatch):
    monkeypatch.delenv("FYRALIS_FIRST_PARTY_EXTENSION_IDS", raising=False)
    assert _is_trusted_first_party(_fp()) is False


def test_first_party_trusted_only_when_allowlisted(monkeypatch):
    monkeypatch.setenv("FYRALIS_FIRST_PARTY_EXTENSION_IDS", "other,spoofy , third")
    assert _is_trusted_first_party(_fp("spoofy")) is True
    assert _is_trusted_first_party(_fp("not-listed")) is False
    # A third-party manifest is never trusted, even if its id is allowlisted.
    monkeypatch.setenv("FYRALIS_FIRST_PARTY_EXTENSION_IDS", "evil")
    assert _is_trusted_first_party(
        ExtensionManifest(id="evil", trust_tier="third_party", feature_flag=None)
    ) is False


async def test_is_enabled_no_flag_follows_allowlist(monkeypatch):
    # feature_flag=None ⇒ is_enabled is purely the host-trust decision (no pool use).
    monkeypatch.delenv("FYRALIS_FIRST_PARTY_EXTENSION_IDS", raising=False)
    assert await is_enabled(None, tenant_id=uuid4(), manifest=_fp("ext-x")) is False
    monkeypatch.setenv("FYRALIS_FIRST_PARTY_EXTENSION_IDS", "ext-x")
    assert await is_enabled(None, tenant_id=uuid4(), manifest=_fp("ext-x")) is True
