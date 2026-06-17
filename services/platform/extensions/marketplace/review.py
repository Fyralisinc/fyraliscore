"""services/platform/extensions/marketplace/review.py — automated submission gate (E4.2).

Both private and public submissions pass this automated gate; only public *listing*
additionally needs a human reviewer + signature. The gate is host-side (it must not
trust the SDK's own validation): manifest lint, scope justification, and
callback-domain verification.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from lib.extensions.host_api.v1 import HOST_API_VERSION, Capabilities, CapabilityError
from lib.extensions.manifest import TRUST_TIERS

_BLOCKED_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


@dataclass
class GateResult:
    passed: bool
    problems: list[str] = field(default_factory=list)
    checks: dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "problems": self.problems, "checks": self.checks}


def automated_gate(
    manifest: dict[str, Any], *, visibility: str = "private", callback_url: str | None = None,
) -> GateResult:
    """Run the automated review gate over a submitted manifest dict."""
    problems: list[str] = []
    checks: dict[str, bool] = {}
    callback_url = callback_url or manifest.get("callback_url")

    # ---- manifest lint ----
    ext_id = (manifest.get("id") or "").strip()
    checks["id"] = bool(ext_id)
    if not ext_id:
        problems.append("id: required")

    version = manifest.get("version") or "0.0.0"
    try:
        Version(version)
        checks["version"] = True
    except InvalidVersion:
        checks["version"] = False
        problems.append(f"version: {version!r} is not a valid version")

    trust_tier = manifest.get("trust_tier", "third_party")
    checks["trust_tier"] = trust_tier in TRUST_TIERS
    if not checks["trust_tier"]:
        problems.append(f"trust_tier: {trust_tier!r} not in {TRUST_TIERS}")

    engines = manifest.get("engines_fyralis_host_api", ">=1.0,<2.0")
    try:
        if HOST_API_VERSION not in SpecifierSet(engines):
            checks["engines"] = False
            problems.append(f"engines_fyralis_host_api: {engines!r} excludes host {HOST_API_VERSION}")
        else:
            checks["engines"] = True
    except InvalidSpecifier:
        checks["engines"] = False
        problems.append(f"engines_fyralis_host_api: {engines!r} is not a valid range")

    publisher = (manifest.get("publisher") or "").strip()
    checks["publisher"] = bool(publisher) and publisher != "unknown"
    if not checks["publisher"]:
        problems.append("publisher: a real publisher is required for listing")

    checks["contributes"] = bool(manifest.get("contributes"))
    if not checks["contributes"]:
        problems.append("contributes: declare at least one seam")

    # ---- scope justification (capabilities parse + INV-1 first-party-only) ----
    caps = None
    try:
        caps = Capabilities.from_dict(manifest.get("capabilities") or {})
        checks["capabilities_parse"] = True
    except CapabilityError as exc:
        checks["capabilities_parse"] = False
        problems.append(f"capabilities: {exc}")

    if caps is not None and trust_tier != "first_party":
        if caps.mutate_reasoning == "contribute_diff":
            problems.append("capabilities.mutate_reasoning: contribute_diff is first-party-only (INV-1)")
        if caps.substrate_write:
            problems.append("capabilities.substrate_write: not grantable to a non-first-party publisher")
    checks["inv1_respected"] = not any("first-party-only" in p or "not grantable" in p for p in problems)

    # ---- callback-domain verification ----
    if callback_url:
        checks["callback_https"] = _callback_ok(callback_url, public=visibility == "public", problems=problems)
    elif visibility == "public" and caps is not None and caps.write_observations:
        # a public extension that writes back should register a verifiable callback
        checks["callback_https"] = False
        problems.append("callback_url: required for a public extension that writes observations")

    return GateResult(passed=not problems, problems=problems, checks=checks)


def _callback_ok(url: str, *, public: bool, problems: list[str]) -> bool:
    try:
        u = urlparse(url)
    except ValueError:
        problems.append("callback_url: unparseable")
        return False
    if u.scheme != "https":
        problems.append("callback_url: must be https")
        return False
    host = (u.hostname or "").lower()
    if not host:
        problems.append("callback_url: missing host")
        return False
    if public and (host in _BLOCKED_HOSTS or host.endswith(".local")):
        problems.append(f"callback_url: {host!r} is not a public, verifiable domain")
        return False
    return True


__all__ = ["automated_gate", "GateResult"]
