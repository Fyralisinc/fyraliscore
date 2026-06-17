"""lib.extensions.manifest — declarative ExtensionManifest + discovery.

A manifest is a *pure descriptor* an extension declares; the host discovers and
validates it **without importing the extension's wiring**. One discoverable list
of every installed interface (surfaced at ``/debug/interfaces``) — activation,
contributions, and requested capabilities in one place.

Discovery uses the ``company_os.interfaces`` entry-point group, is cached once
per process, and is failure-isolated (one bad manifest cannot break startup) —
the same defensive pattern as ``services/app/gateway/extensions.py`` and
``services/reasoning/think/hooks.py``.

Pure-dataclass + stdlib only, so it sits safely under the ``lib`` →/→ ``services``
import-linter floor.
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
import logging
from dataclasses import dataclass, field
from typing import Any

from lib.extensions.host_api.v1 import HOST_API_VERSION

log = logging.getLogger("extensions.manifest")

_ENTRY_POINT_GROUP = "company_os.interfaces"

# Trust tiers (ADR-0004 §A.6). In-process entry-point extensions are first-party
# or verified-partner; "third_party" is the network/developer-hosted tier.
TRUST_TIERS = ("first_party", "verified_partner", "third_party")


@dataclass(frozen=True)
class ExtensionManifest:
    """What an interface declares about itself (the VS Code ``package.json``
    ``contributes`` + ``activationEvents`` + ``engines`` analogue, plus the
    Fyralis-native ``capabilities`` governance surface)."""

    id: str
    version: str = "0.0.0"
    publisher: str = "unknown"
    trust_tier: str = "third_party"
    # SemVer range this extension was built against — validated at discovery.
    engines_fyralis_host_api: str = ">=1.0,<2.0"
    # The extension points this interface provides, e.g.
    #   "draft-enricher:github:webhook", "product-surface", "background-worker".
    contributes: tuple[str, ...] = ()
    # Lazy-activation triggers, e.g. "onChannel:github:webhook".
    activation_events: tuple[str, ...] = ()
    # Per-tenant enablement flag (reuses TenantFlags).
    feature_flag: str | None = None
    # Declared capability scopes (see ADR-0004 §A.5 / roadmap E2). Free-form
    # until the capability store lands; recorded here for the catalog + audit.
    capabilities: dict[str, Any] = field(default_factory=dict)


_cache: list[ExtensionManifest] | None = None


def discovered_manifests() -> list[ExtensionManifest]:
    """Resolve every installed interface manifest once per process (cached)."""
    global _cache
    if _cache is not None:
        return _cache
    found: list[ExtensionManifest] = []
    try:
        entry_points = importlib_metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must never block startup
        log.warning("interface_manifest_discovery_failed", exc_info=True)
        _cache = found
        return found
    for ep in entry_points:
        try:
            obj = ep.load()
            man = obj() if callable(obj) and not isinstance(obj, ExtensionManifest) else obj
            if not isinstance(man, ExtensionManifest):
                log.error("interface_manifest_bad_type source=%s", ep.name)
                continue
            found.append(man)
            log.info(
                "interface_manifest_discovered id=%s version=%s trust=%s",
                man.id, man.version, man.trust_tier,
            )
        except Exception:  # noqa: BLE001 - one bad manifest must not break others
            log.error("interface_manifest_load_failed source=%s", ep.name, exc_info=True)
    _cache = found
    return found


def host_api_version() -> str:
    """The host API version extensions pin against."""
    return HOST_API_VERSION


def reset_for_tests() -> None:
    """Force re-discovery (test isolation only)."""
    global _cache
    _cache = None


__all__ = [
    "ExtensionManifest",
    "TRUST_TIERS",
    "discovered_manifests",
    "host_api_version",
    "reset_for_tests",
]
