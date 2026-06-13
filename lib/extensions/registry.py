"""lib.extensions.registry — the validating manifest loader.

Wraps raw manifest discovery (``lib.extensions.manifest.discovered_manifests``)
with **host-API version enforcement**: a manifest whose
``engines_fyralis_host_api`` SemVer range does not admit the running
``HOST_API_VERSION`` is rejected (logged, surfaced at ``/debug/interfaces``, and
excluded from the active set), so an extension built against an incompatible host
can't silently load. This is the SemVer-pin discipline ADR-0004 §A.4 calls for.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from lib.extensions.host_api.v1 import HOST_API_VERSION
from lib.extensions.manifest import ExtensionManifest, discovered_manifests

log = logging.getLogger("extensions.registry")


@dataclass(frozen=True)
class RejectedManifest:
    manifest: ExtensionManifest
    reason: str


def host_api_compatible(spec: str) -> bool:
    """Does the running HOST_API_VERSION satisfy ``spec`` (a SemVer range)?"""
    try:
        return Version(HOST_API_VERSION) in SpecifierSet(spec)
    except (InvalidSpecifier, InvalidVersion):
        return False


def load_manifests() -> tuple[list[ExtensionManifest], list[RejectedManifest]]:
    """Return ``(compatible, rejected)`` manifests.

    Compatible = the host API version satisfies the manifest's
    ``engines_fyralis_host_api`` range. Rejected manifests are logged with a
    reason and excluded from the active set.
    """
    compatible: list[ExtensionManifest] = []
    rejected: list[RejectedManifest] = []
    for man in discovered_manifests():
        spec = man.engines_fyralis_host_api
        try:
            ok = Version(HOST_API_VERSION) in SpecifierSet(spec)
        except (InvalidSpecifier, InvalidVersion) as exc:
            rejected.append(RejectedManifest(man, f"invalid engines range {spec!r}: {exc}"))
            log.error("interface_rejected id=%s reason=invalid_engines spec=%s", man.id, spec)
            continue
        if ok:
            compatible.append(man)
        else:
            reason = (
                f"requires host API {spec}, running {HOST_API_VERSION}"
            )
            rejected.append(RejectedManifest(man, reason))
            log.warning("interface_rejected id=%s reason=incompatible %s", man.id, reason)
    return compatible, rejected


def active_manifests() -> list[ExtensionManifest]:
    """Just the compatible manifests (the active set)."""
    return load_manifests()[0]


__all__ = [
    "RejectedManifest",
    "host_api_compatible",
    "load_manifests",
    "active_manifests",
]
