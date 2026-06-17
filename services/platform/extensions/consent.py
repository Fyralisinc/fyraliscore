"""services/platform/extensions/consent.py — manifest scopes → admin consent → grant (E3.3).

The consent flow a tenant admin walks before an extension can touch their data:

  1. ``consent_screen(manifest)`` renders the scopes the extension *requests*
     (its declared capabilities + the trust ceiling it would write at), for an
     admin to review.
  2. ``approve(...)`` records the decision: the effective grant is
     ``intersection(declared, approved)`` (an extension can never receive more
     than it asked for, and the admin can narrow it), via ``lifecycle.install``.

Public listing requires manual review/signing (E4.2); private per-tenant installs
are self-approved here with a louder screen.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.extensions.host_api.v1 import Capabilities
from lib.extensions.manifest import ExtensionManifest


def consent_screen(manifest: ExtensionManifest) -> dict[str, Any]:
    """The human-reviewable summary of what the extension is asking for."""
    caps = Capabilities.from_dict(manifest.capabilities)
    read_channels = ("ALL" if caps.read_channels == "all"
                     else list(caps.read_channels))
    return {
        "extension_id": manifest.id,
        "version": manifest.version,
        "publisher": manifest.publisher,
        "trust_tier": manifest.trust_tier,
        "requests": {
            "read_channels": read_channels,
            "substrate_read": sorted(caps.substrate_read),
            "write_observations": caps.write_observations,
            "mutate_reasoning": caps.mutate_reasoning,
            "resource_kinds": sorted(caps.resource_kinds),
        },
        "warning": (
            "This extension runs on the developer's own infrastructure and will "
            "receive the data scopes above for your tenant. Approve only scopes you "
            "trust it with; you can narrow them below."
        ),
    }


async def approve(
    pool: Any,
    *,
    tenant_id: UUID,
    manifest: ExtensionManifest,
    approved: Capabilities | dict[str, Any] | None,
    granted_by: str,
    trust_ceiling: str = "inferential_external",
) -> Any:
    """Record the admin's approval as an effective grant + enable the extension.

    ``approved`` narrows the manifest's declared capabilities (None = approve all
    declared). Delegates to ``lifecycle.install`` which performs the
    intersection(declared, approved) and the host-API compatibility check."""
    from services.platform.extensions import lifecycle

    approved_caps = (
        approved if isinstance(approved, Capabilities)
        else Capabilities.from_dict(approved) if approved is not None
        else None
    )
    return await lifecycle.install(
        pool, tenant_id=tenant_id, manifest=manifest,
        requested_capabilities=approved_caps, trust_ceiling=trust_ceiling,
        granted_by=granted_by, enable=True,
    )


__all__ = ["consent_screen", "approve"]
