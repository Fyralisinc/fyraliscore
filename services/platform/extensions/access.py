"""services.platform.extensions.access — enablement + capability resolution.

Two separate concerns (ADR-0004 §A.5):
  * **enablement** = the per-tenant feature flag (``TenantFlags``);
  * **what it may do once enabled** = the ``extension_grants`` capability set.

``resolve_capabilities`` combines them into the effective ``Capabilities`` (or
``None`` when the extension isn't usable for that tenant), running in
**"enforce, but first-party is fully granted"** mode: a first-party manifest with
no explicit grant row falls back to exactly its declared capabilities, so the
four existing first-party interfaces become auditable without a migration of
operator consent. Third-party / verified-partner extensions get **nothing**
without an explicit grant.

``reader_for`` is the one call a host gives an extension's read path: it returns a
ready ``CapabilityScopedReader`` (or ``None`` if disabled/ungranted).
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.extensions.host_api.v1 import Capabilities
from lib.extensions.manifest import ExtensionManifest

from services.platform.extensions.grants import ExtensionGrantsRepo
from services.platform.extensions.substrate_reader import CapabilityScopedReader


async def is_enabled(pool: Any, *, tenant_id: UUID, manifest: ExtensionManifest) -> bool:
    """Is the extension enabled for this tenant? (its manifest feature flag)."""
    if not manifest.feature_flag:
        # No flag declared: enabled iff first-party (in-process trusted), else off.
        return manifest.trust_tier == "first_party"
    from services.ingest.ingestion.feature_flags.client import TenantFlags

    return await TenantFlags(pool).get_bool(tenant_id, manifest.feature_flag, default=False)


async def resolve_capabilities(
    pool: Any, *, tenant_id: UUID, manifest: ExtensionManifest
) -> Capabilities | None:
    """The effective capabilities for (tenant, extension), or None.

    - active grant present  → the granted (already-intersected) capabilities;
    - no grant, first-party → the manifest's declared capabilities (fully granted);
    - no grant, otherwise   → None (no access without explicit consent).
    """
    grant = await ExtensionGrantsRepo(pool).get(
        tenant_id=tenant_id, extension_id=manifest.id
    )
    if grant is not None:
        return grant.capabilities
    if manifest.trust_tier == "first_party":
        return Capabilities.from_dict(manifest.capabilities)
    return None


async def reader_for(
    pool: Any, *, tenant_id: UUID, manifest: ExtensionManifest, require_role: bool = True
) -> CapabilityScopedReader | None:
    """Build a capability-scoped reader for (tenant, extension), or None when the
    extension is disabled or ungranted for the tenant."""
    if not await is_enabled(pool, tenant_id=tenant_id, manifest=manifest):
        return None
    caps = await resolve_capabilities(pool, tenant_id=tenant_id, manifest=manifest)
    if caps is None:
        return None
    return CapabilityScopedReader(
        pool=pool, tenant_id=tenant_id, capabilities=caps, require_role=require_role
    )


__all__ = ["is_enabled", "resolve_capabilities", "reader_for"]
