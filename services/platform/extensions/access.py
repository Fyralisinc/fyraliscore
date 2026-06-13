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

import logging
import os
from typing import Any
from uuid import UUID

import asyncpg

from lib.extensions.host_api.v1 import Capabilities
from lib.extensions.manifest import ExtensionManifest

from services.platform.extensions.grants import ExtensionGrantsRepo
from services.platform.extensions.substrate_reader import CapabilityScopedReader

log = logging.getLogger("extensions.access")


def _host_first_party_ids() -> frozenset[str]:
    """The HOST-controlled set of extension ids the operator trusts as first-party.

    ``trust_tier`` in a manifest is **self-declared** — an installed package can
    claim ``"first_party"``. So the first-party fast-paths (implicit enablement +
    no-grant capability fallback) are gated on this operator-owned allowlist
    (``FYRALIS_FIRST_PARTY_EXTENSION_IDS``, comma-separated), never on the manifest
    alone. Default empty: with no allowlist, *every* extension needs an explicit
    grant + flag (which the install lifecycle writes) — a self-declared
    ``first_party`` package gets nothing it wasn't granted."""
    raw = os.environ.get("FYRALIS_FIRST_PARTY_EXTENSION_IDS", "")
    return frozenset(p.strip() for p in raw.split(",") if p.strip())


def _is_trusted_first_party(manifest: ExtensionManifest) -> bool:
    """First-party AND on the host allowlist — not merely self-declared."""
    return manifest.trust_tier == "first_party" and manifest.id in _host_first_party_ids()


async def is_enabled(pool: Any, *, tenant_id: UUID, manifest: ExtensionManifest) -> bool:
    """Is the extension enabled for this tenant? (its manifest feature flag)."""
    if not manifest.feature_flag:
        # No flag declared: enabled iff host-trusted first-party, else off.
        return _is_trusted_first_party(manifest)
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
    if _is_trusted_first_party(manifest):
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


async def enricher_allowed(
    pool: Any, *, tenant_id: UUID, manifest: ExtensionManifest
) -> bool:
    """May this extension's contribution run for this tenant right now?

    The host-side gate the ingest seam consults before executing an
    extension-contributed draft enricher (and the analogue any executed
    contribution should use). Allowed iff the extension is **enabled** for the
    tenant AND has a non-``None`` effective capability set
    (``resolve_capabilities``) — i.e. an active grant, or first-party defaults.

    Degradation is chosen to keep the ingest hot path safe:
      * ``is_enabled`` is the primary, cheap, reliable gate (a plain
        ``tenant_flags`` read, no RLS). Disabled → never runs.
      * if the ``extension_grants`` table is absent (migration 0127 not applied
        on this DB), fall back to "first-party runs, others don't" — matching the
        pre-enforcement behavior for the existing first-party interfaces.
      * any other unexpected error → **fail closed** (skip the enricher). The
        enricher is best-effort and wrapped raw-on-failure, so skipping it just
        persists the raw draft — the safe default — rather than running an
        ungoverned contribution.
    """
    try:
        if not await is_enabled(pool, tenant_id=tenant_id, manifest=manifest):
            return False
        caps = await resolve_capabilities(pool, tenant_id=tenant_id, manifest=manifest)
        return caps is not None
    except asyncpg.UndefinedTableError:
        # extension_grants not migrated here: honor enablement only for a
        # host-trusted first-party extension (never a self-declared one).
        return _is_trusted_first_party(manifest) and await is_enabled(
            pool, tenant_id=tenant_id, manifest=manifest
        )
    except Exception:  # noqa: BLE001 - gate must never break ingest; fail closed
        log.warning(
            "extension_gate_error ext=%s tenant=%s (failing closed)",
            manifest.id, tenant_id, exc_info=True,
        )
        return False


__all__ = ["is_enabled", "resolve_capabilities", "reader_for", "enricher_allowed"]
