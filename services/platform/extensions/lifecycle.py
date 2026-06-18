"""services.platform.extensions.lifecycle — install / enable / list an extension
for a tenant.

The one operator-facing flow that turns "an extension is *installed in the
deployment* (its package is on disk, its manifest is discovered)" into "this
*tenant* may use it": it writes the ``extension_grants`` capability row and flips
the per-tenant enablement feature flag, in one governed step. This is the runtime
counterpart to the capability model (ADR-0004 §A.5 / roadmap E2) — until now the
grant/flag mechanics existed only in tests.

``install`` enforces the platform invariants at the boundary:
  * the extension's manifest must be **host-API compatible** (else refuse);
  * the effective grant is the **intersection** of what the manifest declared and
    what the operator approved — an extension can never be granted more than it
    declared (``Capabilities.intersect``);
  * enablement is the manifest's declared ``feature_flag`` (no-op for a
    first-party manifest that declares none — it's implicitly enabled).

``list_installed`` joins the discovered manifests with each tenant's grant +
enablement so a UI / ``/debug/interfaces`` can show install state.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from lib.extensions.host_api.v1 import Capabilities
from lib.extensions.manifest import ExtensionManifest
from lib.extensions.registry import host_api_compatible

from services.ingest.ingestion.feature_flags.client import TenantFlags
from services.platform.extensions.access import is_enabled, resolve_capabilities
from services.platform.extensions.grants import ExtensionGrantsRepo

log = logging.getLogger("extensions.lifecycle")


class ExtensionLifecycleError(Exception):
    """An install/enable operation was refused (e.g. incompatible host API)."""


@dataclass(frozen=True)
class InstallResult:
    extension_id: str
    enabled: bool
    flag: str | None
    granted_capabilities: dict[str, Any]
    trust_ceiling: str


@dataclass(frozen=True)
class InstalledExtension:
    extension_id: str
    version: str
    trust_tier: str
    enabled: bool
    granted: bool
    feature_flag: str | None
    capabilities: dict[str, Any] | None
    granted_by: str | None
    granted_at: datetime | None
    trust_ceiling: str | None


async def install(
    pool: Any,
    *,
    tenant_id: UUID,
    manifest: ExtensionManifest,
    requested_capabilities: Capabilities | dict[str, Any] | None = None,
    trust_ceiling: str = "inferential_external",
    granted_by: str,
    enable: bool = True,
    extra_flags: dict[str, bool] | None = None,
) -> InstallResult:
    """Grant + enable ``manifest`` for ``tenant_id``.

    * ``requested_capabilities`` — the operator-approved scope; intersected with
      the manifest's declared capabilities. ``None`` ⇒ grant exactly what the
      manifest declared.
    * ``enable`` — also flip the manifest's ``feature_flag`` on (the usual case).
    * ``extra_flags`` — additional per-tenant flags to set true/false in the same
      call (e.g. an extension's optional sub-features like ``code_intel.enabled``).
    """
    if not host_api_compatible(manifest.engines_fyralis_host_api):
        raise ExtensionLifecycleError(
            f"{manifest.id}: requires host API {manifest.engines_fyralis_host_api}, "
            f"incompatible with the running host"
        )

    declared = Capabilities.from_dict(manifest.capabilities)
    if requested_capabilities is None:
        effective = declared
    else:
        approved = (
            requested_capabilities
            if isinstance(requested_capabilities, Capabilities)
            else Capabilities.from_dict(requested_capabilities)
        )
        effective = declared.intersect(approved)

    await ExtensionGrantsRepo(pool).grant(
        tenant_id=tenant_id,
        extension_id=manifest.id,
        granted_version=manifest.version,
        capabilities=effective,
        trust_ceiling=trust_ceiling,
        granted_by=granted_by,
    )

    flags = TenantFlags(pool)
    if enable and manifest.feature_flag:
        await flags.set_bool(
            tenant_id, manifest.feature_flag, True, set_by=granted_by,
            note=f"extension install: {manifest.id}",
        )
    for name, value in (extra_flags or {}).items():
        await flags.set_bool(
            tenant_id, name, value, set_by=granted_by,
            note=f"extension install: {manifest.id}",
        )

    enabled = await is_enabled(pool, tenant_id=tenant_id, manifest=manifest)
    log.info(
        "extension_installed ext=%s tenant=%s enabled=%s by=%s",
        manifest.id, tenant_id, enabled, granted_by,
    )
    return InstallResult(
        extension_id=manifest.id,
        enabled=enabled,
        flag=manifest.feature_flag,
        granted_capabilities=effective.to_dict(),
        trust_ceiling=trust_ceiling,
    )


async def uninstall(
    pool: Any, *, tenant_id: UUID, manifest: ExtensionManifest, set_by: str = "operator"
) -> None:
    """Revoke the grant and disable the extension's master ``feature_flag``.

    NOTE: any ``extra_flags`` set at install time (independent sub-feature
    toggles like ``code_intel.enabled``) are NOT reversed here — disabling the
    master flag gates the extension off regardless; clear sub-flags explicitly if
    a full reset is required."""
    await ExtensionGrantsRepo(pool).revoke(tenant_id=tenant_id, extension_id=manifest.id)
    if manifest.feature_flag:
        await TenantFlags(pool).set_bool(
            tenant_id, manifest.feature_flag, False, set_by=set_by,
            note=f"extension uninstall: {manifest.id}",
        )
    log.info("extension_uninstalled ext=%s tenant=%s by=%s", manifest.id, tenant_id, set_by)


async def installed_state(
    pool: Any, *, tenant_id: UUID, manifest: ExtensionManifest
) -> InstalledExtension:
    """The (tenant, extension) install state for one manifest."""
    grant = await ExtensionGrantsRepo(pool).get(
        tenant_id=tenant_id, extension_id=manifest.id
    )
    enabled = await is_enabled(pool, tenant_id=tenant_id, manifest=manifest)
    caps = await resolve_capabilities(pool, tenant_id=tenant_id, manifest=manifest)
    return InstalledExtension(
        extension_id=manifest.id,
        version=manifest.version,
        trust_tier=manifest.trust_tier,
        enabled=enabled,
        granted=grant is not None,
        feature_flag=manifest.feature_flag,
        capabilities=caps.to_dict() if caps is not None else None,
        granted_by=grant.granted_by if grant else None,
        granted_at=grant.granted_at if grant else None,
        trust_ceiling=grant.trust_ceiling if grant else None,
    )


async def list_installed(
    pool: Any, *, tenant_id: UUID, manifests: list[ExtensionManifest]
) -> list[InstalledExtension]:
    """Install state for every supplied manifest (typically active_manifests())."""
    out: list[InstalledExtension] = []
    for man in manifests:
        out.append(await installed_state(pool, tenant_id=tenant_id, manifest=man))
    return out


__all__ = [
    "ExtensionLifecycleError",
    "InstallResult",
    "InstalledExtension",
    "install",
    "uninstall",
    "installed_state",
    "list_installed",
]
