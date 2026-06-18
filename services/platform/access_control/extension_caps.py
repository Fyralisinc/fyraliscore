"""services/platform/access_control/extension_caps.py — capability check for extensions.

``extension_can_read`` is the extension-facing analogue of ``can_read``. An
extension is *infrastructure*, not a person in the org chart, so it runs only the
**structural** layers of access control:

  * Layer 1 — tenant isolation (absolute);
  * channel layer — the observation's ``source_channel`` must be in the grant's
    ``read_channels``;
  * kind layer — the entity kind must be in the grant's ``substrate_read``;
  * resource-kind layer — a resource's kind must be in the grant's ``resource_kinds``.

It deliberately **skips** the actor-relationship layers (author / mentioned /
manager-chain / role grants) that ``can_read`` applies to a *person* — an
extension has no place in the org graph. The result is the same
``AccessDecision`` type ``can_read`` returns, so callers log it identically.

Pure (no DB): operates on a hydrated entity dict + a parsed ``Capabilities``.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.extensions.host_api.v1 import Capabilities

from .checks import AccessDecision


def extension_can_read(
    capabilities: Capabilities,
    entity: dict[str, Any],
    *,
    tenant_id: UUID,
) -> AccessDecision:
    """Whether an extension holding ``capabilities`` may read ``entity``."""
    kind = entity.get("kind")
    if kind is None:
        return AccessDecision(False, "ext_entity_missing_kind")

    # Layer 1 — absolute tenant isolation.
    ent_tenant_raw = entity.get("tenant_id")
    if ent_tenant_raw is not None:
        ent_tenant = (
            ent_tenant_raw if isinstance(ent_tenant_raw, UUID) else UUID(str(ent_tenant_raw))
        )
        if ent_tenant != tenant_id:
            return AccessDecision(False, "ext_tenant_mismatch")

    # Kind layer — must be a granted substrate_read kind.
    if not capabilities.allows_read_kind(str(kind)):
        return AccessDecision(False, f"ext_kind_not_granted:{kind}")

    # Channel layer — observations only.
    if kind == "observation":
        channel = entity.get("source_channel") or ""
        if not capabilities.allows_channel(channel):
            return AccessDecision(False, f"ext_channel_not_granted:{channel}")

    # Resource-kind layer — resources only.
    if kind == "resource":
        rk = entity.get("resource_kind")
        if rk is None or not capabilities.allows_resource_kind(str(rk)):
            return AccessDecision(False, f"ext_resource_kind_not_granted:{rk}")

    return AccessDecision(True, "ext_capability_grant")


__all__ = ["extension_can_read"]
