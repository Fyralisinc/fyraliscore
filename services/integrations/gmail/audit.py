"""services/integrations/gmail/audit.py — append-only audit logs.

Two distinct logs:

1. gmail_install_audit  — install-lifecycle events (install, scope change,
   inclusion update, disable, opt-out add/remove).
2. gmail_read_audit     — per-message read attestation. The per-user
   "we can prove what we read and didn't read" sales asset mentioned
   in the integration context.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.shared.ids import uuid7
from lib.shared.tenant_context import TenantContext


# -- install audit -----------------------------------------------------

INSTALL_ACTIONS = frozenset({
    "gmail.install",
    "gmail.scope_changed",
    "gmail.inclusion_updated",
    "gmail.disabled",
    "gmail.optout_added",
    "gmail.optout_removed",
    "gmail.uninstall",
})


async def write_install_audit(
    tctx: TenantContext,
    *,
    gmail_installation_id: UUID | None,
    action: str,
    actor_email: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    if action not in INSTALL_ACTIONS:
        raise ValueError(f"unknown install audit action: {action!r}")
    await tctx.execute(
        """
        INSERT INTO gmail_install_audit (
          id, tenant_id, gmail_installation_id, action, actor_email, details
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        uuid7(), tctx.tenant_id, gmail_installation_id, action, actor_email,
        # asyncpg jsonb codec accepts a Python dict; cast keeps things explicit.
        __dumps_jsonb(details or {}),
    )


# -- read audit --------------------------------------------------------

async def write_read_audit(
    tctx: TenantContext,
    *,
    gmail_installation_id: UUID,
    email_address: str,
    message_id: str,
    scope_used: str,
    read_path: str,
) -> None:
    if read_path not in ("push", "poll"):
        raise ValueError(f"read_path must be 'push' or 'poll', got {read_path!r}")
    await tctx.execute(
        """
        INSERT INTO gmail_read_audit (
          id, tenant_id, gmail_installation_id, email_address, message_id,
          scope_used, read_path
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        uuid7(), tctx.tenant_id, gmail_installation_id,
        email_address.lower(), message_id, scope_used, read_path,
    )


def __dumps_jsonb(d: dict[str, Any]) -> str:
    # asyncpg's default JSON codec encodes dict → text. For the explicit
    # ::jsonb cast above we go through json.dumps so the value is always
    # well-formed even if the caller passed non-dict scalars.
    import json
    return json.dumps(d, default=str)


__all__ = ["INSTALL_ACTIONS", "write_install_audit", "write_read_audit"]
