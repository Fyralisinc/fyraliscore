"""services/integrations/gmail/optout.py — per-user opt-out store + HTTP.

The opt-out store is the *only* mechanism that subtracts mailboxes from
the admin-authored inclusion set. Two entry points:

1. End user clicks an opt-out link emailed to them (signed token,
   short-lived) → POST /v1/integrations/gmail/optout.
2. Workspace admin opts a user out on their behalf via the admin
   console → same endpoint, admin scope.

Removing an opt-out (re-opting in) is admin-only.

Effect of an opt-out:
  - Insert gmail_mailbox_optouts row.
  - Update gmail_mailbox_watches.state → 'opted_out'.
  - Call users.stop() so Google stops pushing.
  - Future inclusion resolution filters this address out.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import TenantContext

from services.integrations.gmail.audit import write_install_audit


log = structlog.get_logger("integrations.gmail.optout")


async def fetch_optout_emails(
    tctx: TenantContext,
    *,
    gmail_installation_id: UUID,
) -> set[str]:
    rows = await tctx.fetch(
        """
        SELECT email_address FROM gmail_mailbox_optouts
        WHERE gmail_installation_id = $1
        """,
        gmail_installation_id,
    )
    return {r["email_address"].lower() for r in rows}


async def add_optout(
    tctx: TenantContext,
    *,
    gmail_installation_id: UUID,
    email_address: str,
    reason: str | None = None,
    actor_email: str | None = None,
) -> bool:
    """Insert opt-out row + mark watch row opted_out. Returns True if a
    new row was inserted (False if already opted out).

    NOTE: The caller is responsible for calling stop_watch() on the
    Gmail side AFTER this returns. We don't bundle the upstream call
    here because we want the DB state recorded even if Google is
    temporarily unreachable.
    """
    email = email_address.lower()
    inserted = await tctx.fetchval(
        """
        INSERT INTO gmail_mailbox_optouts (
          id, tenant_id, gmail_installation_id, email_address, reason
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (gmail_installation_id, email_address) DO NOTHING
        RETURNING id
        """,
        uuid7(), tctx.tenant_id, gmail_installation_id, email, reason,
    )
    await tctx.execute(
        """
        UPDATE gmail_mailbox_watches
           SET state = 'opted_out'
         WHERE gmail_installation_id = $1 AND email_address = $2
        """,
        gmail_installation_id, email,
    )
    if inserted is not None:
        await write_install_audit(
            tctx,
            gmail_installation_id=gmail_installation_id,
            action="gmail.optout_added",
            actor_email=actor_email,
            details={"email": email, "reason": reason},
        )
        log.info(
            "gmail.optout.added",
            gmail_installation_id=str(gmail_installation_id),
            email=email,
            reason=reason,
        )
    return inserted is not None


async def remove_optout(
    tctx: TenantContext,
    *,
    gmail_installation_id: UUID,
    email_address: str,
    actor_email: str | None = None,
) -> bool:
    """Drop the opt-out row. Watch row is NOT re-activated automatically
    — the next inclusion-resolution tick will see the address re-enter
    the candidate set and the scheduler will (re-)create the watch.
    """
    email = email_address.lower()
    removed = await tctx.fetchval(
        """
        DELETE FROM gmail_mailbox_optouts
        WHERE gmail_installation_id = $1 AND email_address = $2
        RETURNING id
        """,
        gmail_installation_id, email,
    )
    if removed is not None:
        await write_install_audit(
            tctx,
            gmail_installation_id=gmail_installation_id,
            action="gmail.optout_removed",
            actor_email=actor_email,
            details={"email": email},
        )
    return removed is not None


__all__ = ["add_optout", "fetch_optout_emails", "remove_optout"]
