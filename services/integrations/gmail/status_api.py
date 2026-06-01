"""services/integrations/gmail/status_api.py — GET /v1/integrations/gmail/status.

Returns a compact status snapshot for the calling tenant's Gmail
install: watch counts by state, last push/poll timestamps, errored
mailboxes, recent audit. Read-only; safe to call on the hot path.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.shared.tenant_context import tenant_transaction


async def get_gmail_status(*, tenant_id: UUID) -> dict[str, Any]:
    async with tenant_transaction(tenant_id) as tctx:
        install = await tctx.fetchrow(
            """
            SELECT id, workspace_domain, scope, resolved_user_count,
                   resolved_at, created_at, disabled_at
              FROM gmail_installations
             WHERE disabled_at IS NULL
             ORDER BY created_at DESC
             LIMIT 1
            """,
        )
        if install is None:
            return {"connected": False}

        counts_rows = await tctx.fetch(
            """
            SELECT state, COUNT(*) AS n
              FROM gmail_mailbox_watches
             WHERE gmail_installation_id = $1
             GROUP BY state
            """,
            install["id"],
        )
        counts = {r["state"]: int(r["n"]) for r in counts_rows}

        latest = await tctx.fetchrow(
            """
            SELECT MAX(last_push_at) AS last_push,
                   MAX(last_poll_at) AS last_poll
              FROM gmail_mailbox_watches
             WHERE gmail_installation_id = $1
               AND state = 'active'
            """,
            install["id"],
        )

        errored = await tctx.fetch(
            """
            SELECT email_address, last_error, consecutive_poll_failures
              FROM gmail_mailbox_watches
             WHERE gmail_installation_id = $1
               AND state = 'errored'
             LIMIT 50
            """,
            install["id"],
        )

        audit_rows = await tctx.fetch(
            """
            SELECT action, actor_email, occurred_at, details
              FROM gmail_install_audit
             WHERE gmail_installation_id = $1
             ORDER BY occurred_at DESC
             LIMIT 20
            """,
            install["id"],
        )

    return {
        "connected": True,
        "installation_id": str(install["id"]),
        "workspace_domain": install["workspace_domain"],
        "scope": install["scope"],
        "resolved_user_count": install["resolved_user_count"],
        "resolved_at": install["resolved_at"].isoformat() if install["resolved_at"] else None,
        "watches": {
            "total": sum(counts.values()),
            "by_state": counts,
        },
        "last_push_at": latest["last_push"].isoformat() if latest and latest["last_push"] else None,
        "last_poll_at": latest["last_poll"].isoformat() if latest and latest["last_poll"] else None,
        "errored_mailboxes": [
            {
                "email": r["email_address"],
                "last_error": r["last_error"],
                "consecutive_failures": r["consecutive_poll_failures"],
            }
            for r in errored
        ],
        "recent_audit": [
            {
                "action": r["action"],
                "actor_email": r["actor_email"],
                "occurred_at": r["occurred_at"].isoformat(),
                "details": r["details"],
            }
            for r in audit_rows
        ],
    }


__all__ = ["get_gmail_status"]
