"""services/integrations/gmail/uninstall.py — full or per-mailbox teardown.

Two modes:

1. uninstall_install(install_id) — full disconnect:
     - users.stop() for every active mailbox
     - mark all watches 'paused'
     - tear down per-tenant Pub/Sub topic + subscription
     - mark gmail_installations.disabled_at = now()
     - emit audit

2. stop_mailbox(install_id, email) — per-mailbox stop without disabling
   the install. Used by the admin console for manual reconciliation.

Both are idempotent: re-running has no effect once the target state is
reached.
"""
from __future__ import annotations

from uuid import UUID

import structlog

from lib.shared.tenant_context import TenantContext, tenant_transaction

from services.integrations.gmail.audit import write_install_audit
from services.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
    GoogleHttpClient,
)
from services.integrations.gmail.dwd import get_minter
from services.integrations.gmail.pubsub import PubsubAdmin


log = structlog.get_logger("integrations.gmail.uninstall")


async def uninstall_install(
    *,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    actor_email: str | None = None,
) -> None:
    """Full uninstall. Tears down Pub/Sub last (it's the most expensive
    to recreate)."""
    async with tenant_transaction(tenant_id) as tctx:
        install = await tctx.fetchrow(
            """
            SELECT id, scope, disabled_at FROM gmail_installations
             WHERE id = $1
            """,
            gmail_installation_id,
        )
        if install is None:
            return
        if install["disabled_at"] is not None:
            return
        scope = install["scope"]
        watches = await tctx.fetch(
            """
            SELECT email_address FROM gmail_mailbox_watches
             WHERE gmail_installation_id = $1
               AND state IN ('active', 'pending', 'errored')
            """,
            gmail_installation_id,
        )

    if watches:
        minter = get_minter()
        async with GoogleHttpClient(minter) as http:
            gmail = GmailClient(http)
            for w in watches:
                try:
                    await gmail.stop(user_email=w["email_address"], scope=scope)
                except GoogleApiError as exc:
                    log.warning(
                        "gmail.uninstall.stop_failed",
                        email=w["email_address"],
                        error=str(exc)[:200],
                    )

    async with PubsubAdmin() as admin:
        await admin.teardown(tenant_id)

    async with tenant_transaction(tenant_id) as tctx:
        await tctx.execute(
            """
            UPDATE gmail_mailbox_watches
               SET state = 'paused'
             WHERE gmail_installation_id = $1
               AND state IN ('active', 'pending', 'errored')
            """,
            gmail_installation_id,
        )
        await tctx.execute(
            """
            UPDATE gmail_pubsub_topics
               SET teardown_at = now()
             WHERE gmail_installation_id = $1 AND teardown_at IS NULL
            """,
            gmail_installation_id,
        )
        await tctx.execute(
            """
            UPDATE gmail_installations
               SET disabled_at = now()
             WHERE id = $1
            """,
            gmail_installation_id,
        )
        await write_install_audit(
            tctx,
            gmail_installation_id=gmail_installation_id,
            action="gmail.uninstall",
            actor_email=actor_email,
            details={"watches_stopped": len(watches)},
        )
    log.info(
        "gmail.uninstall.completed",
        gmail_installation_id=str(gmail_installation_id),
        watches=len(watches),
    )


async def stop_mailbox(
    *,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    email_address: str,
    actor_email: str | None = None,
) -> None:
    """Stop a single mailbox's watch without disabling the install."""
    async with tenant_transaction(tenant_id) as tctx:
        scope_row = await tctx.fetchrow(
            "SELECT scope FROM gmail_installations WHERE id = $1",
            gmail_installation_id,
        )
        if scope_row is None:
            return
        scope = scope_row["scope"]
    if scope not in (GMAIL_METADATA_SCOPE, GMAIL_READONLY_SCOPE):
        # Stored scopes are short names; the watch-stop call accepts both,
        # but be tolerant.
        pass

    minter = get_minter()
    async with GoogleHttpClient(minter) as http:
        gmail = GmailClient(http)
        try:
            await gmail.stop(user_email=email_address, scope=scope)
        except GoogleApiError as exc:
            log.warning(
                "gmail.stop_mailbox.failed",
                email=email_address,
                error=str(exc)[:200],
            )

    async with tenant_transaction(tenant_id) as tctx:
        await tctx.execute(
            """
            UPDATE gmail_mailbox_watches
               SET state = 'paused'
             WHERE gmail_installation_id = $1
               AND email_address = $2
               AND state IN ('active', 'pending', 'errored')
            """,
            gmail_installation_id, email_address.lower(),
        )


__all__ = ["stop_mailbox", "uninstall_install"]
