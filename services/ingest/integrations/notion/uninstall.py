"""services/ingest/integrations/notion/uninstall.py — IN-14 revocation chokepoint.

Single private function `_disable_installation_notion`, called by the
outbound client (`client.NotionClient._maybe_disable_on_revocation`) on a
401 `unauthorized` — Notion's "integration removed / token revoked"
signal. Structurally mirrors `services/ingest/integrations/github/uninstall.py`,
adapted to Notion:

  - Notion has NO inbound lifecycle webhook (no `installation.suspend` /
    `.unsuspend`), so the chokepoint is OUTBOUND-only. Re-enable happens
    via re-OAuth (the install callback upserts `enabled=TRUE`) or an
    operator. Once re-enabled, the parked backfill shards resume via the
    orphan-scan.
  - The Notion outbound client carries `(tenant_id, workspace_id)` — the
    workspace_id IS `provider_installations.installation_id` — so we
    disable by that natural key (UNIQUE on `(provider, installation_id)`)
    rather than threading the row UUID through every client build.
  - Does NOT delete `encrypted_secrets`: the bot token row outlives a
    transient revocation (a restored token + re-enable resumes cleanly).
  - The App-level webhook `verification_token` is env-config, untouched.

Idempotent: the `enabled = TRUE` guard means only the first concurrent
fire updates the row + writes an audit; later fires see no row and skip.

Logging redaction: never logs the raw bot token; the workspace id is
hashed (`short_workspace_hash`) before it reaches a log line.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import structlog

from lib.shared.ids import uuid7
from services.ingest.integrations.notion.client import short_workspace_hash


log = structlog.get_logger("integrations.notion.uninstall")


async def _disable_installation_notion(
    *,
    pool: Any,
    tenant_id: UUID,
    workspace_id: str,
    reason: str = "outbound_401_unauthorized",
) -> bool:
    """Disable the Notion install for `(tenant_id, workspace_id)`.

      1. UPDATE provider_installations SET enabled=FALSE (guarded on
         enabled=TRUE so the update is idempotent / fire-once).
      2. INSERT installation_audit_log row (best-effort).

    Returns True iff this call performed the disable (the first fire);
    False if the row was already disabled / not found. Never raises — a
    chokepoint failure must not mask the original API error the caller is
    about to surface.
    """
    short = short_workspace_hash(workspace_id)
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    UPDATE provider_installations
                       SET enabled = FALSE
                     WHERE provider = 'notion'
                       AND installation_id = $1
                       AND tenant_id = $2
                       AND enabled = TRUE
                    RETURNING id
                    """,
                    workspace_id,
                    tenant_id,
                )
                if row is None:
                    # Already disabled or unknown — nothing to do. Not the
                    # first fire (idempotent), so no audit row.
                    return False
                try:
                    await conn.execute(
                        """
                        INSERT INTO installation_audit_log
                            (id, tenant_id, installation_row_id, provider,
                             action, status, context)
                        VALUES ($1, $2, $3, 'notion', 'uninstall', 'ok', $4::jsonb)
                        """,
                        uuid7(),
                        tenant_id,
                        row["id"],
                        json.dumps(
                            {"reason": reason, "workspace_id_hash": short},
                        ),
                    )
                except Exception:  # noqa: BLE001 — audit is best-effort
                    log.error(
                        "notion_uninstall_audit_failed",
                        workspace_id_hash=short,
                        reason=reason,
                    )
        log.warning(
            "notion_install_disabled_on_revocation",
            workspace_id_hash=short,
            reason=reason,
        )
        return True
    except Exception as exc:  # noqa: BLE001 — chokepoint must never raise
        log.error(
            "notion_uninstall_chokepoint_failed",
            workspace_id_hash=short,
            reason=reason,
            error_type=type(exc).__name__,
        )
        return False


__all__ = ["_disable_installation_notion"]
