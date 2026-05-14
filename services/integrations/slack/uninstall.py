"""services/integrations/slack/uninstall.py — handle Slack's
`app_uninstalled` and `tokens_revoked` events.

Inbound webhook: Slack POSTs the event to /webhooks/slack/... The
router verifies the signature, resolves the tenant via IN-07, and —
for these two event types — dispatches here BEFORE the ingestion
path. Uninstall events do not produce `Observations`; they disable
the `provider_installations` row and zeroize the token material.

Idempotency
-----------
Repeated uninstall events for the same workspace are no-ops:
  - `disable_installation` UPDATE sets a boolean.
  - `secret_store.delete` tolerates already-deleted refs.
  - An audit row is still written (cheap; gives an operator visibility).
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from services.integrations.slack import metrics


log = structlog.get_logger("integrations.slack.uninstall")


async def handle_app_uninstalled(
    pool: asyncpg.Pool,
    secret_store: Any,
    tenant_resolver: Any,
    tenant_id: UUID,
    installation_row_id: UUID,
    team_id: str,
) -> None:
    """Disable the installation row, delete its associated secrets,
    invalidate the resolver cache, and write the audit row."""
    await _disable_and_zeroize(
        pool, secret_store, tenant_resolver,
        tenant_id, installation_row_id, team_id,
        event_type="app_uninstalled",
    )


async def handle_tokens_revoked(
    pool: asyncpg.Pool,
    secret_store: Any,
    tenant_resolver: Any,
    tenant_id: UUID,
    installation_row_id: UUID,
    team_id: str,
) -> None:
    """Same flow as app_uninstalled. Slack sends this when a user
    revokes the OAuth grant; our response is identical to a full
    uninstall (the bot can no longer act, so the row is dead)."""
    await _disable_and_zeroize(
        pool, secret_store, tenant_resolver,
        tenant_id, installation_row_id, team_id,
        event_type="tokens_revoked",
    )


async def _disable_and_zeroize(
    pool: asyncpg.Pool,
    secret_store: Any,
    tenant_resolver: Any,
    tenant_id: UUID,
    installation_row_id: UUID,
    team_id: str,
    *,
    event_type: str,
) -> None:
    """Shared body of the two uninstall handlers.

    Steps:
      1. Disable the installation row (resolver returns
         UnknownInstallation for subsequent webhooks).
      2. Collect tenant-scoped `encrypted_secrets` rows whose labels
         point at this team's bot or user token.
      3. Delete each secret ref via the secret store. Tolerant of
         missing rows.
      4. Invalidate the resolver cache for `(slack, team_id)`.
      5. Append an audit row.
    """
    status = "ok"
    failure_phase: str | None = None

    try:
        await tenant_resolver.disable_installation(installation_row_id)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "slack_uninstall_disable_failed",
            error_type=type(exc).__name__,
        )
        status = "error"
        failure_phase = "disable_installation"

    # Collect refs to zeroize — even if disable failed, attempt secret
    # cleanup so we minimize residual material.
    refs_to_delete: list[str] = []
    try:
        rows = await pool.fetch(
            """
            SELECT id::text AS id
              FROM encrypted_secrets
             WHERE tenant_id = $1
               AND (label = $2 OR label = $3)
            """,
            tenant_id,
            f"slack_bot_token:{team_id}",
            f"slack_user_token:{team_id}",
        )
        refs_to_delete = [r["id"] for r in rows]
    except Exception as exc:  # noqa: BLE001
        log.error(
            "slack_uninstall_secret_query_failed",
            error_type=type(exc).__name__,
        )
        if status == "ok":
            status = "error"
            failure_phase = "secret_query"

    for ref in refs_to_delete:
        try:
            await secret_store.delete(ref, tenant_id=tenant_id)
        except Exception as exc:  # noqa: BLE001
            log.error(
                "slack_uninstall_secret_delete_failed",
                error_type=type(exc).__name__,
            )
            if status == "ok":
                status = "error"
                failure_phase = "secret_delete"

    # Invalidate resolver cache so the very next webhook for this
    # workspace sees the disabled row.
    cache = getattr(tenant_resolver, "_cache", None)
    if cache is not None:
        try:
            cache.invalidate(("slack", team_id))
        except Exception:  # noqa: BLE001
            pass

    # Best-effort audit row.
    try:
        await pool.execute(
            """
            INSERT INTO installation_audit_log
                (id, tenant_id, installation_row_id, provider, action, status, context)
            VALUES ($1, $2, $3, 'slack', 'uninstall', $4, $5::jsonb)
            """,
            uuid7(),
            tenant_id,
            installation_row_id,
            status,
            json.dumps({
                "event_type": event_type,
                **({"failure_phase": failure_phase} if failure_phase else {}),
                "secrets_zeroized": len(refs_to_delete),
            }),
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "installation_audit_log_write_failed",
            action="uninstall",
            error_type=type(exc).__name__,
        )

    if status == "ok":
        metrics.record_uninstall_outcome("success")
    else:
        metrics.record_uninstall_outcome("error")


__all__ = ["handle_app_uninstalled", "handle_tokens_revoked"]
