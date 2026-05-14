"""services/integrations/discord/uninstall.py — bot-kick chokepoint.

Unlike Slack (`app_uninstalled` / `tokens_revoked` webhook events),
Discord does NOT push an uninstall event when a bot is kicked from a
guild. Detection is the outbound-401 chokepoint: every Discord REST
call goes through `services/integrations/discord/client.py`, and on a
401 (or 403 with `code=50001`) we call `_disable_and_zeroize_discord`.

Per Clarifications Q1 (spec.md), the chokepoint is **idempotent under
concurrent races**:

  - `UPDATE provider_installations SET enabled=FALSE` on an already-
    disabled row is a benign no-op.
  - `secret_store.delete()` suppresses `SecretNotFoundError`.
  - Concurrent fires may produce up to N audit rows for the same kick
    (acceptable; dashboards `SELECT DISTINCT ON (installation_row_id, action)`).
  - No row-level locking (`SELECT … FOR UPDATE`) is used in the hot
    outbound path.

This mirrors `services/integrations/slack/uninstall.py::_disable_and_zeroize`
but is triggered from the client, not the router.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import SecretNotFoundError
from lib.shared.ids import uuid7
from services.integrations.discord import metrics


log = structlog.get_logger("integrations.discord.uninstall")


async def _disable_and_zeroize_discord(
    *,
    pool: asyncpg.Pool,
    secret_store: Any,
    installation_row_id: UUID,
    tenant_id: UUID,
    guild_id: str,
    reason: str = "outbound_401",
    tenant_resolver: Any | None = None,
) -> None:
    """Disable the installation row, delete encrypted bot token and
    public key, invalidate any resolver cache entry, and write an audit
    row. Idempotent — safe to invoke concurrently for the same
    installation.

    Each step is wrapped in a try/except so that a partial failure
    (e.g., the resolver cache invalidation throws) does not block the
    other steps. The audit row's `status` reflects whether any step
    raised.
    """
    status = "ok"
    failure_phase: str | None = None

    # 1. Flip enabled=FALSE. Idempotent — UPDATE on already-disabled row
    # touches zero rows and asyncpg returns "UPDATE 0" cleanly.
    try:
        await pool.execute(
            "UPDATE provider_installations SET enabled=FALSE "
            "WHERE id=$1 AND tenant_id=$2",
            installation_row_id, tenant_id,
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "discord_uninstall_disable_failed",
            error_type=type(exc).__name__,
        )
        status = "error"
        failure_phase = "disable_installation"

    # 2. Collect tenant-scoped encrypted_secrets rows for this guild.
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
            f"discord_bot_token:{guild_id}",
            f"discord_public_key:{guild_id}",
        )
        refs_to_delete = [r["id"] for r in rows]
    except Exception as exc:  # noqa: BLE001
        log.error(
            "discord_uninstall_secret_query_failed",
            error_type=type(exc).__name__,
        )
        if status == "ok":
            status = "error"
            failure_phase = "secret_query"

    # 3. Delete each ref. Tolerate already-deleted (SecretNotFoundError)
    # for concurrent-fire idempotency per Clarifications Q1.
    for ref in refs_to_delete:
        try:
            await secret_store.delete(ref, tenant_id=tenant_id)
        except SecretNotFoundError:
            # Another concurrent chokepoint observer beat us to it.
            pass
        except Exception as exc:  # noqa: BLE001
            log.error(
                "discord_uninstall_secret_delete_failed",
                error_type=type(exc).__name__,
            )
            if status == "ok":
                status = "error"
                failure_phase = "secret_delete"

    # 4. Invalidate the resolver cache for (discord, guild_id) so the
    # next inbound interaction sees enabled=FALSE → unknown_installation.
    if tenant_resolver is not None:
        cache = getattr(tenant_resolver, "_cache", None)
        if cache is not None:
            try:
                cache.invalidate(("discord", guild_id))
            except Exception:  # noqa: BLE001
                pass

    # 5. Audit row (best-effort).
    try:
        await pool.execute(
            """
            INSERT INTO installation_audit_log
                (id, tenant_id, installation_row_id, provider, action, status, context)
            VALUES ($1, $2, $3, 'discord', 'uninstall', $4, $5::jsonb)
            """,
            uuid7(),
            tenant_id,
            installation_row_id,
            status,
            json.dumps({
                "reason": reason,
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


__all__ = ["_disable_and_zeroize_discord"]
