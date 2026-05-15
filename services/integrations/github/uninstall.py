"""services/integrations/github/uninstall.py — IN-13 uninstall chokepoint.

Single private function `_disable_installation_github` called by:
  - Inbound: `lifecycle.dispatch_installation_event` on
    `installation.action='deleted'` or `'suspend'`.
  - Outbound: `client.GithubClient._maybe_disable_on_revocation` on
    401 Bad-credentials or 404 with the apps-not-found `documentation_url`.

Structurally similar to `services/integrations/discord/uninstall.py::_disable_and_zeroize_discord`
but **without secret deletion** (FR-012): the App-level webhook secret
is shared across all customers and MUST NOT be deleted on a single-
tenant uninstall. The cached installation access token IS invalidated
(if a token cache is supplied).

Idempotent: double-fire (inbound + outbound racing) is the documented
property. No row lock is taken; up to two audit rows per chokepoint
event is the accepted cost of correctness without lock contention.

Logging redaction (FR-016 / SC-008): log lines never contain the raw
`installation_id` — only `installation_row_id` (UUID) and
`installation_id_hash` (8-byte BLAKE2b hex).
"""
from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7

from services.integrations.github import metrics


log = structlog.get_logger("integrations.github.uninstall")


def _short_installation_hash(installation_id: str) -> str:
    """8-byte BLAKE2b hex digest used as a log-safe disambiguator."""
    return hashlib.blake2b(
        installation_id.encode("utf-8"), digest_size=8
    ).hexdigest()


async def _disable_installation_github(
    *,
    pool: asyncpg.Pool,
    installation_row_id: UUID,
    tenant_id: UUID,
    installation_id: str | None = None,
    reason: str = "outbound_chokepoint",
    audit_action: str = "uninstall",
    audit_status: str = "ok",
    installation_token_cache: dict[str, Any] | None = None,
    tenant_resolver: Any | None = None,
) -> None:
    """Atomic-enough chokepoint:
      1. UPDATE provider_installations SET enabled=FALSE (row-level).
      2. Invalidate cached installation access token (if cache given).
      3. INSERT installation_audit_log row.
      4. Invalidate tenant-resolver cache (so subsequent webhook
         deliveries see the disabled row as UnknownInstallation).

    Does NOT delete any encrypted_secrets row — the App-level webhook
    secret is shared (FR-012) and must outlive this uninstall.

    Safe to call concurrently for the same `installation_row_id`:
    duplicate UPDATEs are no-ops (enabled was already FALSE), duplicate
    cache invalidation is a no-op, two audit rows are an accepted cost.
    """
    short_hash = (
        _short_installation_hash(installation_id) if installation_id else None
    )

    # Step 1: disable the row. Use a defensive transaction so the
    # UPDATE + audit row land atomically per fire.
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE provider_installations
                   SET enabled = FALSE
                 WHERE id = $1
                   AND provider = 'github'
                RETURNING id, installation_id, enabled
                """,
                installation_row_id,
            )
            if row is None:
                # Defensive: caller should have verified row existence.
                # Log + skip the audit row to avoid a foreign-key error.
                log.warning(
                    "github_uninstall_chokepoint_missing_row",
                    installation_row_id=str(installation_row_id),
                    tenant_id=str(tenant_id),
                    reason=reason,
                )
                return

            # Step 3: audit row.
            try:
                await conn.execute(
                    """
                    INSERT INTO installation_audit_log
                        (id, tenant_id, installation_row_id, provider,
                         action, status, context)
                    VALUES ($1, $2, $3, 'github', $4, $5, $6::jsonb)
                    """,
                    uuid7(),
                    tenant_id,
                    installation_row_id,
                    audit_action,
                    audit_status,
                    json.dumps(
                        {
                            "reason": reason,
                            "installation_id_hash": short_hash,
                        }
                    ),
                )
            except Exception:  # noqa: BLE001 — audit is best-effort
                log.error(
                    "github_uninstall_audit_failed",
                    installation_row_id=str(installation_row_id),
                    tenant_id=str(tenant_id),
                    reason=reason,
                )

    # Step 2: invalidate cached installation access token if a cache
    # dict was supplied (the client owns its cache; we just pop).
    if installation_token_cache is not None and installation_id is not None:
        installation_token_cache.pop(installation_id, None)

    # Step 4: invalidate the tenant resolver's installation cache so
    # the next inbound webhook for this installation sees the disabled
    # row immediately rather than after the cache TTL elapses.
    if tenant_resolver is not None and installation_id is not None:
        try:
            cache = getattr(tenant_resolver, "_cache", None)
            if cache is not None:
                cache.invalidate(("github", installation_id))
        except Exception:  # noqa: BLE001 — cache invalidation never fatal
            pass

    metrics.record_outbound_chokepoint(
        reason="bad_credentials"
        if reason.endswith("401_bad_credentials")
        else "installation_not_found"
        if reason.endswith("404_apps_not_found")
        else "lifecycle_webhook"
    )

    log.info(
        "github_uninstall_chokepoint",
        installation_row_id=str(installation_row_id),
        tenant_id=str(tenant_id),
        installation_id_hash=short_hash,
        reason=reason,
        audit_action=audit_action,
    )


__all__ = ["_disable_installation_github", "_short_installation_hash"]
