"""services/ingest/integrations/miro/onboarding.py — install + provision.

Miro authenticates with a long-lived org-app Bearer token against the canonical
Miro API host. Onboarding mirrors the Brex dedicated-table shape (NOT the OAuth
bot-token path):

  finalize_install() — UPSERT a miro_installations row, INSERT one miro_boards
  row per board to shard, and emit an onboarding_triggers row (source='miro') so
  the existing M6 backfill chain (oauth_poller -> tenant_onboarding ->
  source_onboarding -> shard_fetch -> reconciler) fires. All in one
  tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='miro', installation_id=org_id,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant +
  loads the signing secret via the existing machinery. Backfill uses
  miro_installations; live uses provider_installations — the two are seeded
  together but stay independent.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.miro import metrics


log = structlog.get_logger("integrations.miro.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    boards: list[dict],
    secret_ref: str | None = None,
    org_id: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its boards + an onboarding trigger atomically.

    `boards` is the resolved set of boards to backfill (enumerate via
    MiroClient.list_boards at seed time); each dict carries at least
    ``board_id`` and optionally ``board_name`` / ``board_kind``. Returns the
    miro_installations id. Idempotent on (tenant_id, base_url) and per
    (install, board_id).
    """
    base_url = base_url.rstrip("/")
    # Dedup boards defensively on the natural key.
    seen: set[str] = set()
    deduped: list[dict] = []
    for b in boards:
        board_id = str(b.get("board_id") or b.get("id") or "")
        if board_id and board_id not in seen:
            seen.add(board_id)
            deduped.append({**b, "board_id": board_id})

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO miro_installations (
                id, tenant_id, base_url, secret_ref,
                org_id, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, base_url) DO UPDATE
                SET secret_ref = COALESCE(EXCLUDED.secret_ref, miro_installations.secret_ref),
                    org_id = COALESCE(EXCLUDED.org_id, miro_installations.org_id),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, miro_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, base_url, secret_ref,
            org_id, webhook_secret_ref,
        )

        for b in deduped:
            await tctx.execute(
                """
                INSERT INTO miro_boards (
                    id, tenant_id, miro_installation_id,
                    board_id, board_name, board_kind, state
                ) VALUES ($1, $2, $3, $4, $5, $6, 'active')
                ON CONFLICT (miro_installation_id, board_id)
                    DO UPDATE SET state = 'active',
                                  board_name = COALESCE(EXCLUDED.board_name, miro_boards.board_name),
                                  board_kind = COALESCE(EXCLUDED.board_kind, miro_boards.board_kind)
                """,
                uuid7(), tenant_id, install_id, b["board_id"],
                b.get("board_name") or b.get("name"),
                b.get("board_kind") or b.get("type"),
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Brex/Jira/Drive this is NOT a provider_installations source; the
        # install id rides in installation_row_id purely for the idempotency
        # dedup index. source='miro' is admitted by migration 0102_miro (owned
        # by the shared-file / migration agent).
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'miro', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"base_url": base_url,
                        "boards": [b["board_id"] for b in deduped]}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_boards")
    log.info(
        "miro_install_finalized",
        base_url=base_url, board_count=len(deduped),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    org_id: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses
    to resolve the tenant + load the HMAC signing secret. installation_id is the
    Miro org id (matches tenant_resolver._extract_miro)."""
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'miro', $3, $4, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                secret_ref = EXCLUDED.secret_ref,
                enabled = TRUE
        """,
        uuid7(), tenant_id, org_id, webhook_secret_ref,
    )
    log.info("miro_webhook_installation_registered", org_id=org_id)


__all__ = ["finalize_install", "register_webhook_installation"]
