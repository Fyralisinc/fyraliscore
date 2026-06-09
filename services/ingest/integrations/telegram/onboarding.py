"""services/ingest/integrations/telegram/onboarding.py — install + provision (IN-TELEGRAM).

Telegram authenticates with a persisted MTProto session (StringSession), not an
OAuth bot token. Onboarding mirrors the Jira/Mercury dedicated-table shape:

  finalize_install() — UPSERT a telegram_installations row, INSERT one
  telegram_dialogs row per dialog to shard, seed an empty telegram_update_state
  row (the live cursor the gateway worker advances), and emit an
  onboarding_triggers row (source='telegram') so the existing M6 backfill chain
  (oauth_poller → tenant_onboarding → source_onboarding → shard_fetch →
  reconciler) fires. All in one tenant-scoped transaction.

Topology B (ADR-0003 §6): `session_secret_ref` is the LIVE session the gateway
worker uses; `backfill_session_secret_ref` is a SECOND authorization on the same
account that the backfill fetcher uses, so the two never share one auth_key
across processes. The dialogs the gateway worker sees live are reconciled against
the same telegram_dialogs rows (live + backfill stay coherent).
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction


log = structlog.get_logger("integrations.telegram.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    account_label: str,
    dialogs: list[dict[str, Any]],
    api_id: str | None = None,
    api_hash_secret_ref: str | None = None,
    session_secret_ref: str | None = None,
    backfill_session_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its dialogs + live-state seed + an onboarding trigger.

    `dialogs` is the resolved set to backfill (enumerate via
    `TelegramClient.iter_dialogs` at connect time, or an operator inclusion
    list): each `{dialog_id, dialog_kind, access_hash?, title?}`. Returns the
    telegram_installations id. Idempotent on (tenant_id, account_label) and per
    (install, dialog_id).
    """
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO telegram_installations (
                id, tenant_id, account_label, api_id, api_hash_secret_ref,
                session_secret_ref, backfill_session_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, account_label) DO UPDATE
                SET api_id = COALESCE(EXCLUDED.api_id, telegram_installations.api_id),
                    api_hash_secret_ref = COALESCE(
                        EXCLUDED.api_hash_secret_ref, telegram_installations.api_hash_secret_ref),
                    session_secret_ref = COALESCE(
                        EXCLUDED.session_secret_ref, telegram_installations.session_secret_ref),
                    backfill_session_secret_ref = COALESCE(
                        EXCLUDED.backfill_session_secret_ref,
                        telegram_installations.backfill_session_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, account_label, api_id, api_hash_secret_ref,
            session_secret_ref, backfill_session_secret_ref,
        )

        dialog_ids: list[int] = []
        for d in dialogs:
            did = d.get("dialog_id")
            if not isinstance(did, int):
                continue
            kind = d.get("dialog_kind") or "chat"
            await tctx.execute(
                """
                INSERT INTO telegram_dialogs (
                    id, tenant_id, telegram_installation_id,
                    dialog_id, dialog_kind, access_hash, title, state
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
                ON CONFLICT (telegram_installation_id, dialog_id)
                    DO UPDATE SET state = 'active',
                                 access_hash = COALESCE(
                                     EXCLUDED.access_hash, telegram_dialogs.access_hash),
                                 title = COALESCE(EXCLUDED.title, telegram_dialogs.title)
                """,
                uuid7(), tenant_id, install_id, did, kind,
                d.get("access_hash"), d.get("title"),
            )
            dialog_ids.append(did)

        # Seed the live update-state row (pts/qts/seq/date NULL until the gateway
        # worker's first updates.getState). One row per install.
        await tctx.execute(
            """
            INSERT INTO telegram_update_state (id, tenant_id, telegram_installation_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (telegram_installation_id) DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
        )

        # Emit the onboarding trigger so the M6 backfill chain fires (source
        # 'telegram' admitted by migration 0094). Same dedup-index shape as Jira.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'telegram', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"account_label": account_label, "dialogs": dialog_ids}),
        )

    log.info(
        "telegram_install_finalized",
        account_label=account_label, dialog_count=len(dialog_ids),
    )
    return install_id


__all__ = ["finalize_install"]
