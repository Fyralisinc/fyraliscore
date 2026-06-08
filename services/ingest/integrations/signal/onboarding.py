"""services/ingest/integrations/signal/onboarding.py — install + provision (IN-SIGNAL).

Signal authenticates with a persisted linked-device registration (the libsignal
identity/session store), not an OAuth token. Onboarding mirrors the
Telegram/Jira dedicated-table shape:

  finalize_install() — UPSERT a signal_installations row, INSERT one
  signal_threads row per thread to shard, seed an empty signal_update_state row
  (the live cursor the gateway worker advances), and emit an onboarding_triggers
  row (source='signal') so the existing M6 backfill chain (oauth_poller →
  tenant_onboarding → source_onboarding → shard_fetch → reconciler) fires. All in
  one tenant-scoped transaction.

Topology B (ADR-0003 §6): `session_secret_ref` is the LIVE linked-device session
the gateway worker uses; `backfill_session_secret_ref` is a SECOND linked device
on the same account that the backfill fetcher uses, so the two never share one
device registration across processes. The threads the gateway worker sees live
are reconciled against the same signal_threads rows (live + backfill stay
coherent).

COVERAGE: own/linked-account only — these are the threads the linked Signal
account participates in (self-coverage, like Telegram's user-account session).
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction


log = structlog.get_logger("integrations.signal.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    account_label: str,
    threads: list[dict[str, Any]],
    session_secret_ref: str | None = None,
    backfill_session_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its threads + live-state seed + an onboarding trigger.

    `threads` is the resolved set to backfill (enumerate via
    `SignalClient.iter_threads` at connect time, or an operator inclusion list):
    each `{thread_id, thread_kind?, title?}`. Returns the signal_installations id.
    Idempotent on (tenant_id, account_label) and per (install, thread_id).
    """
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO signal_installations (
                id, tenant_id, account_label,
                session_secret_ref, backfill_session_secret_ref
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id, account_label) DO UPDATE
                SET session_secret_ref = COALESCE(
                        EXCLUDED.session_secret_ref, signal_installations.session_secret_ref),
                    backfill_session_secret_ref = COALESCE(
                        EXCLUDED.backfill_session_secret_ref,
                        signal_installations.backfill_session_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, account_label,
            session_secret_ref, backfill_session_secret_ref,
        )

        thread_ids: list[int] = []
        for t in threads:
            tid = t.get("thread_id")
            if not isinstance(tid, int):
                continue
            kind = t.get("thread_kind") or "direct"
            await tctx.execute(
                """
                INSERT INTO signal_threads (
                    id, tenant_id, signal_installation_id,
                    thread_id, thread_kind, title, state
                ) VALUES ($1, $2, $3, $4, $5, $6, 'active')
                ON CONFLICT (signal_installation_id, thread_id)
                    DO UPDATE SET state = 'active',
                                 title = COALESCE(EXCLUDED.title, signal_threads.title)
                """,
                uuid7(), tenant_id, install_id, tid, kind, t.get("title"),
            )
            thread_ids.append(tid)

        # Seed the live update-state row (cursor NULL until the gateway worker's
        # first sync). One row per install.
        await tctx.execute(
            """
            INSERT INTO signal_update_state (id, tenant_id, signal_installation_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (signal_installation_id) DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
        )

        # Emit the onboarding trigger so the M6 backfill chain fires (source
        # 'signal' admitted by migration 0100). Same dedup-index shape as Telegram.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'signal', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"account_label": account_label, "threads": thread_ids}),
        )

    log.info(
        "signal_install_finalized",
        account_label=account_label, thread_count=len(thread_ids),
    )
    return install_id


__all__ = ["finalize_install"]
