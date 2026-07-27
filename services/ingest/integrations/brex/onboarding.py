"""services/ingest/integrations/brex/onboarding.py — install + provision (finance).

Brex authenticates with a long-lived API token against the canonical Brex
API host. Onboarding mirrors the Jira dedicated-table shape (NOT the OAuth
bot-token path):

  finalize_install() — UPSERT a brex_installations row, INSERT one
  brex_accounts row per account to shard, and emit an onboarding_triggers row
  (source='brex') so the existing M6 backfill chain (oauth_poller ->
  tenant_onboarding -> source_onboarding -> shard_fetch -> reconciler) fires.
  All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='brex', installation_id=organization_id,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant +
  loads the signing secret via the existing machinery. Backfill uses
  brex_installations; live uses provider_installations — the two are seeded
  together but stay independent.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from lib.shared.provider_installations import (
    upsert_provider_installation_for_tenant,
)
from services.ingest.integrations.brex import metrics


log = structlog.get_logger("integrations.brex.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    accounts: list[dict],
    secret_ref: str | None = None,
    organization_id: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its accounts + an onboarding trigger atomically.

    `accounts` is the resolved set of accounts to backfill (enumerate via
    BrexClient.list_accounts at seed time); each dict carries at least
    ``account_id`` and optionally ``account_name`` / ``account_kind``. Returns
    the brex_installations id. Idempotent on the exact
    ``(tenant_id, organization_id)`` provider scope when it is known, with
    ``(tenant_id, base_url)`` retained only for unresolved legacy installs, and
    per ``(install, account_id)``.
    """
    base_url = base_url.rstrip("/")
    organization_id = (
        organization_id.strip()
        if organization_id and organization_id.strip()
        else None
    )
    # Dedup accounts defensively on the natural key.
    seen: set[str] = set()
    deduped: list[dict] = []
    for a in accounts:
        acct_id = str(a.get("account_id") or a.get("id") or "")
        if acct_id and acct_id not in seen:
            seen.add(acct_id)
            deduped.append({**a, "account_id": acct_id})

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO brex_installations (
                id, tenant_id, base_url, secret_ref,
                organization_id, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (
                tenant_id,
                (organization_id IS NULL),
                (COALESCE(organization_id, base_url))
            ) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, brex_installations.secret_ref),
                    organization_id = COALESCE(EXCLUDED.organization_id, brex_installations.organization_id),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, brex_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, base_url, secret_ref,
            organization_id, webhook_secret_ref,
        )

        for a in deduped:
            await tctx.execute(
                """
                INSERT INTO brex_accounts (
                    id, tenant_id, brex_installation_id,
                    account_id, account_name, account_kind, state
                ) VALUES ($1, $2, $3, $4, $5, $6, 'active')
                ON CONFLICT (brex_installation_id, account_id)
                    DO UPDATE SET state = 'active',
                                  account_name = COALESCE(EXCLUDED.account_name, brex_accounts.account_name),
                                  account_kind = COALESCE(EXCLUDED.account_kind, brex_accounts.account_kind)
                """,
                uuid7(), tenant_id, install_id, a["account_id"],
                a.get("account_name") or a.get("name"),
                a.get("account_kind") or a.get("type"),
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Jira/Drive this is NOT a provider_installations source; the install id
        # rides in installation_row_id purely for the idempotency dedup index.
        # source='brex' is admitted by migration 0095_brex (owned by the
        # shared-file / migration agent; blueprint §3).
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'brex', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"base_url": base_url,
                        "accounts": [a["account_id"] for a in deduped]}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_accounts")
    log.info(
        "brex_install_finalized",
        base_url=base_url, account_count=len(deduped),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    organization_id: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses
    to resolve the tenant + load the HMAC signing secret. installation_id is the
    Brex organization id (matches tenant_resolver._extract_brex)."""
    await upsert_provider_installation_for_tenant(
        pool,
        provider="brex",
        tenant_id=tenant_id,
        installation_id=organization_id,
        secret_ref=webhook_secret_ref,
    )
    log.info("brex_webhook_installation_registered", organization_id=organization_id)


__all__ = ["finalize_install", "register_webhook_installation"]
