"""services/ingest/integrations/deel/onboarding.py — install + provision (finance).

Deel authenticates with a long-lived API token against the canonical Deel
API host. Onboarding mirrors the Jira dedicated-table shape (NOT the OAuth
bot-token path):

  finalize_install() — UPSERT a deel_installations row, INSERT one
  deel_contracts row per contract to shard, and emit an onboarding_triggers row
  (source='deel') so the existing M6 backfill chain (oauth_poller ->
  tenant_onboarding -> source_onboarding -> shard_fetch -> reconciler) fires.
  All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='deel', installation_id=organization_id,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant +
  loads the signing secret via the existing machinery. Backfill uses
  deel_installations; live uses provider_installations — the two are seeded
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
from services.ingest.integrations.deel import metrics


log = structlog.get_logger("integrations.deel.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    contracts: list[dict],
    secret_ref: str | None = None,
    organization_id: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its contracts + an onboarding trigger atomically.

    `contracts` is the resolved set of contracts to backfill (enumerate via
    DeelClient.list_contracts at seed time); each dict carries at least
    ``contract_id`` and optionally ``contract_name`` / ``contract_type``. Returns
    the deel_installations id. Idempotent on (tenant_id, base_url) and per
    (install, contract_id).
    """
    base_url = base_url.rstrip("/")
    # Dedup contracts defensively on the natural key.
    seen: set[str] = set()
    deduped: list[dict] = []
    for c in contracts:
        con_id = str(c.get("contract_id") or c.get("id") or "")
        if con_id and con_id not in seen:
            seen.add(con_id)
            deduped.append({**c, "contract_id": con_id})

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO deel_installations (
                id, tenant_id, base_url, secret_ref,
                organization_id, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, base_url) DO UPDATE
                SET secret_ref = COALESCE(EXCLUDED.secret_ref, deel_installations.secret_ref),
                    organization_id = COALESCE(EXCLUDED.organization_id, deel_installations.organization_id),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, deel_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, base_url, secret_ref,
            organization_id, webhook_secret_ref,
        )

        for c in deduped:
            await tctx.execute(
                """
                INSERT INTO deel_contracts (
                    id, tenant_id, deel_installation_id,
                    contract_id, contract_name, contract_type, state
                ) VALUES ($1, $2, $3, $4, $5, $6, 'active')
                ON CONFLICT (deel_installation_id, contract_id)
                    DO UPDATE SET state = 'active',
                                  contract_name = COALESCE(EXCLUDED.contract_name, deel_contracts.contract_name),
                                  contract_type = COALESCE(EXCLUDED.contract_type, deel_contracts.contract_type)
                """,
                uuid7(), tenant_id, install_id, c["contract_id"],
                c.get("contract_name") or c.get("name"),
                c.get("contract_type") or c.get("type"),
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Jira/Drive this is NOT a provider_installations source; the install id
        # rides in installation_row_id purely for the idempotency dedup index.
        # source='deel' is admitted by the deel migration.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'deel', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"base_url": base_url,
                        "contracts": [c["contract_id"] for c in deduped]}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_contracts")
    log.info(
        "deel_install_finalized",
        base_url=base_url, contract_count=len(deduped),
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
    Deel organization id (matches tenant_resolver._extract_deel)."""
    await upsert_provider_installation_for_tenant(
        pool,
        provider="deel",
        tenant_id=tenant_id,
        installation_id=organization_id,
        secret_ref=webhook_secret_ref,
    )
    log.info("deel_webhook_installation_registered", organization_id=organization_id)


__all__ = ["finalize_install", "register_webhook_installation"]
