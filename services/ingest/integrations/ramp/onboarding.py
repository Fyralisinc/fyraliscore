"""services/ingest/integrations/ramp/onboarding.py — install + provision (finance).

Ramp authenticates with OAuth 2.0 client credentials (docs.ramp.com); the
install is scoped to the company ``business_id`` (discovered via the
``GET /business`` probe). Onboarding mirrors the Jira dedicated-table shape:

  finalize_install() — UPSERT a ramp_installations row, INSERT one
  ramp_entities row per entity type to shard (the verified taxonomy:
  transaction / reimbursement / card / user), and emit an onboarding_triggers
  row (source='ramp') so the existing M6 backfill chain fires. All in one
  tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='ramp', installation_id=business_id,
  secret_ref=webhook verifier token) so the webhook edge resolves the tenant +
  loads the signing secret via the existing machinery.

The access token is stored in encrypted_secrets; the install row carries
`secret_ref` (access token) + `token_expires_at`. `refresh_secret_ref` stores
encrypted client-credentials re-mint material when the install was finalized
with a Ramp client_id/client_secret pair. Client-credentials issues NO OAuth
refresh token; expiry is handled by re-minting.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.app.webhooks.provider_installations import (
    upsert_provider_installation_for_tenant,
)
from services.ingest.integrations.ramp import metrics
from services.ingest.integrations.ramp.client import DEFAULT_ENTITIES


log = structlog.get_logger("integrations.ramp.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    business_id: str,
    base_url: str,
    entities: list[str] | None = None,
    secret_ref: str | None = None,
    refresh_secret_ref: str | None = None,
    token_expires_at=None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its entity shards + an onboarding trigger atomically.

    Returns the ramp_installations id. Idempotent on (tenant_id, business_id)
    and per (install, entity_type).
    """
    base_url = base_url.rstrip("/")
    entity_list = list(entities) if entities else list(DEFAULT_ENTITIES)
    # Dedup defensively, preserve order.
    seen: set[str] = set()
    deduped = [e for e in entity_list if e and not (e in seen or seen.add(e))]

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO ramp_installations (
                id, tenant_id, business_id, base_url, secret_ref,
                refresh_secret_ref, token_expires_at, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (tenant_id, business_id) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, ramp_installations.secret_ref),
                    refresh_secret_ref = COALESCE(
                        EXCLUDED.refresh_secret_ref, ramp_installations.refresh_secret_ref),
                    token_expires_at = COALESCE(
                        EXCLUDED.token_expires_at, ramp_installations.token_expires_at),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, ramp_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, business_id, base_url, secret_ref,
            refresh_secret_ref, token_expires_at, webhook_secret_ref,
        )

        for entity in deduped:
            await tctx.execute(
                """
                INSERT INTO ramp_entities (
                    id, tenant_id, ramp_installation_id,
                    entity_type, state
                ) VALUES ($1, $2, $3, $4, 'active')
                ON CONFLICT (ramp_installation_id, entity_type)
                    DO UPDATE SET state = 'active'
                """,
                uuid7(), tenant_id, install_id, entity,
            )

        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'ramp', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"business_id": business_id, "entities": deduped}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_entities")
    log.info(
        "ramp_install_finalized",
        business_id=business_id, entity_count=len(deduped),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    business_id: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses to
    resolve the tenant + load the verifier token. installation_id is the businessId
    (matches tenant_resolver._extract_ramp)."""
    await upsert_provider_installation_for_tenant(
        pool,
        provider="ramp",
        tenant_id=tenant_id,
        installation_id=business_id,
        secret_ref=webhook_secret_ref,
    )
    log.info("ramp_webhook_installation_registered", business_id=business_id)


__all__ = ["finalize_install", "register_webhook_installation"]
