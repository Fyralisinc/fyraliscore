"""services/ingest/integrations/gusto/onboarding.py — install + provision (finance).

Gusto authenticates with OAuth 2.0; every call is scoped to a company
``companyId``. Onboarding mirrors the Jira dedicated-table shape:

  finalize_install() — UPSERT a gusto_installations row, INSERT one
  gusto_entities row per entity type to shard (Invoice/Bill/BillPayment/
  Payment), and emit an onboarding_triggers row (source='gusto') so the
  existing M6 backfill chain fires. All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='gusto', installation_id=company_uuid,
  secret_ref=webhook verifier token) so the webhook edge resolves the tenant +
  loads the signing secret via the existing machinery.

The access + refresh tokens are stored in encrypted_secrets; the install row
carries `secret_ref` (access token) and `refresh_secret_ref` (rotating refresh
token, owned by the oauth_poller in production).
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.gusto import metrics
from services.ingest.integrations.gusto.client import DEFAULT_ENTITIES


log = structlog.get_logger("integrations.gusto.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    company_uuid: str,
    base_url: str,
    entities: list[str] | None = None,
    secret_ref: str | None = None,
    refresh_secret_ref: str | None = None,
    token_expires_at=None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its entity shards + an onboarding trigger atomically.

    Returns the gusto_installations id. Idempotent on (tenant_id, company_uuid)
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
            INSERT INTO gusto_installations (
                id, tenant_id, company_uuid, base_url, secret_ref,
                refresh_secret_ref, token_expires_at, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (tenant_id, company_uuid) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, gusto_installations.secret_ref),
                    refresh_secret_ref = COALESCE(
                        EXCLUDED.refresh_secret_ref, gusto_installations.refresh_secret_ref),
                    token_expires_at = COALESCE(
                        EXCLUDED.token_expires_at, gusto_installations.token_expires_at),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, gusto_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, company_uuid, base_url, secret_ref,
            refresh_secret_ref, token_expires_at, webhook_secret_ref,
        )

        for entity in deduped:
            await tctx.execute(
                """
                INSERT INTO gusto_entities (
                    id, tenant_id, gusto_installation_id,
                    entity_type, state
                ) VALUES ($1, $2, $3, $4, 'active')
                ON CONFLICT (gusto_installation_id, entity_type)
                    DO UPDATE SET state = 'active'
                """,
                uuid7(), tenant_id, install_id, entity,
            )

        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'gusto', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"company_uuid": company_uuid, "entities": deduped}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_entities")
    log.info(
        "gusto_install_finalized",
        company_uuid=company_uuid, entity_count=len(deduped),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    company_uuid: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses to
    resolve the tenant + load the verifier token. installation_id is the companyId
    (matches tenant_resolver._extract_gusto)."""
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'gusto', $3, $4, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                secret_ref = EXCLUDED.secret_ref,
                enabled = TRUE
        """,
        uuid7(), tenant_id, company_uuid, webhook_secret_ref,
    )
    log.info("gusto_webhook_installation_registered", company_uuid=company_uuid)


__all__ = ["finalize_install", "register_webhook_installation"]
