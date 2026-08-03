"""services/ingest/integrations/hibob/onboarding.py — install + provision (People/HR).

HiBob authenticates with a **service user** (id + token, HTTP Basic). Onboarding
mirrors the Gusto entity-model dedicated-table shape:

  finalize_install() — UPSERT a hibob_installations row, INSERT one
  hibob_entities row per entity type to shard (employee / lifecycle / timeoff /
  payroll), and emit an onboarding_triggers row (source='hibob') so the existing
  M6 backfill chain (tenant_onboarding -> source_onboarding -> shard_fetch ->
  reconciler) fires. All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='hibob', installation_id=company_id,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant + loads
  the signing secret via the existing machinery. Backfill uses
  hibob_installations; live uses provider_installations — the two are seeded
  together but stay independent.

The service-user token is stored in encrypted_secrets; the install row carries
``secret_ref`` (the token half) and the public ``service_user_id`` (the id half
of the Basic credential). There is NO refresh token (long-lived credential — the
Brex posture, not OAuth).
"""

from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.provider_installations import (
    upsert_provider_installation_for_tenant,
)
from services.ingest.integrations.hibob import metrics
from services.ingest.integrations.hibob.client import DEFAULT_ENTITIES


log = structlog.get_logger("integrations.hibob.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    company_id: str,
    service_user_id: str,
    base_url: str,
    entities: list[str] | None = None,
    secret_ref: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its entity shards + an onboarding trigger atomically.

    Returns the hibob_installations id. Idempotent on (tenant_id, company_id)
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
            INSERT INTO hibob_installations (
                id, tenant_id, company_id, service_user_id, base_url,
                secret_ref, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, company_id) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    service_user_id = COALESCE(
                        EXCLUDED.service_user_id, hibob_installations.service_user_id),
                    secret_ref = COALESCE(
                        EXCLUDED.secret_ref, hibob_installations.secret_ref),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, hibob_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(),
            tenant_id,
            company_id,
            service_user_id,
            base_url,
            secret_ref,
            webhook_secret_ref,
        )

        for entity in deduped:
            await tctx.execute(
                """
                INSERT INTO hibob_entities (
                    id, tenant_id, hibob_installation_id,
                    entity_type, state
                ) VALUES ($1, $2, $3, $4, 'active')
                ON CONFLICT (hibob_installation_id, entity_type)
                    DO UPDATE SET state = 'active'
                """,
                uuid7(),
                tenant_id,
                install_id,
                entity,
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Gusto/Jira this is NOT a provider_installations source; the install id
        # rides in installation_row_id purely for the idempotency dedup index.
        # source='hibob' is admitted by migration 0105_hibob.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'hibob', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(),
            tenant_id,
            install_id,
            json.dumps({"company_id": company_id, "entities": deduped}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_entities")
    log.info(
        "hibob_install_finalized",
        company_id=company_id,
        entity_count=len(deduped),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    company_id: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses to
    resolve the tenant + load the HMAC signing secret. installation_id is the
    company_id (matches tenant_resolver._extract_hibob, which reads the
    ``companyId`` webhook-body field).

    TODO(human): confirm real HiBob webhook tenant-resolution. In production
        HiBob resolves the destination by the webhook endpoint/secret, NOT a body
        field — the ``companyId`` body field is the gate stand-in. When the real
        per-endpoint secret model is wired, installation_id should key on the
        endpoint identifier, not the company id.
    """
    await upsert_provider_installation_for_tenant(
        pool,
        provider="hibob",
        tenant_id=tenant_id,
        installation_id=company_id,
        secret_ref=webhook_secret_ref,
    )
    log.info("hibob_webhook_installation_registered", company_id=company_id)


__all__ = ["finalize_install", "register_webhook_installation"]
