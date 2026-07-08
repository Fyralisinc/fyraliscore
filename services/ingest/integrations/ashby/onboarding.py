"""services/ingest/integrations/ashby/onboarding.py — install + provision (recruiting).

Ashby authenticates with a long-lived API key against the canonical Ashby API
host. Onboarding mirrors the Gusto entity-model shape (one shard per entity
type), but with API-key auth (NO OAuth refresh token):

  finalize_install() — UPSERT an ashby_installations row, INSERT one
  ashby_entities row per entity type to shard (ATS spine + org-level recruiting
  intelligence objects), and emit an onboarding_triggers row (source='ashby') so
  the existing M6 backfill chain fires. All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='ashby', installation_id=org_id,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant + loads
  the signing secret via the existing machinery. Backfill uses
  ashby_installations; live uses provider_installations — the two are seeded
  together but stay independent.

The API key is stored in encrypted_secrets behind `secret_ref`; there is NO
refresh token (API-key archetype, like Brex/Jira).
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
from services.ingest.integrations.ashby import metrics
from services.ingest.integrations.ashby.client import DEFAULT_ENTITIES


log = structlog.get_logger("integrations.ashby.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    org_id: str,
    base_url: str,
    entities: list[str] | None = None,
    secret_ref: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its entity shards + an onboarding trigger atomically.

    Returns the ashby_installations id. Idempotent on (tenant_id, org_id) and per
    (install, entity_type).
    """
    base_url = base_url.rstrip("/")
    entity_list = list(entities) if entities else list(DEFAULT_ENTITIES)
    # Dedup defensively, preserve order.
    seen: set[str] = set()
    deduped = [e for e in entity_list if e and not (e in seen or seen.add(e))]

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO ashby_installations (
                id, tenant_id, org_id, base_url, secret_ref,
                webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, org_id) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, ashby_installations.secret_ref),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, ashby_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, org_id, base_url, secret_ref,
            webhook_secret_ref,
        )

        for entity in deduped:
            await tctx.execute(
                """
                INSERT INTO ashby_entities (
                    id, tenant_id, ashby_installation_id,
                    entity_type, state
                ) VALUES ($1, $2, $3, $4, 'active')
                ON CONFLICT (ashby_installation_id, entity_type)
                    DO UPDATE SET state = 'active'
                """,
                uuid7(), tenant_id, install_id, entity,
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Gusto/Jira this is NOT a provider_installations source for backfill;
        # the install id rides in installation_row_id purely for the idempotency
        # dedup index. source='ashby' is admitted by migration 0106_ashby.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'ashby', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"org_id": org_id, "entities": deduped}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_entities")
    log.info(
        "ashby_install_finalized",
        org_id=org_id, entity_count=len(deduped),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    org_id: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses to
    resolve the tenant + load the HMAC signing secret.

    R3: installation_id is the per-install ENDPOINT segment — the tenant's Ashby
    webhook is configured with a distinct URL `/webhooks/ashby/{org_id}` (+ its
    own Ashby-Signature secret), and the resolver resolves the tenant from that
    URL path (real Ashby deliveries carry no org id in the body; see
    `tenant_resolver._PATH_RESOLVED_PROVIDERS`). `org_id` is the path segment;
    the body `organizationId` read by `_extract_ashby` is now only a
    legacy/synthetic fallback for posts to the bare endpoint."""
    await upsert_provider_installation_for_tenant(
        pool,
        provider="ashby",
        tenant_id=tenant_id,
        installation_id=org_id,
        secret_ref=webhook_secret_ref,
    )
    log.info("ashby_webhook_installation_registered", org_id=org_id)


__all__ = ["finalize_install", "register_webhook_installation"]
