"""services/ingest/integrations/linkedin/onboarding.py — install + provision.

LinkedIn authenticates with OAuth 2.0; every call is scoped to an
``organization_urn``. Onboarding mirrors the Carta/Gusto dedicated-table shape:

  finalize_install() — UPSERT a linkedin_installations row, INSERT one
  linkedin_entities row per entity type to shard (share/social_action/
  follower_stat), and emit an onboarding_triggers row (source='linkedin')
  so the existing M6 backfill chain fires. All in one tenant-scoped transaction.

LinkedIn is POLL-ONLY: there is NO webhook, so there is NO
register_webhook_installation() and NO webhook_secret_ref. The live edge
(`services/ingest/integrations/linkedin/poll.py`) re-lists changed objects on an
interval and resolves the tenant directly from linkedin_installations.

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
from services.ingest.integrations.linkedin import metrics
from services.ingest.integrations.linkedin.client import DEFAULT_ENTITIES


log = structlog.get_logger("integrations.linkedin.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    organization_urn: str,
    base_url: str,
    entities: list[str] | None = None,
    secret_ref: str | None = None,
    refresh_secret_ref: str | None = None,
    token_expires_at=None,
) -> UUID:
    """UPSERT the install + its entity shards + an onboarding trigger atomically.

    Returns the linkedin_installations id. Idempotent on
    (tenant_id, organization_urn) and per (install, entity_type).
    """
    base_url = base_url.rstrip("/")
    entity_list = list(entities) if entities else list(DEFAULT_ENTITIES)
    # Dedup defensively, preserve order.
    seen: set[str] = set()
    deduped = [e for e in entity_list if e and not (e in seen or seen.add(e))]

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO linkedin_installations (
                id, tenant_id, organization_urn, base_url, secret_ref,
                refresh_secret_ref, token_expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, organization_urn) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, linkedin_installations.secret_ref),
                    refresh_secret_ref = COALESCE(
                        EXCLUDED.refresh_secret_ref, linkedin_installations.refresh_secret_ref),
                    token_expires_at = COALESCE(
                        EXCLUDED.token_expires_at, linkedin_installations.token_expires_at),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, organization_urn, base_url, secret_ref,
            refresh_secret_ref, token_expires_at,
        )

        for entity in deduped:
            await tctx.execute(
                """
                INSERT INTO linkedin_entities (
                    id, tenant_id, linkedin_installation_id,
                    entity_type, state
                ) VALUES ($1, $2, $3, $4, 'active')
                ON CONFLICT (linkedin_installation_id, entity_type)
                    DO UPDATE SET state = 'active'
                """,
                uuid7(), tenant_id, install_id, entity,
            )

        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'linkedin', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"organization_urn": organization_urn, "entities": deduped}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_entities")
    log.info(
        "linkedin_install_finalized",
        organization_urn=organization_urn, entity_count=len(deduped),
    )
    return install_id


__all__ = ["finalize_install"]
