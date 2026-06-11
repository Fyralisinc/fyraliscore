"""services/ingest/integrations/carta/onboarding.py — install + provision (cap-table).

Carta authenticates with OAuth 2.0; every read is scoped to an **issuer**
(`/v1alpha1/issuers/{issuer_id}/...`). The issuer id is stored in
``carta_installations.firm_id`` (the column predates the issuer naming).
Onboarding mirrors the Gusto dedicated-table shape:

  finalize_install() — UPSERT a carta_installations row, INSERT one
  carta_entities row per entity type to shard (stakeholder / shareClass /
  optionGrant / convertibleNote — the real /v1alpha1 issuer collections), and
  emit an onboarding_triggers row (source='carta') so the existing M6 backfill
  chain fires. All in one tenant-scoped transaction.

Carta is POLL-ONLY: there is NO webhook, so there is NO
register_webhook_installation() and NO webhook_secret_ref. The live edge
(`services/ingest/integrations/carta/poll.py`) re-lists changed objects on an
interval and resolves the tenant directly from carta_installations.

Secrets in encrypted_secrets; the install row carries `secret_ref` (the ~1 h
access token) and `refresh_secret_ref` (the client_credentials SECRET used to
RE-MINT the access token — Carta has no OAuth refresh-token grant).
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.carta import metrics
from services.ingest.integrations.carta.client import DEFAULT_ENTITIES


log = structlog.get_logger("integrations.carta.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    firm_id: str,
    base_url: str,
    entities: list[str] | None = None,
    secret_ref: str | None = None,
    refresh_secret_ref: str | None = None,
    token_expires_at=None,
) -> UUID:
    """UPSERT the install + its entity shards + an onboarding trigger atomically.

    Returns the carta_installations id. Idempotent on (tenant_id, firm_id)
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
            INSERT INTO carta_installations (
                id, tenant_id, firm_id, base_url, secret_ref,
                refresh_secret_ref, token_expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, firm_id) DO UPDATE
                SET base_url = EXCLUDED.base_url,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, carta_installations.secret_ref),
                    refresh_secret_ref = COALESCE(
                        EXCLUDED.refresh_secret_ref, carta_installations.refresh_secret_ref),
                    token_expires_at = COALESCE(
                        EXCLUDED.token_expires_at, carta_installations.token_expires_at),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, firm_id, base_url, secret_ref,
            refresh_secret_ref, token_expires_at,
        )

        for entity in deduped:
            await tctx.execute(
                """
                INSERT INTO carta_entities (
                    id, tenant_id, carta_installation_id,
                    entity_type, state
                ) VALUES ($1, $2, $3, $4, 'active')
                ON CONFLICT (carta_installation_id, entity_type)
                    DO UPDATE SET state = 'active'
                """,
                uuid7(), tenant_id, install_id, entity,
            )

        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'carta', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"firm_id": firm_id, "entities": deduped}),
        )

    metrics.record_provision_outcome("success" if deduped else "no_entities")
    log.info(
        "carta_install_finalized",
        firm_id=firm_id, entity_count=len(deduped),
    )
    return install_id


__all__ = ["finalize_install"]
