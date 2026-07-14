"""services/ingest/integrations/fireflies/onboarding.py — install + provision.

Fireflies authenticates with a long-lived API token against the canonical
Fireflies API host. Onboarding mirrors the Jira/Brex dedicated-table shape (NOT
the OAuth bot-token path), but Fireflies is workspace-scoped with NO sharded
child resource — a workspace's transcripts are a single stream — so there is
ONE install table (`fireflies_installations`) and the planner emits exactly one
shard per workspace install:

  finalize_install() — UPSERT a fireflies_installations row carrying the
  workspace_id the planner shards on, and emit an onboarding_triggers row
  (source='fireflies') so the existing M6 backfill chain (oauth_poller ->
  tenant_onboarding -> source_onboarding -> shard_fetch -> reconciler) fires.
  All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='fireflies', installation_id=workspace_id,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant +
  loads the signing secret via the existing machinery. Backfill uses
  fireflies_installations; live uses provider_installations — the two are seeded
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
from services.ingest.integrations.fireflies import metrics


log = structlog.get_logger("integrations.fireflies.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    workspace_id: str,
    workspace_name: str | None = None,
    secret_ref: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + an onboarding trigger atomically.

    `workspace_id` is the Fireflies workspace the token is scoped to — it
    namespaces every transcript's external_id (`fireflies:{workspace_id}:…`) so
    two tenants' transcripts never collide on the global observations UNIQUE.
    Returns the fireflies_installations id. Idempotent on (tenant_id, base_url).
    """
    base_url = base_url.rstrip("/")

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO fireflies_installations (
                id, tenant_id, base_url, workspace_id, workspace_name,
                secret_ref, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, base_url) DO UPDATE
                SET secret_ref = COALESCE(EXCLUDED.secret_ref, fireflies_installations.secret_ref),
                    workspace_id = COALESCE(EXCLUDED.workspace_id, fireflies_installations.workspace_id),
                    workspace_name = COALESCE(EXCLUDED.workspace_name, fireflies_installations.workspace_name),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, fireflies_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, base_url, workspace_id, workspace_name,
            secret_ref, webhook_secret_ref,
        )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Jira/Brex this is NOT a provider_installations source; the install id
        # rides in installation_row_id purely for the idempotency dedup index.
        # source='fireflies' is admitted by migration 0099_fireflies (owned by
        # the shared-file / migration agent).
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'fireflies', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"base_url": base_url, "workspace_id": workspace_id}),
        )

    metrics.record_provision_outcome("success" if workspace_id else "no_transcripts")
    log.info(
        "fireflies_install_finalized",
        base_url=base_url, workspace_id=workspace_id,
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    workspace_id: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses
    to resolve the tenant + load the HMAC signing secret. installation_id is the
    Fireflies workspace id (matches tenant_resolver._extract_fireflies)."""
    await upsert_provider_installation_for_tenant(
        pool,
        provider="fireflies",
        tenant_id=tenant_id,
        installation_id=workspace_id,
        secret_ref=webhook_secret_ref,
    )
    log.info("fireflies_webhook_installation_registered", workspace_id=workspace_id)


__all__ = ["finalize_install", "register_webhook_installation"]
