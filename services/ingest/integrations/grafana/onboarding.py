"""services/ingest/integrations/grafana/onboarding.py — install + provision (IN-GRAFANA).

Grafana authenticates with a service-account token (Bearer) against a per-tenant
instance base_url. Onboarding mirrors the Jira/Mercury dedicated-table shape, but
Grafana has NO per-resource child table (annotations + alert state are org-wide,
so one shard per install):

  finalize_install() — UPSERT a grafana_installations row and emit an
  onboarding_triggers row (source='grafana') so the existing M6 backfill chain
  (oauth_poller -> tenant_onboarding -> source_onboarding -> shard_fetch ->
  reconciler) fires. All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='grafana', installation_id=instance host,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant +
  loads the X-Grafana-Alerting-Signature secret via the existing machinery.
  Backfill uses grafana_installations; live uses provider_installations — the
  two are seeded together but stay independent.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction


log = structlog.get_logger("integrations.grafana.onboarding")


def instance_host(base_url: str) -> str:
    """The instance host used as the provider_installations.installation_id for
    webhook tenant resolution (e.g. https://acme.grafana.net -> acme.grafana.net).
    MUST match what tenant_resolver._extract_grafana derives from the webhook
    payload's `externalURL` host."""
    return base_url.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    org_id: str = "1",
    secret_ref: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + an onboarding trigger atomically.

    Returns the grafana_installations id. Idempotent on (tenant_id, base_url).
    """
    base_url = base_url.rstrip("/")

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO grafana_installations (
                id, tenant_id, base_url, org_id, secret_ref, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, base_url) DO UPDATE
                SET org_id = EXCLUDED.org_id,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, grafana_installations.secret_ref),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, grafana_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, base_url, org_id, secret_ref, webhook_secret_ref,
        )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Jira/Mercury this is NOT a provider_installations source; the install
        # id rides in installation_row_id purely for the idempotency dedup index.
        # source='grafana' is admitted by migration 0080.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'grafana', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"base_url": base_url, "org_id": org_id}),
        )

    log.info("grafana_install_finalized", base_url=base_url, org_id=org_id)
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses
    to resolve the tenant + load the HMAC signing secret. installation_id is the
    instance host (matches tenant_resolver._extract_grafana)."""
    host = instance_host(base_url)
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'grafana', $3, $4, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                secret_ref = EXCLUDED.secret_ref,
                enabled = TRUE
        """,
        uuid7(), tenant_id, host, webhook_secret_ref,
    )
    log.info("grafana_webhook_installation_registered", host=host)


__all__ = ["finalize_install", "register_webhook_installation", "instance_host"]
