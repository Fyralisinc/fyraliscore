"""services/ingest/integrations/jira/onboarding.py — install + provision (IN-17).

Jira authenticates with an account email + API token against a per-tenant site
(see specs/IN-17-jira-integration/plan.md §3/§4). Onboarding mirrors the
Gmail/Calendar dedicated-table shape (NOT the OAuth bot-token path):

  finalize_install() — UPSERT a jira_installations row, INSERT one jira_projects
  row per project to shard, and emit an onboarding_triggers row (source='jira')
  so the existing M6 backfill chain (oauth_poller -> tenant_onboarding ->
  source_onboarding -> shard_fetch -> reconciler) fires. All in one
  tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='jira', installation_id=site host,
  secret_ref=webhook HMAC secret) so the webhook edge resolves the tenant +
  loads the signing secret via the existing machinery. Backfill uses
  jira_installations; live uses provider_installations — the two are seeded
  together but stay independent.
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


log = structlog.get_logger("integrations.jira.onboarding")


def site_host(base_url: str) -> str:
    """The site host used as the provider_installations.installation_id for
    webhook tenant resolution (e.g. https://acme.atlassian.net -> acme.atlassian.net)."""
    return base_url.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    account_email: str,
    project_keys: list[str],
    secret_ref: str | None = None,
    cloud_id: str | None = None,
    webhook_secret_ref: str | None = None,
    project_meta: dict[str, dict] | None = None,
) -> UUID:
    """UPSERT the install + its projects + an onboarding trigger atomically.

    `project_keys` is the resolved set of projects to backfill (enumerate via
    JiraClient.list_projects at seed time). Returns the jira_installations id.
    Idempotent on (tenant_id, base_url) and per (install, project_key).
    """
    base_url = base_url.rstrip("/")
    keys = sorted({k for k in project_keys if k})
    meta = project_meta or {}

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO jira_installations (
                id, tenant_id, base_url, account_email, secret_ref,
                cloud_id, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, base_url) DO UPDATE
                SET account_email = EXCLUDED.account_email,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, jira_installations.secret_ref),
                    cloud_id = COALESCE(EXCLUDED.cloud_id, jira_installations.cloud_id),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, jira_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, base_url, account_email, secret_ref,
            cloud_id, webhook_secret_ref,
        )

        for key in keys:
            m = meta.get(key, {})
            await tctx.execute(
                """
                INSERT INTO jira_projects (
                    id, tenant_id, jira_installation_id,
                    project_key, project_id, project_name, state
                ) VALUES ($1, $2, $3, $4, $5, $6, 'active')
                ON CONFLICT (jira_installation_id, project_key)
                    DO UPDATE SET state = 'active',
                                 project_id = COALESCE(EXCLUDED.project_id, jira_projects.project_id),
                                 project_name = COALESCE(EXCLUDED.project_name, jira_projects.project_name)
                """,
                uuid7(), tenant_id, install_id, key,
                m.get("project_id"), m.get("project_name"),
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Gmail/Calendar this is NOT a provider_installations source; the
        # install id rides in installation_row_id purely for the idempotency
        # dedup index. source='jira' is admitted by migration 0062.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'jira', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"base_url": base_url, "projects": keys}),
        )

    log.info(
        "jira_install_finalized",
        base_url=base_url, project_count=len(keys),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    webhook_secret_ref: str | None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses
    to resolve the tenant + load the HMAC signing secret. installation_id is
    the site host (matches tenant_resolver._extract_jira)."""
    host = site_host(base_url)
    await upsert_provider_installation_for_tenant(
        pool,
        provider="jira",
        tenant_id=tenant_id,
        installation_id=host,
        secret_ref=webhook_secret_ref,
    )
    log.info("jira_webhook_installation_registered", host=host)


__all__ = ["finalize_install", "register_webhook_installation", "site_host"]
