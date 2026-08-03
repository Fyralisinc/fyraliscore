"""services/ingest/integrations/figma/onboarding.py — install + provision (design).

Figma authenticates with a long-lived org/team access token against the
canonical Figma API host. Onboarding mirrors the Brex dedicated-table shape (NOT
the OAuth bot-token path):

  finalize_install() — UPSERT a figma_installations row, INSERT one
  figma_files row per file to shard, and emit an onboarding_triggers row
  (source='figma') so the existing M6 backfill chain (oauth_poller ->
  tenant_onboarding -> source_onboarding -> shard_fetch -> reconciler) fires.
  All in one tenant-scoped transaction.

  register_webhook_installation() — register the LIVE-path row in
  provider_installations (provider='figma', installation_id=webhook_id,
  secret_ref=webhook secret) so the webhook edge resolves the tenant + loads the
  signing secret via the existing machinery. Backfill uses figma_installations;
  live uses provider_installations — the two are seeded together but stay
  independent.

  R2 — install key: REAL Figma Webhooks V2 deliveries carry the Figma-assigned
  `webhook_id` (returned by `POST /v2/webhooks` at registration) and NO
  `team_id` in the event body, so the live row is keyed by `webhook_id` to match
  `tenant_resolver._extract_figma`. The caller captures the webhook_id from the
  webhook-creation response; `team_id` is retained as backfill context only.

  NOTE (webhook auth divergence): real Figma webhooks carry a PASSCODE in the
  request body rather than an HMAC header (see signatures/figma.py). The
  `webhook_secret_ref` therefore points at the per-tenant passcode in production;
  for the synthetic gate it is treated as an HMAC signing secret. The install
  shape is identical either way.
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
from services.ingest.integrations.figma import metrics


log = structlog.get_logger("integrations.figma.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    base_url: str,
    files: list[dict],
    secret_ref: str | None = None,
    team_id: str | None = None,
    webhook_secret_ref: str | None = None,
) -> UUID:
    """UPSERT the install + its files + an onboarding trigger atomically.

    `files` is the resolved set of files to backfill (enumerate via
    FigmaClient.list_files at seed time); each dict carries at least
    ``file_key`` and optionally ``file_name`` / ``project_name``. Returns the
    figma_installations id. Idempotent on (tenant_id, base_url) and per
    (install, file_key).
    """
    base_url = base_url.rstrip("/")
    # Dedup files defensively on the natural key.
    seen: set[str] = set()
    deduped: list[dict] = []
    for f in files:
        file_key = str(f.get("file_key") or f.get("key") or "")
        if file_key and file_key not in seen:
            seen.add(file_key)
            deduped.append({**f, "file_key": file_key})

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO figma_installations (
                id, tenant_id, base_url, secret_ref,
                team_id, webhook_secret_ref
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, base_url) DO UPDATE
                SET secret_ref = COALESCE(EXCLUDED.secret_ref, figma_installations.secret_ref),
                    team_id = COALESCE(EXCLUDED.team_id, figma_installations.team_id),
                    webhook_secret_ref = COALESCE(
                        EXCLUDED.webhook_secret_ref, figma_installations.webhook_secret_ref),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(),
            tenant_id,
            base_url,
            secret_ref,
            team_id,
            webhook_secret_ref,
        )

        for f in deduped:
            await tctx.execute(
                """
                INSERT INTO figma_files (
                    id, tenant_id, figma_installation_id,
                    file_key, file_name, project_name, state
                ) VALUES ($1, $2, $3, $4, $5, $6, 'active')
                ON CONFLICT (figma_installation_id, file_key)
                    DO UPDATE SET state = 'active',
                                  file_name = COALESCE(EXCLUDED.file_name, figma_files.file_name),
                                  project_name = COALESCE(EXCLUDED.project_name, figma_files.project_name)
                """,
                uuid7(),
                tenant_id,
                install_id,
                f["file_key"],
                f.get("file_name") or f.get("name"),
                f.get("project_name") or f.get("project"),
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Brex/Jira this is NOT a provider_installations source; the install id
        # rides in installation_row_id purely for the idempotency dedup index.
        # source='figma' is admitted by migration 0103_figma (owned by the
        # shared-file / migration agent; this file owns the migration here).
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'figma', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(),
            tenant_id,
            install_id,
            json.dumps(
                {"base_url": base_url, "files": [f["file_key"] for f in deduped]}
            ),
        )

    metrics.record_provision_outcome("success" if deduped else "no_files")
    log.info(
        "figma_install_finalized",
        base_url=base_url,
        file_count=len(deduped),
    )
    return install_id


async def register_webhook_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    webhook_id: str,
    webhook_secret_ref: str | None,
    team_id: str | None = None,
) -> None:
    """Register / refresh the provider_installations row the webhook edge uses to
    resolve the tenant + load the signing secret.

    R2: installation_id is the Figma-assigned `webhook_id` (from the
    `POST /v2/webhooks` response) — the real Figma V2 delivery body carries no
    team_id, so webhook_id is the only durable install scope and matches
    tenant_resolver._extract_figma. `team_id` is logged for traceability."""
    await upsert_provider_installation_for_tenant(
        pool,
        provider="figma",
        tenant_id=tenant_id,
        installation_id=webhook_id,
        secret_ref=webhook_secret_ref,
    )
    log.info(
        "figma_webhook_installation_registered",
        webhook_id=webhook_id,
        team_id=team_id,
    )


__all__ = ["finalize_install", "register_webhook_installation"]
