"""services/ingest/integrations/aws/onboarding.py — install + provision (IN-AWS).

AWS authenticates with IAM credentials (SigV4) against a per-account/region
CloudTrail endpoint. Onboarding mirrors the Grafana dedicated-table shape (the
backfill streams a TIME WINDOW of management events), but AWS has NO per-resource
child table (CloudTrail events are account/region-wide, so one shard per install)
AND NO webhook registration — the live edge is a POLL (SQS / EventBridge), not an
HTTP webhook, so there is no provider_installations row to seed:

  finalize_install() — UPSERT an aws_installations row and emit an
  onboarding_triggers row (source='aws') so the existing M6 backfill chain
  (oauth_poller -> tenant_onboarding -> source_onboarding -> shard_fetch ->
  reconciler) fires. All in one tenant-scoped transaction.

The poll live edge resolves the tenant/install directly from aws_installations
(see live_poll.py), so — unlike Grafana's webhook — there is no separate live
registration step here.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction


log = structlog.get_logger("integrations.aws.onboarding")


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    account_id: str,
    region: str = "us-east-1",
    credential_kind: str = "assume_role",
    secret_ref: str | None = None,
    backfill_window_days: int = 90,
) -> UUID:
    """UPSERT the install + an onboarding trigger atomically.

    Returns the aws_installations id. Idempotent on
    (tenant_id, account_id, region).
    """
    account_id = str(account_id)
    region = str(region)

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO aws_installations (
                id, tenant_id, account_id, region,
                credential_kind, secret_ref, backfill_window_days
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, account_id, region) DO UPDATE
                SET credential_kind = EXCLUDED.credential_kind,
                    secret_ref = COALESCE(EXCLUDED.secret_ref, aws_installations.secret_ref),
                    backfill_window_days = EXCLUDED.backfill_window_days,
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, account_id, region,
            credential_kind, secret_ref, int(backfill_window_days),
        )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Grafana this is NOT a provider_installations source; the install id
        # rides in installation_row_id purely for the idempotency dedup index.
        # source='aws' is admitted by migration 0101.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'aws', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"account_id": account_id, "region": region}),
        )

    log.info("aws_install_finalized", account_id=account_id, region=region)
    return install_id


__all__ = ["finalize_install"]
