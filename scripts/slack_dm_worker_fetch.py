"""scripts/slack_dm_worker_fetch.py — production worker-fetch DM backfill driver.

Runs the REAL per-user DM backfill through the genuine planner → fetcher →
raw-tier(S3) → Kafka producer path against Provider Lab, then lets the running
Kafka consumer workers (normalizer → observation_writer) land the observations.
This is the worker-chain counterpart of the inline gateway console
(slack_router.py): it produces IDENTICAL observations (same `slack:message`
channel, same `external_id="{channel}:{ts}"`, same content.channel_type) but
exercises the planner/fetcher worker code instead of inline `ingest()`.

Run INSIDE a worker container (has DATABASE_URL + KAFKA_BOOTSTRAP_SERVERS +
S3_* on the compose network), driven by scripts/slack_dm_worker_demo.sh:

    docker compose ... run --rm --no-deps -e COMPANY_OS_ENV=dev \
        shard_fetch python scripts/slack_dm_worker_fetch.py

What it does (idempotent):
  1. Seed: tenant, kafka_path_enabled flag, slack BOT provider_installation,
     slack_dm_installations (one consenting user), observation partitions.
  2. Start an in-container provider simulator seeded with a
     `make_slack_dm_workspace` fixture (now-anchored ts). Point the real Slack
     clients at its explicit Slack endpoint and enable Provider Lab credentials.
  3. Run the REAL `plan_shards_slack` → channel shards (bot) + slack_dm_window
     shards (per consenting user, via conversations.list(types=im,mpim)).
  4. For each shard, run the REAL `fetch_page_slack` loop → records, write each
     to the raw tier (S3) + publish the RawEnvelope pointer to ingestion.raw.slack
     via the REAL shard_fetch producer functions.
  5. The running normalizer + observation_writer consume the lane → observations.

Env knobs: SLACK_DM_DEMO_TENANT (uuid), SLACK_DM_DEMO_USER (default U_ALICE),
SLACK_DM_DEMO_PER (messages per 1:1 DM, default 6).
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from uuid import UUID, uuid4


# Dedicated demo tenant (distinct from the inline console's tenant) so the
# worker-fetch run is isolated + repeatable.
DEFAULT_TENANT = "00000000-0000-0000-0000-0000000000d3"


def _team_for(tenant_id: UUID) -> str:
    return "T0DMW" + tenant_id.hex[:9].upper()


async def main() -> int:
    os.environ.setdefault("COMPANY_OS_ENV", "dev")  # synthetic import guard

    tenant_id = UUID(os.environ.get("SLACK_DM_DEMO_TENANT", DEFAULT_TENANT))
    user_id = os.environ.get("SLACK_DM_DEMO_USER", "U_ALICE")
    per = int(os.environ.get("SLACK_DM_DEMO_PER", "6"))
    team_id = _team_for(tenant_id)

    from services.ingest.ingestion.workflows.runtime import make_workflow_pool
    pool = await make_workflow_pool(os.environ["DATABASE_URL"])

    # ---- 1. Seed identity + flag + partitions -----------------------
    from services.ingest.ingestion.feature_flags.client import KAFKA_PATH_ENABLED
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) "
        "ON CONFLICT (id) DO NOTHING",
        tenant_id, f"slack-dm-worker-{tenant_id.hex[:8]}",
    )
    await pool.execute(
        "INSERT INTO tenant_flags (tenant_id, flag_name, flag_value, set_by) "
        "VALUES ($1, $2, TRUE, 'slack-dm-worker-demo') "
        "ON CONFLICT (tenant_id, flag_name) DO UPDATE SET flag_value = TRUE",
        tenant_id, KAFKA_PATH_ENABLED,
    )
    bot_id = await pool.fetchval(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'slack', $3, NULL, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET enabled = TRUE, tenant_id = EXCLUDED.tenant_id
        RETURNING id
        """,
        uuid4(), tenant_id, team_id,
    )
    await pool.execute(
        """
        INSERT INTO slack_dm_installations
            (id, tenant_id, team_id, user_id, base_url,
             user_token_secret_ref, granted_user_scopes)
        VALUES ($1, $2, $3, $4, NULL, NULL, $5)
        ON CONFLICT (tenant_id, team_id, user_id) DO UPDATE
            SET disabled_at = NULL
        """,
        uuid4(), tenant_id, team_id, user_id,
        "im:history,mpim:history,im:read,mpim:read,users:read",
    )
    from services.domain.observations.partitions import ensure_partitions
    await ensure_partitions(pool, months_ahead=2)

    # ---- 2. Fixture + loopback Provider Lab -------------------------
    from services.ingest.synthetic.fixtures import make_slack_dm_workspace
    from services.ingest.synthetic.provider_lab import start_provider_lab

    base_ts = time.time() - 120.0  # now-anchored → inside the partition window
    fixture = make_slack_dm_workspace(
        team_id=team_id, user_id=user_id, messages_per_dm=per, base_ts=base_ts,
    )
    provider_lab = start_provider_lab({"slack": [fixture]})
    os.environ["PROVIDER_LAB_URL"] = provider_lab.base_url
    os.environ["SLACK_API_BASE_URL"] = provider_lab.url("slack", "/api")

    summary: dict = {
        "tenant_id": str(tenant_id), "team_id": team_id, "user_id": user_id,
        "shards": 0, "records_published": 0, "by_channel_type": {},
        "shard_kinds": {},
    }
    try:
        # ---- 3. REAL planner: channel + DM shards -------------------
        install = await pool.fetchrow(
            "SELECT id, tenant_id, provider, installation_id, secret_ref, "
            "enabled FROM provider_installations WHERE id = $1",
            bot_id,
        )
        from services.ingest.ingestion.fetchers._clients import build_slack_client
        bot_client = await build_slack_client(install, pool=pool)
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.slack import plan_shards_slack
        async with pool.acquire() as conn:
            ctx = PlannerContext(
                tenant_id=tenant_id, install=install, conn=conn,
                source_client=bot_client,
            )
            shards = await plan_shards_slack(ctx)
        summary["shards"] = len(shards)
        for s in shards:
            summary["shard_kinds"][s.shard_kind] = (
                summary["shard_kinds"].get(s.shard_kind, 0) + 1
            )

        # ---- 4. REAL fetcher loop → raw-tier(S3) → Kafka producer ---
        from services.ingest.ingestion.fetchers.slack import fetch_page_slack
        from services.ingest.ingestion.kafka.producer import (
            IdempotentProducer,
            ProducerConfig,
        )
        from services.ingest.ingestion.raw_tier.s3 import S3Client
        from services.ingest.ingestion.workflows.shard_fetch import (
            DEFAULT_S3_BUCKET,
            _write_record_and_build_message,
        )

        s3 = S3Client(
            os.environ.get("S3_RAW_BUCKET", DEFAULT_S3_BUCKET),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            region_name=os.environ.get("S3_REGION_NAME", "auto"),
        )
        await s3.connect()
        producer = IdempotentProducer(ProducerConfig(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
            ),
            client_id="slack-dm-worker-demo",
        ))
        await producer.start()
        env_prefix = os.environ.get("INGESTION_ENV", "dev")
        try:
            for shard in shards:
                shard_uuid = uuid4()
                cursor: dict | None = None
                while True:
                    result = await fetch_page_slack(
                        install, shard.shard_identifier, cursor,
                    )
                    for rec in result.records:
                        msg = await _write_record_and_build_message(
                            s3,
                            tenant_id=tenant_id, source="slack",
                            shard_id=shard_uuid, cursor=cursor, record=rec,
                            env=env_prefix,
                        )
                        await producer.produce(
                            msg.topic, msg.value, key=msg.key,
                        )
                        summary["records_published"] += 1
                        ct = (rec.get("event") or {}).get(
                            "channel_type",
                        ) or "channel"
                        summary["by_channel_type"][ct] = (
                            summary["by_channel_type"].get(ct, 0) + 1
                        )
                    cursor = result.next_cursor
                    if result.end_of_data:
                        break
            remaining = await producer.flush(timeout_seconds=15.0)
            summary["flush_unacked"] = remaining
        finally:
            await producer.stop()
            await s3.close()
    finally:
        provider_lab.shutdown()
        await pool.close()

    print("SLACK_DM_WORKER_FETCH_RESULT " + json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
