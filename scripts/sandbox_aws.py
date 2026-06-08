#!/usr/bin/env python3
"""scripts/sandbox_aws.py — local end-to-end sandbox for AWS ingestion (IN-AWS),
with NO real AWS credentials.

AWS is an IAM/SigV4 API (CloudTrail LookupEvents) with a historical pull surface
(time-window backfill) and a live POLL push surface (SQS / EventBridge). Real
SigV4 signing is a TODO seam, so this sandbox drives the REAL pipeline against an
in-process `MockAwsClient` (bound through the fetcher's `_open_aws_client` test
seam — the same seam the synthetic gate uses):

    MockAwsClient -> fetch_page_aws (real time-window walk + NextToken cursor +
    high-water) -> handle_aws_event (real ObservationDraft) -> ingest() (real
    observation insert + dedup)

It exercises: install provisioning + onboarding trigger, the account-events
shard, backfill (management event -> signal; alarm-state-change -> state_change),
cross-fetch dedup, the incremental high-water delta, the live POLL path
(handle_polled_event -> the SAME aws:event handler -> a fresh observation), and
the reconciler gap probe — then prints the observations.

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_aws.py
    python scripts/sandbox_aws.py --keep
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

import asyncpg


_DEFAULT_ADMIN_URL = "postgresql://company_os:company_os@localhost:5434/company_os"
_TENANT_ID = UUID("00000000-0000-0000-0000-0000000000a5")  # 'aws'-ish marker
_ACCOUNT_ID = "123456789012"
_REGION = "us-east-1"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _build_fixture() -> dict:
    """A make_aws fixture whose 3 events land relative to NOW (so the all-time
    floor always covers them): 1 management event (signal) + 2 alarm-state
    changes (state_change)."""
    now = datetime.now(timezone.utc)
    return {
        "account_id": _ACCOUNT_ID,
        "region": _REGION,
        "per_page": 50,
        "events": [
            # Management event (IAM principal) -> signal.
            {
                "eventId": "11111111-1111-1111-1111-111111111111",
                "eventName": "RunInstances",
                "eventSource": "ec2.amazonaws.com",
                "eventTime": _ms(now - timedelta(days=3)),
                "awsRegion": _REGION,
                "recipientAccountId": _ACCOUNT_ID,
                "userIdentity": {
                    "type": "AssumedRole",
                    "arn": f"arn:aws:iam::{_ACCOUNT_ID}:role/deploy",
                    "userName": "deploy-bot",
                },
            },
            # Alarm fired -> state_change (actorless).
            {
                "eventId": "22222222-2222-2222-2222-222222222222",
                "eventName": "DescribeAlarms",
                "eventSource": "monitoring.amazonaws.com",
                "eventTime": _ms(now - timedelta(days=2)),
                "awsRegion": _REGION,
                "recipientAccountId": _ACCOUNT_ID,
                "alarmName": "high-cpu-prod",
                "prevState": "OK", "newState": "ALARM",
                "userIdentity": {"type": "AWSService"},
            },
            # Alarm resolved -> state_change.
            {
                "eventId": "33333333-3333-3333-3333-333333333333",
                "eventName": "DescribeAlarms",
                "eventSource": "monitoring.amazonaws.com",
                "eventTime": _ms(now - timedelta(days=1)),
                "awsRegion": _REGION,
                "recipientAccountId": _ACCOUNT_ID,
                "alarmName": "high-cpu-prod",
                "prevState": "ALARM", "newState": "OK",
                "userIdentity": {"type": "AWSService"},
            },
        ],
    }


async def _create_throwaway_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


async def _drop_throwaway_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()", name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        await admin.close()


async def _drain_shard(pool, install_row, shard_identifier) -> list[str]:
    """Run the REAL fetcher loop for the account-events shard, ingesting each
    record. Returns the external_ids of NON-deduped observations."""
    from services.ingest.ingestion.core import ingest
    from services.ingest.ingestion.fetchers.aws import fetch_page_aws

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_aws(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("aws:event", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


def _bind_mock_client(fixture: dict) -> None:
    """Rebind the fetcher + reconciler `_open_aws_client` seam to a MockAwsClient
    over the fixture (the synthetic-gate seam; avoids real SigV4 + _clients.py)."""
    from services.ingest.ingestion.fetchers import aws as aws_fetcher
    from services.ingest.synthetic.mock_clients.aws import MockAwsClient

    async def _open(install):  # noqa: ANN001, ANN202
        client = MockAwsClient(fixture=fixture)

        async def _close() -> None:
            await client.aclose()

        return client, _close

    aws_fetcher._open_aws_client = _open  # type: ignore[attr-defined]


async def run(args) -> int:
    fixture = _build_fixture()
    _bind_mock_client(fixture)
    # Force all-time backfill so the fixtures (relative to now) always land.
    os.environ["AWS_BACKFILL_WINDOW_DAYS"] = "0"

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"aws_sandbox_{uuid4().hex[:8]}"
        await _create_throwaway_db(admin_url, created_db)
        db_url = admin_url.rsplit("/", 1)[0] + "/" + created_db
        _hr("DATABASE"); print(f"  Created throwaway DB: {created_db}")

    from services.app.gateway.db_bootstrap import _register_codecs
    pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5, init=_register_codecs)
    try:
        from lib.shared.migrations import apply_migrations_dir
        from services.domain.observations.partitions import ensure_partitions
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, _REPO_ROOT / "db" / "migrations")
        await ensure_partitions(pool, months_ahead=3)
        await pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'aws-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 1. Provision: aws_installations + onboarding trigger.
        _hr("PROVISION (aws.onboarding.finalize_install)")
        from services.ingest.integrations.aws.onboarding import finalize_install
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, account_id=_ACCOUNT_ID, region=_REGION,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("aws_installations row + onboarding trigger (source=aws)",
               trig is not None and trig["source"] == "aws")

        install_row = await pool.fetchrow(
            "SELECT id, tenant_id, account_id, region, credential_kind, secret_ref, "
            "events_cursor_ms FROM aws_installations WHERE tenant_id=$1", _TENANT_ID,
        )

        # 2. Plan the shard exactly as the planner does.
        _hr("PLAN (planner over the install row)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.aws import plan_shards_aws
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_aws(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_kind for s in shards))
        _check("one account-events shard", len(shards) == 1)

        # 3. Backfill: real fetcher -> real ingest.
        _hr("BACKFILL (events walk -> ingest)")
        ext = await _drain_shard(pool, install_row, shards[0].shard_identifier)
        print(f"  ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='aws:event'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        # 1 management event (signal) + 2 alarm-state changes (state_change) = 3.
        _check("backfill produced 3 observations (1 signal + 2 state_change)",
               counts["tot"] == 3)
        _check("alarm-state-change events landed as state_change", counts["sc"] == 2)

        # 4. Dedup: re-ingest a backfilled event twin -> deduped (immutable id).
        _hr("DEDUP (re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = dict(fixture["events"][0])
        twin["_fyralis_record_type"] = "event"
        twin["_fyralis_account_id"] = _ACCOUNT_ID
        twin["_fyralis_region"] = _REGION
        res = await ingest("aws:event", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing event dedups (immutable external_id)",
               res.deduped is True)

        # 5. Incremental: append a NEW event (newer time) and warm-start the shard
        #    from the high-water -> only the new one lands.
        _hr("INCREMENTAL (high-water delta)")
        hw = await pool.fetchval(
            "SELECT max((content->>'event_time')::bigint) FROM observations "
            "WHERE tenant_id=$1 AND source_channel='aws:event'", _TENANT_ID,
        )
        fixture["events"].insert(0, {
            "eventId": "44444444-4444-4444-4444-444444444444",
            "eventName": "DescribeAlarms",
            "eventSource": "monitoring.amazonaws.com",
            "eventTime": _ms(datetime.now(timezone.utc) - timedelta(minutes=5)),
            "awsRegion": _REGION,
            "recipientAccountId": _ACCOUNT_ID,
            "alarmName": "latency-high-api",
            "prevState": "OK", "newState": "ALARM",
            "userIdentity": {"type": "AWSService"},
        })
        incr_shard = {"shard_kind": "aws_account_events",
                      "installation_id": str(install_id), "account_id": _ACCOUNT_ID,
                      "region": _REGION, "updated_cursor": int(hw)}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observation(s): {incr}")
        _check("incremental delta surfaced exactly the new event", len(incr) == 1)

        # 6. LIVE POLL: dispatch a fresh CloudTrail-shaped event through the
        #    production poll edge -> the SAME aws:event handler -> a new observation.
        _hr("LIVE POLL (handle_polled_event -> aws:event handler)")
        from services.ingest.integrations.aws.live_poll import (
            PollDeps, handle_polled_event,
        )
        poll_event = {
            "eventId": "99999999-9999-9999-9999-999999999999",
            "eventName": "StartInstances",
            "eventSource": "ec2.amazonaws.com",
            "eventTime": _ms(datetime.now(timezone.utc)),
            "awsRegion": _REGION,
            "recipientAccountId": _ACCOUNT_ID,
            "userIdentity": {
                "type": "AssumedRole",
                "arn": f"arn:aws:iam::{_ACCOUNT_ID}:role/ops", "userName": "ops",
            },
        }
        before = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='aws:event'", _TENANT_ID,
        )
        await handle_polled_event(poll_event, PollDeps(pool=pool))
        after = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='aws:event'", _TENANT_ID,
        )
        _check("live poll event landed as a new aws:event observation", after == before + 1)

        # 7. Reconciler gap probe against the (mock) account.
        _hr("RECONCILER GAP PROBE (has_events_since)")
        from services.ingest.synthetic.mock_clients.aws import MockAwsClient
        probe = MockAwsClient(fixture=fixture)
        has_updates = await probe.has_events_since(
            account_id=_ACCOUNT_ID, region=_REGION, from_ms=1,
        )
        _check("reconciler probe detects events since an old high-water", has_updates is True)

        # 8. Inspect.
        _hr("OBSERVATIONS")
        rows = await pool.fetch(
            "SELECT kind, trust_tier, source_channel, external_id, content_text "
            "FROM observations WHERE tenant_id=$1 ORDER BY occurred_at", _TENANT_ID,
        )
        for r in rows:
            print(f"  [{r['kind']:<12} {r['source_channel']:<11}] {r['external_id']}")
            print(f"       {r['content_text']}")
        print(f"\n  total observations: {len(rows)}")
        _check("all observations are authoritative aws:*",
               all(r["trust_tier"] == "authoritative"
                   and r["source_channel"].startswith("aws:") for r in rows))

    finally:
        await pool.close()
        if created_db and not args.keep:
            await _drop_throwaway_db(admin_url, created_db)
            print(f"\n  Dropped throwaway DB {created_db}.")
        elif created_db:
            print(f"\n  Kept throwaway DB {created_db}.")

    _hr("SUMMARY")
    passed = sum(1 for _, ok in _checks if ok)
    for label, ok in _checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    print(f"\n  {passed}/{len(_checks)} checks passed.")
    return 0 if passed == len(_checks) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="AWS ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
