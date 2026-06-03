#!/usr/bin/env python3
"""scripts/sandbox_google_calendar_full.py — FULL worker-topology sandbox for
Google Calendar ingestion (IN-15).

Unlike scripts/sandbox_google_calendar.py (which drives ingest() in-process),
this stands up the real M6 worker chain and proves observations land through
ALL of it:

    onboarding_triggers
      -> oauth_poller -> tenant_onboarding -> source_onboarding
      -> shard_fetch       (writes raw blob to S3 + publishes Kafka ingestion.raw)
      -> normalizer        (consumes ingestion.raw, runs handler, publishes ingestion.normalized)
      -> observation_writer (consumes ingestion.normalized, writes observations)
      -> reconciler

All 7 workers run as REAL subprocesses (BackfillHarness) on this branch's code.
The google_calendar source is mocked in-process at the `_open_calendar_client`
seam (no Google creds). Infra is stood up locally and torn down:

  - Kafka      : a throwaway single-node KRaft container on host :29092
  - S3         : moto (in-process), bucket `fyralis-raw`
  - Postgres   : a throwaway DB on SANDBOX_ADMIN_URL (default :5434), dropped on exit

Run:
    python scripts/sandbox_google_calendar_full.py
    python scripts/sandbox_google_calendar_full.py --keep   # keep DB + Kafka

Requires Docker (for Kafka) and a reachable Postgres admin URL. moto is a
Python dep (already in the project's test deps).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import subprocess
import sys
import time
from uuid import uuid4

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Dev/test harness: loads services/ingest/synthetic (refuses to import under prod env).
os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

import asyncpg

_DEFAULT_ADMIN_URL = "postgresql://company_os:company_os@localhost:5434/company_os"
_KAFKA_CONTAINER = "gcal_full_kafka"
_KAFKA_HOST_PORT = 29092
_KAFKA_IMAGE = "apache/kafka:4.0.2"


def _run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


# ---------------------------------------------------------------------
# Kafka (throwaway single-node KRaft container, host-published)
# ---------------------------------------------------------------------
def _start_kafka() -> None:
    _run(["docker", "rm", "-f", _KAFKA_CONTAINER])
    env = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092",
        "KAFKA_ADVERTISED_LISTENERS": (
            f"PLAINTEXT://localhost:9092,EXTERNAL://localhost:{_KAFKA_HOST_PORT}"
        ),
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT"
        ),
        "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
    }
    cmd = ["docker", "run", "-d", "--name", _KAFKA_CONTAINER,
           "-p", f"{_KAFKA_HOST_PORT}:29092"]
    for k, v in env.items():
        cmd += ["-e", f"{k}={v}"]
    cmd.append(_KAFKA_IMAGE)
    r = _run(cmd)
    if r.returncode != 0:
        raise RuntimeError(f"failed to start kafka: {r.stderr}")
    print(f"  started kafka container {_KAFKA_CONTAINER} on host :{_KAFKA_HOST_PORT}")


def _wait_kafka_and_create_topics(timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    last = ""
    while time.monotonic() < deadline:
        r = _run(["docker", "exec", _KAFKA_CONTAINER,
                  "/opt/kafka/bin/kafka-topics.sh",
                  "--bootstrap-server", "localhost:9092", "--list"])
        if r.returncode == 0:
            for topic in ("ingestion.raw", "ingestion.normalized"):
                _run(["docker", "exec", _KAFKA_CONTAINER,
                      "/opt/kafka/bin/kafka-topics.sh",
                      "--bootstrap-server", "localhost:9092",
                      "--create", "--if-not-exists", "--topic", topic,
                      "--partitions", "1", "--replication-factor", "1"])
            # Let the transaction/idempotence coordinator finish loading
            # before the shard_fetch idempotent producer starts (otherwise it
            # hits "Coordinator load in progress" and publishes late).
            time.sleep(10)
            print("  kafka ready; topics ingestion.raw + ingestion.normalized created")
            return
        last = r.stderr
        time.sleep(2)
    raise RuntimeError(f"kafka not ready in {timeout_s}s: {last}")


def _stop_kafka() -> None:
    _run(["docker", "rm", "-f", _KAFKA_CONTAINER])


# ---------------------------------------------------------------------
# Postgres (throwaway DB)
# ---------------------------------------------------------------------
async def _create_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


async def _drop_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname=$1 AND pid<>pg_backend_pid()", name)
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        await admin.close()


async def run(args) -> int:
    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    db_name = f"gcal_full_{uuid4().hex[:8]}"
    moto = None
    ok = False
    try:
        # 1. Kafka.
        print("==== KAFKA ====")
        _start_kafka()
        _wait_kafka_and_create_topics()
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = f"localhost:{_KAFKA_HOST_PORT}"

        # 2. moto S3 (in-process; reachable by the worker subprocesses).
        print("==== S3 (moto) ====")
        import socket
        from moto.server import ThreadedMotoServer
        import boto3
        _s = socket.socket()
        _s.bind(("127.0.0.1", 0))
        s3_port = _s.getsockname()[1]
        _s.close()
        moto = ThreadedMotoServer(port=s3_port)
        moto.start()
        s3_endpoint = f"http://127.0.0.1:{s3_port}"
        os.environ["S3_ENDPOINT_URL"] = s3_endpoint
        os.environ["S3_RAW_BUCKET"] = "fyralis-raw"
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
        os.environ["INGESTION_ENV"] = "dev"
        boto3.client("s3", endpoint_url=s3_endpoint,
                     aws_access_key_id="testing", aws_secret_access_key="testing",
                     region_name="us-east-1").create_bucket(Bucket="fyralis-raw")
        print(f"  moto S3 at {s3_endpoint}, bucket fyralis-raw")

        # 3. Postgres throwaway DB + migrations + partitions.
        print("==== POSTGRES ====")
        await _create_db(admin_url, db_name)
        db_url = admin_url.rsplit("/", 1)[0] + "/" + db_name
        os.environ["DATABASE_URL"] = db_url
        from services.app.gateway.db_bootstrap import _register_codecs
        pool = await asyncpg.create_pool(dsn=db_url, min_size=2, max_size=10,
                                         init=_register_codecs)
        from lib.shared.migrations import apply_migrations_dir
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, _REPO_ROOT / "db" / "migrations")
        from services.domain.observations.partitions import ensure_partitions
        await ensure_partitions(pool, months_ahead=6)
        print(f"  throwaway DB {db_name} migrated, partitions ensured")

        # 4. Run the FULL 7-worker harness for a google_calendar tenant.
        print("==== HARNESS (7 real worker subprocesses) ====")
        from services.ingest.synthetic.backfill_harness import (
            BackfillHarness, BackfillScenario,
            assert_all_complete, assert_completion_emitted_per_tenant,
            assert_no_duplicate_observations,
            assert_observation_count_matches_fixture,
        )
        scenarios = [
            BackfillScenario(
                tenant_slug="full-gcal",
                source="google_calendar",
                fixture_params={
                    "calendars": ["alice@acme.com", "bob@acme.com"],
                    "events_per_calendar": 3,
                },
                expected_observation_count=6,
            ),
        ]
        harness = BackfillHarness(
            pool=pool, scenarios=scenarios,
            kafka_bootstrap_servers=f"localhost:{_KAFKA_HOST_PORT}",
            completion_deadline_s=180.0,
            # Give the normalizer + observation_writer consumer chain ample
            # time to drain ingestion.raw -> ingestion.normalized -> observations
            # after the (fast) control-plane completion.
            drain_timeout_s=120.0,
        )
        result = await harness.run()

        print("\n==== WORKER SUBPROCESS RESULTS ====")
        for name, rc in sorted(result.subprocess_returncodes.items()):
            print(f"  {name:<20} exit={rc}")
        # Surface any worker stderr so a stall is diagnosable.
        for name, tail in sorted(result.subprocess_stderr_tails.items()):
            t = (tail or "").strip()
            if t:
                print(f"\n  --- {name} stderr tail ---")
                for line in t.splitlines()[-12:]:
                    print(f"    {line}")

        print("\n==== ASSERTIONS (best-effort; observations queried regardless) ====")
        for label, fn in (
            ("all tenants completed onboarding", assert_all_complete),
            ("completion signal emitted per tenant", assert_completion_emitted_per_tenant),
            ("no duplicate observations", assert_no_duplicate_observations),
            ("observation count matches fixture (6)", assert_observation_count_matches_fixture),
        ):
            try:
                fn(result)
                print(f"  [PASS] {label}")
            except Exception as exc:  # noqa: BLE001
                print(f"  [FAIL] {label}: {str(exc)[:300]}")

        # 5. Show the observations that the FULL chain materialized.
        print("\n==== OBSERVATIONS (written by observation_writer) ====")
        rows = await pool.fetch(
            "SELECT kind, trust_tier, external_id, content_text "
            "FROM observations WHERE source_channel='google_calendar:event' "
            "ORDER BY external_id")
        for r in rows:
            print(f"  [{r['kind']:<8}] {r['external_id']}")
            print(f"       {r['content_text']}")
        print(f"\n  total google_calendar observations: {len(rows)}")
        ok = len(rows) == 6
        await pool.close()
    finally:
        if moto is not None:
            moto.stop()
        if not args.keep:
            try:
                await _drop_db(admin_url, db_name)
                print(f"\n  dropped throwaway DB {db_name}")
            except Exception as exc:  # noqa: BLE001
                print(f"  (could not drop DB {db_name}: {exc})")
            _stop_kafka()
            print(f"  stopped kafka container {_KAFKA_CONTAINER}")
        else:
            print(f"\n  kept DB {db_name} + kafka {_KAFKA_CONTAINER}")

    print("\n==== RESULT ====")
    print("  FULL WORKER TOPOLOGY: PASS" if ok else "  FULL WORKER TOPOLOGY: FAIL")
    return 0 if ok else 1


def main() -> int:
    p = argparse.ArgumentParser(description="Full-topology Google Calendar sandbox")
    p.add_argument("--keep", action="store_true", help="keep the DB + kafka container")
    return asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
