#!/usr/bin/env python3
"""scripts/sandbox_fireflies.py — local end-to-end sandbox for Fireflies
ingestion (meeting transcripts), with NO real Fireflies credentials.

Fireflies is an AI meeting-notetaker API (Bearer API token) with BOTH a
historical query surface (GET /workspace, /transcripts, /transcript/{id}) and a
live push surface (HMAC-signed webhooks). This sandbox stands up a REAL local
mock of the Fireflies endpoints and drives the REAL pipeline against it:

    FirefliesClient (real httpx, spammer auth) -> fetch_page_fireflies (real
    cursor + fan-out) -> handle_fireflies_transcript (real ObservationDraft) ->
    ingest() (real observation insert + dedup)

It exercises: workspace resolution, per-workspace backfill (one observation per
transcript, NO snapshot record), the incremental delta (a fresh transcript ->
new observation), the live-webhook path through the SAME handler (asserting
external_id parity / dedup with backfill), cross-path dedup, and the reconciler
gap probe — then prints the observations that landed.

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_fireflies.py
    python scripts/sandbox_fireflies.py --keep
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
_TENANT_ID = UUID("00000000-0000-0000-0000-000000006901")
_BASE_URL = "https://api.fireflies.ai"
_WORKSPACE = "ws-sandbox"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _build_fixtures() -> dict:
    now = datetime.now(timezone.utc)

    def transcript(tid, title, created):
        return {
            "id": tid,
            "transcriptId": tid,
            "workspaceId": _WORKSPACE,
            "title": title,
            "dateTime": created,
            "date": created,
            "version": created,
            "duration": 45,
            "participants": [
                {"name": "Alice", "email": "alice@acme.example"},
                {"name": "Bob", "email": "bob@acme.example"},
            ],
            "summary": {"overview": f"Notes from {title}.",
                        "action_items": [f"Follow up on {title}"]},
            "meetingLink": f"https://app.fireflies.ai/view/{tid}",
        }

    return {
        "workspace": {"id": _WORKSPACE, "name": "Sandbox Workspace"},
        "transcripts": [
            transcript("ts-1001", "Weekly Engineering Sync",
                       _iso(now - timedelta(days=3))),
            transcript("ts-1002", "Customer Discovery Call",
                       _iso(now - timedelta(days=2))),
        ],
        # Incremental delta: a fresh transcript lands after the high-water.
        "delta": [
            transcript("ts-1003", "Product Roadmap Review",
                       _iso(now - timedelta(hours=1))),
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
    from services.ingest.ingestion.core import ingest
    from services.ingest.ingestion.fetchers.fireflies import fetch_page_fireflies

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_fireflies(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("fireflies:transcript", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.fireflies import start_mock_fireflies

    fixtures = _build_fixtures()
    server, base_url = start_mock_fireflies(fixtures)
    os.environ["SYNTHETIC_SOURCE_API_BASE"] = base_url
    _hr("MOCK SERVER")
    print(f"  Fireflies API base : {base_url} (served under /fireflies via spammer routing)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"fireflies_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'fireflies-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Resolve the workspace via the REAL client.
        _hr("RESOLVE WORKSPACE (FirefliesClient.get_workspace)")
        from services.ingest.ingestion.fetchers._clients import build_fireflies_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID,
                  "base_url": _BASE_URL, "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_fireflies_client(_Inst())
        ws = await client.get_workspace()
        workspace_id = ws.get("workspace_id") or ws.get("id")
        print(f"  workspace resolved: {workspace_id}")
        _check("workspace resolution returned the sandbox workspace",
               workspace_id == _WORKSPACE)

        # 3. Provision the install + webhook row.
        _hr("PROVISION (fireflies.onboarding.finalize_install)")
        from services.ingest.integrations.fireflies.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL,
            workspace_id=_WORKSPACE, workspace_name="Sandbox Workspace",
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, workspace_id=_WORKSPACE, webhook_secret_ref=None,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install row provisioned", install_id is not None)
        _check("onboarding trigger emitted (source=fireflies)",
               trig is not None and trig["source"] == "fireflies")

        # 4. Plan shards.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.fireflies import plan_shards_fireflies
        from services.ingest.ingestion.workflows.source_onboarding import _LOAD_FIREFLIES_INSTALL_SQL
        install_row = await pool.fetchrow(_LOAD_FIREFLIES_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_fireflies(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["workspace_id"] for s in shards))
        _check("one shard per workspace install", len(shards) == 1)

        # 5. Backfill: real fetcher -> real ingest.
        _hr("BACKFILL (transcript fan-out -> ingest)")
        for shard in shards:
            ext = await _drain_shard(pool, install_row, shard.shard_identifier)
            print(f"  {shard.shard_identifier['workspace_id']}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='fireflies:transcript'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']}")
        # 2 transcripts -> 2 observations (NO snapshot record).
        _check("backfill produced 2 observations (one per transcript)",
               counts["tot"] == 2)

        # 6. Incremental: warm-start from the high-water -> delta (a fresh meeting).
        _hr("INCREMENTAL (a new transcript lands)")
        hw = await pool.fetchval(
            "SELECT max(content->>'date') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='transcript'", _TENANT_ID,
        )
        incr_shard = {"shard_kind": "fireflies_transcripts", "workspace_id": _WORKSPACE,
                      "transcript_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        _check("incremental delta surfaced a fresh transcript", len(incr) == 1)

        # 7. Dedup: re-ingest a backfilled transcript twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "transcript", "_fyralis_workspace_id": _WORKSPACE,
                "transcript": fixtures["transcripts"][1]}
        res = await ingest("fireflies:transcript", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing transcript dedups (versioned external_id parity)",
               res.deduped is True)

        # 8. LIVE WEBHOOK path: a transcript.completed with the SAME transcript
        #    flows through the SAME handler; its external_id matches the
        #    backfilled transcript, so it dedups (proves backfill+live parity).
        _hr("LIVE WEBHOOK (handler parity with backfill)")
        webhook_payload = {
            "type": "transcript.completed",
            "workspaceId": _WORKSPACE,
            "transcript": fixtures["transcripts"][1],
        }
        res = await ingest("fireflies:transcript", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook transcript dedups against backfilled twin (external_id parity)",
               res.deduped is True)

        # A brand-new live transcript lands as a fresh observation.
        fresh = {
            "type": "transcript.completed", "workspaceId": _WORKSPACE,
            "transcript": {"id": "ts-live-1", "title": "Ad-hoc Sync",
                           "dateTime": _iso(datetime.now(timezone.utc)),
                           "version": _iso(datetime.now(timezone.utc))},
        }
        res = await ingest("fireflies:transcript", fresh, pool=pool, tenant_id=_TENANT_ID)
        _check("new live transcript lands as a fresh observation", res.deduped is False)

        # 9. Reconciler gap probe against the live (mock) workspace.
        _hr("RECONCILER GAP PROBE (list_transcripts since high-water)")
        txns, _, _ = await client.list_transcripts(
            limit=1, offset=0, start=(hw or "2020-01-01")[:10],
        )
        _check("reconciler probe detects a transcript since the high-water", len(txns) >= 1)

        # 10. Inspect.
        _hr("OBSERVATIONS")
        rows = await pool.fetch(
            "SELECT kind, trust_tier, external_id, content_text FROM observations "
            "WHERE tenant_id=$1 ORDER BY occurred_at", _TENANT_ID,
        )
        for r in rows:
            print(f"  [{r['kind']:<12} {r['trust_tier']:<14}] {r['external_id']}")
            print(f"       {r['content_text']}")
        print(f"\n  total observations: {len(rows)}")
        _check("all observations are attested_agent fireflies:transcript",
               all(r["trust_tier"] == "attested_agent" for r in rows))

    finally:
        await pool.close()
        server.shutdown()
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
    parser = argparse.ArgumentParser(description="Fireflies ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
