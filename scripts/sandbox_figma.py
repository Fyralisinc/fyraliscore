#!/usr/bin/env python3
"""scripts/sandbox_figma.py — local end-to-end sandbox for Figma ingestion
(design), with NO real Figma credentials.

Figma is a design REST API (Bearer access token) with BOTH a historical query
surface (GET /v1/files, /v1/files/{key}/events) and a live push surface (Webhooks
V2 — passcode-in-body in reality; HMAC-signed for the synthetic gate). This
sandbox stands up a REAL local mock of the Figma v1 endpoints and drives the REAL
pipeline against it:

    FigmaClient (real httpx, spammer auth) -> fetch_page_figma (real cursor +
    fan-out) -> handle_figma_event (real ObservationDraft) -> ingest()
    (real observation insert + dedup)

It exercises: file enumeration, per-file backfill event fan-out, the incremental
delta (a re-published event -> a NEW observation), the live-webhook path through
the SAME handler (asserting external_id parity / dedup with backfill), cross-path
dedup, and the reconciler gap probe — then prints the observations that landed.

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_figma.py
    python scripts/sandbox_figma.py --keep
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
_BASE_URL = "https://api.figma.com"
_TEAM_ID = "team-sandbox"
_WEBHOOK_ID = "webhook-sandbox"
_FILE = "file-design-system"


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

    def ev(eid, etype, label, created, version=None, status=None):
        event = {
            "id": eid,
            "event_id": eid,
            "event_type": etype,
            "type": etype,
            "team_id": _TEAM_ID,
            "file_key": _FILE,
            "label": label,
            "status": status,
            "user": "ada",
            "createdAt": created,
            "created_at": created,
        }
        if version is not None:
            event["version"] = version
        return event

    return {
        _FILE: {
            "file": {
                "key": _FILE,
                "name": "Design System",
                "editorType": "figma",
                "lastModified": _iso(now - timedelta(days=1)),
            },
            "events": [
                ev("e-1001", "FILE_VERSION_UPDATE", "v1.0 checkpoint",
                   _iso(now - timedelta(days=3)), "v-1"),
                ev("e-1002", "FILE_COMMENT", "needs review",
                   _iso(now - timedelta(days=2))),
            ],
            # Incremental delta: e-1001 is re-published at a NEW version — a
            # fresh observation the poll/reconcile surfaces.
            "delta": [
                ev("e-1001", "FILE_VERSION_UPDATE", "v1.1 checkpoint",
                   _iso(now - timedelta(hours=1)), "v-2"),
            ],
        },
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
    from services.ingest.ingestion.fetchers.figma import fetch_page_figma

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_figma(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("figma:event", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.figma import start_mock_figma

    fixtures = _build_fixtures()
    server, base_url = start_mock_figma(fixtures)
    os.environ["SYNTHETIC_SOURCE_API_BASE"] = base_url
    _hr("MOCK SERVER")
    print(f"  Figma API base : {base_url} (served under /figma via spammer routing)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"figma_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'figma-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Enumerate files via the REAL client.
        _hr("ENUMERATE FILES (FigmaClient.list_files)")
        from services.ingest.ingestion.fetchers._clients import build_figma_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID,
                  "base_url": _BASE_URL, "secret_ref": None, "team_id": _TEAM_ID}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_figma_client(_Inst())
        files = await client.list_files()
        file_keys = [f.get("key") or f.get("file_key") for f in files]
        print(f"  files discovered: {file_keys}")
        _check("file enumeration returned the design file", _FILE in file_keys)

        # 3. Provision the install + webhook row.
        _hr("PROVISION (figma.onboarding.finalize_install)")
        from services.ingest.integrations.figma.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL,
            files=[{"file_key": f.get("key") or f.get("file_key"),
                    "file_name": f.get("name")} for f in files],
            team_id=_TEAM_ID,
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, webhook_id=_WEBHOOK_ID,
            team_id=_TEAM_ID, webhook_secret_ref=None,
        )
        file_count = await pool.fetchval(
            "SELECT count(*) FROM figma_files WHERE figma_installation_id=$1", install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install + file rows provisioned", file_count == len(file_keys))
        _check("onboarding trigger emitted (source=figma)",
               trig is not None and trig["source"] == "figma")

        # 4. Plan shards.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.figma import plan_shards_figma
        from services.ingest.ingestion.workflows.source_onboarding import _LOAD_FIGMA_INSTALL_SQL
        install_row = await pool.fetchrow(_LOAD_FIGMA_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_figma(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["file_key"] for s in shards))
        _check("one shard per file", len(shards) == len(file_keys))

        # 5. Backfill: real fetcher -> real ingest.
        _hr("BACKFILL (event fan-out -> ingest)")
        for shard in shards:
            ext = await _drain_shard(pool, install_row, shard.shard_identifier)
            print(f"  {shard.shard_identifier['file_key']}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) AS tot FROM observations "
            "WHERE tenant_id=$1 AND source_channel='figma:event'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']}")
        # 2 events -> 2 observations (no snapshot record).
        _check("backfill produced 2 observations (event fan-out, no snapshot)",
               counts["tot"] == 2)

        # 6. Incremental: warm-start from the high-water -> delta (e-1001 re-published).
        _hr("INCREMENTAL (re-publish: new version)")
        hw = await pool.fetchval(
            "SELECT max(content->>'created_at') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='event'", _TENANT_ID,
        )
        incr_shard = {"shard_kind": "figma_file_events", "file_key": _FILE,
                      "team_id": _TEAM_ID, "event_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        _check("incremental delta surfaced the re-published event", len(incr) == 1)

        # 7. Dedup: re-ingest a backfilled event twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "event", "_fyralis_team_id": _TEAM_ID,
                "_fyralis_file_key": _FILE,
                "event": fixtures[_FILE]["events"][1]}
        res = await ingest("figma:event", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing event dedups (versioned external_id parity)",
               res.deduped is True)

        # 8. LIVE WEBHOOK path: a FILE_COMMENT with the SAME event flows through
        #    the SAME handler; its external_id matches the backfilled event, so
        #    it dedups (proves backfill+live parity).
        _hr("LIVE WEBHOOK (handler parity with backfill)")
        webhook_payload = {
            "event_type": "FILE_COMMENT",
            "team_id": _TEAM_ID,
            **fixtures[_FILE]["events"][1],
        }
        res = await ingest("figma:event", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook event dedups against backfilled twin (external_id parity)",
               res.deduped is True)

        # A brand-new live event lands as a fresh observation.
        fresh = {
            "event_type": "FILE_VERSION_UPDATE", "team_id": _TEAM_ID,
            "id": "e-live-1", "file_key": _FILE, "version": "v-live",
            "label": "live checkpoint", "user": "grace",
            "createdAt": _iso(datetime.now(timezone.utc)),
        }
        res = await ingest("figma:event", fresh, pool=pool, tenant_id=_TENANT_ID)
        _check("new live event lands as a fresh observation", res.deduped is False)

        # 9. Reconciler gap probe against the live (mock) file.
        _hr("RECONCILER GAP PROBE (list_events since high-water)")
        events, _, _ = await client.list_events(_FILE, limit=1, offset=0,
                                                 start=(hw or "2020-01-01")[:10])
        _check("reconciler probe detects an event since the high-water", len(events) >= 1)

        # 10. Inspect.
        _hr("OBSERVATIONS")
        rows = await pool.fetch(
            "SELECT kind, trust_tier, external_id, content_text FROM observations "
            "WHERE tenant_id=$1 ORDER BY occurred_at", _TENANT_ID,
        )
        for r in rows:
            print(f"  [{r['kind']:<12} {r['trust_tier']:<13}] {r['external_id']}")
            print(f"       {r['content_text']}")
        print(f"\n  total observations: {len(rows)}")
        _check("all observations are authoritative figma:event",
               all(r["trust_tier"] == "authoritative" for r in rows))

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
    parser = argparse.ArgumentParser(description="Figma ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
