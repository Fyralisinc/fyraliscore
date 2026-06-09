#!/usr/bin/env python3
"""scripts/sandbox_gusto.py — local end-to-end sandbox for Gusto
ingestion (finance), with NO real Intuit credentials.

Gusto is an accounting REST API (OAuth 2.0, company-scoped) with a
SQL-like query surface and HMAC-SHA256-signed webhooks. This sandbox stands up a
REAL local mock of the GUSTO v3 query endpoint and drives the REAL pipeline:

    GustoClient (real httpx, spammer auth) -> fetch_page_gusto (real
    cursor + query) -> handle_gusto_object (real ObservationDraft) ->
    ingest() (real observation insert + dedup)

It exercises: entity enumeration (install), per-entity backfill, the incremental
LastUpdatedTime delta (an invoice paid -> state_change), the live-webhook path
(thin change notification), cross-path behavior, and the reconciler gap probe.

Database: DATABASE_URL if set, else a throwaway DB on SANDBOX_ADMIN_URL
(postgresql://company_os:company_os@localhost:5434/company_os), dropped on exit.

Run:
    python scripts/sandbox_gusto.py [--keep]
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
_TENANT_ID = UUID("00000000-0000-0000-0000-000000006401")
_BASE_URL = "https://api.gusto.com"
_COMPANY = "9341452000000001"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S-08:00")


def _build_fixtures() -> dict:
    now = datetime.now(timezone.utc)

    def invoice(iid, sync, doc, total, balance, customer, updated, due=None):
        obj = {
            "Id": iid, "SyncToken": str(sync), "DocNumber": doc,
            "TotalAmt": total, "Balance": balance,
            "CustomerRef": {"value": "1", "name": customer},
            "TxnDate": (now - timedelta(days=20)).strftime("%Y-%m-%d"),
            "MetaData": {"LastUpdatedTime": updated},
        }
        if due:
            obj["DueDate"] = due
        return obj

    def bill(bid, sync, total, balance, vendor, updated):
        return {
            "Id": bid, "SyncToken": str(sync),
            "TotalAmt": total, "Balance": balance,
            "VendorRef": {"value": "7", "name": vendor},
            "TxnDate": (now - timedelta(days=15)).strftime("%Y-%m-%d"),
            "MetaData": {"LastUpdatedTime": updated},
        }

    def payment(pid, sync, total, customer, updated):
        return {
            "Id": pid, "SyncToken": str(sync), "TotalAmt": total,
            "CustomerRef": {"value": "1", "name": customer},
            "MetaData": {"LastUpdatedTime": updated},
        }

    return {
        "Invoice": {
            "rows": [
                invoice("1037", 0, "1037", 5000.00, 5000.00, "Globex",
                        _iso(now - timedelta(days=3)),
                        due=(now - timedelta(days=1)).strftime("%Y-%m-%d")),
                invoice("1038", 0, "1038", 12000.00, 12000.00, "Initech",
                        _iso(now - timedelta(days=2)),
                        due=(now + timedelta(days=20)).strftime("%Y-%m-%d")),
            ],
            # Incremental: invoice 1037 gets PAID (Balance -> 0, SyncToken bumps)
            # — an AR-collected state_change.
            "delta": [
                invoice("1037", 1, "1037", 5000.00, 0.00, "Globex",
                        _iso(now - timedelta(hours=1))),
            ],
        },
        "Bill": {
            "rows": [
                bill("204", 0, 3200.00, 3200.00, "AWS",
                     _iso(now - timedelta(days=4))),
            ],
            "delta": [],
        },
        "BillPayment": {"rows": [], "delta": []},
        "Payment": {
            "rows": [
                payment("88", 0, 8000.00, "Initech",
                        _iso(now - timedelta(days=1))),
            ],
            "delta": [],
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
    from services.ingest.ingestion.fetchers.gusto import fetch_page_gusto

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_gusto(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("gusto:object", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.gusto import start_mock_gusto

    fixtures = _build_fixtures()
    server, base_url = start_mock_gusto(fixtures)
    os.environ["SYNTHETIC_SOURCE_API_BASE"] = base_url
    _hr("MOCK SERVER")
    print(f"  Gusto API base : {base_url} (served under /gusto via spammer routing)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"gusto_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'gusto-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Connectivity probe via the REAL client.
        _hr("PROBE (GustoClient.company_info)")
        from services.ingest.ingestion.fetchers._clients import build_gusto_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID, "company_uuid": _COMPANY,
                  "base_url": _BASE_URL, "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_gusto_client(_Inst())
        info = await client.company_info()
        _check("company_info probe succeeds", "CompanyInfo" in info)

        # 3. Provision the install + webhook row.
        _hr("PROVISION (gusto.onboarding.finalize_install)")
        from services.ingest.integrations.gusto.client import DEFAULT_ENTITIES
        from services.ingest.integrations.gusto.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, company_uuid=_COMPANY, base_url=_BASE_URL,
            entities=list(DEFAULT_ENTITIES),
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, company_uuid=_COMPANY, webhook_secret_ref=None,
        )
        ent_count = await pool.fetchval(
            "SELECT count(*) FROM gusto_entities WHERE gusto_installation_id=$1",
            install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install + entity rows provisioned", ent_count == len(DEFAULT_ENTITIES))
        _check("onboarding trigger emitted (source=gusto)",
               trig is not None and trig["source"] == "gusto")

        # 4. Plan shards.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.gusto import plan_shards_gusto
        from services.ingest.ingestion.workflows.source_onboarding import _LOAD_GUSTO_INSTALL_SQL
        install_row = await pool.fetchrow(_LOAD_GUSTO_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_gusto(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["entity_type"] for s in shards))
        _check("one shard per entity type", len(shards) == len(DEFAULT_ENTITIES))

        # 5. Backfill.
        _hr("BACKFILL (query -> ingest)")
        for shard in shards:
            ext = await _drain_shard(pool, install_row, shard.shard_identifier)
            print(f"  {shard.shard_identifier['entity_type']}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='gusto:object'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        # 2 invoices (1037 overdue -> state_change, 1038 open -> signal) + 1 bill
        # (open -> signal) + 1 payment (signal) = 4 total, 1 state_change.
        _check("backfill produced 4 observations", counts["tot"] == 4)
        _check("overdue invoice landed as state_change", counts["sc"] == 1)

        # 6. Incremental: invoice 1037 paid (Balance -> 0, SyncToken bumps).
        _hr("INCREMENTAL (invoice paid: AR collected)")
        hw = await pool.fetchval(
            "SELECT max(content->>'last_updated') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='invoice'", _TENANT_ID,
        )
        incr_shard = {"shard_kind": "gusto_entity", "entity_type": "Invoice",
                      "company_uuid": _COMPANY, "updated_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        paid = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND content->>'status'='paid'", _TENANT_ID,
        )
        _check("incremental delta surfaced a paid invoice (new SyncToken)", paid == 1)

        # 7. Dedup: re-ingest a backfilled invoice twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "invoice", "_fyralis_company_uuid": _COMPANY,
                "entity": fixtures["Invoice"]["rows"][1]}
        res = await ingest("gusto:object", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing invoice dedups (SyncToken external_id parity)",
               res.deduped is True)

        # 8. LIVE WEBHOOK path: an Intuit eventNotifications change lands as a
        #    fresh thin-change observation through the SAME handler.
        _hr("LIVE WEBHOOK (eventNotifications)")
        webhook_payload = {
            "eventNotifications": [{
                "companyId": _COMPANY,
                "dataChangeEvent": {"entities": [{
                    "name": "Invoice", "id": "1038", "operation": "Update",
                    "lastUpdated": _iso(datetime.now(timezone.utc)),
                }]},
            }],
        }
        res = await ingest("gusto:object", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook change lands as a fresh observation", res.deduped is False)

        # 9. Reconciler gap probe.
        _hr("RECONCILER GAP PROBE (query since high-water)")
        rows, _ = await client.query("Invoice",
                                     where=f"Metadata.LastUpdatedTime > '{hw}'",
                                     start_position=1, max_results=1)
        _check("reconciler probe detects an invoice updated since the high-water",
               len(rows) >= 1)

        # 10. Inspect.
        _hr("OBSERVATIONS")
        obs = await pool.fetch(
            "SELECT kind, trust_tier, external_id, content_text FROM observations "
            "WHERE tenant_id=$1 ORDER BY occurred_at", _TENANT_ID,
        )
        for r in obs:
            print(f"  [{r['kind']:<12} {r['trust_tier']:<13}] {r['external_id']}")
            print(f"       {r['content_text']}")
        print(f"\n  total observations: {len(obs)}")
        _check("all observations are authoritative gusto:object",
               all(r["trust_tier"] == "authoritative" for r in obs))

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
    parser = argparse.ArgumentParser(description="Gusto ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
