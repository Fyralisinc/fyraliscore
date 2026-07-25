#!/usr/bin/env python3
"""scripts/sandbox_gusto.py — local end-to-end sandbox for Gusto
ingestion (finance/payroll), with NO real Gusto credentials.

Gusto is a payroll REST API (OAuth 2.0, company-scoped — verified
docs.gusto.com) with bare-array list endpoints under
`/v1/companies/{company_uuid}/...`, `page`/`per` offset pagination surfaced in
`X-Total-Count`/`X-Page`/`X-Per-Page` response headers, and hex-HMAC-signed
thin webhooks. This sandbox stands up a REAL local mock of that wire contract
and drives the REAL pipeline:

    GustoClient (real httpx, Provider Lab auth) -> fetch_page_gusto (real page
    cursor + check_date high-water) -> handle_gusto_object (real
    ObservationDraft) -> ingest() (real observation insert + dedup)

It exercises: the `GET /v1/companies/{uuid}` probe, per-entity backfill
(employee / payroll), the incremental `start_date`+`date_filter_by=check_date`
payroll window (a NEW payroll lands), the employee full re-walk (a `version`
bump lands exactly one new observation — there is NO updated-since filter on
/employees), the live-webhook path (real flat thin notification), dedup
parity, and the reconciler gap probe.

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
# Verified production host (docs.gusto.com); Provider Lab is explicit locally.
_BASE_URL = "https://api.gusto.com"
_COMPANY = "8b342a55-907e-4ba8-a95d-d29fbf95d6e1"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _date(dt: datetime) -> str:
    return dt.date().isoformat()


def _employee(uuid, version, first, last, *, title, terminated=False,
              hire_days_ago=300, termination_date=None):
    row = {
        "uuid": uuid, "version": version,
        "first_name": first, "last_name": last,
        "work_email": f"{first.lower()}.{last.lower()}@acme.example",
        "department": "Engineering",
        "terminated": terminated, "onboarded": True,
        "current_employment_status": "full_time",
        "payment_method": "Direct Deposit",
        "jobs": [{
            "uuid": f"job-{uuid}", "primary": True, "title": title,
            "hire_date": _date(
                datetime.now(timezone.utc) - timedelta(days=hire_days_ago)),
            "rate": "98000.00", "payment_unit": "Year",
        }],
        "terminations": [],
    }
    if terminated and termination_date:
        row["terminations"] = [{"effective_date": termination_date,
                                "active": False}]
    return row


def _payroll(uuid, check_date, *, processed):
    return {
        "payroll_uuid": uuid, "uuid": uuid, "company_uuid": _COMPANY,
        "check_date": check_date, "processed": processed,
        "processed_date": check_date if processed else None,
        "off_cycle": False, "external": False,
        "pay_period": {
            "start_date": _date(
                datetime.fromisoformat(check_date) - timedelta(days=14)),
            "end_date": _date(
                datetime.fromisoformat(check_date) - timedelta(days=1)),
            "pay_schedule_uuid": "sched-0001",
        },
        # All dollar amounts are decimal STRINGS (real wire shape).
        "totals": {
            "gross_pay": "42000.00", "net_pay": "33600.00",
            "employee_taxes": "8400.00", "employer_taxes": "3360.00",
            "company_debit": "45360.00", "benefits": "0.00",
            "reimbursements": "0.00",
        },
        "employee_compensations": [],
    }


def _build_fixtures() -> dict:
    now = datetime.now(timezone.utc)
    return {
        # Mock-server fixture keys are the singular entity taxonomy; the dict
        # is held by reference, so the phases below mutate it for drift.
        "employee": [
            _employee("emp-1001", "v-aaa1", "Ava", "Reyes",
                      title="Software Engineer"),
            _employee("emp-1002", "v-bbb2", "Noah", "Chen",
                      title="Account Executive"),
            _employee("emp-1003", "v-ccc3", "Mia", "Okafor",
                      title="Ops Manager", terminated=True,
                      termination_date=_date(now - timedelta(days=5))),
        ],
        "payroll": [
            _payroll("pay-2001", _date(now - timedelta(days=20)),
                     processed=True),
            _payroll("pay-2002", _date(now - timedelta(days=6)),
                     processed=False),
        ],
        "company": {
            "uuid": _COMPANY, "name": "Gusto Sandbox Co",
            "trade_name": "Sandbox Co", "company_status": "Approved",
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
    from services.ingest.synthetic.provider_lab.server import start_provider_lab

    fixtures = _build_fixtures()
    server = start_provider_lab({"gusto": [fixtures]})
    base_url = server.url("gusto")
    os.environ["PROVIDER_LAB_URL"] = server.base_url
    os.environ["GUSTO_API_BASE_URL"] = base_url
    _hr("PROVIDER LAB")
    print(f"  Gusto API base : {base_url} (explicit local override)")

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
        _hr("PROBE (GustoClient.company)")
        from services.ingest.ingestion.fetchers._clients import build_gusto_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID, "company_uuid": _COMPANY,
                  "base_url": _BASE_URL, "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_gusto_client(_Inst())
        info = await client.company()
        _check("GET /v1/companies/{uuid} probe succeeds",
               info.get("uuid") == _COMPANY)

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
        from services.ingest.ingestion.installations import load_source_installation
        install_row = await load_source_installation(
            pool,
            source="gusto",
            tenant_id=_TENANT_ID,
            installation_id=install_id,
        )
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_gusto(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["entity_type"] for s in shards))
        _check("one shard per entity kind", len(shards) == len(DEFAULT_ENTITIES))

        # 5. Backfill (page walk -> ingest).
        _hr("BACKFILL (page/per list -> ingest)")
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
        # 3 employees (emp-1003 terminated -> state_change) + 2 payrolls
        # (pay-2001 processed -> state_change, pay-2002 pending -> signal)
        # = 5 total, 2 state_change.
        _check("backfill produced 5 observations", counts["tot"] == 5)
        _check("terminated employee + processed payroll landed as state_change",
               counts["sc"] == 2)

        # 6. Incremental: a NEW payroll past the check_date high-water window.
        _hr("INCREMENTAL (start_date + date_filter_by=check_date: new payroll)")
        hw = await pool.fetchval(
            "SELECT max(content->>'check_date') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='payroll'",
            _TENANT_ID,
        )
        fixtures["payroll"].append(_payroll(
            "pay-2003", _date(datetime.now(timezone.utc) - timedelta(days=1)),
            processed=True,
        ))
        server.replace_fixtures("gusto", [fixtures])
        incr_shard = {"shard_kind": "gusto_entity", "entity_type": "payroll",
                      "company_uuid": _COMPANY, "updated_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        # The inclusive day-granular window re-fetches the boundary payroll —
        # dedup absorbs it; only the NEW processed payroll lands.
        _check("incremental window surfaced the new processed payroll",
               len(incr) == 1 and ":payroll:pay-2003:processed" in incr[0])

        # 7. Employee re-walk: a `version` bump lands exactly one new
        #    observation (NO updated-since filter exists on /employees).
        _hr("EMPLOYEE RE-WALK (version bump -> one new observation)")
        fixtures["employee"][0]["version"] = "v-aaa2"
        server.replace_fixtures("gusto", [fixtures])
        rewalk_shard = {"shard_kind": "gusto_entity", "entity_type": "employee",
                        "company_uuid": _COMPANY}
        rewalk = await _drain_shard(pool, install_row, rewalk_shard)
        print(f"  re-walk ingested {len(rewalk)} new observations: {rewalk}")
        _check("version bump landed exactly one new employee observation",
               len(rewalk) == 1 and ":employee:emp-1001:v-aaa2" in rewalk[0])

        # 8. Dedup: re-ingest a backfilled employee twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "employee",
                "_fyralis_company_uuid": _COMPANY,
                "entity": fixtures["employee"][1]}
        res = await ingest("gusto:object", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an unchanged employee dedups (version external_id parity)",
               res.deduped is True)

        # 9. LIVE WEBHOOK path: a REAL Gusto flat thin notification lands as a
        #    fresh observation through the SAME handler.
        _hr("LIVE WEBHOOK (flat thin notification, resource_uuid=company)")
        webhook_payload = {
            "uuid": str(uuid4()),
            "event_type": "employee.terminated",
            "resource_type": "Company",
            "resource_uuid": _COMPANY,
            "entity_type": "Employee",
            "entity_uuid": "emp-1002",
            "timestamp": int(datetime.now(timezone.utc).timestamp()),
        }
        res = await ingest("gusto:object", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook change lands as a fresh observation", res.deduped is False)

        # 10. Reconciler gap probe (one cheap page past the high-water).
        _hr("RECONCILER GAP PROBE (list_payrolls start_date=high-water)")
        rows, _ = await client.list_payrolls(
            page=1, per=100, start_date=hw, date_filter_by="check_date",
            payroll_types=("regular", "off_cycle"),
        )
        fresh = [r for r in rows
                 if isinstance(r.get("check_date"), str) and r["check_date"] > hw]
        _check("reconciler probe detects a payroll past the high-water",
               len(fresh) >= 1)

        # 11. Inspect.
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
