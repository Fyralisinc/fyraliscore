#!/usr/bin/env python3
"""scripts/sandbox_mercury.py — local end-to-end sandbox for Mercury ingestion
(finance), with NO real Mercury credentials.

Mercury is a banking REST API (Bearer API token) with BOTH a historical query
surface (GET /accounts, /account/{id}/transactions) and a live push surface
(HMAC-signed webhooks). This sandbox stands up the canonical local Provider Lab
implementation of the Mercury v1 endpoints and drives the REAL pipeline:

    MercuryClient (real httpx, Provider Lab auth) -> fetch_page_mercury (real cursor +
    fan-out) -> handle_mercury_transaction (real ObservationDraft) -> ingest()
    (real observation insert + dedup)

It exercises: account enumeration, per-account backfill with the balance-snapshot
+ transaction fan-out, the incremental delta (a transaction status change ->
state_change), the live-webhook path through the SAME handler (asserting
external_id parity / dedup with backfill), cross-path dedup, and the reconciler
gap probe — then prints the observations that landed.

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_mercury.py
    python scripts/sandbox_mercury.py --keep
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
_TENANT_ID = UUID("00000000-0000-0000-0000-000000006301")
_BASE_URL = "https://api.mercury.com/api/v1"
_ORG_ID = "org-sandbox"
_ACCOUNT = "acc-checking"


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

    def txn(tid, amount, counterparty, status, created, kind="externalTransfer"):
        return {
            "id": tid,
            "amount": amount,
            "counterpartyName": counterparty,
            "status": status,
            "kind": kind,
            "createdAt": created,
            "postedAt": created,
            "bankDescription": f"{counterparty} {kind}",
        }

    return {
        _ACCOUNT: {
            "account": {
                "id": _ACCOUNT,
                "name": "Operating Checking",
                "type": "checking",
                "availableBalance": 482350.12,
                "currentBalance": 491200.00,
            },
            "transactions": [
                txn("t-1001", -5000.00, "Acme Cloud", "sent",
                    _iso(now - timedelta(days=3))),
                txn("t-1002", 120000.00, "Stripe Payout", "sent",
                    _iso(now - timedelta(days=2)), kind="incomingPayment"),
            ],
            # Incremental delta: t-1001 FAILS (sent -> failed) — a fresh
            # cash-risk state_change the poll/reconcile surfaces.
            "delta": [
                txn("t-1001", -5000.00, "Acme Cloud", "failed",
                    _iso(now - timedelta(hours=1))),
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
    from services.ingest.ingestion.fetchers.mercury import fetch_page_mercury

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_mercury(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("mercury:transaction", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.provider_lab.server import start_provider_lab

    fixtures = _build_fixtures()
    server = start_provider_lab({"mercury": [fixtures]})
    base_url = server.url("mercury")
    os.environ["PROVIDER_LAB_URL"] = server.base_url
    os.environ["MERCURY_API_BASE_URL"] = base_url
    _hr("PROVIDER LAB")
    print(f"  Mercury API base : {base_url} (explicit local override)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"mercury_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'mercury-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Enumerate accounts via the REAL client.
        _hr("ENUMERATE ACCOUNTS (MercuryClient.list_accounts)")
        from services.ingest.ingestion.fetchers._clients import build_mercury_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID,
                  "base_url": _BASE_URL, "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_mercury_client(_Inst())
        accounts = await client.list_accounts()
        account_ids = [a["id"] for a in accounts]
        print(f"  accounts discovered: {account_ids}")
        _check("account enumeration returned the checking account", _ACCOUNT in account_ids)

        # 3. Provision the install + webhook row.
        _hr("PROVISION (mercury.onboarding.finalize_install)")
        from services.ingest.integrations.mercury.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL,
            accounts=[{"account_id": a["id"], "account_name": a.get("name"),
                       "account_kind": a.get("type")} for a in accounts],
            organization_id=_ORG_ID,
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, organization_id=_ORG_ID, webhook_secret_ref=None,
        )
        acct_count = await pool.fetchval(
            "SELECT count(*) FROM mercury_accounts WHERE mercury_installation_id=$1", install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install + account rows provisioned", acct_count == len(account_ids))
        _check("onboarding trigger emitted (source=mercury)",
               trig is not None and trig["source"] == "mercury")

        # 4. Plan shards.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.mercury import plan_shards_mercury
        from services.ingest.ingestion.installations import load_source_installation
        install_row = await load_source_installation(
            pool,
            source="mercury",
            tenant_id=_TENANT_ID,
            installation_id=install_id,
        )
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_mercury(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["account_id"] for s in shards))
        _check("one shard per account", len(shards) == len(account_ids))

        # 5. Backfill: real fetcher -> real ingest.
        _hr("BACKFILL (snapshot + transaction fan-out -> ingest)")
        for shard in shards:
            ext = await _drain_shard(pool, install_row, shard.shard_identifier)
            print(f"  {shard.shard_identifier['account_id']}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='mercury:transaction'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        # 1 balance snapshot + 2 transactions (both sent -> signal) = 3.
        _check("backfill produced 3 observations (1 snapshot + 2 transactions)",
               counts["tot"] == 3)

        # 6. Incremental: warm-start from the high-water -> delta (t-1001 fails).
        _hr("INCREMENTAL (status transition: sent -> failed)")
        # Mercury applies ``start`` as a timestamp filter over the account's
        # current transaction collection. Mutate that collection before the
        # warm poll instead of relying on the retired mock server's synthetic
        # "any start means delta" branch.
        fixtures[_ACCOUNT]["transactions"][0] = fixtures[_ACCOUNT]["delta"][0]
        server.replace_fixtures("mercury", [fixtures])
        hw = await pool.fetchval(
            "SELECT max(content->>'created_at') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='transaction'", _TENANT_ID,
        )
        incr_shard = {"shard_kind": "mercury_account_txns", "account_id": _ACCOUNT,
                      "txn_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        sc_after = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 AND kind='state_change'",
            _TENANT_ID,
        )
        _check("incremental delta surfaced a failed-payment state_change", sc_after == 1)

        # 7. Dedup: re-ingest a backfilled transaction twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "transaction", "_fyralis_account_id": _ACCOUNT,
                "transaction": fixtures[_ACCOUNT]["transactions"][1]}
        res = await ingest("mercury:transaction", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing transaction dedups (versioned external_id parity)",
               res.deduped is True)

        # 8. LIVE WEBHOOK path: a transaction.created with the SAME txn flows
        #    through the SAME handler; its external_id matches the backfilled
        #    transaction, so it dedups (proves backfill+live parity).
        _hr("LIVE WEBHOOK (handler parity with backfill)")
        webhook_payload = {
            "type": "transaction.created",
            "organizationId": _ORG_ID,
            "_fyralis_account_id": _ACCOUNT,
            "transaction": fixtures[_ACCOUNT]["transactions"][1],
        }
        res = await ingest("mercury:transaction", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook transaction dedups against backfilled twin (external_id parity)",
               res.deduped is True)

        # A brand-new live transaction lands as a fresh observation.
        fresh_txn = {
            "type": "transaction.created", "organizationId": _ORG_ID,
            "_fyralis_account_id": _ACCOUNT,
            "transaction": {"id": "t-live-1", "amount": -250.00,
                            "counterpartyName": "AWS", "status": "sent",
                            "kind": "externalTransfer",
                            "createdAt": _iso(datetime.now(timezone.utc))},
        }
        res = await ingest("mercury:transaction", fresh_txn, pool=pool, tenant_id=_TENANT_ID)
        _check("new live transaction lands as a fresh observation", res.deduped is False)

        # 9. Reconciler gap probe against the live (mock) account.
        _hr("RECONCILER GAP PROBE (list_transactions since high-water)")
        txns, _, _ = await client.list_transactions(_ACCOUNT, limit=1, offset=0,
                                                     start=(hw or "2020-01-01")[:10])
        _check("reconciler probe detects a transaction since the high-water", len(txns) >= 1)

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
        _check("all observations are authoritative mercury:transaction",
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
    parser = argparse.ArgumentParser(description="Mercury ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
