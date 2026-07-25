#!/usr/bin/env python3
"""scripts/sandbox_ramp.py — local end-to-end sandbox for Ramp
ingestion (finance), with NO real Ramp credentials.

Ramp is a spend/card REST API (OAuth 2.0 client credentials, keyset-paginated
collections — verified docs.ramp.com) with HMAC-signed flat webhooks. This
sandbox stands up a REAL local mock of the wire contract
(`{"data": [...], "page": {"next": …}}` envelopes + `POST /token` +
`GET /business`) and drives the REAL pipeline:

    RampClient (real httpx, Provider Lab auth) -> fetch_page_ramp (real keyset
    cursor) -> handle_ramp_transaction (real ObservationDraft) ->
    ingest() (real observation insert + dedup)

It exercises: the `GET /business` probe, per-stream backfill (transaction /
reimbursement / card / user), the incremental `from_date` window (a NEW
transaction lands; NOTE state flips on OLD transactions ride the webhook, not
the date window — `from_date` filters `user_transaction_time`), the
live-webhook path (real flat event -> thin change), dedup parity, and the
reconciler gap probe.

Database: DATABASE_URL if set, else a throwaway DB on SANDBOX_ADMIN_URL
(postgresql://company_os:company_os@localhost:5434/company_os), dropped on exit.

Run:
    python scripts/sandbox_ramp.py [--keep]
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
# Verified production host (docs.ramp.com); Provider Lab is explicit locally.
_BASE_URL = "https://api.ramp.com/developer/v1"
_BUSINESS = "bus-sandbox-0001"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _build_fixtures() -> dict:
    now = datetime.now(timezone.utc)

    def txn(tid, state, amount, merchant, when, holder="Avery Chen"):
        cents = int(round(amount * 100))
        first, last = holder.split(" ", 1)
        return {
            "id": tid, "state": state,
            "amount": amount, "currency_code": "USD",
            "original_transaction_amount": {
                "amount": cents, "currency_code": "USD",
                "minor_unit_conversion_rate": 100,
            },
            "user_transaction_time": _iso(when),
            "settlement_date": _iso(when),
            "merchant_name": merchant,
            "sk_category_name": "Cloud Computing",
            "card_holder": {"first_name": first, "last_name": last},
            "disputes": [],
        }

    def reimb(rid, state, amount, who, when):
        return {
            "id": rid, "state": state,
            "amount": amount, "currency": "USD",
            "created_at": _iso(when - timedelta(days=1)),
            "updated_at": _iso(when),
            "user_full_name": who,
            "merchant": "Conference Travel",
            "type": "OUT_OF_POCKET", "direction": "BUSINESS_TO_USER",
        }

    return {
        "transactions": {
            "rows": [
                txn("txn-1001", "PENDING", 5000.00, "Globex Cloud",
                    now - timedelta(days=3)),
                txn("txn-1002", "DECLINED", 12000.00, "Initech SaaS",
                    now - timedelta(days=2)),
            ],
            # Incremental window (`from_date` on user_transaction_time): a NEW
            # transaction since the high-water. (A state flip on an OLD
            # transaction rides the webhook path, not the date window.)
            "delta": [
                txn("txn-1003", "CLEARED", 750.00, "Acme Tools",
                    now - timedelta(hours=1)),
            ],
        },
        "reimbursements": {
            "rows": [
                reimb("rmb-2001", "PENDING", 320.00, "Jordan Lee",
                      now - timedelta(days=4)),
            ],
            "delta": [],
        },
        "cards": {
            "rows": [{
                "id": "card-3001", "state": "ACTIVE",
                "display_name": "Eng Infra", "last_four": "4242",
                "cardholder_name": "Avery Chen", "is_physical": False,
                "expiration": "2030-01",
                "created_at": _iso(now - timedelta(days=30)),
            }],
            "delta": [],
        },
        "users": {
            "rows": [{
                "id": "usr-4001", "status": "USER_ACTIVE",
                "first_name": "Avery", "last_name": "Chen",
                "email": "avery@example.com", "role": "BUSINESS_ADMIN",
                "is_manager": True,
            }],
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
    from services.ingest.ingestion.fetchers.ramp import fetch_page_ramp

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_ramp(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("ramp:transaction", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.provider_lab.server import start_provider_lab

    fixtures = _build_fixtures()
    server = start_provider_lab(
        {"ramp": [{**fixtures, "business_id": _BUSINESS}]}
    )
    base_url = server.url("ramp")
    os.environ["PROVIDER_LAB_URL"] = server.base_url
    os.environ["RAMP_API_BASE_URL"] = base_url
    _hr("PROVIDER LAB")
    print(f"  Ramp API base : {base_url} (explicit local override)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"ramp_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'ramp-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Connectivity probe via the REAL client.
        _hr("PROBE (RampClient.business)")
        from services.ingest.ingestion.fetchers._clients import build_ramp_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID, "business_id": _BUSINESS,
                  "base_url": _BASE_URL, "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_ramp_client(_Inst())
        info = await client.business()
        _check("GET /business probe succeeds", info.get("id") == _BUSINESS)

        # 3. Provision the install + webhook row.
        _hr("PROVISION (ramp.onboarding.finalize_install)")
        from services.ingest.integrations.ramp.client import DEFAULT_ENTITIES
        from services.ingest.integrations.ramp.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, business_id=_BUSINESS, base_url=_BASE_URL,
            entities=list(DEFAULT_ENTITIES),
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, business_id=_BUSINESS, webhook_secret_ref=None,
        )
        ent_count = await pool.fetchval(
            "SELECT count(*) FROM ramp_entities WHERE ramp_installation_id=$1",
            install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install + entity rows provisioned", ent_count == len(DEFAULT_ENTITIES))
        _check("onboarding trigger emitted (source=ramp)",
               trig is not None and trig["source"] == "ramp")

        # 4. Plan shards.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.ramp import plan_shards_ramp
        from services.ingest.ingestion.installations import load_source_installation
        install_row = await load_source_installation(
            pool,
            source="ramp",
            tenant_id=_TENANT_ID,
            installation_id=install_id,
        )
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_ramp(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["entity_type"] for s in shards))
        _check("one shard per entity stream", len(shards) == len(DEFAULT_ENTITIES))

        # 5. Backfill (keyset walk -> ingest).
        _hr("BACKFILL (keyset list -> ingest)")
        for shard in shards:
            ext = await _drain_shard(pool, install_row, shard.shard_identifier)
            print(f"  {shard.shard_identifier['entity_type']}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='ramp:transaction'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        # 2 transactions (1002 DECLINED -> state_change) + 1 reimbursement +
        # 1 card + 1 user = 5 total, 1 state_change.
        _check("backfill produced 5 observations", counts["tot"] == 5)
        _check("declined transaction landed as state_change", counts["sc"] == 1)

        # 6. Incremental: a NEW transaction past the high-water window.
        _hr("INCREMENTAL (from_date window: new transaction)")
        hw = await pool.fetchval(
            "SELECT max(content->>'user_transaction_time') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='transaction'",
            _TENANT_ID,
        )
        incr_shard = {"shard_kind": "ramp_entity", "entity_type": "transaction",
                      "business_id": _BUSINESS, "updated_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        _check("incremental window surfaced the new cleared transaction",
               len(incr) == 1 and ":txn:txn-1003:cleared" in incr[0])

        # 7. Dedup: re-ingest a backfilled transaction twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "transaction",
                "_fyralis_business_id": _BUSINESS,
                "entity": fixtures["transactions"]["rows"][1]}
        res = await ingest("ramp:transaction", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing transaction dedups (state-versioned external_id parity)",
               res.deduped is True)

        # 8. LIVE WEBHOOK path: a REAL Ramp flat event lands as a fresh
        #    thin-change observation through the SAME handler.
        _hr("LIVE WEBHOOK (flat event, root business_id)")
        webhook_payload = {
            "id": f"evt_{uuid4()}",
            "type": "transactions.cleared",
            "created_at": _iso(datetime.now(timezone.utc)),
            "business_id": _BUSINESS,
            "object": {"id": "txn-1001"},
        }
        res = await ingest("ramp:transaction", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook change lands as a fresh observation", res.deduped is False)

        # 9. Reconciler gap probe (minimal keyset page past the high-water).
        _hr("RECONCILER GAP PROBE (list_transactions from_date=high-water)")
        rows, _ = await client.list_transactions(from_date=hw, page_size=2)
        _check("reconciler probe detects a transaction past the high-water",
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
        _check("all observations are authoritative ramp:transaction",
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
    parser = argparse.ArgumentParser(description="Ramp ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
