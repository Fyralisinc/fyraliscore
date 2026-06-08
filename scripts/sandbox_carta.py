#!/usr/bin/env python3
"""scripts/sandbox_carta.py — local end-to-end sandbox for Carta ingestion
(cap-table), with NO real Carta credentials.

Carta is a cap-table REST API (OAuth 2.0, firm-scoped) with a list/query surface
and a POLL-ONLY live edge (NO webhook). This sandbox stands up a REAL local mock
of the CARTA v1 query endpoint and drives the REAL pipeline:

    CartaClient (real httpx, spammer auth) -> fetch_page_carta (real cursor +
    query) -> handle_carta_object (real ObservationDraft) -> ingest() (real
    observation insert + dedup)

It exercises: entity enumeration (install), per-entity backfill, the incremental
LastUpdatedTime delta (an option grant exercised -> state_change), the live-poll
path (handle_polled_change), cross-path dedup, and the reconciler gap probe.

Because `_clients.py` / `source_onboarding.py` are SHARED files owned by the
wiring phase, this sandbox is SELF-CONTAINED: it rebinds the fetcher's
`_open_carta_client` seam to a real CartaClient pointed at the mock, and loads
the install row with an inline SQL clone of the (future) _LOAD_CARTA_INSTALL_SQL.

Database: DATABASE_URL if set, else a throwaway DB on SANDBOX_ADMIN_URL
(postgresql://company_os:company_os@localhost:5434/company_os), dropped on exit.

Run:
    python scripts/sandbox_carta.py [--keep]
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
_TENANT_ID = UUID("00000000-0000-0000-0000-000000006501")
_BASE_URL = "https://api.carta.com"
_FIRM = "firm_9341452000000001"

# Inline clone of the (future) _LOAD_CARTA_INSTALL_SQL loader — aggregates the
# active entity list onto the install so the planner stays stateless.
_LOAD_CARTA_INSTALL_SQL = """
SELECT ci.id, ci.tenant_id, ci.firm_id, ci.base_url, ci.secret_ref,
       ci.refresh_secret_ref, ci.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', ce.entity_type,
             'updated_cursor', ce.updated_cursor
           ) ORDER BY ce.entity_type
         ) FILTER (WHERE ce.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM carta_installations ci
  LEFT JOIN carta_entities ce
    ON ce.carta_installation_id = ci.id AND ce.state = 'active'
 WHERE ci.tenant_id = $1 AND ci.disabled_at IS NULL
 GROUP BY ci.id
 LIMIT 1
"""


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

    def grant(gid, sync, qty, strike, status, holder, updated):
        return {
            "Id": gid, "SyncToken": str(sync), "DocNumber": f"OG-{gid}",
            "Status": status, "Quantity": qty, "StrikePrice": strike,
            "StakeholderRef": {"value": "1", "name": holder},
            "MetaData": {"LastUpdatedTime": updated},
        }

    def shareholder(sid, sync, shares, holder, updated):
        return {
            "Id": sid, "SyncToken": str(sync), "DocNumber": f"SH-{sid}",
            "Status": "active", "ShareCount": shares,
            "StakeholderRef": {"value": "2", "name": holder},
            "ShareClassRef": {"value": "1", "name": "Common"},
            "MetaData": {"LastUpdatedTime": updated},
        }

    def safe(fid, sync, amount, cap, updated):
        return {
            "Id": fid, "SyncToken": str(sync), "DocNumber": f"SAFE-{fid}",
            "Status": "outstanding", "InvestmentAmount": amount,
            "ValuationCap": cap, "DiscountRate": 0.2,
            "StakeholderRef": {"value": "3", "name": "Seed Fund"},
            "MetaData": {"LastUpdatedTime": updated},
        }

    def share_class(cid, sync, pps, updated):
        return {
            "Id": cid, "SyncToken": str(sync), "DocNumber": f"SC-{cid}",
            "Status": "active", "ShareCount": 10_000_000, "PricePerShare": pps,
            "MetaData": {"LastUpdatedTime": updated},
        }

    return {
        "Shareholder": {
            "rows": [shareholder("2001", 0, 50000, "Founder",
                                 _iso(now - timedelta(days=3)))],
            "delta": [],
        },
        "ShareClass": {
            "rows": [share_class("3001", 0, 1.50,
                                 _iso(now - timedelta(days=3)))],
            "delta": [],
        },
        "SafeNote": {
            "rows": [safe("4001", 0, 250000.00, 8000000.00,
                          _iso(now - timedelta(days=2)))],
            "delta": [],
        },
        "OptionGrant": {
            "rows": [grant("5001", 0, 1000, 0.25, "active", "Employee-1",
                           _iso(now - timedelta(days=2)))],
            # Incremental: grant 5001 gets EXERCISED (Status flips, SyncToken
            # bumps) — a cap-table state_change.
            "delta": [grant("5001", 1, 1000, 0.25, "exercised", "Employee-1",
                            _iso(now - timedelta(hours=1)))],
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


def _make_real_client(base_url: str):
    """Build a REAL CartaClient pointed at the mock (spammer auth, no secrets)."""
    from services.ingest.integrations.carta.client import CartaClient
    return CartaClient(
        base_url=base_url, firm_id=_FIRM, access_token="spam-carta",
        api_base_url=base_url,
    )


async def _drain_shard(pool, install_row, shard_identifier) -> list[str]:
    from services.ingest.ingestion.core import ingest
    from services.ingest.ingestion.fetchers.carta import fetch_page_carta

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_carta(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("carta:object", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.carta import start_mock_carta

    # Import side-effects register the dispatch entries: the handler's @register
    # decorator (carta:object) and the fetcher/planner/reconciler dispatch slots.
    # In production the wiring phase imports these at service startup; the
    # standalone sandbox must import them itself.
    import services.ingest.ingestion.handlers.carta  # noqa: F401
    import services.ingest.ingestion.fetchers.carta  # noqa: F401
    import services.ingest.ingestion.planners.carta  # noqa: F401
    import services.ingest.ingestion.reconcilers.carta  # noqa: F401

    fixtures = _build_fixtures()
    server, base_url = start_mock_carta(fixtures)
    os.environ["SYNTHETIC_SOURCE_API_BASE"] = base_url
    _hr("MOCK SERVER")
    print(f"  Carta API base : {base_url} (served under /carta via spammer routing)")

    # Rebind the fetcher + reconciler seam to a real client at the mock, so the
    # sandbox does not depend on the (wiring-owned) _clients.py builder.
    import services.ingest.ingestion.fetchers.carta as carta_fetcher

    async def _open(install):  # noqa: ANN001
        client = _make_real_client(base_url)
        async def _close() -> None:
            await client.aclose()
        return client, _close

    carta_fetcher._open_carta_client = _open  # type: ignore[assignment]

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"carta_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'carta-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Connectivity probe via the REAL client.
        _hr("PROBE (CartaClient.firm_info)")
        client = _make_real_client(base_url)
        info = await client.firm_info()
        _check("firm_info probe succeeds", "FirmInfo" in info)

        # 3. Provision the install (poll-only — NO webhook registration).
        _hr("PROVISION (carta.onboarding.finalize_install)")
        from services.ingest.integrations.carta.client import DEFAULT_ENTITIES
        from services.ingest.integrations.carta.onboarding import finalize_install
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, firm_id=_FIRM, base_url=_BASE_URL,
            entities=list(DEFAULT_ENTITIES),
        )
        ent_count = await pool.fetchval(
            "SELECT count(*) FROM carta_entities WHERE carta_installation_id=$1",
            install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install + entity rows provisioned", ent_count == len(DEFAULT_ENTITIES))
        _check("onboarding trigger emitted (source=carta)",
               trig is not None and trig["source"] == "carta")

        # 4. Plan shards.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.carta import plan_shards_carta
        install_row = await pool.fetchrow(_LOAD_CARTA_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_carta(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["entity_type"] for s in shards))
        _check("one shard per entity type", len(shards) == len(DEFAULT_ENTITIES))

        # 5. Backfill — 4 entity kinds x 1 row = 4 observations.
        _hr("BACKFILL (query -> ingest)")
        for shard in shards:
            ext = await _drain_shard(pool, install_row, shard.shard_identifier)
            print(f"  {shard.shard_identifier['entity_type']}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='carta:object'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        _check("backfill produced 4 observations", counts["tot"] == 4)

        # 6. Incremental: option grant 5001 exercised (Status flips, SyncToken bumps).
        _hr("INCREMENTAL (option grant exercised: cap-table state_change)")
        hw = await pool.fetchval(
            "SELECT max(content->>'last_updated') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='option_grant'", _TENANT_ID,
        )
        incr_shard = {"shard_kind": "carta_entity", "entity_type": "OptionGrant",
                      "firm_id": _FIRM, "updated_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        exercised = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND content->>'status'='exercised'", _TENANT_ID,
        )
        _check("incremental delta surfaced an exercised grant (new SyncToken)", exercised == 1)

        # 7. Dedup: re-ingest a backfilled grant twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "optiongrant", "_fyralis_firm_id": _FIRM,
                "entity": fixtures["OptionGrant"]["rows"][0]}
        res = await ingest("carta:object", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing grant dedups (SyncToken external_id parity)",
               res.deduped is True)

        # 8. LIVE POLL path: a polled change lands as a fresh observation through
        #    the SAME handler via handle_polled_change.
        _hr("LIVE POLL (handle_polled_change)")
        from services.ingest.integrations.carta.poll import PollDeps, handle_polled_change
        change = {
            "entity_type": "OptionGrant",
            "entity": {
                "Id": "1000001", "SyncToken": "9", "DocNumber": "OG-1000001",
                "Status": "exercised", "Quantity": 500, "StrikePrice": 1.25,
                "StakeholderRef": {"value": "9", "name": "Live Holder"},
                "MetaData": {"LastUpdatedTime": _iso(datetime.now(timezone.utc))},
            },
        }
        deps = PollDeps(pool=pool, tenant_id=_TENANT_ID, installation_id=str(install_id),
                        firm_id=_FIRM)
        before = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='carta:object'", _TENANT_ID)
        await handle_polled_change(change, deps)
        after = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='carta:object'", _TENANT_ID)
        _check("live poll change lands as a fresh observation", after == before + 1)

        # 9. Reconciler gap probe.
        _hr("RECONCILER GAP PROBE (query since high-water)")
        rows, _ = await client.query("OptionGrant",
                                     where=f"Metadata.LastUpdatedTime > '{hw}'",
                                     start_position=1, max_results=1)
        _check("reconciler probe detects a grant updated since the high-water",
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
        _check("all observations are authoritative carta:object",
               all(r["trust_tier"] == "authoritative" for r in obs))

        await client.aclose()

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
    parser = argparse.ArgumentParser(description="Carta ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
