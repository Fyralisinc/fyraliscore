#!/usr/bin/env python3
"""scripts/sandbox_carta.py — local end-to-end sandbox for Carta ingestion
(cap-table), with NO real Carta credentials.

Carta's API Platform is an **issuer** cap-table REST suite under `/v1alpha1`
(OAuth 2.0 access token, ~1 h, no refresh grant) with AIP-158 pageToken list
pagination and a POLL-ONLY live edge (NO webhook). This sandbox stands up a
REAL local Provider Lab implementation of that wire contract and drives the
REAL pipeline:

    CartaClient (real httpx, Provider Lab auth) -> fetch_page_carta (real pageToken
    cursor) -> handle_carta_object (real ObservationDraft, wrapper decoding) ->
    ingest() (real observation insert + dedup)

It exercises: issuer enumeration (install), per-entity backfill across the four
`/v1alpha1` collections, the optionGrants `lastModifiedDatetimeAfter` delta (a
grant exercised -> state_change), the live-poll path (handle_polled_change),
cross-path content-digest dedup, and the reconciler gap probe.

Because `_clients.py` / `source_onboarding.py` are SHARED files owned by the
wiring phase, this sandbox is SELF-CONTAINED: it rebinds the fetcher's
`_open_carta_client` seam to a real CartaClient pointed at Provider Lab, and loads
the install row with an inline SQL clone of the loader SQL.

Database: DATABASE_URL if set, else a throwaway DB on SANDBOX_ADMIN_URL
(postgresql://company_os:company_os@localhost:5434/company_os), dropped on exit.

Run:
    python scripts/sandbox_carta.py [--keep]
"""
from __future__ import annotations

import argparse
import asyncio
import copy
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
# The Carta issuer id (stored in carta_installations.firm_id — the column
# predates the issuer naming).
_FIRM = "f6e1d4a0-0000-4000-8000-00000000ca01"

def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dec(value) -> dict:
    """v1alpha1Decimal / date / datetime wrapper."""
    return {"value": str(value)}


def _money(amount, currency: str = "USD") -> dict:
    """v1alpha1Money."""
    return {"currencyCode": _dec(currency), "amount": _dec(amount)}


def _build_fixtures() -> dict:
    """A make_carta-shaped fixture: one issuer, one row per /v1alpha1
    collection, with real wrapper-shaped fields."""
    now = datetime.now(timezone.utc)
    return {
        "firm_id": _FIRM,
        "page_size": 50,
        "issuer": {"id": _FIRM, "legalName": "Sandbox Issuer Inc."},
        "entities": {
            "stakeholder": [{
                "id": "2001", "issuerId": _FIRM,
                "fullName": "Founder One",
                "email": "founder@sandbox.example",
                "employeeId": "EMP-2001",
                "relationship": "FOUNDER", "entityType": "INDIVIDUAL",
            }],
            "shareClass": [{
                "id": "3001", "issuerId": _FIRM, "name": "Common",
                "prefix": "CS", "type": "COMMON",
                "authorizedShareCount": _dec(10_000_000),
                "parValue": _money("0.0001"),
                "seniority": 1, "pariPassu": False,
            }],
            "convertibleNote": [{
                "id": "4001", "issuerId": _FIRM, "securityLabel": "CN-4001",
                "stakeholderId": "3",
                "cashPaid": _money("250000.00"),
                "priceCap": _money("8000000.00"),
                "discountPercentage": _dec("20"), "interestRate": _dec("5"),
                "issueDatetime": _dec(_iso_z(now - timedelta(days=200))),
                "maturityDatetime": _dec(_iso_z(now + timedelta(days=530))),
            }],
            "optionGrant": [{
                "id": "5001", "issuerId": _FIRM, "securityLabel": "OG-5001",
                "stakeholderId": "1", "shareClassId": "1",
                "stockOptionType": "ISO",
                "quantity": _dec(1000), "outstandingQuantity": _dec(1000),
                "vestedQuantity": _dec(250), "exercisedQuantity": _dec(0),
                "exercisePrice": _money("0.25"),
                "issueDate": _dec((now - timedelta(days=400)).date().isoformat()),
                "lastModifiedDatetime": _dec(_iso_z(now - timedelta(days=2))),
            }],
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
    """Build a real CartaClient pointed at Provider Lab."""
    from services.ingest.integrations.carta.client import CartaClient
    return CartaClient(
        base_url=base_url, issuer_id=_FIRM, access_token="spam-carta",
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
    from services.ingest.synthetic.provider_lab.server import start_provider_lab

    # Import side-effects register the dispatch entries: the handler's @register
    # decorator (carta:object) and the fetcher/planner/reconciler dispatch slots.
    # In production the wiring phase imports these at service startup; the
    # standalone sandbox must import them itself.
    import services.ingest.ingestion.handlers.carta  # noqa: F401
    import services.ingest.ingestion.fetchers.carta  # noqa: F401
    import services.ingest.ingestion.planners.carta  # noqa: F401
    import services.ingest.ingestion.reconcilers.carta  # noqa: F401

    fixtures = _build_fixtures()
    server = start_provider_lab({"carta": [fixtures]})
    base_url = server.url("carta")
    os.environ["PROVIDER_LAB_URL"] = server.base_url
    os.environ["CARTA_API_BASE_URL"] = base_url
    _hr("PROVIDER LAB")
    print(f"  Carta API base : {base_url} (explicit local override)")

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

        # 2. Connectivity probe + issuer enumeration via the REAL client.
        _hr("PROBE (CartaClient.probe / list_issuers)")
        client = _make_real_client(base_url)
        body = await client.probe()
        _check("probe (GET /v1alpha1/issuers?pageSize=1) succeeds",
               bool(body.get("issuers")))
        issuers, _tok = await client.list_issuers()
        _check("issuer enumeration sees the sandbox issuer",
               len(issuers) == 1 and issuers[0].get("id") == _FIRM)

        # 2b. OAuth client_credentials mint endpoint (the re-mint edge).
        import httpx
        async with httpx.AsyncClient() as http:
            tok = await http.post(f"{base_url}/o/access_token/", data={
                "grant_type": "client_credentials", "scope": "read_issuer_info",
            })
        _check("POST /o/access_token/ mints an access token (no refresh_token)",
               tok.status_code == 200
               and tok.json().get("access_token")
               and "refresh_token" not in tok.json())

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
        from services.ingest.ingestion.installations import load_source_installation
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.carta import plan_shards_carta
        install_row = await load_source_installation(
            pool,
            source="carta",
            tenant_id=_TENANT_ID,
            installation_id=install_id,
        )
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_carta(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["entity_type"] for s in shards))
        _check("one shard per entity type", len(shards) == len(DEFAULT_ENTITIES))

        # 5. Backfill — 4 entity kinds x 1 row = 4 observations.
        _hr("BACKFILL (AIP pageToken walk -> ingest)")
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

        # 6. Incremental: grant 5001 gets EXERCISED in the poll window.
        #    Replace the source state through Provider Lab's control plane;
        #    lastModifiedDatetimeAfter (optionGrants only) picks it up and the
        #    content-digest version re-observes the mutation.
        _hr("INCREMENTAL (option grant exercised: cap-table state_change)")
        hw = await pool.fetchval(
            "SELECT max(content->>'last_modified') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='option_grant'", _TENANT_ID,
        )
        orig_grant = copy.deepcopy(fixtures["entities"]["optionGrant"][0])
        grant = fixtures["entities"]["optionGrant"][0]
        grant["exercisedQuantity"] = _dec(1000)
        grant["outstandingQuantity"] = _dec(0)
        grant["lastModifiedDatetime"] = _dec(
            _iso_z(datetime.now(timezone.utc) - timedelta(hours=1)),
        )
        server.replace_fixtures("carta", [fixtures])
        incr_shard = {"shard_kind": "carta_entity", "entity_type": "optionGrant",
                      "firm_id": _FIRM, "updated_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        exercised = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND content->>'status'='exercised'", _TENANT_ID,
        )
        _check("incremental delta surfaced an exercised grant (new digest version)",
               exercised == 1)

        # 7. Dedup: re-ingest the ORIGINAL (pre-mutation) grant twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = {"_fyralis_record_type": "optiongrant", "_fyralis_firm_id": _FIRM,
                "entity": orig_grant}
        res = await ingest("carta:object", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an unchanged grant dedups (content-digest parity)",
               res.deduped is True)

        # 8. LIVE POLL path: a polled change lands as a fresh observation through
        #    the SAME handler via handle_polled_change.
        _hr("LIVE POLL (handle_polled_change)")
        from services.ingest.integrations.carta.poll import PollDeps, handle_polled_change
        change = {
            "entity_type": "optionGrant",
            "entity": {
                "id": "1000001", "issuerId": _FIRM,
                "securityLabel": "OG-1000001", "stakeholderId": "9",
                "stockOptionType": "ISO",
                "quantity": _dec(500), "outstandingQuantity": _dec(0),
                "vestedQuantity": _dec(500), "exercisedQuantity": _dec(500),
                "exercisePrice": _money("1.25"),
                "issueDate": _dec("2026-06-01"),
                "lastModifiedDatetime": _dec(_iso_z(datetime.now(timezone.utc))),
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

        # 9. Reconciler gap probe (optionGrants — the one delta-filterable
        #    collection).
        _hr("RECONCILER GAP PROBE (lastModifiedDatetimeAfter since high-water)")
        rows, _ = await client.list_option_grants(page_size=1, modified_after=hw)
        _check("reconciler probe detects a grant modified since the high-water",
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
