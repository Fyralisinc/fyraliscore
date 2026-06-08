#!/usr/bin/env python3
"""scripts/sandbox_miro.py — local end-to-end sandbox for Miro ingestion
(whiteboard / design), with NO real Miro credentials.

Miro is a whiteboard REST API (org-app Bearer token) with BOTH a historical
query surface (GET /boards, /boards/{id}/items) and a live push surface
(HMAC-signed webhooks). This sandbox stands up a REAL local mock of the Miro v2
endpoints and drives the REAL pipeline against it:

    MiroClient (real httpx, spammer auth) -> fetch_page_miro (real opaque-cursor
    pagination + fan-out) -> handle_miro_item (real ObservationDraft) -> ingest()
    (real observation insert + dedup)

It exercises: board enumeration, per-board backfill with the item fan-out, the
opaque-cursor pagination, an item edit (version bump -> a new observation), the
live-webhook path through the SAME handler (asserting external_id parity / dedup
with backfill), cross-path dedup, and the reconciler gap probe — then prints the
observations that landed.

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_miro.py
    python scripts/sandbox_miro.py --keep
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
from uuid import UUID, uuid4

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

import asyncpg


_DEFAULT_ADMIN_URL = "postgresql://company_os:company_os@localhost:5434/company_os"
_TENANT_ID = UUID("00000000-0000-0000-0000-000000006401")
_BASE_URL = "https://api.miro.com/v2"
_ORG_ID = "org-sandbox"
_BOARD = "board-design"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _build_fixtures() -> dict:
    def item(iid, text, item_type, modified, version):
        return {
            "id": iid,
            "boardId": _BOARD,
            "type": item_type,
            "data": {"content": text},
            "createdBy": {"id": "user-1", "type": "user"},
            "modifiedBy": {"id": "user-1", "type": "user"},
            "createdAt": modified,
            "modifiedAt": modified,
            "version": version,
        }

    return {
        _BOARD: {
            "board": {
                "id": _BOARD,
                "name": "Q3 Roadmap",
                "type": "board",
                "modifiedAt": "2026-01-05T00:00:00Z",
            },
            "items": [
                item("i-1001", "Ship onboarding", "sticky_note",
                     "2026-01-05T10:00:00Z", "1"),
                item("i-1002", "Hire designer", "card",
                     "2026-01-05T11:00:00Z", "1"),
            ],
            # Incremental delta: i-1001 is EDITED (version 1 -> 2) — a fresh
            # observation the poll/reconcile surfaces.
            "delta": [
                item("i-1001", "Ship onboarding (done)", "sticky_note",
                     "2026-01-06T09:00:00Z", "2"),
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
    from services.ingest.ingestion.fetchers.miro import fetch_page_miro

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_miro(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("miro:item", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.miro import start_mock_miro

    fixtures = _build_fixtures()
    server, base_url = start_mock_miro(fixtures)
    os.environ["SYNTHETIC_SOURCE_API_BASE"] = base_url
    _hr("MOCK SERVER")
    print(f"  Miro API base : {base_url} (served under /miro via spammer routing)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"miro_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'miro-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Enumerate boards via the REAL client.
        _hr("ENUMERATE BOARDS (MiroClient.list_boards)")
        from services.ingest.ingestion.fetchers._clients import build_miro_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID,
                  "base_url": _BASE_URL, "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_miro_client(_Inst())
        boards = await client.list_boards()
        board_ids = [b["id"] for b in boards]
        print(f"  boards discovered: {board_ids}")
        _check("board enumeration returned the design board", _BOARD in board_ids)

        # 3. Provision the install + webhook row.
        _hr("PROVISION (miro.onboarding.finalize_install)")
        from services.ingest.integrations.miro.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL,
            boards=[{"board_id": b["id"], "board_name": b.get("name"),
                     "board_kind": b.get("type")} for b in boards],
            org_id=_ORG_ID,
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, org_id=_ORG_ID, webhook_secret_ref=None,
        )
        board_count = await pool.fetchval(
            "SELECT count(*) FROM miro_boards WHERE miro_installation_id=$1", install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install + board rows provisioned", board_count == len(board_ids))
        _check("onboarding trigger emitted (source=miro)",
               trig is not None and trig["source"] == "miro")

        # 4. Build the install row + shard directly (the loader SQL is owned by
        #    the wiring phase). The org_id threads through to the external_id.
        install_row = {"id": install_id, "tenant_id": _TENANT_ID,
                       "base_url": _BASE_URL, "secret_ref": None}
        shard_identifier = {"shard_kind": "miro_board_items", "board_id": _BOARD,
                            "org_id": _ORG_ID, "installation_id": str(install_id)}

        # 5. Backfill: real fetcher -> real ingest.
        _hr("BACKFILL (item fan-out -> ingest)")
        ext = await _drain_shard(pool, install_row, shard_identifier)
        print(f"  {_BOARD}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='miro:item'",
            _TENANT_ID,
        )
        print(f"  observations: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        # 2 items -> 2 observations (both present -> signal).
        _check("backfill produced 2 observations (one per item)", counts["tot"] == 2)

        # 6. Item edit: a fresh version of i-1001 lands as a NEW observation.
        _hr("ITEM EDIT (version bump -> new observation)")
        from services.ingest.ingestion.core import ingest
        edited = {"_fyralis_record_type": "item", "_fyralis_org_id": _ORG_ID,
                  "_fyralis_board_id": _BOARD,
                  "item": fixtures[_BOARD]["delta"][0]}
        res = await ingest("miro:item", edited, pool=pool, tenant_id=_TENANT_ID)
        _check("edited item (version 2) lands as a fresh observation",
               res.deduped is False)

        # 7. Dedup: re-ingest a backfilled item twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        twin = {"_fyralis_record_type": "item", "_fyralis_org_id": _ORG_ID,
                "_fyralis_board_id": _BOARD,
                "item": fixtures[_BOARD]["items"][1]}
        res = await ingest("miro:item", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing item dedups (versioned external_id parity)",
               res.deduped is True)

        # 8. LIVE WEBHOOK path: a board_item.created with the SAME item flows
        #    through the SAME handler; its external_id matches the backfilled
        #    item, so it dedups (proves backfill+live parity).
        _hr("LIVE WEBHOOK (handler parity with backfill)")
        webhook_payload = {
            "event": "board_item.created",
            "_fyralis_org_id": _ORG_ID,
            "_fyralis_board_id": _BOARD,
            "item": fixtures[_BOARD]["items"][1],
        }
        res = await ingest("miro:item", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook item dedups against backfilled twin (external_id parity)",
               res.deduped is True)

        # A brand-new live item lands as a fresh observation.
        fresh_item = {
            "event": "board_item.created",
            "_fyralis_org_id": _ORG_ID, "_fyralis_board_id": _BOARD,
            "item": {"id": "i-live-1", "boardId": _BOARD, "type": "text",
                     "data": {"content": "live note"}, "version": "1",
                     "modifiedAt": "2026-06-09T00:00:00Z"},
        }
        res = await ingest("miro:item", fresh_item, pool=pool, tenant_id=_TENANT_ID)
        _check("new live item lands as a fresh observation", res.deduped is False)

        # 9. Reconciler gap probe against the live (mock) board.
        _hr("RECONCILER GAP PROBE (list_items)")
        items, _, _ = await client.list_items(_BOARD, limit=1, cursor=None)
        _check("reconciler probe lists the board items", len(items) >= 1)

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
        _check("all observations are authoritative miro:item",
               all(r["trust_tier"] == "authoritative" for r in rows)
               and all(r["external_id"].startswith(f"miro:{_ORG_ID}:item:") for r in rows))

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
    parser = argparse.ArgumentParser(description="Miro ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
