#!/usr/bin/env python3
"""scripts/sandbox_google_calendar.py — local end-to-end sandbox for Google
Calendar ingestion (IN-15), with NO real Google credentials.

Google Calendar is poll-only (no webhooks/ngrok) and uses Domain-Wide
Delegation (service account JWT -> token exchange -> Calendar v3 REST). This
sandbox stands up a REAL local mock of the token + Calendar API endpoints and
drives the REAL pipeline against it:

    fake service-account (real RSA key, token_uri -> mock)
        -> get_minter() / GoogleHttpClient  (real DWD JWT mint -> mock /token)
        -> GoogleCalendarClient             (real httpx -> mock /calendars/.../events)
        -> fetch_page_google_calendar       (real cursor + syncToken logic)
        -> handle_google_calendar_event     (real ObservationDraft)
        -> ingest()                         (real observation insert + dedup)

It exercises backfill, the incremental syncToken delta (incl. a cancellation
-> state_change), cross-path dedup, and the reconciler gap probe — then prints
the observations that landed.

Database:
  - If DATABASE_URL is set, it is used as-is (migrations are applied
    idempotently). Use a disposable/sandbox DB, not production.
  - Otherwise a throwaway database is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_google_calendar.py
    python scripts/sandbox_google_calendar.py --keep
    DATABASE_URL=postgresql://.../my_sandbox python scripts/sandbox_google_calendar.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# This is a dev/test harness: it loads services/ingest/synthetic (which refuses to
# import under a prod env) and must not engage prod-only safety guards. Declare
# the env before any service import.
os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

import asyncpg


_DEFAULT_ADMIN_URL = "postgresql://company_os:company_os@localhost:5434/company_os"
_TENANT_ID = UUID("00000000-0000-0000-0000-0000000015ca")
_WORKSPACE = "acme.com"
_SA_EMAIL = "fyralis-gcal@fyralis-sandbox.iam.gserviceaccount.com"
_CALENDARS = ["alice@acme.com", "bob@acme.com"]


# ---------------------------------------------------------------------
# Pretty printing
# ---------------------------------------------------------------------
def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


# ---------------------------------------------------------------------
# Fake service account (real RSA key so the JWT is genuinely signed; the
# mock token endpoint accepts any well-formed assertion).
# ---------------------------------------------------------------------
def _write_fake_sa(token_url: str) -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    sa = {
        "type": "service_account",
        "project_id": "fyralis-sandbox",
        "private_key_id": "sandbox-k1",
        "private_key": pem,
        "client_email": _SA_EMAIL,
        "client_id": "100000000000000000001",
        "token_uri": token_url,  # route DWD exchange at the local mock
    }
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".sa.json", delete=False, prefix="gcal_sandbox_",
    )
    json.dump(sa, f)
    f.close()
    return f.name


# ---------------------------------------------------------------------
# Calendar fixtures (raw Calendar v3 event objects)
# ---------------------------------------------------------------------
def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _build_fixtures() -> dict:
    now = datetime.now(timezone.utc)

    def ev(eid, cal, summary, start, *, attendees=None, status="confirmed", updated=None):
        obj = {
            "kind": "calendar#event", "id": eid, "status": status,
            "summary": summary, "eventType": "default",
            "start": {"dateTime": _iso(start)},
            "end": {"dateTime": _iso(start + timedelta(minutes=30))},
            "organizer": {"email": cal},
            "creator": {"email": cal},
            "htmlLink": f"https://calendar.google.com/event?eid={eid}",
            "updated": _iso(updated or now),
        }
        if attendees is not None:
            obj["attendees"] = attendees
        return obj

    return {
        "alice@acme.com": {
            "events": [
                ev("a-standup", "alice@acme.com", "Eng standup",
                   now + timedelta(days=1, hours=1),
                   attendees=[{"email": "alice@acme.com", "responseStatus": "accepted"},
                              {"email": "bob@acme.com", "responseStatus": "accepted"}]),
                ev("a-investor", "alice@acme.com", "Investor sync — Series B",
                   now + timedelta(days=2),
                   attendees=[{"email": "alice@acme.com", "responseStatus": "accepted"},
                              {"email": "partner@sequoia.com", "responseStatus": "tentative"}]),
            ],
            "delta": [
                # incremental run surfaces a brand-new meeting ...
                ev("a-board", "alice@acme.com", "Board meeting",
                   now + timedelta(days=5),
                   attendees=[{"email": "alice@acme.com", "responseStatus": "accepted"},
                              {"email": "chair@board.org", "responseStatus": "needsAction"}],
                   updated=now + timedelta(minutes=10)),
                # ... and a cancellation (-> state_change).
                {"kind": "calendar#event", "id": "a-standup", "status": "cancelled",
                 "updated": _iso(now + timedelta(minutes=11))},
            ],
        },
        "bob@acme.com": {
            "events": [
                ev("b-1on1", "bob@acme.com", "1:1 with Alice",
                   now + timedelta(days=1, hours=3),
                   attendees=[{"email": "bob@acme.com", "responseStatus": "accepted"},
                              {"email": "alice@acme.com", "responseStatus": "accepted"}]),
            ],
            "delta": [],
        },
    }


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------
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
        # Terminate stragglers, then drop.
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()", name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        await admin.close()


async def _apply_migrations(pool: asyncpg.Pool) -> None:
    from lib.shared.migrations import apply_migrations_dir
    async with pool.acquire() as conn:
        await apply_migrations_dir(conn, _REPO_ROOT / "db" / "migrations")


async def _seed_tenant(pool: asyncpg.Pool) -> None:
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'gcal-sandbox') "
        "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
    )


# ---------------------------------------------------------------------
# Pipeline drive
# ---------------------------------------------------------------------
async def _drain_shard_into_observations(pool, install_row, shard_identifier) -> list[str]:
    """Run the REAL fetcher loop for one shard, ingesting each record.
    Returns (external_ids ingested, last cursor next_sync_token)."""
    from services.ingest.ingestion.core import ingest
    from services.ingest.ingestion.fetchers.google_calendar import fetch_page_google_calendar

    ingested: list[str] = []
    cursor, next_sync_token, guard = None, None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_google_calendar(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest(
                "google_calendar:event", record,
                pool=pool, tenant_id=_TENANT_ID, enqueue_trigger=True,
            )
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if cursor:
            next_sync_token = cursor.get("next_sync_token") or next_sync_token
        if result.end_of_data:
            break
    return ingested, next_sync_token


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.google_calendar import start_mock_calendar

    fixtures = _build_fixtures()

    # 1. Start the mock (token + Calendar API) on a random local port.
    server, base_url, token_url = start_mock_calendar(fixtures)
    _hr("MOCK SERVER")
    print(f"  Calendar API base : {base_url}")
    print(f"  Token endpoint    : {token_url}")

    # 2. Fake service account, env wiring, fresh DWD minter.
    sa_path = _write_fake_sa(token_url)
    os.environ["GMAIL_SERVICE_ACCOUNT_JSON_FILE"] = sa_path
    os.environ["GMAIL_SERVICE_ACCOUNT_CLIENT_ID"] = "100000000000000000001"
    os.environ["GOOGLE_CALENDAR_API_BASE_URL"] = base_url
    from services.ingest.integrations.gmail import dwd as _dwd
    _dwd._reset_minter_for_tests()
    print(f"  Service account   : {sa_path} (impersonation via DWD)")

    # 3. Resolve / create the database.
    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE")
        print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"gcal_sandbox_{uuid4().hex[:8]}"
        await _create_throwaway_db(admin_url, created_db)
        db_url = admin_url.rsplit("/", 1)[0] + "/" + created_db
        _hr("DATABASE")
        print(f"  Created throwaway DB: {created_db}")

    from services.app.gateway.db_bootstrap import _register_codecs
    pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5, init=_register_codecs)
    try:
        await _apply_migrations(pool)
        from services.domain.observations.partitions import ensure_partitions
        await ensure_partitions(pool, months_ahead=3)
        await _seed_tenant(pool)
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 4. Provision the install (writes install + calendars + onboarding trigger).
        _hr("PROVISION (onboarding.finalize_install)")
        from services.ingest.integrations.google_calendar.onboarding import finalize_install
        install_id = await finalize_install(
            pool,
            tenant_id=_TENANT_ID,
            workspace_domain=_WORKSPACE,
            service_account_email=_SA_EMAIL,
            calendar_emails=_CALENDARS,
            inclusion_spec={"users": _CALENDARS},
        )
        cal_count = await pool.fetchval(
            "SELECT count(*) FROM google_calendar_calendars "
            "WHERE google_calendar_installation_id = $1", install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source, trigger_kind FROM onboarding_triggers WHERE tenant_id=$1",
            _TENANT_ID,
        )
        print(f"  install_id={install_id}  calendars={cal_count}")
        _check("install + 2 calendars provisioned", cal_count == 2)
        _check("onboarding trigger emitted (source=google_calendar)",
               trig is not None and trig["source"] == "google_calendar")

        # 5. Plan shards exactly as SourceOnboarding does (loader SQL -> planner).
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.google_calendar import plan_shards_google_calendar
        from services.ingest.ingestion.workflows.source_onboarding import _LOAD_GCAL_INSTALL_SQL
        install_row = await pool.fetchrow(_LOAD_GCAL_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_google_calendar(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["calendar_id"] for s in shards))
        _check("one shard per calendar", len(shards) == 2)

        # 6. Backfill: real fetcher -> real ingest, per shard.
        _hr("BACKFILL (fetcher -> ingest)")
        sync_tokens: dict[str, str | None] = {}
        for shard in shards:
            cal = shard.shard_identifier["calendar_id"]
            ext_ids, tok = await _drain_shard_into_observations(
                pool, install_row, shard.shard_identifier,
            )
            sync_tokens[cal] = tok
            print(f"  {cal}: ingested {len(ext_ids)} -> {ext_ids}  (nextSyncToken={tok})")
        backfilled = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 AND source_channel='google_calendar:event'",
            _TENANT_ID,
        )
        _check("backfill produced 3 observations (2 alice + 1 bob)", backfilled == 3)
        _check("nextSyncToken captured for incremental warm-start",
               sync_tokens.get("alice@acme.com") == "sync-1")

        # 7. Incremental: warm-start alice's calendar from the captured syncToken.
        _hr("INCREMENTAL (syncToken delta: new event + cancellation)")
        incr_shard = {
            "shard_kind": "google_calendar_events",
            "calendar_id": "alice@acme.com",
            "owner_email": "alice@acme.com",
            "installation_id": str(install_id),
            "sync_token": sync_tokens["alice@acme.com"],
        }
        incr_ids, _ = await _drain_shard_into_observations(pool, install_row, incr_shard)
        print(f"  incremental ingested: {incr_ids}")
        new_board = await pool.fetchrow(
            "SELECT kind FROM observations WHERE tenant_id=$1 AND content->>'event_id'=$2",
            _TENANT_ID, "a-board",
        )
        # a-standup now has TWO observations: the confirmed (backfill, signal)
        # and the cancellation (incremental, state_change) — the versioned
        # external_id keeps them distinct.
        cancelled = await pool.fetchrow(
            "SELECT kind FROM observations WHERE tenant_id=$1 "
            "AND content->>'event_id'=$2 AND kind='state_change'",
            _TENANT_ID, "a-standup",
        )
        standup_versions = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 AND content->>'event_id'=$2",
            _TENANT_ID, "a-standup",
        )
        _check("new event from delta ingested as signal",
               new_board is not None and new_board["kind"] == "signal")
        _check("cancellation lands as a distinct state_change observation",
               cancelled is not None and cancelled["kind"] == "state_change")
        _check("a-standup has both confirmed + cancelled observations (versioned id)",
               standup_versions == 2)

        # 8. Dedup: re-ingest a backfilled event (poll twin) -> deduped, no new row.
        _hr("DEDUP (backfill vs poll twin)")
        from services.ingest.ingestion.core import ingest
        twin = dict(fixtures["bob@acme.com"]["events"][0])
        twin["_fyralis_calendar_id"] = "bob@acme.com"
        twin["_fyralis_owner_email"] = "bob@acme.com"
        res = await ingest("google_calendar:event", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing event dedups (external_id parity)", res.deduped is True)

        # 9. Reconciler gap probe against the live (mock) calendar.
        _hr("RECONCILER GAP PROBE (has_updates_since)")
        from services.ingest.integrations.gmail.client import GoogleHttpClient
        from services.ingest.integrations.gmail.dwd import get_minter
        from services.ingest.integrations.google_calendar.client import GoogleCalendarClient
        http = GoogleHttpClient(get_minter())
        await http.__aenter__()
        try:
            client = GoogleCalendarClient(http)
            old_bound = _iso(datetime.now(timezone.utc) - timedelta(days=365))
            has_updates = await client.has_updates_since(
                calendar_id="alice@acme.com", user_email="alice@acme.com",
                updated_min=old_bound,
            )
        finally:
            await http.__aexit__(None, None, None)
        _check("reconciler probe detects updates since an old high-water", has_updates is True)

        # 10. Inspect: dump the observations that landed.
        _hr("OBSERVATIONS")
        rows = await pool.fetch(
            "SELECT source_channel, kind, trust_tier, external_id, content_text, occurred_at "
            "FROM observations WHERE tenant_id=$1 ORDER BY occurred_at",
            _TENANT_ID,
        )
        for r in rows:
            print(f"  [{r['kind']:<12} {r['trust_tier']:<13}] {r['external_id']}")
            print(f"       {r['content_text']}")
        print(f"\n  total observations: {len(rows)}")
        # 3 backfill + 2 from the alice delta (new board signal + standup
        # cancellation state_change).
        _check("final observation count is 5 (3 backfill + 2 from delta)", len(rows) == 5)
        _check("all observations are authoritative google_calendar:event",
               all(r["source_channel"] == "google_calendar:event"
                   and r["trust_tier"] == "authoritative" for r in rows))

        # Mock server actually served the traffic (no accidental real calls).
        _check("mock token endpoint was hit", server.request_hits.get("token", 0) > 0)

    finally:
        await pool.close()
        server.shutdown()
        try:
            os.unlink(sa_path)
        except OSError:
            pass
        if created_db and not args.keep:
            await _drop_throwaway_db(admin_url, created_db)
            print(f"\n  Dropped throwaway DB {created_db}.")
        elif created_db:
            print(f"\n  Kept throwaway DB {created_db} (DATABASE_URL="
                  f"{admin_url.rsplit('/', 1)[0]}/{created_db}).")

    # Summary.
    _hr("SUMMARY")
    passed = sum(1 for _, ok in _checks if ok)
    for label, ok in _checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    print(f"\n  {passed}/{len(_checks)} checks passed.")
    return 0 if passed == len(_checks) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Google Calendar ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
