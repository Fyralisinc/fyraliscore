#!/usr/bin/env python3
"""scripts/sandbox_jira.py — local end-to-end sandbox for Jira ingestion
(IN-17), with NO real Atlassian credentials.

Jira Cloud is a REST API (Basic auth: account_email:api_token) with BOTH a
historical query surface (JQL search) and a live push surface (webhooks). This
sandbox stands up a REAL local mock of the Jira v3 endpoints and drives the
REAL pipeline against it:

    JiraClient (real httpx, spammer auth) -> fetch_page_jira (real cursor + JQL
    + fan-out) -> handle_jira_issue (real ObservationDraft) -> ingest() (real
    observation insert + dedup)

It exercises: project enumeration, per-project backfill with the issue ->
issue/transition/comment fan-out, the incremental `updated >=` delta (a STATUS
transition -> state_change), the live-webhook path through the SAME handler
(asserting external_id parity / dedup with backfill), cross-path dedup, and the
reconciler gap probe — then prints the observations that landed.

This is the dry-run that proves the integration end-to-end BEFORE real creds.
When you supply real Jira creds, the same flow runs against your live site (see
docs/ingestion/jira-sandbox.md).

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_jira.py
    python scripts/sandbox_jira.py --keep
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
_TENANT_ID = UUID("00000000-0000-0000-0000-000000001717")
_BASE_URL = "https://acme.atlassian.net"
_SITE = "acme.atlassian.net"
_EMAIL = "sandbox@acme.example"
_PROJECT = "ENG"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def _build_fixtures() -> dict:
    now = datetime.now(timezone.utc)

    def issue(iid, key, summary, status, updated, *, with_changelog=False,
              from_status=None, with_comment=False, hist_id=None):
        fields = {
            "summary": summary,
            "issuetype": {"name": "Story"},
            "status": {"name": status},
            "priority": {"name": "High"},
            "assignee": {"accountId": "u-bob", "emailAddress": "bob@acme.example", "displayName": "Bob"},
            "reporter": {"accountId": "u-alice", "emailAddress": "alice@acme.example", "displayName": "Alice"},
            "project": {"key": _PROJECT},
            "labels": ["backend"],
            "created": _iso(now - timedelta(days=10)),
            "updated": updated,
            "customfield_10016": 3,
        }
        obj = {
            "id": iid, "key": key,
            "self": f"{_BASE_URL}/rest/api/2/issue/{iid}",
            "fields": fields,
        }
        if with_comment:
            fields["comment"] = {"comments": [{
                "id": f"cm-{iid}", "created": updated, "updated": updated,
                "author": {"emailAddress": "carol@acme.example", "displayName": "Carol"},
                "body": "Looks ready for review.",
            }]}
        if with_changelog:
            obj["changelog"] = {"histories": [{
                "id": hist_id or f"hist-{iid}", "created": updated,
                "author": {"emailAddress": "bob@acme.example", "displayName": "Bob"},
                "items": [{"field": "status", "fromString": from_status or "To Do",
                           "toString": status}],
            }]}
        return obj

    return {
        _PROJECT: {
            "issues": [
                issue("10001", "ENG-1", "Cold-start 500 on Atlas API", "In Progress",
                      _iso(now - timedelta(days=3)),
                      with_changelog=True, from_status="To Do", with_comment=True),
                issue("10002", "ENG-2", "Add retry to Helios poller", "Done",
                      _iso(now - timedelta(days=2)), with_changelog=True,
                      from_status="In Progress"),
            ],
            # Incremental delta: ENG-1 moves In Progress -> Done (a fresh
            # status transition the poll/reconcile surfaces).
            "delta": [
                issue("10001", "ENG-1", "Cold-start 500 on Atlas API", "Done",
                      _iso(now - timedelta(hours=1)),
                      with_changelog=True, from_status="In Progress",
                      hist_id="hist-10001-done"),
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
    """Run the REAL fetcher loop for one shard, ingesting each fanned-out
    record. Returns the external_ids of NON-deduped observations."""
    from services.ingest.ingestion.core import ingest
    from services.ingest.ingestion.fetchers.jira import fetch_page_jira

    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_jira(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest("jira:issue", record, pool=pool, tenant_id=_TENANT_ID)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


async def run(args) -> int:
    from services.ingest.synthetic.mock_servers.jira import start_mock_jira

    fixtures = _build_fixtures()

    # 1. Start the mock; route the spammer single-host base at it (the client
    #    resolves jira_api -> <base>/jira, and the mock matches path suffix).
    server, base_url = start_mock_jira(fixtures)
    os.environ["SYNTHETIC_SOURCE_API_BASE"] = base_url
    _hr("MOCK SERVER")
    print(f"  Jira API base : {base_url} (served under /jira via spammer routing)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE"); print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"jira_sandbox_{uuid4().hex[:8]}"
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
            "INSERT INTO tenants (id, name) VALUES ($1, 'jira-sandbox') "
            "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
        )
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # 2. Enumerate projects via the REAL client (proves list_projects).
        _hr("ENUMERATE PROJECTS (JiraClient.list_projects)")
        from services.ingest.ingestion.fetchers._clients import build_jira_client

        class _Inst:
            _d = {"id": uuid4(), "tenant_id": _TENANT_ID,
                  "base_url": _BASE_URL, "account_email": _EMAIL, "secret_ref": None}

            def __getitem__(self, k): return self._d[k]
            def __contains__(self, k): return k in self._d

        client = await build_jira_client(_Inst())
        projects, _, total = await client.list_projects()
        project_keys = [p["key"] for p in projects]
        print(f"  projects discovered: {project_keys} (total={total})")
        _check("project enumeration returned ENG", "ENG" in project_keys)

        # 3. Provision the install (jira_installations + jira_projects + trigger)
        #    AND the webhook provider_installations row (live path).
        _hr("PROVISION (jira.onboarding.finalize_install)")
        from services.ingest.integrations.jira.onboarding import (
            finalize_install, register_webhook_installation,
        )
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL, account_email=_EMAIL,
            project_keys=project_keys, cloud_id="cloud-sandbox",
        )
        await register_webhook_installation(
            pool, tenant_id=_TENANT_ID, base_url=_BASE_URL, webhook_secret_ref=None,
        )
        proj_count = await pool.fetchval(
            "SELECT count(*) FROM jira_projects WHERE jira_installation_id=$1", install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        _check("install + project rows provisioned", proj_count == len(project_keys))
        _check("onboarding trigger emitted (source=jira)",
               trig is not None and trig["source"] == "jira")

        # 4. Plan shards exactly as SourceOnboarding does.
        _hr("PLAN (planner over the loader SQL)")
        from services.ingest.ingestion.planners.context import PlannerContext
        from services.ingest.ingestion.planners.jira import plan_shards_jira
        from services.ingest.ingestion.workflows.source_onboarding import _LOAD_JIRA_INSTALL_SQL
        install_row = await pool.fetchrow(_LOAD_JIRA_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_jira(ctx)
        print(f"  planned {len(shards)} shard(s): "
              + ", ".join(s.shard_identifier["project_key"] for s in shards))
        _check("one shard per project", len(shards) == len(project_keys))

        # 5. Backfill: real fetcher -> real ingest.
        _hr("BACKFILL (fetcher fan-out -> ingest)")
        for shard in shards:
            ext = await _drain_shard(pool, install_row, shard.shard_identifier)
            print(f"  {shard.shard_identifier['project_key']}: ingested {len(ext)} observations")
        counts = await pool.fetchrow(
            "SELECT count(*) FILTER (WHERE kind='signal') AS sig, "
            "count(*) FILTER (WHERE kind='state_change') AS sc, count(*) AS tot "
            "FROM observations WHERE tenant_id=$1 AND source_channel='jira:issue'", _TENANT_ID,
        )
        print(f"  observations so far: total={counts['tot']} signal={counts['sig']} state_change={counts['sc']}")
        # 2 issues + 2 transitions (both status -> state_change) + 1 comment = 5.
        _check("backfill produced 5 observations (2 issue + 2 transition + 1 comment)",
               counts["tot"] == 5)
        _check("status transitions landed as state_change", counts["sc"] == 2)

        # 6. Incremental: warm-start ENG from the high-water -> delta (a fresh
        #    status transition -> a NEW state_change observation).
        _hr("INCREMENTAL (updated >= delta: status transition)")
        hw = await pool.fetchval(
            "SELECT max(content->>'updated') FROM observations "
            "WHERE tenant_id=$1 AND content->>'object_type'='issue'", _TENANT_ID,
        )
        incr_shard = {"shard_kind": "jira_project_issues", "project_key": _PROJECT,
                      "updated_cursor": hw}
        incr = await _drain_shard(pool, install_row, incr_shard)
        print(f"  incremental ingested {len(incr)} new observations: {incr}")
        sc_after = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 AND kind='state_change'",
            _TENANT_ID,
        )
        _check("incremental delta surfaced a new state_change", sc_after == 3)

        # 7. Dedup: re-ingest a backfilled issue twin -> deduped.
        _hr("DEDUP (backfill vs re-fetch twin)")
        from services.ingest.ingestion.core import ingest
        twin = dict(fixtures[_PROJECT]["issues"][0])
        twin["_fyralis_record_type"] = "issue"
        twin["_fyralis_site"] = _SITE
        res = await ingest("jira:issue", twin, pool=pool, tenant_id=_TENANT_ID)
        _check("re-ingesting an existing issue dedups (versioned external_id parity)",
               res.deduped is True)

        # 8. LIVE WEBHOOK path: a jira:issue_updated with a status change flows
        #    through the SAME handler; its transition external_id matches the
        #    backfilled transition, so it dedups (proves backfill+live parity).
        _hr("LIVE WEBHOOK (handler parity with backfill)")
        webhook_payload = {
            "webhookEvent": "jira:issue_updated",
            "user": {"emailAddress": "bob@acme.example", "displayName": "Bob"},
            "issue": {"id": "10001", "key": "ENG-1",
                      "self": f"{_BASE_URL}/rest/api/2/issue/10001",
                      "fields": {"updated": _iso(datetime.now(timezone.utc) - timedelta(hours=1))}},
            "changelog": {"id": "hist-10001",
                          "items": [{"field": "status", "fromString": "In Progress", "toString": "Done"}]},
        }
        res = await ingest("jira:issue", webhook_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("live webhook transition dedups against backfilled transition "
               "(external_id parity)", res.deduped is True)

        # A brand-new live comment lands as a fresh observation.
        comment_payload = {
            "webhookEvent": "comment_created",
            "issue": {"id": "10002", "key": "ENG-2", "self": f"{_BASE_URL}/x/10002"},
            "comment": {"id": "live-1", "updated": _iso(datetime.now(timezone.utc)),
                        "author": {"emailAddress": "dave@acme.example"}, "body": "shipping now"},
        }
        res = await ingest("jira:issue", comment_payload, pool=pool, tenant_id=_TENANT_ID)
        _check("new live comment lands as a fresh observation", res.deduped is False)

        # 9. Reconciler gap probe against the live (mock) project.
        _hr("RECONCILER GAP PROBE (has_updates_since)")
        old_floor = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y/%m/%d %H:%M")
        has_updates = await client.has_updates_since(project_key=_PROJECT, updated_min_jql=old_floor)
        _check("reconciler probe detects updates since an old high-water", has_updates is True)

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
        _check("all observations are authoritative jira:issue",
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
    parser = argparse.ArgumentParser(description="Jira ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="keep the throwaway database on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
