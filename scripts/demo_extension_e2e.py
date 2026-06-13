#!/usr/bin/env python
"""demo_extension_e2e.py — end-to-end "external extension, installed by a tenant".

Exercises the WHOLE ADR-0004 platform against a fresh, self-contained database,
treating github-intel exactly like an externally-developed extension an operator
installs for a tenant:

  0. Create a throwaway DB; apply the CORE schema, then the EXTENSION-OWNED schema
     via the new ``company_os.migrations`` seam (per-extension ledger).
  1. Create two tenants: A (will install github-intel) and B (will not).
  2. INSTALL github-intel for tenant A via the new lifecycle (grant capabilities +
     enable the feature flag) — the "a tenant needs to use it" step.
  3. ENFORCEMENT: inject a github webhook for tenant B (NOT installed) → the host
     capability gate skips the enricher → the raw signal is ingested with NO
     intelligence. Proof the gate is real and per-tenant.
  4. Index tenant A's code graph (code_intel), then inject a realistic webhook
     sequence through the REAL ingest path → the host gate now ALLOWS the
     enricher → each observation carries content["intelligence"].
  5. Run the ordered worker through the GENERIC host supervisor
     (``python -m lib.extensions.run_workers``, ONCE) → authoritative FSM state +
     the github_signal_enrichment system-of-record + a self-update reindex.
  6. Print the evidence + a pgAdmin runbook so the result is observable.

    DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os \\
      python scripts/demo_extension_e2e.py            # uses DB 'fyralis_ext_demo'
    DEMO_RESET=1 ...                                   # drop+recreate the demo DB
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg

import lib  # noqa: F401  (resolve the installed company-os location)

# Installed tenant (the github evidence lives here) + a control tenant.
TENANT_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
TENANT_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
REPO = "acme/intelligence-demo"
T0 = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)

ADMIN_DSN = os.environ.get(
    "DATABASE_URL", "postgresql://company_os:company_os@localhost:5434/company_os"
)
DEMO_DB = os.environ.get("DEMO_DB", "fyralis_ext_demo")


def _demo_dsn() -> str:
    base, _, _ = ADMIN_DSN.rpartition("/")
    return f"{base}/{DEMO_DB}"


def _core_migrations_dir() -> pathlib.Path:
    return pathlib.Path(lib.__file__).resolve().parents[1] / "db" / "migrations"


def hr(title: str) -> None:
    print("\n" + "=" * 78 + f"\n  {title}\n" + "=" * 78)


# --------------------------------------------------------------- fixture + events
FIXTURE = {
    "app/db.py": "def query(sql):\n    '''Primary DB query.'''\n    return []\n",
    "app/auth.py": (
        "from app.db import query\n\n"
        "def verify_token(token):\n"
        "    '''Verify a bearer token; return the session or None.'''\n"
        "    rows = query('select * from sessions where token = %s')\n"
        "    return rows[0] if rows else None\n"
    ),
    "app/api.py": (
        "from app.auth import verify_token\n"
        "from app.db import query\n\n"
        "def handle_request(req):\n"
        "    verify_token(req.token)\n"
        "    return query('select 1')\n"
    ),
    "app/main.py": "from app.api import handle_request\n\ndef main(req):\n    return handle_request(req)\n",
}


def _repo() -> dict:
    return {"full_name": REPO}


def webhook_sequence() -> list[tuple[str, dict]]:
    def ts(i: int) -> str:
        return (T0 + timedelta(minutes=i)).isoformat()

    return [
        ("issues", {"action": "opened", "repository": _repo(), "sender": {"login": "dana"},
                    "issue": {"number": 12, "title": "Token refresh fails on expiry",
                              "node_id": "I_12@opened", "created_at": ts(0), "updated_at": ts(0)}}),
        ("pull_request", {"action": "opened", "repository": _repo(), "sender": {"login": "alice"},
                          "pull_request": {"number": 42, "title": "Fix token refresh (fixes #12)",
                                           "node_id": "PR_42@opened", "merged": False,
                                           "base": {"ref": "main"},
                                           "head": {"ref": "feat/token-refresh", "sha": "a1a1a1a1"},
                                           "_changed_files": ["app/auth.py"],
                                           "created_at": ts(1), "updated_at": ts(1)}}),
        ("check_run", {"action": "completed", "repository": _repo(),
                       "sender": {"login": "github-actions"},
                       "check_run": {"name": "ci/tests", "status": "completed",
                                     "conclusion": "success", "head_sha": "a1a1a1a1",
                                     "node_id": "CR_1", "completed_at": ts(2)}}),
        ("pull_request", {"action": "closed", "repository": _repo(), "sender": {"login": "alice"},
                          "pull_request": {"number": 42, "title": "Fix token refresh (fixes #12)",
                                           "node_id": "PR_42@merged", "merged": True,
                                           "base": {"ref": "main"},
                                           "head": {"ref": "feat/token-refresh", "sha": "a1a1a1a1"},
                                           "merge_commit_sha": "m1m1m1m1",
                                           "_changed_files": ["app/auth.py"],
                                           "created_at": ts(1), "updated_at": ts(3)}}),
        ("issues", {"action": "closed", "repository": _repo(), "sender": {"login": "dana"},
                    "issue": {"number": 12, "title": "Token refresh fails on expiry",
                              "node_id": "I_12@closed", "created_at": ts(0), "updated_at": ts(4)}}),
        ("push", {"ref": "refs/heads/main", "after": "m1m1m1m1", "repository": _repo(),
                  "sender": {"login": "alice"},
                  "head_commit": {"timestamp": ts(5), "modified": ["app/auth.py"]},
                  "commits": [{"timestamp": ts(5), "modified": ["app/auth.py"]}]}),
        ("push", {"ref": "refs/heads/main", "after": "m2m2m2m2", "repository": _repo(),
                  "sender": {"login": "carol"},
                  "head_commit": {"timestamp": ts(6), "modified": ["app/db.py"]},
                  "commits": [{"timestamp": ts(6), "modified": ["app/db.py"]}]}),
    ]


def write_fixture() -> str:
    root = tempfile.mkdtemp(prefix="ext_e2e_")
    for rel, body in FIXTURE.items():
        p = os.path.join(root, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            f.write(body)
    return root


# --------------------------------------------------------------------- DB setup
async def _create_demo_db() -> None:
    admin = await asyncpg.connect(ADMIN_DSN)
    try:
        exists = await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", DEMO_DB)
        if exists and os.environ.get("DEMO_RESET"):
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname=$1 AND pid<>pg_backend_pid()", DEMO_DB)
            await admin.execute(f'DROP DATABASE "{DEMO_DB}"')
            exists = False
        if not exists:
            await admin.execute(f'CREATE DATABASE "{DEMO_DB}"')
            print(f"created database {DEMO_DB}")
        else:
            print(f"reusing database {DEMO_DB} (set DEMO_RESET=1 to recreate)")
    finally:
        await admin.close()


async def _apply_schema() -> None:
    from lib.shared.migrations import apply_migrations_dir
    from lib.extensions.migrations import apply_extension_migrations, discover_migration_dirs

    conn = await asyncpg.connect(_demo_dsn())
    try:
        core = await apply_migrations_dir(conn, _core_migrations_dir(), on_error="warn")
        print(f"core migrations applied: {len(core)} files")
        dirs = discover_migration_dirs()
        print(f"extension migration contributors: {[d[0] for d in dirs]}")
        ext = await apply_extension_migrations(conn, on_error="warn")
        for ext_id, applied in ext.items():
            print(f"  extension '{ext_id}' schema: {len(applied)} files "
                  f"(ledger schema_migrations_ext_{ext_id})")
    finally:
        await conn.close()


async def _ensure_tenants(pool) -> None:
    async with pool.acquire() as c:
        for tid, name in ((TENANT_A, "ext-demo-installed"), (TENANT_B, "ext-demo-control")):
            await c.execute(
                "INSERT INTO tenants (id, name) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING",
                tid, name)


# --------------------------------------------------------------------- main flow
async def main() -> None:
    hr("STEP 0 — Fresh database + core schema + EXTENSION-OWNED schema")
    await _create_demo_db()
    await _apply_schema()

    from services.app.gateway.db_bootstrap import create_gateway_pool
    from lib.shared.tenant_context import tenant_transaction
    from lib.extensions.manifest import reset_for_tests as reset_manifests
    from lib.extensions.registry import active_manifests
    from services.ingest.ingestion.handlers import get_handler
    from services.ingest.ingestion.core import ingest_from_draft
    from services.platform.extensions import lifecycle
    from code_intel.indexer import index_working_copy

    pool = await create_gateway_pool(_demo_dsn(), min_size=2, max_size=8)
    await _ensure_tenants(pool)

    hr("STEP 1-2 — Install github-intel for tenant A (grant + enable)")
    reset_manifests()
    manifest = next((m for m in active_manifests() if m.id == "github_intel"), None)
    if manifest is None:
        raise SystemExit("github_intel manifest not discovered — is the package installed?")
    result = await lifecycle.install(
        pool, tenant_id=TENANT_A, manifest=manifest,
        granted_by="ops@fyralis.demo",
        extra_flags={"code_intel.enabled": True},
    )
    print(f"installed: {json.dumps(result.__dict__, default=str)}")
    print(f"tenant A = {TENANT_A}   tenant B (control, NOT installed) = {TENANT_B}")

    handler = get_handler("github:webhook")

    hr("STEP 3 — Enforcement: same webhook for tenant B (NOT installed)")
    probe = {"action": "opened", "repository": _repo(), "sender": {"login": "erin"},
             "issue": {"number": 7, "title": "control probe", "node_id": "I_7@opened",
                       "created_at": T0.isoformat(), "updated_at": T0.isoformat()}}
    d_b = await handler(probe, {"X-GitHub-Event": "issues"})
    await ingest_from_draft(channel="github:webhook", draft=d_b, pool=pool,
                            tenant_id=TENANT_B, enqueue_trigger=False)
    print(f"tenant B observation written; content.intelligence present = "
          f"{'intelligence' in d_b.content}   <- host gate BLOCKED (not installed)")

    hr("STEP 4 — Index tenant A code graph + inject webhooks (gate ALLOWS)")
    root = write_fixture()
    stats = await index_working_copy(
        pool=pool, tenant_id=TENANT_A, repo_full_name=REPO, root_path=root,
        commit_sha="c0c0c0c0", branch="main")
    print(f"code graph: snapshot {str(stats.snapshot_id)[:8]} "
          f"files={stats.files} symbols={stats.symbols} edges={stats.edges}")
    enriched = 0
    for event, payload in webhook_sequence():
        draft = await handler(payload, {"X-GitHub-Event": event})
        await ingest_from_draft(channel="github:webhook", draft=draft, pool=pool,
                                tenant_id=TENANT_A, enqueue_trigger=False)
        if "intelligence" in draft.content:
            enriched += 1
    print(f"injected {len(webhook_sequence())} webhooks; inline-enriched = {enriched}")

    await pool.close()

    hr("STEP 5 — Run the ordered worker via the GENERIC host supervisor")
    env = dict(os.environ)
    env.update({
        "DATABASE_URL": _demo_dsn(),
        "EXTENSION_WORKERS_ONCE": "1",
        "GITHUB_INTEL_TENANTS": str(TENANT_A),
        "CODE_INTEL_REINDEX_ROOT": root,
        "INGESTION_HEALTH_PORT": "0",
    })
    proc = subprocess.run(
        [sys.executable, "-m", "lib.extensions.run_workers"],
        env=env, capture_output=True, text=True, timeout=180,
        cwd=str(pathlib.Path(lib.__file__).resolve().parents[1]),
    )
    print("supervisor stdout:\n" + (proc.stdout or "").strip())
    if proc.returncode != 0:
        print("supervisor stderr:\n" + (proc.stderr or "").strip())
        raise SystemExit(f"run_workers exited {proc.returncode}")
    print(f"run_workers exit={proc.returncode} (discovered + ran github_intel.worker once)")

    # ------------------------------------------------------------------ evidence
    pool = await create_gateway_pool(_demo_dsn(), min_size=1, max_size=4)
    hr("RESULT — Evidence the extension is live for tenant A")
    async with tenant_transaction(TENANT_A, pool=pool) as ctx:
        a_total = await ctx.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='github:webhook'", TENANT_A)
        a_enriched = await ctx.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='github:webhook' AND content ? 'intelligence'", TENANT_A)
        sor = await ctx.fetchval(
            "SELECT count(*) FROM github_signal_enrichment WHERE tenant_id=$1", TENANT_A)
        sor_changed = await ctx.fetchval(
            "SELECT count(*) FROM github_signal_enrichment WHERE tenant_id=$1 AND state_changed",
            TENANT_A)
        prs = await ctx.fetch(
            "SELECT pr_number, lifecycle, ci_state, merged FROM github_pr_state "
            "WHERE tenant_id=$1 ORDER BY pr_number", TENANT_A)
        snaps = await ctx.fetch(
            "SELECT left(commit_sha,8) sha, status, file_count, symbol_count, edge_count "
            "FROM code_snapshots WHERE tenant_id=$1 ORDER BY created_at", TENANT_A)
        grant = await ctx.fetchrow(
            "SELECT extension_id, granted_by, trust_ceiling, capabilities "
            "FROM extension_grants WHERE tenant_id=$1", TENANT_A)
    async with tenant_transaction(TENANT_B, pool=pool) as ctx:
        b_total = await ctx.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='github:webhook'", TENANT_B)
        b_enriched = await ctx.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND source_channel='github:webhook' AND content ? 'intelligence'", TENANT_B)

    print(f"extension_grants (A) : {grant['extension_id']} by {grant['granted_by']} "
          f"ceiling={grant['trust_ceiling']}")
    print(f"observations (A)     : {a_total} total, {a_enriched} inline-enriched")
    print(f"observations (B ctrl): {b_total} total, {b_enriched} enriched  "
          f"<- 0 enriched proves per-tenant enforcement")
    print(f"system-of-record (A) : {sor} enrichment rows ({sor_changed} state-changing)")
    print("PR FSM (A)           : " + ", ".join(
        f"#{p['pr_number']} {p['lifecycle']}/{p['ci_state']} merged={p['merged']}" for p in prs))
    print("code snapshots (A)   : " + ", ".join(
        f"{s['sha']}={s['status']}(f{s['file_count']}/s{s['symbol_count']}/e{s['edge_count']})"
        for s in snaps))
    await pool.close()

    ok = (a_enriched > 0 and b_enriched == 0 and sor > 0 and len(snaps) >= 1)
    hr("PGADMIN — observe it")
    print(f"DB: {DEMO_DB}   tenant A: {TENANT_A}")
    print("Inspection SQL: ops/pgadmin/inspect_github_intel.sql")
    print("Bring up pgAdmin: docker compose -f docker-compose.yml -f docker-compose.sandbox.yml "
          "-f docker-compose.pgadmin.yml up -d pgadmin   (http://localhost:5050)")
    print(f"\n{'PASS ✅' if ok else 'INCOMPLETE ⚠️'} — extension end-to-end "
          f"{'verified' if ok else 'did not fully populate'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
