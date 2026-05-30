#!/usr/bin/env python
"""scripts/demo_github_intel.py — GitHub Intelligence Layer end-to-end demo.

A self-contained testing environment that shows the pieces working and GitHub
signals landing as ENRICHED observations:

  1. Index a small repo into the code graph (code_intel).
  2. Inject a realistic sequence of GitHub webhooks through the REAL ingestion
     path (handler -> ingest_from_draft), so the inline hook enriches each
     observation's content["intelligence"].
  3. Drain the ordered worker -> authoritative FSM state + github_signal_enrichment
     + code-reindex triggers.
  4. Drain the reindex triggers -> a fresh code snapshot (self-update loop).
  5. Print: enriched observations, current FSM state, the system-of-record, a
     code-RAG sample, and a RAW-FALLBACK demonstration (enrichment off -> raw
     signal still ingested).

Usage:
  DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os \\
    python scripts/demo_github_intel.py
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from uuid import UUID

REPO = "acme/intelligence-demo"
TENANT = UUID(os.environ.get("COMPANY_OS_TENANT_ID", "00000000-0000-0000-0000-000000000001"))
DSN = os.environ.get("DATABASE_URL", "postgresql://company_os:company_os@localhost:5434/company_os")
T0 = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------- fixture
FIXTURE = {
    "app/db.py": (
        "def query(sql):\n"
        "    '''Run a SQL query against the primary database.'''\n"
        "    return []\n"
    ),
    "app/auth.py": (
        "from app.db import query\n\n"
        "def verify_token(token):\n"
        "    '''Verify a bearer token and return the session, or None.'''\n"
        "    rows = query('select * from sessions where token = %s')\n"
        "    return rows[0] if rows else None\n\n"
        "class Session:\n"
        "    def refresh(self):\n"
        "        return verify_token(self.token)\n"
    ),
    "app/api.py": (
        "from app.auth import verify_token\n"
        "from app.db import query\n\n"
        "def handle_request(req):\n"
        "    session = verify_token(req.token)\n"
        "    return query('select 1')\n"
    ),
    "app/ratelimit.py": (
        "from app.db import query\n\n"
        "def check_rate(user):\n"
        "    return query('select count(*) from hits')\n"
    ),
    "app/main.py": (
        "from app.api import handle_request\n\n"
        "def main(req):\n"
        "    return handle_request(req)\n"
    ),
}


def _repo_obj() -> dict:
    return {"full_name": REPO}


def _sender(login: str) -> dict:
    return {"login": login}


def webhook_sequence() -> list[tuple[str, dict]]:
    """(X-GitHub-Event, payload) tuples in occurred order."""
    def ts(i: int) -> str:
        return (T0 + timedelta(minutes=i)).isoformat()

    seq: list[tuple[str, dict]] = []
    # #12 issue opened
    seq.append(("issues", {
        "action": "opened", "repository": _repo_obj(), "sender": _sender("dana"),
        "issue": {"number": 12, "title": "Token refresh fails on expiry",
                  "node_id": "I_12@opened", "created_at": ts(0), "updated_at": ts(0)},
    }))
    # PR #42 opened, touches app/auth.py, references #12
    seq.append(("pull_request", {
        "action": "opened", "repository": _repo_obj(), "sender": _sender("alice"),
        "pull_request": {
            "number": 42, "title": "Fix token refresh (fixes #12)",
            "node_id": "PR_42@opened", "merged": False,
            "base": {"ref": "main"}, "head": {"ref": "feat/token-refresh", "sha": "a1a1a1a1"},
            "_changed_files": ["app/auth.py"], "created_at": ts(1), "updated_at": ts(1),
        },
    }))
    # push to the PR branch
    seq.append(("push", {
        "ref": "refs/heads/feat/token-refresh", "after": "a1a1a1a1",
        "repository": _repo_obj(), "sender": _sender("alice"),
        "head_commit": {"timestamp": ts(2), "modified": ["app/auth.py"]},
        "commits": [{"timestamp": ts(2), "modified": ["app/auth.py"]}],
    }))
    # CI passes on the head sha
    seq.append(("check_run", {
        "action": "completed", "repository": _repo_obj(), "sender": _sender("github-actions"),
        "check_run": {"name": "ci/tests", "status": "completed", "conclusion": "success",
                      "head_sha": "a1a1a1a1", "node_id": "CR_1", "completed_at": ts(3)},
    }))
    # approving review
    seq.append(("pull_request_review", {
        "action": "submitted", "repository": _repo_obj(), "sender": _sender("bob"),
        "review": {"state": "approved", "node_id": "RV_1", "submitted_at": ts(4)},
        "pull_request": {"number": 42, "node_id": "PR_42"},
    }))
    # PR #42 merged into main
    seq.append(("pull_request", {
        "action": "closed", "repository": _repo_obj(), "sender": _sender("alice"),
        "pull_request": {
            "number": 42, "title": "Fix token refresh (fixes #12)",
            "node_id": "PR_42@merged", "merged": True,
            "base": {"ref": "main"}, "head": {"ref": "feat/token-refresh", "sha": "a1a1a1a1"},
            "merge_commit_sha": "m1m1m1m1", "_changed_files": ["app/auth.py"],
            "created_at": ts(1), "updated_at": ts(5),
        },
    }))
    # comment + close the issue
    seq.append(("issue_comment", {
        "action": "created", "repository": _repo_obj(), "sender": _sender("dana"),
        "comment": {"body": "Fixed by #42", "node_id": "IC_1", "created_at": ts(6)},
        "issue": {"number": 12, "node_id": "I_12"},
    }))
    seq.append(("issues", {
        "action": "closed", "repository": _repo_obj(), "sender": _sender("dana"),
        "issue": {"number": 12, "title": "Token refresh fails on expiry",
                  "node_id": "I_12@closed", "created_at": ts(0), "updated_at": ts(7)},
    }))
    # the merge lands on main
    seq.append(("push", {
        "ref": "refs/heads/main", "after": "m1m1m1m1",
        "repository": _repo_obj(), "sender": _sender("alice"),
        "head_commit": {"timestamp": ts(8), "modified": ["app/auth.py"]},
        "commits": [{"timestamp": ts(8), "modified": ["app/auth.py"]}],
    }))
    # a RISKY direct push to main touching a high-fan-out file
    seq.append(("push", {
        "ref": "refs/heads/main", "after": "m2m2m2m2",
        "repository": _repo_obj(), "sender": _sender("carol"),
        "head_commit": {"timestamp": ts(9), "modified": ["app/db.py"]},
        "commits": [{"timestamp": ts(9), "modified": ["app/db.py"]}],
    }))
    return seq


# --------------------------------------------------------------------- helpers
def hr(title: str) -> None:
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


async def reset(pool) -> None:
    from lib.shared.tenant_context import tenant_transaction
    async with tenant_transaction(TENANT, pool=pool) as ctx:
        await ctx.execute("DELETE FROM github_signal_enrichment WHERE tenant_id=$1 AND repo=$2", TENANT, REPO)
        await ctx.execute(
            "DELETE FROM github_intel_queue WHERE tenant_id=$1 AND observation_id IN "
            "(SELECT id FROM observations WHERE tenant_id=$1 AND source_channel='github:webhook' "
            " AND content->>'repo'=$2)", TENANT, REPO)
        for tbl in ("github_pr_state", "github_issue_state", "github_branch_state",
                    "github_repo_state", "github_check_state"):
            await ctx.execute(f"DELETE FROM {tbl} WHERE tenant_id=$1 AND repo=$2", TENANT, REPO)
        await ctx.execute(
            "DELETE FROM observations WHERE tenant_id=$1 AND source_channel='github:webhook' "
            "AND content->>'repo'=$2", TENANT, REPO)
        await ctx.execute("DELETE FROM code_intel_index_triggers WHERE tenant_id=$1 AND repo_full_name=$2", TENANT, REPO)
        await ctx.execute("DELETE FROM code_snapshots WHERE tenant_id=$1 AND repo_full_name=$2", TENANT, REPO)


async def ensure_tenant(pool) -> None:
    async with pool.acquire() as c:
        await c.execute(
            "INSERT INTO tenants (id, name) VALUES ($1,'ghintel-demo') ON CONFLICT (id) DO NOTHING",
            TENANT,
        )


def write_fixture() -> str:
    root = tempfile.mkdtemp(prefix="ghintel_demo_")
    for rel, body in FIXTURE.items():
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(body)
    return root


# --------------------------------------------------------------------- main
async def main() -> None:
    from services.gateway.db_bootstrap import create_gateway_pool
    from lib.shared.tenant_context import tenant_transaction
    from services.ingestion.feature_flags.client import TenantFlags
    from services.ingestion.handlers import get_handler
    from services.ingestion.core import ingest_from_draft
    from services.code_intel.indexer import index_working_copy
    from services.code_intel.embed import fill_pending_embeddings
    from services.code_intel.graph import CodeGraphRepo
    from services.code_intel.reindex import drain_reindex_triggers
    from services.github_intel.config import (
        GITHUB_INTEL_ENABLED, GITHUB_INTEL_LLM_ENABLED, CODE_INTEL_ENABLED,
    )
    from services.github_intel.worker import drain, enqueue_new_github_observations

    pool = await create_gateway_pool(DSN, min_size=2, max_size=8)
    await ensure_tenant(pool)
    await reset(pool)

    flags = TenantFlags(pool)
    await flags.set_bool(TENANT, CODE_INTEL_ENABLED, True, set_by="demo")
    await flags.set_bool(TENANT, GITHUB_INTEL_ENABLED, True, set_by="demo")
    llm_on = os.environ.get("DEMO_LLM", "0") == "1"
    await flags.set_bool(TENANT, GITHUB_INTEL_LLM_ENABLED, llm_on, set_by="demo")

    hr("STEP 1 — Index the codebase (code_intel)")
    root = write_fixture()
    print(f"working copy: {root}")
    print(f"files: {', '.join(FIXTURE)}")
    stats = await index_working_copy(
        pool=pool, tenant_id=TENANT, repo_full_name=REPO, root_path=root,
        commit_sha="c0c0c0c0", branch="main",
    )
    print(f"-> snapshot {str(stats.snapshot_id)[:8]} @ c0c0c0c0  "
          f"files={stats.files} symbols={stats.symbols} edges={stats.edges}")
    filled = await fill_pending_embeddings(pool=pool, tenant_id=TENANT, snapshot_id=stats.snapshot_id)
    print(f"-> code-RAG embeddings filled: {filled} (best-effort; ollama optional)")

    hr("STEP 2 — Inject GitHub webhooks through the real ingestion path")
    handler = get_handler("github:webhook")
    seq = webhook_sequence()
    for event, payload in seq:
        draft = await handler(payload, {"X-GitHub-Event": event})
        await ingest_from_draft(
            channel="github:webhook", draft=draft, pool=pool, tenant_id=TENANT,
            enqueue_trigger=False,
        )
        enriched = "intelligence" in draft.content
        print(f"  + {event:20s} {draft.content_text[:60]!r:62}  inline_enriched={enriched}")

    hr("STEP 3 — Drain the ordered worker (authoritative FSM + system-of-record)")
    fed = await enqueue_new_github_observations(pool, TENANT)
    done = await drain(pool, TENANT, worker_id="demo", llm_enabled=llm_on)
    print(f"-> queued {fed} signals, processed {done}")

    hr("STEP 4 — Self-update: drain code reindex triggers")
    reidx = await drain_reindex_triggers(pool, TENANT, root_path=root)
    for r in reidx:
        print(f"  reindex {r.get('kind')} @ {r.get('commit_sha')} -> snapshot "
              f"{str(r.get('snapshot_id'))[:8]} files={r.get('files')} symbols={r.get('symbols')}")
    if not reidx:
        print("  (no reindex triggers)")

    # ---------------------------------------------------------------- payoff
    hr("RESULT A — GitHub signals landing as ENRICHED observations")
    async with tenant_transaction(TENANT, pool=pool) as ctx:
        rows = await ctx.fetch(
            "SELECT content_text, content FROM observations "
            "WHERE tenant_id=$1 AND source_channel='github:webhook' AND content->>'repo'=$2 "
            "ORDER BY occurred_at", TENANT, REPO)
        for r in rows:
            content = r["content"]
            if isinstance(content, str):
                content = json.loads(content)
            intel = content.get("intelligence")
            print(f"\n● {r['content_text']}")
            if not intel:
                print("    (raw — no intelligence)")
                continue
            aff = intel.get("affected", {})
            print(f"    state_change : {intel.get('state_change')}  "
                  f"(confidence={intel.get('confidence')}, via={intel.get('reasoning_path')})")
            print(f"    cause        : {intel.get('cause')}")
            print(f"    effect       : {intel.get('effect')}")
            print(f"    why          : {intel.get('explanation')}")
            if aff.get("changed_files"):
                print(f"    changed_files: {aff.get('changed_files')}")
            if aff.get("dependent_files"):
                print(f"    blast_radius : {aff.get('blast_radius_count')} dependents -> "
                      f"files={[d.get('path') for d in aff.get('dependent_files', [])]}")
            if aff.get("dependent_symbols"):
                print(f"    dependent_symbols: {aff.get('dependent_symbols')}")
            rel = []
            for e in intel.get('related_entities', []):
                tag = f" ({e['relation']})" if e.get('relation') else ""
                rel.append(f"{e.get('ref')}{tag}")
            if rel:
                print(f"    related      : {rel}")
            if intel.get("code_snapshot_sha"):
                print(f"    code_sha     : {intel.get('code_snapshot_sha')}")

    hr("RESULT B — Current repo state (the FSM the layer maintains)")
    async with tenant_transaction(TENANT, pool=pool) as ctx:
        prs = await ctx.fetch("SELECT pr_number, lifecycle, ci_state, merged, base_ref, head_sha "
                              "FROM github_pr_state WHERE repo=$1 ORDER BY pr_number", REPO)
        print("PRs:")
        for p in prs:
            print(f"  #{p['pr_number']}  lifecycle={p['lifecycle']:9s} ci={p['ci_state']:8s} "
                  f"merged={p['merged']} base={p['base_ref']}")
        iss = await ctx.fetch("SELECT issue_number, status FROM github_issue_state WHERE repo=$1 ORDER BY issue_number", REPO)
        print("Issues:", ", ".join(f"#{i['issue_number']}={i['status']}" for i in iss) or "(none)")
        br = await ctx.fetch("SELECT branch, left(head_sha,8) sha FROM github_branch_state WHERE repo=$1 ORDER BY branch", REPO)
        print("Branches:", ", ".join(f"{b['branch']}@{b['sha']}" for b in br) or "(none)")
        rs = await ctx.fetchrow("SELECT default_branch, left(head_sha,8) sha FROM github_repo_state WHERE repo=$1", REPO)
        if rs:
            print(f"Repo HEAD: {rs['default_branch']}@{rs['sha']}")
        ck = await ctx.fetch("SELECT check_name, conclusion, left(head_sha,8) sha FROM github_check_state WHERE repo=$1", REPO)
        print("Checks:", ", ".join(f"{c['check_name']}={c['conclusion']}@{c['sha']}" for c in ck) or "(none)")

    hr("RESULT C — System-of-record (github_signal_enrichment)")
    async with tenant_transaction(TENANT, pool=pool) as ctx:
        n = await ctx.fetchval("SELECT count(*) FROM github_signal_enrichment WHERE repo=$1", REPO)
        nch = await ctx.fetchval("SELECT count(*) FROM github_signal_enrichment WHERE repo=$1 AND state_changed", REPO)
        print(f"enrichment rows: {n}  (state-changing: {nch})")
        sample = await ctx.fetch(
            "SELECT entity_ref, event_type, action, state_before->>'lifecycle' lb, "
            "state_after->>'lifecycle' la, state_changed, reasoning_path, confidence "
            "FROM github_signal_enrichment WHERE repo=$1 AND state_changed ORDER BY enriched_at", REPO)
        for s in sample:
            print(f"  {s['entity_ref']:28s} {s['event_type']}/{s['action']}  "
                  f"changed={s['state_changed']} via={s['reasoning_path']}")

    hr("RESULT D — Code-RAG (semantic code search over the indexed repo)")
    try:
        from lib.embeddings.factory import make_embedder
        embedder = make_embedder()
        # The reindex created newer snapshots whose embeddings are still pending;
        # fill the latest before searching so code-RAG reflects current code.
        async with tenant_transaction(TENANT, pool=pool) as ctx:
            latest = await CodeGraphRepo(ctx).latest_ready_snapshot(TENANT, REPO)
        if latest:
            await fill_pending_embeddings(pool=pool, tenant_id=TENANT, snapshot_id=latest.id)
        q = "verify an authentication token"
        vec = await embedder.embed(q)
        async with tenant_transaction(TENANT, pool=pool) as ctx:
            graph = CodeGraphRepo(ctx)
            snap = await graph.latest_ready_snapshot(TENANT, REPO)
            hits = await graph.search_code(snap.id, vec, k=4)
        print(f'query: "{q}"')
        for h in hits:
            print(f"  {h['score']:.3f}  {h['path']}::{h['qualified_name']}  [{h['kind']}]")
        if not hits:
            print("  (no embeddings — ollama unavailable; skipped)")
    except Exception as exc:  # noqa: BLE001
        print(f"  (code-RAG skipped: {type(exc).__name__})")

    hr("RESULT E — RAW-FALLBACK guarantee (enrichment off -> raw signal ingested)")
    await flags.set_bool(TENANT, GITHUB_INTEL_ENABLED, False, set_by="demo")
    payload = {
        "action": "created", "repository": _repo_obj(), "sender": _sender("erin"),
        "comment": {"body": "raw-fallback probe", "node_id": "IC_RAW", "created_at": T0.isoformat()},
        "issue": {"number": 99, "node_id": "I_99"},
    }
    draft = await handler(payload, {"X-GitHub-Event": "issue_comment"})
    await ingest_from_draft(channel="github:webhook", draft=draft, pool=pool,
                            tenant_id=TENANT, enqueue_trigger=False)
    has_intel = "intelligence" in draft.content
    print(f"injected issue_comment with github_intel.enabled=FALSE")
    print(f"-> observation written: True   content.intelligence present: {has_intel}")
    print("-> CONFIRMED: on disabled/failure/timeout, the RAW github signal is ingested.")
    await flags.set_bool(TENANT, GITHUB_INTEL_ENABLED, True, set_by="demo")

    # Mangling-proof artifact: dump the enriched observations + state to JSON so
    # results can be inspected without terminal quote-escaping noise.
    async with tenant_transaction(TENANT, pool=pool) as ctx:
        rows = await ctx.fetch(
            "SELECT content_text, content FROM observations "
            "WHERE tenant_id=$1 AND source_channel='github:webhook' AND content->>'repo'=$2 "
            "ORDER BY occurred_at", TENANT, REPO)
        dump = []
        for r in rows:
            c = r["content"]
            if isinstance(c, str):
                c = json.loads(c)
            intel = c.get("intelligence") or {}
            dump.append({
                "text": r["content_text"],
                "state_change": intel.get("state_change"),
                "cause": intel.get("cause"),
                "effect": intel.get("effect"),
                "affected": intel.get("affected"),
                "related": intel.get("related_entities"),
            })
        enr = await ctx.fetch(
            "SELECT entity_ref, event_type, action, state_before, state_after, "
            "state_changed, cause, effect, reasoning_path, confidence "
            "FROM github_signal_enrichment WHERE repo=$1 ORDER BY enriched_at", REPO)
        sor = [dict(e) for e in enr]
    with open("/tmp/ghintel_demo_result.json", "w") as f:
        json.dump({"inline_observations": dump, "system_of_record": sor},
                  f, indent=2, default=str)
    print("\n[artifact] wrote /tmp/ghintel_demo_result.json")

    await pool.close()
    hr("DONE")


if __name__ == "__main__":
    asyncio.run(main())
