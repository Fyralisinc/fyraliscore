"""Integration: GitHub Intelligence Layer end-to-end (live Postgres, no Kafka).

Proves the full chain on a per-test isolated tenant + repo:
  index code -> inject webhooks via the REAL handler+core path (inline hook
  enriches content.intelligence) -> drain the ordered worker (authoritative FSM
  + system-of-record) -> assertions.

Also proves the raw-on-failure guarantee (disabled flag + inline timeout).

ISOLATION: both observation dedup ((source_channel, external_id), GLOBAL) and
the dev DB's superuser role (which BYPASSES RLS, so cross-tenant rows are
visible) mean a fixed repo/external_id would accumulate across runs. We therefore
scope every run to a UNIQUE repo + unique node_ids derived from the random
tenant (`U`). The conftest `db_pool` fixture guarantees migrations are applied.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

pytestmark = [pytest.mark.integration]

T0 = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)

FIXTURE = {
    "app/db.py": "def query(sql):\n    return []\n",
    "app/auth.py": (
        "from app.db import query\n\n"
        "def verify_token(token):\n"
        "    return query('select * from sessions')\n"
    ),
    "app/api.py": (
        "from app.auth import verify_token\n\n"
        "def handle(req):\n"
        "    return verify_token(req.token)\n"
    ),
}


@pytest.fixture
async def pool(db_pool):
    """Gateway pool (JSONB + vector codecs) over the migrated test DB.

    Depends on the conftest `db_pool` fixture so migrations are guaranteed
    applied, then builds a gateway pool from DATABASE_URL (the codecs the
    observation repo + code-graph reads expect).
    """
    from services.app.gateway.db_bootstrap import create_gateway_pool
    p = await create_gateway_pool(os.environ["DATABASE_URL"])
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture
async def tenant_id(pool) -> UUID:
    tid = uuid4()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            tid, "ghintel-it",
        )
    return tid


@pytest.fixture
def U(tenant_id) -> str:
    """Per-run-unique token (from the random tenant)."""
    return tenant_id.hex[:10]


@pytest.fixture
def repo(U) -> str:
    """Unique repo per run — isolates snapshots/observations/state/enrichment."""
    return f"acme/intel-it-{U}"


def _ts(i: int) -> str:
    return (T0 + timedelta(minutes=i)).isoformat()


def _write_fixture() -> str:
    root = tempfile.mkdtemp(prefix="ghintel_it_")
    for rel, body in FIXTURE.items():
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(body)
    return root


async def _enable(pool, tenant_id):
    from services.ingest.ingestion.feature_flags.client import TenantFlags
    from services.ingest.github_intel.config import GITHUB_INTEL_ENABLED, CODE_INTEL_ENABLED
    flags = TenantFlags(pool)
    await flags.set_bool(tenant_id, CODE_INTEL_ENABLED, True, set_by="test")
    await flags.set_bool(tenant_id, GITHUB_INTEL_ENABLED, True, set_by="test")


async def _inject(pool, tenant_id, event, payload):
    from services.ingest.ingestion.handlers import get_handler
    from services.ingest.ingestion.core import ingest_from_draft
    handler = get_handler("github:webhook")
    draft = await handler(payload, {"X-GitHub-Event": event})
    res = await ingest_from_draft(channel="github:webhook", draft=draft, pool=pool,
                                  tenant_id=tenant_id, enqueue_trigger=False)
    assert res.deduped is False, "test external_id collided — make U more unique"
    return draft


async def test_full_pipeline_enriches_and_tracks_state(pool, tenant_id, U, repo):
    from lib.shared.tenant_context import tenant_transaction
    from services.ingest.code_intel.indexer import index_working_copy
    from services.ingest.github_intel.worker import drain, enqueue_new_github_observations

    await _enable(pool, tenant_id)

    # 1. index the code
    root = _write_fixture()
    stats = await index_working_copy(
        pool=pool, tenant_id=tenant_id, repo_full_name=repo,
        root_path=root, commit_sha=f"sha0-{U}", branch="main",
    )
    assert stats.files == 3
    assert stats.symbols >= 3
    assert stats.edges >= 2

    # 2. inject: PR opened (touches app/db.py) -> approved -> merged
    pr_open = await _inject(pool, tenant_id, "pull_request", {
        "action": "opened", "repository": {"full_name": repo}, "sender": {"login": "alice"},
        "pull_request": {"number": 7, "title": "touch db (fixes #3)", "node_id": f"PR7o-{U}",
                         "merged": False, "base": {"ref": "main"},
                         "head": {"ref": "f", "sha": "h7"}, "_changed_files": ["app/db.py"],
                         "created_at": _ts(0), "updated_at": _ts(0)},
    })
    # inline enrichment present on the SAME draft content
    assert "intelligence" in pr_open.content
    intel = pr_open.content["intelligence"]
    assert intel["entity"]["ref"] == f"{repo}#7"
    # blast radius: db.py is imported by auth.py which is imported by api.py
    dep_files = [d["path"] for d in intel["affected"]["dependent_files"]]
    assert "app/auth.py" in dep_files
    assert "app/api.py" in dep_files

    await _inject(pool, tenant_id, "pull_request_review", {
        "action": "submitted", "repository": {"full_name": repo}, "sender": {"login": "bob"},
        "review": {"state": "approved", "node_id": f"RV7-{U}", "submitted_at": _ts(1)},
        "pull_request": {"number": 7, "node_id": "PR7"},
    })
    await _inject(pool, tenant_id, "pull_request", {
        "action": "closed", "repository": {"full_name": repo}, "sender": {"login": "alice"},
        "pull_request": {"number": 7, "title": "touch db (fixes #3)", "node_id": f"PR7m-{U}",
                         "merged": True, "base": {"ref": "main"},
                         "head": {"ref": "f", "sha": "h7"}, "merge_commit_sha": "m7",
                         "_changed_files": ["app/db.py"], "created_at": _ts(0), "updated_at": _ts(2)},
    })

    # 3. drain the ordered worker -> authoritative state + system-of-record
    fed = await enqueue_new_github_observations(pool, tenant_id)
    assert fed == 3
    done = await drain(pool, tenant_id, worker_id="test", llm_enabled=False)
    assert done == 3

    # 4. assert authoritative FSM state
    async with tenant_transaction(tenant_id, pool=pool) as ctx:
        pr = await ctx.fetchrow(
            "SELECT lifecycle, merged FROM github_pr_state WHERE repo=$1 AND pr_number=7", repo)
        assert pr["lifecycle"] == "merged"
        assert pr["merged"] is True

        # system-of-record: one enrichment per observation, correct entity_ref
        enr = await ctx.fetch(
            "SELECT action, entity_ref, state_changed, reasoning_path "
            "FROM github_signal_enrichment WHERE repo=$1 AND entity_ref=$2 ORDER BY enriched_at",
            repo, f"{repo}#7")
        assert len(enr) == 3
        assert all(e["entity_ref"] == f"{repo}#7" for e in enr)
        merge_row = [e for e in enr if e["action"] == "closed"][0]
        assert merge_row["state_changed"] is True
        assert merge_row["reasoning_path"] == "rule"

        # 5. self-update: the merge emitted a reindex trigger for the default branch
        trig = await ctx.fetchrow(
            "SELECT kind FROM code_intel_index_triggers "
            "WHERE repo_full_name=$1 AND commit_sha=$2", repo, "m7")
        assert trig is not None
        assert trig["kind"] == "merge"


async def test_ordering_guard_late_event_does_not_regress(pool, tenant_id, U, repo):
    """Drain a merge, THEN a late 'opened' (earlier occurred_at) — must not regress."""
    from lib.shared.tenant_context import tenant_transaction
    from services.ingest.github_intel.worker import drain, enqueue_new_github_observations

    await _enable(pool, tenant_id)

    # phase 1: merge lands and is processed
    await _inject(pool, tenant_id, "pull_request", {
        "action": "closed", "repository": {"full_name": repo}, "sender": {"login": "a"},
        "pull_request": {"number": 9, "title": "x", "node_id": f"PR9m-{U}", "merged": True,
                         "base": {"ref": "main"}, "head": {"ref": "f", "sha": "h9"},
                         "merge_commit_sha": "m9", "created_at": _ts(5), "updated_at": _ts(9)},
    })
    await enqueue_new_github_observations(pool, tenant_id)
    await drain(pool, tenant_id, worker_id="test", llm_enabled=False)
    async with tenant_transaction(tenant_id, pool=pool) as ctx:
        assert (await ctx.fetchval(
            "SELECT lifecycle FROM github_pr_state WHERE repo=$1 AND pr_number=9", repo)) == "merged"

    # phase 2: a STALE 'opened' for the same PR arrives late (earlier occurred_at)
    await _inject(pool, tenant_id, "pull_request", {
        "action": "opened", "repository": {"full_name": repo}, "sender": {"login": "a"},
        "pull_request": {"number": 9, "title": "x", "node_id": f"PR9o-{U}", "merged": False,
                         "base": {"ref": "main"}, "head": {"ref": "f", "sha": "h9"},
                         "created_at": _ts(5), "updated_at": _ts(5)},
    })
    await enqueue_new_github_observations(pool, tenant_id)
    await drain(pool, tenant_id, worker_id="test", llm_enabled=False)

    async with tenant_transaction(tenant_id, pool=pool) as ctx:
        # ordering guard: occurred_at(opened)=ts5 < last_event_at(ts9) -> no regression
        assert (await ctx.fetchval(
            "SELECT lifecycle FROM github_pr_state WHERE repo=$1 AND pr_number=9", repo)) == "merged"
        # but the stale event is still enriched (every signal gets context)
        n = await ctx.fetchval(
            "SELECT count(*) FROM github_signal_enrichment WHERE repo=$1 AND entity_ref=$2",
            repo, f"{repo}#9")
        assert n == 2


async def test_raw_on_failure_when_disabled(pool, tenant_id, U, repo):
    """github_intel.enabled=False -> raw signal ingested, NO intelligence key."""
    from services.ingest.ingestion.feature_flags.client import TenantFlags
    from services.ingest.github_intel.config import GITHUB_INTEL_ENABLED
    await TenantFlags(pool).set_bool(tenant_id, GITHUB_INTEL_ENABLED, False, set_by="test")

    draft = await _inject(pool, tenant_id, "issue_comment", {
        "action": "created", "repository": {"full_name": repo}, "sender": {"login": "e"},
        "comment": {"body": "hi", "node_id": f"ICz-{U}", "created_at": _ts(0)},
        "issue": {"number": 1, "node_id": "I1"},
    })
    assert "intelligence" not in draft.content


async def test_inline_timeout_falls_back_to_raw(pool, tenant_id, U, repo, monkeypatch):
    """If the inline enrichment exceeds its budget, the raw signal is ingested."""
    import asyncio
    import services.ingest.github_intel.inline as inline_mod
    await _enable(pool, tenant_id)

    async def _slow(*a, **k):
        await asyncio.sleep(5)

    monkeypatch.setattr(inline_mod, "_enrich", _slow)
    monkeypatch.setattr(inline_mod, "INLINE_TIMEOUT_MS", 100)

    draft = await _inject(pool, tenant_id, "issues", {
        "action": "opened", "repository": {"full_name": repo}, "sender": {"login": "d"},
        "issue": {"number": 5, "title": "t", "node_id": f"I5-{U}", "created_at": _ts(0),
                  "updated_at": _ts(0)},
    })
    assert "intelligence" not in draft.content
