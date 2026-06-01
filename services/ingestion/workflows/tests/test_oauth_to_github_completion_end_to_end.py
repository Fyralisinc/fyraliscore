"""M6.4 — GitHub backfill 5-subprocess E2E (clean + reshare paths).

Uses the real GitHub planner/fetcher/reconciler via the framework
chain. Mocks the GithubClient at the seam (`_open_github_client` in
fetcher.gmail and reconciler.github + the planner's PlannerContext
construction via `_build_source_client`).
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingestion.workflows.reconciler import (
    SIGNAL_KIND_SHARDS_COMPLETED,
    SIGNAL_KIND_SOURCE_COMPLETED,
    TENANT_ONBOARDING_INBOX_ID,
    TENANT_ONBOARDING_INBOX_KIND,
)
from services.ingestion.workflows.tenant_onboarding import (
    BRIDGE_INBOX_ID,
    BRIDGE_INBOX_KIND,
    SIGNAL_KIND_TENANT_COMPLETED as TENANT_ONBOARDING_COMPLETED,
)


# A27.6: the shared moto S3 server provides the raw-tier endpoint the
# M6.7 shard_fetch producer writes to (subprocesses inherit
# S3_ENDPOINT_URL via os.environ.copy()).
pytestmark = [pytest.mark.timeout(300), pytest.mark.usefixtures("moto_s3_server")]


def _ensure_clean_helper() -> str:
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    os.makedirs(helpers_dir, exist_ok=True)
    init_py = os.path.join(helpers_dir, "__init__.py")
    if not os.path.exists(init_py):
        with open(init_py, "w") as f:
            f.write("# Test helpers.\n")

    content = '''"""M6.4 GitHub clean-path helper. Patches the planner's
source-client factory (in source_onboarding) AND the fetcher /
reconciler seams. Clean path: reconciler etag-fast-path returns no
changes → no gap.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import github as gh_fetcher
from services.ingestion.reconcilers import github as gh_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _FakeClient:
    """Both the planner's client AND the fetcher/reconciler client."""

    async def list_installation_repositories(self, installation_id):
        return ["acme/api"]

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        # 1 page, 2 records, end-of-data.
        return ([
            {"id": 1, "title": "issue-1",
             "updated_at": "2025-01-01T00:00:00Z"},
            {"id": 2, "title": "issue-2",
             "updated_at": "2025-01-02T00:00:00Z"},
        ], "W/clean-etag", None)

    async def head_repo_events(self, *, owner, repo, event_type, etag):
        # Etag fast-path: no changes since fetcher's stored etag.
        return (False, etag)


# Source-onboarding planner-side build:
async def _fake_build_source_client(source, pool, install):
    if source == "github":
        return _FakeClient()
    return None


# Fetcher seam:
async def _fake_fetcher_open(install):
    async def close(): return None
    return _FakeClient(), close


# Reconciler seam:
async def _fake_reconciler_open(install):
    async def close(): return None
    return _FakeClient(), close


so_mod._build_source_client = _fake_build_source_client
gh_fetcher._open_github_client = _fake_fetcher_open
gh_reconciler._open_github_client = _fake_reconciler_open
'''
    helpers_file = os.path.join(helpers_dir, "e2e_test_github_clean_dispatch.py")
    with open(helpers_file, "w") as f:
        f.write(content)
    return helpers_dir


def _ensure_reshare_helper() -> str:
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    content = '''"""M6.4 GitHub reshare-path helper. First reconciler pass
detects gap; gap-fill shard backfills; second pass clean.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import github as gh_fetcher
from services.ingestion.reconcilers import github as gh_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _PlannerFetcherClient:
    """Used by planner + fetcher subprocesses. Backfill returns records
    with updated_at <= 2025-01-01; gap-fill fetcher returns 1 record.
    """

    def __init__(self):
        self.fetch_calls = 0

    async def list_installation_repositories(self, installation_id):
        return ["acme/api"]

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        self.fetch_calls += 1
        return ([
            {"id": 1, "title": "issue-1",
             "updated_at": "2025-01-01T00:00:00Z"},
        ], "W/etag-page-1", None)


class _ReconcilerClient:
    """Reconciler-side: stateful so we converge.

    Pass-0: head says changes; list returns newer record → gap.
    Pass-1 onwards: head says no changes → clean.
    """

    def __init__(self):
        self.head_calls = 0

    async def head_repo_events(self, *, owner, repo, event_type, etag):
        self.head_calls += 1
        # First call returns changes; subsequent calls (pass-1+) return
        # clean so the cycle converges.
        return (self.head_calls == 1, f"W/etag-call-{self.head_calls}")

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        return ([
            {"id": 99, "updated_at": "2025-02-01T00:00:00Z"},
        ], "W/post-etag", None)


_PFC = _PlannerFetcherClient()
_REC_CLIENT = _ReconcilerClient()


async def _fake_build_source_client(source, pool, install):
    if source == "github":
        return _PFC
    return None


async def _fake_fetcher_open(install):
    async def close(): return None
    return _PFC, close


async def _fake_reconciler_open(install):
    async def close(): return None
    return _REC_CLIENT, close


so_mod._build_source_client = _fake_build_source_client
gh_fetcher._open_github_client = _fake_fetcher_open
gh_reconciler._open_github_client = _fake_reconciler_open
'''
    helpers_file = os.path.join(helpers_dir, "e2e_test_github_reshare_dispatch.py")
    with open(helpers_file, "w") as f:
        f.write(content)
    return helpers_dir


def _env_for(*, instance_var, instance_value, helpers_dir, extra=None):
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"
    env["PYTHONPATH"] = helpers_dir + os.pathsep + env.get("PYTHONPATH", "")
    env[instance_var] = instance_value
    if extra:
        env.update(extra)
    return env


async def _seed_github_install(pool, *, tenant_id):
    install_id = uuid7()
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'github', '42', TRUE)
        """,
        install_id, tenant_id,
    )
    return install_id


async def _seed_trigger(pool, *, tenant_id):
    tid = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, 'github', 'install', '{}'::jsonb)
        """,
        tid, tenant_id,
    )


async def _run_five_subprocess(*, fresh_db, helpers_dir, helper_name, label):
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"e2e-m64-{label}-{tid.hex[:6]}",
    )
    await _seed_github_install(fresh_db, tenant_id=tid)
    await _seed_trigger(fresh_db, tenant_id=tid)

    inst = lambda role: f"e64-{label}-{role}-{tid.hex[:4]}"
    bootstrap = (
        f"import {helper_name}; "
        "from {svc_main} import main; main()"
    )
    procs: dict[str, subprocess.Popen | None] = {
        k: None for k in ("poller", "orch", "src", "shf", "rec")
    }

    procs["poller"] = subprocess.Popen(
        [sys.executable, "-m", "services.ingestion.workflows.oauth_poller"],
        env=_env_for(
            instance_var="OAUTH_POLLER_INSTANCE",
            instance_value=inst("poll"), helpers_dir=helpers_dir,
            extra={"OAUTH_POLLER_TICK_SEC": "0.1",
                   "OAUTH_POLLER_BATCH": "5"},
        ),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 30.0
    run_id = None
    while time.monotonic() < deadline:
        row = await fresh_db.fetchrow(
            "SELECT id FROM onboarding_runs WHERE tenant_id = $1", tid,
        )
        if row:
            run_id = row["id"]
            break
        await asyncio.sleep(0.1)
    assert run_id is not None

    procs["orch"] = subprocess.Popen(
        [sys.executable, "-m",
         "services.ingestion.workflows.tenant_onboarding"],
        env=_env_for(
            instance_var="ORCHESTRATOR_INSTANCE",
            instance_value=inst("orch"), helpers_dir=helpers_dir,
            extra={"ORCHESTRATOR_TICK_SEC": "0.1",
                   "ORCHESTRATOR_BATCH": "20"},
        ),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    procs["src"] = subprocess.Popen(
        [sys.executable, "-c",
         bootstrap.format(
             svc_main="services.ingestion.workflows.source_onboarding")],
        env=_env_for(
            instance_var="SOURCE_ONBOARDING_INSTANCE",
            instance_value=inst("src"), helpers_dir=helpers_dir,
            extra={"SOURCE_ONBOARDING_TICK_SEC": "0.1",
                   "SOURCE_ONBOARDING_BATCH": "20"},
        ),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    procs["shf"] = subprocess.Popen(
        [sys.executable, "-c",
         bootstrap.format(
             svc_main="services.ingestion.workflows.shard_fetch")],
        env=_env_for(
            instance_var="SHARD_FETCH_INSTANCE",
            instance_value=inst("shf"), helpers_dir=helpers_dir,
            extra={"SHARD_FETCH_TICK_SEC": "0.1",
                   "SHARD_FETCH_BATCH": "5",
                   "SHARD_FETCH_LEASE_SEC": "30.0",
                   "SHARD_FETCH_FLUSH_SEC": "2.0"},
        ),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    procs["rec"] = subprocess.Popen(
        [sys.executable, "-c",
         bootstrap.format(
             svc_main="services.ingestion.workflows.reconciler")],
        env=_env_for(
            instance_var="RECONCILER_INSTANCE",
            instance_value=inst("rec"), helpers_dir=helpers_dir,
            extra={"RECONCILER_TICK_SEC": "0.1",
                   "RECONCILER_BATCH": "20"},
        ),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return tid, run_id, procs


async def _wait_for_bridge(fresh_db, run_id, procs, deadline_s=90.0):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        row = await fresh_db.fetchrow(
            """
            SELECT signal_data FROM workflow_signals
             WHERE workflow_kind = $1 AND workflow_id = $2
               AND signal_kind = $3 AND idempotency_key = $4
            """,
            BRIDGE_INBOX_KIND, BRIDGE_INBOX_ID,
            TENANT_ONBOARDING_COMPLETED, str(run_id),
        )
        if row is not None:
            return row
        await asyncio.sleep(0.2)
    stderrs = {
        k: (p.stderr.read().decode()[:2000] if p and p.stderr else "")
        for k, p in procs.items()
    }
    raise AssertionError(f"Bridge signal not seen. stderrs={stderrs!r}")


def _sigterm_all(procs):
    for name, proc in procs.items():
        if proc is None:
            continue
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(f"{name} stuck. stderr: {stderr[:1000]}")
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        assert rc == 0, f"{name} rc={rc}. stderr: {stderr[:1000]}"


async def test_oauth_trigger_to_github_completion_end_to_end(
    fresh_db: asyncpg.Pool,
) -> None:
    """Clean-path: 1 repo × 2 event_types = 2 shards; both done;
    etag-fastpath says clean; completion to Bridge."""
    helpers_dir = _ensure_clean_helper()
    tid, run_id, procs = await _run_five_subprocess(
        fresh_db=fresh_db, helpers_dir=helpers_dir,
        helper_name="e2e_test_github_clean_dispatch",
        label="clean",
    )
    try:
        await _wait_for_bridge(fresh_db, run_id, procs)

        sor = await fresh_db.fetchrow(
            "SELECT status, reconciled_at, reconciliation_pass_count "
            "FROM source_onboarding_runs "
            "WHERE onboarding_run_id = $1 AND source = 'github'",
            run_id,
        )
        assert sor["status"] == "completed"
        assert sor["reconciled_at"] is not None
        assert sor["reconciliation_pass_count"] == 0

        # Two shards (1 repo × 2 event_types).
        shards = await fresh_db.fetch(
            "SELECT state, shard_kind FROM onboarding_shards "
            "WHERE onboarding_run_id = $1",
            run_id,
        )
        assert len(shards) == 2
        assert all(s["state"] == "done" for s in shards)

        # Exactly one source_onboarding_completed (run-keyed).
        n = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
            SIGNAL_KIND_SOURCE_COMPLETED, f"{run_id}:github",
        ))
        assert n == 1
        _sigterm_all(procs)
    finally:
        for p in procs.values():
            if p is not None and p.poll() is None:
                p.kill()
                p.wait(timeout=5)


async def test_oauth_trigger_to_github_completion_with_reshare(
    fresh_db: asyncpg.Pool,
) -> None:
    """Reshare path: reconciler etag-changed + newer updated_at →
    gap-fill shard with parent_shard_id; second pass clean."""
    helpers_dir = _ensure_reshare_helper()
    tid, run_id, procs = await _run_five_subprocess(
        fresh_db=fresh_db, helpers_dir=helpers_dir,
        helper_name="e2e_test_github_reshare_dispatch",
        label="reshare",
    )
    try:
        # Wait for reshare (pass_count >= 1).
        deadline = time.monotonic() + 90.0
        pass_count = 0
        while time.monotonic() < deadline:
            pass_count = int(await fresh_db.fetchval(
                "SELECT reconciliation_pass_count FROM source_onboarding_runs "
                "WHERE onboarding_run_id = $1 AND source = 'github'",
                run_id,
            ) or 0)
            if pass_count >= 1:
                break
            await asyncio.sleep(0.2)
        assert pass_count >= 1, (
            "GitHub reconciler did not reshare within 90s. "
            f"pass_count={pass_count}"
        )

        await _wait_for_bridge(fresh_db, run_id, procs)

        sor = await fresh_db.fetchrow(
            "SELECT status, reconciled_at, reconciliation_pass_count "
            "FROM source_onboarding_runs "
            "WHERE onboarding_run_id = $1 AND source = 'github'",
            run_id,
        )
        assert sor["status"] == "completed"
        assert sor["reconciled_at"] is not None

        # At least one gap shard with parent_shard_id linkage.
        n_gap = int(await fresh_db.fetchval(
            "SELECT count(*) FROM onboarding_shards "
            "WHERE onboarding_run_id = $1 AND parent_shard_id IS NOT NULL",
            run_id,
        ))
        assert n_gap >= 1

        # Cross-service idempotency: exactly 1 source_onboarding_completed.
        n_src = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
            SIGNAL_KIND_SOURCE_COMPLETED, f"{run_id}:github",
        ))
        assert n_src == 1
        _sigterm_all(procs)
    finally:
        for p in procs.values():
            if p is not None and p.poll() is None:
                p.kill()
                p.wait(timeout=5)
