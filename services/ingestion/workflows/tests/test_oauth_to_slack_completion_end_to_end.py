"""M6.5 — Slack backfill 5-subprocess E2E (clean + reshare)."""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.ingestion.workflows.reconciler import (
    SIGNAL_KIND_SOURCE_COMPLETED,
    TENANT_ONBOARDING_INBOX_ID,
    TENANT_ONBOARDING_INBOX_KIND,
)
from services.ingestion.workflows.tenant_onboarding import (
    BRIDGE_INBOX_ID,
    BRIDGE_INBOX_KIND,
    SIGNAL_KIND_TENANT_COMPLETED as TENANT_ONBOARDING_COMPLETED,
)


# A27.6: shared moto S3 server provides the raw-tier endpoint for the
# M6.7 shard_fetch producer (subprocesses inherit S3_ENDPOINT_URL).
pytestmark = [pytest.mark.timeout(300), pytest.mark.usefixtures("moto_s3_server")]


def _ensure_clean_helper() -> str:
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    os.makedirs(helpers_dir, exist_ok=True)
    content = '''"""Slack clean-path helper."""
from services.ingestion.fetchers import slack as sl_fetcher
from services.ingestion.reconcilers import slack as sl_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _FakeClient:
    async def conversations_list(self):
        return [{"id": "C001", "name": "general", "team_id": "T1"}]

    async def conversations_history(self, *, channel, cursor=None,
                                    oldest=None, limit=None):
        if cursor is None and oldest is None:
            # First/backfill call.
            return [
                {"ts": "1700000.000001", "text": "m1"},
                {"ts": "1700000.000002", "text": "m2"},
            ], None
        # Reconciler check (oldest=newest_seen_ts): no newer messages.
        return [], None


async def _build(source, pool, install):
    if source == "slack":
        return _FakeClient()
    return None


async def _open(install):
    async def close(): return None
    return _FakeClient(), close


so_mod._build_source_client = _build
sl_fetcher._open_slack_client = _open
sl_reconciler._open_slack_client = _open
'''
    f = os.path.join(helpers_dir, "e2e_test_slack_clean_dispatch.py")
    with open(f, "w") as fh:
        fh.write(content)
    return helpers_dir


def _ensure_reshare_helper() -> str:
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    content = '''"""Slack reshare-path helper. Reconciler pass-0 sees
newer messages; pass-1 sees no newer messages."""
from services.ingestion.fetchers import slack as sl_fetcher
from services.ingestion.reconcilers import slack as sl_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _FetcherClient:
    """Used by source_onboarding (planner) AND shard_fetch (fetcher).
    Returns 2 backfill messages; for gap-fill shards (oldest=ts),
    returns 1 newer message and ends.
    """
    async def conversations_list(self):
        return [{"id": "C001", "name": "general", "team_id": "T1"}]

    async def conversations_history(self, *, channel, cursor=None,
                                    oldest=None, limit=None):
        if oldest is not None:
            # Gap-fill shard's fetch (the shard_identifier has gap_baseline_ts).
            # In practice the fetcher passes cursor (next_cursor), not oldest,
            # so this branch is mostly defensive.
            return [{"ts": "1800000.000"}], None
        # Backfill: 2 messages.
        return [
            {"ts": "1700000.001"},
            {"ts": "1700000.002"},
        ], None


class _ReconcilerClient:
    """Stateful: first call returns newer; subsequent calls return empty."""
    def __init__(self):
        self.calls = 0
    async def conversations_history(self, *, channel, cursor=None,
                                    oldest=None, limit=None):
        self.calls += 1
        if self.calls == 1:
            return [{"ts": "1800000.000"}], None
        return [], None


_FC = _FetcherClient()
_RC = _ReconcilerClient()


async def _build(source, pool, install):
    if source == "slack":
        return _FC
    return None


async def _fopen(install):
    async def close(): return None
    return _FC, close


async def _ropen(install):
    async def close(): return None
    return _RC, close


so_mod._build_source_client = _build
sl_fetcher._open_slack_client = _fopen
sl_reconciler._open_slack_client = _ropen
'''
    f = os.path.join(helpers_dir, "e2e_test_slack_reshare_dispatch.py")
    with open(f, "w") as fh:
        fh.write(content)
    return helpers_dir


def _env(*, var, value, helpers_dir, extra=None):
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"
    env["PYTHONPATH"] = helpers_dir + os.pathsep + env.get("PYTHONPATH", "")
    env[var] = value
    if extra:
        env.update(extra)
    return env


async def _seed(pool, *, tenant_id):
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'slack', 'T1', TRUE)
        """,
        uuid7(), tenant_id,
    )
    await pool.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, 'slack', 'install', '{}'::jsonb)
        """,
        uuid7(), tenant_id,
    )


async def _run_pipeline(*, fresh_db, helpers_dir, helper_name, label):
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"e2e-m65-{label}-{tid.hex[:6]}",
    )
    await _seed(fresh_db, tenant_id=tid)
    bootstrap = (
        f"import {helper_name}; from {{svc_main}} import main; main()"
    )
    inst = lambda r: f"m65-{label}-{r}-{tid.hex[:4]}"

    procs: dict[str, subprocess.Popen | None] = {
        k: None for k in ("poller", "orch", "src", "shf", "rec")
    }
    procs["poller"] = subprocess.Popen(
        [sys.executable, "-m", "services.ingestion.workflows.oauth_poller"],
        env=_env(var="OAUTH_POLLER_INSTANCE", value=inst("poll"),
                 helpers_dir=helpers_dir,
                 extra={"OAUTH_POLLER_TICK_SEC": "0.1"}),
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

    for role, svc in [
        ("orch", "tenant_onboarding"),
        ("src", "source_onboarding"),
        ("shf", "shard_fetch"),
        ("rec", "reconciler"),
    ]:
        var = {"orch": "ORCHESTRATOR_INSTANCE",
               "src": "SOURCE_ONBOARDING_INSTANCE",
               "shf": "SHARD_FETCH_INSTANCE",
               "rec": "RECONCILER_INSTANCE"}[role]
        env_extras = {
            "orch": {"ORCHESTRATOR_TICK_SEC": "0.1"},
            "src": {"SOURCE_ONBOARDING_TICK_SEC": "0.1"},
            "shf": {"SHARD_FETCH_TICK_SEC": "0.1",
                    "SHARD_FETCH_FLUSH_SEC": "2.0"},
            "rec": {"RECONCILER_TICK_SEC": "0.1"},
        }[role]
        # tenant_onboarding doesn't need dispatch override; others do.
        if role == "orch":
            cmd = [sys.executable, "-m",
                   f"services.ingestion.workflows.{svc}"]
        else:
            cmd = [sys.executable, "-c",
                   bootstrap.format(
                       svc_main=f"services.ingestion.workflows.{svc}")]
        procs[role] = subprocess.Popen(
            cmd,
            env=_env(var=var, value=inst(role),
                     helpers_dir=helpers_dir, extra=env_extras),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    return tid, run_id, procs


async def _wait_bridge(fresh_db, run_id, procs, deadline_s=120.0):
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
    raise AssertionError(f"Bridge not seen. stderrs={stderrs!r}")


def _sigterm_all(procs):
    for name, p in procs.items():
        if p is None:
            continue
        p.send_signal(signal.SIGTERM)
        try:
            rc = p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=5)
            raise AssertionError(f"{name} stuck")
        stderr = p.stderr.read().decode() if p.stderr else ""
        assert rc == 0, f"{name} rc={rc}. stderr: {stderr[:1000]}"


async def test_oauth_trigger_to_slack_completion_end_to_end(
    fresh_db: asyncpg.Pool,
) -> None:
    helpers_dir = _ensure_clean_helper()
    tid, run_id, procs = await _run_pipeline(
        fresh_db=fresh_db, helpers_dir=helpers_dir,
        helper_name="e2e_test_slack_clean_dispatch", label="clean",
    )
    try:
        await _wait_bridge(fresh_db, run_id, procs)
        sor = await fresh_db.fetchrow(
            "SELECT status, reconciled_at, reconciliation_pass_count "
            "FROM source_onboarding_runs WHERE onboarding_run_id = $1 "
            "AND source = 'slack'", run_id,
        )
        assert sor["status"] == "completed"
        assert sor["reconciliation_pass_count"] == 0
        shards = await fresh_db.fetch(
            "SELECT state, shard_kind FROM onboarding_shards "
            "WHERE onboarding_run_id = $1", run_id,
        )
        assert len(shards) == 1
        assert shards[0]["state"] == "done"
        _sigterm_all(procs)
    finally:
        for p in procs.values():
            if p is not None and p.poll() is None:
                p.kill()
                p.wait(timeout=5)


async def test_oauth_trigger_to_slack_completion_with_reshare(
    fresh_db: asyncpg.Pool,
) -> None:
    helpers_dir = _ensure_reshare_helper()
    tid, run_id, procs = await _run_pipeline(
        fresh_db=fresh_db, helpers_dir=helpers_dir,
        helper_name="e2e_test_slack_reshare_dispatch", label="reshare",
    )
    try:
        # Wait reshare.
        deadline = time.monotonic() + 90.0
        pc = 0
        while time.monotonic() < deadline:
            pc = int(await fresh_db.fetchval(
                "SELECT reconciliation_pass_count FROM source_onboarding_runs "
                "WHERE onboarding_run_id = $1 AND source = 'slack'",
                run_id,
            ) or 0)
            if pc >= 1:
                break
            await asyncio.sleep(0.2)
        assert pc >= 1, f"reshare didn't fire; pass_count={pc}"

        await _wait_bridge(fresh_db, run_id, procs)
        n_gap = int(await fresh_db.fetchval(
            "SELECT count(*) FROM onboarding_shards "
            "WHERE onboarding_run_id = $1 AND parent_shard_id IS NOT NULL",
            run_id,
        ))
        assert n_gap >= 1
        n = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
            SIGNAL_KIND_SOURCE_COMPLETED, f"{run_id}:slack",
        ))
        assert n == 1
        _sigterm_all(procs)
    finally:
        for p in procs.values():
            if p is not None and p.poll() is None:
                p.kill()
                p.wait(timeout=5)
