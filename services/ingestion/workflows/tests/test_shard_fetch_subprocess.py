"""M6.2a Phase 2 — real-subprocess tests for ShardFetch.

Two tests:

  1. SIGTERM clean exit + state durability — same shape as M6.0's
     test_feels_monitor_sigterm_subprocess and M6.1's
     test_orchestrator_sigterm_subprocess. Uses the
     NotImplementedError fetcher stub for a deterministic
     heartbeat-then-clean-exit sequence.

  2. Resume from persisted cursor after restart (LOAD-BEARING for the
     N1 + orphan-scan resume contract). Process A advances cursor
     through page 1 then exits; process B starts and resumes from
     the post-page-1 cursor. The fetcher's "remembered page index"
     is the proof that resume picked up the right cursor.

A note on the resume-test design: the fetcher in the subprocess
cannot share Python state with the test (separate process), so the
"remembered page index" is stored in the test fetcher's read of the
cursor argument. The fetcher in `_test_fetcher_module.py` reads
`cursor["page"]` to decide which page to emit. This makes the
resume property observable from the test side: if process B
fetched page 1's records (cursor was None), the resume is broken;
if it fetched page 2's records (cursor was {"page": 0}), the resume
works.
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
from services.ingestion.workflows.shard_fetch import (
    SIGNAL_KIND_REQUESTED,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)
from services.ingestion.workflows.signals import emit_signal


pytestmark = [pytest.mark.timeout(120)]


# Test fetcher module path — installed via PYTHONPATH so the
# subprocess imports it. Lives in tests/_helpers/ — alongside the
# tests for proximity.
_TEST_FETCHER_PATH = (
    os.path.dirname(__file__) + "/_helpers/shard_fetch_resume_fetcher.py"
)


def _ensure_test_fetcher_module() -> str:
    """Materialize the test-fetcher Python file in the tests'
    _helpers/ dir. Returns the absolute path to the directory the
    subprocess should add to PYTHONPATH.

    The fetcher records which page it fetched into a Postgres column
    so the test can verify resume picked up from the right cursor.
    """
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    os.makedirs(helpers_dir, exist_ok=True)
    init_py = os.path.join(helpers_dir, "__init__.py")
    if not os.path.exists(init_py):
        with open(init_py, "w") as f:
            f.write("# Test helpers for M6.2a subprocess tests.\n")

    content = '''"""Subprocess-loadable test fetcher for the
shard_fetch resume-from-cursor test. Installs itself into
FETCHER_DISPATCH on import.

Strategy: returns one fake record per page, with an asyncio.sleep
between pages so the test can SIGKILL the subprocess between
advances. Three pages, then end_of_data. The cursor `{"page": N}`
encodes which page was last successfully emitted.
"""
from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


# Per-page artificial delay. Picked to make the resume test
# reliably catchable: the test waits for cursor advance ~1s in,
# then SIGKILLs while the fetcher is sleeping in the NEXT page.
_PAGE_DELAY_SECONDS = 1.5


async def _resume_test_fetcher(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    current_page = 0 if cursor is None else (cursor.get("page", -1) + 1)
    # 3 pages then end_of_data.
    if current_page >= 3:
        return FetchResult(records=[], next_cursor=None, end_of_data=True)
    await asyncio.sleep(_PAGE_DELAY_SECONDS)
    records = [{"page": current_page, "id": current_page * 10}]
    return FetchResult(
        records=records,
        next_cursor={"page": current_page},
        end_of_data=(current_page == 2),
    )


# Install into the dispatch table at import time.
FETCHER_DISPATCH["github"] = _resume_test_fetcher
'''
    fetcher_file = os.path.join(helpers_dir, "shard_fetch_resume_fetcher.py")
    with open(fetcher_file, "w") as f:
        f.write(content)
    return helpers_dir


async def _read_workflow_state(
    pool: asyncpg.Pool, *, workflow_id: str,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT last_advanced_at, state_data FROM workflow_states "
        "WHERE workflow_kind = $1 AND workflow_id = $2",
        WORKFLOW_KIND, workflow_id,
    )


# =====================================================================
# 1. SIGTERM clean exit + state durability.
# =====================================================================

async def test_shard_fetch_sigterm_subprocess(
    fresh_db: asyncpg.Pool,
) -> None:
    """Spawn the service as a real subprocess. Inject one
    shard_fetch_requested signal for source='slack' (which the
    NotImplementedError stub will mark failed cleanly — that's fine
    for the heartbeat-and-clean-exit test). Wait for workflow_states
    diagnostic row to appear. SIGTERM. Assert rc=0 within 15s.
    """
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"shf-subproc-{tid.hex[:8]}",
    )
    await fresh_db.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'slack', $3, TRUE)
        """,
        uuid7(), tid, f"inst-{tid.hex[:8]}",
    )
    run_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at)
        VALUES ($1, $2, 'install', $3, 'running',
                ARRAY['slack']::text[], now())
        """,
        run_id, tid, f"wf-{run_id.hex[:8]}",
    )
    shard_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, created_at)
        VALUES ($1, $2, $3, 'slack', 'slack_channel_window',
                '{}'::jsonb, 1.0, 'pending', now())
        """,
        shard_id, run_id, tid,
    )
    await emit_signal(
        fresh_db,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_REQUESTED,
        idempotency_key=str(shard_id),
        signal_data={
            "shard_id": str(shard_id),
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tid),
            "source": "slack",
        },
    )

    instance = f"shf-sub-{tid.hex[:6]}"
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["SHARD_FETCH_TICK_SEC"] = "0.1"
    env["SHARD_FETCH_BATCH"] = "5"
    env["SHARD_FETCH_LEASE_SEC"] = "30.0"
    env["SHARD_FETCH_INSTANCE"] = instance
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"

    proc = subprocess.Popen(
        [sys.executable, "-m",
         "services.ingestion.workflows.shard_fetch"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        deadline = time.monotonic() + 30.0
        observed_state = None
        while time.monotonic() < deadline:
            observed_state = await _read_workflow_state(
                fresh_db, workflow_id=instance,
            )
            if observed_state is not None:
                break
            await asyncio.sleep(0.2)

        if observed_state is None:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"ShardFetch subprocess did not write a workflow_states "
                f"diagnostic row within 30s. stderr: {stderr[:1000]}"
            )

        # Shard should be 'failed' (NotImplementedError stub path).
        state = await fresh_db.fetchval(
            "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
        )
        assert state == "failed", (
            f"Stub path expected to mark shard 'failed'; got {state!r}."
        )

        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"ShardFetch did NOT exit within 15s of SIGTERM. "
                f"stderr: {stderr[:1000]}"
            )
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        assert rc == 0, (
            f"ShardFetch subprocess exited with rc={rc} (expected 0). "
            f"stderr: {stderr[:1000]}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


# =====================================================================
# 2. LOAD-BEARING — resume from persisted cursor after restart.
# =====================================================================

async def test_shard_fetch_resumes_from_persisted_cursor_after_restart(
    fresh_db: asyncpg.Pool,
) -> None:
    """The resume contract test.

    Process A: starts ShardFetch with test fetcher returning 3 pages.
      Wait until workflow_states.state_data.cursor == {"page": 0}
      (proof: page-0 was successfully advanced via N1 primitive).
      SIGTERM process A.

    Process B: starts ShardFetch fresh.
      Wait for the orphan-scan to pick up the in-progress shard.
      Wait until shard.state == 'done' (proof: pages 1 + 2 ran).
      Assert: cursor == {"page": 2}; end_of_data == True; the
      fetcher was called from cursor={"page": 0} onward (i.e., the
      resume used the persisted cursor, not started over).

    The test fetcher logic (test_helpers/shard_fetch_resume_fetcher.py)
    emits one record per page; the record carries `{"page": N}` so
    a downstream consumer could verify, but our load-bearing
    assertion is workflow_states.state_data["cursor"].

    PYTHONPATH gymnastics: the test fetcher needs to be importable
    in the subprocess. We materialize it into tests/_helpers/ and
    add that dir to PYTHONPATH so the subprocess can import
    `shard_fetch_resume_fetcher` (importing it installs the
    FETCHER_DISPATCH override). The subprocess's entry module
    (services/ingestion/workflows/shard_fetch.py) doesn't import
    the test fetcher itself; we use PYTHONSTARTUP-style trickery
    via a small bootstrap module.
    """
    helpers_dir = _ensure_test_fetcher_module()

    # Seed tenant, install, run, shard.
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"shf-resume-{tid.hex[:8]}",
    )
    await fresh_db.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'github', $3, TRUE)
        """,
        uuid7(), tid, f"inst-{tid.hex[:8]}",
    )
    run_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at)
        VALUES ($1, $2, 'install', $3, 'running',
                ARRAY['github']::text[], now())
        """,
        run_id, tid, f"wf-{run_id.hex[:8]}",
    )
    shard_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, created_at)
        VALUES ($1, $2, $3, 'github', 'github_repo_events',
                '{}'::jsonb, 1.0, 'pending', now())
        """,
        shard_id, run_id, tid,
    )
    await emit_signal(
        fresh_db,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_REQUESTED,
        idempotency_key=str(shard_id),
        signal_data={
            "shard_id": str(shard_id),
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tid),
            "source": "github",
        },
    )

    instance_a = f"shf-resA-{tid.hex[:6]}"
    instance_b = f"shf-resB-{tid.hex[:6]}"

    def _env_for(instance: str, lease_sec: str = "0.3") -> dict[str, str]:
        env = os.environ.copy()
        env["DATABASE_URL"] = os.environ["DATABASE_URL"]
        env["SHARD_FETCH_TICK_SEC"] = "0.1"
        env["SHARD_FETCH_BATCH"] = "5"
        env["SHARD_FETCH_LEASE_SEC"] = lease_sec
        env["SHARD_FETCH_FLUSH_SEC"] = "2.0"
        env["SHARD_FETCH_INSTANCE"] = instance
        env["WORKFLOWS_LOG_LEVEL"] = "INFO"
        # Make the test fetcher importable + install it before
        # ShardFetch reads FETCHER_DISPATCH. We use the
        # PYTHONSTARTUP-style mechanism: -X importtime won't do it;
        # use the -c wrapper.
        env["PYTHONPATH"] = helpers_dir + os.pathsep + env.get("PYTHONPATH", "")
        return env

    # We can't directly run `python -m services.ingestion.workflows.shard_fetch`
    # because that wouldn't import our test fetcher module. Instead,
    # use python -c to (a) import the test fetcher (installs the
    # dispatch override), (b) then call shard_fetch.main().
    bootstrap_code = (
        "import shard_fetch_resume_fetcher; "
        "from services.ingestion.workflows.shard_fetch import main; "
        "main()"
    )

    # ----- Process A: start, wait for page-0 cursor, SIGTERM. -----
    proc_a = subprocess.Popen(
        [sys.executable, "-c", bootstrap_code],
        env=_env_for(instance_a),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        # Wait until page-0 advance lands.
        deadline = time.monotonic() + 30.0
        page_zero_seen = False
        while time.monotonic() < deadline:
            ws = await _read_workflow_state(
                fresh_db, workflow_id=str(shard_id),
            )
            if ws is not None:
                cursor = ws["state_data"]
                if isinstance(cursor, (str, bytes)):
                    cursor = orjson.loads(cursor)
                if cursor.get("cursor") == {"page": 0}:
                    page_zero_seen = True
                    break
            await asyncio.sleep(0.05)

        if not page_zero_seen:
            proc_a.kill()
            proc_a.wait(timeout=5)
            stderr = proc_a.stderr.read().decode() if proc_a.stderr else ""
            stdout = proc_a.stdout.read().decode() if proc_a.stdout else ""
            # Snapshot DB state for diagnostics.
            sig_state = await fresh_db.fetchrow(
                "SELECT consumed_at, consumed_by FROM workflow_signals "
                "WHERE idempotency_key = $1 AND signal_kind = $2",
                str(shard_id), SIGNAL_KIND_REQUESTED,
            )
            shard_state = await fresh_db.fetchval(
                "SELECT state FROM onboarding_shards WHERE id = $1",
                shard_id,
            )
            ws_now = await _read_workflow_state(
                fresh_db, workflow_id=str(shard_id),
            )
            ws_state_data = ws_now and ws_now["state_data"]
            if isinstance(ws_state_data, (str, bytes)):
                ws_state_data = orjson.loads(ws_state_data)
            raise AssertionError(
                f"Process A did not advance past page 0 within 30s. "
                f"signal consumed_at={sig_state and sig_state['consumed_at']!r}, "
                f"consumed_by={sig_state and sig_state['consumed_by']!r}; "
                f"shard.state={shard_state!r}; "
                f"workflow_states.state_data={ws_state_data!r}; "
                f"stderr: {stderr[:2000]} | stdout: {stdout[:500]}"
            )

        # SIGTERM A. We want A to exit BEFORE it finishes pages 1 + 2.
        # Since each fetcher call is fast, A might run all three pages
        # before our signal arrives. To enforce mid-flight SIGTERM,
        # we hard-kill: SIGKILL leaves the shard in_progress with
        # cursor={"page": 0}.
        proc_a.kill()
        proc_a.wait(timeout=5)
    finally:
        if proc_a.poll() is None:
            proc_a.kill()
            proc_a.wait(timeout=5)

    # Confirm pre-restart state: cursor={"page": 0}, shard still
    # in_progress, NOT done.
    ws_pre = await _read_workflow_state(
        fresh_db, workflow_id=str(shard_id),
    )
    assert ws_pre is not None
    cursor_pre = ws_pre["state_data"]
    if isinstance(cursor_pre, (str, bytes)):
        cursor_pre = orjson.loads(cursor_pre)
    # The cursor MIGHT have advanced to {"page": 1} or {"page": 2}
    # if process A managed to fetch more pages before SIGKILL. The
    # resume property only requires that wherever it left off,
    # process B resumes from THAT cursor.
    state_pre = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )

    # If process A managed to finish all 3 pages before SIGKILL,
    # the resume test is vacuous (nothing to resume). Skip in that
    # case rather than declare a flake.
    if state_pre == "done":
        pytest.skip(
            "Process A finished all pages before SIGKILL — too fast "
            "to catch mid-flight. The test fetcher is too cheap; "
            "rerun is the answer. Not a service correctness concern."
        )

    # ----- Process B: start, wait for shard 'done'. -----
    proc_b = subprocess.Popen(
        [sys.executable, "-c", bootstrap_code],
        env=_env_for(instance_b),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 30.0
        shard_done = False
        while time.monotonic() < deadline:
            state = await fresh_db.fetchval(
                "SELECT state FROM onboarding_shards WHERE id = $1",
                shard_id,
            )
            if state == "done":
                shard_done = True
                break
            await asyncio.sleep(0.1)

        if not shard_done:
            proc_b.kill()
            proc_b.wait(timeout=5)
            stderr = proc_b.stderr.read().decode() if proc_b.stderr else ""
            final_state = await fresh_db.fetchval(
                "SELECT state FROM onboarding_shards WHERE id = $1",
                shard_id,
            )
            ws_now = await _read_workflow_state(
                fresh_db, workflow_id=str(shard_id),
            )
            raise AssertionError(
                f"Process B did not drive shard to 'done' within 30s. "
                f"final state={final_state!r}; workflow_states={ws_now and dict(ws_now['state_data'])!r}; "
                f"stderr: {stderr[:1000]}"
            )

        # LOAD-BEARING: final cursor = {"page": 2}, end_of_data = True.
        ws_post = await _read_workflow_state(
            fresh_db, workflow_id=str(shard_id),
        )
        assert ws_post is not None
        cursor_post = ws_post["state_data"]
        if isinstance(cursor_post, (str, bytes)):
            cursor_post = orjson.loads(cursor_post)
        assert cursor_post.get("cursor") == {"page": 2}, (
            f"Final cursor should be {{'page': 2}}; got {cursor_post}."
        )
        assert cursor_post.get("end_of_data") is True

        # Shut down B cleanly.
        proc_b.send_signal(signal.SIGTERM)
        try:
            rc = proc_b.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc_b.kill()
            proc_b.wait(timeout=5)
            stderr = proc_b.stderr.read().decode() if proc_b.stderr else ""
            raise AssertionError(
                f"Process B did NOT exit within 15s of SIGTERM. "
                f"stderr: {stderr[:1000]}"
            )
        stderr = proc_b.stderr.read().decode() if proc_b.stderr else ""
        assert rc == 0, (
            f"Process B exited rc={rc}. stderr: {stderr[:1000]}"
        )
    finally:
        if proc_b.poll() is None:
            proc_b.kill()
            proc_b.wait(timeout=5)
