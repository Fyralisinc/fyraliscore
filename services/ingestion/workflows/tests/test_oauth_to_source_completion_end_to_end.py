"""M6.2a Phase 3 — full M6 backfill flow end-to-end (5 subprocesses
post-M6.2b chain insertion).

The milestone-shaping artifact of M6.2a. If this test fails, M6.2a
does NOT ship.

**Updated by M6.2b Phase 1 for the chain change**: SourceOnboarding's
success-path now emits `source_shards_completed` to the Reconciler
inbox instead of `source_onboarding_completed` direct to
TenantOnboarding. The test now spawns a 5th subprocess (Reconciler)
and the chain assertions reflect the additional Reconciler hop.
M6.2b's Phase 2 will ship a re-share-path E2E test that monkeypatches
the dispatch to return `has_gaps=True`.

Architectural value: this test exercises the full M6 backfill
framework end-to-end WITHOUT any per-source planner or fetcher
implementation. M6.3-M6.6 will plug real planners + fetchers into
the dispatch tables; the framework is now proven correct without
that per-source code. The test_planner + test_fetcher used here
are the M6.3-M6.6 stand-ins. The Reconciler dispatch uses the
default-clean stub (per the M6.2b A17 amendment) — no
monkeypatching needed for the clean-path test.

What this test proves end-to-end:

  1. OAuth callback writes `onboarding_triggers` row.
  2. M6.1 oauth_poller (subprocess #1) claims trigger →
     onboarding_run_created emitted.
  3. M6.1 tenant_onboarding (subprocess #2) consumes signal →
     source_onboarding_requested per active install (1 in our
     test: slack).
  4. M6.2a source_onboarding (subprocess #3) consumes →
     test_planner returns 2 shards → INSERT 2 onboarding_shards
     rows → emit 2 shard_fetch_requested signals.
  5. M6.2a shard_fetch (subprocess #4) consumes each →
     test_fetcher returns 5 records + end_of_data per shard →
     N1 advance to Kafka (10 records total) → emit 2
     shard_fetch_completed.
  6. SourceOnboarding consumes both completions → marks parent
     source_onboarding_runs 'completed' → emits
     source_onboarding_completed to TenantOnboarding.
  7. TenantOnboarding consumes → marks onboarding_runs 'complete' →
     emits tenant_onboarding_completed to Bridge inbox.
  8. SIGTERM all four subprocesses; rc=0 within 15s each.

The chain spans all six signal kinds across M6.1 + M6.2a:
  onboarding_run_created → source_onboarding_requested →
  shard_fetch_requested (×2) → shard_fetch_completed (×2) →
  source_onboarding_completed → tenant_onboarding_completed.

Synchronization: Postgres-state-as-checkpoint, no asyncio.sleep
and hope (M6.0/M6.1/M6.2a precedent).

Note on test fetcher latency: real per-source fetchers in
M6.3-M6.6 will have natural API-call latency. The test fetcher
returns instantly, which is fine for the chain-completes-correctly
verification — it's just unusually fast vs. production behavior.
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
    RAW_TOPIC,
    SIGNAL_KIND_COMPLETED as SHARD_FETCH_COMPLETED,
    SIGNAL_KIND_REQUESTED as SHARD_FETCH_REQUESTED,
)
from services.ingestion.workflows.source_onboarding import (
    SHARD_FETCH_INBOX_ID,
    SHARD_FETCH_INBOX_KIND,
    SIGNAL_KIND_COMPLETED as SOURCE_ONBOARDING_COMPLETED,
    SIGNAL_KIND_REQUESTED as SOURCE_ONBOARDING_REQUESTED,
    TENANT_ONBOARDING_INBOX_ID,
    TENANT_ONBOARDING_INBOX_KIND,
)
from services.ingestion.workflows.tenant_onboarding import (
    BRIDGE_INBOX_ID,
    BRIDGE_INBOX_KIND,
    SIGNAL_KIND_RUN_CREATED as ONBOARDING_RUN_CREATED,
    SIGNAL_KIND_TENANT_COMPLETED as TENANT_ONBOARDING_COMPLETED,
)


pytestmark = [pytest.mark.timeout(240)]


# Test planner + fetcher are materialized into tests/_helpers/ so
# both source_onboarding and shard_fetch subprocesses can import
# them (and the import installs the dispatch overrides).
def _ensure_e2e_helpers() -> str:
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    os.makedirs(helpers_dir, exist_ok=True)
    init_py = os.path.join(helpers_dir, "__init__.py")
    if not os.path.exists(init_py):
        with open(init_py, "w") as f:
            f.write("# Test helpers for M6.2a subprocess tests.\n")

    content = '''"""Subprocess-loadable test planner + fetcher for the
M6.2a Phase 3 four-subprocess end-to-end test. Installs both into
their respective dispatch tables on import.

Test planner: returns 2 shards for source='slack' (channel C001
and C002 windows).

Test fetcher: returns 5 records on the first call (cursor is
None), then end_of_data=True with empty records on the second
call. One page per shard; 10 records total across 2 shards.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


async def _e2e_test_planner(ctx: PlannerContext) -> list[Shard]:
    return [
        Shard(
            shard_kind="slack_channel_window",
            shard_identifier={"channel_id": "C001"},
            recency_score=1.0,
        ),
        Shard(
            shard_kind="slack_channel_window",
            shard_identifier={"channel_id": "C002"},
            recency_score=0.9,
        ),
    ]


async def _e2e_test_fetcher(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    if cursor is not None:
        # Second call — end-of-data with no records.
        return FetchResult(records=[], next_cursor=None, end_of_data=True)
    channel = shard_identifier.get("channel_id", "?")
    records = [
        {"channel": channel, "ts": f"1700000000.{i:06d}", "text": f"msg-{i}"}
        for i in range(5)
    ]
    return FetchResult(
        records=records,
        next_cursor={"page": 0},
        end_of_data=False,
    )


# Install both into the dispatch tables at import time.
PLANNER_DISPATCH["slack"] = _e2e_test_planner
FETCHER_DISPATCH["slack"] = _e2e_test_fetcher
'''
    helpers_file = os.path.join(helpers_dir, "e2e_test_dispatch.py")
    with open(helpers_file, "w") as f:
        f.write(content)
    return helpers_dir


def _env_for(
    *, instance_var: str, instance_value: str, helpers_dir: str,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"
    env["PYTHONPATH"] = helpers_dir + os.pathsep + env.get("PYTHONPATH", "")
    env[instance_var] = instance_value
    if extra:
        env.update(extra)
    return env


# ---------------------------------------------------------------------
# The test.
# ---------------------------------------------------------------------
async def test_oauth_trigger_to_source_completion_end_to_end(
    fresh_db: asyncpg.Pool,
) -> None:
    """Four-subprocess full-chain integration test for M6.2a.

    Setup:
      - Tenant with slack provider install.
      - One onboarding_triggers row (the OAuth callback's output).
      - Test planner installed for slack: returns 2 shards.
      - Test fetcher installed for slack: returns 5 records then EOD.

    Run:
      - Spawn 4 subprocesses (poller, orchestrator, source, shard).
      - Poll Postgres for each milestone in the chain.
      - SIGTERM all 4; require rc=0.
    """
    helpers_dir = _ensure_e2e_helpers()

    # Seed.
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"e2e-m62a-{tid.hex[:8]}",
    )
    await fresh_db.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'slack', $3, TRUE)
        """,
        uuid7(), tid, f"inst-{tid.hex[:8]}",
    )
    trigger_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, 'slack', 'install', '{}'::jsonb)
        """,
        trigger_id, tid,
    )

    # Instance names — each subprocess writes a workflow_states row
    # under its own name for ops introspection.
    poller_instance = f"e2e-poll-{tid.hex[:6]}"
    orch_instance = f"e2e-orch-{tid.hex[:6]}"
    src_instance = f"e2e-src-{tid.hex[:6]}"
    shf_instance = f"e2e-shf-{tid.hex[:6]}"
    rec_instance = f"e2e-rec-{tid.hex[:6]}"

    # Bootstrap for the two services that need monkeypatched dispatch.
    # The other three subprocesses (poller, orchestrator, reconciler)
    # don't use the planner/fetcher dispatch tables at all, so they
    # don't need the import bootstrap. The reconciler uses its OWN
    # dispatch (RECONCILER_DISPATCH); for this clean-path test, the
    # default-clean stub is fine — no monkeypatching needed in the
    # subprocess.
    bootstrap_for_dispatch_services = (
        "import e2e_test_dispatch; "
        "from {svc_main} import main; main()"
    )

    procs: dict[str, subprocess.Popen | None] = {
        "poller": None, "orch": None, "src": None,
        "shf": None, "rec": None,
    }

    try:
        # ----- Start subprocess 1: oauth_poller -----
        procs["poller"] = subprocess.Popen(
            [sys.executable, "-m", "services.ingestion.workflows.oauth_poller"],
            env=_env_for(
                instance_var="OAUTH_POLLER_INSTANCE",
                instance_value=poller_instance,
                helpers_dir=helpers_dir,
                extra={
                    "OAUTH_POLLER_TICK_SEC": "0.1",
                    "OAUTH_POLLER_BATCH": "5",
                },
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for trigger to be consumed.
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            consumed = await fresh_db.fetchval(
                "SELECT consumed_at FROM onboarding_triggers WHERE id = $1",
                trigger_id,
            )
            if consumed is not None:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("Poller did not consume trigger within 30s.")

        # Confirm onboarding_run_created signal exists (M6.1 invariant).
        run_id = await fresh_db.fetchval(
            "SELECT id FROM onboarding_runs WHERE tenant_id = $1", tid,
        )
        assert run_id is not None
        n_run_created = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE signal_kind = $1 AND idempotency_key = $2",
            ONBOARDING_RUN_CREATED, str(run_id),
        ))
        assert n_run_created == 1

        # ----- Start subprocess 2: tenant_onboarding -----
        procs["orch"] = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.workflows.tenant_onboarding"],
            env=_env_for(
                instance_var="ORCHESTRATOR_INSTANCE",
                instance_value=orch_instance,
                helpers_dir=helpers_dir,
                extra={
                    "ORCHESTRATOR_TICK_SEC": "0.1",
                    "ORCHESTRATOR_BATCH": "20",
                },
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for source_onboarding_requested signal (1 per
        # active install — we seeded only slack).
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            n_req = int(await fresh_db.fetchval(
                "SELECT count(*) FROM workflow_signals "
                "WHERE workflow_kind = $1 AND workflow_id = $2 "
                "AND signal_kind = $3",
                "source_onboarding", "source_onboarding",
                SOURCE_ONBOARDING_REQUESTED,
            ))
            if n_req >= 1:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError(
                "tenant_onboarding did not emit source_onboarding_requested "
                "within 30s of poller completing."
            )

        # Parent run should be 'running' now.
        parent_status = await fresh_db.fetchval(
            "SELECT status FROM onboarding_runs WHERE id = $1", run_id,
        )
        assert parent_status == "running"

        # ----- Start subprocess 3: source_onboarding (with test planner) -----
        procs["src"] = subprocess.Popen(
            [sys.executable, "-c",
             bootstrap_for_dispatch_services.format(
                 svc_main="services.ingestion.workflows.source_onboarding",
             )],
            env=_env_for(
                instance_var="SOURCE_ONBOARDING_INSTANCE",
                instance_value=src_instance,
                helpers_dir=helpers_dir,
                extra={
                    "SOURCE_ONBOARDING_TICK_SEC": "0.1",
                    "SOURCE_ONBOARDING_BATCH": "20",
                },
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for 2 shards created + 2 shard_fetch_requested emitted.
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            n_shards = int(await fresh_db.fetchval(
                "SELECT count(*) FROM onboarding_shards "
                "WHERE onboarding_run_id = $1",
                run_id,
            ))
            n_shard_req = int(await fresh_db.fetchval(
                "SELECT count(*) FROM workflow_signals "
                "WHERE workflow_kind = $1 AND workflow_id = $2 "
                "AND signal_kind = $3",
                SHARD_FETCH_INBOX_KIND, SHARD_FETCH_INBOX_ID,
                SHARD_FETCH_REQUESTED,
            ))
            if n_shards == 2 and n_shard_req == 2:
                break
            await asyncio.sleep(0.1)
        else:
            stderr = procs["src"].stderr.read().decode() if procs["src"].stderr else ""
            raise AssertionError(
                f"source_onboarding did not fan out to 2 shards within "
                f"30s. n_shards={n_shards}, n_shard_req={n_shard_req}. "
                f"stderr: {stderr[:1000]}"
            )

        # source_onboarding_runs row in_progress.
        src_status = await fresh_db.fetchval(
            "SELECT status FROM source_onboarding_runs "
            "WHERE onboarding_run_id = $1 AND source = 'slack'",
            run_id,
        )
        assert src_status == "in_progress"

        # ----- Start subprocess 4: shard_fetch (with test fetcher) -----
        procs["shf"] = subprocess.Popen(
            [sys.executable, "-c",
             bootstrap_for_dispatch_services.format(
                 svc_main="services.ingestion.workflows.shard_fetch",
             )],
            env=_env_for(
                instance_var="SHARD_FETCH_INSTANCE",
                instance_value=shf_instance,
                helpers_dir=helpers_dir,
                extra={
                    "SHARD_FETCH_TICK_SEC": "0.1",
                    "SHARD_FETCH_BATCH": "5",
                    "SHARD_FETCH_LEASE_SEC": "30.0",
                    "SHARD_FETCH_FLUSH_SEC": "2.0",
                },
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for both shards to reach 'done' state.
        deadline = time.monotonic() + 60.0  # fetch loop + Kafka + signals
        while time.monotonic() < deadline:
            n_done = int(await fresh_db.fetchval(
                "SELECT count(*) FROM onboarding_shards "
                "WHERE onboarding_run_id = $1 AND state = 'done'",
                run_id,
            ))
            if n_done == 2:
                break
            await asyncio.sleep(0.2)
        else:
            stderr = procs["shf"].stderr.read().decode() if procs["shf"].stderr else ""
            raise AssertionError(
                f"shard_fetch did not complete both shards within 60s. "
                f"n_done={n_done}. stderr: {stderr[:1500]}"
            )

        # 2 shard_fetch_completed signals emitted to source_onboarding.
        n_shard_completed = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = 'source_onboarding' "
            "AND signal_kind = $1",
            SHARD_FETCH_COMPLETED,
        ))
        assert n_shard_completed == 2

        # Wait for source_onboarding to roll up completions.
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            src_status = await fresh_db.fetchval(
                "SELECT status FROM source_onboarding_runs "
                "WHERE onboarding_run_id = $1 AND source = 'slack'",
                run_id,
            )
            if src_status == "completed":
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError(
                f"source_onboarding_runs did not reach 'completed' "
                f"within 30s of both shards done. status={src_status!r}"
            )

        # M6.2b chain change: SourceOnboarding emits source_shards_completed
        # to Reconciler (not source_onboarding_completed direct to
        # TenantOnboarding). Verify the emit landed before starting
        # Reconciler.
        n_shards_completed = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = 'reconciler' "
            "AND signal_kind = 'source_shards_completed' "
            "AND idempotency_key = $1",
            f"{run_id}:slack:pass_0",
        ))
        assert n_shards_completed == 1, (
            f"Expected source_shards_completed emit to Reconciler "
            f"inbox post-M6.2a-rollup; got {n_shards_completed}."
        )

        # ----- Start subprocess 5: reconciler -----
        # Default-clean stub for slack — no monkeypatching needed.
        procs["rec"] = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.workflows.reconciler"],
            env=_env_for(
                instance_var="RECONCILER_INSTANCE",
                instance_value=rec_instance,
                helpers_dir=helpers_dir,
                extra={
                    "RECONCILER_TICK_SEC": "0.1",
                    "RECONCILER_BATCH": "20",
                },
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for Reconciler to stamp reconciled_at + emit
        # source_onboarding_completed to TenantOnboarding.
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            reconciled = await fresh_db.fetchval(
                "SELECT reconciled_at FROM source_onboarding_runs "
                "WHERE onboarding_run_id = $1 AND source = 'slack'",
                run_id,
            )
            if reconciled is not None:
                break
            await asyncio.sleep(0.1)
        else:
            stderr = procs["rec"].stderr.read().decode() if procs["rec"].stderr else ""
            raise AssertionError(
                f"Reconciler did not stamp reconciled_at within 30s. "
                f"stderr: {stderr[:1000]}"
            )

        # source_onboarding_completed emitted to tenant_onboarding by
        # Reconciler (the M6.2b chain change — this emit now comes
        # from Reconciler on the CLEAN path, not from SourceOnboarding).
        n_src_completed = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
            SOURCE_ONBOARDING_COMPLETED, f"{run_id}:slack",
        ))
        assert n_src_completed == 1

        # Wait for tenant_onboarding to complete the parent run +
        # emit tenant_onboarding_completed to Bridge.
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            run_status = await fresh_db.fetchval(
                "SELECT status FROM onboarding_runs WHERE id = $1",
                run_id,
            )
            if run_status == "complete":
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError(
                f"Parent onboarding_runs did not reach 'complete' "
                f"within 30s. status={run_status!r}"
            )

        # LOAD-BEARING — final chain output: tenant_onboarding_completed
        # in Bridge inbox.
        bridge_signal = await fresh_db.fetchrow(
            "SELECT signal_data FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            BRIDGE_INBOX_KIND, BRIDGE_INBOX_ID,
            TENANT_ONBOARDING_COMPLETED, str(run_id),
        )
        assert bridge_signal is not None, (
            "tenant_onboarding_completed did not land in Bridge inbox. "
            "The full M6 chain (oauth_poller → tenant_onboarding → "
            "source_onboarding → shard_fetch → back up the chain) is "
            "broken."
        )

        # Optional: 10 records published to ingestion.raw across 2 shards
        # × 5 records each. We don't have a Kafka consumer in this test,
        # but we can verify the shards' workflow_states show pages_fetched
        # advanced past their initial state.
        for shard_id in await fresh_db.fetch(
            "SELECT id FROM onboarding_shards WHERE onboarding_run_id = $1",
            run_id,
        ):
            ws = await fresh_db.fetchrow(
                "SELECT state_data FROM workflow_states "
                "WHERE workflow_kind = 'shard_fetch' AND workflow_id = $1",
                str(shard_id["id"]),
            )
            assert ws is not None
            data_raw = ws["state_data"]
            data = (
                orjson.loads(data_raw) if isinstance(data_raw, (str, bytes))
                else dict(data_raw)
            )
            # Two fetcher calls per shard: first returns 5 records +
            # cursor; second returns end_of_data. Both advance the N1
            # state. pages_fetched should be 2.
            assert data.get("pages_fetched") == 2, (
                f"Shard {shard_id['id']} workflow_states pages_fetched="
                f"{data.get('pages_fetched')}; expected 2."
            )
            assert data.get("end_of_data") is True

        # ----- SIGTERM all 4 subprocesses; require rc=0 within 15s. -----
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
                raise AssertionError(
                    f"{name} subprocess did NOT exit within 15s of "
                    f"SIGTERM. stderr: {stderr[:1000]}"
                )
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            assert rc == 0, (
                f"{name} subprocess exited with rc={rc}. "
                f"stderr: {stderr[:1000]}"
            )

    finally:
        for proc in procs.values():
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
