"""M6.3 Phase 2 — Gmail backfill RE-SHARE-path E2E.

Five real subprocesses with the REAL Gmail planner/fetcher/reconciler
exercising the gap-detection + gmail_history_gap shard path against a
fake Gmail client.

Chain (Pass 0 — backfill → gap detected):
  trigger → run created → request → plan_shards_gmail (1 shard)
  → fetch_page_gmail (mailbox_window): messages.list page → get_profile
    stamps final_history_id="100"
  → shard_fetch_completed (pass_0) → reconcile_gmail: get_profile
    returns "500" → gap detected (100 → 500)
  → new gmail_history_gap shard inserted with parent_shard_id
  → original shard → 'reconciliation_resharded'
  → shard_fetch_requested for gap shard.

Chain (Pass 1 — gap fill → clean):
  fetch_page_gmail (history_gap): history.list returns historyId="500"
    → cursor's final_history_id stamped "500"
  → shard_fetch_completed → source_shards_completed (pass_1)
  → reconcile_gmail: gap shard's final_history_id="500" vs get_profile
    "500" → CLEAN → reconciled_at stamped + source_onboarding_completed
  → TenantOnboarding → tenant_onboarding_completed → Bridge.

LOAD-BEARING assertions:
  (a) reconciliation_pass_count == 1.
  (b) reconciled_at stamped.
  (c) Original mailbox_window shard ends in 'reconciliation_resharded'.
  (d) Exactly one gap shard exists with shard_kind="gmail_history_gap"
      and parent_shard_id linking back to the original.
  (e) Gap shard's final_history_id == "500".
  (f) Exactly ONE source_onboarding_completed in TenantOnboarding's
      inbox (cross-service idempotency across re-share cycle —
      same property as M6.2b reshare test).
  (g) All 5 subprocesses clean SIGTERM rc=0.
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


# A27.6: shared moto S3 server provides the raw-tier endpoint for the
# M6.7 shard_fetch producer (subprocesses inherit S3_ENDPOINT_URL).
pytestmark = [pytest.mark.timeout(300), pytest.mark.usefixtures("moto_s3_server")]


def _ensure_gmail_reshare_helper() -> str:
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    os.makedirs(helpers_dir, exist_ok=True)
    init_py = os.path.join(helpers_dir, "__init__.py")
    if not os.path.exists(init_py):
        with open(init_py, "w") as f:
            f.write("# Test helpers.\n")

    content = '''"""M6.3 reshare-path test-dispatch helper.

Patches `_open_gmail_client` in both fetchers/gmail.py and
reconcilers/gmail.py with subprocess-specific fakes. The shard_fetch
subprocess's fake stamps final_history_id="100" on backfill, then
returns historyId="500" on the gap-fill history.list call. The
reconciler subprocess's fake returns historyId="500" on every
get_profile call — meaning pass-0 (vs 100) detects gap; pass-1
(vs 500) is clean.

Stateless per subprocess: no in-process counter. The reshare cycle's
state surface is `source_onboarding_runs.reconciliation_pass_count`
+ shard states; the fake just returns canned values that produce the
right gap/clean decisions given those state surfaces.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import gmail as gmail_fetcher_mod
from services.ingestion.reconcilers import gmail as gmail_reconciler_mod


class _ShardFetchFakeGmailClient:
    """Used by the shard_fetch subprocess.

    - messages.list: 1 page, 3 messages, end_of_data
    - get_profile (during backfill last page): "100"
    - history.list (during gap-fill): historyId="500", 1 message
    """

    async def messages_list(self, **kwargs: Any) -> dict:
        return {
            "messages": [
                {"id": "m1"}, {"id": "m2"}, {"id": "m3"},
            ],
            "nextPageToken": None,
        }

    async def get_message(self, *, user_email, scope, message_id) -> dict:
        return {
            "id": message_id, "threadId": f"thread-{message_id}",
            "snippet": f"fake {message_id}",
        }

    async def get_profile(self, **kwargs: Any) -> dict:
        # During backfill: the fetcher's last page stamps
        # final_history_id from this. Pin to "100".
        return {"historyId": "100"}

    async def history_list(self, **kwargs: Any) -> dict:
        # Gap fill: 1 event with 1 message; response's historyId is
        # the gap shard's stamped final_history_id ("500").
        return {
            "history": [
                {
                    "id": "h-gap-1",
                    "messagesAdded": [{"message": {"id": "gm1"}}],
                },
            ],
            "historyId": "500",
            "nextPageToken": None,
        }


class _ReconcilerFakeGmailClient:
    """Used by the reconciler subprocess.

    - get_profile: returns "500" on every call.

    Pass 0 (after backfill): mailbox_window shard's final_history_id
    is "100". 500 > 100 → gap.

    Pass 1 (after gap fill): gap shard's final_history_id is "500".
    500 <= 500 → clean. (Original mailbox_window shard is now in
    'reconciliation_resharded' state and excluded from the check.)
    """

    async def get_profile(self, **kwargs: Any) -> dict:
        return {"historyId": "500"}

    # Defensive: not actually called by reconciler, but provide for
    # parity with the client surface.
    async def history_list(self, **kwargs: Any) -> dict:
        return {"history": [], "historyId": "500", "nextPageToken": None}

    async def messages_list(self, **kwargs: Any) -> dict:
        return {"messages": [], "nextPageToken": None}

    async def get_message(self, **kwargs: Any) -> dict:
        return {"id": kwargs.get("message_id"), "threadId": "t"}


# Different fake per subprocess based on which module the seam lives
# in. The shard_fetch subprocess's fetcher.gmail._open_gmail_client
# returns the shard-fetch fake. The reconciler subprocess's
# reconciler.gmail._open_gmail_client returns the reconciler fake.
async def _shard_fetch_open(install: Any):
    fake = _ShardFetchFakeGmailClient()
    async def close() -> None: return None
    return fake, close


async def _reconciler_open(install: Any):
    fake = _ReconcilerFakeGmailClient()
    async def close() -> None: return None
    return fake, close


# Each subprocess imports this helper. The seams in BOTH modules get
# rebound. The shard_fetch subprocess only invokes the fetcher.gmail
# seam (it doesn't import reconciler.gmail unless something triggers
# it). The reconciler subprocess only invokes reconciler.gmail's seam.
# Either way, both rebinds are safe — they don't interfere.
gmail_fetcher_mod._open_gmail_client = _shard_fetch_open
gmail_reconciler_mod._open_gmail_client = _reconciler_open
'''
    helpers_file = os.path.join(
        helpers_dir, "e2e_test_gmail_reshare_dispatch.py",
    )
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


async def _seed_gmail_install(
    pool: asyncpg.Pool, *, tenant_id: UUID,
) -> UUID:
    install_id = uuid7()
    await pool.execute(
        """
        INSERT INTO gmail_installations
            (id, tenant_id, workspace_domain, service_account_email,
             scope, inclusion_spec)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        install_id, tenant_id, "acme.com",
        "svc@acme.iam.gserviceaccount.com",
        "gmail.metadata", "{}",
    )
    await pool.execute(
        """
        INSERT INTO gmail_mailbox_watches
            (id, tenant_id, gmail_installation_id, email_address,
             google_user_id, history_id, state)
        VALUES ($1, $2, $3, $4, $5, $6, 'active')
        """,
        uuid7(), tenant_id, install_id, "alice@acme.com",
        "118273645", "50",
    )
    return install_id


async def test_oauth_trigger_to_gmail_completion_with_reshare(
    fresh_db: asyncpg.Pool,
) -> None:
    """LOAD-BEARING reshare-path E2E. See module docstring."""
    helpers_dir = _ensure_gmail_reshare_helper()

    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"e2e-m63r-{tid.hex[:8]}",
    )
    await _seed_gmail_install(fresh_db, tenant_id=tid)

    trigger_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, 'gmail', 'install', '{}'::jsonb)
        """,
        trigger_id, tid,
    )

    poll_i = f"e63r-poll-{tid.hex[:6]}"
    orch_i = f"e63r-orch-{tid.hex[:6]}"
    src_i = f"e63r-src-{tid.hex[:6]}"
    shf_i = f"e63r-shf-{tid.hex[:6]}"
    rec_i = f"e63r-rec-{tid.hex[:6]}"

    bootstrap = (
        "import e2e_test_gmail_reshare_dispatch; "
        "from {svc_main} import main; main()"
    )

    procs: dict[str, subprocess.Popen | None] = {
        "poller": None, "orch": None, "src": None,
        "shf": None, "rec": None,
    }

    try:
        procs["poller"] = subprocess.Popen(
            [sys.executable, "-m", "services.ingestion.workflows.oauth_poller"],
            env=_env_for(
                instance_var="OAUTH_POLLER_INSTANCE",
                instance_value=poll_i, helpers_dir=helpers_dir,
                extra={"OAUTH_POLLER_TICK_SEC": "0.1",
                       "OAUTH_POLLER_BATCH": "5"},
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        deadline = time.monotonic() + 30.0
        run_id: UUID | None = None
        while time.monotonic() < deadline:
            row = await fresh_db.fetchrow(
                "SELECT id FROM onboarding_runs WHERE tenant_id = $1", tid,
            )
            if row is not None:
                run_id = row["id"]
                break
            await asyncio.sleep(0.1)
        assert run_id is not None

        procs["orch"] = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.workflows.tenant_onboarding"],
            env=_env_for(
                instance_var="ORCHESTRATOR_INSTANCE",
                instance_value=orch_i, helpers_dir=helpers_dir,
                extra={"ORCHESTRATOR_TICK_SEC": "0.1",
                       "ORCHESTRATOR_BATCH": "20"},
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        procs["src"] = subprocess.Popen(
            [sys.executable, "-c",
             bootstrap.format(
                 svc_main="services.ingestion.workflows.source_onboarding",
             )],
            env=_env_for(
                instance_var="SOURCE_ONBOARDING_INSTANCE",
                instance_value=src_i, helpers_dir=helpers_dir,
                extra={"SOURCE_ONBOARDING_TICK_SEC": "0.1",
                       "SOURCE_ONBOARDING_BATCH": "20"},
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        procs["shf"] = subprocess.Popen(
            [sys.executable, "-c",
             bootstrap.format(
                 svc_main="services.ingestion.workflows.shard_fetch",
             )],
            env=_env_for(
                instance_var="SHARD_FETCH_INSTANCE",
                instance_value=shf_i, helpers_dir=helpers_dir,
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
                 svc_main="services.ingestion.workflows.reconciler",
             )],
            env=_env_for(
                instance_var="RECONCILER_INSTANCE",
                instance_value=rec_i, helpers_dir=helpers_dir,
                extra={"RECONCILER_TICK_SEC": "0.1",
                       "RECONCILER_BATCH": "20"},
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for reshare: pass_count >= 1.
        deadline = time.monotonic() + 90.0
        pass_count = 0
        while time.monotonic() < deadline:
            pass_count = int(await fresh_db.fetchval(
                "SELECT reconciliation_pass_count FROM source_onboarding_runs "
                "WHERE onboarding_run_id = $1 AND source = 'gmail'",
                run_id,
            ) or 0)
            if pass_count >= 1:
                break
            await asyncio.sleep(0.2)
        if pass_count < 1:
            stderrs = {
                k: (p.stderr.read().decode()[:1500] if p and p.stderr else "")
                for k, p in procs.items()
            }
            raise AssertionError(
                f"Reconciler did not reshare within 90s. stderrs={stderrs!r}"
            )

        # Wait for Bridge signal (full chain done).
        deadline = time.monotonic() + 90.0
        bridge = None
        while time.monotonic() < deadline:
            bridge = await fresh_db.fetchrow(
                """
                SELECT signal_data FROM workflow_signals
                 WHERE workflow_kind = $1 AND workflow_id = $2
                   AND signal_kind = $3 AND idempotency_key = $4
                """,
                BRIDGE_INBOX_KIND, BRIDGE_INBOX_ID,
                TENANT_ONBOARDING_COMPLETED, str(run_id),
            )
            if bridge is not None:
                break
            await asyncio.sleep(0.2)
        if bridge is None:
            stderrs = {
                k: (p.stderr.read().decode()[:1500] if p and p.stderr else "")
                for k, p in procs.items()
            }
            raise AssertionError(
                f"Bridge signal not seen after reshare. stderrs={stderrs!r}"
            )

        # ----- FINAL ASSERTIONS -----
        sor = await fresh_db.fetchrow(
            "SELECT status, reconciled_at, reconciliation_pass_count "
            "FROM source_onboarding_runs "
            "WHERE onboarding_run_id = $1 AND source = 'gmail'",
            run_id,
        )
        assert sor["status"] == "completed"
        assert sor["reconciled_at"] is not None
        assert sor["reconciliation_pass_count"] == 1, (
            f"Expected pass_count=1; got {sor['reconciliation_pass_count']}"
        )

        shards = await fresh_db.fetch(
            "SELECT id, state, shard_kind, parent_shard_id, shard_identifier "
            "FROM onboarding_shards WHERE onboarding_run_id = $1 "
            "ORDER BY created_at, id",
            run_id,
        )
        assert len(shards) == 2, f"Expected 2 shards; got {len(shards)}"

        # Original: mailbox_window, reconciliation_resharded.
        orig = shards[0]
        assert orig["shard_kind"] == "gmail_mailbox_window"
        assert orig["state"] == "reconciliation_resharded"
        assert orig["parent_shard_id"] is None

        # Gap: history_gap, done, parent linked.
        gap = shards[1]
        assert gap["shard_kind"] == "gmail_history_gap"
        assert gap["state"] == "done"
        assert gap["parent_shard_id"] == orig["id"]

        # Gap shard's final_history_id stamped to "500" from history.list.
        ws_row = await fresh_db.fetchrow(
            "SELECT state_data FROM workflow_states "
            "WHERE workflow_kind = 'shard_fetch' AND workflow_id = $1",
            str(gap["id"]),
        )
        sd_raw = ws_row["state_data"]
        sd = (orjson.loads(sd_raw) if isinstance(sd_raw, (str, bytes))
              else dict(sd_raw))
        assert sd.get("cursor", {}).get("final_history_id") == "500"

        # Cross-service idempotency: exactly ONE source_onboarding_completed.
        n_src_completed = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
            SIGNAL_KIND_SOURCE_COMPLETED, f"{run_id}:gmail",
        ))
        assert n_src_completed == 1, (
            f"Cross-service idempotency broken: {n_src_completed} "
            f"source_onboarding_completed signals."
        )

        # Two source_shards_completed (pass_0 + pass_1).
        n_shards_completed = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = 'reconciler' AND signal_kind = $1",
            SIGNAL_KIND_SHARDS_COMPLETED,
        ))
        assert n_shards_completed == 2

        # SIGTERM all 5 — rc=0 within 15s.
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
                    f"{name} did not exit. stderr: {stderr[:1000]}"
                )
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            assert rc == 0, f"{name} rc={rc}. stderr: {stderr[:1000]}"

    finally:
        for proc in procs.values():
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
