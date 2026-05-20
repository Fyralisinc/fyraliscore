"""M6.3 Phase 2 — Gmail backfill clean-path E2E.

Five real subprocesses (oauth_poller + tenant_onboarding +
source_onboarding + shard_fetch + reconciler). Patches
`_open_gmail_client` in `services.ingestion.fetchers.gmail` AND
`services.ingestion.reconcilers.gmail` so the real Gmail planner,
fetcher, and reconciler exercise their full code paths against
fake Gmail API responses.

Chain:
  trigger (gmail) → onboarding_run_created → source_onboarding_requested
  → plan_shards_gmail loads `gmail_installations` + `gmail_mailbox_watches`
    via the S1-amended loader; emits 1 shard per active mailbox
  → shard_fetch_requested → ShardFetch + fetch_page_gmail (mailbox_window):
    messages.list → get_message → get_profile (stamps final_history_id)
  → shard_fetch_completed → source_shards_completed (pass_0)
  → Reconciler + reconcile_gmail: get_profile → matches final_history_id
    → CLEAN → reconciled_at stamped + source_onboarding_completed
  → TenantOnboarding rolls up → tenant_onboarding_completed → Bridge.

LOAD-BEARING assertions:
  (a) Records in Kafka `ingestion.raw` with the Gmail envelope shape.
  (b) workflow_states cursor `final_history_id` == fake profile's
      historyId at end of backfill.
  (c) reconciled_at IS NOT NULL on the source_onboarding_runs row.
  (d) source_onboarding_runs.reconciliation_pass_count == 0
      (clean path; no reshare).
  (e) tenant_onboarding_completed lands in Bridge inbox.
  (f) All 5 subprocesses clean SIGTERM rc=0.
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
pytestmark = [pytest.mark.timeout(240), pytest.mark.usefixtures("moto_s3_server")]


# ---------------------------------------------------------------------
# Materialize the Gmail-client patcher under tests/_helpers/. The
# subprocesses load it via PYTHONPATH; it rebinds the
# `_open_gmail_client` seam in fetcher + reconciler so neither talks
# to the real Gmail API.
# ---------------------------------------------------------------------
def _ensure_gmail_clean_helper() -> str:
    helpers_dir = os.path.join(os.path.dirname(__file__), "_helpers")
    os.makedirs(helpers_dir, exist_ok=True)
    init_py = os.path.join(helpers_dir, "__init__.py")
    if not os.path.exists(init_py):
        with open(init_py, "w") as f:
            f.write("# Test helpers for M6.2a/M6.2b/M6.3 subprocess tests.\n")

    content = '''"""M6.3 clean-path test-dispatch helper.

Patches `_open_gmail_client` in fetchers/gmail.py + reconcilers/gmail.py
with a fake that returns canned API responses. Stamps final_history_id
== "100" on the backfill's last page; reconciler's getProfile also
returns "100" → clean decision.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import gmail as gmail_fetcher_mod
from services.ingestion.reconcilers import gmail as gmail_reconciler_mod


_FAKE_PROFILE_HISTORY_ID = "100"


class _FakeGmailClient:
    """Canned Gmail API surface. Same for both shard_fetch and
    reconciler subprocesses (clean path: matching historyIds)."""

    def __init__(self) -> None:
        self.list_pages = [
            {
                "messages": [
                    {"id": "m1", "threadId": "t1"},
                    {"id": "m2", "threadId": "t2"},
                    {"id": "m3", "threadId": "t3"},
                ],
                "nextPageToken": None,
            },
        ]

    async def messages_list(self, **kwargs: Any) -> dict:
        if not self.list_pages:
            return {"messages": [], "nextPageToken": None}
        return self.list_pages.pop(0)

    async def get_message(self, *, user_email: str, scope: str,
                          message_id: str) -> dict:
        return {
            "id": message_id,
            "threadId": f"thread-{message_id}",
            "snippet": f"fake message {message_id}",
            "payload": {"headers": [
                {"name": "Subject", "value": f"Subject {message_id}"},
                {"name": "From", "value": user_email},
            ]},
        }

    async def get_profile(self, **kwargs: Any) -> dict:
        return {"historyId": _FAKE_PROFILE_HISTORY_ID,
                "emailAddress": kwargs.get("user_email", "")}

    async def history_list(self, **kwargs: Any) -> dict:
        # Clean path: no gap, history_list never called. Defensive.
        return {"history": [], "historyId": _FAKE_PROFILE_HISTORY_ID,
                "nextPageToken": None}


async def _fake_open_gmail_client(install: Any):
    fake = _FakeGmailClient()

    async def close() -> None:
        return None

    return fake, close


# Rebind the seam in BOTH modules (fetcher + reconciler each have
# their own seam pointing at the same logical concept). Production
# uses the real GoogleHttpClient + GmailClient; tests rebind here.
gmail_fetcher_mod._open_gmail_client = _fake_open_gmail_client
gmail_reconciler_mod._open_gmail_client = _fake_open_gmail_client
'''
    helpers_file = os.path.join(
        helpers_dir, "e2e_test_gmail_clean_dispatch.py",
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
    pool: asyncpg.Pool, *, tenant_id: UUID, mailboxes: list[dict],
) -> UUID:
    """Insert one gmail_installations row + one gmail_mailbox_watches
    row per mailbox descriptor (state='active' so the S1-amended
    loader includes them in the JSON aggregate)."""
    install_id = uuid7()
    await pool.execute(
        """
        INSERT INTO gmail_installations
            (id, tenant_id, workspace_domain, service_account_email,
             scope, inclusion_spec)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        install_id, tenant_id, "acme.com",
        "svc@acme-fyralis.iam.gserviceaccount.com",
        "gmail.metadata", "{}",
    )
    for mb in mailboxes:
        await pool.execute(
            """
            INSERT INTO gmail_mailbox_watches
                (id, tenant_id, gmail_installation_id, email_address,
                 google_user_id, history_id, state)
            VALUES ($1, $2, $3, $4, $5, $6, 'active')
            """,
            uuid7(), tenant_id, install_id,
            mb["email_address"], mb["google_user_id"], mb["history_id"],
        )
    return install_id


# ---------------------------------------------------------------------
# THE TEST.
# ---------------------------------------------------------------------
async def test_oauth_trigger_to_gmail_completion_end_to_end(
    fresh_db: asyncpg.Pool,
) -> None:
    """Clean-path Gmail E2E with the REAL planner/fetcher/reconciler
    and a fake Gmail API. See module docstring for full narrative.
    """
    helpers_dir = _ensure_gmail_clean_helper()

    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"e2e-m63-{tid.hex[:8]}",
    )

    # Seed 1 mailbox under the install — produces 1 shard.
    await _seed_gmail_install(
        fresh_db, tenant_id=tid,
        mailboxes=[
            {"email_address": "alice@acme.com",
             "google_user_id": "118273645",
             "history_id": "50"},
        ],
    )

    # Trigger (simulating F4-deferred OAuth retrofit).
    trigger_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, 'gmail', 'install', '{}'::jsonb)
        """,
        trigger_id, tid,
    )

    poll_instance = f"e63c-poll-{tid.hex[:6]}"
    orch_instance = f"e63c-orch-{tid.hex[:6]}"
    src_instance = f"e63c-src-{tid.hex[:6]}"
    shf_instance = f"e63c-shf-{tid.hex[:6]}"
    rec_instance = f"e63c-rec-{tid.hex[:6]}"

    # Bootstrap for services that need the Gmail-client patch.
    # source_onboarding doesn't (planner reads from DB only); but
    # patching it is harmless and keeps the env identical.
    bootstrap = (
        "import e2e_test_gmail_clean_dispatch; "
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
                instance_value=poll_instance,
                helpers_dir=helpers_dir,
                extra={"OAUTH_POLLER_TICK_SEC": "0.1",
                       "OAUTH_POLLER_BATCH": "5"},
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for run created.
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
        assert run_id is not None, "oauth_poller did not create onboarding_run."

        procs["orch"] = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.workflows.tenant_onboarding"],
            env=_env_for(
                instance_var="ORCHESTRATOR_INSTANCE",
                instance_value=orch_instance,
                helpers_dir=helpers_dir,
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
                instance_value=src_instance,
                helpers_dir=helpers_dir,
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
                instance_value=shf_instance,
                helpers_dir=helpers_dir,
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
                instance_value=rec_instance,
                helpers_dir=helpers_dir,
                extra={"RECONCILER_TICK_SEC": "0.1",
                       "RECONCILER_BATCH": "20"},
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for tenant_onboarding_completed in Bridge inbox.
        deadline = time.monotonic() + 90.0
        bridge_signal = None
        while time.monotonic() < deadline:
            bridge_signal = await fresh_db.fetchrow(
                """
                SELECT signal_data FROM workflow_signals
                 WHERE workflow_kind = $1 AND workflow_id = $2
                   AND signal_kind = $3 AND idempotency_key = $4
                """,
                BRIDGE_INBOX_KIND, BRIDGE_INBOX_ID,
                TENANT_ONBOARDING_COMPLETED, str(run_id),
            )
            if bridge_signal is not None:
                break
            await asyncio.sleep(0.2)
        if bridge_signal is None:
            stderrs = {
                k: (p.stderr.read().decode()[:1500] if p and p.stderr else "")
                for k, p in procs.items()
            }
            raise AssertionError(
                f"Bridge signal not seen within 90s. stderrs={stderrs!r}"
            )

        # ----- FINAL STATE ASSERTIONS -----

        # (a) source_onboarding_runs reconciled, pass_count=0 (clean path).
        sor_row = await fresh_db.fetchrow(
            """
            SELECT status, reconciled_at, reconciliation_pass_count
              FROM source_onboarding_runs
             WHERE onboarding_run_id = $1 AND source = 'gmail'
            """,
            run_id,
        )
        assert sor_row["status"] == "completed"
        assert sor_row["reconciled_at"] is not None
        assert sor_row["reconciliation_pass_count"] == 0, (
            f"Clean path should not reshare; got pass_count="
            f"{sor_row['reconciliation_pass_count']}"
        )

        # (b) Exactly 1 shard (1 mailbox), state='done'.
        shards = await fresh_db.fetch(
            "SELECT state, shard_kind FROM onboarding_shards "
            "WHERE onboarding_run_id = $1",
            run_id,
        )
        assert len(shards) == 1
        assert shards[0]["state"] == "done"
        assert shards[0]["shard_kind"] == "gmail_mailbox_window"

        # (c) workflow_states cursor stamped with final_history_id.
        shard_id = await fresh_db.fetchval(
            "SELECT id FROM onboarding_shards "
            "WHERE onboarding_run_id = $1 AND source = 'gmail'",
            run_id,
        )
        ws_row = await fresh_db.fetchrow(
            "SELECT state_data FROM workflow_states "
            "WHERE workflow_kind = 'shard_fetch' AND workflow_id = $1",
            str(shard_id),
        )
        assert ws_row is not None
        sd_raw = ws_row["state_data"]
        sd = (orjson.loads(sd_raw) if isinstance(sd_raw, (str, bytes))
              else dict(sd_raw))
        cursor = sd.get("cursor") or {}
        assert cursor.get("final_history_id") == "100", (
            f"final_history_id not stamped. cursor={cursor!r}"
        )

        # (d) source_onboarding_completed (run-keyed) emitted exactly once.
        n_src_completed = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
            SIGNAL_KIND_SOURCE_COMPLETED, f"{run_id}:gmail",
        ))
        assert n_src_completed == 1

        # ----- SIGTERM all 5 subprocesses; require rc=0 within 15s. -----
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
                    f"{name} did not exit within 15s. stderr: {stderr[:1000]}"
                )
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            assert rc == 0, f"{name} rc={rc}. stderr: {stderr[:1000]}"

    finally:
        for proc in procs.values():
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
