"""M6.3 — N1 invariant verification with the real Gmail fetcher.

Same shape as M6.2a Phase 2's `test_shard_fetch_N1_invariant_holds`,
but with the Gmail fetcher (not a synthetic per-page test fetcher)
running inside the real `ShardFetch` service against a fake Gmail
client.

The load-bearing assertion: Kafka flush failure during page 2 →
cursor reflects post-page-1, NOT post-page-2. Shard stays
'in_progress'. The "publish-then-advance" invariant holds end-to-
end with the real per-source code.

If this test fails, the regression is in EITHER the Gmail fetcher
OR the N1 primitive. Either way, surface as a substrate finding
before doing anything else.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingestion.fetchers import gmail as gmail_fetcher
from services.ingestion.fetchers.gmail import SHARD_KIND_MAILBOX_WINDOW
from services.ingestion.workflows.shard_fetch import (
    RAW_TOPIC,
    SIGNAL_KIND_COMPLETED,
    SIGNAL_KIND_REQUESTED,
    SOURCE_ONBOARDING_INBOX_ID,
    SOURCE_ONBOARDING_INBOX_KIND,
    ShardFetch,
    ShardFetchConfig,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)
from services.ingestion.workflows.signals import emit_signal
from services.ingestion.workflows.state import load_state


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Same producer / service factory shape as test_shard_fetch.py.
# =====================================================================
class _CapturingProducer:
    def __init__(self, flush_returns: list[int] | None = None) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []
        self.flush_calls = 0
        self._flush_returns = list(flush_returns or [])

    async def produce(
        self, topic: str, value: bytes, *,
        key: bytes | None = None, **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        self.flush_calls += 1
        if self._flush_returns:
            return self._flush_returns.pop(0)
        return 0


class _FakeGmailClient:
    """Returns canned `messages.list` pages + canned `messages.get`
    responses + canned `getProfile`."""

    def __init__(
        self,
        *,
        list_pages: list[dict],
        profile: dict,
    ):
        self.list_pages = list(list_pages)
        self.profile = profile
        self.list_calls = 0
        self.get_calls = 0

    async def messages_list(self, **kwargs):
        self.list_calls += 1
        return self.list_pages.pop(0)

    async def get_message(self, *, user_email, scope, message_id):
        self.get_calls += 1
        return {"id": message_id, "threadId": f"thread-{message_id}"}

    async def get_profile(self, **kwargs):
        return dict(self.profile)


async def _seed_tenant(pool: asyncpg.Pool, label: str = "gn1") -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"{label}-{tid.hex[:8]}",
    )
    return tid


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
        "svc@acme-fyralis.iam.gserviceaccount.com",
        "gmail.metadata", "{}",
    )
    # Also seed a mailbox watch so the install row has shape-equivalence
    # to a real install. ShardFetch's loader doesn't aggregate, but
    # having a real row exercises the schema correctly.
    await pool.execute(
        """
        INSERT INTO gmail_mailbox_watches
            (id, tenant_id, gmail_installation_id, email_address,
             google_user_id, history_id, state)
        VALUES ($1, $2, $3, $4, $5, $6, 'active')
        """,
        uuid7(), tenant_id, install_id, "alice@acme.com",
        "118273645", "100",
    )
    return install_id


async def _seed_onboarding_run(
    pool: asyncpg.Pool, *, tenant_id: UUID,
) -> UUID:
    run_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at)
        VALUES ($1, $2, 'install', $3, 'running', $4::text[], now())
        """,
        run_id, tenant_id, f"wf-{run_id.hex[:8]}", ["gmail"],
    )
    return run_id


async def _seed_source_run(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID,
) -> None:
    await pool.execute(
        """
        INSERT INTO source_onboarding_runs
            (onboarding_run_id, source, tenant_id, status)
        VALUES ($1, 'gmail', $2, 'in_progress')
        """,
        run_id, tenant_id,
    )


async def _seed_gmail_shard(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID,
) -> UUID:
    shard_id = uuid7()
    identifier = {
        "shard_kind": SHARD_KIND_MAILBOX_WINDOW,
        "mailbox_email": "alice@acme.com",
        "user_id": "118273645",
        "initial_history_id": "100",
    }
    await pool.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, created_at)
        VALUES ($1, $2, $3, 'gmail', $4, $5::jsonb, 1.0,
                'pending', now())
        """,
        shard_id, run_id, tenant_id, SHARD_KIND_MAILBOX_WINDOW,
        orjson.dumps(identifier).decode("utf-8"),
    )
    return shard_id


async def _emit_shard_requested(
    pool: asyncpg.Pool, *, shard_id: UUID, run_id: UUID,
    tenant_id: UUID,
) -> None:
    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_REQUESTED,
        idempotency_key=str(shard_id),
        signal_data={
            "shard_id": str(shard_id),
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tenant_id),
            "source": "gmail",
        },
    )


def _service(
    pool: asyncpg.Pool, producer: _CapturingProducer,
) -> ShardFetch:
    return ShardFetch(
        pool, producer,
        config=ShardFetchConfig(
            tick_interval_seconds=0.01,
            max_signals_per_tick=20,
            lease_timeout_seconds=30.0,
            flush_timeout_seconds=1.0,
        ),
    )


# =====================================================================
# LOAD-BEARING: N1 invariant holds with the real Gmail fetcher.
# =====================================================================
async def test_fetch_page_gmail_n1_invariant_holds_at_service_level(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The architecturally most important M6.3 assertion.

    Inject Gmail page sequence:
      page 0: 2 messages, has nextPageToken="p1"     ← flushes cleanly
      page 1: 3 messages, has nextPageToken="p2"     ← flush fails
      page 2: 1 message,  no nextPageToken           ← never reached

    Assert (mirroring M6.2a's test):
      (a) Page-0 (2) + page-1 (3) records published via produce().
      (b) workflow_states cursor reflects POST-PAGE-0 state — `page_token="p1"`,
          NOT `page_token="p2"`. This is THE load-bearing assertion.
      (c) Both flush calls observed.
      (d) Shard stays 'in_progress'.
      (e) shard_fetch_completed NOT emitted.
    """
    fake = _FakeGmailClient(
        list_pages=[
            {
                "messages": [{"id": "m1"}, {"id": "m2"}],
                "nextPageToken": "p1",
            },
            {
                "messages": [{"id": "m3"}, {"id": "m4"}, {"id": "m5"}],
                "nextPageToken": "p2",
            },
            {
                "messages": [{"id": "m6"}],
                "nextPageToken": None,
            },
        ],
        profile={"historyId": "999"},
    )

    async def fake_open(install):
        async def close():
            pass
        return fake, close

    monkeypatch.setattr(gmail_fetcher, "_open_gmail_client", fake_open)

    tid = await _seed_tenant(fresh_db)
    await _seed_gmail_install(fresh_db, tenant_id=tid)
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _seed_source_run(fresh_db, run_id=run_id, tenant_id=tid)
    shard_id = await _seed_gmail_shard(
        fresh_db, run_id=run_id, tenant_id=tid,
    )
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id, tenant_id=tid,
    )

    # Flush returns: 0 (success on page 0) then 3 (3 messages still
    # queued — failure on page 1).
    producer = _CapturingProducer(flush_returns=[0, 3])
    await _service(fresh_db, producer).run(max_ticks=1)

    # (a) Both pages' records went through produce() (N1 enqueues
    # before flushing).
    assert len(producer.published) == 5, (
        f"Expected 5 produce() calls (page-0: 2 + page-1: 3); got "
        f"{len(producer.published)}. The Gmail fetcher may not be "
        f"yielding records in the framework envelope shape."
    )

    # (b) **LOAD-BEARING.** Cursor reflects POST-PAGE-0 — page_token
    # is "p1" (the token returned BY page 0 as the next page) — NOT
    # "p2" (which would be page 1's nextPageToken).
    ws = await load_state(fresh_db, WORKFLOW_KIND, str(shard_id))
    assert ws is not None
    cursor = ws.state_data.get("cursor")
    assert isinstance(cursor, dict), (
        f"cursor is not a dict: {cursor!r}"
    )
    assert cursor.get("page_token") == "p1", (
        f"N1 INVARIANT BROKEN: cursor advanced past page 1's flush "
        f"failure. Got page_token={cursor.get('page_token')!r}; "
        f"expected 'p1' (the post-page-0 token). The "
        f"advance_cursor_atomic_with_kafka_publish primitive may be "
        f"updating state BEFORE the flush succeeds; surface as a "
        f"substrate finding (same shape as A12/A13/A15/A16) — and "
        f"check fetchers/gmail.py for any direct state mutation."
    )
    # And final_history_id was NOT stamped (only happens on last page).
    assert cursor.get("final_history_id") is None, (
        f"final_history_id stamped prematurely: "
        f"{cursor.get('final_history_id')!r}. Reconciler must not "
        f"see this value until ShardFetch completes the last page."
    )

    # (c) Both flush calls observed.
    assert producer.flush_calls == 2

    # (d) Shard stays 'in_progress' — loop exited without marking done.
    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "in_progress", (
        f"Shard state={state!r}; expected 'in_progress' after flush "
        f"failure mid-loop."
    )

    # (e) shard_fetch_completed NOT emitted.
    completed = await fresh_db.fetchrow(
        """
        SELECT id FROM workflow_signals
         WHERE workflow_kind = $1 AND workflow_id = $2
           AND signal_kind = $3 AND idempotency_key = $4
        """,
        SOURCE_ONBOARDING_INBOX_KIND, SOURCE_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_COMPLETED, str(shard_id),
    )
    assert completed is None, (
        "shard_fetch_completed was emitted despite the shard not "
        "being terminal; the service must only emit completion on "
        "clean end-of-data or hard failure."
    )


# =====================================================================
# Happy-path service-integration: real Gmail fetcher runs to completion.
# =====================================================================
async def test_fetch_page_gmail_runs_to_completion_in_service(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror of test_shard_fetch_picks_up_request_and_calls_fetcher
    but with the real Gmail fetcher. Verifies:
      (a) Records published with the Gmail envelope shape.
      (b) Cursor advances; final_history_id stamped on last page.
      (c) Shard 'done', completion emitted.
    """
    fake = _FakeGmailClient(
        list_pages=[
            {
                "messages": [{"id": "m1"}, {"id": "m2"}],
                "nextPageToken": "p1",
            },
            {
                "messages": [{"id": "m3"}],
                "nextPageToken": None,
            },
        ],
        profile={"historyId": "999"},
    )

    async def fake_open(install):
        async def close():
            pass
        return fake, close

    monkeypatch.setattr(gmail_fetcher, "_open_gmail_client", fake_open)

    tid = await _seed_tenant(fresh_db)
    await _seed_gmail_install(fresh_db, tenant_id=tid)
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _seed_source_run(fresh_db, run_id=run_id, tenant_id=tid)
    shard_id = await _seed_gmail_shard(
        fresh_db, run_id=run_id, tenant_id=tid,
    )
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id, tenant_id=tid,
    )

    producer = _CapturingProducer()
    await _service(fresh_db, producer).run(max_ticks=1)

    # (a) 3 records, all on ingestion.raw, all with tenant key.
    assert len(producer.published) == 3
    for topic, value, key in producer.published:
        assert topic == RAW_TOPIC
        assert key == str(tid).encode("utf-8")
        # The envelope wraps the Gmail record.
        envelope = orjson.loads(value)
        assert envelope["source"] == "gmail"
        assert envelope["tenant_id"] == str(tid)
        assert envelope["shard_id"] == str(shard_id)
        assert "message_resource" in envelope["record"]
        assert envelope["record"]["read_path"] == "backfill"

    # (b) Cursor: final_history_id stamped.
    ws = await load_state(fresh_db, WORKFLOW_KIND, str(shard_id))
    assert ws is not None
    cursor = ws.state_data.get("cursor")
    assert cursor["final_history_id"] == "999"
    assert cursor["page_token"] is None  # last page

    # (c) Shard done; completion emitted.
    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "done"
    completion = await fresh_db.fetchrow(
        """
        SELECT signal_data FROM workflow_signals
         WHERE workflow_kind = $1 AND workflow_id = $2
           AND signal_kind = $3 AND idempotency_key = $4
        """,
        SOURCE_ONBOARDING_INBOX_KIND, SOURCE_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_COMPLETED, str(shard_id),
    )
    assert completion is not None
    data_raw = completion["signal_data"]
    data = (
        orjson.loads(data_raw) if isinstance(data_raw, (str, bytes))
        else dict(data_raw)
    )
    assert data["status"] == "done"
    assert data["source"] == "gmail"
