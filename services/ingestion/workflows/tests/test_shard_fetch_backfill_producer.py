"""M6.7 Layer 1 (A27.1) — ShardFetch backfill producer.

ShardFetch writes each fetched record's content-addressed blob to S3
and publishes a `RawEnvelope(ingress_kind="backfill")` pointer to
`ingestion.raw` — the same envelope shape the webhook shadow path
publishes. These tests verify:

  - S3 write happens BEFORE the Kafka publish (N1 ordering extension).
  - The published bytes parse as a RawEnvelope with the backfill
    ingress_kind + a populated raw_s3_key.
  - The S3 blob carries `{record, shard_context, webhook_metadata}`,
    with the fetcher's reserved `webhook_metadata` key lifted out of
    the record.
  - S3 failure marks the shard failed (A19), with a distinguishable
    error message, without crashing the service.
  - Kafka-flush failure after a successful S3 write is safe: the
    cursor doesn't advance, and a re-attempt re-writes idempotently
    (PutIfAbsent no-op) — the N1 invariant holds under the S3 path.
  - The N1 primitive's own contract is unchanged (regression).
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import orjson
import pytest

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.raw_tier.envelope import RawEnvelope
from services.ingestion.workflows.shard_fetch import (
    RAW_TOPIC,
    SIGNAL_KIND_COMPLETED,
    SOURCE_ONBOARDING_INBOX_ID,
    SOURCE_ONBOARDING_INBOX_KIND,
    ShardFetch,
    ShardFetchConfig,
    WORKFLOW_KIND,
)
from services.ingestion.workflows.state import load_state
from services.ingestion.workflows.tests._fake_s3 import FakeS3Client
from services.ingestion.workflows.tests.test_shard_fetch import (
    _CapturingProducer,
    _emit_shard_requested,
    _make_three_page_fetcher,
    _seed_onboarding_run,
    _seed_provider_install,
    _seed_shard,
    _seed_tenant,
)


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Ordering-aware fakes — record the interleaving of S3 puts + produces
# into one shared event log so we can assert S3-write-before-publish.
# =====================================================================
class _OrderingS3(FakeS3Client):
    def __init__(self, events: list[tuple[str, Any]]) -> None:
        super().__init__()
        self._events = events

    async def put_if_absent(self, key: str, body: bytes) -> None:
        self._events.append(("s3_put", key))
        await super().put_if_absent(key, body)


class _OrderingProducer(_CapturingProducer):
    def __init__(self, events: list[tuple[str, Any]],
                 flush_returns: list[int] | None = None) -> None:
        super().__init__(flush_returns=flush_returns)
        self._events = events

    async def produce(self, topic: str, value: bytes, *,
                      key: bytes | None = None, **kw: Any) -> None:
        self._events.append(("produce", topic))
        await super().produce(topic, value, key=key, **kw)


def _service(
    pool: asyncpg.Pool, producer: _CapturingProducer,
    s3_client: FakeS3Client,
) -> ShardFetch:
    return ShardFetch(
        pool, producer,
        config=ShardFetchConfig(
            tick_interval_seconds=0.01,
            max_signals_per_tick=20,
            lease_timeout_seconds=30.0,
            flush_timeout_seconds=1.0,
            ingestion_env="test",
        ),
        s3_client=s3_client,
    )


async def _seed_and_request(
    pool: asyncpg.Pool, *, source: str, shard_kind: str,
    fetcher: Any, monkeypatch: pytest.MonkeyPatch,
    identifier: dict | None = None,
) -> tuple[UUID, UUID]:
    monkeypatch.setitem(FETCHER_DISPATCH, source, fetcher)
    tid = await _seed_tenant(pool)
    await _seed_provider_install(pool, tenant_id=tid, provider=source)
    run_id = await _seed_onboarding_run(pool, tenant_id=tid, source=source)
    shard_id = await _seed_shard(
        pool, run_id=run_id, tenant_id=tid, source=source,
        shard_kind=shard_kind, identifier=identifier,
    )
    await _emit_shard_requested(
        pool, shard_id=shard_id, run_id=run_id, tenant_id=tid, source=source,
    )
    return tid, shard_id


# =====================================================================
# 1. S3 write happens before the Kafka publish.
# =====================================================================
async def test_shard_fetch_writes_to_s3_before_publishing(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, Any]] = []
    s3 = _OrderingS3(events)
    producer = _OrderingProducer(events)
    fetcher = _make_three_page_fetcher([[{"id": "x1"}]])

    tid, shard_id = await _seed_and_request(
        fresh_db, source="github", shard_kind="github_repo_events",
        fetcher=fetcher, monkeypatch=monkeypatch,
        identifier={"repo": "o/r"},
    )
    await _service(fresh_db, producer, s3).run(max_ticks=1)

    # The single record's S3 put must precede the produce of its
    # envelope pointer.
    kinds = [e[0] for e in events]
    assert "s3_put" in kinds and "produce" in kinds
    assert kinds.index("s3_put") < kinds.index("produce"), (
        f"S3 write must happen before the Kafka publish (A27.1); "
        f"event order was {kinds!r}"
    )


# =====================================================================
# 2. Published bytes parse as a backfill RawEnvelope.
# =====================================================================
async def test_shard_fetch_publishes_raw_envelope_shape(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = FakeS3Client()
    producer = _CapturingProducer()
    fetcher = _make_three_page_fetcher([[{"id": "a"}, {"id": "b"}]])

    tid, shard_id = await _seed_and_request(
        fresh_db, source="slack", shard_kind="slack_channel_window",
        fetcher=fetcher, monkeypatch=monkeypatch,
    )
    await _service(fresh_db, producer, s3).run(max_ticks=1)

    assert len(producer.published) == 2
    for topic, value, key in producer.published:
        assert topic == RAW_TOPIC
        assert key == str(tid).encode("utf-8")
        env = RawEnvelope.model_validate(orjson.loads(value))
        assert env.ingress_kind == "backfill"
        assert env.source == "slack"
        assert env.tenant_id == tid
        assert env.raw_s3_key  # non-empty
        assert env.content_hash
        # The pointer's key must address an object actually written.
        assert env.raw_s3_key in s3.store


# =====================================================================
# 3. S3 blob shape: {record, shard_context, webhook_metadata}.
# =====================================================================
async def test_shard_fetch_s3_blob_contains_record_and_metadata(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = FakeS3Client()
    producer = _CapturingProducer()
    # Fetcher emits a handler-conformant body PLUS a reserved
    # webhook_metadata key that the producer lifts out of `record`.
    record = {
        "action": "opened",
        "issue": {"node_id": "I_node"},
        "webhook_metadata": {"X-GitHub-Event": "issues"},
    }
    fetcher = _make_three_page_fetcher([[record]])

    tid, shard_id = await _seed_and_request(
        fresh_db, source="github", shard_kind="github_repo_events",
        fetcher=fetcher, monkeypatch=monkeypatch,
        identifier={"repo": "o/r"},
    )
    await _service(fresh_db, producer, s3).run(max_ticks=1)

    assert len(producer.published) == 1
    _topic, value, _key = producer.published[0]
    env = RawEnvelope.model_validate(orjson.loads(value))
    blob = orjson.loads(s3.store[env.raw_s3_key])

    assert set(blob.keys()) == {"record", "shard_context", "webhook_metadata"}
    # webhook_metadata was lifted OUT of the record.
    assert blob["record"] == {"action": "opened", "issue": {"node_id": "I_node"}}
    assert "webhook_metadata" not in blob["record"]
    assert blob["webhook_metadata"] == {"X-GitHub-Event": "issues"}
    assert blob["shard_context"]["shard_id"] == str(shard_id)


# =====================================================================
# 4. S3 failure marks the shard failed (A19), distinguishable error.
# =====================================================================
async def test_shard_fetch_s3_failure_marks_shard_failed(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = FakeS3Client()
    s3.fail_next_put = True
    producer = _CapturingProducer()
    fetcher = _make_three_page_fetcher([[{"id": "z"}]])

    tid, shard_id = await _seed_and_request(
        fresh_db, source="discord", shard_kind="discord_channel_window",
        fetcher=fetcher, monkeypatch=monkeypatch,
    )
    # Service must not crash.
    await _service(fresh_db, producer, s3).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT state, last_error FROM onboarding_shards WHERE id = $1",
        shard_id,
    )
    assert row["state"] == "failed"
    assert "S3 raw-tier write failed" in (row["last_error"] or ""), (
        f"S3 failure should be tagged distinctly for operators; got "
        f"{row['last_error']!r}"
    )
    # No envelope published — we failed before the N1 publish.
    assert len(producer.published) == 0


# =====================================================================
# 5. Kafka-flush failure after S3 success → cursor unchanged + retry safe.
# =====================================================================
async def test_shard_fetch_kafka_failure_after_s3_success_is_safe(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = FakeS3Client()
    # First flush fails (1 message still queued).
    producer = _CapturingProducer(flush_returns=[1])
    fetcher = _make_three_page_fetcher([[{"id": "p0"}], [{"id": "p1"}]])

    tid, shard_id = await _seed_and_request(
        fresh_db, source="github", shard_kind="github_repo_events",
        fetcher=fetcher, monkeypatch=monkeypatch,
        identifier={"repo": "o/r"},
    )
    await _service(fresh_db, producer, s3).run(max_ticks=1)

    # S3 write happened (blob durable) even though the publish flush
    # failed.
    assert s3.puts == 1
    assert len(s3.store) == 1

    # Cursor did NOT advance — shard stays in_progress.
    ws = await load_state(fresh_db, WORKFLOW_KIND, str(shard_id))
    assert ws is not None
    assert ws.state_data.get("cursor") is None, (
        "cursor advanced despite flush failure — N1 broken under S3 path"
    )
    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "in_progress"

    # A re-attempt re-writes the SAME content-addressed key: PutIfAbsent
    # is a no-op (idempotent), so the store still holds exactly one blob.
    key = next(iter(s3.store))
    await s3.put_if_absent(key, s3.store[key])
    assert len(s3.store) == 1


# =====================================================================
# 6. N1 invariant holds under the S3 path (mid-loop flush failure).
# =====================================================================
async def test_shard_fetch_n1_invariant_holds_under_s3_path(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = FakeS3Client()
    pages = [
        [{"id": "a"}, {"id": "b"}],         # page 0 — clean
        [{"id": "c"}, {"id": "d"}, {"id": "e"}],  # page 1 — flush fails
    ]
    fetcher = _make_three_page_fetcher(pages)
    producer = _CapturingProducer(flush_returns=[0, 3])

    tid, shard_id = await _seed_and_request(
        fresh_db, source="github", shard_kind="github_repo_events",
        fetcher=fetcher, monkeypatch=monkeypatch,
        identifier={"repo": "o/r"},
    )
    await _service(fresh_db, producer, s3).run(max_ticks=1)

    # All 5 records were written to S3 + enqueued via produce.
    assert producer.flush_calls == 2
    assert s3.puts == 5
    assert len(producer.published) == 5

    # Cursor reflects post-page-0 only.
    ws = await load_state(fresh_db, WORKFLOW_KIND, str(shard_id))
    assert ws is not None
    assert ws.state_data.get("cursor") == {"page": 0}
    assert ws.state_data.get("pages_fetched") == 1

    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "in_progress"

    # No completion emitted.
    n = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals WHERE workflow_kind = $1 "
        "AND workflow_id = $2 AND signal_kind = $3 AND idempotency_key = $4",
        SOURCE_ONBOARDING_INBOX_KIND, SOURCE_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_COMPLETED, str(shard_id),
    ))
    assert n == 0


# =====================================================================
# 7. The N1 primitive's contract is unchanged (regression).
# =====================================================================
async def test_advance_cursor_atomic_with_kafka_publish_unchanged(
    fresh_db: asyncpg.Pool,
) -> None:
    """Explicit regression: the primitive still publishes BEFORE it
    advances, and a flush failure leaves the state row untouched. Same
    shape as M6.0's publish-before-persist test, re-asserted here so a
    future change to the backfill producer can't silently mutate the
    primitive's contract."""
    import datetime as dt

    from services.ingestion.workflows.state import (
        CursorAdvanceFlushFailure,
        KafkaMessage,
        WorkflowState,
        advance_cursor_atomic_with_kafka_publish,
        persist_state,
    )

    state = WorkflowState(
        workflow_kind="shard_fetch",
        workflow_id="primitive-regression",
        tenant_id=None,
        state_data={"cursor": {"page": 0}},
        last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
    )
    await persist_state(fresh_db, state)

    producer = _CapturingProducer(flush_returns=[2])
    with pytest.raises(CursorAdvanceFlushFailure):
        await advance_cursor_atomic_with_kafka_publish(
            fresh_db, producer,
            workflow_kind="shard_fetch",
            workflow_id="primitive-regression",
            new_state_data={"cursor": {"page": 1}},
            kafka_messages=[KafkaMessage(topic=RAW_TOPIC, value=b"x", key=b"k")],
            flush_timeout_seconds=1.0,
        )

    # Message was published (enqueued) before the flush barrier.
    assert len(producer.published) == 1
    # State row NOT advanced.
    ws = await load_state(fresh_db, "shard_fetch", "primitive-regression")
    assert ws is not None
    assert ws.state_data.get("cursor") == {"page": 0}
