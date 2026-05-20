"""M5.2 — observation_writer full-mode tests.

These tests cover the writer's flag-branched full-mode path added in
M5.2:

  - flag=TRUE  → `ingest_from_draft` writes an observation to Postgres
  - flag=FALSE → M2 shadow-log no-op behaviour preserved

The load-bearing parity test
`test_writer_observations_match_inline_for_same_input` asserts that
the writer's full-mode output is set-equal to the inline `ingest()`
path's output for the same input. This is the N1 cutover-safety
property — divergence here would mean cutover trades correctness
for throughput.

Test injection: each test passes pre-built pool / TenantFlags /
ActorRepo / EntityAliasRepo / fake producer into `WriterConfig`,
then drives `_handle_message` directly with the JSON-encoded
envelope bytes. No Kafka broker is spun up.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import struct
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.embeddings.ollama import EMBEDDING_DIM
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.ingestion.core import ingest as inline_ingest
from services.ingestion.feature_flags.client import (
    KAFKA_PATH_ENABLED,
    TenantFlags,
)
from services.ingestion.normalizer.models import NormalizedEnvelope
from services.ingestion.writers import observation_writer as writer_module


pytestmark = [pytest.mark.timeout(120)]


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------
# Fixtures + fakes.
# ---------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_writer_state() -> None:
    writer_module.reset_metrics()
    writer_module.reset_shadow_log()


class _DeterministicEmbedder:
    """Mirrors `services/ingestion/tests/conftest.py::_DeterministicEmbedder`.

    A reproducible embedder that returns the same vector for the same
    text. The parity test depends on inline and writer paths producing
    bit-equal embeddings for the same input.
    """

    class _C:
        model = "test-fake"
        expected_dim = EMBEDDING_DIM

    def __init__(self) -> None:
        self.config = self._C()

    async def embed(self, text: str) -> list[float]:
        h = hashlib.sha512((text or "").encode("utf-8")).digest()
        pool = b""
        while len(pool) < EMBEDDING_DIM * 4:
            pool += hashlib.sha512(pool + h).digest()
        vec: list[float] = []
        for i in range(EMBEDDING_DIM):
            raw = struct.unpack("<f", pool[i * 4 : (i + 1) * 4])[0]
            if not (-1e6 < raw < 1e6):
                raw = 0.0
            vec.append(max(-1.0, min(1.0, raw / 1e3)))
        return vec


class _CaptureProducer:
    """IdempotentProducer stand-in. Captures every published record so
    tests can inspect topic + payload without a real Kafka broker.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def start(self) -> None:
        return None

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        return None

    async def produce(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))


async def _seed_tenant(pool: asyncpg.Pool, name: str | None = None) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, name or f"writer-m5-test-{tid.hex[:8]}",
    )
    return tid


def _build_envelope(
    tenant_id: UUID,
    *,
    external_id: str = "C01:1.0",
    content_text: str = "hello from M5.2 parity test",
    content_hash: str | None = None,
) -> NormalizedEnvelope:
    """Build a NormalizedEnvelope for `slack:message`. Trust tier and
    handler-shape match what the Slack handler would produce."""
    if content_hash is None:
        content_hash = hashlib.sha1(
            f"{tenant_id}{external_id}{content_text}".encode()
        ).hexdigest()
    return NormalizedEnvelope(
        envelope_version=1,
        source="slack",
        ingress_kind="webhook",
        tenant_id=tenant_id,
        raw_s3_key=(
            f"dev/slack/{tenant_id}/2026-05/"
            f"{content_hash[:2]}/{content_hash}.json"
        ),
        content_hash=content_hash,
        raw_ingested_at=_NOW,
        source_channel="slack:message",
        content_text=content_text,
        content={
            "channel": "C01",
            "ts": "1.0",
            "text": content_text,
            "team": "T01",
        },
        occurred_at=_NOW,
        trust_tier="attested_agent",
        kind="signal",
        source_actor_ref="slack:U01ALICE",
        external_id=external_id,
        entities_hint=[],
        normalized_at=_NOW,
        ingress_metadata={},
        idem_hints={},
    )


def _envelope_bytes(env: NormalizedEnvelope) -> bytes:
    return orjson.dumps(env.model_dump(mode="json"))


async def _writer_config_with_db(
    pool: asyncpg.Pool,
    *,
    embedder: Any | None = None,
) -> writer_module.WriterConfig:
    return writer_module.WriterConfig(
        pool=pool,
        tenant_flags=TenantFlags(pool),
        actor_repo=ActorRepo(pool),
        alias_repo=EntityAliasRepo(pool),
        embedder=embedder,
    )


async def _enable_kafka_path(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    """Flip `ingestion.kafka_path_enabled=TRUE` for `tenant_id` — the
    operator action that puts a tenant on the writer's full-mode path.
    """
    flags = TenantFlags(pool)
    await flags.set_bool(
        tenant_id, KAFKA_PATH_ENABLED, True,
        set_by="operator:test", note="m5.2 test enable",
    )


# =====================================================================
# 1. LOAD-BEARING — writer full mode produces identical observations
#    to the inline path for the same input.
# =====================================================================

async def test_writer_observations_match_inline_for_same_input(
    fresh_db: asyncpg.Pool,
) -> None:
    """The N1 cutover-safety property: structurally-equivalent inputs
    produce structurally-equivalent observations whether they flow
    through the inline `ingest()` path or the writer's full-mode path.

    Approach: TWO distinct messages (different `ts`, different
    `external_id`), one per path. The observations.unique index is
    `(source_channel, external_id, occurred_at)` — global, not
    per-tenant — so two messages with the same external_id can't
    coexist regardless of tenant. Using different external_ids lets
    both writes land, and the parity assertion compares everything
    EXCEPT the inputs that intentionally differ (ts/external_id/
    content_hash/id/tenant_id/timestamps).

    Assert: kind, source_channel, source_actor_ref, trust_tier,
    embedding_pending, content_text, and the deterministic embedding
    are bit-equal between the two rows. This is the cutover-safety
    invariant: switching a tenant from the inline path to the writer
    path produces the same observation structure for the same
    handler output.
    """
    tenant_inline = await _seed_tenant(fresh_db, "tenant-inline")
    tenant_writer = await _seed_tenant(fresh_db, "tenant-writer")
    await _enable_kafka_path(fresh_db, tenant_writer)

    embedder = _DeterministicEmbedder()

    # Two slack messages with different ts → different external_ids,
    # but identical content_text → identical handler-derived fields
    # downstream of step 1.
    common_text = "hello from M5.2 parity test"
    ts_inline = float(int(_NOW.timestamp()))           # whole-second ts
    ts_writer = float(int(_NOW.timestamp()) + 1)       # +1s

    # ---- A. Inline path: ingest() with a raw Slack webhook. ----
    slack_payload = {
        "event": {
            "type": "message",
            "user": "U01ALICE",
            "text": common_text,
            "channel": "C01",
            "ts": f"{ts_inline:.6f}",
            "team": "T01",
        },
        "team_id": "T01",
        "event_id": "Ev_inline",
        "event_time": int(ts_inline),
    }
    inline_result = await inline_ingest(
        "slack:message",
        slack_payload,
        pool=fresh_db,
        tenant_id=tenant_inline,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=embedder,
    )
    inline_obs = inline_result.observation

    # ---- B. Writer path: feed the M2.3 NormalizedEnvelope shape ----
    #         the normalizer would have emitted from an equivalent
    #         message (same content_text, different ts). We construct
    #         the draft fields the same way the Slack handler does in
    #         core.py step 1 — the parity proof is that downstream of
    #         step 1, both paths must agree.
    env = NormalizedEnvelope(
        envelope_version=1,
        source="slack",
        ingress_kind="webhook",
        tenant_id=tenant_writer,
        raw_s3_key=(
            f"dev/slack/{tenant_writer}/2026-05/aa/{'a'*40}.json"
        ),
        content_hash="b" * 40,
        raw_ingested_at=_NOW,
        source_channel="slack:message",
        content_text=common_text,
        content={
            "channel": "C01",
            "ts": f"{ts_writer:.6f}",
            "text": common_text,
            "team": "T01",
        },
        occurred_at=dt.datetime.fromtimestamp(ts_writer, tz=dt.timezone.utc),
        trust_tier="attested_agent",
        kind="signal",
        source_actor_ref="slack:U01ALICE",
        external_id=f"C01:{ts_writer:.6f}",
        entities_hint=[],
        normalized_at=_NOW,
        ingress_metadata={},
        idem_hints={},
    )

    capture = _CaptureProducer()
    config = await _writer_config_with_db(fresh_db, embedder=embedder)
    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    # Diagnostic breadcrumb — fail with a specific message before
    # the row comparison if the writer routed to the wrong branch.
    m = writer_module.get_metrics()
    assert m["writer.parse_failure"] == 0, m
    assert m["writer.shadow_write_events"] == 0, m
    assert m["writer.full_mode_writes"] == 1, m

    # ---- C. Compare the two observation rows. ----
    writer_row = await fresh_db.fetchrow(
        """SELECT id, tenant_id, kind, source_channel, source_actor_ref,
                  content_text, trust_tier, embedding,
                  embedding_pending, content
             FROM observations WHERE tenant_id = $1""",
        tenant_writer,
    )
    inline_row = await fresh_db.fetchrow(
        """SELECT id, tenant_id, kind, source_channel, source_actor_ref,
                  content_text, trust_tier, embedding,
                  embedding_pending, content
             FROM observations WHERE tenant_id = $1""",
        tenant_inline,
    )
    assert writer_row is not None, (
        "Writer full-mode produced NO observation row — "
        "ingest_from_draft was not called or the insert silently failed."
    )
    assert inline_row is not None

    # IDs, tenant_ids, external_ids, occurred_at, content.ts differ
    # by design (different messages). Everything else must agree.
    for col in (
        "kind", "source_channel", "source_actor_ref",
        "content_text", "trust_tier", "embedding_pending",
    ):
        assert writer_row[col] == inline_row[col], (
            f"Parity violation on column {col!r}: "
            f"writer={writer_row[col]!r} vs inline={inline_row[col]!r}. "
            f"N1 cutover-safety failed — the writer's output diverges "
            f"from the inline path for structurally-equivalent input."
        )

    # Embeddings must agree bit-for-bit when both used the same
    # deterministic embedder on the same content_text. pgvector
    # surfaces as a numpy array or list depending on driver version,
    # so compare element-wise via list cast.
    w_emb = (
        list(writer_row["embedding"]) if writer_row["embedding"] is not None else None
    )
    i_emb = (
        list(inline_row["embedding"]) if inline_row["embedding"] is not None else None
    )
    assert w_emb == i_emb, (
        "Embedding parity failed — deterministic embedder produced "
        "different vectors for inline vs writer paths on the same "
        "content_text."
    )

    # content dict parity on the message-content fields (channel,
    # text, team). `ts` and any reserved `_*` keys legitimately
    # differ between the two rows. asyncpg returns jsonb as a string
    # by default; parse before comparing.
    w_content = json.loads(writer_row["content"]) if isinstance(writer_row["content"], str) else writer_row["content"]
    i_content = json.loads(inline_row["content"]) if isinstance(inline_row["content"], str) else inline_row["content"]
    for k in ("channel", "text", "team"):
        assert w_content.get(k) == i_content.get(k), (
            f"content[{k!r}] parity violation: "
            f"writer={w_content.get(k)!r} vs inline={i_content.get(k)!r}"
        )


# =====================================================================
# 2. flag=FALSE — writer stays shadow-only, no Postgres write.
# =====================================================================

async def test_writer_full_mode_skipped_when_flag_disabled(
    fresh_db: asyncpg.Pool,
) -> None:
    """Tenant with no row in `tenant_flags` (default FALSE per LLD §11)
    must NOT have an observation inserted. The shadow log MUST
    receive the envelope (matches M2.4 behaviour)."""
    tenant_pre_cutover = await _seed_tenant(fresh_db, "tenant-pre-cutover")
    env = _build_envelope(tenant_pre_cutover)

    capture = _CaptureProducer()
    config = await _writer_config_with_db(fresh_db)

    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_pre_cutover,
    )
    assert obs_count == 0, (
        f"Pre-cutover tenant (flag=FALSE) had {obs_count} observation "
        f"rows inserted by the writer — flag-branch is broken; the "
        f"inline path would have double-written for this tenant."
    )
    shadow = writer_module.get_shadow_log()
    assert len(shadow) == 1, (
        f"Pre-cutover tenant produced {len(shadow)} shadow events; "
        f"expected 1 — shadow-log path is broken."
    )
    assert writer_module.get_metrics()["writer.shadow_write_events"] == 1
    assert writer_module.get_metrics()["writer.full_mode_writes"] == 0


# =====================================================================
# 3. flag=TRUE — writer writes to Postgres, no shadow log entry.
# =====================================================================

async def test_writer_full_mode_writes_when_flag_enabled(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_cutover = await _seed_tenant(fresh_db, "tenant-cutover")
    await _enable_kafka_path(fresh_db, tenant_cutover)
    env = _build_envelope(tenant_cutover, external_id="C02:2.0")

    capture = _CaptureProducer()
    config = await _writer_config_with_db(fresh_db, embedder=_DeterministicEmbedder())

    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_cutover,
    )
    assert obs_count == 1, (
        f"Cutover tenant (flag=TRUE) had {obs_count} observations; "
        f"expected 1 — full-mode path is broken."
    )
    # Shadow log untouched (full-mode tenant doesn't double-log).
    assert writer_module.get_shadow_log() == []
    assert writer_module.get_metrics()["writer.full_mode_writes"] == 1
    assert writer_module.get_metrics()["writer.shadow_write_events"] == 0


# =====================================================================
# 4. Pool config — pgbouncer-compatible (fifth statement_cache_size=0).
# =====================================================================

async def test_writer_pool_uses_pgbouncer_compatible_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`make_writer_pool` MUST set `statement_cache_size=0` — the
    fifth activation after M3.1 (DLQ writer), M3.3 (backlog drainer),
    M4.2 (session-state pool), and M5.1 (circuit-breaker pool).
    """
    captured: dict[str, Any] = {}

    async def _spy(dsn: str, **kwargs: Any) -> Any:
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(asyncpg, "create_pool", _spy)
    await writer_module.make_writer_pool("postgresql://x@y/z")

    assert captured["kwargs"]["statement_cache_size"] == 0, (
        f"make_writer_pool did NOT set statement_cache_size=0 — "
        f"writer pool is NOT pgbouncer-compatible. Got "
        f"{captured['kwargs'].get('statement_cache_size')}."
    )
    assert "min_size" in captured["kwargs"]
    assert "max_size" in captured["kwargs"]


# =====================================================================
# 5. Dedup — same envelope delivered twice → one observation row.
# =====================================================================

async def test_writer_full_mode_dedupes_on_redelivery(
    fresh_db: asyncpg.Pool,
) -> None:
    """Kafka at-least-once redelivery is normal. The writer's full
    mode MUST be idempotent — same (source_channel, external_id) →
    `ingest_from_draft` returns deduped=True and no duplicate row
    lands in Postgres.
    """
    tenant_cutover = await _seed_tenant(fresh_db, "tenant-dedup")
    await _enable_kafka_path(fresh_db, tenant_cutover)
    env = _build_envelope(tenant_cutover, external_id="C03:dedup")

    capture = _CaptureProducer()
    config = await _writer_config_with_db(fresh_db, embedder=_DeterministicEmbedder())

    # First delivery.
    await writer_module._handle_message(
        _envelope_bytes(env), config=config,
        dlq_producer=capture, embedding_producer=capture,
    )
    # Second delivery (Kafka redelivery).
    await writer_module._handle_message(
        _envelope_bytes(env), config=config,
        dlq_producer=capture, embedding_producer=capture,
    )

    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_cutover,
    )
    assert obs_count == 1, (
        f"Dedup failed — two deliveries produced {obs_count} rows. "
        f"Either ingest_from_draft's dedup branch is broken or the "
        f"unique index on (source_channel, external_id) is missing."
    )
    metrics = writer_module.get_metrics()
    assert metrics["writer.full_mode_writes"] == 1
    assert metrics["writer.full_mode_dedup_hits"] == 1


# =====================================================================
# 6. Embedding-pending → publishes to ingestion.embedding topic.
# =====================================================================

async def test_writer_full_mode_publishes_embedding_request_on_pending(
    fresh_db: asyncpg.Pool,
) -> None:
    """When the writer has no embedder configured, the observation is
    inserted with `embedding_pending=TRUE` and `ingest_from_draft`
    publishes an envelope to `ingestion.embedding` so the M3.2 worker
    can pick it up.

    This is the M3 contract for embedding work distribution; the
    writer must NOT silently swallow embedding-pending rows.
    """
    tenant_cutover = await _seed_tenant(fresh_db, "tenant-emb-pending")
    await _enable_kafka_path(fresh_db, tenant_cutover)
    env = _build_envelope(tenant_cutover, external_id="C04:embpending")

    capture = _CaptureProducer()
    # embedder=None — observation lands at embedding_pending=TRUE.
    config = await _writer_config_with_db(fresh_db, embedder=None)

    await writer_module._handle_message(
        _envelope_bytes(env), config=config,
        dlq_producer=capture, embedding_producer=capture,
    )

    # Observation row exists at pending=True.
    row = await fresh_db.fetchrow(
        "SELECT id, embedding_pending FROM observations WHERE tenant_id = $1",
        tenant_cutover,
    )
    assert row is not None
    assert row["embedding_pending"] is True, (
        "Observation should be pending — writer was wired with embedder=None."
    )

    # Embedding request published to `ingestion.embedding`.
    emb_publishes = [
        (topic, value) for (topic, value, _key) in capture.published
        if topic == "ingestion.embedding"
    ]
    assert len(emb_publishes) == 1, (
        f"Expected 1 publish to ingestion.embedding; got "
        f"{len(emb_publishes)}. Pending observations would never get "
        f"embedded — M3.3 backlog drainer is the only safety net left."
    )
    payload = json.loads(emb_publishes[0][1])
    assert payload["observation_id"] == str(row["id"])
    assert payload["source"] == "slack"


# =====================================================================
# 7. Permanent error → DLQ + offset committed (no transient retry).
# =====================================================================

async def test_writer_parse_failure_dlqs_and_commits(
    fresh_db: asyncpg.Pool,
) -> None:
    """Malformed envelope on the wire: bump `writer.parse_failure`,
    publish to DLQ (best-effort extracts tenant_id + source from the
    half-broken JSON), and skip past the message. No observation
    written. Same prime directive as M2.4 — never crash on bad bytes.
    """
    tenant_cutover = await _seed_tenant(fresh_db, "tenant-bad-bytes")
    await _enable_kafka_path(fresh_db, tenant_cutover)

    # Bytes that LOOK like a NormalizedEnvelope enough for
    # `extract_dlq_fields_best_effort` to pull tenant_id + source,
    # but fail Pydantic validation (missing required fields).
    bad_bytes = orjson.dumps({
        "envelope_version": 1,
        "source": "slack",
        "tenant_id": str(tenant_cutover),
        # Required fields like ingress_kind, content_text etc. omitted
        # → NormalizedEnvelope.model_validate raises.
    })

    capture = _CaptureProducer()
    config = await _writer_config_with_db(
        fresh_db, embedder=_DeterministicEmbedder(),
    )

    await writer_module._handle_message(
        bad_bytes,
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    metrics = writer_module.get_metrics()
    assert metrics["writer.parse_failure"] == 1, (
        f"Bad bytes should bump writer.parse_failure; got "
        f"{metrics['writer.parse_failure']}."
    )
    # DLQ publish landed on ingestion.dlq.
    dlq_publishes = [
        (topic, value) for (topic, value, _key) in capture.published
        if topic == "ingestion.dlq"
    ]
    assert len(dlq_publishes) == 1, (
        f"Expected 1 publish to ingestion.dlq for the bad message; "
        f"got {len(dlq_publishes)}. publish_dlq probably skipped "
        f"because extract_dlq_fields_best_effort couldn't recover "
        f"tenant_id/source from the bytes."
    )
    # No observations written for the cutover tenant.
    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_cutover,
    )
    assert obs_count == 0


async def test_writer_missing_partition_dlqs_not_crash_loop(
    fresh_db: asyncpg.Pool,
) -> None:
    """A28: an observation whose `occurred_at` falls outside the
    `observations` table's partition coverage triggers asyncpg's
    CheckViolationError (no partition routes the row). The writer must
    classify this as PERMANENT — distinguished from a *named* CHECK
    violation by `constraint_name is None` — and route it to the DLQ
    with a `partition_missing` diagnostic + commit, rather than letting
    it propagate as a transient error that crash-loops the consumer on
    the first out-of-range message. See ticket #44.
    """
    tenant = await _seed_tenant(fresh_db, "tenant-partition-missing")
    await _enable_kafka_path(fresh_db, tenant)

    # Well before the earliest observations partition (coverage starts
    # 2025-01) → Postgres finds no partition for the row.
    out_of_range = dt.datetime(2023, 11, 14, 22, 14, 20, tzinfo=dt.timezone.utc)
    env = _build_envelope(
        tenant, external_id="C01:partition-miss",
    ).model_copy(update={"occurred_at": out_of_range})

    capture = _CaptureProducer()
    config = await _writer_config_with_db(
        fresh_db, embedder=_DeterministicEmbedder(),
    )

    # MUST NOT raise — a raise here is the crash-loop the fix prevents.
    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    metrics = writer_module.get_metrics()
    assert metrics["writer.partition_missing"] == 1, (
        f"Out-of-range occurred_at should bump writer.partition_missing; "
        f"got {metrics['writer.partition_missing']}."
    )
    # Routed to ingestion.dlq with the partition_missing diagnostic.
    dlq_publishes = [
        value for (topic, value, _key) in capture.published
        if topic == "ingestion.dlq"
    ]
    assert len(dlq_publishes) == 1, (
        f"Expected exactly 1 DLQ publish; got {len(dlq_publishes)}."
    )
    dlq = orjson.loads(dlq_publishes[0])
    assert "partition_missing" in dlq["error_summary"], dlq
    assert dlq["error_context"]["reason"] == "partition_missing", dlq
    assert dlq["error_context"]["occurred_at"] == out_of_range.isoformat()
    # No observation written.
    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant,
    )
    assert obs_count == 0
