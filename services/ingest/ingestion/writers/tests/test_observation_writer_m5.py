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

import asyncio
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
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.core import ingest as inline_ingest
from services.ingest.ingestion.feature_flags.client import (
    KAFKA_PATH_ENABLED,
    TenantFlags,
)
from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
from services.ingest.ingestion.writers import observation_writer as writer_module


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
    """Mirrors `services/ingest/ingestion/tests/conftest.py::_DeterministicEmbedder`.

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
    ingress_kind: str = "webhook",
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
        ingress_kind=ingress_kind,
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


async def _disable_kafka_path(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    """Flip `ingestion.kafka_path_enabled=FALSE` for `tenant_id` — the
    kill-switch. Under the inverted default a missing row is full-mode, so a
    test that wants the shadow-only path must set this explicitly.
    """
    flags = TenantFlags(pool)
    await flags.set_bool(
        tenant_id, KAFKA_PATH_ENABLED, False,
        set_by="operator:test", note="m5.2 test kill-switch",
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
    await inline_ingest(
        "slack:message",
        slack_payload,
        pool=fresh_db,
        tenant_id=tenant_inline,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=embedder,
    )

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
    """Tenant with `ingestion.kafka_path_enabled=FALSE` (the explicit
    kill-switch) must NOT have an observation inserted by the writer. The
    shadow log MUST receive the envelope (matches M2.4 behaviour).

    Under the inverted default a missing flag row is full-mode, so the
    kill-switch is seeded explicitly here."""
    tenant_pre_cutover = await _seed_tenant(fresh_db, "tenant-killswitch")
    await _disable_kafka_path(fresh_db, tenant_pre_cutover)
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
    # F2 — the drop is also counted on the distinct alert-worthy metric.
    assert writer_module.get_metrics()["writer.shadow_drop"] == 1


# =====================================================================
# 2b. F1 — backfill is ALWAYS persisted, even with the kill-switch FALSE.
#     F2 — live drops are loud; backfill in the shadow path is an ERROR.
# =====================================================================

async def test_writer_persists_backfill_even_when_flag_disabled(
    fresh_db: asyncpg.Pool,
) -> None:
    """F1 regression guard. A tenant with `kafka_path_enabled=FALSE` (the
    circuit-breaker / operator kill-switch) must STILL have backfill
    observations written — backfill has no inline fallback, so shadow-dropping
    it is silent, unrecoverable data loss."""
    tenant = await _seed_tenant(fresh_db, "tenant-backfill-killswitch")
    await _disable_kafka_path(fresh_db, tenant)
    env = _build_envelope(
        tenant, external_id="C09:9.0", ingress_kind="backfill",
    )

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
        tenant,
    )
    assert obs_count == 1, (
        f"Backfill envelope with flag=FALSE produced {obs_count} observations; "
        f"expected 1 — F1 backfill exemption is broken (silent data loss)."
    )
    # NOT shadow-logged, NOT dropped.
    assert writer_module.get_shadow_log() == []
    metrics = writer_module.get_metrics()
    assert metrics["writer.full_mode_writes"] == 1
    assert metrics["writer.shadow_write_events"] == 0
    assert metrics["writer.shadow_drop"] == 0


async def test_writer_live_still_shadow_dropped_when_flag_disabled(
    fresh_db: asyncpg.Pool,
) -> None:
    """F1 must NOT change live behaviour: a webhook (live) envelope with
    flag=FALSE is still shadow-only (the inline path writes it), so the
    writer must not double-write."""
    tenant = await _seed_tenant(fresh_db, "tenant-live-killswitch")
    await _disable_kafka_path(fresh_db, tenant)
    env = _build_envelope(tenant, external_id="C10:1.0", ingress_kind="webhook")

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
        tenant,
    )
    assert obs_count == 0
    assert len(writer_module.get_shadow_log()) == 1
    assert writer_module.get_metrics()["writer.shadow_drop"] == 1


async def test_writer_backfill_shadow_only_when_no_pool() -> None:
    """With no pool (pure shadow mode, e.g. the M2 soak harness), even a
    backfill envelope stays shadow-only — F1 only forces a WRITE when the
    writer actually has a pool to write through."""
    env = _build_envelope(uuid4(), ingress_kind="backfill")
    capture = _CaptureProducer()
    config = writer_module.WriterConfig(pool=None, tenant_flags=None)

    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    assert len(writer_module.get_shadow_log()) == 1
    assert writer_module.get_metrics()["writer.shadow_drop"] == 1


async def test_backfill_in_shadow_path_logs_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F2 defense-in-depth: if a backfill envelope ever reaches the shadow
    path (which F1 makes impossible in full-mode), it is logged at ERROR with
    the BUG reason so it is never silent."""
    import logging

    env = _build_envelope(uuid4(), ingress_kind="backfill")
    with caplog.at_level(logging.WARNING, logger="services.ingest.ingestion.writers.observation_writer"):
        await writer_module._record_shadow_event(env)

    assert writer_module.get_metrics()["writer.shadow_drop"] == 1
    recs = [r for r in caplog.records if r.message == "writer.shadow_drop"]
    assert recs, "expected a writer.shadow_drop log record"
    assert recs[-1].levelno == logging.ERROR
    assert getattr(recs[-1], "reason", None) == "backfill_envelope_in_shadow_path_BUG"


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
    # F2 — no drop on the happy path.
    assert writer_module.get_metrics()["writer.shadow_drop"] == 0


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

    # Embedding request published to the per-source embedding lane
    # (source-isolation): ingestion.embedding.<source>.
    emb_publishes = [
        (topic, value) for (topic, value, _key) in capture.published
        if topic.startswith("ingestion.embedding")
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
        if topic.startswith("ingestion.dlq")
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


async def _partition_name_for(occurred: dt.datetime) -> str:
    return f"observations_{occurred.strftime('%Y_%m')}"


async def _drop_partition(pool: asyncpg.Pool, occurred: dt.datetime) -> None:
    """Drop the monthly partition covering `occurred` if it exists, so a
    test can deterministically reproduce the missing-partition condition
    regardless of what partitions the shared dev DB happens to carry.
    """
    name = await _partition_name_for(occurred)
    await pool.execute(f'DROP TABLE IF EXISTS "{name}"')


async def _partition_exists(pool: asyncpg.Pool, occurred: dt.datetime) -> bool:
    name = await _partition_name_for(occurred)
    return await pool.fetchval("SELECT to_regclass($1)", name) is not None


async def test_writer_missing_partition_self_heals_and_inserts(
    fresh_db: asyncpg.Pool,
) -> None:
    """Ticket #44 (durable fix): an observation whose `occurred_at` has
    no covering partition triggers asyncpg's unnamed CheckViolationError.
    The writer must NOT silently DLQ it (success-shaped data loss).
    Instead it auto-creates the covering month and retries the insert —
    so the row lands, `writer.partition_missing == 0`, and
    `writer.partition_autocreated` is bumped.

    Replaces the prior `test_writer_missing_partition_dlqs_not_crash_loop`
    which asserted the now-removed DLQ-drop behaviour.
    """
    tenant = await _seed_tenant(fresh_db, "tenant-partition-heal")
    await _enable_kafka_path(fresh_db, tenant)

    # A within-lookback historical backfill timestamp with no partition.
    # Drop the covering month first so the missing-partition condition
    # holds regardless of the shared DB's existing partition set.
    backfill_at = dt.datetime(2017, 7, 14, 22, 14, 20, tzinfo=dt.timezone.utc)
    await _drop_partition(fresh_db, backfill_at)
    assert not await _partition_exists(fresh_db, backfill_at)

    env = _build_envelope(
        tenant, external_id="C01:partition-heal",
    ).model_copy(update={"occurred_at": backfill_at})

    capture = _CaptureProducer()
    config = await _writer_config_with_db(
        fresh_db, embedder=_DeterministicEmbedder(),
    )

    # MUST NOT raise.
    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    metrics = writer_module.get_metrics()
    assert metrics["writer.partition_missing"] == 0, (
        f"Self-heal should drop nothing; got partition_missing="
        f"{metrics['writer.partition_missing']}."
    )
    assert metrics["writer.partition_autocreated"] == 1, (
        f"Backfill row should auto-create its partition; got "
        f"partition_autocreated={metrics['writer.partition_autocreated']}."
    )
    assert metrics["writer.full_mode_writes"] == 1
    # No DLQ publish for the healed row.
    dlq_publishes = [
        value for (topic, value, _key) in capture.published
        if topic.startswith("ingestion.dlq")
    ]
    assert dlq_publishes == [], (
        f"Healed row must not be DLQ'd; got {len(dlq_publishes)} publishes."
    )
    # The covering partition now exists and holds the row.
    assert await _partition_exists(fresh_db, backfill_at)
    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant,
    )
    assert obs_count == 1, f"Backfill row should be inserted; got {obs_count}."


async def test_writer_out_of_bounds_future_dlqs_no_partition(
    fresh_db: asyncpg.Pool,
) -> None:
    """Ticket #44 guardrail: a far-future `occurred_at` (beyond
    FUTURE_SKEW) is corrupt source data, not a legitimate backfill. The
    writer must DLQ it with `reason="out_of_bounds_occurred_at"` and must
    NOT create a partition for it (prevents a bad clock-skewed timestamp
    from spawning a pathological far-future partition).
    """
    tenant = await _seed_tenant(fresh_db, "tenant-oob-future")
    await _enable_kafka_path(fresh_db, tenant)

    # Year 2035: beyond the dev DB's forward coverage and far beyond the
    # 7-day future skew.
    future_at = dt.datetime(2035, 1, 9, 12, 0, 0, tzinfo=dt.timezone.utc)
    await _drop_partition(fresh_db, future_at)

    env = _build_envelope(
        tenant, external_id="C01:oob-future",
    ).model_copy(update={"occurred_at": future_at})

    capture = _CaptureProducer()
    config = await _writer_config_with_db(
        fresh_db, embedder=_DeterministicEmbedder(),
    )

    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    metrics = writer_module.get_metrics()
    assert metrics["writer.partition_out_of_bounds"] == 1, metrics
    assert metrics["writer.partition_autocreated"] == 0, metrics
    assert metrics["writer.partition_missing"] == 0, metrics

    dlq_publishes = [
        value for (topic, value, _key) in capture.published
        if topic.startswith("ingestion.dlq")
    ]
    assert len(dlq_publishes) == 1, dlq_publishes
    dlq = orjson.loads(dlq_publishes[0])
    assert dlq["error_context"]["reason"] == "out_of_bounds_occurred_at", dlq
    assert dlq["error_context"]["occurred_at"] == future_at.isoformat()
    # Crucially: NO partition was spawned for the bad timestamp.
    assert not await _partition_exists(fresh_db, future_at), (
        "Guardrail must not create a partition for out-of-bounds data."
    )
    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant,
    )
    assert obs_count == 0


async def test_writer_out_of_bounds_ancient_dlqs_no_partition(
    fresh_db: asyncpg.Pool,
) -> None:
    """Ticket #44 guardrail (past side): an `occurred_at` older than
    MAX_BACKFILL_LOOKBACK is DLQ'd as out-of-bounds with no partition
    created — the symmetric counterpart to the far-future guard.
    """
    tenant = await _seed_tenant(fresh_db, "tenant-oob-ancient")
    await _enable_kafka_path(fresh_db, tenant)

    # Year 2005: well beyond the ~10-year lookback floor.
    ancient_at = dt.datetime(2005, 3, 2, 9, 30, 0, tzinfo=dt.timezone.utc)
    await _drop_partition(fresh_db, ancient_at)

    env = _build_envelope(
        tenant, external_id="C01:oob-ancient",
    ).model_copy(update={"occurred_at": ancient_at})

    capture = _CaptureProducer()
    config = await _writer_config_with_db(
        fresh_db, embedder=_DeterministicEmbedder(),
    )

    await writer_module._handle_message(
        _envelope_bytes(env),
        config=config,
        dlq_producer=capture,
        embedding_producer=capture,
    )

    metrics = writer_module.get_metrics()
    assert metrics["writer.partition_out_of_bounds"] == 1, metrics
    dlq_publishes = [
        value for (topic, value, _key) in capture.published
        if topic.startswith("ingestion.dlq")
    ]
    assert len(dlq_publishes) == 1
    dlq = orjson.loads(dlq_publishes[0])
    assert dlq["error_context"]["reason"] == "out_of_bounds_occurred_at", dlq
    assert not await _partition_exists(fresh_db, ancient_at)


async def test_writer_partition_self_heal_concurrent_same_month(
    fresh_db: asyncpg.Pool,
) -> None:
    """Ticket #44 concurrency: two writers handling the same missing
    month must not crash and must not let a DuplicateTableError escape.
    Both rows land exactly once (idempotent CREATE TABLE IF NOT EXISTS;
    DuplicateTableError caught as success).
    """
    tenant = await _seed_tenant(fresh_db, "tenant-heal-concurrent")
    await _enable_kafka_path(fresh_db, tenant)

    backfill_at = dt.datetime(2017, 9, 3, 8, 0, 0, tzinfo=dt.timezone.utc)
    await _drop_partition(fresh_db, backfill_at)

    env_a = _build_envelope(
        tenant, external_id="C01:heal-a", content_text="row a",
    ).model_copy(update={"occurred_at": backfill_at})
    env_b = _build_envelope(
        tenant, external_id="C01:heal-b", content_text="row b",
    ).model_copy(update={"occurred_at": backfill_at})

    capture = _CaptureProducer()
    config = await _writer_config_with_db(
        fresh_db, embedder=_DeterministicEmbedder(),
    )

    # Drive both concurrently — they race to create observations_2017_09.
    await asyncio.gather(
        writer_module._handle_message(
            _envelope_bytes(env_a), config=config,
            dlq_producer=capture, embedding_producer=capture,
        ),
        writer_module._handle_message(
            _envelope_bytes(env_b), config=config,
            dlq_producer=capture, embedding_producer=capture,
        ),
    )

    metrics = writer_module.get_metrics()
    assert metrics["writer.partition_missing"] == 0, metrics
    # No DLQ publishes at all.
    dlq_publishes = [
        v for (t, v, _k) in capture.published if t.startswith("ingestion.dlq")
    ]
    assert dlq_publishes == [], dlq_publishes
    # Both rows inserted exactly once.
    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant,
    )
    assert obs_count == 2, f"Expected both rows inserted; got {obs_count}."


async def test_writer_duplicate_table_error_treated_as_success(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ticket #44 concurrency (deterministic): if `ensure_partitions`
    raises DuplicateTableError (the race window Postgres can surface even
    with IF NOT EXISTS), the writer treats it as success and the retried
    insert lands — no crash, no DLQ.

    Simulated race: the partition starts absent (first insert fails with
    the unnamed CheckViolation). We then patch ensure_partitions to
    *create* the partition (as the winning writer would) and immediately
    raise DuplicateTableError (as the losing writer sees). The retry must
    still succeed against the now-existing partition.
    """
    tenant = await _seed_tenant(fresh_db, "tenant-dup-table")
    await _enable_kafka_path(fresh_db, tenant)

    backfill_at = dt.datetime(2017, 11, 20, 6, 0, 0, tzinfo=dt.timezone.utc)
    await _drop_partition(fresh_db, backfill_at)

    real_ensure = writer_module.ensure_partitions

    async def _create_then_raise_dup(pool: Any, **kw: Any) -> list[str]:
        # The winning writer created the partition...
        await real_ensure(pool, **kw)
        # ...and our CREATE raced and lost despite IF NOT EXISTS.
        raise asyncpg.exceptions.DuplicateTableError("raced")

    monkeypatch.setattr(
        writer_module, "ensure_partitions", _create_then_raise_dup,
    )

    env = _build_envelope(
        tenant, external_id="C01:dup-table",
    ).model_copy(update={"occurred_at": backfill_at})

    capture = _CaptureProducer()
    config = await _writer_config_with_db(
        fresh_db, embedder=_DeterministicEmbedder(),
    )

    await writer_module._handle_message(
        _envelope_bytes(env), config=config,
        dlq_producer=capture, embedding_producer=capture,
    )

    metrics = writer_module.get_metrics()
    assert metrics["writer.partition_autocreated"] == 1, metrics
    assert metrics["writer.partition_missing"] == 0, metrics
    dlq_publishes = [
        v for (t, v, _k) in capture.published if t.startswith("ingestion.dlq")
    ]
    assert dlq_publishes == []
    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant,
    )
    assert obs_count == 1
