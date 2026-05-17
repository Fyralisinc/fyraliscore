"""M3.2 — Embedding worker integration tests.

Real Kafka (testcontainers) + real Postgres (fresh_db) + mocked
Ollama. The Ollama mock lets us assert against deterministic vectors
without needing a running Ollama instance — and the load-bearing
re-embed test depends on being able to control the "new" vector
returned by the embed call.

Four tests:

  1. test_embedding_worker_happy_path_succeeds
     One pending observation → worker → embedding populated +
     embedding_pending cleared. Sanity baseline.

  2. test_embedding_worker_supports_reembed_with_existing_embedding
     [LOAD-BEARING] Operator-driven re-embed pattern: insert with
     embedding = <old>, embedding_pending = TRUE. Worker MUST
     overwrite with <new> and clear pending. Without this property
     the LLD's re-embed support is dead.

  3. test_embedding_worker_concurrent_with_inline_safe
     [LOAD-BEARING] Race the worker against an inline path that has
     ALREADY committed the embedding. Worker's UPDATE matches 0
     rows (guard); the inline-set embedding stays untouched.

  4. test_embedding_worker_terminal_ollama_failure_publishes_dlq
     [LOAD-BEARING] Ollama mock always raises. Worker publishes a
     DLQ envelope with failure_kind="embedding.ollama_failure" and
     continues consuming. Observation stays at embedding_pending=
     TRUE — pickup by the M3.3 backlog drainer is the recovery path.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

try:
    import docker as _docker_module  # type: ignore[import-not-found]
    from testcontainers.kafka import KafkaContainer  # type: ignore[import-not-found]
    _HAS_TESTCONTAINERS = True
except ImportError:
    _HAS_TESTCONTAINERS = False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_docker,
    pytest.mark.skipif(
        not _HAS_TESTCONTAINERS,
        reason="testcontainers / docker SDK unavailable",
    ),
    pytest.mark.timeout(180),
]


def _docker_available() -> bool:
    if not _HAS_TESTCONTAINERS:
        return False
    try:
        _docker_module.from_env().ping()
        return True
    except Exception:
        return False


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------
# Ollama stub. The real client retries internally; the stub doesn't
# need to — tests control success/failure directly.
# ---------------------------------------------------------------------
class _StubOllama:
    """Minimal OllamaClient stand-in. Returns `vector` (when set) or
    raises `error` (when set). expected_dim mirrors the real client."""

    expected_dim = 768

    def __init__(
        self,
        vector: list[float] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._vector = vector
        self._error = error
        self.call_count = 0

    async def embed(self, text: str) -> list[float]:
        self.call_count += 1
        if self._error is not None:
            raise self._error
        if self._vector is None:
            raise RuntimeError("test bug: _StubOllama configured with neither vector nor error")
        return self._vector

    async def close(self) -> None:
        pass


def _vec(value: float) -> list[float]:
    """768-element constant vector. Trivially comparable in tests."""
    return [value] * 768


def _vec_string(value: float) -> str:
    """Postgres vector literal form: '[v,v,v,...]'."""
    return "[" + ",".join(repr(value) for _ in range(768)) + "]"


# ---------------------------------------------------------------------
# Postgres / Kafka helpers.
# ---------------------------------------------------------------------
async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"emb-test-{tid.hex[:8]}",
    )
    return tid


async def _ensure_partition(pool: asyncpg.Pool) -> None:
    """Make sure the 2026-05 observations partition exists."""
    from services.observations import partitions
    await partitions.ensure_partitions(pool, as_of=_NOW.date(), months_ahead=1)


async def _insert_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    obs_id: UUID,
    content_text: str,
    embedding: str | None,
    embedding_pending: bool,
    source_channel: str = "slack:message",
    external_id: str | None = None,
) -> None:
    """Direct SQL insert — bypasses the repo so we control every
    field exactly. external_id defaults to None so two inserts
    don't collide on the UNIQUE (source_channel, external_id,
    occurred_at) constraint."""
    if external_id is None:
        external_id = f"ext-{obs_id.hex[:12]}"
    async with pool.acquire() as conn:
        # Use $N::vector cast so the column accepts the literal
        # form without pgvector's asyncpg codec being registered.
        if embedding is None:
            emb_sql = "NULL::vector"
            emb_params: list[Any] = []
        else:
            emb_sql = "$11::vector"
            emb_params = [embedding]

        await conn.execute(
            f"""
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                source_actor_ref, actor_id, content, content_text,
                embedding_pending, embedding,
                trust_tier, external_id
            ) VALUES (
                $1, $2, $3, $4, $5,
                NULL, NULL, $6::jsonb, $7,
                $8, {emb_sql},
                $9, $10
            )
            """,
            obs_id, tenant_id, _NOW, "signal", source_channel,
            "{}", content_text,
            embedding_pending, "T2",
            external_id, *emb_params,
        )


async def _read_observation(
    pool: asyncpg.Pool, obs_id: UUID,
) -> dict:
    """Read embedding (as text), embedding_pending. The codec is
    not registered, so embedding comes back as the string form
    '[v1,v2,...]'."""
    async with pool.acquire() as conn:
        await conn.execute("SET LOCAL row_security = off")
        row = await conn.fetchrow(
            "SELECT embedding::text AS emb_text, embedding_pending "
            "FROM observations WHERE id = $1", obs_id,
        )
    assert row is not None, f"observation {obs_id} missing"
    return dict(row)


def _create_topics(bootstrap: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futs = admin.create_topics([
        NewTopic("ingestion.embedding", num_partitions=4, replication_factor=1),
        NewTopic("ingestion.dlq",       num_partitions=4, replication_factor=1),
    ])
    for f in futs.values():
        f.result(timeout=30)


def _publish_envelope(
    bootstrap: str, *, tenant_id: UUID, source: str, observation_id: UUID,
) -> None:
    from confluent_kafka import Producer as RawProducer
    from services.ingestion.embedding.models import EmbeddingEnvelope
    env = EmbeddingEnvelope(
        tenant_id=tenant_id, source=source,  # type: ignore[arg-type]
        observation_id=observation_id, enqueued_at=_NOW,
    )
    p = RawProducer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "zstd",
    })
    p.produce(
        "ingestion.embedding",
        value=orjson.dumps(env.model_dump(mode="json")),
        key=str(tenant_id).encode("utf-8"),
    )
    p.flush(timeout=30)


def _drain_dlq(bootstrap: str, expected: int, timeout_s: float = 15.0) -> list[dict]:
    from confluent_kafka import Consumer as RawConsumer
    c = RawConsumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"emb-dlq-drain-{uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe(["ingestion.dlq"])
    out: list[dict] = []
    deadline = asyncio.get_event_loop().time() + timeout_s
    while len(out) < expected:
        msg = c.poll(1.0)
        if msg is None:
            if asyncio.get_event_loop().time() > deadline:
                break
            continue
        if msg.error():
            continue
        out.append(orjson.loads(msg.value()))
    c.close()
    return out


# =====================================================================
# 1. Happy path.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_embedding_worker_happy_path_succeeds(fresh_db: asyncpg.Pool):
    """Pending observation → worker → embedding populated, pending
    cleared. Baseline that proves the wire format + UPDATE work."""
    from services.ingestion.writers.embedding_worker import (
        EmbeddingWorkerConfig, run_embedding_worker, get_metrics,
        reset_metrics,
    )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_topics(bootstrap)

        await _ensure_partition(fresh_db)
        tenant_id = await _seed_tenant(fresh_db)
        obs_id = uuid4()
        await _insert_observation(
            fresh_db,
            tenant_id=tenant_id, obs_id=obs_id,
            content_text="please embed this text",
            embedding=None, embedding_pending=True,
        )

        _publish_envelope(
            bootstrap,
            tenant_id=tenant_id, source="slack",
            observation_id=obs_id,
        )

        stub = _StubOllama(vector=_vec(0.1))
        reset_metrics()
        result = await run_embedding_worker(
            EmbeddingWorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="emb-test-1",
                stop_after=1,
            ),
            pool=fresh_db,
            ollama=stub,
        )

        assert result["consumed"] == 1
        assert result["embedded"] == 1
        assert stub.call_count == 1

        row = await _read_observation(fresh_db, obs_id)
        assert row["embedding_pending"] is False
        # The embedding column now contains the 0.1 vector.
        assert row["emb_text"] is not None
        assert row["emb_text"].startswith("[0.1,")

        m = get_metrics()
        assert m["embedding_worker.embeds_succeeded"] == 1, m
        assert m["embedding_worker.embeds_failed"] == 0, m


# =====================================================================
# 2. Re-embed support. LOAD-BEARING.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_embedding_worker_supports_reembed_with_existing_embedding(
    fresh_db: asyncpg.Pool,
):
    """LOAD-BEARING (M3.2 A3 in LLD amendments): the LLD §5.4 guard
    `WHERE embedding_pending = TRUE` allows operator-driven re-embed
    on a row that already has an embedding. The alternative guard
    `WHERE embedding IS NULL` would silently fail because the
    embedding column is NOT NULL on this row.

    Setup:
      - Insert obs with embedding = <old vector (0.7 × 768)>,
        embedding_pending = TRUE (the re-embed pattern).
      - Stub Ollama returns <new vector (0.3 × 768)>.
      - Run worker → assert embedding = <new>, pending = FALSE.

    Without this property, operators cannot trigger re-embedding by
    flipping the flag — they would have to NULL out the embedding
    column first, which races with retrieval queries.
    """
    from services.ingestion.writers.embedding_worker import (
        EmbeddingWorkerConfig, run_embedding_worker,
    )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_topics(bootstrap)

        await _ensure_partition(fresh_db)
        tenant_id = await _seed_tenant(fresh_db)
        obs_id = uuid4()

        # Existing embedding = 0.7 × 768. Operator sets pending=TRUE
        # to force re-compute.
        await _insert_observation(
            fresh_db,
            tenant_id=tenant_id, obs_id=obs_id,
            content_text="re-embed me with the new model",
            embedding=_vec_string(0.7),
            embedding_pending=True,
        )

        _publish_envelope(
            bootstrap,
            tenant_id=tenant_id, source="slack",
            observation_id=obs_id,
        )

        # New embedding = 0.3 × 768. Different from old.
        stub = _StubOllama(vector=_vec(0.3))
        result = await run_embedding_worker(
            EmbeddingWorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="emb-test-2",
                stop_after=1,
            ),
            pool=fresh_db,
            ollama=stub,
        )

        # ===== LOAD-BEARING ASSERTIONS =====
        # 1. The worker DID call Ollama (didn't short-circuit on the
        # already-present embedding).
        assert stub.call_count == 1, (
            f"Worker should have called Ollama for the re-embed "
            f"despite the existing embedding; call_count={stub.call_count}"
        )
        # 2. Worker reported the UPDATE landed.
        assert result["embedded"] == 1

        # 3. The DB row now has the NEW embedding (0.3), NOT the
        # original 0.7. Pending cleared.
        row = await _read_observation(fresh_db, obs_id)
        assert row["embedding_pending"] is False
        assert row["emb_text"] is not None
        assert row["emb_text"].startswith("[0.3,"), (
            f"Re-embed did not overwrite the original 0.7 vector; "
            f"got {row['emb_text'][:60]}..."
        )
        assert not row["emb_text"].startswith("[0.7,"), (
            "Re-embed silently no-op'd — the LLD §5.4 guard wording "
            "regressed to `embedding IS NULL` or equivalent."
        )


# =====================================================================
# 3. Concurrent-with-inline safety. LOAD-BEARING.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_embedding_worker_concurrent_with_inline_safe(
    fresh_db: asyncpg.Pool,
):
    """LOAD-BEARING (M3.2 A3, race-safety branch): inline path and
    worker can both target the same observation during the coexistence
    window. The LLD §5.4 guard `WHERE embedding_pending = TRUE`
    ensures whoever flips the flag first wins; the loser's UPDATE
    matches zero rows.

    Setup:
      - Insert obs with embedding_pending = TRUE, embedding = NULL.
      - SIMULATE the inline path winning first: UPDATE the row to
        embedding = <inline vector (0.5 × 768)>, embedding_pending =
        FALSE. This commits BEFORE the worker reads the row.
      - Run worker against the (now-stale) Kafka message.

    Assertions:
      - Worker observes guard_no_op (the row is no longer pending).
      - DB row keeps the inline vector (0.5), NOT overwritten by the
        stub's 0.9 vector.

    Without this property: the worker would race with inline and
    either path could clobber the other's embedding for the same
    logical observation.
    """
    from services.ingestion.writers.embedding_worker import (
        EmbeddingWorkerConfig, run_embedding_worker, get_metrics,
        reset_metrics,
    )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_topics(bootstrap)

        await _ensure_partition(fresh_db)
        tenant_id = await _seed_tenant(fresh_db)
        obs_id = uuid4()
        await _insert_observation(
            fresh_db,
            tenant_id=tenant_id, obs_id=obs_id,
            content_text="will be embedded inline first",
            embedding=None, embedding_pending=True,
        )

        # Publish the worker's signal FIRST (so it's queued).
        _publish_envelope(
            bootstrap,
            tenant_id=tenant_id, source="slack",
            observation_id=obs_id,
        )

        # Simulate the inline path beating the worker: flip the row
        # to embedding = 0.5, pending = FALSE before the worker runs.
        await fresh_db.execute(
            "UPDATE observations SET embedding = $1::vector, "
            "embedding_pending = FALSE WHERE id = $2",
            _vec_string(0.5), obs_id,
        )

        # Worker pulls the (now-stale) message. Stub returns 0.9; the
        # guard should prevent it from landing.
        stub = _StubOllama(vector=_vec(0.9))
        reset_metrics()
        result = await run_embedding_worker(
            EmbeddingWorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="emb-test-3",
                stop_after=1,
            ),
            pool=fresh_db,
            ollama=stub,
        )

        # ===== LOAD-BEARING ASSERTIONS =====
        # 1. Worker consumed the message and committed offset.
        assert result["consumed"] == 1
        # 2. Worker did NOT register an "embedded" — the SELECT
        # short-circuited the Ollama call because pending was FALSE.
        assert result["embedded"] == 0
        assert stub.call_count == 0, (
            f"Worker called Ollama for a non-pending row "
            f"(stub.call_count={stub.call_count}). The pre-Ollama "
            f"SELECT guard regressed."
        )
        # 3. Metrics show the no-op path was taken.
        m = get_metrics()
        assert m["embedding_worker.guard_no_op"] == 1, m
        assert m["embedding_worker.embeds_succeeded"] == 0, m

        # 4. DB row STILL has the inline-path vector (0.5), unchanged.
        row = await _read_observation(fresh_db, obs_id)
        assert row["embedding_pending"] is False
        assert row["emb_text"] is not None
        assert row["emb_text"].startswith("[0.5,"), (
            f"Worker clobbered the inline path's embedding — guard "
            f"failed. Expected vector starting with 0.5, got "
            f"{row['emb_text'][:60]}..."
        )


# =====================================================================
# 4. Terminal Ollama failure → DLQ. LOAD-BEARING.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_embedding_worker_terminal_ollama_failure_publishes_dlq(
    fresh_db: asyncpg.Pool,
):
    """LOAD-BEARING (M3.2): after the OllamaClient's internal retry
    loop, an OllamaError is TERMINAL. The worker MUST:
      - Publish a DLQ envelope with failure_kind="embedding.ollama_failure".
      - Leave the observation at embedding_pending=TRUE (the M3.3
        backlog drainer will catch it later).
      - Commit the Kafka offset (do NOT loop — the client already
        burned its retries).

    Without this property: a flaky Ollama instance would saturate
    Kafka with redelivered messages and the worker would never
    surface the failure to ops.
    """
    from lib.embeddings.ollama import OllamaError
    from services.ingestion.writers.embedding_worker import (
        EmbeddingWorkerConfig, run_embedding_worker, get_metrics,
        reset_metrics,
    )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_topics(bootstrap)

        await _ensure_partition(fresh_db)
        tenant_id = await _seed_tenant(fresh_db)
        obs_id = uuid4()
        await _insert_observation(
            fresh_db,
            tenant_id=tenant_id, obs_id=obs_id,
            content_text="ollama is offline for this one",
            embedding=None, embedding_pending=True,
        )

        _publish_envelope(
            bootstrap,
            tenant_id=tenant_id, source="slack",
            observation_id=obs_id,
        )

        stub = _StubOllama(
            error=OllamaError("simulated ollama outage after retries"),
        )
        reset_metrics()
        result = await run_embedding_worker(
            EmbeddingWorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="emb-test-4",
                stop_after=1,
            ),
            pool=fresh_db,
            ollama=stub,
        )

        # ===== LOAD-BEARING ASSERTIONS =====
        # 1. The worker consumed + committed (didn't loop).
        assert result["consumed"] == 1
        assert result["embedded"] == 0
        # 2. The observation is STILL pending — recovery path lives.
        row = await _read_observation(fresh_db, obs_id)
        assert row["embedding_pending"] is True, (
            "Embedding stayed cleared after a terminal Ollama failure "
            "— the M3.3 backlog drainer will skip this row."
        )
        assert row["emb_text"] is None or row["emb_text"] == ""

        # 3. Metrics reflect the failure + DLQ publish.
        m = get_metrics()
        assert m["embedding_worker.embeds_failed"] == 1, m
        assert m["embedding_worker.dlq_publish.success"] == 1, m

        # 4. One DLQ envelope landed on `ingestion.dlq` with the
        # right failure_kind + tenant_id + source.
        dlq_msgs = _drain_dlq(bootstrap, expected=1, timeout_s=15.0)
        assert len(dlq_msgs) == 1
        env = dlq_msgs[0]
        assert env["failure_kind"] == "embedding.ollama_failure"
        assert env["tenant_id"] == str(tenant_id)
        assert env["source"] == "slack"
        assert "OllamaError" in env["error_summary"] or "ollama" in env["error_summary"].lower()
        assert env["error_context"]["observation_id"] == str(obs_id)
