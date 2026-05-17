"""M3.1 — DLQ writer integration tests.

Real broker (testcontainers Kafka) + real DB (fresh_db) per the M3.1
work order. Mocking would defeat the load-bearing claims:
  - RLS isolation: requires a real Postgres + a non-superuser role.
  - Idempotent UPSERT: requires the real partial-unique semantics
    around NULL raw_s3_key.
  - Continues-on-DB-error: requires a real connection that can
    actually fail mid-batch.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
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
    pytest.mark.timeout(120),
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


def _dlq_envelope_bytes(
    *,
    tenant_id: UUID,
    failure_kind: str = "normalizer.parse_failure",
    raw_s3_key: str | None = None,
    error_summary: str = "fixture error",
    source: str = "slack",
) -> bytes:
    """Build a wire-format DLQ envelope. Matches the DLQEnvelope schema
    (envelope_version=1, etc.)."""
    return orjson.dumps({
        "envelope_version": 1,
        "tenant_id": str(tenant_id),
        "source": source,
        "failure_kind": failure_kind,
        "raw_s3_key": raw_s3_key,
        "error_summary": error_summary,
        "error_context": {},
        "failed_at": _NOW.isoformat(),
    })


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    """Insert one tenant row + return its id."""
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"dlq-test-{tid.hex[:8]}",
    )
    return tid


async def _publish_to_dlq(
    bootstrap: str,
    envelopes: list[bytes],
) -> None:
    """Publish a list of pre-built envelopes to ingestion.raw[dlq].
    Uses confluent_kafka.Producer directly — bypasses the worker code
    so the test exercises the DLQ writer in isolation."""
    from confluent_kafka import Producer as RawProducer

    p = RawProducer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "zstd",
    })
    for b in envelopes:
        p.produce("ingestion.dlq", value=b, key=b"key")
    p.flush(timeout=30)


def _create_dlq_topic(bootstrap: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futs = admin.create_topics([
        NewTopic("ingestion.dlq", num_partitions=4, replication_factor=1),
    ])
    for f in futs.values():
        f.result(timeout=30)


# =====================================================================
# 1. Happy path — 3 DLQ envelopes → 3 ingestion_failures rows.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_dlq_writer_consumes_and_upserts(fresh_db: asyncpg.Pool):
    from services.ingestion.writers.dlq_writer import (
        DLQWriterConfig, run_dlq_writer,
    )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_dlq_topic(bootstrap)

        # Three tenants, three failure rows.
        tenants = [await _seed_tenant(fresh_db) for _ in range(3)]
        envelopes = [
            _dlq_envelope_bytes(
                tenant_id=t,
                raw_s3_key=f"dev/slack/{t}/2026-05/aa/" + "a" * 40 + ".json",
                error_summary=f"parse failed for tenant {i}",
            )
            for i, t in enumerate(tenants)
        ]
        await _publish_to_dlq(bootstrap, envelopes)

        result = await run_dlq_writer(
            DLQWriterConfig(
                bootstrap_servers=bootstrap,
                consumer_group="dlq-writer-test-1",
                stop_after=3,
            ),
            pool=fresh_db,
        )

        assert result["consumed"] == 3
        assert result["upserted"] == 3

        # Bypass RLS to verify all rows landed (the writer set
        # current_tenant per-row; we want to count across tenants).
        async with fresh_db.acquire() as conn:
            await conn.execute("SET LOCAL row_security = off")
            rows = await conn.fetch(
                "SELECT tenant_id, failure_kind, error_summary "
                "FROM ingestion_failures ORDER BY first_seen_at"
            )
        assert len(rows) == 3
        assert {r["tenant_id"] for r in rows} == set(tenants)
        assert all(r["failure_kind"] == "normalizer_parse_error" for r in rows)


# =====================================================================
# 2. UPSERT idempotency — same envelope twice → 1 row, attempt_count=2.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_dlq_writer_upsert_idempotent(fresh_db: asyncpg.Pool):
    from services.ingestion.writers.dlq_writer import (
        DLQWriterConfig, run_dlq_writer,
    )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_dlq_topic(bootstrap)

        tid = await _seed_tenant(fresh_db)
        raw_key = f"dev/slack/{tid}/2026-05/aa/" + "a" * 40 + ".json"
        envelope = _dlq_envelope_bytes(
            tenant_id=tid, raw_s3_key=raw_key, error_summary="first try",
        )
        # Publish the SAME envelope twice.
        await _publish_to_dlq(bootstrap, [envelope, envelope])

        await run_dlq_writer(
            DLQWriterConfig(
                bootstrap_servers=bootstrap,
                consumer_group="dlq-writer-test-2",
                stop_after=2,
            ),
            pool=fresh_db,
        )

        async with fresh_db.acquire() as conn:
            await conn.execute("SET LOCAL row_security = off")
            rows = await conn.fetch(
                "SELECT id, attempt_count, first_seen_at, last_seen_at "
                "FROM ingestion_failures WHERE tenant_id = $1",
                tid,
            )
        # ===== LOAD-BEARING ASSERTION =====
        assert len(rows) == 1, (
            f"UPSERT key (tenant_id, source, raw_s3_key, failure_kind) "
            f"must dedup; got {len(rows)} rows"
        )
        assert rows[0]["attempt_count"] == 2


# =====================================================================
# 3. RLS isolation — tenant A's query must NOT see tenant B's failure.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_dlq_writer_respects_rls(fresh_db: asyncpg.Pool):
    """Per LLD §11: ingestion_failures has tenant_isolation RLS policy.
    The DLQ writer's upsert sets `current_tenant` per-row, so reads
    from a non-bypass session see only their own tenant's rows.

    This is the LOAD-BEARING M3.1 RLS test. Same skip pattern as
    `test_rls_policy_isolates_by_tenant` in test_migrations.py: the
    local dev DB's `company_os` role has BYPASSRLS, so the
    behavioural check (a non-super tenant_a query MUST NOT see
    tenant_b rows) cannot be meaningfully exercised here. CI runs
    as a non-super role, which IS where this assertion runs for real.
    """
    from services.ingestion.writers.dlq_writer import (
        DLQWriterConfig, run_dlq_writer,
    )

    # Same skip the codebase uses everywhere RLS behaviour is asserted.
    async with fresh_db.acquire() as conn:
        is_super = await conn.fetchval(
            "SELECT usesuper OR usebypassrls FROM pg_user "
            "WHERE usename = current_user"
        )
    if is_super:
        pytest.skip(
            "Connecting role is SUPERUSER/BYPASSRLS — Postgres bypasses "
            "RLS regardless of FORCE. Same skip applies to "
            "test_migrations.test_rls_policy_isolates_by_tenant and "
            "lib/shared/tests/test_rls_isolation.py in this dev env; "
            "CI runs as a non-super role."
        )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_dlq_topic(bootstrap)

        tenant_a = await _seed_tenant(fresh_db)
        tenant_b = await _seed_tenant(fresh_db)

        await _publish_to_dlq(bootstrap, [
            _dlq_envelope_bytes(
                tenant_id=tenant_a,
                raw_s3_key=f"dev/slack/{tenant_a}/2026-05/aa/" + "a" * 40 + ".json",
                error_summary="tenant A failure",
            ),
            _dlq_envelope_bytes(
                tenant_id=tenant_b,
                raw_s3_key=f"dev/slack/{tenant_b}/2026-05/aa/" + "b" * 40 + ".json",
                error_summary="tenant B failure",
            ),
        ])

        await run_dlq_writer(
            DLQWriterConfig(
                bootstrap_servers=bootstrap,
                consumer_group="dlq-writer-test-3",
                stop_after=2,
            ),
            pool=fresh_db,
        )

        # Acquire a fresh connection, leave row_security ON, set
        # current_tenant=A, query: must see ONLY A's row.
        async with fresh_db.acquire() as conn:
            await conn.execute("SET LOCAL row_security = on")
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1, true)",
                str(tenant_a),
            )
            rows = await conn.fetch(
                "SELECT tenant_id, error_summary FROM ingestion_failures"
            )

        # ===== LOAD-BEARING RLS ASSERTION =====
        # Visible exact assertion — set comparison, not paraphrase.
        assert {r["tenant_id"] for r in rows} == {tenant_a}, (
            f"RLS leak — tenant A's session saw rows from tenants "
            f"{ {r['tenant_id'] for r in rows} }, expected only "
            f"{ {tenant_a} }"
        )
        assert len(rows) == 1
        assert rows[0]["error_summary"] == "tenant A failure"


# =====================================================================
# 4. DB error → worker continues to next message.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_dlq_writer_continues_on_postgres_error(
    fresh_db: asyncpg.Pool, monkeypatch,
):
    """Inject a transient DB error for the FIRST envelope; assert
    the writer logs + bumps metric + processes the SECOND envelope
    normally. PRIME DIRECTIVE: a transient DB error must not crash
    the consumer.
    """
    from services.ingestion.writers import dlq_writer as dlq_writer_mod
    from services.ingestion.writers.dlq_writer import (
        DLQWriterConfig, run_dlq_writer,
    )

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _create_dlq_topic(bootstrap)

        tenant_a = await _seed_tenant(fresh_db)
        tenant_b = await _seed_tenant(fresh_db)

        await _publish_to_dlq(bootstrap, [
            _dlq_envelope_bytes(
                tenant_id=tenant_a,
                raw_s3_key=f"dev/slack/{tenant_a}/2026-05/aa/" + "a" * 40 + ".json",
                error_summary="injected fail",
            ),
            _dlq_envelope_bytes(
                tenant_id=tenant_b,
                raw_s3_key=f"dev/slack/{tenant_b}/2026-05/aa/" + "b" * 40 + ".json",
                error_summary="should succeed",
            ),
        ])

        original_upsert = dlq_writer_mod.upsert_failure
        call_count = {"n": 0}

        async def flaky_upsert(conn, env):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise asyncpg.PostgresConnectionError(
                    "simulated transient DB error"
                )
            await original_upsert(conn, env)

        monkeypatch.setattr(
            "services.ingestion.writers.dlq_writer.dlq_writer.upsert_failure",
            flaky_upsert,
        )
        dlq_writer_mod.reset_metrics()

        result = await run_dlq_writer(
            DLQWriterConfig(
                bootstrap_servers=bootstrap,
                consumer_group="dlq-writer-test-4",
                stop_after=2,
            ),
            pool=fresh_db,
        )

        # ===== LOAD-BEARING ASSERTIONS =====
        # Both messages were consumed (writer didn't stall on the
        # first one's error).
        assert result["consumed"] == 2
        # Exactly one upsert succeeded (the second message).
        assert result["upserted"] == 1
        # Metrics reflect the failure.
        m = dlq_writer_mod.get_metrics()
        assert m["dlq_writer.db_error"] == 1, m

        # The DB has tenant B's row only (tenant A failed).
        async with fresh_db.acquire() as conn:
            await conn.execute("SET LOCAL row_security = off")
            rows = await conn.fetch(
                "SELECT tenant_id FROM ingestion_failures"
            )
        assert {r["tenant_id"] for r in rows} == {tenant_b}
