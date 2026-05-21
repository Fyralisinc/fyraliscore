"""Validation-run assertions (A29 / Decision 5).

Layers two run-level invariants on top of the per-tenant
`backfill_harness.assertions` (which this module re-exports for one-stop
import):

  - `assert_external_id_unique_across_paths` — the dedup contract
    (HLD §02 L278): `observations(source_channel, external_id,
    occurred_at)` is globally unique. A backfilled event and a live
    event for the SAME logical event must dedup to one row. The DB index
    enforces it; this asserts no duplicates slipped in (and, once the
    M-Validate-Live phase lands, that cross-path events collapse).

  - `assert_zero_partition_missing` — A28's missing-partition DLQ
    routing must NOT fire in a healthy run (fixtures are in-range). Reads
    `ingestion.dlq` and asserts no `partition_missing` failures for the
    run's tenants. (Run 2 / fault injection — deferred to M-Validate-
    Live — flips this to assert routing WAS observed when injected.)
"""
from __future__ import annotations

import logging
from uuid import UUID

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer

# Re-export the per-tenant backfill assertions so callers import from one
# place.
from services.synthetic.backfill_harness.assertions import (  # noqa: F401
    PropertyViolation,
    assert_all_complete,
    assert_completion_emitted_per_tenant,
    assert_cursor_monotonic_per_shard,
    assert_no_duplicate_observations,
    assert_observation_count_matches_fixture,
    assert_reshare_cycles_completed,
)


log = logging.getLogger(__name__)

_DLQ_TOPIC = "ingestion.dlq"


async def assert_external_id_unique_across_paths(pool: asyncpg.Pool) -> int:
    """Assert `(source_channel, external_id, occurred_at)` is unique across
    ALL observations. Returns the row count checked.

    This is the cross-path dedup invariant. The unique index makes a
    duplicate INSERT fail, so a violation here would mean two rows that
    SHOULD have collapsed didn't share an external_id — a parity break,
    not an index failure.
    """
    dupes = await pool.fetch(
        """
        SELECT source_channel, external_id, occurred_at, count(*) AS n
          FROM observations
         WHERE external_id IS NOT NULL
         GROUP BY source_channel, external_id, occurred_at
        HAVING count(*) > 1
        """
    )
    if dupes:
        sample = [
            f"{d['source_channel']}/{d['external_id']}×{d['n']}"
            for d in dupes[:5]
        ]
        raise PropertyViolation(
            f"{len(dupes)} duplicate (source_channel, external_id, "
            f"occurred_at) group(s) in observations — cross-path dedup "
            f"broken: {sample}"
        )
    total = int(await pool.fetchval("SELECT count(*) FROM observations"))
    return total


async def assert_zero_partition_missing(
    *,
    bootstrap_servers: str,
    tenant_ids: set[UUID] | None = None,
    poll_timeout_ms: int = 3000,
) -> int:
    """Assert no `partition_missing` DLQ failures were produced this run.

    Reads `ingestion.dlq` from the beginning with a fresh consumer group
    (single bounded `getmany`, never an unbounded `async for` — the
    latter blocks when idle), counts envelopes whose error_context marks
    a partition-missing failure (optionally filtered to `tenant_ids`).
    Returns the count; raises if > 0.
    """
    consumer = AIOKafkaConsumer(
        _DLQ_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=f"validation-dlq-probe-{UUID(int=0)}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    offending: list[str] = []
    try:
        batches = await consumer.getmany(timeout_ms=poll_timeout_ms)
        for _tp, messages in batches.items():
            for msg in messages:
                try:
                    env = orjson.loads(msg.value)
                except Exception:  # noqa: BLE001
                    continue
                ctx = env.get("error_context") or {}
                summary = env.get("error_summary") or ""
                is_partition = (
                    ctx.get("reason") == "partition_missing"
                    or "partition_missing" in summary
                )
                if not is_partition:
                    continue
                if tenant_ids is not None:
                    tid = env.get("tenant_id")
                    if tid is None or UUID(str(tid)) not in tenant_ids:
                        continue
                offending.append(summary[:120])
    finally:
        await consumer.stop()

    if offending:
        raise PropertyViolation(
            f"{len(offending)} partition_missing DLQ failure(s) in a "
            f"healthy run (fixtures should be in partition range): "
            f"{offending[:3]}"
        )
    return 0
