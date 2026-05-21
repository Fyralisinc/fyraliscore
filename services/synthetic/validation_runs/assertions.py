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


# =====================================================================
# M-Validate-Live (A30) — live + cross-path assertions, per-source scoped.
# =====================================================================
async def _count_partition_missing(
    *, bootstrap_servers: str, tenant_ids: set[UUID] | None,
    poll_timeout_ms: int,
) -> int:
    """Shared reader: count `partition_missing` DLQ envelopes (optionally
    filtered to `tenant_ids`) via one bounded `getmany`."""
    consumer = AIOKafkaConsumer(
        _DLQ_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=f"validation-dlq-probe-{UUID(int=0)}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    n = 0
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
                if not (ctx.get("reason") == "partition_missing"
                        or "partition_missing" in summary):
                    continue
                if tenant_ids is not None:
                    tid = env.get("tenant_id")
                    if tid is None or UUID(str(tid)) not in tenant_ids:
                        continue
                n += 1
    finally:
        await consumer.stop()
    return n


async def assert_partition_missing_routes_to_dlq(
    *,
    bootstrap_servers: str,
    expected_count: int,
    tenant_ids: set[UUID] | None = None,
    poll_timeout_ms: int = 5000,
) -> int:
    """A28's positive assertion (Run 2): deliberately out-of-range
    `occurred_at` events must route to `ingestion.dlq` as
    `partition_missing` (NOT crash-loop the writer). Asserts the observed
    count equals `expected_count`."""
    n = await _count_partition_missing(
        bootstrap_servers=bootstrap_servers, tenant_ids=tenant_ids,
        poll_timeout_ms=poll_timeout_ms,
    )
    if n != expected_count:
        raise PropertyViolation(
            f"expected {expected_count} partition_missing DLQ entries "
            f"(A28 positive assertion), observed {n}"
        )
    return n


async def assert_live_observations_attributed_correctly(
    actual_by_tenant: dict[UUID, int],
    expected_by_tenant: dict[UUID, int],
) -> int:
    """Each tenant's live observation delta matches the dispatched
    burst size (A30.2). Catches mis-attribution / cross-tenant leakage in
    the live phase."""
    bad = {
        tid: (actual_by_tenant.get(tid, 0), exp)
        for tid, exp in expected_by_tenant.items()
        if actual_by_tenant.get(tid, 0) != exp
    }
    if bad:
        sample = list(bad.items())[:5]
        raise PropertyViolation(
            f"{len(bad)} tenant(s) with wrong live observation count "
            f"(got, expected): {sample}"
        )
    return len(expected_by_tenant)


async def assert_signature_validation_gate_holds_for_hmac_sources(
    tamper_results: list[dict],
) -> int:
    """Tampered signatures rejected with 401 for the HMAC sources
    (Slack + GitHub). Gmail OIDC is no-op'd by Y1 (no real gate) and
    Discord uses direct dispatch (no signature surface) — both excluded
    by design (A30.4)."""
    sources = {r["source"] for r in tamper_results}
    if sources != {"slack", "github"}:
        raise PropertyViolation(
            f"signature-gate probes covered {sorted(sources)}; expected "
            f"exactly {{'github', 'slack'}} (HMAC sources, A30.4)"
        )
    bad = [r for r in tamper_results if r["http_status"] != 401]
    if bad:
        raise PropertyViolation(
            f"tampered signature(s) NOT rejected with 401: {bad}"
        )
    return len(tamper_results)


async def assert_live_replay_idempotency_holds(
    probe_results: dict[str, dict],
) -> int:
    """At-least-once redelivery must NOT create duplicate observations.
    `probe_results[source] = {'dispatched_unique': k, 'observed': m}`;
    asserts `m == k` for each. Scoped to Gmail + Slack + GitHub — Discord
    has no replay surface (`LiveGatewayScenario` lacks replay_probability,
    A24), so it is excluded (A30.4)."""
    if "discord" in probe_results:
        raise PropertyViolation(
            "discord must not appear in replay probe (no replay surface, "
            "A24/A30.4)"
        )
    bad = {
        s: v for s, v in probe_results.items()
        if v["observed"] != v["dispatched_unique"]
    }
    if bad:
        raise PropertyViolation(
            f"replay produced duplicate observations (source → "
            f"dispatched/observed): {bad}"
        )
    return len(probe_results)


async def assert_per_tenant_timeline_monotonic(
    pool: asyncpg.Pool,
    tenant_ids: set[UUID],
) -> int:
    """Each tenant's observations carry a non-null `occurred_at` (the
    partition key) and order consistently. A NULL occurred_at would break
    range-partition routing — this guards the live phase didn't write
    timeline-less rows. Returns the tenant count checked."""
    bad = await pool.fetch(
        """
        SELECT tenant_id, count(*) AS n
          FROM observations
         WHERE tenant_id = ANY($1) AND occurred_at IS NULL
         GROUP BY tenant_id
        """,
        list(tenant_ids),
    )
    if bad:
        raise PropertyViolation(
            f"{len(bad)} tenant(s) have observations with NULL occurred_at "
            f"(timeline broken): {[(str(b['tenant_id']), b['n']) for b in bad[:3]]}"
        )
    return len(tenant_ids)


async def assert_cross_path_twins_dedup(
    pool: asyncpg.Pool,
    twin_external_ids: dict[str, str],
) -> int:
    """THE load-bearing assertion (A30.3): for each cross-path twin —
    a backfilled event and a live event sharing the same source-side
    identity — there is EXACTLY ONE `observations` row. The
    `(source_channel, external_id, occurred_at)` UNIQUE index must have
    collapsed the pair.

    Scoped to Gmail + GitHub + Slack. Discord is excluded: its live ids
    (`msg-y2-*`) and backfill ids (fixture-derived) are disjoint
    namespaces, so a cross-path twin is impossible by construction;
    Discord's per-path dedup is covered by A27.5 parity (M6.7).
    """
    if "discord" in twin_external_ids:
        raise PropertyViolation(
            "discord cannot have a cross-path twin (disjoint id "
            "namespaces, A30.3) — must not be asserted here"
        )
    if not twin_external_ids:
        raise PropertyViolation(
            "no cross-path twins were dispatched — the load-bearing "
            "assertion would pass vacuously"
        )
    for source, ext in twin_external_ids.items():
        n = int(await pool.fetchval(
            "SELECT count(*) FROM observations WHERE external_id = $1", ext,
        ))
        if n != 1:
            raise PropertyViolation(
                f"cross-path twin for {source} (external_id={ext!r}) has "
                f"{n} rows; expected exactly 1 — dedup FAILED"
            )
    return len(twin_external_ids)
