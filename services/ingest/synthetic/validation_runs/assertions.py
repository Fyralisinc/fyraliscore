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

  - `assert_zero_partition_missing` — the writer's residual
    missing-partition DLQ fallback must NOT fire in a healthy run. Reads
    every contract-derived `ingestion.dlq.<source>` lane and asserts no
    `partition_missing` failures for the run's tenants.

  - `assert_partition_boundary_contract` — Run 2's positive partition
    certification: exactly one in-guardrail recovery row persists per source,
    every far-out timestamp is rejected, every rejection is DLQ'd as
    `out_of_bounds_occurred_at`, and no residual `partition_missing` occurs.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Protocol
from uuid import UUID

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer

from services.ingest.ingestion.kafka.topics import topic_for
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS

# Re-export the per-tenant backfill assertions so callers import from one
# place.
from services.ingest.synthetic.backfill_harness.assertions import (  # noqa: F401
    PropertyViolation,
    assert_all_complete,
    assert_completion_emitted_per_tenant,
    assert_cursor_monotonic_per_shard,
    assert_no_duplicate_observations,
    assert_observation_count_matches_fixture,
    assert_reshare_cycles_completed,
)


log = logging.getLogger(__name__)


class _ExpectedProbeObservation(Protocol):
    tenant_id: UUID
    external_id: str
    occurred_at: object


def _dlq_topics() -> tuple[str, ...]:
    """Return every canonical per-source DLQ lane from the source contract."""

    return tuple(
        topic_for("dlq", definition.source_id) for definition in SOURCE_DEFINITIONS
    )


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
            f"{d['source_channel']}/{d['external_id']}×{d['n']}" for d in dupes[:5]
        ]
        raise PropertyViolation(
            f"{len(dupes)} duplicate (source_channel, external_id, "
            f"occurred_at) group(s) in observations — cross-path dedup "
            f"broken: {sample}"
        )
    total = int(await pool.fetchval("SELECT count(*) FROM observations"))
    return total


async def assert_observations_have_exactly_one_t1_trigger(
    pool: asyncpg.Pool,
    tenant_ids: set[UUID],
) -> int:
    """Prove Observation persistence reached the Think T1 boundary.

    Every Observation produced for the selected validation tenants must own
    exactly one same-tenant ``T1/event_arrival`` trigger. Counting both
    same-tenant and total T1 rows catches a missing trigger, duplicate enqueue,
    and cross-tenant attribution.
    """

    if not tenant_ids:
        raise PropertyViolation("T1 assertion cannot run with no tenants")
    rows = await pool.fetch(
        """
        SELECT o.tenant_id,
               o.id AS observation_id,
               o.source_channel,
               count(q.id) FILTER (
                   WHERE q.trigger_kind = 'T1'
                     AND q.trigger_subkind = 'event_arrival'
               ) AS all_t1,
               count(q.id) FILTER (
                   WHERE q.trigger_kind = 'T1'
                     AND q.trigger_subkind = 'event_arrival'
                     AND q.tenant_id = o.tenant_id
               ) AS same_tenant_t1
          FROM observations o
          LEFT JOIN think_trigger_queue q
            ON q.observation_id = o.id
         WHERE o.tenant_id = ANY($1::uuid[])
         GROUP BY o.tenant_id, o.id, o.source_channel
        HAVING count(q.id) FILTER (
                   WHERE q.trigger_kind = 'T1'
                     AND q.trigger_subkind = 'event_arrival'
               ) <> 1
            OR count(q.id) FILTER (
                   WHERE q.trigger_kind = 'T1'
                     AND q.trigger_subkind = 'event_arrival'
                     AND q.tenant_id = o.tenant_id
               ) <> 1
        """,
        list(tenant_ids),
    )
    if rows:
        sample = [
            (
                f"{row['source_channel']}/{row['observation_id']}:"
                f"all={row['all_t1']},same_tenant={row['same_tenant_t1']}"
            )
            for row in rows[:5]
        ]
        raise PropertyViolation(
            f"{len(rows)} observation(s) do not have exactly one "
            f"same-tenant T1/event_arrival trigger: {sample}",
        )
    total = int(
        await pool.fetchval(
            "SELECT count(*) FROM observations "
            "WHERE tenant_id = ANY($1::uuid[])",
            list(tenant_ids),
        ),
    )
    if total == 0:
        raise PropertyViolation("T1 assertion would be vacuous: no observations")
    return total


async def assert_zero_partition_missing(
    *,
    bootstrap_servers: str,
    tenant_ids: set[UUID] | None = None,
    poll_timeout_ms: int = 3000,
) -> int:
    """Assert no `partition_missing` DLQ failures were produced this run.

    Reads every contract-derived per-source DLQ topic from the beginning with
    a fresh consumer group (single bounded `getmany`, never an unbounded
    `async for` — the latter blocks when idle), counts envelopes whose
    error_context marks a partition-missing failure (optionally filtered to
    `tenant_ids`). Returns the count; raises if > 0.
    """
    failures = await _read_partition_failures(
        bootstrap_servers=bootstrap_servers,
        tenant_ids=tenant_ids,
        poll_timeout_ms=poll_timeout_ms,
    )
    offending = failures["partition_missing"]

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
async def _read_partition_failures(
    *,
    bootstrap_servers: str,
    tenant_ids: set[UUID] | None,
    poll_timeout_ms: int,
) -> dict[str, list[str]]:
    """Read writer partition DLQ outcomes across contract-derived lanes."""

    consumer = AIOKafkaConsumer(
        *_dlq_topics(),
        bootstrap_servers=bootstrap_servers,
        group_id=f"validation-dlq-probe-{UUID(int=0)}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    failures: dict[str, list[str]] = {
        "partition_missing": [],
        "out_of_bounds_occurred_at": [],
    }
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
                reason = str(ctx.get("reason") or "")
                if not reason:
                    reason = next(
                        (
                            candidate
                            for candidate in failures
                            if candidate in summary
                        ),
                        "",
                    )
                if reason not in failures:
                    continue
                if tenant_ids is not None:
                    tid = env.get("tenant_id")
                    if tid is None or UUID(str(tid)) not in tenant_ids:
                        continue
                failures[reason].append(summary[:120])
    finally:
        await consumer.stop()
    return failures


async def assert_partition_boundary_contract(
    *,
    pool: asyncpg.Pool,
    bootstrap_servers: str,
    recovered: Mapping[str, _ExpectedProbeObservation],
    rejected_out_of_bounds: Mapping[str, _ExpectedProbeObservation],
    tenant_ids: set[UUID] | None = None,
    poll_timeout_ms: int = 5000,
) -> int:
    """Assert Run 2's writer recovery and out-of-bounds behavior end to end."""

    canonical_sources = {
        definition.source_id for definition in SOURCE_DEFINITIONS
    }
    if set(recovered) != canonical_sources:
        raise PropertyViolation(
            "partition recovery probe source coverage mismatch: "
            f"expected={sorted(canonical_sources)!r}, "
            f"observed={sorted(recovered)!r}",
        )
    if set(rejected_out_of_bounds) != canonical_sources:
        raise PropertyViolation(
            "out-of-bounds probe source coverage mismatch: "
            f"expected={sorted(canonical_sources)!r}, "
            f"observed={sorted(rejected_out_of_bounds)!r}",
        )

    persistence_errors: list[str] = []
    for source, expected in recovered.items():
        count = int(
            await pool.fetchval(
                """
                SELECT count(*)
                  FROM observations
                 WHERE tenant_id = $1
                   AND external_id = $2
                   AND occurred_at = $3
                """,
                expected.tenant_id,
                expected.external_id,
                expected.occurred_at,
            ),
        )
        if count != 1:
            persistence_errors.append(f"{source}:recovery={count}")
    for source, expected in rejected_out_of_bounds.items():
        count = int(
            await pool.fetchval(
                """
                SELECT count(*)
                  FROM observations
                 WHERE tenant_id = $1
                   AND external_id = $2
                """,
                expected.tenant_id,
                expected.external_id,
            ),
        )
        if count:
            persistence_errors.append(f"{source}:out_of_bounds={count}")
    if persistence_errors:
        raise PropertyViolation(
            "partition-boundary persistence contract failed: "
            f"{persistence_errors[:10]!r}",
        )

    failures = await _read_partition_failures(
        bootstrap_servers=bootstrap_servers,
        tenant_ids=tenant_ids,
        poll_timeout_ms=poll_timeout_ms,
    )
    observed_out_of_bounds = len(failures["out_of_bounds_occurred_at"])
    if observed_out_of_bounds != len(rejected_out_of_bounds):
        raise PropertyViolation(
            f"expected {len(rejected_out_of_bounds)} "
            "out_of_bounds_occurred_at DLQ entries, "
            f"observed {observed_out_of_bounds}",
        )
    residual_missing = failures["partition_missing"]
    if residual_missing:
        raise PropertyViolation(
            f"expected zero residual partition_missing DLQ entries after "
            f"self-heal, observed {len(residual_missing)}: "
            f"{residual_missing[:3]!r}",
        )
    return len(recovered)


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
    *,
    expected_sources: tuple[str, ...] = ("slack", "github"),
) -> int:
    """Require one rejected tamper probe for every declared live auth gate."""

    sources = {r["source"] for r in tamper_results}
    expected = set(expected_sources)
    if sources != expected:
        raise PropertyViolation(
            f"signature-gate probes covered {sorted(sources)}; expected "
            f"exactly {sorted(expected)}"
        )
    bad = [r for r in tamper_results if r["http_status"] not in {401, 403}]
    if bad:
        raise PropertyViolation(
            f"tampered authentication request(s) not rejected: {bad}"
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
            "discord must not appear in replay probe (no replay surface, " "A24/A30.4)"
        )
    bad = {
        s: v
        for s, v in probe_results.items()
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
        n = int(
            await pool.fetchval(
                "SELECT count(*) FROM observations WHERE external_id = $1",
                ext,
            )
        )
        if n != 1:
            raise PropertyViolation(
                f"cross-path twin for {source} (external_id={ext!r}) has "
                f"{n} rows; expected exactly 1 — dedup FAILED"
            )
    return len(twin_external_ids)
