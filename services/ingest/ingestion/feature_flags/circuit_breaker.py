"""services/ingest/ingestion/feature_flags/circuit_breaker.py
   — Ingestion cutover circuit breaker.

Per ingestion LLD §11.2 (cutover circuit breaker workflow) +
04-implementation-plan.md §M5 condition (3).

=== Design summary ===

Long-running asyncio service. Every `tick_interval_sec` (default 60s):

  1. Measure consumer-group lag on EVERY per-source raw lane
     (`ingestion.raw.<source>`) against its per-source normalizer group
     (`normalizer.<source>`) — the data plane is source-isolated, so a
     tenant can lag on one source's lane while healthy on another.
  2. Sample active tenants from `ingestion.tenant_traffic_signal`
     (the 1% deterministic-hash signal topic; LLD §11.3). Each signal
     carries the tenant's `(source, raw_partition)` so the breaker knows
     which lanes a tenant is actually on.
  3. For each active tenant, take its WORST lane — the maximum lag across
     every (source, partition) it is active on — and check whether that
     exceeds `breach_threshold_sec` (default 60s).
  4. Update per-tenant breach-window counter:
       — In active set AND breached → counter += 1
       — In active set AND healthy  → counter = 0 (recovery within window)
       — Not in active set          → counter unchanged (no traffic = no signal)
  5. When counter reaches `breach_window_ticks` (default 5),
     **TRIP**:
       a. Flip `ingestion.kafka_path_enabled` to FALSE for that tenant
          via `TenantFlags.set_bool(set_by="auto:circuit_breaker")`.
       b. Mark tenant tripped in `circuit_breaker_state`.
       c. Emit `circuit_breaker.tripped` ops alert.

=== Auto-recovery is DISABLED — flag flips are operator-driven ===

Once a tenant is tripped, this service does NOT auto-flip the flag
back. Auto-recovery during an incident produces flapping — the broker
briefly recovers, the breaker re-enables the Kafka path, the broker
re-fails, ad nauseam. Operator must:

  1. Investigate the underlying broker health.
  2. Manually re-enable with an explicit
     `TenantFlags.set_bool(value=True, set_by="operator:<id>")` call.

Step 2 is the entire operator procedure. On the breaker's next tick
after the flip, it observes `kafka_path_enabled=TRUE` for a tenant
whose state row says `tripped=TRUE` and auto-resets its own
bookkeeping (counter→0, tripped→FALSE). This is auto-reset of
BREAKER STATE, not auto-recovery of the FLAG: the flag flip is
operator-controlled, but breaker bookkeeping does not require a
second manual step.

Tenants whose flag is already FALSE (operator-disabled, or tripped by
this breaker on a prior incident) are skipped entirely in step 4 above —
the breaker has nothing to flip for them, and re-flipping FALSE-on-FALSE
would clobber the `set_by` audit trail. (Under the inverted default,
there is no longer a "pre-cutover" population: a tenant with no flag row
is kafka-first and so a breach candidate.)

=== Service shape — matches M3.3's embedding backlog drainer ===

  • `BreakerConfig` dataclass for env-var-driven knobs.
  • `_load_state_for_tenants` / `_persist_state` cursor-style helpers.
  • `run_circuit_breaker(...)` main loop with `stop_event` + `max_ticks`
    for test injection.
  • SIGTERM handler in `main()` sets `stop_event`; the loop completes
    the current tick (at most one persist UPSERT per active tenant)
    and exits clean.
  • Cursor state PERSISTED before sleep so a SIGTERM mid-tick
    doesn't lose the just-computed counter values.

=== Path A — pgbouncer-compatible pool ===

Fourth activation of `statement_cache_size=0` after M3.1, M3.3, M4.2.
The `make_breaker_pool()` helper mirrors `make_session_state_pool()`
exactly.

=== Lag + active-set measurement are INJECTED ===

Production wiring uses real Kafka via `_measure_kafka_lag_default`
and `_sample_active_tenants_default` (both query Kafka via
confluent_kafka.admin / Consumer). Tests inject mock functions to
exercise the state machine without spinning up Kafka.

Subprocess tests inject the same functions via env-var-driven JSON
("M5_BREAKER_FAKE_LAG_PARTITIONS" / "M5_BREAKER_FAKE_ACTIVE_TENANTS")
read by `main()`. This pattern matches M4.3's _subprocess_entrypoint:
real production code path, synthetic injection only at the Kafka
boundary, REAL Postgres for state persistence.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg

from lib.shared.db import configure_connection_timeouts
from services.ingest.ingestion.feature_flags.client import (
    KAFKA_PATH_ENABLED,
    TenantFlags,
)
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)


log = logging.getLogger(__name__)


# In-process metrics. M-Temporal will swap this for the Temporal
# Schedule's built-in metrics + OTel emission.
_metrics: dict[str, float] = {
    "breaker.ticks":                  0.0,
    "breaker.active_tenants_sampled": 0.0,
    "breaker.breach_increments":      0.0,
    "breaker.recovery_resets":        0.0,
    "breaker.trips":                  0.0,
    "breaker.skipped_already_tripped": 0.0,
    "breaker.skipped_flag_disabled":  0.0,
    "breaker.bookkeeping_reset_on_operator_reenable": 0.0,
    "breaker.lag_measurement_failures": 0.0,
    "breaker.signal_read_failures":   0.0,
    "breaker.flag_flip_failures":     0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass
class BreakerConfig:
    """Configuration for one circuit breaker instance.

    Env-var-driven for production (see `main()`); fields are public
    for test injection.
    """

    instance_name: str = "default"
    tick_interval_sec: float = 60.0
    breach_threshold_sec: int = 60   # lag > this = breach for the tick
    breach_window_ticks: int = 5     # 5 consecutive breaches = trip
    # SOURCE-ISOLATED LAG MEASUREMENT (resolves the source-isolation
    # follow-up in docs/ingestion/source-isolation.md). The raw stage is
    # split into per-source topics `ingestion.raw.<source>`, each consumed
    # by a per-source normalizer group `<normalizer_group_base>.<source>`
    # (see services.ingest.ingestion.kafka.topics). The breaker derives the
    # full topic+group set from that module, measures lag on every lane, and
    # trips a tenant on its WORST lane — so a single lagging source lane is
    # enough to pull the tenant back to inline, while a tenant healthy on all
    # its active lanes is never false-tripped.
    #
    # `normalizer_group_base` is the normalizer worker's bare consumer-group
    # id (WorkerConfig.consumer_group, "normalizer"); the per-source groups
    # are built via topics.consumer_group(base, source). Keep it in lockstep
    # with the normalizer.
    #
    # The signal topic is CONTROL-plane (per-tenant, not per-source) and so
    # stays a single topic; its records carry the source in the payload.
    normalizer_group_base: str = "normalizer"
    signal_topic: str = "ingestion.tenant_traffic_signal"
    signal_lookback_sec: int = 90    # read this much recent signal data
    kafka_bootstrap: str = "localhost:9092"


# ---------------------------------------------------------------------
# State + SQL.
# ---------------------------------------------------------------------
@dataclass
class _TenantBreachState:
    tenant_id: UUID
    consecutive_breach_ticks: int
    tripped: bool
    tripped_at: dt.datetime | None
    last_tick_at: dt.datetime


@dataclass(frozen=True)
class _WorstLane:
    source: str | None
    partition: int | None
    lag_seconds: float
    active_lanes: int


_LOAD_STATE_SQL = """
SELECT tenant_id, consecutive_breach_ticks, tripped, tripped_at, last_tick_at
  FROM circuit_breaker_state
 WHERE instance_name = $1
"""

_UPSERT_STATE_SQL = """
INSERT INTO circuit_breaker_state (
    instance_name, tenant_id, consecutive_breach_ticks,
    tripped, tripped_at, last_tick_at
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (instance_name, tenant_id) DO UPDATE SET
    consecutive_breach_ticks = EXCLUDED.consecutive_breach_ticks,
    tripped                  = EXCLUDED.tripped,
    tripped_at               = EXCLUDED.tripped_at,
    last_tick_at             = EXCLUDED.last_tick_at
"""


async def _load_state(
    pool: asyncpg.Pool, instance: str,
) -> dict[UUID, _TenantBreachState]:
    rows = await pool.fetch(_LOAD_STATE_SQL, instance)
    return {
        row["tenant_id"]: _TenantBreachState(
            tenant_id=row["tenant_id"],
            consecutive_breach_ticks=row["consecutive_breach_ticks"],
            tripped=row["tripped"],
            tripped_at=row["tripped_at"],
            last_tick_at=row["last_tick_at"],
        )
        for row in rows
    }


async def _persist_state(
    pool: asyncpg.Pool, instance: str, state: _TenantBreachState,
) -> None:
    await pool.execute(
        _UPSERT_STATE_SQL,
        instance, state.tenant_id, state.consecutive_breach_ticks,
        state.tripped, state.tripped_at, state.last_tick_at,
    )


# ---------------------------------------------------------------------
# Pool helper — pgbouncer-compatible. Fourth activation after M3.1,
# M3.3, M4.2. Mirrors session_state.py::make_session_state_pool exactly.
# ---------------------------------------------------------------------
async def make_breaker_pool(
    dsn: str,
    *,
    max_size: int = 5,
    command_timeout: float = 30.0,
) -> asyncpg.Pool:
    """Construct an asyncpg pool for the circuit breaker's state +
    flag UPSERTs. `statement_cache_size=0` per the M1.3 ADR Q1
    pgbouncer-transaction-mode contract (same as
    `services.ingest.integrations.discord.gateway.session_state.make_session_state_pool`).
    """
    return await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=max_size,
        command_timeout=command_timeout,
        init=configure_connection_timeouts,
        statement_cache_size=0,  # pgbouncer transaction mode (M1.3 ADR Q1)
    )


# ---------------------------------------------------------------------
# Default Kafka measurement functions — production wiring.
# Tests inject mocks instead of calling these.
# ---------------------------------------------------------------------
# lag map is keyed source -> {partition: lag_seconds}; active map is keyed
# tenant -> {source: raw_partition} (a tenant can be active on >1 source lane).
LagPerSourceFn = Callable[..., Awaitable[dict[str, dict[int, float]]]]
ActiveTenantsFn = Callable[..., Awaitable[dict[UUID, dict[str, int]]]]
AlertFn = Callable[[UUID, dict[str, Any]], Awaitable[None]]


async def _measure_kafka_lag_default(
    *,
    bootstrap: str,
    normalizer_group_base: str,
) -> dict[str, dict[int, float]]:
    """M-Load: real per-source Kafka lag reader via confluent_kafka.

    Returns ``{source: {partition: lag_seconds}}`` — one inner map per
    ``ingestion.raw.<source>`` lane, measured against the normalizer's
    per-source consumer group ``<normalizer_group_base>.<source>``. A source
    with no committed offsets yet (no traffic) maps to ``{}``.

    The topic+group pairs are derived from
    `services.ingest.ingestion.kafka.topics`, so the breaker can never drift
    from the data plane's actual lane set: add a source to the envelope
    literal and a new lane is measured automatically. A single AdminClient +
    probe Consumer are reused across all lanes. Tests rebind this with a mock.

    The body runs entirely with **synchronous** confluent_kafka C-extension
    calls (`.result(timeout=…)`, `get_watermark_offsets`, `poll`), so it is
    offloaded to a worker thread via `asyncio.to_thread`. Running it directly
    on the event loop would block the loop — and thus the heartbeat ticker —
    for up to ``len(INGESTION_SOURCES) × timeout`` seconds when the broker is
    slow, which is exactly the degraded-broker incident the breaker exists to
    handle (the blocking probe would otherwise *be* the wedge `/healthz`
    detects). The probe Consumer/AdminClient are created, used, and closed
    entirely inside the thread, so no confluent handle crosses the boundary.
    """
    return await asyncio.to_thread(
        _measure_kafka_lag_sync,
        bootstrap=bootstrap,
        normalizer_group_base=normalizer_group_base,
    )


def _measure_kafka_lag_sync(
    *,
    bootstrap: str,
    normalizer_group_base: str,
) -> dict[str, dict[int, float]]:
    """Synchronous body of :func:`_measure_kafka_lag_default` — runs in a
    worker thread (see that function's docstring). All confluent_kafka calls
    here are blocking C-extension calls."""
    # Lazy import — confluent_kafka is a heavy dep; not all callers need it.
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import Consumer

    from services.ingest.ingestion.kafka.topics import (
        INGESTION_SOURCES,
        consumer_group as _consumer_group,
        topic_for,
    )

    admin = AdminClient({"bootstrap.servers": bootstrap})
    probe = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"{normalizer_group_base}-lagprobe",
        "enable.auto.commit": False,
    })
    try:
        out: dict[str, dict[int, float]] = {}
        for source in INGESTION_SOURCES:
            topic = topic_for("raw", source)
            group = _consumer_group(normalizer_group_base, source)
            try:
                out[source] = _measure_topic_lag_seconds(admin, probe, topic, group)
            except Exception as exc:  # noqa: BLE001
                # Isolate per-lane failures: one source's probe erroring must
                # not blind the breaker to the other nine lanes (with ten
                # lanes per tick that blast radius is real). Consistent with
                # the per-partition "can't read → 0, don't false-trip" rule,
                # an unmeasurable lane is treated as no-evidence-of-lag this
                # tick rather than aborting the whole measurement.
                _bump("breaker.lag_measurement_failures")
                log.warning(
                    "circuit_breaker.lane_lag_measurement_failed",
                    extra={
                        "source": source,
                        "topic": topic,
                        "group": group,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:200],
                    },
                )
                out[source] = {}
        return out
    finally:
        probe.close()


def _measure_topic_lag_seconds(
    admin: Any, consumer: Any, topic: str, group: str,
) -> dict[int, float]:
    """Lag-in-seconds for one (topic, consumer-group), returned as
    ``{partition: lag_seconds}`` (empty if the group has no committed offsets
    on the topic). Synchronous confluent calls; correlates committed offset
    to broker-side message timestamp:
      1. AdminClient.list_consumer_group_offsets → committed per partition.
      2. get_watermark_offsets → (low, high); lag_messages = high - committed.
      3. Consume one message AT the committed offset, read its CreateTime;
         lag_seconds = now - createtime. (0s if committed == high.)
    """
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    import time as _time

    # 1. Committed offsets for this group on this topic.
    cgtp = ConsumerGroupTopicPartitions(group, topic_partitions=None)
    fut = admin.list_consumer_group_offsets([cgtp])
    result = fut[group].result(timeout=10.0)
    committed_by_partition: dict[int, int] = {}
    for tp in result.topic_partitions:
        if tp.topic == topic and tp.offset >= 0:
            committed_by_partition[tp.partition] = tp.offset
    if not committed_by_partition:
        return {}

    # 2 + 3. Watermark (high) offsets + message-timestamp probe per partition.
    out: dict[int, float] = {}
    for partition, committed in committed_by_partition.items():
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=5.0,
        )
        if committed >= high:
            out[partition] = 0.0
            continue
        # Read one message at `committed` to get its timestamp.
        consumer.assign([TopicPartition(topic, partition, committed)])
        msg = consumer.poll(timeout=5.0)
        if msg is None or msg.error():
            # Couldn't read; conservative — report 0 to avoid spurious
            # alerts. Operator runbook (m-load-runbook.md) explains.
            out[partition] = 0.0
            continue
        ts_kind, ts_ms = msg.timestamp()
        if ts_ms <= 0:
            out[partition] = 0.0
            continue
        now_ms = int(_time.time() * 1000)
        out[partition] = max(0.0, (now_ms - ts_ms) / 1000.0)
    return out


async def _sample_active_tenants_default(
    *,
    bootstrap: str,
    signal_topic: str,
    lookback_sec: int,
) -> dict[UUID, dict[str, int]]:
    """M-Load: real Kafka consumer reading the traffic-signal topic.

    Reads back `lookback_sec` of `ingestion.tenant_traffic_signal`,
    returns `{tenant_id: {source: raw_partition}}` for tenants that emitted
    signals in the window — capturing every source lane a tenant was active
    on, so the breaker can take the worst-case lag across them.
    `traffic_signal.py` produces these signals with `key=tenant_id_bytes`
    and a JSON value carrying `source` + `raw_partition`.

    Offloaded to a worker thread (see :func:`_measure_kafka_lag_default`) —
    the body is blocking confluent_kafka C-extension calls and must not run
    on the breaker's event loop.
    """
    return await asyncio.to_thread(
        _sample_active_tenants_sync,
        bootstrap=bootstrap,
        signal_topic=signal_topic,
        lookback_sec=lookback_sec,
    )


def _sample_active_tenants_sync(
    *,
    bootstrap: str,
    signal_topic: str,
    lookback_sec: int,
) -> dict[UUID, dict[str, int]]:
    """Synchronous body of :func:`_sample_active_tenants_default` — runs in a
    worker thread. Blocking confluent_kafka calls only."""
    from confluent_kafka import Consumer, TopicPartition
    import json
    import time as _time

    cutoff_ms = int((_time.time() - lookback_sec) * 1000)
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"breaker-tenant-sampler-{int(_time.time())}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    try:
        # Get partition list for the topic.
        cluster_md = consumer.list_topics(signal_topic, timeout=5.0)
        topic_md = cluster_md.topics.get(signal_topic)
        if topic_md is None or topic_md.error is not None:
            return {}
        partitions = list(topic_md.partitions.keys())

        # Seek each partition to the offset closest to cutoff_ms.
        offsets_for_times = consumer.offsets_for_times(
            [TopicPartition(signal_topic, p, cutoff_ms) for p in partitions],
            timeout=5.0,
        )
        assignments = [
            TopicPartition(tp.topic, tp.partition, tp.offset)
            for tp in offsets_for_times if tp.offset >= 0
        ]
        if not assignments:
            return {}
        consumer.assign(assignments)

        out: dict[UUID, dict[str, int]] = {}
        deadline = _time.monotonic() + 5.0  # 5s read budget
        while _time.monotonic() < deadline:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                # `None` means "no message delivered in this 0.5s poll", NOT
                # "end of partition" — confluent returns it on any empty poll
                # window. Breaking here truncated the read at the first gap,
                # silently dropping tenants that emitted earlier in the
                # lookback window (freezing their breach counter and delaying
                # a trip). Keep polling until the 5s deadline drains the
                # assigned offsets.
                continue
            if msg.error():
                continue
            ts_kind, ts_ms = msg.timestamp()
            if ts_ms > 0 and ts_ms < cutoff_ms:
                continue
            try:
                payload = json.loads(msg.value())
                tenant_id_raw = payload.get("tenant_id")
                source = payload.get("source")
                raw_partition = int(payload.get("raw_partition", 0))
                tid = UUID(tenant_id_raw)
                if not isinstance(source, str) or not source:
                    # Pre-source-split signal (or malformed) — can't map it to
                    # a lane, so skip rather than guess.
                    continue
                # Last writer wins for a (tenant, source); the breaker only
                # needs which partition the lane is on, not every sample.
                out.setdefault(tid, {})[source] = raw_partition
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
        return out
    finally:
        consumer.close()


async def _default_alert(tenant_id: UUID, payload: dict[str, Any]) -> None:
    """Default alert: a structlog warning, plus a best-effort POST to an
    ops-alerts webhook when `INGESTION_ALERT_WEBHOOK_URL` is configured
    (a Slack/PagerDuty/generic incoming webhook). The POST is
    fire-and-forget and never raises — alerting must not perturb the
    breaker tick.
    """
    log.warning(
        "circuit_breaker.tripped",
        extra={"tenant_id": str(tenant_id), **payload},
    )
    webhook = os.environ.get("INGESTION_ALERT_WEBHOOK_URL", "").strip()
    if not webhook:
        return
    try:
        import httpx

        body = {
            "event": "circuit_breaker.tripped",
            "tenant_id": str(tenant_id),
            **payload,
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(webhook, json=body)
    except Exception as exc:  # noqa: BLE001 — alerting is best-effort
        log.warning(
            "circuit_breaker.alert_webhook_failed",
            extra={"error": str(exc)[:200]},
        )


# ---------------------------------------------------------------------
# Tick logic — extracted for unit testability.
# ---------------------------------------------------------------------
async def _read_tick_inputs(
    *,
    config: BreakerConfig,
    measure_lag_fn: LagPerSourceFn,
    active_tenants_fn: ActiveTenantsFn,
) -> tuple[dict[str, dict[int, float]], dict[UUID, dict[str, int]]] | None:
    try:
        lag_per_source = await measure_lag_fn(
            bootstrap=config.kafka_bootstrap,
            normalizer_group_base=config.normalizer_group_base,
        )
    except Exception as exc:  # noqa: BLE001
        _bump("breaker.lag_measurement_failures")
        log.warning(
            "circuit_breaker.lag_measurement_failed",
            extra={"error_type": type(exc).__name__, "error": str(exc)[:200]},
        )
        return None

    try:
        active = await active_tenants_fn(
            bootstrap=config.kafka_bootstrap,
            signal_topic=config.signal_topic,
            lookback_sec=config.signal_lookback_sec,
        )
    except Exception as exc:  # noqa: BLE001
        _bump("breaker.signal_read_failures")
        log.warning(
            "circuit_breaker.signal_read_failed",
            extra={"error_type": type(exc).__name__, "error": str(exc)[:200]},
        )
        return None

    _bump("breaker.active_tenants_sampled", float(len(active)))
    return lag_per_source, active


async def _handle_flag_disabled_tenant(
    *,
    pool: asyncpg.Pool,
    config: BreakerConfig,
    entry: _TenantBreachState | None,
    now: dt.datetime,
) -> None:
    if entry is not None and entry.tripped:
        _bump("breaker.skipped_already_tripped")
        entry.last_tick_at = now
        await _persist_state(pool, config.instance_name, entry)
    else:
        _bump("breaker.skipped_flag_disabled")


def _reset_bookkeeping_on_operator_reenable(
    *,
    config: BreakerConfig,
    tenant_id: UUID,
    entry: _TenantBreachState,
) -> None:
    _bump("breaker.bookkeeping_reset_on_operator_reenable")
    entry.consecutive_breach_ticks = 0
    entry.tripped = False
    entry.tripped_at = None
    log.info(
        "circuit_breaker.bookkeeping_reset_on_operator_reenable",
        extra={
            "tenant_id": str(tenant_id),
            "instance_name": config.instance_name,
        },
    )


def _entry_for_tenant(
    state: dict[UUID, _TenantBreachState],
    tenant_id: UUID,
    *,
    now: dt.datetime,
) -> _TenantBreachState:
    entry = state.get(tenant_id)
    if entry is not None:
        return entry
    entry = _TenantBreachState(
        tenant_id=tenant_id,
        consecutive_breach_ticks=0,
        tripped=False,
        tripped_at=None,
        last_tick_at=now,
    )
    state[tenant_id] = entry
    return entry


def _worst_lane_for_tenant(
    *,
    lag_per_source: dict[str, dict[int, float]],
    source_partitions: dict[str, int],
) -> _WorstLane:
    worst = _WorstLane(
        source=None,
        partition=None,
        lag_seconds=0.0,
        active_lanes=len(source_partitions),
    )
    for src, part in source_partitions.items():
        lane_lag = lag_per_source.get(src, {}).get(part, 0.0)
        if worst.source is None or lane_lag > worst.lag_seconds:
            worst = _WorstLane(
                source=src,
                partition=part,
                lag_seconds=lane_lag,
                active_lanes=len(source_partitions),
            )
    return worst


def _update_breach_counter(
    *,
    entry: _TenantBreachState,
    worst_lane: _WorstLane,
    config: BreakerConfig,
) -> None:
    if worst_lane.lag_seconds > config.breach_threshold_sec:
        entry.consecutive_breach_ticks += 1
        _bump("breaker.breach_increments")
        return
    if entry.consecutive_breach_ticks > 0:
        _bump("breaker.recovery_resets")
    entry.consecutive_breach_ticks = 0


async def _trip_tenant(
    *,
    config: BreakerConfig,
    pool: asyncpg.Pool,
    tenant_flags: TenantFlags,
    alert_fn: AlertFn,
    tenant_id: UUID,
    entry: _TenantBreachState,
    worst_lane: _WorstLane,
    now: dt.datetime,
) -> None:
    try:
        await tenant_flags.set_bool(
            tenant_id,
            KAFKA_PATH_ENABLED,
            False,
            set_by="auto:circuit_breaker",
            note=(
                f"lag>{config.breach_threshold_sec}s for "
                f"{config.breach_window_ticks} consecutive ticks on "
                f"worst lane source={worst_lane.source} "
                f"partition={worst_lane.partition} "
                f"({worst_lane.active_lanes} active lane(s))"
            ),
        )
    except Exception:  # noqa: BLE001
        _bump("breaker.flag_flip_failures")
        log.exception(
            "circuit_breaker.flag_flip_failed",
            extra={"tenant_id": str(tenant_id)},
        )
        await _persist_state(pool, config.instance_name, entry)
        return

    entry.tripped = True
    entry.tripped_at = now
    await _persist_state(pool, config.instance_name, entry)
    _bump("breaker.trips")
    try:
        await alert_fn(tenant_id, {
            "source": worst_lane.source,
            "partition": worst_lane.partition,
            "lag_seconds": worst_lane.lag_seconds,
            "threshold_seconds": config.breach_threshold_sec,
            "window_ticks": config.breach_window_ticks,
            "active_lanes": worst_lane.active_lanes,
            "tripped_at": now.isoformat(),
        })
    except Exception:  # noqa: BLE001
        log.exception(
            "circuit_breaker.alert_failed",
            extra={"tenant_id": str(tenant_id)},
        )


async def _process_active_tenant(
    *,
    config: BreakerConfig,
    pool: asyncpg.Pool,
    tenant_flags: TenantFlags,
    state: dict[UUID, _TenantBreachState],
    lag_per_source: dict[str, dict[int, float]],
    tenant_id: UUID,
    source_partitions: dict[str, int],
    alert_fn: AlertFn,
    now: dt.datetime,
) -> None:
    flag_value = await tenant_flags.kafka_path_enabled(tenant_id)
    entry = state.get(tenant_id)

    if flag_value is False:
        await _handle_flag_disabled_tenant(
            pool=pool,
            config=config,
            entry=entry,
            now=now,
        )
        return

    if entry is not None and entry.tripped:
        _reset_bookkeeping_on_operator_reenable(
            config=config,
            tenant_id=tenant_id,
            entry=entry,
        )

    entry = _entry_for_tenant(state, tenant_id, now=now)
    worst_lane = _worst_lane_for_tenant(
        lag_per_source=lag_per_source,
        source_partitions=source_partitions,
    )
    _update_breach_counter(
        entry=entry,
        worst_lane=worst_lane,
        config=config,
    )
    entry.last_tick_at = now

    if entry.consecutive_breach_ticks >= config.breach_window_ticks:
        await _trip_tenant(
            config=config,
            pool=pool,
            tenant_flags=tenant_flags,
            alert_fn=alert_fn,
            tenant_id=tenant_id,
            entry=entry,
            worst_lane=worst_lane,
            now=now,
        )
    else:
        await _persist_state(pool, config.instance_name, entry)


async def _process_tick(
    *,
    config: BreakerConfig,
    pool: asyncpg.Pool,
    tenant_flags: TenantFlags,
    state: dict[UUID, _TenantBreachState],
    measure_lag_fn: LagPerSourceFn,
    active_tenants_fn: ActiveTenantsFn,
    alert_fn: AlertFn,
    now: dt.datetime | None = None,
) -> None:
    """One tick: measure per-source lag → sample active tenants → update
    state → flip flags + alert on sustained breach. Mutates `state` in place
    AND persists every modified row to Postgres before returning.

    Extracted from `run_circuit_breaker` so unit tests can drive
    one tick at a time with deterministic injected inputs.
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    _bump("breaker.ticks")

    tick_inputs = await _read_tick_inputs(
        config=config,
        measure_lag_fn=measure_lag_fn,
        active_tenants_fn=active_tenants_fn,
    )
    if tick_inputs is None:
        return
    lag_per_source, active = tick_inputs

    for tenant_id, source_partitions in active.items():
        await _process_active_tenant(
            config=config,
            pool=pool,
            tenant_flags=tenant_flags,
            state=state,
            lag_per_source=lag_per_source,
            tenant_id=tenant_id,
            source_partitions=source_partitions,
            alert_fn=alert_fn,
            now=now,
        )


# ---------------------------------------------------------------------
# Public entry — long-running loop.
# ---------------------------------------------------------------------
async def run_circuit_breaker(
    config: BreakerConfig,
    pool: asyncpg.Pool,
    *,
    tenant_flags: TenantFlags,
    measure_lag_fn: LagPerSourceFn = _measure_kafka_lag_default,
    active_tenants_fn: ActiveTenantsFn = _sample_active_tenants_default,
    alert_fn: AlertFn = _default_alert,
    stop_event: asyncio.Event | None = None,
    max_ticks: int | None = None,
    heartbeat: Heartbeat | None = None,
) -> dict[str, int]:
    """Main loop. Returns when `stop_event` is set OR `max_ticks` reached.

    One iteration per `tick_interval_sec`. State is loaded once at
    start; each tick reads + updates the in-memory dict and persists
    changed rows. A SIGTERM mid-tick will let the current tick
    finish (per-tenant persist is atomic) before exiting.

    `heartbeat`, if supplied, is touched at the start of each tick so the
    /healthz surface can tell a wedged loop (Kafka probe hung → no touch →
    stale → 503) from a merely idle one (the ticker keeps touching during
    the inter-tick sleep).
    """
    stop_event = stop_event or asyncio.Event()
    state = await _load_state(pool, config.instance_name)
    ticks = 0

    while not stop_event.is_set():
        if max_ticks is not None and ticks >= max_ticks:
            break
        ticks += 1
        if heartbeat is not None:
            heartbeat.touch()

        await _process_tick(
            config=config,
            pool=pool,
            tenant_flags=tenant_flags,
            state=state,
            measure_lag_fn=measure_lag_fn,
            active_tenants_fn=active_tenants_fn,
            alert_fn=alert_fn,
        )

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=config.tick_interval_sec,
            )
        except asyncio.TimeoutError:
            pass

    return {
        "ticks": ticks,
        "trips": int(_metrics["breaker.trips"]),
    }


# ---------------------------------------------------------------------
# CLI entry — signal handling + pool bootstrap.
# ---------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=os.environ.get("CIRCUIT_BREAKER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = BreakerConfig(
        instance_name=os.environ.get("BREAKER_INSTANCE_NAME", "default"),
        tick_interval_sec=float(
            os.environ.get("BREAKER_TICK_INTERVAL_SEC", "60")
        ),
        breach_threshold_sec=int(
            os.environ.get("BREAKER_THRESHOLD_SEC", "60")
        ),
        breach_window_ticks=int(
            os.environ.get("BREAKER_WINDOW_TICKS", "5")
        ),
        normalizer_group_base=os.environ.get(
            "BREAKER_NORMALIZER_GROUP_BASE", "normalizer"
        ),
        kafka_bootstrap=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
    )

    # Test injection mechanism: env-var-driven synthetic Kafka
    # responses. Same pattern as M4.3's _subprocess_entrypoint —
    # real production code path through Postgres state + flag flips,
    # synthetic injection only at the Kafka boundary.
    fake_lag_env = os.environ.get("M5_BREAKER_FAKE_LAG_PARTITIONS")
    fake_active_env = os.environ.get("M5_BREAKER_FAKE_ACTIVE_TENANTS")

    if fake_lag_env is not None:
        # Format: '{"slack": {"0": 120.5, "1": 30.0}}'  (source -> partition -> lag)
        fake_lag = {
            src: {int(p): float(lag) for p, lag in parts.items()}
            for src, parts in json.loads(fake_lag_env).items()
        }

        async def _fake_lag(**_kwargs: Any) -> dict[str, dict[int, float]]:
            return {src: dict(parts) for src, parts in fake_lag.items()}
        measure_lag_fn: LagPerSourceFn = _fake_lag
    else:
        measure_lag_fn = _measure_kafka_lag_default

    if fake_active_env is not None:
        # Format: '{"<uuid>": {"slack": 0, "github": 1}}'  (tenant -> source -> partition)
        fake_active = {
            UUID(k): {src: int(p) for src, p in lanes.items()}
            for k, lanes in json.loads(fake_active_env).items()
        }

        async def _fake_active(**_kwargs: Any) -> dict[UUID, dict[str, int]]:
            return {tid: dict(lanes) for tid, lanes in fake_active.items()}
        active_tenants_fn: ActiveTenantsFn = _fake_active
    else:
        active_tenants_fn = _sample_active_tenants_default

    async def _run() -> None:
        pool = await make_breaker_pool(os.environ["DATABASE_URL"])
        tenant_flags = TenantFlags(pool)

        stop_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

        # Health surface: /healthz (200 while ticking, 503 if wedged) +
        # /metrics (the breaker.* counters). Enabled when INGESTION_HEALTH_PORT
        # is set (compose sets 9300); a no-op otherwise, so tests/local runs
        # are unaffected. The ticker touches the heartbeat during the
        # inter-tick sleep so an idle breaker stays healthy.
        heartbeat = Heartbeat()
        health_server = start_health_server(
            get_metrics=get_metrics, heartbeat=heartbeat,
        )
        try:
            await asyncio.gather(
                run_circuit_breaker(
                    config=config,
                    pool=pool,
                    tenant_flags=tenant_flags,
                    measure_lag_fn=measure_lag_fn,
                    active_tenants_fn=active_tenants_fn,
                    stop_event=stop_event,
                    heartbeat=heartbeat,
                ),
                run_heartbeat_ticker(heartbeat, stop_event),
            )
        finally:
            if health_server is not None:
                health_server.shutdown()
            await pool.close()

    asyncio.run(_run())


__all__ = [
    "BreakerConfig",
    "_TenantBreachState",
    "get_metrics",
    "main",
    "make_breaker_pool",
    "reset_metrics",
    "run_circuit_breaker",
]
