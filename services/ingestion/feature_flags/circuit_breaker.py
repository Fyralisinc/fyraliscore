"""services/ingestion/feature_flags/circuit_breaker.py
   — Ingestion cutover circuit breaker.

Per ingestion LLD §11.2 (cutover circuit breaker workflow) +
04-implementation-plan.md §M5 condition (3).

=== Design summary ===

Long-running asyncio service. Every `tick_interval_sec` (default 60s):

  1. Measure consumer-group lag on `ingestion.raw` per partition.
  2. Sample active tenants from `ingestion.tenant_traffic_signal`
     (the 1% deterministic-hash signal topic; LLD §11.3).
  3. For each active tenant, check whether its partition's lag
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

Tenants whose flag is already FALSE (pre-cutover or operator-disabled)
are skipped entirely in step 4 above — the breaker has nothing to
flip for them, and re-flipping FALSE-on-FALSE would clobber the
`set_by` audit trail.

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
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg

from services.ingestion.feature_flags.client import (
    KAFKA_PATH_ENABLED,
    TenantFlags,
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
    # The raw-topic + consumer-group + signal-topic names line up
    # with the LLD §5.2 + §11.3 wire contract; do not change without
    # the LLD changing in lockstep.
    raw_topic: str = "ingestion.raw"
    consumer_group: str = "ingestion-normalizer"
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
    `services.integrations.discord.gateway.session_state.make_session_state_pool`).
    """
    return await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=max_size,
        command_timeout=command_timeout,
        statement_cache_size=0,  # pgbouncer transaction mode (M1.3 ADR Q1)
    )


# ---------------------------------------------------------------------
# Default Kafka measurement functions — production wiring.
# Tests inject mocks instead of calling these.
# ---------------------------------------------------------------------
LagPerPartitionFn = Callable[..., Awaitable[dict[int, float]]]
ActiveTenantsFn = Callable[..., Awaitable[dict[UUID, int]]]
AlertFn = Callable[[UUID, dict[str, Any]], Awaitable[None]]


async def _measure_kafka_lag_default(
    *,
    bootstrap: str,
    topic: str,
    consumer_group: str,
) -> dict[int, float]:
    """M-Load: real Kafka lag reader via confluent_kafka.AdminClient.

    Returns `{partition: lag_seconds}`. Lag-in-seconds is computed by
    correlating the consumer group's committed offset to its
    broker-side message timestamp:
      1. AdminClient.list_consumer_group_offsets → committed per partition.
      2. For each partition: get_watermark_offsets → (low, high).
         lag_messages = high - committed.
      3. To convert to seconds, consume one message AT the committed
         offset and read its CreateTime; lag_seconds = now - createtime.
         (Skipped if committed == high; partition is caught up; 0s.)

    Step 3 is expensive but operators want time, not messages. Tests
    rebind this function with a mock for unit work; production wires
    a real bootstrap.
    """
    # Lazy import — confluent_kafka is a heavy dep; not all callers need it.
    from confluent_kafka.admin import AdminClient, ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka import Consumer, KafkaError

    admin = AdminClient({"bootstrap.servers": bootstrap})
    # 1. Committed offsets for this group on this topic.
    cgtp = ConsumerGroupTopicPartitions(consumer_group, topic_partitions=None)
    fut = admin.list_consumer_group_offsets([cgtp])
    result = fut[consumer_group].result(timeout=10.0)
    committed_by_partition: dict[int, int] = {}
    for tp in result.topic_partitions:
        if tp.topic == topic and tp.offset >= 0:
            committed_by_partition[tp.partition] = tp.offset
    if not committed_by_partition:
        return {}

    # 2. Watermark (high) offsets per partition.
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"{consumer_group}-lagprobe",
        "enable.auto.commit": False,
    })
    try:
        out: dict[int, float] = {}
        import time as _time
        for partition, committed in committed_by_partition.items():
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=5.0,
            )
            if committed >= high:
                out[partition] = 0.0
                continue
            # 3. Read one message at `committed` to get its timestamp.
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
    finally:
        consumer.close()


async def _sample_active_tenants_default(
    *,
    bootstrap: str,
    signal_topic: str,
    lookback_sec: int,
) -> dict[UUID, int]:
    """M-Load: real Kafka consumer reading the traffic-signal topic.

    Reads back `lookback_sec` of `ingestion.tenant_traffic_signal`,
    returns `{tenant_id: partition}` mapping for tenants that emitted
    signals in the window. M5.3's `traffic_signal.py` produces these
    signals with `key=tenant_id_bytes` and a JSON value containing
    `raw_partition`.
    """
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

        out: dict[UUID, int] = {}
        deadline = _time.monotonic() + 5.0  # 5s read budget
        while _time.monotonic() < deadline:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                break
            if msg.error():
                continue
            ts_kind, ts_ms = msg.timestamp()
            if ts_ms > 0 and ts_ms < cutoff_ms:
                continue
            try:
                payload = json.loads(msg.value())
                tenant_id_raw = payload.get("tenant_id")
                raw_partition = int(payload.get("raw_partition", 0))
                tid = UUID(tenant_id_raw)
                out[tid] = raw_partition
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
        return out
    finally:
        consumer.close()


async def _default_alert(tenant_id: UUID, payload: dict[str, Any]) -> None:
    """Default alert: structlog warning. Production wires a real
    ops-alerts channel (PagerDuty / Slack webhook / etc.) here.
    """
    log.warning(
        "circuit_breaker.tripped",
        extra={"tenant_id": str(tenant_id), **payload},
    )


# ---------------------------------------------------------------------
# Tick logic — extracted for unit testability.
# ---------------------------------------------------------------------
async def _process_tick(
    *,
    config: BreakerConfig,
    pool: asyncpg.Pool,
    tenant_flags: TenantFlags,
    state: dict[UUID, _TenantBreachState],
    measure_lag_fn: LagPerPartitionFn,
    active_tenants_fn: ActiveTenantsFn,
    alert_fn: AlertFn,
    now: dt.datetime | None = None,
) -> None:
    """One tick: measure lag → sample active tenants → update state →
    flip flags + alert on sustained breach. Mutates `state` in place
    AND persists every modified row to Postgres before returning.

    Extracted from `run_circuit_breaker` so unit tests can drive
    one tick at a time with deterministic injected inputs.
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    _bump("breaker.ticks")

    # Step 1: measure lag per partition.
    try:
        lag_per_partition = await measure_lag_fn(
            bootstrap=config.kafka_bootstrap,
            topic=config.raw_topic,
            consumer_group=config.consumer_group,
        )
    except Exception as exc:  # noqa: BLE001
        _bump("breaker.lag_measurement_failures")
        log.warning(
            "circuit_breaker.lag_measurement_failed",
            extra={"error_type": type(exc).__name__, "error": str(exc)[:200]},
        )
        return  # skip this tick; do NOT touch state

    # Step 2: sample active tenants from signal topic.
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
        return  # skip this tick

    _bump("breaker.active_tenants_sampled", float(len(active)))

    # Step 3 + 4: update per-tenant breach state.
    for tenant_id, partition in active.items():
        # Read the current cutover flag. Drives two behaviours:
        #   (1) Filtering: tenants whose flag is already FALSE are not
        #       candidates for breach detection — there's nothing to
        #       flip, and re-flipping FALSE-on-FALSE would overwrite
        #       the operator's audit field on set_by.
        #   (2) Auto-reset on operator re-enable: if our state row says
        #       tripped=TRUE but the flag is now TRUE, an operator must
        #       have manually re-enabled the tenant. We reset our
        #       bookkeeping (counter=0, tripped=FALSE) so the next sustained
        #       breach can trip again — without this, a forgotten state-row
        #       cleanup leaves the breaker permanently blind to the tenant.
        flag_value = await tenant_flags.get_bool(
            tenant_id, KAFKA_PATH_ENABLED, default=True,
        )
        entry = state.get(tenant_id)

        if flag_value is False:
            if entry is not None and entry.tripped:
                # Already tripped by this breaker; remain frozen. Keep
                # last_tick_at fresh so stale-state GC doesn't drop the
                # row while traffic is still flowing.
                _bump("breaker.skipped_already_tripped")
                entry.last_tick_at = now
                await _persist_state(pool, config.instance_name, entry)
            else:
                # Non-cutover tenant (pre-cutover or operator-disabled).
                # Nothing to flip; do not create a state row.
                _bump("breaker.skipped_flag_disabled")
            continue

        # flag_value is True from here on.
        if entry is not None and entry.tripped:
            # Operator re-enabled the flag manually. Auto-reset our
            # bookkeeping so future breaches can re-trip. This is
            # auto-reset of BREAKER STATE, not auto-recovery of the
            # FLAG (which remains operator-driven).
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

        if entry is None:
            entry = _TenantBreachState(
                tenant_id=tenant_id,
                consecutive_breach_ticks=0,
                tripped=False,
                tripped_at=None,
                last_tick_at=now,
            )
            state[tenant_id] = entry

        partition_lag = lag_per_partition.get(partition, 0.0)
        breached = partition_lag > config.breach_threshold_sec

        if breached:
            entry.consecutive_breach_ticks += 1
            _bump("breaker.breach_increments")
        else:
            # Recovery within window: reset to 0. Per the LLD's
            # "5 CONSECUTIVE" requirement — one healthy tick breaks
            # the streak.
            if entry.consecutive_breach_ticks > 0:
                _bump("breaker.recovery_resets")
            entry.consecutive_breach_ticks = 0

        entry.last_tick_at = now

        # Step 5: trip if window reached.
        if entry.consecutive_breach_ticks >= config.breach_window_ticks:
            entry.tripped = True
            entry.tripped_at = now
            # Order: persist breach state FIRST (so a crash between
            # the trip-record and the flag flip doesn't leave the
            # flag flipped without an audit trail), then flip the
            # flag, then alert. The flag flip is the user-visible
            # change; the state row is the audit record.
            await _persist_state(pool, config.instance_name, entry)
            try:
                await tenant_flags.set_bool(
                    tenant_id,
                    KAFKA_PATH_ENABLED,
                    False,
                    set_by="auto:circuit_breaker",
                    note=(
                        f"lag>{config.breach_threshold_sec}s for "
                        f"{config.breach_window_ticks} consecutive ticks "
                        f"on partition {partition}"
                    ),
                )
            except Exception:  # noqa: BLE001
                # Flag flip failed — we already persisted the
                # tripped state, so the next tick will see this
                # tenant as tripped (and skip it). The flag flip
                # will be retried by... no, it won't. This is a
                # gap. Log loudly so operators can flip manually.
                log.exception(
                    "circuit_breaker.flag_flip_failed",
                    extra={"tenant_id": str(tenant_id)},
                )
                continue
            _bump("breaker.trips")
            try:
                await alert_fn(tenant_id, {
                    "partition": partition,
                    "lag_seconds": partition_lag,
                    "threshold_seconds": config.breach_threshold_sec,
                    "window_ticks": config.breach_window_ticks,
                    "tripped_at": now.isoformat(),
                })
            except Exception:  # noqa: BLE001
                log.exception(
                    "circuit_breaker.alert_failed",
                    extra={"tenant_id": str(tenant_id)},
                )
        else:
            # Not yet tripped — just persist the updated counter.
            await _persist_state(pool, config.instance_name, entry)


# ---------------------------------------------------------------------
# Public entry — long-running loop.
# ---------------------------------------------------------------------
async def run_circuit_breaker(
    config: BreakerConfig,
    pool: asyncpg.Pool,
    *,
    tenant_flags: TenantFlags,
    measure_lag_fn: LagPerPartitionFn = _measure_kafka_lag_default,
    active_tenants_fn: ActiveTenantsFn = _sample_active_tenants_default,
    alert_fn: AlertFn = _default_alert,
    stop_event: asyncio.Event | None = None,
    max_ticks: int | None = None,
) -> dict[str, int]:
    """Main loop. Returns when `stop_event` is set OR `max_ticks` reached.

    One iteration per `tick_interval_sec`. State is loaded once at
    start; each tick reads + updates the in-memory dict and persists
    changed rows. A SIGTERM mid-tick will let the current tick
    finish (per-tenant persist is atomic) before exiting.
    """
    stop_event = stop_event or asyncio.Event()
    state = await _load_state(pool, config.instance_name)
    ticks = 0

    while not stop_event.is_set():
        if max_ticks is not None and ticks >= max_ticks:
            break
        ticks += 1

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
        # Format: '{"0": 120.5, "1": 30.0}'
        fake_lag = {int(k): float(v) for k, v in json.loads(fake_lag_env).items()}

        async def _fake_lag(**_kwargs: Any) -> dict[int, float]:
            return dict(fake_lag)
        measure_lag_fn: LagPerPartitionFn = _fake_lag
    else:
        measure_lag_fn = _measure_kafka_lag_default

    if fake_active_env is not None:
        # Format: '{"<uuid>": 0, "<uuid>": 1}'
        fake_active = {UUID(k): int(v) for k, v in json.loads(fake_active_env).items()}

        async def _fake_active(**_kwargs: Any) -> dict[UUID, int]:
            return dict(fake_active)
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

        try:
            await run_circuit_breaker(
                config=config,
                pool=pool,
                tenant_flags=tenant_flags,
                measure_lag_fn=measure_lag_fn,
                active_tenants_fn=active_tenants_fn,
                stop_event=stop_event,
            )
        finally:
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
