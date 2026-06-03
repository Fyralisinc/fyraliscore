#!/usr/bin/env python3
"""Live smoke test for the cutover circuit breaker's Kafka readers.

The breaker's unit tests inject mocks at the Kafka boundary, so the two
functions that actually talk to a broker —
`circuit_breaker._measure_kafka_lag_default` and
`._sample_active_tenants_default` — are never exercised by the suite. This
script closes that gap against a REAL broker.

It:
  1. Creates two raw lanes (`ingestion.raw.slack`, `ingestion.raw.github`)
     and the control-plane signal topic.
  2. Produces messages with a backdated CreateTime on the slack lane and
     commits the `normalizer.slack` group BEHIND the head → a known ~120s
     lag; commits `normalizer.github` AT the head → caught up (0s).
  3. Emits a traffic signal placing one tenant on (source=slack, partition=0).
  4. Calls the real reader functions and asserts the readings are sane —
     including that the 8 sources with no topic/group return {} (not crash),
     which is the per-source robustness the live path must have.
  5. If DATABASE_URL is set, runs a full unmocked `_process_tick` loop and
     asserts the tenant's `ingestion.kafka_path_enabled` flag flips to FALSE
     in Postgres — the entire production path, nothing mocked.

Usage:
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    DATABASE_URL=postgresql://... \
        python scripts/smoke_circuit_breaker_lag.py

Exit code 0 = all assertions passed.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from uuid import uuid4, UUID

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from services.ingest.ingestion.feature_flags.circuit_breaker import (
    _measure_kafka_lag_default,
    _sample_active_tenants_default,
)
from services.ingest.ingestion.kafka.topics import topic_for

BOOT = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_BASE = os.environ.get("BREAKER_NORMALIZER_GROUP_BASE", "normalizer")
SIGNAL_TOPIC = "ingestion.tenant_traffic_signal"

SLACK_TOPIC = topic_for("raw", "slack")     # ingestion.raw.slack
GITHUB_TOPIC = topic_for("raw", "github")   # ingestion.raw.github
LAG_SECONDS = 120
N_SLACK = 5
N_GITHUB = 3


def _create_topics(admin: AdminClient) -> None:
    new = [
        NewTopic(SLACK_TOPIC, num_partitions=2, replication_factor=1),
        NewTopic(GITHUB_TOPIC, num_partitions=2, replication_factor=1),
        NewTopic(SIGNAL_TOPIC, num_partitions=2, replication_factor=1),
    ]
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result(timeout=15)
            print(f"  created {topic}")
        except Exception as exc:  # already-exists is fine
            print(f"  {topic}: {type(exc).__name__} (likely exists) — ok")


def _produce_lag_fixture(producer: Producer) -> None:
    now_ms = int(time.time() * 1000)
    old_ms = now_ms - LAG_SECONDS * 1000
    # slack p0: backdated → the lane the normalizer is behind on.
    for i in range(N_SLACK):
        producer.produce(
            SLACK_TOPIC, value=b'{"smoke":"slack"}', partition=0, timestamp=old_ms,
        )
    # github p0: current → caught-up lane.
    for i in range(N_GITHUB):
        producer.produce(
            GITHUB_TOPIC, value=b'{"smoke":"github"}', partition=0, timestamp=now_ms,
        )
    producer.flush(15)


def _commit_with_retry(consumer: Consumer, tps: list[TopicPartition]) -> None:
    """A freshly-booted broker can briefly report NOT_COORDINATOR /
    COORDINATOR_LOAD_IN_PROGRESS while the internal `__consumer_offsets` topic
    initialises. Retry the commit through that window (harness-only concern)."""
    from confluent_kafka import KafkaException
    last: Exception | None = None
    for _ in range(40):  # ~20s
        try:
            consumer.commit(offsets=tps, asynchronous=False)
            return
        except KafkaException as exc:
            last = exc
            time.sleep(0.5)
    raise last  # type: ignore[misc]


def _commit_group_offsets() -> None:
    # normalizer.slack committed at offset 0 → next message to process is the
    # 120s-old one at offset 0 → lag ≈ 120s.
    c_slack = Consumer({
        "bootstrap.servers": BOOT,
        "group.id": f"{GROUP_BASE}.slack",
        "enable.auto.commit": False,
    })
    _commit_with_retry(c_slack, [TopicPartition(SLACK_TOPIC, 0, 0)])
    c_slack.close()

    # normalizer.github committed AT the head → caught up → 0s.
    c_github = Consumer({
        "bootstrap.servers": BOOT,
        "group.id": f"{GROUP_BASE}.github",
        "enable.auto.commit": False,
    })
    _low, high = c_github.get_watermark_offsets(
        TopicPartition(GITHUB_TOPIC, 0), timeout=10.0,
    )
    _commit_with_retry(c_github, [TopicPartition(GITHUB_TOPIC, 0, high)])
    c_github.close()


def _emit_signal(producer: Producer, tenant_id: UUID) -> None:
    body = json.dumps({
        "tenant_id": str(tenant_id),
        "source": "slack",
        "ingress_kind": "webhook",
        "raw_partition": 0,
        "emitted_at_ms": int(time.time() * 1000),
    }).encode("utf-8")
    producer.produce(SIGNAL_TOPIC, value=body, key=str(tenant_id).encode("utf-8"))
    producer.flush(15)


async def _phase_a_readers(tenant_id: UUID) -> None:
    print("\n[Phase A] real reader functions against the live broker")
    lag = await _measure_kafka_lag_default(
        bootstrap=BOOT, normalizer_group_base=GROUP_BASE,
    )
    active = await _sample_active_tenants_default(
        bootstrap=BOOT, signal_topic=SIGNAL_TOPIC, lookback_sec=300,
    )
    print(f"  lag (non-empty lanes): { {s: v for s, v in lag.items() if v} }")
    print(f"  active tenants       : {active}")

    slack_lag = lag.get("slack", {}).get(0, 0.0)
    github_lag = lag.get("github", {}).get(0, 0.0)
    assert LAG_SECONDS - 30 <= slack_lag <= LAG_SECONDS + 60, (
        f"slack lag {slack_lag:.1f}s not within sane band around {LAG_SECONDS}s"
    )
    assert github_lag == 0.0, f"github lane should be caught up, got {github_lag}s"
    # The 8 sources with no topic/group must degrade to {} — not raise.
    empty = [s for s, v in lag.items() if v == {}]
    assert len(empty) >= 8, (
        f"expected the untouched sources to read as empty; empty={empty}"
    )
    assert active.get(tenant_id) == {"slack": 0}, (
        f"active map wrong: {active.get(tenant_id)}"
    )
    print("  ✅ Phase A: slack≈120s breach, github 0s, untouched lanes empty, "
          "tenant mapped to its slack lane")


async def _phase_b_full_tick(tenant_id: UUID) -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("\n[Phase B] skipped — DATABASE_URL not set")
        return
    print("\n[Phase B] full unmocked _process_tick loop → flag flip in Postgres")
    import asyncpg
    from services.ingest.ingestion.feature_flags.circuit_breaker import (
        BreakerConfig, _process_tick, _TenantBreachState,
    )
    from services.ingest.ingestion.feature_flags.client import (
        KAFKA_PATH_ENABLED, TenantFlags,
    )

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3, statement_cache_size=0)
    try:
        # Register EVERY currently-active tenant (signals from prior smoke runs
        # accumulate on the shared topic) so the tick's state/flag writes can't
        # FK-fail on a stale tenant. Keeps the script re-runnable.
        active_now = await _sample_active_tenants_default(
            bootstrap=BOOT, signal_topic=SIGNAL_TOPIC, lookback_sec=300,
        )
        for tid in {tenant_id, *active_now}:
            await pool.execute(
                "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                tid, f"smoke-{tid.hex[:8]}",
            )
        flags = TenantFlags(pool)
        config = BreakerConfig(
            instance_name=f"smoke-{tenant_id.hex[:8]}",
            tick_interval_sec=0.01,
            breach_threshold_sec=60,
            breach_window_ticks=5,
            normalizer_group_base=GROUP_BASE,
            kafka_bootstrap=BOOT,
        )
        alerts: list = []

        async def _alert(tid: UUID, payload: dict) -> None:
            alerts.append((tid, payload))

        state: dict[UUID, _TenantBreachState] = {}
        for _ in range(5):  # 5 sustained breaches → trip
            await _process_tick(
                config=config, pool=pool, tenant_flags=flags, state=state,
                measure_lag_fn=_measure_kafka_lag_default,
                active_tenants_fn=_sample_active_tenants_default,
                alert_fn=_alert,
            )

        row = await pool.fetchrow(
            "SELECT flag_value, set_by FROM tenant_flags "
            "WHERE tenant_id = $1 AND flag_name = $2",
            tenant_id, KAFKA_PATH_ENABLED,
        )
        assert row is not None and row["flag_value"] is False, (
            f"flag did not flip to FALSE after 5 breached ticks: {row}"
        )
        assert row["set_by"] == "auto:circuit_breaker"
        assert alerts and alerts[0][1]["source"] == "slack", (
            f"alert missing/not slack lane: {alerts}"
        )
        print(f"  ✅ Phase B: flag flipped FALSE by auto:circuit_breaker; "
              f"alert lane={alerts[0][1]['source']} lag={alerts[0][1]['lag_seconds']:.0f}s")
    finally:
        await pool.close()


def main() -> int:
    print(f"smoke: bootstrap={BOOT} group_base={GROUP_BASE}")
    admin = AdminClient({"bootstrap.servers": BOOT})
    producer = Producer({"bootstrap.servers": BOOT})
    tenant_id = uuid4()

    print("\n[setup] topics + lag fixture + signal")
    _create_topics(admin)
    _produce_lag_fixture(producer)
    _commit_group_offsets()
    _emit_signal(producer, tenant_id)

    asyncio.run(_phase_a_readers(tenant_id))
    asyncio.run(_phase_b_full_tick(tenant_id))
    print("\nALL SMOKE ASSERTIONS PASSED ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
