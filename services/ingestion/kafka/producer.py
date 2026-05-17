"""Idempotent Kafka producer for the ingestion data plane.

Per LLD §5.2 (normalizer publishes `ingestion.normalized`) and §M2
work order (webhook/gateway/pubsub publish `ingestion.raw`).

Why confluent-kafka and not aiokafka:
  - LLD §5.1 specifies `enable.idempotence=true` for the raw-side
    producers. confluent-kafka's idempotent producer is the
    canonical implementation; aiokafka's idempotence support exists
    but lags the C client in correctness corner cases (it's the
    same Python wrapper used in `tests/synthesis_harness` already).
  - confluent-kafka is sync; we wrap with `asyncio.to_thread` so
    callers see an async surface. The producer maintains an
    internal background poll thread, so `.produce()` returns
    immediately and the delivery report callback fires later.
  - The `flush()` call (used at shutdown and for synchronous wait
    semantics in tests) IS a blocking-thread call.

Lifecycle:
  - One `IdempotentProducer` per worker process. Attached to
    `app.state.kafka_producer` for the gateway, owned by the
    process supervisor for the normalizer.
  - `start()` is a no-op (confluent-kafka has no async init);
    `stop()` calls `.flush()` with a timeout and tears down the
    background thread.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from confluent_kafka import Producer


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProducerConfig:
    """Configuration for the idempotent producer.

    Per LLD §5.2 / M2 work order — these are the production-grade
    settings. The fields here are the defaults; callers override
    `bootstrap_servers` (and rarely anything else) via env.
    """

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "fyralis-ingestion"
    # ---- Idempotence + durability ----
    # `enable.idempotence=true` requires:
    #   acks=all, max.in.flight.requests.per.connection<=5,
    #   retries>0, transactional.id NOT set (we don't run
    #   begin/commit_transaction — see M1.3 LLD-fix discussion).
    enable_idempotence: bool = True
    acks: str = "all"
    max_in_flight: int = 5
    retries: int = 2147483647  # confluent-kafka effective max
    compression_type: str = "zstd"
    linger_ms: int = 5
    extra: dict[str, Any] = field(default_factory=dict)

    def to_confluent_dict(self) -> dict[str, Any]:
        """Render the dataclass as a confluent-kafka client config
        dict (dotted keys, string values where librdkafka expects).
        """
        d: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "enable.idempotence": self.enable_idempotence,
            "acks": self.acks,
            "max.in.flight.requests.per.connection": self.max_in_flight,
            "retries": self.retries,
            "compression.type": self.compression_type,
            "linger.ms": self.linger_ms,
        }
        d.update(self.extra)
        return d


class IdempotentProducer:
    """Async wrapper over confluent_kafka.Producer with idempotent
    semantics.

    Thread safety: confluent_kafka.Producer is thread-safe for
    `.produce()` and `.poll()` from arbitrary threads. We only use
    the asyncio thread + the producer's internal background thread,
    so the `to_thread` indirection is the boundary.
    """

    def __init__(self, config: ProducerConfig | None = None) -> None:
        self._config = config or ProducerConfig()
        self._producer: Producer | None = None
        self._closed = False

    @property
    def is_started(self) -> bool:
        return self._producer is not None and not self._closed

    async def start(self) -> None:
        """Construct the underlying Producer. Idempotent."""
        if self._producer is not None:
            return
        cfg = self._config.to_confluent_dict()
        # Producer() runs librdkafka init synchronously; offload.
        self._producer = await asyncio.to_thread(Producer, cfg)
        log.info(
            "kafka_producer_started",
            extra={
                "bootstrap_servers": self._config.bootstrap_servers,
                "client_id": self._config.client_id,
            },
        )

    async def produce(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        on_delivery: Callable[[Any, Any], None] | None = None,
    ) -> None:
        """Enqueue a message. Returns when the message is in
        librdkafka's local queue — NOT when it lands on the broker.

        Use `flush()` if synchronous broker-ack semantics are needed
        (tests, shutdown). In production, the idempotent producer
        retries on the background thread; `on_delivery` callbacks
        report success/failure asynchronously.
        """
        if self._producer is None:
            raise RuntimeError("producer not started — call start() first")

        def _produce_sync() -> None:
            assert self._producer is not None
            self._producer.produce(
                topic=topic,
                value=value,
                key=key,
                headers=headers,
                on_delivery=on_delivery,
            )
            # poll(0) drains delivery-report callbacks without blocking;
            # without this, callbacks pile up and memory grows.
            self._producer.poll(0)

        await asyncio.to_thread(_produce_sync)

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        """Block until all in-flight messages are delivered or the
        timeout elapses. Returns the count of messages still in the
        queue (0 = all delivered).
        """
        if self._producer is None:
            return 0
        return await asyncio.to_thread(self._producer.flush, timeout_seconds)

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        """Flush remaining messages and tear down."""
        if self._producer is None:
            return
        remaining = await self.flush(timeout_seconds)
        if remaining:
            log.warning(
                "kafka_producer_stop_undelivered",
                extra={"remaining": remaining},
            )
        # confluent_kafka.Producer has no explicit close; dropping the
        # reference + GC stops the background thread once the queue
        # drains. Explicit-flush + None assignment is enough.
        self._producer = None
        self._closed = True


__all__ = ["IdempotentProducer", "ProducerConfig"]
