"""Contract-only supervisor for session-oriented source connectors."""

from __future__ import annotations

import argparse
import asyncio
import os
from typing import Any

import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)


log = structlog.get_logger("scripts.run_connector_gateway_worker")
_SOURCES = ("discord", "telegram", "signal")


async def _run_installation(
    source: str,
    install: Any,
    router: Any,
    stop_event: asyncio.Event,
) -> None:
    attempt = 0
    while not stop_event.is_set():
        try:
            await router.run_gateway(source, install, stop_event)
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = min(60.0, float(2 ** min(attempt, 6)))
            log.error(
                "connector_gateway_session_failed",
                source=source,
                installation_id=str(install["id"]),
                attempt=attempt,
                retry_in_seconds=delay,
                error=str(exc)[:300],
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except TimeoutError:
                continue


async def _main(source: str) -> int:
    dsn = os.environ.get("DATABASE_URL")
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not dsn or not brokers:
        log.error(
            "connector_gateway_missing_configuration",
            missing=[
                name
                for name, value in (
                    ("DATABASE_URL", dsn),
                    ("KAFKA_BOOTSTRAP_SERVERS", brokers),
                )
                if not value
            ],
        )
        return 2

    import asyncpg

    from lib.shared.db import (
        asyncpg_pool_runtime_kwargs,
        configure_connection_timeouts,
    )
    from lib.shared.secrets import build_secret_store
    from services.ingest.connector_platform.workflow_wiring import (
        build_workflow_connector_wiring,
    )
    from services.ingest.ingestion.kafka import IdempotentProducer, ProducerConfig
    from services.ingest.ingestion.raw_tier.s3 import S3Client

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=int(os.environ.get("SOURCE_GATEWAY_POSTGRES_POOL_SIZE", "8")),
        init=configure_connection_timeouts,
        **asyncpg_pool_runtime_kwargs(
            dsn=dsn,
            process_env_var="SOURCE_GATEWAY_PGBOUNCER_COMPATIBLE",
        ),
    )
    register_pool(f"{source}_gateway_worker", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health(f"{source}_gateway_worker", stop_event)
    producer = IdempotentProducer(
        ProducerConfig(
            bootstrap_servers=brokers,
            client_id=f"{source}-contract-gateway",
        )
    )
    raw_client = S3Client(
        os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
    )
    wiring = None
    tasks: dict[str, asyncio.Task[None]] = {}
    try:
        await producer.start()
        await raw_client.connect()
        wiring = build_workflow_connector_wiring(
            pool=pool,
            secret_store=build_secret_store(pool),
            s3_raw_client=raw_client,
            kafka_producer=producer,
        )
        while not stop_event.is_set():
            await wiring.refresh_routing()
            installs = await pool.fetch(
                """
                SELECT *
                  FROM source_connector_installations
                 WHERE connector_id = $1
                   AND desired_state = 'Ready'
                   AND observed_phase IN ('Ready', 'Degraded')
                   AND removed_at IS NULL
                """,
                f"fyralis/{source}",
            )
            active = {str(row["id"]): row for row in installs}
            for install_id, task in tuple(tasks.items()):
                if install_id not in active or task.done():
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    tasks.pop(install_id, None)
            for install_id, install in active.items():
                if install_id not in tasks:
                    tasks[install_id] = asyncio.create_task(
                        _run_installation(
                            source, install, wiring.router, stop_event
                        ),
                        name=f"{source}-gateway-{install_id}",
                    )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=30)
            except TimeoutError:
                continue
    finally:
        stop_event.set()
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        if wiring is not None:
            await wiring.close()
        await producer.stop()
        await raw_client.close()
        await health_shutdown()
        await pool.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=_SOURCES)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_main(args.source)))


if __name__ == "__main__":
    main()
