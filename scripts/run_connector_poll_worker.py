"""Contract-only incremental polling worker for all poll-capable sources."""

from __future__ import annotations

import asyncio
import os

import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)


log = structlog.get_logger("scripts.run_connector_poll_worker")


async def _main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not dsn or not brokers:
        log.error(
            "connector_poll_missing_configuration",
            database_url=bool(dsn),
            kafka_bootstrap_servers=bool(brokers),
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
    from services.ingest.source_contract.capabilities import INCREMENTAL_POLL_V1
    from services.ingest.source_contract.source_catalog import source_ids

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=int(os.environ.get("SOURCE_POLL_POSTGRES_POOL_SIZE", "12")),
        init=configure_connection_timeouts,
        **asyncpg_pool_runtime_kwargs(
            dsn=dsn,
            process_env_var="SOURCE_POLL_PGBOUNCER_COMPATIBLE",
        ),
    )
    register_pool("connector_poll_worker", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health("connector_poll_worker", stop_event)
    producer = IdempotentProducer(
        ProducerConfig(
            bootstrap_servers=brokers,
            client_id="source-connector-poll-worker",
        )
    )
    raw_client = S3Client(
        os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
    )
    wiring = None
    try:
        await producer.start()
        await raw_client.connect()
        wiring = build_workflow_connector_wiring(
            pool=pool,
            secret_store=build_secret_store(pool),
            s3_raw_client=raw_client,
            kafka_producer=producer,
        )
        poll_sources = tuple(
            source
            for source in source_ids()
            if INCREMENTAL_POLL_V1.ref
            in wiring.composition.registry.for_source(source).manifest.available_capability_refs
        )
        while not stop_event.is_set():
            await wiring.refresh_routing()
            installs = await pool.fetch(
                """
                SELECT *
                  FROM source_connector_installations
                 WHERE connector_id = ANY($1::text[])
                   AND desired_state = 'Ready'
                   AND observed_phase IN ('Ready', 'Degraded')
                   AND removed_at IS NULL
                 ORDER BY next_reconcile_at, id
                """,
                [f"fyralis/{source}" for source in poll_sources],
            )
            for install in installs:
                source = str(install["connector_id"]).removeprefix("fyralis/")
                try:
                    for _ in range(100):
                        count, has_more = await wiring.router.poll_and_emit(
                            source, install
                        )
                        if not has_more:
                            break
                    log.info(
                        "connector_poll_completed",
                        source=source,
                        installation_id=str(install["id"]),
                        final_page_records=count,
                    )
                except Exception as exc:
                    log.error(
                        "connector_poll_failed",
                        source=source,
                        installation_id=str(install["id"]),
                        error=str(exc)[:300],
                    )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=float(os.environ.get("SOURCE_POLL_INTERVAL_SECONDS", "60")),
                )
            except TimeoutError:
                continue
    finally:
        stop_event.set()
        if wiring is not None:
            await wiring.close()
        await producer.stop()
        await raw_client.close()
        await health_shutdown()
        await pool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
