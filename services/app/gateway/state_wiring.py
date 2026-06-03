"""Gateway startup wiring for integration state and ingestion data plane."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

import asyncpg
from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger
from services.app.gateway.settings import GatewaySettings


log = get_logger("gateway")


@dataclass(frozen=True, slots=True)
class IngestionDataPlaneWiring:
    """Result of wiring Kafka/S3 data-plane clients."""

    wired: bool
    owned: bool

    def __bool__(self) -> bool:
        return self.wired


def wire_integration_runtime_state(app_: FastAPI, pool: asyncpg.Pool) -> None:
    """Wire shared integration/webhook runtime state."""
    import time

    from lib.shared.secrets import build_secret_store
    from services.app.webhooks.secrets import assert_prod_safety_invariants
    from services.app.webhooks.tenant_resolver import (
        InstallationCache,
        TenantResolverDeps,
        build_tenant_resolver,
        default_metrics,
    )

    assert_prod_safety_invariants()

    if getattr(app_.state, "pool", None) is None:
        app_.state.pool = pool

    if getattr(app_.state, "secret_store", None) is None:
        app_.state.secret_store = build_secret_store(pool)

    if getattr(app_.state, "tenant_resolver", None) is None:
        app_.state.tenant_resolver = build_tenant_resolver(
            TenantResolverDeps(
                pool=pool,
                cache=InstallationCache(),
                clock=time.monotonic,
                metrics=default_metrics(),
            )
        )

    if getattr(app_.state, "tenant_flags", None) is None:
        from services.ingest.ingestion.feature_flags import TenantFlags

        app_.state.tenant_flags = TenantFlags(pool)

async def wire_ingestion_data_plane(
    app_: FastAPI,
    *,
    settings: GatewaySettings,
) -> IngestionDataPlaneWiring:
    """Wire Kafka/S3 ingestion data-plane clients onto ``app.state``."""
    if (
        getattr(app_.state, "kafka_producer", None) is not None
        and getattr(app_.state, "s3_raw_client", None) is not None
    ):
        return IngestionDataPlaneWiring(wired=True, owned=False)
    brokers = settings.kafka_bootstrap_servers
    if not brokers:
        if settings.require_ingestion_data_plane:
            raise RuntimeError(
                "KAFKA_BOOTSTRAP_SERVERS is required when "
                "GATEWAY_REQUIRE_INGESTION_DATA_PLANE=1"
            )
        return IngestionDataPlaneWiring(wired=False, owned=False)
    producer = None
    s3_client = None
    try:
        from types import SimpleNamespace

        from services.ingest.ingestion.kafka.producer import (
            IdempotentProducer,
            ProducerConfig,
        )
        from services.ingest.ingestion.raw_tier.s3 import S3Client

        producer = IdempotentProducer(
            ProducerConfig(
                bootstrap_servers=brokers,
                client_id="gateway-ingress",
            )
        )
        await asyncio.wait_for(
            producer.start(),
            timeout=settings.ingestion_data_plane_startup_timeout_s,
        )
        app_.state.kafka_producer = producer
        s3_client = S3Client(
            settings.s3_raw_bucket,
            endpoint_url=settings.s3_endpoint_url,
        )
        await asyncio.wait_for(
            s3_client.connect(),
            timeout=settings.ingestion_data_plane_startup_timeout_s,
        )
        app_.state.s3_raw_client = s3_client
        app_.state.notion_data_plane = SimpleNamespace(
            producer=producer, s3_client=s3_client,
        )
        log.info("ingestion_data_plane_wired", brokers=brokers)
        return IngestionDataPlaneWiring(wired=True, owned=True)
    except Exception as exc:  # noqa: BLE001 - optional unless required=True
        log.error(
            "ingestion_data_plane_wiring_failed",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        await _close_partial_data_plane(
            app_,
            producer=producer,
            s3_client=s3_client,
        )
        if settings.require_ingestion_data_plane:
            raise
        return IngestionDataPlaneWiring(wired=False, owned=False)


async def close_ingestion_data_plane(app_: FastAPI) -> None:
    """Tear down clients wired by ``wire_ingestion_data_plane``."""
    producer = getattr(app_.state, "kafka_producer", None)
    s3_client = getattr(app_.state, "s3_raw_client", None)
    await _close_partial_data_plane(
        app_,
        producer=producer,
        s3_client=s3_client,
    )


async def _close_partial_data_plane(
    app_: FastAPI,
    *,
    producer: object | None,
    s3_client: object | None,
) -> None:
    """Close any data-plane clients that were created before a failure."""
    if producer is not None:
        try:
            await producer.stop()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "ingestion_data_plane_producer_stop_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
    if s3_client is not None:
        try:
            await s3_client.close()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "ingestion_data_plane_s3_close_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
    if getattr(app_.state, "kafka_producer", None) is producer:
        app_.state.kafka_producer = None
    if getattr(app_.state, "s3_raw_client", None) is s3_client:
        app_.state.s3_raw_client = None
    notion_data_plane = getattr(app_.state, "notion_data_plane", None)
    if notion_data_plane is not None and (
        getattr(notion_data_plane, "producer", None) is producer
        or getattr(notion_data_plane, "s3_client", None) is s3_client
    ):
        app_.state.notion_data_plane = None


__all__ = [
    "IngestionDataPlaneWiring",
    "wire_integration_runtime_state",
    "wire_ingestion_data_plane",
    "close_ingestion_data_plane",
]
