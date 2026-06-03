"""Gateway startup wiring for integration state and ingestion data plane."""
from __future__ import annotations

import os

import asyncpg
from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger


log = get_logger("gateway")


def wire_in08_state(app_: FastAPI, pool: asyncpg.Pool) -> None:
    """Wire integration secret, tenant, feature-flag, and GitHub state."""
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

    if getattr(app_.state, "github_client", None) is None:
        from services.ingest.integrations.github.client import GithubClient

        app_.state.github_client = GithubClient(
            pool=pool,
            tenant_resolver=app_.state.tenant_resolver,
        )
    if getattr(app_.state, "github_replay_cache", None) is None:
        from services.ingest.integrations.github.replay_cache import (
            make_replay_cache,
        )

        app_.state.github_replay_cache = make_replay_cache()


async def wire_ingestion_data_plane(app_: FastAPI) -> None:
    """Wire Kafka/S3 ingestion data-plane clients onto ``app.state``."""
    if getattr(app_.state, "kafka_producer", None) is not None:
        return
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not brokers:
        return
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
        await producer.start()
        s3_client = S3Client(
            os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        )
        await s3_client.connect()
        app_.state.kafka_producer = producer
        app_.state.s3_raw_client = s3_client
        app_.state.notion_data_plane = SimpleNamespace(
            producer=producer, s3_client=s3_client,
        )
        log.info("ingestion_data_plane_wired", brokers=brokers)
    except Exception as exc:  # noqa: BLE001 - never block startup
        log.error(
            "ingestion_data_plane_wiring_failed",
            error_type=type(exc).__name__,
            error=str(exc),
        )


async def close_ingestion_data_plane(app_: FastAPI) -> None:
    """Tear down clients wired by ``wire_ingestion_data_plane``."""
    producer = getattr(app_.state, "kafka_producer", None)
    s3_client = getattr(app_.state, "s3_raw_client", None)
    if producer is not None:
        try:
            await producer.stop()
        except Exception:  # noqa: BLE001
            pass
    if s3_client is not None:
        try:
            await s3_client.close()
        except Exception:  # noqa: BLE001
            pass
