"""Gateway startup wiring for integration state and ingestion data plane."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import asyncpg
from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger
from services.app.gateway.settings import GatewaySettings


log = get_logger("gateway")


_CONNECTOR_INSTALLATIONS_PROBE = (
    "SELECT 1 FROM source_connector_installations LIMIT 1"
)


@dataclass(frozen=True, slots=True)
class IntegrationRuntimeState:
    """Structured shared runtime for integration and webhook paths."""

    pool: asyncpg.Pool
    secret_store: object
    tenant_resolver: object


@dataclass(frozen=True, slots=True)
class IntegrationRuntimeWiring:
    """Result of wiring shared integration runtime state."""

    runtime: IntegrationRuntimeState
    pool_alias_created: bool
    secret_store_created: bool
    tenant_resolver_created: bool


@dataclass(frozen=True, slots=True)
class IntegrationRuntimeProbeResult:
    """Readiness probe result for one integration runtime dependency."""

    component: str
    ok: bool
    detail: str | None = None
    error_type: str | None = None


class IntegrationRuntimeWiringError(RuntimeError):
    """Raised when one integration runtime subcomponent cannot be wired."""

    def __init__(self, component: str, original: BaseException) -> None:
        super().__init__(str(original))
        self.component = component
        self.original = original


class IntegrationRuntimeValidationError(RuntimeError):
    """Raised when a required integration runtime validation probe fails."""

    def __init__(self, result: IntegrationRuntimeProbeResult) -> None:
        super().__init__(result.detail or result.component)
        self.result = result
        self.component = result.component


@dataclass(frozen=True, slots=True)
class IngestionDataPlaneWiring:
    """Result of wiring Kafka/S3 data-plane clients."""

    wired: bool
    owned: bool

    def __bool__(self) -> bool:
        return self.wired


def assert_integration_runtime_safety() -> None:
    """Fail startup when shared integration security invariants are unsafe."""
    from services.app.webhooks.secrets import assert_prod_safety_invariants

    assert_prod_safety_invariants()


def wire_pool_alias(app_: FastAPI, pool: asyncpg.Pool) -> bool:
    """Attach the compatibility pool alias, refusing mismatched aliases."""
    existing_pool = getattr(app_.state, "pool", None)
    if existing_pool is None:
        app_.state.pool = pool
        return True
    if existing_pool is not pool:
        raise RuntimeError(
            "app.state.pool is already wired to a different pool"
        )
    return False


def wire_secret_store(app_: FastAPI, pool: asyncpg.Pool) -> tuple[object, bool]:
    """Attach or reuse the shared encrypted secret store."""
    from lib.shared.secrets import build_secret_store

    existing = getattr(app_.state, "secret_store", None)
    if existing is not None:
        return existing, False
    secret_store = build_secret_store(pool)
    app_.state.secret_store = secret_store
    return secret_store, True


def wire_tenant_resolver(app_: FastAPI, pool: asyncpg.Pool) -> tuple[object, bool]:
    """Attach or reuse the DB-backed provider-installation resolver."""
    import time

    from services.app.webhooks.tenant_resolver import (
        InstallationCache,
        TenantResolverDeps,
        build_tenant_resolver,
        default_metrics,
    )

    existing = getattr(app_.state, "tenant_resolver", None)
    if existing is not None:
        return existing, False
    tenant_resolver = build_tenant_resolver(
        TenantResolverDeps(
            pool=pool,
            cache=InstallationCache(),
            clock=time.monotonic,
            metrics=default_metrics(),
        )
    )
    app_.state.tenant_resolver = tenant_resolver
    return tenant_resolver, True


def attach_integration_runtime_state(
    app_: FastAPI,
    runtime: IntegrationRuntimeState,
) -> None:
    """Attach the structured runtime and ensure legacy aliases are aligned."""
    existing_runtime = getattr(app_.state, "integration_runtime", None)
    if existing_runtime is not None and existing_runtime is not runtime:
        if getattr(existing_runtime, "pool", None) is not runtime.pool:
            raise RuntimeError(
                "app.state.integration_runtime is wired to a different pool"
            )
        for name in ("secret_store", "tenant_resolver"):
            if getattr(existing_runtime, name, None) is not getattr(runtime, name):
                raise RuntimeError(
                    "app.state.integration_runtime has drifted "
                    f"from {name}"
                )

    for name, value in (
        ("pool", runtime.pool),
        ("secret_store", runtime.secret_store),
        ("tenant_resolver", runtime.tenant_resolver),
    ):
        existing = getattr(app_.state, name, None)
        if existing is not None and existing is not value:
            raise RuntimeError(
                f"app.state.{name} is already wired to a different object"
            )
        setattr(app_.state, name, value)
    app_.state.integration_runtime = runtime


def wire_integration_runtime_state(
    app_: FastAPI,
    pool: asyncpg.Pool,
) -> IntegrationRuntimeWiring:
    """Wire shared integration/webhook runtime state."""
    try:
        assert_integration_runtime_safety()
    except Exception as exc:  # noqa: BLE001
        raise IntegrationRuntimeWiringError(
            "integration_state.safety",
            exc,
        ) from exc

    existing_runtime = getattr(app_.state, "integration_runtime", None)
    if existing_runtime is not None:
        runtime = existing_runtime
        try:
            if getattr(runtime, "pool", None) is not pool:
                raise RuntimeError(
                    "app.state.integration_runtime is wired to a different pool"
                )
            attach_integration_runtime_state(app_, runtime)
        except Exception as exc:  # noqa: BLE001
            raise IntegrationRuntimeWiringError(
                "integration_state.runtime",
                exc,
            ) from exc
        return IntegrationRuntimeWiring(
            runtime=runtime,
            pool_alias_created=False,
            secret_store_created=False,
            tenant_resolver_created=False,
        )

    try:
        pool_alias_created = wire_pool_alias(app_, pool)
    except Exception as exc:  # noqa: BLE001
        raise IntegrationRuntimeWiringError(
            "integration_state.pool",
            exc,
        ) from exc
    try:
        secret_store, secret_store_created = wire_secret_store(app_, pool)
    except Exception as exc:  # noqa: BLE001
        raise IntegrationRuntimeWiringError(
            "integration_state.secret_store",
            exc,
        ) from exc
    try:
        tenant_resolver, tenant_resolver_created = wire_tenant_resolver(
            app_, pool,
        )
    except Exception as exc:  # noqa: BLE001
        raise IntegrationRuntimeWiringError(
            "integration_state.tenant_resolver",
            exc,
        ) from exc
    runtime = IntegrationRuntimeState(
        pool=pool,
        secret_store=secret_store,
        tenant_resolver=tenant_resolver,
    )
    try:
        attach_integration_runtime_state(app_, runtime)
    except Exception as exc:  # noqa: BLE001
        raise IntegrationRuntimeWiringError(
            "integration_state.runtime",
            exc,
        ) from exc
    return IntegrationRuntimeWiring(
        runtime=runtime,
        pool_alias_created=pool_alias_created,
        secret_store_created=secret_store_created,
        tenant_resolver_created=tenant_resolver_created,
    )


async def probe_integration_runtime_state(
    app_state: Any,
    *,
    timeout_s: float,
) -> tuple[IntegrationRuntimeProbeResult, ...]:
    """Probe required DB objects used by integration runtime state."""
    runtime = getattr(app_state, "integration_runtime", None)
    pool = getattr(runtime, "pool", None) if runtime is not None else None
    if pool is None:
        pool = getattr(app_state, "pool", None)
    if pool is None:
        return (
            IntegrationRuntimeProbeResult(
                component="integration_state.runtime",
                ok=False,
                detail="pool_missing",
            ),
        )

    async def _probe(component: str, sql: str) -> IntegrationRuntimeProbeResult:
        try:
            await asyncio.wait_for(pool.fetchval(sql), timeout=timeout_s)
        except TimeoutError as exc:
            return IntegrationRuntimeProbeResult(
                component=component,
                ok=False,
                detail=f"probe exceeded {timeout_s:g}s",
                error_type=type(exc).__name__,
            )
        except Exception as exc:  # noqa: BLE001
            return IntegrationRuntimeProbeResult(
                component=component,
                ok=False,
                detail="probe_failed",
                error_type=type(exc).__name__,
            )
        return IntegrationRuntimeProbeResult(component=component, ok=True)

    return (
        await _probe(
            "integration_state.schema.source_connector_installations",
            _CONNECTOR_INSTALLATIONS_PROBE,
        ),
    )


async def validate_integration_runtime_state(
    app_state: Any,
    *,
    timeout_s: float,
) -> tuple[IntegrationRuntimeProbeResult, ...]:
    """Validate integration runtime dependencies, raising on first failure."""
    results = await probe_integration_runtime_state(
        app_state,
        timeout_s=timeout_s,
    )
    for result in results:
        if not result.ok:
            raise IntegrationRuntimeValidationError(result)
    return results

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
    "IntegrationRuntimeProbeResult",
    "IntegrationRuntimeState",
    "IntegrationRuntimeValidationError",
    "IntegrationRuntimeWiring",
    "IntegrationRuntimeWiringError",
    "IngestionDataPlaneWiring",
    "attach_integration_runtime_state",
    "wire_integration_runtime_state",
    "assert_integration_runtime_safety",
    "probe_integration_runtime_state",
    "validate_integration_runtime_state",
    "wire_pool_alias",
    "wire_secret_store",
    "wire_tenant_resolver",
    "wire_ingestion_data_plane",
    "close_ingestion_data_plane",
]
