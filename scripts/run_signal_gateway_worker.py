"""Signal live gateway launcher for one exact installation.

The process holds one installation's signal-cli HTTP JSON-RPC/SSE receive
session and drives ``dispatch.handle_update`` for every incoming message. Its
database reads, secret lookup, update-state writes, Redis lease, and provider
transport context all use the same required ``(tenant_id, installation_id)``.
Automatic installation selection is deliberately unsupported.

Required environment:

* ``DATABASE_URL`` and ``REDIS_URL``;
* ``SIGNAL_TENANT_ID`` and ``SIGNAL_INSTALLATION_ID`` (UUIDs);
* ``SIGNAL_JSONRPC_ENDPOINT`` (the signal-cli ``--http`` RPC endpoint).

``SIGNAL_SSE_ENDPOINT`` is optional for the native ``/api/v1/rpc`` endpoint,
whose ``/api/v1/events`` URL is derived automatically. It is required for a
custom Provider Lab JSON-RPC path that cannot be derived. The supported
signal-cli version is pinned by ``SIGNAL_CLI_VERSION`` (default ``0.14.4.1``).

Signal has no official server API. The operator links signal-cli as a secondary
device, runs ``signal-cli -a <number> daemon --http HOST:PORT``, and deploys one
worker binding per Signal installation. A shared multi-account daemon is an
explicit opt-in via ``SIGNAL_CLI_MULTI_ACCOUNT=1``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import structlog

log = structlog.get_logger("scripts.run_signal_gateway_worker")


@dataclass(frozen=True, slots=True)
class SignalRuntimeBinding:
    """Tenant-safe material needed by one live gateway worker."""

    tenant_id: UUID
    installation_id: UUID
    account_label: str
    session: str
    thread_rows: tuple[Any, ...]


def _required_text_env(
    name: str,
    environ: Mapping[str, str] | None = None,
) -> str:
    source = os.environ if environ is None else environ
    value = source.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _required_uuid_env(
    name: str,
    environ: Mapping[str, str] | None = None,
) -> UUID:
    value = _required_text_env(name, environ)
    try:
        parsed = UUID(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a UUID") from exc
    if parsed.int == 0:
        raise ValueError(f"{name} must not be the nil UUID")
    return parsed


def required_runtime_identity(
    environ: Mapping[str, str] | None = None,
) -> tuple[UUID, UUID]:
    """Return the mandatory, exact Signal tenant/installation identity."""

    return (
        _required_uuid_env("SIGNAL_TENANT_ID", environ),
        _required_uuid_env("SIGNAL_INSTALLATION_ID", environ),
    )


def signal_lease_key(tenant_id: UUID, installation_id: UUID) -> str:
    """One lease per tenant-bound linked-device installation."""

    return f"gateway:signal:{tenant_id}:{installation_id}:leader_lock"


def signal_worker_identity(tenant_id: UUID, installation_id: UUID) -> str:
    """Stable observability identity for a single installation process."""

    return f"signal_gateway_worker:{tenant_id}:{installation_id}"


async def _resolve_secret(secret_store, ref, tenant_id):  # noqa: ANN001
    if not ref:
        return None
    raw = await secret_store.get(ref, tenant_id=tenant_id)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def load_signal_runtime_binding(
    executor: Any,
    secret_store: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
) -> SignalRuntimeBinding:
    """Load one exact active installation and its tenant-owned credentials."""

    install = await executor.fetchrow(
        """
        SELECT id, tenant_id, account_label, session_secret_ref
          FROM signal_installations
         WHERE tenant_id = $1
           AND id = $2
           AND disabled_at IS NULL
        """,
        tenant_id,
        installation_id,
    )
    if install is None:
        raise LookupError(
            "active Signal installation was not found for the exact tenant "
            "and installation identity"
        )
    try:
        row_tenant_id = UUID(str(install["tenant_id"]))
        row_installation_id = UUID(str(install["id"]))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "Signal installation returned invalid identity data"
        ) from exc
    if row_tenant_id != tenant_id or row_installation_id != installation_id:
        raise RuntimeError(
            "Signal installation loader returned a different tenant or "
            "installation identity"
        )

    account_label = str(install["account_label"] or "").strip()
    session = await _resolve_secret(
        secret_store,
        install["session_secret_ref"],
        tenant_id,
    )
    session = (session or "").strip()
    if not account_label or not session:
        raise RuntimeError(
            "Signal installation is missing its account label or live "
            "linked-device session"
        )

    thread_rows = await executor.fetch(
        """
        SELECT thread_id, thread_kind, title
          FROM signal_threads
         WHERE tenant_id = $1
           AND signal_installation_id = $2
           AND state = 'active'
         ORDER BY thread_id
        """,
        tenant_id,
        installation_id,
    )
    return SignalRuntimeBinding(
        tenant_id=tenant_id,
        installation_id=installation_id,
        account_label=account_label,
        session=session,
        thread_rows=tuple(thread_rows),
    )


async def persist_signal_sync_cursor(
    executor: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    cursor: int | None,
) -> None:
    """Advance only the exact installation's live cursor."""

    result = await executor.execute(
        """
        UPDATE signal_update_state
           SET sync_cursor = $3,
               updated_at = now()
         WHERE tenant_id = $1
           AND signal_installation_id = $2
        """,
        tenant_id,
        installation_id,
        cursor,
    )
    if result != "UPDATE 1":
        raise RuntimeError(
            "Signal live update state is missing for the exact tenant and "
            "installation identity"
        )


async def _main() -> int:
    from services.ingest.source_contract.runtime import (
        validate_live_worker_startup,
    )

    validate_live_worker_startup("signal", "signal_gateway_worker")
    # Importing this script as a module must remain side-effect free for the
    # exact-binding tests. Executing it from scripts/ still needs this helper to
    # bootstrap the repository root before importing Fyralis packages.
    from worker_observability import (
        install_signal_handlers,
        register_pool,
        start_worker_health,
    )

    dsn = os.environ.get("DATABASE_URL")
    redis_url = os.environ.get("REDIS_URL")
    if not dsn:
        log.error("signal_gateway_missing_env", var="DATABASE_URL")
        return 2
    if not redis_url:
        log.error("signal_gateway_missing_env", var="REDIS_URL")
        return 2
    try:
        tenant_id, installation_id = required_runtime_identity()
        jsonrpc_endpoint = _required_text_env("SIGNAL_JSONRPC_ENDPOINT")
    except ValueError as exc:
        log.error("signal_gateway_invalid_runtime_binding", error=str(exc))
        return 2
    worker_identity = signal_worker_identity(tenant_id, installation_id)
    lease_key = signal_lease_key(tenant_id, installation_id)

    import asyncpg
    from redis.asyncio import Redis as AsyncRedis

    from lib.embeddings.ollama import OllamaClient
    from lib.shared.db import (
        asyncpg_pool_runtime_kwargs,
        configure_connection_timeouts,
        positive_int_env,
    )
    from lib.shared.secrets import build_secret_store
    from lib.shared.tenant_context import tenant_transaction
    from services.domain.actors.repo import ActorRepo
    from services.domain.entity_aliases.repo import EntityAliasRepo
    from services.ingest.ingestion.feature_flags import TenantFlags
    from services.ingest.ingestion.kafka import IdempotentProducer, ProducerConfig
    from services.ingest.ingestion.raw_tier.s3 import S3Client
    from services.ingest.integrations.discord.gateway.leader_lock import LeaderLock
    from services.ingest.integrations.signal.gateway.dispatch import DispatchDeps
    from services.ingest.integrations.signal.gateway.worker import (
        SignalGatewayWorker,
        build_thread_index,
    )

    pool_max = positive_int_env("SOURCE_GATEWAY_POSTGRES_POOL_SIZE", default=4)
    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="SOURCE_GATEWAY_POSTGRES_PGBOUNCER_COMPATIBLE",
    )
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=pool_max,
        init=configure_connection_timeouts,
        **runtime_kwargs,
    )
    register_pool(worker_identity, pool)
    redis: AsyncRedis | None = None
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health(worker_identity, stop_event)
    refresh_task: asyncio.Task | None = None
    lock = None
    worker: SignalGatewayWorker | None = None
    kafka_producer = None
    s3_raw_client = None
    try:
        secret_store = build_secret_store(pool)
        try:
            async with tenant_transaction(tenant_id, pool=pool) as tctx:
                binding = await load_signal_runtime_binding(
                    tctx,
                    secret_store,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                )
        except (LookupError, RuntimeError) as exc:
            log.error(
                "signal_gateway_invalid_installation_binding",
                tenant_id=str(tenant_id),
                installation_id=str(installation_id),
                error=str(exc),
            )
            return 2

        thread_index = build_thread_index(binding.thread_rows)

        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        try:
            embedder = OllamaClient()
        except Exception:  # noqa: BLE001
            embedder = None

        # Data plane (kafka-first) — guarded; failure falls back to inline.
        tenant_flags = None
        brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        if brokers:
            try:
                kafka_producer = IdempotentProducer(
                    ProducerConfig(
                        bootstrap_servers=brokers,
                        client_id=(f"signal-gateway-{str(installation_id)[:12]}"),
                    )
                )
                await kafka_producer.start()
                s3_raw_client = S3Client(
                    os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
                    endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
                )
                await s3_raw_client.connect()
                tenant_flags = TenantFlags(pool)
                log.info("signal_gateway_data_plane_wired", brokers=brokers)
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "signal_gateway_data_plane_wiring_failed",
                    error=str(exc)[:200],
                )
                kafka_producer = s3_raw_client = tenant_flags = None

        async def _save_state(cursor):  # noqa: ANN001
            async with tenant_transaction(tenant_id, pool=pool) as tctx:
                await persist_signal_sync_cursor(
                    tctx,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    cursor=cursor,
                )

        deps = DispatchDeps(
            pool=pool,
            tenant_id=tenant_id,
            installation_id=str(installation_id),
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            s3_raw_client=s3_raw_client,
            kafka_producer=kafka_producer,
            tenant_flags=tenant_flags,
        )

        # ---- one active receiver per exact installation ----
        redis = AsyncRedis.from_url(redis_url, decode_responses=False)
        lock = LeaderLock(redis, key=lease_key)
        if not await lock.acquire():
            log.error(
                "signal_gateway_lease_held_elsewhere",
                key=lease_key,
                tenant_id=str(tenant_id),
                installation_id=str(installation_id),
            )
            return 3  # transient — orchestrator restarts to stand by

        lease_lost = False

        async def _refresh_loop() -> None:
            nonlocal lease_lost
            while not stop_event.is_set():
                await asyncio.sleep(10)
                if not await lock.refresh():
                    lease_lost = True
                    log.warning(
                        "signal_gateway_lease_lost",
                        key=lease_key,
                        tenant_id=str(tenant_id),
                        installation_id=str(installation_id),
                    )
                    stop_event.set()
                    return

        refresh_task = asyncio.create_task(_refresh_loop())

        worker = SignalGatewayWorker(
            deps=deps,
            session=binding.session,
            account_label=binding.account_label,
            thread_index=thread_index,
            save_state=_save_state,
            jsonrpc_endpoint=jsonrpc_endpoint,
            sse_endpoint=os.environ.get("SIGNAL_SSE_ENDPOINT") or None,
            signal_cli_version=(os.environ.get("SIGNAL_CLI_VERSION") or "0.14.4.1"),
        )
        log.info(
            "signal_gateway_starting",
            worker_identity=worker_identity,
            tenant_id=str(tenant_id),
            installation_id=str(installation_id),
            lease_key=lease_key,
        )
        run_task = asyncio.create_task(worker.run_forever())
        stop_task = asyncio.create_task(stop_event.wait())
        done, _ = await asyncio.wait(
            {run_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        try:
            if run_task in done:
                run_task.result()  # surface any exception
                return 0
            return 3 if lease_lost else 0
        finally:
            stop_task.cancel()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, stop_task, return_exceptions=True)
    finally:
        if refresh_task is not None:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass
        if lock is not None:
            try:
                await lock.release()
            except Exception:  # noqa: BLE001
                pass
        if worker is not None:
            await worker.aclose()
        if kafka_producer is not None:
            try:
                await kafka_producer.stop()
            except Exception:  # noqa: BLE001
                pass
        if redis is not None:
            try:
                await redis.aclose()
            except Exception:  # noqa: BLE001
                pass
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
