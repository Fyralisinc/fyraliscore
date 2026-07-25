"""Installation-scoped Telegram live MTProto gateway launcher.

The process binds one exact ``(tenant_id, installation_id)`` and never chooses a
"first active" installation. Its database reads, secret resolution, state
writes, ProviderTransport context, and Redis lease all use that same identity.
Deploy one worker binding per Telegram installation.

Env:
  DATABASE_URL                 (required) Postgres DSN.
  REDIS_URL                    (required) the installation lease/quota store.
  TELEGRAM_TENANT_ID           (required) exact tenant UUID.
  TELEGRAM_INSTALLATION_ID     (required) exact installation UUID.
  KAFKA_BOOTSTRAP_SERVERS      (optional) wire the data plane for the kafka-first
                               path; absent → inline ingest().

Telethon is an OPTIONAL dependency (pip install 'fyraliscore[telegram]'); without
it the worker exits with a clear error.
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

log = structlog.get_logger("scripts.run_telegram_gateway_worker")


@dataclass(frozen=True, slots=True)
class TelegramRuntimeBinding:
    tenant_id: UUID
    installation_id: UUID
    session: str
    api_id: int
    api_hash: str
    dialog_rows: tuple[Any, ...]


def _required_uuid_env(
    name: str,
    environ: Mapping[str, str] | None = None,
) -> UUID:
    source = os.environ if environ is None else environ
    raw = source.get(name, "").strip()
    if not raw:
        raise ValueError(f"{name} is required")
    try:
        value = UUID(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a UUID") from exc
    if value.int == 0:
        raise ValueError(f"{name} must not be the nil UUID")
    return value


def required_runtime_identity(
    environ: Mapping[str, str] | None = None,
) -> tuple[UUID, UUID]:
    return (
        _required_uuid_env("TELEGRAM_TENANT_ID", environ),
        _required_uuid_env("TELEGRAM_INSTALLATION_ID", environ),
    )


def telegram_lease_key(tenant_id: UUID, installation_id: UUID) -> str:
    return f"gateway:telegram:{tenant_id}:{installation_id}:leader_lock"


def telegram_worker_identity(
    tenant_id: UUID,
    installation_id: UUID,
) -> str:
    return f"telegram_gateway_worker:{tenant_id}:{installation_id}"


async def _resolve_secret(secret_store, ref, tenant_id):  # noqa: ANN001
    if not ref:
        return None
    raw = await secret_store.get(ref, tenant_id=tenant_id)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def load_telegram_runtime_binding(
    executor: Any,
    secret_store: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
) -> TelegramRuntimeBinding:
    """Load one exact active installation and its tenant-owned credentials."""
    install = await executor.fetchrow(
        """
        SELECT id, tenant_id, api_id, api_hash_secret_ref, session_secret_ref
          FROM telegram_installations
         WHERE tenant_id = $1
           AND id = $2
           AND disabled_at IS NULL
        """,
        tenant_id,
        installation_id,
    )
    if install is None:
        raise LookupError(
            "active Telegram installation was not found for the exact tenant "
            "and installation identity"
        )
    row_tenant = UUID(str(install["tenant_id"]))
    row_installation = UUID(str(install["id"]))
    if row_tenant != tenant_id or row_installation != installation_id:
        raise RuntimeError(
            "Telegram installation loader returned a different identity"
        )
    session = (
        await _resolve_secret(
            secret_store,
            install["session_secret_ref"],
            tenant_id,
        )
        or ""
    ).strip()
    api_hash = (
        await _resolve_secret(
            secret_store,
            install["api_hash_secret_ref"],
            tenant_id,
        )
        or ""
    ).strip()
    try:
        api_id = int(install["api_id"])
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Telegram installation has an invalid api_id") from exc
    if not session or not api_hash or api_id <= 0:
        raise RuntimeError(
            "Telegram installation is missing its live session credentials"
        )
    rows = await executor.fetch(
        """
        SELECT dialog_id, dialog_kind, title
          FROM telegram_dialogs
         WHERE tenant_id = $1
           AND telegram_installation_id = $2
           AND state = 'active'
         ORDER BY dialog_id
        """,
        tenant_id,
        installation_id,
    )
    return TelegramRuntimeBinding(
        tenant_id=tenant_id,
        installation_id=installation_id,
        session=session,
        api_id=api_id,
        api_hash=api_hash,
        dialog_rows=tuple(rows),
    )


async def persist_telegram_update_state(
    executor: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    pts: int | None,
    qts: int | None,
    seq: int | None,
    date: Any,
) -> None:
    result = await executor.execute(
        """
        UPDATE telegram_update_state
           SET pts = $3,
               qts = $4,
               seq = $5,
               update_date = $6,
               updated_at = now()
         WHERE tenant_id = $1
           AND telegram_installation_id = $2
        """,
        tenant_id,
        installation_id,
        pts,
        qts,
        seq,
        date,
    )
    if result != "UPDATE 1":
        raise RuntimeError(
            "Telegram update state is missing for the exact installation"
        )


async def _main() -> int:
    # Keep module import side-effect free for exact-binding tests while direct
    # script execution still resolves the scripts-local observability helper.
    from worker_observability import (
        install_signal_handlers,
        register_pool,
        start_worker_health,
    )

    dsn = os.environ.get("DATABASE_URL")
    redis_url = os.environ.get("REDIS_URL")
    if not dsn:
        log.error("telegram_gateway_missing_env", var="DATABASE_URL")
        return 2
    if not redis_url:
        log.error("telegram_gateway_missing_env", var="REDIS_URL")
        return 2
    try:
        tenant_id, installation_id = required_runtime_identity()
    except ValueError as exc:
        log.error("telegram_gateway_invalid_runtime_binding", error=str(exc))
        return 2
    worker_identity = telegram_worker_identity(tenant_id, installation_id)
    lease_key = telegram_lease_key(tenant_id, installation_id)

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
    from services.ingest.integrations.telegram.gateway.dispatch import DispatchDeps
    from services.ingest.integrations.telegram.gateway.worker import (
        TelegramGatewayWorker,
        build_dialog_index,
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
    worker: TelegramGatewayWorker | None = None
    kafka_producer = None
    s3_raw_client = None
    try:
        secret_store = build_secret_store(pool)

        try:
            async with tenant_transaction(tenant_id, pool=pool) as tctx:
                binding = await load_telegram_runtime_binding(
                    tctx,
                    secret_store,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                )
        except (LookupError, RuntimeError) as exc:
            log.error(
                "telegram_gateway_invalid_installation_binding",
                tenant_id=str(tenant_id),
                installation_id=str(installation_id),
                error=str(exc),
            )
            return 2
        dialog_index = build_dialog_index(list(binding.dialog_rows))

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
                        client_id=(
                            f"telegram-gateway-"
                            f"{str(installation_id)[:12]}"
                        ),
                    )
                )
                await kafka_producer.start()
                s3_raw_client = S3Client(
                    os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
                    endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
                )
                await s3_raw_client.connect()
                tenant_flags = TenantFlags(pool)
                log.info("telegram_gateway_data_plane_wired", brokers=brokers)
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "telegram_gateway_data_plane_wiring_failed",
                    error=str(exc)[:200],
                )
                kafka_producer = s3_raw_client = tenant_flags = None

        async def _save_state(pts, qts, seq, date):  # noqa: ANN001
            async with tenant_transaction(tenant_id, pool=pool) as tctx:
                await persist_telegram_update_state(
                    tctx,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    pts=pts,
                    qts=qts,
                    seq=seq,
                    date=date,
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

        # ---- single-instance lease (acquire BEFORE connecting) ----
        redis = AsyncRedis.from_url(redis_url, decode_responses=False)
        lock = LeaderLock(redis, key=lease_key)
        if not await lock.acquire():
            log.error(
                "telegram_gateway_lease_held_elsewhere",
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
                        "telegram_gateway_lease_lost",
                        key=lease_key,
                        tenant_id=str(tenant_id),
                        installation_id=str(installation_id),
                    )
                    stop_event.set()
                    return

        refresh_task = asyncio.create_task(_refresh_loop())

        worker = TelegramGatewayWorker(
            deps=deps,
            session=binding.session,
            api_id=binding.api_id,
            api_hash=binding.api_hash,
            dialog_index=dialog_index,
            save_state=_save_state,
        )
        log.info(
            "telegram_gateway_starting",
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
