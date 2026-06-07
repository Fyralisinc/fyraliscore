"""scripts/run_telegram_gateway_worker.py — Telegram live gateway launcher.

The Discord-gateway-worker analog for Telegram (MTProto). Holds ONE install's
LIVE updates connection and drives `dispatch.handle_update` for every incoming
message, shadow-writing onto `ingestion.raw.telegram` (kafka-first) or falling
back to inline `core.ingest`. A Telegram authorization may be driven by only one
live connection at a time, so the launcher acquires the
`gateway:telegram:leader_lock` Redis lease BEFORE connecting (mirrors Discord).

Env:
  DATABASE_URL                 (required) Postgres DSN.
  REDIS_URL                    (required) the single-instance lease store.
  KAFKA_BOOTSTRAP_SERVERS      (optional) wire the data plane for the kafka-first
                               path; absent → inline ingest().
  TELEGRAM_INSTALLATION_ID     (optional) which telegram_installations row to
                               run; absent → the first active install.

Telethon is an OPTIONAL dependency (pip install 'fyraliscore[telegram]'); without
it the worker exits with a clear error.
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys

import structlog


log = structlog.get_logger("scripts.run_telegram_gateway_worker")

_LEASE_KEY = "gateway:telegram:leader_lock"


async def _resolve_secret(secret_store, ref, tenant_id):  # noqa: ANN001
    if not ref:
        return None
    raw = await secret_store.get(ref, tenant_id=tenant_id)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def _main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    redis_url = os.environ.get("REDIS_URL")
    if not dsn:
        log.error("telegram_gateway_missing_env", var="DATABASE_URL")
        return 2
    if not redis_url:
        log.error("telegram_gateway_missing_env", var="REDIS_URL")
        return 2

    import asyncpg
    from redis.asyncio import Redis as AsyncRedis

    from lib.embeddings.ollama import OllamaClient
    from lib.shared.secrets import build_secret_store
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

    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=4)
    redis: AsyncRedis | None = None
    stop_event = asyncio.Event()
    refresh_task: asyncio.Task | None = None
    worker: TelegramGatewayWorker | None = None
    kafka_producer = None
    s3_raw_client = None
    try:
        secret_store = build_secret_store(pool)

        # Select the install to run.
        inst_id = os.environ.get("TELEGRAM_INSTALLATION_ID")
        if inst_id:
            install = await pool.fetchrow(
                "SELECT * FROM telegram_installations WHERE id = $1::uuid "
                "AND disabled_at IS NULL", inst_id,
            )
        else:
            install = await pool.fetchrow(
                "SELECT * FROM telegram_installations WHERE disabled_at IS NULL "
                "ORDER BY created_at LIMIT 1",
            )
        if install is None:
            log.error("telegram_gateway_no_active_install")
            return 2
        tenant_id = install["tenant_id"]

        session = await _resolve_secret(
            secret_store, install["session_secret_ref"], tenant_id,
        )
        api_hash = await _resolve_secret(
            secret_store, install["api_hash_secret_ref"], tenant_id,
        )
        if not (session and install["api_id"] and api_hash):
            log.error("telegram_gateway_missing_live_session")
            return 2

        dialog_rows = await pool.fetch(
            "SELECT dialog_id, dialog_kind, title FROM telegram_dialogs "
            "WHERE telegram_installation_id = $1 AND state = 'active'",
            install["id"],
        )
        dialog_index = build_dialog_index(dialog_rows)

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
                        client_id="telegram-gateway-worker",
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
            await pool.execute(
                "UPDATE telegram_update_state SET pts=$2, qts=$3, seq=$4, "
                "update_date=$5, updated_at=now() WHERE telegram_installation_id=$1",
                install["id"], pts, qts, seq, date,
            )

        deps = DispatchDeps(
            pool=pool,
            tenant_id=tenant_id,
            installation_id=str(install["id"]),
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            s3_raw_client=s3_raw_client,
            kafka_producer=kafka_producer,
            tenant_flags=tenant_flags,
        )

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass

        # ---- single-instance lease (acquire BEFORE connecting) ----
        redis = AsyncRedis.from_url(redis_url, decode_responses=False)
        lock = LeaderLock(redis, key=_LEASE_KEY)
        if not await lock.acquire():
            log.error("telegram_gateway_lease_held_elsewhere", key=_LEASE_KEY)
            return 3  # transient — orchestrator restarts to stand by

        async def _refresh_loop() -> None:
            while not stop_event.is_set():
                await asyncio.sleep(10)
                if not await lock.refresh():
                    log.warning("telegram_gateway_lease_lost")
                    stop_event.set()
                    return

        refresh_task = asyncio.create_task(_refresh_loop())

        worker = TelegramGatewayWorker(
            deps=deps,
            session=session,
            api_id=int(install["api_id"]),
            api_hash=api_hash,
            dialog_index=dialog_index,
            save_state=_save_state,
        )
        log.info("telegram_gateway_starting", tenant_id=str(tenant_id))
        run_task = asyncio.create_task(worker.run_forever())
        done, _ = await asyncio.wait(
            {run_task, asyncio.create_task(stop_event.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if run_task in done:
            run_task.result()  # surface any exception
        return 0
    finally:
        if refresh_task is not None:
            refresh_task.cancel()
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
        await pool.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
