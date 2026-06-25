"""scripts/run_signal_gateway_worker.py — Signal live gateway launcher.

The Telegram-gateway-worker analog for Signal (linked-device, signal-cli
JSON-RPC). Holds ONE install's LIVE receive session and drives
`dispatch.handle_update` for every incoming message, shadow-writing onto
`ingestion.raw.signal` (kafka-first) or falling back to inline `core.ingest`. A
Signal linked device should be driven by only one live receive loop at a time, so
the launcher acquires the `gateway:signal:leader_lock` Redis lease BEFORE
connecting (mirrors Telegram / Discord).

Env:
  DATABASE_URL                 (required) Postgres DSN.
  REDIS_URL                    (required) the single-instance lease store.
  KAFKA_BOOTSTRAP_SERVERS      (optional) wire the data plane for the kafka-first
                               path; absent → inline ingest().
  SIGNAL_INSTALLATION_ID       (optional) which signal_installations row to run;
                               absent → the first active install.

OPERATOR SETUP (Signal has no official server API): this worker talks JSON-RPC to
a running **signal-cli** daemon holding the linked-device identity for this
install's `account_label`. Before this worker can ingest anything the operator
must (1) link signal-cli as a secondary device to a real Signal number
(`signal-cli link`, scan the QR from the phone's Linked Devices), and (2) run the
daemon in JSON-RPC mode (`signal-cli -a <number> daemon --tcp HOST:PORT` or
`--socket PATH`). Point the worker at it via SIGNAL_JSONRPC_ENDPOINT (see
integrations/signal/client.py). signal-cli is an OPTIONAL/external dependency;
without a reachable daemon the worker exits with a clear error.
"""
from __future__ import annotations

import asyncio
import os
import sys

import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)

log = structlog.get_logger("scripts.run_signal_gateway_worker")

_LEASE_KEY = "gateway:signal:leader_lock"


async def _resolve_secret(secret_store, ref, tenant_id):  # noqa: ANN001
    if not ref:
        return None
    raw = await secret_store.get(ref, tenant_id=tenant_id)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def _main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    redis_url = os.environ.get("REDIS_URL")
    if not dsn:
        log.error("signal_gateway_missing_env", var="DATABASE_URL")
        return 2
    if not redis_url:
        log.error("signal_gateway_missing_env", var="REDIS_URL")
        return 2

    import asyncpg
    from redis.asyncio import Redis as AsyncRedis

    from lib.embeddings.ollama import OllamaClient
    from lib.shared.db import (
        asyncpg_pool_runtime_kwargs,
        configure_connection_timeouts,
        positive_int_env,
    )
    from lib.shared.secrets import build_secret_store
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
    register_pool("signal_gateway_worker", pool)
    redis: AsyncRedis | None = None
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health("signal_gateway_worker", stop_event)
    refresh_task: asyncio.Task | None = None
    lock = None
    worker: SignalGatewayWorker | None = None
    kafka_producer = None
    s3_raw_client = None
    try:
        secret_store = build_secret_store(pool)

        # Select the install to run.
        inst_id = os.environ.get("SIGNAL_INSTALLATION_ID")
        if inst_id:
            install = await pool.fetchrow(
                "SELECT * FROM signal_installations WHERE id = $1::uuid "
                "AND disabled_at IS NULL", inst_id,
            )
        else:
            install = await pool.fetchrow(
                "SELECT * FROM signal_installations WHERE disabled_at IS NULL "
                "ORDER BY created_at LIMIT 1",
            )
        if install is None:
            log.error("signal_gateway_no_active_install")
            return 2
        tenant_id = install["tenant_id"]

        # The LIVE linked-device session (distinct from the backfill device per
        # Topology B). signal-cli holds the identity; the secret ref points at the
        # daemon attach material / account selector resolved by SignalClient.
        session = await _resolve_secret(
            secret_store, install["session_secret_ref"], tenant_id,
        )
        if not (session and install["account_label"]):
            log.error("signal_gateway_missing_live_session")
            return 2

        thread_rows = await pool.fetch(
            "SELECT thread_id, thread_kind, title FROM signal_threads "
            "WHERE signal_installation_id = $1 AND state = 'active'",
            install["id"],
        )
        thread_index = build_thread_index(thread_rows)

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
                        client_id="signal-gateway-worker",
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
            # Signal's sync state is a single advancing cursor (unlike Telegram's
            # pts/qts/seq/date), persisted on signal_update_state.
            await pool.execute(
                "UPDATE signal_update_state SET sync_cursor=$2, updated_at=now() "
                "WHERE signal_installation_id=$1",
                install["id"], cursor,
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

        # ---- single-instance lease (acquire BEFORE connecting) ----
        redis = AsyncRedis.from_url(redis_url, decode_responses=False)
        lock = LeaderLock(redis, key=_LEASE_KEY)
        if not await lock.acquire():
            log.error("signal_gateway_lease_held_elsewhere", key=_LEASE_KEY)
            return 3  # transient — orchestrator restarts to stand by

        lease_lost = False

        async def _refresh_loop() -> None:
            nonlocal lease_lost
            while not stop_event.is_set():
                await asyncio.sleep(10)
                if not await lock.refresh():
                    lease_lost = True
                    log.warning("signal_gateway_lease_lost")
                    stop_event.set()
                    return

        refresh_task = asyncio.create_task(_refresh_loop())

        worker = SignalGatewayWorker(
            deps=deps,
            session=session,
            account_label=install["account_label"],
            thread_index=thread_index,
            save_state=_save_state,
        )
        log.info("signal_gateway_starting", tenant_id=str(tenant_id))
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
