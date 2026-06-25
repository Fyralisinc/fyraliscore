"""Launcher for services.ingest.integrations.discord.gateway.worker — one process.

Mirrors the shape of `scripts/run_think_worker.py` and
`scripts/run_post_commit_worker.py`. Loads env, builds deps, runs the
worker until SIGTERM / SIGINT or fatal Discord close.

High availability (M4.3 wiring). A Discord bot token may be connected by
exactly ONE gateway session at a time — two replicas with the same token
double-deliver every frame. This launcher composes the
`gateway/lifecycle.py` primitives so a multi-replica deploy is safe:

  1. **Single-instance lease** (M4.1) — acquire a Redis lease BEFORE
     connecting; refresh it every 10s; on loss (another pod took over),
     stop consuming instead of fighting for the surface. Redis is
     mandatory: without the lease there is nothing preventing two
     replicas from double-delivering, so a missing ``REDIS_URL`` fails
     loud (exit 2) rather than silently dropping the guarantee.
  2. **Crash-RESUME** (M4.2) — load the persisted `gateway_session_state`
     on startup so a restart RESUMEs (Discord replays the buffered
     frames) instead of re-IDENTIFYing and dropping them, and hand the
     client an `on_dispatched` save hook so the session cursor is
     persisted after every dispatched frame. Keyed by the Discord
     application id; when ``DISCORD_CLIENT_ID`` is unset the lease still
     protects against double-delivery but RESUME is disabled (logged).

Exit codes:
  0 = clean shutdown (SIGTERM/SIGINT)
  1 = fatal Discord close (auth/intents misconfigured) — do NOT auto-restart
  2 = configuration error at startup (missing env, incl. REDIS_URL)
  3 = could not acquire the single-instance lease before the timeout, OR
      the lease was lost mid-run (another replica is active) — transient;
      the orchestrator should restart to retry / stand by.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import signal
import sys
import time
from typing import Any

import asyncpg
import structlog
from redis.asyncio import Redis as AsyncRedis

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.embeddings.ollama import OllamaClient  # noqa: E402
from lib.shared.secrets import (  # noqa: E402
    build_secret_store,
    load_app_secret_text_from_env,
)
from services.domain.actors.repo import ActorRepo  # noqa: E402
from services.domain.entity_aliases.repo import EntityAliasRepo  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.ingest.ingestion.feature_flags import TenantFlags  # noqa: E402
from services.ingest.ingestion.kafka.producer import (  # noqa: E402
    IdempotentProducer,
    ProducerConfig,
)
from services.ingest.ingestion.raw_tier.s3 import S3Client  # noqa: E402
from services.ingest.integrations.discord.gateway.dispatch import DispatchDeps  # noqa: E402
from services.ingest.integrations.discord.gateway.leader_lock import (  # noqa: E402
    DEFAULT_REFRESH_INTERVAL_SECONDS,
    LeaderLock,
)
from services.ingest.integrations.discord.gateway.lifecycle import (  # noqa: E402
    LifecycleConfig,
    acquire_lease_with_backoff,
    lease_refresh_loop,
    make_save_hook,
    persisted_to_in_memory,
)
from services.ingest.integrations.discord.gateway.session_state import (  # noqa: E402
    load_session_state,
)
from services.ingest.integrations.discord.gateway.worker import GatewayWorker  # noqa: E402
from services.app.webhooks.tenant_resolver import (  # noqa: E402
    InstallationCache,
    TenantResolverDeps,
    build_tenant_resolver,
    default_metrics,
)


class _LauncherConfig:
    def __init__(
        self,
        *,
        dsn: str,
        bot_token: str,
        application_id: str | None,
        redis_url: str,
    ) -> None:
        self.dsn = dsn
        self.bot_token = bot_token
        self.application_id = application_id
        self.redis_url = redis_url


class _DataPlane:
    def __init__(
        self,
        *,
        kafka_producer: IdempotentProducer | None = None,
        s3_raw_client: S3Client | None = None,
        tenant_flags: TenantFlags | None = None,
    ) -> None:
        self.kafka_producer = kafka_producer
        self.s3_raw_client = s3_raw_client
        self.tenant_flags = tenant_flags


class _HealthRuntime:
    def __init__(
        self,
        *,
        stop_event: asyncio.Event,
        health_ticker: asyncio.Task[None],
        health: Any,
    ) -> None:
        self.stop_event = stop_event
        self.health_ticker = health_ticker
        self.health = health


class _RuntimeResources:
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        health: _HealthRuntime,
    ) -> None:
        self.pool = pool
        self.health = health
        self.secret_store: Any | None = None
        self.data_plane: _DataPlane | None = None
        self.redis: AsyncRedis | None = None
        self.lock: LeaderLock | None = None
        self.refresh_task: asyncio.Task[None] | None = None


class _ResumeRuntime:
    def __init__(self, *, initial_state: Any | None, on_dispatched: Any | None) -> None:
        self.initial_state = initial_state
        self.on_dispatched = on_dispatched


def _load_config(log) -> tuple[_LauncherConfig | None, int | None]:
    try:
        dsn = os.environ["DATABASE_URL"]
        bot_token = load_app_secret_text_from_env("DISCORD_BOT_TOKEN")
    except KeyError as exc:
        log.error("discord_gateway_missing_env", var=str(exc))
        return None, 2
    except Exception as exc:  # noqa: BLE001
        log.error(
            "discord_gateway_secret_resolution_failed",
            var="DISCORD_BOT_TOKEN",
            error_type=type(exc).__name__,
        )
        return None, 2
    if not bot_token:
        log.error("discord_gateway_missing_env", var="DISCORD_BOT_TOKEN")
        return None, 2

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        log.error("discord_gateway_missing_env", var="REDIS_URL")
        return None, 2

    return (
        _LauncherConfig(
            dsn=dsn,
            bot_token=bot_token,
            application_id=os.environ.get("DISCORD_CLIENT_ID"),
            redis_url=redis_url,
        ),
        None,
    )


async def _create_pool(dsn: str) -> asyncpg.Pool:
    from lib.shared.db import asyncpg_pool_runtime_kwargs

    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="SOURCE_GATEWAY_POSTGRES_PGBOUNCER_COMPATIBLE",
    )
    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=4,
        init=_register_codecs,
        **runtime_kwargs,
    )


def _start_health_runtime() -> _HealthRuntime:
    from services.ingest.ingestion.observability import (
        Heartbeat,
        run_heartbeat_ticker,
        start_health_server,
    )

    stop_event = asyncio.Event()
    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=dict, heartbeat=heartbeat)
    health_ticker = asyncio.ensure_future(
        run_heartbeat_ticker(heartbeat, stop_event)
    )
    return _HealthRuntime(
        stop_event=stop_event,
        health_ticker=health_ticker,
        health=health,
    )


def _install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass


async def _wire_data_plane(
    pool: asyncpg.Pool,
    *,
    log,
) -> _DataPlane:
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not brokers:
        return _DataPlane()

    try:
        kafka_producer = IdempotentProducer(
            ProducerConfig(
                bootstrap_servers=brokers,
                client_id="discord-gateway-worker",
            )
        )
        await kafka_producer.start()
        s3_raw_client = S3Client(
            os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        )
        await s3_raw_client.connect()
        tenant_flags = TenantFlags(pool)
        log.info("discord_gateway_data_plane_wired", brokers=brokers)
        return _DataPlane(
            kafka_producer=kafka_producer,
            s3_raw_client=s3_raw_client,
            tenant_flags=tenant_flags,
        )
    except Exception as exc:  # noqa: BLE001 — never block the worker
        log.error(
            "discord_gateway_data_plane_wiring_failed",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return _DataPlane()


async def _build_dispatch_deps(
    pool: asyncpg.Pool,
    *,
    application_id: str | None,
    log,
) -> tuple[DispatchDeps, Any, _DataPlane]:
    secret_store = build_secret_store(pool)
    tenant_resolver = build_tenant_resolver(
        TenantResolverDeps(
            pool=pool,
            cache=InstallationCache(),
            clock=time.monotonic,
            metrics=default_metrics(),
        )
    )
    actor_repo = ActorRepo(pool)
    alias_repo = EntityAliasRepo(pool)
    try:
        embedder = OllamaClient()
    except Exception:  # noqa: BLE001
        embedder = None

    data_plane = await _wire_data_plane(pool, log=log)
    deps = DispatchDeps(
        pool=pool,
        tenant_resolver=tenant_resolver,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        application_id=application_id,
        s3_raw_client=data_plane.s3_raw_client,
        kafka_producer=data_plane.kafka_producer,
        tenant_flags=data_plane.tenant_flags,
    )
    return deps, secret_store, data_plane


async def _acquire_leader_lock(
    *,
    redis_url: str,
    application_id: str | None,
    stop_event: asyncio.Event,
    log,
) -> tuple[AsyncRedis, LeaderLock, int | None]:
    redis = AsyncRedis.from_url(redis_url, decode_responses=False)
    lock = LeaderLock(redis)
    config = LifecycleConfig(application_id=application_id or "discord")
    log.info(
        "discord_gateway_acquiring_lease",
        key=lock.key,
        ttl_s=lock.ttl_seconds,
    )
    if await acquire_lease_with_backoff(lock, config=config, stop_event=stop_event):
        log.info("discord_gateway_lease_acquired", lease_value=lock.lease_value)
        return redis, lock, None
    if stop_event.is_set():
        log.info("discord_gateway_shutdown_during_lease_acquire")
        return redis, lock, 0
    log.error("discord_gateway_lease_acquire_timeout")
    return redis, lock, 3


async def _build_resume_runtime(
    pool: asyncpg.Pool,
    *,
    application_id: str | None,
    lock: LeaderLock,
    log,
) -> _ResumeRuntime:
    if not application_id:
        log.warning("discord_gateway_resume_disabled_no_application_id")
        return _ResumeRuntime(initial_state=None, on_dispatched=None)

    try:
        persisted = await load_session_state(pool, application_id=application_id)
        initial_state = persisted_to_in_memory(persisted)
    except Exception:  # noqa: BLE001 — never block startup on a load hiccup
        log.exception("discord_gateway_state_load_failed")
        initial_state = None

    on_dispatched = make_save_hook(
        pool,
        application_id=application_id,
        lease_holder=lock.lease_value,
    )
    log.info(
        "discord_gateway_resume_state",
        resuming=initial_state is not None,
        last_seq=getattr(initial_state, "last_seq", None),
    )
    return _ResumeRuntime(
        initial_state=initial_state,
        on_dispatched=on_dispatched,
    )


async def _run_worker_with_lease_refresh(
    worker: GatewayWorker,
    *,
    lock: LeaderLock,
    stop_event: asyncio.Event,
    log,
) -> tuple[int, asyncio.Task[None]]:
    lease_lost = False

    async def _on_lease_lost() -> None:
        nonlocal lease_lost
        lease_lost = True
        log.error("discord_gateway_lease_lost", lease_value=lock.lease_value)
        worker.request_shutdown()

    refresh_task = asyncio.create_task(
        lease_refresh_loop(
            lock,
            interval_s=DEFAULT_REFRESH_INTERVAL_SECONDS,
            on_lost=_on_lease_lost,
            stop_event=stop_event,
        )
    )
    exit_code = await worker.run_forever()
    if lease_lost and exit_code == 0:
        exit_code = 3
    return exit_code, refresh_task


async def _cleanup_resources(resources: _RuntimeResources) -> None:
    resources.health.stop_event.set()
    resources.health.health_ticker.cancel()
    await asyncio.gather(resources.health.health_ticker, return_exceptions=True)
    if resources.health.health is not None:
        resources.health.health.shutdown()
    if resources.refresh_task is not None:
        try:
            await resources.refresh_task
        except Exception:  # noqa: BLE001
            pass
    if resources.lock is not None:
        try:
            await resources.lock.release()
        except Exception:  # noqa: BLE001
            pass
    if resources.redis is not None:
        try:
            await resources.redis.aclose()
        except Exception:  # noqa: BLE001
            pass
    if resources.data_plane is not None:
        if resources.data_plane.kafka_producer is not None:
            try:
                await resources.data_plane.kafka_producer.stop()
            except Exception:  # noqa: BLE001
                pass
        if resources.data_plane.s3_raw_client is not None:
            try:
                await resources.data_plane.s3_raw_client.close()
            except Exception:  # noqa: BLE001
                pass
    await resources.pool.close()
    if resources.secret_store is not None and hasattr(resources.secret_store, "aclose"):
        try:
            await resources.secret_store.aclose()
        except Exception:  # noqa: BLE001
            pass


async def _main() -> int:
    log = structlog.get_logger("scripts.run_discord_gateway_worker")
    config, early_exit = _load_config(log)
    if early_exit is not None:
        return early_exit
    assert config is not None

    pool = await _create_pool(config.dsn)
    health = _start_health_runtime()
    resources = _RuntimeResources(pool=pool, health=health)
    try:
        deps, resources.secret_store, resources.data_plane = await _build_dispatch_deps(
            pool=pool,
            application_id=config.application_id,
            log=log,
        )

        # ---- M4.3 single-instance lease (acquire BEFORE connecting) ---
        # Launcher-owned signal handlers so SIGTERM during the (possibly
        # multi-minute) lease-acquire backoff aborts cleanly. Once the
        # worker's run loop starts it re-installs its own handlers
        # (asyncio allows one handler per signal) and owns shutdown.
        _install_shutdown_handlers(health.stop_event)

        resources.redis, resources.lock, lease_exit = await _acquire_leader_lock(
            redis_url=config.redis_url,
            application_id=config.application_id,
            stop_event=health.stop_event,
            log=log,
        )
        if lease_exit is not None:
            return lease_exit

        # ---- M4.2 crash-RESUME (load persisted session + save hook) ---
        # RESUME is keyed by the Discord application id (the
        # gateway_session_state row). Without it we can still hold the
        # lease (single-instance safety) but cannot persist/RESUME.
        assert resources.lock is not None
        resume = await _build_resume_runtime(
            pool,
            application_id=config.application_id,
            lock=resources.lock,
            log=log,
        )

        worker = GatewayWorker(
            bot_token_provider=lambda: load_app_secret_text_from_env(
                "DISCORD_BOT_TOKEN",
            ),
            deps=deps,
            initial_state=resume.initial_state,
            on_dispatched=resume.on_dispatched,
        )

        # ---- M4.1 refresh tick — runs alongside the WS loop -----------
        # On refresh failure the lease was lost (we paused past the 30s
        # TTL and another pod took over); request a graceful worker
        # shutdown rather than fight for the surface.
        log.info(
            "discord_gateway_worker_booting",
            has_application_id=bool(config.application_id),
            resume_enabled=resume.on_dispatched is not None,
        )
        exit_code, resources.refresh_task = await _run_worker_with_lease_refresh(
            worker,
            lock=resources.lock,
            stop_event=health.stop_event,
            log=log,
        )
        return exit_code
    finally:
        # Stop the refresh tick + release the lease FIRST so a successor
        # can acquire immediately (no waiting out the 30s TTL).
        await _cleanup_resources(resources)


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
