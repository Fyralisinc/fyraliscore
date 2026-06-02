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

import asyncpg
import structlog
from redis.asyncio import Redis as AsyncRedis

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.embeddings.ollama import OllamaClient  # noqa: E402
from lib.shared.secrets import build_secret_store  # noqa: E402
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


async def _main() -> int:
    log = structlog.get_logger("scripts.run_discord_gateway_worker")
    try:
        dsn = os.environ["DATABASE_URL"]
        bot_token = os.environ["DISCORD_BOT_TOKEN"]
    except KeyError as exc:
        log.error("discord_gateway_missing_env", var=str(exc))
        return 2

    application_id = os.environ.get("DISCORD_CLIENT_ID")

    # The Redis lease is the ONLY thing that keeps two replicas from
    # double-delivering every Discord frame. Refuse to boot without it
    # (fail loud) rather than silently running unprotected.
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        log.error("discord_gateway_missing_env", var="REDIS_URL")
        return 2

    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=4, init=_register_codecs,
    )
    redis: AsyncRedis | None = None
    refresh_task: asyncio.Task[None] | None = None
    lock: LeaderLock | None = None
    stop_event = asyncio.Event()
    try:
        secret_store = build_secret_store(pool)
        # Reuse the same resolver shape the HTTP gateway uses (IN-07).
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

        # Ingestion data plane: when KAFKA_BOOTSTRAP_SERVERS is set, wire a
        # Kafka producer + raw-tier S3 client + per-tenant flag reader so
        # MESSAGE_CREATE frames traverse the full pipeline (shadow_write →
        # ingestion.raw → normalizer → observation_writer) for tenants with
        # `ingestion.kafka_path_enabled=TRUE`, instead of inline ingest().
        # The Gateway client's dispatch loop drives delivery (it flushes the
        # producer every batch — services/ingest/integrations/discord/gateway/
        # client.py), so no explicit flush is needed here. Guarded +
        # swallow-on-failure: a Kafka/S3 outage must not stop the worker
        # consuming Discord events (it falls back to inline ingest()).
        kafka_producer: IdempotentProducer | None = None
        s3_raw_client: S3Client | None = None
        tenant_flags: TenantFlags | None = None
        brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        if brokers:
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
            except Exception as exc:  # noqa: BLE001 — never block the worker
                log.error(
                    "discord_gateway_data_plane_wiring_failed",
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                kafka_producer = None
                s3_raw_client = None
                tenant_flags = None

        deps = DispatchDeps(
            pool=pool,
            tenant_resolver=tenant_resolver,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            application_id=application_id,
            s3_raw_client=s3_raw_client,
            kafka_producer=kafka_producer,
            tenant_flags=tenant_flags,
        )

        # ---- M4.3 single-instance lease (acquire BEFORE connecting) ---
        # Launcher-owned signal handlers so SIGTERM during the (possibly
        # multi-minute) lease-acquire backoff aborts cleanly. Once the
        # worker's run loop starts it re-installs its own handlers
        # (asyncio allows one handler per signal) and owns shutdown.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass

        redis = AsyncRedis.from_url(redis_url, decode_responses=False)
        lock = LeaderLock(redis)  # 30s TTL / 10s refresh (M4.1 defaults)
        config = LifecycleConfig(application_id=application_id or "discord")
        log.info(
            "discord_gateway_acquiring_lease",
            key=lock.key, ttl_s=lock.ttl_seconds,
        )
        if not await acquire_lease_with_backoff(
            lock, config=config, stop_event=stop_event,
        ):
            if stop_event.is_set():
                # SIGTERM raced the acquire backoff — a clean stop.
                log.info("discord_gateway_shutdown_during_lease_acquire")
                return 0
            log.error("discord_gateway_lease_acquire_timeout")
            return 3  # transient — orchestrator restarts to retry
        log.info("discord_gateway_lease_acquired", lease_value=lock.lease_value)

        # ---- M4.2 crash-RESUME (load persisted session + save hook) ---
        # RESUME is keyed by the Discord application id (the
        # gateway_session_state row). Without it we can still hold the
        # lease (single-instance safety) but cannot persist/RESUME.
        initial_state = None
        on_dispatched = None
        if application_id:
            try:
                persisted = await load_session_state(
                    pool, application_id=application_id,
                )
                initial_state = persisted_to_in_memory(persisted)
            except Exception:  # noqa: BLE001 — never block startup on a load hiccup
                log.exception("discord_gateway_state_load_failed")
                initial_state = None
            on_dispatched = make_save_hook(
                pool, application_id=application_id,
                lease_holder=lock.lease_value,
            )
            log.info(
                "discord_gateway_resume_state",
                resuming=initial_state is not None,
                last_seq=getattr(initial_state, "last_seq", None),
            )
        else:
            log.warning("discord_gateway_resume_disabled_no_application_id")

        worker = GatewayWorker(
            bot_token=bot_token,
            deps=deps,
            initial_state=initial_state,
            on_dispatched=on_dispatched,
        )

        # ---- M4.1 refresh tick — runs alongside the WS loop -----------
        # On refresh failure the lease was lost (we paused past the 30s
        # TTL and another pod took over); request a graceful worker
        # shutdown rather than fight for the surface.
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

        log.info(
            "discord_gateway_worker_booting",
            has_application_id=bool(application_id),
            resume_enabled=on_dispatched is not None,
        )
        exit_code = await worker.run_forever()
        if lease_lost and exit_code == 0:
            # Worker shut down because another replica took the lease.
            exit_code = 3
        return exit_code
    finally:
        # Stop the refresh tick + release the lease FIRST so a successor
        # can acquire immediately (no waiting out the 30s TTL).
        stop_event.set()
        if refresh_task is not None:
            try:
                await refresh_task
            except Exception:  # noqa: BLE001
                pass
        if lock is not None:
            try:
                await lock.release()
            except Exception:  # noqa: BLE001
                pass
        if redis is not None:
            try:
                await redis.aclose()
            except Exception:  # noqa: BLE001
                pass
        if "kafka_producer" in locals() and kafka_producer is not None:
            try:
                await kafka_producer.stop()
            except Exception:  # noqa: BLE001
                pass
        if "s3_raw_client" in locals() and s3_raw_client is not None:
            try:
                await s3_raw_client.close()
            except Exception:  # noqa: BLE001
                pass
        await pool.close()
        if "secret_store" in locals() and hasattr(secret_store, "aclose"):
            try:
                await secret_store.aclose()
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
