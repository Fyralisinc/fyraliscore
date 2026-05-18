"""CLI entrypoint: `python -m services.ingestion.workflows`.

Per [04-implementation-plan.md §M6.0] Phase 2: the substrate's CLI
front. Runs ONE asyncio service; the choice is by `WORKFLOW_SERVICE`
env var so the same `__main__.py` boots different concrete services
without per-service entrypoints.

============================================================
SIGTERM HANDLING (test_feels_monitor_sigterm_subprocess gate)
============================================================
The CLI installs `SIGTERM` + `SIGINT` handlers that set an
`asyncio.Event`. `LongRunningService.run(stop_event=...)` awaits this
event; the current tick completes, the state is persisted via
`persist_state`, and the process exits with code 0.

This is the SAME loop logic Phase 1's
`test_long_running_service_handles_stop_event_cleanly` already
verifies in-process; the subprocess test
(`test_feels_monitor_sigterm_subprocess`) adds the OS-signal
plumbing on top — same shape as M3.3's
`test_embedding_backlog_sigterm_resume`.

============================================================
ENV
============================================================
  DATABASE_URL              — Postgres DSN (required).
  KAFKA_BOOTSTRAP_SERVERS   — Kafka bootstrap (default localhost:9092).
  WORKFLOW_SERVICE          — service to boot. Currently:
                              "feels_onboarded_monitor" (only).
                              M6.1+ adds: "oauth_poller",
                              "tenant_onboarding", ...
  FEELS_MONITOR_TICK_SEC    — override tick interval (default 30.0).
  FEELS_MONITOR_RECENCY_DAYS— override recency window (default 7).
  FEELS_MONITOR_MIN_OBS     — observations threshold (default 1).
  WORKFLOWS_LOG_LEVEL       — log level (default INFO).
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal

from services.ingestion.kafka.producer import (
    IdempotentProducer,
    ProducerConfig,
)
from services.ingestion.workflows.feels_onboarded_monitor import (
    FeelsMonitorConfig,
    FeelsOnboardedMonitor,
)
from services.ingestion.workflows.oauth_poller import (
    OAuthPoller,
    OAuthPollerConfig,
    WORKFLOW_ID_DEFAULT as OAUTH_POLLER_INSTANCE_DEFAULT,
)
from services.ingestion.workflows.runtime import (
    LongRunningService,
    make_workflow_pool,
)
from services.ingestion.workflows.shard_fetch import (
    DEFAULT_DIAGNOSTIC_INSTANCE as SHARD_FETCH_INSTANCE_DEFAULT,
    ShardFetch,
    ShardFetchConfig,
)
from services.ingestion.workflows.source_onboarding import (
    SourceOnboarding,
    SourceOnboardingConfig,
    WORKFLOW_ID_DEFAULT as SOURCE_ONBOARDING_INSTANCE_DEFAULT,
)
from services.ingestion.workflows.tenant_onboarding import (
    TenantOnboardingConfig,
    TenantOnboardingOrchestrator,
    WORKFLOW_ID_DEFAULT as ORCHESTRATOR_INSTANCE_DEFAULT,
)


log = logging.getLogger(__name__)


def _build_feels_monitor_config() -> FeelsMonitorConfig:
    return FeelsMonitorConfig(
        tick_interval_seconds=float(
            os.environ.get("FEELS_MONITOR_TICK_SEC", "30.0"),
        ),
        recency_window_days=int(
            os.environ.get("FEELS_MONITOR_RECENCY_DAYS", "7"),
        ),
        min_observations_for_feels_onboarded=int(
            os.environ.get("FEELS_MONITOR_MIN_OBS", "1"),
        ),
    )


async def _run_service(name: str) -> None:
    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    producer = IdempotentProducer(ProducerConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        client_id=f"workflow-{name}",
    ))
    await producer.start()

    service: LongRunningService
    if name == "feels_onboarded_monitor":
        service = FeelsOnboardedMonitor(
            pool, producer,
            config=_build_feels_monitor_config(),
        )
    elif name == "oauth_poller":
        service = OAuthPoller(
            pool,
            config=OAuthPollerConfig(
                tick_interval_seconds=float(
                    os.environ.get("OAUTH_POLLER_TICK_SEC", "5.0"),
                ),
                max_triggers_per_tick=int(
                    os.environ.get("OAUTH_POLLER_BATCH", "50"),
                ),
                instance_name=os.environ.get(
                    "OAUTH_POLLER_INSTANCE",
                    OAUTH_POLLER_INSTANCE_DEFAULT,
                ),
            ),
        )
    elif name == "tenant_onboarding":
        service = TenantOnboardingOrchestrator(
            pool,
            config=TenantOnboardingConfig(
                tick_interval_seconds=float(
                    os.environ.get("ORCHESTRATOR_TICK_SEC", "10.0"),
                ),
                max_signals_per_tick=int(
                    os.environ.get("ORCHESTRATOR_BATCH", "50"),
                ),
                instance_name=os.environ.get(
                    "ORCHESTRATOR_INSTANCE",
                    ORCHESTRATOR_INSTANCE_DEFAULT,
                ),
            ),
        )
    elif name == "source_onboarding":
        service = SourceOnboarding(
            pool,
            config=SourceOnboardingConfig(
                tick_interval_seconds=float(
                    os.environ.get("SOURCE_ONBOARDING_TICK_SEC", "5.0"),
                ),
                max_signals_per_tick=int(
                    os.environ.get("SOURCE_ONBOARDING_BATCH", "50"),
                ),
                instance_name=os.environ.get(
                    "SOURCE_ONBOARDING_INSTANCE",
                    SOURCE_ONBOARDING_INSTANCE_DEFAULT,
                ),
            ),
        )
    elif name == "shard_fetch":
        service = ShardFetch(
            pool, producer,
            config=ShardFetchConfig(
                tick_interval_seconds=float(
                    os.environ.get("SHARD_FETCH_TICK_SEC", "5.0"),
                ),
                max_signals_per_tick=int(
                    os.environ.get("SHARD_FETCH_BATCH", "10"),
                ),
                lease_timeout_seconds=float(
                    os.environ.get("SHARD_FETCH_LEASE_SEC", "30.0"),
                ),
                flush_timeout_seconds=float(
                    os.environ.get("SHARD_FETCH_FLUSH_SEC", "5.0"),
                ),
                instance_name=os.environ.get(
                    "SHARD_FETCH_INSTANCE",
                    SHARD_FETCH_INSTANCE_DEFAULT,
                ),
            ),
        )
    else:
        raise SystemExit(
            f"WORKFLOW_SERVICE={name!r} not recognized. "
            f"Known: feels_onboarded_monitor, oauth_poller, "
            f"tenant_onboarding, source_onboarding, shard_fetch."
        )

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("workflow.service.started", extra={"service": name})
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.service.shutting_down", extra={"service": name})
        await producer.stop()
        await pool.close()
    log.info("workflow.service.exited", extra={"service": name})


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("WORKFLOWS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    service_name = os.environ.get(
        "WORKFLOW_SERVICE", "feels_onboarded_monitor",
    )
    asyncio.run(_run_service(service_name))


if __name__ == "__main__":
    main()
