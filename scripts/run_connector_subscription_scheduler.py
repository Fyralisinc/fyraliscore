"""Renew Google watch channels exclusively through the connector contract."""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
import os

import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)


log = structlog.get_logger("scripts.run_connector_subscription_scheduler")
_SOURCES = ("gmail", "google_calendar", "google_drive")


async def _main(source: str) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("connector_subscription_missing_database_url")
        return 2

    import asyncpg

    from lib.shared.db import (
        asyncpg_pool_runtime_kwargs,
        configure_connection_timeouts,
    )
    from lib.shared.secrets import build_secret_store
    from services.ingest.connector_platform.workflow_wiring import (
        build_workflow_connector_wiring,
    )

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=int(os.environ.get("SOURCE_SCHEDULER_POSTGRES_POOL_SIZE", "8")),
        init=configure_connection_timeouts,
        **asyncpg_pool_runtime_kwargs(
            dsn=dsn,
            process_env_var="SOURCE_SCHEDULER_PGBOUNCER_COMPATIBLE",
        ),
    )
    register_pool(f"{source}_watch_scheduler", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health(f"{source}_watch_scheduler", stop_event)
    wiring = build_workflow_connector_wiring(
        pool=pool,
        secret_store=build_secret_store(pool),
    )
    try:
        while not stop_event.is_set():
            await wiring.refresh_routing()
            rows = await pool.fetch(
                """
                SELECT install.*,
                       (state.values ->> 'expires_at_epoch_seconds')::bigint AS expires_at
                  FROM source_connector_installations AS install
                  LEFT JOIN source_connector_installation_data AS state
                    ON state.installation_id = install.id
                   AND state.namespace = 'subscription.state'
                 WHERE install.connector_id = $1
                   AND install.desired_state = 'Ready'
                   AND install.observed_phase IN ('Ready', 'Degraded')
                   AND install.removed_at IS NULL
                   AND (
                     state.installation_id IS NULL
                     OR (state.values ->> 'expires_at_epoch_seconds') IS NULL
                     OR (state.values ->> 'expires_at_epoch_seconds')::bigint
                          < extract(epoch FROM now() + interval '24 hours')::bigint
                   )
                 ORDER BY install.next_reconcile_at
                 LIMIT 50
                """,
                f"fyralis/{source}",
            )
            for install in rows:
                try:
                    subscription = await wiring.router.ensure_subscription(
                        source, install
                    )
                    log.info(
                        "connector_subscription_ensured",
                        source=source,
                        installation_id=str(install["id"]),
                        expires_at=subscription.expires_at_epoch_seconds,
                        checked_at=datetime.now(UTC).isoformat(),
                    )
                except Exception as exc:
                    log.error(
                        "connector_subscription_failed",
                        source=source,
                        installation_id=str(install["id"]),
                        error=str(exc)[:300],
                    )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=900)
            except TimeoutError:
                continue
    finally:
        await wiring.close()
        await health_shutdown()
        await pool.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=_SOURCES)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_main(args.source)))


if __name__ == "__main__":
    main()
