"""OAuth install-state cleanup task for the gateway lifespan."""
from __future__ import annotations

import asyncio

import asyncpg

from lib.shared.db_leases import PostgresLease
from services.app.gateway.logging_config import get_logger


log = get_logger("gateway")

OAUTH_SWEEPER_LEASE_NAME = "gateway:oauth_state_sweeper"
OAUTH_SWEEPER_LEASE_TTL_SECONDS = 120.0


async def sweep_oauth_install_states_once(pool: asyncpg.Pool) -> str:
    """Delete expired/consumed OAuth install state rows in bounded batches."""
    return await pool.execute(
        """
        DELETE FROM oauth_install_states
         WHERE id IN (
            SELECT id FROM oauth_install_states
             WHERE expires_at < now() - INTERVAL '1 hour'
                OR (consumed_at IS NOT NULL
                    AND consumed_at < now() - INTERVAL '1 hour')
             LIMIT 1000
         )
        """,
    )


async def sweep_oauth_install_states_once_protected(
    pool: asyncpg.Pool,
    *,
    lease_name: str = OAUTH_SWEEPER_LEASE_NAME,
    lease_ttl_seconds: float = OAUTH_SWEEPER_LEASE_TTL_SECONDS,
) -> str | None:
    """Run one sweep if this replica owns the deployment-wide sweeper lease."""

    lease = PostgresLease(
        pool,
        lease_name=lease_name,
        ttl_seconds=lease_ttl_seconds,
        metadata={"component": "oauth_state_sweeper"},
    )
    if not await lease.acquire():
        log.info("oauth_install_states_sweep_skipped_lease_held")
        return None
    try:
        return await sweep_oauth_install_states_once(pool)
    finally:
        await lease.release()


async def _run_oauth_state_sweeper(
    pool: asyncpg.Pool,
    *,
    interval_s: float,
    lease_enabled: bool,
    lease_ttl_seconds: float,
) -> None:
    while True:
        try:
            await asyncio.sleep(interval_s)
            if lease_enabled:
                deleted = await sweep_oauth_install_states_once_protected(
                    pool,
                    lease_ttl_seconds=lease_ttl_seconds,
                )
            else:
                deleted = await sweep_oauth_install_states_once(pool)
            log.info("oauth_install_states_sweep", deleted_summary=deleted)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - background task logs and retries
            log.error(
                "oauth_install_states_sweep_error",
                error_type=type(exc).__name__,
            )


def start_oauth_state_sweeper(
    pool: asyncpg.Pool,
    *,
    interval_s: float = 300,
    lease_enabled: bool = True,
    lease_ttl_seconds: float = OAUTH_SWEEPER_LEASE_TTL_SECONDS,
) -> asyncio.Task[None]:
    """Start the periodic OAuth install-state cleanup task."""
    return asyncio.create_task(
        _run_oauth_state_sweeper(
            pool,
            interval_s=interval_s,
            lease_enabled=lease_enabled,
            lease_ttl_seconds=lease_ttl_seconds,
        )
    )


async def stop_oauth_state_sweeper(task: asyncio.Task[None] | None) -> None:
    """Cancel and await the OAuth sweeper task during gateway shutdown."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "oauth_install_states_sweeper_stop_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
