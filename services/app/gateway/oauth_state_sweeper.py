"""OAuth install-state cleanup task for the gateway lifespan."""
from __future__ import annotations

import asyncio

import asyncpg

from services.app.gateway.logging_config import get_logger


log = get_logger("gateway")


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


async def _run_oauth_state_sweeper(
    pool: asyncpg.Pool,
    *,
    interval_s: float,
) -> None:
    while True:
        try:
            await asyncio.sleep(interval_s)
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
) -> asyncio.Task[None]:
    """Start the periodic OAuth install-state cleanup task."""
    return asyncio.create_task(
        _run_oauth_state_sweeper(pool, interval_s=interval_s)
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
