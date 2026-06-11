"""lib/observability/pools.py — asyncpg pool gauges + acquire-wait timing.

Pools are registered by name (`register_pool("gateway", pool)`); a default-
registry collector renders `db_pool_*` gauges at scrape time, so there is
no background sampler to keep alive. The acquire-wait histogram is fed by
call sites that time `pool.acquire()` (lib.shared.db.transaction does this
for the shared pool).

Only duck-typed asyncpg.Pool accessors are used (get_size / get_idle_size /
get_min_size / get_max_size) so this module imports nothing heavy.
"""
from __future__ import annotations

import threading
from typing import Any

from lib.observability.metrics import (
    default_registry,
    histogram,
)

_lock = threading.Lock()
_pools: dict[str, Any] = {}

# Buckets tuned for lock-style waits: most acquires are sub-millisecond;
# anything over a second means the pool is saturated.
ACQUIRE_WAIT_BUCKETS = (
    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0,
)

acquire_wait_seconds = histogram(
    "db_pool_acquire_wait_seconds",
    "Time spent waiting for a connection from the asyncpg pool.",
    ("pool",),
    buckets=ACQUIRE_WAIT_BUCKETS,
)


def register_pool(name: str, pool: Any) -> None:
    """Track a pool for scrape-time gauge rendering. Re-registering the
    same name replaces the previous pool (process restart of a subsystem)."""
    with _lock:
        _pools[name] = pool


def unregister_pool(name: str) -> None:
    with _lock:
        _pools.pop(name, None)


def observe_acquire_wait(pool_name: str, seconds: float) -> None:
    acquire_wait_seconds.observe(seconds, pool=pool_name)


_FAMILY_HELP = (
    ("db_pool_size", "Current number of connections in the pool."),
    ("db_pool_idle", "Idle connections in the pool."),
    ("db_pool_in_use", "Connections currently checked out."),
    ("db_pool_min", "Configured pool min_size."),
    ("db_pool_max", "Configured pool max_size."),
)


def _render_pool_gauges() -> str:
    with _lock:
        pools = dict(_pools)
    if not pools:
        return ""
    stats: dict[str, dict[str, float]] = {}
    for name, pool in sorted(pools.items()):
        try:
            size = pool.get_size()
            idle = pool.get_idle_size()
            stats[name] = {
                "db_pool_size": size,
                "db_pool_idle": idle,
                "db_pool_in_use": size - idle,
                "db_pool_min": pool.get_min_size(),
                "db_pool_max": pool.get_max_size(),
            }
        except Exception:  # noqa: BLE001 — pool mid-close; skip this scrape
            continue
    if not stats:
        return ""
    lines: list[str] = []
    for family, help_text in _FAMILY_HELP:
        lines.append(f"# HELP {family} {help_text}")
        lines.append(f"# TYPE {family} gauge")
        for name, values in stats.items():
            lines.append(f'{family}{{pool="{name}"}} {values[family]:g}')
    return "\n".join(lines) + "\n"


default_registry().add_collector(_render_pool_gauges)


def _reset_for_tests() -> None:
    with _lock:
        _pools.clear()


__all__ = [
    "register_pool",
    "unregister_pool",
    "observe_acquire_wait",
    "ACQUIRE_WAIT_BUCKETS",
]
