"""services/ingestion/db_config.py — pool-mode registry.

Documents which ingestion worker classes use which Postgres pool mode.
M1 ships this module empty (no workers exist yet); downstream milestones
populate it as they bring their workers online.

Per ingestion LLD §5.2:
  - Path-B writer (M5) — pgbouncer transaction mode. Every observation
    write acquires a connection from a tightly-bounded pool fronted by
    pgbouncer; `statement_cache_size=0` is mandatory.
  - ShardFetchWorkflow activity workers (M3) — pgbouncer transaction
    mode. Same reasoning: high fan-out, short connections.
  - OnboardingTriggerPollerWorkflow activity (M2) — DIRECT pool. One
    poller per pod, holds connection through the LOCK SKIP LOCKED
    transaction; prepared statements amortise well.
  - Reconciler activity (M4) — DIRECT pool. Single-pod cadence.

Each entry below is consumed by the worker-startup code in
services/ingestion/workers/*.py (to be added in M2+) when constructing
its pool via `lib.shared.db.init_pool`.

Reading this file by itself is sufficient for an operator to know which
workers must be re-pointed at a pgbouncer endpoint vs. a direct DB
endpoint during the M5 ramp.
"""
from __future__ import annotations

from typing import Literal, TypedDict


PoolMode = Literal["direct", "pgbouncer_transaction"]


class WorkerPoolConfig(TypedDict):
    """Per-worker pool config metadata.

    `mode`             — pool mode required by the worker.
    `min_size`         — asyncpg pool min connections.
    `max_size`         — asyncpg pool max connections.
    `command_timeout`  — asyncpg per-statement timeout in seconds.
    `rationale`        — one-line explanation; lands in startup logs.
    """

    mode: PoolMode
    min_size: int
    max_size: int
    command_timeout: float
    rationale: str


# Empty in M1 — populated by M2+ as workers come online. Keep the dict
# typed and import-stable so the worker-startup paths can reference
# it from day one of M2 without import errors.
WORKER_POOLS: dict[str, WorkerPoolConfig] = {}


__all__ = ["PoolMode", "WorkerPoolConfig", "WORKER_POOLS"]
