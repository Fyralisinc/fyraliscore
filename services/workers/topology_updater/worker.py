"""
services/workers/topology_updater/worker.py — drains topo_dirty_queue
and recomputes topo_embeddings via the alpha-anchored rule with
damped propagation (S2, migration 0032).

Loop
----
  1. Dequeue up to BATCH_SIZE pending rows (highest delta_magnitude
     first, then FIFO).
  2. For each row:
       - call TopoRepo.recompute_topo() → returns delta
       - if delta > DELTA_EPSILON: enqueue this Model's neighbors
         at hop_depth + 1 with damped delta (delta × γ)
       - if damped delta < DELTA_TERMINATE_EPSILON: don't propagate
         (subtree terminates)
       - mark the row processed
  3. Sleep INTERVAL_S, repeat.

Why damping
-----------
Without damping, every edge change would propagate indefinitely
through the graph. With γ = 0.5, propagation reaches depth 3-4
before the magnitude falls below ε_terminate. In a dense substrate,
that's typically ~50 Models touched per change — bounded, fast,
and converges.

Why priority queue
------------------
A `falsifier_triggered_upstream` archive cascade should propagate
faster than a routine new-edge insert. The dirty queue's
delta_magnitude column gives us that priority for free; the worker
just orders by it.

Why no FOR UPDATE SKIP LOCKED
-----------------------------
v1 runs a single topology updater. If multiple workers are deployed
later, switch to a row-level lease pattern (same as the Think
worker). For now: simpler is better.

Public API
----------
  run_once(pool, *, tenant_id=None, batch_size=BATCH_SIZE)
      -> RunReport
      Single sweep. Returns counts for telemetry.

Tunable
-------
  - TOPO_UPDATER_INTERVAL_S (default 60s) — between-sweep sleep
  - TOPO_UPDATER_BATCH_SIZE (default 100) — rows per sweep
  - TOPO_DELTA_EPSILON / TOPO_DELTA_TERMINATE_EPSILON / TOPO_DAMPING_GAMMA
    (lib/topology/embeddings.py) — the propagation knobs
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from lib.topology.embeddings import (
    DAMPING_GAMMA,
    DELTA_EPSILON,
    DELTA_TERMINATE_EPSILON,
)
from services.topology.topo_repo import TopoRepo


_log = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = float(os.environ.get("TOPO_UPDATER_INTERVAL_S", "60"))
DEFAULT_BATCH_SIZE = int(os.environ.get("TOPO_UPDATER_BATCH_SIZE", "100"))


@dataclass
class RunReport:
    """Telemetry for one sweep."""
    rows_processed: int = 0
    rows_significant: int = 0      # delta > epsilon (will propagate)
    rows_below_epsilon: int = 0    # delta <= epsilon (won't propagate)
    rows_failed: int = 0
    neighbors_enqueued: int = 0
    errors: list[str] = field(default_factory=list)


async def _process_one_row(
    conn: asyncpg.Connection,
    row: dict[str, Any],
    *,
    topo_repo: TopoRepo,
    report: RunReport,
) -> None:
    """Process a single dirty-queue row in its own savepoint so a
    single Model's failure doesn't poison the rest of the batch."""
    queue_row_id = row["id"]
    model_id = row["model_id"]
    tenant_id = row["tenant_id"]
    hop_depth = row["hop_depth"]
    try:
        async with conn.transaction():
            result = await topo_repo.recompute_topo(
                conn,
                model_id=model_id,
                tenant_id=tenant_id,
            )
            delta = result["delta"]
            await topo_repo.mark_processed(
                conn, queue_row_id=queue_row_id
            )

            # Propagation decision: if the delta exceeds epsilon
            # AND the damped delta at the next hop would still be
            # above the termination threshold, enqueue neighbors.
            damped_delta = delta * DAMPING_GAMMA
            if delta > DELTA_EPSILON and damped_delta >= DELTA_TERMINATE_EPSILON:
                count = await topo_repo.enqueue_neighbors(
                    conn,
                    model_id=model_id,
                    tenant_id=tenant_id,
                    hop_depth=hop_depth,
                    delta_magnitude=damped_delta,
                )
                report.neighbors_enqueued += count
                report.rows_significant += 1
            else:
                report.rows_below_epsilon += 1
        report.rows_processed += 1
    except Exception as e:  # noqa: BLE001 — telemetry catch-all
        report.rows_failed += 1
        report.errors.append(f"{model_id}: {type(e).__name__}: {e}")
        # Mark failed (don't set processed_at; row will retry).
        try:
            await topo_repo.mark_failed(
                conn,
                queue_row_id=queue_row_id,
                error=f"{type(e).__name__}: {e}",
            )
        except Exception:
            pass


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> RunReport:
    """Single sweep. Drains up to `batch_size` rows from the dirty
    queue; returns a RunReport.

    Each row is processed in its own savepoint so a single Model's
    failure doesn't poison the batch.
    """
    topo_repo = TopoRepo(pool=pool)
    report = RunReport()
    async with pool.acquire() as conn:
        rows = await topo_repo.dequeue_pending(
            conn, tenant_id=tenant_id, limit=batch_size
        )
        for row in rows:
            await _process_one_row(
                conn, row, topo_repo=topo_repo, report=report
            )
    if report.rows_failed:
        _log.warning(
            "topology_updater errors during sweep",
            extra={
                "errors": report.errors[:5],  # cap log payload
                "total_failed": report.rows_failed,
            },
        )
    return report


async def run_forever(
    pool: asyncpg.Pool,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """Long-running entrypoint. Sleeps `interval_s` between sweeps.
    Used by the worker process; tests use run_once directly."""
    _log.info(
        "topology_updater started",
        extra={"interval_s": interval_s, "batch_size": batch_size},
    )
    while True:
        try:
            await run_once(pool, batch_size=batch_size)
        except Exception:  # noqa: BLE001
            _log.exception("topology_updater sweep crashed")
        await asyncio.sleep(interval_s)


__all__ = [
    "RunReport",
    "run_once",
    "run_forever",
    "DEFAULT_INTERVAL_S",
    "DEFAULT_BATCH_SIZE",
]
