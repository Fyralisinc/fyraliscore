"""
services/workers/neighborhood_detector/worker.py — periodic
community-detection sweep over the active edge graph (S2,
migration 0032).

Loop
----
  Every INTERVAL_S (default 1h):
    For each tenant:
      - call NeighborhoodsRepo.recompute_for_tenant()
      - log RecomputeReport telemetry

The recompute is fully orchestrated inside the repo (load Models +
edges, detect communities, prune singletons, match to existing
neighborhoods for stable IDs, upsert + dissolve, refresh
membership). The worker is just the scheduler.

Public API
----------
  run_once(pool, *, tenant_id=None) -> dict[tenant_id, RecomputeReport]
      Single sweep. If tenant_id is None, processes every tenant
      with at least one active Model.

Tunable
-------
  - NEIGHBORHOOD_DETECTOR_INTERVAL_S (default 3600s = 1h) — sweep
    cadence
  - lib/topology/community.py constants — algorithm thresholds
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.topology.events_repo import TopologyEventsRepo
from services.topology.neighborhoods_repo import (
    NeighborhoodsRepo,
    RecomputeReport,
)


_log = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = float(
    os.environ.get("NEIGHBORHOOD_DETECTOR_INTERVAL_S", "3600")
)

# T6 enqueue: enqueue at most this many T6 triggers per phase event
# kind per recompute pass. Prevents a tenant-wide cascade (e.g. 200
# emergence events at once) from saturating the Think queue.
T6_ENQUEUE_PER_KIND_LIMIT = int(
    os.environ.get("NEIGHBORHOOD_DETECTOR_T6_LIMIT_PER_KIND", "10")
)


async def _list_tenants(conn: asyncpg.Connection) -> list[UUID]:
    rows = await conn.fetch(
        "SELECT DISTINCT tenant_id FROM models WHERE status = 'active'"
    )
    return [r["tenant_id"] for r in rows]


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
) -> dict[UUID, RecomputeReport]:
    """One detection sweep. Returns per-tenant reports.

    For each tenant, the recompute + T6 enqueue happen in ONE
    transaction. This is critical: if the T6 enqueue fails the
    phase events must roll back (they reference neighborhoods the
    LLM hasn't been asked about yet, and a stuck event without a
    trigger gets re-emitted on the next run, creating duplicates).
    """
    repo = NeighborhoodsRepo(pool=pool)
    events_repo = TopologyEventsRepo()
    out: dict[UUID, RecomputeReport] = {}
    async with pool.acquire() as conn:
        if tenant_id is None:
            tenants = await _list_tenants(conn)
        else:
            tenants = [tenant_id]
        for tid in tenants:
            try:
                async with conn.transaction():
                    report = await repo.recompute_for_tenant(
                        conn, tenant_id=tid
                    )
                    if report.phase_events_emitted:
                        await _enqueue_t6_for_events(
                            conn, events_repo, tid, report.phase_event_ids,
                        )
                out[tid] = report
                if report.communities_after_prune > 0 or report.phase_events_emitted:
                    _log.info(
                        "neighborhood_detector recomputed",
                        extra={
                            "tenant_id": str(tid),
                            "models": report.models_seen,
                            "edges": report.edges_seen,
                            "communities": report.communities_after_prune,
                            "matched": report.matched_to_existing,
                            "new": report.new_neighborhoods,
                            "dissolved": report.dissolved_neighborhoods,
                            "phase_events": report.phase_events_emitted,
                        },
                    )
            except Exception:  # noqa: BLE001
                _log.exception(
                    "neighborhood_detector failed for tenant",
                    extra={"tenant_id": str(tid)},
                )
    return out


async def _enqueue_t6_for_events(
    conn: asyncpg.Connection,
    events_repo: TopologyEventsRepo,
    tenant_id: UUID,
    event_ids: list[UUID],
) -> int:
    """Enqueue one T6 think_trigger_queue row per fresh event, up to
    `T6_ENQUEUE_PER_KIND_LIMIT` per phase-event kind. Marks each
    enqueued event processed_at=now() in the same transaction so a
    crash mid-loop doesn't re-enqueue duplicates on the next sweep.

    Returns the number of T6 triggers enqueued.
    """
    if not event_ids:
        return 0
    # Re-read events so we have the kind + neighborhood_id +
    # named_signature available for the T6 payload.
    rows = await conn.fetch(
        """
        SELECT id, kind, neighborhood_id,
               predecessor_neighborhood_ids,
               sibling_neighborhood_ids,
               member_model_ids, magnitude, named_signature
        FROM topology_events
        WHERE id = ANY($1::uuid[]) AND processed_at IS NULL
        """,
        event_ids,
    )
    per_kind_count: dict[str, int] = {}
    enqueued = 0
    for r in rows:
        kind = r["kind"]
        if per_kind_count.get(kind, 0) >= T6_ENQUEUE_PER_KIND_LIMIT:
            # Mark over-the-cap events processed so they don't
            # accumulate. The CEO view still shows them in
            # `topology_events` — they just don't trigger a Think run.
            await events_repo.mark_processed(conn, event_id=r["id"])
            continue
        payload = {
            "topology_event_id": str(r["id"]),
            "topology_event_kind": kind,
            "neighborhood_id": (
                str(r["neighborhood_id"])
                if r["neighborhood_id"] else None
            ),
            "predecessor_neighborhood_ids": [
                str(x) for x in (r["predecessor_neighborhood_ids"] or [])
            ],
            "sibling_neighborhood_ids": [
                str(x) for x in (r["sibling_neighborhood_ids"] or [])
            ],
            "member_model_ids": [
                str(x) for x in (r["member_model_ids"] or [])
            ],
            "magnitude": r["magnitude"],
            "named_signature": r["named_signature"],
            "seed_natural_text": _seed_text_for_event(
                kind, r["named_signature"], r["magnitude"],
            ),
        }
        await conn.execute(
            """
            INSERT INTO think_trigger_queue
              (id, tenant_id, trigger_kind, trigger_subkind, payload)
            VALUES ($1, $2, 'T6', $3, $4::jsonb)
            """,
            uuid7(),
            tenant_id,
            kind,
            json.dumps(payload, default=str),
        )
        await events_repo.mark_processed(conn, event_id=r["id"])
        per_kind_count[kind] = per_kind_count.get(kind, 0) + 1
        enqueued += 1
    return enqueued


def _seed_text_for_event(
    kind: str, signature: str | None, magnitude: float | None,
) -> str:
    """Build a short seed_natural_text for the T6 trigger so the
    embedding-based pathways have something concrete to work with.
    Pulled from the event taxonomy + heuristic name."""
    label = signature or "unnamed neighborhood"
    if kind == "emergence":
        return f"A new neighborhood has emerged: {label}."
    if kind == "dissolution":
        return f"The neighborhood {label} has dissolved."
    if kind == "split":
        return f"The neighborhood {label} has split into multiple sub-clusters."
    if kind == "merge":
        return f"Multiple neighborhoods have merged into {label}."
    if kind == "drift":
        return (
            f"The composition of {label} has drifted significantly "
            f"(magnitude {magnitude:.2f})."
            if magnitude is not None
            else f"The composition of {label} has drifted significantly."
        )
    return f"Topology event: {kind} for {label}."


async def run_forever(
    pool: asyncpg.Pool,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
) -> None:
    _log.info(
        "neighborhood_detector started",
        extra={"interval_s": interval_s},
    )
    while True:
        try:
            await run_once(pool)
        except Exception:  # noqa: BLE001
            _log.exception("neighborhood_detector sweep crashed")
        await asyncio.sleep(interval_s)


__all__ = [
    "run_once",
    "run_forever",
    "DEFAULT_INTERVAL_S",
]
