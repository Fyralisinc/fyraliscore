"""Durable, bounded-cardinality rollout evidence writer."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any
from uuid import uuid4

log = logging.getLogger(__name__)
RevisionSupplier = Callable[[], int | None]


class PostgresRolloutEvidenceSink:
    """Translate synchronous contract metrics into durable rollout events.

    Host metric ports are synchronous by contract. The sink therefore schedules
    small append-only writes on the active event loop and exposes ``flush`` for
    orderly worker shutdown. Rows never contain tenant, installation, or
    execution identifiers, keeping the evidence stream bounded in cardinality.
    """

    def __init__(self, pool: Any, revision: RevisionSupplier) -> None:
        self._pool = pool
        self._revision = revision
        self._tasks: set[asyncio.Task[None]] = set()

    def _schedule(self, **event: object) -> None:
        revision = self._revision()
        if revision is None:
            return
        try:
            task = asyncio.get_running_loop().create_task(
                self._write(revision=revision, **event)
            )
        except RuntimeError:
            log.warning("source_connector.rollout_evidence.no_event_loop")
            return
        self._tasks.add(task)
        task.add_done_callback(self._completed)

    def _completed(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        try:
            task.result()
        except Exception:
            log.exception("source_connector.rollout_evidence.write_failed")

    async def _write(
        self,
        *,
        revision: int,
        event_type: str,
        connector_id: str,
        capability: str,
        implementation: str | None = None,
        outcome: str | None = None,
        duration_ms: float | None = None,
    ) -> None:
        # The contract-only schema deliberately has no implementation
        # dimension. Keep the argument for host-metric compatibility, but
        # persist the sole valid owner.
        implementation = "connector"
        await self._pool.execute(
            """
            INSERT INTO source_connector_rollout_events (
              id, revision, event_type, connector_id, capability,
              implementation, outcome, duration_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            uuid4(),
            revision,
            event_type,
            connector_id,
            capability,
            implementation,
            outcome,
            duration_ms,
        )

    def increment(
        self,
        name: str,
        value: int,
        attributes: tuple[tuple[str, str], ...],
    ) -> None:
        if name != "source_connector.rollout.execution":
            return
        values = dict(attributes)
        for _ in range(max(0, value)):
            self._schedule(
                event_type="execution",
                connector_id=values.get("connector_id", "unknown"),
                capability=values.get("capability", "unknown"),
                implementation=values.get("implementation"),
                outcome=values.get("outcome"),
            )

    def observe(
        self,
        name: str,
        value: float,
        attributes: tuple[tuple[str, str], ...],
    ) -> None:
        if name != "source_connector.rollout.duration_ms":
            return
        values = dict(attributes)
        self._schedule(
            event_type="duration",
            connector_id=values.get("connector_id", "unknown"),
            capability=values.get("capability", "unknown"),
            implementation=values.get("implementation"),
            outcome=values.get("outcome"),
            duration_ms=value,
        )

    def record_lifecycle(self, *, connector_id: str, outcome: str) -> None:
        self._schedule(
            event_type="lifecycle",
            connector_id=connector_id,
            capability="lifecycle.reconcile",
            outcome=outcome,
        )

    def record_dlq(
        self,
        *,
        connector_id: str,
        capability: str,
        implementation: str,
    ) -> None:
        self._schedule(
            event_type="dlq",
            connector_id=connector_id,
            capability=capability,
            implementation=implementation,
        )

    async def flush(self) -> None:
        while self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)

    async def prune(self, *, retention_hours: int = 24) -> None:
        await self._pool.execute(
            """
            DELETE FROM source_connector_rollout_events
             WHERE occurred_at < now() - make_interval(hours => $1)
            """,
            retention_hours,
        )


__all__ = ["PostgresRolloutEvidenceSink", "RevisionSupplier"]
