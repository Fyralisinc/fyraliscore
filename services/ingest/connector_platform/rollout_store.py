"""Durable rollout revisions, cohorts, propagation audit, and rollback."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import uuid4

from services.ingest.connector_runtime.rollout import (
    RolloutMetrics,
    RolloutRevision,
    RolloutStage,
    RolloutThresholds,
)


def _thresholds(value: Mapping[str, Any] | None) -> RolloutThresholds:
    data = dict(value or {})
    return RolloutThresholds(
        minimum_executions=int(data.get("minimum_executions", 100)),
        maximum_error_rate=float(data.get("maximum_error_rate", 0.02)),
        maximum_p95_ms=float(data.get("maximum_p95_ms", 30_000)),
        maximum_lifecycle_failures=int(data.get("maximum_lifecycle_failures", 0)),
        maximum_dlq_rate=float(data.get("maximum_dlq_rate", 0.001)),
    )


def _threshold_values(value: RolloutThresholds) -> dict[str, int | float]:
    return {
        "minimum_executions": value.minimum_executions,
        "maximum_error_rate": value.maximum_error_rate,
        "maximum_p95_ms": value.maximum_p95_ms,
        "maximum_lifecycle_failures": value.maximum_lifecycle_failures,
        "maximum_dlq_rate": value.maximum_dlq_rate,
    }


def _revision(row: Any) -> RolloutRevision:
    cohort = dict(row["cohort"] or {})
    return RolloutRevision(
        revision=int(row["revision"]),
        policy=dict(row["policy"]),
        stage=RolloutStage(cohort.get("stage", "full")),
        tenant_cohort=tuple(str(item) for item in cohort.get("tenant_ids", ())),
        thresholds=_thresholds(row["rollback_thresholds"]),
    )


class PostgresRolloutRepository:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def load_active(self) -> RolloutRevision | None:
        row = await self._pool.fetchrow(
            """
            SELECT revision, policy, cohort, rollback_thresholds
              FROM source_connector_routing_revisions
             WHERE status = 'active'
            """
        )
        return _revision(row) if row is not None else None

    async def read_metrics(self, revision: RolloutRevision) -> RolloutMetrics:
        row = await self._pool.fetchrow(
            """
            SELECT count(*) FILTER (
                       WHERE event_type = 'execution'
                         AND implementation = 'connector'
                   ) AS executions,
                   count(*) FILTER (
                       WHERE event_type = 'execution'
                         AND implementation = 'connector'
                         AND outcome = 'failed'
                   ) AS failures,
                   COALESCE(percentile_cont(0.95) WITHIN GROUP (
                       ORDER BY duration_ms
                   ) FILTER (
                       WHERE event_type = 'duration'
                         AND implementation = 'connector'
                   ), 0) AS connector_p95_ms,
                   count(*) FILTER (
                       WHERE event_type = 'lifecycle' AND outcome = 'failed'
                   ) AS lifecycle_failures,
                   count(*) FILTER (
                       WHERE event_type = 'dlq'
                         AND implementation = 'connector'
                   )::double precision / GREATEST(
                       count(*) FILTER (
                           WHERE event_type = 'execution'
                             AND implementation = 'connector'
                       ), 1
                   ) AS connector_dlq_rate
              FROM source_connector_rollout_events
             WHERE revision = $1
               AND occurred_at >= now() - interval '30 minutes'
            """,
            revision.revision,
        )
        return RolloutMetrics(
            executions=int(row["executions"]),
            failures=int(row["failures"]),
            connector_p95_ms=float(row["connector_p95_ms"]),
            lifecycle_failures=int(row["lifecycle_failures"]),
            connector_dlq_rate=float(row["connector_dlq_rate"]),
        )

    async def prune_evidence(self, *, retention_hours: int = 24) -> None:
        await self._pool.execute(
            """
            DELETE FROM source_connector_rollout_events
             WHERE occurred_at < now() - make_interval(hours => $1)
            """,
            retention_hours,
        )

    async def create_staged(
        self,
        *,
        revision: int,
        policy: Mapping[str, object],
        stage: RolloutStage,
        tenant_cohort: Sequence[str],
        thresholds: RolloutThresholds,
        actor: str,
    ) -> RolloutRevision:
        await self._pool.execute(
            """
            INSERT INTO source_connector_routing_revisions (
              revision, policy, status, cohort, rollback_thresholds, created_by
            ) VALUES ($1, $2::jsonb, 'staged', $3::jsonb, $4::jsonb, $5)
            """,
            revision,
            json.dumps(dict(policy)),
            json.dumps({"stage": stage.value, "tenant_ids": list(tenant_cohort)}),
            json.dumps(_threshold_values(thresholds)),
            actor,
        )
        await self.audit(
            revision,
            action="staged",
            actor=actor,
            reason=f"staged {stage.value} rollout",
        )
        return RolloutRevision(
            revision,
            policy,
            stage,
            tuple(tenant_cohort),
            thresholds,
        )

    async def activate(self, revision: int, *, actor: str, reason: str) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                previous = await connection.fetchval(
                    """
                    SELECT revision
                      FROM source_connector_routing_revisions
                     WHERE status = 'active'
                     FOR UPDATE
                    """
                )
                if previous is not None:
                    await connection.execute(
                        """
                        UPDATE source_connector_routing_revisions
                           SET status = 'rolled_back', superseded_by = $2
                         WHERE revision = $1
                        """,
                        previous,
                        revision,
                    )
                status = await connection.execute(
                    """
                    UPDATE source_connector_routing_revisions
                       SET status = 'active', activated_at = now()
                     WHERE revision = $1 AND status = 'staged'
                    """,
                    revision,
                )
                if not status.endswith(" 1"):
                    raise ValueError("rollout revision is not staged")
                await self._audit_with(
                    connection,
                    revision,
                    action="activated",
                    actor=actor,
                    reason=reason,
                )

    async def rollback_to_previous(
        self,
        failed_revision: int,
        *,
        actor: str,
        reason: str,
        metrics: Mapping[str, object],
    ) -> RolloutRevision:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                current = await connection.fetchrow(
                    """
                    SELECT revision
                      FROM source_connector_routing_revisions
                     WHERE status = 'active'
                     FOR UPDATE
                    """
                )
                if current is None or int(current["revision"]) != failed_revision:
                    raise RuntimeError("active rollout changed before rollback")
                previous = await connection.fetchrow(
                    """
                    SELECT policy, cohort, rollback_thresholds
                      FROM source_connector_routing_revisions
                     WHERE revision < $1
                     ORDER BY revision DESC
                     LIMIT 1
                    """,
                    failed_revision,
                )
                if previous is None:
                    raise RuntimeError("no previous connector artifact revision exists")
                next_revision = int(
                    await connection.fetchval(
                        "SELECT COALESCE(max(revision), 0) + 1 FROM source_connector_routing_revisions"
                    )
                )
                policy = {"revision": next_revision, "global": "connector"}
                cohort = dict(previous["cohort"] or {})
                cohort["stage"] = "full"
                cohort["tenant_ids"] = []
                await connection.execute(
                    """
                    UPDATE source_connector_routing_revisions
                       SET status = 'rolled_back', superseded_by = $2
                     WHERE revision = $1
                    """,
                    failed_revision,
                    next_revision,
                )
                await connection.execute(
                    """
                    INSERT INTO source_connector_routing_revisions (
                      revision, policy, status, cohort, rollback_thresholds,
                      created_by, activated_at
                    ) VALUES (
                      $1, $2::jsonb, 'active', $3::jsonb, '{}'::jsonb, $4, now()
                    )
                    """,
                    next_revision,
                    json.dumps(policy),
                    json.dumps(cohort),
                    actor,
                )
                await self._audit_with(
                    connection,
                    failed_revision,
                    action="artifact_rollback",
                    actor=actor,
                    reason=reason,
                    metrics=metrics,
                )
        return RolloutRevision(
            revision=next_revision,
            policy=policy,
            stage=RolloutStage.FULL,
            thresholds=RolloutThresholds(minimum_executions=0),
        )

    async def audit(
        self,
        revision: int,
        *,
        action: str,
        actor: str,
        reason: str,
        metrics: Mapping[str, object] | None = None,
    ) -> None:
        await self._audit_with(
            self._pool,
            revision,
            action=action,
            actor=actor,
            reason=reason,
            metrics=metrics,
        )

    @staticmethod
    async def _audit_with(
        executor: Any,
        revision: int,
        *,
        action: str,
        actor: str,
        reason: str,
        metrics: Mapping[str, object] | None = None,
    ) -> None:
        await executor.execute(
            """
            INSERT INTO source_connector_rollout_audit (
              id, revision, action, actor, reason, metrics_snapshot
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """,
            uuid4(),
            revision,
            action,
            actor,
            reason,
            json.dumps(dict(metrics or {})),
        )


__all__ = ["PostgresRolloutRepository"]
