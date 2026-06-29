"""Customer-side BYOC product-health snapshot collector.

The collector runs inside the customer data plane and emits only aggregate
metadata that the BYOC control plane is allowed to store: counters, bounded
status codes, source names, and timestamps. It never selects raw observation
payloads, prompts, logs, vector values, model contents, credentials, URLs, or
error summaries.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from services.platform.runtime.byoc_product_health import (
    ByocProductHealthIssue,
    ByocProductHealthSnapshotPayload,
    ByocProductModelHealth,
    ByocProductPipelineHealth,
    ByocProductSourceHealth,
    ByocProductThinkHealth,
    ByocProductVectorHealth,
    ProductHealthStatus,
    product_health_snapshot_payload,
)


_SAFE_CODE_RE = re.compile(r"[^A-Za-z0-9_.:-]+")
_SOURCE_LIMIT = 50
_FORBIDDEN_SOURCE_FRAGMENTS = (
    "://",
    "bearer ",
    "password=",
    "postgresql://",
    "secret=",
    "token=",
)


class ProductHealthDatabase(Protocol):
    async def fetchrow(self, query: str, *args: Any) -> Any: ...

    async def fetch(self, query: str, *args: Any) -> Any: ...


@dataclass(frozen=True)
class ByocProductHealthCollectorIdentity:
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    tenant_id: str | UUID | None = None


async def collect_product_health_snapshot(
    db: ProductHealthDatabase,
    *,
    identity: ByocProductHealthCollectorIdentity,
    nonce: str,
    collected_at: datetime | None = None,
) -> ByocProductHealthSnapshotPayload:
    """Collect a metadata-only product-health snapshot from a Fyralis database."""

    tenant_id = _normalize_tenant_id(identity.tenant_id)
    registry = _TableRegistry(db)

    sources = await _collect_sources(db, registry=registry, tenant_id=tenant_id)
    pipeline = await _collect_pipeline(
        db,
        registry=registry,
        tenant_id=tenant_id,
        sources=sources,
    )
    think = await _collect_think(db, registry=registry, tenant_id=tenant_id)
    models = await _collect_models(db, registry=registry, tenant_id=tenant_id)
    vector_index = await _collect_vector_index(
        db,
        registry=registry,
        tenant_id=tenant_id,
    )
    issues = _collect_issues(
        sources=sources,
        pipeline=pipeline,
        think=think,
        models=models,
        vector_index=vector_index,
        collected_at=collected_at,
    )
    overall_status = _overall_status(
        sources=sources,
        pipeline=pipeline,
        think=think,
        models=models,
        vector_index=vector_index,
    )

    return product_health_snapshot_payload(
        deployment_id=identity.deployment_id,
        customer_id=identity.customer_id,
        agent_id=identity.agent_id,
        agent_version=identity.agent_version,
        artifact_revision=identity.artifact_revision,
        overall_status=overall_status,
        collected_at=collected_at or datetime.now(UTC),
        nonce=nonce,
        sources=sources,
        pipeline=pipeline,
        think=think,
        models=models,
        vector_index=vector_index,
        issues=issues,
    )


class _TableRegistry:
    def __init__(self, db: ProductHealthDatabase) -> None:
        self._db = db
        self._cache: dict[str, bool] = {}

    async def exists(self, table_name: str) -> bool:
        if table_name not in self._cache:
            row = await self._db.fetchrow(
                "SELECT to_regclass($1) IS NOT NULL AS exists",
                table_name,
            )
            self._cache[table_name] = bool(_row_get(row, "exists", False))
        return self._cache[table_name]


async def _collect_sources(
    db: ProductHealthDatabase,
    *,
    registry: _TableRegistry,
    tenant_id: str | None,
) -> tuple[ByocProductSourceHealth, ...]:
    aggregate: dict[str, dict[str, Any]] = {}

    if await registry.exists("observations"):
        where, args = _tenant_where(tenant_id)
        rows = await db.fetch(
            f"""
            /* byoc_product_health:observations_by_source */
            SELECT
              source_channel AS source,
              count(*)::bigint AS items_ingested_count,
              max(ingested_at) AS last_success_at
            FROM observations
            {where}
            GROUP BY source_channel
            ORDER BY count(*) DESC, source_channel
            LIMIT {_SOURCE_LIMIT}
            """,
            *args,
        )
        for row in rows:
            source = _source_code(_row_get(row, "source", "unknown"))
            stats = _source_stats(aggregate, source)
            stats["items_ingested_count"] += _as_int(
                _row_get(row, "items_ingested_count", 0)
            )
            stats["last_success_at"] = _max_datetime(
                stats.get("last_success_at"),
                _row_get(row, "last_success_at"),
            )
            stats["observed"] = True

    if await registry.exists("ingestion_failures"):
        where, args = _where(
            tenant_id=tenant_id,
            conditions=("resolved_at IS NULL",),
        )
        rows = await db.fetch(
            f"""
            /* byoc_product_health:unresolved_ingestion_failures */
            SELECT
              source,
              count(*)::bigint AS items_failed_count,
              count(*) FILTER (WHERE failure_kind = 'oauth_revoked_mid_run')::bigint
                AS auth_failure_count,
              max(last_seen_at) AS latest_failure_at
            FROM ingestion_failures
            {where}
            GROUP BY source
            ORDER BY count(*) DESC, source
            LIMIT {_SOURCE_LIMIT}
            """,
            *args,
        )
        for row in rows:
            source = _source_code(_row_get(row, "source", "unknown"))
            stats = _source_stats(aggregate, source)
            stats["items_failed_count"] += _as_int(
                _row_get(row, "items_failed_count", 0)
            )
            stats["auth_failure_count"] += _as_int(
                _row_get(row, "auth_failure_count", 0)
            )
            stats["latest_failure_at"] = _max_datetime(
                stats.get("latest_failure_at"),
                _row_get(row, "latest_failure_at"),
            )
            stats["observed"] = True

    if await registry.exists("onboarding_shards"):
        where, args = _tenant_where(tenant_id)
        rows = await db.fetch(
            f"""
            /* byoc_product_health:onboarding_shards_by_source */
            SELECT
              source,
              count(*) FILTER (WHERE state IN ('pending', 'in_progress'))::bigint
                AS queue_depth_count,
              count(*) FILTER (WHERE state = 'failed')::bigint AS failed_shard_count,
              count(*) FILTER (WHERE state = 'in_progress')::bigint
                AS active_shard_count,
              sum(observations_seen)::bigint AS observations_seen_count,
              max(last_cursor_advance) AS last_cursor_advance
            FROM onboarding_shards
            {where}
            GROUP BY source
            ORDER BY source
            LIMIT {_SOURCE_LIMIT}
            """,
            *args,
        )
        for row in rows:
            source = _source_code(_row_get(row, "source", "unknown"))
            stats = _source_stats(aggregate, source)
            stats["queue_depth_count"] += _as_int(
                _row_get(row, "queue_depth_count", 0)
            )
            stats["failed_shard_count"] += _as_int(
                _row_get(row, "failed_shard_count", 0)
            )
            stats["active_shard_count"] += _as_int(
                _row_get(row, "active_shard_count", 0)
            )
            if stats["items_ingested_count"] == 0:
                stats["items_ingested_count"] += _as_int(
                    _row_get(row, "observations_seen_count", 0)
                )
            stats["last_success_at"] = _max_datetime(
                stats.get("last_success_at"),
                _row_get(row, "last_cursor_advance"),
            )
            stats["observed"] = True

    if await registry.exists("source_onboarding_runs"):
        where, args = _tenant_where(tenant_id)
        rows = await db.fetch(
            f"""
            /* byoc_product_health:source_onboarding_runs_by_source */
            SELECT
              source,
              count(*) FILTER (WHERE status = 'pending')::bigint
                AS pending_run_count,
              count(*) FILTER (WHERE status = 'in_progress')::bigint
                AS active_run_count,
              count(*) FILTER (WHERE status = 'failed')::bigint
                AS failed_run_count,
              max(completed_at) FILTER (WHERE status = 'completed')
                AS latest_completed_at
            FROM source_onboarding_runs
            {where}
            GROUP BY source
            ORDER BY source
            LIMIT {_SOURCE_LIMIT}
            """,
            *args,
        )
        for row in rows:
            source = _source_code(_row_get(row, "source", "unknown"))
            stats = _source_stats(aggregate, source)
            stats["queue_depth_count"] += _as_int(_row_get(row, "pending_run_count", 0))
            stats["active_shard_count"] += _as_int(_row_get(row, "active_run_count", 0))
            stats["failed_shard_count"] += _as_int(_row_get(row, "failed_run_count", 0))
            stats["last_success_at"] = _max_datetime(
                stats.get("last_success_at"),
                _row_get(row, "latest_completed_at"),
            )
            stats["observed"] = True

    source_health = [
        _source_health(source, stats)
        for source, stats in sorted(
            aggregate.items(),
            key=lambda item: (-_as_int(item[1].get("items_failed_count")), item[0]),
        )
    ]
    return tuple(source_health[:_SOURCE_LIMIT])


async def _collect_pipeline(
    db: ProductHealthDatabase,
    *,
    registry: _TableRegistry,
    tenant_id: str | None,
    sources: tuple[ByocProductSourceHealth, ...],
) -> ByocProductPipelineHealth:
    observed = bool(sources)
    queue_lag_count = sum(source.queue_depth_count for source in sources)
    retry_backlog_count = sum(source.items_failed_count for source in sources)
    dead_letter_count = sum(source.items_failed_count for source in sources)
    dropped_item_count = 0

    if await registry.exists("ingestion_failures"):
        observed = True
        where, args = _where(
            tenant_id=tenant_id,
            conditions=("resolved_at IS NULL",),
        )
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:pipeline_unresolved_failures */
            SELECT
              count(*)::bigint AS unresolved_count,
              count(*) FILTER (WHERE attempt_count > 1)::bigint AS retry_count
            FROM ingestion_failures
            {where}
            """,
            *args,
        )
        dead_letter_count = _as_int(_row_get(row, "unresolved_count", dead_letter_count))
        retry_backlog_count = _as_int(_row_get(row, "retry_count", retry_backlog_count))

        where, args = _where(
            tenant_id=tenant_id,
            conditions=("resolution_kind = 'discarded'",),
        )
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:pipeline_discarded_failures */
            SELECT count(*)::bigint AS dropped_item_count
            FROM ingestion_failures
            {where}
            """,
            *args,
        )
        dropped_item_count = _as_int(_row_get(row, "dropped_item_count", 0))

    if await registry.exists("onboarding_shards"):
        observed = True
        where, args = _tenant_where(tenant_id)
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:pipeline_onboarding_queue */
            SELECT count(*) FILTER (
              WHERE state IN ('pending', 'in_progress')
            )::bigint AS queue_lag_count
            FROM onboarding_shards
            {where}
            """,
            *args,
        )
        queue_lag_count = _as_int(_row_get(row, "queue_lag_count", queue_lag_count))

    status: ProductHealthStatus
    if not observed:
        status = "unknown"
    elif dead_letter_count > 0:
        status = "action_required"
    elif queue_lag_count > 0 or retry_backlog_count > 0 or dropped_item_count > 0:
        status = "degraded"
    else:
        status = "ready"

    return ByocProductPipelineHealth(
        status=status,
        queue_lag_count=queue_lag_count,
        dead_letter_count=dead_letter_count,
        retry_backlog_count=retry_backlog_count,
        dropped_item_count=dropped_item_count,
    )


async def _collect_think(
    db: ProductHealthDatabase,
    *,
    registry: _TableRegistry,
    tenant_id: str | None,
) -> ByocProductThinkHealth:
    observed = False
    run_count = 0
    failed_run_count = 0
    queued_run_count = 0
    latest_run_at: datetime | None = None

    if await registry.exists("think_runs"):
        observed = True
        where, args = _tenant_where(tenant_id)
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:think_runs */
            SELECT
              count(*)::bigint AS run_count,
              count(*) FILTER (WHERE status = 'failed')::bigint AS failed_run_count,
              max(started_at) AS latest_run_at
            FROM think_runs
            {where}
            """,
            *args,
        )
        run_count = _as_int(_row_get(row, "run_count", 0))
        failed_run_count = _as_int(_row_get(row, "failed_run_count", 0))
        latest_run_at = _row_get(row, "latest_run_at")

    if await registry.exists("think_trigger_queue"):
        observed = True
        where, args = _where(
            tenant_id=tenant_id,
            conditions=("completed_at IS NULL",),
        )
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:think_trigger_queue */
            SELECT count(*)::bigint AS queued_run_count
            FROM think_trigger_queue
            {where}
            """,
            *args,
        )
        queued_run_count = _as_int(_row_get(row, "queued_run_count", 0))

    if not observed:
        status: ProductHealthStatus = "unknown"
        breaker_status = "unknown"
    elif failed_run_count > 0:
        status = "degraded"
        breaker_status = "unknown"
    elif queued_run_count > 0:
        status = "degraded"
        breaker_status = "closed"
    else:
        status = "ready"
        breaker_status = "closed"

    return ByocProductThinkHealth(
        status=status,
        run_count=run_count,
        failed_run_count=failed_run_count,
        queued_run_count=queued_run_count,
        latest_run_at=latest_run_at,
        breaker_status=breaker_status,
    )


async def _collect_models(
    db: ProductHealthDatabase,
    *,
    registry: _TableRegistry,
    tenant_id: str | None,
) -> ByocProductModelHealth:
    if not await registry.exists("models"):
        return ByocProductModelHealth(
            status="unknown",
            model_count=0,
            model_build_count=0,
            failed_build_count=0,
            model_relation_count=0,
            orphan_model_count=0,
            stale_relation_count=0,
            latest_build_at=None,
            graph_status="unknown",
        )

    where, args = _tenant_where(tenant_id, alias="m")
    row = await db.fetchrow(
        f"""
        /* byoc_product_health:models */
        SELECT count(*)::bigint AS model_count
        FROM models m
        {where}
        """,
        *args,
    )
    model_count = _as_int(_row_get(row, "model_count", 0))

    model_relation_count = 0
    latest_build_at: datetime | None = None
    if await registry.exists("model_composition_members"):
        where, args = _tenant_where(tenant_id)
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:model_composition_members */
            SELECT
              count(*)::bigint AS model_relation_count,
              max(created_at) AS latest_build_at
            FROM model_composition_members
            {where}
            """,
            *args,
        )
        model_relation_count = _as_int(_row_get(row, "model_relation_count", 0))
        latest_build_at = _row_get(row, "latest_build_at")

    orphan_model_count = 0
    graph_status: ProductHealthStatus = "ready"
    if await registry.exists("model_belief_addresses"):
        if tenant_id is None:
            where, args = "WHERE b.model_id IS NULL", ()
        else:
            where, args = (
                "WHERE m.tenant_id = $1::uuid AND b.model_id IS NULL",
                (tenant_id,),
            )
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:model_belief_orphans */
            SELECT count(*)::bigint AS orphan_model_count
            FROM models m
            LEFT JOIN model_belief_addresses b
              ON b.model_id = m.id
            {where}
            """,
            *args,
        )
        orphan_model_count = _as_int(_row_get(row, "orphan_model_count", 0))
        graph_status = "degraded" if orphan_model_count > 0 else "ready"
        where, args = _tenant_where(tenant_id, alias="b")
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:model_belief_latest_update */
            SELECT max(updated_at) AS latest_build_at
            FROM model_belief_addresses b
            {where}
            """,
            *args,
        )
        latest_build_at = _max_datetime(latest_build_at, _row_get(row, "latest_build_at"))

    status: ProductHealthStatus = "degraded" if orphan_model_count > 0 else "ready"
    return ByocProductModelHealth(
        status=status,
        model_count=model_count,
        model_build_count=model_count,
        failed_build_count=0,
        model_relation_count=model_relation_count,
        orphan_model_count=orphan_model_count,
        stale_relation_count=0,
        latest_build_at=latest_build_at,
        graph_status=graph_status,
    )


async def _collect_vector_index(
    db: ProductHealthDatabase,
    *,
    registry: _TableRegistry,
    tenant_id: str | None,
) -> ByocProductVectorHealth:
    observed = False
    vector_count = 0
    backlog_count = 0
    latest_job_at: datetime | None = None

    if await registry.exists("observations"):
        observed = True
        where, args = _tenant_where(tenant_id)
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:observation_vectors */
            SELECT
              count(*) FILTER (WHERE embedding IS NOT NULL)::bigint AS vector_count,
              count(*) FILTER (WHERE embedding_pending = TRUE)::bigint AS backlog_count,
              max(ingested_at) FILTER (
                WHERE embedding IS NOT NULL OR embedding_pending = TRUE
              ) AS latest_job_at
            FROM observations
            {where}
            """,
            *args,
        )
        vector_count += _as_int(_row_get(row, "vector_count", 0))
        backlog_count += _as_int(_row_get(row, "backlog_count", 0))
        latest_job_at = _max_datetime(latest_job_at, _row_get(row, "latest_job_at"))

    if await registry.exists("models"):
        observed = True
        where, args = _tenant_where(tenant_id)
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:model_vectors */
            SELECT count(*) FILTER (WHERE embedding IS NOT NULL)::bigint AS vector_count
            FROM models
            {where}
            """,
            *args,
        )
        vector_count += _as_int(_row_get(row, "vector_count", 0))

    if await registry.exists("code_embeddings"):
        observed = True
        where, args = _tenant_where(tenant_id)
        row = await db.fetchrow(
            f"""
            /* byoc_product_health:code_vectors */
            SELECT
              count(*) FILTER (WHERE embedding IS NOT NULL)::bigint AS vector_count,
              count(*) FILTER (WHERE embedding_pending = TRUE)::bigint AS backlog_count
            FROM code_embeddings
            {where}
            """,
            *args,
        )
        vector_count += _as_int(_row_get(row, "vector_count", 0))
        backlog_count += _as_int(_row_get(row, "backlog_count", 0))

    status: ProductHealthStatus
    if not observed:
        status = "unknown"
        retrieval_status: ProductHealthStatus = "unknown"
    elif backlog_count > 0:
        status = "degraded"
        retrieval_status = "ready" if vector_count > 0 else "degraded"
    else:
        status = "ready"
        retrieval_status = "ready"

    return ByocProductVectorHealth(
        status=status,
        vector_count=vector_count,
        backlog_count=backlog_count,
        failed_job_count=0,
        latest_job_at=latest_job_at,
        retrieval_status=retrieval_status,
    )


def _collect_issues(
    *,
    sources: tuple[ByocProductSourceHealth, ...],
    pipeline: ByocProductPipelineHealth,
    think: ByocProductThinkHealth,
    models: ByocProductModelHealth,
    vector_index: ByocProductVectorHealth,
    collected_at: datetime | None,
) -> tuple[ByocProductHealthIssue, ...]:
    observed_at = collected_at or datetime.now(UTC)
    issues: list[ByocProductHealthIssue] = []

    source_failure_count = sum(source.items_failed_count for source in sources)
    auth_failure_count = sum(
        1 for source in sources if source.auth_status == "action_required"
    )
    if source_failure_count > 0:
        issues.append(
            _issue(
                code="source_ingest_failures",
                severity="warning",
                component="source_ingestion",
                observed_count=source_failure_count,
                observed_at=observed_at,
            )
        )
    if auth_failure_count > 0:
        issues.append(
            _issue(
                code="source_auth_action_required",
                severity="warning",
                component="source_auth",
                observed_count=auth_failure_count,
                observed_at=observed_at,
            )
        )

    if pipeline.dead_letter_count > 0:
        issues.append(
            _issue(
                code="pipeline_dead_letters",
                severity="warning",
                component="pipeline",
                observed_count=pipeline.dead_letter_count,
                observed_at=observed_at,
            )
        )
    if think.failed_run_count > 0:
        issues.append(
            _issue(
                code="think_run_failures",
                severity="warning",
                component="think",
                observed_count=think.failed_run_count,
                observed_at=observed_at,
            )
        )
    if models.orphan_model_count > 0:
        issues.append(
            _issue(
                code="model_orphans_detected",
                severity="info",
                component="models",
                observed_count=models.orphan_model_count,
                observed_at=observed_at,
            )
        )
    if vector_index.backlog_count > 0:
        issues.append(
            _issue(
                code="vector_backlog_pending",
                severity="info",
                component="vector_index",
                observed_count=vector_index.backlog_count,
                observed_at=observed_at,
            )
        )

    return tuple(issues[:50])


def _source_health(source: str, stats: dict[str, Any]) -> ByocProductSourceHealth:
    items_ingested_count = _as_int(stats.get("items_ingested_count", 0))
    items_failed_count = _as_int(stats.get("items_failed_count", 0))
    queue_depth_count = _as_int(stats.get("queue_depth_count", 0))
    auth_failure_count = _as_int(stats.get("auth_failure_count", 0))
    failed_shard_count = _as_int(stats.get("failed_shard_count", 0))
    active_shard_count = _as_int(stats.get("active_shard_count", 0))

    if auth_failure_count > 0:
        auth_status = "action_required"
    elif stats.get("observed"):
        auth_status = "ready"
    else:
        auth_status = "unknown"

    if failed_shard_count > 0:
        backfill_status = "blocked"
    elif active_shard_count > 0 or queue_depth_count > 0:
        backfill_status = "running"
    elif stats.get("observed"):
        backfill_status = "idle"
    else:
        backfill_status = "unknown"

    if auth_status == "action_required" or failed_shard_count > 0:
        status = "failing"
    elif items_failed_count > 0 or queue_depth_count > 0:
        status = "degraded"
    elif stats.get("observed"):
        status = "ready"
    else:
        status = "unknown"

    lag_seconds = None
    last_success_at = stats.get("last_success_at")
    latest_failure_at = stats.get("latest_failure_at")
    if isinstance(last_success_at, datetime) and isinstance(latest_failure_at, datetime):
        if latest_failure_at > last_success_at:
            lag_seconds = int((latest_failure_at - last_success_at).total_seconds())

    return ByocProductSourceHealth(
        source=source,
        status=status,
        auth_status=auth_status,
        backfill_status=backfill_status,
        items_ingested_count=items_ingested_count,
        items_failed_count=items_failed_count,
        queue_depth_count=queue_depth_count,
        lag_seconds=lag_seconds,
        last_success_at=last_success_at,
    )


def _overall_status(
    *,
    sources: tuple[ByocProductSourceHealth, ...],
    pipeline: ByocProductPipelineHealth,
    think: ByocProductThinkHealth,
    models: ByocProductModelHealth,
    vector_index: ByocProductVectorHealth,
) -> ProductHealthStatus:
    component_statuses = (
        pipeline.status,
        think.status,
        models.status,
        vector_index.status,
        *(source.status for source in sources),
    )
    if not sources and all(status == "unknown" for status in component_statuses):
        return "unknown"
    if any(status in {"action_required", "failing"} for status in component_statuses):
        return "action_required"
    if any(status == "degraded" for status in component_statuses):
        return "degraded"
    return "ready"


def _source_stats(aggregate: dict[str, dict[str, Any]], source: str) -> dict[str, Any]:
    if source not in aggregate:
        aggregate[source] = {
            "items_ingested_count": 0,
            "items_failed_count": 0,
            "queue_depth_count": 0,
            "auth_failure_count": 0,
            "failed_shard_count": 0,
            "active_shard_count": 0,
            "observed": False,
            "last_success_at": None,
            "latest_failure_at": None,
        }
    return aggregate[source]


def _issue(
    *,
    code: str,
    severity: str,
    component: str,
    observed_count: int,
    observed_at: datetime,
) -> ByocProductHealthIssue:
    return ByocProductHealthIssue(
        code=code,
        severity=severity,
        component=component,
        observed_count=max(1, observed_count),
        first_observed_at=observed_at,
        latest_observed_at=observed_at,
    )


def _tenant_where(
    tenant_id: str | None,
    *,
    alias: str | None = None,
) -> tuple[str, tuple[Any, ...]]:
    if tenant_id is None:
        return "", ()
    prefix = f"{alias}." if alias else ""
    return f"WHERE {prefix}tenant_id = $1::uuid", (tenant_id,)


def _where(
    *,
    tenant_id: str | None,
    conditions: tuple[str, ...],
) -> tuple[str, tuple[Any, ...]]:
    parts = list(conditions)
    args: tuple[Any, ...] = ()
    if tenant_id is not None:
        parts.append("tenant_id = $1::uuid")
        args = (tenant_id,)
    return "WHERE " + " AND ".join(parts), args


def _source_code(raw: Any) -> str:
    value = str(raw or "unknown").strip().lower()
    if any(fragment in value for fragment in _FORBIDDEN_SOURCE_FRAGMENTS):
        return "unknown"
    value = _SAFE_CODE_RE.sub("_", value).strip("_.:-")
    if not value:
        return "unknown"
    return value[:100]


def _normalize_tenant_id(raw: str | UUID | None) -> str | None:
    if raw is None:
        return None
    return str(UUID(str(raw)))


def _as_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return default


def _max_datetime(left: Any, right: Any) -> datetime | None:
    left_dt = left if isinstance(left, datetime) else None
    right_dt = right if isinstance(right, datetime) else None
    if left_dt is None:
        return right_dt
    if right_dt is None:
        return left_dt
    return max(left_dt, right_dt)


__all__ = [
    "ByocProductHealthCollectorIdentity",
    "ProductHealthDatabase",
    "collect_product_health_snapshot",
]
