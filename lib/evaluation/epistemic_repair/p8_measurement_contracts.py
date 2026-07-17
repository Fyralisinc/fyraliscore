"""Fail-closed measurement manifests for the remaining P8 scale evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class QueueFamilyMeasure:
    family: str
    table: str
    tenant_column: str | None
    pending_predicate: str
    terminal_failure_predicate: str | None


QUEUE_FAMILIES = (
    QueueFamilyMeasure("think_trigger", "think_trigger_queue", "tenant_id", "completed_at IS NULL", None),
    QueueFamilyMeasure("model_reevaluation", "model_reeval_queue", "tenant_id", "processed_at IS NULL", None),
    QueueFamilyMeasure("entity_review", "entity_review_queue", "tenant_id", "resolved_at IS NULL", None),
    QueueFamilyMeasure("post_commit", "pending_post_commit_actions", "tenant_id", "processed_at IS NULL AND dead_lettered_at IS NULL", "dead_lettered_at IS NOT NULL"),
    QueueFamilyMeasure("topology_dirty", "topo_dirty_queue", "tenant_id", "processed_at IS NULL", None),
    QueueFamilyMeasure("summarization_items", "summarization_batch_items", "tenant_id", "status IN ('queued','submitting','submitted')", "status = 'failed'"),
    QueueFamilyMeasure("projection_refresh", "projection_refresh_jobs", "tenant_id", "status IN ('pending','leased')", "status = 'dead_letter'"),
)


async def validate_queue_manifest(conn: Any) -> dict[str, object]:
    rows = await conn.fetch(
        """SELECT table_name, column_name FROM information_schema.columns
           WHERE table_schema='public' AND table_name=ANY($1::text[])""",
        [item.table for item in QUEUE_FAMILIES],
    )
    columns: dict[str, set[str]] = {}
    for row in rows:
        columns.setdefault(row["table_name"], set()).add(row["column_name"])
    missing = [item.table for item in QUEUE_FAMILIES if item.table not in columns]
    missing_tenant = [
        item.table for item in QUEUE_FAMILIES
        if item.tenant_column and item.tenant_column not in columns.get(item.table, set())
    ]
    return {
        "manifest_complete": not missing and not missing_tenant,
        "families": len(QUEUE_FAMILIES),
        "missing_tables": missing,
        "missing_tenant_columns": missing_tenant,
    }


def exact_token_receipt_is_usable(row: dict[str, object]) -> bool:
    """Only provider-reported usage can satisfy the exact-token P8 gate."""

    return bool(
        row.get("usage_exactness") == "reported"
        and isinstance(row.get("input_tokens"), int)
        and int(row["input_tokens"]) > 0
        and isinstance(row.get("output_tokens"), int)
        and int(row["output_tokens"]) >= 0
        and row.get("physical_attempt_id")
        and row.get("logical_call_id")
    )


def projection_refresh_measure_is_usable(row: dict[str, object]) -> bool:
    """Require real enqueued and terminal jobs plus unique coalescing keys."""

    enqueued = int(row.get("enqueued_jobs", 0))
    processed = int(row.get("processed_jobs", 0))
    dead = int(row.get("dead_letter_jobs", 0))
    unique = int(row.get("unique_subject_family_versions", 0))
    return bool(
        enqueued > 0 and unique > 0 and processed + dead == enqueued
        and dead == 0 and enqueued / unique <= 1.10
    )


def queue_curve_is_usable(samples: dict[str, list[int]]) -> bool:
    """Every registered family must have a nonempty barrier-aligned curve."""

    expected = {item.family for item in QUEUE_FAMILIES}
    return set(samples) == expected and all(
        values and all(value >= 0 for value in values)
        for values in samples.values()
    )
