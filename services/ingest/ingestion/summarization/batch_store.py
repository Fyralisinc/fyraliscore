"""Postgres queue helpers for Batch API document summarization."""
from __future__ import annotations

from uuid import UUID

import asyncpg

from services.ingest.ingestion.summarization.models import SummarizationEnvelope


def batch_custom_id(observation_id: UUID) -> str:
    return f"summarization:{observation_id}"


async def enqueue_batch_item(
    *,
    pool: asyncpg.Pool,
    env: SummarizationEnvelope,
) -> bool:
    """Persist a backfill summarization request for the batch worker.

    Returns True when a new row was queued. Existing rows are left in place so
    Kafka redelivery does not create duplicate Batch API work.
    """
    row = await pool.fetchrow(
        """
        INSERT INTO summarization_batch_items (
            tenant_id, source, observation_id, raw_s3_key, ingress_kind,
            custom_id, status
        ) VALUES (
            $1, $2, $3, $4, $5, $6, 'queued'
        )
        ON CONFLICT (tenant_id, observation_id) DO NOTHING
        RETURNING id
        """,
        env.tenant_id,
        env.source,
        env.observation_id,
        env.raw_s3_key,
        env.ingress_kind,
        batch_custom_id(env.observation_id),
    )
    return row is not None


async def mark_batch_item_failed(
    *,
    pool: asyncpg.Pool,
    item_id: UUID,
    error: str,
) -> None:
    await pool.execute(
        """
        UPDATE summarization_batch_items
           SET status = 'failed',
               last_error = $2,
               completed_at = now()
         WHERE id = $1
        """,
        item_id,
        error[:1000],
    )


__all__ = [
    "batch_custom_id",
    "enqueue_batch_item",
    "mark_batch_item_failed",
]
