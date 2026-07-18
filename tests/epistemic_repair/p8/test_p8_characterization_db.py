from __future__ import annotations

import os

import asyncpg
import pytest

from services.evaluation.epistemic_repair.p8_characterization_db import run_db_characterization


pytestmark = pytest.mark.asyncio


async def test_db_characterization_binds_exact_retrieval_feedback_and_projection_counts() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        result = await run_db_characterization(conn)
    finally:
        await tx.rollback()
        await conn.close()
    assert result["retrieval"]["denominator"] == 600
    assert result["retrieval"]["slices"]["multi_hop_relation"]["denominator"] == 120
    assert result["feedback"]["denominator"] == 720
    assert result["feedback"]["slices"]["models_first"]["denominator"] == 360
    assert result["feedback"]["slices"]["models_plus_raw_control"]["denominator"] == 360
    assert result["projection_refresh"]["enqueue_attempts"] == 24
    assert result["projection_refresh"]["enqueued_jobs"] == 12
    assert result["projection_refresh"]["processed_jobs"] == 12
    assert result["projection_refresh"]["dead_letter_jobs"] == 0
