from __future__ import annotations

import os

import asyncpg
import pytest

from lib.evaluation.epistemic_repair.p8_measurement_contracts import (
    QUEUE_FAMILIES,
    exact_token_receipt_is_usable,
    projection_refresh_measure_is_usable,
    queue_curve_is_usable,
    validate_queue_manifest,
)


@pytest.mark.asyncio
async def test_queue_manifest_binds_every_current_production_family() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    conn = await asyncpg.connect(dsn)
    try:
        result = await validate_queue_manifest(conn)
    finally:
        await conn.close()
    assert result["families"] == 6
    assert result["manifest_complete"] is (
        not result["missing_tables"] and not result["missing_tenant_columns"]
    )


def test_token_gate_rejects_estimates_and_unavailable_usage() -> None:
    base = {"physical_attempt_id": "a", "logical_call_id": "l", "input_tokens": 10, "output_tokens": 2}
    assert exact_token_receipt_is_usable({**base, "usage_exactness": "reported"}) is True
    assert exact_token_receipt_is_usable({**base, "usage_exactness": "estimated"}) is False
    assert exact_token_receipt_is_usable({**base, "usage_exactness": "unavailable"}) is False


def test_queue_and_projection_contracts_require_complete_real_denominators() -> None:
    samples = {item.family: [2, 1, 0] for item in QUEUE_FAMILIES}
    assert queue_curve_is_usable(samples) is True
    assert queue_curve_is_usable({"projection_refresh": [0]}) is False
    assert projection_refresh_measure_is_usable({
        "enqueued_jobs": 10, "processed_jobs": 10, "dead_letter_jobs": 0,
        "unique_subject_family_versions": 10,
    }) is True
    assert projection_refresh_measure_is_usable({
        "enqueued_jobs": 0, "processed_jobs": 0, "dead_letter_jobs": 0,
        "unique_subject_family_versions": 0,
    }) is False
