from __future__ import annotations

import os

import pytest

from services.feedback_quality_vertical import run_feedback_quality_vertical


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_governed_feedback_improves_later_company_model_quality(fresh_db, tmp_path):
    result = await run_feedback_quality_vertical(
        dsn=os.environ["DATABASE_URL"],
        output_path=tmp_path / "feedback-quality.json",
    )

    assert result["population"] == {
        "arms": 2, "later_batches_per_arm": 3,
        "signals_per_later_batch": 2, "correction_episodes": 1,
    }
    assert result["measurements"]["adaptive_later_quality"] == 1.0
    assert result["measurements"]["frozen_later_quality"] == 0.0
    assert result["measurements"]["adaptive_minus_frozen_quality"] == 1.0
    assert result["verdict"] == "meets_policy"
    assert all(result["checks"].values())
