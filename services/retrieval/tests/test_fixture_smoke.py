"""Fast smoke test to ensure the fixture builder works end-to-end."""
from __future__ import annotations

import uuid

import asyncpg
import pytest

from services.retrieval.tests._fixtures import build_fixture


pytestmark = pytest.mark.integration


async def test_fixture_builder_smoke(
    tx_conn: asyncpg.Connection,
    fresh_db: asyncpg.Pool,
    tenant: uuid.UUID,
) -> None:
    # Build a small subset to keep this quick (not the full 200/100 —
    # full size is exercised by the real retrieval tests).
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_actors=3,
        n_goals=10,
        n_commitments=6,
        n_observations=10,
        n_models=8,
        n_customers=2,
        n_decisions=2,
    )
    assert fs.tenant_id == tenant
    assert len(fs.actor_ids) == 3
    assert len(fs.observation_ids) == 10
    # Builder produces: 1 root + 5 children + max(0, n_goals - 6) grandchildren.
    assert len(fs.goal_ids) == 10
    assert len(fs.commitment_ids) == 6
    assert len(fs.model_ids) == 8
    assert len(fs.customer_resource_ids) == 2
    assert fs.hero_commitment_id is not None
    assert fs.hero_goal_id is not None
    assert fs.hero_customer_id is not None
    assert fs.hero_actor_id is not None
