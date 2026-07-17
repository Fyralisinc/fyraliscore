from __future__ import annotations

import json
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.evaluation.company_physics_vertical import run_company_physics_vertical


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_sealed_company_physics_vertical(fresh_db, tmp_path):
    async def install_json_codec(conn):
        for type_name in ("json", "jsonb"):
            await conn.set_type_codec(
                type_name,
                encoder=lambda value: (
                    json.dumps(value) if not isinstance(value, str) else value
                ),
                decoder=json.loads,
                schema="pg_catalog",
            )

    configured_pool = await asyncpg.create_pool(
        os.environ["DATABASE_URL"], min_size=1, max_size=3,
        init=install_json_codec,
    )
    tenant_id = uuid4()
    try:
        async with configured_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tenants (id,name) VALUES ($1,'sealed-company-physics')",
                tenant_id,
            )
        output = tmp_path / "objective.json"
        result = await run_company_physics_vertical(
            pool=configured_pool, tenant_id=tenant_id, output_path=output
        )
    finally:
        await configured_pool.close()

    assert json.loads(output.read_text())["objective_sha256"] == result[
        "objective_sha256"
    ]
    assert result["evaluator_schema_version"] == "gold-entity-pipeline-v4"
    assert result["population"] == {"signals": 7, "batches": 1, "batch_size": 7}
    assert result["discovery"]["structured_calls"] == 1
    assert result["safety_metrics"]["resolver_owned_canonical_alias_writes"] == 0
    assert result["safety_metrics"]["harmful_false_link_rate"] == 0.0
    assert result["canonical_link_metrics"]["accuracy"] == 1.0
    assert result["lineage_metrics"] == {
        "grounding": 1.0, "semantic": 1.0, "relation": 1.0,
    }
    assert result["semantic_metrics"]["belief_model_materialization_rate"] == 1.0
    assert result["semantic_metrics"]["no_admission_no_model_safety_rate"] == 1.0
    assert result["topology_metrics"]["relation_direction_accuracy"] == 1.0
    assert result["topology_metrics"]["relation_type_accuracy"] == 1.0
    assert result["topology_metrics"]["unexpected_relation_rate"] == 0.0
    assert result["durable_counts"]["mentions"] >= 7
    assert result["durable_counts"]["candidate_sets"] >= 7
    assert result["durable_counts"]["assessments"] >= 7
    assert result["durable_counts"]["admissions"] >= 7
    assert result["durable_counts"]["terminal_fates"] >= 7
    assert result["durable_counts"]["models"] >= 2
    assert result["durable_counts"]["active_edges"] == 1
