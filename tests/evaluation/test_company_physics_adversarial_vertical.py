from __future__ import annotations

import json
import os
from uuid import uuid4

import asyncpg
import pytest

from services.company_physics_adversarial_vertical import (
    run_company_physics_adversarial_vertical,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_sealed_company_physics_adversarial_vertical(fresh_db, tmp_path):
    async def install_json_codec(conn):
        for type_name in ("json", "jsonb"):
            await conn.set_type_codec(
                type_name,
                encoder=lambda value: json.dumps(value) if not isinstance(value, str) else value,
                decoder=json.loads, schema="pg_catalog",
            )

    pool = await asyncpg.create_pool(
        os.environ["DATABASE_URL"], min_size=1, max_size=3, init=install_json_codec,
    )
    tenant_id = uuid4()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tenants (id,name) VALUES ($1,'adversarial-company-physics')",
                tenant_id,
            )
        output = tmp_path / "adversarial.json"
        result = await run_company_physics_adversarial_vertical(
            pool=pool, tenant_id=tenant_id, output_path=output,
        )
    finally:
        await pool.close()

    assert json.loads(output.read_text())["objective_sha256"] == result["objective_sha256"]
    assert result["schema_version"] == "sealed-company-physics-adversarial-objective-v2"
    assert result["population"]["signal_batches"] == 1
    assert result["population"]["adversarial_relation_attempts"] == 4
    assert result["consequence_tier_denominators"]["critical"] == {
        "attempts": 2, "safe_rejections": 2, "safe_rejection_rate": 1.0,
    }
    assert result["consequence_tier_denominators"]["high"] == {
        "attempts": 2, "safe_rejections": 2, "safe_rejection_rate": 1.0,
    }
    assert result["multi_hop"]["observed_active_hops_before_correction"] == 2
    assert result["multi_hop"]["cycle_closure_rejected"] is True
    assert result["multi_hop"]["mention_lineage_count"] == 3
    assert result["correction_propagation"] == {
        "pre_correction_active_hops": 2,
        "first_hop_retired": True,
        "downstream_reevaluation_enqueued": True,
        "second_hop_preserved_pending_reevaluation": True,
        "transitive_repair_claimed": False,
    }
    assert result["open_world_abstention"]["novel_and_homonym_cases"] == 2
    assert result["open_world_abstention"]["safe_decision_rate"] == 1.0
    assert result["open_world_abstention"]["harmful_false_link_rate"] == 0.0
