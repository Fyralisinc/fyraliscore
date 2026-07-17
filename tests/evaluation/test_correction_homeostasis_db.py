import json
import os
from uuid import uuid4

import asyncpg
import pytest

from services.correction_homeostasis_db_vertical import run_correction_homeostasis_db_vertical


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_correction_homeostasis_persists_across_runtime_restart(fresh_db, tmp_path):
    async def install_json_codec(conn):
        for type_name in ("json", "jsonb"):
            await conn.set_type_codec(
                type_name,
                encoder=lambda value: json.dumps(value) if not isinstance(value, str) else value,
                decoder=json.loads,
                schema="pg_catalog",
            )

    configured_pool = await asyncpg.create_pool(
        os.environ["DATABASE_URL"], min_size=1, max_size=3, init=install_json_codec,
    )
    tenant_id = uuid4()
    async with configured_pool.acquire() as conn:
        await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,'correction-db-proof')", tenant_id)
    output = tmp_path / "correction-homeostasis-db.json"
    try:
        result = await run_correction_homeostasis_db_vertical(
            pool=configured_pool, tenant_id=tenant_id, output_path=output,
        )
    finally:
        await configured_pool.close()

    assert json.loads(output.read_text())["objective_sha256"] == result["objective_sha256"]
    assert result["evaluation"]["verdict"] == "meets_policy"
    assert result["evaluation"]["continuous_score"] == 1.0
    evidence = result["database_evidence"]
    assert evidence["correction_count"] == 2
    assert evidence["fenced_model_count"] == 8
    assert evidence["archived_root_count"] == 2
    assert evidence["reeval_pair_count"] == 8
    assert evidence["replay_new_reeval_pair_count"] == 0
    assert evidence["cycle_write_rejections"] == 2
    assert evidence["before_restart"] == evidence["after_restart"]
