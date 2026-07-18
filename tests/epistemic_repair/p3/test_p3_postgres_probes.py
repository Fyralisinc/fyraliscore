from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import asyncpg
import pytest

from services.evaluation.epistemic_repair.p3_postgres_probes import run_p3_postgres_probes


ROOT = Path(__file__).resolve().parents[3]


def test_identity_writer_registry_has_no_bypass() -> None:
    path = ROOT / "scripts/check_identity_writer_registry.py"
    spec = importlib.util.spec_from_file_location("identity_registry", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.violations() == []


@pytest.mark.asyncio
async def test_p3_postgres_authority_scope_and_tenant_proofs() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        proof = await run_p3_postgres_probes(conn)
        assert proof.hg02_conforms
        assert proof.hg02_applied_count == 5
        assert proof.hg02_replay_extra_effects == 0
        assert proof.hg02_bypass_rejected
        assert proof.hg06_conforms and proof.hg06_scope_count == 5
        assert proof.hg14_conforms and proof.hg14_cross_tenant_incidents == 0
        assert not proof.violation_codes
    finally:
        await transaction.rollback()
        await conn.close()


def test_tenant_probe_is_nonempty_database_safe() -> None:
    source = (
        ROOT / "services/evaluation/epistemic_repair/p3_postgres_probes.py"
    ).read_text()
    assert "WHERE tenant_id=$1" in source
    assert "relation.relrowsecurity" in source
    assert "policy.policyname='tenant_isolation'" in source


def test_identity_authority_migration_guards_every_mutation() -> None:
    sql = (ROOT / "db/migrations/0228_entity_identity_command_authority.sql").read_text()
    assert "BEFORE INSERT OR UPDATE OR DELETE ON entity_aliases" in sql
    assert "app.entity_identity_command" in sql
    assert "require_entity_identity_command_authority" in sql
