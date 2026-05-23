"""
Background relationship maintenance tests — orphans, outliers,
archival suggestions, percentile snapshots, and the key invariant
that no Model row is mutated.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.retrieval.maintenance import (
    MaintenanceReport,
    background_relationship_maintenance,
)

from services.retrieval.tests._fixtures import build_fixture


pytestmark = pytest.mark.integration


async def test_maintenance_no_models_no_work(tx_conn, fresh_db, tenant):
    """Tenant with zero Models → empty report, no log rows."""
    report = await background_relationship_maintenance(tenant, tx_conn)
    assert isinstance(report, MaintenanceReport)
    assert report.active_models_scanned == 0
    assert report.orphans_flagged == 0
    assert report.archival_suggestions == 0
    # No log rows written.
    n = await tx_conn.fetchval(
        "SELECT COUNT(*) FROM relationship_maintenance_log WHERE tenant_id = $1",
        tenant,
    )
    assert n == 0


async def test_maintenance_orphan_flag(tx_conn, fresh_db, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Pick a Model and point its supporting_model_ids at an existing
    # Model, then archive that supporting Model. The "dependent" Model
    # now has 100% archived supports → orphan.
    victim = fs.model_ids[25]
    support = fs.model_ids[30]
    # Write support
    await tx_conn.execute(
        "UPDATE models SET supporting_model_ids = $2::uuid[] WHERE id = $1",
        victim, [support],
    )
    # Archive the support.
    await tx_conn.execute(
        """
        UPDATE models
        SET status = 'archived', archived_at = now(), archive_reason = 'manual'
        WHERE id = $1
        """,
        support,
    )
    report = await background_relationship_maintenance(tenant, tx_conn)
    assert report.orphans_flagged >= 1

    rows = await tx_conn.fetch(
        """
        SELECT * FROM relationship_maintenance_log
        WHERE tenant_id = $1 AND entry_kind = 'orphan_flagged'
        """,
        tenant,
    )
    subject_ids = {r["subject_model_id"] for r in rows}
    assert victim in subject_ids


async def test_maintenance_archival_suggestion(tx_conn, fresh_db, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Force a Model to look stale: activation 0.02, last_retrieved_at 60d ago.
    target = fs.model_ids[40]
    cutoff = datetime(2026, 2, 15, tzinfo=timezone.utc)
    await tx_conn.execute(
        """
        UPDATE models
        SET activation = 0.02, last_retrieved_at = $2
        WHERE id = $1
        """,
        target, cutoff,
    )
    report = await background_relationship_maintenance(tenant, tx_conn)
    assert report.archival_suggestions >= 1
    rows = await tx_conn.fetch(
        """
        SELECT subject_model_id, payload FROM relationship_maintenance_log
        WHERE tenant_id = $1 AND entry_kind = 'archival_suggested'
        """,
        tenant,
    )
    import json
    found = False
    for r in rows:
        if r["subject_model_id"] == target:
            payload = r["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            assert payload["activation"] == pytest.approx(0.02)
            assert payload["reason"] == "low_activation_stale_retrieval"
            found = True
    assert found


async def test_maintenance_percentile_snapshot(tx_conn, fresh_db, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    report = await background_relationship_maintenance(tenant, tx_conn)
    # Fixture has 100 Models across 5+ proposition kinds — percentile
    # snapshots should be non-zero.
    assert report.percentile_snapshots >= 1
    rows = await tx_conn.fetch(
        """
        SELECT entry_kind, payload FROM relationship_maintenance_log
        WHERE tenant_id = $1 AND entry_kind = 'percentile_snapshot'
        """,
        tenant,
    )
    assert len(rows) >= 1
    import json
    for r in rows:
        payload = r["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert "p10" in payload
        assert "p50" in payload
        assert "p90" in payload
        assert payload["p10"] <= payload["p50"] <= payload["p90"]


async def test_maintenance_does_not_mutate_models(tx_conn, fresh_db, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    rows_before = await tx_conn.fetch(
        "SELECT id, status, activation, last_retrieved_at, retrieval_count, "
        "archived_at FROM models WHERE tenant_id = $1",
        tenant,
    )
    before = {r["id"]: dict(r) for r in rows_before}

    await background_relationship_maintenance(tenant, tx_conn)

    rows_after = await tx_conn.fetch(
        "SELECT id, status, activation, last_retrieved_at, retrieval_count, "
        "archived_at FROM models WHERE tenant_id = $1",
        tenant,
    )
    after = {r["id"]: dict(r) for r in rows_after}
    for mid, row_a in after.items():
        b = before[mid]
        assert row_a["status"] == b["status"]
        assert row_a["activation"] == b["activation"]
        assert row_a["last_retrieved_at"] == b["last_retrieved_at"]
        assert row_a["retrieval_count"] == b["retrieval_count"]
        assert row_a["archived_at"] == b["archived_at"]


async def test_maintenance_run_id_groups_entries(tx_conn, fresh_db, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    report = await background_relationship_maintenance(tenant, tx_conn)
    # Every row from this run has run_id == report.run_id.
    rows = await tx_conn.fetch(
        "SELECT run_id FROM relationship_maintenance_log WHERE tenant_id = $1",
        tenant,
    )
    run_ids = {r["run_id"] for r in rows}
    assert run_ids == {report.run_id}


async def test_maintenance_tenant_isolation(
    tx_conn, fresh_db, tenant, other_tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Run for other_tenant (empty).
    report = await background_relationship_maintenance(other_tenant, tx_conn)
    assert report.active_models_scanned == 0

    # Run for tenant (has the fixture).
    report2 = await background_relationship_maintenance(tenant, tx_conn)
    assert report2.active_models_scanned > 0

    # Log rows for other_tenant == 0; for tenant > 0.
    n1 = await tx_conn.fetchval(
        "SELECT COUNT(*) FROM relationship_maintenance_log WHERE tenant_id = $1",
        other_tenant,
    )
    n2 = await tx_conn.fetchval(
        "SELECT COUNT(*) FROM relationship_maintenance_log WHERE tenant_id = $1",
        tenant,
    )
    assert n1 == 0
    assert n2 > 0
