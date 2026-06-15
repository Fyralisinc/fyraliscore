from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.reasoning.sage.health import build_sage_health_report


pytestmark = pytest.mark.asyncio


class _HealthReportConn:
    async def fetchval(self, query: str, *args):
        if "to_regclass" in query:
            table_name = args[0].removeprefix("public.")
            return table_name if table_name in self.present_tables else None
        if "LEFT JOIN model_structural_features" in query:
            return 1
        if "FROM models" in query and "status = 'active'" in query:
            return 4
        if "count(DISTINCT e.inquiry_session_id)" in query:
            return 2
        if "status = 'review_ready'" in query:
            return 7200
        raise AssertionError(f"Unexpected fetchval query: {query}")

    async def fetchrow(self, query: str, *_args):
        if "FROM model_edge_structural_features" in query:
            return {
                "feature_rows": 5,
                "newest_updated_at": self.now,
                "newest_age_seconds": 120,
            }
        if "FROM model_structural_features" in query:
            return {
                "feature_rows": 2,
                "newest_updated_at": self.now,
                "newest_age_seconds": 300,
            }
        if "FROM sage_topology_optimizer_runs" in query:
            return {
                "running": 1,
                "completed": 3,
                "failed": 1,
                "last_completed_at": self.now,
                "last_completed_age_seconds": 5000,
                "oldest_running_age_seconds": 1900,
            }
        if "FROM relationship_candidates" in query:
            return {
                "open_edge_type_candidates": 3,
                "oldest_open_edge_type_age_seconds": 3600,
            }
        raise AssertionError(f"Unexpected fetchrow query: {query}")

    async def fetch(self, query: str, *_args):
        if "FROM relationship_ontology_proposals" in query:
            return [
                {"status": "review_ready", "n": 2},
                {"status": "accepted", "n": 1},
            ]
        raise AssertionError(f"Unexpected fetch query: {query}")

    present_tables = {
        "model_structural_features",
        "model_edge_structural_features",
        "inquiry_outcome_events",
        "sage_topology_optimizer_runs",
        "relationship_candidates",
        "relationship_ontology_proposals",
    }
    now = datetime(2026, 6, 13, tzinfo=timezone.utc)


async def test_sage_health_report_preserves_degraded_contract():
    tenant_id = uuid4()

    report = await build_sage_health_report(
        _HealthReportConn(),
        tenant_id=tenant_id,
        structural_freshness_hours=6,
        optimizer_lag_minutes=30,
    )

    assert report["status"] == "degraded"
    assert report["tenant_id"] == str(tenant_id)
    assert report["checks"]["active_models"] == 4
    assert report["checks"]["structural_features"] == {
        "table_present": True,
        "edge_table_present": True,
        "feature_rows": 2,
        "active_model_coverage": 0.5,
        "newest_updated_at": "2026-06-13T00:00:00+00:00",
        "newest_age_seconds": 300.0,
        "stale_or_missing_active_models": 1,
        "freshness_slo_hours": 6,
    }
    assert report["checks"]["edge_structural_features"] == {
        "feature_rows": 5,
        "newest_updated_at": "2026-06-13T00:00:00+00:00",
        "newest_age_seconds": 120.0,
    }
    assert report["checks"]["topology_optimizer"]["pending_sessions"] == 2
    assert report["checks"]["relationship_candidates"][
        "open_edge_type_candidates"
    ] == 3
    assert report["checks"]["relationship_ontology_proposals"]["by_status"] == {
        "review_ready": 2,
        "accepted": 1,
    }
    assert {finding["code"] for finding in report["findings"]} == {
        "structural_features_stale_or_missing",
        "topology_optimizer_lagging",
        "topology_optimizer_run_stuck",
        "ontology_proposals_ready_for_review",
    }
