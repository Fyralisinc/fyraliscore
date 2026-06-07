from __future__ import annotations

from uuid import uuid4

from services.workers.sage_structural_features.worker import (
    RunReport,
    TenantStructuralFeatureReport,
)


def test_run_report_rolls_up_tenant_counts() -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    report = RunReport(
        tenant_reports={
            tenant_a: TenantStructuralFeatureReport(
                tenant_id=tenant_a,
                models_written=3,
                edges_written=5,
            ),
            tenant_b: TenantStructuralFeatureReport(
                tenant_id=tenant_b,
                models_written=7,
                edges_written=11,
                error="boom",
            ),
        }
    )

    assert report.tenants_scanned == 2
    assert report.models_written == 10
    assert report.edges_written == 16
    assert report.errors == 1

