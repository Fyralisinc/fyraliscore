from __future__ import annotations

from uuid import uuid4

from services.workers.relationship_ontology_proposals.worker import (
    RunReport,
    TenantOntologyProposalReport,
)


def test_run_report_rolls_up_counts() -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    report = RunReport(
        tenant_reports={
            tenant_a: TenantOntologyProposalReport(
                tenant_id=tenant_a,
                proposals_upserted=2,
                review_ready=1,
            ),
            tenant_b: TenantOntologyProposalReport(
                tenant_id=tenant_b,
                proposals_upserted=3,
                review_ready=2,
                error="boom",
            ),
        }
    )

    assert report.tenants_scanned == 2
    assert report.proposals_upserted == 5
    assert report.review_ready == 3
    assert report.errors == 1

