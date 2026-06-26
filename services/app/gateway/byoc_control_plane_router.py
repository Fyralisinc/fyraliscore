"""BYOC control-plane intake routes for sanitized data-plane evidence."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status

from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageIntakeRecord,
    ByocEvidencePackageIntakeStore,
    ByocEvidencePackageReceipt,
    ByocEvidencePackageSubmissionRequest,
    InMemoryByocEvidencePackageIntakeStore,
    PostgresByocEvidencePackageIntakeStore,
    validate_evidence_package_submission,
)


def build_byoc_control_plane_router(
    *,
    store: ByocEvidencePackageIntakeStore | None = None,
    signing_secret: str | None = None,
    signing_key_ref: str | None = None,
) -> APIRouter:
    router = APIRouter(
        prefix="/byoc/control-plane",
        tags=["byoc-control-plane"],
    )

    @router.post(
        "/evidence-packages",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def submit_evidence_package(
        request: Request,
        submission: ByocEvidencePackageSubmissionRequest,
    ) -> ByocEvidencePackageReceipt:
        secret = signing_secret or getattr(
            request.app.state,
            "byoc_evidence_intake_secret",
            None,
        )
        expected_key_ref = signing_key_ref or getattr(
            request.app.state,
            "byoc_evidence_intake_key_ref",
            None,
        )
        if not secret:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"errors": ["BYOC evidence intake is not configured"]},
            )
        violations = validate_evidence_package_submission(
            submission,
            signing_secret=secret,
            expected_key_ref=expected_key_ref,
        )
        if violations:
            status_code = (
                status.HTTP_403_FORBIDDEN
                if any("signature" in violation.code for violation in violations)
                else status.HTTP_400_BAD_REQUEST
            )
            raise HTTPException(
                status_code=status_code,
                detail={"errors": [violation.render() for violation in violations]},
            )
        intake_store = store or _store_from_state(request)
        return await intake_store.put(submission)

    @router.get("/evidence-packages/{receipt_id}")
    async def get_evidence_package_receipt(
        request: Request,
        receipt_id: str,
    ) -> ByocEvidencePackageIntakeRecord:
        record = await (store or _store_from_state(request)).get(receipt_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "receipt_not_found"},
            )
        return record

    return router


def _store_from_state(request: Request) -> ByocEvidencePackageIntakeStore:
    existing = getattr(request.app.state, "byoc_evidence_intake_store", None)
    if existing is not None:
        return existing
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresByocEvidencePackageIntakeStore(pool)
        request.app.state.byoc_evidence_intake_store = created
        return created
    created = InMemoryByocEvidencePackageIntakeStore()
    request.app.state.byoc_evidence_intake_store = created
    return created


__all__ = ["build_byoc_control_plane_router"]
