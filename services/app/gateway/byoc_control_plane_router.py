"""BYOC control-plane intake routes for sanitized data-plane evidence."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import ValidationError

from lib.shared.errors import SecretStoreError
from services.app.gateway.byoc_control_plane_keys import (
    ByocControlPlaneKeyPurpose,
    ResolvedByocControlPlaneKey,
    resolver_from_app_state,
)
from services.platform.runtime.byoc_control_plane_intake import (
    READ_AUTH_KEY_REF_HEADER,
    ByocEvidencePackageIntakeRecord,
    ByocEvidencePackageIntakeStore,
    ByocEvidencePackageReceipt,
    ByocEvidencePackageReceiptList,
    ByocEvidencePackageReceiptQuery,
    ByocEvidencePackageSubmissionRequest,
    InMemoryByocEvidencePackageIntakeStore,
    PostgresByocEvidencePackageIntakeStore,
    validate_evidence_receipt_read_auth_headers,
    validate_evidence_package_submission,
)
from services.platform.runtime.byoc_control_panel_state import (
    ByocControlPanelState,
    ByocControlPanelStateQuery,
    build_byoc_control_panel_state,
)
from services.platform.runtime.byoc_deployment_overview import (
    ByocDeploymentOverview,
    ByocDeploymentOverviewQuery,
    build_byoc_deployment_overview,
)
from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStateUpdateReceipt,
    ByocAgentDesiredStateUpdateRequest,
    ByocAgentFleetList,
    ByocAgentFleetQuery,
    ByocAgentRegistryStore,
    InMemoryByocAgentRegistryStore,
    PostgresByocAgentRegistryStore,
    validate_desired_state_update_request,
)
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportIntakeStore,
    ByocPreflightReportReceipt,
    ByocPreflightReportReceiptList,
    ByocPreflightReportReceiptQuery,
    ByocPreflightReportSubmissionRequest,
    InMemoryByocPreflightReportIntakeStore,
    PostgresByocPreflightReportIntakeStore,
    validate_preflight_report_submission,
)
from services.platform.runtime.byoc_product_health import (
    ByocProductHealth,
    ByocProductHealthIntakeStore,
    ByocProductHealthQuery,
    ByocProductHealthReceipt,
    ByocProductHealthSnapshotRequest,
    InMemoryByocProductHealthIntakeStore,
    PostgresByocProductHealthIntakeStore,
    validate_product_health_snapshot_submission,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceIntakeStore,
    ByocRunnerEvidenceReceipt,
    ByocRunnerEvidenceReceiptList,
    ByocRunnerEvidenceReceiptQuery,
    ByocRunnerEvidenceSubmissionRequest,
    InMemoryByocRunnerEvidenceIntakeStore,
    PostgresByocRunnerEvidenceIntakeStore,
    validate_runner_evidence_submission,
)


def build_byoc_control_plane_router(
    *,
    store: ByocEvidencePackageIntakeStore | None = None,
    runner_evidence_store: ByocRunnerEvidenceIntakeStore | None = None,
    preflight_report_store: ByocPreflightReportIntakeStore | None = None,
    agent_registry_store: ByocAgentRegistryStore | None = None,
    product_health_store: ByocProductHealthIntakeStore | None = None,
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
        resolved_key = await _resolve_control_plane_key(
            request,
            purpose="evidence_package_submission",
            key_ref=submission.signature.key_ref,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        violations = validate_evidence_package_submission(
            submission,
            signing_secret=resolved_key.secret,
            expected_key_ref=resolved_key.key_ref,
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

    @router.post(
        "/product-health-snapshots",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def submit_product_health_snapshot(
        request: Request,
        snapshot: ByocProductHealthSnapshotRequest,
    ) -> ByocProductHealthReceipt:
        resolved_key = await _resolve_control_plane_key(
            request,
            purpose="product_health_submission",
            key_ref=snapshot.signature.key_ref,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        violations = validate_product_health_snapshot_submission(
            snapshot,
            signing_secret=resolved_key.secret,
            expected_key_ref=resolved_key.key_ref,
        )
        if violations:
            status_code = (
                status.HTTP_403_FORBIDDEN
                if any("signature" in violation.path for violation in violations)
                else status.HTTP_400_BAD_REQUEST
            )
            raise HTTPException(
                status_code=status_code,
                detail={"errors": [violation.render() for violation in violations]},
            )
        intake_store = product_health_store or _product_health_store_from_state(
            request
        )
        return await intake_store.put(snapshot)

    @router.post(
        "/preflight-reports",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def submit_preflight_report(
        request: Request,
        submission: ByocPreflightReportSubmissionRequest,
    ) -> ByocPreflightReportReceipt:
        resolved_key = await _resolve_control_plane_key(
            request,
            purpose="evidence_package_submission",
            key_ref=submission.signature.key_ref,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        violations = validate_preflight_report_submission(
            submission,
            signing_secret=resolved_key.secret,
            expected_key_ref=resolved_key.key_ref,
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
        intake_store = preflight_report_store or _preflight_report_store_from_state(
            request
        )
        return await intake_store.put(submission)

    @router.get("/preflight-reports")
    async def list_preflight_report_receipts(
        request: Request,
        deployment_id: str | None = None,
        customer_id: str | None = None,
        limit: int = 50,
    ) -> ByocPreflightReportReceiptList:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        try:
            query = ByocPreflightReportReceiptQuery(
                deployment_id=deployment_id,
                customer_id=customer_id,
                limit=limit,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        intake_store = preflight_report_store or _preflight_report_store_from_state(
            request
        )
        return await intake_store.list_receipts(query)

    @router.post(
        "/runner-evidence",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def submit_runner_evidence(
        request: Request,
        submission: ByocRunnerEvidenceSubmissionRequest,
    ) -> ByocRunnerEvidenceReceipt:
        resolved_key = await _resolve_control_plane_key(
            request,
            purpose="evidence_package_submission",
            key_ref=submission.signature.key_ref,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        violations = validate_runner_evidence_submission(
            submission,
            signing_secret=resolved_key.secret,
            expected_key_ref=resolved_key.key_ref,
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
        intake_store = runner_evidence_store or _runner_evidence_store_from_state(
            request
        )
        return await intake_store.put(submission)

    @router.get("/runner-evidence")
    async def list_runner_evidence_receipts(
        request: Request,
        deployment_id: str | None = None,
        customer_id: str | None = None,
        limit: int = 50,
    ) -> ByocRunnerEvidenceReceiptList:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        try:
            query = ByocRunnerEvidenceReceiptQuery(
                deployment_id=deployment_id,
                customer_id=customer_id,
                limit=limit,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        intake_store = runner_evidence_store or _runner_evidence_store_from_state(
            request
        )
        return await intake_store.list_receipts(query)

    @router.post(
        "/agent-desired-state",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def update_agent_desired_state(
        request: Request,
        update: ByocAgentDesiredStateUpdateRequest,
    ) -> ByocAgentDesiredStateUpdateReceipt:
        resolved_key = await _resolve_control_plane_key(
            request,
            purpose="desired_state_update",
            key_ref=update.signature.key_ref,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        violations = validate_desired_state_update_request(
            update,
            signing_secret=resolved_key.secret,
            expected_key_ref=resolved_key.key_ref,
        )
        if violations:
            status_code = (
                status.HTTP_403_FORBIDDEN
                if any("signature" in violation.path for violation in violations)
                else status.HTTP_400_BAD_REQUEST
            )
            raise HTTPException(
                status_code=status_code,
                detail={"errors": [violation.render() for violation in violations]},
            )
        registry = agent_registry_store or _agent_registry_store_from_state(request)
        receipt = await registry.update_desired_state(update)
        if receipt is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"errors": ["agent_id: agent_not_enrolled"]},
            )
        return receipt

    @router.get("/agents")
    async def list_agents(
        request: Request,
        deployment_id: str | None = None,
        customer_id: str | None = None,
        agent_id: str | None = None,
        limit: int = 50,
    ) -> ByocAgentFleetList:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        try:
            query = ByocAgentFleetQuery(
                deployment_id=deployment_id,
                customer_id=customer_id,
                agent_id=agent_id,
                limit=limit,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        registry = agent_registry_store or _agent_registry_store_from_state(request)
        return await registry.list_agents(query)

    @router.get("/deployment-overview")
    async def get_deployment_overview(
        request: Request,
        deployment_id: str,
        customer_id: str | None = None,
    ) -> ByocDeploymentOverview:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        try:
            query = ByocDeploymentOverviewQuery(
                deployment_id=deployment_id,
                customer_id=customer_id,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        registry = agent_registry_store or _agent_registry_store_from_state(request)
        evidence_store = store or _store_from_state(request)
        preflight_store = preflight_report_store or _preflight_report_store_from_state(
            request
        )
        runner_store = runner_evidence_store or _runner_evidence_store_from_state(
            request
        )
        agents = await registry.list_agents(
            ByocAgentFleetQuery(
                deployment_id=query.deployment_id,
                customer_id=query.customer_id,
                limit=100,
            )
        )
        evidence_packages = await evidence_store.list_receipts(
            ByocEvidencePackageReceiptQuery(
                deployment_id=query.deployment_id,
                customer_id=query.customer_id,
                limit=20,
            )
        )
        preflight_reports = await preflight_store.list_receipts(
            ByocPreflightReportReceiptQuery(
                deployment_id=query.deployment_id,
                customer_id=query.customer_id,
                limit=20,
            )
        )
        runner_evidence = await runner_store.list_receipts(
            ByocRunnerEvidenceReceiptQuery(
                deployment_id=query.deployment_id,
                customer_id=query.customer_id,
                limit=20,
            )
        )
        return build_byoc_deployment_overview(
            query=query,
            agents=agents,
            evidence_packages=evidence_packages,
            preflight_reports=preflight_reports,
            runner_evidence=runner_evidence,
        )

    @router.get("/control-panel-state")
    async def get_control_panel_state(
        request: Request,
        deployment_id: str,
        customer_id: str | None = None,
        recent_limit: int = 10,
    ) -> ByocControlPanelState:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        try:
            query = ByocControlPanelStateQuery(
                deployment_id=deployment_id,
                customer_id=customer_id,
                recent_limit=recent_limit,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        return await read_byoc_control_panel_state_from_request(
            request,
            query=query,
            agent_registry_store=agent_registry_store,
            evidence_package_store=store,
            preflight_report_store=preflight_report_store,
            runner_evidence_store=runner_evidence_store,
            product_health_store=product_health_store,
        )

    @router.get("/product-health")
    async def get_product_health(
        request: Request,
        deployment_id: str,
        customer_id: str | None = None,
    ) -> ByocProductHealth:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        try:
            query = ByocProductHealthQuery(
                deployment_id=deployment_id,
                customer_id=customer_id,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        return await read_byoc_product_health_from_request(
            request,
            query=query,
            product_health_store=product_health_store,
        )

    @router.get("/evidence-packages")
    async def list_evidence_package_receipts(
        request: Request,
        deployment_id: str | None = None,
        customer_id: str | None = None,
        limit: int = 50,
    ) -> ByocEvidencePackageReceiptList:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        try:
            query = ByocEvidencePackageReceiptQuery(
                deployment_id=deployment_id,
                customer_id=customer_id,
                limit=limit,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        return await (store or _store_from_state(request)).list_receipts(query)

    @router.get("/evidence-packages/{receipt_id}")
    async def get_evidence_package_receipt(
        request: Request,
        receipt_id: str,
    ) -> ByocEvidencePackageIntakeRecord:
        await _require_receipt_read_auth(
            request,
            signing_secret=signing_secret,
            signing_key_ref=signing_key_ref,
        )
        record = await (store or _store_from_state(request)).get(receipt_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "receipt_not_found"},
            )
        return record

    return router


async def read_byoc_control_panel_state_from_request(
    request: Request,
    *,
    query: ByocControlPanelStateQuery,
    agent_registry_store: ByocAgentRegistryStore | None = None,
    evidence_package_store: ByocEvidencePackageIntakeStore | None = None,
    preflight_report_store: ByocPreflightReportIntakeStore | None = None,
    runner_evidence_store: ByocRunnerEvidenceIntakeStore | None = None,
    product_health_store: ByocProductHealthIntakeStore | None = None,
) -> ByocControlPanelState:
    """Build sanitized control-panel state from gateway stores."""

    registry = agent_registry_store or _agent_registry_store_from_state(request)
    evidence_store = evidence_package_store or _store_from_state(request)
    preflight_store = preflight_report_store or _preflight_report_store_from_state(
        request
    )
    runner_store = runner_evidence_store or _runner_evidence_store_from_state(
        request
    )
    product_store = product_health_store or _product_health_store_from_state(request)
    agents = await registry.list_agents(
        ByocAgentFleetQuery(
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=100,
        )
    )
    evidence_packages = await evidence_store.list_receipts(
        ByocEvidencePackageReceiptQuery(
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.recent_limit,
        )
    )
    preflight_reports = await preflight_store.list_receipts(
        ByocPreflightReportReceiptQuery(
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.recent_limit,
        )
    )
    runner_evidence = await runner_store.list_receipts(
        ByocRunnerEvidenceReceiptQuery(
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
            limit=query.recent_limit,
        )
    )
    product_health = await product_store.latest(
        ByocProductHealthQuery(
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
        )
    )
    overview = build_byoc_deployment_overview(
        query=ByocDeploymentOverviewQuery(
            deployment_id=query.deployment_id,
            customer_id=query.customer_id,
        ),
        agents=agents,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
    )
    return build_byoc_control_panel_state(
        query=query,
        overview=overview,
        agents=agents,
        product_health=product_health,
        evidence_packages=evidence_packages,
        preflight_reports=preflight_reports,
        runner_evidence=runner_evidence,
    )


async def read_byoc_product_health_from_request(
    request: Request,
    *,
    query: ByocProductHealthQuery,
    product_health_store: ByocProductHealthIntakeStore | None = None,
) -> ByocProductHealth:
    store = product_health_store or _product_health_store_from_state(request)
    return await store.latest(query)


async def _require_receipt_read_auth(
    request: Request,
    *,
    signing_secret: str | None,
    signing_key_ref: str | None,
) -> None:
    key_ref = request.headers.get(READ_AUTH_KEY_REF_HEADER)
    if not key_ref:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "errors": [
                    "read_auth.headers: missing_read_auth_headers: "
                    "signed receipt read headers are required"
                ]
            },
        )
    resolved_key = await _resolve_control_plane_key(
        request,
        purpose="receipt_read",
        key_ref=key_ref,
        signing_secret=signing_secret,
        signing_key_ref=signing_key_ref,
    )
    violations = validate_evidence_receipt_read_auth_headers(
        request.headers,
        method=request.method,
        path=request.url.path,
        query=request.url.query,
        signing_secret=resolved_key.secret,
        expected_key_ref=resolved_key.key_ref,
    )
    if violations:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"errors": [violation.render() for violation in violations]},
        )


async def _resolve_control_plane_key(
    request: Request,
    *,
    purpose: ByocControlPlaneKeyPurpose,
    key_ref: str,
    signing_secret: str | None,
    signing_key_ref: str | None,
) -> ResolvedByocControlPlaneKey:
    if signing_secret:
        expected_key_ref = signing_key_ref or key_ref
        if key_ref != expected_key_ref:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"errors": ["signature.key_ref: unknown_key_ref"]},
            )
        return ResolvedByocControlPlaneKey(
            key_ref=expected_key_ref,
            secret=signing_secret,
            source="static_test_secret",
        )

    resolver = resolver_from_app_state(request.app.state)
    if resolver is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"errors": ["BYOC evidence intake is not configured"]},
        )
    try:
        resolved = await resolver.resolve(
            purpose=purpose,
            key_ref=key_ref,
        )
    except SecretStoreError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "errors": [
                    "BYOC evidence signing key could not be resolved",
                ]
            },
        ) from exc
    if resolved is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"errors": ["signature.key_ref: unknown_key_ref"]},
        )
    return resolved


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


def _runner_evidence_store_from_state(
    request: Request,
) -> ByocRunnerEvidenceIntakeStore:
    existing = getattr(request.app.state, "byoc_runner_evidence_intake_store", None)
    if existing is not None:
        return existing
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresByocRunnerEvidenceIntakeStore(pool)
        request.app.state.byoc_runner_evidence_intake_store = created
        return created
    created = InMemoryByocRunnerEvidenceIntakeStore()
    request.app.state.byoc_runner_evidence_intake_store = created
    return created


def _preflight_report_store_from_state(
    request: Request,
) -> ByocPreflightReportIntakeStore:
    existing = getattr(request.app.state, "byoc_preflight_report_intake_store", None)
    if existing is not None:
        return existing
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresByocPreflightReportIntakeStore(pool)
        request.app.state.byoc_preflight_report_intake_store = created
        return created
    created = InMemoryByocPreflightReportIntakeStore()
    request.app.state.byoc_preflight_report_intake_store = created
    return created


def _agent_registry_store_from_state(request: Request) -> ByocAgentRegistryStore:
    existing = getattr(request.app.state, "byoc_agent_registry_store", None)
    if existing is not None:
        return existing
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresByocAgentRegistryStore(pool)
        request.app.state.byoc_agent_registry_store = created
        return created
    created = InMemoryByocAgentRegistryStore()
    request.app.state.byoc_agent_registry_store = created
    return created


def _product_health_store_from_state(
    request: Request,
) -> ByocProductHealthIntakeStore:
    existing = getattr(request.app.state, "byoc_product_health_intake_store", None)
    if existing is not None:
        return existing
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresByocProductHealthIntakeStore(pool)
        request.app.state.byoc_product_health_intake_store = created
        return created
    created = InMemoryByocProductHealthIntakeStore()
    request.app.state.byoc_product_health_intake_store = created
    return created


__all__ = [
    "build_byoc_control_plane_router",
    "read_byoc_control_panel_state_from_request",
    "read_byoc_product_health_from_request",
]
