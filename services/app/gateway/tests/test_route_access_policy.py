from __future__ import annotations

from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

import services.app.gateway.extensions as gateway_extensions
from services.app.gateway.middleware import (
    BearerAuthMiddleware,
    RateLimitMiddleware,
    RequestContextMiddleware,
)
from services.app.gateway.route_access import (
    RouteAccess,
    classify_gateway_route,
    iter_gateway_route_inventory,
)
from services.app.gateway.route_mounts import mount_gateway_routes
from services.app.gateway.settings import GatewaySettings

_PROD_BOOTSTRAP_SECRET = "prod-bootstrap-secret-32chars-minimum"


def _production_settings() -> GatewaySettings:
    return GatewaySettings(
        environment="production",
        auth_bootstrap_secret=_PROD_BOOTSTRAP_SECRET,
        debug_endpoints_enabled=False,
        finance_panel_enabled=False,
        slack_dm_panel_enabled=False,
        spec_demo_routes_enabled=False,
        websocket_query_token_auth_enabled=False,
        view_ceo_static_tokens_enabled=False,
        mount_sim=False,
        require_realtime=False,
        require_github_integration=False,
        require_ingestion_data_plane=True,
        start_grt_scheduler=True,
    )


def _inventory_app(*, debug_endpoints_enabled: bool = False) -> FastAPI:
    settings = GatewaySettings(debug_endpoints_enabled=debug_endpoints_enabled)
    app = FastAPI()
    app.state.gateway_settings = settings
    mount_gateway_routes(app, settings=settings, emit_mount_logs=False)
    return app


def _inventory_app_with_settings(settings: GatewaySettings) -> FastAPI:
    app = FastAPI()
    app.state.gateway_settings = settings
    mount_gateway_routes(app, settings=settings, emit_mount_logs=False)
    return app


def _middleware_app() -> FastAPI:
    app = _inventory_app()
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(BearerAuthMiddleware)
    app.add_middleware(RequestContextMiddleware)
    return app


def test_static_gateway_route_inventory_classifies_security_boundaries() -> None:
    entries = iter_gateway_route_inventory(_inventory_app())
    by_path = {entry.path: entry for entry in entries}

    assert by_path["/healthz"].policy.access is RouteAccess.PUBLIC
    assert by_path["/observations"].policy.access is RouteAccess.BEARER
    assert by_path["/ingest/{channel:path}"].policy.access is RouteAccess.BEARER
    assert by_path["/ingest/{channel:path}"].policy.gateway_bearer_required is True
    assert by_path["/webhooks/{provider}"].policy.access is RouteAccess.PROVIDER_SIGNED
    # Stable connector webhooks enter through the generic provider-signed
    # surface; Instagram remains an explicit supplemental webhook adapter.
    assert (
        by_path["/integrations/instagram/webhook"].policy.access
        is RouteAccess.PROVIDER_SIGNED
    )
    assert by_path["/ext/oauth/token"].policy.access is RouteAccess.EXTENSION_AUTH
    assert by_path["/ext/v1/observations"].policy.access is RouteAccess.EXTENSION_AUTH
    connector_callback = by_path["/integrations/{source}/callback"].policy
    assert connector_callback.access is RouteAccess.SELF_AUTHENTICATED
    assert connector_callback.gateway_bearer_required is False
    connector_install = by_path["/integrations/{source}/install"].policy
    assert connector_install.access is RouteAccess.BEARER
    assert connector_install.gateway_bearer_required is True
    byoc_intake = by_path["/byoc/control-plane/evidence-packages"].policy
    assert byoc_intake.access is RouteAccess.SELF_AUTHENTICATED
    assert byoc_intake.gateway_bearer_required is False
    byoc_runner_evidence = by_path["/byoc/control-plane/runner-evidence"].policy
    assert byoc_runner_evidence.access is RouteAccess.SELF_AUTHENTICATED
    assert byoc_runner_evidence.gateway_bearer_required is False
    byoc_preflight = by_path["/byoc/control-plane/preflight-reports"].policy
    assert byoc_preflight.access is RouteAccess.SELF_AUTHENTICATED
    assert byoc_preflight.gateway_bearer_required is False
    byoc_control_panel_state = by_path["/byoc/control-plane/control-panel-state"].policy
    assert byoc_control_panel_state.access is RouteAccess.SELF_AUTHENTICATED
    assert byoc_control_panel_state.gateway_bearer_required is False
    byoc_browser_control_panel = by_path["/byoc/control-panel/state"].policy
    assert byoc_browser_control_panel.access is RouteAccess.BEARER
    assert byoc_browser_control_panel.gateway_bearer_required is True
    byoc_browser_deployments = by_path["/byoc/control-panel/deployments"].policy
    assert byoc_browser_deployments.access is RouteAccess.BEARER
    assert byoc_browser_deployments.gateway_bearer_required is True
    byoc_agent = by_path["/byoc/agent/enroll"].policy
    assert byoc_agent.access is RouteAccess.SELF_AUTHENTICATED
    assert byoc_agent.gateway_bearer_required is False
    assert (
        by_path["/byoc/agent/desired-state"].policy.access
        is RouteAccess.SELF_AUTHENTICATED
    )
    assert by_path["/api/admin/dead-letters"].policy.access is RouteAccess.ADMIN
    assert by_path["/api/admin/dead-letters"].policy.gateway_bearer_required is True

    internal = by_path["/internal/synthesis-reader/read"].policy
    assert internal.access is RouteAccess.INTERNAL
    assert internal.gateway_bearer_required is True


def test_debug_routes_are_absent_from_default_static_inventory() -> None:
    entries = iter_gateway_route_inventory(_inventory_app())

    assert all(entry.policy.access is not RouteAccess.DEBUG for entry in entries)


def test_spec_demo_routes_are_absent_when_disabled() -> None:
    entries = iter_gateway_route_inventory(
        _inventory_app_with_settings(
            GatewaySettings(spec_demo_routes_enabled=False),
        )
    )

    assert all(not entry.path.startswith("/v1/spec/") for entry in entries)


def test_production_mount_skips_non_production_extensions(
    monkeypatch,
) -> None:
    router = APIRouter()

    @router.get("/v1/demo/ping")
    def demo_ping() -> dict[str, bool]:
        return {"ok": True}

    extension = gateway_extensions.GatewayExtension(
        name="demo",
        routers=[router],
        public_path_prefixes=("/v1/demo",),
    )
    monkeypatch.setattr(
        gateway_extensions,
        "discovered_extensions",
        lambda: [extension],
    )

    dev_paths = {
        entry.path
        for entry in iter_gateway_route_inventory(
            _inventory_app_with_settings(GatewaySettings())
        )
    }
    prod_paths = {
        entry.path
        for entry in iter_gateway_route_inventory(
            _inventory_app_with_settings(_production_settings())
        )
    }

    assert "/v1/demo/ping" in dev_paths
    assert "/v1/demo/ping" not in prod_paths
    assert gateway_extensions.extension_public_path_prefixes(production=False) == (
        "/v1/demo",
    )
    assert gateway_extensions.extension_public_path_prefixes(production=True) == ()


def test_debug_routes_are_classified_when_explicitly_enabled() -> None:
    assert (
        classify_gateway_route("/debug/document-ingest/status").access
        is RouteAccess.DEBUG
    )
    assert classify_gateway_route("/debug/whatsapp/recent").access is RouteAccess.DEBUG


def test_only_health_readiness_and_metrics_are_fully_public() -> None:
    entries = iter_gateway_route_inventory(_inventory_app())
    public_paths = {
        entry.path for entry in entries if entry.policy.access is RouteAccess.PUBLIC
    }

    assert public_paths == {
        "/healthz",
        "/readyz",
        "/metrics",
        "/legal/local-test-privacy",
    }


def test_extension_routes_use_extension_auth_not_gateway_bearer_auth() -> None:
    client = TestClient(_middleware_app())

    resp = client.get("/ext/whoami")

    assert resp.status_code == 401
    assert resp.json() == {"error": "missing_bearer_token"}
