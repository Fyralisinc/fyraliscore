"""Contract-driven mounting tests for provider-specific ingress routes."""
from __future__ import annotations

import httpx
import pytest
from fastapi import APIRouter, FastAPI

import services.app.gateway.ceo_view_wiring as ceo_view_wiring
import services.app.gateway.route_mounts as route_mounts
from services.app.gateway.settings import GatewaySettings
from services.app.webhooks.router import build_webhooks_router
from services.ingest.source_contract import DEDICATED_INGRESS_CATALOG


def _mount(*, debug: bool = False) -> FastAPI:
    app = FastAPI()
    route_mounts._mount_dedicated_ingress_routers(
        app,
        settings=GatewaySettings(debug_endpoints_enabled=debug),
    )
    return app


def test_every_contract_route_and_method_is_mounted_once() -> None:
    app = _mount()
    mounted = [
        (str(route.path), method)
        for route in app.routes
        for method in (getattr(route, "methods", None) or ())
    ]
    for ingress in DEDICATED_INGRESS_CATALOG.values():
        for method in ingress.methods:
            assert mounted.count((ingress.route_path, method)) == 1


def test_gateway_mount_orders_specific_push_routes_before_catchall() -> None:
    app = FastAPI()
    settings = GatewaySettings(
        debug_endpoints_enabled=False,
        finance_panel_enabled=False,
        slack_dm_panel_enabled=False,
    )
    route_mounts.mount_gateway_routes(
        app,
        settings=settings,
        emit_mount_logs=False,
    )
    paths = [str(route.path) for route in app.routes]
    catchall_index = paths.index("/webhooks/{provider}/{subpath:path}")

    for ingress_id in (
        "gmail_pubsub",
        "google_calendar_push",
        "google_drive_push",
    ):
        path = DEDICATED_INGRESS_CATALOG[ingress_id].route_path
        assert paths.index(path) < catchall_index


def test_whatsapp_debug_routes_follow_gateway_setting() -> None:
    production_paths = {route.path for route in _mount(debug=False).routes}
    debug_paths = {route.path for route in _mount(debug=True).routes}

    assert "/debug/whatsapp/register" not in production_paths
    assert "/debug/whatsapp/register" in debug_paths
    assert "/debug/whatsapp/recent" in debug_paths


@pytest.mark.asyncio
async def test_dedicated_gmail_route_precedes_generic_webhook_catchall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", raising=False)
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_ENDPOINT", raising=False)
    monkeypatch.delenv("GMAIL_PUBSUB_PUSH_OIDC_SA", raising=False)

    app = _mount()
    app.include_router(build_webhooks_router())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/webhooks/gmail/pubsub")

    assert response.status_code == 503
    assert response.json()["reason"] == "gmail_pubsub_oidc_env_missing"


def test_mount_fails_when_factory_does_not_serve_declared_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingress = DEDICATED_INGRESS_CATALOG["gmail_pubsub"]
    monkeypatch.setattr(
        route_mounts,
        "DEDICATED_INGRESS_DEFINITIONS",
        (ingress,),
    )
    monkeypatch.setattr(
        route_mounts,
        "resolve_callable_reference",
        lambda _binding: lambda: APIRouter(),
    )

    with pytest.raises(RuntimeError, match="did not mount POST"):
        _mount()


def test_legacy_ceo_view_push_mount_registry_is_absent() -> None:
    assert not hasattr(ceo_view_wiring, "_include_push_ingress_routers")
