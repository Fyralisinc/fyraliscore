from __future__ import annotations

from scripts.audit_gateway_route_access import _check
from services.app.gateway.route_access import RouteAccess


def test_route_access_audit_accepts_bearer_authenticated_ingest() -> None:
    rows = [
        {
            "methods": ["POST"],
            "path": "/ingest/{channel:path}",
            "name": "post_ingest",
            "tags": ["gateway-core"],
            "access": RouteAccess.BEARER.value,
            "gateway_bearer_required": True,
            "reason": "default gateway actor-session bearer auth",
        }
    ]

    assert _check(rows, debug_endpoints_enabled=False) == []


def test_route_access_audit_rejects_public_ingest() -> None:
    rows = [
        {
            "methods": ["POST"],
            "path": "/ingest/{channel:path}",
            "name": "post_ingest",
            "tags": ["gateway-core"],
            "access": RouteAccess.PROVIDER_SIGNED.value,
            "gateway_bearer_required": False,
            "reason": "provider webhook authenticated by provider signature",
        }
    ]

    errors = _check(rows, debug_endpoints_enabled=False)

    assert len(errors) == 1
    assert "/ingest/{channel}" in errors[0]


def test_route_access_audit_rejects_generic_bearer_admin_route() -> None:
    rows = [
        {
            "methods": ["GET"],
            "path": "/api/admin/dead-letters",
            "name": "list_dead_letters",
            "tags": ["admin"],
            "access": RouteAccess.BEARER.value,
            "gateway_bearer_required": True,
            "reason": "default gateway actor-session bearer auth",
        }
    ]

    errors = _check(rows, debug_endpoints_enabled=False)

    assert len(errors) == 1
    assert "/api/admin/*" in errors[0]


def test_route_access_audit_rejects_spec_routes_in_production() -> None:
    rows = [
        {
            "methods": ["GET"],
            "path": "/v1/spec/forecasts",
            "name": "list_forecasts",
            "tags": ["spec"],
            "access": RouteAccess.BEARER.value,
            "gateway_bearer_required": True,
            "reason": "default gateway actor-session bearer auth",
        }
    ]

    errors = _check(rows, debug_endpoints_enabled=False, production=True)

    assert len(errors) == 1
    assert "production route must not mount /v1/spec" in errors[0]


def test_route_access_audit_accepts_current_production_source_invariants() -> None:
    assert _check([], debug_endpoints_enabled=False, production=True) == []
