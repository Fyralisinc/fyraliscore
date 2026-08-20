from __future__ import annotations

from pathlib import Path

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


def test_route_access_audit_accepts_local_privacy_document() -> None:
    rows = [
        {
            "methods": ["GET"],
            "path": "/legal/local-test-privacy",
            "name": "local_test_privacy",
            "tags": ["gateway-core"],
            "access": RouteAccess.PUBLIC.value,
            "gateway_bearer_required": False,
            "reason": "OAuth providers must be able to display the local privacy document",
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


def test_route_access_audit_rejects_public_substrate_route_in_production() -> None:
    rows = [
        {
            "methods": ["GET"],
            "path": "/models",
            "name": "get_models",
            "tags": ["substrate"],
            "access": RouteAccess.PUBLIC.value,
            "gateway_bearer_required": False,
            "reason": "public route",
        }
    ]

    errors = _check(rows, debug_endpoints_enabled=False, production=True)

    assert any(
        "substrate routes must remain gateway bearer-authenticated" in e
        for e in errors
    )


def test_route_access_audit_rejects_substrate_router_without_access_checks(
    tmp_path: Path,
) -> None:
    gateway_dir = tmp_path / "services" / "app" / "gateway"
    gateway_dir.mkdir(parents=True)
    (gateway_dir / "substrate_router.py").write_text(
        """
def build_substrate_router():
    return None
""".lstrip(),
        encoding="utf-8",
    )

    errors = _check(
        [],
        debug_endpoints_enabled=False,
        production=True,
        repo_root=tmp_path,
    )

    assert any(
        "substrate rows must be filtered through can_read" in e for e in errors
    )
    assert any("substrate override reads must be recorded" in e for e in errors)
    assert any("access_override_log" in e for e in errors)


def test_route_access_audit_accepts_current_production_source_invariants() -> None:
    assert _check([], debug_endpoints_enabled=False, production=True) == []
