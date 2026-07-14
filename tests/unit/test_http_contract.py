"""HTTP route contract checks for frontend-consumed gateway routes."""
from __future__ import annotations

import json
from pathlib import Path

from services.app.gateway.main import build_app


REPO_ROOT = Path(__file__).resolve().parents[2]


def _demo_extension_installed() -> bool:
    from services.app.gateway.extensions import discovered_extensions, reset_for_tests

    reset_for_tests()
    for ext in discovered_extensions():
        if "demo" in ext.name.lower():
            return True
        if any(prefix.startswith("/v1/demo") for prefix in ext.public_path_prefixes):
            return True
        for router in ext.routers:
            for route in router.routes:
                if str(getattr(route, "path", "")).startswith("/v1/demo"):
                    return True
    return False


def test_contracted_gateway_routes_are_mounted() -> None:
    contract = json.loads(
        (REPO_ROOT / "contracts" / "http-routes.json").read_text()
    )
    demo_extension_installed = _demo_extension_installed()
    app = build_app(configure_logging=False)
    mounted = {
        (method, route.path)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }

    missing = [
        f"{route['method']} {route['path']} ({route['name']})"
        for route in contract["routes"]
        if demo_extension_installed or not route["path"].startswith("/v1/demo/")
        if (route["method"], route["path"]) not in mounted
    ]

    assert not missing, "contracted HTTP routes are not mounted:\n" + "\n".join(
        missing
    )
