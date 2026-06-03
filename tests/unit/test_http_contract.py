"""HTTP route contract checks for frontend-consumed gateway routes."""
from __future__ import annotations

import json
from pathlib import Path

from services.app.gateway.main import build_app


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_contracted_gateway_routes_are_mounted() -> None:
    contract = json.loads(
        (REPO_ROOT / "contracts" / "http-routes.json").read_text()
    )
    app = build_app(configure_logging=False)
    mounted = {
        (method, route.path)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }

    missing = [
        f"{route['method']} {route['path']} ({route['name']})"
        for route in contract["routes"]
        if (route["method"], route["path"]) not in mounted
    ]

    assert not missing, "contracted HTTP routes are not mounted:\n" + "\n".join(
        missing
    )
