from __future__ import annotations

from fastapi import FastAPI

from services.app.gateway.route_mounts import _mount_native_connect_routers


def test_native_connect_routers_are_mounted() -> None:
    app = FastAPI()

    _mount_native_connect_routers(app, emit_mount_logs=False)

    paths = {route.path for route in app.routes}
    for source in (
        "ashby",
        "aws",
        "brex",
        "carta",
        "deel",
        "discord",
        "figma",
        "fireflies",
        "github",
        "gmail",
        "google_calendar",
        "google_drive",
        "grafana",
        "gusto",
        "hibob",
        "jira",
        "linkedin",
        "mercury",
        "miro",
        "notion",
        "quickbooks",
        "ramp",
        "signal",
        "slack",
        "telegram",
        "whatsapp",
        "facebook_pages",
    ):
        assert f"/integrations/{source}/connect/preflight" in paths
        assert f"/integrations/{source}/connect/finalize" in paths
