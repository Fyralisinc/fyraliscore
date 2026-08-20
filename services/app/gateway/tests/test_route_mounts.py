from __future__ import annotations

from fastapi import FastAPI

from services.ingest.connector_platform.install_router import build_install_router
from services.ingest.integrations.facebook_pages.oauth import (
    router as facebook_pages_oauth_router,
)
from services.ingest.integrations.instagram.oauth import (
    router as instagram_oauth_router,
)
from services.ingest.integrations.router import build_integrations_router


def test_contract_install_and_supplemental_oauth_routes_are_mounted() -> None:
    app = FastAPI()
    app.include_router(facebook_pages_oauth_router)
    app.include_router(instagram_oauth_router)
    app.include_router(build_integrations_router())
    app.include_router(build_install_router())

    paths = set(app.openapi()["paths"])
    assert "/integrations/{source}/install" in paths
    assert "/integrations/{source}/callback" in paths
    assert "/integrations/facebook_pages/install" in paths
    assert "/integrations/facebook_pages/callback" in paths
    assert "/integrations/instagram/install" in paths
    assert "/integrations/instagram/callback" in paths
