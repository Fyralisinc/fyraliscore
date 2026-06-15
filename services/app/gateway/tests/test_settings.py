from __future__ import annotations

import pytest
import httpx
from fastapi import FastAPI

from services.app.gateway.core_router import build_core_router
from services.app.gateway.settings import GatewaySettings


def test_gateway_sensitive_panels_default_disabled() -> None:
    settings = GatewaySettings.from_env({})
    assert settings.finance_panel_enabled is False
    assert settings.slack_dm_panel_enabled is False


def test_gateway_production_rejects_default_tenant_fallbacks() -> None:
    base = {
        "FYRALIS_ENV": "production",
        "AUTH_BOOTSTRAP_SECRET": "secret",
        "FINANCE_PANEL_ENABLED": "false",
        "SLACK_DM_PANEL_ENABLED": "false",
    }

    with pytest.raises(ValueError, match="DEFAULT_TENANT_ID"):
        GatewaySettings.from_env(
            {**base, "DEFAULT_TENANT_ID": "00000000-0000-0000-0000-000000000001"}
        )

    with pytest.raises(ValueError, match="COMPANY_OS_TENANT_ID"):
        GatewaySettings.from_env(
            {
                **base,
                "COMPANY_OS_TENANT_ID": "00000000-0000-0000-0000-000000000001",
            }
        )


def test_gateway_production_requires_bootstrap_secret_and_disabled_panels() -> None:
    with pytest.raises(ValueError, match="AUTH_BOOTSTRAP_SECRET"):
        GatewaySettings.from_env({"FYRALIS_ENV": "production"})

    with pytest.raises(ValueError, match="FINANCE_PANEL_ENABLED=false"):
        GatewaySettings.from_env(
            {"FYRALIS_ENV": "production", "AUTH_BOOTSTRAP_SECRET": "secret"}
        )

    with pytest.raises(ValueError, match="SLACK_DM_PANEL_ENABLED"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": "secret",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "true",
            }
        )

    settings = GatewaySettings.from_env(
        {
            "FYRALIS_ENV": "production",
            "AUTH_BOOTSTRAP_SECRET": "secret",
            "FINANCE_PANEL_ENABLED": "false",
            "SLACK_DM_PANEL_ENABLED": "false",
        }
    )
    assert settings.auth_bootstrap_secret == "secret"
    assert settings.finance_panel_enabled is False
    assert settings.slack_dm_panel_enabled is False


@pytest.mark.asyncio
async def test_auth_session_fails_closed_without_bootstrap_secret_in_production() -> None:
    app = FastAPI()
    app.include_router(build_core_router())
    app.state.deps = object()
    app.state.gateway_settings = GatewaySettings(environment="production")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/auth/session",
            json={
                "actor_id": "00000000-0000-0000-0000-000000000001",
                "tenant_id": "00000000-0000-0000-0000-000000000002",
            },
        )

    assert resp.status_code == 503
    assert resp.json() == {"error": "bootstrap_secret_required"}
