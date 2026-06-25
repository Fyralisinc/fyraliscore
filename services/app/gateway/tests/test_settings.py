from __future__ import annotations

import pytest
import httpx
from fastapi import FastAPI

from services.app.gateway.core_router import build_core_router
from services.app.gateway.settings import GatewaySettings


_PROD_BOOTSTRAP_SECRET = "prod-bootstrap-secret-32chars-minimum"


def test_gateway_sensitive_panels_default_disabled() -> None:
    settings = GatewaySettings.from_env({})
    assert settings.debug_endpoints_enabled is False
    assert settings.finance_panel_enabled is False
    assert settings.slack_dm_panel_enabled is False


def test_gateway_spec_demo_routes_default_to_dev_only_enabled() -> None:
    settings = GatewaySettings.from_env({})
    assert settings.spec_demo_routes_enabled is True
    assert settings.websocket_query_token_auth_enabled is False
    assert settings.websocket_session_cookie_name == "fyralis_session"
    assert settings.view_ceo_static_tokens_enabled is True

    disabled = GatewaySettings.from_env({"SPEC_DEMO_ROUTES_ENABLED": "0"})
    assert disabled.spec_demo_routes_enabled is False

    ws_disabled = GatewaySettings.from_env(
        {"WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0"}
    )
    assert ws_disabled.websocket_query_token_auth_enabled is False
    ws_enabled = GatewaySettings.from_env(
        {"WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "1"}
    )
    assert ws_enabled.websocket_query_token_auth_enabled is True

    custom_cookie = GatewaySettings.from_env(
        {"WEBSOCKET_SESSION_COOKIE_NAME": "fyralis_overlay_session"}
    )
    assert custom_cookie.websocket_session_cookie_name == "fyralis_overlay_session"

    with pytest.raises(ValueError, match="WEBSOCKET_SESSION_COOKIE_NAME"):
        GatewaySettings.from_env({"WEBSOCKET_SESSION_COOKIE_NAME": "bad name"})


def test_gateway_debug_endpoints_require_explicit_opt_in() -> None:
    settings = GatewaySettings.from_env({"DEBUG_ENDPOINTS_ENABLED": "1"})
    assert settings.debug_endpoints_enabled is True


def test_gateway_production_rejects_default_tenant_fallbacks() -> None:
    base = {
        "FYRALIS_ENV": "production",
        "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
        "DEBUG_ENDPOINTS_ENABLED": "0",
        "FINANCE_PANEL_ENABLED": "false",
        "SLACK_DM_PANEL_ENABLED": "false",
        "SPEC_DEMO_ROUTES_ENABLED": "0",
        "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
        "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
        "GATEWAY_MOUNT_SIM": "0",
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

    with pytest.raises(ValueError, match="DEFAULT_ACTOR_ID"):
        GatewaySettings.from_env(
            {**base, "DEFAULT_ACTOR_ID": "00000000-0000-0000-0000-000000000001"}
        )


def test_gateway_production_requires_bootstrap_secret_and_disabled_panels() -> None:
    with pytest.raises(ValueError, match="AUTH_BOOTSTRAP_SECRET"):
        GatewaySettings.from_env({"FYRALIS_ENV": "production"})

    with pytest.raises(ValueError, match="AUTH_BOOTSTRAP_SECRET.*32"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": "secret",
            }
        )

    with pytest.raises(ValueError, match="FINANCE_PANEL_ENABLED=false"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
            }
        )

    with pytest.raises(ValueError, match="SLACK_DM_PANEL_ENABLED"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "true",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="DEBUG_ENDPOINTS_ENABLED=.+production"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="DEBUG_ENDPOINTS_ENABLED"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "1",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="SPEC_DEMO_ROUTES_ENABLED"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "1",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "1",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="VIEW_CEO_STATIC_TOKENS_ENABLED"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "1",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="VIEW_CEO_STATIC_TOKENS"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
                "VIEW_CEO_STATIC_TOKENS": (
                    "token:00000000-0000-0000-0000-000000000001"
                ),
            }
        )

    with pytest.raises(ValueError, match="SPEC_DEMO_ROUTES_ENABLED=false"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED=false"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "0",
            }
        )

    with pytest.raises(ValueError, match="GATEWAY_MOUNT_SIM"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
                "GATEWAY_MOUNT_SIM": "1",
            }
        )

    with pytest.raises(ValueError, match="GATEWAY_MOUNT_SIM=false"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
                "DEBUG_ENDPOINTS_ENABLED": "0",
                "FINANCE_PANEL_ENABLED": "false",
                "SLACK_DM_PANEL_ENABLED": "false",
                "SPEC_DEMO_ROUTES_ENABLED": "0",
                "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
                "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
            }
        )

    settings = GatewaySettings.from_env(
        {
            "FYRALIS_ENV": "production",
            "AUTH_BOOTSTRAP_SECRET": _PROD_BOOTSTRAP_SECRET,
            "DEBUG_ENDPOINTS_ENABLED": "0",
            "FINANCE_PANEL_ENABLED": "false",
            "SLACK_DM_PANEL_ENABLED": "false",
            "SPEC_DEMO_ROUTES_ENABLED": "0",
            "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
            "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
            "GATEWAY_MOUNT_SIM": "0",
        }
    )
    assert settings.auth_bootstrap_secret == _PROD_BOOTSTRAP_SECRET
    assert settings.debug_endpoints_enabled is False
    assert settings.finance_panel_enabled is False
    assert settings.slack_dm_panel_enabled is False
    assert settings.spec_demo_routes_enabled is False
    assert settings.websocket_query_token_auth_enabled is False
    assert settings.websocket_session_cookie_name == "fyralis_session"
    assert settings.mount_sim is False


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
