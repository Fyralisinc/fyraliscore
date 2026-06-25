from __future__ import annotations

import pytest
import httpx
from fastapi import FastAPI

import lib.shared.secrets.provider_contract as provider_contract
from services.app.gateway.core_router import build_core_router
from services.app.gateway.settings import GatewaySettings


_PROD_BOOTSTRAP_SECRET = "prod-bootstrap-secret-32chars-minimum"
_PROD_BOOTSTRAP_SECRET_REF = "prod/fyralis/auth-bootstrap-secret"
_SHORT_BOOTSTRAP_SECRET_REF = "prod/fyralis/auth-bootstrap-secret-short"


@pytest.fixture(autouse=True)
def _stub_managed_auth_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_aws_loader(config) -> bytes:
        if config.master_kek_secret_ref == _SHORT_BOOTSTRAP_SECRET_REF:
            return b"secret"
        if config.master_kek_secret_ref == _PROD_BOOTSTRAP_SECRET_REF:
            return _PROD_BOOTSTRAP_SECRET.encode("utf-8")
        return b"managed-secret-value"

    monkeypatch.setattr(
        provider_contract,
        "_load_from_aws_secrets_manager",
        _fake_aws_loader,
    )


def _prod_env(**overrides: str) -> dict[str, str]:
    values = {
        "FYRALIS_ENV": "production",
        "SECRET_STORE_BACKEND": "fernet",
        "MASTER_KEK_PROVIDER": "aws-secrets-manager",
        "MASTER_KEK_SECRET_REF": "prod/fyralis/master-kek",
        "AUTH_BOOTSTRAP_SECRET_SECRET_REF": _PROD_BOOTSTRAP_SECRET_REF,
        "DEBUG_ENDPOINTS_ENABLED": "0",
        "FINANCE_PANEL_ENABLED": "false",
        "SLACK_DM_PANEL_ENABLED": "false",
        "SPEC_DEMO_ROUTES_ENABLED": "0",
        "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
        "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
        "GATEWAY_MOUNT_SIM": "0",
        "GATEWAY_REQUIRE_REALTIME": "0",
        "GATEWAY_REQUIRE_GITHUB_INTEGRATION": "0",
        "GATEWAY_REQUIRE_INGESTION_DATA_PLANE": "1",
        "GATEWAY_START_GRT_SCHEDULER": "1",
    }
    values.update(overrides)
    return values


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
    base = _prod_env()

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


def test_gateway_production_detection_uses_all_runtime_env_labels() -> None:
    company_only = _prod_env()
    company_only.pop("FYRALIS_ENV")
    company_only["COMPANY_OS_ENV"] = "prod"
    settings = GatewaySettings.from_env(company_only)
    assert settings.is_production is True
    assert settings.environment == "prod"
    assert settings.spec_demo_routes_enabled is False

    mixed_label = _prod_env()
    mixed_label["FYRALIS_ENV"] = "staging"
    mixed_label["APP_ENV"] = "production"
    settings = GatewaySettings.from_env(mixed_label)
    assert settings.is_production is True
    assert settings.environment == "prod"

    missing_explicit_prod_guard = _prod_env()
    missing_explicit_prod_guard.pop("FYRALIS_ENV")
    missing_explicit_prod_guard["COMPANY_OS_ENV"] = "production"
    missing_explicit_prod_guard.pop("DEBUG_ENDPOINTS_ENABLED")
    with pytest.raises(ValueError, match="DEBUG_ENDPOINTS_ENABLED"):
        GatewaySettings.from_env(missing_explicit_prod_guard)


def test_gateway_production_requires_bootstrap_secret_and_disabled_panels() -> None:
    with pytest.raises(ValueError, match="AUTH_BOOTSTRAP_SECRET"):
        GatewaySettings.from_env({"FYRALIS_ENV": "production"})

    with pytest.raises(ValueError, match="AUTH_BOOTSTRAP_SECRET.*32"):
        GatewaySettings.from_env(
            {
                "FYRALIS_ENV": "production",
                "SECRET_STORE_BACKEND": "fernet",
                "MASTER_KEK_PROVIDER": "aws-secrets-manager",
                "MASTER_KEK_SECRET_REF": "prod/fyralis/master-kek",
                "AUTH_BOOTSTRAP_SECRET_SECRET_REF": _SHORT_BOOTSTRAP_SECRET_REF,
            }
        )

    with pytest.raises(ValueError, match="FINANCE_PANEL_ENABLED=false"):
        values = _prod_env(DEBUG_ENDPOINTS_ENABLED="0")
        values.pop("FINANCE_PANEL_ENABLED")
        GatewaySettings.from_env(values)

    with pytest.raises(ValueError, match="SLACK_DM_PANEL_ENABLED"):
        GatewaySettings.from_env(_prod_env(SLACK_DM_PANEL_ENABLED="true"))

    with pytest.raises(ValueError, match="DEBUG_ENDPOINTS_ENABLED=.+production"):
        values = _prod_env()
        values.pop("DEBUG_ENDPOINTS_ENABLED")
        GatewaySettings.from_env(values)

    with pytest.raises(ValueError, match="DEBUG_ENDPOINTS_ENABLED"):
        GatewaySettings.from_env(_prod_env(DEBUG_ENDPOINTS_ENABLED="1"))

    with pytest.raises(ValueError, match="SPEC_DEMO_ROUTES_ENABLED"):
        GatewaySettings.from_env(_prod_env(SPEC_DEMO_ROUTES_ENABLED="1"))

    with pytest.raises(ValueError, match="WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED"):
        GatewaySettings.from_env(_prod_env(WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED="1"))

    with pytest.raises(ValueError, match="VIEW_CEO_STATIC_TOKENS_ENABLED"):
        GatewaySettings.from_env(_prod_env(VIEW_CEO_STATIC_TOKENS_ENABLED="1"))

    with pytest.raises(ValueError, match="VIEW_CEO_STATIC_TOKENS"):
        GatewaySettings.from_env(
            _prod_env(
                **{
                    "VIEW_CEO_STATIC_TOKENS": (
                        "token:00000000-0000-0000-0000-000000000001"
                    ),
                },
            )
        )

    with pytest.raises(ValueError, match="SPEC_DEMO_ROUTES_ENABLED=false"):
        values = _prod_env()
        values.pop("SPEC_DEMO_ROUTES_ENABLED")
        GatewaySettings.from_env(values)

    with pytest.raises(ValueError, match="WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED=false"):
        values = _prod_env()
        values.pop("WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED")
        GatewaySettings.from_env(values)

    with pytest.raises(ValueError, match="GATEWAY_MOUNT_SIM"):
        GatewaySettings.from_env(_prod_env(GATEWAY_MOUNT_SIM="1"))

    with pytest.raises(ValueError, match="GATEWAY_MOUNT_SIM=false"):
        values = _prod_env()
        values.pop("GATEWAY_MOUNT_SIM")
        GatewaySettings.from_env(values)

    settings = GatewaySettings.from_env(_prod_env())
    assert settings.auth_bootstrap_secret == _PROD_BOOTSTRAP_SECRET
    assert settings.debug_endpoints_enabled is False
    assert settings.finance_panel_enabled is False
    assert settings.slack_dm_panel_enabled is False
    assert settings.spec_demo_routes_enabled is False
    assert settings.websocket_query_token_auth_enabled is False
    assert settings.websocket_session_cookie_name == "fyralis_session"
    assert settings.mount_sim is False
    assert settings.require_realtime is False
    assert settings.require_github_integration is False
    assert settings.require_ingestion_data_plane is True
    assert settings.start_grt_scheduler is True


def test_gateway_production_requires_explicit_runtime_intent_flags() -> None:
    for key in (
        "GATEWAY_REQUIRE_REALTIME",
        "GATEWAY_REQUIRE_GITHUB_INTEGRATION",
        "GATEWAY_START_GRT_SCHEDULER",
    ):
        values = _prod_env()
        values.pop(key)
        with pytest.raises(ValueError, match=key):
            GatewaySettings.from_env(values)

    values = _prod_env()
    values.pop("GATEWAY_REQUIRE_INGESTION_DATA_PLANE")
    with pytest.raises(ValueError, match="GATEWAY_REQUIRE_INGESTION_DATA_PLANE"):
        GatewaySettings.from_env(values)

    with pytest.raises(ValueError, match="GATEWAY_REQUIRE_INGESTION_DATA_PLANE=1"):
        GatewaySettings.from_env(
            _prod_env(GATEWAY_REQUIRE_INGESTION_DATA_PLANE="0")
        )

    settings = GatewaySettings.from_env(
        _prod_env(
            GATEWAY_REQUIRE_REALTIME="1",
            GATEWAY_REQUIRE_GITHUB_INTEGRATION="1",
            GATEWAY_START_GRT_SCHEDULER="0",
        )
    )
    assert settings.require_realtime is True
    assert settings.require_github_integration is True
    assert settings.start_grt_scheduler is False


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
