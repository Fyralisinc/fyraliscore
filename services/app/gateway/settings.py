"""Gateway application settings resolved once at app construction."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


def _env_bool(
    env: Mapping[str, str],
    name: str,
    *,
    default: bool,
) -> bool:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"{name} must be a boolean value: 1/0, true/false, yes/no, on/off"
    )


def _env_optional_bool(
    env: Mapping[str, str],
    name: str,
) -> bool | None:
    raw = env.get(name)
    if raw is None or raw == "":
        return None
    return _env_bool(env, name, default=False)


def _env_name(source: Mapping[str, str]) -> str:
    values = [
        (source.get("FYRALIS_ENV") or "").strip().lower(),
        (source.get("COMPANY_OS_ENV") or "").strip().lower(),
        (source.get("APP_ENV") or "").strip().lower(),
        (source.get("ENVIRONMENT") or "").strip().lower(),
    ]
    if any(value in {"prod", "production"} for value in values):
        return "prod"
    return next((value for value in values if value), "development")


def _is_production(env_name: str) -> bool:
    return env_name in {"prod", "production"}


def _required_disabled_in_production(
    env: Mapping[str, str],
    name: str,
    *,
    production: bool,
) -> bool:
    if not production:
        return _env_bool(env, name, default=False)
    if name not in env or env.get(name, "") == "":
        raise ValueError(f"{name}=false must be set explicitly in production")
    enabled = _env_bool(env, name, default=False)
    if enabled:
        raise ValueError(f"{name} must be disabled in production")
    return False


def _required_bool_in_production(
    env: Mapping[str, str],
    name: str,
    *,
    production: bool,
    default: bool,
) -> bool:
    if not production:
        return _env_bool(env, name, default=default)
    if name not in env or env.get(name, "") == "":
        raise ValueError(f"{name}=0 or 1 must be set explicitly in production")
    return _env_bool(env, name, default=default)


def _required_enabled_in_production(
    env: Mapping[str, str],
    name: str,
    *,
    production: bool,
    default: bool,
) -> bool:
    value = _required_bool_in_production(
        env,
        name,
        production=production,
        default=default,
    )
    if production and not value:
        raise ValueError(f"{name}=1 must be set in production")
    return value


def _demo_routes_enabled(
    env: Mapping[str, str],
    name: str,
    *,
    production: bool,
) -> bool:
    if production:
        return _required_disabled_in_production(
            env,
            name,
            production=production,
        )
    return _env_bool(env, name, default=True)


def _env_float(
    env: Mapping[str, str],
    name: str,
    *,
    default: float,
) -> float:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _env_cookie_name(
    env: Mapping[str, str],
    name: str,
    *,
    default: str,
) -> str:
    raw = env.get(name)
    value = default if raw is None or raw == "" else raw.strip()
    if not value:
        raise ValueError(f"{name} must not be empty")
    if any(char in value for char in " \t\r\n;=,"):
        raise ValueError(f"{name} must be a valid cookie name")
    return value


_MIN_BOOTSTRAP_SECRET_LENGTH = 32
_UNSAFE_BOOTSTRAP_SECRET_VALUES = frozenset(
    {
        "secret",
        "password",
        "changeme",
        "change-me",
        "change_me",
        "dev-secret",
        "test-secret",
        "bootstrap-secret",
    }
)


def _auth_bootstrap_secret(
    source: Mapping[str, str],
    *,
    production: bool,
) -> str | None:
    value = (source.get("AUTH_BOOTSTRAP_SECRET") or "").strip()
    if not value:
        if production:
            raise ValueError("AUTH_BOOTSTRAP_SECRET must be set in production")
        return None
    if production:
        if len(value) < _MIN_BOOTSTRAP_SECRET_LENGTH:
            raise ValueError(
                "AUTH_BOOTSTRAP_SECRET must be at least "
                f"{_MIN_BOOTSTRAP_SECRET_LENGTH} characters in production",
            )
        if value.lower() in _UNSAFE_BOOTSTRAP_SECRET_VALUES:
            raise ValueError(
                "AUTH_BOOTSTRAP_SECRET must not use a known unsafe placeholder",
            )
    return value


@dataclass(frozen=True, slots=True)
class GatewaySettings:
    """Settings used by the gateway app factory and lifespan startup."""

    log_level: str = "INFO"
    environment: str = "development"
    ollama_url: str | None = None
    auth_bootstrap_secret: str | None = None
    ceo_view_enabled: bool = True
    require_realtime: bool = False
    require_github_integration: bool = False
    require_ingestion_data_plane: bool = False
    start_grt_scheduler: bool = True
    mount_sim: bool | None = None
    debug_endpoints_enabled: bool = False
    finance_panel_enabled: bool = False
    slack_dm_panel_enabled: bool = False
    spec_demo_routes_enabled: bool = True
    websocket_query_token_auth_enabled: bool = False
    websocket_session_cookie_name: str = "fyralis_session"
    view_ceo_static_tokens_enabled: bool = True
    default_tenant_id: str | None = None
    view_ceo_token: str = "ceo-dogfood-token"
    view_ceo_display_name: str = "Rachin"
    view_ceo_timezone: str = "Asia/Kathmandu"
    kafka_bootstrap_servers: str | None = None
    s3_raw_bucket: str = "fyralis-raw"
    s3_endpoint_url: str | None = None
    oauth_sweep_interval_s: float = 300.0
    db_startup_timeout_s: float = 30.0
    integration_runtime_probe_timeout_s: float = 5.0
    realtime_startup_timeout_s: float = 10.0
    ceo_view_startup_timeout_s: float = 30.0
    ingestion_data_plane_startup_timeout_s: float = 30.0

    @property
    def is_production(self) -> bool:
        return _is_production(self.environment)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
    ) -> "GatewaySettings":
        source = env if env is not None else os.environ
        environment = _env_name(source)
        production = _is_production(environment)
        auth_bootstrap_secret = _auth_bootstrap_secret(
            source,
            production=production,
        )
        default_tenant_id = source.get("DEFAULT_TENANT_ID") or None
        company_os_tenant_id = source.get("COMPANY_OS_TENANT_ID") or None
        if production and (default_tenant_id or company_os_tenant_id):
            raise ValueError(
                "DEFAULT_TENANT_ID and COMPANY_OS_TENANT_ID must be unset "
                "in production",
            )
        if production and source.get("DEFAULT_ACTOR_ID"):
            raise ValueError(
                "DEFAULT_ACTOR_ID must be unset in production; actor identity "
                "must come from gateway auth",
            )
        if production and source.get("VIEW_CEO_STATIC_TOKENS"):
            raise ValueError(
                "VIEW_CEO_STATIC_TOKENS must be unset in production; use "
                "gateway actor sessions or customer IdP-backed auth instead",
            )
        return cls(
            log_level=source.get("LOG_LEVEL", "INFO"),
            environment=environment,
            ollama_url=source.get("OLLAMA_URL") or None,
            auth_bootstrap_secret=auth_bootstrap_secret,
            ceo_view_enabled=_env_bool(
                source,
                "GATEWAY_CEO_VIEW_ENABLED",
                default=True,
            ),
            require_realtime=_required_bool_in_production(
                source,
                "GATEWAY_REQUIRE_REALTIME",
                production=production,
                default=False,
            ),
            require_github_integration=_required_bool_in_production(
                source,
                "GATEWAY_REQUIRE_GITHUB_INTEGRATION",
                production=production,
                default=False,
            ),
            require_ingestion_data_plane=_required_enabled_in_production(
                source,
                "GATEWAY_REQUIRE_INGESTION_DATA_PLANE",
                production=production,
                default=False,
            ),
            start_grt_scheduler=_required_bool_in_production(
                source,
                "GATEWAY_START_GRT_SCHEDULER",
                production=production,
                default=True,
            ),
            debug_endpoints_enabled=_required_disabled_in_production(
                source,
                "DEBUG_ENDPOINTS_ENABLED",
                production=production,
            ),
            finance_panel_enabled=_required_disabled_in_production(
                source,
                "FINANCE_PANEL_ENABLED",
                production=production,
            ),
            slack_dm_panel_enabled=_required_disabled_in_production(
                source,
                "SLACK_DM_PANEL_ENABLED",
                production=production,
            ),
            spec_demo_routes_enabled=_demo_routes_enabled(
                source,
                "SPEC_DEMO_ROUTES_ENABLED",
                production=production,
            ),
            websocket_query_token_auth_enabled=(
                _required_disabled_in_production(
                    source,
                    "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED",
                    production=production,
                )
                if production
                else _env_bool(
                    source,
                    "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED",
                    default=False,
                )
            ),
            websocket_session_cookie_name=_env_cookie_name(
                source,
                "WEBSOCKET_SESSION_COOKIE_NAME",
                default="fyralis_session",
            ),
            view_ceo_static_tokens_enabled=_demo_routes_enabled(
                source,
                "VIEW_CEO_STATIC_TOKENS_ENABLED",
                production=production,
            ),
            mount_sim=(
                _required_disabled_in_production(
                    source,
                    "GATEWAY_MOUNT_SIM",
                    production=production,
                )
                if production
                else _env_optional_bool(source, "GATEWAY_MOUNT_SIM")
            ),
            default_tenant_id=default_tenant_id,
            view_ceo_token=source.get("VIEW_CEO_TOKEN") or "ceo-dogfood-token",
            view_ceo_display_name=source.get("VIEW_CEO_DISPLAY_NAME") or "Rachin",
            view_ceo_timezone=source.get("VIEW_CEO_TIMEZONE") or "Asia/Kathmandu",
            kafka_bootstrap_servers=source.get("KAFKA_BOOTSTRAP_SERVERS")
            or None,
            s3_raw_bucket=source.get("S3_RAW_BUCKET") or "fyralis-raw",
            s3_endpoint_url=source.get("S3_ENDPOINT_URL") or None,
            oauth_sweep_interval_s=_env_float(
                source,
                "GATEWAY_OAUTH_SWEEP_INTERVAL_S",
                default=300.0,
            ),
            db_startup_timeout_s=_env_float(
                source,
                "GATEWAY_DB_STARTUP_TIMEOUT_S",
                default=30.0,
            ),
            integration_runtime_probe_timeout_s=_env_float(
                source,
                "GATEWAY_INTEGRATION_RUNTIME_PROBE_TIMEOUT_S",
                default=5.0,
            ),
            realtime_startup_timeout_s=_env_float(
                source,
                "GATEWAY_REALTIME_STARTUP_TIMEOUT_S",
                default=10.0,
            ),
            ceo_view_startup_timeout_s=_env_float(
                source,
                "GATEWAY_CEO_VIEW_STARTUP_TIMEOUT_S",
                default=30.0,
            ),
            ingestion_data_plane_startup_timeout_s=_env_float(
                source,
                "GATEWAY_INGESTION_DATA_PLANE_STARTUP_TIMEOUT_S",
                default=30.0,
            ),
        )


__all__ = ["GatewaySettings"]
