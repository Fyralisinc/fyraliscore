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


@dataclass(frozen=True, slots=True)
class GatewaySettings:
    """Settings used by the gateway app factory and lifespan startup."""

    log_level: str = "INFO"
    ollama_url: str | None = None
    auth_bootstrap_secret: str | None = None
    ceo_view_enabled: bool = True
    require_realtime: bool = False
    require_github_integration: bool = False
    require_ingestion_data_plane: bool = False
    start_grt_scheduler: bool = True
    mount_sim: bool | None = None
    finance_panel_enabled: bool = True
    slack_dm_panel_enabled: bool = True
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

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
    ) -> "GatewaySettings":
        source = env if env is not None else os.environ
        return cls(
            log_level=source.get("LOG_LEVEL", "INFO"),
            ollama_url=source.get("OLLAMA_URL") or None,
            auth_bootstrap_secret=source.get("AUTH_BOOTSTRAP_SECRET") or None,
            ceo_view_enabled=_env_bool(
                source,
                "GATEWAY_CEO_VIEW_ENABLED",
                default=True,
            ),
            require_realtime=_env_bool(
                source,
                "GATEWAY_REQUIRE_REALTIME",
                default=False,
            ),
            require_github_integration=_env_bool(
                source,
                "GATEWAY_REQUIRE_GITHUB_INTEGRATION",
                default=False,
            ),
            require_ingestion_data_plane=_env_bool(
                source,
                "GATEWAY_REQUIRE_INGESTION_DATA_PLANE",
                default=False,
            ),
            start_grt_scheduler=_env_bool(
                source,
                "GATEWAY_START_GRT_SCHEDULER",
                default=True,
            ),
            mount_sim=_env_optional_bool(source, "GATEWAY_MOUNT_SIM"),
            finance_panel_enabled=_env_bool(
                source,
                "FINANCE_PANEL_ENABLED",
                default=True,
            ),
            slack_dm_panel_enabled=_env_bool(
                source,
                "SLACK_DM_PANEL_ENABLED",
                default=True,
            ),
            default_tenant_id=source.get("DEFAULT_TENANT_ID") or None,
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
