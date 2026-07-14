"""Control-plane configuration, loaded from the environment with sane defaults.

A single, immutable ``ControlPlaneConfig`` that every CP service reads so that
"where is the registry", "what is the Mimir URL", "what port does the auth proxy
listen on", and "where is the trust root" have one answer derived consistently
from the environment.

Design notes
------------
* **Defaults are dev-friendly, prod-overridable.** Every value has a sane default
  pointing at the in-repo ``ca/`` location and the compose service names from C5
  (``mimir``, ``loki`` on ``cp-net``), and every value is overridable by an
  environment variable prefixed ``CP_``.
* **Path resolution is anchored at the control-plane root**, discovered by
  walking up from this file until the directory containing ``SPRINT_PLAN.md`` is
  found. That makes ``tenant_registry.json`` resolve correctly whether the code
  runs from the repo, a container, or a test.
* The model is a frozen pydantic model so config is read-only once built.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .errors import ConfigError

__all__ = [
    "ControlPlaneConfig",
    "control_plane_root",
    "load_config",
    "get_config",
]


def control_plane_root() -> Path:
    """Locate the ``control-plane/`` root directory.

    Honors ``CP_ROOT`` if set; otherwise walks up from this file looking for the
    directory that contains ``SPRINT_PLAN.md`` (the authoritative marker). Falls
    back to this module's parent's parent (``lib/`` → ``control-plane/``).
    """
    env_root = os.environ.get("CP_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    here = Path(__file__).resolve()
    for candidate in here.parents:
        if (candidate / "SPRINT_PLAN.md").is_file():
            return candidate
    # Fallback: lib/ is directly under control-plane/.
    return here.parent.parent


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


class ControlPlaneConfig(BaseModel):
    """Immutable control-plane configuration resolved from the environment."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # --- filesystem anchors -------------------------------------------------
    root: Path = Field(description="control-plane/ root directory")
    ca_dir: Path = Field(description="Trust-root directory (CA + tenant registry)")
    tenant_registry_path: Path = Field(
        description="Path to ca/tenant_registry.json (read by the registry reader)"
    )
    trust_root_path: Path = Field(
        description="Path to the Fyralis CA bundle the auth proxy verifies against"
    )
    signing_keyring_path: Path = Field(
        description="Path to the ed25519 keyring (key_id -> keys) used for C2 signing"
    )

    # --- listeners ----------------------------------------------------------
    auth_proxy_port: int = Field(default=8443, ge=1, le=65535)
    console_port: int = Field(default=8081, ge=1, le=65535)
    onboarding_port: int = Field(default=8082, ge=1, le=65535)
    metrics_port: int = Field(
        default=9464, ge=1, le=65535, description="Prometheus /metrics scrape port"
    )

    # --- upstreams (C5: cp-net service names) -------------------------------
    mimir_url: str = Field(default="http://mimir:9009")
    loki_url: str = Field(default="http://loki:3100")
    grafana_url: str = Field(default="http://grafana:3000")

    # --- multi-tenancy header (C5) -----------------------------------------
    scope_org_header: str = Field(
        default="X-Scope-OrgID",
        description="Header the auth proxy injects with the verified tenant_id",
    )

    # --- behavior knobs -----------------------------------------------------
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")
    heartbeat_yellow_after_s: int = Field(
        default=90,
        ge=1,
        description="Heartbeat age (s) past which health degrades to yellow",
    )
    heartbeat_red_after_s: int = Field(
        default=300,
        ge=1,
        description="Heartbeat age (s) past which health degrades to red",
    )

    @field_validator("log_format")
    @classmethod
    def _valid_format(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in {"json", "console"}:
            raise ConfigError(f"CP_LOG_FORMAT must be json|console, got {v!r}")
        return v

    @field_validator("heartbeat_red_after_s")
    @classmethod
    def _red_after_yellow(cls, v: int, info) -> int:
        yellow = info.data.get("heartbeat_yellow_after_s")
        if yellow is not None and v <= yellow:
            raise ConfigError(
                "CP_HEARTBEAT_RED_AFTER_S must be greater than "
                "CP_HEARTBEAT_YELLOW_AFTER_S"
            )
        return v


def load_config(**overrides) -> ControlPlaneConfig:
    """Build a :class:`ControlPlaneConfig` from the environment + overrides.

    Reads ``CP_*`` environment variables, falls back to dev-sane defaults, and
    lets explicit keyword overrides win (used by tests and embedding callers).
    """
    root = control_plane_root()
    ca_dir = Path(_env("CP_CA_DIR", str(root / "ca")))

    base = dict(
        root=root,
        ca_dir=ca_dir,
        tenant_registry_path=Path(
            _env("CP_TENANT_REGISTRY", str(ca_dir / "tenant_registry.json"))
        ),
        trust_root_path=Path(
            _env("CP_TRUST_ROOT", str(ca_dir / "fyralis-ca.crt"))
        ),
        signing_keyring_path=Path(
            _env("CP_SIGNING_KEYRING", str(root / "signing" / "keyring.json"))
        ),
        auth_proxy_port=_env_int("CP_AUTH_PROXY_PORT", 8443),
        console_port=_env_int("CP_CONSOLE_PORT", 8081),
        onboarding_port=_env_int("CP_ONBOARDING_PORT", 8082),
        metrics_port=_env_int("CP_METRICS_PORT", 9464),
        mimir_url=_env("CP_MIMIR_URL", "http://mimir:9009"),
        loki_url=_env("CP_LOKI_URL", "http://loki:3100"),
        grafana_url=_env("CP_GRAFANA_URL", "http://grafana:3000"),
        scope_org_header=_env("CP_SCOPE_ORG_HEADER", "X-Scope-OrgID"),
        log_level=_env("CP_LOG_LEVEL", "INFO"),
        log_format=_env("CP_LOG_FORMAT", "json"),
        heartbeat_yellow_after_s=_env_int("CP_HEARTBEAT_YELLOW_AFTER_S", 90),
        heartbeat_red_after_s=_env_int("CP_HEARTBEAT_RED_AFTER_S", 300),
    )
    base.update(overrides)
    try:
        return ControlPlaneConfig(**base)
    except ConfigError:
        raise
    except Exception as exc:  # pydantic ValidationError → ConfigError
        raise ConfigError(f"invalid control-plane config: {exc}") from exc


@lru_cache(maxsize=1)
def get_config() -> ControlPlaneConfig:
    """Process-wide cached config (cleared by calling ``get_config.cache_clear()``)."""
    return load_config()
