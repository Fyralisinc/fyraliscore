"""AgentConfig — where the outbound-only Fyralis agent finds everything.

The agent runs in the **customer VPC**. It needs to know:

* the **console URL** to POST heartbeats to (outbound https only — I2),
* how often to heartbeat (``interval_s``) and how aggressively to retry when the
  console is unreachable (backoff knobs — I3),
* the local files that describe *this* deployment: the ``VERSION`` file, the
  identity (tenant/deployment/region), the signed **license**, the **trust
  root** it verifies signed config/release against (I6), and the local
  **/healthz** SLI source used to derive health.

Everything is environment-driven (prefix ``AGENT_``) with dev-sane defaults that
point at files inside ``agent/`` so the daemon and the test-suite read the same
config object. The model is frozen — config is read-only once built.

NOTE: there is no listen host/port here *by design* — the agent never listens
(I2). The only network egress it performs is the outbound POST to ``console_url``.
"""

from __future__ import annotations

import os
from pathlib import Path

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)
from lib import TelemetryTier
from lib.errors import ConfigError
from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = ["AgentConfig", "load_agent_config", "AGENT_DIR"]

AGENT_DIR = _bootstrap.AGENT_DIR
SIGNING_DIR = _bootstrap.SIGNING_DIR


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_opt(name: str) -> str | None:
    v = os.environ.get(name)
    return v if v not in (None, "") else None


def _read_secret(value_name: str, file_name: str) -> str | None:
    """Resolve a secret from ``$<value_name>`` or, failing that, the file at
    ``$<file_name>`` (so the token can be mounted as a read-only secret file the
    way the compose does, without ever putting it on a command line / in `env`).
    Returns ``None`` if neither is set / the file is empty or unreadable.
    """
    direct = _env_opt(value_name)
    if direct is not None:
        return direct
    path = _env_opt(file_name)
    if path is None:
        return None
    try:
        txt = Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return txt or None


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


class AgentConfig(BaseModel):
    """Immutable agent configuration resolved from the environment."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # --- console (the ONLY egress target) ---------------------------------
    console_url: str = Field(
        description="Base URL of the vendor console (outbound https). "
        "Heartbeats POST to <console_url>/api/v1/heartbeat (I2: outbound only)."
    )
    # Bearer token the agent presents on the console WRITE path (I4). Shipped in
    # the onboarding bundle (agent-config.json `console_token`) and mounted as a
    # secret; without it the console rejects heartbeats with 401. None/"" => the
    # agent sends no Authorization header (dev consoles with auth disabled).
    console_token: str | None = Field(
        default=None,
        description="Bearer token for the console write path (CONSOLE_INGEST_TOKEN).",
    )

    # --- identity of THIS deployment --------------------------------------
    tenant_id: str = Field(min_length=1)
    deployment_id: str = Field(min_length=1)
    region: str = Field(min_length=1)
    telemetry_tier: TelemetryTier = TelemetryTier.T1

    # --- local files describing this deployment ---------------------------
    version_file: Path = Field(description="Plain-text VERSION file (one line)")
    license_path: Path = Field(description="Signed license JSON (license bundle)")
    trust_root_path: Path = Field(
        description="signing/trust_root.json the agent verifies signed bundles against (I6)"
    )
    config_dir: Path = Field(
        description="Directory where pulled+verified config bundles are applied"
    )

    # --- local SLI / health source ----------------------------------------
    healthz_url: str = Field(
        description="Local data-plane health endpoint the agent probes (in-VPC, "
        "e.g. http://127.0.0.1:8080/healthz). Used to derive SLI-driven health."
    )
    healthz_timeout_s: float = Field(default=2.0, gt=0)

    # --- heartbeat / loop knobs -------------------------------------------
    interval_s: float = Field(
        default=30.0, gt=0, description="Seconds between heartbeat collections"
    )
    heartbeat_timeout_s: float = Field(default=5.0, gt=0)

    # --- buffer + backoff (I3: never block / crash on console outage) -----
    buffer_path: Path = Field(
        description="Append-only JSONL queue of heartbeats that failed to send"
    )
    buffer_max_records: int = Field(
        default=10_000, ge=1, description="Cap on buffered records (oldest dropped past this)"
    )
    backoff_base_s: float = Field(default=1.0, gt=0)
    backoff_max_s: float = Field(default=60.0, gt=0)

    # --- env fallback for version -----------------------------------------
    version_env: str | None = Field(
        default=None, description="Fallback version string if VERSION file is absent"
    )

    @field_validator("console_url", "healthz_url")
    @classmethod
    def _strip_trailing_slash(cls, v: str) -> str:
        v = v.strip()
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ConfigError(f"URL must be http(s)://, got {v!r}")
        return v.rstrip("/")

    @field_validator("telemetry_tier", mode="before")
    @classmethod
    def _coerce_tier(cls, v):
        return TelemetryTier.parse(v)

    @field_validator("backoff_max_s")
    @classmethod
    def _max_ge_base(cls, v: float, info) -> float:
        base = info.data.get("backoff_base_s")
        if base is not None and v < base:
            raise ConfigError("AGENT_BACKOFF_MAX_S must be >= AGENT_BACKOFF_BASE_S")
        return v

    # --- derived endpoints -------------------------------------------------

    @property
    def heartbeat_url(self) -> str:
        return f"{self.console_url}/api/v1/heartbeat"

    @property
    def register_url(self) -> str:
        return f"{self.console_url}/api/v1/register"


def load_agent_config(**overrides) -> AgentConfig:
    """Build an :class:`AgentConfig` from ``AGENT_*`` env vars + overrides.

    Defaults anchor at files inside ``agent/`` so a fresh checkout + a generated
    license/version is immediately runnable, and tests can override any field.
    """
    base = dict(
        console_url=_env("AGENT_CONSOLE_URL", "https://console:8080"),
        console_token=_read_secret("AGENT_CONSOLE_TOKEN", "AGENT_CONSOLE_TOKEN_FILE"),
        tenant_id=_env("AGENT_TENANT_ID", "acme"),
        deployment_id=_env("AGENT_DEPLOYMENT_ID", "acme-use1-0001"),
        region=_env("AGENT_REGION", "us-east-1"),
        telemetry_tier=_env("AGENT_TELEMETRY_TIER", "T1"),
        version_file=Path(_env("AGENT_VERSION_FILE", str(AGENT_DIR / "VERSION"))),
        license_path=Path(_env("AGENT_LICENSE_PATH", str(AGENT_DIR / "license.json"))),
        trust_root_path=Path(
            _env("AGENT_TRUST_ROOT", str(SIGNING_DIR / "trust_root.json"))
        ),
        config_dir=Path(_env("AGENT_CONFIG_DIR", str(AGENT_DIR / "applied-config"))),
        healthz_url=_env("AGENT_HEALTHZ_URL", "http://127.0.0.1:8088/healthz"),
        healthz_timeout_s=_env_float("AGENT_HEALTHZ_TIMEOUT_S", 2.0),
        interval_s=_env_float("AGENT_INTERVAL_S", 30.0),
        heartbeat_timeout_s=_env_float("AGENT_HEARTBEAT_TIMEOUT_S", 5.0),
        buffer_path=Path(_env("AGENT_BUFFER_PATH", str(AGENT_DIR / "buffer.jsonl"))),
        buffer_max_records=_env_int("AGENT_BUFFER_MAX_RECORDS", 10_000),
        backoff_base_s=_env_float("AGENT_BACKOFF_BASE_S", 1.0),
        backoff_max_s=_env_float("AGENT_BACKOFF_MAX_S", 60.0),
        version_env=_env_opt("AGENT_VERSION"),
    )
    base.update(overrides)
    try:
        return AgentConfig(**base)
    except ConfigError:
        raise
    except Exception as exc:  # pydantic ValidationError -> ConfigError
        raise ConfigError(f"invalid agent config: {exc}") from exc
