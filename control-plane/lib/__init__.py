"""Fyralis BYOC control plane — shared library.

The cross-cutting primitives and models every control-plane component depends on,
implemented exactly against the SPRINT_PLAN shared contracts (C1–C5, I1/I4/I6):

* :mod:`~control_plane.lib.tenant`     — ``TenantId`` + the read-only
  ``TenantRegistry`` over ``ca/tenant_registry.json`` (C1).
* :mod:`~control_plane.lib.tiers`      — ``TelemetryTier`` (T1/T2/T3) + the
  cumulative ``TierPolicy`` table (C3).
* :mod:`~control_plane.lib.deployment` — the C4 ``DeploymentRecord`` + health
  derivation (``green/yellow/red``).
* :mod:`~control_plane.lib.config`     — env-driven ``ControlPlaneConfig``.
* :mod:`~control_plane.lib.errors`     — shared error hierarchy.
* :mod:`~control_plane.lib.logging`    — structlog setup.
* :mod:`~control_plane.lib.primitives` — fingerprinting, canonical JSON, RFC-3339
  time (the P1 low-level primitives).

This module exports the *public* surface; downstream code should import from
``control_plane.lib`` rather than reaching into the submodules.
"""

from __future__ import annotations

from .config import (
    ControlPlaneConfig,
    control_plane_root,
    get_config,
    load_config,
)
from .deployment import (
    DEFAULT_RED_AFTER_S,
    DEFAULT_YELLOW_AFTER_S,
    DeploymentRecord,
    Health,
    derive_health,
)
from .desired_state import (
    ACTION_ALLOWLIST,
    DesiredState,
    compute_drift,
)
from .errors import (
    ConfigError,
    ControlPlaneError,
    DeploymentError,
    RegistryError,
    RegistryFormatError,
    RegistryNotFoundError,
    SignatureVerificationError,
    SigningError,
    TenantInactiveError,
    TenantNotFoundError,
    TenantRevokedError,
    TierError,
)
from .logging import configure_logging, get_logger
from .primitives import (
    canonical_json_bytes,
    canonical_json_str,
    fingerprint_der,
    fingerprint_pem,
    parse_rfc3339,
    sha256_hex,
    to_rfc3339,
    utcnow,
)
from .tenant import TenantId, TenantRecord, TenantRegistry
from .tiers import (
    TIER_POLICIES,
    SignalClass,
    TelemetryTier,
    TierPolicy,
    tier_policy,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # config
    "ControlPlaneConfig",
    "control_plane_root",
    "get_config",
    "load_config",
    # tenant / registry (C1)
    "TenantId",
    "TenantRecord",
    "TenantRegistry",
    # tiers (C3)
    "TelemetryTier",
    "TierPolicy",
    "SignalClass",
    "tier_policy",
    "TIER_POLICIES",
    # deployment (C4)
    "DeploymentRecord",
    "Health",
    "derive_health",
    "DEFAULT_YELLOW_AFTER_S",
    "DEFAULT_RED_AFTER_S",
    # desired state (console-roadmap §2/§4)
    "DesiredState",
    "compute_drift",
    "ACTION_ALLOWLIST",
    # primitives
    "utcnow",
    "to_rfc3339",
    "parse_rfc3339",
    "canonical_json_bytes",
    "canonical_json_str",
    "sha256_hex",
    "fingerprint_der",
    "fingerprint_pem",
    # logging
    "configure_logging",
    "get_logger",
    # errors
    "ControlPlaneError",
    "ConfigError",
    "RegistryError",
    "RegistryNotFoundError",
    "RegistryFormatError",
    "TenantNotFoundError",
    "TenantRevokedError",
    "TenantInactiveError",
    "TierError",
    "DeploymentError",
    "SigningError",
    "SignatureVerificationError",
]
