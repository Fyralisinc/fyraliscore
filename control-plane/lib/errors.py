"""Shared error types for the Fyralis BYOC control plane.

Every control-plane component imports these so that failures speak a common
vocabulary across the auth proxy, agent, onboarding, licensing, and console.

The hierarchy is intentionally shallow and stable:

    ControlPlaneError                     ← base; catch-all for CP failures
    ├── ConfigError                       ← bad/missing control-plane config
    ├── RegistryError                     ← tenant_registry.json problems
    │   ├── RegistryNotFoundError         ← registry file does not exist
    │   ├── RegistryFormatError           ← registry file is malformed
    │   └── TenantNotFoundError           ← fingerprint not in the registry
    ├── TenantRevokedError                ← fingerprint present but revoked (I4)
    ├── TenantInactiveError               ← fingerprint present but not active
    ├── TierError                         ← invalid / unknown telemetry tier
    ├── DeploymentError                   ← invalid deployment record
    └── SigningError                      ← signature mint/verify failures (I6)
        └── SignatureVerificationError    ← signature did NOT verify

These are deliberately distinct so that security-critical call sites can
*reject* on the precise condition (revoked vs. unknown vs. inactive) rather
than collapsing everything into a generic 403 — the SPRINT_PLAN C1/C2 contracts
require differentiated handling.
"""

from __future__ import annotations

__all__ = [
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


class ControlPlaneError(Exception):
    """Base class for every error raised inside the control plane."""


class ConfigError(ControlPlaneError):
    """Control-plane configuration is missing or invalid."""


# --- registry / tenant identity (C1) ---------------------------------------


class RegistryError(ControlPlaneError):
    """Something is wrong with the tenant registry as a whole."""


class RegistryNotFoundError(RegistryError):
    """The tenant_registry.json file does not exist at the configured path."""


class RegistryFormatError(RegistryError):
    """The tenant registry exists but is not valid JSON / not the C1 shape."""


class TenantNotFoundError(RegistryError):
    """A presented cert fingerprint has no entry in the registry.

    Per C1 this MUST be rejected (403): an unknown fingerprint is never
    treated as a valid tenant.
    """


class TenantRevokedError(ControlPlaneError):
    """A presented cert fingerprint maps to a tenant whose status is ``revoked``.

    Per C1 / Invariant I4 this MUST be rejected (403).
    """


class TenantInactiveError(ControlPlaneError):
    """A fingerprint is present but its status is neither active nor revoked.

    Treated as a hard reject — only an explicit ``active`` status is trusted.
    """


# --- telemetry tiers (C3) --------------------------------------------------


class TierError(ControlPlaneError):
    """An unknown / unparseable telemetry tier value was supplied."""


# --- deployment record (C4) ------------------------------------------------


class DeploymentError(ControlPlaneError):
    """A deployment record failed validation against the C4 contract."""


# --- signing (C2 / I6) -----------------------------------------------------


class SigningError(ControlPlaneError):
    """A signing or verification operation failed for a non-crypto reason.

    e.g. unknown ``key_id``, malformed manifest, missing keyring entry.
    """


class SignatureVerificationError(SigningError):
    """A signature was checked against the bytes + key and did NOT verify.

    Per Invariant I6 the caller MUST NOT apply the artifact.
    """
