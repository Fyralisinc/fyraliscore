#!/usr/bin/env python3
"""deps.py — the shared dependency namespace passed to every console router.

The console is a **router-plugin** surface (console-roadmap): the foundation owns
``app.py`` and the core endpoints; each FEATURE adds a module under
``console/routers/`` exposing ``def register(app, deps): ...``. ``app.py`` scans
that dir and calls every ``register`` with one :class:`ConsoleDeps` object, so a
feature never edits ``app.py`` and gets everything it needs off ``deps``.

What a feature gets off ``deps``
--------------------------------
* ``deps.store``               — the :class:`~store.DeploymentStore` (registry +
  desired/applied facets).
* ``deps.signer``              — a callable signing a JSON dict as ``kind`` in
  {"config","release"} → ``{sig, manifest, signed_by}`` (wraps the REAL
  ``signing/`` lib; never re-implements crypto, I6).
* ``deps.audit``               — ``deps.audit.append(event: dict)`` onto the
  hash-chained audit log (wraps ``audit/audit_log.py``, I5). Every operator
  WRITE calls this.
* ``deps.require_operator``    — a FastAPI dependency that 401s unless a valid
  ``OPERATOR_TOKEN`` bearer is presented (fail-closed 503 if unconfigured). Put
  on EVERY operator WRITE endpoint (I4 operator-vs-agent identity split).
* ``deps.require_agent_write`` — the EXISTING console write-token dependency
  (``CONSOLE_INGEST_TOKEN``), for agent-facing reads like GET desired.
* ``deps.settings``            — a read-only :class:`ConsoleSettings` (mimir_url,
  fleet_org_id, …) for read-only proxy features.

Crypto + audit are wrapped here ONCE so all features share a single signing key
and a single tamper-evident trail.
"""

from __future__ import annotations

import hmac
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from fastapi import Header, HTTPException

# --- import the control-plane root + signing/audit dirs (same anchor as app) --
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_SIGNING_DIR = _ROOT / "signing"
_AUDIT_DIR = _ROOT / "audit"
for _p in (str(_ROOT), str(_SIGNING_DIR), str(_AUDIT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402  (control-plane/signing)


__all__ = [
    "ConsoleDeps",
    "ConsoleSettings",
    "ConsoleAudit",
    "build_signer",
    "make_require_operator",
    "OPERATOR_AUTH_ENV",
]

OPERATOR_AUTH_ENV = "OPERATOR_TOKEN"


# --------------------------------------------------------------------------- #
# settings (read-only proxy features)                                          #
# --------------------------------------------------------------------------- #


@dataclass
class ConsoleSettings:
    """Read-only settings a feature router may need (e.g. a Mimir/alerts proxy)."""

    mimir_url: str = ""
    fleet_org_id: str = ""
    loki_url: str = ""
    grafana_url: str = ""

    @classmethod
    def from_env(cls) -> "ConsoleSettings":
        return cls(
            mimir_url=os.environ.get("CP_MIMIR_URL", os.environ.get("MIMIR_URL", "")),
            fleet_org_id=os.environ.get(
                "CP_FLEET_ORG_ID", os.environ.get("FLEET_ORG_ID", "fleet")
            ),
            loki_url=os.environ.get("CP_LOKI_URL", os.environ.get("LOKI_URL", "")),
            grafana_url=os.environ.get(
                "CP_GRAFANA_URL", os.environ.get("GRAFANA_URL", "")
            ),
        )


# --------------------------------------------------------------------------- #
# operator auth (distinct from the agent's CONSOLE_INGEST_TOKEN)               #
# --------------------------------------------------------------------------- #
#
# OPERATOR_TOKEN gates operator WRITES (desired-state mutations). It is a SECOND,
# independent bearer from the agent's CONSOLE_INGEST_TOKEN so the two identities
# (operator vs agent, I4) cannot be conflated: an agent token must NOT authorize
# an operator write, and vice-versa. Single "admin" role for the demo.
#
# TODO(next-sprint, roadmap §6): replace this shared static bearer with operator
# SSO/OIDC + per-operator identity + RBAC (roles beyond a single "admin"), and
# put an authenticated session in front of the operator READ surface too. Today
# reads stay open on the operator LAN; only WRITES require this token.


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    """Pull the token out of an ``Authorization: Bearer <token>`` header."""
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


def make_require_operator(operator_token: Optional[str]) -> Callable[..., None]:
    """Build the ``require_operator`` FastAPI dependency over ``operator_token``.

    Fail-closed, mirroring the agent write-auth:
      * no token configured on the server  -> 503 (misconfigured; never open).
      * missing/malformed/non-matching bearer -> 401.
    Comparison is constant-time (``hmac.compare_digest``).
    """

    def require_operator(authorization: Optional[str] = Header(default=None)) -> None:
        if not operator_token:
            raise HTTPException(
                status_code=503,
                detail="operator write auth not configured (OPERATOR_TOKEN unset)",
            )
        presented = _extract_bearer(authorization)
        if presented is None or not hmac.compare_digest(presented, operator_token):
            raise HTTPException(
                status_code=401,
                detail="missing or invalid operator bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return require_operator


# --------------------------------------------------------------------------- #
# signer (wraps signing/ — never re-implements crypto, I6)                     #
# --------------------------------------------------------------------------- #


def build_signer(
    *, trust_root_path: Optional[str] = None, keys_dir: Optional[str] = None
) -> Callable[..., Dict[str, Any]]:
    """Return a ``signer(payload: dict, *, kind="config", version="0")`` callable.

    Signs the canonical JSON of ``payload`` with the trust root's active key and
    returns ``{sig, manifest, signed_by}`` — the exact shape that lands in
    ``DesiredState.desired_config_sig`` so the agent can VERIFY before apply (I6).
    The signed payload is the v2 BINDING (``signed_payload_for``), identical to
    ``sign_bundle.py``, so a relabeled manifest fails verification.

    If the trust root / active private key is unavailable the returned callable
    raises ``RuntimeError`` on call (fail-loud at sign time, never a fake sig).
    """
    trust_root_path = trust_root_path or str(_SIGNING_DIR / "trust_root.json")
    keys_dir = keys_dir or str(_SIGNING_DIR / "keys")

    def _signer(
        payload: Dict[str, Any], *, kind: str = "config", version: str = "0"
    ) -> Dict[str, Any]:
        if kind not in ("config", "release"):
            raise ValueError(f"signer kind must be config|release, got {kind!r}")
        if not os.path.exists(trust_root_path):
            raise RuntimeError(
                f"no trust root at {trust_root_path}; cannot sign (run keygen.py)"
            )
        with open(trust_root_path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
        key_id = doc.get("active_key_id")
        if not key_id:
            raise RuntimeError("trust root has no active_key_id; cannot sign")
        priv_path = os.path.join(keys_dir, f"{key_id}.private.pem")
        if not os.path.exists(priv_path):
            raise RuntimeError(
                f"private key for {key_id!r} not found at {priv_path}; "
                "this host cannot sign (verifier-only?)"
            )
        with open(priv_path, "rb") as fh:
            priv = sl.load_private_key_pem(fh.read())

        signed_bytes = sl.canonical_json_bytes(payload)
        binding = sl.signed_payload_for(
            artifact_kind=kind,
            version=str(version),
            key_id=key_id,
            signed_bytes=signed_bytes,
        )
        raw_sig = sl.sign(binding, priv)
        manifest = sl.build_manifest(
            artifact_kind=kind,
            version=str(version),
            signed_bytes=signed_bytes,
            key_id=key_id,
        )
        return {
            "sig": sl.b64e(raw_sig),
            "manifest": manifest,
            "signed_by": key_id,
        }

    return _signer


# --------------------------------------------------------------------------- #
# audit (wraps audit/audit_log.py — hash-chained, I5)                          #
# --------------------------------------------------------------------------- #


class ConsoleAudit:
    """Thin ``append(event: dict)`` facade over the hash-chained audit log (I5).

    Lazily opens the shared CP audit log on first append so importing the console
    never touches the filesystem. ``append`` maps an event dict onto the log's
    (actor, action, target, metadata) record. Best-effort: an audit failure is
    logged but never blocks the operator write the console already committed
    (the write is the source of truth; the trail is tamper-EVIDENT, not a 2-phase
    commit). For a stricter "no write without an audit row" feature, call the
    underlying log directly.
    """

    def __init__(
        self,
        *,
        log_path: Optional[str] = None,
        signing_keyring: Any = None,
        trust_root_path: Optional[str] = None,
    ) -> None:
        # Default the audit-log path from $CONSOLE_AUDIT_LOG when not given, so the
        # containerized console writes to its persisted audit-data volume rather
        # than audit_log's HERE-relative default (/app/audit, the baked code dir).
        self._log_path = log_path or os.environ.get("CONSOLE_AUDIT_LOG") or None
        self._keyring = signing_keyring
        self._trust_root_path = trust_root_path
        self._log = None

    def _ensure(self):
        if self._log is None:
            import audit_log as al  # control-plane/audit, lazy

            self._log = al.open_log(
                self._log_path,
                signing_keyring=self._keyring,
                trust_root_path=self._trust_root_path,
            )
        return self._log

    def append(self, event: Dict[str, Any]):
        """Append one event ``{actor, action, target, metadata?}`` to the trail.

        Returns the written ``AuditEntry`` on success, or ``None`` if appending
        failed (logged, never raised — an operator write is not rolled back by an
        audit hiccup).
        """
        try:
            log = self._ensure()
            return log.append(
                actor=str(event.get("actor", "operator")),
                action=str(event.get("action", "unknown")),
                target=str(event.get("target", "")),
                metadata=dict(event.get("metadata", {})),
            )
        except Exception:  # never block the write on an audit failure
            import logging

            logging.getLogger("fyralis.console").warning(
                "audit append failed for event %r (write not rolled back)",
                event.get("action"),
            )
            return None


# --------------------------------------------------------------------------- #
# the deps namespace                                                           #
# --------------------------------------------------------------------------- #


@dataclass
class ConsoleDeps:
    """The single object handed to every router's ``register(app, deps)``."""

    store: Any
    signer: Callable[..., Dict[str, Any]]
    audit: ConsoleAudit
    require_operator: Callable[..., None]
    require_agent_write: Callable[..., None]
    settings: ConsoleSettings = field(default_factory=ConsoleSettings.from_env)
