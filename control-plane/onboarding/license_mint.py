#!/usr/bin/env python3
"""license_mint.py — mint + ed25519-sign a per-tenant license, verify-before-use.

The P4 LICENSE bundle (shared contract) is a signed JSON document::

    {
      "tenant_id":     "acme",
      "deployment_id": "acme-use1-7f3a",
      "plan":          "standard",
      "issued_at":     "2026-06-24T00:00:00Z",
      "expires_at":    "2027-06-24T00:00:00Z",
      "features":      ["metrics", "logs", ...]
    }

signed by ``control-plane/signing`` (ed25519, detached sig + manifest, C2). The
agent VERIFIES the signature before use (I6) and refuses to operate once expired.

This module does **not** reimplement signing — it reuses the committed
``signing/sign_bundle.py`` (mint side, needs the private key) and
``signing/verify_bundle.py`` (agent side, public trust root). The license JSON is
written with ``.license.json`` in the name so ``infer_artifact_kind`` classifies
it as ``"license"`` and the signed bytes are the compact-canonical JSON (so the
signature is independent of formatting/key order).

The plan → features map and license duration live here (the only license policy
in the BYOC MVP); everything cryptographic is delegated to ``signing``.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
_SIGNING_DIR = os.path.join(_CP_ROOT, "signing")
_LICENSING_DIR = os.path.join(_CP_ROOT, "licensing")
for _p in (_CP_ROOT, _SIGNING_DIR, _LICENSING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import sign_bundle  # noqa: E402  (signing/sign_bundle.py)
import verify_bundle  # noqa: E402  (signing/verify_bundle.py)

from lib.errors import ControlPlaneError  # noqa: E402
from lib.primitives import parse_rfc3339, to_rfc3339, utcnow  # noqa: E402

# Prefer the canonical control-plane/licensing minter when it is present (the P4
# "issue a signed license via control-plane/licensing" path). It produces the
# SAME signed-license trio (same document shape, same C2 canonical bytes, same
# signing key) so bundles are wire-identical either way. If the sibling area is
# absent, we fall back to the self-contained signer below — onboarding never hard-
# depends on another agent's dir.
try:
    import issue_license as _cp_licensing  # noqa: E402  (licensing/issue_license.py)
except Exception:  # pragma: no cover - absence is a valid runtime state
    _cp_licensing = None

__all__ = [
    "LicenseError",
    "PLAN_FEATURES",
    "DEFAULT_LICENSE_DAYS",
    "build_license_doc",
    "mint_license",
    "verify_license_file",
]


class LicenseError(ControlPlaneError):
    """A license could not be minted, signed, or verified."""


# --- license policy (the only license policy in the BYOC MVP) ---------------

# Plan → entitled feature set. Tiers (T1/T2/T3) gate what telemetry may *egress*
# (see lib/tiers.py); features here are coarse product entitlements carried in the
# license and (optionally) checked by the agent / console.
PLAN_FEATURES: dict[str, list[str]] = {
    "trial": ["metrics"],
    "standard": ["metrics", "logs", "fleet-dashboards"],
    "enterprise": ["metrics", "logs", "traces", "fleet-dashboards", "sso", "audit-export"],
}

DEFAULT_LICENSE_DAYS = 365


def features_for_plan(plan: str) -> list[str]:
    """Return the feature list for ``plan`` (raises on an unknown plan)."""
    key = plan.strip().lower()
    if key not in PLAN_FEATURES:
        raise LicenseError(
            f"unknown plan {plan!r}; known plans: {sorted(PLAN_FEATURES)}"
        )
    return list(PLAN_FEATURES[key])


def build_license_doc(
    *,
    tenant_id: str,
    deployment_id: str,
    plan: str,
    issued_at: Optional[_dt.datetime] = None,
    valid_days: int = DEFAULT_LICENSE_DAYS,
    features: Optional[list[str]] = None,
) -> dict:
    """Construct the (unsigned) P4 license dict.

    ``expires_at = issued_at + valid_days``. ``features`` defaults to the plan's
    entitlement set. Timestamps are RFC-3339 UTC strings (contract).
    """
    issued = issued_at or utcnow()
    expires = issued + _dt.timedelta(days=valid_days)
    return {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "plan": plan.strip().lower(),
        "issued_at": to_rfc3339(issued),
        "expires_at": to_rfc3339(expires),
        "features": features if features is not None else features_for_plan(plan),
    }


def mint_license(
    *,
    tenant_id: str,
    deployment_id: str,
    plan: str,
    out_dir: str,
    valid_days: int = DEFAULT_LICENSE_DAYS,
    issued_at: Optional[_dt.datetime] = None,
    key_id: Optional[str] = None,
    features: Optional[list[str]] = None,
) -> dict:
    """Write + sign a license into ``out_dir``.

    Produces three files (named ``<tenant>.license.json[.sig|.manifest.json]``)::

        <tenant>.license.json            # the license document
        <tenant>.license.json.sig        # detached ed25519 signature (base64)
        <tenant>.license.json.manifest.json

    Returns a dict with the paths, the parsed license, and the signing key_id.
    The signature is over the compact-canonical JSON (C2), so the document on disk
    can be pretty-printed without breaking verification.

    Raises :class:`LicenseError` if signing material is unavailable (e.g. no
    ``signing/trust_root.json`` / private key on this host).

    When ``control-plane/licensing`` is importable, minting is delegated to its
    ``issue_license`` (the canonical P4 license minter); otherwise the equivalent
    self-contained signer below is used. Either path yields the identical signed-
    license trio, so the returned dict shape is the same.
    """
    if _cp_licensing is not None:
        try:
            return _mint_via_licensing(
                tenant_id=tenant_id,
                deployment_id=deployment_id,
                plan=plan,
                out_dir=out_dir,
                valid_days=valid_days,
                issued_at=issued_at,
                key_id=key_id,
                features=features,
            )
        except LicenseError:
            raise
        except Exception as exc:
            raise LicenseError(
                f"control-plane/licensing failed to issue a license for "
                f"{tenant_id}: {exc}"
            ) from exc

    os.makedirs(out_dir, exist_ok=True)
    doc = build_license_doc(
        tenant_id=tenant_id,
        deployment_id=deployment_id,
        plan=plan,
        issued_at=issued_at,
        valid_days=valid_days,
        features=features,
    )
    lic_path = os.path.join(out_dir, f"{tenant_id}.license.json")
    with open(lic_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")

    # Version the manifest by the license's expiry date (operator-meaningful).
    version = doc["expires_at"][:10]
    try:
        sig_path, manifest_path = sign_bundle.sign_file(
            lic_path, key_id=key_id, kind="license", version=version
        )
    except Exception as exc:  # FileNotFoundError (no trust root/key), RuntimeError, ...
        raise LicenseError(
            f"could not sign license for {tenant_id}: {exc} "
            "(is signing/ bootstrapped? run signing/keygen.py)"
        ) from exc

    with open(manifest_path, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    return {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "plan": doc["plan"],
        "license": doc,
        "license_path": lic_path,
        "sig_path": sig_path,
        "manifest_path": manifest_path,
        "key_id": manifest.get("key_id"),
        "expires_at": doc["expires_at"],
        "features": doc["features"],
        "minted_by": "onboarding.license_mint",
    }


def _mint_via_licensing(
    *,
    tenant_id: str,
    deployment_id: str,
    plan: str,
    out_dir: str,
    valid_days: int,
    issued_at: Optional[_dt.datetime],
    key_id: Optional[str],
    features: Optional[list[str]],
) -> dict:
    """Delegate license minting to ``control-plane/licensing.issue_license``.

    Normalizes that module's return dict to this module's contract (adds
    ``key_id``/``expires_at``/``features``/``plan`` derived from the license doc +
    manifest). The license file is named ``<tenant>.license.json`` to match the
    onboarding bundle layout.
    """
    issued_iso = to_rfc3339(issued_at) if issued_at is not None else None
    # Onboarding owns the plan->features entitlement policy (PLAN_FEATURES). The
    # licensing minter does not default features from the plan (it stores exactly
    # what it's given), so pass the plan's feature set explicitly to keep the
    # entitlement identical whether the canonical minter or our fallback runs.
    feats = features if features is not None else features_for_plan(plan)
    out = _cp_licensing.issue_license(
        tenant_id=tenant_id,
        deployment_id=deployment_id,
        plan=plan,
        duration_days=float(valid_days),
        features=feats,
        out_dir=out_dir,
        filename=f"{tenant_id}.license.json",
        key_id=key_id,
        issued_at=issued_iso,
    )
    doc = out["license"]
    manifest_path = out["manifest_path"]
    manifest = {}
    if manifest_path and os.path.exists(manifest_path):
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    return {
        "tenant_id": doc.get("tenant_id", tenant_id),
        "deployment_id": doc.get("deployment_id", deployment_id),
        "plan": doc.get("plan", plan.strip().lower()),
        "license": doc,
        "license_path": out["license_path"],
        "sig_path": out["sig_path"],
        "manifest_path": manifest_path,
        "key_id": manifest.get("key_id"),
        "expires_at": doc["expires_at"],
        "features": doc.get("features", []),
        "minted_by": "control-plane/licensing",
    }


def verify_license_file(
    license_path: str,
    *,
    trust_root_path: Optional[str] = None,
    now: Optional[_dt.datetime] = None,
    require_unexpired: bool = True,
) -> dict:
    """Verify a license's signature (I6) and, optionally, that it is unexpired.

    This is what the *agent* runs before operating. Returns the parsed license on
    success; raises :class:`LicenseError` if the signature is invalid / the key is
    unknown-or-retired, or (when ``require_unexpired``) if it has expired.
    """
    res = verify_bundle.verify_file(license_path, trust_root_path=trust_root_path)
    if not res.ok:
        raise LicenseError(f"license signature verification failed: {res.reason}")

    with open(license_path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)

    if require_unexpired:
        now = now or utcnow()
        try:
            expires = parse_rfc3339(doc["expires_at"])
        except (KeyError, ValueError) as exc:
            raise LicenseError(f"license has no valid expires_at: {exc}") from exc
        if expires <= now:
            raise LicenseError(
                f"license for {doc.get('tenant_id')!r} expired at {doc['expires_at']} "
                "(agent refuses to operate; I6/expiry)"
            )
    return doc
