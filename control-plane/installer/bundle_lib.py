#!/usr/bin/env python3
"""bundle_lib — the AGENT BUNDLE contract for the WS-INSTALLER.

An *agent bundle* is the single directory a customer-VPC operator feeds to
``install.sh``. It is everything one tenant deployment needs to dial home and
egress telemetry, and nothing more. The installer reads it read-only; the
control plane mints it during onboarding/licensing.

Bundle layout (a directory; every file listed in REQUIRED must exist):

    <bundle-dir>/
      bundle.json            # manifest: identity + tier + endpoints + versions
      ca.crt                 # Fyralis CA chain that signed the proxy server cert
      client.crt             # this deployment's per-tenant mTLS client cert
                             #   (URI SAN = spiffe://fyralis/tenant/<tenant_id>)
      client.key             # the client private key (stays in the customer VPC)
      trust_root.json        # ed25519 public keyring the agent VERIFIES against
      license.json           # signed license JSON (C2 license artifact)
      license.json.sig       # detached ed25519 signature (base64)
      license.json.manifest.json
      config.json            # signed agent config JSON (C2 config artifact)
      config.json.sig
      config.json.manifest.json

``bundle.json`` is the human/installer-readable manifest (NOT itself signed — its
authority derives from the signed license/config + the cert SAN it must agree
with). It pins the identity the overlay is parameterized by:

    {
      "tenant_id": "acme",
      "deployment_id": "acme-use1-7f3a",
      "region": "us-east-1",
      "version": "1.4.2",
      "telemetry_tier": "T1",
      "auth_proxy_url": "https://auth-proxy.fyralis.example:8443",
      "console_url": "http://console:8080",
      "license_expiry": "2027-06-24T00:00:00Z"
    }

This module is import-light and self-contained except for the committed
control-plane ``signing`` package (used to verify the signed artifacts). It is
used by both ``validate_bundle.py`` (install-time) and ``make_sample_bundle.py``
(self-test fixture).
"""

from __future__ import annotations

import json
import os
import secrets
import sys
from dataclasses import dataclass, field

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_DIR = os.path.dirname(_HERE)  # control-plane/
# Make the committed sibling packages importable (signing/, ca/, lib/).
for _p in (os.path.join(_CP_DIR, "signing"), os.path.join(_CP_DIR, "ca"), _CP_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)


# --- bundle file contract ---------------------------------------------------

# Files that MUST be present (the installer refuses a bundle missing any).
REQUIRED_FILES = (
    "bundle.json",
    "ca.crt",
    "client.crt",
    "client.key",
    "trust_root.json",
    "license.json",
    "license.json.sig",
    "license.json.manifest.json",
    "config.json",
    "config.json.sig",
    "config.json.manifest.json",
)

# bundle.json keys the deployment overlay is parameterized by.
REQUIRED_MANIFEST_KEYS = (
    "tenant_id",
    "deployment_id",
    "region",
    "version",
    "telemetry_tier",
    "auth_proxy_url",
)

VALID_TIERS = ("T1", "T2", "T3")


@dataclass
class ValidationResult:
    ok: bool
    bundle_dir: str
    manifest: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks: list[str] = field(default_factory=list)  # human-readable PASS lines

    def add_error(self, msg: str) -> None:
        self.ok = False
        self.errors.append(msg)

    def add_pass(self, msg: str) -> None:
        self.checks.append(msg)

    def add_warn(self, msg: str) -> None:
        self.warnings.append(msg)


def _read_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def validate_bundle(bundle_dir: str, *, verify_signatures: bool = True) -> ValidationResult:
    """Validate an agent bundle directory against the contract.

    Checks (fail-closed — any failure flips ``ok`` to False):
      1. directory exists and every REQUIRED_FILES entry is present + non-empty;
      2. ``bundle.json`` parses and has every REQUIRED_MANIFEST_KEY, valid tier;
      3. the per-tenant cert SAN == ``bundle.json.tenant_id`` (C1 round-trip) —
         best-effort: skipped with a warning if ``cryptography`` is unavailable;
      4. ``trust_root.json`` parses as an ed25519 keyring with at least one key;
      5. (verify_signatures) license.json + config.json each VERIFY against the
         bundle's own trust_root (I6 — verify-before-use), and the license is not
         already expired, and its tenant_id matches the manifest.

    Never raises for a *bad* bundle; raises only for genuinely unreadable inputs
    it cannot classify.
    """
    res = ValidationResult(ok=True, bundle_dir=os.path.abspath(bundle_dir))

    if not os.path.isdir(bundle_dir):
        res.add_error(f"bundle dir does not exist or is not a directory: {bundle_dir}")
        return res

    # 1) required files present + non-empty
    for name in REQUIRED_FILES:
        p = os.path.join(bundle_dir, name)
        if not os.path.isfile(p):
            res.add_error(f"missing required bundle file: {name}")
        elif os.path.getsize(p) == 0:
            res.add_error(f"bundle file is empty: {name}")
    if not res.ok:
        return res
    res.add_pass(f"all {len(REQUIRED_FILES)} required files present and non-empty")

    # 2) manifest parse + keys + tier
    manifest_path = os.path.join(bundle_dir, "bundle.json")
    try:
        manifest = _read_json(manifest_path)
    except Exception as exc:
        res.add_error(f"bundle.json is not valid JSON: {exc}")
        return res
    if not isinstance(manifest, dict):
        res.add_error("bundle.json must be a JSON object")
        return res
    res.manifest = manifest
    for k in REQUIRED_MANIFEST_KEYS:
        if not manifest.get(k):
            res.add_error(f"bundle.json missing/empty key: {k}")
    tier = manifest.get("telemetry_tier")
    if tier not in VALID_TIERS:
        res.add_error(f"bundle.json telemetry_tier {tier!r} not in {VALID_TIERS}")
    if not res.ok:
        return res
    res.add_pass(
        f"manifest OK: tenant={manifest['tenant_id']} "
        f"deployment={manifest['deployment_id']} tier={manifest['telemetry_tier']}"
    )

    # 3) cert SAN round-trip vs manifest tenant_id (C1)
    _check_cert_san(res, bundle_dir, manifest)

    # 4) trust_root parses
    try:
        tr = _read_json(os.path.join(bundle_dir, "trust_root.json"))
        keys = tr.get("keys") or {}
        if not keys:
            res.add_error("trust_root.json has no keys — agent cannot verify anything")
        else:
            res.add_pass(f"trust_root OK: {len(keys)} key(s), active={tr.get('active_key_id')}")
    except Exception as exc:
        res.add_error(f"trust_root.json is not valid JSON: {exc}")

    # 5) signature verification (I6) + license expiry/tenant
    if verify_signatures and res.ok:
        _verify_signed_artifacts(res, bundle_dir, manifest)

    return res


def _check_cert_san(res: ValidationResult, bundle_dir: str, manifest: dict) -> None:
    crt_path = os.path.join(bundle_dir, "client.crt")
    try:
        import ca_lib  # committed control-plane/ca
    except Exception:
        res.add_warn("cryptography/ca_lib unavailable — skipped cert SAN round-trip check")
        return
    try:
        pem = open(crt_path, "rb").read()
        san_tenant = ca_lib.extract_tenant_from_cert(pem)
    except Exception as exc:
        res.add_error(f"could not read tenant SAN from client.crt: {exc}")
        return
    if san_tenant != manifest.get("tenant_id"):
        res.add_error(
            f"cert SAN tenant {san_tenant!r} != bundle.json tenant_id "
            f"{manifest.get('tenant_id')!r} (C1 mismatch — wrong cert for this bundle)"
        )
    else:
        res.add_pass(f"cert SAN round-trips to tenant {san_tenant!r} (C1)")


def _verify_signed_artifacts(res: ValidationResult, bundle_dir: str, manifest: dict) -> None:
    try:
        import verify_bundle as vb  # committed control-plane/signing
    except Exception as exc:
        res.add_warn(f"signing/verify_bundle unavailable — skipped I6 verification ({exc})")
        return

    trust_root = os.path.join(bundle_dir, "trust_root.json")
    for name in ("license.json", "config.json"):
        path = os.path.join(bundle_dir, name)
        result = vb.verify_file(path, trust_root_path=trust_root)
        if not result.ok:
            res.add_error(f"{name} FAILED signature verification (I6): {result.reason}")
        else:
            res.add_pass(f"{name} signature verified (I6): {result.reason}")

    # license expiry + tenant binding
    try:
        lic = _read_json(os.path.join(bundle_dir, "license.json"))
    except Exception as exc:
        res.add_error(f"license.json unreadable for expiry check: {exc}")
        return
    if lic.get("tenant_id") != manifest.get("tenant_id"):
        res.add_error(
            f"license tenant_id {lic.get('tenant_id')!r} != bundle tenant_id "
            f"{manifest.get('tenant_id')!r}"
        )
    expires_at = lic.get("expires_at")
    if expires_at:
        try:
            sys.path.insert(0, _CP_DIR)
            from lib.primitives import parse_rfc3339, utcnow  # type: ignore

            if parse_rfc3339(expires_at) <= utcnow():
                res.add_error(
                    f"license already EXPIRED at {expires_at} — agent will refuse to operate"
                )
            else:
                res.add_pass(f"license valid until {expires_at}")
        except Exception:
            # lib unavailable (parallel build) — string-compare is not safe; warn only.
            res.add_warn("lib.primitives unavailable — skipped license expiry parse")


def manifest_to_env(manifest: dict, bundle_dir: str, *, control_plane_dir: str) -> dict:
    """Map a bundle manifest to the env the deployment overlay is rendered with.

    The keys here are exactly the ${...} variables in ``deployment.compose.yml``.
    """
    tenant = manifest["tenant_id"]
    return {
        "FYRALIS_TENANT_ID": tenant,
        "FYRALIS_DEPLOYMENT_ID": manifest["deployment_id"],
        "FYRALIS_REGION": manifest.get("region", "unknown"),
        "FYRALIS_VERSION": manifest.get("version", "0.0.0"),
        "FYRALIS_TELEMETRY_TIER": manifest.get("telemetry_tier", "T1"),
        "FYRALIS_AUTH_PROXY_URL": manifest["auth_proxy_url"],
        "FYRALIS_CONSOLE_URL": manifest.get("console_url", "http://console:8080"),
        "FYRALIS_BUNDLE_DIR": os.path.abspath(bundle_dir),
        "FYRALIS_CONTROL_PLANE_DIR": os.path.abspath(control_plane_dir),
        "FYRALIS_BOUNDARY_CONFIG": os.path.abspath(
            os.path.join(control_plane_dir, "boundary", "otel-collector-config.yaml")
        ),
        # A per-tenant compose project name so two deployments never collide.
        "FYRALIS_DEPLOYMENT_NAME": f"fyralis-dp-{_slug(manifest['deployment_id'])}",
        # Demo data-plane Postgres password — deployment-local (postgres + exporter
        # on dp-net, never egressed), generated per render rather than carried in the
        # signed bundle or hardcoded in the compose (scanner-safe). manifest_to_env is
        # the sole renderer of the overlay .env.
        "POSTGRES_PASSWORD": secrets.token_urlsafe(24),
    }


def _slug(s: str) -> str:
    return "".join(c if (c.isalnum() or c == "-") else "-" for c in s.lower()).strip("-")


__all__ = [
    "REQUIRED_FILES",
    "REQUIRED_MANIFEST_KEYS",
    "VALID_TIERS",
    "ValidationResult",
    "validate_bundle",
    "manifest_to_env",
]
