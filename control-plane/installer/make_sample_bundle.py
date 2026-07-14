#!/usr/bin/env python3
"""make_sample_bundle — mint a REAL, fully-self-contained sample agent bundle.

Produces a bundle directory that ``validate_bundle.py`` / ``install.sh --dry-run``
accept end-to-end, using the COMMITTED control-plane primitives:

  * ``ca/ca_lib`` — an ephemeral root+intermediate CA and a real per-tenant leaf
    cert whose URI SAN is ``spiffe://fyralis/tenant/<tenant_id>`` (C1);
  * ``signing/signing_lib`` — an ephemeral ed25519 keyring; license.json and
    config.json are signed with it and the keyring's PUBLIC trust root is written
    into the bundle so the agent (and the installer's I6 check) verify against it.

This is a self-contained fixture: it does NOT require the control plane to have
provisioned a CA or signing keys on disk. In production the control plane mints
the bundle during onboarding/licensing with the real fleet CA + signing key; the
*shape* is identical.

Usage:
    python make_sample_bundle.py [out_dir] [--tenant acme] [--region us-east-1] \
        [--tier T1] [--expired]

Defaults to ``./sample-bundle`` next to this script.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_DIR = os.path.dirname(_HERE)
for _p in (os.path.join(_CP_DIR, "ca"), os.path.join(_CP_DIR, "signing"), _CP_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ca_lib  # noqa: E402  (control-plane/ca)
import signing_lib as sl  # noqa: E402  (control-plane/signing)


def _rfc3339(dt: _dt.datetime) -> str:
    return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write(path: str, data: bytes, *, mode: int = 0o644) -> None:
    with open(path, "wb") as fh:
        fh.write(data)
    os.chmod(path, mode)


def _sign_into_bundle(
    out_dir: str, name: str, doc: dict, ring: sl.Keyring, *, kind: str, version: str
) -> None:
    """Write ``<name>`` + ``.sig`` + ``.manifest.json`` for a JSON artifact (C2)."""
    path = os.path.join(out_dir, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")

    # Canonical signed bytes for a license/config artifact (order-independent).
    signed_bytes = sl.canonical_bytes_for_file(path, kind)
    key_id = ring.active_key_id
    # I6: sign the canonical manifest binding (not the raw bytes) so relabels are rejected.
    payload = sl.signed_payload_for(
        artifact_kind=kind, version=str(version), key_id=key_id, signed_bytes=signed_bytes
    )
    _, raw_sig = ring.sign_with_active(payload)

    with open(path + ".sig", "w", encoding="utf-8") as fh:
        fh.write(sl.b64e(raw_sig) + "\n")
    manifest = sl.build_manifest(
        artifact_kind=kind, version=str(version), signed_bytes=signed_bytes, key_id=key_id
    )
    with open(path + ".manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")


def make_bundle(
    out_dir: str,
    *,
    tenant_id: str = "acme",
    region: str = "us-east-1",
    tier: str = "T1",
    version: str = "1.4.2",
    auth_proxy_url: str = "https://auth-proxy.fyralis.example:8443",
    console_url: str = "http://console:8080",
    expired: bool = False,
) -> dict:
    os.makedirs(out_dir, exist_ok=True)

    # --- 1) ephemeral CA + per-tenant leaf cert (C1) -----------------------
    root, intermediate = ca_lib.bootstrap_hierarchy()
    leaf = ca_lib.issue_tenant_cert(tenant_id, intermediate, valid_days=90)
    # SAN must round-trip (proves C1 before we even ship it).
    assert ca_lib.extract_tenant_from_cert(leaf.cert_pem()) == tenant_id

    _write(os.path.join(out_dir, "client.crt"), leaf.cert_pem())
    _write(os.path.join(out_dir, "client.key"), leaf.key_pem(), mode=0o600)
    # ca.crt = the chain the boundary trusts for the proxy server cert.
    ca_chain = ca_lib.cert_to_pem(intermediate.cert) + ca_lib.cert_to_pem(root.cert)
    _write(os.path.join(out_dir, "ca.crt"), ca_chain)

    # --- 2) ephemeral signing keyring + trust root (C2 / I6) ---------------
    ring = sl.Keyring()
    ring.generate_active_key("cp-signing-sample")
    with open(os.path.join(out_dir, "trust_root.json"), "w", encoding="utf-8") as fh:
        json.dump(ring.to_trust_root(), fh, indent=2, sort_keys=True)
        fh.write("\n")

    now = _dt.datetime.now(_dt.timezone.utc)
    deployment_id = f"{tenant_id}-{_region_slug(region)}-7f3a"
    if expired:
        expires_at = _rfc3339(now - _dt.timedelta(days=1))
    else:
        expires_at = _rfc3339(now + _dt.timedelta(days=365))

    # --- 3) signed license.json (C2 license shape) -------------------------
    license_doc = {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "plan": "enterprise",
        "issued_at": _rfc3339(now),
        "expires_at": expires_at,
        "features": ["telemetry_t1", "telemetry_t2", "fleet_console"],
    }
    _sign_into_bundle(out_dir, "license.json", license_doc, ring, kind="license", version=expires_at)

    # --- 4) signed config.json (agent config) ------------------------------
    config_doc = {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "telemetry_tier": tier,
        "heartbeat_interval_s": 30,
        "auth_proxy_url": auth_proxy_url,
        "console_url": console_url,
        "config_version": 7,
    }
    _sign_into_bundle(out_dir, "config.json", config_doc, ring, kind="config", version="7")

    # --- 5) bundle.json manifest (identity the overlay is parameterized by) -
    bundle_manifest = {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "region": region,
        "version": version,
        "telemetry_tier": tier,
        "auth_proxy_url": auth_proxy_url,
        "console_url": console_url,
        "license_expiry": expires_at,
        "generated_at": _rfc3339(now),
        "note": "SAMPLE bundle minted by make_sample_bundle.py — ephemeral CA + signing key.",
    }
    with open(os.path.join(out_dir, "bundle.json"), "w", encoding="utf-8") as fh:
        json.dump(bundle_manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")

    return bundle_manifest


def _region_slug(region: str) -> str:
    # us-east-1 -> use1
    parts = region.split("-")
    if len(parts) == 3:
        return parts[0] + parts[1][0] + parts[2]
    return "".join(c for c in region if c.isalnum())[:4] or "r0"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out_dir", nargs="?", default=os.path.join(_HERE, "sample-bundle"))
    ap.add_argument("--tenant", default="acme")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--tier", default="T1", choices=["T1", "T2", "T3"])
    ap.add_argument("--expired", action="store_true", help="mint an already-expired license (negative test)")
    args = ap.parse_args(argv)

    manifest = make_bundle(
        args.out_dir, tenant_id=args.tenant, region=args.region, tier=args.tier, expired=args.expired
    )
    print(f"sample bundle written to {os.path.abspath(args.out_dir)}")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
