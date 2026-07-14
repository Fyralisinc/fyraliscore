#!/usr/bin/env python3
"""issue_license.py — mint a signed license bundle (the LICENSE JSON contract, ed25519-signed).

This is the control-plane side: given a tenant + deployment + plan + features + an expiry
duration, it produces a *signed license bundle* the agent can verify and operate on. A bundle
is the trio the signing layer (C2) writes:

    <outdir>/license.json                  # the LICENSE document (license_model.License)
    <outdir>/license.json.sig              # detached ed25519 signature over the canonical bytes
    <outdir>/license.json.manifest.json    # {artifact:"license", version, sha256, key_id, algo, signed_at}

Signing is delegated to ``control-plane/signing/sign_bundle`` (we do NOT re-implement crypto
here): we write the document, then call ``sign_file(path, kind="license", ...)`` which signs
with the trust root's active key. The agent later runs ``validator.validate(...)`` which calls
``verify_bundle`` against the same trust root + checks expiry/tenant/revocation.

Usage::

    # 1-day enterprise license for acme's deployment, two features
    python issue_license.py \
        --tenant-id acme --deployment-id acme-use1-7f3a \
        --plan enterprise --duration-days 1 \
        --feature telemetry_t3 --feature byoc \
        --out /tmp/lic-acme

    # already-expired (negative duration) — for fail-closed testing
    python issue_license.py --tenant-id acme --deployment-id acme-use1-7f3a \
        --plan trial --duration-days -1 --out /tmp/lic-expired

    # explicit absolute expiry
    python issue_license.py --tenant-id acme --deployment-id acme-use1-7f3a \
        --expires-at 2027-06-24T00:00:00Z --out /tmp/lic-acme

Exits non-zero with a clear message if the signing key / trust root is unavailable
(``signing/keygen.py`` must have minted a key first).
"""

from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from license_model import License  # noqa: E402

# The real signing library — same code path used for releases/configs (C2/I6).
import sign_bundle as sb  # noqa: E402  (control-plane/signing/sign_bundle.py)


DEFAULT_LICENSE_FILENAME = "license.json"


def issue_license(
    *,
    tenant_id: str,
    deployment_id: str,
    plan: str = "standard",
    duration_days: float | None = None,
    duration_seconds: float | None = None,
    expires_at: str | None = None,
    features: list[str] | None = None,
    out_dir: str,
    filename: str = DEFAULT_LICENSE_FILENAME,
    key_id: str | None = None,
    license_id: str | None = None,
    issued_at: str | None = None,
) -> dict:
    """Mint + sign a license bundle. Returns a summary dict.

    Writes ``<out_dir>/<filename>`` plus ``.sig`` + ``.manifest.json``. The ``version``
    recorded in the signing manifest is the license's ``expires_at`` date (so a quick
    ``ls`` / manifest read tells an operator when a bundle lapses).
    """
    lic = License.mint(
        tenant_id=tenant_id,
        deployment_id=deployment_id,
        plan=plan,
        duration_days=duration_days,
        duration_seconds=duration_seconds,
        expires_at=expires_at,
        features=features,
        license_id=license_id,
        issued_at=issued_at,
    )

    os.makedirs(out_dir, exist_ok=True)
    license_path = os.path.join(out_dir, filename)
    with open(license_path, "w", encoding="utf-8") as fh:
        fh.write(lic.to_json(indent=2))

    # Manifest "version" = the expiry date (yyyy-mm-dd) for operator legibility.
    manifest_version = lic.expires_at.split("T", 1)[0]

    # Delegate to the REAL signing lib. Raises (clear message) if no key / trust root.
    sig_path, manifest_path = sb.sign_file(
        license_path, key_id=key_id, kind="license", version=manifest_version
    )

    return {
        "license": lic.to_dict(),
        "license_path": license_path,
        "sig_path": sig_path,
        "manifest_path": manifest_path,
        "license_id": lic.license_id,
        "fingerprint": lic.fingerprint(),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Issue a signed, expiring license bundle (ed25519 via control-plane/signing)."
    )
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--deployment-id", required=True)
    ap.add_argument("--plan", default="standard", help="trial|standard|pro|enterprise|<custom>")
    ap.add_argument(
        "--feature",
        dest="features",
        action="append",
        default=[],
        help="a granted feature flag (repeatable)",
    )

    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--duration-days", type=float, help="lifetime in days (added to now/issued-at)")
    grp.add_argument("--duration-seconds", type=float, help="lifetime in seconds")
    grp.add_argument("--expires-at", help="explicit RFC-3339 expiry, e.g. 2027-06-24T00:00:00Z")

    ap.add_argument("--issued-at", default=None, help="override issued_at (RFC-3339); default now")
    ap.add_argument("--license-id", default=None, help="override the generated license_id")
    ap.add_argument("--key-id", default=None, help="signing key id (default: trust root active key)")
    ap.add_argument(
        "--out",
        dest="out_dir",
        required=True,
        help="output directory for the bundle (license.json + .sig + .manifest.json)",
    )
    ap.add_argument("--filename", default=DEFAULT_LICENSE_FILENAME)
    args = ap.parse_args(argv)

    try:
        result = issue_license(
            tenant_id=args.tenant_id,
            deployment_id=args.deployment_id,
            plan=args.plan,
            duration_days=args.duration_days,
            duration_seconds=args.duration_seconds,
            expires_at=args.expires_at,
            features=args.features,
            out_dir=args.out_dir,
            filename=args.filename,
            key_id=args.key_id,
            license_id=args.license_id,
            issued_at=args.issued_at,
        )
    except Exception as exc:  # clear operator-facing error, non-zero exit
        print(f"issue failed: {exc}", file=sys.stderr)
        return 1

    lic = result["license"]
    print("issued signed license")
    print(f"  tenant_id     : {lic['tenant_id']}")
    print(f"  deployment_id : {lic['deployment_id']}")
    print(f"  plan          : {lic['plan']}")
    print(f"  features      : {', '.join(lic['features']) or '(none)'}")
    print(f"  issued_at     : {lic['issued_at']}")
    print(f"  expires_at    : {lic['expires_at']}")
    print(f"  license_id    : {lic['license_id']}")
    print(f"  -> {result['license_path']}")
    print(f"  -> {result['sig_path']}")
    print(f"  -> {result['manifest_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
