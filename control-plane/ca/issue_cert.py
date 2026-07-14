#!/usr/bin/env python3
"""issue_cert.py — mint a per-tenant mTLS client cert and register it.

    python issue_cert.py issue <tenant_id> [options]

Reads the intermediate CA from ``ca/pki/`` (created by ``bootstrap_ca.py``),
issues a leaf cert whose URI SAN is ``spiffe://fyralis/tenant/<tenant_id>``
(contract C1), writes the cert + private key, and **adds a row to
``ca/tenant_registry.json``** keyed on the leaf's SHA-256 fingerprint:

    { "<fp>": { "tenant_id": "<id>", "issued_at": "<rfc3339>", "status": "active" } }

Output (default ``ca/pki/tenants/<tenant_id>/``):

    <tenant_id>.crt        # tenant leaf certificate
    <tenant_id>.key        # tenant private key (gitignored)
    <tenant_id>.bundle.crt # leaf + intermediate + root (handy for the agent)

The bundle is what a data-plane agent presents during the mTLS handshake; the
private key stays in the customer VPC. The control plane only needs the registry
row to authorize the cert later.
"""

from __future__ import annotations

import argparse
import os
import stat
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ca_lib  # noqa: E402
import registry  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PKI_DIR = os.path.join(_HERE, "pki")


def _resolve_password(spec: str | None) -> bytes | None:
    if not spec:
        return None
    if spec.startswith("env:"):
        val = os.environ.get(spec[4:])
        if not val:
            raise SystemExit("env var %s for key password is empty/unset" % spec[4:])
        return val.encode()
    if spec.startswith("pass:"):
        return spec[5:].encode() or None
    raise SystemExit("--ca-key-password must be 'env:VAR' or 'pass:literal'")


def _load_intermediate(pki_dir: str, ca_key_password: bytes | None) -> ca_lib.CertKeyPair:
    inter_crt = os.path.join(pki_dir, "intermediate.crt")
    inter_key = os.path.join(pki_dir, "keys", "intermediate.key")
    for p in (inter_crt, inter_key):
        if not os.path.exists(p):
            raise SystemExit(
                "missing CA material %s — run bootstrap_ca.py first" % p
            )
    cert = ca_lib.load_cert(open(inter_crt, "rb").read())
    key = ca_lib.load_key(open(inter_key, "rb").read(), password=ca_key_password)
    return ca_lib.CertKeyPair(cert=cert, key=key)


def _load_root_cert(pki_dir: str):
    root_crt = os.path.join(pki_dir, "root.crt")
    if not os.path.exists(root_crt):
        return None
    return ca_lib.load_cert(open(root_crt, "rb").read())


def _write(path: str, data: bytes, *, secret: bool = False) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR if secret else 0o644)


def issue(
    tenant_id: str,
    *,
    pki_dir: str = DEFAULT_PKI_DIR,
    out_dir: str | None = None,
    valid_days: int = 90,
    ca_key_password: bytes | None = None,
    tenant_key_password: bytes | None = None,
    registry_path: str = registry.DEFAULT_REGISTRY_PATH,
) -> dict:
    intermediate = _load_intermediate(pki_dir, ca_key_password)
    root_cert = _load_root_cert(pki_dir)

    leaf = ca_lib.issue_tenant_cert(tenant_id, intermediate, valid_days=valid_days)
    fp = leaf.fingerprint_sha256()

    out_dir = out_dir or os.path.join(pki_dir, "tenants", tenant_id)
    crt_path = os.path.join(out_dir, "%s.crt" % tenant_id)
    key_path = os.path.join(out_dir, "%s.key" % tenant_id)
    bundle_path = os.path.join(out_dir, "%s.bundle.crt" % tenant_id)

    _write(crt_path, leaf.cert_pem())
    _write(key_path, leaf.key_pem(password=tenant_key_password), secret=True)

    bundle = leaf.cert_pem() + ca_lib.cert_to_pem(intermediate.cert)
    if root_cert is not None:
        bundle += ca_lib.cert_to_pem(root_cert)
    _write(bundle_path, bundle)

    # Register the cert so the proxy can authorize it (and we can revoke it).
    row = registry.add_entry(fp, tenant_id, path=registry_path)

    # Sanity: the SAN must round-trip to the requested tenant id.
    parsed = ca_lib.extract_tenant_from_cert(leaf.cert_pem())
    assert parsed == tenant_id, "SAN round-trip mismatch: %r != %r" % (parsed, tenant_id)

    return {
        "tenant_id": tenant_id,
        "fingerprint_sha256": fp,
        "cert_path": crt_path,
        "key_path": key_path,
        "bundle_path": bundle_path,
        "registry_row": row,
        "registry_path": registry_path,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Issue a per-tenant mTLS client cert.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_issue = sub.add_parser("issue", help="issue a cert for <tenant_id>")
    p_issue.add_argument("tenant_id", help="tenant identifier (goes in the SPIFFE SAN)")
    p_issue.add_argument("--pki-dir", default=DEFAULT_PKI_DIR)
    p_issue.add_argument("--out-dir", default=None, help="cert output dir")
    p_issue.add_argument("--valid-days", type=int, default=90)
    p_issue.add_argument("--registry", default=registry.DEFAULT_REGISTRY_PATH)
    p_issue.add_argument(
        "--ca-key-password", default=os.environ.get("CA_KEY_PASSWORD_SPEC"),
        help="if the CA key is encrypted: 'env:VAR' or 'pass:literal'",
    )
    p_issue.add_argument(
        "--tenant-key-password", default=None,
        help="encrypt the tenant key: 'env:VAR' or 'pass:literal'",
    )

    args = parser.parse_args(argv)

    if args.command == "issue":
        result = issue(
            args.tenant_id,
            pki_dir=args.pki_dir,
            out_dir=args.out_dir,
            valid_days=args.valid_days,
            ca_key_password=_resolve_password(args.ca_key_password),
            tenant_key_password=_resolve_password(args.tenant_key_password),
            registry_path=args.registry,
        )
        print("Issued tenant cert.")
        print("  tenant_id:    %s" % result["tenant_id"])
        print("  fingerprint:  %s" % result["fingerprint_sha256"])
        print("  cert:         %s" % result["cert_path"])
        print("  key (secret): %s" % result["key_path"])
        print("  bundle:       %s" % result["bundle_path"])
        print("  registry:     %s (status=%s)" % (
            result["registry_path"], result["registry_row"]["status"]))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
