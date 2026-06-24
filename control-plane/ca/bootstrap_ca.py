#!/usr/bin/env python3
"""bootstrap_ca.py — create the Fyralis root + intermediate CA on disk.

Writes the public CA materials under ``ca/pki/`` and the **private keys** under
``ca/pki/keys/`` (gitignored by the repo-level ``**/keys/`` rule). The layout:

    ca/pki/
      root.crt              # self-signed root certificate (public)
      intermediate.crt      # intermediate certificate (public)
      ca-chain.crt          # intermediate + root, the bundle verifiers load
      keys/
        root.key            # root private key   (OFFLINE in prod)
        intermediate.key    # intermediate key   (online signer)

The root signs the intermediate; the intermediate signs tenant leaves
(``issue_cert.py``). In production this whole step is performed by **step-ca**
(see ``config/ca.json``); this Python path is the local/CI/testable equivalent.

Usage
-----
    python bootstrap_ca.py                 # create under ./pki, refuse to clobber
    python bootstrap_ca.py --force         # overwrite an existing CA
    python bootstrap_ca.py --pki-dir DIR   # custom output dir
    python bootstrap_ca.py --key-password env:CA_KEY_PASSWORD   # encrypt keys

Idempotency: without ``--force`` the command refuses to overwrite an existing
root key, so you cannot accidentally rotate the trust anchor (which would
invalidate every issued cert).
"""

from __future__ import annotations

import argparse
import os
import stat
import sys

# Allow running as a script (``python bootstrap_ca.py``) or as a module.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ca_lib  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PKI_DIR = os.path.join(_HERE, "pki")


def _resolve_password(spec: str | None) -> bytes | None:
    """Resolve a ``--key-password`` spec into bytes (or None).

    Supported forms: ``env:VAR`` (read from env), ``pass:literal`` (inline; dev
    only), or empty/absent (unencrypted keys — fine for local dev).
    """
    if not spec:
        return None
    if spec.startswith("env:"):
        val = os.environ.get(spec[4:])
        if not val:
            raise SystemExit("env var %s for key password is empty/unset" % spec[4:])
        return val.encode()
    if spec.startswith("pass:"):
        return spec[5:].encode() or None
    raise SystemExit("--key-password must be 'env:VAR' or 'pass:literal'")


def _write(path: str, data: bytes, *, secret: bool = False) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
    # Lock down private keys to owner-only.
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR if secret else 0o644)


def bootstrap(pki_dir: str, *, force: bool, key_password: bytes | None) -> dict:
    keys_dir = os.path.join(pki_dir, "keys")
    paths = {
        "root_crt": os.path.join(pki_dir, "root.crt"),
        "intermediate_crt": os.path.join(pki_dir, "intermediate.crt"),
        "chain_crt": os.path.join(pki_dir, "ca-chain.crt"),
        "root_key": os.path.join(keys_dir, "root.key"),
        "intermediate_key": os.path.join(keys_dir, "intermediate.key"),
    }

    if not force and os.path.exists(paths["root_key"]):
        raise SystemExit(
            "refusing to overwrite existing CA at %s (use --force to rotate the "
            "trust anchor — this invalidates every issued cert)" % paths["root_key"]
        )

    root = ca_lib.generate_root_ca()
    intermediate = ca_lib.generate_intermediate(root)

    _write(paths["root_crt"], root.cert_pem())
    _write(paths["intermediate_crt"], intermediate.cert_pem())
    _write(paths["chain_crt"], ca_lib.chain_pem(intermediate, root))
    _write(paths["root_key"], root.key_pem(password=key_password), secret=True)
    _write(
        paths["intermediate_key"],
        intermediate.key_pem(password=key_password),
        secret=True,
    )

    return {
        "paths": paths,
        "root_fingerprint": ca_lib.fingerprint_sha256(root.cert),
        "intermediate_fingerprint": ca_lib.fingerprint_sha256(intermediate.cert),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bootstrap the Fyralis CA (root + intermediate).")
    parser.add_argument("--pki-dir", default=DEFAULT_PKI_DIR, help="output dir (default: ./pki)")
    parser.add_argument("--force", action="store_true", help="overwrite an existing CA")
    parser.add_argument(
        "--key-password",
        default=os.environ.get("CA_KEY_PASSWORD_SPEC"),
        help="encrypt CA keys: 'env:VAR' or 'pass:literal' (default: unencrypted)",
    )
    args = parser.parse_args(argv)

    key_password = _resolve_password(args.key_password)
    result = bootstrap(args.pki_dir, force=args.force, key_password=key_password)

    print("Fyralis CA bootstrapped.")
    print("  root cert:         %s" % result["paths"]["root_crt"])
    print("  intermediate cert: %s" % result["paths"]["intermediate_crt"])
    print("  ca chain:          %s" % result["paths"]["chain_crt"])
    print("  root key (secret): %s" % result["paths"]["root_key"])
    print("  inter key (secret):%s" % result["paths"]["intermediate_key"])
    print("  root fingerprint:         %s" % result["root_fingerprint"])
    print("  intermediate fingerprint: %s" % result["intermediate_fingerprint"])
    if key_password is None:
        print("  NOTE: CA keys are UNENCRYPTED (dev). Use --key-password in prod.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
