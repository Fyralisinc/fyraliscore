#!/usr/bin/env python3
"""sign_bundle — sign a control-plane artifact (release tarball / license JSON / config JSON).

Per contract C2/I6: writes a **detached** ed25519 signature plus a **manifest** alongside the
artifact. The control plane runs this for every file it ships to a data plane; the agent runs
``verify_bundle`` before applying.

Outputs (next to ``<file>``)
----------------------------
    <file>.sig             # base64 of the raw 64-byte ed25519 signature over the canonical bytes
    <file>.manifest.json   # { artifact, version, sha256, key_id, algo:"ed25519", signed_at }

Canonical signed bytes (C2)
---------------------------
* release tarballs / opaque blobs  -> the exact file bytes.
* license / config JSON            -> compact-canonical UTF-8 JSON (sorted keys, no whitespace),
                                      so signing is independent of key order / formatting.
The ``sha256`` in the manifest is a *redundant* integrity check over those same canonical bytes;
the signed quantity is the ed25519 signature, not the hash.

Usage
-----
    python sign_bundle.py sign release-1.4.2.tar.gz --version 1.4.2
    python sign_bundle.py sign license-acme.json     --kind license --version 2027-06-24
    python sign_bundle.py sign agent-config.json     --kind config  --version 7
    python sign_bundle.py sign blob.bin --key-id cp-signing-2026-06   # pin a specific signer

By default it signs with the trust root's ``active_key_id`` (loading that key's private PEM from
``signing/keys/``). Exits non-zero with a clear message if the private key is unavailable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import signing_lib as sl  # noqa: E402

KEYS_DIR = os.path.join(HERE, "keys")
TRUST_ROOT_PATH = os.path.join(HERE, "trust_root.json")


def _load_trust_root() -> dict:
    if not os.path.exists(TRUST_ROOT_PATH):
        raise FileNotFoundError(
            f"no trust root at {TRUST_ROOT_PATH}; run keygen.py first to mint a signing key"
        )
    with open(TRUST_ROOT_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_private_key(key_id: str):
    priv_path = os.path.join(KEYS_DIR, f"{key_id}.private.pem")
    if not os.path.exists(priv_path):
        raise FileNotFoundError(
            f"private key for key_id {key_id!r} not found at {priv_path}; "
            "this host cannot sign with that key (verifier-only?)"
        )
    with open(priv_path, "rb") as fh:
        return sl.load_private_key_pem(fh.read())


def sign_file(
    path: str,
    *,
    key_id: str | None = None,
    kind: str | None = None,
    version: str = "0",
) -> tuple[str, str]:
    """Sign ``path`` and write ``<path>.sig`` + ``<path>.manifest.json``.

    Returns ``(sig_path, manifest_path)``.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"file to sign not found: {path}")

    doc = _load_trust_root()
    if key_id is None:
        key_id = doc.get("active_key_id")
        if not key_id:
            raise RuntimeError("trust root has no active_key_id; run keygen.py --activate")
    if key_id not in doc.get("keys", {}):
        raise RuntimeError(f"key_id {key_id!r} is not in the trust root")

    priv = _load_private_key(key_id)

    artifact_kind = kind or sl.infer_artifact_kind(path)
    signed_bytes = sl.canonical_bytes_for_file(path, artifact_kind)

    # Sign the canonical bytes; sanity-check the signer matches the trust-root pubkey.
    raw_sig = sl.sign(signed_bytes, priv)
    expected_pub = doc["keys"][key_id]["pubkey"]
    if sl.public_key_to_b64(priv.public_key()) != expected_pub:
        raise RuntimeError(
            f"private key for {key_id!r} does not match the trust-root public key "
            "(key/trust-root mismatch)"
        )

    sig_path = path + ".sig"
    manifest_path = path + ".manifest.json"

    with open(sig_path, "w", encoding="utf-8") as fh:
        fh.write(sl.b64e(raw_sig) + "\n")

    manifest = sl.build_manifest(
        artifact_kind=artifact_kind,
        version=str(version),
        signed_bytes=signed_bytes,
        key_id=key_id,
    )
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")

    return sig_path, manifest_path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Sign a control-plane artifact (ed25519, detached).")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("sign", help="sign <file> -> <file>.sig + <file>.manifest.json")
    sp.add_argument("file", help="path to the artifact (tarball / license JSON / config JSON)")
    sp.add_argument(
        "--kind",
        choices=["release", "license", "config"],
        default=None,
        help="artifact kind (default: inferred from filename)",
    )
    sp.add_argument("--version", default="0", help="version string recorded in the manifest")
    sp.add_argument(
        "--key-id",
        default=None,
        help="signing key id (default: trust root active_key_id)",
    )
    args = ap.parse_args(argv)

    try:
        sig_path, manifest_path = sign_file(
            args.file, key_id=args.key_id, kind=args.kind, version=args.version
        )
    except Exception as exc:  # surface a clear operator-facing error, non-zero exit
        print(f"sign failed: {exc}", file=sys.stderr)
        return 1

    with open(manifest_path, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    print(f"signed {args.file}")
    print(f"  artifact   : {manifest['artifact']}")
    print(f"  key_id     : {manifest['key_id']}")
    print(f"  sha256     : {manifest['sha256']}")
    print(f"  signed_at  : {manifest['signed_at']}")
    print(f"  -> {sig_path}")
    print(f"  -> {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
