#!/usr/bin/env python3
"""keygen — generate a control-plane ed25519 signing keypair and emit the public trust root.

Per contract C2/I6 (SPRINT_PLAN.md): the control plane signs every artifact it ships
to a data plane. This CLI mints a signing key under ``signing/keys/`` (the private key
is **gitignored** via the repo ``.gitignore`` rule ``**/keys/``) and writes/updates the
**public** trust root at ``signing/trust_root.json`` (``key_id -> pubkey``), which agents
ship to verify-before-apply.

Layout produced
---------------
    signing/keys/<key_id>.private.pem   # PKCS#8 PEM, gitignored, mode 0600 — NEVER commit
    signing/keys/<key_id>.public.b64    # raw pubkey, base64 (convenience / audit)
    signing/trust_root.json             # { version, active_key_id, keys: {key_id: {pubkey,...}} }

Usage
-----
    python keygen.py                       # default key_id cp-signing-<YYYY-MM>
    python keygen.py --key-id cp-signing-2026-06
    python keygen.py --key-id cp-signing-2026-09 --activate   # add + make active (rotation)

The trust root is **merged**, not overwritten: running keygen again adds the new key and
retains existing public keys (so prior signatures keep verifying). Use ``--activate`` to make
the new key the active signer (retiring the previous active key); omit it to add a key in
``retired`` status (e.g. pre-staging the next key).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import stat
import sys

# Allow ``python keygen.py`` from anywhere: make the package dir importable.
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import signing_lib as sl  # noqa: E402

KEYS_DIR = os.path.join(HERE, "keys")
TRUST_ROOT_PATH = os.path.join(HERE, "trust_root.json")


def default_key_id() -> str:
    return "cp-signing-" + _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m")


def load_trust_root() -> dict:
    if os.path.exists(TRUST_ROOT_PATH):
        with open(TRUST_ROOT_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {"version": 1, "active_key_id": None, "keys": {}}


def write_trust_root(doc: dict) -> None:
    # Pretty-printed and sorted so the trust root diffs cleanly in git.
    with open(TRUST_ROOT_PATH, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate a CP ed25519 signing keypair + trust root.")
    ap.add_argument("--key-id", default=None, help="key id (default: cp-signing-<YYYY-MM>)")
    ap.add_argument(
        "--activate",
        action="store_true",
        help="make this the active signing key (retires the previous active key)",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing private key file for this key_id",
    )
    args = ap.parse_args(argv)

    key_id = args.key_id or default_key_id()
    os.makedirs(KEYS_DIR, exist_ok=True)

    priv_path = os.path.join(KEYS_DIR, f"{key_id}.private.pem")
    pub_path = os.path.join(KEYS_DIR, f"{key_id}.public.b64")

    if os.path.exists(priv_path) and not args.force:
        print(
            f"refusing to overwrite existing private key {priv_path} "
            f"(use --force to replace, or pick a new --key-id)",
            file=sys.stderr,
        )
        return 2

    # 1. Generate keypair.
    priv, pub = sl.generate_keypair()

    # 2. Persist the private key (PKCS#8 PEM), locked down 0600.
    pem = sl.private_key_to_pem(priv)
    fd = os.open(priv_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(pem)
    os.chmod(priv_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600

    # 3. Convenience public-key sidecar (also captured in the trust root).
    pub_b64 = sl.public_key_to_b64(pub)
    with open(pub_path, "w", encoding="utf-8") as fh:
        fh.write(pub_b64 + "\n")

    # 4. Merge into the trust root, preserving all prior public keys.
    doc = load_trust_root()
    keys = doc.setdefault("keys", {})
    new_status = "active" if (args.activate or not keys) else "retired"
    if args.activate or new_status == "active":
        # Retire whatever was active before.
        for meta in keys.values():
            if meta.get("status") == "active":
                meta["status"] = "retired"
        doc["active_key_id"] = key_id
    keys[key_id] = {"pubkey": pub_b64, "algo": sl.ALGO, "status": new_status}
    doc["version"] = doc.get("version", 1)
    write_trust_root(doc)

    print(f"generated signing key  : {key_id}  (status={new_status})")
    print(f"  private (gitignored)  : {priv_path}")
    print(f"  public (b64)          : {pub_path}")
    print(f"  pubkey                : {pub_b64}")
    print(f"trust root updated      : {TRUST_ROOT_PATH}")
    print(f"  active_key_id         : {doc['active_key_id']}")
    print(f"  known key_ids         : {', '.join(sorted(keys))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
