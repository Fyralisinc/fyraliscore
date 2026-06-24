#!/usr/bin/env python3
"""verify_bundle — verify a signed artifact against the trust root BEFORE apply (C2 / I6).

This is the function the **agent** (and any consumer in the data plane) calls before
applying a release / license / config the control plane shipped. It is the enforcement
point for invariant I6: *"an artifact whose signature fails verification, or whose key_id
is unknown/retired, is never applied."*

Verification steps (all must pass)
----------------------------------
 1. Load ``<file>``, ``<file>.sig`` (base64 detached sig), ``<file>.manifest.json``.
 2. Recompute the **canonical signed bytes** for the artifact kind in the manifest.
 3. Resolve ``manifest.key_id`` in ``trust_root.json``. Unknown key_id  -> REJECT.
    Key present but ``status == "retired"`` -> REJECT *for new applies* (still cryptographically
    valid, but the policy is "don't apply artifacts signed by a retired key"); ``--allow-retired``
    relaxes this for the rotation/back-verify case.
 4. ``algo`` must be ``ed25519``.
 5. ed25519-verify the signature over the canonical bytes with the trust-root pubkey. Fail -> REJECT.
 6. Recompute sha256 of the canonical bytes and compare to ``manifest.sha256`` (redundant). Fail -> REJECT.

On success: prints an OK line and exits 0. On ANY failure: prints a clear ``VERIFY FAILED: ...``
message to stderr and exits non-zero (the caller must NOT apply the artifact).

Usage
-----
    python verify_bundle.py verify release-1.4.2.tar.gz
    python verify_bundle.py verify license-acme.json --allow-retired   # accept retired-key sigs

Programmatic (what the agent imports):
    from verify_bundle import verify_file, VerifyResult
    res = verify_file("agent-config.json")
    if not res.ok:
        log_audit("rejected", res.reason); refuse_apply()
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import signing_lib as sl  # noqa: E402

TRUST_ROOT_PATH = os.path.join(HERE, "trust_root.json")


@dataclass
class VerifyResult:
    ok: bool
    reason: str
    key_id: str | None = None
    artifact: str | None = None
    version: str | None = None


def _load_trust_root(trust_root_path: str) -> dict:
    if not os.path.exists(trust_root_path):
        raise FileNotFoundError(
            f"no trust root at {trust_root_path}; agent has no public keys to verify against"
        )
    with open(trust_root_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def verify_file(
    path: str,
    *,
    trust_root_path: str | None = None,
    sig_path: str | None = None,
    manifest_path: str | None = None,
    allow_retired: bool = False,
) -> VerifyResult:
    """Verify ``path`` against the trust root. Returns a :class:`VerifyResult` (never raises
    for a *bad* artifact — only for genuinely missing inputs / unreadable trust root)."""
    trust_root_path = trust_root_path or TRUST_ROOT_PATH
    sig_path = sig_path or (path + ".sig")
    manifest_path = manifest_path or (path + ".manifest.json")

    if not os.path.isfile(path):
        return VerifyResult(False, f"artifact not found: {path}")
    if not os.path.isfile(sig_path):
        return VerifyResult(False, f"detached signature not found: {sig_path}")
    if not os.path.isfile(manifest_path):
        return VerifyResult(False, f"manifest not found: {manifest_path}")

    doc = _load_trust_root(trust_root_path)
    ring = sl.Keyring.from_trust_root(doc)

    with open(manifest_path, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    key_id = manifest.get("key_id")
    artifact_kind = manifest.get("artifact")
    version = manifest.get("version")
    algo = manifest.get("algo", sl.ALGO)

    if algo != sl.ALGO:
        return VerifyResult(False, f"unsupported algo {algo!r} (expected {sl.ALGO})", key_id, artifact_kind, version)

    # --- key_id policy: unknown -> reject; retired -> reject unless allowed --------------
    entry = ring.get(key_id)
    if entry is None:
        return VerifyResult(
            False,
            f"unknown key_id {key_id!r}: not in trust root (refusing to apply)",
            key_id, artifact_kind, version,
        )
    if entry.status == "retired" and not allow_retired:
        return VerifyResult(
            False,
            f"key_id {key_id!r} is RETIRED: refusing to apply a new artifact signed by a "
            "retired key (re-sign with the active key, or pass --allow-retired to back-verify)",
            key_id, artifact_kind, version,
        )

    # --- recompute canonical bytes + load signature --------------------------------------
    try:
        signed_bytes = sl.canonical_bytes_for_file(path, artifact_kind or "release")
    except Exception as exc:
        return VerifyResult(False, f"could not read/canonicalize artifact: {exc}", key_id, artifact_kind, version)

    with open(sig_path, "r", encoding="utf-8") as fh:
        sig_text = fh.read().strip()
    try:
        raw_sig = sl.b64d(sig_text)
    except Exception:
        return VerifyResult(False, "signature file is not valid base64", key_id, artifact_kind, version)

    # --- the actual cryptographic check (this is what authenticity rests on) -------------
    if not ring.verify_with(key_id, signed_bytes, raw_sig):
        return VerifyResult(
            False,
            "ed25519 signature INVALID — artifact tampered, wrong key, or corrupt signature "
            "(refusing to apply)",
            key_id, artifact_kind, version,
        )

    # --- redundant integrity cross-check (sha256 in manifest) ----------------------------
    digest = sl.sha256_hex(signed_bytes)
    if manifest.get("sha256") and digest != manifest["sha256"]:
        return VerifyResult(
            False,
            f"sha256 mismatch: manifest={manifest['sha256']} actual={digest} "
            "(manifest/artifact inconsistent — refusing to apply)",
            key_id, artifact_kind, version,
        )

    note = " (retired key, back-verify)" if entry.status == "retired" else ""
    return VerifyResult(
        True,
        f"signature OK: {artifact_kind} v{version} signed by {key_id}{note}",
        key_id, artifact_kind, version,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Verify a signed artifact before apply (ed25519, I6).")
    sub = ap.add_subparsers(dest="cmd", required=True)
    vp = sub.add_parser("verify", help="verify <file> against trust_root.json")
    vp.add_argument("file", help="path to the artifact to verify")
    vp.add_argument(
        "--trust-root",
        default=None,
        help=f"trust root path (default: {TRUST_ROOT_PATH})",
    )
    vp.add_argument(
        "--allow-retired",
        action="store_true",
        help="accept signatures from a retired key (rotation back-verify); default rejects",
    )
    args = ap.parse_args(argv)

    res = verify_file(args.file, trust_root_path=args.trust_root, allow_retired=args.allow_retired)
    if res.ok:
        print(f"VERIFY OK: {res.reason}")
        return 0
    print(f"VERIFY FAILED: {res.reason}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
