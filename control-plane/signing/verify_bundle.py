#!/usr/bin/env python3
"""verify_bundle — verify a signed artifact against the trust root BEFORE apply (C2 / I6).

This is the function the **agent** (and any consumer in the data plane) calls before
applying a release / license / config the control plane shipped. It is the enforcement
point for invariant I6: *"an artifact whose signature fails verification, or whose key_id
is unknown/retired, is never applied."*

Verification steps (all must pass)
----------------------------------
 1. Load ``<file>``, ``<file>.sig`` (base64 detached sig), ``<file>.manifest.json``.
 2. Recompute the **canonical artifact bytes** for the artifact kind in the manifest.
 3. Resolve ``manifest.key_id`` in ``trust_root.json``. Unknown key_id  -> REJECT.
    Key present but ``status == "retired"`` -> REJECT *for new applies* (still cryptographically
    valid, but the policy is "don't apply artifacts signed by a retired key"); ``--allow-retired``
    relaxes this for the rotation/back-verify case.
 4. ``algo`` must be ``ed25519``.
 5. Recompute the **signed payload**:
      * ``sig_binding == "v2"`` (default for newly-signed bundles, I6): the canonical binding
        ``{binding, algo, artifact, version, key_id, artifact_sha256}`` — so a RELABELED manifest
        (version / artifact-kind / key_id swapped, artifact bytes unchanged) yields a different
        payload and FAILS here. This is the relabel-rejection enforcement point.
      * no ``sig_binding`` (legacy v1 bundles): the canonical artifact bytes directly. Accepted
        only when ``allow_legacy_v1`` is set (default True for back-compat), so old pre-binding
        bundles still verify; pass ``allow_legacy_v1=False`` to require v2 binding.
    ed25519-verify the signature over that payload with the trust-root pubkey. Fail -> REJECT.
 6. Recompute sha256 of the canonical artifact bytes and compare to ``manifest.sha256``. Fail -> REJECT.

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
    allow_legacy_v1: bool = True,
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

    # --- recompute canonical artifact bytes + load signature -----------------------------
    try:
        signed_bytes = sl.canonical_bytes_for_file(path, artifact_kind or "release")
    except Exception as exc:
        return VerifyResult(False, f"could not read/canonicalize artifact: {exc}", key_id, artifact_kind, version)

    digest = sl.sha256_hex(signed_bytes)

    with open(sig_path, "r", encoding="utf-8") as fh:
        sig_text = fh.read().strip()
    try:
        raw_sig = sl.b64d(sig_text)
    except Exception:
        return VerifyResult(False, "signature file is not valid base64", key_id, artifact_kind, version)

    # --- determine the signed payload per the manifest binding version (I6) --------------
    sig_binding = manifest.get("sig_binding", sl.SIG_BINDING_V1)
    if sig_binding == sl.SIG_BINDING_V2:
        # The signature covers the canonical binding of the manifest identity fields +
        # artifact sha256. Recomputing it from the (possibly relabeled) manifest means any
        # swapped field (version / artifact-kind / key_id) produces a payload that was never
        # signed -> the ed25519 check below REJECTS the relabeled bundle.
        signed_payload = sl.signing_payload_from_manifest(manifest, digest)
    elif sig_binding == sl.SIG_BINDING_V1:
        if not allow_legacy_v1:
            return VerifyResult(
                False,
                "manifest has legacy unbound signature (sig_binding=v1) but v2 manifest "
                "binding is required — re-sign with the current signer (refusing to apply)",
                key_id, artifact_kind, version,
            )
        # Legacy: signature is over the raw canonical artifact bytes (relabel-vulnerable).
        signed_payload = signed_bytes
    else:
        return VerifyResult(
            False,
            f"unknown manifest sig_binding {sig_binding!r} (refusing to apply)",
            key_id, artifact_kind, version,
        )

    # --- the actual cryptographic check (this is what authenticity rests on) -------------
    if not ring.verify_with(key_id, signed_payload, raw_sig):
        return VerifyResult(
            False,
            "ed25519 signature INVALID — artifact tampered, manifest relabeled, wrong key, "
            "or corrupt signature (refusing to apply)",
            key_id, artifact_kind, version,
        )

    # --- redundant integrity cross-check (sha256 in manifest) ----------------------------
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
    vp.add_argument(
        "--require-binding",
        action="store_true",
        help="reject legacy v1 (unbound) manifests; require the v2 manifest binding (I6)",
    )
    args = ap.parse_args(argv)

    res = verify_file(
        args.file,
        trust_root_path=args.trust_root,
        allow_retired=args.allow_retired,
        allow_legacy_v1=not args.require_binding,
    )
    if res.ok:
        print(f"VERIFY OK: {res.reason}")
        return 0
    print(f"VERIFY FAILED: {res.reason}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
