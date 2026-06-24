#!/usr/bin/env python3
"""rotation — demonstrate ed25519 signing-key rotation with non-breaking verification.

Contract C2/I6 requires "rotation by key id" where a **retained** public key still verifies
artifacts signed before the rotation. This script proves that property end-to-end, entirely
within ``signing/`` (it writes a small demo workspace under ``signing/rotation_demo/`` and
leaves the rest of the trust root untouched).

What it shows
-------------
 1. Mint key K1, make it active, sign artifact A1 with K1.
 2. Rotate to key K2 (K1 -> retired but its pubkey retained). Sign artifact A2 with K2.
 3. Verify:
      * A2 verifies under the active key K2.                              (new sigs OK)
      * A1 still verifies under the retired key K1 (back-verify).         (old sigs OK)
      * A1 does NOT verify against K2, and a tampered A1 fails.           (no false accepts)

Run:  python rotation.py            # prints a PASS/FAIL transcript, exits 0 on success.

This uses ``signing_lib`` directly (in-memory keyring + a self-contained on-disk trust root in
the demo dir) so it never disturbs the operational ``signing/trust_root.json`` or ``signing/keys/``.
"""

from __future__ import annotations

import json
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import signing_lib as sl  # noqa: E402
from verify_bundle import verify_file  # noqa: E402

DEMO_DIR = os.path.join(HERE, "rotation_demo")


def _write_signed(ring: sl.Keyring, path: str, content: bytes, kind: str, version: str) -> None:
    """Sign ``content`` with the ring's active key and write artifact + .sig + .manifest.json."""
    with open(path, "wb") as fh:
        fh.write(content)
    signed_bytes = sl.canonical_bytes_for_file(path, kind)
    key_id, raw_sig = ring.sign_with_active(signed_bytes)
    with open(path + ".sig", "w", encoding="utf-8") as fh:
        fh.write(sl.b64e(raw_sig) + "\n")
    manifest = sl.build_manifest(
        artifact_kind=kind, version=version, signed_bytes=signed_bytes, key_id=key_id
    )
    with open(path + ".manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")


def _write_trust_root(ring: sl.Keyring, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(ring.to_trust_root(), fh, indent=2, sort_keys=True)
        fh.write("\n")


def run_demo(workdir: str = DEMO_DIR) -> bool:
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir, exist_ok=True)
    trust_root = os.path.join(workdir, "trust_root.json")

    checks: list[tuple[str, bool]] = []

    def check(label: str, cond: bool) -> None:
        checks.append((label, cond))
        print(f"  [{'PASS' if cond else 'FAIL'}] {label}")

    # -- 1. K1 active, sign A1 --------------------------------------------------------
    ring = sl.Keyring()
    k1 = ring.generate_active_key("cp-signing-2026-06")
    a1 = os.path.join(workdir, "config-A1.json")
    _write_signed(ring, a1, sl.canonical_json_bytes({"feature_x": True, "n": 1}), "config", "1")
    _write_trust_root(ring, trust_root)
    print(f"step 1: minted active key {k1.key_id}, signed A1")

    r = verify_file(a1, trust_root_path=trust_root)
    check("A1 verifies under active key K1", r.ok and r.key_id == "cp-signing-2026-06")

    # -- 2. rotate to K2, sign A2 -----------------------------------------------------
    k2 = ring.rotate_to("cp-signing-2026-09")  # K1 retired (pubkey retained), K2 active
    a2 = os.path.join(workdir, "config-A2.json")
    _write_signed(ring, a2, sl.canonical_json_bytes({"feature_x": False, "n": 2}), "config", "2")
    _write_trust_root(ring, trust_root)  # now contains K1(retired) + K2(active)
    print(f"step 2: rotated active -> {k2.key_id}; K1 now retired (pubkey retained), signed A2")

    check("active_key_id is K2 after rotation", ring.active_key_id == "cp-signing-2026-09")
    check("K1 retained in trust root as retired",
          ring.get("cp-signing-2026-06") is not None
          and ring.get("cp-signing-2026-06").status == "retired")

    # -- 3. the rotation guarantee ----------------------------------------------------
    r2 = verify_file(a2, trust_root_path=trust_root)
    check("A2 (new) verifies under active key K2", r2.ok and r2.key_id == "cp-signing-2026-09")

    # A1 was signed by the now-retired K1. Default policy rejects retired-key applies;
    # back-verify (--allow-retired) must still pass — that is the "old sigs still verify" guarantee.
    r1_default = verify_file(a1, trust_root_path=trust_root)
    check("A1 (retired-key) REJECTED by default apply-policy", not r1_default.ok)

    r1_back = verify_file(a1, trust_root_path=trust_root, allow_retired=True)
    check("A1 STILL cryptographically verifies under retained K1 (back-verify)",
          r1_back.ok and r1_back.key_id == "cp-signing-2026-06")

    # No false accepts: A1's bytes must not verify under K2, and tampering must fail.
    a1_bytes = sl.canonical_bytes_for_file(a1, "config")
    a1_sig = sl.b64d(open(a1 + ".sig").read().strip())
    check("A1 does NOT verify under K2 (different key)",
          not ring.verify_with("cp-signing-2026-09", a1_bytes, a1_sig))

    tampered = bytearray(a1_bytes)
    tampered[0] ^= 0x01
    check("tampered A1 bytes do NOT verify under K1",
          not ring.verify_with("cp-signing-2026-06", bytes(tampered), a1_sig))

    ok = all(c for _, c in checks)
    print()
    print(f"rotation demo: {'ALL PASS' if ok else 'FAILURES PRESENT'} "
          f"({sum(c for _, c in checks)}/{len(checks)} checks)")
    print(f"demo artifacts in: {workdir}")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if run_demo() else 1)
