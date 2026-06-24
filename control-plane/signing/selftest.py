#!/usr/bin/env python3
"""selftest — self-contained functional test of the signing subsystem (C2 / I6).

Runs the full required flow in an **isolated temp workspace** (never touches the operational
``signing/keys/`` or ``signing/trust_root.json``):

    keygen -> sign a sample bundle -> verify OK -> tamper -> verify FAILS
           -> rotate -> old + new both verify

Run:        python selftest.py          # exit 0 = all pass, non-zero = a check failed
Importable: from selftest import run_selftest; assert run_selftest()

Also runnable under pytest: the ``test_*`` functions assert the same properties.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import signing_lib as sl  # noqa: E402
from verify_bundle import verify_file  # noqa: E402


def _sign_into(ring: sl.Keyring, path: str, content: bytes, kind: str, version: str) -> None:
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


def _publish_trust_root(ring: sl.Keyring, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(ring.to_trust_root(), fh, indent=2, sort_keys=True)


def run_selftest(verbose: bool = True) -> bool:
    results: list[tuple[str, bool]] = []

    def check(label: str, cond: bool) -> None:
        results.append((label, cond))
        if verbose:
            print(f"  [{'PASS' if cond else 'FAIL'}] {label}")

    with tempfile.TemporaryDirectory(prefix="signing-selftest-") as work:
        tr = os.path.join(work, "trust_root.json")

        # 1. keygen (K1 active) + sign a sample bundle of each artifact kind.
        ring = sl.Keyring()
        ring.generate_active_key("cp-signing-2026-06")

        cfg = os.path.join(work, "agent-config.json")
        _sign_into(ring, cfg, sl.canonical_json_bytes({"tier": "T1", "n": 1}), "config", "7")

        lic = os.path.join(work, "license-acme.json")
        _sign_into(ring, lic, sl.canonical_json_bytes({"tenant_id": "acme", "exp": "2027"}), "license", "2027")

        rel = os.path.join(work, "release-1.4.2.tar.gz")
        _sign_into(ring, rel, b"\x1f\x8b" + os.urandom(256), "release", "1.4.2")

        _publish_trust_root(ring, tr)

        # 2. verify OK.
        check("config verifies OK", verify_file(cfg, trust_root_path=tr).ok)
        check("license verifies OK", verify_file(lic, trust_root_path=tr).ok)
        check("release tarball verifies OK", verify_file(rel, trust_root_path=tr).ok)

        # 3. tamper -> verify FAILS (body tamper + signature tamper + manifest key swap).
        with open(cfg, "wb") as fh:
            fh.write(sl.canonical_json_bytes({"tier": "T3", "n": 1}))  # changed content
        check("tampered config FAILS verification", not verify_file(cfg, trust_root_path=tr).ok)

        # corrupt the release signature
        sig = sl.b64d(open(rel + ".sig").read().strip())
        bad = bytearray(sig); bad[0] ^= 0x01
        open(rel + ".sig", "w").write(sl.b64e(bytes(bad)) + "\n")
        check("corrupt signature FAILS verification", not verify_file(rel, trust_root_path=tr).ok)

        # unknown key_id in manifest
        m = json.load(open(lic + ".manifest.json"))
        m["key_id"] = "cp-signing-FORGED"
        json.dump(m, open(lic + ".manifest.json", "w"))
        check("unknown key_id FAILS verification", not verify_file(lic, trust_root_path=tr).ok)

        # 4. rotate -> old + new both verify.
        # Re-sign a clean A1 with K1 first, then rotate to K2 and sign A2.
        a1 = os.path.join(work, "config-A1.json")
        _sign_into(ring, a1, sl.canonical_json_bytes({"epoch": 1}), "config", "1")
        ring.rotate_to("cp-signing-2026-09")  # K1 retired (retained), K2 active
        a2 = os.path.join(work, "config-A2.json")
        _sign_into(ring, a2, sl.canonical_json_bytes({"epoch": 2}), "config", "2")
        _publish_trust_root(ring, tr)

        check("post-rotation active key is K2", ring.active_key_id == "cp-signing-2026-09")
        # new artifact verifies under active key
        r_new = verify_file(a2, trust_root_path=tr)
        check("new artifact (A2) verifies under active K2", r_new.ok and r_new.key_id == "cp-signing-2026-09")
        # old artifact still verifies under the RETAINED retired key (back-verify)
        r_old = verify_file(a1, trust_root_path=tr, allow_retired=True)
        check("old artifact (A1) STILL verifies under retained K1", r_old.ok and r_old.key_id == "cp-signing-2026-06")
        # and default apply-policy rejects the retired-key artifact
        check("retired-key artifact rejected by default apply-policy", not verify_file(a1, trust_root_path=tr).ok)

    ok = all(c for _, c in results)
    if verbose:
        print()
        print(f"SELFTEST: {'ALL PASS' if ok else 'FAILURES'} ({sum(c for _, c in results)}/{len(results)})")
    return ok


# --- pytest entrypoints (optional) ------------------------------------------------- #

def test_sign_verify_tamper_rotate():
    assert run_selftest(verbose=False)


def test_core_primitives_roundtrip():
    priv, pub = sl.generate_keypair()
    data = b"hello fyralis"
    sig = sl.sign(data, priv)
    assert sl.verify(data, sig, pub) is True
    assert sl.verify(data + b"!", sig, pub) is False


if __name__ == "__main__":
    raise SystemExit(0 if run_selftest() else 1)
