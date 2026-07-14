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
    key_id = ring.active_key_id
    # I6: sign the canonical manifest binding (not the raw bytes) so relabels are rejected.
    payload = sl.signed_payload_for(
        artifact_kind=kind, version=version, key_id=key_id, signed_bytes=signed_bytes
    )
    _, raw_sig = ring.sign_with_active(payload)
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

        # 3b. I6 RELABEL: swap a manifest identity field while keeping the artifact bytes
        #     intact -> the v2 binding no longer matches the signed payload -> REJECT.
        relabel = os.path.join(work, "relabel-config.json")
        _sign_into(ring, relabel, sl.canonical_json_bytes({"tier": "T1"}), "config", "7")
        check("freshly-signed relabel target verifies OK", verify_file(relabel, trust_root_path=tr).ok)

        # (i) relabel the VERSION (e.g. claim a different release version), bytes unchanged
        rm = json.load(open(relabel + ".manifest.json"))
        rm_orig = dict(rm)
        rm["version"] = "9999"
        json.dump(rm, open(relabel + ".manifest.json", "w"))
        check(
            "RELABELED version FAILS verification (I6)",
            not verify_file(relabel, trust_root_path=tr).ok,
        )

        # (ii) relabel the ARTIFACT-KIND (config -> license), bytes unchanged
        rm2 = dict(rm_orig)
        rm2["artifact"] = "license"
        rm2["version"] = rm_orig["version"]  # keep version honest; only kind swapped
        json.dump(rm2, open(relabel + ".manifest.json", "w"))
        check(
            "RELABELED artifact-kind FAILS verification (I6)",
            not verify_file(relabel, trust_root_path=tr).ok,
        )

        # (iii) restore the original manifest -> verifies again (relabel was the only change)
        json.dump(rm_orig, open(relabel + ".manifest.json", "w"))
        check("restored manifest verifies OK again", verify_file(relabel, trust_root_path=tr).ok)

        # (iv) the artifact bytes themselves are still bound (via sha256 in the binding):
        #      tamper the bytes, keep the manifest -> REJECT.
        with open(relabel, "wb") as fh:
            fh.write(sl.canonical_json_bytes({"tier": "T3"}))
        check(
            "tampered bytes (manifest unchanged) FAILS verification (I6)",
            not verify_file(relabel, trust_root_path=tr).ok,
        )

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


def test_relabel_rejected_i6():
    """I6: a signed bundle relabeled in the manifest (version / kind / key_id) must REJECT,
    while the artifact bytes are byte-for-byte unchanged."""
    with tempfile.TemporaryDirectory(prefix="signing-relabel-") as work:
        tr = os.path.join(work, "trust_root.json")
        ring = sl.Keyring()
        ring.generate_active_key("cp-signing-2026-06")

        cfg = os.path.join(work, "agent-config.json")
        _sign_into(ring, cfg, sl.canonical_json_bytes({"tier": "T1"}), "config", "7")
        _publish_trust_root(ring, tr)

        # baseline: verifies, and is marked v2-bound
        res = verify_file(cfg, trust_root_path=tr)
        assert res.ok, res.reason
        man = json.load(open(cfg + ".manifest.json"))
        assert man["sig_binding"] == sl.SIG_BINDING_V2

        orig_bytes = open(cfg, "rb").read()

        # relabel version, keep artifact bytes -> REJECT
        m = dict(man); m["version"] = "42"
        json.dump(m, open(cfg + ".manifest.json", "w"))
        assert not verify_file(cfg, trust_root_path=tr).ok
        assert open(cfg, "rb").read() == orig_bytes  # bytes untouched

        # relabel artifact-kind, keep artifact bytes -> REJECT
        m = dict(man); m["artifact"] = "release"
        json.dump(m, open(cfg + ".manifest.json", "w"))
        assert not verify_file(cfg, trust_root_path=tr).ok

        # restore original manifest -> verifies again
        json.dump(man, open(cfg + ".manifest.json", "w"))
        assert verify_file(cfg, trust_root_path=tr).ok


def test_legacy_v1_backcompat_and_require_binding():
    """A legacy v1 bundle (signature over raw artifact bytes, no sig_binding field) still
    verifies by default, but is rejected when binding is required — AND a v1 bundle cannot
    be relabeled with a forged v2 marker (its signature won't match the v2 payload)."""
    with tempfile.TemporaryDirectory(prefix="signing-v1-") as work:
        tr = os.path.join(work, "trust_root.json")
        ring = sl.Keyring()
        ring.generate_active_key("cp-signing-2026-06")
        _publish_trust_root(ring, tr)

        cfg = os.path.join(work, "legacy-config.json")
        body = sl.canonical_json_bytes({"tier": "T1"})
        with open(cfg, "wb") as fh:
            fh.write(body)
        signed_bytes = sl.canonical_bytes_for_file(cfg, "config")
        # LEGACY signing: signature over the raw canonical bytes, manifest WITHOUT sig_binding.
        _, raw_sig = ring.sign_with_active(signed_bytes)
        with open(cfg + ".sig", "w") as fh:
            fh.write(sl.b64e(raw_sig) + "\n")
        legacy_manifest = {
            "artifact": "config",
            "version": "7",
            "sha256": sl.sha256_hex(signed_bytes),
            "key_id": ring.active_key_id,
            "algo": sl.ALGO,
            "signed_at": sl.now_rfc3339(),
        }
        json.dump(legacy_manifest, open(cfg + ".manifest.json", "w"))

        # default: legacy v1 accepted (back-compat)
        assert verify_file(cfg, trust_root_path=tr).ok
        # require-binding: legacy v1 rejected
        assert not verify_file(cfg, trust_root_path=tr, allow_legacy_v1=False).ok

        # forge a v2 marker onto the legacy manifest -> the legacy sig won't match the v2
        # payload, so it's rejected (can't upgrade a v1 sig to a v2 claim).
        m = dict(legacy_manifest); m["sig_binding"] = sl.SIG_BINDING_V2
        json.dump(m, open(cfg + ".manifest.json", "w"))
        assert not verify_file(cfg, trust_root_path=tr).ok


if __name__ == "__main__":
    raise SystemExit(0 if run_selftest() else 1)
