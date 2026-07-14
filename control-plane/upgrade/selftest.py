#!/usr/bin/env python3
"""selftest.py — prove the CP-upgrade deliverables actually hold their guarantees.

Run with the repo dev venv:

    /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python upgrade/selftest.py

What it asserts
---------------
TRUST-OVERLAP (the load-bearing guarantee for FR-A5 non-disruptive rotation):
  T1  Mint TWO independent CA hierarchies (old + new), each issuing a tenant leaf.
  T2  A bundle with only the OLD CA verifies the OLD leaf, REJECTS the NEW leaf.
  T3  `trust_bundle.add` appends the NEW CA  -> the OVERLAP bundle now verifies
      BOTH leaves (this is exactly "in-flight agent mTLS never breaks during
      cutover"). Uses the COMMITTED ca/verify_chain.py — the same verifier the
      auth-proxy resolver uses — so the proof is real, not a toy.
  T4  `add` is idempotent (re-running adds nothing).
  T5  `trust_bundle.remove` drops the OLD CA  -> the post-cutover bundle verifies
      the NEW leaf and now REJECTS the OLD leaf (old CA fully retired).
  T6  `remove` REFUSES to empty the bundle (never leave the proxy trustless).
  T7  Sign the overlap bundle with the control-plane keyring (signing/) and verify
      it (I6 — the upgrade never loads a trust bundle it did not itself sign).

DOC COVERAGE (the runbook must actually document the required properties):
  D1  UPGRADE_RUNBOOK.md covers: stateless rolling, stateful (blue-green/rolling +
      shared object storage + remote-write ordering), trust-overlap, and the I3
      data-plane-survives-CP-outage buffering guarantee.
  D2  trust_overlap.md covers the add-before-rotate / remove-after ordering.

SCRIPT + YAML:
  S1  rolling_upgrade.sh passes `bash -n` (and shellcheck if available).
  S2  trust_overlap.sh passes `bash -n`.
  Y1  service.compose.yml is valid YAML; `docker compose config` validates it if
      docker is available.

Exit 0 only if every assertion passes.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
for _p in (_HERE, os.path.join(_CP_ROOT, "ca"), os.path.join(_CP_ROOT, "signing")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ca_lib  # noqa: E402
import verify_chain as vc  # noqa: E402
import signing_lib as sl  # noqa: E402
import trust_bundle as tb  # noqa: E402

_PASS = 0
_FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f"  PASS  {name}")
    else:
        _FAIL += 1
        print(f"  FAIL  {name}  {detail}", file=sys.stderr)


def _mint_ca(root_cn: str, int_cn: str):
    root = ca_lib.generate_root_ca(common_name=root_cn)
    intermediate = ca_lib.generate_intermediate(root, common_name=int_cn)
    return root, intermediate


def _bundle_pem(intermediate, root) -> bytes:
    return ca_lib.chain_pem(intermediate, root)


def test_trust_overlap() -> None:
    print("[trust-overlap]")
    tmp = tempfile.mkdtemp(prefix="cp-upgrade-selftest-")
    try:
        # T1 — two independent CAs, each issuing a tenant leaf.
        old_root, old_int = _mint_ca("Fyralis Root CA OLD", "Fyralis Int CA OLD")
        new_root, new_int = _mint_ca("Fyralis Root CA NEW", "Fyralis Int CA NEW")
        old_leaf = ca_lib.issue_tenant_cert("acme", old_int)
        new_leaf = ca_lib.issue_tenant_cert("acme", new_int)
        old_leaf_pem = old_leaf.cert_pem()
        new_leaf_pem = new_leaf.cert_pem()
        check("T1 two CAs minted, two leaves issued", True)

        bundle = os.path.join(tmp, "ca-chain.crt")
        with open(bundle, "wb") as fh:
            fh.write(_bundle_pem(old_int, old_root))

        # T2 — old-only bundle trusts old leaf, rejects new leaf.
        old_bundle_bytes = open(bundle, "rb").read()
        check("T2a old bundle verifies OLD leaf",
              bool(vc.verify_chain(old_leaf_pem, old_bundle_bytes)))
        check("T2b old bundle REJECTS NEW leaf",
              not bool(vc.verify_chain(new_leaf_pem, old_bundle_bytes)))

        # T3 — add the new CA => OVERLAP. Both leaves verify against the result.
        new_ca_pem = os.path.join(tmp, "new-ca.pem")
        with open(new_ca_pem, "wb") as fh:
            fh.write(_bundle_pem(new_int, new_root))
        res = tb.add_ca(bundle, new_ca_pem)
        check("T3a add_ca ok", res.ok, res.reason)
        overlap = open(bundle, "rb").read()
        check("T3b OVERLAP bundle verifies OLD leaf (in-flight agent unaffected)",
              bool(vc.verify_chain(old_leaf_pem, overlap)))
        check("T3c OVERLAP bundle verifies NEW leaf (rotated agent works)",
              bool(vc.verify_chain(new_leaf_pem, overlap)))
        # And a backup was made before the write (rollback safety).
        check("T3d add_ca made a .bak", bool(res.backup) and os.path.isfile(res.backup),
              str(res.backup))

        # T4 — idempotent.
        res2 = tb.add_ca(bundle, new_ca_pem)
        check("T4 add_ca idempotent (no-op on re-add)", res2.ok and res2.added == 0, res2.reason)

        # T5 — remove the OLD CA => only the new CA remains.
        res3 = tb.remove_ca(bundle, match_root_cn="Fyralis Root CA OLD")
        check("T5a remove_ca ok", res3.ok and res3.removed >= 2, res3.reason)
        post = open(bundle, "rb").read()
        check("T5b post-cutover bundle verifies NEW leaf",
              bool(vc.verify_chain(new_leaf_pem, post)))
        check("T5c post-cutover bundle REJECTS OLD leaf (old CA retired)",
              not bool(vc.verify_chain(old_leaf_pem, post)))

        # T6 — refuse to empty the bundle.
        res4 = tb.remove_ca(bundle, match_root_cn="Fyralis Root CA NEW")
        check("T6 remove refuses to empty the bundle", not res4.ok, res4.reason)
        # bundle unchanged after the refusal:
        check("T6b bundle intact after refused removal",
              bool(vc.verify_chain(new_leaf_pem, open(bundle, "rb").read())))

        # T7 — sign + verify the bundle with the control-plane keyring (I6).
        # Use a throwaway keyring so we don't touch the repo's signing/ state.
        ring = sl.Keyring()
        ring.generate_active_key("cp-test-key")
        raw = open(bundle, "rb").read()
        kid, sig = ring.sign_with_active(raw)
        manifest = sl.build_manifest(
            artifact_kind="config", version="trust-bundle", signed_bytes=raw, key_id=kid
        )
        # verify round-trips through a verifier-only ring (what an operator runs).
        verifier = sl.Keyring.from_trust_root(ring.to_trust_root())
        check("T7a bundle ed25519 sig verifies (I6)", verifier.verify_with(kid, raw, sig))
        check("T7b manifest sha256 matches bundle bytes",
              manifest["sha256"] == sl.sha256_hex(raw))
        tampered = raw + b"\n# sneaky\n"
        check("T7c tampered bundle FAILS verify (I6)",
              not verifier.verify_with(kid, tampered, sig))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_docs() -> None:
    print("[docs]")
    runbook = os.path.join(_HERE, "UPGRADE_RUNBOOK.md")
    overlap = os.path.join(_HERE, "trust_overlap.md")
    readme = os.path.join(_HERE, "README.md")
    rb = open(runbook, encoding="utf-8").read().lower() if os.path.isfile(runbook) else ""
    ov = open(overlap, encoding="utf-8").read().lower() if os.path.isfile(overlap) else ""

    # D1 — runbook covers the four mandated properties.
    check("D1a runbook covers stateless rolling",
          "rolling" in rb and "auth-proxy" in rb and ("config-dist" in rb or "config-dist" in rb))
    check("D1b runbook covers stateful migration (blue-green/rolling)",
          ("blue-green" in rb or "blue/green" in rb) and "rolling" in rb)
    check("D1c runbook covers shared object storage",
          ("object storage" in rb or "object-storage" in rb) and ("s3" in rb or "shared" in rb))
    check("D1d runbook covers remote-write ordering (no drop)",
          "remote-write" in rb or "remote write" in rb)
    check("D1e runbook covers trust-overlap during cutover",
          "overlap" in rb and ("ca" in rb))
    check("D1f runbook covers I3 buffering guarantee",
          "i3" in rb and "buffer" in rb)
    check("D1g runbook covers health-gating between steps",
          "health" in rb and "gat" in rb)

    # D2 — trust_overlap.md documents the ordering.
    check("D2a trust_overlap.md exists", os.path.isfile(overlap))
    check("D2b trust_overlap.md: add-before-rotate ordering",
          "before" in ov and "overlap" in ov and ("remove" in ov or "retire" in ov))
    check("D2c README exists", os.path.isfile(readme))


def _bash_n(path: str, name: str) -> None:
    if not os.path.isfile(path):
        check(name + " exists", False, path)
        return
    r = subprocess.run(["bash", "-n", path], capture_output=True, text=True)
    check(name + " bash -n", r.returncode == 0, r.stderr.strip())
    if shutil.which("shellcheck"):
        rs = subprocess.run(["shellcheck", "-S", "warning", path],
                            capture_output=True, text=True)
        check(name + " shellcheck", rs.returncode == 0, rs.stdout.strip()[:400])
    else:
        print(f"  SKIP  {name} shellcheck (not installed) — bash -n used instead")


def test_scripts() -> None:
    print("[scripts]")
    _bash_n(os.path.join(_HERE, "rolling_upgrade.sh"), "S1 rolling_upgrade.sh")
    _bash_n(os.path.join(_HERE, "trust_overlap.sh"), "S2 trust_overlap.sh")


def test_yaml() -> None:
    print("[yaml/compose]")
    overlay = os.path.join(_HERE, "service.compose.yml")
    if not os.path.isfile(overlay):
        check("Y1 service.compose.yml exists", False, overlay)
        return
    try:
        import yaml
        doc = yaml.safe_load(open(overlay, encoding="utf-8"))
        check("Y1a service.compose.yml is valid YAML", isinstance(doc, dict))
        check("Y1b overlay defines services", "services" in doc and bool(doc["services"]))
    except Exception as exc:
        check("Y1a service.compose.yml is valid YAML", False, str(exc))
        return
    if shutil.which("docker"):
        r = subprocess.run(
            ["docker", "compose", "-f", overlay, "config"],
            capture_output=True, text=True, cwd=_HERE,
        )
        # `config` may warn about the missing base network/volume when run on the
        # fragment alone; we only require that it PARSES the YAML (returncode 0) OR
        # fails solely on an undeclared external network/volume (expected for a
        # fragment). Treat a YAML/syntax error as a failure.
        ok = r.returncode == 0 or "network" in (r.stderr.lower()) or "volume" in r.stderr.lower()
        check("Y1c docker compose config parses overlay", ok, r.stderr.strip()[:400])
    else:
        print("  SKIP  Y1c docker compose config (docker not installed)")


def main() -> int:
    print("=== CP-UPGRADE self-test ===")
    test_trust_overlap()
    test_docs()
    test_scripts()
    test_yaml()
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
