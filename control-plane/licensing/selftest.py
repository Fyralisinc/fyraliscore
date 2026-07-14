#!/usr/bin/env python3
"""selftest.py — end-to-end proof of the WS-LICENSE behaviour, via the REAL signing lib.

Scenarios (each must hold; any failure exits non-zero):

  1. ALLOW         — issue a 1-day license for acme + validate() → ALLOW.
  2. DENY (tamper) — flip a field in the signed license.json → signature breaks → DENY.
  3. DENY (expired)— issue an already-expired license (negative duration) → DENY.
  4. DENY (revoke) — revoke a still-valid license → validate() → DENY.

Plus negative-space guards that prove fail-closed:
  5. DENY (wrong tenant)      — a license for acme validated as bossco → DENY.
  6. DENY (wrong deployment)  — right tenant, wrong deployment_id → DENY.
  7. DENY (unknown key)       — verify against a *different* trust root → DENY.

Everything signs with ``control-plane/signing`` and verifies through
``signing/verify_bundle`` — no crypto is faked. It runs against a throwaway trust root +
keys + revocation list under a temp dir so it never touches the repo's signing state.

Run::  python selftest.py        (exit 0 = all green)
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402
import sign_bundle as sb  # noqa: E402
import verify_bundle as vb  # noqa: E402

import issue_license as il  # noqa: E402
import validator as vd  # noqa: E402
import revoke as rev  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"
_results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    _results.append((name, bool(cond), detail))
    mark = PASS if cond else FAIL
    line = f"[{mark}] {name}"
    if detail:
        line += f"  — {detail}"
    print(line)


def _mint_throwaway_trust_root(signing_root: str, key_id: str = "cp-signing-selftest") -> str:
    """Mint a real ed25519 key + trust root under ``signing_root`` (isolated from the repo)."""
    keys_dir = os.path.join(signing_root, "keys")
    os.makedirs(keys_dir, exist_ok=True)
    priv, pub = sl.generate_keypair()
    with open(os.path.join(keys_dir, f"{key_id}.private.pem"), "wb") as fh:
        fh.write(sl.private_key_to_pem(priv))
    ring = sl.Keyring()
    ring.add_key(key_id, public=pub, private=priv, make_active=True)
    trust_root_path = os.path.join(signing_root, "trust_root.json")
    with open(trust_root_path, "w", encoding="utf-8") as fh:
        json.dump(ring.to_trust_root(), fh, indent=2, sort_keys=True)
    return trust_root_path


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ws-license-selftest-")
    print(f"# WS-LICENSE selftest (workdir {tmp})\n")

    # The real signing module resolves its keys/trust-root via module-level constants.
    # Point them at our throwaway signing root so we never touch the repo's signing state.
    signing_root = os.path.join(tmp, "signing")
    trust_root_path = _mint_throwaway_trust_root(signing_root)
    sb.KEYS_DIR = os.path.join(signing_root, "keys")
    sb.TRUST_ROOT_PATH = trust_root_path
    vb.TRUST_ROOT_PATH = trust_root_path
    vd.DEFAULT_TRUST_ROOT_PATH = trust_root_path

    revocations_path = os.path.join(tmp, "revocations.json")

    TENANT = "acme"
    DEPLOY = "acme-use1-7f3a"

    # ----------------------------------------------------------------- #
    # 1. ALLOW: 1-day license for acme.                                 #
    # ----------------------------------------------------------------- #
    good_dir = os.path.join(tmp, "lic-good")
    res = il.issue_license(
        tenant_id=TENANT,
        deployment_id=DEPLOY,
        plan="enterprise",
        duration_days=1,
        features=["telemetry_t3", "byoc"],
        out_dir=good_dir,
    )
    # The bundle must be the signed trio.
    bundle_ok = all(
        os.path.isfile(p) for p in (res["license_path"], res["sig_path"], res["manifest_path"])
    )
    check("issue: 1-day license bundle written (json + sig + manifest)", bundle_ok)

    d = vd.validate(
        license_dir=good_dir,
        expected_tenant_id=TENANT,
        expected_deployment_id=DEPLOY,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check("validate(valid 1-day license) -> ALLOW", d.allow, d.reason)
    check("  all four gates passed", d.allow and all(d.checks.values()), str(d.checks))

    # ----------------------------------------------------------------- #
    # 2. DENY (tamper): flip a field in license.json -> sig breaks.     #
    # ----------------------------------------------------------------- #
    tamper_dir = os.path.join(tmp, "lic-tamper")
    shutil.copytree(good_dir, tamper_dir)
    tpath = os.path.join(tamper_dir, "license.json")
    with open(tpath, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    doc["plan"] = "enterprise_ELEVATED"  # privilege escalation attempt
    doc["features"].append("god_mode")
    with open(tpath, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
    d = vd.validate(
        license_dir=tamper_dir,
        expected_tenant_id=TENANT,
        expected_deployment_id=DEPLOY,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check(
        "validate(tampered field) -> DENY (signature fail)",
        d.deny and d.code == vd.CODE_BAD_SIGNATURE,
        f"{d.code}: {d.reason}",
    )

    # ----------------------------------------------------------------- #
    # 3. DENY (expired): negative-duration license.                     #
    # ----------------------------------------------------------------- #
    exp_dir = os.path.join(tmp, "lic-expired")
    il.issue_license(
        tenant_id=TENANT,
        deployment_id=DEPLOY,
        plan="trial",
        duration_days=-1,  # expired one day ago, but validly signed
        out_dir=exp_dir,
    )
    d = vd.validate(
        license_dir=exp_dir,
        expected_tenant_id=TENANT,
        expected_deployment_id=DEPLOY,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check(
        "validate(already-expired, validly signed) -> DENY (expired)",
        d.deny and d.code == vd.CODE_EXPIRED,
        f"{d.code}: {d.reason}",
    )

    # ----------------------------------------------------------------- #
    # 4. DENY (revoke): revoke a still-valid license.                   #
    # ----------------------------------------------------------------- #
    rev_dir = os.path.join(tmp, "lic-revoke")
    rres = il.issue_license(
        tenant_id=TENANT,
        deployment_id=DEPLOY,
        plan="pro",
        duration_days=30,
        out_dir=rev_dir,
    )
    # Sanity: valid before revoke.
    d_before = vd.validate(
        license_dir=rev_dir,
        expected_tenant_id=TENANT,
        expected_deployment_id=DEPLOY,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check("  (pre-revoke) 30-day license -> ALLOW", d_before.allow, d_before.reason)

    rev.add_revocation(
        rtype="license_id",
        value=rres["license_id"],
        reason="selftest revoke-before-expiry",
        path=revocations_path,
    )
    d_after = vd.validate(
        license_dir=rev_dir,
        expected_tenant_id=TENANT,
        expected_deployment_id=DEPLOY,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check(
        "validate(revoked-before-expiry) -> DENY (revoked)",
        d_after.deny and d_after.code == vd.CODE_REVOKED,
        f"{d_after.code}: {d_after.reason}",
    )

    # ----------------------------------------------------------------- #
    # 5/6. DENY (identity): wrong tenant / wrong deployment.            #
    # ----------------------------------------------------------------- #
    d_wt = vd.validate(
        license_dir=good_dir,
        expected_tenant_id="bossco",  # someone else's tenant
        expected_deployment_id=DEPLOY,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check(
        "validate(license reused by wrong tenant) -> DENY (tenant mismatch)",
        d_wt.deny and d_wt.code == vd.CODE_TENANT_MISMATCH,
        f"{d_wt.code}: {d_wt.reason}",
    )
    d_wd = vd.validate(
        license_dir=good_dir,
        expected_tenant_id=TENANT,
        expected_deployment_id="acme-use1-DEADBEEF",
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check(
        "validate(right tenant, wrong deployment) -> DENY (deployment mismatch)",
        d_wd.deny and d_wd.code == vd.CODE_DEPLOYMENT_MISMATCH,
        f"{d_wd.code}: {d_wd.reason}",
    )

    # ----------------------------------------------------------------- #
    # 7. DENY (unknown key): verify against a DIFFERENT trust root.     #
    # ----------------------------------------------------------------- #
    other_signing = os.path.join(tmp, "signing-other")
    other_trust_root = _mint_throwaway_trust_root(other_signing, key_id="cp-signing-other")
    d_uk = vd.validate(
        license_dir=good_dir,
        expected_tenant_id=TENANT,
        expected_deployment_id=DEPLOY,
        trust_root_path=other_trust_root,  # signed by a key this root doesn't know
        revocations_path=revocations_path,
    )
    check(
        "validate(signed by unknown key) -> DENY (bad signature / unknown key)",
        d_uk.deny and d_uk.code == vd.CODE_BAD_SIGNATURE,
        f"{d_uk.code}: {d_uk.reason}",
    )

    # ----------------------------------------------------------------- #
    # 8. validate_for_deployment binds identity from a DeploymentRecord #
    # ----------------------------------------------------------------- #
    rec = {"tenant_id": TENANT, "deployment_id": DEPLOY}
    d_rec = vd.validate_for_deployment(
        rec,
        license_dir=good_dir,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
    )
    check("validate_for_deployment(matching record) -> ALLOW", d_rec.allow, d_rec.reason)

    print()
    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_total = len(_results)
    all_green = n_pass == n_total
    print(f"# {n_pass}/{n_total} checks passed — {'ALL GREEN' if all_green else 'FAILURES PRESENT'}")

    shutil.rmtree(tmp, ignore_errors=True)
    return 0 if all_green else 1


if __name__ == "__main__":
    raise SystemExit(main())
