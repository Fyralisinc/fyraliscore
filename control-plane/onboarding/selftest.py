#!/usr/bin/env python3
"""selftest.py — end-to-end self-test for WS-ONBOARD (FR-E atomicity).

Runs the onboarding transaction against:
  * a **real CA** (a throwaway intermediate hierarchy bootstrapped into a temp
    ``pki/`` so the committed ``ca/pki`` is never touched),
  * **real signing** (a throwaway ed25519 trust root + key),
  * an **in-process console** honoring the P4 REST contract (``fake_console``),
  * a **throwaway tenant registry** (temp JSON; the committed registry is untouched).

Two scenarios:

  HAPPY PATH (``onboard --tenant acme --region us-east --plan standard``):
    asserts a bundle is produced with a valid cert (tenant SAN = ``acme``) and a
    valid (signature-verifies + unexpired) license, the registry has an *active*
    ``acme`` entry, and the console lists the deployment.

  ROLLBACK (force a failure after the cert/license/bundle steps):
    asserts FULL rollback — no ``acme`` registry entry remains, no bundle dir
    remains, and the console no longer lists the deployment.

Exit 0 on success; non-zero (and a printed reason) on any failed assertion. No
network and no external services required.

    python selftest.py
"""

from __future__ import annotations

import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
_CA_DIR = os.path.join(_CP_ROOT, "ca")
_SIGNING_DIR = os.path.join(_CP_ROOT, "signing")
for _p in (_CP_ROOT, _CA_DIR, _SIGNING_DIR, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# CA / signing bootstrap (committed primitives)
import bootstrap_ca  # noqa: E402  (ca/bootstrap_ca.py)
import signing_lib as sl  # noqa: E402  (signing/signing_lib.py)
import sign_bundle  # noqa: E402
import verify_bundle  # noqa: E402

import ca_lib  # noqa: E402  (ca/ca_lib.py)
import registry as ca_registry  # noqa: E402

import fake_console  # noqa: E402
import license_mint as lm  # noqa: E402
import console_client as cc  # noqa: E402
import onboard as ob  # noqa: E402


_FAILS = 0


def check(cond: bool, msg: str) -> None:
    global _FAILS
    status = "ok  " if cond else "FAIL"
    print(f"  [{status}] {msg}")
    if not cond:
        _FAILS += 1


def _bootstrap_throwaway_signing(keys_dir: str, trust_root_path: str, key_id: str) -> None:
    """Mint an ed25519 keypair + a trust root in a throwaway dir, then point the
    committed ``sign_bundle``/``verify_bundle`` modules at them (module-level path
    constants — overridden only for the duration of this in-process test)."""
    os.makedirs(keys_dir, exist_ok=True)
    priv, pub = sl.generate_keypair()
    with open(os.path.join(keys_dir, f"{key_id}.private.pem"), "wb") as fh:
        fh.write(sl.private_key_to_pem(priv))
    doc = {
        "version": 1,
        "active_key_id": key_id,
        "keys": {key_id: {"pubkey": sl.public_key_to_b64(pub), "status": "active"}},
    }
    import json

    with open(trust_root_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")

    # Redirect the signer + verifier at the throwaway material.
    sign_bundle.TRUST_ROOT_PATH = trust_root_path
    sign_bundle.KEYS_DIR = keys_dir
    verify_bundle.TRUST_ROOT_PATH = trust_root_path


def run() -> int:
    tmp = tempfile.mkdtemp(prefix="onboard-selftest-")
    print(f"sandbox: {tmp}")

    pki_dir = os.path.join(tmp, "pki")
    keys_dir = os.path.join(tmp, "signing-keys")
    trust_root = os.path.join(tmp, "trust_root.json")
    registry_path = os.path.join(tmp, "tenant_registry.json")
    bundles_root = os.path.join(tmp, "bundles")

    # --- real CA + real signing material (throwaway) ----------------------
    bootstrap_ca.bootstrap(pki_dir, force=True, key_password=None)
    _bootstrap_throwaway_signing(keys_dir, trust_root, key_id="cp-signing-selftest")
    print("bootstrapped throwaway CA + ed25519 trust root")

    # =====================================================================
    print("\n== scenario 1: HAPPY PATH ==")
    app = fake_console.build_app()
    result = ob.onboard(
        tenant="acme",
        region="us-east",
        plan="standard",
        console_app=app,
        bundles_root=bundles_root,
        pki_dir=pki_dir,
        registry_path=registry_path,
        trust_root_path=trust_root,
        telemetry_tier="T1",
    )

    # bundle exists with a valid cert (SAN=acme)
    bundle = result.bundle_dir
    crt_path = os.path.join(bundle, "cert", "acme.crt")
    key_path = os.path.join(bundle, "cert", "acme.key")
    check(os.path.isdir(bundle), f"bundle dir created: {bundle}")
    check(os.path.isfile(crt_path), "bundle contains tenant cert")
    check(os.path.isfile(key_path), "bundle contains tenant private key")
    san = ca_lib.extract_tenant_from_cert(open(crt_path, "rb").read())
    check(san == "acme", f"cert SAN tenant == acme (got {san!r})")

    # valid signed license (signature verifies + unexpired)
    lic_path = os.path.join(bundle, "acme.license.json")
    check(os.path.isfile(lic_path), "bundle contains license")
    try:
        lic_doc = lm.verify_license_file(lic_path, trust_root_path=trust_root)
        check(True, "license signature verifies + is unexpired")
        check(lic_doc["tenant_id"] == "acme", "license tenant_id == acme")
        check(lic_doc["deployment_id"] == result.deployment_id, "license deployment_id matches")
        check("metrics" in lic_doc["features"], "license carries plan features")
    except Exception as exc:
        check(False, f"license verify raised: {exc}")

    # signed agent config verifies + points at outbound-only console
    cfg_path = os.path.join(bundle, "agent-config.json")
    cfg_res = verify_bundle.verify_file(cfg_path, trust_root_path=trust_root)
    check(cfg_res.ok, f"agent-config signature verifies ({cfg_res.reason})")
    import json

    cfg = json.load(open(cfg_path))
    check(cfg.get("outbound_only") is True, "agent config is outbound-only (I2)")
    check(os.path.isfile(os.path.join(bundle, "trust_root.json")),
          "bundle ships the public trust root (agent verifies with it)")

    # registry has an ACTIVE acme entry
    rows = ca_registry.find_by_tenant("acme", path=registry_path)
    check(len(rows) == 1, f"registry has exactly one acme row (got {len(rows)})")
    active = [r for r in rows.values() if r.get("status") == "active"]
    check(len(active) == 1, "the acme registry row is active")
    check(result.fingerprint in rows, "registry keyed by the issued cert fingerprint")

    # console lists the deployment
    client = cc.ASGIConsoleClient(app)
    listed = client.has_deployment(result.deployment_id)
    check(listed, f"console lists deployment {result.deployment_id}")
    fleet = client.list_deployments()
    check(any(d["tenant_id"] == "acme" for d in fleet), "console fleet includes acme")
    client.close()

    happy_deployment_id = result.deployment_id

    # =====================================================================
    print("\n== scenario 2: ROLLBACK (forced failure) ==")
    app2 = fake_console.build_app()
    threw = False
    try:
        ob.onboard(
            tenant="acme",
            region="us-east",
            plan="standard",
            console_app=app2,
            bundles_root=bundles_root,
            pki_dir=pki_dir,
            registry_path=registry_path,
            trust_root_path=trust_root,
            fail_after="heartbeat",  # fail late: cert + license + bundle + heartbeat all applied
        )
    except ob.OnboardError:
        threw = True
    check(threw, "onboard raised OnboardError on injected failure")

    # No NEW acme registry entry remains (only the happy-path one from scenario 1).
    rows_after = ca_registry.find_by_tenant("acme", path=registry_path)
    check(len(rows_after) == 1, f"rolled back the new acme registry row "
          f"(still exactly the 1 from scenario 1; got {len(rows_after)})")
    check(result.fingerprint in rows_after, "scenario-1 row untouched by scenario-2 rollback")

    # No bundle dir remains for the failed onboarding. Its deployment_id differs
    # from scenario 1; assert NO bundle other than the happy one exists.
    bundle_dirs = sorted(os.listdir(bundles_root)) if os.path.isdir(bundles_root) else []
    check(bundle_dirs == [happy_deployment_id],
          f"only the happy-path bundle remains (got {bundle_dirs})")

    # The console must not list the failed deployment (rolled back).
    client2 = cc.ASGIConsoleClient(app2)
    fleet2 = client2.list_deployments()
    check(len(fleet2) == 0, f"failed onboarding left no console deployment (got {len(fleet2)})")
    client2.close()

    # =====================================================================
    print("\n== scenario 2b: ROLLBACK after an EARLY step (cert) — no orphan bundle ==")
    # Regression guard: a failure right after the cert step (which already created
    # bundle_dir/cert/) must still leave NO bundle dir behind.
    app2b = fake_console.build_app()
    threw2b = False
    try:
        ob.onboard(
            tenant="acme",
            region="us-east",
            plan="standard",
            console_app=app2b,
            bundles_root=bundles_root,
            pki_dir=pki_dir,
            registry_path=registry_path,
            trust_root_path=trust_root,
            fail_after="cert",  # earliest side-effecting step
        )
    except ob.OnboardError:
        threw2b = True
    check(threw2b, "onboard raised on failure right after the cert step")
    bundle_dirs_2b = sorted(os.listdir(bundles_root)) if os.path.isdir(bundles_root) else []
    check(bundle_dirs_2b == [happy_deployment_id],
          f"early-step rollback left no orphan bundle dir (got {bundle_dirs_2b})")
    rows_2b = ca_registry.find_by_tenant("acme", path=registry_path)
    check(len(rows_2b) == 1, f"early-step rollback removed the new acme row (got {len(rows_2b)})")

    # =====================================================================
    print("\n== scenario 3: rollback on a REAL step failure (no signing material) ==")
    # Point signing at a non-existent trust root so the LICENSE step genuinely
    # fails (not an injected fault) and assert the cert/registry row is rolled back.
    saved_tr = sign_bundle.TRUST_ROOT_PATH
    sign_bundle.TRUST_ROOT_PATH = os.path.join(tmp, "does-not-exist.json")
    app3 = fake_console.build_app()
    threw3 = False
    try:
        ob.onboard(
            tenant="beta",
            region="us-west",
            plan="standard",
            console_app=app3,
            bundles_root=bundles_root,
            pki_dir=pki_dir,
            registry_path=registry_path,
            trust_root_path=trust_root,
        )
    except ob.OnboardError:
        threw3 = True
    finally:
        sign_bundle.TRUST_ROOT_PATH = saved_tr
    check(threw3, "onboard raised when license signing genuinely failed")
    beta_rows = ca_registry.find_by_tenant("beta", path=registry_path)
    check(len(beta_rows) == 0, f"beta cert/registry row rolled back on real failure "
          f"(got {len(beta_rows)})")

    # =====================================================================
    print("\n== scenario 3b: ROLLBACK against the REAL console (orphan-row guard) ==")
    # The fake console only mints ids on register (no row until heartbeat), so it
    # cannot expose the orphaned-console-row bug. The REAL console's
    # POST /api/v1/register CREATES a row immediately — so this scenario drives
    # onboarding against console/app.py, forces a failure AFTER register, and
    # asserts the console has ZERO rows for the tenant, the registry has no active
    # entry, and no bundle remains (FR-E atomicity, incl. console deregister).
    _CONSOLE_DIR = os.path.join(_CP_ROOT, "console")
    if _CONSOLE_DIR not in sys.path:
        sys.path.insert(0, _CONSOLE_DIR)
    import app as real_console_app  # console/app.py
    import store as real_console_store  # console/store.py

    # The REAL console requires a write-path bearer token (I4); build it with one
    # and authenticate every onboarding write against it.
    real_token = "selftest-console-ingest-token"
    real_store = real_console_store.DeploymentStore(persist=False)
    real_app = real_console_app.create_app(real_store, ingest_token=real_token)

    # Sanity: this console DOES create a row on register (unlike the fake one).
    pre_client = cc.ASGIConsoleClient(real_app, token=real_token)
    reg = pre_client.register(region="us-east", plan="standard", tenant_id="gamma")
    check(pre_client.has_deployment(reg["deployment_id"]),
          "real console creates a deployment row on register (precondition)")
    # Clean that probe row up so it doesn't pollute the assertion below.
    pre_client.deregister(reg["deployment_id"])
    pre_client.close()

    threw3b = False
    try:
        ob.onboard(
            tenant="gamma",
            region="us-east",
            plan="standard",
            console_app=real_app,            # the REAL console, not the fake
            console_token=real_token,        # authenticate writes (I4)
            bundles_root=bundles_root,
            pki_dir=pki_dir,
            registry_path=registry_path,
            trust_root_path=trust_root,
            fail_after="cert",               # fail AFTER register + cert
        )
    except ob.OnboardError:
        threw3b = True
    check(threw3b, "onboard raised on failure after register against the real console")

    # The console must have ZERO rows for gamma (the register-created row + the
    # heartbeat row, if any, were all deregistered by the rollback).
    real_client = cc.ASGIConsoleClient(real_app)
    real_fleet = real_client.list_deployments()
    gamma_rows = [d for d in real_fleet if d["tenant_id"] == "gamma"]
    check(len(gamma_rows) == 0,
          f"real console has ZERO rows for gamma after rollback (got {len(gamma_rows)})")
    check(len(real_fleet) == 0,
          f"real console fleet is empty after rollback (got {len(real_fleet)})")
    real_client.close()

    # The registry has no ACTIVE gamma entry (cert undo ran).
    gamma_reg = ca_registry.find_by_tenant("gamma", path=registry_path)
    gamma_active = [r for r in gamma_reg.values() if r.get("status") == "active"]
    check(len(gamma_active) == 0,
          f"no active gamma registry entry after rollback (got {len(gamma_active)})")
    check(len(gamma_reg) == 0,
          f"no gamma registry row remains at all after rollback (got {len(gamma_reg)})")

    # No bundle remains for gamma (only the happy-path acme bundle).
    bundle_dirs_3b = sorted(os.listdir(bundles_root)) if os.path.isdir(bundles_root) else []
    check(bundle_dirs_3b == [happy_deployment_id],
          f"no gamma bundle remains after rollback (got {bundle_dirs_3b})")

    # =====================================================================
    print("\n== scenario 4: offboard the happy-path tenant ==")
    import offboard as off

    summary = off.offboard(
        tenant="acme",
        deployment_id=happy_deployment_id,
        console_app=app,  # same app from scenario 1 (still lists acme)
        registry_path=registry_path,
        bundles_root=bundles_root,
        purge_registry=False,
        purge_bundle=True,
    )
    check(len(summary["revoked_fingerprints"]) == 1, "offboard revoked the acme cert")
    rows_off = ca_registry.find_by_tenant("acme", path=registry_path)
    revoked = [r for r in rows_off.values() if r.get("status") == "revoked"]
    check(len(revoked) == 1, "acme registry row is now revoked (proxy 403s the cert)")
    check(summary["deregistered_from_console"], "offboard deregistered acme from console")
    check(not os.path.isdir(os.path.join(bundles_root, happy_deployment_id)),
          "offboard purged the acme bundle")

    # =====================================================================
    print()
    if _FAILS:
        print(f"SELFTEST FAILED: {_FAILS} assertion(s) failed")
        return 1
    print("SELFTEST PASSED: all assertions green")
    print(f"(sandbox left at {tmp} for inspection)")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
