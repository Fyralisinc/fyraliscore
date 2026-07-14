#!/usr/bin/env python3
"""selftest — validate the WS-INSTALLER deliverables end-to-end.

Run:
    /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python selftest.py

Checks (all must pass):
  A. deployment.compose.yml parses and declares the boundary + agent services,
     the data-plane subset, and the bundle mounts (cert/key/ca + bundle dir).
  B. `docker compose -f deployment.compose.yml config` parses cleanly when a
     rendered env is supplied (skipped with a note if docker is unavailable).
  C. make_sample_bundle mints a REAL bundle that bundle_lib.validate_bundle
     accepts (cert SAN round-trip, trust-root, I6 signature verify, license fresh).
  D. install.sh --dry-run on that sample bundle exits 0 and reports VALID.
  E. negative: an EXPIRED-license bundle is REJECTED (fail-closed), and a
     tampered config is REJECTED (I6).
  F. manifest_to_env maps every ${...} var the overlay's required interpolations
     need.

No network, no containers required for A/C/D/E/F. B is best-effort.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile

import yaml

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_DIR = os.path.dirname(_HERE)
for _p in (_HERE, _CP_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import bundle_lib  # noqa: E402
import make_sample_bundle  # noqa: E402

OVERLAY = os.path.join(_HERE, "deployment.compose.yml")

PASS = 0
FAIL = 0
NOTES: list[str] = []


def check(cond: bool, msg: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {msg}")
    else:
        FAIL += 1
        print(f"  [FAIL] {msg}")


def note(msg: str) -> None:
    NOTES.append(msg)
    print(f"  [NOTE] {msg}")


def _load_overlay() -> dict:
    """Load the overlay YAML, tolerating compose ${VAR:?err} interpolation.

    yaml.safe_load treats ${...} as plain scalar text, which is exactly what we
    want for structural assertions (we are not interpolating, just inspecting).
    """
    with open(OVERLAY, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# A. overlay structure
# ---------------------------------------------------------------------------
def test_overlay_structure() -> None:
    print("A. deployment.compose.yml structure")
    doc = _load_overlay()
    svcs = doc.get("services", {})
    check("boundary" in svcs, "service 'boundary' present")
    check("agent" in svcs, "service 'agent' present")
    # data-plane subset (referenced from the repo-root compose images)
    for dp in ("postgres", "redis", "kafka", "postgres-exporter", "kafka-exporter", "redis-exporter"):
        check(dp in svcs, f"data-plane service '{dp}' present")

    # boundary mounts: bundle cert/key/ca + the boundary config
    bvols = svcs["boundary"]["volumes"]
    blob = "\n".join(bvols)
    check("/etc/otelcol/config.yaml" in blob, "boundary mounts the OTel collector config")
    check("ca.crt:/etc/fyralis/ca/ca.crt" in blob, "boundary mounts bundle ca.crt")
    check("client.crt:/etc/fyralis/agent/client.crt" in blob, "boundary mounts bundle client.crt")
    check("client.key:/etc/fyralis/agent/client.key" in blob, "boundary mounts bundle client.key")
    check("FYRALIS_BUNDLE_DIR" in blob, "boundary cert mounts are parameterized by ${FYRALIS_BUNDLE_DIR}")

    # boundary identity env (C4 keys the collector resource processor reads)
    benv = svcs["boundary"]["environment"]
    for k in ("FYRALIS_TENANT_ID", "FYRALIS_DEPLOYMENT_ID", "FYRALIS_REGION",
              "FYRALIS_TELEMETRY_TIER", "FYRALIS_AUTH_PROXY_URL"):
        check(k in benv, f"boundary env has {k}")

    # agent: bundle mount + buffer + NO inbound ports (I2)
    avols = "\n".join(svcs["agent"]["volumes"])
    check("FYRALIS_BUNDLE_DIR" in avols and "/etc/fyralis/bundle" in avols,
          "agent mounts the bundle dir read-only")
    check("agent-buffer:/var/lib/fyralis/buffer" in avols,
          "agent has a persistent outbound buffer volume (I3)")
    check("ports" not in svcs["agent"],
          "agent declares NO inbound ports (I2: outbound-only)")
    aenv = svcs["agent"]["environment"]
    check("FYRALIS_TRUST_ROOT" in aenv, "agent env points at the bundle trust_root (I6 verify)")
    check("FYRALIS_LICENSE_PATH" in aenv, "agent env points at the bundle license")
    check("FYRALIS_CONSOLE_URL" in aenv, "agent env has the console URL (heartbeat target)")

    # networks
    check("dp-net" in doc.get("networks", {}), "dp-net network declared (boundary scrapes by name)")


# ---------------------------------------------------------------------------
# B. docker compose config (best-effort)
# ---------------------------------------------------------------------------
def _docker_compose_available() -> list[str] | None:
    try:
        if subprocess.run(["docker", "compose", "version"], capture_output=True).returncode == 0:
            return ["docker", "compose"]
    except FileNotFoundError:
        pass
    try:
        if subprocess.run(["docker-compose", "version"], capture_output=True).returncode == 0:
            return ["docker-compose"]
    except FileNotFoundError:
        pass
    return None


def test_compose_config(bundle_dir: str) -> None:
    print("B. docker compose config parse")
    dc = _docker_compose_available()
    if dc is None:
        note("docker compose unavailable — skipping real `config` parse (A covers structure)")
        return
    env = bundle_lib.manifest_to_env(
        bundle_lib.validate_bundle(bundle_dir, verify_signatures=False).manifest,
        bundle_dir,
        control_plane_dir=_CP_DIR,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
        for k, v in env.items():
            fh.write(f"{k}={v}\n")
        env_file = fh.name
    try:
        proc = subprocess.run(
            dc + ["-f", OVERLAY, "--env-file", env_file, "config"],
            capture_output=True, text=True,
        )
        ok = proc.returncode == 0
        check(ok, "`docker compose -f deployment.compose.yml config` parses")
        if not ok:
            print("    stderr:", proc.stderr.strip().splitlines()[-5:])
        else:
            out = yaml.safe_load(proc.stdout)
            rsvcs = out.get("services", {})
            check("boundary" in rsvcs and "agent" in rsvcs,
                  "rendered config still has boundary + agent")
            # cert mounts must have resolved to the real bundle dir (no unresolved ${})
            bmounts = yaml.safe_dump(rsvcs["boundary"].get("volumes", []))
            check(bundle_dir in bmounts or os.path.abspath(bundle_dir) in bmounts,
                  "boundary cert mounts resolved to the bundle dir")
    finally:
        os.unlink(env_file)


# ---------------------------------------------------------------------------
# C/D/E/F. bundle validation + install --dry-run + negatives
# ---------------------------------------------------------------------------
def test_valid_bundle(bundle_dir: str) -> None:
    print("C. sample bundle validates (cert SAN + trust-root + I6 + fresh license)")
    res = bundle_lib.validate_bundle(bundle_dir)
    for c in res.checks:
        print(f"      · {c}")
    for w in res.warnings:
        print(f"      ~ {w}")
    check(res.ok, "validate_bundle accepts the freshly-minted sample bundle")
    # the strong checks must actually have run (not silently skipped)
    joined = " ".join(res.checks)
    check("C1" in joined, "cert SAN round-trip (C1) actually executed")
    check("I6" in joined, "signature verification (I6) actually executed")


def test_install_dry_run(bundle_dir: str) -> None:
    print("D. install.sh --dry-run")
    proc = subprocess.run(
        ["bash", os.path.join(_HERE, "install.sh"), "--dry-run", bundle_dir],
        capture_output=True, text=True,
    )
    out = proc.stdout + proc.stderr
    check(proc.returncode == 0, "install.sh --dry-run exits 0 on a valid bundle")
    check("RESULT: VALID" in out, "install.sh --dry-run reports RESULT: VALID")
    check("Stopping before render" in out, "install.sh --dry-run does NOT launch anything")


def test_negatives() -> None:
    print("E. negative cases (fail-closed)")
    # E1 — expired license is rejected.
    with tempfile.TemporaryDirectory() as d:
        bd = os.path.join(d, "expired-bundle")
        make_sample_bundle.make_bundle(bd, tenant_id="acme", expired=True)
        res = bundle_lib.validate_bundle(bd)
        check(not res.ok, "expired-license bundle is REJECTED")
        check(any("EXPIRED" in e for e in res.errors), "rejection cites the expired license")
        # install.sh --dry-run must also fail
        proc = subprocess.run(
            ["bash", os.path.join(_HERE, "install.sh"), "--dry-run", bd],
            capture_output=True, text=True,
        )
        check(proc.returncode != 0, "install.sh --dry-run exits non-zero on expired bundle")

    # E2 — tampered config breaks the signature (I6).
    with tempfile.TemporaryDirectory() as d:
        bd = os.path.join(d, "tampered-bundle")
        make_sample_bundle.make_bundle(bd, tenant_id="acme")
        cfg = os.path.join(bd, "config.json")
        data = open(cfg).read().replace('"T1"', '"T3"')  # change without re-signing
        open(cfg, "w").write(data)
        res = bundle_lib.validate_bundle(bd)
        check(not res.ok, "tampered config.json is REJECTED (I6)")
        check(any("config.json" in e and "I6" in e for e in res.errors),
              "rejection cites config.json signature failure")

    # E3 — wrong-tenant cert (cert SAN != manifest) is rejected.
    with tempfile.TemporaryDirectory() as d:
        good = os.path.join(d, "good")
        other = os.path.join(d, "other")
        make_sample_bundle.make_bundle(good, tenant_id="acme")
        make_sample_bundle.make_bundle(other, tenant_id="globex")
        # swap acme's client cert for globex's -> SAN mismatch
        import shutil
        shutil.copy(os.path.join(other, "client.crt"), os.path.join(good, "client.crt"))
        res = bundle_lib.validate_bundle(good)
        # may be a warning if cryptography is unavailable; assert it is caught when present
        if any("SAN" in c for c in res.checks) or any("C1" in e or "SAN" in e for e in res.errors):
            check(not res.ok, "cert-SAN/tenant mismatch is REJECTED (C1)")
        else:
            note("ca_lib/cryptography unavailable — SAN mismatch check skipped")


def test_manifest_env_completeness(bundle_dir: str) -> None:
    print("F. manifest_to_env covers the overlay's required interpolations")
    res = bundle_lib.validate_bundle(bundle_dir, verify_signatures=False)
    env = bundle_lib.manifest_to_env(res.manifest, bundle_dir, control_plane_dir=_CP_DIR)
    raw = open(OVERLAY).read()
    # required interpolations look like ${VAR:?...}
    required = set(re.findall(r"\$\{([A-Z_]+):\?", raw))
    missing = sorted(v for v in required if v not in env)
    check(not missing, f"every required ${{VAR:?}} is supplied by manifest_to_env (missing: {missing})")
    # the boundary config path the overlay defaults to must exist
    check(os.path.isfile(env["FYRALIS_BOUNDARY_CONFIG"]),
          "FYRALIS_BOUNDARY_CONFIG points at the real boundary collector config")


def main() -> int:
    print("=" * 70)
    print("WS-INSTALLER self-test")
    print("=" * 70)
    with tempfile.TemporaryDirectory() as d:
        bundle_dir = os.path.join(d, "sample-bundle")
        make_sample_bundle.make_bundle(bundle_dir, tenant_id="acme", region="us-east-1", tier="T1")

        test_overlay_structure()
        test_valid_bundle(bundle_dir)
        test_install_dry_run(bundle_dir)
        test_negatives()
        test_manifest_env_completeness(bundle_dir)
        test_compose_config(bundle_dir)

    print("=" * 70)
    print(f"RESULT: {PASS} passed, {FAIL} failed, {len(NOTES)} notes")
    print("=" * 70)
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
