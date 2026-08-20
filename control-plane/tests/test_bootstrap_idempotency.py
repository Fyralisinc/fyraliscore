"""test_bootstrap_idempotency.py — bootstrap onboard idempotency + registry consistency.

Guards LIMITATIONS L-3: the old ``bootstrap.sh`` onboard guard only checked for
``_runtime/agent/license.json`` (+ ``client.crt`` + ``.env``) and skipped
onboarding when those existed — IGNORING the tracked, persistent
``ca/tenant_registry.json`` the auth-proxy bind-mounts. A partially-onboarded
prior run (cert/bundle present but the registry row reset, or ``_runtime`` staged
without a bundle) printed "already onboarded" and exited 0, leaving the proxy to
403 every push.

These tests run the REAL ``bootstrap.sh --no-docker`` (so ``--no-docker`` mode is
exercised) inside a throwaway /tmp COPY of the control-plane tree, so the
committed tree / registry are never touched. The smoke step the script runs at
the end is stubbed out in the copy (we are testing the onboard/reconcile path,
not the metric round-trip — that is e2e_smoke.py's job).

Scenarios
---------
  1. RUN TWICE        — bootstrap, then bootstrap again: assert EXACTLY one active
                        registry row, one complete bundle, one staged runtime, one
                        ``.env`` deployment binding — no duplicates, no orphans, and
                        the second run converges to the SAME deployment (true
                        idempotency, not a re-onboard).
  2. PARTIAL: registry reset — simulate the exact L-3 trap (reset the registry to
                        ``{}`` while ``_runtime`` + bundle survive), re-run: assert
                        the reconcile RE-onboards and the active row is restored
                        (so the proxy no longer 403s), converging to one consistent
                        tenant.
  3. PARTIAL: runtime wiped   — drop the staged ``_runtime`` material while the
                        registry row + bundle survive, re-run: assert the runtime is
                        restaged and exactly one consistent tenant remains.

Run::

    pytest test_bootstrap_idempotency.py -v
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
_CP_SRC = _HERE.parent  # control-plane/

VENV_PY = "/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python"
PYTHON = VENV_PY if os.path.exists(VENV_PY) else sys.executable

# Stub smoke: bootstrap.sh runs tests/e2e_smoke.py at the end of --no-docker; in
# the copy we replace it with a no-op so the test stays fast + deterministic (the
# real smoke is exercised by make smoke / test_e2e.py).
_SMOKE_STUB = "print('[stub smoke] skipped in idempotency test')\n"

TENANT = "acme"


# --------------------------------------------------------------------------- #
# Helpers: build a throwaway copy, run bootstrap, inspect the converged state. #
# --------------------------------------------------------------------------- #
def _copy_tree(dst: Path) -> Path:
    """Copy the control-plane tree into a throwaway dir (skip heavy/generated)."""
    ignore = shutil.ignore_patterns(
        "__pycache__", "*.pyc", ".git", "pki", "keys", "trust_root.json",
        "_runtime", "bundles", "tls", ".env",
    )
    shutil.copytree(_CP_SRC, dst, ignore=ignore, symlinks=True)
    # Start from a CLEAN registry (the committed one is {}; be explicit).
    (dst / "ca" / "tenant_registry.json").write_text("{}\n")
    # Replace the smoke with a no-op stub.
    (dst / "tests" / "e2e_smoke.py").write_text(_SMOKE_STUB)
    return dst


def _run_bootstrap(cp: Path) -> subprocess.CompletedProcess:
    """Run ./bootstrap.sh --no-docker in the copy with PYTHON pinned."""
    env = dict(os.environ)
    env["PYTHON"] = PYTHON
    env["TENANT"] = TENANT
    proc = subprocess.run(
        ["bash", str(cp / "bootstrap.sh"), "--no-docker"],
        cwd=str(cp),
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    return proc


def _reconcile_verdict(cp: Path) -> dict:
    """Ask reconcile.py (in the copy) for the full JSON verdict."""
    proc = subprocess.run(
        [PYTHON, str(cp / "onboarding" / "reconcile.py"), "--tenant", TENANT, "--json"],
        cwd=str(cp),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, f"reconcile failed: {proc.stderr}"
    return json.loads(proc.stdout)


def _registry(cp: Path) -> dict:
    return json.loads((cp / "ca" / "tenant_registry.json").read_text())


def _bundles(cp: Path) -> list[str]:
    broot = cp / "onboarding" / "bundles"
    if not broot.is_dir():
        return []
    return sorted(p.name for p in broot.iterdir() if p.is_dir())


def _env_deployment_id(cp: Path) -> str | None:
    envf = cp / ".env"
    if not envf.is_file():
        return None
    for line in envf.read_text().splitlines():
        if line.startswith("AGENT_DEPLOYMENT_ID="):
            return line.split("=", 1)[1].strip() or None
    return None


def _assert_single_consistent(cp: Path, expect_deployment: str | None = None) -> str:
    """Assert EXACTLY one active row, one complete bundle, runtime + .env agree.

    Returns the single deployment_id. If ``expect_deployment`` is given, asserts
    convergence to that exact deployment (idempotency, not a re-onboard).
    """
    reg = _registry(cp)
    rows = {fp: r for fp, r in reg.items() if r.get("tenant_id") == TENANT}
    active = {fp: r for fp, r in rows.items() if r.get("status") == "active"}
    assert len(active) == 1, f"expected exactly 1 active registry row, got {len(active)}: {rows}"
    assert len(rows) == 1, f"expected exactly 1 registry row total (no revoked orphans), got {rows}"

    bundles = _bundles(cp)
    assert len(bundles) == 1, f"expected exactly 1 bundle dir, got {bundles}"
    dep = bundles[0]

    # Bundle is COMPLETE (cert + license trio + agent-config + trust root + manifest).
    bdir = cp / "onboarding" / "bundles" / dep
    for rel in (
        "BUNDLE.json", "agent-config.json", "agent-config.json.sig", "trust_root.json",
        f"cert/{TENANT}.crt", f"cert/{TENANT}.key",
        f"{TENANT}.license.json", f"{TENANT}.license.json.sig",
        f"{TENANT}.license.json.manifest.json",
    ):
        assert (bdir / rel).is_file(), f"bundle missing {rel}"

    # The bundle's cert fingerprint == the single active registry row's key.
    manifest = json.loads((bdir / "BUNDLE.json").read_text())
    fp = manifest["cert_fingerprint_sha256"].lower()
    assert fp in active, f"bundle fingerprint {fp[:16]}… is not the active registry row {list(active)}"

    # Runtime fully staged (the four files the compose mounts into the agent).
    rt = cp / "_runtime"
    for rel in (
        "agent/license.json", "agent/license.json.sig", "agent/license.json.manifest.json",
        "agent/client.crt", "agent/client.key", "ca/ca.crt",
    ):
        assert (rt / rel).is_file(), f"_runtime missing {rel}"

    # .env binds compose to THIS deployment.
    assert _env_deployment_id(cp) == dep, ".env AGENT_DEPLOYMENT_ID != the single bundle deployment"

    # The reconciler itself agrees this is CONSISTENT (one console deployment).
    verdict = _reconcile_verdict(cp)
    assert verdict["action"] == "consistent", f"reconcile not consistent: {verdict['reasons']}"
    assert len(verdict["active_fingerprints"]) == 1
    assert len([b for b in verdict["bundles"] if b["complete"]]) == 1

    if expect_deployment is not None:
        assert dep == expect_deployment, (
            f"deployment changed across runs ({expect_deployment} -> {dep}); "
            "a CONSISTENT re-run must NOT re-onboard"
        )
    return dep


@pytest.fixture()
def cp(tmp_path: Path) -> Path:
    return _copy_tree(tmp_path / "control-plane")


# --------------------------------------------------------------------------- #
# Scenario 1 — run bootstrap TWICE: converges to one consistent tenant.       #
# --------------------------------------------------------------------------- #
def test_bootstrap_run_twice_is_idempotent_and_consistent(cp: Path):
    first = _run_bootstrap(cp)
    assert first.returncode == 0, f"first bootstrap failed:\n{first.stdout}\n{first.stderr}"
    dep1 = _assert_single_consistent(cp)

    second = _run_bootstrap(cp)
    assert second.returncode == 0, f"second bootstrap failed:\n{second.stdout}\n{second.stderr}"
    # The 2nd run must SKIP onboarding (true idempotency) and converge to the SAME
    # deployment — no duplicate rows / bundles / orphans.
    assert "already onboarded CONSISTENTLY" in (second.stdout + second.stderr), (
        "second run did not take the consistent (skip) path:\n" + second.stdout + second.stderr
    )
    _assert_single_consistent(cp, expect_deployment=dep1)


# --------------------------------------------------------------------------- #
# Scenario 2 — PARTIAL: registry reset to {} (the exact L-3 trap) is healed.   #
# --------------------------------------------------------------------------- #
def test_partial_registry_reset_is_reconciled(cp: Path):
    first = _run_bootstrap(cp)
    assert first.returncode == 0, f"bootstrap failed:\n{first.stdout}\n{first.stderr}"
    dep1 = _assert_single_consistent(cp)

    # Simulate the L-3 trap: the tracked registry is reset to {} (git stash /
    # checkout / pull / interrupted clean) while _runtime + the bundle survive.
    (cp / "ca" / "tenant_registry.json").write_text("{}\n")
    assert (cp / "_runtime" / "agent" / "license.json").is_file()  # _runtime survived
    assert _bundles(cp) == [dep1]                                   # bundle survived

    # The OLD guard would print "already onboarded" and exit 0, leaving an empty
    # registry (proxy 403s every push). The new reconcile must detect PARTIAL and
    # re-onboard so an ACTIVE row is restored.
    verdict = _reconcile_verdict(cp)
    assert verdict["action"] == "partial", f"expected partial, got {verdict}"

    second = _run_bootstrap(cp)
    assert second.returncode == 0, f"reconcile bootstrap failed:\n{second.stdout}\n{second.stderr}"
    assert "PARTIAL" in (second.stdout + second.stderr), (
        "second run did not take the reconcile path:\n" + second.stdout + second.stderr
    )
    # Converged: exactly one active row again, one complete bundle, runtime+.env
    # all agreeing — and critically the registry is no longer {} so the proxy will
    # accept the boundary cert.
    dep2 = _assert_single_consistent(cp)
    assert _registry(cp) != {}, "registry still empty after reconcile (proxy would 403 every push)"
    # The fingerprint now in the registry matches the (possibly fresh) bundle.
    manifest = json.loads((cp / "onboarding" / "bundles" / dep2 / "BUNDLE.json").read_text())
    assert manifest["cert_fingerprint_sha256"].lower() in _registry(cp)


# --------------------------------------------------------------------------- #
# Scenario 3 — PARTIAL: staged _runtime wiped (bundle + registry survive).     #
# --------------------------------------------------------------------------- #
def test_partial_runtime_wiped_is_reconciled(cp: Path):
    first = _run_bootstrap(cp)
    assert first.returncode == 0, f"bootstrap failed:\n{first.stdout}\n{first.stderr}"
    _assert_single_consistent(cp)

    # Wipe the staged runtime the compose mounts (e.g. someone cleared _runtime/)
    # while the registry row + bundle survive — an inconsistent fragment set.
    shutil.rmtree(cp / "_runtime" / "agent")
    verdict = _reconcile_verdict(cp)
    assert verdict["action"] == "partial", f"expected partial, got {verdict}"

    second = _run_bootstrap(cp)
    assert second.returncode == 0, f"reconcile bootstrap failed:\n{second.stdout}\n{second.stderr}"
    # Converged again: one of each, runtime restaged.
    _assert_single_consistent(cp)


# --------------------------------------------------------------------------- #
# Scenario 4 — PARTIAL: duplicate bundles are swept down to one.               #
# --------------------------------------------------------------------------- #
def test_partial_duplicate_bundle_is_swept(cp: Path):
    first = _run_bootstrap(cp)
    assert first.returncode == 0, f"bootstrap failed:\n{first.stdout}\n{first.stderr}"
    dep1 = _assert_single_consistent(cp)

    # Inject a SECOND (orphan) bundle dir for the tenant (e.g. a half-finished
    # onboard left a stray dir). reconcile must treat >1 complete bundle as
    # PARTIAL and sweep BOTH, then re-onboard exactly one.
    dup = cp / "onboarding" / "bundles" / f"{TENANT}-use1-dup9"
    shutil.copytree(cp / "onboarding" / "bundles" / dep1, dup)
    # Fix the duplicate's BUNDLE.json deployment_id so it is self-consistent.
    m = json.loads((dup / "BUNDLE.json").read_text())
    m["deployment_id"] = f"{TENANT}-use1-dup9"
    (dup / "BUNDLE.json").write_text(json.dumps(m, indent=2, sort_keys=True) + "\n")

    assert sorted(_bundles(cp)) == sorted([dep1, f"{TENANT}-use1-dup9"])
    verdict = _reconcile_verdict(cp)
    assert verdict["action"] == "partial", f"expected partial, got {verdict}"

    second = _run_bootstrap(cp)
    assert second.returncode == 0, f"reconcile bootstrap failed:\n{second.stdout}\n{second.stderr}"
    # Exactly one bundle remains and everything is consistent again.
    _assert_single_consistent(cp)
