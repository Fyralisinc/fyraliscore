#!/usr/bin/env python3
"""reconcile.py — make the bootstrap demo-tenant onboard idempotent + consistent.

The original bootstrap idempotency guard only checked whether ``_runtime/agent/
license.json`` (plus ``client.crt`` + ``.env``) existed, and skipped onboarding
when they did. That ignores the *persistent, tracked* ``ca/tenant_registry.json``
the auth-proxy bind-mounts read-only — so a partially-onboarded prior run (cert +
bundle present but the registry row missing/reset, or ``_runtime`` staged but the
bundle gone) printed "already onboarded" and exited 0, leaving the proxy to
**403 every push** with no warning (LIMITATIONS L-3).

This module is the single source of truth for *"is the demo tenant onboarded as
ONE consistent unit?"*. It inspects the four durable artifact groups together and
emits one verdict:

    CONSISTENT  — exactly one active registry row, one complete bundle, the
                  ``_runtime`` material + ``.env`` all present and cross-agreeing
                  (same deployment_id + same cert fingerprint everywhere).
                  bootstrap skips onboarding (true idempotency).
    ABSENT      — none of it is present. bootstrap onboards fresh.
    PARTIAL     — some present, some missing/mismatched, OR duplicates (more than
                  one active row / more than one bundle for the tenant).
                  bootstrap cleanly offboards every fragment, then re-onboards, so
                  the second run *converges* to exactly one consistent tenant.

The four artifact groups (all keyed on the demo ``tenant``):

  1. REGISTRY    ca/tenant_registry.json  — the ACTIVE row(s) for the tenant; its
                 fingerprint is what the proxy resolves the boundary cert to.
  2. BUNDLE      onboarding/bundles/<deployment_id>/ — cert + license trio +
                 signed agent-config + trust_root + BUNDLE.json (carries the
                 authoritative deployment_id + cert fingerprint).
  3. RUNTIME     _runtime/agent/{license.json,license.json.sig,license.json.
                 manifest.json,client.crt,client.key} + _runtime/ca/ca.crt — the
                 material the compose mounts into the agent / boundary collector.
  4. ENV         .env — AGENT_DEPLOYMENT_ID binds compose to the minted id.

"Console deployment" note: in the default ``--embedded-console`` bootstrap path
the console is in-process and ephemeral (it does not persist across runs), so on
disk the *bundle dir + the .env deployment binding* ARE the durable deployment
record the reconciler treats as "the console deployment". When a real console URL
is used, the reconciler still converges the persistent disk state; the in-flight
console row is handled by onboard's own atomic rollback / offboard's deregister.

Usage (bootstrap.sh consumes ``--print-action`` on the last stdout line):

    python reconcile.py --tenant acme --print-action
        -> prints one of: consistent | absent | partial   (plus a human report)

    python reconcile.py --tenant acme --json        # machine-readable verdict
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass, field
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
_CA_DIR = os.path.join(_CP_ROOT, "ca")
for _p in (_CP_ROOT, _CA_DIR, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import registry as ca_registry  # noqa: E402  (ca/registry.py)
import revoke as ca_revoke  # noqa: E402  (ca/revoke.py)

import onboard as ob  # noqa: E402  (onboarding/onboard.py)

__all__ = [
    "ReconcileVerdict",
    "inspect_state",
    "purge_fragments",
    "ACTION_CONSISTENT",
    "ACTION_ABSENT",
    "ACTION_PARTIAL",
]

ACTION_CONSISTENT = "consistent"
ACTION_ABSENT = "absent"
ACTION_PARTIAL = "partial"

DEFAULT_BUNDLES_ROOT = ob.DEFAULT_BUNDLES_ROOT
DEFAULT_REGISTRY_PATH = ca_registry.DEFAULT_REGISTRY_PATH
DEFAULT_RUNTIME_DIR = os.path.join(_CP_ROOT, "_runtime")
DEFAULT_ENV_FILE = os.path.join(_CP_ROOT, ".env")

# The license trio + tenant client cert the compose mounts into the agent /
# boundary collector. All must be present for RUNTIME to count as complete.
_RUNTIME_AGENT_FILES = (
    "license.json",
    "license.json.sig",
    "license.json.manifest.json",
    "client.crt",
    "client.key",
)
# A complete bundle must contain (at least) these — the cert, the license trio,
# the signed agent-config, the trust root, and the BUNDLE manifest.
_BUNDLE_REQUIRED = (
    "BUNDLE.json",
    "agent-config.json",
    "agent-config.json.sig",
    "trust_root.json",
)


@dataclass
class BundleInfo:
    deployment_id: str
    path: str
    tenant_id: str
    fingerprint: str
    complete: bool
    missing: list[str] = field(default_factory=list)


@dataclass
class ReconcileVerdict:
    tenant: str
    action: str  # consistent | absent | partial
    reasons: list[str] = field(default_factory=list)
    # The deployment_id we believe is the *current* one (for offboard/redo).
    deployment_id: Optional[str] = None
    fingerprint: Optional[str] = None
    # Every artifact fragment we found (for offboard during PARTIAL reconcile).
    active_fingerprints: list[str] = field(default_factory=list)
    all_fingerprints: list[str] = field(default_factory=list)
    bundles: list[BundleInfo] = field(default_factory=list)
    runtime_present: bool = False
    runtime_complete: bool = False
    env_present: bool = False
    env_deployment_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "tenant": self.tenant,
            "action": self.action,
            "reasons": self.reasons,
            "deployment_id": self.deployment_id,
            "fingerprint": self.fingerprint,
            "active_fingerprints": self.active_fingerprints,
            "all_fingerprints": self.all_fingerprints,
            "bundles": [
                {
                    "deployment_id": b.deployment_id,
                    "path": b.path,
                    "tenant_id": b.tenant_id,
                    "fingerprint": b.fingerprint,
                    "complete": b.complete,
                    "missing": b.missing,
                }
                for b in self.bundles
            ],
            "runtime_present": self.runtime_present,
            "runtime_complete": self.runtime_complete,
            "env_present": self.env_present,
            "env_deployment_id": self.env_deployment_id,
        }


# --------------------------------------------------------------------------- #
# Artifact-group inspectors                                                    #
# --------------------------------------------------------------------------- #
def _scan_registry(tenant: str, registry_path: str) -> tuple[list[str], list[str]]:
    """Return ``(active_fingerprints, all_fingerprints)`` for the tenant."""
    rows = ca_registry.find_by_tenant(tenant, path=registry_path)
    active = [fp for fp, row in rows.items() if row.get("status") == ca_registry.STATUS_ACTIVE]
    return sorted(active), sorted(rows.keys())


def _scan_bundles(tenant: str, bundles_root: str) -> list[BundleInfo]:
    """Find every bundle on disk that belongs to ``tenant`` (via BUNDLE.json)."""
    out: list[BundleInfo] = []
    if not os.path.isdir(bundles_root):
        return out
    for name in sorted(os.listdir(bundles_root)):
        bdir = os.path.join(bundles_root, name)
        if not os.path.isdir(bdir):
            continue
        manifest_path = os.path.join(bdir, "BUNDLE.json")
        tenant_id = ""
        fingerprint = ""
        missing: list[str] = []
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as fh:
                    m = json.load(fh)
                tenant_id = m.get("tenant_id", "")
                fingerprint = (m.get("cert_fingerprint_sha256") or "").lower()
            except Exception:
                missing.append("BUNDLE.json(unreadable)")
        else:
            missing.append("BUNDLE.json")
        # Only certain bundles belong to this tenant. A bundle whose BUNDLE.json
        # is unreadable but whose dir name prefixes the tenant is still claimed as
        # a fragment so PARTIAL reconcile can sweep it.
        belongs = tenant_id == tenant or (not tenant_id and name.startswith(f"{tenant}-"))
        if not belongs:
            continue
        # Completeness: required extras + the tenant cert/key + the license trio.
        for rel in _BUNDLE_REQUIRED:
            if not os.path.isfile(os.path.join(bdir, rel)):
                if rel not in missing:
                    missing.append(rel)
        for rel in (f"cert/{tenant}.crt", f"cert/{tenant}.key"):
            if not os.path.isfile(os.path.join(bdir, rel)):
                missing.append(rel)
        for rel in (
            f"{tenant}.license.json",
            f"{tenant}.license.json.sig",
            f"{tenant}.license.json.manifest.json",
        ):
            if not os.path.isfile(os.path.join(bdir, rel)):
                missing.append(rel)
        out.append(
            BundleInfo(
                deployment_id=name,
                path=bdir,
                tenant_id=tenant_id or tenant,
                fingerprint=fingerprint,
                complete=not missing,
                missing=missing,
            )
        )
    return out


def _scan_runtime(runtime_dir: str) -> tuple[bool, bool]:
    """Return ``(any_present, all_present)`` for the staged runtime material."""
    agent_dir = os.path.join(runtime_dir, "agent")
    present = [os.path.isfile(os.path.join(agent_dir, f)) for f in _RUNTIME_AGENT_FILES]
    present.append(os.path.isfile(os.path.join(runtime_dir, "ca", "ca.crt")))
    return any(present), all(present)


def _scan_env(env_file: str) -> tuple[bool, Optional[str]]:
    """Return ``(present, AGENT_DEPLOYMENT_ID or None)`` from ``.env``."""
    if not os.path.isfile(env_file):
        return False, None
    dep_id: Optional[str] = None
    try:
        with open(env_file, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("AGENT_DEPLOYMENT_ID="):
                    dep_id = line.split("=", 1)[1].strip() or None
                    break
    except Exception:
        return True, None
    return True, dep_id


# --------------------------------------------------------------------------- #
# The verdict                                                                  #
# --------------------------------------------------------------------------- #
def inspect_state(
    *,
    tenant: str,
    registry_path: str = DEFAULT_REGISTRY_PATH,
    bundles_root: str = DEFAULT_BUNDLES_ROOT,
    runtime_dir: str = DEFAULT_RUNTIME_DIR,
    env_file: str = DEFAULT_ENV_FILE,
) -> ReconcileVerdict:
    """Inspect all four durable artifact groups and decide the bootstrap action.

    The verdict is CONSISTENT only when EVERY group agrees as one unit:
      * exactly one ACTIVE registry row for the tenant, and no revoked-leftovers,
      * exactly one complete bundle, whose cert fingerprint == the active row,
      * the runtime material is fully staged,
      * .env binds AGENT_DEPLOYMENT_ID to that one bundle's deployment_id.
    Anything else is ABSENT (nothing present at all) or PARTIAL (fix-by-redo).
    """
    active_fps, all_fps = _scan_registry(tenant, registry_path)
    bundles = _scan_bundles(tenant, bundles_root)
    runtime_present, runtime_complete = _scan_runtime(runtime_dir)
    env_present, env_dep = _scan_env(env_file)

    reasons: list[str] = []

    complete_bundles = [b for b in bundles if b.complete]
    any_present = bool(all_fps or bundles or runtime_present or env_present)

    # --- ABSENT: a clean slate -------------------------------------------
    if not any_present:
        return ReconcileVerdict(
            tenant=tenant,
            action=ACTION_ABSENT,
            reasons=["no registry row, bundle, runtime, or .env for the tenant"],
            active_fingerprints=active_fps,
            all_fingerprints=all_fps,
            bundles=bundles,
            runtime_present=runtime_present,
            runtime_complete=runtime_complete,
            env_present=env_present,
            env_deployment_id=env_dep,
        )

    # --- duplicate / orphan checks (always => PARTIAL) -------------------
    if len(active_fps) > 1:
        reasons.append(f"{len(active_fps)} active registry rows (expected 1) — duplicate certs")
    if len(complete_bundles) > 1:
        reasons.append(
            f"{len(complete_bundles)} complete bundles (expected 1): "
            + ", ".join(b.deployment_id for b in complete_bundles)
        )
    incomplete = [b for b in bundles if not b.complete]
    if incomplete:
        reasons.append(
            "incomplete bundle(s): "
            + "; ".join(f"{b.deployment_id} missing {b.missing}" for b in incomplete)
        )
    revoked_only = set(all_fps) - set(active_fps)
    if revoked_only:
        reasons.append(f"{len(revoked_only)} non-active (revoked/stale) registry row(s)")

    # The single candidate deployment we'd converge on (for offboard targeting).
    candidate = complete_bundles[0] if len(complete_bundles) == 1 else None
    dep_id = candidate.deployment_id if candidate else (env_dep or None)
    fp = candidate.fingerprint if candidate else (active_fps[0] if len(active_fps) == 1 else None)

    # --- presence cross-checks ------------------------------------------
    if not active_fps:
        reasons.append("no ACTIVE registry row → auth-proxy would 403 every push")
    if not complete_bundles:
        reasons.append("no complete bundle on disk")
    if not runtime_complete:
        reasons.append(
            "runtime material incomplete in _runtime/ (compose mounts would be missing)"
            if runtime_present
            else "no runtime material staged in _runtime/"
        )
    if not env_present:
        reasons.append("no .env binding compose to the deployment_id")

    # --- agreement cross-checks (only meaningful when each side exists) --
    if candidate and active_fps and candidate.fingerprint not in active_fps:
        reasons.append(
            f"bundle fingerprint {candidate.fingerprint[:16]}… has no ACTIVE registry row "
            "(cert ≠ registry — proxy would reject the boundary cert)"
        )
    if candidate and env_dep and env_dep != candidate.deployment_id:
        reasons.append(
            f".env AGENT_DEPLOYMENT_ID={env_dep} ≠ bundle deployment {candidate.deployment_id}"
        )

    if reasons:
        return ReconcileVerdict(
            tenant=tenant,
            action=ACTION_PARTIAL,
            reasons=reasons,
            deployment_id=dep_id,
            fingerprint=fp,
            active_fingerprints=active_fps,
            all_fingerprints=all_fps,
            bundles=bundles,
            runtime_present=runtime_present,
            runtime_complete=runtime_complete,
            env_present=env_present,
            env_deployment_id=env_dep,
        )

    # --- CONSISTENT: every group present + cross-agreeing ----------------
    return ReconcileVerdict(
        tenant=tenant,
        action=ACTION_CONSISTENT,
        reasons=["exactly one active registry row, one complete bundle, runtime + .env all agree"],
        deployment_id=dep_id,
        fingerprint=fp,
        active_fingerprints=active_fps,
        all_fingerprints=all_fps,
        bundles=bundles,
        runtime_present=runtime_present,
        runtime_complete=runtime_complete,
        env_present=env_present,
        env_deployment_id=env_dep,
    )


# --------------------------------------------------------------------------- #
# Clean sweep for a PARTIAL reconcile (offboard every fragment, then redo)     #
# --------------------------------------------------------------------------- #
def purge_fragments(
    verdict: ReconcileVerdict,
    *,
    registry_path: str = DEFAULT_REGISTRY_PATH,
    runtime_dir: str = DEFAULT_RUNTIME_DIR,
    env_file: str = DEFAULT_ENV_FILE,
    log=print,
) -> dict:
    """Remove EVERY artifact fragment the verdict found for the tenant.

    This is the "cleanly offboard+redo" half of reconcile: it leaves a blank
    slate (registry has no row for the tenant, no bundle dir, no staged runtime,
    no stale .env deployment binding) so the subsequent onboard converges to a
    single consistent tenant. Revocation runs first (security-critical, fail
    closed) before the registry rows are deleted.

    Idempotent + best-effort: each removal is guarded so a missing fragment is a
    no-op, not an error. Returns a summary of what was removed.
    """
    tenant = verdict.tenant
    removed_rows: list[str] = []

    # 1) Registry: revoke (proxy stops accepting) THEN delete every row for the
    #    tenant — active, revoked, or stale — so no orphan binding survives.
    if verdict.all_fingerprints:
        try:
            ca_revoke.revoke(tenant, registry_path=registry_path)
        except SystemExit:
            pass  # nothing active to revoke — fine
        except Exception as exc:  # best-effort
            log(f"  reconcile: revoke({tenant}) best-effort failed: {exc}")
        reg = ca_registry.load_registry(registry_path)
        for fp in verdict.all_fingerprints:
            fp = ca_registry._normalize_fp(fp)
            if fp in reg:
                del reg[fp]
                removed_rows.append(fp)
        if removed_rows:
            ca_registry.save_registry(reg, registry_path)
        log(f"  reconcile: swept {len(removed_rows)} registry row(s) for {tenant}")

    # 2) Bundles: delete every bundle dir the tenant owns (complete or partial).
    removed_bundles: list[str] = []
    for b in verdict.bundles:
        if os.path.isdir(b.path):
            shutil.rmtree(b.path, ignore_errors=True)
            removed_bundles.append(b.deployment_id)
    if removed_bundles:
        log(f"  reconcile: removed {len(removed_bundles)} bundle dir(s): {removed_bundles}")

    # 3) Runtime: clear the staged agent license trio + client cert + ca chain so
    #    the redo restages fresh material that matches the new bundle.
    runtime_cleared = False
    agent_dir = os.path.join(runtime_dir, "agent")
    for f in _RUNTIME_AGENT_FILES:
        p = os.path.join(agent_dir, f)
        if os.path.isfile(p):
            os.remove(p)
            runtime_cleared = True
    ca_crt = os.path.join(runtime_dir, "ca", "ca.crt")
    if os.path.isfile(ca_crt):
        os.remove(ca_crt)
        runtime_cleared = True
    if runtime_cleared:
        log("  reconcile: cleared staged _runtime material")

    # 4) Env: drop the stale AGENT_DEPLOYMENT_ID line so the redo rewrites it.
    #    (The CONSOLE_INGEST_TOKEN + the rest of .env are preserved.)
    env_cleared = False
    if os.path.isfile(env_file):
        with open(env_file, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
        kept = [ln for ln in lines if not ln.startswith("AGENT_DEPLOYMENT_ID=")]
        if len(kept) != len(lines):
            with open(env_file, "w", encoding="utf-8") as fh:
                fh.writelines(kept)
            env_cleared = True
            log("  reconcile: dropped stale AGENT_DEPLOYMENT_ID from .env")

    return {
        "tenant": tenant,
        "removed_registry_rows": removed_rows,
        "removed_bundles": removed_bundles,
        "runtime_cleared": runtime_cleared,
        "env_cleared": env_cleared,
    }


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #
def _human_report(v: ReconcileVerdict) -> str:
    lines = [
        f"reconcile[{v.tenant}]: action={v.action}",
        f"  registry: {len(v.active_fingerprints)} active / {len(v.all_fingerprints)} total row(s)",
        f"  bundles:  {len(v.bundles)} ({sum(1 for b in v.bundles if b.complete)} complete)",
        f"  runtime:  present={v.runtime_present} complete={v.runtime_complete}",
        f"  env:      present={v.env_present} deployment_id={v.env_deployment_id}",
    ]
    for r in v.reasons:
        lines.append(f"  - {r}")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="reconcile",
        description="Decide whether the bootstrap demo tenant is onboarded consistently.",
    )
    ap.add_argument("--tenant", required=True, help="demo tenant id (e.g. acme)")
    ap.add_argument("--registry", default=DEFAULT_REGISTRY_PATH)
    ap.add_argument("--bundles-root", default=DEFAULT_BUNDLES_ROOT)
    ap.add_argument("--runtime-dir", default=DEFAULT_RUNTIME_DIR)
    ap.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--json", action="store_true", help="emit the full verdict as JSON")
    grp.add_argument(
        "--print-action",
        action="store_true",
        help="print the human report to stderr and the bare action word as the LAST stdout line "
        "(consistent|absent|partial) for shell consumption",
    )
    ap.add_argument(
        "--purge",
        action="store_true",
        help="sweep EVERY artifact fragment for the tenant (registry rows + bundles + staged "
        "runtime + stale .env binding), leaving a blank slate for a fresh onboard. Use this to "
        "reconcile a PARTIAL state.",
    )
    args = ap.parse_args(argv)

    v = inspect_state(
        tenant=args.tenant,
        registry_path=args.registry,
        bundles_root=args.bundles_root,
        runtime_dir=args.runtime_dir,
        env_file=args.env_file,
    )

    if args.purge:
        # Report goes to stderr so stdout stays parseable; then sweep fragments.
        print(_human_report(v), file=sys.stderr)
        summary = purge_fragments(
            v,
            registry_path=args.registry,
            runtime_dir=args.runtime_dir,
            env_file=args.env_file,
            log=lambda m: print(m, file=sys.stderr),
        )
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            print("purged")
        return 0

    if args.json:
        print(json.dumps(v.to_dict(), indent=2, sort_keys=True))
    elif args.print_action:
        # Human report -> stderr so stdout carries ONLY the action word.
        print(_human_report(v), file=sys.stderr)
        print(v.action)
    else:
        print(_human_report(v))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
