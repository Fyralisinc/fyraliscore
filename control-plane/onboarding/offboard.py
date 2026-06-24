#!/usr/bin/env python3
"""offboard.py — revoke + deregister an onboarded tenant (inverse of onboard).

    offboard --tenant acme [--deployment acme-use1-7f3a] [--purge-bundle]

Offboarding tears down a *committed* onboarding (whereas onboard's rollback tears
down an *in-flight* one). It:

 1. REVOKE  — flip every active cert for the tenant to ``revoked`` in
              ``ca/tenant_registry.json`` (the auth proxy now 403s the cert — the
              proxy binding is severed). With ``--purge-registry`` the rows are
              also deleted so no trace remains.
 2. DEREGISTER — best-effort remove the deployment from the console (embedded
              fake console only; the P4 contract has no DELETE verb, so against a
              real console the record is left to age to red/expired).
 3. PURGE   — with ``--purge-bundle``, delete the agent bundle dir on this host.

By default offboarding *revokes* (reversible-looking but security-effective) and
leaves an audit trail (the ``revoked`` row). ``--purge-registry`` makes it leave
no registry trace, matching what onboarding's rollback does.

Revocation is the security-critical step and is done first: even if a later step
fails, the cert is already rejected by the proxy (fail-closed).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
_CA_DIR = os.path.join(_CP_ROOT, "ca")
for _p in (_CP_ROOT, _CA_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import registry as ca_registry  # noqa: E402  (ca/registry.py)
import revoke as ca_revoke  # noqa: E402  (ca/revoke.py)

from lib.errors import ControlPlaneError  # noqa: E402

import console_client as cc  # noqa: E402

__all__ = ["OffboardError", "offboard"]

DEFAULT_BUNDLES_ROOT = os.path.join(_HERE, "bundles")
DEFAULT_REGISTRY_PATH = ca_registry.DEFAULT_REGISTRY_PATH


class OffboardError(ControlPlaneError):
    """Offboarding could not complete."""


def _log(msg: str) -> None:
    print(msg, flush=True)


def _delete_registry_rows(fingerprints: list[str], *, registry_path: str) -> int:
    reg = ca_registry.load_registry(registry_path)
    n = 0
    for fp in fingerprints:
        fp = ca_registry._normalize_fp(fp)
        if fp in reg:
            del reg[fp]
            n += 1
    if n:
        ca_registry.save_registry(reg, registry_path)
    return n


def offboard(
    *,
    tenant: str,
    deployment_id: Optional[str] = None,
    console_url: Optional[str] = None,
    console_app: object = None,
    registry_path: str = DEFAULT_REGISTRY_PATH,
    bundles_root: str = DEFAULT_BUNDLES_ROOT,
    purge_registry: bool = False,
    purge_bundle: bool = False,
) -> dict:
    """Revoke the tenant's cert(s), deregister, and (optionally) purge the bundle.

    Returns a summary dict. Raises :class:`OffboardError` only if the tenant has
    no certs to revoke *and* nothing else to do (so the operator notices a typo).
    """
    # --- 1) REVOKE every active cert for the tenant -----------------------
    rows = ca_registry.find_by_tenant(tenant, path=registry_path)
    fingerprints = list(rows.keys())
    revoked: list[str] = []
    if fingerprints:
        result = ca_revoke.revoke(tenant, registry_path=registry_path)
        revoked = result["revoked"]
        _log(f"[1/3] revoked {len(revoked)} cert(s) for {tenant} "
             f"(already-revoked: {len(result['already_revoked'])})")
    else:
        _log(f"[1/3] no certs registered for tenant {tenant!r} (nothing to revoke)")

    purged_rows = 0
    if purge_registry and fingerprints:
        purged_rows = _delete_registry_rows(fingerprints, registry_path=registry_path)
        _log(f"      purged {purged_rows} registry row(s) (no trace left)")

    # --- 2) DEREGISTER from the console (best-effort) ---------------------
    deregistered = False
    if (console_url or console_app) and deployment_id:
        try:
            client = cc.make_console_client(console_url=console_url, app=console_app)
        except cc.ConsoleError as exc:
            _log(f"[2/3] console unavailable, skipping deregister: {exc}")
            client = None
        if client is not None:
            try:
                app = getattr(client, "app", None)
                if app is not None and hasattr(app, "state") and hasattr(app.state, "store"):
                    deregistered = app.state.store.remove(deployment_id)
                    _log(f"[2/3] deregistered {deployment_id} from console" if deregistered
                         else f"[2/3] {deployment_id} not present in console (already gone)")
                else:
                    _log("[2/3] real console has no DELETE verb; record will age to red/expired")
            finally:
                client.close()
    else:
        _log("[2/3] no console/deployment_id given; skipping deregister")

    # --- 3) PURGE the local bundle ----------------------------------------
    bundle_removed = False
    if purge_bundle and deployment_id:
        bundle_dir = os.path.join(bundles_root, deployment_id)
        if os.path.isdir(bundle_dir):
            shutil.rmtree(bundle_dir, ignore_errors=True)
            bundle_removed = True
            _log(f"[3/3] removed bundle {bundle_dir}")
        else:
            _log(f"[3/3] no bundle at {bundle_dir}")
    else:
        _log("[3/3] bundle left in place (pass --purge-bundle + --deployment to remove)")

    if not fingerprints and not deregistered and not bundle_removed:
        raise OffboardError(
            f"nothing to offboard for tenant {tenant!r} "
            "(no certs, no console deployment, no bundle)"
        )

    summary = {
        "tenant_id": tenant,
        "deployment_id": deployment_id,
        "revoked_fingerprints": revoked,
        "purged_registry_rows": purged_rows,
        "deregistered_from_console": deregistered,
        "bundle_removed": bundle_removed,
    }
    _log(f"OFFBOARDED {tenant}.")
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="offboard",
        description="Revoke + deregister an onboarded BYOC tenant.",
    )
    ap.add_argument("--tenant", required=True, help="tenant id to offboard")
    ap.add_argument("--deployment", default=None, help="deployment id (for console + bundle)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--console-url", default=os.environ.get("CONSOLE_URL"))
    g.add_argument("--embedded-console", action="store_true",
                   help="use an in-process fake console (dev/demo)")
    ap.add_argument("--registry", default=DEFAULT_REGISTRY_PATH)
    ap.add_argument("--bundles-root", default=DEFAULT_BUNDLES_ROOT)
    ap.add_argument("--purge-registry", action="store_true",
                    help="also delete the revoked rows (leave no registry trace)")
    ap.add_argument("--purge-bundle", action="store_true",
                    help="delete the local agent bundle dir")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    console_app = None
    if args.embedded_console:
        import fake_console
        console_app = fake_console.build_app()

    try:
        summary = offboard(
            tenant=args.tenant,
            deployment_id=args.deployment,
            console_url=args.console_url,
            console_app=console_app,
            registry_path=args.registry,
            bundles_root=args.bundles_root,
            purge_registry=args.purge_registry,
            purge_bundle=args.purge_bundle,
        )
    except OffboardError as exc:
        print(f"offboard failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
