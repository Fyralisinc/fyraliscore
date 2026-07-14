#!/usr/bin/env python3
"""revoke.py — revoke a tenant cert by fingerprint or tenant id.

    python revoke.py revoke <fingerprint|tenant_id> [--registry PATH]
    python revoke.py status <fingerprint|tenant_id>     # inspect without changing
    python revoke.py list [--revoked-only]              # dump the registry

Revocation here is a **registry status flip** to ``"revoked"`` — the auth proxy
consults ``tenant_registry.json`` on every request and rejects (403) any cert
whose row is missing or revoked (contract C1). We do **not** implement CRL/OCSP
(see README caveats); the registry lookup is the revocation mechanism.

The positional argument is resolved as:

* a **64-hex-char fingerprint** → revoke exactly that cert;
* otherwise a **tenant_id** → revoke *every* active cert ever issued to that
  tenant (rotation may have produced several).

``is_revoked(fingerprint)`` is re-exported from :mod:`registry` for the proxy.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import registry  # noqa: E402

# Re-export so callers can do ``from revoke import is_revoked`` per the brief.
from registry import is_revoked  # noqa: E402,F401

_FP_RE = re.compile(r"^[0-9a-f]{64}$")


def _looks_like_fingerprint(value: str) -> bool:
    return bool(_FP_RE.match(value.strip().lower().replace(":", "")))


def revoke(identifier: str, *, registry_path: str = registry.DEFAULT_REGISTRY_PATH) -> dict:
    """Revoke by fingerprint (exact) or tenant_id (all that tenant's certs).

    Returns a summary dict listing the fingerprints that were flipped.
    """
    flipped = []
    already = []

    if _looks_like_fingerprint(identifier):
        targets = {identifier: registry.get_entry(identifier, path=registry_path)}
        if targets[identifier] is None:
            raise SystemExit("no registry entry for fingerprint %s" % identifier)
    else:
        targets = registry.find_by_tenant(identifier, path=registry_path)
        if not targets:
            raise SystemExit("no certs registered for tenant_id %r" % identifier)

    for fp, row in targets.items():
        if row and row.get("status") == registry.STATUS_REVOKED:
            already.append(fp)
            continue
        registry.set_status(fp, registry.STATUS_REVOKED, path=registry_path)
        flipped.append(fp)

    return {
        "identifier": identifier,
        "revoked": flipped,
        "already_revoked": already,
        "registry_path": registry_path,
    }


def status(identifier: str, *, registry_path: str = registry.DEFAULT_REGISTRY_PATH) -> dict:
    if _looks_like_fingerprint(identifier):
        row = registry.get_entry(identifier, path=registry_path)
        return {identifier: row} if row else {}
    return registry.find_by_tenant(identifier, path=registry_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Revoke / inspect tenant certs.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_rev = sub.add_parser("revoke", help="revoke by <fingerprint|tenant_id>")
    p_rev.add_argument("identifier")
    p_rev.add_argument("--registry", default=registry.DEFAULT_REGISTRY_PATH)

    p_stat = sub.add_parser("status", help="show status of <fingerprint|tenant_id>")
    p_stat.add_argument("identifier")
    p_stat.add_argument("--registry", default=registry.DEFAULT_REGISTRY_PATH)

    p_list = sub.add_parser("list", help="dump the registry")
    p_list.add_argument("--registry", default=registry.DEFAULT_REGISTRY_PATH)
    p_list.add_argument("--revoked-only", action="store_true")

    args = parser.parse_args(argv)

    if args.command == "revoke":
        result = revoke(args.identifier, registry_path=args.registry)
        if result["revoked"]:
            print("Revoked %d cert(s):" % len(result["revoked"]))
            for fp in result["revoked"]:
                print("  %s" % fp)
        if result["already_revoked"]:
            print("Already revoked (%d): %s" % (
                len(result["already_revoked"]), ", ".join(result["already_revoked"])))
        return 0

    if args.command == "status":
        rows = status(args.identifier, registry_path=args.registry)
        if not rows:
            print("no registry entry for %r" % args.identifier)
            return 1
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0

    if args.command == "list":
        reg = registry.load_registry(args.registry)
        if args.revoked_only:
            reg = {fp: r for fp, r in reg.items() if r.get("status") == registry.STATUS_REVOKED}
        print(json.dumps(reg, indent=2, sort_keys=True))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
