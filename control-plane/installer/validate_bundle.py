#!/usr/bin/env python3
"""validate_bundle — CLI over bundle_lib.validate_bundle (install-time gate).

    python validate_bundle.py <bundle-dir> [--no-verify-sigs] [--json]

Exit 0 if the bundle is valid; non-zero otherwise. ``install.sh`` calls this as
its first step (and is the whole of ``install.sh --dry-run``). It also emits, on
success, a shell-sourceable env block (the ${...} vars the deployment overlay is
rendered with) when ``--print-env`` is passed, so install.sh can render the .env
without re-parsing bundle.json itself.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import bundle_lib  # noqa: E402


def _shell_quote(v: str) -> str:
    return "'" + str(v).replace("'", "'\\''") + "'"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate a Fyralis agent bundle.")
    ap.add_argument("bundle_dir", help="path to the agent bundle directory")
    ap.add_argument("--no-verify-sigs", action="store_true", help="skip I6 signature verification")
    ap.add_argument("--json", action="store_true", help="machine-readable result")
    ap.add_argument(
        "--print-env",
        action="store_true",
        help="on success, also print the sourceable deployment env to stdout",
    )
    ap.add_argument(
        "--control-plane-dir",
        default=os.path.dirname(_HERE),
        help="control-plane root (for boundary config + lib mount paths)",
    )
    args = ap.parse_args(argv)

    res = bundle_lib.validate_bundle(
        args.bundle_dir, verify_signatures=not args.no_verify_sigs
    )

    if args.json:
        print(json.dumps(
            {
                "ok": res.ok,
                "bundle_dir": res.bundle_dir,
                "manifest": res.manifest,
                "checks": res.checks,
                "warnings": res.warnings,
                "errors": res.errors,
            },
            indent=2,
        ))
    else:
        print(f"Validating agent bundle: {res.bundle_dir}")
        for c in res.checks:
            print(f"  [PASS] {c}")
        for w in res.warnings:
            print(f"  [WARN] {w}")
        for e in res.errors:
            print(f"  [FAIL] {e}", file=sys.stderr)
        print("RESULT:", "VALID" if res.ok else "INVALID")

    if res.ok and args.print_env:
        env = bundle_lib.manifest_to_env(
            res.manifest, args.bundle_dir, control_plane_dir=args.control_plane_dir
        )
        print("# --- deployment env (source me) ---")
        for k, v in env.items():
            print(f"export {k}={_shell_quote(v)}")

    return 0 if res.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
