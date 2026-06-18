#!/usr/bin/env python
"""manage_extension.py — operator CLI for the extension install/enable lifecycle.

The human/ops front door to ``services.platform.extensions.lifecycle``: install
(grant + enable) an extension for a tenant, revoke it, or list install state.
This is the "a tenant needs to use this extension" step in the ADR-0004
lifecycle, treating github-intel exactly like any externally-developed,
operator-installed interface.

Examples (DATABASE_URL must point at the target DB)::

    # what's installable + per-tenant state
    python scripts/manage_extension.py list --tenant <uuid>

    # install github-intel for a tenant (grant declared caps + enable the flag),
    # also turning on its optional code-graph sub-feature
    python scripts/manage_extension.py install --tenant <uuid> \
        --extension github_intel --granted-by ops@fyralis \
        --flag code_intel.enabled=true --flag github_intel.llm_enabled=false

    python scripts/manage_extension.py uninstall --tenant <uuid> --extension github_intel
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from uuid import UUID

import asyncpg

from lib.extensions.registry import active_manifests, load_manifests
from services.platform.extensions import lifecycle


def _parse_flags(pairs: list[str] | None) -> dict[str, bool]:
    out: dict[str, bool] = {}
    for raw in pairs or []:
        if "=" not in raw:
            raise SystemExit(f"--flag must be name=true|false, got {raw!r}")
        name, _, value = raw.partition("=")
        out[name.strip()] = value.strip().lower() in {"1", "true", "yes", "on", "y"}
    return out


def _manifest_by_id(ext_id: str):
    for man in active_manifests():
        if man.id == ext_id:
            return man
    return None


async def _with_pool():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required")
    return await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)


async def _cmd_list(args) -> None:
    pool = await _with_pool()
    try:
        compatible, rejected = load_manifests()
        if args.tenant:
            tid = UUID(args.tenant)
            rows = await lifecycle.list_installed(pool, tenant_id=tid, manifests=compatible)
            print(json.dumps(
                {"tenant": str(tid), "installed": [r.__dict__ for r in rows],
                 "rejected": [{"id": rj.manifest.id, "reason": rj.reason} for rj in rejected]},
                default=str, indent=2,
            ))
        else:
            print(json.dumps(
                {"available": [{"id": m.id, "version": m.version,
                                "trust_tier": m.trust_tier,
                                "feature_flag": m.feature_flag} for m in compatible],
                 "rejected": [{"id": rj.manifest.id, "reason": rj.reason} for rj in rejected]},
                indent=2,
            ))
    finally:
        await pool.close()


async def _cmd_install(args) -> None:
    man = _manifest_by_id(args.extension)
    if man is None:
        raise SystemExit(
            f"extension {args.extension!r} not found among host-API-compatible "
            f"manifests; is its package installed?"
        )
    pool = await _with_pool()
    try:
        result = await lifecycle.install(
            pool,
            tenant_id=UUID(args.tenant),
            manifest=man,
            granted_by=args.granted_by,
            enable=not args.no_enable,
            extra_flags=_parse_flags(args.flag),
        )
        print(json.dumps(result.__dict__, default=str, indent=2))
    finally:
        await pool.close()


async def _cmd_uninstall(args) -> None:
    man = _manifest_by_id(args.extension)
    if man is None:
        raise SystemExit(f"extension {args.extension!r} not found")
    pool = await _with_pool()
    try:
        await lifecycle.uninstall(
            pool, tenant_id=UUID(args.tenant), manifest=man, set_by=args.granted_by
        )
        print(json.dumps({"uninstalled": args.extension, "tenant": args.tenant}))
    finally:
        await pool.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Fyralis extension install/enable lifecycle")
    sub = p.add_subparsers(dest="cmd", required=True)

    pl = sub.add_parser("list", help="list installable extensions (+ per-tenant state)")
    pl.add_argument("--tenant", help="tenant UUID (omit for catalog only)")
    pl.set_defaults(fn=_cmd_list)

    pi = sub.add_parser("install", help="grant + enable an extension for a tenant")
    pi.add_argument("--tenant", required=True)
    pi.add_argument("--extension", required=True)
    pi.add_argument("--granted-by", default="operator")
    pi.add_argument("--no-enable", action="store_true", help="grant only, don't flip the flag")
    pi.add_argument("--flag", action="append", help="extra flag name=true|false (repeatable)")
    pi.set_defaults(fn=_cmd_install)

    pu = sub.add_parser("uninstall", help="revoke + disable an extension for a tenant")
    pu.add_argument("--tenant", required=True)
    pu.add_argument("--extension", required=True)
    pu.add_argument("--granted-by", default="operator")
    pu.set_defaults(fn=_cmd_uninstall)

    args = p.parse_args()
    asyncio.run(args.fn(args))


if __name__ == "__main__":
    main()
