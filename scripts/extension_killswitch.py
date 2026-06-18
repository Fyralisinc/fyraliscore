#!/usr/bin/env python
"""scripts/extension_killswitch.py — break-glass global disable for an extension (E3.5).

While killed, the host issues no tokens and serves no read/write/stream for the
extension, for ANY tenant. Per-tenant disable is grant revocation
(scripts/manage_extension.py uninstall), not this.

    DATABASE_URL=... python scripts/extension_killswitch.py disable --id acme --by ops --reason "abuse"
    ... python scripts/extension_killswitch.py enable --id acme
    ... python scripts/extension_killswitch.py status --id acme
"""
from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg

from services.platform.extensions.killswitch import KillSwitch


async def _run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set")
        return 2
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        ks = KillSwitch(pool)
        if args.cmd == "disable":
            await ks.disable(args.id, disabled_by=args.by, reason=args.reason)
            print(f"killed: {args.id}")
        elif args.cmd == "enable":
            ok = await ks.enable(args.id)
            print("re-enabled" if ok else "was not killed")
        elif args.cmd == "status":
            print("KILLED" if await ks.is_killed(args.id) else "active")
    finally:
        await pool.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("disable")
    d.add_argument("--id", required=True)
    d.add_argument("--by", required=True)
    d.add_argument("--reason", default=None)
    for name in ("enable", "status"):
        p = sub.add_parser(name)
        p.add_argument("--id", required=True)
    return asyncio.run(_run(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
