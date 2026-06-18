#!/usr/bin/env python
"""scripts/marketplace.py — operate the extension marketplace (E4).

    DATABASE_URL=... python scripts/marketplace.py submit manifest.json --by dev [--public]
    ... python scripts/marketplace.py review-sign --id acme --version 1.0.0 --by reviewer
    ... python scripts/marketplace.py list
    ... python scripts/marketplace.py install --id acme --tenant <uuid> --by admin
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from uuid import UUID

import asyncpg

from services.platform.extensions.marketplace import MarketplaceRepo


async def _run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set")
        return 2
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    repo = MarketplaceRepo(pool)
    try:
        if args.cmd == "submit":
            with open(args.manifest) as fh:
                manifest = json.load(fh)
            res = await repo.submit(manifest, submitted_by=args.by,
                                    visibility="public" if args.public else "private")
            print(json.dumps(res, default=str, indent=2))
            return 0 if res["status"] != "rejected" else 1
        if args.cmd == "review-sign":
            print(json.dumps(await repo.review_and_sign(
                extension_id=args.id, version=args.version, reviewed_by=args.by),
                default=str, indent=2))
        elif args.cmd == "list":
            for r in await repo.list_published():
                print(f"{r['extension_id']:30} {r['version']:10} {r['visibility']:8} {r['publisher']}")
        elif args.cmd == "install":
            res = await repo.install_listing(
                tenant_id=UUID(args.tenant), extension_id=args.id, granted_by=args.by,
                version=args.version)
            print(f"installed {args.id} for tenant {args.tenant}: {res}")
    finally:
        await pool.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit"); s.add_argument("manifest"); s.add_argument("--by", required=True)
    s.add_argument("--public", action="store_true")
    rs = sub.add_parser("review-sign")
    rs.add_argument("--id", required=True); rs.add_argument("--version", required=True)
    rs.add_argument("--by", required=True)
    sub.add_parser("list")
    ins = sub.add_parser("install")
    ins.add_argument("--id", required=True); ins.add_argument("--tenant", required=True)
    ins.add_argument("--by", required=True); ins.add_argument("--version", default=None)
    return asyncio.run(_run(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
