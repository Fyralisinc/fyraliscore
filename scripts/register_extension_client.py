#!/usr/bin/env python
"""scripts/register_extension_client.py — manage extension OAuth2 clients (M1).

Register / rotate / revoke the client_credentials an extension uses to authenticate
to the host (`extension_oauth_clients`, migration 0128). The plaintext secret is
printed ONCE on register/rotate and never stored — capture it then.

    DATABASE_URL=... python scripts/register_extension_client.py register \
        --extension-id github_intel --env sandbox --created-by ops \
        --callback-url https://ext.example.com/fyralis/webhook
    ... python scripts/register_extension_client.py rotate  --client-id ext_...
    ... python scripts/register_extension_client.py revoke  --client-id ext_...
"""
from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg

from services.platform.extensions.identity import ExtensionOAuthClientsRepo


async def _run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set")
        return 2
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        repo = ExtensionOAuthClientsRepo(pool)
        if args.cmd == "register":
            client = await repo.register(
                extension_id=args.extension_id, created_by=args.created_by,
                environment=args.env, display_name=args.display_name,
                callback_url=args.callback_url,
            )
            print(f"client_id     = {client.client_id}")
            print(f"client_secret = {client.client_secret}   # shown once — store it now")
            print(f"extension_id  = {client.extension_id}  env={client.environment}")
        elif args.cmd == "rotate":
            secret = await repo.rotate_secret(args.client_id)
            print(f"client_secret = {secret}" if secret else "no such active client")
            return 0 if secret else 1
        elif args.cmd == "revoke":
            ok = await repo.revoke(args.client_id)
            print("revoked" if ok else "no such active client")
            return 0 if ok else 1
    finally:
        await pool.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    reg = sub.add_parser("register")
    reg.add_argument("--extension-id", required=True)
    reg.add_argument("--created-by", required=True)
    reg.add_argument("--env", default="production", choices=["sandbox", "production"])
    reg.add_argument("--display-name", default=None)
    reg.add_argument("--callback-url", default=None)
    for name in ("rotate", "revoke"):
        p = sub.add_parser(name)
        p.add_argument("--client-id", required=True)
    return asyncio.run(_run(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
