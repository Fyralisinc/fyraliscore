#!/usr/bin/env python3
"""scripts/sandbox_seed_secret.py — wire a webhook signing secret into the
encrypted secret store and point a provider_installations row at it.

Under prod guards (FYRALIS_ENV=prod) the gateway resolves webhook signing
secrets ONLY from `provider_installations.secret_ref` → the envelope-
encrypted store (services/app/webhooks/secrets.py). Slack and Discord populate
this automatically during their OAuth callback. GitHub does NOT — its
install callback writes the installation row with secret_ref=NULL — so the
GitHub App webhook secret must be seeded here once, after install.

Run it INSIDE the sandbox stack so it shares the gateway's DATABASE_URL +
MASTER_KEK:

    docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \\
        exec gateway python scripts/sandbox_seed_secret.py \\
        github <installation_id> '<WEBHOOK_SECRET_GITHUB value>'

Tenant defaults to $COMPANY_OS_TENANT_ID; override with --tenant.
Idempotent-ish: each run allocates a fresh encrypted_secrets ref and
repoints the installation row at it (the old ref is left in place, which
is how rotation works — the verifier only follows the current ref).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
from uuid import UUID

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import asyncpg

from lib.shared.secrets import build_secret_store
from services.app.gateway.db_bootstrap import _register_codecs

_PROVIDERS = ("github", "slack", "discord", "linear", "stripe")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sandbox_seed_secret")
    p.add_argument("provider", choices=_PROVIDERS)
    p.add_argument("installation_id", help="provider installation id (GitHub App installation id, Slack team id, Discord guild id)")
    p.add_argument("secret_value", help="the raw signing secret (GitHub webhook secret; Discord Ed25519 public key; Slack signing secret)")
    p.add_argument("--tenant", default=os.environ.get("COMPANY_OS_TENANT_ID"), help="tenant UUID (default: $COMPANY_OS_TENANT_ID)")
    p.add_argument("--label", default=None, help="secret label (default: <provider>_webhook_secret:<installation_id>)")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"), help="postgres DSN (default: $DATABASE_URL)")
    return p.parse_args()


async def _run(args: argparse.Namespace) -> int:
    if not args.tenant:
        print("ERROR: no tenant — pass --tenant or set COMPANY_OS_TENANT_ID", file=sys.stderr)
        return 1
    if not args.dsn:
        print("ERROR: no DSN — pass --dsn or set DATABASE_URL", file=sys.stderr)
        return 1
    tenant_id = UUID(args.tenant)
    label = args.label or f"{args.provider}_webhook_secret:{args.installation_id}"

    pool = await asyncpg.create_pool(dsn=args.dsn, min_size=1, max_size=2, init=_register_codecs)
    try:
        store = build_secret_store(pool)  # reads MASTER_KEK from env
        ref = await store.put(args.secret_value, label=label, tenant_id=tenant_id)

        # Repoint the existing installation row (created by the OAuth
        # callback) at the freshly stored secret.
        status = await pool.execute(
            """
            UPDATE provider_installations
               SET secret_ref = $1, enabled = TRUE
             WHERE provider = $2
               AND installation_id = $3
               AND tenant_id = $4
            """,
            ref, args.provider, str(args.installation_id), tenant_id,
        )
        if status.split()[-1] == "0":
            print(
                f"WARNING: stored secret (ref={ref}) but found NO "
                f"provider_installations row for "
                f"(provider={args.provider}, installation_id={args.installation_id}, "
                f"tenant={tenant_id}). Run the OAuth install first, then re-run "
                f"this, or register the row with scripts/webhook_install.py.",
                file=sys.stderr,
            )
            return 1
        print(
            f"OK: secret stored (ref={ref}) and provider_installations "
            f"row repointed for {args.provider}/{args.installation_id} "
            f"(tenant={tenant_id})."
        )
        return 0
    finally:
        await pool.close()


def main() -> None:
    raise SystemExit(asyncio.run(_run(_parse_args())))


if __name__ == "__main__":
    main()
