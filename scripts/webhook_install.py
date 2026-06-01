#!/usr/bin/env python3
"""
scripts/webhook_install.py — admin CLI for the webhook tenant
resolution registry (IN-07).

Usage:
    python scripts/webhook_install.py register \\
        --provider slack \\
        --installation-id T_ACME_123 \\
        --tenant-id 11111111-1111-7111-8111-111111111111 \\
        [--secret-ref arn:secrets:slack/acme/v1]

    python scripts/webhook_install.py disable --id <row_uuid>
    python scripts/webhook_install.py enable  --id <row_uuid>
    python scripts/webhook_install.py update-secret-ref \\
        --id <row_uuid> --secret-ref <new_ref>

All subcommands read DATABASE_URL from the environment (or take
--dsn). Operator-level authorization is implicit via shell access
(FR-017 — no auth-on-the-wire needed for a CLI).

Exit codes:
    0 = success (JSON result printed to stdout)
    1 = bad arguments / conflict / not found (structured error to stderr)

Print is the script's stdout/stderr contract; it is NOT "service code"
under Constitution §VIII's no-print() rule. The resolver itself
(services/webhooks/tenant_resolver.py) uses structlog exclusively.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from uuid import UUID

import asyncpg

from lib.shared.errors import (
    CompanyOSError,
    InstallationConflictError,
    InstallationNotFoundError,
)
from services.webhooks.tenant_resolver import (
    InstallationCache,
    RegisterInstallationRequest,
    TenantResolverDeps,
    build_tenant_resolver,
    noop_metrics,
)


_PROVIDERS: tuple[str, ...] = ("slack", "github", "linear", "stripe", "discord")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="webhook_install",
        description="Admin CLI for provider_installations (IN-07).",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN (default: $DATABASE_URL).",
    )
    sub = parser.add_subparsers(dest="action", required=True)

    p_reg = sub.add_parser(
        "register",
        help="Register a new (provider, installation_id, tenant_id).",
    )
    p_reg.add_argument("--provider", required=True, choices=_PROVIDERS)
    p_reg.add_argument("--installation-id", required=True)
    p_reg.add_argument("--tenant-id", required=True)
    p_reg.add_argument("--secret-ref", default=None)

    for verb, helptext in (
        ("disable", "Disable an existing installation by row id."),
        ("enable", "Re-enable a previously-disabled installation."),
    ):
        p = sub.add_parser(verb, help=helptext)
        p.add_argument("--id", required=True)

    p_upd = sub.add_parser(
        "update-secret-ref",
        help="Update the secret_ref pointer on an existing installation.",
    )
    p_upd.add_argument("--id", required=True)
    p_upd.add_argument(
        "--secret-ref",
        default=None,
        help="New secret_ref value, or omit to set NULL.",
    )

    return parser.parse_args()


def _print_err(err: CompanyOSError) -> None:
    sys.stderr.write(json.dumps(err.to_dict(), default=str))
    sys.stderr.write("\n")


async def _run(args: argparse.Namespace) -> int:
    if not args.dsn:
        sys.stderr.write(
            json.dumps(
                {
                    "code": "missing_dsn",
                    "message": "Set $DATABASE_URL or pass --dsn.",
                    "context": {},
                }
            )
        )
        sys.stderr.write("\n")
        return 1

    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=2)
    try:
        deps = TenantResolverDeps(
            pool=pool,
            cache=InstallationCache(),
            clock=__import__("time").monotonic,
            metrics=noop_metrics(),
        )
        resolver = build_tenant_resolver(deps)

        if args.action == "register":
            req = RegisterInstallationRequest(
                provider=args.provider,  # type: ignore[arg-type]
                tenant_id=UUID(args.tenant_id),
                installation_id=args.installation_id,
                secret_ref=args.secret_ref,
            )
            try:
                installation = await resolver.register_installation(req)
            except InstallationConflictError as e:
                _print_err(e)
                return 1
            print(
                json.dumps(
                    {
                        "outcome": "registered",
                        "id": str(installation.id),
                        "provider": installation.provider,
                        "installation_id": installation.installation_id,
                        "tenant_id": str(installation.tenant_id),
                        "secret_ref": installation.secret_ref,
                        "enabled": installation.enabled,
                        "installed_at": installation.installed_at.isoformat(),
                    }
                )
            )
            return 0

        # disable / enable / update-secret-ref all key by row id
        row_id = UUID(args.id)
        try:
            if args.action == "disable":
                await resolver.disable_installation(row_id)
                outcome = "disabled"
            elif args.action == "enable":
                await resolver.enable_installation(row_id)
                outcome = "enabled"
            elif args.action == "update-secret-ref":
                await resolver.update_secret_ref(row_id, args.secret_ref)
                outcome = "secret_ref_updated"
            else:  # pragma: no cover — argparse rejects unknown subcommands
                raise SystemExit(f"unknown action: {args.action}")
        except InstallationNotFoundError as e:
            _print_err(e)
            return 1

        print(json.dumps({"outcome": outcome, "id": str(row_id)}))
        return 0

    finally:
        await pool.close()


def main() -> int:
    args = _parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
