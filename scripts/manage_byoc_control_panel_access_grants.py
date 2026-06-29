#!/usr/bin/env python3
"""Create, list, or revoke metadata-only BYOC control-panel access grants."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from typing import Any, Sequence
from uuid import UUID

from pydantic import ValidationError

from services.platform.runtime.byoc_control_panel_access import (
    ByocControlPanelAccessGrant,
    ByocControlPanelAccessQuery,
    PostgresByocControlPanelAccessGrantStore,
    model_json_schema_bundle,
)


DEFAULT_DSN_ENV = "DATABASE_URL"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    schema_parser = subparsers.add_parser(
        "schema",
        help="Print the metadata-only grant/query/decision schema bundle.",
    )
    schema_parser.set_defaults(command="schema")

    upsert_parser = subparsers.add_parser(
        "upsert",
        help="Create or replace a tenant/customer/deployment grant.",
    )
    _add_database_args(upsert_parser)
    _add_identity_args(upsert_parser, require_customer=True)
    upsert_parser.add_argument(
        "--deployment-id",
        action="append",
        required=True,
        help="BYOC deployment id. Repeat for multiple deployments.",
    )
    upsert_parser.add_argument(
        "--role",
        required=True,
        choices=("viewer", "operator", "admin"),
        help="Control-panel role to grant.",
    )
    upsert_parser.add_argument(
        "--granted-at",
        help="ISO timestamp for the grant. Defaults to the current UTC time.",
    )
    upsert_parser.add_argument("--expires-at", help="Optional ISO expiry timestamp.")
    upsert_parser.add_argument(
        "--disabled",
        action="store_true",
        help="Store the grant disabled.",
    )
    upsert_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the sanitized grant without writing Postgres.",
    )

    list_parser = subparsers.add_parser(
        "list",
        help="List grants for a hosted tenant, optionally narrowed by BYOC ids.",
    )
    _add_database_args(list_parser)
    _add_identity_args(list_parser, require_customer=False)
    list_parser.add_argument("--deployment-id", help="Optional BYOC deployment id.")

    revoke_parser = subparsers.add_parser(
        "revoke",
        help="Disable one tenant/customer/deployment grant.",
    )
    _add_database_args(revoke_parser)
    _add_identity_args(revoke_parser, require_customer=True)
    revoke_parser.add_argument(
        "--deployment-id",
        required=True,
        help="BYOC deployment id to revoke.",
    )
    revoke_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the revoke target without writing Postgres.",
    )
    return parser.parse_args(argv)


def _add_database_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dsn",
        help=f"Postgres DSN. Defaults to ${DEFAULT_DSN_ENV}.",
    )


def _add_identity_args(
    parser: argparse.ArgumentParser,
    *,
    require_customer: bool,
) -> None:
    parser.add_argument("--tenant-id", required=True, help="Hosted gateway tenant UUID.")
    parser.add_argument(
        "--customer-id",
        required=require_customer,
        help="BYOC customer id.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        return asyncio.run(_run(args))
    except (TypeError, ValueError, ValidationError) as exc:
        _print_errors(
            "BYOC control-panel access grant command failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1
    except RuntimeError as exc:
        _print_errors("BYOC control-panel access grant command failed", [str(exc)])
        return 2


async def _run(args: argparse.Namespace) -> int:
    if args.command == "schema":
        _print_json(model_json_schema_bundle())
        return 0
    if args.command == "upsert":
        grant = _grant_from_args(args)
        if args.dry_run:
            _print_json(
                {
                    "schema_version": "fyralis.byoc.control_panel_access_admin.v1",
                    "action": "upsert",
                    "dry_run": True,
                    "stored_scope": "sanitized_control_panel_access_metadata_only",
                    "grant": _model_json(grant),
                }
            )
            return 0
        store, pool = await _store_from_args(args)
        try:
            persisted = await store.put(grant)
        finally:
            await _close_pool(pool)
        _print_json(
            {
                "schema_version": "fyralis.byoc.control_panel_access_admin.v1",
                "action": "upsert",
                "dry_run": False,
                "stored_scope": "sanitized_control_panel_access_metadata_only",
                "grant": _model_json(persisted),
            }
        )
        return 0
    if args.command == "list":
        query = ByocControlPanelAccessQuery(
            tenant_id=UUID(args.tenant_id),
            customer_id=args.customer_id,
            deployment_id=args.deployment_id or "dep_placeholder",
        )
        store, pool = await _store_from_args(args)
        try:
            grants = await store.list_grants(
                tenant_id=query.tenant_id,
                customer_id=query.customer_id,
                deployment_id=args.deployment_id,
            )
        finally:
            await _close_pool(pool)
        _print_json(
            {
                "schema_version": "fyralis.byoc.control_panel_access_admin_list.v1",
                "result_count": len(grants),
                "stored_scope": "sanitized_control_panel_access_metadata_only",
                "items": [_model_json(grant) for grant in grants],
            }
        )
        return 0
    if args.command == "revoke":
        query = ByocControlPanelAccessQuery(
            tenant_id=UUID(args.tenant_id),
            customer_id=args.customer_id,
            deployment_id=args.deployment_id,
        )
        if args.dry_run:
            _print_json(
                {
                    "schema_version": "fyralis.byoc.control_panel_access_admin.v1",
                    "action": "revoke",
                    "dry_run": True,
                    "stored_scope": "sanitized_control_panel_access_metadata_only",
                    "target": _model_json(query),
                }
            )
            return 0
        store, pool = await _store_from_args(args)
        try:
            revoked = await store.revoke(
                tenant_id=query.tenant_id,
                customer_id=query.customer_id or "",
                deployment_id=query.deployment_id,
            )
        finally:
            await _close_pool(pool)
        _print_json(
            {
                "schema_version": "fyralis.byoc.control_panel_access_admin.v1",
                "action": "revoke",
                "dry_run": False,
                "status": "revoked" if revoked is not None else "not_found",
                "stored_scope": "sanitized_control_panel_access_metadata_only",
                "grant": _model_json(revoked) if revoked is not None else None,
            }
        )
        return 0
    raise RuntimeError(f"unsupported command: {args.command}")


def _grant_from_args(args: argparse.Namespace) -> ByocControlPanelAccessGrant:
    granted_at = _parse_timestamp(args.granted_at) or datetime.now(UTC)
    expires_at = _parse_timestamp(args.expires_at)
    if expires_at is not None and expires_at <= granted_at:
        raise ValueError("--expires-at must be later than --granted-at")
    return ByocControlPanelAccessGrant(
        schema_version="fyralis.byoc.control_panel_access_grant.v1",
        tenant_id=UUID(args.tenant_id),
        customer_id=args.customer_id,
        deployment_ids=tuple(args.deployment_id),
        role=args.role,
        enabled=not args.disabled,
        granted_at=granted_at,
        expires_at=expires_at,
        stored_scope="sanitized_control_panel_access_metadata_only",
    )


async def _store_from_args(
    args: argparse.Namespace,
) -> tuple[PostgresByocControlPanelAccessGrantStore, Any]:
    dsn = args.dsn or os.environ.get(DEFAULT_DSN_ENV)
    if not dsn:
        raise RuntimeError(f"--dsn or {DEFAULT_DSN_ENV} is required")
    try:
        import asyncpg
    except ImportError as exc:  # pragma: no cover - depends on runtime image.
        raise RuntimeError("asyncpg is required for Postgres grant operations") from exc
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    return PostgresByocControlPanelAccessGrantStore(pool), pool


async def _close_pool(pool: Any) -> None:
    close = getattr(pool, "close", None)
    if close is not None:
        await close()


def _parse_timestamp(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _model_json(model: Any) -> dict[str, Any]:
    return json.loads(model.model_dump_json())


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _print_errors(title: str, errors: list[str]) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
