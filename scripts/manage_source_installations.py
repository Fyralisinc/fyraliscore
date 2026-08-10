#!/usr/bin/env python3
"""Inspect and change contract-owned source connector installations."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
from typing import Any
from uuid import UUID

import asyncpg

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lib.shared.ids import uuid7
from services.app.gateway.db_bootstrap import _register_codecs
from services.platform.operator_auth import require_tenant_operator


class SourceInstallationCliError(ValueError):
    """Operator-facing validation error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage source_connector_installations rows.",
    )
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("status", "pause", "resume", "maintenance", "uninstall"):
        child = subparsers.add_parser(command)
        child.add_argument("--tenant", required=True, type=UUID)
        child.add_argument("--operator-actor", required=True, type=UUID)
        child.add_argument("--installation-id", type=UUID)
        child.add_argument("--source")
        if command != "status":
            child.add_argument("--reason", required=True)
    return parser


async def run_command(
    args: argparse.Namespace,
    *,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    await require_tenant_operator(
        conn,
        tenant_id=args.tenant,
        actor_id=args.operator_actor,
        error_type=SourceInstallationCliError,
    )
    connector_id = _connector_id(args.source)
    if args.command == "status":
        rows = await conn.fetch(
            """
            SELECT id, connector_id, external_installation_id, desired_state,
                   observed_phase, generation, observed_generation,
                   enabled_capabilities, conditions, updated_at
              FROM source_connector_installations
             WHERE tenant_id = $1
               AND ($2::uuid IS NULL OR id = $2)
               AND ($3::text IS NULL OR connector_id = $3)
             ORDER BY connector_id, id
            """,
            args.tenant,
            args.installation_id,
            connector_id,
        )
        await _record_action(
            conn,
            args=args,
            action="source_installation.status",
            resource_id=rows[0]["id"] if len(rows) == 1 else None,
            metadata={"source": args.source, "result_count": len(rows)},
        )
        return {
            "ok": True,
            "action": "status",
            "tenant_id": str(args.tenant),
            "installations": [_jsonable(row) for row in rows],
        }

    if args.installation_id is None and connector_id is None:
        raise SourceInstallationCliError(
            "a mutating command requires --installation-id or --source"
        )
    desired = {
        "pause": "Paused",
        "resume": "Ready",
        "maintenance": "Maintenance",
        "uninstall": "Removed",
    }[args.command]
    rows = await conn.fetch(
        """
        UPDATE source_connector_installations
           SET desired_state = $4,
               generation = generation + 1,
               next_reconcile_at = now(),
               provenance = provenance || jsonb_build_object(
                   'last_operator_actor', $5::text,
                   'last_operator_reason', $6::text,
                   'last_operator_action', $7::text
               ),
               updated_at = now()
         WHERE tenant_id = $1
           AND ($2::uuid IS NULL OR id = $2)
           AND ($3::text IS NULL OR connector_id = $3)
           AND observed_phase <> 'Removed'
        RETURNING id, connector_id, external_installation_id, desired_state,
                  observed_phase, generation, observed_generation,
                  enabled_capabilities, conditions, updated_at
        """,
        args.tenant,
        args.installation_id,
        connector_id,
        desired,
        args.operator_actor,
        args.reason,
        args.command,
    )
    if not rows:
        raise SourceInstallationCliError("no matching active installation")
    await conn.execute(
        """
        UPDATE source_connector_authority_grants AS authority
           SET authority_generation = GREATEST(
                   authority.authority_generation,
                   install.generation
               ),
               updated_at = now()
          FROM source_connector_installations AS install
         WHERE authority.installation_id = install.id
           AND install.id = ANY($1::uuid[])
           AND authority.revoked_at IS NULL
        """,
        [row["id"] for row in rows],
    )
    await _record_action(
        conn,
        args=args,
        action=f"source_installation.{args.command}",
        resource_id=rows[0]["id"] if len(rows) == 1 else None,
        metadata={
            "source": args.source,
            "reason": args.reason,
            "desired_state": desired,
            "result_count": len(rows),
        },
    )
    return {
        "ok": True,
        "action": args.command,
        "tenant_id": str(args.tenant),
        "installations": [_jsonable(row) for row in rows],
    }


def _connector_id(source: str | None) -> str | None:
    if source is None:
        return None
    value = source.strip().lower()
    if not value:
        raise SourceInstallationCliError("source must not be empty")
    return value if value.startswith("fyralis/") else f"fyralis/{value}"


def _jsonable(row: Any) -> dict[str, Any]:
    return {
        key: (
            value.isoformat()
            if hasattr(value, "isoformat")
            else str(value)
            if isinstance(value, UUID)
            else value
        )
        for key, value in dict(row).items()
    }


async def _record_action(
    conn: asyncpg.Connection,
    *,
    args: argparse.Namespace,
    action: str,
    resource_id: UUID | None,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES (
            $1, $2, $3, $4, 'source_connector_installation', $5, $6::jsonb, now()
        )
        """,
        uuid7(),
        args.tenant,
        args.operator_actor,
        action,
        resource_id,
        json.dumps(metadata, default=str, sort_keys=True),
    )


async def _main(args: argparse.Namespace) -> int:
    if not args.dsn:
        raise SourceInstallationCliError("--dsn or DATABASE_URL is required")
    conn = await asyncpg.connect(args.dsn, statement_cache_size=0)
    try:
        await _register_codecs(conn)
        async with conn.transaction():
            result = await run_command(args, conn=conn)
        print(json.dumps(result, indent=2, default=str))
        return 0
    finally:
        await conn.close()


def main() -> int:
    try:
        return asyncio.run(_main(build_parser().parse_args()))
    except SourceInstallationCliError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
