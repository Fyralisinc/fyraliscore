#!/usr/bin/env python3
"""Manage source installation status from an operator shell.

Examples:

  python scripts/manage_source_installations.py status \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --operator-actor 00000000-0000-0000-0000-000000000002

  python scripts/manage_source_installations.py pause \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --operator-actor 00000000-0000-0000-0000-000000000002 \
    --provider slack \
    --installation-id T012345 \
    --reason "provider outage"

  NEW_SLACK_TOKEN=xoxb-... python scripts/manage_source_installations.py rotate-secret \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --operator-actor 00000000-0000-0000-0000-000000000002 \
    --provider slack \
    --installation-id T012345 \
    --new-secret-env NEW_SLACK_TOKEN \
    --reason "customer token rotation"

  python scripts/manage_source_installations.py uninstall \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --operator-actor 00000000-0000-0000-0000-000000000002 \
    --provider slack \
    --installation-id T012345 \
    --reason "customer requested uninstall"
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.shared.ids import uuid7  # noqa: E402
from lib.shared.secrets import SecretStore, build_secret_store  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.platform.operator_auth import require_tenant_operator  # noqa: E402


class SourceInstallationCliError(ValueError):
    """Operator-facing validation error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect, pause, or resume provider_installations rows.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="List installation status.")
    _add_common_args(status_parser)
    _add_selector_args(status_parser, required=False)

    pause_parser = subparsers.add_parser("pause", help="Disable an installation.")
    _add_common_args(pause_parser)
    _add_selector_args(pause_parser, required=True)
    pause_parser.add_argument("--reason", required=True, help="Operator reason.")

    resume_parser = subparsers.add_parser("resume", help="Enable an installation.")
    _add_common_args(resume_parser)
    _add_selector_args(resume_parser, required=True)
    resume_parser.add_argument("--reason", required=True, help="Operator reason.")

    uninstall_parser = subparsers.add_parser(
        "uninstall",
        help=(
            "Disable an installation and remove its generic provider "
            "secret reference."
        ),
    )
    _add_common_args(uninstall_parser)
    _add_selector_args(uninstall_parser, required=True)
    uninstall_parser.add_argument("--reason", required=True, help="Operator reason.")
    uninstall_parser.add_argument(
        "--keep-secret-ref",
        action="store_true",
        help=(
            "Do not delete or clear provider_installations.secret_ref. Use only "
            "when the ref is known to be shared app-level material."
        ),
    )

    rotate_parser = subparsers.add_parser(
        "rotate-secret",
        help="Rotate the encrypted secret referenced by an installation.",
    )
    _add_common_args(rotate_parser)
    _add_selector_args(rotate_parser, required=True)
    _add_secret_input_args(rotate_parser)
    rotate_parser.add_argument("--reason", required=True, help="Operator reason.")

    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tenant", required=True, help="Tenant UUID.")
    parser.add_argument(
        "--operator-actor",
        required=True,
        help="Actor UUID performing this operator action.",
    )


def _add_selector_args(parser: argparse.ArgumentParser, *, required: bool) -> None:
    selector = parser.add_mutually_exclusive_group(required=required)
    selector.add_argument("--installation-row-id", help="provider_installations.id.")
    selector.add_argument(
        "--installation-id",
        help="Provider-native installation id. Requires --provider.",
    )
    parser.add_argument("--provider", help="Provider/source name, for example slack.")


def _add_secret_input_args(parser: argparse.ArgumentParser) -> None:
    secret_source = parser.add_mutually_exclusive_group(required=True)
    secret_source.add_argument(
        "--new-secret-env",
        help="Environment variable containing the replacement secret.",
    )
    secret_source.add_argument(
        "--new-secret-file",
        help="File containing the replacement secret.",
    )
    secret_source.add_argument(
        "--new-secret-stdin",
        action="store_true",
        help="Read the replacement secret from stdin.",
    )


def _parse_uuid(value: str | None, *, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise SourceInstallationCliError(f"{field} must be a UUID") from exc


async def run_command(
    args: argparse.Namespace,
    *,
    conn: asyncpg.Connection,
    secret_store: SecretStore | None = None,
) -> dict[str, Any]:
    tenant_id = _parse_uuid(args.tenant, field="tenant")
    operator_actor_id = _parse_uuid(
        args.operator_actor,
        field="operator_actor",
    )
    await require_tenant_operator(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_actor_id,
        error_type=SourceInstallationCliError,
    )

    if args.command == "status":
        rows = await _select_installations(conn, tenant_id=tenant_id, args=args)
        await _record_operator_action(
            conn,
            tenant_id=tenant_id,
            actor_id=operator_actor_id,
            action="source_installation.status",
            resource_id=rows[0]["id"] if len(rows) == 1 else None,
            metadata={
                "provider": args.provider,
                "installation_id": args.installation_id,
                "installation_row_id": args.installation_row_id,
                "result_count": len(rows),
            },
        )
        return {
            "ok": True,
            "action": "status",
            "tenant_id": str(tenant_id),
            "installations": [_jsonable_installation(row) for row in rows],
        }

    if args.command in {"pause", "resume"}:
        enabled = args.command == "resume"
        row = await _set_installation_enabled(
            conn,
            tenant_id=tenant_id,
            args=args,
            enabled=enabled,
        )
        action = f"source_installation.{args.command}"
        await _record_operator_action(
            conn,
            tenant_id=tenant_id,
            actor_id=operator_actor_id,
            action=action,
            resource_id=row["id"],
            metadata={
                "provider": row["provider"],
                "installation_id": row["installation_id"],
                "reason": args.reason,
                "enabled_before": row["enabled_before"],
                "enabled_after": row["enabled"],
            },
        )
        return {
            "ok": True,
            "action": args.command,
            "tenant_id": str(tenant_id),
            "installation": _jsonable_installation(row),
        }

    if args.command == "uninstall":
        if not args.keep_secret_ref and secret_store is None:
            raise SourceInstallationCliError(
                "uninstall requires a configured secret store unless "
                "--keep-secret-ref is set"
            )
        selected = await _select_single_installation_for_update(
            conn,
            tenant_id=tenant_id,
            args=args,
            required_command="uninstall",
        )
        delete_secret = bool(selected["secret_ref"]) and not args.keep_secret_ref
        if delete_secret:
            assert secret_store is not None
            await secret_store.delete(
                selected["secret_ref"],
                tenant_id=tenant_id,
            )
        row = await _uninstall_installation(
            conn,
            tenant_id=tenant_id,
            args=args,
            clear_secret_ref=not args.keep_secret_ref,
        )
        await _record_operator_action(
            conn,
            tenant_id=tenant_id,
            actor_id=operator_actor_id,
            action="source_installation.uninstall",
            resource_id=row["id"],
            metadata={
                "provider": row["provider"],
                "installation_id": row["installation_id"],
                "reason": args.reason,
                "enabled_before": row["enabled_before"],
                "enabled_after": row["enabled"],
                "had_secret_ref": selected["secret_ref"] is not None,
                "secret_ref_deleted": delete_secret,
                "secret_ref_cleared": row["has_secret_ref"] is False,
                "provider_specific_cleanup_required": True,
                "data_deletion_required_separately": True,
            },
        )
        installation = _jsonable_installation(row)
        installation["secret_ref_deleted"] = delete_secret
        installation["provider_specific_cleanup_required"] = True
        return {
            "ok": True,
            "action": "uninstall",
            "tenant_id": str(tenant_id),
            "installation": installation,
        }

    if args.command == "rotate-secret":
        if secret_store is None:
            raise SourceInstallationCliError(
                "rotate-secret requires a configured secret store"
            )
        row = await _select_single_installation_for_update(
            conn,
            tenant_id=tenant_id,
            args=args,
            required_command="rotate-secret",
        )
        secret_ref = row["secret_ref"]
        if not secret_ref:
            raise SourceInstallationCliError(
                "source installation has no secret_ref to rotate"
            )
        new_secret, secret_source = _read_new_secret(args)
        await secret_store.rotate(secret_ref, new_secret, tenant_id=tenant_id)
        await _record_operator_action(
            conn,
            tenant_id=tenant_id,
            actor_id=operator_actor_id,
            action="source_installation.secret.rotate",
            resource_id=row["id"],
            metadata={
                "provider": row["provider"],
                "installation_id": row["installation_id"],
                "reason": args.reason,
                "secret_source": secret_source,
                "secret_ref_rotated": True,
            },
        )
        installation = _jsonable_installation(row)
        installation["secret_ref_rotated"] = True
        return {
            "ok": True,
            "action": "rotate-secret",
            "tenant_id": str(tenant_id),
            "installation": installation,
        }

    raise SourceInstallationCliError(f"unknown command {args.command!r}")


async def _select_installations(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    args: argparse.Namespace,
) -> list[asyncpg.Record]:
    clauses = ["tenant_id = $1"]
    values: list[Any] = [tenant_id]
    if args.provider:
        values.append(args.provider)
        clauses.append(f"provider = ${len(values)}")
    if args.installation_id:
        if not args.provider:
            raise SourceInstallationCliError("--installation-id requires --provider")
        values.append(args.installation_id)
        clauses.append(f"installation_id = ${len(values)}")
    if args.installation_row_id:
        values.append(_parse_uuid(args.installation_row_id, field="installation_row_id"))
        clauses.append(f"id = ${len(values)}")

    rows = await conn.fetch(
        f"""
        SELECT pi.id, pi.tenant_id, pi.provider, pi.installation_id,
               pi.enabled, pi.installed_at,
               secret_ref IS NOT NULL AS has_secret_ref,
               selected_repositories IS NOT NULL AS has_selected_repositories,
               latest.status AS latest_onboarding_status,
               latest.started_at AS latest_onboarding_started_at,
               latest.completed_at AS latest_onboarding_completed_at,
               latest.reconciled_at AS latest_onboarding_reconciled_at,
               COALESCE(
                   latest.failure_reason IS NOT NULL,
                   FALSE
               ) AS latest_onboarding_has_failure_reason,
               success.completed_at AS last_successful_sync_at
        FROM provider_installations pi
        LEFT JOIN LATERAL (
            SELECT status, started_at, completed_at, reconciled_at,
                   failure_reason, created_at
            FROM source_onboarding_runs sor
            WHERE sor.tenant_id = pi.tenant_id
              AND sor.source = pi.provider
              AND sor.installation_row_id = pi.id
            ORDER BY COALESCE(completed_at, started_at, created_at) DESC
            LIMIT 1
        ) latest ON TRUE
        LEFT JOIN LATERAL (
            SELECT completed_at
            FROM source_onboarding_runs sor
            WHERE sor.tenant_id = pi.tenant_id
              AND sor.source = pi.provider
              AND sor.installation_row_id = pi.id
              AND sor.status = 'completed'
              AND sor.completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 1
        ) success ON TRUE
        WHERE {' AND '.join(f"pi.{clause}" for clause in clauses)}
        ORDER BY pi.provider, pi.installed_at DESC, pi.installation_id
        """,
        *values,
    )
    return list(rows)


async def _set_installation_enabled(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    args: argparse.Namespace,
    enabled: bool,
) -> asyncpg.Record:
    if args.installation_id and not args.provider:
        raise SourceInstallationCliError("--installation-id requires --provider")

    clauses = ["tenant_id = $1"]
    values: list[Any] = [tenant_id]
    if args.installation_row_id:
        values.append(
            _parse_uuid(args.installation_row_id, field="installation_row_id")
        )
        clauses.append(f"id = ${len(values)}")
    elif args.installation_id and args.provider:
        values.extend([args.provider, args.installation_id])
        clauses.append(f"provider = ${len(values) - 1}")
        clauses.append(f"installation_id = ${len(values)}")
    else:
        raise SourceInstallationCliError(
            "pause/resume requires --installation-row-id or "
            "--provider plus --installation-id"
        )

    values.append(enabled)
    enabled_param = len(values)
    row = await conn.fetchrow(
        f"""
        WITH selected AS (
          SELECT id, enabled AS enabled_before
          FROM provider_installations
          WHERE {' AND '.join(clauses)}
          FOR UPDATE
        )
        UPDATE provider_installations pi
           SET enabled = ${enabled_param}
          FROM selected
         WHERE pi.id = selected.id
        RETURNING pi.id, pi.tenant_id, pi.provider, pi.installation_id,
                  pi.enabled, pi.installed_at,
                  pi.secret_ref IS NOT NULL AS has_secret_ref,
                  pi.selected_repositories IS NOT NULL AS has_selected_repositories,
                  selected.enabled_before
        """,
        *values,
    )
    if row is None:
        raise SourceInstallationCliError("source installation not found")
    return row


async def _select_single_installation_for_update(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    args: argparse.Namespace,
    required_command: str,
) -> asyncpg.Record:
    if args.installation_id and not args.provider:
        raise SourceInstallationCliError("--installation-id requires --provider")

    clauses = ["tenant_id = $1"]
    values: list[Any] = [tenant_id]
    if args.installation_row_id:
        values.append(_parse_uuid(args.installation_row_id, field="installation_row_id"))
        clauses.append(f"id = ${len(values)}")
    elif args.installation_id and args.provider:
        values.extend([args.provider, args.installation_id])
        clauses.append(f"provider = ${len(values) - 1}")
        clauses.append(f"installation_id = ${len(values)}")
    else:
        raise SourceInstallationCliError(
            f"{required_command} requires --installation-row-id or "
            "--provider plus --installation-id"
        )

    row = await conn.fetchrow(
        f"""
        SELECT id, tenant_id, provider, installation_id, secret_ref, enabled,
               installed_at,
               secret_ref IS NOT NULL AS has_secret_ref,
               selected_repositories IS NOT NULL AS has_selected_repositories
        FROM provider_installations
        WHERE {' AND '.join(clauses)}
        FOR UPDATE
        """,
        *values,
    )
    if row is None:
        raise SourceInstallationCliError("source installation not found")
    return row


async def _uninstall_installation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    args: argparse.Namespace,
    clear_secret_ref: bool,
) -> asyncpg.Record:
    if args.installation_id and not args.provider:
        raise SourceInstallationCliError("--installation-id requires --provider")

    clauses = ["tenant_id = $1"]
    values: list[Any] = [tenant_id]
    if args.installation_row_id:
        values.append(_parse_uuid(args.installation_row_id, field="installation_row_id"))
        clauses.append(f"id = ${len(values)}")
    elif args.installation_id and args.provider:
        values.extend([args.provider, args.installation_id])
        clauses.append(f"provider = ${len(values) - 1}")
        clauses.append(f"installation_id = ${len(values)}")
    else:
        raise SourceInstallationCliError(
            "uninstall requires --installation-row-id or "
            "--provider plus --installation-id"
        )

    secret_assignment = "secret_ref = NULL," if clear_secret_ref else ""
    row = await conn.fetchrow(
        f"""
        WITH selected AS (
          SELECT id, enabled AS enabled_before
          FROM provider_installations
          WHERE {' AND '.join(clauses)}
          FOR UPDATE
        )
        UPDATE provider_installations pi
           SET enabled = FALSE,
               {secret_assignment}
               installed_at = pi.installed_at
          FROM selected
         WHERE pi.id = selected.id
        RETURNING pi.id, pi.tenant_id, pi.provider, pi.installation_id,
                  pi.enabled, pi.installed_at,
                  pi.secret_ref IS NOT NULL AS has_secret_ref,
                  pi.selected_repositories IS NOT NULL AS has_selected_repositories,
                  selected.enabled_before
        """,
        *values,
    )
    if row is None:
        raise SourceInstallationCliError("source installation not found")
    return row


def _read_new_secret(args: argparse.Namespace) -> tuple[bytes, str]:
    if args.new_secret_env:
        if args.new_secret_env not in os.environ:
            raise SourceInstallationCliError(
                f"environment variable {args.new_secret_env!r} is not set"
            )
        value: bytes | str = os.environ[args.new_secret_env]
        source = "env"
    elif args.new_secret_file:
        try:
            value = pathlib.Path(args.new_secret_file).read_bytes().rstrip(b"\r\n")
        except OSError as exc:
            raise SourceInstallationCliError(
                f"could not read secret file: {exc}"
            ) from exc
        source = "file"
    elif args.new_secret_stdin:
        value = sys.stdin.buffer.read().rstrip(b"\r\n")
        source = "stdin"
    else:
        raise SourceInstallationCliError(
            "provide --new-secret-env, --new-secret-file, or --new-secret-stdin"
        )

    if isinstance(value, str):
        secret = value.encode("utf-8")
    else:
        secret = value
    if not secret:
        raise SourceInstallationCliError("new secret must be non-empty")
    return secret, source


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    action: str,
    resource_id: UUID | None,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES ($1, $2, $3, $4, 'provider_installation', $5, $6::jsonb, now())
        """,
        uuid7(),
        tenant_id,
        actor_id,
        action,
        resource_id,
        json.dumps(metadata, default=str, sort_keys=True),
    )


def _jsonable_installation(row: asyncpg.Record) -> dict[str, Any]:
    out: dict[str, Any] = {}
    keys = set(row.keys())
    for key in (
        "id",
        "tenant_id",
        "provider",
        "installation_id",
        "enabled",
        "installed_at",
        "has_secret_ref",
        "has_selected_repositories",
        "enabled_before",
        "latest_onboarding_status",
        "latest_onboarding_started_at",
        "latest_onboarding_completed_at",
        "latest_onboarding_reconciled_at",
        "latest_onboarding_has_failure_reason",
        "last_successful_sync_at",
    ):
        if key not in keys:
            continue
        value = row[key]
        if isinstance(value, UUID):
            out[key] = str(value)
        elif isinstance(value, datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    if "latest_onboarding_status" in keys:
        out["source_health"] = _source_health(row)
    return out


def _source_health(row: asyncpg.Record) -> str:
    keys = set(row.keys())
    enabled = row["enabled"] if "enabled" in keys else True
    if enabled is False:
        return "paused"
    latest_status = row["latest_onboarding_status"]
    if latest_status == "failed":
        return "degraded"
    if latest_status in {"pending", "in_progress"}:
        return "syncing"
    if row["last_successful_sync_at"] is not None:
        return "healthy"
    return "installed_no_sync"


async def _main_async(args: argparse.Namespace) -> int:
    if not args.dsn:
        print(
            json.dumps({"ok": False, "error": "DATABASE_URL is not set"}),
            file=sys.stderr,
        )
        return 2
    conn = await asyncpg.connect(dsn=args.dsn)
    try:
        await _register_codecs(conn)
        secret_store = (
            build_secret_store(conn)  # type: ignore[arg-type]
            if args.command in {"rotate-secret", "uninstall"}
            else None
        )
        async with conn.transaction():
            result = await run_command(args, conn=conn, secret_store=secret_store)
    finally:
        await conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except SourceInstallationCliError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
