#!/usr/bin/env python3
"""Manage dedicated source installation rows from an operator shell.

This complements ``scripts/manage_source_installations.py``. That generic tool
operates on ``provider_installations`` rows used by webhook/auth edges; this
tool operates on source-specific installation tables that own access-token,
refresh-token, and webhook-secret refs directly.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import pathlib
import sys
from typing import Any
from uuid import UUID

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.shared.errors import SecretNotFoundError  # noqa: E402
from lib.shared.ids import uuid7  # noqa: E402
from lib.shared.secrets import SecretStore, build_secret_store  # noqa: E402
from lib.shared.tenant_context import bind_tenant  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.ingest.source_contract import (  # noqa: E402
    INSTALLATION_MANAGEMENT_CATALOG,
    InstallationManagementDefinition,
)
from services.platform.operator_auth import require_tenant_operator  # noqa: E402


class DedicatedSourceInstallationCliError(ValueError):
    """Operator-facing validation error."""


INSTALLATION_MANAGEMENT_SPECS = INSTALLATION_MANAGEMENT_CATALOG

SECRET_FIELD_TO_COLUMN = {
    "access": "secret_ref",
    "refresh": "refresh_secret_ref",
    "webhook": "webhook_secret_ref",
    "api-hash": "api_hash_secret_ref",
    "session": "session_secret_ref",
    "backfill-session": "backfill_session_secret_ref",
    "app-secret": "app_secret_ref",
    "verify-token": "verify_token_ref",
    "access-token": "access_token_ref",
}

WEBHOOK_CLEANUP_NOT_APPLICABLE = "not_applicable"
WEBHOOK_CLEANUP_LOCAL_RESOLVER_DISABLED = "local_resolver_disabled"
WEBHOOK_CLEANUP_PROVIDER_ROW_MISSING = "provider_row_missing"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect, pause, resume, rotate, or uninstall dedicated "
        "source installation rows.",
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
        help="Disable an installation and remove its dedicated secret refs.",
    )
    _add_common_args(uninstall_parser)
    _add_selector_args(uninstall_parser, required=True)
    uninstall_parser.add_argument("--reason", required=True, help="Operator reason.")
    uninstall_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report refs that would be removed without mutating anything.",
    )

    rotate_parser = subparsers.add_parser(
        "rotate-secret",
        help="Rotate one encrypted secret referenced by an installation.",
    )
    _add_common_args(rotate_parser)
    _add_selector_args(rotate_parser, required=True)
    rotate_parser.add_argument(
        "--secret-field",
        choices=tuple(SECRET_FIELD_TO_COLUMN),
        required=True,
        help="Which referenced secret to rotate.",
    )
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
    parser.add_argument(
        "--source",
        choices=tuple(sorted(INSTALLATION_MANAGEMENT_SPECS)),
        required=True,
        help="Dedicated source table to manage.",
    )


def _add_selector_args(parser: argparse.ArgumentParser, *, required: bool) -> None:
    selector = parser.add_mutually_exclusive_group(required=required)
    selector.add_argument("--installation-row-id", help="Dedicated install row UUID.")
    selector.add_argument(
        "--scope-id",
        help=(
            "Source-native scope id, for example realm_id, organization_urn, "
            "or AWS account_id."
        ),
    )
    parser.add_argument(
        "--region",
        help="AWS region. Valid only with --source aws.",
    )


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
        raise DedicatedSourceInstallationCliError(f"{field} must be a UUID") from exc


async def run_command(
    args: argparse.Namespace,
    *,
    conn: asyncpg.Connection,
    secret_store: SecretStore | None = None,
) -> dict[str, Any]:
    tenant_id = _parse_uuid(args.tenant, field="tenant")
    operator_actor_id = _parse_uuid(args.operator_actor, field="operator_actor")
    spec = INSTALLATION_MANAGEMENT_SPECS[args.source]

    async with conn.transaction():
        async with bind_tenant(conn, tenant_id):
            await require_tenant_operator(
                conn,
                tenant_id=tenant_id,
                actor_id=operator_actor_id,
                error_type=DedicatedSourceInstallationCliError,
            )

            if args.command == "status":
                rows = await _select_installations(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    args=args,
                    for_update=False,
                )
                await _record_operator_action(
                    conn,
                    tenant_id=tenant_id,
                    actor_id=operator_actor_id,
                    action="source_installation.status",
                    resource_id=rows[0]["id"] if len(rows) == 1 else None,
                    resource_type=spec.table,
                    metadata={
                        "source": spec.source,
                        "scope_column": spec.scope_column,
                        "scope_id_hash": (
                            _hash_scope_value(args.scope_id)
                        ),
                        "installation_row_id": args.installation_row_id,
                        "result_count": len(rows),
                    },
                )
                return {
                    "ok": True,
                    "action": "status",
                    "tenant_id": str(tenant_id),
                    "source": spec.source,
                    "installations": [_jsonable_installation(row, spec) for row in rows],
                }

            if args.command in {"pause", "resume"}:
                disable = args.command == "pause"
                row = await _set_disabled_at(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    args=args,
                    disabled=disable,
                )
                webhook_row_updated = await _set_provider_installation_enabled(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    installation_id=_provider_installation_id(row, spec),
                    enabled=not disable,
                )
                await _record_operator_action(
                    conn,
                    tenant_id=tenant_id,
                    actor_id=operator_actor_id,
                    action=f"source_installation.{args.command}",
                    resource_id=row["id"],
                    resource_type=spec.table,
                    metadata={
                        "source": spec.source,
                        "scope_column": spec.scope_column,
                        "scope_id_hash": _hash_scope_value(row[spec.scope_column]),
                        "reason": args.reason,
                        "enabled_before": row["disabled_before"] is None,
                        "enabled_after": row["disabled_at"] is None,
                        "webhook_provider_row_updated": webhook_row_updated,
                    },
                )
                installation = _jsonable_installation(row, spec)
                installation["webhook_provider_row_updated"] = webhook_row_updated
                return {
                    "ok": True,
                    "action": args.command,
                    "tenant_id": str(tenant_id),
                    "source": spec.source,
                    "installation": installation,
                }

            if args.command == "rotate-secret":
                if secret_store is None:
                    raise DedicatedSourceInstallationCliError(
                        "rotate-secret requires a configured secret store"
                    )
                secret_column = _column_for_secret_field(spec, args.secret_field)
                row = await _select_single_installation(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    args=args,
                    required_command="rotate-secret",
                    for_update=True,
                )
                secret_ref = row[secret_column]
                if not secret_ref:
                    raise DedicatedSourceInstallationCliError(
                        f"source installation has no {secret_column} to rotate"
                    )
                new_secret, secret_source = _read_new_secret(args)
                await secret_store.rotate(str(secret_ref), new_secret, tenant_id=tenant_id)
                await _record_operator_action(
                    conn,
                    tenant_id=tenant_id,
                    actor_id=operator_actor_id,
                    action="source_installation.secret.rotate",
                    resource_id=row["id"],
                    resource_type=spec.table,
                    metadata={
                        "source": spec.source,
                        "scope_column": spec.scope_column,
                        "scope_id_hash": _hash_scope_value(row[spec.scope_column]),
                        "reason": args.reason,
                        "secret_field": args.secret_field,
                        "secret_source": secret_source,
                        "secret_ref_rotated": True,
                    },
                )
                installation = _jsonable_installation(row, spec)
                installation["secret_ref_rotated"] = True
                installation["rotated_secret_field"] = args.secret_field
                return {
                    "ok": True,
                    "action": "rotate-secret",
                    "tenant_id": str(tenant_id),
                    "source": spec.source,
                    "installation": installation,
                }

            if args.command == "uninstall":
                row = await _select_single_installation(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    args=args,
                    required_command="uninstall",
                    for_update=True,
                )
                refs = _refs_from_row(row, spec)
                if refs and not args.dry_run and secret_store is None:
                    raise DedicatedSourceInstallationCliError(
                        "uninstall requires a configured secret store unless "
                        "--dry-run is set or the installation has no secret refs"
                    )
                if args.dry_run:
                    return {
                        "ok": True,
                        "action": "uninstall",
                        "dry_run": True,
                        "tenant_id": str(tenant_id),
                        "source": spec.source,
                        "installation": {
                            **_jsonable_installation(row, spec),
                            "refs_seen": len(refs),
                            "refs_deleted": 0,
                            "secret_delete_errors": 0,
                            "webhook_cleanup_status": (
                                _webhook_cleanup_status(
                                    spec=spec,
                                    provider_row_updated=False,
                                )
                            ),
                            "webhook_cleanup_complete": False,
                        },
                    }

                assert secret_store is not None
                deleted_columns: set[str] = set()
                delete_errors: dict[str, str] = {}
                for column, ref in refs.items():
                    try:
                        await secret_store.delete(str(ref), tenant_id=tenant_id)
                        deleted_columns.add(column)
                    except SecretNotFoundError:
                        deleted_columns.add(column)
                    except Exception as exc:  # pragma: no cover - backend-specific
                        delete_errors[column] = exc.__class__.__name__

                provider_installation_id = _provider_installation_id(row, spec)
                updated = await _uninstall_installation(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    row_id=row["id"],
                    clear_columns=tuple(sorted(deleted_columns)),
                )
                webhook_row_updated = await _uninstall_provider_installation(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    installation_id=provider_installation_id,
                    clear_secret_ref="webhook_secret_ref" in deleted_columns,
                )
                webhook_cleanup_status = _webhook_cleanup_status(
                    spec=spec,
                    provider_row_updated=webhook_row_updated,
                )
                webhook_cleanup_complete = _webhook_cleanup_complete(
                    webhook_cleanup_status
                )
                native_watch_rows_cleared = await _clear_native_google_watch_state(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    installation_row_id=row["id"],
                )
                await _record_operator_action(
                    conn,
                    tenant_id=tenant_id,
                    actor_id=operator_actor_id,
                    action="source_installation.uninstall",
                    resource_id=row["id"],
                    resource_type=spec.table,
                    metadata={
                        "source": spec.source,
                        "scope_column": spec.scope_column,
                        "scope_id_hash": _hash_scope_value(row[spec.scope_column]),
                        "reason": args.reason,
                        "enabled_before": row["disabled_at"] is None,
                        "enabled_after": False,
                        "refs_seen": len(refs),
                        "refs_deleted": len(deleted_columns),
                        "secret_delete_errors": len(delete_errors),
                        "ref_columns_cleared": sorted(deleted_columns),
                        "webhook_deregistration_required": (
                            "webhook_secret_ref" in spec.ref_columns
                        ),
                        "webhook_provider_row_updated": webhook_row_updated,
                        "webhook_cleanup_status": webhook_cleanup_status,
                        "webhook_cleanup_complete": webhook_cleanup_complete,
                        "native_google_watch_rows_cleared": (
                            native_watch_rows_cleared
                        ),
                        "data_deletion_required_separately": True,
                    },
                )
                await _record_installation_audit(
                    conn,
                    tenant_id=tenant_id,
                    provider=spec.source,
                    status="ok" if not delete_errors else "error",
                    context={
                        "scope_column": spec.scope_column,
                        "scope_id_hash": _hash_scope_value(row[spec.scope_column]),
                        "refs_seen": len(refs),
                        "refs_deleted": len(deleted_columns),
                        "webhook_cleanup_status": webhook_cleanup_status,
                        "webhook_cleanup_complete": webhook_cleanup_complete,
                        **(
                            {"secret_delete_errors": len(delete_errors)}
                            if delete_errors
                            else {}
                        ),
                    },
                )
                installation = _jsonable_installation(updated, spec)
                installation["refs_seen"] = len(refs)
                installation["refs_deleted"] = len(deleted_columns)
                installation["secret_delete_errors"] = len(delete_errors)
                installation["webhook_deregistration_required"] = (
                    "webhook_secret_ref" in spec.ref_columns
                )
                installation["webhook_provider_row_updated"] = webhook_row_updated
                installation["webhook_cleanup_status"] = webhook_cleanup_status
                installation["webhook_cleanup_complete"] = webhook_cleanup_complete
                installation["native_google_watch_rows_cleared"] = (
                    native_watch_rows_cleared
                )
                return {
                    "ok": True,
                    "action": "uninstall",
                    "tenant_id": str(tenant_id),
                    "source": spec.source,
                    "installation": installation,
                }

    raise DedicatedSourceInstallationCliError(f"unknown command {args.command!r}")


def _column_for_secret_field(spec: InstallationManagementDefinition, field: str) -> str:
    column = SECRET_FIELD_TO_COLUMN[field]
    if column not in spec.ref_columns:
        raise DedicatedSourceInstallationCliError(
            f"{field!r} secret is not supported for source {spec.source!r}"
        )
    return column


def _selector_clause(
    *,
    args: argparse.Namespace,
    spec: InstallationManagementDefinition,
    values: list[Any],
) -> list[str]:
    clauses = ["tenant_id = $1"]
    if args.installation_row_id:
        values.append(_parse_uuid(args.installation_row_id, field="installation_row_id"))
        clauses.append(f"id = ${len(values)}")
    if args.scope_id:
        values.append(args.scope_id)
        clauses.append(f"{spec.scope_column} = ${len(values)}")
    if getattr(args, "region", None):
        selectable_columns = {
            spec.scope_column,
            *spec.extra_output_columns,
            *spec.status_detail_columns,
        }
        if "region" not in selectable_columns:
            raise DedicatedSourceInstallationCliError(
                f"--region is not declared by the {spec.source} "
                "installation contract"
            )
        values.append(args.region)
        clauses.append(f"region = ${len(values)}")
    return clauses


async def _select_installations(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: InstallationManagementDefinition,
    args: argparse.Namespace,
    for_update: bool,
) -> list[asyncpg.Record]:
    values: list[Any] = [tenant_id]
    clauses = _selector_clause(args=args, spec=spec, values=values)
    entity_count_sql = _entity_count_sql(spec)
    projection_sql = _installation_projection_sql(
        spec,
        entity_count_sql=entity_count_sql,
    )
    rows = await conn.fetch(
        f"""
        SELECT {projection_sql}
          FROM {spec.table} i
         WHERE {' AND '.join(f"i.{clause}" for clause in clauses)}
         ORDER BY i.created_at DESC, i.{spec.scope_column}
         {'FOR UPDATE OF i' if for_update else ''}
        """,
        *values,
    )
    return list(rows)


async def _select_single_installation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: InstallationManagementDefinition,
    args: argparse.Namespace,
    required_command: str,
    for_update: bool,
) -> asyncpg.Record:
    rows = await _select_installations(
        conn,
        tenant_id=tenant_id,
        spec=spec,
        args=args,
        for_update=for_update,
    )
    if not rows:
        raise DedicatedSourceInstallationCliError("source installation not found")
    if len(rows) > 1:
        raise DedicatedSourceInstallationCliError(
            f"{required_command} matched multiple installations; narrow the selector"
        )
    return rows[0]


async def _set_disabled_at(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: InstallationManagementDefinition,
    args: argparse.Namespace,
    disabled: bool,
) -> asyncpg.Record:
    values: list[Any] = [tenant_id]
    clauses = _selector_clause(args=args, spec=spec, values=values)
    state_assignment = _state_assignment_sql(spec, disabled=disabled)
    entity_count_sql = _entity_count_subquery_sql(spec)
    projection_sql = _installation_projection_sql(
        spec,
        entity_count_sql=entity_count_sql,
        extra_fields=("selected.disabled_before",),
    )
    row = await conn.fetchrow(
        f"""
        WITH selected AS (
          SELECT id, {_disabled_before_sql(spec)}
            FROM {spec.table}
           WHERE {' AND '.join(clauses)}
           FOR UPDATE
        )
        UPDATE {spec.table} i
           SET {state_assignment}
          FROM selected
         WHERE i.id = selected.id
        RETURNING {projection_sql}
        """,
        *values,
    )
    if row is None:
        raise DedicatedSourceInstallationCliError("source installation not found")
    return row


async def _uninstall_installation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: InstallationManagementDefinition,
    row_id: UUID,
    clear_columns: tuple[str, ...],
) -> asyncpg.Record:
    for column in clear_columns:
        if column not in spec.ref_columns:
            raise DedicatedSourceInstallationCliError(
                f"cannot clear unknown ref column {column!r}"
            )
    assignments = [_state_assignment_sql(spec, disabled=True)]
    assignments.extend(f"{column} = NULL" for column in clear_columns)
    entity_count_sql = _entity_count_subquery_sql(spec)
    projection_sql = _installation_projection_sql(
        spec,
        entity_count_sql=entity_count_sql,
    )
    row = await conn.fetchrow(
        f"""
        UPDATE {spec.table} i
           SET {', '.join(assignments)}
         WHERE i.tenant_id = $1
           AND i.id = $2
        RETURNING {projection_sql}
        """,
        tenant_id,
        row_id,
    )
    if row is None:
        raise DedicatedSourceInstallationCliError("source installation not found")
    return row


async def _set_provider_installation_enabled(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: InstallationManagementDefinition,
    installation_id: str | None,
    enabled: bool,
) -> bool:
    if "webhook_secret_ref" not in spec.ref_columns or not installation_id:
        return False
    status = await conn.execute(
        """
        UPDATE provider_installations
           SET enabled = $1
         WHERE tenant_id = $2
           AND provider = $3
           AND installation_id = $4
        """,
        enabled,
        tenant_id,
        spec.source,
        installation_id,
    )
    return _rows_changed(status) > 0


async def _uninstall_provider_installation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: InstallationManagementDefinition,
    installation_id: str | None,
    clear_secret_ref: bool,
) -> bool:
    if "webhook_secret_ref" not in spec.ref_columns or not installation_id:
        return False
    secret_assignment = "secret_ref = NULL," if clear_secret_ref else ""
    status = await conn.execute(
        f"""
        UPDATE provider_installations
           SET enabled = FALSE,
               {secret_assignment}
               installed_at = installed_at
         WHERE tenant_id = $1
           AND provider = $2
           AND installation_id = $3
        """,
        tenant_id,
        spec.source,
        installation_id,
    )
    return _rows_changed(status) > 0


def _webhook_cleanup_status(
    *,
    spec: InstallationManagementDefinition,
    provider_row_updated: bool,
) -> str:
    if "webhook_secret_ref" not in spec.ref_columns:
        return WEBHOOK_CLEANUP_NOT_APPLICABLE
    if provider_row_updated:
        return WEBHOOK_CLEANUP_LOCAL_RESOLVER_DISABLED
    return WEBHOOK_CLEANUP_PROVIDER_ROW_MISSING


def _webhook_cleanup_complete(status: str) -> bool:
    return status in {
        WEBHOOK_CLEANUP_NOT_APPLICABLE,
        WEBHOOK_CLEANUP_LOCAL_RESOLVER_DISABLED,
    }


async def _clear_native_google_watch_state(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: InstallationManagementDefinition,
    installation_row_id: UUID,
) -> int:
    if (
        not spec.native_google_watch_table
        or spec.entity_table is None
        or spec.entity_install_column is None
    ):
        return 0
    status = await conn.execute(
        f"""
        UPDATE {spec.entity_table}
           SET watch_state = 'inactive',
               watch_channel_id = NULL,
               watch_resource_id = NULL,
               watch_token = NULL,
               watch_expiration = NULL
         WHERE tenant_id = $1
           AND {spec.entity_install_column} = $2
           AND (
                watch_state <> 'inactive'
                OR watch_channel_id IS NOT NULL
                OR watch_resource_id IS NOT NULL
                OR watch_token IS NOT NULL
                OR watch_expiration IS NOT NULL
           )
        """,
        tenant_id,
        installation_row_id,
    )
    return _rows_changed(status)


def _entity_count_sql(spec: InstallationManagementDefinition) -> str:
    if spec.entity_table is None or spec.entity_install_column is None:
        return "0::int"
    return (
        "(SELECT count(*)::int "
        f"FROM {spec.entity_table} e "
        "WHERE e.tenant_id = i.tenant_id "
        f"AND e.{spec.entity_install_column} = i.id)"
    )


def _entity_count_subquery_sql(spec: InstallationManagementDefinition) -> str:
    return _entity_count_sql(spec)


def _installation_projection_sql(
    spec: InstallationManagementDefinition,
    *,
    entity_count_sql: str,
    extra_fields: tuple[str, ...] = (),
) -> str:
    fields = [
        "i.id",
        "i.tenant_id",
        f"i.{spec.scope_column}",
        f"{_base_url_sql(spec)} AS base_url",
        "i.created_at",
        f"{_disabled_state_sql(spec)} AS disabled_at",
    ]
    fields.extend(f"i.{column}" for column in spec.ref_columns)
    fields.extend(f"i.{column}" for column in spec.extra_output_columns)
    fields.extend(extra_fields)
    fields.append(f"{entity_count_sql} AS entity_count")
    return ",\n               ".join(fields)


def _base_url_sql(spec: InstallationManagementDefinition) -> str:
    if spec.base_url_column is None:
        return "NULL::text"
    return f"i.{spec.base_url_column}"


def _disabled_state_sql(spec: InstallationManagementDefinition) -> str:
    if spec.enabled_column is None:
        return "i.disabled_at"
    timestamp = f"i.{spec.updated_at_column}" if spec.updated_at_column else "i.created_at"
    return f"CASE WHEN i.{spec.enabled_column} THEN NULL ELSE {timestamp} END"


def _disabled_before_sql(spec: InstallationManagementDefinition) -> str:
    if spec.enabled_column is None:
        return "disabled_at AS disabled_before"
    timestamp = spec.updated_at_column or "created_at"
    return f"CASE WHEN {spec.enabled_column} THEN NULL ELSE {timestamp} END AS disabled_before"


def _state_assignment_sql(spec: InstallationManagementDefinition, *, disabled: bool) -> str:
    if spec.enabled_column is None:
        return "disabled_at = now()" if disabled else "disabled_at = NULL"
    enabled_value = "FALSE" if disabled else "TRUE"
    assignments = [f"{spec.enabled_column} = {enabled_value}"]
    if spec.updated_at_column:
        assignments.append(f"{spec.updated_at_column} = now()")
    return ", ".join(assignments)


def _provider_installation_id(row: asyncpg.Record, spec: InstallationManagementDefinition) -> str | None:
    column = spec.webhook_installation_id_column
    if column is None:
        return None
    raw = row[column]
    if raw is None:
        return None
    value = str(raw)
    if spec.webhook_installation_id_transform == "host":
        return _host_from_url(value)
    return value


def _host_from_url(value: str) -> str:
    return (
        value.replace("https://", "")
        .replace("http://", "")
        .rstrip("/")
        .split("/")[0]
    )


def _rows_changed(status: str) -> int:
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (IndexError, ValueError):
        return 0


def _refs_from_row(row: asyncpg.Record, spec: InstallationManagementDefinition) -> dict[str, str]:
    refs: dict[str, str] = {}
    for column in spec.ref_columns:
        value = row[column]
        if value:
            refs[column] = str(value)
    return refs


def _read_new_secret(args: argparse.Namespace) -> tuple[bytes, str]:
    if args.new_secret_env:
        if args.new_secret_env not in os.environ:
            raise DedicatedSourceInstallationCliError(
                f"environment variable {args.new_secret_env!r} is not set"
            )
        value: bytes | str = os.environ[args.new_secret_env]
        source = "env"
    elif args.new_secret_file:
        try:
            value = pathlib.Path(args.new_secret_file).read_bytes().rstrip(b"\r\n")
        except OSError as exc:
            raise DedicatedSourceInstallationCliError(
                f"could not read secret file: {exc}"
            ) from exc
        source = "file"
    elif args.new_secret_stdin:
        value = sys.stdin.buffer.read().rstrip(b"\r\n")
        source = "stdin"
    else:
        raise DedicatedSourceInstallationCliError(
            "provide --new-secret-env, --new-secret-file, or --new-secret-stdin"
        )

    secret = value.encode("utf-8") if isinstance(value, str) else value
    if not secret:
        raise DedicatedSourceInstallationCliError("new secret must be non-empty")
    return secret, source


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    action: str,
    resource_id: UUID | None,
    resource_type: str,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
        """,
        uuid7(),
        tenant_id,
        actor_id,
        action,
        resource_type,
        resource_id,
        json.dumps(metadata, default=str, sort_keys=True),
    )


async def _record_installation_audit(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    provider: str,
    status: str,
    context: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO installation_audit_log (
            id, tenant_id, installation_row_id, provider, action, status, context
        ) VALUES ($1, $2, NULL, $3, 'uninstall', $4, $5::jsonb)
        """,
        uuid7(),
        tenant_id,
        provider,
        status,
        json.dumps(context, default=str, sort_keys=True),
    )


def _hash_scope_value(scope_id: Any) -> str | None:
    if scope_id is None:
        return None
    value = str(scope_id)
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _jsonable_installation(row: asyncpg.Record, spec: InstallationManagementDefinition) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(row["id"]),
        "tenant_id": str(row["tenant_id"]),
        "source": spec.source,
        "scope_column": spec.scope_column,
        "scope_id": row[spec.scope_column],
        "base_url": row["base_url"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "disabled_at": (
            row["disabled_at"].isoformat() if row["disabled_at"] else None
        ),
        "enabled": row["disabled_at"] is None,
        "entity_count": int(row["entity_count"]),
    }
    for column in spec.extra_output_columns:
        out[column] = row[column]
    if "disabled_before" in set(row.keys()):
        out["enabled_before"] = row["disabled_before"] is None
    for column in spec.ref_columns:
        out[f"has_{column}"] = row[column] is not None
    return out


async def _amain(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.dsn:
        print(json.dumps({"ok": False, "error": "DATABASE_URL is not set"}))
        return 2
    pool = await asyncpg.create_pool(
        args.dsn,
        min_size=1,
        max_size=2,
        init=_register_codecs,
    )
    try:
        async with pool.acquire() as conn:
            result = await run_command(
                args,
                conn=conn,
                secret_store=build_secret_store(pool),
            )
    except DedicatedSourceInstallationCliError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 2
    finally:
        await pool.close()

    print(json.dumps(result, default=str, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
