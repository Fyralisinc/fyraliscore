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
from dataclasses import dataclass
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
from services.platform.operator_auth import require_tenant_operator  # noqa: E402


class DedicatedSourceInstallationCliError(ValueError):
    """Operator-facing validation error."""


@dataclass(frozen=True, slots=True)
class SourceSpec:
    source: str
    table: str
    scope_column: str
    ref_columns: tuple[str, ...]
    entity_table: str
    entity_install_column: str


SPECS: dict[str, SourceSpec] = {
    "quickbooks": SourceSpec(
        source="quickbooks",
        table="quickbooks_installations",
        scope_column="realm_id",
        ref_columns=("secret_ref", "refresh_secret_ref", "webhook_secret_ref"),
        entity_table="quickbooks_entities",
        entity_install_column="quickbooks_installation_id",
    ),
    "gusto": SourceSpec(
        source="gusto",
        table="gusto_installations",
        scope_column="company_uuid",
        ref_columns=("secret_ref", "refresh_secret_ref", "webhook_secret_ref"),
        entity_table="gusto_entities",
        entity_install_column="gusto_installation_id",
    ),
    "ramp": SourceSpec(
        source="ramp",
        table="ramp_installations",
        scope_column="business_id",
        ref_columns=("secret_ref", "refresh_secret_ref", "webhook_secret_ref"),
        entity_table="ramp_entities",
        entity_install_column="ramp_installation_id",
    ),
    "carta": SourceSpec(
        source="carta",
        table="carta_installations",
        scope_column="firm_id",
        ref_columns=("secret_ref", "refresh_secret_ref"),
        entity_table="carta_entities",
        entity_install_column="carta_installation_id",
    ),
    "linkedin": SourceSpec(
        source="linkedin",
        table="linkedin_installations",
        scope_column="organization_urn",
        ref_columns=("secret_ref", "refresh_secret_ref"),
        entity_table="linkedin_entities",
        entity_install_column="linkedin_installation_id",
    ),
}

SECRET_FIELD_TO_COLUMN = {
    "access": "secret_ref",
    "refresh": "refresh_secret_ref",
    "webhook": "webhook_secret_ref",
}


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
        choices=tuple(sorted(SPECS)),
        required=True,
        help="Dedicated source table to manage.",
    )


def _add_selector_args(parser: argparse.ArgumentParser, *, required: bool) -> None:
    selector = parser.add_mutually_exclusive_group(required=required)
    selector.add_argument("--installation-row-id", help="Dedicated install row UUID.")
    selector.add_argument(
        "--scope-id",
        help="Source-native scope id, for example realm_id or organization_urn.",
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
    spec = SPECS[args.source]

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
                            _hash_scope(args.scope_id) if args.scope_id else None
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
                    scope_id=row[spec.scope_column],
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
                        "scope_id_hash": _hash_scope(row[spec.scope_column]),
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
                        "scope_id_hash": _hash_scope(row[spec.scope_column]),
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
                if not args.dry_run and secret_store is None:
                    raise DedicatedSourceInstallationCliError(
                        "uninstall requires a configured secret store unless "
                        "--dry-run is set"
                    )
                row = await _select_single_installation(
                    conn,
                    tenant_id=tenant_id,
                    spec=spec,
                    args=args,
                    required_command="uninstall",
                    for_update=True,
                )
                refs = _refs_from_row(row, spec)
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
                    scope_id=row[spec.scope_column],
                    clear_secret_ref="webhook_secret_ref" in deleted_columns,
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
                        "scope_id_hash": _hash_scope(row[spec.scope_column]),
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
                        "scope_id_hash": _hash_scope(row[spec.scope_column]),
                        "refs_seen": len(refs),
                        "refs_deleted": len(deleted_columns),
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
                return {
                    "ok": True,
                    "action": "uninstall",
                    "tenant_id": str(tenant_id),
                    "source": spec.source,
                    "installation": installation,
                }

    raise DedicatedSourceInstallationCliError(f"unknown command {args.command!r}")


def _column_for_secret_field(spec: SourceSpec, field: str) -> str:
    column = SECRET_FIELD_TO_COLUMN[field]
    if column not in spec.ref_columns:
        raise DedicatedSourceInstallationCliError(
            f"{field!r} secret is not supported for source {spec.source!r}"
        )
    return column


def _selector_clause(
    *,
    args: argparse.Namespace,
    spec: SourceSpec,
    values: list[Any],
) -> list[str]:
    clauses = ["tenant_id = $1"]
    if args.installation_row_id:
        values.append(_parse_uuid(args.installation_row_id, field="installation_row_id"))
        clauses.append(f"id = ${len(values)}")
    if args.scope_id:
        values.append(args.scope_id)
        clauses.append(f"{spec.scope_column} = ${len(values)}")
    return clauses


async def _select_installations(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: SourceSpec,
    args: argparse.Namespace,
    for_update: bool,
) -> list[asyncpg.Record]:
    values: list[Any] = [tenant_id]
    clauses = _selector_clause(args=args, spec=spec, values=values)
    rows = await conn.fetch(
        f"""
        SELECT i.id, i.tenant_id, i.{spec.scope_column}, i.base_url,
               i.created_at, i.disabled_at,
               {', '.join(f'i.{column}' for column in spec.ref_columns)},
               COALESCE(entity_counts.entity_count, 0)::int AS entity_count
          FROM {spec.table} i
          LEFT JOIN LATERAL (
              SELECT count(*) AS entity_count
                FROM {spec.entity_table} e
               WHERE e.tenant_id = i.tenant_id
                 AND e.{spec.entity_install_column} = i.id
          ) entity_counts ON TRUE
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
    spec: SourceSpec,
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
    spec: SourceSpec,
    args: argparse.Namespace,
    disabled: bool,
) -> asyncpg.Record:
    values: list[Any] = [tenant_id]
    clauses = _selector_clause(args=args, spec=spec, values=values)
    disabled_expression = "now()" if disabled else "NULL"
    row = await conn.fetchrow(
        f"""
        WITH selected AS (
          SELECT id, disabled_at AS disabled_before
            FROM {spec.table}
           WHERE {' AND '.join(clauses)}
           FOR UPDATE
        )
        UPDATE {spec.table} i
           SET disabled_at = {disabled_expression}
          FROM selected
         WHERE i.id = selected.id
        RETURNING i.id, i.tenant_id, i.{spec.scope_column}, i.base_url,
                  i.created_at, i.disabled_at,
                  {', '.join(f'i.{column}' for column in spec.ref_columns)},
                  selected.disabled_before,
                  (
                      SELECT count(*)::int
                        FROM {spec.entity_table} e
                       WHERE e.tenant_id = i.tenant_id
                         AND e.{spec.entity_install_column} = i.id
                  ) AS entity_count
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
    spec: SourceSpec,
    row_id: UUID,
    clear_columns: tuple[str, ...],
) -> asyncpg.Record:
    for column in clear_columns:
        if column not in spec.ref_columns:
            raise DedicatedSourceInstallationCliError(
                f"cannot clear unknown ref column {column!r}"
            )
    assignments = ["disabled_at = now()"]
    assignments.extend(f"{column} = NULL" for column in clear_columns)
    row = await conn.fetchrow(
        f"""
        UPDATE {spec.table} i
           SET {', '.join(assignments)}
         WHERE i.tenant_id = $1
           AND i.id = $2
        RETURNING i.id, i.tenant_id, i.{spec.scope_column}, i.base_url,
                  i.created_at, i.disabled_at,
                  {', '.join(f'i.{column}' for column in spec.ref_columns)},
                  (
                      SELECT count(*)::int
                        FROM {spec.entity_table} e
                       WHERE e.tenant_id = i.tenant_id
                         AND e.{spec.entity_install_column} = i.id
                  ) AS entity_count
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
    spec: SourceSpec,
    scope_id: str,
    enabled: bool,
) -> bool:
    if "webhook_secret_ref" not in spec.ref_columns:
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
        scope_id,
    )
    return _rows_changed(status) > 0


async def _uninstall_provider_installation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    spec: SourceSpec,
    scope_id: str,
    clear_secret_ref: bool,
) -> bool:
    if "webhook_secret_ref" not in spec.ref_columns:
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
        scope_id,
    )
    return _rows_changed(status) > 0


def _rows_changed(status: str) -> int:
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (IndexError, ValueError):
        return 0


def _refs_from_row(row: asyncpg.Record, spec: SourceSpec) -> dict[str, str]:
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


def _hash_scope(scope_id: str) -> str:
    return hashlib.sha256(scope_id.encode("utf-8")).hexdigest()[:16]


def _jsonable_installation(row: asyncpg.Record, spec: SourceSpec) -> dict[str, Any]:
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
