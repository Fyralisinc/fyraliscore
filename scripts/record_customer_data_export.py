#!/usr/bin/env python3
"""Record a bounded customer data export audit entry.

This command does not generate or move export data. It records the approved
tenant-scoped export metadata that must survive support tickets, staging object
cleanup, and retention windows.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import string
import sys
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.shared.ids import uuid7  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.platform.operator_auth import require_tenant_operator  # noqa: E402


DATA_CLASSES = frozenset(
    (
        "account_metadata",
        "audit_trails",
        "generated_reasoning",
        "large_objects",
        "metrics",
        "raw_ingestion_objects",
        "substrate",
    )
)
PURPOSES = frozenset(
    (
        "contractual_export",
        "customer_request",
        "legal_hold",
        "migration",
        "security_review",
    )
)
DESTINATION_BOUNDARIES = frozenset(
    (
        "byoc_environment",
        "customer_approved_storage",
        "customer_boundary",
    )
)
_REFERENCE_CHARS = set(string.ascii_letters + string.digits + "._:-/")


class CustomerDataExportAuditError(ValueError):
    """Operator-facing validation error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record sanitized customer data export audit metadata.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    parser.add_argument("--tenant", required=True, help="Tenant UUID.")
    parser.add_argument(
        "--operator-actor",
        required=True,
        help="Actor UUID performing this operator action.",
    )
    parser.add_argument(
        "--approver-actor",
        required=True,
        help="Actor UUID that approved the export scope.",
    )
    parser.add_argument(
        "--export-reference",
        required=True,
        help="Non-secret support/compliance reference for this export.",
    )
    parser.add_argument(
        "--data-class",
        action="append",
        choices=sorted(DATA_CLASSES),
        required=True,
        help="Data class included in the export. May be supplied multiple times.",
    )
    parser.add_argument(
        "--purpose",
        choices=sorted(PURPOSES),
        required=True,
        help="Approved export purpose.",
    )
    parser.add_argument(
        "--destination-boundary",
        choices=sorted(DESTINATION_BOUNDARIES),
        required=True,
        help="Approved security boundary receiving the export.",
    )
    parser.add_argument(
        "--window-start",
        required=True,
        help="Inclusive export window start, ISO-8601 with timezone.",
    )
    parser.add_argument(
        "--window-end",
        required=True,
        help="Inclusive export window end, ISO-8601 with timezone.",
    )
    parser.add_argument(
        "--manifest-sha256",
        help="Optional SHA-256 digest of the final export manifest.",
    )
    parser.add_argument(
        "--encrypted",
        action="store_true",
        help="Required attestation that the export was encrypted.",
    )
    parser.add_argument(
        "--temporary-staging-deleted",
        action="store_true",
        help="Required attestation that temporary staging objects were deleted.",
    )
    return parser


async def run_command(
    args: argparse.Namespace,
    *,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    tenant_id = _parse_uuid(args.tenant, field="tenant")
    operator_actor_id = _parse_uuid(args.operator_actor, field="operator_actor")
    approver_actor_id = _parse_uuid(args.approver_actor, field="approver_actor")
    window_start = _parse_timestamp(args.window_start, field="window_start")
    window_end = _parse_timestamp(args.window_end, field="window_end")
    if window_start > window_end:
        raise CustomerDataExportAuditError("window_start must be before window_end")

    data_classes = sorted(set(args.data_class or ()))
    if not data_classes:
        raise CustomerDataExportAuditError("at least one data class is required")

    if not args.encrypted:
        raise CustomerDataExportAuditError("--encrypted attestation is required")
    if not args.temporary_staging_deleted:
        raise CustomerDataExportAuditError(
            "--temporary-staging-deleted attestation is required"
        )

    export_reference = _bounded_reference(args.export_reference)
    manifest_sha256 = _validate_manifest_sha256(args.manifest_sha256)

    await require_tenant_operator(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_actor_id,
        error_type=CustomerDataExportAuditError,
    )

    metadata = {
        "export_reference": export_reference,
        "data_classes": data_classes,
        "purpose": args.purpose,
        "destination_boundary": args.destination_boundary,
        "approver_actor_id": str(approver_actor_id),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "encrypted": True,
        "temporary_staging_deleted": True,
        "manifest_sha256_present": manifest_sha256 is not None,
        "privacy_contract": {
            "contains_payloads": False,
            "contains_object_keys": False,
            "contains_tokens_or_secrets": False,
        },
    }
    await _record_operator_action(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_actor_id,
        metadata=metadata,
    )
    return {
        "ok": True,
        "action": "customer_data_export.record",
        "tenant_id": str(tenant_id),
        "export_reference": export_reference,
        "data_classes": data_classes,
        "manifest_sha256_present": manifest_sha256 is not None,
    }


def _parse_uuid(value: str | None, *, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise CustomerDataExportAuditError(f"{field} must be a UUID") from exc


def _parse_timestamp(value: str, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CustomerDataExportAuditError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise CustomerDataExportAuditError(f"{field} must include a timezone")
    return parsed


def _bounded_reference(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise CustomerDataExportAuditError("export_reference is required")
    if len(text) > 120 or any(char not in _REFERENCE_CHARS for char in text):
        raise CustomerDataExportAuditError(
            "export_reference must be 1-120 chars of letters, digits, . _ : - or /"
        )
    return text


def _validate_manifest_sha256(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().casefold()
    if len(text) != 64 or any(char not in string.hexdigits for char in text):
        raise CustomerDataExportAuditError(
            "manifest_sha256 must be a SHA-256 hex digest"
        )
    return text


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES (
            $1, $2, $3, 'customer_data_export.record',
            'customer_data_export', NULL, $4::jsonb, now()
        )
        """,
        uuid7(),
        tenant_id,
        actor_id,
        json.dumps(metadata, default=str, sort_keys=True),
    )


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
        async with conn.transaction():
            result = await run_command(args, conn=conn)
    finally:
        await conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except CustomerDataExportAuditError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
