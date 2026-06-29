from __future__ import annotations

import argparse
import json

import asyncpg
import pytest

from scripts.record_customer_data_export import (
    CustomerDataExportAuditError,
    build_parser,
    run_command,
)
from scripts.tests.conftest import insert_actor
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _metadata(row: asyncpg.Record) -> dict[str, object]:
    value = row["metadata"]
    return json.loads(value) if isinstance(value, str) else value


@pytest.mark.asyncio
async def test_record_customer_data_export_writes_bounded_operator_audit(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Export operator")
        approver_actor = await insert_actor(conn, tenant, "Export approver")
        await grant_role(
            operator_actor,
            "tenant",
            None,
            "admin",
            approver_actor,
            conn=conn,
            tenant_id=tenant,
        )

        result = await run_command(
            _parse(
                [
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--approver-actor",
                    str(approver_actor),
                    "--export-reference",
                    "SEC-123/export-1",
                    "--data-class",
                    "substrate",
                    "--data-class",
                    "audit_trails",
                    "--purpose",
                    "customer_request",
                    "--destination-boundary",
                    "customer_boundary",
                    "--window-start",
                    "2026-06-01T00:00:00+00:00",
                    "--window-end",
                    "2026-06-25T00:00:00+00:00",
                    "--manifest-sha256",
                    "a" * 64,
                    "--encrypted",
                    "--temporary-staging-deleted",
                ]
            ),
            conn=conn,
        )

        audit_row = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'customer_data_export.record'
            """,
            tenant,
        )

    assert result == {
        "ok": True,
        "action": "customer_data_export.record",
        "tenant_id": str(tenant),
        "export_reference": "SEC-123/export-1",
        "data_classes": ["audit_trails", "substrate"],
        "manifest_sha256_present": True,
    }
    assert audit_row is not None
    assert audit_row["actor_id"] == operator_actor
    assert audit_row["resource_type"] == "customer_data_export"
    assert audit_row["resource_id"] is None
    metadata = _metadata(audit_row)
    assert metadata == {
        "approver_actor_id": str(approver_actor),
        "data_classes": ["audit_trails", "substrate"],
        "destination_boundary": "customer_boundary",
        "encrypted": True,
        "export_reference": "SEC-123/export-1",
        "manifest_sha256_present": True,
        "privacy_contract": {
            "contains_object_keys": False,
            "contains_payloads": False,
            "contains_tokens_or_secrets": False,
        },
        "purpose": "customer_request",
        "temporary_staging_deleted": True,
        "window_end": "2026-06-25T00:00:00+00:00",
        "window_start": "2026-06-01T00:00:00+00:00",
    }
    encoded = json.dumps(metadata, sort_keys=True)
    assert "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" not in encoded


@pytest.mark.asyncio
async def test_record_customer_data_export_requires_completion_attestations(
    tenant,
) -> None:
    with pytest.raises(CustomerDataExportAuditError, match="encrypted"):
        await run_command(
            _parse(
                [
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(tenant),
                    "--approver-actor",
                    str(tenant),
                    "--export-reference",
                    "SEC-123/export-1",
                    "--data-class",
                    "substrate",
                    "--purpose",
                    "customer_request",
                    "--destination-boundary",
                    "customer_boundary",
                    "--window-start",
                    "2026-06-01T00:00:00+00:00",
                    "--window-end",
                    "2026-06-25T00:00:00+00:00",
                ]
            ),
            conn=object(),  # type: ignore[arg-type]
        )
