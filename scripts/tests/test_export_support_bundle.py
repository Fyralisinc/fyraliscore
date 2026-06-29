from __future__ import annotations

import json

import asyncpg
import pytest

from lib.shared.ids import uuid7
from scripts.export_support_bundle import export_support_bundle
from scripts.tests.conftest import insert_actor
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_export_support_bundle_omits_raw_payload_fields(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        actor_id = await insert_actor(conn, tenant, "Support operator")
        target_actor_id = await insert_actor(conn, tenant, "Customer admin")
        await grant_role(
            target_actor_id,
            "tenant",
            None,
            "admin",
            actor_id,
            conn=conn,
            tenant_id=tenant,
        )
        await conn.execute(
            """
            INSERT INTO operator_action_log (
                id, tenant_id, actor_id, action, resource_type, resource_id,
                metadata, occurred_at
            )
            VALUES ($1, $2, $3, 'role.grant', 'actor_role', $4, $5::jsonb, now())
            """,
            uuid7(),
            tenant,
            actor_id,
            target_actor_id,
            json.dumps(
                {
                    "role": "admin",
                    "raw_token": "must-not-appear",
                    "prompt": "must-not-appear",
                }
            ),
        )
        await conn.execute(
            """
            INSERT INTO ingestion_failures (
                id, tenant_id, source, failure_kind, raw_s3_key, error_summary,
                error_context, attempt_count, first_seen_at, last_seen_at
            )
            VALUES (
                $1, $2, 'slack', 'normalizer_parse_error',
                's3://private-bucket/customer/object-key-must-not-appear.json',
                'raw payload must-not-appear',
                $3::jsonb, 3, now(), now()
            )
            """,
            uuid7(),
            tenant,
            json.dumps({"access_token": "must-not-appear"}),
        )
        await conn.execute(
            """
            INSERT INTO backup_recovery_status (
                component, check_name, status, freshness_slo_seconds,
                details, last_success_at, last_attempt_at
            )
            VALUES (
                'postgres', 'backup', 'ok', 129600,
                '{"provider":"test"}'::jsonb, now(), now()
            )
            ON CONFLICT (component, check_name)
            DO UPDATE SET status = EXCLUDED.status,
                          freshness_slo_seconds = EXCLUDED.freshness_slo_seconds,
                          details = EXCLUDED.details,
                          last_success_at = EXCLUDED.last_success_at,
                          last_attempt_at = EXCLUDED.last_attempt_at
            """
        )

        bundle = await export_support_bundle(
            conn,
            tenant_id=tenant,
            operator_actor_id=target_actor_id,
            window_hours=24,
            deployment_sha="abc123",
        )
        audit_row = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'support_bundle.export'
            """,
            tenant,
        )
        await conn.execute(
            """
            DELETE FROM backup_recovery_status
            WHERE component = 'postgres' AND check_name = 'backup'
            """
        )

    assert audit_row is not None
    assert audit_row["actor_id"] == target_actor_id
    assert audit_row["resource_type"] == "support_bundle"
    assert audit_row["resource_id"] is None
    audit_metadata = audit_row["metadata"]
    if isinstance(audit_metadata, str):
        audit_metadata = json.loads(audit_metadata)
    assert audit_metadata == {
        "deployment_sha_present": True,
        "privacy_contract": {
            "contains_raw_payloads": False,
            "contains_object_keys": False,
            "contains_prompts_or_completions": False,
            "contains_tokens_or_secrets": False,
            "contains_operator_metadata_blobs": False,
        },
        "schema_version": 1,
        "window_hours": 24,
    }

    encoded = json.dumps(bundle, sort_keys=True)
    assert bundle["tenant"]["id"] == str(tenant)
    assert bundle["deployment"]["sha"] == "abc123"
    assert bundle["privacy_contract"] == {
        "contains_raw_payloads": False,
        "contains_object_keys": False,
        "contains_prompts_or_completions": False,
        "contains_tokens_or_secrets": False,
        "contains_operator_metadata_blobs": False,
    }
    assert bundle["actor_role_counts"] == [
        {
            "active_count": 1,
            "entity_type": "tenant",
            "revoked_count": 0,
            "role": "admin",
        }
    ]
    assert bundle["operator_action_counts"] == [
        {
            "action": "role.grant",
            "count": 1,
            "last_seen_at": bundle["operator_action_counts"][0]["last_seen_at"],
            "resource_type": "actor_role",
        }
    ]
    assert bundle["ingestion_failure_counts"] == [
        {
            "count": 1,
            "failure_kind": "normalizer_parse_error",
            "last_seen_at": bundle["ingestion_failure_counts"][0]["last_seen_at"],
            "max_attempt_count": 3,
            "source": "slack",
            "state": "open",
        }
    ]
    assert "must-not-appear" not in encoded
    assert "object-key-must-not-appear" not in encoded
    assert "raw payload" not in encoded
