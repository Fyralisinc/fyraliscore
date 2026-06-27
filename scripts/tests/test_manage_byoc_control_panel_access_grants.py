from __future__ import annotations

import json

from scripts.manage_byoc_control_panel_access_grants import main


TENANT_ID = "11111111-1111-4111-8111-111111111111"
CUSTOMER_ID = "cus_admin01"
DEPLOYMENT_ID = "dep_admin01"


def test_manage_control_panel_access_schema_exports_bundle(capsys) -> None:
    result = main(["schema"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "fyralis.byoc.control_panel_access_bundle.v1"
    assert payload["stored_scope"] == "sanitized_control_panel_access_metadata_only"


def test_manage_control_panel_access_upsert_dry_run_prints_sanitized_grant(
    capsys,
) -> None:
    result = main(
        [
            "upsert",
            "--tenant-id",
            TENANT_ID,
            "--customer-id",
            CUSTOMER_ID,
            "--deployment-id",
            DEPLOYMENT_ID,
            "--role",
            "operator",
            "--granted-at",
            "2026-06-27T12:00:00Z",
            "--expires-at",
            "2026-07-27T12:00:00Z",
            "--dry-run",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    rendered = json.dumps(payload).lower()
    assert payload["action"] == "upsert"
    assert payload["dry_run"] is True
    assert payload["grant"]["tenant_id"] == TENANT_ID
    assert payload["grant"]["customer_id"] == CUSTOMER_ID
    assert payload["grant"]["deployment_ids"] == [DEPLOYMENT_ID]
    assert payload["grant"]["role"] == "operator"
    assert "read_key" not in rendered
    assert "endpoint_url" not in rendered
    assert "payload" not in rendered
    assert "secret" not in rendered


def test_manage_control_panel_access_revoke_dry_run_prints_target(capsys) -> None:
    result = main(
        [
            "revoke",
            "--tenant-id",
            TENANT_ID,
            "--customer-id",
            CUSTOMER_ID,
            "--deployment-id",
            DEPLOYMENT_ID,
            "--dry-run",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["action"] == "revoke"
    assert payload["target"]["tenant_id"] == TENANT_ID
    assert payload["target"]["customer_id"] == CUSTOMER_ID
    assert payload["target"]["deployment_id"] == DEPLOYMENT_ID


def test_manage_control_panel_access_list_requires_dsn(monkeypatch, capsys) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    result = main(["list", "--tenant-id", TENANT_ID])

    assert result == 2
    captured = capsys.readouterr()
    assert "--dsn or DATABASE_URL is required" in captured.err
