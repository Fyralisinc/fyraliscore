from __future__ import annotations

import json

from services.platform.runtime.byoc_control_panel_contract import (
    SCHEMA_BUNDLE_VERSION,
    build_example_control_panel_state,
    model_json_schema_bundle,
    render_control_panel_schema_bundle_json,
    render_control_panel_state_example_json,
)


def test_control_panel_contract_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()
    rendered = render_control_panel_schema_bundle_json()

    assert bundle["schema_version"] == SCHEMA_BUNDLE_VERSION
    assert bundle["query"]["properties"]["recent_limit"]["maximum"] == 20
    assert bundle["control_panel_state"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.control_panel_state.v1"
    )
    assert bundle["stored_scope"] == "sanitized_control_panel_metadata_only"
    assert json.loads(rendered) == bundle


def test_control_panel_contract_example_is_sanitized_metadata_only() -> None:
    state = build_example_control_panel_state()
    rendered = render_control_panel_state_example_json(state)
    payload = json.loads(rendered)

    assert state.schema_version == "fyralis.byoc.control_panel_state.v1"
    assert state.deployment_id == "dep_control01"
    assert state.customer_id == "cus_control01"
    assert state.stored_scope == "sanitized_control_panel_metadata_only"
    assert payload["overview"]["metadata_sources"] == [
        "agent_fleet",
        "evidence_package_receipts",
        "preflight_report_receipts",
        "runner_evidence_receipts",
    ]
    assert {section["key"] for section in payload["sections"]} == {
        "deployment_overview",
        "agent_fleet",
        "evidence_packages",
        "preflight_reports",
        "runner_evidence",
    }
    assert payload["agent_fleet"]["result_count"] == 1
    assert payload["evidence_packages"]["result_count"] == 1
    assert payload["preflight_reports"]["result_count"] == 1
    assert payload["runner_evidence"]["result_count"] == 1
    assert "install_token" not in rendered.lower()
    assert "secret_ref" not in rendered.lower()
    assert "signature" not in rendered.lower()
    assert "authorization" not in rendered.lower()
    assert "bearer " not in rendered.lower()
    assert "token=" not in rendered.lower()
    assert "postgresql://" not in rendered.lower()
    assert "arn:aws" not in rendered.lower()
    assert '"preflight_report":' not in rendered
    assert '"checks":' not in rendered
