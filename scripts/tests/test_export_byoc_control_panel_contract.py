from __future__ import annotations

import json
from pathlib import Path

from scripts.export_byoc_control_panel_contract import main


def test_export_byoc_control_panel_contract_prints_example(capsys) -> None:
    code = main(["--example"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.control_panel_state.v1"
    assert payload["stored_scope"] == "sanitized_control_panel_metadata_only"
    assert payload["deployment_id"] == "dep_control01"
    assert "install_token" not in rendered.lower()
    assert "secret_ref" not in rendered.lower()
    assert "signature" not in rendered.lower()
    assert captured.err == ""


def test_export_byoc_control_panel_contract_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == (
        "fyralis.byoc.control_panel_contract_bundle.v1"
    )
    assert payload["control_panel_state"]["properties"]["schema_version"][
        "const"
    ] == "fyralis.byoc.control_panel_state.v1"
    assert payload["query"]["properties"]["recent_limit"]["maximum"] == 20


def test_export_byoc_control_panel_contract_prints_access_schema(capsys) -> None:
    code = main(["--access-schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == (
        "fyralis.byoc.control_panel_access_bundle.v1"
    )
    assert payload["grant"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.control_panel_access_grant.v1"
    )
    assert payload["decision"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.control_panel_access_decision.v1"
    )
    assert payload["stored_scope"] == "sanitized_control_panel_access_metadata_only"


def test_export_byoc_control_panel_contract_writes_output(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "control-panel-state.example.json"

    code = main(["--example", "--output", str(output)])

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.control_panel_state.v1"
    assert payload["stored_scope"] == "sanitized_control_panel_metadata_only"
