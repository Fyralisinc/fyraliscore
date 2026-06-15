from __future__ import annotations

from services.reasoning.think.strict_schema import RAW_DIFF_STRICT_SCHEMA


def test_strict_schema_uses_registered_edge_kind_vocabulary() -> None:
    edge_kind_schema = (
        RAW_DIFF_STRICT_SCHEMA["properties"]["edge_ops"]["items"]
        ["properties"]["edge_kind"]
    )

    assert edge_kind_schema["type"] == "string"
    assert "pattern" not in edge_kind_schema
    assert {
        "supports",
        "contradicts",
        "blocks",
        "early_warning_for",
    } <= set(edge_kind_schema["enum"])


def test_strict_schema_exposes_resource_ops_and_new_predictions() -> None:
    required = set(RAW_DIFF_STRICT_SCHEMA["required"])
    properties = RAW_DIFF_STRICT_SCHEMA["properties"]

    assert {"resource_ops", "new_predictions"} <= required
    assert properties["resource_ops"]["type"] == "array"
    assert properties["new_predictions"]["type"] == "array"
    assert properties["new_predictions"]["items"]["properties"]["op"]["enum"] == ["insert"]


def test_strict_resource_op_covers_applier_fields() -> None:
    resource_op = RAW_DIFF_STRICT_SCHEMA["properties"]["resource_ops"]["items"]
    props = resource_op["properties"]

    assert set(resource_op["required"]) == {
        "op",
        "resource_id",
        "commitment_id",
        "payload",
        "patch",
        "kind",
        "delta",
        "quantity",
        "actual_quantity",
    }
    assert set(props["op"]["enum"]) == {
        "create",
        "transaction",
        "deploy",
        "release",
        "update",
    }
    assert {"acquire", "spend", "weaken", "expire"} <= set(
        props["kind"]["anyOf"][0]["enum"]
    )
