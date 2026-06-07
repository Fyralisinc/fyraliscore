from __future__ import annotations

from services.think.strict_schema import RAW_DIFF_STRICT_SCHEMA


def test_strict_schema_allows_dynamic_edge_kind_shape() -> None:
    edge_kind_schema = (
        RAW_DIFF_STRICT_SCHEMA["properties"]["edge_ops"]["items"]
        ["properties"]["edge_kind"]
    )

    assert edge_kind_schema == {
        "type": "string",
        "pattern": "^[a-z][a-z0-9_]{2,63}$",
    }
    assert "enum" not in edge_kind_schema
