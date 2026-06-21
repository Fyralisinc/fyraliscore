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

    assert {
        "memory_lifecycle_ops",
        "relation_claim_ops",
        "relation_frame_ops",
        "resource_ops",
        "new_predictions",
    } <= required
    assert properties["memory_lifecycle_ops"]["type"] == "array"
    assert properties["memory_lifecycle_ops"]["items"]["properties"]["action"]["enum"] == [
        "confirm",
        "falsify",
        "revise",
        "unchanged",
        "archive",
        "supersede",
    ]
    assert properties["relation_claim_ops"]["type"] == "array"
    assert properties["relation_frame_ops"]["type"] == "array"
    assert properties["resource_ops"]["type"] == "array"
    assert properties["new_predictions"]["type"] == "array"
    assert properties["new_predictions"]["items"]["properties"]["op"]["enum"] == ["insert"]


def test_strict_schema_exposes_bounded_relation_frame_contract() -> None:
    frame = RAW_DIFF_STRICT_SCHEMA["properties"]["relation_frame_ops"]["items"]
    participant = frame["properties"]["participants"]["items"]

    assert frame["properties"]["relation_kind"]["enum"] == ["blocked_workstream"]
    assert frame["properties"]["write_policy"]["enum"] == [
        "project_edges",
        "candidate",
        "needs_review",
        "no_projection",
    ]
    assert {
        "blocker",
        "blocked_work",
        "owner",
        "downstream_risk",
        "possible_resolution",
    } <= set(participant["properties"]["role"]["enum"])


def test_scope_entities_allow_candidate_ref_types_with_uuid_ids() -> None:
    claim_insert = RAW_DIFF_STRICT_SCHEMA["properties"]["claim_ops"]["items"]
    scope_entity = claim_insert["properties"]["entry"]["properties"][
        "scope_entities"
    ]["items"]

    assert scope_entity["properties"]["type"] == {"type": "string"}
    assert "enum" not in scope_entity["properties"]["type"]
    assert scope_entity["properties"]["id"]["pattern"].startswith("^")


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
