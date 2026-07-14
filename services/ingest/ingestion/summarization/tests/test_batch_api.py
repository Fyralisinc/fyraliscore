"""Unit tests for the OpenAI Batch API request-line builder.

Layer 0 (Phase 0) changed ``DocumentSummarySchema.action_items`` from
``list[str]`` to ``list[ActionItem]{who, what, due}``, so the JSON schema the
batch lane sends to the OpenAI Batch API (``text.format`` of type
``json_schema``) is now NESTED — ``action_items`` is an array of a ``$ref`` into
``$defs/ActionItem``. These tests assert the request line carries a well-formed
nested structured-output schema (Task #3 decision: nested is tolerated; the body
sets ``strict: False`` which permits ``$defs``/``$ref`` and nested objects). No
incompatibility, so no flag/guard — just a regression lock on the wire shape.
"""
from __future__ import annotations

import orjson

from services.ingest.ingestion.summarization.batch_api import (
    BATCH_ENDPOINT,
    build_batch_request_line,
)


def _line_body(**kwargs) -> dict:
    line = build_batch_request_line(**kwargs)
    obj = orjson.loads(line)
    assert obj["custom_id"] == kwargs["custom_id"]
    assert obj["method"] == "POST"
    assert obj["url"] == BATCH_ENDPOINT
    return obj["body"]


def test_batch_request_line_carries_nested_json_schema():
    body = _line_body(
        custom_id="obs-1",
        source_text="Acme sync. Priya to send the SOW by 2026-06-17. SOC2 at risk.",
        metadata={"source_channel": "fireflies:transcript"},
    )
    fmt = body["text"]["format"]
    assert fmt["type"] == "json_schema"
    # Nested object schemas require non-strict mode; the builder sets it.
    assert fmt["strict"] is False
    schema = fmt["schema"]

    # Top-level schema is the DocumentSummarySchema object.
    assert schema["type"] == "object"
    props = schema["properties"]
    assert set(props) >= {"summary", "key_points", "decisions", "action_items", "risks"}

    # action_items is the NESTED part: an array whose items $ref the ActionItem
    # definition (this is what the list[str] -> list[ActionItem] change produced).
    action_items = props["action_items"]
    assert action_items["type"] == "array"
    ref = action_items["items"]["$ref"]
    assert ref == "#/$defs/ActionItem"

    # The referenced definition is itself a well-formed object schema with the
    # commitment fields the document-memory substrate consumes.
    defs = schema["$defs"]
    assert "ActionItem" in defs
    action_item = defs["ActionItem"]
    assert action_item["type"] == "object"
    assert set(action_item["properties"]) == {"who", "what", "due"}
    # `what` is required; `who`/`due` are optional (nullable).
    assert action_item["required"] == ["what"]
    assert action_item["properties"]["what"]["type"] == "string"


def test_batch_request_line_is_valid_json_and_resolvable_schema():
    body = _line_body(
        custom_id="obs-2",
        source_text="A short note.",
        metadata={"source_channel": "google_drive:file"},
    )
    schema = body["text"]["format"]["schema"]
    # Every $ref under the schema must resolve into $defs (no dangling refs the
    # Batch API would reject).
    defs = schema.get("$defs", {})

    def _refs(node):
        if isinstance(node, dict):
            if "$ref" in node and isinstance(node["$ref"], str):
                yield node["$ref"]
            for v in node.values():
                yield from _refs(v)
        elif isinstance(node, list):
            for v in node:
                yield from _refs(v)

    for ref in _refs(schema):
        assert ref.startswith("#/$defs/"), ref
        assert ref.split("/")[-1] in defs, ref

    # The whole body round-trips through orjson (i.e. it is JSON-serializable as
    # the submitter requires).
    assert orjson.loads(orjson.dumps(body)) == body
