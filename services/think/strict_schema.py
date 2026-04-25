"""Strict-mode JSON schema for DeepSeek tool-calling output.

DeepSeek strict mode requires: every object property listed in `required`,
`additionalProperties: false` everywhere, no `Any`-typed fields, and only
these JSON-schema features: object, string, number, integer, boolean,
array, enum, anyOf, const.

This schema is a deliberate SUBSET of `RawDiff`: it only constrains
`claim_ops` (which is what our tests measure). `act_ops`, `resource_ops`,
and `new_predictions` are omitted from the schema — Pydantic defaults
them to empty lists at parse time. Acts/resource generation can be added
back when specific shapes need to be enforced.
"""
from __future__ import annotations


_UUID_PATTERN = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
_UUID_STR = {"type": "string", "pattern": _UUID_PATTERN}


def _proposition_variant(kind: str, fields: list[str]) -> dict:
    """One concrete proposition kind as a strict object."""
    properties: dict = {"kind": {"type": "string", "enum": [kind]}}
    for f in fields:
        properties[f] = {"type": "string"}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", *fields],
        "properties": properties,
    }


_PROPOSITION_KINDS: list[dict] = [
    _proposition_variant("state",                 ["subject", "assertion"]),
    _proposition_variant("relation",              ["subject", "relation", "object"]),
    _proposition_variant("prediction",            ["expected", "resolution"]),
    _proposition_variant("pattern",               ["signature", "observed_tendency", "trigger_conditions"]),
    _proposition_variant("pattern_instance",      ["pattern_id", "matched_context"]),
    _proposition_variant("capability_assessment", ["capability_id", "assessment"]),
    _proposition_variant("hypothesis",            ["hypothesis_text", "test_conditions"]),
    _proposition_variant("concern",               ["about", "nature", "raised_by"]),
    _proposition_variant("market_assessment",     ["subject_external", "assessment"]),
    _proposition_variant("environmental_trend",   ["signature", "direction", "strength"]),
]


_FALSIFIER_VARIANTS: list[dict] = [
    {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "pattern", "within_window"],
        "properties": {
            "kind": {"type": "string", "enum": ["observation_pattern"]},
            "pattern": {"type": "string"},
            "within_window": {"type": "string"},
        },
    },
    {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "commitment_ref", "contradicting_state"],
        "properties": {
            "kind": {"type": "string", "enum": ["commitment_outcome"]},
            "commitment_ref": _UUID_STR,
            "contradicting_state": {"type": "string"},
        },
    },
    {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "evaluate_at", "check"],
        "properties": {
            "kind": {"type": "string", "enum": ["prediction_deadline"]},
            "evaluate_at": {"type": "string"},
            "check": {"type": "string"},
        },
    },
    {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "resource_ref", "metric", "value"],
        "properties": {
            "kind": {"type": "string", "enum": ["resource_threshold"]},
            "resource_ref": _UUID_STR,
            "metric": {"type": "string"},
            "value": {"type": "number"},
        },
    },
    {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "contesting_actors"],
        "properties": {
            "kind": {"type": "string", "enum": ["explicit_contestation"]},
            "contesting_actors": {"type": "array", "items": _UUID_STR},
        },
    },
    {"type": "null"},
]


_SCOPE_TEMPORAL = {
    "type": "object",
    "additionalProperties": False,
    "required": ["valid_from", "valid_until"],
    "properties": {
        "valid_from": {"type": "string"},
        "valid_until": {"anyOf": [{"type": "string"}, {"type": "null"}]},
    },
}


_SCOPE_ENTITY = {
    "type": "object",
    "additionalProperties": False,
    "required": ["type", "id"],
    "properties": {
        "type": {"type": "string"},
        "id": _UUID_STR,
    },
}


_CLAIM_OP_INSERT_ENTRY = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "born_from_event_id",
        "proposition",
        "natural",
        "confidence",
        "scope_actors",
        "scope_entities",
        "scope_temporal",
        "falsifier",
    ],
    "properties": {
        "born_from_event_id": _UUID_STR,
        "proposition": {"anyOf": _PROPOSITION_KINDS},
        "natural": {"type": "string"},
        "confidence": {"type": "number"},
        "scope_actors": {"type": "array", "items": _UUID_STR},
        "scope_entities": {"type": "array", "items": _SCOPE_ENTITY},
        "scope_temporal": _SCOPE_TEMPORAL,
        "falsifier": {"anyOf": _FALSIFIER_VARIANTS},
    },
}


_CLAIM_OP_INSERT = {
    "type": "object",
    "additionalProperties": False,
    "required": ["op", "entry"],
    "properties": {
        "op": {"type": "string", "enum": ["insert"]},
        "entry": _CLAIM_OP_INSERT_ENTRY,
    },
}


RAW_DIFF_STRICT_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["trigger_ref", "tenant_id", "claim_ops", "reasoning_trace"],
    "properties": {
        "trigger_ref": _UUID_STR,
        "tenant_id": _UUID_STR,
        "claim_ops": {"type": "array", "items": _CLAIM_OP_INSERT},
        "reasoning_trace": {"type": "string"},
    },
}


__all__ = ["RAW_DIFF_STRICT_SCHEMA"]
