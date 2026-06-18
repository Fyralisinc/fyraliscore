"""Strict-mode JSON schema for DeepSeek tool-calling output.

DeepSeek strict mode requires: every object property listed in `required`,
`additionalProperties: false` everywhere, no `Any`-typed fields, and only
these JSON-schema features: object, string, number, integer, boolean,
array, enum, anyOf, const.

This schema is a deliberate SUBSET of `RawDiff`: it constrains
`claim_ops`, first-class `relation_claim_ops`, `relation_frame_ops`,
`edge_ops`, ontology gaps, resource operations, and first-class prediction
inserts. `act_ops` remain omitted from the
strict schema — Pydantic defaults them to an empty list at parse time.
Acts can be added back when specific shapes need to be enforced.
"""
from __future__ import annotations


_UUID_PATTERN = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
_UUID_STR = {"type": "string", "pattern": _UUID_PATTERN}
_NULLABLE_STRING = {"anyOf": [{"type": "string"}, {"type": "null"}]}
_NULLABLE_NUMBER = {"anyOf": [{"type": "number"}, {"type": "null"}]}
_NULLABLE_INTEGER = {"anyOf": [{"type": "integer"}, {"type": "null"}]}
_NULLABLE_BOOLEAN = {"anyOf": [{"type": "boolean"}, {"type": "null"}]}


def _proposition_variant(kind: str, fields: list[str], *, claim_role: str | None = None) -> dict:
    """One concrete proposition stance/role shape as a strict object."""
    properties: dict = {"kind": {"type": "string", "enum": [kind]}}
    required = ["kind", *fields]
    if claim_role is not None:
        properties["claim_role"] = {"type": "string", "enum": [claim_role]}
        required.append("claim_role")
    for f in fields:
        if f == "domain_tags":
            properties[f] = {"type": "array", "items": {"type": "string"}}
        else:
            properties[f] = {"type": "string"}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": properties,
    }


_PROPOSITION_KINDS: list[dict] = [
    _proposition_variant("observation", ["event"], claim_role="fact"),
    _proposition_variant("belief", ["subject", "assertion"], claim_role="fact"),
    _proposition_variant("belief", ["subject", "relation", "object"], claim_role="relation"),
    _proposition_variant("prediction", ["expected", "resolution"], claim_role="prediction"),
    _proposition_variant("belief", ["signature", "observed_tendency", "trigger_conditions"], claim_role="pattern"),
    _proposition_variant("belief", ["capability_id", "assessment"], claim_role="capability"),
    _proposition_variant("belief", ["hypothesis_text", "test_conditions"], claim_role="hypothesis"),
    _proposition_variant("belief", ["about", "nature", "raised_by"], claim_role="concern"),
    _proposition_variant("belief", ["subject_external", "assessment", "domain_tags"], claim_role="pattern"),
    {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "kind",
            "claim_role",
            "abstraction_level",
            "situation",
            "summary",
            "member_model_ids",
            "relationship_summary",
            "status",
            # Compositional fields — strict mode requires every property
            # be listed in `required`, so we make them required-but-
            # nullable. Pydantic accepts null/absent as None.
            "pressure_type",
            "shared_mechanism",
            "judgment_change",
            "affected_decisions",
            "affected_customers",
            "affected_teams",
            "evidence_event_ids",
            "open_falsifier",
        ],
        "properties": {
            "kind": {"type": "string", "enum": ["belief"]},
            "claim_role": {"type": "string", "enum": ["situation"]},
            "abstraction_level": {"type": "string", "enum": ["composite"]},
            "situation": {"type": "string"},
            "summary": {"type": "string"},
            "member_model_ids": {"type": "array", "items": _UUID_STR},
            "relationship_summary": {"type": "string"},
            "status": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "pressure_type": {
                "anyOf": [
                    {
                        "type": "string",
                        "enum": [
                            "capacity",
                            "trust",
                            "revenue",
                            "compliance",
                            "decision",
                            "execution",
                            "market",
                            "resource",
                        ],
                    },
                    {"type": "null"},
                ],
            },
            "shared_mechanism": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
            },
            "judgment_change": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
            },
            "affected_decisions": {
                "anyOf": [
                    {"type": "array", "items": {"type": "string"}},
                    {"type": "null"},
                ],
            },
            "affected_customers": {
                "anyOf": [
                    {"type": "array", "items": {"type": "string"}},
                    {"type": "null"},
                ],
            },
            "affected_teams": {
                "anyOf": [
                    {"type": "array", "items": {"type": "string"}},
                    {"type": "null"},
                ],
            },
            "evidence_event_ids": {
                "anyOf": [
                    {"type": "array", "items": _UUID_STR},
                    {"type": "null"},
                ],
            },
            "open_falsifier": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
            },
        },
    },
    # recommendation has a structured shape, not all-string fields, so
    # build it manually rather than via the _proposition_variant helper.
    {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "kind",
            "claim_role",
            "target_act_ref",
            "proposed_change",
            "expected_impact",
            "qualitative_impact",
            "target_actor_id",
        ],
        "properties": {
            "kind": {"type": "string", "enum": ["norm"]},
            "claim_role": {"type": "string", "enum": ["recommendation"]},
            "target_act_ref": {
                "anyOf": [
                    {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["type", "id"],
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["goal", "commitment", "decision", "resource"],
                            },
                            "id": {"anyOf": [_UUID_STR, {"type": "null"}]},
                        },
                    },
                    {"type": "null"},
                ],
            },
            "proposed_change": {
                "type": "object",
                "additionalProperties": False,
                "required": ["operation", "payload"],
                "properties": {
                    "operation": {
                        "type": "string",
                        "enum": ["create", "update", "archive", "transition"],
                    },
                    # `payload` varies by operation+target type. DeepSeek
                    # strict mode rejects a bare `{"type": "object"}`
                    # ("An object with no properties is not allowed"),
                    # and rejects `additionalProperties: true`. So we
                    # enumerate every payload field the recommendations
                    # applier reads (services.product.recommendations.handlers,
                    # services.reasoning.think.applier) as nullable. Fields that
                    # don't apply to the current operation come back as
                    # null and the applier ignores them.
                    "payload": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "new_state",
                            "title",
                            "description",
                            "altitude",
                            "parent_goal_id",
                            "success_criteria",
                            "target_date",
                            "field",
                            "new_value",
                            "reason",
                            "kind",
                            "identity",
                            "current_value",
                            "metadata",
                            "utilization_state",
                            "controllability",
                            "temporal_character",
                            "valuation_confidence",
                        ],
                        "properties": {
                            "new_state":            {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "title":                {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "description":          {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "altitude":             {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "parent_goal_id":       {"anyOf": [_UUID_STR, {"type": "null"}]},
                            "success_criteria":     {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "target_date":          {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "field":                {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "new_value":            {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "reason":               {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "kind":                 {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "identity":             {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "current_value":        {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "metadata":             {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "utilization_state":    {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "controllability":      {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "temporal_character":   {"anyOf": [{"type": "string"}, {"type": "null"}]},
                            "valuation_confidence": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                        },
                    },
                },
            },
            "expected_impact": {
                "anyOf": [{"type": "number"}, {"type": "null"}],
            },
            "qualitative_impact": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
            },
            "target_actor_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        },
    },
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


_EDGE_KIND_ENUM = [
    "supports",
    "contributes_to_resolution",
    "instance_of",
    "superseded_by",
    "contradicts",
    "weakens",
    "causes",
    "explains",
    "predicts",
    "blocks",
    "enables",
    "same_issue_as",
    "co_occurs_with",
    "analogous_to",
    "alternative_to",
    "early_warning_for",
]


_RELATION_CLAIM_OP = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "op",
        "source_model_id",
        "target_model_id",
        "predicate",
        "edge_kind",
        "weight",
        "direction",
        "endpoint_binding_status",
        "write_policy",
        "status",
        "confidence",
        "binding_confidence",
        "evidence_event_ids",
        "evidence_model_ids",
        "evidence_text",
        "explanation",
    ],
    "properties": {
        "op": {"type": "string", "enum": ["upsert"]},
        "source_model_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "target_model_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "predicate": {"type": "string"},
        "edge_kind": {"type": "string", "enum": _EDGE_KIND_ENUM},
        "weight": {"anyOf": [{"type": "number"}, {"type": "null"}]},
        "direction": {
            "type": "string",
            "enum": [
                "source_to_target",
                "target_to_source",
                "symmetric",
                "unknown",
            ],
        },
        "endpoint_binding_status": {
            "type": "string",
            "enum": ["bound", "partially_bound", "unbound", "ambiguous"],
        },
        "write_policy": {
            "type": "string",
            "enum": ["accepted_edge", "candidate", "needs_review", "no_edge"],
        },
        "status": {
            "type": "string",
            "enum": [
                "active",
                "accepted",
                "candidate",
                "needs_review",
                "rejected",
                "retired",
            ],
        },
        "confidence": {"type": "number"},
        "binding_confidence": {"type": "number"},
        "evidence_event_ids": {"type": "array", "items": _UUID_STR},
        "evidence_model_ids": {"type": "array", "items": _UUID_STR},
        "evidence_text": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "explanation": {"anyOf": [{"type": "string"}, {"type": "null"}]},
    },
}


_RELATION_FRAME_PARTICIPANT_OP = {
    "type": "object",
    "additionalProperties": False,
    "required": ["model_id", "role", "binding_confidence"],
    "properties": {
        "model_id": _UUID_STR,
        "role": {
            "type": "string",
            "enum": [
                "blocker",
                "blocked_work",
                "owner",
                "downstream_risk",
                "possible_resolution",
            ],
        },
        "binding_confidence": {"type": "number"},
    },
}


_RELATION_FRAME_OP = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "op",
        "relation_kind",
        "participants",
        "participant_binding_status",
        "write_policy",
        "status",
        "confidence",
        "evidence_event_ids",
        "evidence_model_ids",
        "evidence_text",
        "explanation",
    ],
    "properties": {
        "op": {"type": "string", "enum": ["upsert"]},
        "relation_kind": {"type": "string", "enum": ["blocked_workstream"]},
        "participants": {
            "type": "array",
            "items": _RELATION_FRAME_PARTICIPANT_OP,
        },
        "participant_binding_status": {
            "type": "string",
            "enum": ["bound", "partially_bound", "unbound", "ambiguous"],
        },
        "write_policy": {
            "type": "string",
            "enum": ["project_edges", "candidate", "needs_review", "no_projection"],
        },
        "status": {
            "type": "string",
            "enum": [
                "active",
                "candidate",
                "accepted",
                "needs_review",
                "disputed",
                "rejected",
                "retired",
            ],
        },
        "confidence": {"type": "number"},
        "evidence_event_ids": {"type": "array", "items": _UUID_STR},
        "evidence_model_ids": {"type": "array", "items": _UUID_STR},
        "evidence_text": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "explanation": {"anyOf": [{"type": "string"}, {"type": "null"}]},
    },
}


_EDGE_OP = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "op",
        "source_model_id",
        "target_model_id",
        "edge_kind",
        "weight",
        "confidence",
        "evidence_event_ids",
        "evidence_model_ids",
        "explanation",
        "review_status",
        "reason",
    ],
    "properties": {
        "op": {"type": "string", "enum": ["add", "retire"]},
        "source_model_id": _UUID_STR,
        "target_model_id": _UUID_STR,
        "edge_kind": {"type": "string", "enum": _EDGE_KIND_ENUM},
        "weight": {"anyOf": [{"type": "number"}, {"type": "null"}]},
        "confidence": {"type": "number"},
        "evidence_event_ids": {"type": "array", "items": _UUID_STR},
        "evidence_model_ids": {"type": "array", "items": _UUID_STR},
        "explanation": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "review_status": {
            "type": "string",
            "enum": [
                "accepted",
                "candidate",
                "needs_review",
                "disputed",
                "rejected",
                "retired",
            ],
        },
        "reason": {"anyOf": [{"type": "string"}, {"type": "null"}]},
    },
}


_ONTOLOGY_GAP_OP = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "op",
        "source_model_id",
        "target_model_id",
        "proposed_edge_kind",
        "description",
        "relationship_summary",
        "parent_kind",
        "nearest_existing_kind",
        "directionality",
        "inverse_label",
        "dropped_dimensions",
        "evidence_event_ids",
        "evidence_model_ids",
        "confidence",
        "impact",
        "actionability",
        "urgency",
        "uncertainty",
        "authority_required",
        "novelty",
    ],
    "properties": {
        "op": {"type": "string", "enum": ["propose_edge_type"]},
        "source_model_id": _UUID_STR,
        "target_model_id": _UUID_STR,
        "proposed_edge_kind": {"type": "string"},
        "description": {"type": "string"},
        "relationship_summary": {"type": "string"},
        "parent_kind": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "nearest_existing_kind": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "directionality": {
            "type": "string",
            "enum": ["directed", "symmetric", "unknown"],
        },
        "inverse_label": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "dropped_dimensions": {"type": "array", "items": {"type": "string"}},
        "evidence_event_ids": {"type": "array", "items": _UUID_STR},
        "evidence_model_ids": {"type": "array", "items": _UUID_STR},
        "confidence": {"type": "number"},
        "impact": {"type": "number"},
        "actionability": {"type": "number"},
        "urgency": {"type": "number"},
        "uncertainty": {"type": "number"},
        "authority_required": {"type": "number"},
        "novelty": {"type": "number"},
    },
}


_RESOURCE_TRANSACTION_KIND_ENUM = [
    "acquire",
    "deploy",
    "release",
    "spend",
    "strengthen",
    "weaken",
    "expire",
]


_RESOURCE_SCALAR_OBJECT_FIELDS = [
    "amount_cents",
    "arr_cents",
    "available_units",
    "contract_state",
    "currency",
    "deployed_units",
    "description",
    "metric",
    "notes",
    "reason",
    "region",
    "renewal_date",
    "strength",
    "strength_delta",
    "total_units",
    "unit",
    "units",
    "value",
]


_RESOURCE_SCALAR_OBJECT = {
    "type": "object",
    "additionalProperties": False,
    "required": _RESOURCE_SCALAR_OBJECT_FIELDS,
    "properties": {
        "amount_cents": _NULLABLE_INTEGER,
        "arr_cents": _NULLABLE_INTEGER,
        "available_units": _NULLABLE_INTEGER,
        "contract_state": _NULLABLE_STRING,
        "currency": _NULLABLE_STRING,
        "deployed_units": _NULLABLE_INTEGER,
        "description": _NULLABLE_STRING,
        "metric": _NULLABLE_STRING,
        "notes": _NULLABLE_STRING,
        "reason": _NULLABLE_STRING,
        "region": _NULLABLE_STRING,
        "renewal_date": _NULLABLE_STRING,
        "strength": _NULLABLE_STRING,
        "strength_delta": _NULLABLE_INTEGER,
        "total_units": _NULLABLE_INTEGER,
        "unit": _NULLABLE_STRING,
        "units": _NULLABLE_INTEGER,
        "value": {
            "anyOf": [{"type": "string"}, {"type": "number"}, {"type": "null"}],
        },
    },
}


_NULLABLE_RESOURCE_SCALAR_OBJECT = {
    "anyOf": [_RESOURCE_SCALAR_OBJECT, {"type": "null"}],
}


_RESOURCE_PAYLOAD_OBJECT = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "kind",
        "identity",
        "description",
        "current_value",
        "metadata",
        "metadata_patch",
        "utilization_state",
        "controllability",
        "temporal_character",
        "valuation_confidence",
        "created_by_event_id",
        "last_updated_by_event_id",
        "occurred_at",
        "source_event_id",
        "started_at",
        "released_at",
    ],
    "properties": {
        "kind": _NULLABLE_STRING,
        "identity": _NULLABLE_STRING,
        "description": _NULLABLE_STRING,
        "current_value": _NULLABLE_RESOURCE_SCALAR_OBJECT,
        "metadata": _NULLABLE_RESOURCE_SCALAR_OBJECT,
        "metadata_patch": _NULLABLE_RESOURCE_SCALAR_OBJECT,
        "utilization_state": {
            "anyOf": [
                {
                    "type": "string",
                    "enum": ["available", "deployed", "committed", "depleted", "expired"],
                },
                {"type": "null"},
            ],
        },
        "controllability": {
            "anyOf": [
                {
                    "type": "string",
                    "enum": ["owned", "joint", "borrowed", "leased", "limited"],
                },
                {"type": "null"},
            ],
        },
        "temporal_character": {
            "anyOf": [
                {
                    "type": "string",
                    "enum": ["permanent", "time_limited", "renewable", "consumable"],
                },
                {"type": "null"},
            ],
        },
        "valuation_confidence": _NULLABLE_NUMBER,
        "created_by_event_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "last_updated_by_event_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "occurred_at": _NULLABLE_STRING,
        "source_event_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "started_at": _NULLABLE_STRING,
        "released_at": _NULLABLE_STRING,
    },
}


_NULLABLE_RESOURCE_PAYLOAD = {
    "anyOf": [_RESOURCE_PAYLOAD_OBJECT, {"type": "null"}],
}


_RESOURCE_PATCH_OBJECT = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "current_value",
        "utilization_state",
        "controllability",
        "temporal_character",
        "valuation_confidence",
    ],
    "properties": {
        "current_value": _NULLABLE_RESOURCE_SCALAR_OBJECT,
        "utilization_state": {
            "anyOf": [
                {
                    "type": "string",
                    "enum": ["available", "deployed", "committed", "depleted", "expired"],
                },
                {"type": "null"},
            ],
        },
        "controllability": {
            "anyOf": [
                {
                    "type": "string",
                    "enum": ["owned", "joint", "borrowed", "leased", "limited"],
                },
                {"type": "null"},
            ],
        },
        "temporal_character": {
            "anyOf": [
                {
                    "type": "string",
                    "enum": ["permanent", "time_limited", "renewable", "consumable"],
                },
                {"type": "null"},
            ],
        },
        "valuation_confidence": _NULLABLE_NUMBER,
    },
}


_NULLABLE_RESOURCE_PATCH = {"anyOf": [_RESOURCE_PATCH_OBJECT, {"type": "null"}]}


_RESOURCE_OP = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "op",
        "resource_id",
        "commitment_id",
        "payload",
        "patch",
        "kind",
        "delta",
        "quantity",
        "actual_quantity",
    ],
    "properties": {
        "op": {
            "type": "string",
            "enum": ["create", "transaction", "deploy", "release", "update"],
        },
        "resource_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "commitment_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "payload": _NULLABLE_RESOURCE_PAYLOAD,
        "patch": _NULLABLE_RESOURCE_PATCH,
        "kind": {
            "anyOf": [
                {"type": "string", "enum": _RESOURCE_TRANSACTION_KIND_ENUM},
                {"type": "null"},
            ],
        },
        "delta": _NULLABLE_RESOURCE_SCALAR_OBJECT,
        "quantity": _NULLABLE_RESOURCE_SCALAR_OBJECT,
        "actual_quantity": _NULLABLE_RESOURCE_SCALAR_OBJECT,
    },
}


_MEMORY_LIFECYCLE_OP = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "op",
        "model_id",
        "action",
        "evidence_event_ids",
        "evidence_model_ids",
        "confidence_delta",
        "confidence",
        "resolution_outcome",
        "rationale",
        "reason",
        "superseded_by_model_id",
        "metadata",
    ],
    "properties": {
        "op": {"type": "string", "enum": ["reconcile"]},
        "model_id": _UUID_STR,
        "action": {
            "type": "string",
            "enum": [
                "confirm",
                "falsify",
                "revise",
                "unchanged",
                "archive",
                "supersede",
            ],
        },
        "evidence_event_ids": {"type": "array", "items": _UUID_STR},
        "evidence_model_ids": {"type": "array", "items": _UUID_STR},
        "confidence_delta": _NULLABLE_NUMBER,
        "confidence": _NULLABLE_NUMBER,
        "resolution_outcome": {"anyOf": [{"type": "boolean"}, {"type": "null"}]},
        "rationale": {"type": "string"},
        "reason": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "superseded_by_model_id": {"anyOf": [_UUID_STR, {"type": "null"}]},
        "metadata": {"type": "object", "additionalProperties": True},
    },
}


RAW_DIFF_STRICT_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "trigger_ref",
        "tenant_id",
        "claim_ops",
        "memory_lifecycle_ops",
        "relation_claim_ops",
        "relation_frame_ops",
        "edge_ops",
        "ontology_gap_ops",
        "resource_ops",
        "new_predictions",
        "reasoning_trace",
    ],
    "properties": {
        "trigger_ref": _UUID_STR,
        "tenant_id": _UUID_STR,
        "claim_ops": {"type": "array", "items": _CLAIM_OP_INSERT},
        "memory_lifecycle_ops": {
            "type": "array",
            "items": _MEMORY_LIFECYCLE_OP,
        },
        "relation_claim_ops": {"type": "array", "items": _RELATION_CLAIM_OP},
        "relation_frame_ops": {"type": "array", "items": _RELATION_FRAME_OP},
        "edge_ops": {"type": "array", "items": _EDGE_OP},
        "ontology_gap_ops": {"type": "array", "items": _ONTOLOGY_GAP_OP},
        "resource_ops": {"type": "array", "items": _RESOURCE_OP},
        "new_predictions": {"type": "array", "items": _CLAIM_OP_INSERT},
        "reasoning_trace": {"type": "string"},
    },
}


RAW_DIFF_CLAIMS_ONLY_STRICT_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "trigger_ref",
        "tenant_id",
        "claim_ops",
        "reasoning_trace",
    ],
    "properties": {
        "trigger_ref": _UUID_STR,
        "tenant_id": _UUID_STR,
        "claim_ops": {"type": "array", "items": _CLAIM_OP_INSERT},
        "reasoning_trace": {"type": "string"},
    },
}


__all__ = ["RAW_DIFF_STRICT_SCHEMA", "RAW_DIFF_CLAIMS_ONLY_STRICT_SCHEMA"]
