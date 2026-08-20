"""Canonical read shapes for hydrating ModelRow instances.

The model layer owns the canonical ModelRow SQL projection. Retrieval and
projection repos can compose their own queries, but they should not duplicate
the column list or row coercion rules.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from lib.shared.db import RowHydrationError
from lib.shared.types import ModelRow


MODEL_ROW_SELECT_COLS = (
    "id",
    "tenant_id",
    "born_from_event_id",
    "proposition",
    '"natural" AS natural',
    "embedding",
    "scope_actors",
    "scope_entities",
    "scope_temporal",
    "confidence",
    "activation",
    "falsifier",
    "signal_readings",
    "reading_contestable",
    "supporting_event_ids",
    "supporting_model_ids",
    "evidential_weight",
    "status",
    "archived_at",
    "archive_reason",
    "created_at",
    "last_retrieved_at",
    "retrieval_count",
    "evaluate_at",
    "resolution_criteria",
    "contributing_models",
    "visible_to_subjects",
    "proposition_kind",
    "claim_role",
    "abstraction_level",
    "time_mode",
    "modality",
    "polarity",
    "domain_tags",
    "COALESCE((SELECT mst.semantic_terms "
    "FROM model_semantic_terms mst "
    "WHERE mst.model_id = id), '{}'::text[]) AS semantic_terms",
    "memory_grammar_version",
    "confirmed_count",
    "contested_count",
    "last_confirmed_at",
    "confidence_at_assertion",
    "resolved_at",
    "resolution_outcome",
    "activation_coefficient",
    "target_actor_id",
    "caused_act_change_id",
)
MODEL_ROW_SELECT_SQL = ", ".join(MODEL_ROW_SELECT_COLS)

_JSON_FIELDS = (
    "proposition",
    "scope_entities",
    "scope_temporal",
    "falsifier",
    "signal_readings",
    "resolution_criteria",
)


def hydrate_model_row(
    record: Mapping[str, Any],
    *,
    drop_internal_fields: bool = False,
    null_invalid_embedding: bool = False,
    use_vector_to_list: bool = False,
    wrap_errors: bool = False,
) -> ModelRow:
    """Hydrate a ModelRow from an asyncpg record or mapping.

    Options encode the small historical differences between callers:
    ModelsRepo wraps validation failures, while retrieval/projection helpers
    strip query-private columns and null out unparsable vector values before
    letting Pydantic raise normally.
    """
    raw = dict(record)
    if drop_internal_fields:
        for key in list(raw):
            if str(key).startswith("_"):
                raw.pop(key, None)

    for key in _JSON_FIELDS:
        value = raw.get(key)
        if isinstance(value, (bytes, bytearray)):
            value = value.decode()
        if isinstance(value, str):
            try:
                raw[key] = json.loads(value)
            except json.JSONDecodeError:
                pass

    _coerce_embedding(
        raw,
        null_invalid_embedding=null_invalid_embedding,
        use_vector_to_list=use_vector_to_list,
    )

    try:
        return ModelRow.model_validate(raw)
    except Exception as exc:
        if not wrap_errors:
            raise
        raise RowHydrationError(
            f"could not hydrate models row: {exc}",
            row_keys=list(raw.keys()),
        ) from exc


def _coerce_embedding(
    raw: dict[str, Any],
    *,
    null_invalid_embedding: bool,
    use_vector_to_list: bool,
) -> None:
    embedding = raw.get("embedding")
    if embedding is None or isinstance(embedding, list):
        return

    original = embedding
    if isinstance(embedding, (bytes, bytearray)):
        embedding = embedding.decode()
    if isinstance(embedding, str):
        try:
            raw["embedding"] = json.loads(embedding)
        except (json.JSONDecodeError, ValueError):
            _set_invalid_embedding(raw, original, null_invalid_embedding)
        return

    if use_vector_to_list:
        to_list = getattr(embedding, "to_list", None)
        if callable(to_list):
            raw["embedding"] = to_list()
            return

    try:
        raw["embedding"] = [float(value) for value in embedding]
    except (TypeError, ValueError):
        _set_invalid_embedding(raw, original, null_invalid_embedding)


def _set_invalid_embedding(
    raw: dict[str, Any],
    original: Any,
    null_invalid_embedding: bool,
) -> None:
    raw["embedding"] = None if null_invalid_embedding else original


__all__ = [
    "MODEL_ROW_SELECT_COLS",
    "MODEL_ROW_SELECT_SQL",
    "hydrate_model_row",
]
