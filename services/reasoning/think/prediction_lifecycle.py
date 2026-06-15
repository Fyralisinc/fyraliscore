"""Prediction lifecycle helpers for Think-applied Models.

Think can emit a prediction-shaped Model without going through the
``new_predictions`` post-commit surface. This module keeps the two durable
prediction ledgers in sync:

* ``models.evaluate_at`` / ``models.resolution_criteria`` feed the deadline
  resolver.
* ``model_predictions`` feeds SAGE residual detection and lifecycle metrics.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow
from services.reasoning.sage.model_predictions.repo import ModelPredictionsRepo
from services.reasoning.sage.model_predictions.types import (
    ExpectedObservation,
    ModelPrediction,
)


def prepare_prediction_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Fill lifecycle defaults for a prediction insert entry.

    The helper is intentionally conservative: it only derives ``evaluate_at``
    from explicit temporal information already present in the entry. It does
    not invent a due date for timeless predictions.
    """

    if not _is_prediction_entry(entry):
        return entry
    prepared = dict(entry)
    proposition = _json_obj(prepared.get("proposition"))
    resolution = _json_obj(proposition.get("resolution"))
    falsifier = _json_obj(prepared.get("falsifier"))

    if prepared.get("resolution_criteria") is None:
        criteria = _prediction_resolution_criteria(proposition, resolution, falsifier)
        if criteria:
            prepared["resolution_criteria"] = criteria

    if prepared.get("evaluate_at") is None:
        evaluate_at = _infer_evaluate_at(prepared, resolution, falsifier)
        if evaluate_at is not None:
            prepared["evaluate_at"] = evaluate_at

    return prepared


async def materialize_model_prediction(
    conn: asyncpg.Connection,
    *,
    model: ModelRow,
) -> UUID | None:
    """Create the internal ``model_predictions`` row for a prediction Model.

    Idempotent by ``(tenant_id, model_id, prediction)`` so retries or tests that
    call the helper twice do not create duplicate active expectations.
    """

    if not _is_prediction_model(model):
        return None

    prediction_text = _prediction_text(model)
    existing = await conn.fetchval(
        """
        SELECT id
        FROM model_predictions
        WHERE tenant_id = $1
          AND model_id = $2
          AND prediction = $3
        ORDER BY created_at ASC
        LIMIT 1
        """,
        model.tenant_id,
        model.id,
        prediction_text,
    )
    if existing is not None:
        return existing

    repo = ModelPredictionsRepo(tenant_id=model.tenant_id)
    inserted = await repo.insert(
        ModelPrediction(
            tenant_id=model.tenant_id,
            model_id=model.id,
            prediction=prediction_text,
            expected_observation=_expected_observation_for_model(model),
            check_after=model.evaluate_at,
            status="active",
            confidence=model.confidence,
        ),
        conn=conn,
    )
    return inserted.id


async def sync_model_prediction_resolution(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    resolution_outcome: bool | None,
    observation_id: UUID | None = None,
) -> int:
    """Mirror Model resolution into active internal prediction rows."""

    if resolution_outcome is None:
        return 0
    status = "confirmed" if resolution_outcome else "falsified"
    rows = await conn.fetch(
        """
        UPDATE model_predictions
        SET status = $3,
            resolved_at = now(),
            resolved_by_observation_id = COALESCE($4, resolved_by_observation_id),
            updated_at = now()
        WHERE tenant_id = $1
          AND model_id = $2
          AND status = 'active'
        RETURNING id
        """,
        tenant_id,
        model_id,
        status,
        observation_id,
    )
    return len(rows)


def _is_prediction_entry(entry: dict[str, Any]) -> bool:
    proposition = _json_obj(entry.get("proposition"))
    return (
        proposition.get("kind") == "prediction"
        or proposition.get("claim_role") == "prediction"
        or entry.get("claim_role") == "prediction"
    )


def _is_prediction_model(model: ModelRow) -> bool:
    return model.proposition_kind == "prediction" or model.claim_role == "prediction"


def _prediction_text(model: ModelRow) -> str:
    prop = _json_obj(model.proposition)
    expected = prop.get("expected")
    if isinstance(expected, str) and expected.strip():
        return expected.strip()
    if isinstance(expected, dict):
        text = expected.get("text") or expected.get("assertion") or expected.get("summary")
        if isinstance(text, str) and text.strip():
            return text.strip()
    return (model.natural or "Prediction").strip()


def _expected_observation_for_model(model: ModelRow) -> ExpectedObservation:
    prop = _json_obj(model.proposition)
    resolution = _json_obj(prop.get("resolution"))
    falsifier = _json_obj(model.falsifier)
    criteria = _json_obj(model.resolution_criteria)

    expected_kind = (
        resolution.get("kind")
        or criteria.get("kind")
        or ("metric_delta" if criteria.get("value_constraint") else None)
        or falsifier.get("kind")
        or "unspecified"
    )
    value_constraint = (
        _json_obj(resolution.get("value_constraint"))
        or _json_obj(criteria.get("value_constraint"))
        or _json_obj(resolution.get("constraint"))
        or None
    )
    falsification_rule = _first_text(
        resolution.get("falsification_rule"),
        criteria.get("falsification_rule"),
        falsifier.get("pattern"),
        falsifier.get("check"),
        prop.get("resolution"),
    )
    extras = {
        "source": "think_prediction_model",
        "model_id": str(model.id),
        "expected": _jsonable(prop.get("expected")),
        "resolution": _jsonable(prop.get("resolution")),
        "supporting_event_ids": [str(uid) for uid in model.supporting_event_ids],
    }
    return ExpectedObservation(
        kind=str(expected_kind),
        scope_entities=list(model.scope_entities or []),
        scope_actors=list(model.scope_actors or []),
        value_constraint=value_constraint,
        falsification_rule=falsification_rule,
        extras={k: v for k, v in extras.items() if v not in (None, [], {})},
    )


def _prediction_resolution_criteria(
    proposition: dict[str, Any],
    resolution: dict[str, Any],
    falsifier: dict[str, Any],
) -> dict[str, Any]:
    criteria: dict[str, Any] = {
        "source": "think_prediction_lifecycle",
        "expected": _jsonable(proposition.get("expected")),
        "resolution": _jsonable(proposition.get("resolution")),
    }
    if resolution.get("kind") is not None:
        criteria["kind"] = resolution.get("kind")
    if resolution.get("value_constraint") is not None:
        criteria["value_constraint"] = resolution.get("value_constraint")
    if resolution.get("constraint") is not None and "value_constraint" not in criteria:
        criteria["value_constraint"] = resolution.get("constraint")
    rule = _first_text(
        resolution.get("falsification_rule"),
        falsifier.get("pattern"),
        falsifier.get("check"),
        proposition.get("resolution"),
    )
    if rule:
        criteria["falsification_rule"] = rule
    return {k: v for k, v in criteria.items() if v not in (None, {}, [])}


def _infer_evaluate_at(
    entry: dict[str, Any],
    resolution: dict[str, Any],
    falsifier: dict[str, Any],
) -> datetime | None:
    for candidate in (
        resolution.get("evaluate_at"),
        resolution.get("check_after"),
        resolution.get("deadline"),
        resolution.get("by"),
        falsifier.get("evaluate_at"),
        falsifier.get("check_after"),
        falsifier.get("deadline"),
        falsifier.get("by"),
        _json_obj(entry.get("scope_temporal")).get("valid_until"),
        _json_obj(entry.get("scope_temporal")).get("deadline"),
    ):
        parsed = _parse_datetime(candidate)
        if parsed is not None:
            return parsed

    window = falsifier.get("within_window")
    if isinstance(window, str) and window.strip():
        try:
            from services.workers.deadline_resolver.evaluators import parse_window

            delta = parse_window(window)
        except Exception:  # noqa: BLE001
            delta = None
        if delta is not None:
            scope_temporal = _json_obj(entry.get("scope_temporal"))
            base = (
                _parse_datetime(scope_temporal.get("valid_from"))
                or _parse_datetime(entry.get("born_from_event_occurred_at"))
                or _parse_datetime(entry.get("created_at"))
                or _parse_datetime(entry.get("asserted_at"))
                or datetime.now(timezone.utc)
            )
            return base + delta
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (dict, list)):
        return value
    return str(value)


def _first_text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for key in ("text", "summary", "pattern", "check", "description"):
                item = value.get(key)
                if isinstance(item, str) and item.strip():
                    return item.strip()
    return None


__all__ = [
    "materialize_model_prediction",
    "prepare_prediction_entry",
    "sync_model_prediction_resolution",
]
