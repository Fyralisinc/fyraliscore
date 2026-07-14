"""Logical facets over canonical Model rows.

The database still stores a wide ModelRow for compatibility, but callers should
think of that row as a belief kernel plus facets. This module makes that split
explicit without forcing a physical schema migration.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from lib.shared.types import ModelArchiveReason, ModelRow, ModelStatus


MODEL_FACET_SCHEMA_VERSION = "model_facets_v1"
ModelFacetName = Literal[
    "core",
    "semantic",
    "evidence",
    "runtime",
    "retrieval",
    "prediction",
    "recommendation",
]
BASE_MODEL_FACETS: tuple[ModelFacetName, ...] = (
    "core",
    "semantic",
    "evidence",
    "runtime",
    "retrieval",
)


class _Facet(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelCoreFacet(_Facet):
    id: UUID
    tenant_id: UUID
    born_from_event_id: UUID
    proposition: dict[str, Any]
    natural: str
    scope_actors: list[UUID] = Field(default_factory=list)
    scope_entities: list[dict[str, Any]] = Field(default_factory=list)
    scope_temporal: dict[str, Any]
    confidence: float
    falsifier: dict[str, Any] | None = None
    proposition_kind: str | None = None
    claim_role: str | None = None
    abstraction_level: str | None = None
    time_mode: str | None = None
    modality: str | None = None
    polarity: str | None = None
    memory_grammar_version: str = "v1"


class ModelSemanticFacet(_Facet):
    embedding: list[float]
    domain_tags: list[str] = Field(default_factory=list)
    semantic_terms: list[str] = Field(default_factory=list)


class ModelEvidenceFacet(_Facet):
    signal_readings: list[dict[str, Any]] = Field(default_factory=list)
    reading_contestable: bool = True
    supporting_event_ids: list[UUID] = Field(default_factory=list)
    supporting_model_ids: list[UUID] = Field(default_factory=list)
    evidential_weight: float = 0.5
    confirmed_count: int = 0
    contested_count: int = 0
    last_confirmed_at: datetime | None = None
    confidence_at_assertion: float


class ModelRuntimeFacet(_Facet):
    status: ModelStatus = "active"
    archived_at: datetime | None = None
    archive_reason: ModelArchiveReason | None = None
    created_at: datetime
    visible_to_subjects: bool = True
    activation_coefficient: float = 1.0


class ModelRetrievalFacet(_Facet):
    activation: float
    last_retrieved_at: datetime | None = None
    retrieval_count: int = 0


class ModelPredictionFacet(_Facet):
    is_prediction: bool
    evaluate_at: datetime | None = None
    resolution_criteria: dict[str, Any] | None = None
    contributing_models: list[UUID] = Field(default_factory=list)
    resolved_at: datetime | None = None
    resolution_outcome: bool | None = None


class ModelRecommendationFacet(_Facet):
    is_recommendation: bool
    target_actor_id: UUID | None = None
    caused_act_change_id: UUID | None = None


class ModelFacetBundle(_Facet):
    schema_version: str = MODEL_FACET_SCHEMA_VERSION
    names: tuple[ModelFacetName, ...]
    core: ModelCoreFacet
    semantic: ModelSemanticFacet
    evidence: ModelEvidenceFacet
    runtime: ModelRuntimeFacet
    retrieval: ModelRetrievalFacet
    prediction: ModelPredictionFacet
    recommendation: ModelRecommendationFacet


def model_facet_names(model: ModelRow | asyncpg.Record | Mapping[str, Any]) -> tuple[ModelFacetName, ...]:
    """Return the facet groups that materially apply to this Model."""
    raw = _raw_model(model)
    names: list[ModelFacetName] = list(BASE_MODEL_FACETS)
    if _is_prediction(raw):
        names.append("prediction")
    if _is_recommendation(raw):
        names.append("recommendation")
    return tuple(names)


def split_model_facets(model: ModelRow | asyncpg.Record | Mapping[str, Any]) -> ModelFacetBundle:
    """Split a hydrated Model row into stable logical facets."""
    raw = _raw_model(model)
    return ModelFacetBundle(
        names=model_facet_names(raw),
        core=ModelCoreFacet(
            id=_uuid(raw["id"]),
            tenant_id=_uuid(raw["tenant_id"]),
            born_from_event_id=_uuid(raw["born_from_event_id"]),
            proposition=dict(raw.get("proposition") or {}),
            natural=str(raw.get("natural") or ""),
            scope_actors=_uuid_list(raw.get("scope_actors")),
            scope_entities=list(raw.get("scope_entities") or []),
            scope_temporal=dict(raw.get("scope_temporal") or {}),
            confidence=float(raw.get("confidence") or 0.0),
            falsifier=_dict_or_none(raw.get("falsifier")),
            proposition_kind=_str_or_none(raw.get("proposition_kind")),
            claim_role=_str_or_none(raw.get("claim_role")),
            abstraction_level=_str_or_none(raw.get("abstraction_level")),
            time_mode=_str_or_none(raw.get("time_mode")),
            modality=_str_or_none(raw.get("modality")),
            polarity=_str_or_none(raw.get("polarity")),
            memory_grammar_version=str(raw.get("memory_grammar_version") or "v1"),
        ),
        semantic=ModelSemanticFacet(
            embedding=[float(value) for value in raw.get("embedding") or []],
            domain_tags=[str(value) for value in raw.get("domain_tags") or []],
            semantic_terms=[str(value) for value in raw.get("semantic_terms") or []],
        ),
        evidence=ModelEvidenceFacet(
            signal_readings=list(raw.get("signal_readings") or []),
            reading_contestable=bool(raw.get("reading_contestable", True)),
            supporting_event_ids=_uuid_list(raw.get("supporting_event_ids")),
            supporting_model_ids=_uuid_list(raw.get("supporting_model_ids")),
            evidential_weight=float(raw.get("evidential_weight") or 0.0),
            confirmed_count=int(raw.get("confirmed_count") or 0),
            contested_count=int(raw.get("contested_count") or 0),
            last_confirmed_at=raw.get("last_confirmed_at"),
            confidence_at_assertion=float(raw.get("confidence_at_assertion") or 0.0),
        ),
        runtime=ModelRuntimeFacet(
            status=raw.get("status") or "active",
            archived_at=raw.get("archived_at"),
            archive_reason=raw.get("archive_reason"),
            created_at=raw["created_at"],
            visible_to_subjects=bool(raw.get("visible_to_subjects", True)),
            activation_coefficient=float(raw.get("activation_coefficient") or 1.0),
        ),
        retrieval=ModelRetrievalFacet(
            activation=float(raw.get("activation") or 0.0),
            last_retrieved_at=raw.get("last_retrieved_at"),
            retrieval_count=int(raw.get("retrieval_count") or 0),
        ),
        prediction=ModelPredictionFacet(
            is_prediction=_is_prediction(raw),
            evaluate_at=raw.get("evaluate_at"),
            resolution_criteria=_dict_or_none(raw.get("resolution_criteria")),
            contributing_models=_uuid_list(raw.get("contributing_models")),
            resolved_at=raw.get("resolved_at"),
            resolution_outcome=raw.get("resolution_outcome"),
        ),
        recommendation=ModelRecommendationFacet(
            is_recommendation=_is_recommendation(raw),
            target_actor_id=_optional_uuid(raw.get("target_actor_id")),
            caused_act_change_id=_optional_uuid(raw.get("caused_act_change_id")),
        ),
    )


def _raw_model(model: ModelRow | asyncpg.Record | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(model, ModelRow):
        return model.model_dump(mode="python")
    return dict(model)


def _is_prediction(raw: Mapping[str, Any]) -> bool:
    proposition = raw.get("proposition")
    prop = proposition if isinstance(proposition, Mapping) else {}
    return (
        raw.get("proposition_kind") == "prediction"
        or prop.get("kind") == "prediction"
        or raw.get("evaluate_at") is not None
        or raw.get("resolution_criteria") is not None
        or bool(raw.get("contributing_models"))
        or raw.get("resolved_at") is not None
        or raw.get("resolution_outcome") is not None
    )


def _is_recommendation(raw: Mapping[str, Any]) -> bool:
    proposition = raw.get("proposition")
    prop = proposition if isinstance(proposition, Mapping) else {}
    return (
        raw.get("proposition_kind") == "recommendation"
        or prop.get("kind") == "recommendation"
        or raw.get("target_actor_id") is not None
        or raw.get("caused_act_change_id") is not None
    )


def _uuid(value: Any) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _optional_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    return _uuid(value)


def _uuid_list(values: Any) -> list[UUID]:
    if not isinstance(values, (list, tuple)):
        return []
    return [_uuid(value) for value in values]


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return dict(value) if isinstance(value, Mapping) else None


def _str_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "BASE_MODEL_FACETS",
    "MODEL_FACET_SCHEMA_VERSION",
    "ModelCoreFacet",
    "ModelEvidenceFacet",
    "ModelFacetBundle",
    "ModelFacetName",
    "ModelPredictionFacet",
    "ModelRecommendationFacet",
    "ModelRetrievalFacet",
    "ModelRuntimeFacet",
    "ModelSemanticFacet",
    "model_facet_names",
    "split_model_facets",
]
