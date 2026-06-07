"""Dependency-safe batch planning for Model inserts.

This is intentionally a planner, not a bypass around ``ModelsRepo``.
It pre-constructs all Model drafts, assigns stable ids, builds the
intra-batch dependency graph, rejects cycles before any write, and
groups inserts into dependency strata.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence
from uuid import UUID

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate

from services.models.constructor import ConstructedModel, construct_model


@dataclass(frozen=True, slots=True)
class PlannedModel:
    """One constructed Model plus its batch dependency metadata."""

    index: int
    id: UUID
    constructed: ConstructedModel
    depends_on: tuple[UUID, ...]


@dataclass(frozen=True, slots=True)
class ModelBatchPlan:
    """Topologically sorted plan for a batch of Model inserts."""

    models: tuple[PlannedModel, ...]
    strata: tuple[tuple[PlannedModel, ...], ...]

    @property
    def ids(self) -> tuple[UUID, ...]:
        return tuple(model.id for model in self.models)


def plan_model_batch(proposed: Sequence[ModelCreate]) -> ModelBatchPlan:
    """Construct and dependency-sort a batch of Models.

    Dependencies are intra-batch references through:
      * supporting_model_ids
      * contributing_models
      * situation proposition member_model_ids

    External references are left alone and validated by the existing repo
    insert path against the live database.
    """

    prepared: list[tuple[int, UUID, ConstructedModel]] = []
    seen_ids: set[UUID] = set()
    duplicate_ids: list[str] = []

    for index, raw in enumerate(proposed):
        model_id = raw.id or uuid7()
        if model_id in seen_ids:
            duplicate_ids.append(str(model_id))
        seen_ids.add(model_id)
        constructed = construct_model(raw.model_copy(update={"id": model_id}))
        prepared.append((index, model_id, constructed))

    if duplicate_ids:
        raise ValidationError(
            "batch contains duplicate Model ids",
            field="models.id",
            duplicate_ids=duplicate_ids,
        )

    batch_ids = {model_id for _, model_id, _ in prepared}
    planned: list[PlannedModel] = []
    for index, model_id, constructed in prepared:
        dependencies = _intra_batch_dependencies(
            constructed.proposed,
            model_id=model_id,
            batch_ids=batch_ids,
        )
        planned.append(PlannedModel(
            index=index,
            id=model_id,
            constructed=constructed,
            depends_on=tuple(sorted(dependencies, key=str)),
        ))

    strata = _topological_strata(planned)
    return ModelBatchPlan(
        models=tuple(planned),
        strata=tuple(tuple(stratum) for stratum in strata),
    )


def _intra_batch_dependencies(
    proposed: ModelCreate,
    *,
    model_id: UUID,
    batch_ids: set[UUID],
) -> set[UUID]:
    dependencies: set[UUID] = set()

    def add(raw: Any, *, field: str) -> None:
        if raw is None:
            return
        try:
            dep_id = UUID(str(raw))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"{field} must contain UUID values",
                field=field,
                value=str(raw),
            ) from exc
        if dep_id == model_id:
            raise ValidationError(
                "Model batch contains a self-dependency",
                field=field,
                model_id=str(model_id),
            )
        if dep_id in batch_ids:
            dependencies.add(dep_id)

    for dep in proposed.supporting_model_ids or ():
        add(dep, field="supporting_model_ids")
    for dep in proposed.contributing_models or ():
        add(dep, field="contributing_models")

    prop = proposed.proposition if isinstance(proposed.proposition, dict) else {}
    if prop.get("claim_role") == "situation" or prop.get("legacy_kind") == "situation":
        for dep in prop.get("member_model_ids") or ():
            add(dep, field="proposition.member_model_ids")

    return dependencies


def _topological_strata(planned: Sequence[PlannedModel]) -> list[list[PlannedModel]]:
    remaining: dict[UUID, PlannedModel] = {model.id: model for model in planned}
    dependencies: dict[UUID, set[UUID]] = {
        model.id: set(model.depends_on)
        for model in planned
    }
    strata: list[list[PlannedModel]] = []
    emitted: set[UUID] = set()

    while remaining:
        ready_ids = [
            model_id for model_id, deps in dependencies.items()
            if model_id in remaining and deps <= emitted
        ]
        if not ready_ids:
            cycle_ids = sorted(str(model_id) for model_id in remaining)
            raise ValidationError(
                "Model batch contains an intra-batch dependency cycle",
                field="models",
                model_ids=cycle_ids,
            )
        ready_ids.sort(key=lambda model_id: remaining[model_id].index)
        stratum = [remaining.pop(model_id) for model_id in ready_ids]
        strata.append(stratum)
        emitted.update(ready_ids)

    return strata


__all__ = [
    "ModelBatchPlan",
    "PlannedModel",
    "plan_model_batch",
]
