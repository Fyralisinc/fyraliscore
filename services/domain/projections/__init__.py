"""Rebuildable operating projections over canonical Models."""
from services.domain.projections.catalog import (
    ENTRY_POINT_GROUP,
    all_projectors,
    available_projection_names,
    build_projection_registry,
    projection_choices,
    projector_factories,
    projectors_for,
    register_projector_factory,
)
from services.domain.projections.constraints import ConstraintProjector
from services.domain.projections.employee_profiles import EmployeeProfileProjector
from services.domain.projections.repo import (
    ProjectionContext,
    ProjectionRecord,
    ProjectionRepo,
    ProjectionStaleness,
)
from services.domain.projections.resources import ResourceProjector
from services.domain.projections.runtime import ProjectionRegistry, ProjectionRunner
from services.domain.projections.runtime import ProjectionRunError, ProjectionRunReport
from services.domain.projections.subjects import (
    ProjectionSubject,
    ProjectionSubjectResolver,
    ProjectionSubjectSeed,
    available_subject_resolver_names,
    register_subject_resolver,
    resolve_projection_subjects,
)
from services.domain.projections.types import ModelEvent, ProjectionSnapshot

__all__ = [
    "ConstraintProjector",
    "EmployeeProfileProjector",
    "ENTRY_POINT_GROUP",
    "ModelEvent",
    "ProjectionContext",
    "ProjectionRecord",
    "ProjectionRegistry",
    "ProjectionRunError",
    "ProjectionRunReport",
    "ProjectionRepo",
    "ProjectionRunner",
    "ProjectionSnapshot",
    "ProjectionSubject",
    "ProjectionSubjectResolver",
    "ProjectionSubjectSeed",
    "ProjectionStaleness",
    "ResourceProjector",
    "all_projectors",
    "available_projection_names",
    "available_subject_resolver_names",
    "build_projection_registry",
    "projection_choices",
    "projector_factories",
    "projectors_for",
    "register_projector_factory",
    "register_subject_resolver",
    "resolve_projection_subjects",
]
