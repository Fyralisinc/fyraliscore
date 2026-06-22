"""Catalog of core projection factories.

The runtime registry remains extension-friendly, but core services should not
hand-roll the same projector list in each caller. This module is the single
small place that maps stable projection names to projector constructors.

Extension path: installed packages may contribute a projector through the
``company_os.projections`` entry-point group. The entry-point name is the stable
projection name; the entry point resolves to a zero-arg projector factory/class
or a projector instance. Discovery is cached and failure-isolated.
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
import logging
from collections.abc import Callable, Sequence
from typing import Any, cast

from services.domain.projections.constraints import ConstraintProjector
from services.domain.projections.employee_profiles import EmployeeProfileProjector
from services.domain.projections.resources import ResourceProjector
from services.domain.projections.runtime import ProjectionRegistry
from services.domain.projections.types import Projector


ProjectorFactory = Callable[[], Projector]

log = logging.getLogger("domain.projections.catalog")

ENTRY_POINT_GROUP = "company_os.projections"
DEFAULT_PROJECTION_NAMES = ("constraints",)

CORE_PROJECTOR_FACTORIES: dict[str, ProjectorFactory] = {
    "constraints": ConstraintProjector,
    "employee_profiles": EmployeeProfileProjector,
    "resources": ResourceProjector,
}
_REGISTERED_PROJECTOR_FACTORIES: dict[str, ProjectorFactory] = {}
_DISCOVERED_PROJECTOR_FACTORIES: dict[str, ProjectorFactory] | None = None

_REQUIRED_PROJECTOR_ATTRS = (
    "name",
    "version",
    "matches",
    "affected_subjects",
    "project_subject",
)


def register_projector_factory(
    name: str,
    factory: ProjectorFactory,
    *,
    replace: bool = False,
) -> None:
    """Register a first-party projector factory without editing core catalog code."""
    normalized = _normalize_name(name)
    if normalized in CORE_PROJECTOR_FACTORIES:
        raise ValueError(f"cannot replace core projection: {normalized}")
    if not replace and normalized in _REGISTERED_PROJECTOR_FACTORIES:
        raise ValueError(f"projection already registered: {normalized}")
    _REGISTERED_PROJECTOR_FACTORIES[normalized] = _named_factory(
        normalized,
        factory,
        source=f"registered:{normalized}",
    )


def projector_factories() -> dict[str, ProjectorFactory]:
    """Return core, registered, and discovered projector factories."""
    factories = dict(CORE_PROJECTOR_FACTORIES)
    factories.update(_REGISTERED_PROJECTOR_FACTORIES)
    for name, factory in _discover_projector_factories().items():
        if name in factories:
            log.error("projection_factory_duplicate name=%s source=entry_point", name)
            continue
        factories[name] = factory
    return factories


def available_projection_names() -> tuple[str, ...]:
    """Return known projection names in deterministic order."""
    return tuple(sorted(projector_factories()))


def projection_choices(*, include_all: bool = True) -> tuple[str, ...]:
    """Choices suitable for CLIs and config validation."""
    names = available_projection_names()
    if include_all:
        return ("all", *names)
    return names


def projectors_for(
    names: Sequence[str] | None,
    *,
    default_names: Sequence[str] = DEFAULT_PROJECTION_NAMES,
) -> list[Projector]:
    """Instantiate projectors by name, expanding ``all`` and deduping."""
    selected = [_normalize_name(name) for name in (names or default_names)]
    if "all" in selected:
        selected = list(available_projection_names())

    projectors: list[Projector] = []
    seen: set[str] = set()
    for name in selected:
        normalized = _normalize_name(name)
        if normalized in seen:
            continue
        seen.add(normalized)
        try:
            factory = projector_factories()[normalized]
        except KeyError as exc:
            raise ValueError(f"unknown projection: {normalized}") from exc
        projectors.append(factory())
    return projectors


def all_projectors() -> list[Projector]:
    """Instantiate every known core, registered, and discovered projector."""
    return projectors_for(("all",))


def build_projection_registry(
    names: Sequence[str] | None = None,
    *,
    default_names: Sequence[str] = DEFAULT_PROJECTION_NAMES,
) -> ProjectionRegistry:
    """Build a runtime registry from projection names."""
    return ProjectionRegistry(projectors_for(names, default_names=default_names))


def reset_for_tests() -> None:
    """Clear mutable projection registrations and discovery cache."""
    global _DISCOVERED_PROJECTOR_FACTORIES
    _REGISTERED_PROJECTOR_FACTORIES.clear()
    _DISCOVERED_PROJECTOR_FACTORIES = None


def _discover_projector_factories() -> dict[str, ProjectorFactory]:
    global _DISCOVERED_PROJECTOR_FACTORIES
    if _DISCOVERED_PROJECTOR_FACTORIES is not None:
        return _DISCOVERED_PROJECTOR_FACTORIES

    found: dict[str, ProjectorFactory] = {}
    try:
        entry_points = importlib_metadata.entry_points(group=ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must not break projection startup
        log.warning("projection_entry_point_discovery_failed", exc_info=True)
        _DISCOVERED_PROJECTOR_FACTORIES = found
        return found

    for ep in entry_points:
        name = _normalize_name(ep.name)
        if not name:
            log.error("projection_entry_point_bad_name source=%s", ep.name)
            continue
        if name in CORE_PROJECTOR_FACTORIES or name in _REGISTERED_PROJECTOR_FACTORIES:
            log.error("projection_entry_point_duplicate name=%s", name)
            continue
        if name in found:
            log.error("projection_entry_point_duplicate name=%s", name)
            continue
        try:
            loaded = ep.load()
        except Exception:  # noqa: BLE001 - one bad extension must not break others
            log.error("projection_entry_point_load_failed source=%s", ep.name, exc_info=True)
            continue
        factory = _factory_from_loaded(name, loaded, source=f"entry_point:{ep.name}")
        if factory is None:
            continue
        found[name] = factory
        log.info("projection_entry_point_discovered name=%s", name)

    _DISCOVERED_PROJECTOR_FACTORIES = found
    return found


def _factory_from_loaded(
    name: str,
    loaded: Any,
    *,
    source: str,
) -> ProjectorFactory | None:
    if _is_projector_instance(loaded):
        projector = _validate_projector(name, loaded, source=source)
        return lambda: projector
    if callable(loaded):
        try:
            _validate_projector(name, loaded(), source=source)
        except Exception:  # noqa: BLE001 - one bad extension must not break others
            log.error(
                "projection_entry_point_factory_invalid name=%s source=%s",
                name,
                source,
                exc_info=True,
            )
            return None
        return _named_factory(name, cast(ProjectorFactory, loaded), source=source)
    log.error("projection_entry_point_bad_type name=%s source=%s", name, source)
    return None


def _named_factory(
    name: str,
    factory: ProjectorFactory,
    *,
    source: str,
) -> ProjectorFactory:
    def _build() -> Projector:
        return _validate_projector(name, factory(), source=source)

    return _build


def _validate_projector(name: str, projector: Any, *, source: str) -> Projector:
    if not _is_projector_instance(projector):
        raise ValueError(f"projection {name!r} from {source} is not a Projector")
    if projector.name != name:
        raise ValueError(
            f"projection factory {source} returned {projector.name!r}, expected {name!r}"
        )
    return cast(Projector, projector)


def _is_projector_instance(value: Any) -> bool:
    if isinstance(value, type):
        return False
    for attr in _REQUIRED_PROJECTOR_ATTRS:
        if not hasattr(value, attr):
            return False
    return (
        isinstance(getattr(value, "name", None), str)
        and isinstance(getattr(value, "version", None), str)
        and callable(getattr(value, "matches", None))
        and callable(getattr(value, "affected_subjects", None))
        and callable(getattr(value, "project_subject", None))
    )


def _normalize_name(name: str) -> str:
    return str(name or "").strip()
