"""Projection subject inference.

Projectors materialize snapshots; subject resolvers decide which snapshots are
relevant for a retrieval seed. Keeping this next to projections means extension
projections can teach retrieval how to find their own subjects without editing
retrieval code.
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


ProjectionSubject = tuple[str, str]
SubjectResolverFn = Callable[["ProjectionSubjectSeed"], Sequence[ProjectionSubject]]

log = logging.getLogger("domain.projections.subjects")

ENTRY_POINT_GROUP = "company_os.projection_subject_resolvers"

_CONSTRAINT_PROJECTION = "constraints"
_EMPLOYEE_PROFILE_PROJECTION = "employee_profiles"
_RESOURCE_PROJECTION = "resources"

_FINANCIAL_TERMS = {
    "budget",
    "burn",
    "capital",
    "cash",
    "finance",
    "financial",
    "funding",
    "revenue",
    "runway",
}
_CAPACITY_TERMS = {
    "capacity",
    "employee",
    "employees",
    "headcount",
    "hiring",
    "onboarding",
    "people",
    "team",
    "workload",
}
_RELATIONAL_TERMS = {
    "churn",
    "customer",
    "customers",
    "partner",
    "relationship",
    "renewal",
    "retention",
    "trust",
    "vendor",
}
_INFRASTRUCTURE_TERMS = {
    "aws",
    "deployment",
    "grafana",
    "incident",
    "infrastructure",
    "latency",
    "production",
    "reliability",
}
_REGULATORY_TERMS = {
    "audit",
    "compliance",
    "policy",
    "regulatory",
    "security",
}
_IP_TERMS = {
    "brand",
    "code",
    "ip",
    "patent",
    "product",
    "source_code",
}
_CONSTRAINT_TERMS = {
    "blocked",
    "blocker",
    "bottleneck",
    "constraint",
    "dependency",
    "obligation",
    "risk",
    "scarcity",
}


@dataclass(frozen=True)
class ProjectionSubjectSeed:
    tenant_id: UUID
    seed_natural_text: str | None = None
    seed_entities: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    scope_actors: tuple[UUID, ...] = field(default_factory=tuple)
    subkind: str | None = None
    topology_event_kind: str | None = None
    seed_signature: dict[str, Any] | None = None
    region_spec: dict[str, Any] | None = None


@dataclass(frozen=True)
class ProjectionSubjectResolver:
    name: str
    fn: SubjectResolverFn

    def resolve(self, seed: ProjectionSubjectSeed) -> tuple[ProjectionSubject, ...]:
        return tuple(self.fn(seed))


CORE_SUBJECT_RESOLVERS: dict[str, ProjectionSubjectResolver]
_REGISTERED_SUBJECT_RESOLVERS: dict[str, ProjectionSubjectResolver] = {}
_DISCOVERED_SUBJECT_RESOLVERS: dict[str, ProjectionSubjectResolver] | None = None


def register_subject_resolver(
    name: str,
    fn: SubjectResolverFn,
    *,
    replace: bool = False,
) -> None:
    """Register a first-party subject resolver."""
    normalized = _normalize_name(name)
    if normalized in CORE_SUBJECT_RESOLVERS:
        raise ValueError(f"cannot replace core subject resolver: {normalized}")
    if not replace and normalized in _REGISTERED_SUBJECT_RESOLVERS:
        raise ValueError(f"subject resolver already registered: {normalized}")
    _REGISTERED_SUBJECT_RESOLVERS[normalized] = ProjectionSubjectResolver(
        normalized,
        fn,
    )


def subject_resolvers() -> dict[str, ProjectionSubjectResolver]:
    resolvers = dict(CORE_SUBJECT_RESOLVERS)
    resolvers.update(_REGISTERED_SUBJECT_RESOLVERS)
    for name, resolver in _discover_subject_resolvers().items():
        if name in resolvers:
            log.error("projection_subject_resolver_duplicate name=%s", name)
            continue
        resolvers[name] = resolver
    return resolvers


def available_subject_resolver_names() -> tuple[str, ...]:
    return tuple(sorted(subject_resolvers()))


def resolve_projection_subjects(
    seed: ProjectionSubjectSeed,
    *,
    resolver_names: Sequence[str] | None = None,
) -> list[ProjectionSubject]:
    """Resolve candidate projection subjects, deduped in resolver order."""
    selected = [_normalize_name(name) for name in resolver_names or ()]
    resolvers = subject_resolvers()
    if selected:
        ordered = []
        for name in selected:
            try:
                ordered.append(resolvers[name])
            except KeyError as exc:
                raise ValueError(f"unknown subject resolver: {name}") from exc
    else:
        ordered = [resolvers[name] for name in sorted(resolvers)]

    out: list[ProjectionSubject] = []
    seen: set[ProjectionSubject] = set()
    for resolver in ordered:
        try:
            subjects = resolver.resolve(seed)
        except Exception:  # noqa: BLE001 - one bad resolver must not break retrieval
            log.error(
                "projection_subject_resolver_failed name=%s",
                resolver.name,
                exc_info=True,
            )
            continue
        for subject in subjects:
            normalized = _normalize_subject(subject)
            if normalized is None or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
    return out


def projection_subject_candidates(seed: ProjectionSubjectSeed) -> list[ProjectionSubject]:
    """Compatibility alias for callers that expect candidate wording."""
    return resolve_projection_subjects(seed)


def reset_for_tests() -> None:
    global _DISCOVERED_SUBJECT_RESOLVERS
    _REGISTERED_SUBJECT_RESOLVERS.clear()
    _DISCOVERED_SUBJECT_RESOLVERS = None


def _constraint_subjects(seed: ProjectionSubjectSeed) -> list[ProjectionSubject]:
    subjects: list[ProjectionSubject] = []
    seen: set[ProjectionSubject] = set()
    text = _token_text(seed)

    for entity in seed.seed_entities:
        entity_type = str(entity.get("type") or "").strip()
        entity_id = str(entity.get("id") or "").strip()
        if not entity_type or not entity_id:
            continue
        for variant in _entity_type_variants(entity_type):
            _append_subject(
                subjects,
                seen,
                _CONSTRAINT_PROJECTION,
                f"{variant}:{entity_id}:constraints",
            )

    if _contains_any(text, _FINANCIAL_TERMS):
        _append_subject(subjects, seen, _CONSTRAINT_PROJECTION, "company:runway")
        _append_subject(
            subjects,
            seen,
            _CONSTRAINT_PROJECTION,
            "company:financial_capacity",
        )
    if _contains_any(text, _CAPACITY_TERMS):
        _append_subject(subjects, seen, _CONSTRAINT_PROJECTION, "company:capacity")
    if _contains_any(text, _CONSTRAINT_TERMS):
        _append_subject(
            subjects,
            seen,
            _CONSTRAINT_PROJECTION,
            f"tenant:{seed.tenant_id}:constraints",
        )
    return subjects


def _resource_subjects(seed: ProjectionSubjectSeed) -> list[ProjectionSubject]:
    subjects: list[ProjectionSubject] = []
    seen: set[ProjectionSubject] = set()
    text = _token_text(seed)

    for entity in seed.seed_entities:
        entity_type = str(entity.get("type") or "").strip()
        entity_id = str(entity.get("id") or "").strip()
        if not entity_type or not entity_id:
            continue
        for variant in _entity_type_variants(entity_type):
            _append_subject(
                subjects,
                seen,
                _RESOURCE_PROJECTION,
                f"{variant}:{entity_id}:resources",
            )

    if _contains_any(text, _FINANCIAL_TERMS):
        _append_subject(subjects, seen, _RESOURCE_PROJECTION, "company:financial")
    if _contains_any(text, _CAPACITY_TERMS):
        _append_subject(subjects, seen, _RESOURCE_PROJECTION, "company:capacity")
    if _contains_any(text, _RELATIONAL_TERMS):
        _append_subject(subjects, seen, _RESOURCE_PROJECTION, "company:relational")
    if _contains_any(text, _INFRASTRUCTURE_TERMS):
        _append_subject(subjects, seen, _RESOURCE_PROJECTION, "company:infrastructure")
    if _contains_any(text, _REGULATORY_TERMS):
        _append_subject(subjects, seen, _RESOURCE_PROJECTION, "company:regulatory")
    if _contains_any(text, _IP_TERMS):
        _append_subject(subjects, seen, _RESOURCE_PROJECTION, "company:ip")
    if "resource" in text or "resources" in text:
        _append_subject(
            subjects,
            seen,
            _RESOURCE_PROJECTION,
            f"tenant:{seed.tenant_id}:resources",
        )
    return subjects


def _employee_profile_subjects(seed: ProjectionSubjectSeed) -> list[ProjectionSubject]:
    subjects: list[ProjectionSubject] = []
    seen: set[ProjectionSubject] = set()
    text = _token_text(seed)

    for actor_id in seed.scope_actors:
        _append_subject(
            subjects,
            seen,
            _EMPLOYEE_PROFILE_PROJECTION,
            f"employee:{actor_id}:profile",
        )

    for entity in seed.seed_entities:
        entity_type = str(entity.get("type") or "").strip().casefold()
        entity_id = str(entity.get("id") or "").strip()
        if entity_type not in {"actor", "employee", "person"} or not entity_id:
            continue
        _append_subject(
            subjects,
            seen,
            _EMPLOYEE_PROFILE_PROJECTION,
            f"employee:{entity_id}:profile",
        )

    if subjects:
        return subjects
    if _contains_any(text, _CAPACITY_TERMS) and "profile" in text:
        _append_subject(
            subjects,
            seen,
            _EMPLOYEE_PROFILE_PROJECTION,
            f"tenant:{seed.tenant_id}:employee_profiles",
        )
    return subjects


CORE_SUBJECT_RESOLVERS = {
    "constraints": ProjectionSubjectResolver("constraints", _constraint_subjects),
    "employee_profiles": ProjectionSubjectResolver(
        "employee_profiles",
        _employee_profile_subjects,
    ),
    "resources": ProjectionSubjectResolver("resources", _resource_subjects),
}


def _discover_subject_resolvers() -> dict[str, ProjectionSubjectResolver]:
    global _DISCOVERED_SUBJECT_RESOLVERS
    if _DISCOVERED_SUBJECT_RESOLVERS is not None:
        return _DISCOVERED_SUBJECT_RESOLVERS

    found: dict[str, ProjectionSubjectResolver] = {}
    try:
        entry_points = importlib_metadata.entry_points(group=ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must not break retrieval startup
        log.warning("projection_subject_resolver_discovery_failed", exc_info=True)
        _DISCOVERED_SUBJECT_RESOLVERS = found
        return found

    for ep in entry_points:
        name = _normalize_name(ep.name)
        if not name:
            log.error("projection_subject_resolver_bad_name source=%s", ep.name)
            continue
        if name in CORE_SUBJECT_RESOLVERS or name in _REGISTERED_SUBJECT_RESOLVERS:
            log.error("projection_subject_resolver_duplicate name=%s", name)
            continue
        if name in found:
            log.error("projection_subject_resolver_duplicate name=%s", name)
            continue
        try:
            loaded = ep.load()
            resolver = _resolver_from_loaded(name, loaded, source=f"entry_point:{ep.name}")
        except Exception:  # noqa: BLE001 - one bad extension must not break others
            log.error(
                "projection_subject_resolver_load_failed source=%s",
                ep.name,
                exc_info=True,
            )
            continue
        if resolver is None:
            continue
        found[name] = resolver
        log.info("projection_subject_resolver_discovered name=%s", name)

    _DISCOVERED_SUBJECT_RESOLVERS = found
    return found


def _resolver_from_loaded(
    name: str,
    loaded: Any,
    *,
    source: str,
) -> ProjectionSubjectResolver | None:
    if isinstance(loaded, ProjectionSubjectResolver):
        return _validate_resolver(name, loaded, source=source)
    if callable(loaded):
        resolver = loaded()
        if not isinstance(resolver, ProjectionSubjectResolver):
            log.error(
                "projection_subject_resolver_bad_type name=%s source=%s",
                name,
                source,
            )
            return None
        return _validate_resolver(name, resolver, source=source)
    log.error(
        "projection_subject_resolver_bad_type name=%s source=%s",
        name,
        source,
    )
    return None


def _validate_resolver(
    name: str,
    resolver: ProjectionSubjectResolver,
    *,
    source: str,
) -> ProjectionSubjectResolver:
    if resolver.name != name:
        raise ValueError(
            f"subject resolver {source} returned {resolver.name!r}, expected {name!r}"
        )
    return resolver


def _as_json_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except TypeError:
        return str(value)


def _token_text(seed: ProjectionSubjectSeed) -> str:
    parts = [
        seed.seed_natural_text or "",
        seed.subkind or "",
        seed.topology_event_kind or "",
        _as_json_text(seed.seed_signature),
        _as_json_text(seed.region_spec),
    ]
    return " ".join(part for part in parts if part).casefold()


def _contains_any(text: str, terms: set[str]) -> bool:
    return any(term in text for term in terms)


def _append_subject(
    subjects: list[ProjectionSubject],
    seen: set[ProjectionSubject],
    projection_name: str,
    subject_key: str,
) -> None:
    key = (projection_name, subject_key)
    if key in seen:
        return
    seen.add(key)
    subjects.append(key)


def _entity_type_variants(entity_type: str) -> tuple[str, ...]:
    normalized = entity_type.strip()
    if normalized == "customer_resource":
        return ("customer", "customer_resource")
    return (normalized,) if normalized else ()


def _normalize_subject(subject: ProjectionSubject) -> ProjectionSubject | None:
    if not isinstance(subject, tuple) or len(subject) != 2:
        return None
    projection_name = str(subject[0] or "").strip()
    subject_key = str(subject[1] or "").strip()
    if not projection_name or not subject_key:
        return None
    return (projection_name, subject_key)


def _normalize_name(name: str) -> str:
    return str(name or "").strip()
