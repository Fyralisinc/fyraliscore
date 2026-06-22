from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.domain.projections.subjects import (
    ENTRY_POINT_GROUP,
    ProjectionSubjectResolver,
    ProjectionSubjectSeed,
    available_subject_resolver_names,
    importlib_metadata,
    projection_subject_candidates,
    register_subject_resolver,
    reset_for_tests,
    resolve_projection_subjects,
)


class _FakeEP:
    def __init__(self, name: str, obj) -> None:
        self.name = name
        self._obj = obj

    def load(self):
        return self._obj


@pytest.fixture(autouse=True)
def _isolate_subject_resolvers(monkeypatch):
    monkeypatch.setattr(importlib_metadata, "entry_points", lambda group=None: [])
    reset_for_tests()
    yield
    reset_for_tests()


def test_core_subject_resolvers_use_text_and_entity_scope() -> None:
    tenant_id = uuid7()
    customer_id = uuid7()
    actor_id = uuid7()
    seed = ProjectionSubjectSeed(
        tenant_id=tenant_id,
        seed_natural_text="Cash runway and hiring capacity affect customer renewal.",
        seed_entities=({"type": "customer_resource", "id": str(customer_id)},),
        scope_actors=(actor_id,),
    )

    subjects = resolve_projection_subjects(seed)

    assert ("constraints", "company:runway") in subjects
    assert ("constraints", "company:financial_capacity") in subjects
    assert ("constraints", "company:capacity") in subjects
    assert ("resources", "company:financial") in subjects
    assert ("resources", "company:capacity") in subjects
    assert ("resources", "company:relational") in subjects
    assert ("constraints", f"customer:{customer_id}:constraints") in subjects
    assert ("resources", f"customer:{customer_id}:resources") in subjects
    assert ("employee_profiles", f"employee:{actor_id}:profile") in subjects


def test_employee_profile_subject_resolver_uses_actor_and_employee_entity_scope() -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    employee_entity_id = uuid7()
    seed = ProjectionSubjectSeed(
        tenant_id=tenant_id,
        seed_entities=({"type": "employee", "id": str(employee_entity_id)},),
        scope_actors=(actor_id,),
    )

    subjects = resolve_projection_subjects(seed, resolver_names=["employee_profiles"])

    assert subjects == [
        ("employee_profiles", f"employee:{actor_id}:profile"),
        ("employee_profiles", f"employee:{employee_entity_id}:profile"),
    ]


def test_core_subject_resolvers_dedupe_malformed_subjects() -> None:
    tenant_id = uuid7()

    def _resolver(seed: ProjectionSubjectSeed):
        return [
            ("customers", "company:customers"),
            ("customers", "company:customers"),
            ("", "missing-projection"),
            ("customers", ""),
        ]

    register_subject_resolver("customers", _resolver)

    assert resolve_projection_subjects(
        ProjectionSubjectSeed(tenant_id=tenant_id),
        resolver_names=["customers"],
    ) == [("customers", "company:customers")]


def test_register_subject_resolver_extends_resolver_set() -> None:
    tenant_id = uuid7()

    def _resolver(seed: ProjectionSubjectSeed):
        return [("customers", f"tenant:{seed.tenant_id}:customers")]

    register_subject_resolver("customers", _resolver)

    assert "customers" in available_subject_resolver_names()
    assert resolve_projection_subjects(
        ProjectionSubjectSeed(tenant_id=tenant_id),
        resolver_names=["customers"],
    ) == [("customers", f"tenant:{tenant_id}:customers")]


def test_register_subject_resolver_rejects_core_name() -> None:
    with pytest.raises(ValueError):
        register_subject_resolver("constraints", lambda seed: [])


def test_unknown_subject_resolver_is_rejected() -> None:
    with pytest.raises(ValueError):
        resolve_projection_subjects(
            ProjectionSubjectSeed(tenant_id=uuid7()),
            resolver_names=["missing"],
        )


def test_entry_point_subject_resolver_is_discovered(monkeypatch) -> None:
    tenant_id = uuid7()

    def _factory() -> ProjectionSubjectResolver:
        return ProjectionSubjectResolver(
            "customers",
            lambda seed: [("customers", f"tenant:{seed.tenant_id}:customers")],
        )

    def _entry_points(group=None):
        return [_FakeEP("customers", _factory)] if group == ENTRY_POINT_GROUP else []

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert resolve_projection_subjects(
        ProjectionSubjectSeed(tenant_id=tenant_id),
        resolver_names=["customers"],
    ) == [("customers", f"tenant:{tenant_id}:customers")]


def test_bad_entry_point_subject_resolver_is_isolated(monkeypatch) -> None:
    class _Exploding:
        name = "broken"

        def load(self):
            raise ImportError("cannot import subject resolver")

    def _factory() -> ProjectionSubjectResolver:
        return ProjectionSubjectResolver(
            "customers",
            lambda seed: [("customers", f"tenant:{seed.tenant_id}:customers")],
        )

    def _entry_points(group=None):
        if group != ENTRY_POINT_GROUP:
            return []
        return [_Exploding(), _FakeEP("customers", _factory)]

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert available_subject_resolver_names() == (
        "constraints",
        "customers",
        "employee_profiles",
        "resources",
    )


def test_resolver_runtime_failure_is_isolated() -> None:
    tenant_id = uuid7()

    def _bad(seed: ProjectionSubjectSeed):
        raise RuntimeError("resolver failed")

    def _good(seed: ProjectionSubjectSeed):
        return [("customers", f"tenant:{seed.tenant_id}:customers")]

    register_subject_resolver("bad", _bad)
    register_subject_resolver("customers", _good)

    assert projection_subject_candidates(ProjectionSubjectSeed(tenant_id=tenant_id))[-1] == (
        "customers",
        f"tenant:{tenant_id}:customers",
    )
