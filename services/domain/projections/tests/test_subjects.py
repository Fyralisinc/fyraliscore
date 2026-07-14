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
    assert (
        "decision_surfaces",
        f"customer:{customer_id}:decision_surface",
    ) in subjects
    assert (
        "decision_surfaces",
        f"actor:{actor_id}:decision_surface",
    ) in subjects
    assert ("employee_profiles", f"employee:{actor_id}:profile") in subjects
    assert ("customers", f"customer:{customer_id}:customers") in subjects


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


def test_entity_subject_resolvers_use_entity_scope_and_text_fallback() -> None:
    tenant_id = uuid7()
    commitment_id = uuid7()
    goal_id = uuid7()
    decision_id = uuid7()
    seed = ProjectionSubjectSeed(
        tenant_id=tenant_id,
        seed_natural_text="The roadmap goal needs an approval decision.",
        seed_entities=(
            {"type": "commitment", "id": str(commitment_id)},
            {"type": "goal", "id": str(goal_id)},
            {"type": "decision", "id": str(decision_id)},
        ),
    )

    subjects = resolve_projection_subjects(seed)

    assert ("commitments", f"commitment:{commitment_id}:commitments") in subjects
    assert ("goals", f"goal:{goal_id}:goals") in subjects
    assert ("decisions", f"decision:{decision_id}:decisions") in subjects

    fallback_subjects = resolve_projection_subjects(
        ProjectionSubjectSeed(
            tenant_id=tenant_id,
            seed_natural_text="A commitment and customer renewal decision need review.",
        ),
        resolver_names=["commitments", "customers", "decisions"],
    )

    assert fallback_subjects == [
        ("commitments", f"tenant:{tenant_id}:commitments"),
        ("customers", f"tenant:{tenant_id}:customers"),
        ("decisions", f"tenant:{tenant_id}:decisions"),
    ]


def test_core_subject_resolvers_dedupe_malformed_subjects() -> None:
    tenant_id = uuid7()

    def _resolver(seed: ProjectionSubjectSeed):
        return [
            ("forecasts", "company:forecasts"),
            ("forecasts", "company:forecasts"),
            ("", "missing-projection"),
            ("forecasts", ""),
        ]

    register_subject_resolver("forecasts", _resolver)

    assert resolve_projection_subjects(
        ProjectionSubjectSeed(tenant_id=tenant_id),
        resolver_names=["forecasts"],
    ) == [("forecasts", "company:forecasts")]


def test_register_subject_resolver_extends_resolver_set() -> None:
    tenant_id = uuid7()

    def _resolver(seed: ProjectionSubjectSeed):
        return [("forecasts", f"tenant:{seed.tenant_id}:forecasts")]

    register_subject_resolver("forecasts", _resolver)

    assert "forecasts" in available_subject_resolver_names()
    assert resolve_projection_subjects(
        ProjectionSubjectSeed(tenant_id=tenant_id),
        resolver_names=["forecasts"],
    ) == [("forecasts", f"tenant:{tenant_id}:forecasts")]


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
            "forecasts",
            lambda seed: [("forecasts", f"tenant:{seed.tenant_id}:forecasts")],
        )

    def _entry_points(group=None):
        return [_FakeEP("forecasts", _factory)] if group == ENTRY_POINT_GROUP else []

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert resolve_projection_subjects(
        ProjectionSubjectSeed(tenant_id=tenant_id),
        resolver_names=["forecasts"],
    ) == [("forecasts", f"tenant:{tenant_id}:forecasts")]


def test_bad_entry_point_subject_resolver_is_isolated(monkeypatch) -> None:
    class _Exploding:
        name = "broken"

        def load(self):
            raise ImportError("cannot import subject resolver")

    def _factory() -> ProjectionSubjectResolver:
        return ProjectionSubjectResolver(
            "forecasts",
            lambda seed: [("forecasts", f"tenant:{seed.tenant_id}:forecasts")],
        )

    def _entry_points(group=None):
        if group != ENTRY_POINT_GROUP:
            return []
        return [_Exploding(), _FakeEP("forecasts", _factory)]

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert available_subject_resolver_names() == (
        "commitments",
        "constraints",
        "customers",
        "decision_surfaces",
        "decisions",
        "employee_profiles",
        "forecasts",
        "goals",
        "resources",
    )


def test_resolver_runtime_failure_is_isolated() -> None:
    tenant_id = uuid7()

    def _bad(seed: ProjectionSubjectSeed):
        raise RuntimeError("resolver failed")

    def _good(seed: ProjectionSubjectSeed):
        return [("forecasts", f"tenant:{seed.tenant_id}:forecasts")]

    register_subject_resolver("bad", _bad)
    register_subject_resolver("forecasts", _good)

    assert projection_subject_candidates(ProjectionSubjectSeed(tenant_id=tenant_id))[-1] == (
        "forecasts",
        f"tenant:{tenant_id}:forecasts",
    )
