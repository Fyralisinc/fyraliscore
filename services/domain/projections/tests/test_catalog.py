from __future__ import annotations

import pytest

from services.domain.projections.catalog import (
    ENTRY_POINT_GROUP,
    all_projectors,
    available_projection_names,
    build_projection_registry,
    importlib_metadata,
    projection_choices,
    projectors_for,
    register_projector_factory,
    reset_for_tests,
)
from services.domain.projections.types import ModelEvent, ProjectionSnapshot
from services.domain.projections.constraints import ConstraintProjector
from services.domain.projections.employee_profiles import EmployeeProfileProjector
from services.domain.projections.resources import ResourceProjector


class _FakeEP:
    def __init__(self, name: str, obj) -> None:
        self.name = name
        self._obj = obj

    def load(self):
        return self._obj


class _DummyProjector:
    version = "v1"

    def __init__(self, name: str = "customers") -> None:
        self.name = name

    def matches(self, event: ModelEvent) -> bool:
        return False

    async def affected_subjects(self, conn, event: ModelEvent):
        return []

    async def project_subject(
        self,
        conn,
        *,
        tenant_id,
        subject_key: str,
        source_event_ids,
    ) -> ProjectionSnapshot:
        return ProjectionSnapshot(
            tenant_id=tenant_id,
            projection_name=self.name,
            projection_version=self.version,
            subject_key=subject_key,
            payload={},
            source_event_ids=tuple(source_event_ids),
        )


@pytest.fixture(autouse=True)
def _isolate_catalog(monkeypatch):
    monkeypatch.setattr(importlib_metadata, "entry_points", lambda group=None: [])
    reset_for_tests()
    yield
    reset_for_tests()


def test_available_projection_names_are_deterministic() -> None:
    assert available_projection_names() == (
        "constraints",
        "employee_profiles",
        "resources",
    )
    assert projection_choices() == (
        "all",
        "constraints",
        "employee_profiles",
        "resources",
    )


def test_projectors_for_defaults_to_constraints() -> None:
    projectors = projectors_for(None)

    assert len(projectors) == 1
    assert isinstance(projectors[0], ConstraintProjector)


def test_projectors_for_all_returns_core_projectors() -> None:
    projectors = all_projectors()

    assert {projector.name for projector in projectors} == {
        "constraints",
        "employee_profiles",
        "resources",
    }
    assert any(isinstance(projector, EmployeeProfileProjector) for projector in projectors)
    assert any(isinstance(projector, ResourceProjector) for projector in projectors)


def test_projectors_for_dedupes_names() -> None:
    projectors = projectors_for(["constraints", "constraints"])

    assert [projector.name for projector in projectors] == ["constraints"]


def test_projectors_for_rejects_unknown_projection() -> None:
    with pytest.raises(ValueError):
        projectors_for(["missing"])


def test_build_projection_registry_uses_catalog_dedupe() -> None:
    registry = build_projection_registry(["constraints", "constraints"])

    assert [projector.name for projector in registry.projectors] == ["constraints"]


def test_register_projector_factory_extends_catalog() -> None:
    register_projector_factory("customers", lambda: _DummyProjector("customers"))

    assert available_projection_names() == (
        "constraints",
        "customers",
        "employee_profiles",
        "resources",
    )
    assert [projector.name for projector in projectors_for(["customers"])] == [
        "customers"
    ]
    assert {projector.name for projector in all_projectors()} == {
        "constraints",
        "customers",
        "employee_profiles",
        "resources",
    }


def test_register_projector_factory_rejects_core_name() -> None:
    with pytest.raises(ValueError):
        register_projector_factory("constraints", lambda: _DummyProjector("constraints"))


def test_entry_point_projector_factory_is_discovered(monkeypatch) -> None:
    def _factory() -> _DummyProjector:
        return _DummyProjector("customers")

    def _entry_points(group=None):
        return [_FakeEP("customers", _factory)] if group == ENTRY_POINT_GROUP else []

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert "customers" in available_projection_names()
    assert [projector.name for projector in projectors_for(["customers"])] == [
        "customers"
    ]


def test_entry_point_projector_instance_is_discovered(monkeypatch) -> None:
    def _entry_points(group=None):
        if group != ENTRY_POINT_GROUP:
            return []
        return [_FakeEP("customers", _DummyProjector("customers"))]

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert [projector.name for projector in projectors_for(["customers"])] == [
        "customers"
    ]


def test_bad_entry_point_is_isolated(monkeypatch) -> None:
    class _Exploding:
        name = "broken"

        def load(self):
            raise ImportError("cannot import projection extension")

    def _entry_points(group=None):
        if group != ENTRY_POINT_GROUP:
            return []
        return [
            _Exploding(),
            _FakeEP("customers", lambda: _DummyProjector("customers")),
        ]

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert available_projection_names() == (
        "constraints",
        "customers",
        "employee_profiles",
        "resources",
    )


def test_bad_entry_point_factory_is_skipped(monkeypatch) -> None:
    def _entry_points(group=None):
        if group != ENTRY_POINT_GROUP:
            return []
        return [
            _FakeEP("broken", lambda: object()),
            _FakeEP("customers", lambda: _DummyProjector("customers")),
        ]

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)
    reset_for_tests()

    assert available_projection_names() == (
        "constraints",
        "customers",
        "employee_profiles",
        "resources",
    )
