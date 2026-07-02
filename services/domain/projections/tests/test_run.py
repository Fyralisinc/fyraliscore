from __future__ import annotations

import pytest

import services.domain.projections.catalog as catalog
from services.domain.projections.constraints import ConstraintProjector
from services.domain.projections.decision_surfaces import DecisionSurfaceProjector
from services.domain.projections.employee_profiles import EmployeeProfileProjector
from services.domain.projections.entity_surfaces import (
    CommitmentProjector,
    CustomerProjector,
    DecisionProjector,
    GoalProjector,
)
from services.domain.projections.resources import ResourceProjector
from services.domain.projections.run import _projectors_for, parse_args


@pytest.fixture(autouse=True)
def _isolate_catalog(monkeypatch):
    monkeypatch.setattr(catalog.importlib_metadata, "entry_points", lambda group=None: [])
    catalog.reset_for_tests()
    yield
    catalog.reset_for_tests()


def test_projectors_for_defaults_to_constraints() -> None:
    projectors = _projectors_for([])

    assert len(projectors) == 1
    assert isinstance(projectors[0], ConstraintProjector)


def test_projectors_for_all_dedupes_registered_projectors() -> None:
    projectors = _projectors_for(["all", "constraints"])

    assert {projector.name for projector in projectors} == {
        "commitments",
        "constraints",
        "customers",
        "decisions",
        "decision_surfaces",
        "employee_profiles",
        "goals",
        "resources",
    }
    assert any(isinstance(projector, CommitmentProjector) for projector in projectors)
    assert any(isinstance(projector, CustomerProjector) for projector in projectors)
    assert any(isinstance(projector, DecisionProjector) for projector in projectors)
    assert any(isinstance(projector, DecisionSurfaceProjector) for projector in projectors)
    assert any(isinstance(projector, EmployeeProfileProjector) for projector in projectors)
    assert any(isinstance(projector, GoalProjector) for projector in projectors)
    assert any(isinstance(projector, ResourceProjector) for projector in projectors)


def test_projectors_for_rejects_unknown_projection() -> None:
    with pytest.raises(ValueError):
        _projectors_for(["missing"])


def test_parse_args_defaults_projection_to_none_for_runtime_default() -> None:
    args = parse_args([])

    assert args.projection is None
    assert args.limit == 500
    assert args.watch is False
