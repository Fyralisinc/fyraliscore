from __future__ import annotations

from pathlib import Path

from scripts.check_cross_tenant_boundary_tests import (
    BoundaryTestRequirement,
    REQUIREMENTS,
    validate_cross_tenant_boundary_tests,
)


def test_checked_in_cross_tenant_boundary_contract_passes() -> None:
    assert validate_cross_tenant_boundary_tests() == []


def test_detects_missing_boundary_test_file(tmp_path: Path) -> None:
    requirement = BoundaryTestRequirement(
        layer="gateway",
        path="missing/test_gateway.py",
        test_name="test_tenant_a_cannot_see_tenant_b_observations",
    )

    violations = validate_cross_tenant_boundary_tests(
        repo_root=tmp_path,
        requirements=(requirement,),
        enforce_required_layers=False,
    )

    assert [violation.message for violation in violations] == [
        "gateway boundary test file is missing: missing/test_gateway.py"
    ]


def test_detects_missing_boundary_test_function(tmp_path: Path) -> None:
    path = tmp_path / "tests" / "test_gateway.py"
    path.parent.mkdir(parents=True)
    path.write_text("def test_other():\n    pass\n", encoding="utf-8")
    requirement = BoundaryTestRequirement(
        layer="gateway",
        path="tests/test_gateway.py",
        test_name="test_tenant_a_cannot_see_tenant_b_observations",
    )

    violations = validate_cross_tenant_boundary_tests(
        repo_root=tmp_path,
        requirements=(requirement,),
        enforce_required_layers=False,
    )

    assert [violation.message for violation in violations] == [
        "gateway boundary test is missing: "
        "tests/test_gateway.py::test_tenant_a_cannot_see_tenant_b_observations"
    ]


def test_detects_lost_cross_tenant_evidence_terms(tmp_path: Path) -> None:
    path = tmp_path / "tests" / "test_worker.py"
    path.parent.mkdir(parents=True)
    path.write_text(
        "async def test_tenant_isolation():\n    assert True\n",
        encoding="utf-8",
    )
    requirement = BoundaryTestRequirement(
        layer="worker",
        path="tests/test_worker.py",
        test_name="test_tenant_isolation",
        required_terms=("other_tenant_id", "trig_b"),
    )

    violations = validate_cross_tenant_boundary_tests(
        repo_root=tmp_path,
        requirements=(requirement,),
        enforce_required_layers=False,
    )

    assert [violation.message for violation in violations] == [
        "worker boundary test lost required cross-tenant evidence terms in "
        "tests/test_worker.py: other_tenant_id, trig_b"
    ]


def test_contract_covers_required_layers() -> None:
    assert {requirement.layer for requirement in REQUIREMENTS} == {
        "database",
        "gateway",
        "repository",
        "worker",
        "realtime",
    }
