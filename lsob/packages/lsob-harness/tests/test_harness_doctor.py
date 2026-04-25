"""Doctor: happy path (workspace installed + fixtures present) and degraded path."""

from __future__ import annotations

from pathlib import Path

from lsob_harness.doctor import run_doctor


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]


def test_doctor_happy_path_against_real_workspace() -> None:
    report = run_doctor(workspace_root=WORKSPACE_ROOT)
    names = {c.name for c in report.checks}
    assert {
        "workspace-installed",
        "anthropic-api-key",
        "docker-daemon",
        "fixtures",
        "baselines",
    } <= names

    # Required checks must pass: workspace + fixtures
    required = [c for c in report.checks if c.required]
    workspace_ok = next(c for c in required if c.name == "workspace-installed").ok
    fixtures_ok = next(c for c in required if c.name == "fixtures").ok
    assert workspace_ok is True
    assert fixtures_ok is True


def test_doctor_degraded_when_fixtures_missing(tmp_path: Path) -> None:
    # Point fixtures_root at an empty tmp dir
    fake_fixtures = tmp_path / "empty_fixtures"
    fake_fixtures.mkdir()
    report = run_doctor(workspace_root=tmp_path, fixtures_root=fake_fixtures)
    fixtures_check = next(c for c in report.checks if c.name == "fixtures")
    assert fixtures_check.ok is False
    assert fixtures_check.required is True
    # Report therefore not ok
    assert report.ok is False
    assert report.exit_code() == 1


def test_doctor_exit_code_when_required_pass() -> None:
    report = run_doctor(workspace_root=WORKSPACE_ROOT)
    # anthropic key / docker / baselines may be missing, but they are non-required.
    assert report.exit_code() == 0
