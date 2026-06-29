from __future__ import annotations

import argparse
from typing import Mapping, Sequence

import pytest

from scripts.run_migration_rehearsal import (
    MigrationRehearsalCliError,
    build_parser,
    run_rehearsal,
)


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def test_rehearsal_requires_staging_clone_confirmation() -> None:
    args = _parse(["--dsn", "postgresql://staging-clone"])

    with pytest.raises(MigrationRehearsalCliError, match="confirm-staging-clone"):
        run_rehearsal(args, runner=lambda *_args: 0)


def test_rehearsal_runs_migrations_and_schema_drift() -> None:
    calls: list[list[str]] = []

    def _runner(
        command: Sequence[str],
        env: Mapping[str, str] | None,
        timeout_seconds: int,
    ) -> int:
        calls.append(list(command))
        assert env is not None
        assert env["DATABASE_URL"] == "postgresql://staging-clone"
        assert timeout_seconds == 900
        return 0

    result = run_rehearsal(
        _parse(
            [
                "--dsn",
                "postgresql://staging-clone",
                "--confirm-staging-clone",
            ]
        ),
        runner=_runner,
    )

    assert result.ok is True
    assert [step.name for step in result.steps] == [
        "apply_migrations",
        "schema_drift",
    ]
    assert [call[1] for call in calls] == [
        "scripts/apply_db_migrations.py",
        "scripts/check_schema_drift.py",
    ]


def test_rehearsal_stops_after_failed_migration_apply() -> None:
    calls = 0

    def _runner(
        command: Sequence[str],
        env: Mapping[str, str] | None,
        timeout_seconds: int,
    ) -> int:
        nonlocal calls
        calls += 1
        return 1

    result = run_rehearsal(
        _parse(
            [
                "--dsn",
                "postgresql://staging-clone",
                "--confirm-staging-clone",
            ]
        ),
        runner=_runner,
    )

    assert result.ok is False
    assert calls == 1
    assert [step.name for step in result.steps] == ["apply_migrations"]


def test_rehearsal_can_include_readiness_gates() -> None:
    calls: list[list[str]] = []

    def _runner(
        command: Sequence[str],
        env: Mapping[str, str] | None,
        timeout_seconds: int,
    ) -> int:
        calls.append(list(command))
        return 0

    result = run_rehearsal(
        _parse(
            [
                "--dsn",
                "postgresql://staging-clone",
                "--confirm-staging-clone",
                "--run-readiness-gates",
            ]
        ),
        runner=_runner,
    )

    assert result.ok is True
    assert [step.name for step in result.steps] == [
        "apply_migrations",
        "schema_drift",
        "operational_readiness_gates",
    ]
    assert calls[-1][1] == "scripts/run_operational_readiness_gates.py"
