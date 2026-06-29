from __future__ import annotations

import argparse
import json
import sys

import pytest

from scripts.run_backup_job import BackupJobCliError
from scripts.run_restore_rehearsal import (
    _parse_command_json,
    build_parser,
    run_restore_rehearsal,
)


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _python_command(source: str) -> str:
    return json.dumps([sys.executable, "-c", source])


def test_restore_rehearsal_success_runs_restore_then_verify() -> None:
    args = _parse(
        [
            "--component",
            "postgres",
            "--details-json",
            '{"provider":"pgbackrest","scope":"pitr-rehearsal"}',
            "--restore-command-json",
            _python_command("print('restore output is discarded')"),
            "--verify-command-json",
            _python_command("print('verify output is discarded')"),
        ]
    )

    result = run_restore_rehearsal(args)

    assert result.status == "ok"
    assert result.restore_exit_code == 0
    assert result.verify_exit_code == 0
    assert result.details["provider"] == "pgbackrest"
    assert result.details["restore_command"] == sys.executable.rsplit("/", 1)[-1]
    assert "restore output" not in str(result.details)
    assert "verify output" not in str(result.details)


def test_restore_rehearsal_skips_verify_when_restore_fails() -> None:
    args = _parse(
        [
            "--component",
            "object_store",
            "--details-json",
            '{"provider":"aws-s3","scope":"sample-prefix"}',
            "--restore-command-json",
            _python_command("import sys; print('object-key'); sys.exit(9)"),
            "--verify-command-json",
            _python_command("raise SystemExit(0)"),
        ]
    )

    result = run_restore_rehearsal(args)

    assert result.status == "failed"
    assert result.restore_exit_code == 9
    assert result.verify_exit_code is None
    assert "object-key" not in str(result.details)


def test_restore_rehearsal_rejects_unsafe_details() -> None:
    args = _parse(
        [
            "--component",
            "postgres",
            "--details-json",
            '{"tenant_id":"00000000-0000-0000-0000-000000000001"}',
            "--restore-command-json",
            _python_command("raise SystemExit(0)"),
            "--verify-command-json",
            _python_command("raise SystemExit(0)"),
        ]
    )

    with pytest.raises(BackupJobCliError, match="sensitive keys"):
        run_restore_rehearsal(args)


def test_restore_rehearsal_rejects_malformed_command_json() -> None:
    with pytest.raises(BackupJobCliError, match="non-empty JSON array"):
        _parse_command_json("{}", option="--restore-command-json")
