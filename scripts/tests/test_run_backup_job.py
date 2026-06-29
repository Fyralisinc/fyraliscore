from __future__ import annotations

import argparse
import sys

import pytest

from scripts.run_backup_job import (
    BackupJobCliError,
    _sanitize_details,
    build_parser,
    run_backup_job,
)


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def test_backup_job_success_records_bounded_metadata() -> None:
    args = _parse(
        [
            "--component",
            "postgres",
            "--details-json",
            '{"provider":"pg_dump","job":"daily-base"}',
            "--",
            sys.executable,
            "-c",
            "print('payload-like output must be discarded')",
        ]
    )

    result = run_backup_job(args)

    assert result.status == "ok"
    assert result.exit_code == 0
    assert result.details["provider"] == "pg_dump"
    assert result.details["job"] == "daily-base"
    assert result.details["command"] == sys.executable.rsplit("/", 1)[-1]
    assert "payload-like output" not in str(result.details)


def test_backup_job_failure_records_failed_without_command_output() -> None:
    args = _parse(
        [
            "--component",
            "object_store",
            "--details-json",
            '{"provider":"aws-s3","job":"raw-tier-sync"}',
            "--",
            sys.executable,
            "-c",
            "import sys; print('s3://bucket/customer/object'); sys.exit(7)",
        ]
    )

    result = run_backup_job(args)

    assert result.status == "failed"
    assert result.exit_code == 7
    assert result.timed_out is False
    assert "s3://bucket" not in str(result.details)


def test_backup_job_rejects_sensitive_detail_keys() -> None:
    with pytest.raises(BackupJobCliError, match="sensitive keys"):
        _sanitize_details('{"object_key":"tenant/raw/object.json"}')


def test_backup_job_rejects_secret_looking_detail_values() -> None:
    with pytest.raises(BackupJobCliError, match="secret-looking"):
        _sanitize_details('{"provider":"s3","sample":"Bearer abc123"}')


def test_backup_job_rejects_missing_command() -> None:
    args = _parse(["--component", "postgres"])

    with pytest.raises(BackupJobCliError, match="backup command"):
        run_backup_job(args)
