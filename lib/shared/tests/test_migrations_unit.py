"""Pure-unit tests for `_needs_no_transaction` detection in
lib/shared/migrations.py. No DB required.

The integration counterpart (apply against a real Postgres) lives in
`test_migrations.py`.
"""
from __future__ import annotations

import pathlib

import pytest

from lib.shared.migrations import _assert_unique_prefixes, _needs_no_transaction


_MIGRATIONS_DIR = (
    pathlib.Path(__file__).resolve().parents[3] / "db" / "migrations"
)


def test_real_migrations_have_unique_prefixes() -> None:
    """Regression for P0-2: the shipped db/migrations/ set must never
    contain two files sharing a numeric prefix (apply-order would be
    locale-dependent and the schema non-deterministic across envs)."""
    files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    assert files, f"no migrations found in {_MIGRATIONS_DIR}"
    _assert_unique_prefixes(files)  # raises RuntimeError on a dupe


def test_assert_unique_prefixes_rejects_duplicates(tmp_path) -> None:
    (tmp_path / "0001_a.sql").write_text("SELECT 1;")
    (tmp_path / "0001_b.sql").write_text("SELECT 1;")
    with pytest.raises(RuntimeError, match="duplicate migration prefixes"):
        _assert_unique_prefixes(sorted(tmp_path.glob("*.sql")))


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("CREATE TABLE t (id INT);", False),
        ("CREATE INDEX CONCURRENTLY foo_idx ON t(id);", True),
        ("create index concurrently foo_idx on t(id);", True),
        ("REINDEX INDEX CONCURRENTLY foo_idx;", True),
        (
            "-- NOTE: this used CONCURRENTLY originally\nCREATE TABLE t(id INT);",
            False,
        ),
        ("-- migration:no-transaction\nVACUUM ANALYZE t;", True),
        ("-- MIGRATION:NO-TRANSACTION\nVACUUM ANALYZE t;", True),
        # Word-boundary guard: must not fire on substring matches.
        ("CREATE TABLE nonconcurrently_table (id INT);", False),
    ],
)
def test_needs_no_transaction_detection(sql: str, expected: bool) -> None:
    assert _needs_no_transaction(sql) is expected
