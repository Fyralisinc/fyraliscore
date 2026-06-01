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
    """The shipped db/migrations/ set is scanned for duplicate numeric
    prefixes. On the merged (cannonical) branch main contributes two
    intentional dual-prefixed migrations (0014, 0043), so the check is a
    tolerated warning rather than a hard failure — this must still not
    raise. See lib/shared/migrations.py."""
    files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    assert files, f"no migrations found in {_MIGRATIONS_DIR}"
    _assert_unique_prefixes(files)  # tolerated-warning, must not raise


def test_assert_unique_prefixes_warns_on_duplicates(tmp_path, caplog) -> None:
    # Merged-branch policy (cannonical): duplicate prefixes are tolerated
    # with a logged warning rather than a RuntimeError, because main ships
    # intentional dual prefixes (0014, 0043). See lib/shared/migrations.py.
    import logging

    (tmp_path / "0001_a.sql").write_text("SELECT 1;")
    (tmp_path / "0001_b.sql").write_text("SELECT 1;")
    with caplog.at_level(logging.WARNING):
        _assert_unique_prefixes(sorted(tmp_path.glob("*.sql")))  # must NOT raise
    assert any(
        "duplicate migration prefixes" in r.getMessage() for r in caplog.records
    )


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
