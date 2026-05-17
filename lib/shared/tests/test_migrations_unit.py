"""Pure-unit tests for `_needs_no_transaction` detection in
lib/shared/migrations.py. No DB required.

The integration counterpart (apply against a real Postgres) lives in
`test_migrations.py`.
"""
from __future__ import annotations

import pytest

from lib.shared.migrations import _needs_no_transaction


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
