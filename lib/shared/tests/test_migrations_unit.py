"""Pure-unit tests for `_needs_no_transaction` detection in
lib/shared/migrations.py. No DB required.

The integration counterpart (apply against a real Postgres) lives in
`test_migrations.py`.
"""
from __future__ import annotations

import pathlib

import pytest

from lib.shared.migrations import (
    _assert_unique_prefixes,
    _baseline_obsolete_demo_scaffolding_if_final_state,
    _needs_no_transaction,
)


_MIGRATIONS_DIR = (
    pathlib.Path(__file__).resolve().parents[3] / "db" / "migrations"
)


def test_real_migrations_have_unique_prefixes() -> None:
    """The shipped db/migrations/ set has one unique numeric prefix per file."""
    files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    assert files, f"no migrations found in {_MIGRATIONS_DIR}"
    _assert_unique_prefixes(files)


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


class _FakeMigrationConn:
    def __init__(self, state: dict[str, bool]) -> None:
        self._state = state
        self.recorded: list[str] = []

    async def fetchrow(self, _sql: str) -> dict[str, bool]:
        return self._state

    async def execute(
        self, _sql: str, filename: str, _checksum: str | None = None
    ) -> None:
        self.recorded.append(filename)


@pytest.mark.asyncio
async def test_baselines_obsolete_demo_scaffolding_in_post_demo_state() -> None:
    conn = _FakeMigrationConn(
        {
            "has_tenants": True,
            "has_demo_configs": False,
            "has_demo_sessions": False,
            "has_demo_session_costs": False,
            "has_demo_config_id": False,
        }
    )
    already_applied: set[str] = set()
    migration_filenames = {
        "0023_demo_infrastructure.sql",
        "0026_single_demo_company.sql",
        "0028_pelago_demo_config.sql",
        "0093_drop_demo_scaffolding.sql",
    }

    await _baseline_obsolete_demo_scaffolding_if_final_state(
        conn,
        already_applied=already_applied,
        ledger_table="schema_migrations",
        migration_filenames=migration_filenames,
    )

    assert conn.recorded == [
        "0023_demo_infrastructure.sql",
        "0026_single_demo_company.sql",
        "0028_pelago_demo_config.sql",
        "0093_drop_demo_scaffolding.sql",
    ]
    assert already_applied == migration_filenames


@pytest.mark.asyncio
async def test_demo_scaffolding_baseline_refuses_active_demo_tables() -> None:
    conn = _FakeMigrationConn(
        {
            "has_tenants": True,
            "has_demo_configs": True,
            "has_demo_sessions": False,
            "has_demo_session_costs": False,
            "has_demo_config_id": False,
        }
    )
    already_applied: set[str] = set()

    await _baseline_obsolete_demo_scaffolding_if_final_state(
        conn,
        already_applied=already_applied,
        ledger_table="schema_migrations",
        migration_filenames={"0023_demo_infrastructure.sql"},
    )

    assert conn.recorded == []
    assert already_applied == set()
