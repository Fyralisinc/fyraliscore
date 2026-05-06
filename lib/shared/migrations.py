"""lib/shared/migrations.py — transaction-safe migration runner.

T3 fix (see tests/synthesis_harness/REPORT.md §9): the hand-rolled
migration runners scattered across conftests + the harness +
scripts/docker-migrate.sh used `await conn.execute(file_text)` for
each file. asyncpg's `execute` does NOT wrap multi-statement SQL in
a transaction, so a failure on statement N left statements 1..N-1
applied AND left the connection in an aborted-transaction state
("current transaction is aborted, commands ignored until end of
transaction block"), which then poisoned every subsequent migration
on the same connection.

This module provides one canonical entry point — `apply_migration` —
that wraps each file in `async with conn.transaction():`. On any
failure inside the file, asyncpg rolls the transaction back, the
connection is clean, and the caller sees the original error
unmolested.

Use this from every test conftest, every harness bootstrap, and any
new migration tooling. The production shell-side runner
(`scripts/docker-migrate.sh`) gets the same guarantee via psql's
`--single-transaction` flag — see that script for details.
"""
from __future__ import annotations

import logging
import pathlib
from collections.abc import Iterable

import asyncpg


logger = logging.getLogger(__name__)


class MigrationError(Exception):
    """A specific migration file failed to apply.

    Wraps the underlying asyncpg / Postgres error and carries the
    file name so callers and tests can branch on which migration
    broke.
    """

    def __init__(
        self,
        filename: str,
        cause: BaseException,
    ) -> None:
        super().__init__(f"migration {filename!r} failed: {cause}")
        self.filename = filename
        self.cause = cause


async def apply_migration(
    conn: asyncpg.Connection,
    sql_text: str,
    *,
    name: str,
) -> None:
    """Apply a single migration's SQL inside a transaction.

    Any error inside the migration rolls the whole file back. The
    caller's connection is guaranteed clean afterwards — no aborted
    transaction state to worry about on the next call.

    Raises `MigrationError` wrapping the original exception with the
    migration's name attached, so callers can tell which file broke.
    """
    try:
        async with conn.transaction():
            await conn.execute(sql_text)
    except Exception as exc:  # noqa: BLE001
        raise MigrationError(name, exc) from exc


async def apply_migrations_dir(
    conn: asyncpg.Connection,
    migrations_dir: pathlib.Path,
    *,
    on_error: str = "stop",
) -> list[str]:
    """Apply every `*.sql` file in `migrations_dir` in lex order.

    `on_error`:
      * `"stop"` (default) — re-raise the first MigrationError. This
        is the right policy for fresh databases and CI: a broken
        migration must surface loudly.
      * `"warn"` — log a warning and skip the failing file. This is
        the right policy for the harness and other test bootstraps
        that re-apply already-applied migrations against a
        long-lived dev database; later files in the directory may
        be no-ops because the schema already exists, and treating
        every failure as fatal would prevent the harness from ever
        running against a populated DB.

    Returns the list of filenames that applied successfully.
    """
    if on_error not in ("stop", "warn"):
        raise ValueError(f"on_error must be 'stop' or 'warn'; got {on_error!r}")

    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise RuntimeError(f"no migrations found in {migrations_dir}")

    applied: list[str] = []
    for path in files:
        try:
            await apply_migration(conn, path.read_text(), name=path.name)
            applied.append(path.name)
        except MigrationError as e:
            if on_error == "stop":
                raise
            # Note: stdlib logging reserves `filename` and `module` on
            # LogRecord, so we use prefixed keys to avoid the
            # "Attempt to overwrite 'filename'" KeyError.
            logger.warning(
                "migration_skipped: %s — %s",
                e.filename, str(e.cause),
                extra={
                    "migration_filename": e.filename,
                    "migration_cause": str(e.cause),
                },
            )
    return applied


__all__ = ["MigrationError", "apply_migration", "apply_migrations_dir"]
