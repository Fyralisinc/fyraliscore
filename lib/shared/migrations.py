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
import os
import pathlib

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

    # Test-environment only: relax the tenant_id foreign keys.
    if os.environ.get("COMPANY_OS_ENV") == "test":
        await _relax_tenant_fks_for_tests(conn)

    return applied


async def _relax_tenant_fks_for_tests(conn: asyncpg.Connection) -> None:
    """Drop the tenant_id foreign keys (migration 0037) — TEST DB ONLY.

    0037 promotes every `tenant_id` to `REFERENCES tenants(id)
    DEFERRABLE INITIALLY IMMEDIATE`. Its header documents the contract:
    the FK is "never realized in tests" — tests are expected to wrap
    the body in a transaction and ROLLBACK with `SET CONSTRAINTS ALL
    DEFERRED`. Much of the suite predates that and uses the autocommit
    + TRUNCATE pattern, so the IMMEDIATE check fires on the first
    INSERT of a uuid7() tenant_id that has no tenants row.

    Gated on COMPANY_OS_ENV=test (set by CI and test conftests, never
    in production), this is the single choke point every test bootstrap
    funnels through, so it covers the root conftest *and* the many
    per-package pool fixtures that call apply_migrations_dir directly.
    apply_migrations_dir re-adds the FK on each re-run, so the drop has
    to follow every application. No test asserts FK-firing behavior.

    Only the parent/standalone constraint is dropped (conislocal); the
    inherited copies on partition children cannot be dropped directly
    and disappear when the parent's is dropped.
    """
    await conn.execute(
        """
        DO $$
        DECLARE r record;
        BEGIN
          FOR r IN
            SELECT conrelid::regclass AS tbl, conname
            FROM pg_constraint
            WHERE contype = 'f' AND conname ~ '_tenant_fk$' AND conislocal
          LOOP
            EXECUTE format(
              'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I', r.tbl, r.conname
            );
          END LOOP;
        END $$;
        """
    )


__all__ = ["MigrationError", "apply_migration", "apply_migrations_dir"]
