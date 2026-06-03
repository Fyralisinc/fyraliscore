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

Non-transactional migrations (CONCURRENTLY) — added for ingestion LLD §1.6.
Postgres forbids `CREATE INDEX CONCURRENTLY` (and similar
`ALTER INDEX … CONCURRENTLY`, `REINDEX CONCURRENTLY`, `DROP INDEX
CONCURRENTLY`) inside an explicit transaction block. The migration
runner detects these files and runs them OUTSIDE the transaction
wrapper. Two opt-in signals are honoured:

  1. The SQL text contains the keyword `CONCURRENTLY` (word-boundary,
     case-insensitive, ignoring `-- …` line comments).
  2. The file contains a directive line `-- migration:no-transaction`
     anywhere in its body.

Files that match either signal lose the atomic-rollback guarantee
above — Postgres commits each statement individually. This is the
expected trade-off for non-blocking index builds; callers should
ensure such files contain a single statement so a mid-file failure
doesn't leave a half-built artifact.
"""
from __future__ import annotations

import logging
import pathlib
import re
from collections.abc import Iterable
from datetime import date

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


# Strip `-- …` line comments before scanning for keywords. SQL block
# comments (`/* … */`) are not used in this project's migrations; if
# that changes the regex below needs widening.
_LINE_COMMENT_RE = re.compile(r"--[^\n]*", flags=re.ASCII)
_CONCURRENTLY_RE = re.compile(r"\bCONCURRENTLY\b", flags=re.IGNORECASE)
_NO_TXN_DIRECTIVE_RE = re.compile(
    r"^\s*--\s*migration:no-transaction\s*$",
    flags=re.IGNORECASE | re.MULTILINE,
)


_PREFIX_RE = re.compile(r"^(\d+)_")


def _assert_unique_prefixes(files: Iterable[pathlib.Path]) -> None:
    """Reject a migrations set with duplicate numeric prefixes.

    Two files sharing a prefix (e.g. `0014_a.sql` and `0014_b.sql`)
    make the apply order depend on locale collation, which can diverge
    across environments and silently produce a non-deterministic
    schema. This is always a defect — fail loudly before applying
    anything, regardless of `on_error`.
    """
    seen: dict[str, str] = {}
    dupes: list[str] = []
    for path in files:
        m = _PREFIX_RE.match(path.name)
        if m is None:
            continue
        prefix = m.group(1)
        if prefix in seen:
            dupes.append(f"{prefix}: {seen[prefix]} + {path.name}")
        else:
            seen[prefix] = path.name
    if dupes:
        raise RuntimeError(
            "duplicate migration prefixes detected: " + "; ".join(dupes)
        )


def _needs_no_transaction(sql_text: str) -> bool:
    """True iff this migration must run outside a transaction.

    See module docstring; for ingestion LLD §1.6 (0049 entity_aliases
    functional index).
    """
    if _NO_TXN_DIRECTIVE_RE.search(sql_text) is not None:
        return True
    stripped = _LINE_COMMENT_RE.sub("", sql_text)
    return _CONCURRENTLY_RE.search(stripped) is not None


async def apply_migration(
    conn: asyncpg.Connection,
    sql_text: str,
    *,
    name: str,
) -> None:
    """Apply a single migration's SQL.

    Default path — wraps in `async with conn.transaction():`. Any
    error rolls the whole file back; the caller's connection is
    guaranteed clean afterwards.

    Non-transactional path — if the SQL contains `CONCURRENTLY` or a
    `-- migration:no-transaction` directive, the wrapper is skipped
    and each statement commits individually. Used for
    `CREATE INDEX CONCURRENTLY` builds that Postgres forbids inside
    an explicit transaction (ingestion LLD §1.6).

    Raises `MigrationError` wrapping the original exception with the
    migration's name attached, so callers can tell which file broke.
    """
    try:
        if _needs_no_transaction(sql_text):
            # No txn wrapper. Mid-file failure may leave partial state;
            # such files should contain a single CONCURRENTLY statement.
            await conn.execute(sql_text)
        else:
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
    _assert_unique_prefixes(files)

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

    # Fresh test/dev DBs only get current-month + 3 partitions from the
    # foundation migration; widen the window so historical inserts work.
    await ensure_test_partition_window(conn)
    return applied


# ---------------------------------------------------------------------
# Test/dev partition window.
#
# The foundation migration range-partitions `observations` and
# `resource_transactions` by month and attaches only the current month +
# 3 ahead. Tests routinely insert recent-historical rows (e.g. a 30-day
# customer-health timeline), which land in PAST months with no partition →
# "no partition of relation ... found for row". On a fresh test/dev DB we
# widen the window backward (and a little forward) so those inserts land.
#
# Inlined here rather than importing
# services.domain.observations.partitions, so `lib` stays independent of
# `services` (enforced by the import-linter contract in pyproject.toml).
# Runs only for parents that are actually range-partitioned in this DB, so
# it is a no-op for unrelated migration directories. Fully idempotent.
# ---------------------------------------------------------------------
_PARTITIONED_PARENTS = ("observations", "resource_transactions")


def _shift_month(d: date, delta: int) -> date:
    """First-of-month `delta` calendar months from `d` (delta may be negative)."""
    total = d.year * 12 + (d.month - 1) + delta
    return date(total // 12, total % 12 + 1, 1)


async def ensure_test_partition_window(
    conn: asyncpg.Connection,
    *,
    months_back: int = 12,
    months_ahead: int = 3,
) -> list[str]:
    """Attach monthly partitions spanning [today-months_back, today+months_ahead]
    for the foundation range-partitioned parents.

    Anchored to the database's own ``CURRENT_DATE`` (matching the foundation
    migration). Idempotent via ``CREATE TABLE IF NOT EXISTS ... PARTITION OF``.
    Returns the names of partitions newly created. No-op for parents that are
    not range-partitioned tables in this database.
    """
    today: date = await conn.fetchval("SELECT CURRENT_DATE")
    start = _shift_month(today.replace(day=1), -months_back)
    span = months_back + 1 + months_ahead
    created: list[str] = []
    for parent in _PARTITIONED_PARENTS:
        is_partitioned = await conn.fetchval(
            "SELECT c.relkind = 'p' FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = $1 AND n.nspname = 'public'",
            parent,
        )
        if not is_partitioned:
            continue
        cur = start
        for _ in range(span):
            nxt = _shift_month(cur, 1)
            name = f"{parent}_{cur.strftime('%Y_%m')}"
            existed = await conn.fetchval("SELECT to_regclass($1)", name)
            await conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{name}" PARTITION OF "{parent}" '
                f"FOR VALUES FROM ('{cur.isoformat()}') TO ('{nxt.isoformat()}')"
            )
            if existed is None:
                created.append(name)
            cur = nxt
    return created


__all__ = [
    "MigrationError",
    "apply_migration",
    "apply_migrations_dir",
    "ensure_test_partition_window",
    "_assert_unique_prefixes",
]
