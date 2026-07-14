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

from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from datetime import date
import hashlib
import logging
import pathlib
import re

import asyncpg

from lib.observability.metrics import (
    SCHEMA_APPLIED_TOTAL,
    SCHEMA_LAST_FAILED,
    SCHEMA_VERSION,
)


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

# Shared schema/bootstrap advisory lock. Test suites and local tools may
# apply migrations or run full-schema TRUNCATEs against the same dev DB in
# parallel; serialize those global mutations to avoid migration/TRUNCATE
# deadlocks while leaving ordinary tenant-isolated test work concurrent.
SCHEMA_BOOTSTRAP_ADVISORY_LOCK_ID = 819_771_700_513_431_337


@asynccontextmanager
async def schema_bootstrap_lock(
    conn: asyncpg.Connection,
) -> AsyncGenerator[None, None]:
    await conn.execute(
        "SELECT pg_advisory_lock($1::bigint)",
        SCHEMA_BOOTSTRAP_ADVISORY_LOCK_ID,
    )
    try:
        yield
    finally:
        await conn.execute(
            "SELECT pg_advisory_unlock($1::bigint)",
            SCHEMA_BOOTSTRAP_ADVISORY_LOCK_ID,
        )


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


_LEDGER_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_OBSOLETE_DEMO_SCAFFOLDING_MIGRATIONS = (
    "0023_demo_infrastructure.sql",
    "0026_single_demo_company.sql",
    "0028_pelago_demo_config.sql",
    "0093_drop_demo_scaffolding.sql",
)


def _ledger_ddl(table: str) -> str:
    # `checksum` is the digest of the migration file bytes at apply time
    # (BYOC §12 G1) — formalized in db/migrations/0155_schema_migrations.sql.
    # Added here too so a DB bootstrapped purely by this lazy CREATE (tests,
    # extension ledgers) gets the same shape. ADD COLUMN IF NOT EXISTS widens
    # a pre-G1 two-column ledger created by an older runner.
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        "  filename text PRIMARY KEY,\n"
        "  checksum text,\n"
        "  applied_at timestamptz NOT NULL DEFAULT now()\n"
        ");\n"
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS checksum text"
    )


def _migration_checksum(sql_text: str) -> str:
    """SHA-256 of the migration file's bytes — captured at apply time so the
    fleet control plane can detect a silently-edited applied migration (drift).
    """
    return hashlib.sha256(sql_text.encode("utf-8")).hexdigest()


def _version_from_filename(filename: str) -> int:
    """Numeric prefix of a migration filename (`0155_…` -> 155), the monotonic
    schema version. 0 when the name has no numeric prefix (extension ledgers).
    """
    m = _PREFIX_RE.match(filename)
    return int(m.group(1)) if m else 0


async def _ensure_schema_migrations(
    conn: asyncpg.Connection, table: str = "schema_migrations"
) -> None:
    await conn.execute(_ledger_ddl(table))


async def _record_applied_migration(
    conn: asyncpg.Connection,
    filename: str,
    table: str = "schema_migrations",
    checksum: str | None = None,
) -> None:
    await conn.execute(
        f"INSERT INTO {table}(filename, checksum) VALUES ($1, $2) "
        "ON CONFLICT (filename) DO UPDATE SET checksum = "
        f"COALESCE({table}.checksum, EXCLUDED.checksum)",
        filename,
        checksum,
    )


async def _baseline_obsolete_demo_scaffolding_if_final_state(
    conn: asyncpg.Connection,
    *,
    already_applied: set[str],
    ledger_table: str,
    migration_filenames: set[str],
) -> None:
    """Record removed demo scaffolding migrations for post-demo core schemas.

    Older local/test databases predate the Python migration ledger and may
    already be in the final core state produced by 0093: the shared ``tenants``
    table remains, demo tables are gone, and ``tenants.demo_config_id`` is
    gone. Replaying 0023 then 0093 against that state repeatedly adds and drops
    the same column; Postgres keeps dropped attributes internally, so a
    long-lived DB can eventually hit the per-table column limit. When the final
    state is already present, baseline those obsolete demo-only migrations in
    the ledger instead of replaying the add/drop loop. Fresh DBs are unaffected
    because ``tenants`` does not exist yet.
    """

    obsolete_present = set(_OBSOLETE_DEMO_SCAFFOLDING_MIGRATIONS).intersection(
        migration_filenames
    )
    missing = obsolete_present.difference(already_applied)
    if not missing:
        return

    state = await conn.fetchrow(
        """
        SELECT
          to_regclass('public.tenants') IS NOT NULL AS has_tenants,
          to_regclass('public.demo_configs') IS NOT NULL AS has_demo_configs,
          to_regclass('public.demo_sessions') IS NOT NULL AS has_demo_sessions,
          to_regclass('public.demo_session_costs') IS NOT NULL
            AS has_demo_session_costs,
          EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'tenants'
              AND column_name = 'demo_config_id'
          ) AS has_demo_config_id
        """
    )
    if not state:
        return
    if not state["has_tenants"]:
        return
    if (
        state["has_demo_configs"]
        or state["has_demo_sessions"]
        or state["has_demo_session_costs"]
        or state["has_demo_config_id"]
    ):
        return

    for filename in _OBSOLETE_DEMO_SCAFFOLDING_MIGRATIONS:
        if filename in missing:
            await _record_applied_migration(conn, filename, ledger_table)
            already_applied.add(filename)


async def _publish_schema_metrics(
    conn: asyncpg.Connection, table: str = "schema_migrations"
) -> None:
    """Set fyralis_schema_version / _applied_count from the ledger so every
    worker that renders the default registry exposes the deployment's schema
    state to the fleet control plane (BYOC §12 G1). Best-effort: a read failure
    must never break a migration run, so it is swallowed.
    """
    # Only the host's global line is reported as the schema version — an
    # extension's private ledger has its own numbering and would otherwise
    # clobber the gauge.
    if table != "schema_migrations":
        return
    try:
        rows = await conn.fetch(f"SELECT filename FROM {table}")
    except Exception:  # noqa: BLE001 — metrics must not break migrations
        return
    names = [r["filename"] for r in rows]
    SCHEMA_APPLIED_TOTAL.set(len(names))
    SCHEMA_VERSION.set(max((_version_from_filename(n) for n in names), default=0))


async def apply_migrations_dir(
    conn: asyncpg.Connection,
    migrations_dir: pathlib.Path,
    *,
    on_error: str = "stop",
    ledger_table: str = "schema_migrations",
    ensure_partitions: bool = True,
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

    `ledger_table` namespaces the applied-migrations ledger. The default
    (`schema_migrations`) is the host's global line; an **extension-owned**
    migration set passes its own ledger (e.g. `schema_migrations_ext_github_intel`)
    so its filenames can never collide with the host's in the shared `filename`
    PK — the key requirement for letting an extension own its schema independently
    of the host's numbering (ADR-0004 extension-owned schema).

    Returns the list of filenames that applied successfully.
    """
    if on_error not in ("stop", "warn"):
        raise ValueError(f"on_error must be 'stop' or 'warn'; got {on_error!r}")
    if not _LEDGER_NAME_RE.match(ledger_table):
        raise ValueError(f"invalid ledger_table name: {ledger_table!r}")

    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise RuntimeError(f"no migrations found in {migrations_dir}")
    _assert_unique_prefixes(files)

    async with schema_bootstrap_lock(conn):
        # Create the ledger inside the bootstrap lock so concurrent
        # bootstraps serialize the CREATE TABLE, then read what's already
        # applied so re-runs against a long-lived DB skip recorded files.
        await _ensure_schema_migrations(conn, ledger_table)
        rows = await conn.fetch(f"SELECT filename FROM {ledger_table}")
        already_applied = {row["filename"] for row in rows}
        await _baseline_obsolete_demo_scaffolding_if_final_state(
            conn,
            already_applied=already_applied,
            ledger_table=ledger_table,
            migration_filenames={path.name for path in files},
        )

        applied: list[str] = []
        for path in files:
            if path.name in already_applied:
                continue
            sql_text = path.read_text()
            try:
                await apply_migration(conn, sql_text, name=path.name)
                await _record_applied_migration(
                    conn,
                    path.name,
                    ledger_table,
                    checksum=_migration_checksum(sql_text),
                )
                already_applied.add(path.name)
                applied.append(path.name)
                # This file applied cleanly — clear any failure flag left from
                # a prior wedged attempt on the same file (BYOC §12 G1).
                SCHEMA_LAST_FAILED.set(0, filename=path.name)
            except MigrationError as e:
                # BYOC §12 G1 — promote "a migration failed" to a fleet-visible
                # gauge so the control plane can alert without log-grepping.
                # Set BEFORE re-raising so the `stop` path (production/CI) also
                # records which file wedged the deployment.
                SCHEMA_LAST_FAILED.set(1, filename=e.filename)
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

        # BYOC §12 G1 — publish the deployment's schema version + applied-count
        # from the ledger (no-op for extension ledgers; best-effort).
        await _publish_schema_metrics(conn, ledger_table)

        # Fresh test/dev DBs only get current-month + 3 partitions from the
        # foundation migration; widen the window so historical inserts work.
        # Skipped (ensure_partitions=False) on the production extension-migrate
        # path, which must NOT create observation/resource partitions as a side
        # effect of applying an extension's schema.
        if ensure_partitions:
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
    "schema_bootstrap_lock",
    "_assert_unique_prefixes",
]
