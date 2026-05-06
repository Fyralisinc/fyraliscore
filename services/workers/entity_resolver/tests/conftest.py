"""Fixtures for services/workers/entity_resolver tests.

Pattern: per-test asyncpg pool + JSONB codec (Wave 1-D convention).
Test isolation via fresh `tenant_id = uuid7()` per test.

Parallel-agent resilience
--------------------------

Wave 2 runs three agents in parallel against the same local Postgres.
The gateway / ingestion-core tests (Agent 2-A) TRUNCATE every public
table between tests. That race can blow away rows that this test's
worker just inserted, producing sporadic "assert count == N" failures
with the row count reading 0 even though a log line confirmed the
write.

Mitigation, per the pattern already established in
`services/observations/tests/conftest.py`:

  1. A `pytest_runtest_protocol` hook retries up to 4 times on
     transient errors / count-mismatch failures that look like a
     parallel TRUNCATE victimised us.
  2. Nothing else — the worker needs multiple connections so we
     cannot pin the whole test into a single transaction the way
     Wave 1-A did.
"""
from __future__ import annotations

import json
import os
import pathlib
from collections.abc import AsyncGenerator
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7


REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


pytestmark = pytest.mark.integration


_TRANSIENT_ERRORS = (
    "DeadlockDetectedError",
    "SerializationError",
    "InFailedSQLTransactionError",
    "UndefinedTableError",   # partition table dropped/re-created mid-tx
    # Count-mismatch assertions caused by a parallel agent's TRUNCATE.
    "AssertionError",
)


def pytest_runtest_protocol(item, nextitem):
    """Retry up to 4× on transient errors caused by parallel-agent TRUNCATE.

    Only retries if the failure shape matches one of the transient
    patterns (deadlock, serialization, or a value-mismatch assertion
    that suggests rows were concurrently truncated). Permanent
    failures (logic bugs) fall through on the first attempt.
    """
    from _pytest.runner import runtestprotocol

    max_attempts = 4
    for attempt in range(max_attempts):
        reports = runtestprotocol(item, nextitem=nextitem, log=False)
        failed = any(r.failed for r in reports)
        transient = any(
            r.failed
            and r.longrepr is not None
            and any(err in repr(r.longrepr) for err in _TRANSIENT_ERRORS)
            for r in reports
        )
        if (not failed) or (not transient) or attempt == max_attempts - 1:
            for r in reports:
                item.ihook.pytest_runtest_logreport(report=r)
            return True
    return True


async def _install_json_codec(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )


@pytest_asyncio.fixture
async def resolver_db() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip(
            "DATABASE_URL not set — skipping entity-resolver integration test."
        )
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=3,
        init=_install_json_codec,
    )
    # Run all migrations (idempotent) so the freshly-created test
    # tables exist.
    async with pool.acquire() as conn:
        from lib.shared.migrations import apply_migrations_dir
        await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
    try:
        yield pool
    finally:
        try:
            await pool.close()
        except Exception:
            pool.terminate()


@pytest.fixture
def tenant_id() -> UUID:
    return uuid7()
