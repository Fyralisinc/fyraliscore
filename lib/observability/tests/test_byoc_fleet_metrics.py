"""Regression tests for the BYOC fleet-instrumentation metrics (gaps G1/G4/G5).

The 6-commit BYOC instrumentation track (up to cdee4b9) added fleet-canonical
metric singletons on the default registry but shipped with ZERO automated
tests. This module covers the THREE pure-metrics gaps that need no database:

  G1 — schema-version ledger metrics. Driving the real migration runner
       (`apply_migrations_dir`) over a no-DB fake connection must set
       fyralis_schema_version / fyralis_schema_applied_count from the ledger
       and flip fyralis_schema_last_failed_migration on a broken file.
  G4 — Think validation/cost mirrors. The two fyralis_-prefixed counters that
       mirror the in-memory think-local families onto the default registry.
  G5 — expected-vs-running worker set. The eager-published
       fyralis_worker_compose_present / fyralis_worker_expected gauges that
       encode the authoritative expected worker set in code (so the control
       plane can see coded-but-undeployed classes like anomaly_processor).

The metrics module is ZERO-DEPENDENCY (no prometheus_client, no DB, no I/O),
so every test here runs under plain CPython. Assertions are on specific
labeled exposition lines / `.get()` readings, never whole-output equality,
because the default registry is process-global (mirrors test_metrics.py).
"""
from __future__ import annotations

import pathlib

import pytest

from lib.observability.metrics import (
    EXPECTED_WORKER_CLASSES,
    SCHEMA_APPLIED_TOTAL,
    SCHEMA_LAST_FAILED,
    SCHEMA_VERSION,
    THINK_LLM_COST_USD,
    THINK_VALIDATION_DROPPED_OPS,
    WORKER_COMPOSE_PRESENT,
    WORKER_EXPECTED,
    publish_expected_worker_set,
    render_default,
    reset_default_for_tests,
)


@pytest.fixture(autouse=True)
def _clean_default_registry():
    # The BYOC singletons live on the process-global default registry; clear
    # it before AND after so cross-test leakage can't mask a missing emission
    # or create a phantom one.
    reset_default_for_tests()
    yield
    reset_default_for_tests()
    # Re-establish the eager G5 lines the import-time call published, so any
    # later test that imports the module sees the production default again.
    publish_expected_worker_set()


# =====================================================================
# G5 — expected-vs-running worker set (eager, no DB, no sampler).
# =====================================================================


class TestExpectedWorkerSet:
    def test_publish_sets_expected_and_compose_present_per_class(self) -> None:
        publish_expected_worker_set()
        # Every coded worker class is EXPECTED to run.
        for worker_class in EXPECTED_WORKER_CLASSES:
            assert WORKER_EXPECTED.get(worker_class=worker_class) == 1.0

    def test_deployed_classes_present_undeployed_classes_absent(self) -> None:
        """The G5 gap made concrete: anomaly_processor / deadline_resolver are
        coded but absent from compose, so their compose_present gauge reads 0
        while the deployed classes read 1 — the directly-alertable
        'reasoning silently never fires' series."""
        publish_expected_worker_set()
        assert WORKER_COMPOSE_PRESENT.get(worker_class="think_worker") == 1.0
        assert WORKER_COMPOSE_PRESENT.get(worker_class="post_commit_worker") == 1.0
        # The two coded-but-NOT-deployed classes.
        assert WORKER_COMPOSE_PRESENT.get(worker_class="anomaly_processor") == 0.0
        assert WORKER_COMPOSE_PRESENT.get(worker_class="deadline_resolver") == 0.0
        # Consistency with the authoritative source-of-truth map.
        for worker_class, in_compose in EXPECTED_WORKER_CLASSES.items():
            assert WORKER_COMPOSE_PRESENT.get(worker_class=worker_class) == (
                1.0 if in_compose else 0.0
            )

    def test_eager_compose_present_line_renders(self) -> None:
        """The set is static (no runtime state, no collector), so the lines
        MUST appear on a plain render_default() scrape — the form the control
        plane diffs against up{job=~'fyralis-.*'}."""
        publish_expected_worker_set()
        text = render_default()
        assert "# TYPE fyralis_worker_compose_present gauge" in text
        assert (
            'fyralis_worker_compose_present{worker_class="anomaly_processor"} 0'
            in text
        )
        assert (
            'fyralis_worker_compose_present{worker_class="think_worker"} 1' in text
        )
        assert 'fyralis_worker_expected{worker_class="think_worker"} 1' in text

    def test_publish_is_idempotent(self) -> None:
        # Static set → re-publishing must not double-count (these are gauges,
        # not counters, but a regression to .inc() would surface here).
        publish_expected_worker_set()
        publish_expected_worker_set()
        assert WORKER_COMPOSE_PRESENT.get(worker_class="anomaly_processor") == 0.0
        assert WORKER_EXPECTED.get(worker_class="think_worker") == 1.0


# =====================================================================
# G4 — Think validation/cost mirrors on the DEFAULT registry.
# =====================================================================


class TestThinkMirrors:
    def test_validation_dropped_ops_counter_increments_by_reason_and_op(
        self,
    ) -> None:
        THINK_VALIDATION_DROPPED_OPS.inc(reason="schema", op_type="add_edge")
        THINK_VALIDATION_DROPPED_OPS.inc(reason="schema", op_type="add_edge")
        THINK_VALIDATION_DROPPED_OPS.inc(reason="dup", op_type="upsert_model")
        assert (
            THINK_VALIDATION_DROPPED_OPS.get(reason="schema", op_type="add_edge")
            == 2.0
        )
        assert (
            THINK_VALIDATION_DROPPED_OPS.get(reason="dup", op_type="upsert_model")
            == 1.0
        )
        text = render_default()
        assert "# TYPE fyralis_think_validation_dropped_ops_total counter" in text
        # Label order follows the declared tuple ("reason", "op_type").
        assert (
            'fyralis_think_validation_dropped_ops_total'
            '{reason="schema",op_type="add_edge"} 2' in text
        )

    def test_llm_cost_counter_accumulates_by_trigger_kind(self) -> None:
        THINK_LLM_COST_USD.inc(0.0021, trigger_kind="signal")
        THINK_LLM_COST_USD.inc(0.0079, trigger_kind="signal")
        THINK_LLM_COST_USD.inc(0.05, trigger_kind="anomaly")
        assert THINK_LLM_COST_USD.get(trigger_kind="signal") == pytest.approx(0.01)
        assert THINK_LLM_COST_USD.get(trigger_kind="anomaly") == pytest.approx(0.05)
        text = render_default()
        assert "# TYPE fyralis_think_llm_cost_usd_total counter" in text


# =====================================================================
# G1 — schema-version ledger metrics, driven through the REAL runner.
# =====================================================================
#
# A no-DB fake connection implements just the asyncpg surface
# apply_migrations_dir touches: execute() (advisory lock + DDL + INSERT),
# fetch() (ledger read), and transaction() (per-file txn wrapper). The fake
# records every recorded filename so the runner's own _publish_schema_metrics
# read-back sees them — exercising the production metric-set sites, not a
# reimplementation.


class _FakeTxn:
    """Async-context-manager stand-in for asyncpg's conn.transaction()."""

    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FakeTxn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False  # never suppress — let MigrationError propagate


class _FakeConn:
    """Minimal asyncpg.Connection stand-in for the migration runner.

    `fail_on` is a set of filenames whose APPLY (the INSERT-less execute of the
    migration body) should raise, simulating a broken/wedged migration. The
    runner wraps that in MigrationError(name, cause).
    """

    def __init__(self, fail_on: set[str] | None = None) -> None:
        self._fail_on = fail_on or set()
        self._ledger: list[str] = []
        self._applying: str | None = None

    def transaction(self) -> _FakeTxn:
        return _FakeTxn(self)

    async def execute(self, sql: str, *args) -> str:
        # The migration body apply for a "broken" file: apply_migration passes
        # the file's SQL text as the sole statement; we mark it broken by its
        # recorded marker so the runner raises MigrationError.
        if isinstance(sql, str) and sql.startswith("__BROKEN__:"):
            raise RuntimeError("syntax error at or near ...")
        # The ledger INSERT records the applied filename (1st positional arg).
        if isinstance(sql, str) and sql.upper().lstrip().startswith("INSERT INTO"):
            self._ledger.append(args[0])
        return "OK"

    async def fetch(self, sql: str, *args) -> list[dict]:
        # The runner reads `SELECT filename FROM <ledger>` twice: once up-front
        # (already-applied) and once in _publish_schema_metrics.
        return [{"filename": name} for name in self._ledger]

    async def fetchval(self, sql: str, *args):  # pragma: no cover - unused here
        return None


def _write_migration(directory: pathlib.Path, name: str, *, broken: bool) -> None:
    # A broken file carries the __BROKEN__ marker the fake conn raises on; a
    # good file carries an inert statement (the fake conn just returns OK).
    body = f"__BROKEN__:{name}" if broken else "SELECT 1;"
    (directory / name).write_text(body)


async def test_g1_publishes_version_and_applied_count_from_ledger(
    tmp_path: pathlib.Path,
) -> None:
    from lib.shared.migrations import apply_migrations_dir

    _write_migration(tmp_path, "0001_foundation.sql", broken=False)
    _write_migration(tmp_path, "0042_add_thing.sql", broken=False)
    _write_migration(tmp_path, "0155_schema_migrations.sql", broken=False)

    conn = _FakeConn()
    applied = await apply_migrations_dir(
        conn, tmp_path, on_error="stop", ensure_partitions=False,
    )

    assert applied == [
        "0001_foundation.sql",
        "0042_add_thing.sql",
        "0155_schema_migrations.sql",
    ]
    # Highest numeric prefix is the monotonic schema version.
    assert SCHEMA_VERSION.get() == 155.0
    assert SCHEMA_APPLIED_TOTAL.get() == 3.0
    # Every file applied cleanly → no last-failed flag set to 1.
    text = render_default()
    assert "fyralis_schema_version 155" in text
    assert "fyralis_schema_applied_count 3" in text


async def test_g1_clean_apply_clears_last_failed_flag(
    tmp_path: pathlib.Path,
) -> None:
    from lib.shared.migrations import apply_migrations_dir

    _write_migration(tmp_path, "0001_foundation.sql", broken=False)
    conn = _FakeConn()
    await apply_migrations_dir(
        conn, tmp_path, on_error="stop", ensure_partitions=False,
    )
    # A clean apply explicitly sets the per-file failure gauge to 0.
    assert SCHEMA_LAST_FAILED.get(filename="0001_foundation.sql") == 0.0


async def test_g1_broken_migration_sets_last_failed_flag(
    tmp_path: pathlib.Path,
) -> None:
    from lib.shared.migrations import MigrationError, apply_migrations_dir

    _write_migration(tmp_path, "0001_foundation.sql", broken=False)
    _write_migration(tmp_path, "0099_wedged.sql", broken=True)
    conn = _FakeConn()

    # on_error="stop" (production/CI): the broken file re-raises, but the
    # G1 gauge MUST be set BEFORE the raise so the control plane sees which
    # file wedged the deployment.
    with pytest.raises(MigrationError) as exc:
        await apply_migrations_dir(
            conn, tmp_path, on_error="stop", ensure_partitions=False,
        )
    assert exc.value.filename == "0099_wedged.sql"
    assert SCHEMA_LAST_FAILED.get(filename="0099_wedged.sql") == 1.0
    # The good file that applied first stays cleared.
    assert SCHEMA_LAST_FAILED.get(filename="0001_foundation.sql") == 0.0
    # The wedged-file series is alertable directly off the scrape.
    text = render_default()
    assert (
        'fyralis_schema_last_failed_migration{filename="0099_wedged.sql"} 1'
        in text
    )


async def test_g1_extension_ledger_does_not_clobber_host_schema_version(
    tmp_path: pathlib.Path,
) -> None:
    """_publish_schema_metrics is a no-op for a non-default ledger_table, so an
    extension's private numbering can't overwrite the host's schema gauge."""
    from lib.shared.migrations import apply_migrations_dir

    # Seed the host gauge with a known value first.
    SCHEMA_VERSION.set(155.0)
    SCHEMA_APPLIED_TOTAL.set(3.0)

    _write_migration(tmp_path, "0001_ext_init.sql", broken=False)
    conn = _FakeConn()
    await apply_migrations_dir(
        conn,
        tmp_path,
        on_error="stop",
        ledger_table="schema_migrations_ext_github_intel",
        ensure_partitions=False,
    )
    # The extension's "0001" did NOT clobber the host's 155.
    assert SCHEMA_VERSION.get() == 155.0
    assert SCHEMA_APPLIED_TOTAL.get() == 3.0
