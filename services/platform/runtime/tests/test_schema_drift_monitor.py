from __future__ import annotations

import services.platform.schema_drift_monitor as monitor
from lib.observability.metrics import default_registry


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[int, ...]]] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[int, ...]) -> None:
        self.executed.append((sql, params))


class _Conn:
    def __init__(self) -> None:
        self.cursor_obj = _Cursor()
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def test_schema_drift_monitor_counts_bounded_categories(monkeypatch) -> None:
    default_registry().reset_for_tests()
    conn = _Conn()
    connect_calls: list[dict] = []

    def _connect(_dsn: str, **kwargs) -> _Conn:
        connect_calls.append(kwargs)
        return conn

    monkeypatch.setattr(
        monitor.schema_drift,
        "compare",
        lambda _conn: [
            "RLS missing: unsafe_customer_table is not enabled",
            "COLUMN type drift: sensitive_table.email (expected text, live jsonb)",
            "INDEX missing: events.events_actor_idx",
        ],
    )

    snapshot = monitor.run_schema_drift_check(
        "postgresql://example/db",
        connect=_connect,
        connect_timeout_seconds=7,
        statement_timeout_ms=1234,
    )
    rendered = monitor.render_schema_drift_metrics()

    assert snapshot.status == "drift"
    assert snapshot.findings_total == 3
    assert snapshot.findings_by_category["rls"] == 1
    assert snapshot.findings_by_category["column"] == 1
    assert snapshot.findings_by_category["index"] == 1
    assert conn.closed is True
    assert connect_calls == [{"connect_timeout": 7}]
    assert conn.cursor_obj.executed == [("SET statement_timeout = %s", (1234,))]

    assert 'schema_drift_check_status{status="drift"} 1' in rendered
    assert 'schema_drift_findings{category="rls"} 1' in rendered
    assert 'schema_drift_findings{category="column"} 1' in rendered
    assert 'schema_drift_findings{category="index"} 1' in rendered
    assert "unsafe_customer_table" not in rendered
    assert "sensitive_table" not in rendered


def test_schema_drift_monitor_reports_connect_errors_without_findings() -> None:
    default_registry().reset_for_tests()

    def _connect(_dsn: str, **_kwargs):
        raise RuntimeError("database unavailable")

    snapshot = monitor.run_schema_drift_check(
        "postgresql://example/db",
        connect=_connect,
    )
    rendered = monitor.render_schema_drift_metrics()

    assert snapshot.status == "error"
    assert snapshot.error == "RuntimeError"
    assert snapshot.findings_total == 0
    assert 'schema_drift_check_status{status="error"} 1' in rendered
    assert 'schema_drift_checks_total{result="error"} 1' in rendered
    assert 'schema_drift_findings{category="rls"} 0' in rendered


def test_schema_drift_classifier_covers_expected_prefixes() -> None:
    assert monitor.classify_drift("EXTENSION missing: vector") == "extension"
    assert monitor.classify_drift("TABLE missing: observations") == "table"
    assert (
        monitor.classify_drift(
            "TABLE observations: partitioning mismatch "
            "(expected is_partitioned=True, live=False)"
        )
        == "partition"
    )
    assert monitor.classify_drift("RLS force missing: observations is not forced") == "rls"
    assert monitor.classify_drift("COLUMN missing: models.foo") == "column"
    assert monitor.classify_drift("INDEX missing: models.models_idx") == "index"
    assert monitor.classify_drift("CHECK missing: models.models_status_check") == "check"
    assert monitor.classify_drift("something surprising") == "unknown"
