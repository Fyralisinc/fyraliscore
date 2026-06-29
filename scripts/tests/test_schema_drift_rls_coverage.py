from __future__ import annotations

import scripts.check_schema_drift as drift


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _Conn:
    def cursor(self) -> _Cursor:
        return _Cursor()


def test_schema_drift_fails_live_tenant_table_without_rls(monkeypatch) -> None:
    monkeypatch.setattr(drift, "EXPECTED_EXTENSIONS", set())
    monkeypatch.setattr(drift, "EXPECTED_TABLES", {})
    monkeypatch.setattr(drift, "fetch_live_extensions", lambda _cur: set())
    monkeypatch.setattr(drift, "fetch_live_tables", lambda _cur: set())
    monkeypatch.setattr(drift, "fetch_live_partitioned_parents", lambda _cur: set())
    monkeypatch.setattr(drift, "fetch_live_tenant_tables", lambda _cur: {"unsafe"})
    monkeypatch.setattr(drift, "fetch_live_rls", lambda _cur, _table: (False, False))

    drifts = drift.compare(_Conn())

    assert "RLS missing: unsafe is not enabled" in drifts
    assert "RLS force missing: unsafe is not forced" in drifts
