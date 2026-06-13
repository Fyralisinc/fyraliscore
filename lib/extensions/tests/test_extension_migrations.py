"""Unit tests for the extension-owned migration seam (lib/extensions/migrations.py).

Pure (no DB): discovery (path resolution, callable + str forms, failure
isolation) and the per-extension ledger naming that prevents filename collisions
with the host's global ledger.
"""
from __future__ import annotations

import pytest

from lib.extensions import migrations


class _FakeEP:
    def __init__(self, name, obj):
        self.name = name
        self._obj = obj

    def load(self):
        return self._obj


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(
        migrations.importlib_metadata, "entry_points", lambda group=None: []
    )
    migrations.reset_for_tests()
    yield
    migrations.reset_for_tests()


def _patch_eps(monkeypatch, eps):
    def fake(group=None):
        return eps if group == migrations._ENTRY_POINT_GROUP else []

    monkeypatch.setattr(migrations.importlib_metadata, "entry_points", fake)
    migrations.reset_for_tests()


def test_ledger_namespacing_is_per_extension_and_sanitized():
    assert migrations._ledger_for("github_intel") == "schema_migrations_ext_github_intel"
    # Non-identifier chars are collapsed so the table name is always valid.
    assert migrations._ledger_for("Acme.Corp/Ext") == "schema_migrations_ext_acme_corp_ext"
    assert migrations._ledger_for("") == "schema_migrations_ext_unknown"


def test_ledger_name_is_length_capped_and_unique_for_long_ids():
    a = "x" * 60 + "_alpha"
    b = "x" * 60 + "_beta"
    la, lb = migrations._ledger_for(a), migrations._ledger_for(b)
    # Both fit in Postgres' 63-byte identifier limit...
    assert len(la) <= 63 and len(lb) <= 63
    # ...and remain DISTINCT (a hash suffix preserves uniqueness past truncation).
    assert la != lb


async def test_apply_extension_migrations_rejects_ledger_collision(monkeypatch):
    # Two distinct ids that sanitize to the SAME ledger must fail fast, never
    # silently share a ledger (which would skip one's migrations as 'applied').
    monkeypatch.setattr(
        migrations, "discover_migration_dirs",
        lambda: [("acme.corp", __import__("pathlib").Path("/x")),
                 ("acme_corp", __import__("pathlib").Path("/y"))],
    )
    with pytest.raises(RuntimeError, match="ledger collision"):
        await migrations.apply_extension_migrations(conn=None)


def test_discovers_str_and_callable_paths(tmp_path, monkeypatch):
    d1 = tmp_path / "ext1"
    d1.mkdir()
    d2 = tmp_path / "ext2"
    d2.mkdir()
    _patch_eps(monkeypatch, [
        _FakeEP("ext1", str(d1)),            # plain string path
        _FakeEP("ext2", lambda: str(d2)),    # zero-arg callable returning path
    ])
    found = dict((eid, p) for eid, p in migrations.discover_migration_dirs())
    assert set(found) == {"ext1", "ext2"}
    assert found["ext1"] == d1 and found["ext2"] == d2


def test_nonexistent_path_is_skipped(tmp_path, monkeypatch):
    good = tmp_path / "good"
    good.mkdir()
    _patch_eps(monkeypatch, [
        _FakeEP("good", str(good)),
        _FakeEP("missing", str(tmp_path / "does-not-exist")),
    ])
    assert [eid for eid, _ in migrations.discover_migration_dirs()] == ["good"]


def test_exploding_entry_point_is_isolated(tmp_path, monkeypatch):
    class _Boom:
        name = "boom"

        def load(self):
            raise ImportError("nope")

    good = tmp_path / "good"
    good.mkdir()
    _patch_eps(monkeypatch, [_Boom(), _FakeEP("good", str(good))])
    # One bad contributor must not stop the others (or raise).
    assert [eid for eid, _ in migrations.discover_migration_dirs()] == ["good"]
