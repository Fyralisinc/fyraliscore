"""Unit tests for the draft-enricher registry (services/ingest/ingestion/enrichers.py).

Pure (no DB): exercises the E2 seam that generalizes the former hardcoded github
inline hook — registration (decorator + entry-point discovery), ordered
execution, the raw-on-failure guarantee, and introspection.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.extensions.host_api.v1 import DraftEnricher
from services.ingest.ingestion import enrichers


class _Draft:
    """Minimal duck-typed stand-in for ObservationDraft."""

    def __init__(self) -> None:
        self.content: dict = {}
        self.content_text = "raw"


@pytest.fixture(autouse=True)
def _isolate_registry(monkeypatch):
    # Neutralize entry-point discovery so installed extensions (e.g. the
    # github-intel package, when co-installed in the dev venv) don't leak into
    # these hermetic unit tests. Tests that exercise discovery re-patch
    # entry_points explicitly (later setattr on the same monkeypatch wins).
    monkeypatch.setattr(
        enrichers.importlib_metadata, "entry_points", lambda group=None: []
    )
    enrichers.reset_for_tests()
    yield
    enrichers.reset_for_tests()


class _FakeEP:
    def __init__(self, name, obj):
        self.name = name
        self._obj = obj

    def load(self):
        return self._obj


def _patch_entry_points(monkeypatch, eps):
    def fake_entry_points(group=None):
        return eps if group == enrichers._ENTRY_POINT_GROUP else []

    monkeypatch.setattr(enrichers.importlib_metadata, "entry_points", fake_entry_points)


async def _allow_gate(enr, *, pool, tenant_id):
    """Gate stub for discovery-mechanism tests (the gate itself is tested separately)."""
    return True


async def test_no_op_when_none_registered():
    draft = _Draft()
    # Must not raise and must not mutate the draft.
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())
    assert draft.content == {}
    assert enrichers.registered_channels() == []


async def test_runs_in_registration_order():
    calls: list[str] = []

    @enrichers.register_enricher("github:webhook", name="first")
    async def _first(draft, *, pool, tenant_id):
        calls.append("first")
        draft.content.setdefault("order", []).append("first")

    @enrichers.register_enricher("github:webhook", name="second")
    async def _second(draft, *, pool, tenant_id):
        calls.append("second")
        draft.content.setdefault("order", []).append("second")

    draft = _Draft()
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())

    assert calls == ["first", "second"]
    assert draft.content["order"] == ["first", "second"]
    assert enrichers.registered_channels() == ["github:webhook"]
    assert enrichers.enricher_names("github:webhook") == ["first", "second"]


async def test_raw_on_failure_swallowed_and_later_enrichers_still_run():
    @enrichers.register_enricher("github:webhook", name="boom")
    async def _boom(draft, *, pool, tenant_id):
        raise RuntimeError("enrichment exploded")

    @enrichers.register_enricher("github:webhook", name="ok")
    async def _ok(draft, *, pool, tenant_id):
        draft.content["intelligence"] = {"effect": "computed"}

    draft = _Draft()
    # The raising enricher must NOT propagate (raw-on-failure), and the later
    # enricher must still run.
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())
    assert draft.content == {"intelligence": {"effect": "computed"}}


async def test_only_runs_matching_channel():
    ran: list[str] = []

    @enrichers.register_enricher("github:webhook", name="gh")
    async def _gh(draft, *, pool, tenant_id):
        ran.append("gh")

    draft = _Draft()
    await enrichers.run_enrichers("slack:message", draft, pool=None, tenant_id=uuid4())
    assert ran == []


async def test_entry_point_discovery(monkeypatch):
    ran: list[str] = []

    async def _ext_enricher(draft, *, pool, tenant_id):
        ran.append("ext")
        draft.content["intelligence"] = {"effect": "ext"}

    ep = _FakeEP(
        "github_intel",
        DraftEnricher(channel="github:webhook", fn=_ext_enricher,
                      name="github_intel.inline", manifest_id="github_intel"),
    )
    _patch_entry_points(monkeypatch, [ep])
    # This test exercises discovery + execution, not the gate — allow it.
    monkeypatch.setattr(enrichers, "_gate_allows", _allow_gate)
    enrichers.reset_for_tests()  # force re-discovery under the patch

    draft = _Draft()
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())

    assert ran == ["ext"]
    assert draft.content["intelligence"] == {"effect": "ext"}
    assert "github:webhook" in enrichers.registered_channels()
    assert enrichers.enricher_names("github:webhook") == ["github_intel.inline"]


async def test_entry_point_callable_returning_list(monkeypatch):
    async def _a(draft, *, pool, tenant_id):
        draft.content.setdefault("seen", []).append("a")

    async def _b(draft, *, pool, tenant_id):
        draft.content.setdefault("seen", []).append("b")

    def _factory():
        return [
            DraftEnricher(channel="github:webhook", fn=_a, name="a", manifest_id="multi"),
            DraftEnricher(channel="github:webhook", fn=_b, name="b", manifest_id="multi"),
        ]

    _patch_entry_points(monkeypatch, [_FakeEP("multi", _factory)])
    monkeypatch.setattr(enrichers, "_gate_allows", _allow_gate)
    enrichers.reset_for_tests()

    draft = _Draft()
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())
    assert draft.content["seen"] == ["a", "b"]


def _gated_ep(monkeypatch, ran, *, manifest_present=True):
    """Register a discovered enricher carrying manifest_id='x' and a matching
    (or absent) manifest, returning a knob to set the gate decision."""
    from lib.extensions.manifest import ExtensionManifest

    async def _fn(draft, *, pool, tenant_id):
        ran.append("x")
        draft.content["ran"] = True

    ep = _FakeEP(
        "x",
        DraftEnricher(channel="github:webhook", fn=_fn, name="x.inline", manifest_id="x"),
    )
    _patch_entry_points(monkeypatch, [ep])
    enrichers.reset_for_tests()
    mans = [ExtensionManifest(id="x", trust_tier="third_party")] if manifest_present else []
    monkeypatch.setattr(enrichers, "active_manifests", lambda: mans)


async def test_gated_enricher_skipped_when_not_allowed(monkeypatch):
    import services.platform.extensions.access as access

    ran: list[str] = []
    _gated_ep(monkeypatch, ran)

    async def _deny(pool, *, tenant_id, manifest):
        return False

    monkeypatch.setattr(access, "enricher_allowed", _deny)

    draft = _Draft()
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())
    assert ran == []  # gate blocked it
    assert draft.content == {}


async def test_gated_enricher_runs_when_allowed(monkeypatch):
    import services.platform.extensions.access as access

    ran: list[str] = []
    _gated_ep(monkeypatch, ran)

    async def _allow(pool, *, tenant_id, manifest):
        assert manifest.id == "x"
        return True

    monkeypatch.setattr(access, "enricher_allowed", _allow)

    draft = _Draft()
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())
    assert ran == ["x"]
    assert draft.content.get("ran") is True


async def test_gated_enricher_skipped_when_manifest_undiscoverable(monkeypatch):
    ran: list[str] = []
    _gated_ep(monkeypatch, ran, manifest_present=False)
    # No manifest → governance unverifiable → skip (and never crash).
    draft = _Draft()
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())
    assert ran == []


async def test_bad_entry_point_is_isolated(monkeypatch):
    """A broken entry point must not break discovery of the good ones."""

    class _Exploding:
        @property
        def name(self):
            return "broken"

        def load(self):
            raise ImportError("cannot import broken extension")

    async def _good(draft, *, pool, tenant_id):
        draft.content["ok"] = True

    good = _FakeEP("good", DraftEnricher(channel="github:webhook", fn=_good,
                                         name="good", manifest_id="good"))
    _patch_entry_points(monkeypatch, [_Exploding(), good])
    monkeypatch.setattr(enrichers, "_gate_allows", _allow_gate)
    enrichers.reset_for_tests()

    draft = _Draft()
    await enrichers.run_enrichers("github:webhook", draft, pool=None, tenant_id=uuid4())
    assert draft.content.get("ok") is True
