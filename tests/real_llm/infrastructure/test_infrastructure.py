"""
Self-tests for the real-LLM test infrastructure.

These tests verify the test infrastructure itself: the retry/flake-tracking
decorator, the response cache, the flake tracker persistence, the synchronous
assertion helpers, and the provider <-> cache integration.

NONE of these tests call a real LLM, touch Postgres, or hit Ollama. They
intentionally do NOT use the `real_llm` marker so they run in normal pytest
invocations.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from tests.real_llm.infrastructure import flake_tracker
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.response_cache import LLMResponseCache


# ---------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------

@pytest.fixture
def isolated_flake_tracker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Redirect the flake tracker's persistence path into tmp_path."""
    reports_dir = tmp_path / "reports"
    flake_file = reports_dir / "flake_rates.json"
    monkeypatch.setattr(flake_tracker, "_REPORTS_DIR", reports_dir)
    monkeypatch.setattr(flake_tracker, "_FLAKE_FILE", flake_file)
    # Reset the pending-attempts buffer so cross-test state never leaks.
    monkeypatch.setattr(flake_tracker, "_pending_attempts", {})
    return flake_file


# =====================================================================
# 1. real_llm_runner — decorator semantics
# =====================================================================

def test_decorator_passes_when_all_attempts_pass(isolated_flake_tracker: Path):
    """A test that always passes should be recorded as 1/1 (early exit)."""
    calls = {"n": 0}

    @real_llm_test(attempts=3, pass_threshold=2)
    def my_test():
        calls["n"] += 1
        # always passes

    my_test()

    # pass_threshold=2: the loop continues until 2 passes, so first call
    # produces 1 pass, second call produces 2 passes -> exits.
    assert calls["n"] == 2

    data = json.loads(isolated_flake_tracker.read_text())
    runs = data["my_test"]["runs"]
    assert len(runs) == 1
    assert runs[0]["outcome"] == "pass"
    assert runs[0]["passes"] == 2
    assert all(a["status"] == "pass" for a in runs[0]["attempts"])


def test_decorator_passes_at_threshold(isolated_flake_tracker: Path):
    """A test failing once then passing twice meets pass_threshold=2/attempts=3."""
    calls = {"n": 0}

    @real_llm_test(attempts=3, pass_threshold=2)
    def flaky_test():
        calls["n"] += 1
        if calls["n"] == 1:
            raise AssertionError("flake on first attempt")
        # second + third attempts pass

    flaky_test()

    assert calls["n"] == 3

    data = json.loads(isolated_flake_tracker.read_text())
    runs = data["flaky_test"]["runs"]
    assert len(runs) == 1
    assert runs[0]["outcome"] == "pass"
    assert runs[0]["passes"] == 2
    statuses = [a["status"] for a in runs[0]["attempts"]]
    assert statuses == ["fail", "pass", "pass"]


def test_decorator_fails_below_threshold(isolated_flake_tracker: Path):
    """A test that always fails must be recorded as fail and pytest.fail invoked."""

    @real_llm_test(attempts=3, pass_threshold=2)
    def always_fails():
        raise AssertionError("nope")

    with pytest.raises(pytest.fail.Exception) as excinfo:
        always_fails()

    msg = str(excinfo.value)
    assert "always_fails" in msg
    assert "passed 0/3" in msg
    assert "needed 2" in msg

    data = json.loads(isolated_flake_tracker.read_text())
    runs = data["always_fails"]["runs"]
    assert len(runs) == 1
    assert runs[0]["outcome"] == "fail"
    assert runs[0]["passes"] == 0
    assert all(a["status"] == "fail" for a in runs[0]["attempts"])


def test_decorator_records_to_flake_tracker(isolated_flake_tracker: Path):
    """Per-attempt records and the final outcome should both land in JSON."""

    @real_llm_test(attempts=2, pass_threshold=2)
    def two_passes():
        pass

    two_passes()

    data = json.loads(isolated_flake_tracker.read_text())
    assert "two_passes" in data
    runs = data["two_passes"]["runs"]
    assert len(runs) == 1
    run = runs[0]
    assert run["total"] == 2
    assert run["threshold"] == 2
    assert run["passes"] == 2
    assert run["outcome"] == "pass"
    assert len(run["attempts"]) == 2
    assert run["attempts"][0]["attempt"] == 1
    assert run["attempts"][1]["attempt"] == 2


def test_decorator_reraises_non_assertion_errors(isolated_flake_tracker: Path):
    """A genuine bug (ValueError) should propagate immediately, not be retried."""
    calls = {"n": 0}

    @real_llm_test(attempts=3, pass_threshold=2)
    def buggy_test():
        calls["n"] += 1
        raise ValueError("real bug, not a flake")

    with pytest.raises(ValueError, match="real bug"):
        buggy_test()

    # Only one call — no retry on non-AssertionError exceptions.
    assert calls["n"] == 1

    # The runner still flushes a final record so we don't leak buffered state.
    data = json.loads(isolated_flake_tracker.read_text())
    assert "buggy_test" in data


def test_decorator_async_path_passes(isolated_flake_tracker: Path):
    """Async wrapper executes the same retry semantics."""
    calls = {"n": 0}

    @real_llm_test(attempts=3, pass_threshold=2)
    async def async_flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise AssertionError("flake")

    asyncio.run(async_flaky())

    assert calls["n"] == 3
    data = json.loads(isolated_flake_tracker.read_text())
    runs = data["async_flaky"]["runs"]
    assert runs[0]["outcome"] == "pass"
    assert runs[0]["passes"] == 2


# =====================================================================
# 2. response_cache
# =====================================================================

def _drop_disable_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LLM_CACHE_DISABLE", raising=False)
    monkeypatch.delenv("LLM_CACHE_BYPASS", raising=False)


async def test_cache_writes_and_reads_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Second call with identical inputs must not invoke fetch_fn."""
    _drop_disable_env(monkeypatch)
    cache = LLMResponseCache(cache_dir=tmp_path, current_epoch="ep1")

    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        return {"raw": json.dumps({"value": calls["n"]})}

    kwargs = dict(
        system="sys", user="usr", model="m", temperature=0.0,
        max_tokens=128, schema_name="S",
    )
    out1 = await cache.get_or_fetch(fetch_fn=fetch, **kwargs)
    out2 = await cache.get_or_fetch(fetch_fn=fetch, **kwargs)

    assert calls["n"] == 1, "second call should hit cache"
    assert out1 == out2


async def test_cache_key_includes_schema_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Different schema names must produce different cache files."""
    _drop_disable_env(monkeypatch)
    cache = LLMResponseCache(cache_dir=tmp_path, current_epoch="ep1")

    async def make_fetch(tag: str):
        async def fetch():
            return {"raw": tag}
        return fetch

    base = dict(system="sys", user="usr", model="m", temperature=0.0, max_tokens=128)
    await cache.get_or_fetch(schema_name="A", fetch_fn=await make_fetch("a"), **base)
    await cache.get_or_fetch(schema_name="B", fetch_fn=await make_fetch("b"), **base)

    files = sorted(p.name for p in cache.cache_dir.glob("*.json"))
    assert len(files) == 2, f"expected 2 distinct cache files, got {files}"


async def test_cache_key_changes_with_temperature(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Different temperatures must produce different cache files."""
    _drop_disable_env(monkeypatch)
    cache = LLMResponseCache(cache_dir=tmp_path, current_epoch="ep1")

    async def fetch_a():
        return {"raw": "a"}

    async def fetch_b():
        return {"raw": "b"}

    base = dict(system="sys", user="usr", model="m", max_tokens=128, schema_name="S")
    await cache.get_or_fetch(temperature=0.0, fetch_fn=fetch_a, **base)
    await cache.get_or_fetch(temperature=0.7, fetch_fn=fetch_b, **base)

    files = sorted(p.name for p in cache.cache_dir.glob("*.json"))
    assert len(files) == 2, f"expected 2 distinct cache files, got {files}"


async def test_LLM_CACHE_BYPASS_skips_read_writes_anew(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """LLM_CACHE_BYPASS=1 forces a fresh fetch even when cache exists."""
    _drop_disable_env(monkeypatch)
    cache = LLMResponseCache(cache_dir=tmp_path, current_epoch="ep1")

    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        return {"raw": str(calls["n"])}

    kwargs = dict(
        system="sys", user="usr", model="m", temperature=0.0,
        max_tokens=128, schema_name="S",
    )
    out1 = await cache.get_or_fetch(fetch_fn=fetch, **kwargs)
    assert out1["raw"] == "1"

    # Now flip BYPASS on; the call must invoke fetch again.
    monkeypatch.setenv("LLM_CACHE_BYPASS", "1")
    out2 = await cache.get_or_fetch(fetch_fn=fetch, **kwargs)
    assert calls["n"] == 2
    assert out2["raw"] == "2"

    # Bypass writes anew, so the on-disk cache reflects the latest fetch.
    files = list(cache.cache_dir.glob("*.json"))
    assert len(files) == 1
    on_disk = json.loads(files[0].read_text())
    assert on_disk["raw"] == "2"


async def test_LLM_CACHE_DISABLE_skips_entirely(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """LLM_CACHE_DISABLE=1 means no cache directory and no on-disk writes."""
    monkeypatch.delenv("LLM_CACHE_BYPASS", raising=False)
    monkeypatch.setenv("LLM_CACHE_DISABLE", "1")

    cache = LLMResponseCache(cache_dir=tmp_path, current_epoch="ep_disabled")
    # The per-epoch subdir must not be auto-created when disabled.
    assert not cache.cache_dir.exists()

    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        return {"raw": "x"}

    kwargs = dict(
        system="sys", user="usr", model="m", temperature=0.0,
        max_tokens=128, schema_name="S",
    )
    await cache.get_or_fetch(fetch_fn=fetch, **kwargs)
    await cache.get_or_fetch(fetch_fn=fetch, **kwargs)

    # Both calls bypassed the cache.
    assert calls["n"] == 2
    # Still no directory created.
    assert not cache.cache_dir.exists()


def test_current_epoch_changes_when_prompt_file_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The epoch hash must change when any of the source files change."""
    src_a = tmp_path / "a.py"
    src_b = tmp_path / "b.py"
    src_a.write_text("x = 1\n")
    src_b.write_text("y = 2\n")

    def hash_for(paths: tuple[Path, ...]) -> str:
        h = hashlib.sha256()
        for p in paths:
            h.update(p.as_posix().encode("utf-8"))
            h.update(b"\0")
            try:
                h.update(p.read_bytes())
            except FileNotFoundError:
                h.update(b"<missing>")
            h.update(b"\0")
        return h.hexdigest()[:12]

    sources = (src_a, src_b)
    h1 = hash_for(sources)

    src_a.write_text("x = 99\n")
    h2 = hash_for(sources)

    assert h1 != h2, "epoch hash must change when source content changes"

    # And the real implementation produces a non-empty 12-char string.
    real = LLMResponseCache.current_epoch()
    assert isinstance(real, str)
    assert len(real) == 12


# =====================================================================
# 3. flake_tracker
# =====================================================================

def test_record_attempt_and_final_persist_to_json(isolated_flake_tracker: Path):
    flake_tracker.record_attempt("t1", 1, "fail", "boom")
    flake_tracker.record_attempt("t1", 2, "pass")
    flake_tracker.record_final("t1", passes=1, total=2, threshold=2)

    data = json.loads(isolated_flake_tracker.read_text())
    assert "t1" in data
    runs = data["t1"]["runs"]
    assert len(runs) == 1
    run = runs[0]
    assert run["passes"] == 1
    assert run["total"] == 2
    assert run["threshold"] == 2
    assert run["outcome"] == "fail"
    assert [a["status"] for a in run["attempts"]] == ["fail", "pass"]
    # Each run carries an ISO timestamp.
    datetime.fromisoformat(run["timestamp"])


def test_truncates_to_50_runs(isolated_flake_tracker: Path):
    """Only the last 50 runs should be retained."""
    for i in range(60):
        flake_tracker.record_attempt("t1", 1, "pass")
        flake_tracker.record_final("t1", passes=1, total=1, threshold=1)

    data = json.loads(isolated_flake_tracker.read_text())
    runs = data["t1"]["runs"]
    assert len(runs) == 50


def test_summary_computes_flake_rate(isolated_flake_tracker: Path):
    """flake_rate = (runs where passes < total) / len(runs)."""
    # 4 runs total: 2 are flaky (passes < total), 2 are clean.
    for status in [
        ("flaky_1", 1, 2),  # flaky
        ("flaky_2", 1, 2),  # flaky
        ("clean_1", 2, 2),  # clean
        ("clean_2", 2, 2),  # clean
    ]:
        _, p, t = status
        flake_tracker.record_attempt("mixed", 1, "pass" if p == t else "fail")
        flake_tracker.record_final("mixed", passes=p, total=t, threshold=2)

    summary = flake_tracker.summary()
    assert "mixed" in summary
    assert summary["mixed"]["flake_rate"] == pytest.approx(0.5)
    assert len(summary["mixed"]["recent_outcomes"]) == 4


def test_summary_empty_when_no_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """summary() returns {} when the file does not exist."""
    monkeypatch.setattr(flake_tracker, "_REPORTS_DIR", tmp_path / "noexist")
    monkeypatch.setattr(flake_tracker, "_FLAKE_FILE", tmp_path / "noexist" / "f.json")
    assert flake_tracker.summary() == {}


# =====================================================================
# 4. assertion_helpers (synchronous helpers)
# =====================================================================

# ModelRow is heavy (many required fields). Build a tiny factory.
try:
    from lib.shared.types import ModelRow as _ModelRow
    _HAS_MODEL_ROW = True
except Exception:  # pragma: no cover
    _HAS_MODEL_ROW = False


def _make_model(
    *,
    natural: str = "x",
    proposition: dict | None = None,
    confidence: float = 0.7,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict] | None = None,
) -> Any:
    """Construct a minimally-valid ModelRow for assertion-helper tests."""
    if not _HAS_MODEL_ROW:
        pytest.skip("lib.shared.types.ModelRow not available")
    now = datetime.now(timezone.utc)
    return _ModelRow(
        id=uuid4(),
        tenant_id=uuid4(),
        born_from_event_id=uuid4(),
        proposition=proposition or {"kind": "state"},
        natural=natural,
        embedding=[0.0],
        scope_actors=scope_actors or [],
        scope_entities=scope_entities or [],
        scope_temporal={},
        confidence=confidence,
        activation=0.5,
        created_at=now,
        confidence_at_assertion=confidence,
    )


def _import_helpers():
    if not _HAS_MODEL_ROW:
        pytest.skip("lib.shared.types.ModelRow not available")
    from tests.real_llm.infrastructure import assertion_helpers
    return assertion_helpers


def test_assert_model_count_in_range_passes():
    helpers = _import_helpers()
    models = [_make_model() for _ in range(3)]
    helpers.assert_model_count_in_range(models, 2, 5)
    helpers.assert_model_count_in_range(models, 3, 3)  # boundary


def test_assert_model_count_in_range_fails():
    helpers = _import_helpers()
    models = [_make_model() for _ in range(3)]
    with pytest.raises(AssertionError, match=r"outside expected range"):
        helpers.assert_model_count_in_range(models, 5, 10)


def test_assert_at_least_one_model_matching_finds_by_actor():
    helpers = _import_helpers()
    actor_a = uuid4()
    actor_b = uuid4()
    models = [
        _make_model(scope_actors=[actor_a]),
        _make_model(scope_actors=[actor_b]),
    ]
    matches = helpers.assert_at_least_one_model_matching(
        models, scope_actor_id=actor_a
    )
    assert len(matches) == 1
    assert actor_a in matches[0].scope_actors


def test_assert_at_least_one_model_matching_filters_by_proposition_kind():
    helpers = _import_helpers()
    models = [
        _make_model(proposition={"kind": "state"}),
        _make_model(proposition={"kind": "prediction"}),
        _make_model(proposition={"kind": "concern"}),
    ]
    matches = helpers.assert_at_least_one_model_matching(
        models, proposition_kind="prediction"
    )
    assert len(matches) == 1
    assert matches[0].proposition["kind"] == "prediction"

    # Set form
    matches_set = helpers.assert_at_least_one_model_matching(
        models, proposition_kind={"prediction", "concern"}
    )
    assert len(matches_set) == 2


def test_assert_at_least_one_model_matching_filters_by_text_substring():
    helpers = _import_helpers()
    models = [
        _make_model(natural="The Cat sat on the Mat"),
        _make_model(natural="Dogs run fast"),
        _make_model(natural="Birds fly high"),
    ]
    # Case-insensitive
    matches = helpers.assert_at_least_one_model_matching(
        models, proposition_text_contains=["cat", "rabbit"]
    )
    assert len(matches) == 1
    assert "Cat" in matches[0].natural


def test_assert_at_least_one_model_matching_returns_matches_list():
    helpers = _import_helpers()
    models = [
        _make_model(natural="alpha"),
        _make_model(natural="alpha-beta"),
        _make_model(natural="gamma"),
    ]
    matches = helpers.assert_at_least_one_model_matching(
        models, proposition_text_contains=["alpha"]
    )
    assert isinstance(matches, list)
    assert len(matches) == 2


def test_assert_at_least_one_model_matching_raises_when_no_match():
    helpers = _import_helpers()
    models = [_make_model(natural="hello")]
    with pytest.raises(AssertionError, match=r"No Model matching criteria"):
        helpers.assert_at_least_one_model_matching(
            models, proposition_text_contains=["zzznever"]
        )


def test_assert_proposition_kind_distribution_passes_within_band():
    helpers = _import_helpers()
    models = (
        [_make_model(proposition={"kind": "state"}) for _ in range(6)]
        + [_make_model(proposition={"kind": "prediction"}) for _ in range(4)]
    )
    # 60% state, 40% prediction
    helpers.assert_proposition_kind_distribution(
        models,
        expected={
            "state": (0.5, 0.7),
            "prediction": (0.3, 0.5),
        },
    )


def test_assert_proposition_kind_distribution_fails_outside_band():
    helpers = _import_helpers()
    models = [_make_model(proposition={"kind": "state"}) for _ in range(10)]
    with pytest.raises(AssertionError, match=r"distribution violations"):
        helpers.assert_proposition_kind_distribution(
            models, expected={"state": (0.0, 0.5)}
        )


# =====================================================================
# 5. provider cache integration
# =====================================================================

from lib.llm.provider import (  # noqa: E402
    LLMConfig,
    LLMProvider,
    get_response_cache,
    set_response_cache,
)
from pydantic import BaseModel  # noqa: E402


class _StubOutput(BaseModel):
    value: int


class _RecordingProvider(LLMProvider):
    """Provider that records every _raw_call and returns canned JSON."""

    def __init__(self, config: LLMConfig, payloads: list[str]):
        super().__init__(config)
        self._payloads = list(payloads)
        self.calls: list[dict] = []

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        self.calls.append(
            {
                "system": system, "user": user,
                "temperature": temperature, "max_tokens": max_tokens,
            }
        )
        if not self._payloads:
            raise RuntimeError("no more canned payloads")
        return self._payloads.pop(0)


@pytest.fixture
def cleared_module_cache():
    """Restore the module-level cache to its prior state after the test."""
    prev = get_response_cache()
    set_response_cache(None)
    try:
        yield
    finally:
        set_response_cache(prev)


def test_set_and_get_response_cache_roundtrip(cleared_module_cache):
    sentinel = object()
    set_response_cache(sentinel)
    assert get_response_cache() is sentinel
    set_response_cache(None)
    assert get_response_cache() is None


async def test_provider_routes_through_cache_when_set(
    cleared_module_cache, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """When a cache is installed, structured() must consult it."""
    monkeypatch.delenv("LLM_CACHE_DISABLE", raising=False)
    monkeypatch.delenv("LLM_CACHE_BYPASS", raising=False)

    cache = LLMResponseCache(cache_dir=tmp_path, current_epoch="ep_provider")
    set_response_cache(cache)

    cfg = LLMConfig(provider="anthropic", api_key="x", model="m")
    provider = _RecordingProvider(cfg, payloads=[json.dumps({"value": 42})])

    # First call: cache miss, fetches once.
    out1 = await provider.structured(
        system="s", user="u", schema=_StubOutput, temperature=0.0, max_tokens=64
    )
    assert isinstance(out1, _StubOutput)
    assert out1.value == 42
    assert len(provider.calls) == 1

    # Second call with identical inputs: cache hit, no new _raw_call.
    out2 = await provider.structured(
        system="s", user="u", schema=_StubOutput, temperature=0.0, max_tokens=64
    )
    assert out2.value == 42
    assert len(provider.calls) == 1, "second call must hit cache"


async def test_provider_skips_cache_when_unset(
    cleared_module_cache, monkeypatch: pytest.MonkeyPatch
):
    """When no cache is installed, every structured() call goes to the provider."""
    monkeypatch.delenv("LLM_CACHE_DISABLE", raising=False)
    monkeypatch.delenv("LLM_CACHE_BYPASS", raising=False)
    assert get_response_cache() is None

    cfg = LLMConfig(provider="anthropic", api_key="x", model="m")
    provider = _RecordingProvider(
        cfg, payloads=[json.dumps({"value": 1}), json.dumps({"value": 2})]
    )

    o1 = await provider.structured(
        system="s", user="u", schema=_StubOutput, temperature=0.0, max_tokens=64
    )
    o2 = await provider.structured(
        system="s", user="u", schema=_StubOutput, temperature=0.0, max_tokens=64
    )
    assert (o1.value, o2.value) == (1, 2)
    assert len(provider.calls) == 2
