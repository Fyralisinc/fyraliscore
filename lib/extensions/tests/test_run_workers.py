"""Unit tests for the generic extension-worker supervisor (lib/extensions/run_workers.py).

Pure (no DB): discovery failure-isolation, host-API gating of contributed
workers, and the per-worker supervise loop (one-shot, exception-swallowing,
shutdown-respecting).
"""
from __future__ import annotations

import asyncio

import pytest

from lib.extensions import run_workers
from lib.extensions.host_api.v1 import BackgroundWorker
from lib.extensions.manifest import ExtensionManifest
from lib.observability.health import Heartbeat


class _FakeEP:
    def __init__(self, name, obj):
        self.name = name
        self._obj = obj

    def load(self):
        return self._obj


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(
        run_workers.importlib_metadata, "entry_points", lambda group=None: []
    )
    run_workers.reset_for_tests()
    yield
    run_workers.reset_for_tests()


def _patch_eps(monkeypatch, eps):
    def fake(group=None):
        return eps if group == run_workers._ENTRY_POINT_GROUP else []

    monkeypatch.setattr(run_workers.importlib_metadata, "entry_points", fake)
    run_workers.reset_for_tests()


async def _noop(*, pool, shutdown):
    return None


def test_discovers_worker_instance_list_and_callable(monkeypatch):
    w1 = BackgroundWorker(name="a", run=_noop, manifest_id="m")
    w2 = BackgroundWorker(name="b", run=_noop, manifest_id="m")
    _patch_eps(monkeypatch, [
        _FakeEP("inst", w1),
        _FakeEP("factory", lambda: w2),
        _FakeEP("listy", lambda: [w1, w2]),
    ])
    names = sorted(w.name for w in run_workers.discover_workers())
    assert names == ["a", "a", "b", "b"]


def test_bad_type_and_exploding_ep_are_isolated(monkeypatch):
    class _Boom:
        name = "boom"

        def load(self):
            raise ImportError("nope")

    good = BackgroundWorker(name="good", run=_noop, manifest_id=None)
    _patch_eps(monkeypatch, [
        _Boom(),
        _FakeEP("bad", "not-a-worker"),
        _FakeEP("good", good),
    ])
    # Discovery never raises; the good one survives.
    assert [w.name for w in run_workers.discover_workers()] == ["good"]


def test_active_workers_gated_by_active_manifest(monkeypatch):
    incompatible = BackgroundWorker(name="x", run=_noop, manifest_id="x")
    compatible = BackgroundWorker(name="y", run=_noop, manifest_id="y")
    in_repo = BackgroundWorker(name="z", run=_noop, manifest_id=None)
    _patch_eps(monkeypatch, [
        _FakeEP("x", incompatible), _FakeEP("y", compatible), _FakeEP("z", in_repo),
    ])
    # Only manifest 'y' is active; 'x' is filtered out, 'z' (in-repo) always runs.
    monkeypatch.setattr(
        run_workers, "active_manifests",
        lambda: [ExtensionManifest(id="y", trust_tier="first_party")],
    )
    assert sorted(w.name for w in run_workers.active_workers()) == ["y", "z"]


async def test_supervise_once_runs_a_single_pass():
    calls: list[int] = []

    async def _run(*, pool, shutdown):
        calls.append(1)

    w = BackgroundWorker(name="o", run=_run, manifest_id=None, mode="interval")
    stop = asyncio.Event()
    await run_workers._supervise(w, pool=None, shutdown=stop, heartbeat=Heartbeat(), once=True)
    assert calls == [1]


async def test_supervise_swallows_exceptions_in_once_mode():
    async def _boom(*, pool, shutdown):
        raise RuntimeError("pass failed")

    w = BackgroundWorker(name="boom", run=_boom, manifest_id=None)
    stop = asyncio.Event()
    # Must not propagate — prime directive.
    await run_workers._supervise(w, pool=None, shutdown=stop, heartbeat=Heartbeat(), once=True)


async def test_supervise_loop_exits_on_shutdown():
    passes = {"n": 0}

    async def _run(*, pool, shutdown):
        passes["n"] += 1

    # Tiny interval; set shutdown after the first pass so the loop exits promptly.
    w = BackgroundWorker(name="loop", run=_run, manifest_id=None, interval_s=0.01)
    stop = asyncio.Event()

    async def _stopper():
        await asyncio.sleep(0.03)
        stop.set()

    await asyncio.gather(
        run_workers._supervise(w, pool=None, shutdown=stop, heartbeat=Heartbeat(), once=False),
        _stopper(),
    )
    assert passes["n"] >= 1


def test_interval_worker_rejects_nonpositive_interval():
    with pytest.raises(ValueError):
        BackgroundWorker(name="bad", run=_noop, interval_s=0, mode="interval")
    with pytest.raises(ValueError):
        BackgroundWorker(name="bad", run=_noop, interval_s=-1, mode="interval")
    # forever mode ignores interval_s, so 0 is allowed there.
    BackgroundWorker(name="ok", run=_noop, interval_s=0, mode="forever")


async def test_supervise_skips_forever_worker_in_once_mode():
    calls: list[int] = []

    async def _run(*, pool, shutdown):
        calls.append(1)

    w = BackgroundWorker(name="fv", run=_run, manifest_id=None, mode="forever")
    stop = asyncio.Event()
    # In once mode a forever worker has no bounded pass → it must be skipped,
    # not invoked (else it would block on shutdown.wait() forever).
    await run_workers._supervise(w, pool=None, shutdown=stop, heartbeat=Heartbeat(), once=True)
    assert calls == []


async def test_supervise_retries_crashed_forever_worker_until_shutdown():
    attempts = {"n": 0}

    async def _run(*, pool, shutdown):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("forever worker crashed")  # crash on first attempt
        shutdown.set()  # second attempt returns cleanly on shutdown

    w = BackgroundWorker(name="fv", run=_run, manifest_id=None, mode="forever", interval_s=0.01)
    stop = asyncio.Event()
    # A crashed forever worker is re-invoked (not abandoned); it stops once it
    # returns cleanly with shutdown set. (Backoff after the crash is bounded.)
    await asyncio.wait_for(
        run_workers._supervise(w, pool=None, shutdown=stop, heartbeat=Heartbeat(), once=False),
        timeout=5,
    )
    assert attempts["n"] == 2


async def test_forever_mode_runs_once_and_returns():
    """A 'forever' worker owns its loop; the supervisor calls run exactly once."""
    calls: list[int] = []

    async def _run(*, pool, shutdown):
        calls.append(1)  # returns immediately → supervisor must not re-loop it

    w = BackgroundWorker(name="f", run=_run, manifest_id=None, mode="forever")
    stop = asyncio.Event()
    await run_workers._supervise(w, pool=None, shutdown=stop, heartbeat=Heartbeat(), once=False)
    assert calls == [1]
