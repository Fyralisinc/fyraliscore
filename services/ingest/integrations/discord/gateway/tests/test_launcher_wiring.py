"""Regression tests for scripts/run_discord_gateway_worker.py HA wiring.

The M4.1 lease + M4.2 crash-RESUME primitives were built and tested
(see test_leader_lock.py / test_session_state.py / test_gateway_lifecycle.py),
but the production *launcher* constructed `GatewayWorker(bot_token=, deps=)`
directly — never acquiring the lease, never passing `on_dispatched`. Two
replicas would double-deliver; a restart always re-IDENTIFYd and dropped
Discord's buffered frames.

These tests pin the launcher's composition so that regression can't
silently come back:

  - missing REDIS_URL fails loud (the lease is the only double-delivery
    guard) → exit 2;
  - the single-instance lease is acquired BEFORE the worker runs;
  - the persisted session state + save hook are threaded into the worker
    (crash-RESUME) when an application id is configured;
  - without an application id the lease still protects but RESUME is off;
  - losing the lease mid-run shuts the worker down and exits 3 (transient);
  - a lease-acquire timeout exits 3 without ever constructing the worker.

Everything heavy (Postgres / Redis / Kafka / the real worker) is stubbed
on the loaded launcher module — this is a pure composition test.
"""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import types

import pytest


pytestmark = pytest.mark.asyncio


_LAUNCHER_PATH = (
    pathlib.Path(__file__).resolve().parents[6]
    / "scripts" / "run_discord_gateway_worker.py"
)

# Sentinels threaded through the wiring so assertions are identity-based.
_SENTINEL_PERSISTED = object()
_SENTINEL_STATE = types.SimpleNamespace(last_seq=99, session_id="sess-X")
_SENTINEL_HOOK = object()


def _load_launcher():
    """Import scripts/run_discord_gateway_worker.py as a standalone module
    (scripts/ is not a package)."""
    spec = importlib.util.spec_from_file_location(
        "_run_discord_gateway_worker_under_test", _LAUNCHER_PATH,
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------
# Test doubles for the launcher's collaborators.
# ---------------------------------------------------------------------
class _FakePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _FakeRedis:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class _FakeLock:
    def __init__(self, redis, **_kw) -> None:
        self.redis = redis
        self.key = "gateway:discord:leader_lock"
        self.ttl_seconds = 30
        self.lease_value = "lease-test-value"
        self.released = False

    async def release(self) -> bool:
        self.released = True
        return True


class _FakeWorker:
    """Captures constructor kwargs; run_forever blocks until shutdown."""

    instances: list["_FakeWorker"] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.shutdown = asyncio.Event()
        self.request_shutdown_called = False
        self.run_forever_started = False
        _FakeWorker.instances.append(self)

    def request_shutdown(self) -> None:
        self.request_shutdown_called = True
        self.shutdown.set()

    async def run_forever(self) -> int:
        self.run_forever_started = True
        # Default: return immediately (clean shutdown). Tests that need
        # the "lease lost mid-run" path drive request_shutdown() via the
        # patched refresh loop, which sets `shutdown`.
        if not self.shutdown.is_set():
            self.shutdown.set()
        await self.shutdown.wait()
        return 0


def _install_common_stubs(mod, monkeypatch, *, events: list[str]) -> None:
    """Stub every heavy collaborator the launcher pulls in. `events`
    records ordering so tests can assert acquire-before-run."""

    async def _fake_create_pool(*_a, **_kw):
        return _FakePool()

    monkeypatch.setattr(mod.asyncpg, "create_pool", _fake_create_pool)
    monkeypatch.setattr(
        mod, "AsyncRedis",
        types.SimpleNamespace(from_url=lambda *_a, **_kw: _FakeRedis()),
    )
    monkeypatch.setattr(mod, "LeaderLock", _FakeLock)
    monkeypatch.setattr(mod, "GatewayWorker", _FakeWorker)

    # Lightweight dep builders.
    monkeypatch.setattr(mod, "build_secret_store", lambda _pool: object())
    monkeypatch.setattr(mod, "build_tenant_resolver", lambda _deps: object())
    monkeypatch.setattr(mod, "TenantResolverDeps", lambda **_kw: object())
    monkeypatch.setattr(mod, "InstallationCache", lambda: object())
    monkeypatch.setattr(mod, "default_metrics", lambda: object())
    monkeypatch.setattr(mod, "ActorRepo", lambda _pool: object())
    monkeypatch.setattr(mod, "EntityAliasRepo", lambda _pool: object())
    monkeypatch.setattr(mod, "OllamaClient", lambda: object())
    monkeypatch.setattr(mod, "DispatchDeps", lambda **kw: types.SimpleNamespace(**kw))

    # M4.2 RESUME path stubs.
    async def _fake_load(_pool, **_kw):
        events.append("load_state")
        return _SENTINEL_PERSISTED

    monkeypatch.setattr(mod, "load_session_state", _fake_load)
    monkeypatch.setattr(
        mod, "persisted_to_in_memory",
        lambda persisted: _SENTINEL_STATE if persisted is _SENTINEL_PERSISTED else None,
    )
    monkeypatch.setattr(
        mod, "make_save_hook",
        lambda _pool, **_kw: _SENTINEL_HOOK,
    )


@pytest.fixture(autouse=True)
def _reset_worker_instances():
    _FakeWorker.instances.clear()
    yield
    _FakeWorker.instances.clear()


@pytest.fixture(autouse=True)
async def _no_signal_handlers():
    """Neutralize the launcher's add_signal_handler so the test doesn't
    register SIGTERM/SIGINT handlers on the pytest event loop."""
    loop = asyncio.get_running_loop()
    orig = loop.add_signal_handler
    loop.add_signal_handler = lambda *_a, **_kw: None  # type: ignore[method-assign]
    try:
        yield
    finally:
        loop.add_signal_handler = orig  # type: ignore[method-assign]


def _base_env(monkeypatch, *, application_id: str | None, redis_url: str | None) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "bot-token")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)  # skip data-plane block
    if application_id is None:
        monkeypatch.delenv("DISCORD_CLIENT_ID", raising=False)
    else:
        monkeypatch.setenv("DISCORD_CLIENT_ID", application_id)
    if redis_url is None:
        monkeypatch.delenv("REDIS_URL", raising=False)
    else:
        monkeypatch.setenv("REDIS_URL", redis_url)


# ---------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------
async def test_missing_redis_url_fails_loud(monkeypatch):
    """Without REDIS_URL there is no double-delivery guard — exit 2, and
    never construct a worker."""
    mod = _load_launcher()
    events: list[str] = []
    _install_common_stubs(mod, monkeypatch, events=events)
    _base_env(monkeypatch, application_id="app-1", redis_url=None)

    rc = await mod._main()

    assert rc == 2
    assert _FakeWorker.instances == [], "worker must not start without the lease"


async def test_lease_acquired_and_resume_wired(monkeypatch):
    """Happy path: lease acquired BEFORE the worker runs, and the worker
    is constructed with the persisted initial_state + save hook."""
    mod = _load_launcher()
    events: list[str] = []
    _install_common_stubs(mod, monkeypatch, events=events)
    _base_env(monkeypatch, application_id="app-1", redis_url="redis://localhost:6379/0")

    async def _fake_acquire(lock, *, config, stop_event):
        events.append("acquire")
        return True

    async def _fake_refresh(lock, *, interval_s, on_lost, stop_event):
        events.append("refresh_started")
        await stop_event.wait()

    monkeypatch.setattr(mod, "acquire_lease_with_backoff", _fake_acquire)
    monkeypatch.setattr(mod, "lease_refresh_loop", _fake_refresh)

    rc = await mod._main()

    assert rc == 0
    assert len(_FakeWorker.instances) == 1
    worker = _FakeWorker.instances[0]
    assert worker.run_forever_started is True
    # Lease acquired before the worker's run loop started.
    assert events.index("acquire") < events.index("refresh_started")
    # Crash-RESUME wiring threaded into the worker.
    assert worker.kwargs["initial_state"] is _SENTINEL_STATE
    assert worker.kwargs["on_dispatched"] is _SENTINEL_HOOK
    assert "bot_token" not in worker.kwargs
    assert worker.kwargs["bot_token_provider"]() == "bot-token"


async def test_no_application_id_keeps_lease_disables_resume(monkeypatch):
    """No DISCORD_CLIENT_ID → the lease still guards double-delivery, but
    RESUME persistence is disabled (no state load, no save hook)."""
    mod = _load_launcher()
    events: list[str] = []
    _install_common_stubs(mod, monkeypatch, events=events)
    _base_env(monkeypatch, application_id=None, redis_url="redis://localhost:6379/0")

    async def _fake_acquire(lock, *, config, stop_event):
        events.append("acquire")
        return True

    async def _fake_refresh(lock, *, interval_s, on_lost, stop_event):
        await stop_event.wait()

    monkeypatch.setattr(mod, "acquire_lease_with_backoff", _fake_acquire)
    monkeypatch.setattr(mod, "lease_refresh_loop", _fake_refresh)

    rc = await mod._main()

    assert rc == 0
    assert "acquire" in events, "lease still acquired without an application id"
    assert "load_state" not in events, "no RESUME state load without application id"
    worker = _FakeWorker.instances[0]
    assert worker.kwargs["initial_state"] is None
    assert worker.kwargs["on_dispatched"] is None


async def test_lease_lost_mid_run_exits_3(monkeypatch):
    """Refresh loop reports the lease lost → worker shut down → exit 3."""
    mod = _load_launcher()
    events: list[str] = []
    _install_common_stubs(mod, monkeypatch, events=events)
    _base_env(monkeypatch, application_id="app-1", redis_url="redis://localhost:6379/0")

    async def _fake_acquire(lock, *, config, stop_event):
        return True

    async def _fake_refresh(lock, *, interval_s, on_lost, stop_event):
        # Simulate the lease being lost: invoke the launcher's on_lost,
        # which must request a worker shutdown.
        await on_lost()

    # Worker waits for an external shutdown rather than self-completing.
    async def _run_forever(self):
        self.run_forever_started = True
        await self.shutdown.wait()
        return 0

    monkeypatch.setattr(mod, "acquire_lease_with_backoff", _fake_acquire)
    monkeypatch.setattr(mod, "lease_refresh_loop", _fake_refresh)
    monkeypatch.setattr(_FakeWorker, "run_forever", _run_forever)

    rc = await mod._main()

    assert rc == 3
    worker = _FakeWorker.instances[0]
    assert worker.request_shutdown_called is True


async def test_lease_acquire_timeout_exits_3_without_worker(monkeypatch):
    """Lease never acquired (timeout, not SIGTERM) → exit 3, no worker."""
    mod = _load_launcher()
    events: list[str] = []
    _install_common_stubs(mod, monkeypatch, events=events)
    _base_env(monkeypatch, application_id="app-1", redis_url="redis://localhost:6379/0")

    async def _fake_acquire(lock, *, config, stop_event):
        return False  # timeout; stop_event NOT set

    monkeypatch.setattr(mod, "acquire_lease_with_backoff", _fake_acquire)

    rc = await mod._main()

    assert rc == 3
    assert _FakeWorker.instances == [], "worker must not start if the lease times out"
