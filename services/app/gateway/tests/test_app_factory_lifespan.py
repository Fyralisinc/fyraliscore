"""Gateway app factory lifespan and readiness behavior."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from services.app.gateway.rate_limit import RateLimiter
from services.app.gateway.settings import GatewaySettings


class FakePool:
    def __init__(self) -> None:
        self.closed = False
        self.fetchval_calls = 0

    async def fetchval(self, query: str, *args: Any) -> int:
        self.fetchval_calls += 1
        expected = {
            "SELECT 1",
            "SELECT 1 FROM provider_installations LIMIT 1",
            "SELECT 1 FROM tenant_flags LIMIT 1",
        }
        if query not in expected:
            raise AssertionError(f"unexpected query: {query}")
        return 1

    async def close(self) -> None:
        self.closed = True


class MissingTenantFlagsPool(FakePool):
    async def fetchval(self, query: str, *args: Any) -> int:
        if query == "SELECT 1 FROM tenant_flags LIMIT 1":
            raise RuntimeError("tenant_flags table missing")
        return await super().fetchval(query, *args)


class FakeDispatcher:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class FakeEmbedder:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _settings(
    *,
    require_realtime: bool = False,
    require_github_integration: bool = False,
    require_ingestion_data_plane: bool = False,
    **overrides: Any,
) -> GatewaySettings:
    values = {
        "log_level": "WARNING",
        "ollama_url": None,
        "auth_bootstrap_secret": None,
        "ceo_view_enabled": False,
        "require_realtime": require_realtime,
        "require_github_integration": require_github_integration,
        "require_ingestion_data_plane": require_ingestion_data_plane,
        "oauth_sweep_interval_s": 60.0,
    }
    values.update(overrides)
    return GatewaySettings(
        **values,
    )


def _patch_lightweight_startup(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    import services.app.gateway.main as main_module
    import services.app.realtime.main as realtime_module

    calls: dict[str, Any] = {
        "closed_pools": [],
        "closed_data_plane": 0,
        "oauth_tasks": [],
        "stopped_oauth_tasks": [],
        "dispatcher": None,
        "dispatchers": [],
        "github_clients": [],
        "closed_github_clients": [],
    }

    async def run_extension_startup_hooks(
        app: Any,
        pool: Any,
        *,
        production: bool = False,
    ) -> None:
        del production
        return None

    def wire_integration_runtime_state(app: Any, pool: Any) -> Any:
        app.state.pool = pool
        secret_store = object()
        tenant_resolver = object()
        tenant_flags = object()
        app.state.secret_store = secret_store
        app.state.tenant_resolver = tenant_resolver
        app.state.tenant_flags = tenant_flags
        runtime = SimpleNamespace(
            pool=pool,
            secret_store=secret_store,
            tenant_resolver=tenant_resolver,
            tenant_flags=tenant_flags,
        )
        app.state.integration_runtime = runtime
        return SimpleNamespace(
            runtime=runtime,
            pool_alias_created=True,
            secret_store_created=True,
            tenant_resolver_created=True,
            tenant_flags_created=True,
        )

    def wire_github_gateway_state(
        app: Any,
        *,
        pool: Any,
        tenant_resolver: Any,
    ) -> Any:
        client = SimpleNamespace(closed=False)
        app.state.github_client = client
        app.state.github_replay_cache = object()
        calls["github_clients"].append(client)
        return SimpleNamespace(wired=True, owns_client=True)

    async def close_github_gateway_state(
        app: Any,
        *,
        client: Any = None,
    ) -> None:
        target = client or getattr(app.state, "github_client", None)
        if target is not None:
            target.closed = True
            calls["closed_github_clients"].append(target)
        if getattr(app.state, "github_client", None) is target:
            app.state.github_client = None
        app.state.github_replay_cache = None

    async def wire_ingestion_data_plane(
        app: Any,
        *,
        settings: GatewaySettings,
    ) -> bool:
        return False

    async def close_ingestion_data_plane(app: Any) -> None:
        calls["closed_data_plane"] += 1

    def start_oauth_state_sweeper(
        pool: Any,
        *,
        interval_s: float = 300.0,
    ) -> object:
        task = object()
        calls["oauth_tasks"].append((task, interval_s))
        return task

    async def stop_oauth_state_sweeper(task: object | None) -> None:
        calls["stopped_oauth_tasks"].append(task)

    async def close_gateway_pool(pool: FakePool | None = None) -> None:
        calls["closed_pools"].append(pool)
        if pool is not None:
            await pool.close()

    def configure_realtime(app: Any, *, pool: Any, start: bool = False) -> Any:
        dispatcher = FakeDispatcher()
        realtime = SimpleNamespace(dispatcher=dispatcher)
        app.state.realtime = realtime
        calls["dispatcher"] = dispatcher
        calls["dispatchers"].append(dispatcher)
        return realtime

    monkeypatch.setattr(
        main_module, "run_extension_startup_hooks", run_extension_startup_hooks
    )
    monkeypatch.setattr(
        main_module,
        "wire_integration_runtime_state",
        wire_integration_runtime_state,
    )
    monkeypatch.setattr(
        main_module,
        "wire_github_gateway_state",
        wire_github_gateway_state,
    )
    monkeypatch.setattr(
        main_module,
        "close_github_gateway_state",
        close_github_gateway_state,
    )
    monkeypatch.setattr(
        main_module,
        "wire_ingestion_data_plane",
        wire_ingestion_data_plane,
    )
    monkeypatch.setattr(
        main_module,
        "close_ingestion_data_plane",
        close_ingestion_data_plane,
    )
    monkeypatch.setattr(
        main_module,
        "start_oauth_state_sweeper",
        start_oauth_state_sweeper,
    )
    monkeypatch.setattr(
        main_module,
        "stop_oauth_state_sweeper",
        stop_oauth_state_sweeper,
    )
    monkeypatch.setattr(main_module, "close_gateway_pool", close_gateway_pool)
    monkeypatch.setattr(
        realtime_module,
        "configure_realtime",
        configure_realtime,
    )
    return calls


@pytest.mark.asyncio
async def test_injected_pool_is_not_closed_by_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    calls = _patch_lightweight_startup(monkeypatch)
    pool = FakePool()
    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(),
        configure_logging=False,
    )

    async with app.router.lifespan_context(app):
        assert app.state.deps.pool is pool
        assert app.state.startup_status.ready is True

    assert pool.closed is False
    assert calls["closed_pools"] == []


@pytest.mark.asyncio
async def test_gateway_created_pool_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    calls = _patch_lightweight_startup(monkeypatch)
    pool = FakePool()

    async def create_gateway_pool() -> FakePool:
        return pool

    monkeypatch.setattr(main_module, "create_gateway_pool", create_gateway_pool)
    monkeypatch.setattr(main_module, "ActorRepo", lambda pool: object())
    monkeypatch.setattr(main_module, "EntityAliasRepo", lambda pool: object())

    app = main_module.build_app(
        settings=_settings(),
        configure_logging=False,
    )

    async with app.router.lifespan_context(app):
        assert app.state.deps.pool is pool

    assert pool.closed is True
    assert calls["closed_pools"] == [pool]


@pytest.mark.asyncio
async def test_production_extension_startup_failure_aborts_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()

    async def failing_extension_hooks(
        app: Any,
        pool: Any,
        *,
        production: bool = False,
    ) -> None:
        del app, pool
        assert production is True
        raise RuntimeError("customer extension not configured")

    monkeypatch.setattr(
        main_module,
        "run_extension_startup_hooks",
        failing_extension_hooks,
    )

    startup_status = main_module.StartupStatus()
    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(environment="production"),
        configure_logging=False,
    )
    app.state.startup_status = startup_status

    with pytest.raises(RuntimeError, match="customer extension"):
        async with app.router.lifespan_context(app):
            pass

    component = startup_status.components["extensions"]
    assert startup_status.failed is True
    assert component.status == "failed"
    assert component.required is True


@pytest.mark.asyncio
async def test_gateway_created_pool_reenters_lifespan_with_fresh_deps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    calls = _patch_lightweight_startup(monkeypatch)
    pools = [FakePool(), FakePool()]

    async def create_gateway_pool() -> FakePool:
        return pools[len(calls["closed_pools"])]

    monkeypatch.setattr(main_module, "create_gateway_pool", create_gateway_pool)
    monkeypatch.setattr(main_module, "ActorRepo", lambda pool: object())
    monkeypatch.setattr(main_module, "EntityAliasRepo", lambda pool: object())

    app = main_module.build_app(
        settings=_settings(),
        configure_logging=False,
    )

    async with app.router.lifespan_context(app):
        assert app.state.deps.pool is pools[0]

    assert pools[0].closed is True
    assert getattr(app.state, "deps", None) is None
    assert getattr(app.state, "pool", None) is None
    assert getattr(app.state, "realtime", None) is None
    assert getattr(app.state, "github_client", None) is None
    assert getattr(app.state, "github_replay_cache", None) is None

    async with app.router.lifespan_context(app):
        assert app.state.deps.pool is pools[1]

    assert pools[1].closed is True
    assert calls["closed_pools"] == pools
    assert len(calls["dispatchers"]) == 2
    assert len(calls["closed_github_clients"]) == 2


@pytest.mark.asyncio
async def test_injected_embedder_is_not_closed_by_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()
    embedder = FakeEmbedder()
    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        embedder=embedder,  # type: ignore[arg-type]
        rate_limiter=RateLimiter(),
        settings=_settings(),
        configure_logging=False,
    )

    async with app.router.lifespan_context(app):
        assert app.state.deps.embedder is embedder

    assert embedder.closed is False
    assert app.state.deps.embedder is embedder


@pytest.mark.asyncio
async def test_gateway_created_embedder_is_closed_and_deps_are_rebuilt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()
    created_embedders = [FakeEmbedder(), FakeEmbedder()]

    def create_embedder(config: object) -> FakeEmbedder:
        return created_embedders.pop(0)

    monkeypatch.setattr(main_module, "OllamaClient", create_embedder)
    monkeypatch.setattr(
        main_module.OllamaConfig,
        "from_env",
        staticmethod(lambda: object()),
    )

    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(ollama_url="http://ollama.test"),
        configure_logging=False,
    )
    first_embedder = app.state.deps.embedder

    async with app.router.lifespan_context(app):
        assert app.state.deps.embedder is first_embedder

    assert first_embedder.closed is True
    assert getattr(app.state, "deps", None) is None

    async with app.router.lifespan_context(app):
        second_embedder = app.state.deps.embedder
        assert second_embedder is not first_embedder

    assert second_embedder.closed is True


@pytest.mark.asyncio
async def test_required_ingestion_failure_cleans_started_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    calls = _patch_lightweight_startup(monkeypatch)
    pool = FakePool()

    async def create_gateway_pool() -> FakePool:
        return pool

    async def wire_ingestion_data_plane(
        app: Any,
        *,
        settings: GatewaySettings,
    ) -> bool:
        raise RuntimeError("kafka unavailable")

    monkeypatch.setattr(main_module, "create_gateway_pool", create_gateway_pool)
    monkeypatch.setattr(main_module, "ActorRepo", lambda pool: object())
    monkeypatch.setattr(main_module, "EntityAliasRepo", lambda pool: object())
    monkeypatch.setattr(
        main_module,
        "wire_ingestion_data_plane",
        wire_ingestion_data_plane,
    )

    app = main_module.build_app(
        settings=_settings(require_ingestion_data_plane=True),
        configure_logging=False,
    )

    with pytest.raises(RuntimeError, match="kafka unavailable"):
        async with app.router.lifespan_context(app):
            pass

    dispatcher = calls["dispatcher"]
    assert dispatcher is not None
    assert dispatcher.started is True
    assert dispatcher.stopped is True
    assert calls["closed_data_plane"] == 0
    assert calls["stopped_oauth_tasks"] == [
        calls["oauth_tasks"][0][0],
    ]
    assert pool.closed is True
    assert app.state.startup_status.failed is True
    assert app.state.startup_status.ready is False
    assert (
        app.state.startup_status.components["ingestion_data_plane"].status
        == "failed"
    )


@pytest.mark.asyncio
async def test_db_pool_startup_timeout_marks_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    _patch_lightweight_startup(monkeypatch)

    async def create_gateway_pool() -> FakePool:
        await asyncio.sleep(0.05)
        return FakePool()

    monkeypatch.setattr(main_module, "create_gateway_pool", create_gateway_pool)

    app = main_module.build_app(
        settings=_settings(db_startup_timeout_s=0.001),
        configure_logging=False,
    )

    with pytest.raises(TimeoutError, match="db_pool startup exceeded"):
        async with app.router.lifespan_context(app):
            pass

    assert app.state.startup_status.failed is True
    assert app.state.startup_status.phase == "failed"


def test_integration_runtime_rejects_pool_alias_drift() -> None:
    from services.app.gateway.state_wiring import (
        IntegrationRuntimeWiringError,
        wire_integration_runtime_state,
    )

    app = FastAPI()
    app.state.pool = FakePool()

    with pytest.raises(IntegrationRuntimeWiringError) as exc_info:
        wire_integration_runtime_state(app, FakePool())  # type: ignore[arg-type]

    assert exc_info.value.component == "integration_state.pool"


def test_integration_runtime_safety_runs_for_existing_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.app.gateway.state_wiring import (
        IntegrationRuntimeState,
        IntegrationRuntimeWiringError,
        wire_integration_runtime_state,
    )

    pool = FakePool()
    app = FastAPI()
    app.state.integration_runtime = IntegrationRuntimeState(
        pool=pool,  # type: ignore[arg-type]
        secret_store=object(),
        tenant_resolver=object(),
        tenant_flags=object(),
    )
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")

    with pytest.raises(IntegrationRuntimeWiringError) as exc_info:
        wire_integration_runtime_state(app, pool)  # type: ignore[arg-type]

    assert exc_info.value.component == "integration_state.safety"


@pytest.mark.asyncio
async def test_integration_runtime_schema_failure_is_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module
    from services.app.gateway.state_wiring import IntegrationRuntimeValidationError

    _patch_lightweight_startup(monkeypatch)
    pool = MissingTenantFlagsPool()

    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(),
        configure_logging=False,
    )

    with pytest.raises(IntegrationRuntimeValidationError):
        async with app.router.lifespan_context(app):
            pass

    assert app.state.startup_status.failed is True
    component = app.state.startup_status.components[
        "integration_state.schema.tenant_flags"
    ]
    assert component.status == "failed"
    assert component.required is True


@pytest.mark.asyncio
async def test_optional_realtime_startup_failure_degrades_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module
    import services.app.realtime.main as realtime_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()
    dispatcher = FakeDispatcher()

    async def failing_start() -> None:
        dispatcher.started = True
        raise RuntimeError("listen unavailable")

    dispatcher.start = failing_start  # type: ignore[method-assign]

    def configure_realtime(app: Any, *, pool: Any, start: bool = False) -> Any:
        realtime = SimpleNamespace(dispatcher=dispatcher)
        app.state.realtime = realtime
        return realtime

    monkeypatch.setattr(
        realtime_module,
        "configure_realtime",
        configure_realtime,
    )

    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(require_realtime=False),
        configure_logging=False,
    )

    async with app.router.lifespan_context(app):
        assert app.state.startup_status.ready is True
        assert app.state.startup_status.failed is False
        realtime_status = app.state.startup_status.components["realtime"]
        assert realtime_status.status == "degraded"
        assert realtime_status.required is False
        assert getattr(app.state, "realtime", None) is None

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get("/readyz")

    assert dispatcher.started is True
    assert dispatcher.stopped is True
    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is True
    assert payload["components"]["realtime"]["status"] == "degraded"
    assert payload["components"]["realtime"]["required"] is False


@pytest.mark.asyncio
async def test_required_realtime_startup_failure_fails_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module
    import services.app.realtime.main as realtime_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()
    dispatcher = FakeDispatcher()

    async def failing_start() -> None:
        dispatcher.started = True
        raise RuntimeError("listen unavailable")

    dispatcher.start = failing_start  # type: ignore[method-assign]

    def configure_realtime(app: Any, *, pool: Any, start: bool = False) -> Any:
        realtime = SimpleNamespace(dispatcher=dispatcher)
        app.state.realtime = realtime
        return realtime

    monkeypatch.setattr(
        realtime_module,
        "configure_realtime",
        configure_realtime,
    )

    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(require_realtime=True),
        configure_logging=False,
    )

    with pytest.raises(RuntimeError, match="listen unavailable"):
        async with app.router.lifespan_context(app):
            pass

    assert dispatcher.started is True
    assert dispatcher.stopped is True
    assert getattr(app.state, "realtime", None) is None
    assert app.state.startup_status.failed is True
    assert app.state.startup_status.components["realtime"].status == "failed"
    assert app.state.startup_status.components["realtime"].required is True


@pytest.mark.asyncio
async def test_optional_github_wiring_failure_degrades_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()

    def wire_github_gateway_state(
        app: Any,
        *,
        pool: Any,
        tenant_resolver: Any,
    ) -> Any:
        raise RuntimeError("github unavailable")

    monkeypatch.setattr(
        main_module,
        "wire_github_gateway_state",
        wire_github_gateway_state,
    )

    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(require_github_integration=False),
        configure_logging=False,
    )

    async with app.router.lifespan_context(app):
        assert app.state.startup_status.ready is True
        assert app.state.startup_status.failed is False
        github_status = app.state.startup_status.components[
            "github_gateway_state"
        ]
        assert github_status.status == "degraded"
        assert github_status.required is False

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get("/readyz")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is True
    assert payload["components"]["github_gateway_state"]["status"] == "degraded"
    assert payload["components"]["github_gateway_state"]["required"] is False


@pytest.mark.asyncio
async def test_required_github_wiring_failure_fails_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()

    async def create_gateway_pool() -> FakePool:
        return pool

    def wire_github_gateway_state(
        app: Any,
        *,
        pool: Any,
        tenant_resolver: Any,
    ) -> Any:
        raise RuntimeError("github unavailable")

    monkeypatch.setattr(main_module, "create_gateway_pool", create_gateway_pool)
    monkeypatch.setattr(main_module, "ActorRepo", lambda pool: object())
    monkeypatch.setattr(main_module, "EntityAliasRepo", lambda pool: object())
    monkeypatch.setattr(
        main_module,
        "wire_github_gateway_state",
        wire_github_gateway_state,
    )

    app = main_module.build_app(
        settings=_settings(require_github_integration=True),
        configure_logging=False,
    )

    with pytest.raises(RuntimeError, match="github unavailable"):
        async with app.router.lifespan_context(app):
            pass

    assert app.state.startup_status.failed is True
    assert (
        app.state.startup_status.components["github_gateway_state"].status
        == "failed"
    )
    assert (
        app.state.startup_status.components["github_gateway_state"].required
        is True
    )


@pytest.mark.asyncio
async def test_readyz_reports_started_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.main as main_module

    _patch_lightweight_startup(monkeypatch)
    pool = FakePool()
    app = main_module.build_app(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        rate_limiter=RateLimiter(),
        settings=_settings(),
        configure_logging=False,
    )

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get("/readyz")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is True
    assert payload["components"]["db"]["status"] == "ok"
    assert payload["components"]["secret_store"]["status"] == "ok"
    assert payload["components"]["tenant_resolver"]["status"] == "ok"
    assert payload["components"]["tenant_flags"]["status"] == "ok"
    assert (
        payload["components"]["integration_state.schema.provider_installations"][
            "status"
        ]
        == "ok"
    )
    assert (
        payload["components"]["integration_state.schema.tenant_flags"]["status"]
        == "ok"
    )
    assert payload["components"]["realtime"]["status"] == "ok"
    assert payload["components"]["realtime"]["required"] is False
    assert payload["components"]["github_gateway_state"]["status"] == "ok"
    assert payload["components"]["github_gateway_state"]["required"] is False
    assert payload["components"]["ingestion_data_plane"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_data_plane_stops_started_producer_when_s3_connect_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.ingest.ingestion.kafka.producer as producer_module
    import services.ingest.ingestion.raw_tier.s3 as s3_module
    from services.app.gateway.state_wiring import wire_ingestion_data_plane

    producer_instances: list[Any] = []
    s3_instances: list[Any] = []

    class FakeProducer:
        def __init__(self, config: object) -> None:
            self.started = False
            self.stopped = False
            producer_instances.append(self)

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

    class FailingS3Client:
        def __init__(
            self,
            bucket: str,
            *,
            endpoint_url: str | None = None,
        ) -> None:
            self.closed = False
            s3_instances.append(self)

        async def connect(self) -> None:
            raise RuntimeError("s3 failed")

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(producer_module, "IdempotentProducer", FakeProducer)
    monkeypatch.setattr(s3_module, "S3Client", FailingS3Client)

    app = FastAPI()
    with pytest.raises(RuntimeError, match="s3 failed"):
        await wire_ingestion_data_plane(
            app,
            settings=_settings(
                require_ingestion_data_plane=True,
                kafka_bootstrap_servers="localhost:9092",
                s3_raw_bucket="raw",
                s3_endpoint_url="http://s3.test",
                ingestion_data_plane_startup_timeout_s=1.0,
            ),
        )

    assert producer_instances[0].started is True
    assert producer_instances[0].stopped is True
    assert s3_instances[0].closed is True
    assert getattr(app.state, "kafka_producer", None) is None
    assert getattr(app.state, "s3_raw_client", None) is None
