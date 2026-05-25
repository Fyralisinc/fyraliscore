"""Real-LLM test fixtures: provider, response cache, repos, embedder, scenarios."""
from __future__ import annotations

import os

os.environ.setdefault("COMPANY_OS_ENV", "test")

# Real-LLM tests follow the configured provider. For older local setups where
# only DEEPSEEK_API_KEY exists and no provider is configured, keep the historic
# DeepSeek default. If .env explicitly says `LLM_PROVIDER=codex`, do not
# silently swap the test run back to DeepSeek.
configured_provider = (os.environ.get("LLM_PROVIDER") or "").lower()
if os.environ.get("DEEPSEEK_API_KEY") and configured_provider in {"", "deepseek"}:
    os.environ["LLM_PROVIDER"] = "deepseek"
    configured_model = os.environ.get("LLM_MODEL") or ""
    if os.environ.get("REAL_LLM_MODEL"):
        os.environ["LLM_MODEL"] = os.environ["REAL_LLM_MODEL"]
    elif (
        not configured_model
        or configured_model.startswith(("claude-", "gpt-"))
    ):
        os.environ["LLM_MODEL"] = "deepseek-chat"

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from uuid import UUID

import asyncpg
import httpx
import pytest
import pytest_asyncio

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.llm.provider import (
    LLMConfig,
    LLMConfigError,
    LLMProvider,
    build_provider,
    close_codex_app_server_client,
    set_response_cache,
)
from lib.shared.ids import uuid7
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.db_bootstrap import _register_codecs
from tests.real_llm.infrastructure.response_cache import LLMResponseCache
from tests.real_llm.infrastructure.scenario_loader import (
    Scenario,
    load_scenario,
    materialize,
)


# ---------------------------------------------------------------------
# Override the root `db_pool` fixture so JSONB columns hydrate as
# dicts. The root fixture intentionally stays minimal; production code
# (gateway/db_bootstrap) installs the codec via `init=`. Real-LLM tests
# exercise repos that depend on dict-typed jsonb (e.g. resources_repo,
# which Pydantic-validates `current_value: dict`), so we need the same
# treatment here.
# ---------------------------------------------------------------------
@pytest_asyncio.fixture(scope="function")
async def db_pool(request: pytest.FixtureRequest) -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip(
            "DATABASE_URL not set — skipping real-LLM test. "
            "Start docker-compose up and copy .env.example to .env."
        )
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=5, init=_register_codecs
    )
    # Apply migrations idempotently (mirrors root conftest behaviour).
    repo_root = Path(__file__).resolve().parents[2]
    migrations_dir = repo_root / "db" / "migrations"
    async with pool.acquire() as conn:
        from lib.shared.migrations import apply_migrations_dir
        await apply_migrations_dir(conn, migrations_dir)
    try:
        yield pool
    finally:
        await pool.close()


_REAL_LLM_OPT_IN_ENV = "RUN_REAL_LLM"


@pytest.fixture
def tenant_id() -> UUID:
    """Fresh uuid7 tenant per real-LLM test."""
    return uuid7()


@pytest.fixture(scope="session")
def response_cache() -> LLMResponseCache:
    """Session-scoped response cache wired into lib.llm.provider."""
    cache = LLMResponseCache(
        cache_dir=Path("tests/real_llm/cache"),
        current_epoch=LLMResponseCache.current_epoch(),
    )
    set_response_cache(cache)
    return cache


@pytest.fixture(scope="session")
def provider(response_cache: LLMResponseCache) -> LLMProvider:
    """LLM provider built from env config; cache is installed via response_cache fixture."""
    try:
        cfg = LLMConfig.from_env()
    except LLMConfigError as e:
        pytest.skip(f"LLM provider not configured: {e}")
    if not cfg.api_key:
        pytest.skip(
            "LLM auth not configured; export a provider API key or run "
            "`codex login` for LLM_PROVIDER=codex."
        )
    return build_provider(cfg)


@pytest_asyncio.fixture(autouse=True)
async def llm_runtime_state() -> AsyncGenerator[None, None]:
    """Keep live-provider runtime state scoped to one pytest event loop.

    Codex app-server transports and provider circuit-breaker locks are
    async runtime objects. Real-LLM tests run on fresh pytest-asyncio
    loops, so each test should start with a clean breaker window and
    close any app-server subprocess before the loop is torn down.
    """
    from services.think.circuit_breaker import reset_breakers

    reset_breakers()
    try:
        yield
    finally:
        await close_codex_app_server_client()
        reset_breakers()


@pytest_asyncio.fixture
async def think_worker(
    fresh_db: asyncpg.Pool, provider: LLMProvider
) -> AsyncGenerator[None, None]:
    """Background ThinkWorker that drains think_trigger_queue during the test.

    Tests that call wait_for_think_to_drain() depend on this fixture being
    active so triggers actually get processed.
    """
    from services.think.worker import ThinkWorker, WorkerConfig

    cfg = WorkerConfig.from_env()
    # Keep real-LLM tests aligned with production safety defaults. The Think
    # transaction currently spans retrieval + LLM + apply, so per-tenant
    # parallelism above 1 is an explicit stress setting, not the safe default.
    cfg.poll_interval_s = 0.05
    cfg.max_concurrency_per_tenant = int(
        os.environ.get(
            "REAL_LLM_THINK_CONCURRENCY",
            cfg.max_concurrency_per_tenant,
        )
    )
    cfg.trigger_max_attempts = int(
        os.environ.get(
            "REAL_LLM_TRIGGER_MAX_ATTEMPTS",
            cfg.trigger_max_attempts,
        )
    )
    worker = ThinkWorker(pool=fresh_db, config=cfg, llm_provider=provider)

    async def _noop_promote() -> None:
        return None
    worker._promote_reeval_rows = _noop_promote  # type: ignore[assignment]

    task = asyncio.create_task(worker.run())
    try:
        yield
    finally:
        await worker.stop()
        try:
            await asyncio.wait_for(task, timeout=10)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


@pytest.fixture
def actor_repo(fresh_db: asyncpg.Pool) -> ActorRepo:
    """ActorRepo bound to the fresh per-test pool."""
    return ActorRepo(fresh_db)


@pytest.fixture
def alias_repo(fresh_db: asyncpg.Pool) -> EntityAliasRepo:
    """EntityAliasRepo bound to the fresh per-test pool."""
    return EntityAliasRepo(fresh_db)


@pytest_asyncio.fixture(scope="function")
async def embedder() -> AsyncGenerator[OllamaClient, None]:
    """Live Ollama embedder; skips the test if Ollama is unreachable.

    Function-scoped because OllamaClient owns an httpx.AsyncClient that
    is bound to the event loop that created it. With pytest-asyncio
    1.x's per-test loops, a session-scoped client breaks on the second
    test ("Event loop is closed").
    """
    cfg = OllamaConfig.from_env()
    try:
        async with httpx.AsyncClient(timeout=2.0) as probe:
            resp = await probe.get(f"{cfg.base_url.rstrip('/')}/api/tags")
            resp.raise_for_status()
    except Exception as e:
        pytest.skip(f"Ollama not reachable at {cfg.base_url}: {e}")
    client = OllamaClient(cfg)
    try:
        yield client
    finally:
        await client.close()


@pytest_asyncio.fixture
async def scenario_01(fresh_db: asyncpg.Pool) -> AsyncGenerator[Scenario, None]:
    """Scenario 01 (early startup) — loaded and materialized with a fresh tenant."""
    scenario = load_scenario("early_startup")
    await materialize(scenario, pool=fresh_db)
    yield scenario


@pytest_asyncio.fixture
async def scenario_02(fresh_db: asyncpg.Pool) -> AsyncGenerator[Scenario, None]:
    """Scenario 02 (growth-stage SaaS) — loaded and materialized with a fresh tenant."""
    scenario = load_scenario("growth_saas")
    await materialize(scenario, pool=fresh_db)
    yield scenario


@pytest_asyncio.fixture
async def scenario_03(fresh_db: asyncpg.Pool) -> AsyncGenerator[Scenario, None]:
    """Scenario 03 (enterprise engineering) — loaded and materialized with a fresh tenant."""
    scenario = load_scenario("enterprise_eng")
    await materialize(scenario, pool=fresh_db)
    yield scenario


@pytest_asyncio.fixture
async def scenario_04(fresh_db: asyncpg.Pool) -> AsyncGenerator[Scenario, None]:
    """Scenario 04 (scale-chaos B2B) — loaded and materialized with a fresh tenant."""
    scenario = load_scenario("scale_chaos_b2b")
    await materialize(scenario, pool=fresh_db)
    yield scenario


@pytest_asyncio.fixture
async def scenario_05(fresh_db: asyncpg.Pool) -> AsyncGenerator[Scenario, None]:
    """Scenario 05 (industrial ops) — loaded and materialized with a fresh tenant."""
    scenario = load_scenario("industrial_ops")
    await materialize(scenario, pool=fresh_db)
    yield scenario


@pytest_asyncio.fixture
async def scenario_06(fresh_db: asyncpg.Pool) -> AsyncGenerator[Scenario, None]:
    """Scenario 06 (fintech risk) — loaded and materialized with a fresh tenant."""
    scenario = load_scenario("fintech_risk")
    await materialize(scenario, pool=fresh_db)
    yield scenario


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Auto-skip real_llm tests unless RUN_REAL_LLM=1 or `-m real_llm` is set."""
    opt_in = os.environ.get(_REAL_LLM_OPT_IN_ENV) == "1"
    marker_expr = (config.getoption("-m") or "").strip()
    explicit_marker = "real_llm" in marker_expr
    if opt_in or explicit_marker:
        return
    skip_marker = pytest.mark.skip(
        reason=(
            "real-LLM tests are opt-in; set RUN_REAL_LLM=1 or run with "
            "`-m real_llm` to enable."
        )
    )
    for item in items:
        # Use the actual marker — `"real_llm" in item.keywords` also matches
        # the directory name `tests/real_llm/`, which would skip every test
        # under that tree (including infrastructure self-tests that
        # intentionally do NOT use the marker).
        if item.get_closest_marker("real_llm") is not None:
            item.add_marker(skip_marker)
