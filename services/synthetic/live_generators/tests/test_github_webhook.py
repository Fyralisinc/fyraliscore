"""Z1-github tests — GithubWebhookGenerator drives the GitHub live path.

Verifies:
  - Basic issues + pull_request events → HTTP 200/201 + observation.
  - Mock-state coordination (dispatched event lands in mock fixture).
  - Burst patterns + multi-tenant parallel dispatch with per-tenant
    attribution.
  - Replay idempotency (same delivery_id + node_id → router replay
    cache drops the redelivery; no double-count).
  - Signature validation (tampered sig → 401, no observation).
  - Tenant-resolution gate (unknown installation → 401, no observation).
  - Mixed event types (issues + pull_request) routed correctly.
  - Composition with an X3-style backfill observation.

Requires live Postgres (uses the top-level `fresh_db` fixture).
"""
from __future__ import annotations

import uuid
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.main import build_app
from services.gateway.rate_limit import RateLimiter
from services.synthetic.fault_profiles import RATE_LIMITED
from services.synthetic.fixtures import make_github_repos
from services.synthetic.live_generators.github_webhook import (
    GithubWebhookGenerator,
)
from services.synthetic.mock_clients import MockGithubClient
from services.synthetic.scenarios import (
    GithubTenantTraffic,
    LiveGithubScenario,
)


pytestmark = pytest.mark.integration


_SECRET = "z1-github-test-secret"


@pytest.fixture(autouse=True)
def _github_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """GitHub uses an App-level webhook secret read from
    WEBHOOK_SECRET_GITHUB (no per-tenant fallback flag needed).
    MASTER_KEK keeps the secret-store factory off its dev-warning
    branch under `filterwarnings = error`."""
    monkeypatch.setenv("WEBHOOK_SECRET_GITHUB", _SECRET)
    monkeypatch.setenv(
        "MASTER_KEK", "KuT6Cixjs4991zhixcpj1QAFbiQj3b9N8meZV2AJJyw=",
    )


def _build_app(pool: asyncpg.Pool):
    return build_app(
        pool=pool,
        actor_repo=ActorRepo(pool),
        alias_repo=EntityAliasRepo(pool),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )


async def _seed_github_install(
    pool: asyncpg.Pool, installation_id: str,
) -> UUID:
    """Insert a tenant + a github provider_installations row keyed by
    installation_id (selected_repositories NULL = all repos)."""
    tenant_id = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id, f"z1-github-{tenant_id.hex[:8]}",
    )
    await pool.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'github', $3, NULL, TRUE)",
        uuid7(), tenant_id, installation_id,
    )
    return tenant_id


def _mock(installation_id: str) -> MockGithubClient:
    return MockGithubClient(
        fixture=make_github_repos(
            org_or_user="octo", repos=1, events_per_repo=0,
            installation_id=installation_id,
        ),
    )


# =====================================================================
# Tests.
# =====================================================================
@pytest.mark.asyncio
async def test_github_webhook_driver_basic_issue_event_succeeds(
    fresh_db: asyncpg.Pool,
) -> None:
    iid = "999001"
    tenant_id = await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock(iid), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_issue_event(
            installation_id=iid, repo_full_name="octo/repo-a",
            action="opened", issue_title="bug: flaky rate limiter",
        )

    assert result.http_status in (200, 201), result.response_body
    assert result.observation_id is not None
    row = await fresh_db.fetchrow(
        "SELECT tenant_id, source_channel, content FROM observations "
        "WHERE id = $1",
        UUID(result.observation_id),
    )
    assert row is not None
    assert row["tenant_id"] == tenant_id
    assert row["source_channel"] == "github:webhook"


@pytest.mark.asyncio
async def test_github_webhook_driver_basic_pull_request_event_succeeds(
    fresh_db: asyncpg.Pool,
) -> None:
    iid = "999002"
    tenant_id = await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock(iid), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_pull_request_event(
            installation_id=iid, repo_full_name="octo/repo-a",
            action="opened", pr_title="add caching layer",
        )

    assert result.http_status in (200, 201), result.response_body
    assert result.observation_id is not None
    n = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert n == 1


@pytest.mark.asyncio
async def test_github_webhook_driver_coordinates_mock_state(
    fresh_db: asyncpg.Pool,
) -> None:
    iid = "999003"
    await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)
    mock = _mock(iid)
    async with GithubWebhookGenerator(
        app=app, mock_client=mock, signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_issue_event(
            installation_id=iid, repo_full_name="octo/coord",
            issue_title="coordinated",
        )

    page, _etag, _next = await mock.list_repo_events(
        owner="octo", repo="coord", event_type="issues",
    )
    assert any(e["id"] == result.node_id for e in page)


@pytest.mark.asyncio
async def test_github_webhook_driver_burst_pattern_executes_correctly(
    fresh_db: asyncpg.Pool,
) -> None:
    iid = "999004"
    tenant_id = await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)
    scenario = LiveGithubScenario(
        tenants=[
            GithubTenantTraffic(
                tenant_slug="burst", installation_id=iid,
                repo_full_name="octo/burst",
                event_pattern=[(20, 3), (20, 3), (20, 4)],
            ),
        ],
        event_type="issues",
    )
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock(iid), signing_secret=_SECRET,
    ) as gen:
        result = await gen.run_scenario(scenario)

    assert len(result.results) == 10
    assert all(r.http_status in (200, 201) for r in result.results)
    count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert count == 10


@pytest.mark.asyncio
async def test_github_webhook_driver_multi_tenant_parallel(
    fresh_db: asyncpg.Pool,
) -> None:
    iids = [f"99010{i}" for i in range(5)]
    tenant_ids = {i: await _seed_github_install(fresh_db, i) for i in iids}
    app = _build_app(fresh_db)
    scenario = LiveGithubScenario(
        tenants=[
            GithubTenantTraffic(
                tenant_slug=f"mt-{i}", installation_id=iids[i],
                repo_full_name=f"octo/mt-{i}",
                event_pattern=[(0, i + 1)],
            )
            for i in range(5)
        ],
        event_type="issues",
    )
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock("shared"), signing_secret=_SECRET,
    ) as gen:
        result = await gen.run_scenario(scenario)

    assert len(result.results) == sum(range(1, 6))  # 15
    for i, iid in enumerate(iids):
        n = int(await fresh_db.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1",
            tenant_ids[iid],
        ))
        assert n == i + 1, f"install {iid} expected {i+1}, got {n}"


@pytest.mark.asyncio
async def test_github_webhook_driver_replay_idempotency(
    fresh_db: asyncpg.Pool,
) -> None:
    iid = "999005"
    tenant_id = await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)
    scenario = LiveGithubScenario(
        tenants=[
            GithubTenantTraffic(
                tenant_slug="replay", installation_id=iid,
                repo_full_name="octo/replay",
                event_pattern=[(0, 5)],
            ),
        ],
        event_type="issues",
        replay_probability=1.0,  # every event re-delivered (same delivery)
    )
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock(iid), signing_secret=_SECRET,
        rng_seed=11,
    ) as gen:
        result = await gen.run_scenario(scenario)

    assert result.duplicates_sent == 5
    assert len([r for r in result.results if r.was_replay]) == 5
    # Re-deliveries are dropped (router replay cache) → only 5 distinct
    # observations.
    count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert count == 5, f"expected 5 deduped observations, got {count}"


@pytest.mark.asyncio
async def test_github_webhook_driver_invalid_signature_rejected(
    fresh_db: asyncpg.Pool,
) -> None:
    iid = "999006"
    tenant_id = await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock(iid), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_issue_event(
            installation_id=iid, repo_full_name="octo/repo-a",
            tamper_signature=True,
        )

    assert result.http_status == 401
    n = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert n == 0


@pytest.mark.asyncio
async def test_github_webhook_driver_unknown_installation_returns_401(
    fresh_db: asyncpg.Pool,
) -> None:
    # No provider_installations row → valid signature, but tenant
    # resolution fails → 401, no observation.
    app = _build_app(fresh_db)
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock("404404"), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_issue_event(
            installation_id="404404", repo_full_name="octo/repo-a",
        )

    assert result.http_status == 401
    n = int(await fresh_db.fetchval("SELECT count(*) FROM observations"))
    assert n == 0


@pytest.mark.asyncio
async def test_github_webhook_driver_mixed_event_types(
    fresh_db: asyncpg.Pool,
) -> None:
    iid = "999007"
    tenant_id = await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)
    async with GithubWebhookGenerator(
        app=app, mock_client=_mock(iid), signing_secret=_SECRET,
    ) as gen:
        r_issue = await gen.simulate_issue_event(
            installation_id=iid, repo_full_name="octo/mixed",
            issue_title="an issue",
        )
        r_pr = await gen.simulate_pull_request_event(
            installation_id=iid, repo_full_name="octo/mixed",
            pr_title="a pr",
        )

    assert r_issue.http_status in (200, 201), r_issue.response_body
    assert r_pr.http_status in (200, 201), r_pr.response_body

    rows = await fresh_db.fetch(
        "SELECT content->>'event_type' AS et FROM observations "
        "WHERE tenant_id = $1 ORDER BY et",
        tenant_id,
    )
    event_types = sorted(r["et"] for r in rows)
    assert event_types == ["issues", "pull_request"], event_types


@pytest.mark.asyncio
async def test_github_webhook_driver_composable_with_x3_harness(
    fresh_db: asyncpg.Pool,
) -> None:
    """Composition smoke test: a pre-seeded X3-style backfill
    observation co-exists with Z1-driven live observations under the
    same tenant. (X3's full subprocess chain needs Kafka; here we
    insert a backfill-shaped row and confirm the GitHub webhook path
    writes live observations alongside it.)"""
    iid = "999008"
    tenant_id = await _seed_github_install(fresh_db, iid)
    app = _build_app(fresh_db)

    backfill_obs_id = await fresh_db.fetchval(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            external_id, content, content_text, trust_tier
        ) VALUES ($1, $2, now(), 'signal', 'github:webhook',
                  'BACKFILL_NODE_001', '{}'::jsonb, 'backfill',
                  'authoritative')
        RETURNING id
        """,
        uuid.uuid4(), tenant_id,
    )
    assert backfill_obs_id is not None

    async with GithubWebhookGenerator(
        app=app, mock_client=_mock(iid), signing_secret=_SECRET,
    ) as gen:
        for i in range(2):
            r = await gen.simulate_issue_event(
                installation_id=iid, repo_full_name="octo/compose",
                issue_title=f"live-{i}",
            )
            assert r.http_status in (200, 201), r.response_body

    count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert count == 3, f"expected 3 (1 backfill + 2 live), got {count}"
