"""Z1-slack tests — SlackWebhookGenerator drives the Slack live path.

Verifies:
  - Basic message → HTTP 200/201 + observation written.
  - Mock-state coordination (dispatched message lands in mock fixture).
  - Burst patterns + multi-tenant parallel dispatch with per-tenant
    attribution.
  - Replay idempotency (same ts → dedup; no double-count).
  - Signature validation (tampered sig → 401, no observation).
  - Tenant-resolution gate (unknown team_id → 401, no observation).
  - Fault-profile inertness for the webhook path (documented).
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
from services.synthetic.fixtures import make_slack_workspace
from services.synthetic.live_generators.slack_webhook import (
    SlackWebhookGenerator,
)
from services.synthetic.mock_clients import MockSlackClient
from services.synthetic.scenarios import LiveSlackScenario, SlackTenantTraffic


pytestmark = pytest.mark.integration


_SECRET = "z1-slack-test-secret"


@pytest.fixture(autouse=True)
def _slack_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wire the dev env-var signing path the webhook router resolves
    when `provider_installations.secret_ref IS NULL` (same seam the
    IN-06/IN-08 webhook tests use)."""
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", _SECRET)
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")
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


async def _seed_slack_install(pool: asyncpg.Pool, team_id: str) -> UUID:
    """Insert a tenant + a slack provider_installations row keyed by
    team_id so the webhook router resolves the tenant."""
    tenant_id = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id, f"z1-slack-{tenant_id.hex[:8]}",
    )
    await pool.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'slack', $3, NULL, TRUE)",
        uuid7(), tenant_id, team_id,
    )
    return tenant_id


def _mock(team_id: str) -> MockSlackClient:
    return MockSlackClient(
        fixture=make_slack_workspace(
            team_id=team_id, channels=1, messages_per_channel=0,
        ),
    )


# =====================================================================
# Tests.
# =====================================================================
@pytest.mark.asyncio
async def test_slack_webhook_driver_basic_message_succeeds(
    fresh_db: asyncpg.Pool,
) -> None:
    team_id = "T_BASIC"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock(team_id), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_message(
            team_id=team_id, channel_id="C_BASIC",
            content="shipped the rate limiter fix",
        )

    assert result.http_status in (200, 201), result.response_body
    assert result.observation_id is not None
    row = await fresh_db.fetchrow(
        "SELECT tenant_id, source_channel, content_text "
        "FROM observations WHERE id = $1",
        UUID(result.observation_id),
    )
    assert row is not None
    assert row["tenant_id"] == tenant_id
    assert row["source_channel"] == "slack:message"
    assert "rate limiter" in (row["content_text"] or "")


@pytest.mark.asyncio
async def test_slack_webhook_driver_coordinates_mock_state(
    fresh_db: asyncpg.Pool,
) -> None:
    team_id = "T_COORD"
    await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    mock = _mock(team_id)
    async with SlackWebhookGenerator(
        app=app, mock_client=mock, signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_message(
            team_id=team_id, channel_id="C_COORD", content="hello mock",
        )

    # The dispatched message's ts is present in the mock's live history.
    page, _ = await mock.conversations_history(channel="C_COORD")
    assert any(m["ts"] == result.message_ts for m in page)


@pytest.mark.asyncio
async def test_slack_webhook_driver_burst_pattern_executes_correctly(
    fresh_db: asyncpg.Pool,
) -> None:
    team_id = "T_BURST"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    scenario = LiveSlackScenario(
        tenants=[
            SlackTenantTraffic(
                tenant_slug="burst", team_id=team_id,
                channel_id="C_BURST",
                # small back-to-back bursts (no long sleeps in tests).
                message_pattern=[(20, 3), (20, 3), (20, 4)],
            ),
        ],
    )
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock(team_id), signing_secret=_SECRET,
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
async def test_slack_webhook_driver_multi_tenant_parallel(
    fresh_db: asyncpg.Pool,
) -> None:
    teams = [f"T_MT_{i}" for i in range(5)]
    tenant_ids = {t: await _seed_slack_install(fresh_db, t) for t in teams}
    app = _build_app(fresh_db)
    scenario = LiveSlackScenario(
        tenants=[
            SlackTenantTraffic(
                tenant_slug=f"mt-{i}", team_id=teams[i],
                channel_id=f"C_MT_{i}",
                message_pattern=[(0, i + 1)],  # tenant i sends i+1 msgs
            )
            for i in range(5)
        ],
    )
    # One mock per workspace; the driver under test takes a single mock,
    # so drive each tenant with its own generator concurrently is
    # overkill — instead use one generator with a shared mock (the mock
    # is only state-fidelity here; resolution is by team_id in the DB).
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock("T_MT_SHARED"), signing_secret=_SECRET,
    ) as gen:
        result = await gen.run_scenario(scenario)

    assert len(result.results) == sum(range(1, 6))  # 1+2+3+4+5 = 15
    for i, team in enumerate(teams):
        n = int(await fresh_db.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1",
            tenant_ids[team],
        ))
        assert n == i + 1, f"tenant {team} expected {i+1} obs, got {n}"


@pytest.mark.asyncio
async def test_slack_webhook_driver_replay_idempotency(
    fresh_db: asyncpg.Pool,
) -> None:
    team_id = "T_REPLAY"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    scenario = LiveSlackScenario(
        tenants=[
            SlackTenantTraffic(
                tenant_slug="replay", team_id=team_id,
                channel_id="C_REPLAY",
                message_pattern=[(0, 5)],
            ),
        ],
        replay_probability=1.0,  # every message immediately re-delivered
    )
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock(team_id), signing_secret=_SECRET,
        rng_seed=7,
    ) as gen:
        result = await gen.run_scenario(scenario)

    # 5 unique + 5 replays dispatched.
    assert result.duplicates_sent == 5
    assert len([r for r in result.results if r.was_replay]) == 5
    # But only 5 distinct observations (external_id dedup on ts).
    count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert count == 5, f"expected 5 deduped observations, got {count}"


@pytest.mark.asyncio
async def test_slack_webhook_driver_invalid_signature_rejected(
    fresh_db: asyncpg.Pool,
) -> None:
    team_id = "T_BADSIG"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock(team_id), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_message(
            team_id=team_id, channel_id="C_BADSIG", content="spoofed",
            tamper_signature=True,
        )

    assert result.http_status == 401
    n = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert n == 0


@pytest.mark.asyncio
async def test_slack_webhook_driver_unknown_tenant_returns_401(
    fresh_db: asyncpg.Pool,
) -> None:
    # No provider_installations row for this team_id — valid signature,
    # but tenant resolution fails → 401, no observation.
    app = _build_app(fresh_db)
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock("T_UNKNOWN"), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_message(
            team_id="T_UNKNOWN", channel_id="C_X", content="no home",
        )

    assert result.http_status == 401
    n = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations",
    ))
    assert n == 0


@pytest.mark.asyncio
async def test_slack_webhook_driver_fault_profile_rate_limit(
    fresh_db: asyncpg.Pool,
) -> None:
    """The Slack webhook ingest path does NOT call the Slack API, so a
    RATE_LIMITED mock profile is inert for webhook dispatch — the
    webhook still succeeds. This test documents that independence."""
    team_id = "T_FAULT"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    mock = MockSlackClient(
        fixture=make_slack_workspace(
            team_id=team_id, channels=1, messages_per_channel=0,
        ),
        profile=RATE_LIMITED,
    )
    async with SlackWebhookGenerator(
        app=app, mock_client=mock, signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_message(
            team_id=team_id, channel_id="C_FAULT", content="still works",
        )

    assert result.http_status in (200, 201), result.response_body
    n = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert n == 1


@pytest.mark.asyncio
async def test_slack_webhook_driver_composable_with_x3_harness(
    fresh_db: asyncpg.Pool,
) -> None:
    """Composition smoke test: a pre-seeded X3-style backfill
    observation co-exists with Z1-driven live observations under the
    same tenant.

    We don't run X3's full subprocess chain here (Kafka required);
    instead we insert a 'backfill-shaped' observation row and confirm
    the Slack webhook path writes live observations alongside it in the
    shared `observations` table without interference."""
    team_id = "T_COMPOSE"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)

    backfill_obs_id = await fresh_db.fetchval(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            external_id, content, content_text, trust_tier
        ) VALUES ($1, $2, now(), 'message', 'slack:message',
                  'backfill-C:1700000000.000001', '{}'::jsonb,
                  'backfill', 'attested_agent')
        RETURNING id
        """,
        uuid.uuid4(), tenant_id,
    )
    assert backfill_obs_id is not None

    async with SlackWebhookGenerator(
        app=app, mock_client=_mock(team_id), signing_secret=_SECRET,
    ) as gen:
        for i in range(2):
            r = await gen.simulate_message(
                team_id=team_id, channel_id="C_COMPOSE",
                content=f"live-{i}",
            )
            assert r.http_status in (200, 201), r.response_body

    count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert count == 3, f"expected 3 (1 backfill + 2 live), got {count}"


# =====================================================================
# Phase 0 (A30.2) — identity injection for cross-path twins.
# =====================================================================
@pytest.mark.asyncio
async def test_slack_webhook_default_kwarg_preserves_existing_behavior(
    fresh_db: asyncpg.Pool,
) -> None:
    """No `ts` kwarg → auto-minted monotonic ts; external_id derives
    from it. Guards the backward-compatible default path."""
    team_id = "T_DEFAULT"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock(team_id), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_message(
            team_id=team_id, channel_id="C_DEFAULT", content="auto",
        )

    assert result.http_status in (200, 201), result.response_body
    # Auto-minted ts has the `{epoch}.{seq}` shape.
    assert "." in result.message_ts
    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    assert row["external_id"] == f"C_DEFAULT:{result.message_ts}"


@pytest.mark.asyncio
async def test_slack_webhook_injected_identity_propagates_to_observation(
    fresh_db: asyncpg.Pool,
) -> None:
    """Injected `ts` flows to external_id (`{channel}:{ts}`). This is
    the twin seam: a live event can match a backfilled event's
    external_id + occurred_at (both derive from ts)."""
    team_id = "T_INJECT"
    tenant_id = await _seed_slack_install(fresh_db, team_id)
    app = _build_app(fresh_db)
    injected_ts = "1767225600.000123"
    async with SlackWebhookGenerator(
        app=app, mock_client=_mock(team_id), signing_secret=_SECRET,
    ) as gen:
        result = await gen.simulate_message(
            team_id=team_id, channel_id="C_INJECT", content="twin",
            ts=injected_ts,
        )

    assert result.message_ts == injected_ts
    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    assert row["external_id"] == f"C_INJECT:{injected_ts}"
