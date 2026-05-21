"""Y1 GmailPubSubGenerator tests.

Drives the Gmail Pub/Sub live-ingestion path end-to-end in-process via
FastAPI ASGI. Verifies:
  - Single push → HTTP 200 + observations written.
  - Mock Gmail state ↔ notification historyId coordination.
  - Burst pattern execution (timing + observation count).
  - Multi-tenant parallel isolation.
  - Replay idempotency (at-least-once-delivery simulation).
  - Signature validation hook works (no production OIDC bypass needed).
  - Fault profile (RATE_LIMITED) absorbed by handler.
  - Composability with X3 backfill harness.
"""
from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest
from fastapi import FastAPI

from services.synthetic.fault_profiles import (
    HAPPY_PATH,
    RATE_LIMITED,
)
from services.synthetic.fixtures import make_gmail_mailbox
from services.synthetic.live_generators import GmailPubSubGenerator
from services.synthetic.mock_clients import MockGmailClient
from services.synthetic.scenarios import (
    LivePubSubScenario,
    PerTenantBurst,
)


pytestmark = pytest.mark.integration


# =====================================================================
# Test-app + env fixtures.
# =====================================================================
@pytest.fixture(autouse=True)
def _pubsub_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Required env for the gmail_pubsub router to import + run.
    The generator monkeypatches `verify_pubsub_oidc_token` to a no-op
    so these values aren't validated, but the import-time reads still
    need to succeed."""
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE",
        "https://y1-test.example.com/webhooks/gmail/pubsub",
    )
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_OIDC_SA",
        "pubsub-pusher@y1-test.iam.gserviceaccount.com",
    )


def _build_app(pool: asyncpg.Pool) -> FastAPI:
    """Build a minimal FastAPI app with the Gmail Pub/Sub router
    mounted. Provides the `app.state.deps.pool` the handler reads."""
    from services.webhooks.gmail_pubsub import router as gmail_router

    app = FastAPI()
    app.include_router(gmail_router)

    class _Deps:
        pass

    deps = _Deps()
    deps.pool = pool
    app.state.deps = deps
    return app


# =====================================================================
# Tests.
# =====================================================================
@pytest.mark.asyncio
async def test_pubsub_generator_basic_push_succeeds(
    fresh_db: asyncpg.Pool,
) -> None:
    """Single push for a single mailbox: HTTP 200 + handler ingests
    the new messages."""
    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="alice@y1.com", messages=0,
            starting_history_id=1000,
        ),
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db,
        mailboxes={"alice@y1.com": client},
    ) as gen:
        result = await gen.simulate_push(
            mailbox_email="alice@y1.com", new_messages=3,
        )

    assert result.http_status == 200, result.response_body
    assert result.response_body.get("status") == "ok"
    # 3 ingested via the push path.
    assert result.response_body.get("ingested") == 3


@pytest.mark.asyncio
async def test_pubsub_generator_coordinates_mock_gmail_state(
    fresh_db: asyncpg.Pool,
) -> None:
    """The notification historyId equals the mock's advanced state."""
    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="b@y1.com", messages=0, starting_history_id=2000,
        ),
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes={"b@y1.com": client},
    ) as gen:
        result = await gen.simulate_push(
            mailbox_email="b@y1.com", new_messages=4,
        )
    # Mock advanced by 4 events from baseline 2000.
    assert client._fixture["current_history_id"] == "2004"
    assert result.new_history_id == "2004"


@pytest.mark.asyncio
async def test_pubsub_generator_burst_pattern_executes_correctly(
    fresh_db: asyncpg.Pool,
) -> None:
    """A bursty pattern dispatches notifications within the time
    window and the handler ingests every burst's messages."""
    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="bursty@y1.com", messages=0,
            starting_history_id=3000,
        ),
    )
    scenario = LivePubSubScenario(
        tenants=[
            PerTenantBurst(
                tenant_slug="bursty",
                mailbox_email="bursty@y1.com",
                # 5 small bursts back-to-back; no large sleeps in tests.
                burst_pattern=[(50, 2), (50, 2), (50, 2), (50, 2),
                               (50, 2)],
            ),
        ],
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db,
        mailboxes={"bursty@y1.com": client},
    ) as gen:
        result = await gen.run_scenario(scenario)

    assert len(result.pushes) == 5
    assert all(p.http_status == 200 for p in result.pushes)
    # Total ingested across all pushes = 10.
    total = sum(
        int(p.response_body.get("ingested", 0)) for p in result.pushes
    )
    assert total == 10


@pytest.mark.asyncio
async def test_pubsub_generator_multi_tenant_parallel(
    fresh_db: asyncpg.Pool,
) -> None:
    """3 tenants, different patterns, run concurrently. Each tenant's
    observations belong only to that tenant; no cross-contamination."""
    app = _build_app(fresh_db)
    mailboxes = {
        f"multi-{i}@y1.com": MockGmailClient(
            fixture=make_gmail_mailbox(
                email=f"multi-{i}@y1.com", messages=0,
                starting_history_id=4000 + i * 1000,
            ),
        )
        for i in range(3)
    }
    scenario = LivePubSubScenario(
        tenants=[
            PerTenantBurst(
                tenant_slug=f"multi-{i}",
                mailbox_email=f"multi-{i}@y1.com",
                burst_pattern=[(0, 2)],
            )
            for i in range(3)
        ],
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes=mailboxes,
    ) as gen:
        result = await gen.run_scenario(scenario)
        bindings_snapshot = {
            e: b.tenant_id for e, b in gen._bindings.items()
        }

    assert len(result.pushes) == 3
    assert all(p.http_status == 200 for p in result.pushes)

    # Per-tenant observation count = 2 (no cross-contamination).
    for email, tenant_id in bindings_snapshot.items():
        count = int(await fresh_db.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1",
            tenant_id,
        ))
        assert count == 2, (
            f"tenant {email} expected 2 observations, got {count}"
        )


@pytest.mark.asyncio
async def test_pubsub_generator_replay_idempotency(
    fresh_db: asyncpg.Pool,
) -> None:
    """Set replay_probability=1.0 so EVERY push is followed by a
    duplicate. Assert observation count matches unique push count
    (duplicates dedupe at the writer)."""
    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="replay@y1.com", messages=0,
            starting_history_id=5000,
        ),
    )
    scenario = LivePubSubScenario(
        tenants=[
            PerTenantBurst(
                tenant_slug="replay",
                mailbox_email="replay@y1.com",
                burst_pattern=[(0, 2), (0, 2)],  # 4 unique messages
            ),
        ],
        replay_probability=1.0,
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db,
        mailboxes={"replay@y1.com": client},
        rng_seed=0,
    ) as gen:
        result = await gen.run_scenario(scenario)
        tenant_id = gen._bindings["replay@y1.com"].tenant_id

    # 2 real bursts + 2 replays = 4 pushes total.
    assert len(result.pushes) == 4
    assert result.duplicates_sent == 2
    # But only 4 unique messages → 4 observations.
    obs_count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert obs_count == 4, (
        f"replay should dedupe; expected 4 observations, got {obs_count}"
    )


@pytest.mark.asyncio
async def test_pubsub_generator_handles_signature_validation(
    fresh_db: asyncpg.Pool,
) -> None:
    """The generator installs a no-op OIDC verifier on enter and
    restores the original on exit. Verify both halves."""
    from services.webhooks import gmail_pubsub as gp_mod

    original_verify = gp_mod.verify_pubsub_oidc_token
    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="sig@y1.com", messages=0, starting_history_id=6000,
        ),
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes={"sig@y1.com": client},
    ) as gen:
        # Inside: verifier is patched (we know because the push
        # succeeds without a real OIDC token).
        assert gp_mod.verify_pubsub_oidc_token is not original_verify
        result = await gen.simulate_push(
            mailbox_email="sig@y1.com", new_messages=1,
        )
        assert result.http_status == 200

    # After context exit: original restored.
    assert gp_mod.verify_pubsub_oidc_token is original_verify


@pytest.mark.asyncio
async def test_pubsub_generator_fault_profile_rate_limit(
    fresh_db: asyncpg.Pool,
) -> None:
    """Mock Gmail with RATE_LIMITED profile: after N requests the
    mock raises GoogleRateLimited. The push handler's existing
    rate-limit branch translates this into HTTP 200 +
    `status=rate_limited` (NOT a crash)."""
    app = _build_app(fresh_db)
    # Threshold = 0 so the very first internal API call rate-limits.
    from services.synthetic.fault_profiles import FaultProfile
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="rl@y1.com", messages=0, starting_history_id=7000,
        ),
        profile=FaultProfile(rate_limit_after_n_requests=0),
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes={"rl@y1.com": client},
    ) as gen:
        result = await gen.simulate_push(
            mailbox_email="rl@y1.com", new_messages=1,
        )

    assert result.http_status == 200, result.response_body
    assert result.response_body.get("status") == "rate_limited"


@pytest.mark.asyncio
async def test_pubsub_generator_composable_with_x3_seeding(
    fresh_db: asyncpg.Pool,
) -> None:
    """Composition smoke test: Y1 generator + a pre-seeded X3-style
    observation count co-exist. Verifies the test surface scales to
    'install via X3, then drive live notifications via Y1' usage
    patterns.

    We don't run X3's full subprocess chain here (Kafka required);
    instead we manually insert a 'backfill-shaped' observation row and
    confirm the Y1 push handler writes live observations alongside it
    without interfering."""
    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="compose@y1.com", messages=0,
            starting_history_id=8000,
        ),
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes={"compose@y1.com": client},
    ) as gen:
        tenant_id = gen._bindings["compose@y1.com"].tenant_id

        # Simulate a prior backfill observation (X3-style write).
        backfill_obs_id = await fresh_db.fetchval(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                external_id, content, content_text, trust_tier
            ) VALUES ($1, $2, now(), 'message', 'gmail:',
                      'backfill-msg-001', '{}'::jsonb, '',
                      'trusted')
            RETURNING id
            """,
            __import__("uuid").uuid4(), tenant_id,
        )
        assert backfill_obs_id is not None

        # Now drive a live push.
        result = await gen.simulate_push(
            mailbox_email="compose@y1.com", new_messages=2,
        )
        assert result.http_status == 200

    # Total observations for this tenant: 1 (backfill) + 2 (live) = 3.
    count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    ))
    assert count == 3, f"expected 3 observations (1 bf + 2 live), got {count}"


# =====================================================================
# Phase 0 (A30.1 / A30.2) — install reuse + identity injection.
# =====================================================================
@pytest.mark.asyncio
async def test_pubsub_generator_default_kwarg_preserves_existing_behavior(
    fresh_db: asyncpg.Pool,
) -> None:
    """No message_id/internal_date → auto-minted id; external_id has the
    `gmail:{install}:msg-y1-...` shape. Backward-compat guard."""
    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="def@y1.com", messages=0, starting_history_id=9100,
        ),
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes={"def@y1.com": client},
    ) as gen:
        tenant_id = gen._bindings["def@y1.com"].tenant_id
        install_id = gen._bindings["def@y1.com"].gmail_installation_id
        result = await gen.simulate_push(
            mailbox_email="def@y1.com", new_messages=1,
        )

    assert result.http_status == 200, result.response_body
    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    # external_id derives from the Message-ID header (`y1-{nonce}-{i}`),
    # not the resource id.
    assert row["external_id"].startswith(f"gmail:{install_id}:y1-")


@pytest.mark.asyncio
async def test_pubsub_generator_injected_identity_propagates_to_observation(
    fresh_db: asyncpg.Pool,
) -> None:
    """Injected message_id + internal_date flow to external_id
    (`gmail:{install}:{message_id}`) and occurred_at. The twin seam."""
    from datetime import datetime, timezone

    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email="inj@y1.com", messages=0, starting_history_id=9200,
        ),
    )
    injected_mid = "msg-y1-twin-0001"
    injected_idate = "1767225600000"  # 2026-01-01T00:00:00Z in epoch ms
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes={"inj@y1.com": client},
    ) as gen:
        tenant_id = gen._bindings["inj@y1.com"].tenant_id
        install_id = gen._bindings["inj@y1.com"].gmail_installation_id
        result = await gen.simulate_push(
            mailbox_email="inj@y1.com", new_messages=1,
            message_id=injected_mid, internal_date=injected_idate,
        )

    assert result.http_status == 200, result.response_body
    row = await fresh_db.fetchrow(
        "SELECT external_id, occurred_at FROM observations "
        "WHERE tenant_id = $1",
        tenant_id,
    )
    assert row["external_id"] == f"gmail:{install_id}:{injected_mid}"
    assert row["occurred_at"] == datetime(
        2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc,
    )


@pytest.mark.asyncio
async def test_pubsub_generator_reuses_existing_install(
    fresh_db: asyncpg.Pool,
) -> None:
    """A30.1: when a gmail_mailbox_watches row already exists for the
    email (X3-backfill-style), the generator binds to its tenant +
    install instead of minting a fresh one. This is what lets a live
    push share backfill's install so the cross-path twin's external_id
    collides."""
    from uuid import uuid4 as _uuid4

    from lib.shared.ids import uuid7

    email = "reuse@y1.com"
    pre_tenant = _uuid4()
    pre_install = uuid7()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        pre_tenant, "pre-reuse",
    )
    await fresh_db.execute(
        "INSERT INTO gmail_installations "
        "(id, tenant_id, workspace_domain, service_account_email, scope) "
        "VALUES ($1, $2, $3, $4, 'gmail.metadata')",
        pre_install, pre_tenant, "y1.com", "sa@y1-test.iam.gserviceaccount.com",
    )
    await fresh_db.execute(
        "INSERT INTO gmail_mailbox_watches "
        "(id, tenant_id, gmail_installation_id, email_address, "
        " history_id, state) "
        "VALUES ($1, $2, $3, $4, $5, 'active')",
        uuid7(), pre_tenant, pre_install, email, "1000",
    )

    app = _build_app(fresh_db)
    client = MockGmailClient(
        fixture=make_gmail_mailbox(
            email=email, messages=0, starting_history_id=1000,
        ),
    )
    async with GmailPubSubGenerator(
        app=app, pool=fresh_db, mailboxes={email: client},
    ) as gen:
        binding = gen._bindings[email]
        assert binding.tenant_id == pre_tenant
        assert binding.gmail_installation_id == pre_install
        result = await gen.simulate_push(
            mailbox_email=email, new_messages=1,
        )
        assert result.http_status == 200, result.response_body

    # No NEW tenant was created; the observation lands under pre_tenant.
    n_tenants = int(await fresh_db.fetchval("SELECT count(*) FROM tenants"))
    assert n_tenants == 1, "generator must not create a second tenant"
    count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        pre_tenant,
    ))
    assert count == 1
