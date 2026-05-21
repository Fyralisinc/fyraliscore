"""Phase 1 (A30.1) — live-phase orchestration tests.

These exercise `composition.py` against a live Postgres, seeding
X3-harness-shaped installs directly (no Kafka / no backfill subprocess
chain — live ingestion is inline). They verify the four generators
compose in one process, produce observations per source, attribute them
to the seeded installs, and that twin-identity capture + drain work.
"""
from __future__ import annotations

import uuid
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.synthetic.validation_runs.composition import (
    LiveTarget,
    SigningSecrets,
    build_live_drivers,
    capture_twin_identities,
    live_target_for,
    run_live_phase,
    wait_for_live_consumer_drain,
)


pytestmark = pytest.mark.integration


_SOURCES = ("gmail", "github", "slack", "discord")


async def _seed_install(
    pool: asyncpg.Pool, source: str, slug: str,
) -> LiveTarget:
    """Seed a tenant + the X3-shaped install for one source, returning
    its LiveTarget (addressing derived exactly as the runner does)."""
    tenant_id = uuid.uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id, slug,
    )
    fixture_params: dict = {}
    if source == "gmail":
        email = f"{slug}@val.example"
        fixture_params = {"email": email}
        install_id = uuid7()
        await pool.execute(
            "INSERT INTO gmail_installations "
            "(id, tenant_id, workspace_domain, service_account_email, scope) "
            "VALUES ($1, $2, $3, $4, 'gmail.metadata')",
            install_id, tenant_id, f"{slug}.example",
            "sa@v-test.iam.gserviceaccount.com",
        )
        await pool.execute(
            "INSERT INTO gmail_mailbox_watches "
            "(id, tenant_id, gmail_installation_id, email_address, "
            " history_id, state) VALUES ($1, $2, $3, $4, $5, 'active')",
            uuid7(), tenant_id, install_id, email, "1000",
        )
    else:
        fixture_params = {"org_or_user": slug}
        await pool.execute(
            "INSERT INTO provider_installations "
            "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
            "VALUES ($1, $2, $3, $4, NULL, TRUE)",
            uuid7(), tenant_id, source, f"x3-{slug}-{source}",
        )
    return live_target_for(tenant_id, source, slug, fixture_params)


async def _seed_one_per_source(pool: asyncpg.Pool) -> list[LiveTarget]:
    return [
        await _seed_install(pool, src, f"val-{src}-0")
        for src in _SOURCES
    ]


# =====================================================================
# Tests.
# =====================================================================
@pytest.mark.asyncio
async def test_live_drivers_compose_in_one_process(
    fresh_db: asyncpg.Pool,
) -> None:
    targets = await _seed_one_per_source(fresh_db)
    drivers = await build_live_drivers(fresh_db, targets, SigningSecrets())
    try:
        assert drivers.gmail_pubsub is not None
        assert drivers.discord_gateway is not None
        assert drivers.slack_webhook is not None
        assert drivers.github_webhook is not None
        assert drivers.fastapi_app is not drivers.gmail_app
    finally:
        from services.synthetic.validation_runs.composition import (
            teardown_live_drivers,
        )
        await teardown_live_drivers(drivers)


@pytest.mark.asyncio
async def test_live_phase_orchestration_produces_observations_per_source(
    fresh_db: asyncpg.Pool,
) -> None:
    targets = await _seed_one_per_source(fresh_db)
    drivers = await build_live_drivers(fresh_db, targets, SigningSecrets())
    from services.synthetic.validation_runs.composition import (
        teardown_live_drivers,
    )
    try:
        result = await run_live_phase(
            fresh_db, drivers, targets, twins={}, events_per_tenant=3,
        )
    finally:
        await teardown_live_drivers(drivers)

    # Each source produced exactly 3 live observations (1 tenant each).
    for src in _SOURCES:
        assert result.per_source_counts[src] == 3, (
            f"{src}: {result.per_source_counts}"
        )
    # Tamper probes fired for slack + github only (no observation).
    tamper_sources = {r["source"] for r in result.tamper_results}
    assert tamper_sources == {"slack", "github"}
    assert all(r["http_status"] == 401 for r in result.tamper_results)


@pytest.mark.asyncio
async def test_live_phase_targets_seeded_provider_installations(
    fresh_db: asyncpg.Pool,
) -> None:
    targets = await _seed_one_per_source(fresh_db)
    drivers = await build_live_drivers(fresh_db, targets, SigningSecrets())
    from services.synthetic.validation_runs.composition import (
        teardown_live_drivers,
    )
    try:
        await run_live_phase(
            fresh_db, drivers, targets, twins={}, events_per_tenant=2,
        )
    finally:
        await teardown_live_drivers(drivers)

    # Every live observation is attributed to one of the seeded tenants.
    seeded = {t.tenant_id for t in targets}
    rows = await fresh_db.fetch("SELECT DISTINCT tenant_id FROM observations")
    got = {r["tenant_id"] for r in rows}
    assert got <= seeded, f"unexpected tenants: {got - seeded}"
    assert got == seeded, f"missing tenants: {seeded - got}"


@pytest.mark.asyncio
async def test_twin_pair_identity_capture_returns_real_backfill_identity(
    fresh_db: asyncpg.Pool,
) -> None:
    targets = await _seed_one_per_source(fresh_db)
    # Insert a backfill-shaped observation per twin source.
    by_source = {t.source: t for t in targets}
    fixtures = {
        "slack": ("slack:message", "C_LIVE_val-slack-0:1767225600.000001"),
        "github": ("github:webhook", "I_kwDObackfill0001"),
        "gmail": ("gmail:", "gmail:install-x:y1-backfill-0@example.com"),
    }
    for src, (channel, ext) in fixtures.items():
        await fresh_db.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                external_id, content, content_text, trust_tier
            ) VALUES ($1, $2, '2026-01-01T00:00:00+00:00', 'message',
                      $3, $4, '{}'::jsonb, 'bf', 'trusted')
            """,
            uuid.uuid4(), by_source[src].tenant_id, channel, ext,
        )

    twins = await capture_twin_identities(fresh_db, targets)
    assert set(twins.keys()) == {"slack", "github", "gmail"}
    for src, (_channel, ext) in fixtures.items():
        assert twins[src].external_id == ext
        assert twins[src].tenant_id == by_source[src].tenant_id


@pytest.mark.asyncio
async def test_live_consumer_drain_waits_for_stable_observation_count(
    fresh_db: asyncpg.Pool,
) -> None:
    targets = await _seed_one_per_source(fresh_db)
    drivers = await build_live_drivers(fresh_db, targets, SigningSecrets())
    from services.synthetic.validation_runs.composition import (
        teardown_live_drivers,
    )
    try:
        await run_live_phase(
            fresh_db, drivers, targets, twins={}, events_per_tenant=1,
        )
    finally:
        await teardown_live_drivers(drivers)

    # Live writes are inline → the count is already stable; drain returns
    # True quickly.
    stable = await wait_for_live_consumer_drain(
        fresh_db, {t.tenant_id for t in targets},
        stable_for_s=1.0, poll_interval_s=0.3, timeout_s=10.0,
    )
    assert stable is True
