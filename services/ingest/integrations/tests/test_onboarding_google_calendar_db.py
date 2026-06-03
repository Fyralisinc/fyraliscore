"""DB-backed onboarding test for Google Calendar (IN-15).

Marked `integration` (real Postgres, auto-skipped when DATABASE_URL is
unset). Proves the DWD onboarding path: finalize_install writes the
installation + per-calendar rows + an onboarding trigger, and the
SourceOnboarding loader SQL aggregates the calendars exactly as the planner
consumes them.
"""
from __future__ import annotations

from uuid import uuid4

import asyncpg
import pytest

from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.google_calendar import plan_shards_google_calendar
from services.ingest.ingestion.workflows.source_onboarding import _LOAD_GCAL_INSTALL_SQL
from services.ingest.integrations.google_calendar.onboarding import finalize_install


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_gcal_rows(fresh_db):
    """Remove any `google_calendar` rows this test leaves behind.

    The conftest `db_pool` fixture re-applies ALL migrations at the start of
    every test, BEFORE `fresh_db` truncates. Migration 0059's source CHECK
    predates `google_calendar`, so a `google_calendar` row surviving into the
    next test's migration re-run makes 0059's ADD CONSTRAINT fail validation.
    `google_calendar` is the first source added after the last widening
    migration, so it is the first to hit this. Production is forward-only and
    unaffected; this teardown keeps the shared test DB re-migratable.
    """
    yield
    await fresh_db.execute(
        "DELETE FROM onboarding_triggers WHERE source = 'google_calendar'",
    )
    await fresh_db.execute("DELETE FROM google_calendar_calendars")
    await fresh_db.execute("DELETE FROM google_calendar_installations")


async def _seed_tenant(pool: asyncpg.Pool):
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'gcal-test')", tid,
    )
    return tid


async def test_finalize_install_writes_install_calendars_and_trigger(fresh_db):
    tid = await _seed_tenant(fresh_db)

    install_id = await finalize_install(
        fresh_db,
        tenant_id=tid,
        workspace_domain="acme.com",
        service_account_email="svc@acme.iam.gserviceaccount.com",
        calendar_emails=["alice@acme.com", "bob@acme.com"],
        inclusion_spec={"users": ["alice@acme.com"], "groups": ["eng@acme.com"]},
    )

    # Installation row.
    install = await fresh_db.fetchrow(
        "SELECT * FROM google_calendar_installations WHERE id = $1", install_id,
    )
    assert install["workspace_domain"] == "acme.com"
    assert install["scope"] == "calendar.readonly"
    assert install["resolved_calendar_count"] == 2

    # One calendar row per resolved email.
    cals = await fresh_db.fetch(
        "SELECT calendar_id, owner_email, state FROM google_calendar_calendars "
        "WHERE google_calendar_installation_id = $1 ORDER BY calendar_id",
        install_id,
    )
    assert [c["calendar_id"] for c in cals] == ["alice@acme.com", "bob@acme.com"]
    assert all(c["state"] == "active" for c in cals)

    # Onboarding trigger emitted so the M6 backfill chain fires.
    trig = await fresh_db.fetchrow(
        "SELECT source, trigger_kind, installation_row_id FROM onboarding_triggers "
        "WHERE tenant_id = $1", tid,
    )
    assert trig["source"] == "google_calendar"
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] == install_id


async def test_loader_sql_aggregates_calendars_for_planner(fresh_db):
    tid = await _seed_tenant(fresh_db)
    install_id = await finalize_install(
        fresh_db,
        tenant_id=tid,
        workspace_domain="acme.com",
        service_account_email="svc@acme.iam.gserviceaccount.com",
        calendar_emails=["alice@acme.com", "bob@acme.com"],
    )

    # The SourceOnboarding loader aggregates calendars onto the install row.
    row = await fresh_db.fetchrow(_LOAD_GCAL_INSTALL_SQL, tid)
    assert row["id"] == install_id

    # Feed it to the planner exactly as SourceOnboarding does.
    ctx = PlannerContext(tenant_id=tid, install=row, conn=None, source_client=None)
    shards = await plan_shards_google_calendar(ctx)
    assert {s.shard_identifier["calendar_id"] for s in shards} == {
        "alice@acme.com", "bob@acme.com",
    }


async def test_reinstall_is_idempotent(fresh_db):
    tid = await _seed_tenant(fresh_db)
    kw = dict(
        tenant_id=tid, workspace_domain="acme.com",
        service_account_email="svc@acme.iam.gserviceaccount.com",
        calendar_emails=["alice@acme.com"],
    )
    id1 = await finalize_install(fresh_db, **kw)
    id2 = await finalize_install(fresh_db, **kw)
    assert id1 == id2  # UPSERT on (tenant_id, workspace_domain)
    # Exactly one trigger row (ON CONFLICT DO NOTHING).
    count = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers WHERE tenant_id = $1", tid,
    )
    assert count == 1
