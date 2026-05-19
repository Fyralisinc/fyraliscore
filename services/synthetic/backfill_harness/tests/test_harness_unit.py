"""Unit-ish tests for `BackfillHarness` components that don't require
the full 5-subprocess chain (which needs a real Kafka broker).

Verifies:
  - Per-tenant install + onboarding_triggers writes are atomic and
    idempotent (via the same partial-unique-index path as production).
  - Fixture registry is correctly written to disk.
  - Helper module is generated correctly and importable.

The full E2E harness run (Phase B + C, spawning subprocesses) requires
KAFKA_BOOTSTRAP_SERVERS pointing at a real broker. That path is
exercised by `test_harness_e2e.py` which is gated by an env var
(same shape as `tests/load/test_cutover_dryrun.py` from M-Load).
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from uuid import UUID

import asyncpg
import pytest

from services.synthetic.backfill_harness import (
    BackfillHarness,
    BackfillScenario,
)


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_harness_writes_install_and_trigger_per_tenant_gmail(
    fresh_db: asyncpg.Pool,
) -> None:
    """For Gmail scenarios: harness writes gmail_installations row +
    onboarding_triggers row with gmail_installation_id populated."""
    scenario = BackfillScenario(
        tenant_slug="harness-gmail",
        source="gmail",
        fixture_params={"email": "alice@x3.com", "messages": 3},
    )
    harness = BackfillHarness(pool=fresh_db, scenarios=[scenario])

    # Drive only the setup phase: seed tenants + fixtures + invoke
    # OAuth-equivalent install writes. Skip subprocess spawn.
    outcomes = [
        type(harness)._make_outcome_for_test(scenario)
        if hasattr(type(harness), "_make_outcome_for_test")
        else _stub_outcome(scenario)
    ]
    import tempfile
    harness._workdir = tempfile.mkdtemp(prefix="x3-harness-unit-")
    await harness._setup_tenants_and_fixtures(outcomes)
    await harness._invoke_oauth_callbacks(outcomes)

    # Tenant row exists.
    n_tenants = int(await fresh_db.fetchval(
        "SELECT count(*) FROM tenants WHERE id = $1",
        outcomes[0].tenant_id,
    ))
    assert n_tenants == 1

    # Gmail install row exists.
    install = await fresh_db.fetchrow(
        "SELECT id FROM gmail_installations WHERE tenant_id = $1",
        outcomes[0].tenant_id,
    )
    assert install is not None

    # onboarding_triggers row with gmail_installation_id populated and
    # installation_row_id NULL.
    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id "
        "FROM onboarding_triggers WHERE tenant_id = $1 AND source = 'gmail'",
        outcomes[0].tenant_id,
    )
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["gmail_installation_id"] == install["id"]
    assert trig["installation_row_id"] is None


@pytest.mark.asyncio
async def test_harness_writes_install_and_trigger_per_tenant_slack(
    fresh_db: asyncpg.Pool,
) -> None:
    """For Slack scenarios: harness writes provider_installations row +
    onboarding_triggers row with installation_row_id populated."""
    scenario = BackfillScenario(
        tenant_slug="harness-slack",
        source="slack",
        fixture_params={"team_id": "T1", "channels": 1,
                        "messages_per_channel": 5},
    )
    harness = BackfillHarness(pool=fresh_db, scenarios=[scenario])
    outcomes = [_stub_outcome(scenario)]
    import tempfile
    harness._workdir = tempfile.mkdtemp(prefix="x3-harness-unit-")
    await harness._setup_tenants_and_fixtures(outcomes)
    await harness._invoke_oauth_callbacks(outcomes)

    install = await fresh_db.fetchrow(
        "SELECT id FROM provider_installations "
        "WHERE tenant_id = $1 AND provider = 'slack'",
        outcomes[0].tenant_id,
    )
    assert install is not None

    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id "
        "FROM onboarding_triggers WHERE tenant_id = $1 AND source = 'slack'",
        outcomes[0].tenant_id,
    )
    assert trig is not None
    assert trig["installation_row_id"] == install["id"]
    assert trig["gmail_installation_id"] is None


@pytest.mark.asyncio
async def test_harness_install_idempotent_on_retry(
    fresh_db: asyncpg.Pool,
) -> None:
    """Calling the install-write path twice for the same tenant +
    source produces exactly one trigger row (idempotent via the X1
    partial unique indexes)."""
    scenario = BackfillScenario(
        tenant_slug="idem-test", source="github",
        fixture_params={"org_or_user": "octo", "repos": 1},
    )
    harness = BackfillHarness(pool=fresh_db, scenarios=[scenario])
    outcomes = [_stub_outcome(scenario)]
    import tempfile
    harness._workdir = tempfile.mkdtemp(prefix="x3-harness-unit-")
    await harness._setup_tenants_and_fixtures(outcomes)
    await harness._invoke_oauth_callbacks(outcomes)
    await harness._invoke_oauth_callbacks(outcomes)  # retry

    n = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'github'",
        outcomes[0].tenant_id,
    ))
    assert n == 1, f"expected 1 trigger after retry, got {n}"


def test_helper_module_is_generated_and_importable(tmp_path) -> None:
    """The generated helper module loads cleanly and exposes the
    expected `_install_factories` entry point."""
    from services.synthetic.backfill_harness.harness import _write_helper

    helper_name = "x3_helper_test_unit"
    helpers_dir = _write_helper(str(tmp_path), helper_name)
    module_path = os.path.join(helpers_dir, f"{helper_name}.py")
    assert os.path.exists(module_path)

    # Importable from disk without errors at module level (factories
    # install themselves at import).
    spec = importlib.util.spec_from_file_location(helper_name, module_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[helper_name] = mod
    try:
        spec.loader.exec_module(mod)
        # The helper should have populated module globals.
        assert hasattr(mod, "_install_factories")
        assert hasattr(mod, "_lookup_by_tenant_id")
    finally:
        del sys.modules[helper_name]


@pytest.mark.asyncio
async def test_registry_written_contains_per_tenant_fixtures(
    fresh_db: asyncpg.Pool, tmp_path,
) -> None:
    scenarios = [
        BackfillScenario(
            tenant_slug="t1", source="gmail",
            fixture_params={"email": "a@x.com", "messages": 3},
        ),
        BackfillScenario(
            tenant_slug="t2", source="slack",
            fixture_params={"team_id": "T1", "channels": 1,
                            "messages_per_channel": 5},
        ),
    ]
    harness = BackfillHarness(pool=fresh_db, scenarios=scenarios)
    outcomes = [_stub_outcome(s) for s in scenarios]
    harness._workdir = str(tmp_path)
    await harness._setup_tenants_and_fixtures(outcomes)

    path = harness._write_registry(outcomes)
    assert os.path.exists(path)

    with open(path) as f:
        data = json.load(f)
    assert "entries" in data
    assert len(data["entries"]) == 2
    sources = {e["source"] for e in data["entries"]}
    assert sources == {"gmail", "slack"}
    for entry in data["entries"]:
        assert "tenant_id" in entry
        assert "fixture" in entry


# ---- helpers ----
def _stub_outcome(scenario: BackfillScenario):
    from uuid import uuid4
    from services.synthetic.backfill_harness import TenantOutcome
    return TenantOutcome(
        scenario=scenario, tenant_id=uuid4(),
        expected_reshare=False,
    )
