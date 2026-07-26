"""Unit-ish tests for `BackfillHarness` components that don't require
the full 5-subprocess chain (which needs a real Kafka broker).

Verifies:
  - Per-tenant install + onboarding_triggers writes are atomic and
    idempotent (via the same partial-unique-index path as production).
  - Certification-owned fixtures seed Provider Lab.
  - Only the production-client Provider Lab mode exists.

The full E2E harness run (Phase B + C, spawning subprocesses) requires
KAFKA_BOOTSTRAP_SERVERS pointing at a real broker. That path is
exercised by `test_harness_e2e.py` which is gated by an env var
(same shape as `tests/load/test_cutover_dryrun.py` from M-Load).
"""
from __future__ import annotations

import inspect
import json
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from services.ingest.synthetic.backfill_harness import (
    BackfillHarness,
    BackfillScenario,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    source_definition,
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
        fixture_params={"email": "alice@x3.example", "messages": 3},
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
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(outcomes[0].tenant_id),
            )
            install = await conn.fetchrow(
                "SELECT id FROM gmail_installations WHERE tenant_id = $1",
                outcomes[0].tenant_id,
            )
            trig = await conn.fetchrow(
                "SELECT trigger_kind, installation_row_id, gmail_installation_id "
                "FROM onboarding_triggers "
                "WHERE tenant_id = $1 AND source = 'gmail'",
                outcomes[0].tenant_id,
            )
    assert install is not None

    # onboarding_triggers row with gmail_installation_id populated and
    # installation_row_id NULL.
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["gmail_installation_id"] == install["id"]
    assert trig["installation_row_id"] is None
    assert outcomes[0].trigger_id is not None
    assert outcomes[0].installation_row_id == install["id"]


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

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(outcomes[0].tenant_id),
            )
            install = await conn.fetchrow(
                "SELECT id FROM provider_installations "
                "WHERE tenant_id = $1 AND provider = 'slack'",
                outcomes[0].tenant_id,
            )
            trig = await conn.fetchrow(
                "SELECT trigger_kind, installation_row_id, gmail_installation_id "
                "FROM onboarding_triggers "
                "WHERE tenant_id = $1 AND source = 'slack'",
                outcomes[0].tenant_id,
            )
    assert install is not None

    assert trig is not None
    assert trig["installation_row_id"] == install["id"]
    assert trig["gmail_installation_id"] is None
    assert outcomes[0].trigger_id is not None
    assert outcomes[0].installation_row_id == install["id"]


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
    assert outcomes[0].trigger_id is not None
    assert outcomes[0].installation_row_id is not None


@pytest.mark.asyncio
async def test_all_26_history_installation_bindings_write_triggers(
    fresh_db: asyncpg.Pool,
) -> None:
    history_sources = [
        source_id
        for source_id in CANONICAL_SOURCE_IDS
        if source_definition(source_id).history is not None
    ]
    scenarios = [
        BackfillScenario(
            tenant_slug=f"binding-{index}-{source_id}",
            source=source_id,
        )
        for index, source_id in enumerate(history_sources)
    ]
    harness = BackfillHarness(
        pool=fresh_db,
        scenarios=scenarios,
        concurrency=8,
    )
    outcomes = [_stub_outcome(scenario) for scenario in scenarios]

    await harness._setup_tenants_and_fixtures(outcomes)
    harness._prepare_provider_lab_fixtures(outcomes)
    await harness._invoke_oauth_callbacks(outcomes)

    assert len(history_sources) == 26
    assert {
        outcome.scenario.source: outcome.install_error
        for outcome in outcomes
        if outcome.install_error is not None
    } == {}
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = ANY($1::uuid[])",
        [outcome.tenant_id for outcome in outcomes],
    ) == 26
    assert all(outcome.trigger_id is not None for outcome in outcomes)
    assert all(outcome.installation_row_id is not None for outcome in outcomes)


def test_harness_binds_only_the_new_sibling_trigger() -> None:
    scenario = BackfillScenario(tenant_slug="siblings", source="slack")
    outcome = _stub_outcome(scenario)
    old_trigger = uuid4()
    new_trigger = uuid4()
    old_installation = uuid4()
    new_installation = uuid4()

    BackfillHarness._bind_exact_installation_trigger(
        outcome,
        before=[
            {
                "id": old_trigger,
                "installation_row_id": old_installation,
                "gmail_installation_id": None,
            },
        ],
        after=[
            {
                "id": old_trigger,
                "installation_row_id": old_installation,
                "gmail_installation_id": None,
            },
            {
                "id": new_trigger,
                "installation_row_id": new_installation,
                "gmail_installation_id": None,
            },
        ],
    )

    assert outcome.trigger_id == new_trigger
    assert outcome.installation_row_id == new_installation


def test_harness_rejects_ambiguous_preexisting_sibling_triggers() -> None:
    scenario = BackfillScenario(tenant_slug="siblings", source="slack")
    outcome = _stub_outcome(scenario)
    rows = [
        {
            "id": uuid4(),
            "installation_row_id": uuid4(),
            "gmail_installation_id": None,
        },
        {
            "id": uuid4(),
            "installation_row_id": uuid4(),
            "gmail_installation_id": None,
        },
    ]

    with pytest.raises(RuntimeError, match="cannot attribute an exact"):
        BackfillHarness._bind_exact_installation_trigger(
            outcome,
            before=rows,
            after=rows,
        )


@pytest.mark.asyncio
async def test_completion_lookup_uses_exact_trigger_workflow_id() -> None:
    scenario = BackfillScenario(tenant_slug="exact-run", source="slack")
    outcome = _stub_outcome(scenario)
    outcome.trigger_id = uuid4()
    run_id = uuid4()

    class _Pool:
        async def fetchrow(self, query, tenant_id, workflow_id):
            assert tenant_id == outcome.tenant_id
            assert workflow_id == f"onboarding:{outcome.trigger_id}"
            assert "workflow_id = $2" in query
            assert "ORDER BY" not in query
            assert "LIMIT 1" not in query
            return {"id": run_id, "status": "complete", "completed_at": object()}

        async def fetchval(
            self,
            _query,
            workflow_kind,
            workflow_id,
            signal_kind,
            idempotency_key,
        ):
            assert workflow_kind == "bridge"
            assert workflow_id == "bridge"
            assert signal_kind == "tenant_onboarding_completed"
            assert idempotency_key == str(run_id)
            return 1

    harness = BackfillHarness(
        pool=_Pool(),  # type: ignore[arg-type]
        scenarios=[scenario],
        completion_deadline_s=0.5,
    )

    await harness._wait_for_completions([outcome])

    assert outcome.onboarding_run_id == run_id
    assert outcome.completion_observed is True
    assert outcome.completion_signal_count == 1


def test_harness_has_no_client_mode_switch_or_generated_helper() -> None:
    from services.ingest.synthetic.backfill_harness import harness as module

    assert "real_clients" not in inspect.signature(BackfillHarness).parameters
    assert not hasattr(module, "_write_helper")
    assert not hasattr(module, "_HELPER_TEMPLATE")


def test_provider_lab_fixtures_are_certification_owned_per_tenant(
) -> None:
    scenarios = [
        BackfillScenario(
            tenant_slug="t1", source="gmail",
            fixture_params={"email": "a@x.example", "messages": 3},
        ),
        BackfillScenario(
            tenant_slug="t2", source="slack",
            fixture_params={"team_id": "T1", "channels": 1,
                            "messages_per_channel": 5},
        ),
    ]
    harness = BackfillHarness(
        pool=None,  # type: ignore[arg-type]
        scenarios=scenarios,
    )
    outcomes = [_stub_outcome(s) for s in scenarios]
    harness._prepare_provider_lab_fixtures(outcomes)

    assert set(harness._provider_lab_fixtures) == {"gmail", "slack"}
    assert all(outcome.fixture is not None for outcome in outcomes)
    assert outcomes[1].fixture["team_id"] == "x3-t2-slack"


# =====================================================================
# M6.7 Layer 4 (A27.4) — observation_writer flag + 7-subprocess wiring.
# =====================================================================
@pytest.mark.asyncio
async def test_harness_writes_kafka_path_enabled_flag(
    fresh_db: asyncpg.Pool,
) -> None:
    """Setup flips `ingestion.kafka_path_enabled=TRUE` per tenant so the
    observation_writer writes (instead of shadow-logging a no-op)."""
    from services.ingest.ingestion.feature_flags.client import KAFKA_PATH_ENABLED

    scenario = BackfillScenario(
        tenant_slug="flag-test", source="gmail",
        fixture_params={"email": "a@x.example", "messages": 1},
    )
    harness = BackfillHarness(pool=fresh_db, scenarios=[scenario])
    outcomes = [_stub_outcome(scenario)]
    await harness._setup_tenants_and_fixtures(outcomes)

    flag = await fresh_db.fetchval(
        "SELECT flag_value FROM tenant_flags "
        "WHERE tenant_id = $1 AND flag_name = $2",
        outcomes[0].tenant_id, KAFKA_PATH_ENABLED,
    )
    assert flag is True


def test_harness_service_specs_include_normalizer_and_writer(
) -> None:
    """The roster is 7 subprocesses: the 5 M6 framework services + the
    normalizer + the observation_writer (A27.4). Asserting the spec
    avoids spawning real processes."""
    scenario = BackfillScenario(
        tenant_slug="specs", source="slack",
        fixture_params={"team_id": "T", "channels": 1,
                        "messages_per_channel": 1},
    )
    harness = BackfillHarness(pool=None, scenarios=[scenario])  # type: ignore[arg-type]
    specs = harness._service_specs()
    assert set(specs) == {
        "oauth_poller", "tenant_onboarding", "source_onboarding",
        "shard_fetch", "reconciler", "normalizer", "observation_writer",
    }
    assert len(specs) == 7
    assert specs["normalizer"][0] == "services.ingest.ingestion.normalizer.worker"
    assert specs["observation_writer"][0] == (
        "services.ingest.ingestion.writers.observation_writer"
    )


def test_harness_base_env_wires_s3(monkeypatch) -> None:
    """The shared subprocess env carries the S3 raw-tier wiring the
    shard_fetch producer + normalizer need (A27.4)."""
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://moto.local:5000")
    monkeypatch.setenv("S3_RAW_BUCKET", "fyralis-raw")
    monkeypatch.setenv("INGESTION_ENV", "test")
    scenario = BackfillScenario(
        tenant_slug="env", source="discord",
        fixture_params={"guild_id": "G", "channels": 1,
                        "messages_per_channel": 1},
    )
    harness = BackfillHarness(pool=None, scenarios=[scenario])  # type: ignore[arg-type]
    harness._provider_lab = SimpleNamespace(
        base_url="http://127.0.0.1:8787",
    )
    env = harness._base_env()
    assert env["S3_RAW_BUCKET"] == "fyralis-raw"
    assert env["INGESTION_ENV"] == "test"
    assert env["S3_ENDPOINT_URL"] == "http://moto.local:5000"


def test_harness_env_uses_provider_lab_and_explicit_endpoint_overrides() -> None:
    scenario = BackfillScenario(
        tenant_slug="provider-lab-env",
        source="github",
        fixture_params={"org_or_user": "acme", "repos": 1},
    )
    harness = BackfillHarness(
        pool=None,  # type: ignore[arg-type]
        scenarios=[scenario],
    )
    harness._provider_lab = SimpleNamespace(
        base_url="http://127.0.0.1:8787",
    )

    env = harness._base_env()

    assert env["PROVIDER_LAB_URL"] == "http://127.0.0.1:8787"
    assert env["GITHUB_API_BASE_URL"] == "http://127.0.0.1:8787/github"
    assert env["GMAIL_API_BASE_URL"] == (
        "http://127.0.0.1:8787/gmail/gmail/v1"
    )


def test_harness_starts_seeded_provider_lab(tmp_path) -> None:
    import httpx

    from services.ingest.synthetic.fixtures import make_slack_workspace

    scenario = BackfillScenario(
        tenant_slug="provider-lab-server",
        source="slack",
        fixture_params={"team_id": "T_LAB", "channels": 1},
    )
    harness = BackfillHarness(
        pool=None,  # type: ignore[arg-type]
        scenarios=[scenario],
        provider_lab_rate_limit_every=2,
    )
    harness._workdir = str(tmp_path)
    fixture = make_slack_workspace(
        team_id="T_LAB",
        channels=1,
        messages_per_channel=1,
    )
    harness._provider_lab_fixtures = {"slack": [fixture]}

    harness._start_provider_lab()
    try:
        response = httpx.get(
            harness._provider_lab.url("slack", "/api/conversations.list"),
            headers={"Authorization": "Bearer lab-slack::T_LAB"},
        )
        service_account = json.loads(
            (tmp_path / "provider_lab_sa.json").read_text(),
        )
        faults = harness._provider_lab.app.state.provider_lab.faults.snapshot()
    finally:
        harness._teardown_services()

    assert response.status_code == 200
    assert response.json()["channels"][0]["id"] == fixture["channels"][0]["id"]
    assert service_account["token_uri"].startswith("http://127.0.0.1:")
    assert service_account["token_uri"].endswith("/gmail/token")
    assert faults
    assert all(fault["status_code"] == 429 for fault in faults)


@pytest.mark.asyncio
async def test_harness_ensure_s3_bucket_noop_without_endpoint(
    monkeypatch,
) -> None:
    """With no S3_ENDPOINT_URL the producer targets real AWS (which
    owns its bucket), so bucket creation is a clean no-op — and must
    not raise."""
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    scenario = BackfillScenario(
        tenant_slug="noendpoint", source="slack",
        fixture_params={"team_id": "T", "channels": 1,
                        "messages_per_channel": 1},
    )
    harness = BackfillHarness(pool=None, scenarios=[scenario])  # type: ignore[arg-type]
    assert harness._s3_endpoint is None
    # Idempotent + safe: calling twice is a no-op.
    await harness._ensure_s3_bucket()
    await harness._ensure_s3_bucket()


# ---- helpers ----
def _stub_outcome(scenario: BackfillScenario):
    from uuid import uuid4
    from services.ingest.synthetic.backfill_harness import TenantOutcome
    return TenantOutcome(
        scenario=scenario, tenant_id=uuid4(),
        expected_reshare=False,
    )
