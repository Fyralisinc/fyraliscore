"""`BackfillHarness` — OAuth-callback-driven multi-tenant backfill orchestrator.

Per A22. Operates in three phases:

  Phase A — Setup:
    1. Seed tenants (one row in `tenants` per scenario).
    2. Build per-tenant fixtures via X2 generators.
    3. Write the multi-tenant helper module to a temp directory; the
       helper registers fixture-aware mock-client factories at import.
    4. Invoke OAuth callbacks in-process (via httpx ASGITransport) to
       write install + onboarding_triggers rows.

  Phase B — Run:
    5. Spawn 7 shared subprocesses (oauth_poller, tenant_onboarding,
       source_onboarding, shard_fetch, reconciler, normalizer,
       observation_writer) with PYTHONPATH +
       `-c "import <helper>; from <svc> import main; main()"`.
    6. Concurrently (bounded by `concurrency`) poll for each tenant's
       `tenant_onboarding_completed` signal in the Bridge inbox.

  Phase C — Teardown:
    7. SIGTERM all 7 subprocesses; assert rc == 0 (15s grace).
    8. Collect observations, signals, state-table snapshots into
       `TenantOutcome` records.
    9. Return `HarnessResult` for assertion checks.

Concurrency model: per the X3 audit, the services are tenant-agnostic
at the claim layer — one shared subprocess set serves all tenants.

M6.7 (A27.4): the backfill chain only PRODUCES observations when the
normalizer + observation_writer run AND the tenant has
`ingestion.kafka_path_enabled=TRUE`. So the harness (a) writes that
flag per tenant at setup, (b) spawns the two extra subprocesses
(5→7), wiring S3 env into the shard_fetch producer + normalizer, and
(c) creates the moto raw bucket. S3_ENDPOINT_URL must point at a moto
server (the E2E runner provides it alongside real Kafka); without it
the harness still runs but the producer's S3 writes go to real AWS.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal as sig_module
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from lib.shared.ids import uuid7
from services.ingestion.feature_flags.client import KAFKA_PATH_ENABLED
from services.synthetic.backfill_harness.scenarios import BackfillScenario
from services.synthetic.fixtures import (
    make_discord_guild,
    make_github_repos,
    make_gmail_mailbox,
    make_slack_workspace,
)


# Raw-tier (S3) env for the backfill producer + normalizer (A27.4).
# S3_ENDPOINT_URL points at moto in the E2E runner; bucket defaults to
# the production raw bucket name.
_DEFAULT_S3_BUCKET = "fyralis-raw"


log = logging.getLogger(__name__)


# The `tenant_onboarding_completed` signal is emitted to the BRIDGE
# inbox, not the tenant_onboarding inbox — see
# services/ingestion/workflows/tenant_onboarding.py:160-161,508-509
# (BRIDGE_INBOX_KIND/ID) and this module's docstring ("Bridge inbox").
# The prior constants ("tenant_onboarding") watched the wrong inbox, so
# `_wait_for_completions` never observed completion. Latent because the
# X3 E2E path had never executed end-to-end.
BRIDGE_INBOX_KIND = "bridge"
BRIDGE_INBOX_ID = "bridge"
SIGNAL_KIND_TENANT_COMPLETED = "tenant_onboarding_completed"


# =====================================================================
# Result types.
# =====================================================================
@dataclass
class TenantOutcome:
    """Per-tenant collected state after the harness run."""

    scenario: BackfillScenario
    tenant_id: UUID
    onboarding_run_id: UUID | None = None
    completion_observed: bool = False
    completion_signal_count: int = 0
    observations: list[dict[str, Any]] = field(default_factory=list)
    cursor_history: dict[str, list[dict[str, Any]]] = field(
        default_factory=dict,
    )
    reconciliation_pass_count: int = 0
    expected_reshare: bool = False
    install_error: str | None = None


@dataclass
class HarnessResult:
    """Aggregate result returned by `BackfillHarness.run()`."""

    outcomes: list[TenantOutcome]
    subprocess_returncodes: dict[str, int] = field(default_factory=dict)
    subprocess_stderr_tails: dict[str, str] = field(default_factory=dict)
    wall_time_seconds: float = 0.0


# =====================================================================
# Fixture builder dispatch.
# =====================================================================
def _build_fixture(source: str, params: dict[str, Any]) -> dict[str, Any]:
    if source == "gmail":
        return make_gmail_mailbox(**params)
    if source == "github":
        return make_github_repos(**params)
    if source == "slack":
        return make_slack_workspace(**params)
    if source == "discord":
        return make_discord_guild(**params)
    raise ValueError(f"unknown source: {source!r}")


# =====================================================================
# Helper-module writer.
# =====================================================================
_HELPER_TEMPLATE = '''"""Auto-generated X3 harness helper.

Loaded into each M6 subprocess via PYTHONPATH + `-c "import {module}"`.
Reads the per-tenant fixture registry from FIXTURE_REGISTRY_PATH,
registers fixture-aware mock-client factories, and overrides
PLANNER / FETCHER / RECONCILER dispatch where the source's planner /
reconciler needs the source_client wired or the pool-provider injected.
"""
from __future__ import annotations

import json
import os
from typing import Any
from uuid import UUID

from services.synthetic.fault_profiles import FaultProfile
from services.synthetic.mock_clients import (
    MockDiscordClient, MockGithubClient, MockGmailClient, MockSlackClient,
)


_REGISTRY: dict[str, Any] = {{}}


def _load_registry() -> None:
    global _REGISTRY
    path = os.environ.get("X3_FIXTURE_REGISTRY_PATH")
    if not path or not os.path.exists(path):
        return
    with open(path) as f:
        _REGISTRY = json.load(f)


_load_registry()


def _profile_from(p: dict[str, Any] | None) -> FaultProfile:
    if not p:
        return FaultProfile()
    return FaultProfile(**p)


def _lookup_by_tenant_id(tenant_id: Any) -> dict[str, Any] | None:
    tid = str(tenant_id)
    for entry in _REGISTRY.get("entries", []):
        if entry["tenant_id"] == tid:
            return entry
    return None


def _make_mock(source: str, fixture: dict[str, Any],
               profile: FaultProfile) -> Any:
    if source == "gmail":
        return MockGmailClient(fixture=fixture, profile=profile)
    if source == "github":
        return MockGithubClient(fixture=fixture, profile=profile)
    if source == "slack":
        return MockSlackClient(fixture=fixture, profile=profile)
    if source == "discord":
        return MockDiscordClient(fixture=fixture, profile=profile)
    raise ValueError(f"unknown source: {{source!r}}")


# Per-source factory installer. Bound at import time.
def _install_factories() -> None:
    from services.ingestion.fetchers import gmail as gf
    from services.ingestion.fetchers import github as ghf
    from services.ingestion.fetchers import slack as sf
    from services.ingestion.fetchers import discord as df
    from services.ingestion.reconcilers import gmail as gr
    from services.ingestion.reconcilers import github as ghr
    from services.ingestion.reconcilers import slack as sr
    from services.ingestion.reconcilers import discord as dr
    from services.ingestion.workflows import source_onboarding as so

    for source, modules in (
        ("gmail", (gf, gr)),
        ("github", (ghf, ghr)),
        ("slack", (sf, sr)),
        ("discord", (df, dr)),
    ):
        async def _factory(install, _source=source):  # noqa: B023
            entry = _lookup_by_tenant_id(install["tenant_id"])
            if entry is None or entry["source"] != _source:
                raise RuntimeError(
                    f"X3 helper: no fixture for tenant={{install['tenant_id']}} "
                    f"source={{_source}}"
                )
            mock = _make_mock(
                _source, entry["fixture"],
                _profile_from(entry.get("fault_profile")),
            )
            async def _close() -> None: return None
            return mock, _close
        for mod in modules:
            setattr(mod, f"_open_{{source}}_client", _factory)

    # source_onboarding builds a source_client via _build_source_client
    # for the planners that enumerate resources at plan time. GitHub
    # (repos), Slack (channels), and Discord (guilds/channels) all need
    # a client; the Discord planner RAISES on source_client=None, so it
    # must be wired here. Gmail's planner reads DB state only → None.
    async def _build_source_client(source: str, pool: Any, install: Any):
        if source not in ("github", "slack", "discord"):
            return None
        entry = _lookup_by_tenant_id(install["tenant_id"])
        if entry is None or entry["source"] != source:
            return None
        return _make_mock(
            source, entry["fixture"],
            _profile_from(entry.get("fault_profile")),
        )
    so._build_source_client = _build_source_client


_install_factories()
'''


def _write_helper(workdir: str, helper_module: str) -> str:
    """Write the helper module under `workdir` and return its
    directory (for PYTHONPATH)."""
    helpers_dir = os.path.join(workdir, "_x3_helpers")
    os.makedirs(helpers_dir, exist_ok=True)
    init_path = os.path.join(helpers_dir, "__init__.py")
    if not os.path.exists(init_path):
        with open(init_path, "w") as f:
            f.write("")
    module_path = os.path.join(helpers_dir, f"{helper_module}.py")
    with open(module_path, "w") as f:
        f.write(_HELPER_TEMPLATE.format(module=helper_module))
    return helpers_dir


# =====================================================================
# Harness.
# =====================================================================
class BackfillHarness:
    """Orchestrates multi-tenant synthetic backfill end-to-end.

    Args:
      pool: asyncpg pool connected to the test Postgres.
      scenarios: List of BackfillScenarios.
      concurrency: Max in-flight tenants during the install / poll
        phases. Default 4.
      completion_deadline_s: Per-tenant deadline for waiting on
        tenant_onboarding_completed. Default 60s.
      kafka_bootstrap_servers: KAFKA_BOOTSTRAP_SERVERS env for the
        subprocesses. Default "localhost:9092".
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        scenarios: list[BackfillScenario],
        concurrency: int = 4,
        completion_deadline_s: float = 60.0,
        kafka_bootstrap_servers: str = "localhost:9092",
        drain_timeout_s: float = 30.0,
        drain_poll_interval_s: float = 2.0,
        real_clients: bool = False,
        spammer_rate_limit_every: int = 0,
    ) -> None:
        self._pool = pool
        self._scenarios = scenarios
        self._concurrency = max(1, concurrency)
        self._deadline_s = completion_deadline_s
        self._kafka_bootstrap = kafka_bootstrap_servers
        # Real-client mode (A30.7): instead of monkeypatching in-process
        # mock clients, spawn the source-mock SPAMMER on a real port and
        # let the subprocesses' REAL source clients hit it over HTTP
        # (token exchange → authed request → pagination → 429 backoff).
        # The X3 mock helper is NOT installed; backfill resolves
        # `*_API_BASE_URL` → the spammer via lib.integrations.endpoints.
        # Gmail is the proven vertical (clean email-keyed identity); the
        # spammer is seeded from the SAME registry so counts match.
        self._real_clients = real_clients
        self._spammer_rate_limit_every = spammer_rate_limit_every
        self._spammer: Any = None
        self._sa_json_path: str | None = None
        # Configurable consumer-drain window (A30.6): the 30s default
        # matches the historical hardcode (Runs 1-3); the concurrent
        # runner raises it so a higher-volume backfill+live soak can
        # fully drain. `run()` uses these; `drain()` accepts overrides.
        self._drain_timeout_s = drain_timeout_s
        self._drain_poll_interval_s = drain_poll_interval_s
        # Populated by `setup()`; the concurrent orchestrator reads
        # these tenant_ids/slugs to address the same installs live.
        self._outcomes: list[TenantOutcome] | None = None
        self._start: float = 0.0
        # Raw-tier (S3) wiring for the M6.7 backfill producer + normalizer.
        self._s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
        self._s3_bucket = os.environ.get("S3_RAW_BUCKET", _DEFAULT_S3_BUCKET)
        self._ingestion_env = os.environ.get("INGESTION_ENV", "dev")
        self._workdir: str | None = None
        self._helpers_dir: str | None = None
        self._helper_module = (
            f"x3_helper_{uuid4().hex[:8]}"
        )
        self._registry_path: str | None = None
        self._procs: dict[str, subprocess.Popen | None] = {}

    async def run(self) -> HarnessResult:
        """Backward-compatible sequential composition (Runs 1-3):
        setup → spawn → wait-backfill → drain → collect → teardown.

        The concurrent orchestrator (Run 4) calls the phase methods
        directly so it can interleave the live phase with the backfill
        producer drive and drain the shared consumer chain ONCE."""
        await self.setup()
        try:
            self.start_services()
            await self.wait_for_backfill()
            await self.drain()
            await self.collect()
        finally:
            stderrs = self._teardown_services()
            self._last_elapsed = time.monotonic() - self._start
        return self.build_result(stderrs)

    # ---- Decomposed phases (used directly by the concurrent runner) ----
    async def setup(self) -> list[TenantOutcome]:
        """Phase A: seed tenants + fixtures + flags, ensure S3 bucket,
        write helper + registry, write install/onboarding rows. Returns
        (and stores) the outcomes so a caller can read tenant_ids before
        the producer/live phases run."""
        self._start = time.monotonic()
        self._outcomes = [
            TenantOutcome(
                scenario=s,
                tenant_id=uuid4(),
                expected_reshare=_scenario_expects_reshare(s),
            )
            for s in self._scenarios
        ]
        self._workdir = tempfile.mkdtemp(prefix="x3-harness-")
        await self._setup_tenants_and_fixtures(self._outcomes)
        await self._ensure_s3_bucket()
        self._registry_path = self._write_registry(self._outcomes)
        if self._real_clients:
            # No mock helper: the real source clients hit the spammer,
            # seeded from the registry we just wrote.
            self._start_spammer()
        else:
            self._helpers_dir = _write_helper(
                self._workdir, self._helper_module,
            )
        await self._invoke_oauth_callbacks(self._outcomes)
        return self._outcomes

    def _start_spammer(self) -> None:
        """Spawn the spammer on a real port + write the gmail DWD service-
        account JSON whose token_uri points at it (real-client mode)."""
        from services.synthetic.spammer.process import SpammerProcess

        self._spammer = SpammerProcess(
            registry_path=self._registry_path,
            rate_limit_every=self._spammer_rate_limit_every,
        ).start()
        self._sa_json_path = self._write_spammer_sa_json(
            f"{self._spammer.base_url}/gmail/token",
        )

    def _write_spammer_sa_json(self, token_uri: str) -> str:
        """A throwaway service-account JSON (real RSA key so the DWD
        minter signs a valid RS256 JWT) whose token_uri is the spammer."""
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()
        sa = {
            "type": "service_account", "project_id": "x3-spammer",
            "private_key_id": "k1", "private_key": pem,
            "client_email": "sa@x3-test.iam.gserviceaccount.com",
            "client_id": "1", "token_uri": token_uri,
        }
        path = os.path.join(self._workdir, "spammer_sa.json")
        with open(path, "w") as f:
            json.dump(sa, f)
        return path

    @property
    def outcomes(self) -> list[TenantOutcome]:
        if self._outcomes is None:
            raise RuntimeError("BackfillHarness.setup() not called yet")
        return self._outcomes

    def start_services(self) -> None:
        """Phase B(i): spawn the 7 shared subprocesses (producer chain +
        normalizer + observation_writer consumers). The consumers drain
        BOTH backfill and live-via-Kafka envelopes from `ingestion.raw`."""
        self._spawn_services()

    async def wait_for_backfill(self) -> None:
        """Phase B(ii): wait for every tenant's `tenant_onboarding_completed`
        producer signal."""
        await self._wait_for_completions(self.outcomes)

    async def drain(
        self,
        *,
        timeout_s: float | None = None,
        poll_interval_s: float | None = None,
    ) -> None:
        """Phase B(iii): wait for the consumer chain to materialize
        observations. Timeout defaults to the configured drain window
        (A30.6) but the concurrent runner may override per call."""
        await self._wait_for_observations_to_drain(
            self.outcomes,
            timeout_s=timeout_s if timeout_s is not None else self._drain_timeout_s,
            poll_interval_s=(
                poll_interval_s if poll_interval_s is not None
                else self._drain_poll_interval_s
            ),
        )

    async def collect(self) -> None:
        """Phase C(i): collect per-tenant observations + state snapshots."""
        await self._collect_state(self.outcomes)

    def teardown(self) -> dict[str, str]:
        """Phase C(ii): SIGTERM the subprocesses; return stderr tails."""
        stderrs = self._teardown_services()
        self._last_elapsed = time.monotonic() - self._start
        return stderrs

    def build_result(self, stderrs: dict[str, str]) -> HarnessResult:
        return HarnessResult(
            outcomes=self.outcomes,
            subprocess_returncodes={
                k: (v.returncode if v else -999)
                for k, v in self._procs.items()
            },
            subprocess_stderr_tails=stderrs,
            wall_time_seconds=getattr(self, "_last_elapsed", 0.0),
        )

    # ---- Phase A: Setup ----
    async def _setup_tenants_and_fixtures(
        self, outcomes: list[TenantOutcome],
    ) -> None:
        for outcome in outcomes:
            await self._pool.execute(
                "INSERT INTO tenants (id, name) VALUES ($1, $2) "
                "ON CONFLICT (id) DO NOTHING",
                outcome.tenant_id,
                f"x3-{outcome.scenario.tenant_slug}-"
                f"{outcome.tenant_id.hex[:8]}",
            )
            # A27.4 — flip the cutover flag so observation_writer writes
            # (instead of shadow-logging a no-op) for this tenant.
            await self._pool.execute(
                "INSERT INTO tenant_flags "
                "    (tenant_id, flag_name, flag_value, set_by) "
                "VALUES ($1, $2, TRUE, 'x3-harness') "
                "ON CONFLICT (tenant_id, flag_name) "
                "    DO UPDATE SET flag_value = TRUE",
                outcome.tenant_id, KAFKA_PATH_ENABLED,
            )

    async def _ensure_s3_bucket(self) -> None:
        """Create the moto raw bucket if it doesn't exist (A27.4).

        No-op when S3_ENDPOINT_URL is unset (the producer then targets
        real AWS, which owns its own bucket lifecycle). Idempotent —
        BucketAlreadyOwnedByYou / BucketAlreadyExists are swallowed."""
        if not self._s3_endpoint:
            return
        import aioboto3

        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self._s3_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        ) as s3:
            try:
                await s3.create_bucket(Bucket=self._s3_bucket)
            except Exception as exc:  # noqa: BLE001
                # Already-exists is success; anything else is logged but
                # not fatal — the producer's put_if_absent will surface a
                # genuine misconfiguration loudly per-shard.
                if "BucketAlreadyOwnedByYou" not in repr(exc) and (
                    "BucketAlreadyExists" not in repr(exc)
                ):
                    log.warning("x3.s3_bucket_create_failed: %r", exc)

    def _align_identity(
        self, outcome: TenantOutcome, params: dict[str, Any],
    ) -> dict[str, Any]:
        """Real-client mode: make the fixture's source-native identity match
        the install row's `installation_id` (`x3-{slug}-{source}`), so the
        spammer (seeded from this registry), the backfill client's token, and
        the live targets all address the same tenant. Gmail keys on email
        (already unique) and needs no alignment."""
        source = outcome.scenario.source
        ident = f"x3-{outcome.scenario.tenant_slug}-{source}"
        if source == "github":
            return {**params, "installation_id": ident}
        if source == "slack":
            return {**params, "team_id": ident}
        if source == "discord":
            return {**params, "guild_id": ident}
        return params

    def _write_registry(self, outcomes: list[TenantOutcome]) -> str:
        entries = []
        for outcome in outcomes:
            params = outcome.scenario.fixture_params
            if self._real_clients:
                params = self._align_identity(outcome, params)
            fixture = _build_fixture(outcome.scenario.source, params)
            entries.append({
                "tenant_id": str(outcome.tenant_id),
                "tenant_slug": outcome.scenario.tenant_slug,
                "source": outcome.scenario.source,
                "fixture": fixture,
                "fault_profile": (
                    outcome.scenario.fault_profile.__dict__
                    if outcome.scenario.fault_profile.__dict__
                    else None
                ),
            })
        path = os.path.join(self._workdir, "registry.json")
        with open(path, "w") as f:
            json.dump({"entries": entries}, f)
        return path

    async def _invoke_oauth_callbacks(
        self, outcomes: list[TenantOutcome],
    ) -> None:
        """Drive each tenant's OAuth callback in-process via ASGI to
        write install + onboarding_triggers atomically per A20.

        For simplicity, the harness uses a streamlined direct-DB path
        instead of full OAuth callbacks: it writes the install row
        and the onboarding_triggers row in one transaction, mirroring
        what the production callback does. This avoids the secret-
        store / Kafka producer / state-token complexity that the
        production callback layer adds — those layers aren't what X3
        is testing. X3 is testing the M6 chain from
        `onboarding_triggers` onward.
        """
        sem = asyncio.Semaphore(self._concurrency)

        async def _one(outcome: TenantOutcome) -> None:
            async with sem:
                try:
                    await self._write_install_and_trigger(outcome)
                except Exception as exc:  # noqa: BLE001
                    outcome.install_error = (
                        f"{type(exc).__name__}: {exc}"
                    )

        await asyncio.gather(*(_one(o) for o in outcomes))

    async def _write_install_and_trigger(
        self, outcome: TenantOutcome,
    ) -> None:
        source = outcome.scenario.source
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if source == "gmail":
                    install_id = await conn.fetchval(
                        """
                        INSERT INTO gmail_installations (
                          id, tenant_id, workspace_domain,
                          service_account_email, scope
                        ) VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (tenant_id, workspace_domain)
                            DO UPDATE SET disabled_at = NULL
                        RETURNING id
                        """,
                        uuid7(), outcome.tenant_id,
                        f"x3-{outcome.scenario.tenant_slug}.example",
                        "sa@x3-test.iam.gserviceaccount.com",
                        "gmail.metadata",
                    )
                    # Seed one active mailbox watch so the planner's
                    # S1-amended loader (_LOAD_GMAIL_INSTALL_SQL) returns
                    # a non-empty `mailboxes` aggregate → one shard. Without
                    # this the gmail planner emits zero shards (clean run,
                    # zero records). `history_id` = starting_history_id so
                    # clean scenarios show no gap and reshare scenarios
                    # (history_events>0 → current_history_id advances)
                    # still trigger the reconciler gap-fill.
                    fp = outcome.scenario.fixture_params
                    await conn.execute(
                        """
                        INSERT INTO gmail_mailbox_watches (
                            id, tenant_id, gmail_installation_id,
                            email_address, google_user_id, history_id,
                            state
                        ) VALUES ($1, $2, $3, $4, $5, $6, 'active')
                        """,
                        uuid7(), outcome.tenant_id, install_id,
                        fp["email"],
                        f"x3-{outcome.tenant_id.hex[:12]}",
                        str(fp.get("starting_history_id", 1000)),
                    )
                    await conn.execute(
                        """
                        INSERT INTO onboarding_triggers (
                            id, tenant_id, source, trigger_kind,
                            gmail_installation_id, payload
                        ) VALUES ($1, $2, 'gmail', 'install', $3,
                                  '{}'::jsonb)
                        ON CONFLICT (tenant_id, source,
                                     gmail_installation_id)
                            WHERE gmail_installation_id IS NOT NULL
                            DO NOTHING
                        """,
                        uuid7(), outcome.tenant_id, install_id,
                    )
                else:
                    install_id = await conn.fetchval(
                        """
                        INSERT INTO provider_installations
                            (id, tenant_id, provider, installation_id,
                             secret_ref, enabled)
                        VALUES ($1, $2, $3, $4, NULL, TRUE)
                        ON CONFLICT (provider, installation_id) DO UPDATE
                            SET enabled = TRUE
                        RETURNING id
                        """,
                        uuid7(), outcome.tenant_id, source,
                        f"x3-{outcome.scenario.tenant_slug}-{source}",
                    )
                    await conn.execute(
                        """
                        INSERT INTO onboarding_triggers (
                            id, tenant_id, source, trigger_kind,
                            installation_row_id, payload
                        ) VALUES ($1, $2, $3, 'install', $4,
                                  '{}'::jsonb)
                        ON CONFLICT (tenant_id, source,
                                     installation_row_id)
                            WHERE installation_row_id IS NOT NULL
                            DO NOTHING
                        """,
                        uuid7(), outcome.tenant_id, source, install_id,
                    )

    # ---- Phase B: Run ----
    def _base_env(self) -> dict[str, str]:
        """Build the shared subprocess env (testable in isolation)."""
        env = os.environ.copy()
        env["KAFKA_BOOTSTRAP_SERVERS"] = self._kafka_bootstrap
        env["WORKFLOWS_LOG_LEVEL"] = "WARNING"
        if self._real_clients:
            # Point the REAL source clients at the spammer (config-only).
            base = self._spammer.base_url
            env["SYNTHETIC_SOURCE_API_BASE"] = base
            env["GMAIL_API_BASE_URL"] = f"{base}/gmail/gmail/v1"
            env["GMAIL_SERVICE_ACCOUNT_JSON_FILE"] = self._sa_json_path or ""
        else:
            env["PYTHONPATH"] = (
                (self._helpers_dir or "") + os.pathsep
                + env.get("PYTHONPATH", "")
            )
            env["X3_FIXTURE_REGISTRY_PATH"] = self._registry_path or ""
        # A27.4 — S3 raw-tier wiring for the shard_fetch producer + the
        # normalizer. INGESTION_ENV pins the key prefix on both sides.
        env["S3_RAW_BUCKET"] = self._s3_bucket
        env["INGESTION_ENV"] = self._ingestion_env
        if self._s3_endpoint:
            env["S3_ENDPOINT_URL"] = self._s3_endpoint
            # moto accepts any creds, but botocore still requires SOME to
            # be resolvable. Supply dummy creds (matching _ensure_s3_bucket)
            # unless the operator already exported real ones — so the
            # producer's S3 write doesn't NoCredentialsError out of the box.
            env.setdefault("AWS_ACCESS_KEY_ID", "test")
            env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
            env.setdefault("AWS_DEFAULT_REGION", "us-east-1")
        return env

    def _service_specs(self) -> dict[str, tuple[str, dict[str, str]]]:
        """The (name → (module, extra_env)) spec for every subprocess.

        7 entries (A27.4): the 5 M6 framework services + the normalizer
        + the observation_writer. Extracted so tests can assert the
        roster without spawning real processes."""
        return {
            "oauth_poller": (
                "services.ingestion.workflows.oauth_poller",
                {"OAUTH_POLLER_TICK_SEC": "0.1",
                 "OAUTH_POLLER_BATCH": "10",
                 "OAUTH_POLLER_INSTANCE": f"x3-poll-{uuid4().hex[:6]}"},
            ),
            "tenant_onboarding": (
                "services.ingestion.workflows.tenant_onboarding",
                {"ORCHESTRATOR_TICK_SEC": "0.1",
                 "ORCHESTRATOR_BATCH": "20",
                 "ORCHESTRATOR_INSTANCE": f"x3-orch-{uuid4().hex[:6]}"},
            ),
            "source_onboarding": (
                "services.ingestion.workflows.source_onboarding",
                {"SOURCE_ONBOARDING_TICK_SEC": "0.1",
                 "SOURCE_ONBOARDING_BATCH": "20",
                 "SOURCE_ONBOARDING_INSTANCE":
                     f"x3-src-{uuid4().hex[:6]}"},
            ),
            "shard_fetch": (
                "services.ingestion.workflows.shard_fetch",
                {"SHARD_FETCH_TICK_SEC": "0.1",
                 "SHARD_FETCH_BATCH": "10",
                 "SHARD_FETCH_LEASE_SEC": "30.0",
                 "SHARD_FETCH_FLUSH_SEC": "2.0",
                 "SHARD_FETCH_INSTANCE":
                     f"x3-shf-{uuid4().hex[:6]}"},
            ),
            "reconciler": (
                "services.ingestion.workflows.reconciler",
                {"RECONCILER_TICK_SEC": "0.1",
                 "RECONCILER_BATCH": "20",
                 "RECONCILER_INSTANCE":
                     f"x3-rec-{uuid4().hex[:6]}"},
            ),
            # A27.4 — the consumer half of the chain: normalizer turns
            # RawEnvelope pointers into NormalizedEnvelopes; the writer
            # writes observations (full-mode, gated by the per-tenant
            # kafka_path_enabled flag set at setup).
            "normalizer": (
                "services.ingestion.normalizer.worker",
                {"NORMALIZER_LOG_LEVEL": "WARNING"},
            ),
            "observation_writer": (
                "services.ingestion.writers.observation_writer",
                {"WRITER_LOG_LEVEL": "WARNING"},
            ),
        }

    def _spawn_services(self) -> None:
        env = self._base_env()
        for name, (mod, extra_env) in self._service_specs().items():
            penv = env.copy()
            penv.update(extra_env)
            if self._real_clients:
                # No mock helper import — the default real openers run.
                code = f"from {mod} import main; main()"
            else:
                code = (
                    f"import {self._helper_module}; "
                    f"from {mod} import main; main()"
                )
            cmd = [sys.executable, "-c", code]
            self._procs[name] = subprocess.Popen(
                cmd,
                env=penv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    async def _wait_for_completions(
        self, outcomes: list[TenantOutcome],
    ) -> None:
        sem = asyncio.Semaphore(self._concurrency)

        async def _one(outcome: TenantOutcome) -> None:
            async with sem:
                deadline = time.monotonic() + self._deadline_s
                while time.monotonic() < deadline:
                    row = await self._pool.fetchrow(
                        """
                        SELECT onboarding_run_id
                          FROM onboarding_runs
                         WHERE tenant_id = $1
                         ORDER BY started_at DESC NULLS LAST
                         LIMIT 1
                        """,
                        outcome.tenant_id,
                    ) if False else None  # placeholder for typing
                    # Reads via run lookup → tenant_onboarding_completed
                    # signal in Bridge inbox keyed by run_id.
                    row = await self._pool.fetchrow(
                        """
                        SELECT id, status, completed_at
                          FROM onboarding_runs
                         WHERE tenant_id = $1
                         ORDER BY started_at DESC NULLS LAST
                         LIMIT 1
                        """,
                        outcome.tenant_id,
                    )
                    if row is not None:
                        outcome.onboarding_run_id = row["id"]
                        n = int(await self._pool.fetchval(
                            """
                            SELECT count(*) FROM workflow_signals
                             WHERE workflow_kind = $1
                               AND workflow_id = $2
                               AND signal_kind = $3
                               AND idempotency_key = $4
                            """,
                            BRIDGE_INBOX_KIND,
                            BRIDGE_INBOX_ID,
                            SIGNAL_KIND_TENANT_COMPLETED,
                            str(row["id"]),
                        ))
                        if n > 0:
                            outcome.completion_observed = True
                            outcome.completion_signal_count = n
                            return
                    await asyncio.sleep(0.2)

        await asyncio.gather(*(_one(o) for o in outcomes))

    async def _wait_for_observations_to_drain(
        self,
        outcomes: list[TenantOutcome],
        *,
        timeout_s: float = 30.0,
        poll_interval_s: float = 2.0,
    ) -> None:
        """Wait for the asynchronous consumer chain to materialize
        observations before collecting state.

        `_wait_for_completions` observes the PRODUCER side
        (`tenant_onboarding_completed`). But the CONSUMER chain —
        normalizer (ingestion.raw → ingestion.normalized) →
        observation_writer (→ `observations`) — runs on its own Kafka
        clock and lags producer completion. Without this wait the
        harness reads `observations` before the writer has caught up
        and sees zero, even when the chain is healthy.

        Per-tenant drain target = `expected_observation_count` when the
        scenario specifies one (> 0), else 1. The fallback-to-1 matters
        because several E2E tests leave the count unspecified (0) yet
        assert `len(observations) >= 1` per source; waiting only on
        positive expected counts would let those tenants be collected
        before the writer caught up. Returns once every target is met OR
        the timeout fires. A timeout is NOT silently absorbed: control
        returns and the downstream assertion surfaces the shortfall as a
        real diagnostic signal (a genuinely stalled chain, not a
        too-short wait — the default budget drains in seconds when the
        chain is healthy).
        """
        expected = {
            o.tenant_id: max(o.scenario.expected_observation_count, 1)
            for o in outcomes
        }
        if not expected:
            return
        tenant_ids = list(expected.keys())
        deadline = time.monotonic() + timeout_s
        while True:
            rows = await self._pool.fetch(
                """
                SELECT tenant_id, count(*) AS n
                  FROM observations
                 WHERE tenant_id = ANY($1::uuid[])
                 GROUP BY tenant_id
                """,
                tenant_ids,
            )
            counts = {r["tenant_id"]: int(r["n"]) for r in rows}
            if all(counts.get(tid, 0) >= n for tid, n in expected.items()):
                return
            if time.monotonic() >= deadline:
                return
            await asyncio.sleep(poll_interval_s)

    # ---- Phase C: Teardown ----
    def _teardown_services(self) -> dict[str, str]:
        stderrs: dict[str, str] = {}
        for name, proc in self._procs.items():
            if proc is None:
                continue
            try:
                proc.send_signal(sig_module.SIGTERM)
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                if proc.stderr is not None:
                    stderrs[name] = proc.stderr.read().decode(
                        errors="replace",
                    )[-2000:]
            except Exception as exc:  # noqa: BLE001
                stderrs[name] = f"teardown error: {exc!r}"
        if self._spammer is not None:
            try:
                tail = self._spammer.stop()
                if tail:
                    stderrs["spammer"] = tail
            except Exception as exc:  # noqa: BLE001
                stderrs["spammer"] = f"spammer teardown error: {exc!r}"
            self._spammer = None
        return stderrs

    async def _collect_state(
        self, outcomes: list[TenantOutcome],
    ) -> None:
        for outcome in outcomes:
            if outcome.onboarding_run_id is None:
                continue
            # observations
            rows = await self._pool.fetch(
                "SELECT external_id, source_channel, occurred_at "
                "FROM observations WHERE tenant_id = $1",
                outcome.tenant_id,
            )
            outcome.observations = [dict(r) for r in rows]

            # reconciliation pass count
            pass_count = await self._pool.fetchval(
                """
                SELECT reconciliation_pass_count
                  FROM source_onboarding_runs
                 WHERE onboarding_run_id = $1
                 LIMIT 1
                """,
                outcome.onboarding_run_id,
            )
            outcome.reconciliation_pass_count = int(pass_count or 0)

            # cursor history per shard
            shards = await self._pool.fetch(
                "SELECT id FROM onboarding_shards "
                "WHERE onboarding_run_id = $1",
                outcome.onboarding_run_id,
            )
            for shard in shards:
                state_row = await self._pool.fetchrow(
                    """
                    SELECT state_data FROM workflow_states
                     WHERE workflow_kind = 'shard_fetch'
                       AND workflow_id = $1
                    """,
                    str(shard["id"]),
                )
                if state_row is None:
                    continue
                data = state_row["state_data"]
                if isinstance(data, str):
                    data = json.loads(data)
                outcome.cursor_history[str(shard["id"])] = [data]


def _scenario_expects_reshare(s: BackfillScenario) -> bool:
    """True when the fixture parameters indicate a reshare-triggering
    shape (history_events > 0 for Gmail; analogous knobs for other
    sources may be added later)."""
    if s.source == "gmail":
        return int(s.fixture_params.get("history_events", 0)) > 0
    return False
