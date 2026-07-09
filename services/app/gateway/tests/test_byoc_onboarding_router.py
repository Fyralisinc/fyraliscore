from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI

from services.app.gateway.byoc_onboarding_router import (
    _ALL_REHEARSAL_SOURCES,
    _SOURCE_LIVE_INGRESS_PATHS,
    _SOURCE_NATIVE_CONNECT_CONTRACTS,
    _discord_source_access_payload,
    _discord_source_access_payload_for_installations,
    _execute_source_auto_connect_background_run,
    _ensure_rehearsal_actor,
    _materialize_source_auto_connect_run,
    _source_auto_connect_persisted_run_record,
    _source_auto_connect_run_descriptor,
    _source_auto_connect_state,
    _source_deployment_context,
    _source_installation_row,
    _source_provider_handoff,
    _source_rehearsal_status_payload,
    build_byoc_onboarding_router,
)
from lib.shared.errors import DiscordApiError
from services.platform.runtime.source_browser_agent_recipes import (
    browser_agent_recipe_for_source,
    missing_browser_agent_recipe_sources,
)
from services.platform.runtime.source_browser_agent_workflow import (
    source_browser_agent_run_for_payload,
)
from services.platform.runtime.byoc_onboarding_intents import (
    InMemoryOnboardingIntentStore,
)


class _RecordingSecretStore:
    def __init__(self) -> None:
        self.values: list[tuple[str, str]] = []

    async def put(self, plaintext, *, label, tenant_id):
        self.values.append((label, str(plaintext)))
        return f"secret-ref:{label}"


class _NativeInstallPool:
    def __init__(self, row):
        self.row = row
        self.query = ""
        self.args = ()

    async def fetchrow(self, query, *args):
        self.query = query
        self.args = args
        return self.row


class _GatewayPrefixedObservationPool:
    def __init__(self) -> None:
        self.observation_args = ()
        self.observation_count_args = ()

    async def fetchrow(self, query, *args):
        if "FROM ramp_installations" in query:
            return None
        if "FROM onboarding_triggers" in query:
            return {"total": 0, "consumed": 0}
        if "FROM ingestion_failures" in query:
            return {"total": 0}
        if "FROM source_onboarding_runs" in query:
            return {"sync_started_at": None}
        if "FROM onboarding_shards" in query:
            return {"failure": None}
        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query, *args):
        if "FROM onboarding_runs" in query:
            return []
        if "FROM onboarding_shards" in query:
            return []
        if "FROM observations" in query:
            self.observation_args = args
            return [
                {
                    "id": uuid4(),
                    "kind": "connection_proof",
                    "source_channel": "gateway:ramp:connection-proof",
                    "occurred_at": datetime(2026, 7, 1, 10, 35, tzinfo=UTC),
                    "content_text": "Ramp gateway proof landed.",
                }
            ]
        raise AssertionError(f"unexpected fetch query: {query}")

    async def fetchval(self, query, *args):
        if "FROM observations" in query:
            self.observation_count_args = args
            return 1
        raise AssertionError(f"unexpected fetchval query: {query}")


class _DiscordAccessPool:
    def __init__(
        self,
        *,
        stats_rows: list[dict] | None = None,
        access_state: dict[str, dict] | None = None,
    ) -> None:
        self.stats_args = ()
        self.stats_rows = stats_rows
        self.access_state = access_state or {}
        self.triggers: list[dict] = []

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def transaction(self):
        return self

    async def fetch(self, query, *args):
        if "source_channel = 'discord:message'" in query:
            self.stats_args = args
            if self.stats_rows is not None:
                return self.stats_rows
            return [{
                "channel_id": "c-ready",
                "observation_count": 2,
                "last_observation_at": datetime(2026, 7, 8, 9, 44, tzinfo=UTC),
            }]
        if "FROM source_resource_access_state" in query and "SELECT resource_id" in query:
            return [
                {
                    "resource_id": resource_id,
                    "permission_status": row.get("permission_status", "unknown"),
                }
                for resource_id, row in self.access_state.items()
            ]
        if "UPDATE source_resource_access_state" in query:
            _, _, resource_id, replay_from_statuses, observation_count, metadata = args
            row = self.access_state.get(resource_id)
            if not row or row.get("permission_status") not in replay_from_statuses:
                return []
            row.update(
                {
                    "permission_status": "ready",
                    "observation_count": observation_count,
                    "last_ready_replay_at": datetime(2026, 7, 8, 9, 44, tzinfo=UTC),
                    "metadata": json.loads(metadata),
                }
            )
            return [{"resource_id": resource_id}]
        raise AssertionError(f"unexpected fetch query: {query}")

    async def execute(self, query, *args):
        if "INSERT INTO source_resource_access_state" in query:
            _, _, resource_id, permission_status, observation_count, metadata = args
            previous = self.access_state.get(resource_id, {})
            previous.update(
                {
                    "permission_status": permission_status,
                    "observation_count": observation_count,
                    "metadata": json.loads(metadata),
                }
            )
            self.access_state[resource_id] = previous
            return "INSERT 0 1"
        if "INSERT INTO onboarding_triggers" in query:
            trigger_id, tenant_id, payload = args
            self.triggers.append(
                {
                    "id": trigger_id,
                    "tenant_id": tenant_id,
                    "payload": json.loads(payload),
                }
            )
            return "INSERT 0 1"
        raise AssertionError(f"unexpected execute query: {query}")


class _DiscordMultiAccessPool(_DiscordAccessPool):
    def __init__(self) -> None:
        super().__init__(
            stats_rows=[
                {
                    "channel_id": "channel-guild-a",
                    "observation_count": 3,
                    "last_observation_at": datetime(2026, 7, 8, 9, 44, tzinfo=UTC),
                },
                {
                    "channel_id": "channel-guild-b",
                    "observation_count": 5,
                    "last_observation_at": datetime(2026, 7, 8, 9, 45, tzinfo=UTC),
                },
            ]
        )


def test_auto_connect_state_surfaces_only_real_admin_gates() -> None:
    payload = {
        "status": {
            "installed": False,
            "observation_count": 0,
        },
        "missing_configuration": [],
        "install_url": None,
        "finalize_mode": "native_finalizer_required",
        "automation_profile": {
            "human_step_count": 1,
            "human_steps": [
                {
                    "id": "create_provider_token",
                    "label": "Create a least-privilege Ramp token or service user.",
                    "reason": "Ramp requires an accountable admin action.",
                    "can_agent_complete": False,
                }
            ],
            "automated_actions": [
                "prepare provider handoff and gateway routes",
                "discover business scope",
            ],
        },
    }

    state = _source_auto_connect_state("ramp", payload)

    assert state["state"] == "admin_gate"
    assert state["label"] == "Admin gate"
    assert "provider-required" in state["message"]
    assert state["human_step_count"] == 1
    assert state["automated_actions"] == [
        "prepare provider handoff and gateway routes",
        "discover business scope",
    ]
    assert state["browser_agent"]["source"] == "ramp"
    assert state["browser_agent_run"]["source"] == "ramp"
    assert state["browser_agent_run"]["state"] == "waiting_for_admin"


def test_auto_connect_run_descriptor_is_sanitized_and_executable() -> None:
    payload = {
        "native_connect": {
            "kind": "ramp_native_connect",
            "preflight_path": "/integrations/ramp/connect/preflight",
            "finalize_path": "/integrations/ramp/connect/finalize",
            "payload_fields": ["business_id", "access_token"],
        }
    }
    browser_agent_run = {
        "state": "waiting_for_admin",
        "launch_mode": "customer_cloud_admin_present_browser",
        "can_start": True,
        "handoff_url": "https://developers.ramp.com/",
        "current_action": {
            "id": "open_provider_settings",
            "owner": "fyralis_agent",
        },
        "action_queue": [
            {"id": "open_provider_settings", "owner": "fyralis_agent"},
            {"id": "run_native_preflight", "owner": "fyralis_agent"},
            {"id": "approve_provider_scope", "owner": "provider_admin"},
            {"id": "run_native_finalize", "owner": "fyralis_agent"},
        ],
    }

    descriptor = _source_auto_connect_run_descriptor(
        "ramp",
        payload,
        browser_agent_run,
    )

    assert descriptor["schema_version"] == "fyralis.byoc.source.auto_connect_run.v1"
    assert descriptor["source"] == "ramp"
    assert descriptor["status"] == "waiting_for_admin"
    assert descriptor["can_start"] is True
    assert descriptor["automated_action_count"] == 3
    assert descriptor["human_action_count"] == 1
    assert descriptor["native_connect_kind"] == "ramp_native_connect"
    assert "--execute-browser-dom" in descriptor["command_args"]
    assert "--execute-native" in descriptor["command_args"]
    assert descriptor["native_payload_template_path_hint"].endswith(
        "browser-agent-provider-setup/native-payload-template.json"
    )
    assert descriptor["raw_secret_values_included"] is False
    assert "access_token" not in descriptor["command_preview"]


def test_auto_connect_materializes_sanitized_background_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_SOURCE_AUTO_CONNECT_WORKDIR", str(tmp_path))
    browser_agent_run = source_browser_agent_run_for_payload(
        "ramp",
        {
            "browser_agent": browser_agent_recipe_for_source("ramp"),
            "provider_console_url": "https://developers.ramp.com/",
            "native_connect": _SOURCE_NATIVE_CONNECT_CONTRACTS["ramp"],
            "status": {"installed": False, "observation_count": 0},
            "missing_configuration": [],
            "automation_profile": {
                "human_steps": [],
                "automated_actions": ["prepare provider handoff"],
            },
        },
    )
    descriptor = _source_auto_connect_run_descriptor(
        "ramp",
        {"native_connect": _SOURCE_NATIVE_CONNECT_CONTRACTS["ramp"]},
        browser_agent_run,
    )

    record = _materialize_source_auto_connect_run(
        "ramp",
        browser_agent_run,
        descriptor,
    )

    artifact_path = Path(record["run_artifact_path_hint"])
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload, sort_keys=True)
    assert artifact_path.is_file()
    assert payload["schema_version"] == "fyralis.byoc.source.connection_artifact.v1"
    assert payload["browser_agent_run"]["source"] == "ramp"
    assert payload["auto_connect_run"]["background_status"] == "queued"
    assert payload["auto_connect_run"]["run_artifact_path_hint"] == str(artifact_path)
    assert record["background_status"] == "queued"
    assert record["background_runner_mode"] == "artifact_materialization"
    assert str(tmp_path) in record["run_artifact_path_hint"]
    assert "ramp-access-token" not in serialized
    assert "raw_secret_values_included" in serialized


def test_auto_connect_status_recovers_persisted_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_SOURCE_AUTO_CONNECT_WORKDIR", str(tmp_path))
    browser_agent_run = source_browser_agent_run_for_payload(
        "slack",
        {
            "browser_agent": browser_agent_recipe_for_source("slack"),
            "provider_console_url": "https://api.slack.com/apps",
            "status": {"installed": False, "observation_count": 0},
            "missing_configuration": [],
            "automation_profile": {
                "human_steps": [],
                "automated_actions": ["prepare provider handoff"],
            },
        },
    )
    descriptor = _source_auto_connect_run_descriptor(
        "slack",
        {},
        browser_agent_run,
    )
    record = _materialize_source_auto_connect_run(
        "slack",
        browser_agent_run,
        descriptor,
    )
    receipt_path = Path(record["receipt_path_hint"])
    receipt_path.write_text(
        json.dumps(
            {
                "schema_version": "fyralis.byoc.source.browser_agent_runner_receipt.v1",
                "source": "slack",
                "generated_at": "2026-07-01T10:35:00+00:00",
                "status": "waiting_for_admin",
                "run_state": "waiting_for_admin",
                "handoff_url": "https://api.slack.com/apps",
                "handoff_opened": False,
                "native_connect_kind": None,
                "automated_action_count": 3,
                "human_action_count": 1,
                "completed_action_count": 2,
                "waiting_action_count": 1,
                "generated_artifacts": {},
                "action_results": [],
                "raw_secret_values_included": False,
                "raw_payloads_exported": False,
                "stored_scope": "sanitized_browser_agent_runner_metadata_only",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    recovered = _source_auto_connect_persisted_run_record("slack")

    assert recovered is not None
    assert recovered["source"] == "slack"
    assert recovered["background_status"] == "waiting_for_admin"
    assert recovered["status"] == "waiting_for_admin"
    assert recovered["background_finished_at"] == "2026-07-01T10:35:00+00:00"
    assert recovered["run_artifact_path_hint"] == record["run_artifact_path_hint"]
    assert recovered["receipt_path_hint"] == record["receipt_path_hint"]
    assert recovered["raw_secret_values_included"] is False


@pytest.mark.asyncio
async def test_auto_connect_background_runner_writes_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_SOURCE_AUTO_CONNECT_WORKDIR", str(tmp_path))
    browser_agent_run = source_browser_agent_run_for_payload(
        "slack",
        {
            "browser_agent": browser_agent_recipe_for_source("slack"),
            "provider_console_url": "https://api.slack.com/apps",
            "oauth_redirect_url": "https://customer.example/integrations/slack/callback",
            "events_request_url": "https://customer.example/webhooks/slack/events",
            "status": {"installed": False, "observation_count": 0},
            "missing_configuration": [],
            "automation_profile": {
                "human_steps": [],
                "automated_actions": ["prepare provider handoff"],
            },
        },
    )
    descriptor = _source_auto_connect_run_descriptor(
        "slack",
        {},
        browser_agent_run,
    )
    record = _materialize_source_auto_connect_run(
        "slack",
        browser_agent_run,
        descriptor,
    )
    store: dict[str, dict[str, object]] = {"slack": dict(record)}

    await _execute_source_auto_connect_background_run(
        "slack",
        Path(record["run_artifact_path_hint"]),
        Path(record["receipt_path_hint"]),
        "https://customer.example",
        store,
        "slack",
    )

    receipt_path = Path(record["receipt_path_hint"])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["schema_version"] == (
        "fyralis.byoc.source.browser_agent_runner_receipt.v1"
    )
    assert receipt["source"] == "slack"
    assert receipt["status"] in {"running", "waiting_for_admin"}
    assert store["slack"]["background_status"] == receipt["status"]
    assert receipt["raw_secret_values_included"] is False
    assert receipt["generated_artifacts"]


@pytest.mark.asyncio
async def test_source_rehearsal_status_reads_gateway_prefixed_observations() -> None:
    pool = _GatewayPrefixedObservationPool()
    tenant_id = uuid4()

    payload = await _source_rehearsal_status_payload(
        pool,
        tenant_id=tenant_id,
        source="ramp",
    )

    assert pool.observation_args == (
        tenant_id,
        "ramp:%",
        "gateway:ramp:%",
    )
    assert pool.observation_count_args == (
        tenant_id,
        "ramp:%",
        "gateway:ramp:%",
    )
    assert payload["observation_count"] == 1
    assert payload["observations"] == [
        {
            "id": payload["observations"][0]["id"],
            "kind": "connection_proof",
            "source_channel": "gateway:ramp:connection-proof",
            "occurred_at": "2026-07-01T10:35:00+00:00",
            "content_text": "Ramp gateway proof landed.",
        }
    ]


@pytest.mark.asyncio
async def test_discord_source_access_payload_marks_private_channels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _DiscordAccessPool()
    tenant_id = uuid4()
    installation_row_id = uuid4()

    class _ProbeDiscordClient:
        def __init__(self, **kwargs) -> None:
            self.guild_id = kwargs["guild_id"]
            self.closed = False

        async def list_guild_channels(self, guild_id):
            assert guild_id == self.guild_id
            return [
                {
                    "id": "cat-private",
                    "type": 4,
                    "name": "Private",
                    "permission_overwrites": [
                        {"id": "guild-1", "type": 0, "deny": str(1 << 10)}
                    ],
                },
                {
                    "id": "c-blocked",
                    "type": 0,
                    "name": "moderator-only",
                    "parent_id": "cat-private",
                    "position": 1,
                },
                {
                    "id": "c-ready",
                    "type": 0,
                    "name": "verify-here",
                    "parent_id": "cat-private",
                    "position": 2,
                },
                {"id": "voice", "type": 2, "name": "Voice"},
            ]

        async def list_guilds(self):
            return [{"id": "guild-1", "name": "Acme Ops"}]

        async def get_messages(self, *, channel_id, limit):
            assert limit == 1
            if channel_id == "c-blocked":
                raise DiscordApiError(
                    "missing access",
                    code="discord_channel_forbidden",
                    context={"channel_id": channel_id},
                )
            return []

        async def aclose(self):
            self.closed = True

    monkeypatch.setattr(
        "services.app.gateway.byoc_onboarding_router.DiscordClient",
        _ProbeDiscordClient,
    )

    payload = await _discord_source_access_payload(
        pool,
        tenant_id=tenant_id,
        install={
            "id": installation_row_id,
            "installation_id": "guild-1",
            "enabled": True,
            "has_secret": True,
            "installed_at": datetime(2026, 7, 8, tzinfo=UTC),
        },
    )

    assert pool.stats_args == (tenant_id,)
    assert payload["access_summary"] == {
        "total": 2,
        "ready": 1,
        "missing_access": 1,
        "needs_admin": 0,
        "not_selected": 0,
        "unknown": 0,
        "selected": 1,
        "observed": 1,
    }
    resources = {item["resource_id"]: item for item in payload["access_resources"]}
    assert resources["c-ready"]["permission_status"] == "ready"
    assert resources["c-ready"]["observation_count"] == 2
    assert resources["c-ready"]["installation_name"] == "Acme Ops"
    assert resources["c-ready"]["visibility"] == "private"
    assert resources["c-blocked"]["permission_status"] == "missing_access"
    assert resources["c-blocked"]["parent_name"] == "Private"
    assert resources["c-blocked"]["visibility"] == "private"
    assert resources["c-blocked"]["can_backfill"] is False
    assert "moderator-only" in payload["access_next_actions"][0]


@pytest.mark.asyncio
async def test_discord_source_access_payload_queues_replay_after_access_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _DiscordAccessPool(
        stats_rows=[],
        access_state={
            "c-now-ready": {
                "permission_status": "missing_access",
                "last_ready_replay_at": None,
            }
        },
    )
    tenant_id = uuid4()
    installation_row_id = uuid4()

    class _ProbeDiscordClient:
        def __init__(self, **kwargs) -> None:
            self.guild_id = kwargs["guild_id"]

        async def list_guild_channels(self, guild_id):
            assert guild_id == self.guild_id
            return [
                {
                    "id": "cat-private",
                    "type": 4,
                    "name": "Private",
                    "permission_overwrites": [
                        {"id": "guild-1", "type": 0, "deny": str(1 << 10)}
                    ],
                },
                {
                    "id": "c-now-ready",
                    "type": 0,
                    "name": "important-urls",
                    "parent_id": "cat-private",
                    "position": 1,
                },
            ]

        async def list_guilds(self):
            return [{"id": "guild-1", "name": "Acme Ops"}]

        async def get_messages(self, *, channel_id, limit):
            assert channel_id == "c-now-ready"
            assert limit == 1
            return []

        async def aclose(self):
            return None

    monkeypatch.setattr(
        "services.app.gateway.byoc_onboarding_router.DiscordClient",
        _ProbeDiscordClient,
    )

    payload = await _discord_source_access_payload(
        pool,
        tenant_id=tenant_id,
        install={
            "id": installation_row_id,
            "installation_id": "guild-1",
            "enabled": True,
            "has_secret": True,
            "installed_at": datetime(2026, 7, 8, tzinfo=UTC),
        },
    )

    assert payload["access_summary"]["ready"] == 1
    assert len(pool.triggers) == 1
    trigger_payload = pool.triggers[0]["payload"]
    assert trigger_payload["reason"] == "discord_channel_access_granted"
    assert trigger_payload["installation_row_id"] == str(installation_row_id)
    assert trigger_payload["guild_id"] == "guild-1"
    assert trigger_payload["channel_ids"] == ["c-now-ready"]
    assert any(
        "queued backfill" in action
        for action in payload["access_next_actions"]
    )


@pytest.mark.asyncio
async def test_discord_source_access_payload_includes_active_threads_without_archived_sweep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _DiscordAccessPool(stats_rows=[])
    tenant_id = uuid4()
    installation_row_id = uuid4()

    class _ThreadDiscordClient:
        def __init__(self, **kwargs) -> None:
            self.guild_id = kwargs["guild_id"]

        async def list_guild_channels(self, guild_id):
            assert guild_id == self.guild_id
            return [
                {"id": "general", "type": 0, "name": "general", "position": 1},
                {"id": "news", "type": 5, "name": "news", "position": 2},
                {"id": "forum", "type": 15, "name": "forum", "position": 3},
                {"id": "media", "type": 16, "name": "media", "position": 4},
                {"id": "voice", "type": 2, "name": "Voice"},
            ]

        async def list_active_guild_threads(self, guild_id):
            assert guild_id == self.guild_id
            return [
                {
                    "id": "active-thread",
                    "type": 11,
                    "name": "active-thread",
                    "parent_id": "general",
                }
            ]

        async def list_channel_archived_threads(self, channel_id, *, archive_kind):
            raise AssertionError("access status should not sweep archived threads")

        async def list_guilds(self):
            return [{"id": "guild-1", "name": "Acme Ops"}]

        async def get_messages(self, *, channel_id, limit):
            assert limit == 1
            return []

        async def aclose(self):
            return None

    monkeypatch.setattr(
        "services.app.gateway.byoc_onboarding_router.DiscordClient",
        _ThreadDiscordClient,
    )

    payload = await _discord_source_access_payload(
        pool,
        tenant_id=tenant_id,
        install={
            "id": installation_row_id,
            "installation_id": "guild-1",
            "enabled": True,
            "has_secret": True,
            "installed_at": datetime(2026, 7, 8, tzinfo=UTC),
        },
    )

    resources = {item["resource_id"]: item for item in payload["access_resources"]}
    assert set(resources) == {
        "general",
        "news",
        "active-thread",
    }
    assert resources["active-thread"]["parent_name"] == "general"
    assert payload["access_summary"]["ready"] == 3


@pytest.mark.asyncio
async def test_discord_source_access_payload_aggregates_multiple_guilds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    installation_a = uuid4()
    installation_b = uuid4()

    class _MultiGuildDiscordClient:
        def __init__(self, **kwargs) -> None:
            self.guild_id = kwargs["guild_id"]

        async def list_guild_channels(self, guild_id):
            assert guild_id == self.guild_id
            return [
                {
                    "id": f"channel-{guild_id}",
                    "type": 0,
                    "name": f"general-{guild_id}",
                    "position": 1,
                }
            ]

        async def list_guilds(self):
            return [
                {"id": "guild-a", "name": "Alpha Team"},
                {"id": "guild-b", "name": "Beta Team"},
            ]

        async def get_messages(self, *, channel_id, limit):
            assert limit == 1
            return []

        async def aclose(self):
            return None

    monkeypatch.setattr(
        "services.app.gateway.byoc_onboarding_router.DiscordClient",
        _MultiGuildDiscordClient,
    )

    payload = await _discord_source_access_payload_for_installations(
        _DiscordMultiAccessPool(),
        tenant_id=tenant_id,
        installations=[
            {
                "id": installation_a,
                "installation_id": "guild-a",
                "enabled": True,
                "has_secret": True,
                "installed_at": datetime(2026, 7, 8, tzinfo=UTC),
            },
            {
                "id": installation_b,
                "installation_id": "guild-b",
                "enabled": True,
                "has_secret": True,
                "installed_at": datetime(2026, 7, 8, tzinfo=UTC),
            },
        ],
    )

    assert payload["access_summary"] == {
        "total": 2,
        "ready": 2,
        "missing_access": 0,
        "needs_admin": 0,
        "not_selected": 0,
        "unknown": 0,
        "selected": 2,
        "observed": 2,
    }
    resources = {item["installation_id"]: item for item in payload["access_resources"]}
    assert set(resources) == {"guild-a", "guild-b"}
    assert resources["guild-a"]["resource_id"] == "channel-guild-a"
    assert resources["guild-a"]["installation_name"] == "Alpha Team"
    assert resources["guild-a"]["visibility"] == "public"
    assert resources["guild-b"]["resource_id"] == "channel-guild-b"
    assert resources["guild-b"]["installation_name"] == "Beta Team"


@pytest.mark.asyncio
async def test_discord_provider_handoff_uses_administrator_for_full_server_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DISCORD_CLIENT_ID", "discord-client")
    monkeypatch.setenv("DISCORD_REDIRECT_URI", "https://example.test/callback")
    monkeypatch.setenv("DISCORD_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("DISCORD_APPLICATION_ID", "discord-app")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", "webhook-secret")

    from services.ingest.integrations.discord import oauth as discord_oauth

    async def _state_token(*args, **kwargs):
        return "state-token"

    monkeypatch.setattr(discord_oauth, "issue_state_token", _state_token)

    payload = await _source_provider_handoff(
        "discord",
        pool=object(),
        tenant_id=uuid4(),
        request=SimpleNamespace(base_url="http://gateway.test/"),
        request_payload={"access_mode": "full_server_sync"},
    )

    assert payload["discord_access_mode"] == "full_server_sync"
    assert payload["discord_permissions"] == "8"
    assert "permissions=8" in payload["install_url"]
    assert "scope=applications.commands+bot" in payload["install_url"]


def test_browser_agent_recipes_cover_all_rehearsal_sources() -> None:
    assert missing_browser_agent_recipe_sources(_ALL_REHEARSAL_SOURCES) == set()


def test_native_connect_contracts_cover_mounted_source_routers() -> None:
    assert set(_SOURCE_NATIVE_CONNECT_CONTRACTS) >= {
        "ashby",
        "aws",
        "brex",
        "carta",
        "deel",
        "discord",
        "figma",
        "fireflies",
        "github",
        "gmail",
        "google_calendar",
        "google_drive",
        "grafana",
        "gusto",
        "hibob",
        "jira",
        "linkedin",
        "mercury",
        "miro",
        "notion",
        "quickbooks",
        "ramp",
        "signal",
        "slack",
        "telegram",
        "whatsapp",
    }
    assert _SOURCE_NATIVE_CONNECT_CONTRACTS["ramp"]["finalize_path"] == (
        "/integrations/ramp/connect/finalize"
    )
    assert "team_id" in _SOURCE_NATIVE_CONNECT_CONTRACTS["figma"]["payload_fields"]
    assert _SOURCE_NATIVE_CONNECT_CONTRACTS["gmail"]["preflight_path"] == (
        "/integrations/gmail/connect/preflight"
    )
    assert _SOURCE_NATIVE_CONNECT_CONTRACTS["gmail"]["preflight_payload_fields"] == [
        "workspace_domain",
        "admin_email",
        "scope",
    ]
    assert _SOURCE_NATIVE_CONNECT_CONTRACTS["gmail"]["scope_aliases"] == [
        "gmail.metadata"
    ]
    assert _SOURCE_NATIVE_CONNECT_CONTRACTS["google_calendar"][
        "preflight_payload_fields"
    ] == ["workspace_domain", "admin_email", "scope"]
    assert "google_calendar" not in _SOURCE_LIVE_INGRESS_PATHS


def test_google_browser_agent_run_uses_native_dwd_contract() -> None:
    run = source_browser_agent_run_for_payload(
        "google_calendar",
        {
            "browser_agent": browser_agent_recipe_for_source("google_calendar"),
            "native_connect": {
                "kind": "google_workspace_dwd",
                "preflight_path": "/integrations/google_calendar/connect/preflight",
                "finalize_path": "/integrations/google_calendar/connect/finalize",
                "payload_fields": [
                    "workspace_domain",
                    "admin_email",
                    "scope",
                    "inclusion_spec",
                ],
            },
            "status": {
                "installed": False,
                "observation_count": 0,
            },
            "missing_configuration": [],
            "automation_profile": {
                "human_steps": [
                    {
                        "id": "authorize_workspace_dwd",
                        "label": "Approve Google Calendar Domain-Wide Delegation scopes.",
                        "reason": "Google Workspace requires an admin approval.",
                        "can_agent_complete": False,
                    }
                ],
                "automated_actions": [
                    "prepare Google Workspace DWD preflight and finalize contract"
                ],
            },
        },
    )

    assert run["native_connect"]["kind"] == "google_workspace_dwd"
    assert run["native_connect"]["preflight_path"].endswith(
        "/google_calendar/connect/preflight"
    )
    assert any(action["id"] == "run_native_preflight" for action in run["action_queue"])
    assert any(action["id"] == "run_native_finalize" for action in run["action_queue"])


def test_slack_browser_agent_run_includes_generated_setup_bundle() -> None:
    run = source_browser_agent_run_for_payload(
        "slack",
        {
            "browser_agent": browser_agent_recipe_for_source("slack"),
            "provider_console_url": "https://api.slack.com/apps",
            "oauth_redirect_url": (
                "https://fyralis-ingress.customer.example/integrations/slack/callback"
            ),
            "events_request_url": (
                "https://fyralis-ingress.customer.example/webhooks/slack/events"
            ),
            "status": {
                "installed": False,
                "observation_count": 0,
            },
            "missing_configuration": [],
            "automation_profile": {
                "human_steps": [
                    {
                        "id": "authorize_oauth_app_or_dwd_locally",
                        "label": "Approve Slack OAuth scopes.",
                        "reason": "Slack requires an admin approval.",
                        "can_agent_complete": False,
                    }
                ],
                "automated_actions": ["prepare provider handoff and gateway routes"],
            },
        },
    )

    bundle = run["provider_setup_bundle"]
    assert bundle["kind"] == "slack_app_manifest"
    assert bundle["oauth_redirect_url"].endswith("/integrations/slack/callback")
    assert bundle["events_request_url"].endswith("/webhooks/slack/events")
    assert bundle["browser_dom_plan"]["schema_version"] == (
        "fyralis.byoc.source.browser_dom_plan.v1"
    )
    assert any(
        step["action"] == "paste_or_upload_manifest"
        for step in bundle["browser_dom_plan"]["steps"]
    )
    assert any(
        action["id"] == "generate_slack_app_manifest"
        for action in run["action_queue"]
    )
    assert "channels:history" in bundle["artifacts"][0]["content"]


def test_browser_agent_provider_setup_bundles_are_specific_for_all_sources() -> None:
    expected_kinds = {
        "ashby": "api_token_provider_setup",
        "aws": "aws_iam_role_setup",
        "brex": "api_token_provider_setup",
        "carta": "oauth_provider_setup",
        "deel": "api_token_provider_setup",
        "discord": "discord_application_setup",
        "figma": "api_token_provider_setup",
        "fireflies": "api_token_provider_setup",
        "github": "github_app_manifest",
        "gmail": "google_workspace_dwd_setup",
        "google_calendar": "google_workspace_dwd_setup",
        "google_drive": "google_workspace_dwd_setup",
        "grafana": "api_token_provider_setup",
        "gusto": "oauth_provider_setup",
        "hibob": "api_token_provider_setup",
        "jira": "jira_api_token_webhook_setup",
        "linkedin": "oauth_provider_setup",
        "mercury": "api_token_provider_setup",
        "miro": "api_token_provider_setup",
        "notion": "notion_integration_setup",
        "quickbooks": "oauth_provider_setup",
        "ramp": "api_token_provider_setup",
        "signal": "local_gateway_session_setup",
        "slack": "slack_app_manifest",
        "telegram": "local_gateway_session_setup",
        "whatsapp": "whatsapp_webhook_setup",
    }

    assert set(expected_kinds) == _ALL_REHEARSAL_SOURCES
    for source, expected_kind in expected_kinds.items():
        run = source_browser_agent_run_for_payload(
            source,
            {
                "browser_agent": browser_agent_recipe_for_source(source),
                "provider_console_url": browser_agent_recipe_for_source(source)[
                    "provider_console_url"
                ],
                "oauth_redirect_url": (
                    f"https://fyralis-ingress.customer.example/{source}/callback"
                ),
                "events_request_url": (
                    f"https://fyralis-ingress.customer.example/{source}/events"
                ),
                "status": {
                    "installed": False,
                    "observation_count": 0,
                },
                "missing_configuration": [],
                "automation_profile": {
                    "human_steps": [],
                    "automated_actions": [],
                },
            },
        )
        bundle = run["provider_setup_bundle"]

        assert bundle["kind"] == expected_kind
        assert bundle["kind"] != "generic_provider_setup_contract"
        assert bundle["artifacts"]
        assert bundle["browser_tasks"]
        assert bundle["browser_dom_plan"]["schema_version"] == (
            "fyralis.byoc.source.browser_dom_plan.v1"
        )
        assert bundle["browser_dom_plan"]["steps"]
        assert any(
            action["kind"] == "materialize_provider_setup_bundle"
            for action in bundle["agent_actions"]
        )
        assert any(
            action["kind"] == "materialize_browser_dom_plan"
            for action in bundle["agent_actions"]
        )
        if source == "aws":
            expected_aws_url = (
                "https://console.aws.amazon.com/cloudformation/home#/stacks/create/template"
            )
            artifact_filenames = {artifact["filename"] for artifact in bundle["artifacts"]}
            assert bundle["provider_console_url"] == (
                expected_aws_url
            )
            assert bundle["browser_dom_plan"]["provider_console_url"] == (
                expected_aws_url
            )
            assert "fyralis-aws-source-role-cloudformation.json" in artifact_filenames


def test_aws_browser_agent_bundle_autofills_cloudformation_deployment_context():
    assuming_principal_arn = "arn:aws:iam::587628268464:role/fyralis-runtime"
    run = source_browser_agent_run_for_payload(
        "aws",
        {
            "browser_agent": browser_agent_recipe_for_source("aws"),
            "provider_console_url": (
                "https://console.aws.amazon.com/cloudformation/home#/stacks/create/template"
            ),
            "native_connect": _SOURCE_NATIVE_CONNECT_CONTRACTS["aws"],
            "deployment_context": {
                "aws_region": "us-west-2",
                "aws_assuming_principal_arn": assuming_principal_arn,
            },
            "status": {
                "installed": False,
                "observation_count": 0,
            },
            "missing_configuration": [],
            "automation_profile": {
                "human_steps": [],
                "automated_actions": [],
            },
        },
    )
    bundle = run["provider_setup_bundle"]
    setup_artifact = next(
        artifact
        for artifact in bundle["artifacts"]
        if artifact["filename"] == "fyralis-aws-iam-role-setup.json"
    )
    cloudformation_artifact = next(
        artifact
        for artifact in bundle["artifacts"]
        if artifact["filename"] == "fyralis-aws-source-role-cloudformation.json"
    )
    template = cloudformation_artifact["json"]
    parameter = template["Parameters"]["FyralisAssumingPrincipalArn"]
    assume_statement = template["Resources"]["FyralisSourceReadOnlyRole"][
        "Properties"
    ]["AssumeRolePolicyDocument"]["Statement"][0]
    parameter_fields = next(
        step["fields"]
        for step in bundle["browser_dom_plan"]["steps"]
        if step["id"] == "fill_aws_source_role_stack_details"
    )

    assert parameter["Default"] == assuming_principal_arn
    assert assume_statement["Principal"] == {"AWS": "*"}
    assert assume_statement["Condition"]["ArnEquals"] == {
        "aws:PrincipalArn": {"Ref": "FyralisAssumingPrincipalArn"}
    }
    assert "FyralisAssumingPrincipalArn" not in (
        setup_artifact["json"]["cloudformation"]["required_admin_parameters"]
    )
    assert {
        "name": "FyralisAssumingPrincipalArn",
        "value": assuming_principal_arn,
        "selectors": [
            "input[name*=FyralisAssumingPrincipalArn]",
            "input[id*=FyralisAssumingPrincipalArn]",
            "input",
        ],
    } in parameter_fields
    assert run["deployment_context"] == {
        "aws_region": "us-west-2",
        "aws_assuming_principal_arn": assuming_principal_arn,
    }


def test_aws_deployment_context_ignores_demo_role_and_resolves_outputs():
    assert _source_deployment_context(
        {
            "deployment_context": {
                "aws_region": "ap-south-1",
                "aws_assuming_principal_arn": (
                    "arn:aws:iam::123456789012:role/FyralisByocSetupRole"
                ),
                "deployment_outputs": {
                    "SourceRuntimeRoleArn": (
                        "arn:aws:iam::587628268464:role/fyralis-runtime"
                    )
                },
            }
        }
    ) == {
        "aws_region": "ap-south-1",
        "aws_assuming_principal_arn": (
            "arn:aws:iam::587628268464:role/fyralis-runtime"
        ),
        "setup_role_arn": "arn:aws:iam::587628268464:role/fyralis-runtime",
        "source_runtime_role_source": "request_deployment_context",
    }


def test_aws_auto_connect_blocks_until_byoc_runtime_role_is_known(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.chdir(tmp_path)
    payload = {
        "status": {
            "installed": False,
            "observation_count": 0,
        },
        "automation_profile": {
            "human_steps": [],
            "automated_actions": [],
        },
        "missing_configuration": [],
        "install_url": None,
        "finalize_mode": "native_finalizer_required",
        "browser_agent": browser_agent_recipe_for_source("aws"),
        "native_connect": _SOURCE_NATIVE_CONNECT_CONTRACTS["aws"],
        "deployment_context": {"aws_region": "ap-south-1"},
    }

    state = _source_auto_connect_state("aws", payload)

    assert state["state"] == "blocked"
    assert state["label"] == "BYOC runtime missing"
    assert "SourceRuntimeRoleArn" in state["message"]
    assert state["install_url"] == (
        "https://ap-south-1.console.aws.amazon.com/cloudformation/home"
        "?region=ap-south-1#/stacks/create/template"
    )
    template_path = (
        tmp_path
        / ".fyralis/sources/aws/byoc-runtime/"
        "fyralis-byoc-source-runtime-role-cloudformation.json"
    )
    assert template_path.is_file()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "table_name", "count_key"),
    [
        ("gmail", "gmail_installations", "resolved_user_count"),
        (
            "google_calendar",
            "google_calendar_installations",
            "resolved_calendar_count",
        ),
        ("google_drive", "google_drive_installations", "resolved_target_count"),
    ],
)
async def test_google_native_install_rows_are_visible_to_onboarding_status(
    source: str,
    table_name: str,
    count_key: str,
) -> None:
    installed_at = datetime(2026, 7, 6, tzinfo=UTC)
    pool = _NativeInstallPool(
        {
            "installation_id": "acme.example",
            "enabled": True,
            "has_secret": True,
            "installed_at": installed_at,
            "service_account_email": "fyralis-dwd@acme.example",
            "scope": "readonly",
            count_key: 3,
            "include_shared_drives": True,
            "resolved_at": installed_at,
        }
    )

    row = await _source_installation_row(
        pool,
        tenant_id=uuid4(),
        source=source,
    )

    assert table_name in pool.query
    assert row is not None
    assert row["installation_id"] == "acme.example"
    assert row["enabled"] is True


@pytest.mark.asyncio
async def test_github_app_install_row_is_credential_covered_without_row_secret() -> None:
    installed_at = datetime(2026, 7, 6, tzinfo=UTC)
    pool = _NativeInstallPool(
        {
            "installation_id": "12345678",
            "enabled": True,
            "has_secret": False,
            "installed_at": installed_at,
        }
    )

    row = await _source_installation_row(
        pool,
        tenant_id=uuid4(),
        source="github",
    )

    assert "provider_installations" in pool.query
    assert pool.args[1] == "github"
    assert row is not None
    assert row["installation_id"] == "12345678"
    assert row["has_secret"] is True
    assert row["details"]["credential_scope"] == (
        "github_app_level_private_key_and_webhook_secret"
    )


def _app() -> tuple[FastAPI, InMemoryOnboardingIntentStore]:
    store = InMemoryOnboardingIntentStore()
    app = FastAPI()
    app.include_router(build_byoc_onboarding_router(store=store))
    return app, store


def _gateway_app(gateway_pool, secret_store=None) -> FastAPI:
    app = FastAPI()
    app.state.pool = gateway_pool
    if secret_store is not None:
        app.state.secret_store = secret_store
    app.include_router(build_byoc_onboarding_router(store=InMemoryOnboardingIntentStore()))
    return app


@pytest.mark.asyncio
async def test_design_partner_plan_selection_creates_sanitized_intent() -> None:
    app, store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/intents",
            json={
                "plan_code": "design_partner_byoc_pilot",
                "procurement_channel": "design_partner",
                "entrypoint": "get_fyralis",
            },
        )

    assert response.status_code == 201
    payload = response.json()
    rendered = response.text.lower()
    assert payload["schema_version"] == "fyralis.platform.onboarding_intent.v1"
    assert re.match(r"^ofi_[0-9a-f]{32}$", payload["intent_id"])
    assert payload["plan_code"] == "design_partner_byoc_pilot"
    assert payload["status"] == "draft"
    assert payload["customer_id"] is None
    assert payload["deployment_id"] is None
    assert payload["stored_scope"] == "sanitized_onboarding_metadata_only"
    assert store.events[0]["event_type"] == "plan_selected"
    assert "secret" not in rendered
    assert "token" not in rendered
    assert "credential" not in rendered


@pytest.mark.asyncio
async def test_enterprise_plan_selection_is_not_implemented_yet() -> None:
    app, _store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/intents",
            json={
                "plan_code": "enterprise_byoc",
                "procurement_channel": "sales",
                "entrypoint": "get_fyralis",
            },
        )

    assert response.status_code == 501
    assert response.json()["detail"]["error"] == "unsupported_onboarding_plan"


@pytest.mark.asyncio
async def test_slack_rehearsal_is_not_enabled_by_default() -> None:
    app, _store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/slack/rehearsal/prepare"
        )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "source_rehearsal_not_enabled"


@pytest.mark.asyncio
async def test_generic_source_prepare_returns_actionable_inputs(
    gateway_pool,
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    monkeypatch.setenv("FYRALIS_SOURCE_REHEARSAL_ENABLED", "1")
    monkeypatch.setenv("COMPANY_OS_TENANT_ID", str(tenant_id))
    monkeypatch.setenv("COMPANY_OS_CEO_ACTOR_ID", str(actor_id))

    app = _gateway_app(gateway_pool, _RecordingSecretStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/sources/hibob/rehearsal/prepare"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "hibob"
    assert payload["authorization_mode"] == "customer_local_provider_refs"
    assert payload["required_inputs"] == ["service_user_id", "service_user_token"]
    assert "company_id" in payload["optional_inputs"]
    assert "webhook_secret" in payload["optional_inputs"]
    assert payload["finalize_mode"] == "native_finalizer_required"
    assert payload["automation_profile"]["automation_level"] == (
        "fully_automated_after_customer_ref"
    )
    assert payload["automation_profile"]["human_step_count"] == 2
    assert payload["status"]["next_action"] == (
        "Submit the required HiBob connection details."
    )


@pytest.mark.asyncio
async def test_native_table_source_finalize_is_not_faked_by_generic_route(
    gateway_pool,
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    secret_store = _RecordingSecretStore()
    monkeypatch.setenv("FYRALIS_SOURCE_REHEARSAL_ENABLED", "1")
    monkeypatch.setenv("COMPANY_OS_TENANT_ID", str(tenant_id))
    monkeypatch.setenv("COMPANY_OS_CEO_ACTOR_ID", str(actor_id))

    app = _gateway_app(gateway_pool, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/sources/hibob/rehearsal/finalize",
            json={
                "inputs": {
                    "company_id": "hibob-company-1",
                    "service_user_token": "super-secret-token",
                    "webhook_secret": "webhook-super-secret",
                }
            },
        )

    assert response.status_code == 501
    assert response.json()["detail"]["error"] == "source_native_finalize_required"
    assert secret_store.values == []

    install_count = await gateway_pool.fetchval(
        """
        SELECT count(*)::int
          FROM provider_installations
         WHERE tenant_id = $1 AND provider = 'hibob'
        """,
        tenant_id,
    )
    assert install_count == 0

    trigger_count = await gateway_pool.fetchval(
        """
        SELECT count(*)::int
          FROM onboarding_triggers
         WHERE tenant_id = $1 AND source = 'hibob'
        """,
        tenant_id,
    )
    assert trigger_count == 0


@pytest.mark.asyncio
async def test_callback_owned_source_requires_provider_callback(
    gateway_pool,
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    monkeypatch.setenv("FYRALIS_SOURCE_REHEARSAL_ENABLED", "1")
    monkeypatch.setenv("COMPANY_OS_TENANT_ID", str(tenant_id))
    monkeypatch.setenv("COMPANY_OS_CEO_ACTOR_ID", str(actor_id))

    app = _gateway_app(gateway_pool, _RecordingSecretStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/sources/github/rehearsal/finalize",
            json={"inputs": {"installation_id": "123", "secret": "ignored"}},
        )

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "provider_callback_finalize_required"


@pytest.mark.asyncio
async def test_ramp_status_reads_native_install_row(
    gateway_pool,
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    monkeypatch.setenv("FYRALIS_SOURCE_REHEARSAL_ENABLED", "1")
    monkeypatch.setenv("COMPANY_OS_TENANT_ID", str(tenant_id))
    monkeypatch.setenv("COMPANY_OS_CEO_ACTOR_ID", str(actor_id))
    await _ensure_rehearsal_actor(gateway_pool, tenant_id=tenant_id, actor_id=actor_id)

    install_id = uuid4()
    await gateway_pool.execute(
        """
        INSERT INTO ramp_installations (
            id, tenant_id, business_id, base_url, secret_ref, webhook_secret_ref
        ) VALUES ($1, $2, 'biz_123', 'https://api.ramp.com/developer/v1',
                  'secret-ref:ramp-access', 'secret-ref:ramp-webhook')
        """,
        install_id,
        tenant_id,
    )

    app = _gateway_app(gateway_pool, _RecordingSecretStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/platform/onboarding/sources/ramp/rehearsal/status"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["installed"] is True
    assert payload["installation"] == {
        "installation_id": "biz_123",
        "enabled": True,
        "has_secret": True,
        "installed_at": payload["installation"]["installed_at"],
        "details": {
            "base_url": "https://api.ramp.com/developer/v1",
            "token_expires_at": None,
            "webhook_registered": True,
        },
    }


@pytest.mark.asyncio
async def test_whatsapp_finalize_writes_native_install_and_proof(
    gateway_pool,
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    secret_store = _RecordingSecretStore()
    monkeypatch.setenv("FYRALIS_SOURCE_REHEARSAL_ENABLED", "1")
    monkeypatch.setenv("COMPANY_OS_TENANT_ID", str(tenant_id))
    monkeypatch.setenv("COMPANY_OS_CEO_ACTOR_ID", str(actor_id))

    app = _gateway_app(gateway_pool, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/sources/whatsapp/rehearsal/finalize",
            json={
                "inputs": {
                    "phone_number_id": "15551234567",
                    "business_account_id": "waba-1",
                    "display_phone_number": "+1 555 123 4567",
                    "app_secret": "app-secret",
                    "verify_token": "verify-token",
                    "access_token": "graph-access-token",
                }
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["source"] == "whatsapp"
    assert payload["installation_id"] == "15551234567"
    assert payload["status"]["installed"] is True
    assert payload["status"]["trigger_count"] == 0
    assert payload["status"]["observation_count"] == 1
    assert sorted(label for label, _value in secret_store.values) == [
        "whatsapp_access_token:15551234567",
        "whatsapp_app_secret:15551234567",
        "whatsapp_verify_token:15551234567",
    ]

    install = await gateway_pool.fetchrow(
        """
        SELECT phone_number_id, waba_id, display_phone_number,
               app_secret, verify_token, access_token,
               app_secret_ref, verify_token_ref, access_token_ref, enabled
          FROM whatsapp_installations
         WHERE tenant_id = $1 AND phone_number_id = '15551234567'
        """,
        tenant_id,
    )
    assert install["waba_id"] == "waba-1"
    assert install["display_phone_number"] == "+1 555 123 4567"
    assert install["app_secret"] is None
    assert install["verify_token"] is None
    assert install["access_token"] is None
    assert install["app_secret_ref"] == "secret-ref:whatsapp_app_secret:15551234567"
    assert install["verify_token_ref"] == "secret-ref:whatsapp_verify_token:15551234567"
    assert install["access_token_ref"] == "secret-ref:whatsapp_access_token:15551234567"
    assert install["enabled"] is True

    proof = await gateway_pool.fetchrow(
        """
        SELECT source_channel, content_text, content::text
          FROM observations
         WHERE tenant_id = $1 AND source_channel = 'whatsapp:connection'
        """,
        tenant_id,
    )
    assert proof is not None
    assert "WhatsApp connection finalized" in proof["content_text"]
    assert "app_secret" in proof["content"]
    assert "verify_token" in proof["content"]
    assert "app-secret" not in proof["content"]
    assert "verify-token" not in proof["content"]


@pytest.mark.asyncio
async def test_rehearsal_actor_gets_tenant_admin_grant(gateway_pool) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()

    await _ensure_rehearsal_actor(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
    )

    row = await gateway_pool.fetchrow(
        """
        SELECT role, entity_type, entity_id, revoked_at
          FROM actor_roles
         WHERE tenant_id = $1
           AND actor_id = $2
           AND role = 'admin'
        """,
        tenant_id,
        actor_id,
    )

    assert row is not None
    assert row["entity_type"] == "tenant"
    assert row["entity_id"] is None
    assert row["revoked_at"] is None


@pytest.mark.asyncio
async def test_design_partner_intake_mints_customer_tenant_and_deployment_ids() -> None:
    app, store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/platform/onboarding/intents",
            json={
                "plan_code": "design_partner_byoc_pilot",
                "procurement_channel": "design_partner",
                "entrypoint": "get_fyralis",
            },
        )
        intent_id = created.json()["intent_id"]

        response = await client.post(
            f"/platform/onboarding/intents/{intent_id}/design-partner-intake",
            json={
                "company_name": "Acme Finance",
                "setup_owner_email": "Platform-Owner@Acme.Example",
                "target_cloud": "aws",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "workspace_created"
    assert re.match(r"^cus_[0-9a-f]{16}$", payload["customer_id"])
    assert re.match(r"^dep_[0-9a-f]{16}$", payload["deployment_id"])
    assert payload["tenant_id"]
    assert payload["company_name"] == "Acme Finance"
    assert payload["setup_owner_email"] == "platform-owner@acme.example"
    assert payload["target_cloud"] == "aws"
    assert [event["event_type"] for event in store.events] == [
        "plan_selected",
        "design_partner_intake_submitted",
        "workspace_created",
    ]


@pytest.mark.asyncio
async def test_design_partner_intake_requires_existing_intent() -> None:
    app, _store = _app()
    missing_id = "ofi_00000000000000000000000000000000"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            f"/platform/onboarding/intents/{missing_id}/design-partner-intake",
            json={
                "company_name": "Acme Finance",
                "setup_owner_email": "platform-owner@acme.example",
                "target_cloud": "aws",
            },
        )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "onboarding_intent_not_found"
