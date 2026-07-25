"""Exact tenant/install invariants for the Telegram gateway launcher."""
from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from scripts.run_telegram_gateway_worker import (
    load_telegram_runtime_binding,
    persist_telegram_update_state,
    required_runtime_identity,
    telegram_lease_key,
    telegram_worker_identity,
)
from services.platform.runtime.process_manifest import process_by_name


pytestmark = pytest.mark.asyncio

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy" / "helm" / "fyralis"


class _Store:
    async def get(self, ref, *, tenant_id):  # noqa: ANN001, ANN202
        return f"secret:{tenant_id}:{ref}"


class _Executor:
    def __init__(self, tenant_id, installation_id):  # noqa: ANN001
        self.tenant_id = tenant_id
        self.installation_id = installation_id
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query, *args):  # noqa: ANN001, ANN202
        self.calls.append((query, args))
        return {
            "id": self.installation_id,
            "tenant_id": self.tenant_id,
            "api_id": "12345",
            "api_hash_secret_ref": "api-hash-ref",
            "session_secret_ref": "session-ref",
        }

    async def fetch(self, query, *args):  # noqa: ANN001, ANN202
        self.calls.append((query, args))
        return [
            {
                "dialog_id": 7,
                "dialog_kind": "chat",
                "title": "Exact",
            }
        ]

    async def execute(self, query, *args):  # noqa: ANN001, ANN202
        self.calls.append((query, args))
        return "UPDATE 1"


async def test_runtime_identity_is_mandatory_and_exact() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    assert required_runtime_identity(
        {
            "TELEGRAM_TENANT_ID": str(tenant_id),
            "TELEGRAM_INSTALLATION_ID": str(installation_id),
        }
    ) == (tenant_id, installation_id)
    with pytest.raises(ValueError, match="TELEGRAM_TENANT_ID is required"):
        required_runtime_identity({})
    with pytest.raises(ValueError, match="TELEGRAM_INSTALLATION_ID is required"):
        required_runtime_identity(
            {"TELEGRAM_TENANT_ID": str(tenant_id)}
        )


async def test_worker_and_lease_identity_are_installation_scoped() -> None:
    tenant_id, first, second = uuid4(), uuid4(), uuid4()
    assert telegram_lease_key(tenant_id, first) != telegram_lease_key(
        tenant_id,
        second,
    )
    assert telegram_worker_identity(
        tenant_id,
        first,
    ) != telegram_worker_identity(tenant_id, second)
    definition = process_by_name("telegram_gateway_worker")
    assert definition.singleton is False
    assert "installation-scoped" in definition.description.casefold()


async def test_loader_and_state_write_use_same_exact_identity() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    executor = _Executor(tenant_id, installation_id)

    binding = await load_telegram_runtime_binding(
        executor,
        _Store(),
        tenant_id=tenant_id,
        installation_id=installation_id,
    )
    await persist_telegram_update_state(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        pts=1,
        qts=2,
        seq=3,
        date=None,
    )

    assert binding.tenant_id == tenant_id
    assert binding.installation_id == installation_id
    assert binding.api_id == 12345
    assert len(binding.dialog_rows) == 1
    assert "LIMIT 1" not in "\n".join(query for query, _args in executor.calls)
    assert all(
        args[:2] == (tenant_id, installation_id)
        for _query, args in executor.calls
    )


async def test_compose_declares_one_explicit_telegram_binding() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    worker = compose["services"]["telegram_gateway_worker"]

    assert "container_name" not in worker
    assert worker["env_file"] == ".env.production"
    env_example = (ROOT / ".env.production.example").read_text()
    assert "TELEGRAM_TENANT_ID=" in env_example
    assert "TELEGRAM_INSTALLATION_ID=" in env_example


async def test_helm_renders_one_telegram_worker_per_installation() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["telegramGateway"] == {
        "enabled": False,
        "installations": [],
    }

    template = (CHART / "templates" / "telegram-gateways.yaml").read_text()
    helpers = (CHART / "templates" / "_helpers.tpl").read_text()
    assert "range $installation := .Values.telegramGateway.installations" in template
    assert "$bindingHash :=" in template
    assert "replicas: 1" in template
    assert "TELEGRAM_TENANT_ID" in template
    assert "TELEGRAM_INSTALLATION_ID" in template
    assert "scripts/run_telegram_gateway_worker.py" in template
    assert "duplicate telegramGateway tenant/installation binding" in helpers
    assert "app.extraEnv.%s is installation-scoped" in helpers


async def test_helm_schema_requires_exact_telegram_binding_fields() -> None:
    schema = json.loads((CHART / "values.schema.json").read_text())
    telegram = schema["properties"]["telegramGateway"]
    item = telegram["properties"]["installations"]["items"]

    assert set(item["required"]) == {
        "name",
        "tenantId",
        "installationId",
    }
    assert item["properties"]["tenantId"]["pattern"]
    assert item["properties"]["installationId"]["pattern"]
