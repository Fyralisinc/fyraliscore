from __future__ import annotations

import json
from pathlib import Path

import yaml

from services.platform.runtime.process_manifest import production_processes


ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy" / "helm" / "fyralis"


def test_compose_signal_worker_has_one_explicit_installation_binding() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    worker = compose["services"]["signal_gateway_worker"]

    assert "container_name" not in worker
    assert worker["env_file"] == ".env.production"
    env_example = (ROOT / ".env.production.example").read_text()
    for key in (
        "SIGNAL_TENANT_ID=",
        "SIGNAL_INSTALLATION_ID=",
        "SIGNAL_JSONRPC_ENDPOINT=",
        "SIGNAL_CLI_VERSION=0.14.4.1",
    ):
        assert key in env_example

    process = next(
        item for item in production_processes() if item.name == "signal_gateway_worker"
    )
    assert process.singleton is False
    assert "installation-scoped" in process.description.casefold()
    assert "http json-rpc/sse" in process.description.casefold()


def test_helm_signal_gateway_is_a_per_installation_runtime() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    signal = values["signalGateway"]
    assert signal == {
        "enabled": False,
        "signalCliVersion": "0.14.4.1",
        "installations": [],
    }

    template = (CHART / "templates" / "signal-gateways.yaml").read_text(
        encoding="utf-8"
    )
    helpers = (CHART / "templates" / "_helpers.tpl").read_text(encoding="utf-8")
    assert "range $installation := .Values.signalGateway.installations" in template
    assert "$bindingHash :=" in template
    assert "replicas: 1" in template
    assert "SIGNAL_TENANT_ID" in template
    assert "SIGNAL_INSTALLATION_ID" in template
    assert "SIGNAL_JSONRPC_ENDPOINT" in template
    assert "scripts/run_signal_gateway_worker.py" in template
    assert "duplicate signalGateway tenant/installation binding" in helpers
    assert "app.extraEnv.%s is installation-scoped" in helpers


def test_helm_schema_pins_signal_cli_and_exact_binding_fields() -> None:
    schema = json.loads((CHART / "values.schema.json").read_text())
    signal = schema["properties"]["signalGateway"]
    properties = signal["properties"]
    item = properties["installations"]["items"]

    assert properties["signalCliVersion"]["const"] == "0.14.4.1"
    assert set(item["required"]) == {
        "name",
        "tenantId",
        "installationId",
        "jsonrpcEndpoint",
    }
    assert item["properties"]["tenantId"]["pattern"]
    assert item["properties"]["installationId"]["pattern"]
    assert item["properties"]["jsonrpcEndpoint"]["pattern"].startswith("^https?")
