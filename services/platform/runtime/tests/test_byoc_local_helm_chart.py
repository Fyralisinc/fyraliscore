from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[4]
CHART = ROOT / "deploy" / "helm" / "fyralis"


def test_local_rehearsal_helm_chart_has_required_runtime_contract() -> None:
    chart = yaml.safe_load((CHART / "Chart.yaml").read_text(encoding="utf-8"))
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))

    assert chart["name"] == "fyralis"
    assert chart["type"] == "application"
    assert values["image"] == {
        "repository": "fyralis/local",
        "tag": "dev",
        "pullPolicy": "IfNotPresent",
    }
    assert values["postgres"]["enabled"] is True
    assert values["kafka"]["enabled"] is True
    assert values["minio"]["enabled"] is True
    assert values["redis"]["enabled"] is True
    assert values["gateway"]["enabled"] is True
    assert values["workers"]["enabled"] is True
    assert values["signalGateway"] == {
        "enabled": False,
        "signalCliVersion": "0.14.4.1",
        "installations": [],
    }
    assert values["minio"]["bucket"] == "fyralis-raw"
    assert values["minio"]["blobBucket"] == "fyralis-blobs"

    figma_oauth = values["app"]["figmaOAuth"]
    assert figma_oauth == {
        "enabled": False,
        "clientId": "",
        "redirectUri": "",
        "uiBaseUrl": "",
        "allowHttpLoopback": False,
        "scopes": (
            "current_user:read,file_metadata:read,file_content:read,"
            "file_comments:read,file_versions:read"
        ),
        "existingSecret": "",
    }
    assert "clientSecret" not in figma_oauth

    workers = values["workers"]["items"]
    assert workers["oauth-poller"]["enabled"] is True
    assert workers["oauth-poller"]["command"] == [
        "python",
        "-m",
        "services.ingest.ingestion.workflows.oauth_poller",
    ]
    assert workers["reconciler"]["enabled"] is True
    assert workers["periodic-reconciler"]["enabled"] is True
    assert workers["periodic-reconciler"]["command"] == [
        "python",
        "-m",
        "services.ingest.ingestion.workflows.periodic_reconciler",
    ]


def test_local_rehearsal_helm_chart_templates_core_surfaces() -> None:
    templates = {path.name for path in (CHART / "templates").glob("*.yaml")}

    assert {
        "configmap.yaml",
        "secret.yaml",
        "postgres.yaml",
        "kafka.yaml",
        "minio.yaml",
        "redis.yaml",
        "jobs.yaml",
        "gateway.yaml",
        "signal-gateways.yaml",
        "workers.yaml",
    }.issubset(templates)

    rendered_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((CHART / "templates").glob("*.yaml"))
    )
    assert "KAFKA_BOOTSTRAP_SERVERS" in rendered_source
    assert "DATABASE_URL" in rendered_source
    assert "S3_BLOB_BUCKET" in rendered_source
    assert (
        "mc mb --ignore-existing local/{{ .Values.minio.blobBucket }}"
        in rendered_source
    )
    assert "services.ingest.ingestion.workflows.oauth_poller" in (
        CHART / "values.yaml"
    ).read_text(encoding="utf-8")
    assert "services.ingest.ingestion.workflows.periodic_reconciler" in (
        CHART / "values.yaml"
    ).read_text(encoding="utf-8")

    configmap = (CHART / "templates" / "configmap.yaml").read_text(encoding="utf-8")
    workers_template = (CHART / "templates" / "workers.yaml").read_text(
        encoding="utf-8"
    )
    gateway_template = (CHART / "templates" / "gateway.yaml").read_text(
        encoding="utf-8"
    )
    assert "FIGMA_CLIENT_SECRET:" not in configmap
    assert "OAUTH_STATE_HMAC_KEY:" not in configmap
    assert "FIGMA_CLIENT_SECRET_SECRET_REF:" not in configmap
    assert "OAUTH_STATE_HMAC_KEY_SECRET_REF:" not in configmap
    assert "app.figmaOAuth.existingSecret" in workers_template
    assert "app.figmaOAuth.existingSecret" in gateway_template
    assert "SIGNAL_TENANT_ID" in rendered_source
    assert "SIGNAL_INSTALLATION_ID" in rendered_source
    assert "run_signal_gateway_worker.py" in rendered_source
    assert "scripts/docker-migrate.sh" in (CHART / "values.yaml").read_text(
        encoding="utf-8"
    )
    assert "scripts/provision_kafka_topics.py" in (CHART / "values.yaml").read_text(
        encoding="utf-8"
    )
    assert "services.app.gateway.main:app" in (CHART / "values.yaml").read_text(
        encoding="utf-8"
    )
