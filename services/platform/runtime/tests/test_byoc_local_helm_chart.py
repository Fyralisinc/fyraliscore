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
        "workers.yaml",
    }.issubset(templates)

    rendered_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((CHART / "templates").glob("*.yaml"))
    )
    assert "KAFKA_BOOTSTRAP_SERVERS" in rendered_source
    assert "DATABASE_URL" in rendered_source
    assert "scripts/docker-migrate.sh" in (CHART / "values.yaml").read_text(
        encoding="utf-8"
    )
    assert "scripts/provision_kafka_topics.py" in (
        CHART / "values.yaml"
    ).read_text(encoding="utf-8")
    assert "services.app.gateway.main:app" in (CHART / "values.yaml").read_text(
        encoding="utf-8"
    )
