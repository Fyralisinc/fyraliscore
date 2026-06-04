from __future__ import annotations

from pathlib import Path

import yaml

from scripts.render_runtime_process_manifest import render_dogfood_tsv
from services.platform.runtime.process_manifest import (
    dogfood_processes,
    production_processes,
)


ROOT = Path(__file__).resolve().parents[4]


class _Args:
    python_bin = ".venv/bin/python"
    uvicorn_bin = ".venv/bin/uvicorn"
    gateway_port = "8000"
    uvicorn_log_level = "info"


def _compose_services() -> dict:
    with open(ROOT / "docker-compose.yml", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data["services"]


def test_dogfood_manifest_renders_startup_rows() -> None:
    rows = [
        line.split("\t")
        for line in render_dogfood_tsv(_Args()).strip().splitlines()
    ]

    assert [row[0] for row in rows] == [p.name for p in dogfood_processes()]
    assert {row[0]: row[2] for row in rows} == {
        "gateway": "gateway.log",
        "think_worker": "think_worker.log",
        "post_commit_worker": "post_commit_worker.log",
        "topology_sweeper": "topology_sweeper.log",
        "ui": "ui.log",
    }
    assert rows[0][3].startswith("exec .venv/bin/uvicorn ")
    assert rows[-1][1] == "ui"
    assert rows[-1][3] == "exec npm run dev"


def test_production_manifest_matches_compose_services() -> None:
    services = _compose_services()
    manifest = {
        p.compose_service: p for p in production_processes() if p.compose_service
    }

    missing = sorted(set(manifest) - set(services))
    assert missing == []

    for service_name, process in manifest.items():
        service = services[service_name]
        expected_command = process.compose_command()
        if "command" in service and expected_command is not None:
            assert service["command"] == expected_command
        if process.has_healthcheck:
            assert "healthcheck" in service


def test_compose_python_runtime_services_are_manifested() -> None:
    services = _compose_services()
    manifest_services = {
        p.compose_service for p in production_processes() if p.compose_service
    }

    python_runtime_services = {
        name
        for name, service in services.items()
        if isinstance(service.get("command"), str)
        and (
            service["command"].startswith("python -m services.")
            or service["command"].startswith("python scripts/run_")
        )
    }

    assert sorted(python_runtime_services - manifest_services) == []
