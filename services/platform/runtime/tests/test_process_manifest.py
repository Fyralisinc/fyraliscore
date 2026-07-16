from __future__ import annotations

from pathlib import Path

import yaml

from scripts.render_runtime_process_manifest import (
    render_dogfood_tsv,
    render_production_json,
    render_production_markdown,
)
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


def test_production_manifest_renders_operator_inventory() -> None:
    markdown = render_production_markdown()
    assert "| Process | Family | Compose service | Healthcheck | Singleton | Command |" in markdown
    assert "| think_worker | reasoning | think_worker | True | False | `python scripts/run_think_worker.py` |" in markdown
    assert "| housekeeper_worker | reasoning | housekeeper_worker | True | False | `python scripts/run_housekeeper_worker.py` |" in markdown
    assert "| source_semantic_worker | reasoning | source_semantic_worker | True | False | `python scripts/run_source_semantic_worker.py` |" in markdown
    assert "intervention_episode_coordinator" not in markdown
    assert "agency_activation_worker" not in markdown
    assert "work_scheduler_worker" not in markdown


def test_production_manifest_json_contains_compose_metadata() -> None:
    import json

    rows = json.loads(render_production_json())
    by_name = {row["name"]: row for row in rows}

    assert by_name["circuit_breaker"]["singleton"] is True
    assert by_name["circuit_breaker"]["has_healthcheck"] is True
    assert by_name["circuit_breaker"]["compose_service"] == "circuit_breaker"


def test_production_manifest_matches_compose_services() -> None:
    services = _compose_services()
    manifest = {
        p.compose_service: p for p in production_processes() if p.compose_service
    }

    missing = sorted(set(manifest) - set(services))
    assert missing == []
    assert len(manifest) == len({p.compose_service for p in manifest.values()})

    for service_name, process in manifest.items():
        service = services[service_name]
        expected_command = process.compose_command()
        if "command" in service and expected_command is not None:
            assert service["command"] == expected_command
        if process.has_healthcheck:
            assert "healthcheck" in service
        elif "healthcheck" in service and expected_command is not None:
            raise AssertionError(
                f"{service_name} has a compose healthcheck but the runtime "
                "manifest does not mark has_healthcheck=True"
            )


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

    missing_healthchecks = sorted(
        name for name in python_runtime_services if "healthcheck" not in services[name]
    )
    assert missing_healthchecks == []


def test_manifest_python_commands_have_existing_entrypoints() -> None:
    missing: list[str] = []
    for process in production_processes():
        command = process.command
        if not command or command[0] != "python":
            continue
        if len(command) >= 2 and command[1].endswith(".py"):
            path = ROOT / command[1]
            if not path.exists():
                missing.append(f"{process.name}: {command[1]}")
        elif len(command) >= 3 and command[1] == "-m":
            module_path = ROOT.joinpath(*command[2].split("."))
            if not module_path.with_suffix(".py").exists() and not (
                module_path / "__main__.py"
            ).exists():
                missing.append(f"{process.name}: {command[2]}")

    assert missing == []
