"""The deadline_resolver compose service parses and has the right entrypoint.

Phase-2 (docs/plans/document-memory-substrate.md §4.6 / §7 step 11): proactive
deadline firing for prediction Models (incl. document-memory commitments)
requires ``deadline_resolver`` in the worker fleet. This test runs
``docker compose config`` PARSE-ONLY (it never starts the service) and asserts
the merged service exists with its launcher command.

Skips cleanly when docker / compose is unavailable (CI sandboxes without a
docker daemon) so the suite stays runnable everywhere. ``docker compose config``
is offline (no daemon needed) but the binary must be present.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_COMPOSE = _REPO_ROOT / "docker-compose.yml"


def _have_compose() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        r = subprocess.run(
            ["docker", "compose", "version"],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
        )
        return r.returncode == 0
    except Exception:  # noqa: BLE001
        return False


pytestmark = pytest.mark.skipif(
    not _COMPOSE.exists() or not _have_compose(),
    reason="docker compose not available (parse-only test)",
)


def _compose_config() -> dict:
    """Return the merged compose config as a dict (parse-only).

    Several services declare ``env_file: .env.production`` (gitignored), which
    compose requires to exist even for `config`. Create an empty placeholder
    only when absent, and remove exactly that placeholder afterward — never
    touching a real, pre-existing file.
    """
    env_prod = _REPO_ROOT / ".env.production"
    created = False
    if not env_prod.exists():
        env_prod.write_text("")
        created = True
    try:
        proc = subprocess.run(
            ["docker", "compose", "config", "--format", "json"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=180,
        )
    finally:
        if created and env_prod.exists():
            env_prod.unlink()
    assert proc.returncode == 0, f"docker compose config failed:\n{proc.stderr}"
    return json.loads(proc.stdout)


def test_compose_config_parses():
    cfg = _compose_config()
    assert "services" in cfg and cfg["services"]


def test_deadline_resolver_service_exists_with_launcher_entrypoint():
    cfg = _compose_config()
    services = cfg["services"]
    assert "deadline_resolver" in services, "deadline_resolver missing from compose"
    svc = services["deadline_resolver"]

    # Entrypoint: the dedicated launcher (worker.py exposes no __main__).
    command = svc.get("command")
    assert command == ["python", "scripts/run_deadline_resolver_worker.py"], command
    # And the launcher it points at actually exists in the tree.
    assert (_REPO_ROOT / "scripts" / "run_deadline_resolver_worker.py").exists()

    # Single-instance global scan, health-checked like the other DB-poll workers.
    assert svc.get("container_name") == "company_os_deadline_resolver"
    assert "healthcheck" in svc and svc["healthcheck"].get("test")
