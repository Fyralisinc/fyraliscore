from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path("scripts/run_epistemic_repair_p7_production.py").resolve()
SPEC = importlib.util.spec_from_file_location("p7_run_script", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_p7_lock_is_exclusive_and_released(tmp_path: Path) -> None:
    lock = tmp_path / "p7.lock"
    with MODULE._exclusive_run_lock(lock):
        assert lock.exists()
        with pytest.raises(SystemExit, match="already exists"):
            with MODULE._exclusive_run_lock(lock):
                pass
    assert not lock.exists()


def test_p7_preflight_rejects_non_cli_transport(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(MODULE.subprocess, "check_output", lambda command, **_: (
        "a" * 40 if command[1] == "rev-parse" else ""
    ))
    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_TRANSPORT", "app_server")
    with pytest.raises(RuntimeError, match="requires exact environment"):
        MODULE._clean_cli_provenance(tmp_path)


def test_p7_preflight_rejects_dirty_worktree(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(MODULE.subprocess, "check_output", lambda command, **_: (
        "a" * 40 if command[1] == "rev-parse" else " M dirty.py"
    ))
    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_TRANSPORT", "cli")
    with pytest.raises(SystemExit, match="isolated clean worktree"):
        MODULE._clean_cli_provenance(tmp_path)


def test_p7_partial_artifacts_require_restart_from_zero(tmp_path: Path) -> None:
    output = tmp_path / "execution.json"
    scores = tmp_path / "scores.json"
    MODULE._require_restart_from_zero(output, scores)
    output.write_text("partial")
    with pytest.raises(SystemExit, match="does not resume partial executions"):
        MODULE._require_restart_from_zero(output, scores)
