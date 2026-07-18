from pathlib import Path
import subprocess

import pytest

from lib.contracts.kernel import canonical_sha256
from scripts.run_epistemic_repair_p8_coherent_rerun import execute_plan, resolve_command


class _Lock:
    def __init__(self, acquired=True):
        self.acquired = acquired
        self.calls = []

    async def fetchval(self, sql, key):
        self.calls.append((sql, key))
        return self.acquired if "try_advisory" in sql else True


def _repository(tmp_path: Path) -> tuple[Path, str]:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "p8@example.test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "P8"], cwd=tmp_path, check=True)
    (tmp_path / "tracked").write_text("v1")
    subprocess.run(["git", "add", "tracked"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "baseline"], cwd=tmp_path, check=True)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    return tmp_path, head


def _plan(repository, head, commands):
    body = {"repository": str(repository), "commit_sha": head, "commands": commands}
    return {**body, "plan_digest": canonical_sha256(body)}


def test_resolver_only_accepts_whole_tokens() -> None:
    assert resolve_command(["x", "${P8_DATABASE_URL}"], {"P8_DATABASE_URL": "postgres://safe"})[-1] == "postgres://safe"
    with pytest.raises(RuntimeError, match="embedded"):
        resolve_command(["--dsn=${P8_DATABASE_URL}"], {"P8_DATABASE_URL": "postgres://safe"})


@pytest.mark.asyncio
async def test_coordinator_locks_rechecks_and_stops_on_first_failure(tmp_path: Path) -> None:
    repository, head = _repository(tmp_path)
    commands_seen = []

    async def runner(command, cwd):
        commands_seen.append(command)
        return 7 if command[0] == "fail" else 0

    lock = _Lock()
    with pytest.raises(RuntimeError, match="stage 2"):
        await execute_plan(
            _plan(repository, head, [["ok", "${P8_DATABASE_URL}"], ["fail"], ["never"]]),
            environment={"P8_DATABASE_URL": "postgres://safe"}, lock_connection=lock, runner=runner,
        )
    assert commands_seen == [["ok", "postgres://safe"], ["fail"]]
    assert "try_advisory_lock" in lock.calls[0][0]
    assert "advisory_unlock" in lock.calls[-1][0]


@pytest.mark.asyncio
async def test_coordinator_refuses_busy_database_lock(tmp_path: Path) -> None:
    repository, head = _repository(tmp_path)
    with pytest.raises(RuntimeError, match="holds the advisory lock"):
        await execute_plan(
            _plan(repository, head, [["never"]]), environment={},
            lock_connection=_Lock(acquired=False), runner=lambda *_: None,
        )
