#!/usr/bin/env python3
"""Execute a prepared P8 plan under process and database-wide exclusive locks."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Awaitable, Callable

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.contracts.kernel import canonical_sha256


_ENV_TOKEN = re.compile(r"^\$\{([A-Z][A-Z0-9_]*)\}$")
_ADVISORY_LOCK_ID = 0x50385F52554E  # ASCII-like stable namespace: P8_RUN


def verify_repository(repository: Path, expected_head: str) -> None:
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    if head != expected_head:
        raise RuntimeError(f"P8 HEAD changed: expected {expected_head}, observed {head}")
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=repository, text=True,
    ).strip()
    if dirty:
        raise RuntimeError("P8 tracked worktree changed after plan preparation")


def resolve_command(command: list[str], environment: dict[str, str]) -> list[str]:
    """Resolve whole-token variables only; never invoke a shell."""
    resolved = []
    for token in command:
        match = _ENV_TOKEN.fullmatch(token)
        if match:
            name = match.group(1)
            value = environment.get(name)
            if not value:
                raise RuntimeError(f"required environment variable {name} is unset")
            resolved.append(value)
        elif "${" in token:
            raise RuntimeError(f"embedded environment substitution is forbidden: {token}")
        else:
            resolved.append(token)
    return resolved


async def _subprocess_runner(command: list[str], cwd: Path) -> int:
    process = await asyncio.create_subprocess_exec(*command, cwd=cwd)
    return await process.wait()


async def execute_plan(
    plan: dict[str, object], *, environment: dict[str, str],
    lock_connection, runner: Callable[[list[str], Path], Awaitable[int]] = _subprocess_runner,
) -> None:
    digest = plan.get("plan_digest")
    body = dict(plan)
    body.pop("plan_digest", None)
    if digest != canonical_sha256(body):
        raise RuntimeError("P8 rerun plan digest mismatch")
    repository = Path(str(plan["repository"]))
    expected_head = str(plan["commit_sha"])
    effective_env = dict(environment)
    effective_env["P8_EXPECTED_HEAD"] = expected_head
    acquired = await lock_connection.fetchval("SELECT pg_try_advisory_lock($1)", _ADVISORY_LOCK_ID)
    if acquired is not True:
        raise RuntimeError("another P8/database evaluator holds the advisory lock")
    try:
        for ordinal, raw in enumerate(plan["commands"], 1):
            verify_repository(repository, expected_head)
            command = resolve_command(list(raw), effective_env)
            code = await runner(command, repository)
            if code != 0:
                raise RuntimeError(f"P8 stage {ordinal} failed with exit code {code}")
            verify_repository(repository, expected_head)
    finally:
        await lock_connection.fetchval("SELECT pg_advisory_unlock($1)", _ADVISORY_LOCK_ID)


async def _main_async(args: argparse.Namespace) -> int:
    plan = json.loads(args.plan.read_text())
    database_url = os.environ.get("P8_DATABASE_URL")
    if not database_url:
        raise SystemExit("P8_DATABASE_URL is required")
    args.lock_file.parent.mkdir(parents=True, exist_ok=True)
    with args.lock_file.open("a+") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SystemExit("another local P8 coordinator holds the OS lock") from exc
        connection = await asyncpg.connect(database_url)
        try:
            await execute_plan(plan, environment=dict(os.environ), lock_connection=connection)
        finally:
            await connection.close()
    print(f"p8_coherent_rerun_complete=true commit_sha={plan['commit_sha']}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--lock-file", type=Path, default=Path("/tmp/fyralis-p8-coherent-rerun.lock"))
    return asyncio.run(_main_async(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
