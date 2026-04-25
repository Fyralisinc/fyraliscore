"""Invoke ``bench_ingest.py`` as a subprocess and assert it exits cleanly."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = (
    WORKSPACE_ROOT / "packages" / "lsob-harness" / "scripts" / "bench_ingest.py"
)


def test_bench_ingest_script_runs() -> None:
    assert SCRIPT.exists(), f"missing benchmark script at {SCRIPT}"
    # Run with the current interpreter (the test-runner's venv), not `uv run`,
    # so the test stays hermetic to the same environment that executed pytest.
    proc = subprocess.run(
        [sys.executable, str(SCRIPT)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, (
        f"bench_ingest exit={proc.returncode}\n"
        f"--stdout--\n{proc.stdout}\n"
        f"--stderr--\n{proc.stderr}\n"
    )
    # Output should include the header and a speedup row.
    assert "impl" in proc.stdout
    assert "speedup" in proc.stdout
    assert "ParallelIngester" in proc.stdout
