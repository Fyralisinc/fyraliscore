"""Persistent flake-rate tracker for real-LLM tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
_FLAKE_FILE = _REPORTS_DIR / "flake_rates.json"
_MAX_RUNS_PER_TEST = 50

_pending_attempts: dict[str, list[dict]] = {}


def _ensure_reports_dir() -> None:
    """Create the reports directory if it does not exist."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _load() -> dict:
    """Load the flake-rates JSON file, returning {} if missing or unreadable."""
    if not _FLAKE_FILE.exists():
        return {}
    try:
        return json.loads(_FLAKE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save(data: dict) -> None:
    """Persist the flake-rates JSON file."""
    _ensure_reports_dir()
    _FLAKE_FILE.write_text(json.dumps(data, indent=2, sort_keys=True))


def record_attempt(test_name: str, attempt: int, status: str, error: str | None = None) -> None:
    """Buffer one attempt's outcome for the given test until record_final flushes it."""
    entry = {
        "attempt": attempt,
        "status": status,
        "error": error,
    }
    _pending_attempts.setdefault(test_name, []).append(entry)


def record_final(test_name: str, passes: int, total: int, threshold: int) -> None:
    """Persist the final outcome for a test, including all buffered attempts."""
    attempts = _pending_attempts.pop(test_name, [])
    outcome = "pass" if passes >= threshold else "fail"
    run_record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "passes": passes,
        "total": total,
        "threshold": threshold,
        "outcome": outcome,
        "attempts": attempts,
    }
    data = _load()
    test_entry = data.setdefault(test_name, {"runs": []})
    runs = test_entry.setdefault("runs", [])
    runs.append(run_record)
    if len(runs) > _MAX_RUNS_PER_TEST:
        del runs[: len(runs) - _MAX_RUNS_PER_TEST]
    _save(data)


def summary() -> dict:
    """Return per-test flake-rate stats and recent outcomes from persisted history."""
    data = _load()
    out: dict[str, dict] = {}
    for test_name, entry in data.items():
        runs = entry.get("runs", [])
        if not runs:
            out[test_name] = {"flake_rate": 0.0, "recent_outcomes": []}
            continue
        flaky = sum(1 for r in runs if r.get("passes", 0) < r.get("total", 0))
        out[test_name] = {
            "flake_rate": flaky / len(runs),
            "recent_outcomes": [r.get("outcome") for r in runs[-10:]],
        }
    return out
