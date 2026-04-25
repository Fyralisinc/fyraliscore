"""Shared pytest fixtures for the harness tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from lsob_contracts import AblationConfig, EvalResult, RunManifest


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = WORKSPACE_ROOT / "fixtures"


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture()
def mini_corpus_a(fixtures_dir: Path) -> Path:
    return fixtures_dir / "mini_corpus_a.json"


@pytest.fixture()
def mini_corpus_b(fixtures_dir: Path) -> Path:
    return fixtures_dir / "mini_corpus_b.json"


@pytest.fixture()
def sample_manifest() -> RunManifest:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return RunManifest(
        run_id="sut-mini-a-none-20260101T000000-nogit",
        company="MiniA",
        months_simulated=1,
        baseline="mock",
        ablation=AblationConfig(name="none"),
        seed=42,
        git_sha="nogit",
        started_at=now,
        finished_at=now,
        corpus_uri="fixtures/mini_corpus_a.json",
        layers=[1, 2, 3, 4, 5, 6],
    )


@pytest.fixture()
def sample_eval_results() -> list[EvalResult]:
    return [
        EvalResult(
            layer_id=1,
            metric_name="L1.noop",
            value=1.0,
            confidence_interval=(0.8, 1.2),
            breakdown_by={"checkpoint": "2026-01-31"},
        ),
        EvalResult(
            layer_id=3,
            metric_name="L3.noop",
            value=3.0,
            breakdown_by={},
        ),
    ]
