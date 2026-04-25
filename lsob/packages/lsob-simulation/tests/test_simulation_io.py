"""I/O + validator tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from lsob_contracts import (
    PersonalityDistribution,
    SimulationConfig,
)
from lsob_simulation import (
    Simulator,
    read_corpus,
    validate_corpus,
    validate_corpus_file,
    write_corpus,
)


def _mini_config() -> SimulationConfig:
    return SimulationConfig(
        company_id="IOTest",
        num_actors=3,
        actor_personality_distribution=PersonalityDistribution(
            reliable=0.5, optimistic=0.25, pessimistic=0.15, flaky=0.1
        ),
        commitment_generation_rate=0.1,
        customer_count=2,
        seed=7,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_months=1,
    )


def test_write_and_read_roundtrip_json(tmp_path: Path):
    sim = Simulator(_mini_config())
    corpus = sim.run()
    out = tmp_path / "out.json"
    write_corpus(corpus, out)
    back = read_corpus(out)
    assert back.meta.corpus_id == corpus.meta.corpus_id
    assert len(back.signals) == len(corpus.signals)
    assert len(back.ground_truth) == len(corpus.ground_truth)


def test_write_and_read_roundtrip_jsonl_zst(tmp_path: Path):
    sim = Simulator(_mini_config())
    corpus = sim.run()
    out = tmp_path / "out.jsonl.zst"
    write_corpus(corpus, out)
    assert out.exists()
    # File should be binary (zstd compressed) — not readable as JSON.
    raw = out.read_bytes()
    assert len(raw) > 0
    back = read_corpus(out)
    assert back.meta.corpus_id == corpus.meta.corpus_id
    assert len(back.signals) == len(corpus.signals)
    assert len(back.ground_truth) == len(corpus.ground_truth)
    # Content text must match.
    assert back.signals[0].content_text == corpus.signals[0].content_text


def test_validator_on_generated_corpus(tmp_path: Path):
    sim = Simulator(_mini_config())
    corpus = sim.run()
    p = tmp_path / "corpus.json"
    write_corpus(corpus, p)
    report = validate_corpus_file(p)
    assert report.ok, f"{report.errors}"
    assert report.signal_count == len(corpus.signals)


def test_validator_flags_unknown_actor_reference(tmp_path: Path):
    sim = Simulator(_mini_config())
    corpus = sim.run()
    # Corrupt: set one signal to an unknown author.
    corpus.signals[0].author_id = "ghost-actor"
    report = validate_corpus(corpus)
    assert not report.ok
    assert any("ghost-actor" in e for e in report.errors)


def test_validator_rejects_unsupported_suffix(tmp_path: Path):
    # Create a file with an unsupported extension.
    bad = tmp_path / "corpus.xml"
    bad.write_text("<corpus/>")
    with pytest.raises(ValueError):
        read_corpus(bad)
