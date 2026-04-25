"""Simulator-level tests: run mini configs, determinism, turbulence, configs parse."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import yaml

from lsob_contracts import (
    PersonalityDistribution,
    SimulationConfig,
    TurbulenceEvent,
    TurbulenceKind,
)
from lsob_simulation import (
    Simulator,
    TemplateSignalGenerator,
    load_config,
    validate_corpus,
)

CONFIGS_DIR = Path(__file__).resolve().parents[1] / "configs"


def _mini_config(**overrides) -> SimulationConfig:
    base = dict(
        company_id="MiniTest",
        num_actors=3,
        actor_personality_distribution=PersonalityDistribution(
            reliable=0.5, optimistic=0.25, pessimistic=0.15, flaky=0.1
        ),
        commitment_generation_rate=0.2,
        customer_count=2,
        turbulence_events=[],
        seed=1234,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_months=1,
    )
    base.update(overrides)
    return SimulationConfig(**base)


def test_mini_corpus_generates_and_validates():
    cfg = _mini_config()
    # 5 ticks = ~5 days → shrink duration by using 1 month but we only care that it runs fast.
    sim = Simulator(cfg, signal_generator=TemplateSignalGenerator())
    corpus = sim.run()
    assert corpus.meta.company_id == "MiniTest"
    # At least some signals should have been produced.
    assert len(corpus.signals) > 0
    # Monthly ground truth snapshots.
    assert len(corpus.ground_truth) >= 1
    report = validate_corpus(corpus)
    assert report.ok, f"validation failed: {report.errors}"


def test_simulator_is_deterministic_under_same_seed():
    cfg = _mini_config(seed=99)
    a = Simulator(cfg).run()
    b = Simulator(cfg).run()
    assert len(a.signals) == len(b.signals)
    # Signal IDs are deterministic counter-based; full text should match too.
    for s1, s2 in zip(a.signals, b.signals):
        assert s1.signal_id == s2.signal_id
        assert s1.author_id == s2.author_id
        assert s1.content_text == s2.content_text
        assert s1.timestamp == s2.timestamp


def test_different_seeds_produce_different_corpora():
    a = Simulator(_mini_config(seed=1)).run()
    b = Simulator(_mini_config(seed=2)).run()
    # Very unlikely to be identical.
    assert (len(a.signals), len(b.signals)) and (a.signals != b.signals)


def test_turbulence_reorg_applied_at_month_6_parses():
    # Smoke test that a config with a scheduled reorg event runs to completion.
    cfg = _mini_config(
        duration_months=7,
        num_actors=4,
        turbulence_events=[
            TurbulenceEvent(
                event_id="reorg-x",
                kind=TurbulenceKind.reorg,
                scheduled_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
                magnitude=0.5,
            )
        ],
    )
    sim = Simulator(cfg)
    corpus = sim.run()
    assert corpus.meta.months_simulated == 7
    # Ground truth count matches duration.
    assert len(corpus.ground_truth) == 7


def test_three_preset_configs_parse():
    for name in ("CompanyA.yaml", "CompanyB.yaml", "CompanyC.yaml"):
        p = CONFIGS_DIR / name
        raw = yaml.safe_load(p.read_text())
        cfg = SimulationConfig.model_validate(raw)
        assert cfg.company_id == name.replace(".yaml", "")

    # CompanyC has a reorg at month 6.
    c = load_config(CONFIGS_DIR / "CompanyC.yaml")
    assert any(e.kind == TurbulenceKind.reorg for e in c.turbulence_events)
