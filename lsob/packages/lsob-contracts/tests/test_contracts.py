from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from lsob_contracts import (
    AblationConfig,
    Corpus,
    CorpusMeta,
    EntityRef,
    EvalResult,
    GroundTruth,
    RunManifest,
    Signal,
    SUTConfig,
    Trigger,
)
from lsob_contracts.models import SourceChannel


TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_signal_valid():
    s = Signal(
        signal_id="s1",
        source_channel=SourceChannel.slack,
        author_id="alice",
        content_text="deployed",
        timestamp=TS,
    )
    assert s.metadata == {}


def test_signal_rejects_extra():
    with pytest.raises(ValidationError):
        Signal(
            signal_id="s1",
            source_channel="slack",
            author_id="alice",
            content_text="x",
            timestamp=TS,
            hacker="true",
        )


def test_corpus_roundtrip():
    meta = CorpusMeta(
        corpus_id="c1",
        company_id="A",
        months_simulated=1,
        seed=42,
        config_hash="abc",
        start_date=TS,
        end_date=TS,
    )
    gt = GroundTruth(timestamp=TS)
    c = Corpus(meta=meta, signals=[], ground_truth=[gt])
    dumped = c.model_dump_json()
    parsed = Corpus.model_validate_json(dumped)
    assert parsed.meta.seed == 42
    assert parsed.ground_truth[0].timestamp == TS


def test_eval_result_ci_optional():
    r = EvalResult(layer_id=1, metric_name="recall@10", value=0.72)
    assert r.confidence_interval is None
    r2 = EvalResult(
        layer_id=1,
        metric_name="recall@10",
        value=0.72,
        confidence_interval=(0.68, 0.76),
    )
    assert r2.confidence_interval == (0.68, 0.76)


def test_ablation_any_disabled():
    a = AblationConfig()
    assert not a.any_disabled()
    b = AblationConfig(name="no-bridge", disable_bridge=True)
    assert b.any_disabled()


def test_run_manifest_requires_corpus_uri():
    rm = RunManifest(
        run_id="r1",
        company="A",
        months_simulated=12,
        baseline="company-os",
        ablation=AblationConfig(),
        seed=42,
        git_sha="deadbeef",
        started_at=TS,
        corpus_uri="corpus://A-v1",
        layers=[1, 2, 3, 4, 5, 6],
    )
    assert rm.finished_at is None


def test_entity_ref_kind_validated():
    EntityRef(kind="commitment", id="c1")
    with pytest.raises(ValidationError):
        EntityRef(kind="unknown", id="c1")


def test_sut_config_params_default():
    c = SUTConfig(sut_name="vanilla-rag")
    assert c.params == {}


def test_trigger_payload_default():
    t = Trigger(trigger_id="t1", kind="commitment_slip_risk", timestamp=TS)
    assert t.payload == {}
