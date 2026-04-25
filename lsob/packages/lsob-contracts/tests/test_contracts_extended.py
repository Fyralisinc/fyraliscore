from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from lsob_contracts import (
    ActOp,
    ActorPersona,
    AtRiskItem,
    AtRiskReport,
    Belief,
    BeliefQuery,
    ClaimOp,
    CommitmentTruth,
    Corpus,
    CorpusMeta,
    CustomerTruth,
    DiffOp,
    EntityRef,
    EvaluationContext,
    PatternTruth,
    PersonalityDistribution,
    ResourceOp,
    SimulationConfig,
    TurbulenceEvent,
    TurbulenceKind,
)
from lsob_contracts.protocols import Baseline, Evaluator, SystemUnderTest

TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_personality_distribution_sums():
    d = PersonalityDistribution()
    d.validate_sum()
    bad = PersonalityDistribution(reliable=0.1, optimistic=0.1, pessimistic=0.1, flaky=0.1)
    with pytest.raises(ValueError):
        bad.validate_sum()


def test_turbulence_event_validated():
    TurbulenceEvent(event_id="t1", kind=TurbulenceKind.pivot, scheduled_at=TS)
    with pytest.raises(ValidationError):
        TurbulenceEvent(event_id="t1", kind="alien-invasion", scheduled_at=TS)


def test_simulation_config_duration_bounds():
    SimulationConfig(company_id="A", num_actors=1, start_date=TS, duration_months=12)
    with pytest.raises(ValidationError):
        SimulationConfig(company_id="A", num_actors=1, start_date=TS, duration_months=0)
    with pytest.raises(ValidationError):
        SimulationConfig(company_id="A", num_actors=1, start_date=TS, duration_months=100)


def test_actor_persona_bounds():
    ActorPersona(
        actor_id="a1",
        name="Alice",
        role="eng",
        reliability_parameter=0.5,
    )
    with pytest.raises(ValidationError):
        ActorPersona(
            actor_id="a1",
            name="Alice",
            role="eng",
            reliability_parameter=1.5,
        )


def test_commitment_truth_outcome_validated():
    CommitmentTruth(
        commitment_id="c1",
        owner_actor_id="a1",
        created_at=TS,
        asserted_duration_days=5,
        true_duration_days=10,
        true_complexity="high",
        true_outcome="will_slip",
    )
    with pytest.raises(ValidationError):
        CommitmentTruth(
            commitment_id="c1",
            owner_actor_id="a1",
            created_at=TS,
            asserted_duration_days=5,
            true_duration_days=10,
            true_complexity="high",
            true_outcome="exploded",
        )


def test_customer_truth_health_validated():
    CustomerTruth(
        customer_id="acme",
        revenue_value=1000.0,
        true_health_trajectory=["healthy", "warning", "critical"],
    )
    with pytest.raises(ValidationError):
        CustomerTruth(
            customer_id="acme",
            revenue_value=1000.0,
            true_health_trajectory=["mega-healthy"],
        )


def test_pattern_truth_defaults():
    p = PatternTruth(
        pattern_id="p1",
        description="x",
        emergence_at=TS,
        detection_eligible_after=TS + timedelta(days=7),
    )
    assert p.scope == {}


def test_diff_op_assembly():
    diff = DiffOp(
        diff_id="d1",
        produced_at=TS,
        claim_ops=[
            ClaimOp(claim_id="c1", proposition="X slips", proposition_kind="prediction", asserted_confidence=0.7)
        ],
        act_ops=[
            ActOp(entity_ref="C-ingest", from_state="on_track", to_state="at_risk")
        ],
        resource_ops=[ResourceOp(op="allocate", resource_ref="alice", target_ref="C-ingest", amount=0.5)],
    )
    assert len(diff.claim_ops) == 1
    assert diff.act_ops[0].to_state == "at_risk"


def test_claim_op_confidence_bounds():
    with pytest.raises(ValidationError):
        ClaimOp(claim_id="c1", proposition="x", proposition_kind="y", asserted_confidence=1.2)


def test_belief_query_defaults():
    q = BeliefQuery(
        query_id="q1",
        entity_ref=EntityRef(kind="commitment", id="c1"),
        timestamp=TS,
    )
    assert q.k == 10


def test_at_risk_report_empty():
    r = AtRiskReport(timestamp=TS)
    assert r.items == []


def test_at_risk_item_score_bounds():
    with pytest.raises(ValidationError):
        AtRiskItem(
            entity_ref=EntityRef(kind="commitment", id="c1"),
            risk_score=1.2,
            risk_kind="slip",
        )


def test_evaluation_context_holds_sut_reference():
    meta = CorpusMeta(
        corpus_id="c",
        company_id="A",
        months_simulated=1,
        seed=1,
        config_hash="h",
        start_date=TS,
        end_date=TS,
    )
    ctx = EvaluationContext(
        corpus=Corpus(meta=meta, signals=[], ground_truth=[]),
        sut=object(),
        ground_truth_checkpoint=TS,
        run_id="r1",
    )
    assert ctx.run_id == "r1"


def test_belief_model():
    b = Belief(
        claim_id="c1",
        proposition="x",
        proposition_kind="state",
        asserted_confidence=0.5,
        last_updated=TS,
    )
    assert b.entities == []


def test_protocol_runtime_checks():
    class FakeSUT:
        name = "fake"
        max_concurrent_ingestion = 1

        async def startup(self, config): ...
        async def apply_ablation(self, a): ...
        async def ingest_signal(self, s): ...
        async def query_beliefs_at(self, q): return []
        async def query_at_risk_at(self, t): return AtRiskReport(timestamp=t)
        async def produce_diff_for_trigger(self, t): return DiffOp(diff_id="d", produced_at=t.timestamp)
        async def shutdown(self): ...

    assert isinstance(FakeSUT(), SystemUnderTest)

    class FakeEvaluator:
        layer_id = 1
        metric_names = ["m"]
        async def evaluate(self, ctx): return []

    assert isinstance(FakeEvaluator(), Evaluator)

    class FakeBaseline:
        name = "b"
        def construct_sut(self, cfg): return FakeSUT()

    assert isinstance(FakeBaseline(), Baseline)


def test_fixture_corpora_load():
    """The three hand-written fixtures must validate against the current contracts."""
    import json
    from pathlib import Path

    fixtures_dir = Path(__file__).resolve().parents[3] / "fixtures"
    assert fixtures_dir.is_dir()
    seen = 0
    for name in ("mini_corpus_a.json", "mini_corpus_b.json", "mini_corpus_c.json"):
        path = fixtures_dir / name
        data = json.loads(path.read_text())
        corpus = Corpus.model_validate(data)
        assert len(corpus.signals) == 10
        assert len(corpus.ground_truth) >= 1
        seen += 1
    assert seen == 3
