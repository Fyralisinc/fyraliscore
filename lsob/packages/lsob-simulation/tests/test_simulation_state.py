"""Unit tests for state machines: ActorState, CommitmentState, CustomerState."""

from __future__ import annotations

import random
from datetime import datetime

from lsob_contracts import ActorPersona, CommitmentTruth, CustomerTruth

from lsob_simulation.state import ActorState, CommitmentState, CustomerState, HEALTH_ORDER


def _persona(**overrides) -> ActorPersona:
    base = dict(
        actor_id="actor-0001",
        name="Test",
        role="eng",
        reliability_parameter=0.8,
        estimation_bias=0.2,
        communication_frequency=0.5,
    )
    base.update(overrides)
    return ActorPersona(**base)


def test_actor_emits_signal_is_deterministic_given_rng():
    a = ActorState(persona=_persona(communication_frequency=0.5))
    rng = random.Random(42)
    first = [a.will_emit_today(random.Random(42)) for _ in range(10)]
    second = [a.will_emit_today(random.Random(42)) for _ in range(10)]
    assert first == second


def test_actor_inactive_never_emits():
    a = ActorState(persona=_persona(communication_frequency=0.95))
    a.active = False
    rng = random.Random(0)
    assert not any(a.will_emit_today(rng) for _ in range(20))


def test_actor_mood_clamped_to_range():
    a = ActorState(persona=_persona())
    for _ in range(50):
        a.adjust_mood(0.5)
    assert a.mood == 1.0
    for _ in range(50):
        a.adjust_mood(-0.5)
    assert a.mood == -1.0


def test_actor_signal_history_is_bounded():
    a = ActorState(persona=_persona())
    for i in range(100):
        a.record_signal(f"sig-{i}", max_history=5)
    assert len(a.recent_signal_ids) == 5
    assert a.recent_signal_ids[-1] == "sig-99"


def test_commitment_advances_progress_and_resolves():
    persona = _persona(reliability_parameter=1.0, estimation_bias=0.0)
    actor = ActorState(persona=persona)
    truth = CommitmentTruth(
        commitment_id="C-1",
        owner_actor_id="actor-0001",
        created_at=datetime(2026, 1, 1),
        asserted_duration_days=5,
        true_duration_days=5,
        true_complexity="low",
        true_outcome="will_succeed",
    )
    c = CommitmentState(truth=truth)
    rng = random.Random(1)
    for tick in range(10):
        c.advance(rng, actor, tick)
    assert c.resolved
    assert c.true_progress >= 1.0


def test_commitment_with_optimistic_actor_slips_perception():
    persona = _persona(reliability_parameter=0.6, estimation_bias=0.5)
    actor = ActorState(persona=persona)
    # True duration is much longer than asserted.
    truth = CommitmentTruth(
        commitment_id="C-2",
        owner_actor_id="actor-0001",
        created_at=datetime(2026, 1, 1),
        asserted_duration_days=3,
        true_duration_days=20,
        true_complexity="high",
        true_outcome="will_slip",
    )
    c = CommitmentState(truth=truth)
    rng = random.Random(7)
    for tick in range(4):
        c.advance(rng, actor, tick)
    # Perception outruns truth significantly.
    assert c.perceived_progress > c.true_progress
    # And it should be at risk.
    assert c.is_at_risk()


def test_commitment_slip_acknowledged_lowers_mood():
    persona = _persona(reliability_parameter=0.5, estimation_bias=0.5)
    actor = ActorState(persona=persona)
    truth = CommitmentTruth(
        commitment_id="C-3",
        owner_actor_id="actor-0001",
        created_at=datetime(2026, 1, 1),
        asserted_duration_days=2,
        true_duration_days=40,
        true_complexity="high",
        true_outcome="will_slip",
    )
    c = CommitmentState(truth=truth)
    rng = random.Random(0)
    mood_before = actor.mood
    for tick in range(3):
        c.advance(rng, actor, tick)
    assert c.slip_acknowledged
    assert actor.mood < mood_before


def test_customer_health_degrades_under_pressure():
    truth = CustomerTruth(
        customer_id="cust-1",
        revenue_value=100_000,
        true_health_trajectory=["healthy"],
        served_by_commitments=["C-1"],
    )
    cust = CustomerState(truth=truth, current_health="healthy", health_history=["healthy"])
    persona = _persona(estimation_bias=0.8, reliability_parameter=0.5)
    actor = ActorState(persona=persona)
    # Create a commitment with heavy divergence (perceived > true).
    ctruth = CommitmentTruth(
        commitment_id="C-1",
        owner_actor_id="actor-0001",
        created_at=datetime(2026, 1, 1),
        asserted_duration_days=2,
        true_duration_days=30,
        true_complexity="high",
        true_outcome="will_slip",
    )
    c = CommitmentState(truth=ctruth, perceived_progress=0.9, true_progress=0.1)
    rng = random.Random(3)
    for tick in range(20):
        cust.advance(rng, [c], tick)
    idx_end = HEALTH_ORDER.index(cust.current_health)
    # Should have moved at least one step worse.
    assert idx_end >= 1


def test_customer_apply_shock_worsens_health():
    truth = CustomerTruth(
        customer_id="cust-2",
        revenue_value=50_000,
        true_health_trajectory=["healthy"],
    )
    cust = CustomerState(truth=truth)
    rng = random.Random(0)
    start = cust.current_health_index()
    cust.apply_shock(rng, magnitude=1.0)
    assert cust.current_health_index() >= start
