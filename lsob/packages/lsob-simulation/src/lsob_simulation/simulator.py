"""Tick-based deterministic simulator producing Corpus objects."""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

from lsob_contracts import (
    ActorPersona,
    ActorProfile,
    CommitmentSeed,
    CommitmentTruth,
    Corpus,
    CorpusMeta,
    CustomerProfile,
    CustomerTruth,
    SimulationConfig,
    SourceChannel,
    TurbulenceEvent,
    TurbulenceKind,
)

from lsob_simulation.ground_truth import GroundTruthRecorder, PatternTruthEntry
from lsob_simulation.signal_gen import SignalGenerator, TemplateSignalGenerator
from lsob_simulation.state import ActorState, CommitmentState, CustomerState


_ROLE_CYCLE = [
    "senior-eng",
    "eng",
    "pm",
    "cs-lead",
    "sales",
    "designer",
    "sre",
    "data",
    "exec",
]

_PERSONA_KEY_PARAMS = {
    # (reliability, bias, comms)
    "reliable": (0.88, 0.05, 0.55),
    "optimistic": (0.7, 0.45, 0.65),
    "pessimistic": (0.82, -0.35, 0.45),
    "flaky": (0.55, 0.1, 0.7),
}


def _persona_params_for_profile(profile: ActorProfile) -> tuple[float, float, float]:
    """Resolve (reliability, bias, comms) for a profile, using explicit
    overrides where provided and falling back to the persona-class defaults."""
    base_rel, base_bias, base_comm = _PERSONA_KEY_PARAMS[profile.persona_kind]
    rel = profile.reliability_parameter if profile.reliability_parameter is not None else base_rel
    bias = profile.estimation_bias if profile.estimation_bias is not None else base_bias
    comm = profile.communication_frequency if profile.communication_frequency is not None else base_comm
    return rel, bias, comm


@dataclass
class _TurbulenceApplication:
    event: TurbulenceEvent
    applied_tick: int


class Simulator:
    """Runs the tick-based simulation and produces a Corpus.

    Responsibilities:
      - advance commitment / customer / actor state per tick
      - emit signals through the SignalGenerator
      - inject turbulence events on their scheduled days
      - record monthly ground truth snapshots
    """

    def __init__(
        self,
        config: SimulationConfig,
        *,
        signal_generator: SignalGenerator | None = None,
    ) -> None:
        self.config = config
        self.signal_generator: SignalGenerator = signal_generator or TemplateSignalGenerator()
        self._actor_states: list[ActorState] = []
        self._commitment_states: list[CommitmentState] = []
        self._customer_states: list[CustomerState] = []
        self._patterns: list[PatternTruthEntry] = []
        self._applied_turbulence: list[_TurbulenceApplication] = []
        self._signals: list = []  # list[Signal]
        self._signal_counter = 0
        self._commitment_counter = 0
        self._pending_seeds: list[CommitmentSeed] = []
        self._tick_signal_callback = None  # set by sharded runner; (tick, signals_for_tick) -> None
        self._gt_recorder = GroundTruthRecorder(
            start_date=config.start_date, duration_months=config.duration_months
        )

    # ----------------- Public API -----------------

    def run(self) -> Corpus:
        config = self.config
        config.actor_personality_distribution.validate_sum()

        base_rng = random.Random(config.seed)
        self._bootstrap_actors(base_rng)
        self._bootstrap_customers(base_rng)
        self._seed_initial_commitments(base_rng)
        self._seed_patterns()

        total_ticks = config.duration_months * 30
        for tick in range(total_ticks):
            rng = random.Random(config.seed + tick + 1)
            current_date = config.start_date + timedelta(days=tick)
            self._apply_turbulence(rng, current_date, tick)
            self._materialize_due_seeds(rng, tick)
            self._advance_commitments(rng, tick)
            self._advance_customers(rng, tick)
            signals_before = len(self._signals)
            gt_before = len(self._gt_recorder.snapshots)
            self._generate_signals(rng, current_date, tick)
            self._maybe_create_new_commitments(rng, current_date, tick)
            self._maybe_emit_ground_truth(current_date)
            if self._tick_signal_callback is not None:
                new_signals = self._signals[signals_before:]
                new_gt = self._gt_recorder.snapshots[gt_before:]
                self._tick_signal_callback(self, tick, current_date, new_signals, new_gt)

        # Force a final ground-truth at end if we haven't hit the last checkpoint.
        end_date = config.start_date + timedelta(days=total_ticks)
        if len(self._gt_recorder.snapshots) < config.duration_months:
            self._emit_ground_truth(end_date)

        meta = self._build_meta(end_date)
        corpus = Corpus(meta=meta, signals=list(self._signals), ground_truth=list(self._gt_recorder.snapshots))
        return corpus

    # ----------------- Bootstrapping -----------------

    def _bootstrap_actors(self, rng: random.Random) -> None:
        # Rich-profile path — when actor_profiles is non-empty, build directly
        # from the named profiles (deterministic; no RNG draws). Backwards-
        # compatible: when actor_profiles is empty, the original generic path
        # below runs unchanged so CompanyA/B/C corpora stay byte-identical.
        profiles = self.config.actor_profiles
        if profiles:
            for profile in profiles:
                rel, bias, comm = _persona_params_for_profile(profile)
                persona = ActorPersona(
                    actor_id=profile.actor_id,
                    name=profile.name,
                    role=profile.role,
                    reliability_parameter=max(0.0, min(1.0, rel)),
                    estimation_bias=max(-1.0, min(1.0, bias)),
                    communication_frequency=max(0.0, min(1.0, comm)),
                    reactive_to_patterns=[],
                    role_family=profile.role_family,
                    manager_id=profile.manager_id,
                    email=profile.email,
                    brief=profile.brief,
                )
                self._actor_states.append(ActorState(persona=persona))
            return

        dist = self.config.actor_personality_distribution
        # Distribute personalities across actors deterministically.
        N = self.config.num_actors
        counts = {
            "reliable": round(dist.reliable * N),
            "optimistic": round(dist.optimistic * N),
            "pessimistic": round(dist.pessimistic * N),
            "flaky": round(dist.flaky * N),
        }
        # Pad / trim to hit exactly N.
        total = sum(counts.values())
        if total < N:
            counts["reliable"] += N - total
        elif total > N:
            # Shrink reliable first.
            counts["reliable"] = max(0, counts["reliable"] - (total - N))
        assigned = 0
        for ptype, count in counts.items():
            base_rel, base_bias, base_comm = _PERSONA_KEY_PARAMS[ptype]
            for _ in range(count):
                idx = assigned
                actor_id = f"actor-{idx:04d}"
                role = _ROLE_CYCLE[idx % len(_ROLE_CYCLE)]
                persona = ActorPersona(
                    actor_id=actor_id,
                    name=f"{ptype.title()} {idx}",
                    role=role,
                    reliability_parameter=round(base_rel + (rng.random() - 0.5) * 0.1, 3),
                    estimation_bias=round(base_bias + (rng.random() - 0.5) * 0.1, 3),
                    communication_frequency=round(base_comm + (rng.random() - 0.5) * 0.15, 3),
                    reactive_to_patterns=[],
                )
                # Clamp into valid ranges.
                persona.reliability_parameter = max(0.0, min(1.0, persona.reliability_parameter))
                persona.estimation_bias = max(-1.0, min(1.0, persona.estimation_bias))
                persona.communication_frequency = max(0.0, min(1.0, persona.communication_frequency))
                self._actor_states.append(ActorState(persona=persona))
                assigned += 1

    def _bootstrap_customers(self, rng: random.Random) -> None:
        # Rich-profile path — backwards compatible: empty profiles → original
        # auto-generation runs unchanged.
        profiles = self.config.customer_profiles
        if profiles:
            for p in profiles:
                self._customer_states.append(
                    CustomerState(
                        truth=CustomerTruth(
                            customer_id=p.customer_id,
                            revenue_value=p.arr_usd,
                            true_health_trajectory=[p.initial_health],
                            served_by_commitments=[],
                            company_name=p.company_name,
                            segment=p.segment,
                        ),
                        current_health=p.initial_health,
                        health_history=[p.initial_health],
                    )
                )
            return

        for i in range(self.config.customer_count):
            cid = f"cust-{i:04d}"
            self._customer_states.append(
                CustomerState(
                    truth=CustomerTruth(
                        customer_id=cid,
                        revenue_value=round(50_000 + rng.random() * 500_000, 2),
                        true_health_trajectory=["healthy"],
                        served_by_commitments=[],
                    ),
                    current_health="healthy",
                    health_history=["healthy"],
                )
            )

    def _next_commitment_id(self) -> str:
        cid = f"C-{self._commitment_counter:05d}"
        self._commitment_counter += 1
        return cid

    def _seed_initial_commitments(self, rng: random.Random) -> None:
        # Rich-profile path: stash all commitment_seeds for the per-tick loop
        # to materialize at their scheduled created_offset_days. We skip the
        # default per-actor stub seeding so the spec author has full control
        # over which commitments exist.
        if self.config.commitment_seeds:
            self._pending_seeds = list(self.config.commitment_seeds)
            return

        # Seed 1 commitment per ~2 actors at tick 0 to give the sim some state.
        for i, actor in enumerate(self._actor_states):
            if i % 2 != 0:
                continue
            self._create_commitment(rng, actor, self.config.start_date, tick=0)

    def _materialize_due_seeds(self, rng: random.Random, tick: int) -> None:
        """Spawn any commitment_seeds whose created_offset_days == tick."""
        if not getattr(self, "_pending_seeds", None):
            return
        actor_index = {a.persona.actor_id: a for a in self._actor_states}
        remaining: list[CommitmentSeed] = []
        for seed in self._pending_seeds:
            if seed.created_offset_days != tick:
                remaining.append(seed)
                continue
            owner = actor_index.get(seed.owner_actor_id)
            if owner is None:
                # Owner not present (typo or post-departure assignment) — drop.
                continue
            self._create_commitment_from_seed(rng, owner, seed, tick=tick)
        self._pending_seeds = remaining

    def _create_commitment_from_seed(
        self,
        rng: random.Random,
        actor: ActorState,
        seed: CommitmentSeed,
        tick: int,
    ) -> CommitmentState:
        complexity = seed.true_complexity
        complexity_multiplier = {"low": 0.9, "med": 1.3, "high": 2.1}[complexity]
        bias_adjust = 1.0 - (actor.persona.estimation_bias * 0.4)
        true_duration = max(
            1, int(round(seed.asserted_duration_days * complexity_multiplier * bias_adjust))
        )
        if seed.intended_outcome is not None:
            outcome = seed.intended_outcome
        else:
            roll = rng.random()
            if roll < 0.1 + (1 - actor.persona.reliability_parameter) * 0.2 and complexity != "low":
                outcome = "will_slip"
            elif roll < 0.15:
                outcome = "will_be_cancelled"
            else:
                outcome = "will_succeed"
        when = self.config.start_date + timedelta(days=tick)
        truth = CommitmentTruth(
            commitment_id=seed.commitment_id,
            owner_actor_id=actor.persona.actor_id,
            created_at=when,
            asserted_duration_days=seed.asserted_duration_days,
            true_duration_days=true_duration,
            true_complexity=complexity,
            true_outcome=outcome,
            resolution_event_at=None,
            hidden_dependencies=[],
            title=seed.title,
            customer_id=seed.customer_id,
            goal_id=seed.goal_id,
        )
        c = CommitmentState(truth=truth, created_tick=tick)
        self._commitment_states.append(c)
        actor.current_workload += 1
        # Link to named customer when given; otherwise round-robin like the
        # generic path so signals still flow to a CS/sales actor.
        if seed.customer_id:
            target = next(
                (cs for cs in self._customer_states if cs.truth.customer_id == seed.customer_id),
                None,
            )
            if target is not None:
                target.truth.served_by_commitments.append(truth.commitment_id)
        elif self._customer_states:
            target = self._customer_states[
                len(self._commitment_states) % len(self._customer_states)
            ]
            target.truth.served_by_commitments.append(truth.commitment_id)
        return c

    def _create_commitment(
        self,
        rng: random.Random,
        actor: ActorState,
        when: datetime,
        tick: int,
    ) -> CommitmentState:
        asserted = rng.randrange(3, 15)
        complexity = rng.choice(["low", "med", "high"])
        complexity_multiplier = {"low": 0.9, "med": 1.3, "high": 2.1}[complexity]
        # Bias: optimistic actors assert shorter; pessimistic assert longer.
        bias_adjust = 1.0 - (actor.persona.estimation_bias * 0.4)
        true_duration = max(1, int(round(asserted * complexity_multiplier * bias_adjust)))
        # Outcome is sampled based on reliability & complexity.
        roll = rng.random()
        if roll < 0.1 + (1 - actor.persona.reliability_parameter) * 0.2 and complexity != "low":
            outcome = "will_slip"
        elif roll < 0.15:
            outcome = "will_be_cancelled"
        else:
            outcome = "will_succeed"
        truth = CommitmentTruth(
            commitment_id=self._next_commitment_id(),
            owner_actor_id=actor.persona.actor_id,
            created_at=when,
            asserted_duration_days=asserted,
            true_duration_days=true_duration,
            true_complexity=complexity,
            true_outcome=outcome,
            resolution_event_at=None,
            hidden_dependencies=[],
        )
        c = CommitmentState(truth=truth, created_tick=tick)
        self._commitment_states.append(c)
        actor.current_workload += 1
        # Assign to a customer if any exist and we haven't over-assigned yet.
        if self._customer_states:
            target = self._customer_states[len(self._commitment_states) % len(self._customer_states)]
            target.truth.served_by_commitments.append(truth.commitment_id)
        return c

    def _seed_patterns(self) -> None:
        start = self.config.start_date
        # A generic pattern we bake in for every run: optimistic actors slip more.
        self._patterns.append(
            PatternTruthEntry(
                pattern_id="P-optimist-slippage",
                description="Optimistic actors' commitments slip systematically.",
                scope={"persona": "optimistic"},
                emergence_at=start,
                detection_eligible_after=start + timedelta(days=30),
            )
        )

    # ----------------- Per-tick dynamics -----------------

    def _apply_turbulence(
        self, rng: random.Random, current_date: datetime, tick: int
    ) -> None:
        for event in self.config.turbulence_events:
            if any(a.event.event_id == event.event_id for a in self._applied_turbulence):
                continue
            if current_date >= event.scheduled_at:
                self._apply_single_turbulence(rng, event, tick)
                self._applied_turbulence.append(
                    _TurbulenceApplication(event=event, applied_tick=tick)
                )

    def _apply_single_turbulence(
        self, rng: random.Random, event: TurbulenceEvent, tick: int
    ) -> None:
        mag = event.magnitude
        if event.kind in (TurbulenceKind.exec_departure, TurbulenceKind.layoff):
            # Targeted-departure path: when payload.actor_id is set (e.g. a
            # spec-authored named VP Eng departure), deactivate that specific
            # actor and broadcast a mood hit across the org. Otherwise fall
            # back to the legacy random-victim sampling so existing
            # CompanyA/B/C corpora stay byte-identical.
            target_actor_id = event.payload.get("actor_id") if event.payload else None
            if target_actor_id is not None:
                target = next(
                    (a for a in self._actor_states if a.persona.actor_id == target_actor_id),
                    None,
                )
                if target is not None:
                    target.active = False
                    target.adjust_mood(-1.0)
                # Org-wide morale hit proportional to magnitude.
                for a in self._actor_states:
                    if a is target:
                        continue
                    a.adjust_mood(-0.2 * mag)
                    a.shocks_absorbed += 1
            else:
                to_hit = max(1, int(len(self._actor_states) * mag * 0.1))
                for _ in range(to_hit):
                    if not self._actor_states:
                        break
                    victim = self._actor_states[rng.randrange(len(self._actor_states))]
                    victim.active = False
                    victim.adjust_mood(-1.0)
        if event.kind == TurbulenceKind.pivot:
            # Pivot: cancel some open commitments, push actors' moods down.
            for c in self._commitment_states:
                if not c.resolved and rng.random() < mag * 0.4:
                    c.truth = c.truth.model_copy(update={"true_outcome": "will_be_cancelled"})
            for a in self._actor_states:
                a.adjust_mood(-0.4 * mag)
                a.shocks_absorbed += 1
        if event.kind == TurbulenceKind.major_customer_loss:
            # Targeted-customer-loss path: same pattern as above. When
            # payload.customer_id is set, churn that specific customer (and
            # apply a softer health shock across the rest). Else legacy
            # random sampling.
            target_customer_id = event.payload.get("customer_id") if event.payload else None
            if target_customer_id is not None:
                target_cust = next(
                    (c for c in self._customer_states if c.truth.customer_id == target_customer_id),
                    None,
                )
                if target_cust is not None:
                    target_cust.current_health = "churned"
                # Softer health drift across remaining customers.
                for cust in self._customer_states:
                    if cust is target_cust:
                        continue
                    if rng.random() < mag * 0.1:
                        cust.apply_shock(rng, mag * 0.5)
            else:
                for cust in self._customer_states:
                    if rng.random() < mag * 0.2:
                        cust.current_health = "churned"
        if event.kind == TurbulenceKind.reorg:
            for a in self._actor_states:
                a.adjust_mood(-0.2 * mag)
                a.shocks_absorbed += 1
            for cust in self._customer_states:
                cust.apply_shock(rng, mag)

    def _advance_commitments(self, rng: random.Random, tick: int) -> None:
        actor_index = {a.persona.actor_id: a for a in self._actor_states}
        for c in self._commitment_states:
            owner = actor_index.get(c.owner_actor_id)
            if owner is None:
                continue
            was_resolved = c.resolved
            c.advance(rng, owner, tick)
            if c.resolved and not was_resolved:
                owner.current_workload = max(0, owner.current_workload - 1)
                c.truth.resolution_event_at = self.config.start_date + timedelta(days=tick)
                # Map truth outcome to resolved outcome.
                if c.truth.true_outcome == "will_slip":
                    c.truth = c.truth.model_copy(update={"true_outcome": "slipped_but_completed"})
                elif c.truth.true_outcome == "will_succeed":
                    c.truth = c.truth.model_copy(update={"true_outcome": "succeeded"})
                elif c.truth.true_outcome == "will_be_cancelled":
                    c.truth = c.truth.model_copy(update={"true_outcome": "cancelled"})

    def _advance_customers(self, rng: random.Random, tick: int) -> None:
        commitments_by_id = {c.truth.commitment_id: c for c in self._commitment_states}
        for cust in self._customer_states:
            serving = [
                commitments_by_id[cid]
                for cid in cust.truth.served_by_commitments
                if cid in commitments_by_id
            ]
            cust.advance(rng, serving, tick)

    def _generate_signals(
        self, rng: random.Random, current_date: datetime, tick: int
    ) -> None:
        # Density-driven path: when SignalDensity is configured, target a
        # specific daily volume distributed across active actors. Otherwise
        # the original per-actor "one signal if will_emit_today" path runs
        # unchanged so legacy CompanyA/B/C corpora stay byte-identical.
        if self.config.signal_density is not None:
            self._generate_signals_density_driven(rng, current_date, tick)
            return

        commits_by_owner: dict[str, list[CommitmentState]] = {}
        for c in self._commitment_states:
            commits_by_owner.setdefault(c.owner_actor_id, []).append(c)
        for actor in self._actor_states:
            if not actor.will_emit_today(rng):
                continue
            actor_commits = [c for c in commits_by_owner.get(actor.persona.actor_id, []) if not c.resolved]
            commitment = actor_commits[rng.randrange(len(actor_commits))] if actor_commits else None
            customer = None
            if actor.persona.role in ("cs-lead", "sales") and self._customer_states:
                customer = self._customer_states[rng.randrange(len(self._customer_states))]
            trigger_kind = _pick_trigger(rng, commitment, customer, actor, tick)
            channel = _pick_channel(rng, trigger_kind, actor)
            signal_id = f"sig-{self._signal_counter:08d}"
            self._signal_counter += 1
            ts_offset = rng.randrange(8 * 3600, 19 * 3600)  # 8am-7pm
            ts = current_date + timedelta(seconds=ts_offset)
            signal = self.signal_generator.generate(
                actor=actor,
                tick=tick,
                timestamp=ts,
                rng=rng,
                commitment=commitment,
                customer=customer,
                channel=channel,
                trigger_kind=trigger_kind,
                signal_id=signal_id,
            )
            self._signals.append(signal)
            actor.record_signal(signal_id)

    # ----------------- Density-driven emission -----------------

    def _is_crisis_tick(self, current_date: datetime) -> bool:
        """A tick is in a crisis window if it falls within ±crisis_window_days
        of any turbulence event's scheduled_at."""
        density = self.config.signal_density
        if density is None or density.crisis_window_days <= 0:
            return False
        window = timedelta(days=density.crisis_window_days)
        for ev in self.config.turbulence_events:
            if ev.scheduled_at - window <= current_date <= ev.scheduled_at + window:
                return True
        return False

    def _generate_signals_density_driven(
        self, rng: random.Random, current_date: datetime, tick: int
    ) -> None:
        density = self.config.signal_density
        assert density is not None
        active_actors = [a for a in self._actor_states if a.active]
        if not active_actors:
            return
        if self._is_crisis_tick(current_date):
            target = rng.randint(density.crisis_min, density.crisis_max)
        else:
            target = rng.randint(density.baseline_min, density.baseline_max)
        if target <= 0:
            return

        # Weight actors by communication_frequency, with a small floor so
        # nobody is silent. Pre-compute per-tick weights once.
        weights = [max(0.05, a.persona.communication_frequency) for a in active_actors]
        weight_sum = sum(weights)

        commits_by_owner: dict[str, list[CommitmentState]] = {}
        for c in self._commitment_states:
            if not c.resolved:
                commits_by_owner.setdefault(c.owner_actor_id, []).append(c)

        # Spread N signals across actors. Use per-emission RNG draws so the
        # actor distribution naturally tracks weights.
        for _ in range(target):
            r = rng.random() * weight_sum
            actor: ActorState | None = None
            cum = 0.0
            for a, w in zip(active_actors, weights):
                cum += w
                if r <= cum:
                    actor = a
                    break
            if actor is None:
                actor = active_actors[-1]

            actor_commits = commits_by_owner.get(actor.persona.actor_id, [])
            commitment = (
                actor_commits[rng.randrange(len(actor_commits))]
                if actor_commits else None
            )
            customer: CustomerState | None = None
            role = actor.persona.role
            role_family = actor.persona.role_family
            customer_facing = (
                role_family in ("sales", "customer_success")
                or role in ("cs-lead", "sales")
            )
            # Customer-facing actors hit a customer roughly half their emissions;
            # everyone else occasionally surfaces an account they touch.
            if customer_facing and self._customer_states and rng.random() < 0.55:
                customer = self._customer_states[rng.randrange(len(self._customer_states))]
            elif (
                commitment is not None
                and commitment.truth.customer_id
                and rng.random() < 0.35
            ):
                customer = next(
                    (cs for cs in self._customer_states
                     if cs.truth.customer_id == commitment.truth.customer_id),
                    None,
                )
            elif self._customer_states and rng.random() < 0.05:
                customer = self._customer_states[rng.randrange(len(self._customer_states))]

            trigger_kind = _pick_trigger(rng, commitment, customer, actor, tick)
            channel = _pick_channel(rng, trigger_kind, actor)
            signal_id = f"sig-{self._signal_counter:08d}"
            self._signal_counter += 1
            ts_offset = rng.randrange(7 * 3600, 21 * 3600)  # 7am-9pm
            ts = current_date + timedelta(seconds=ts_offset)
            peers = self._peers_for(actor, active_actors)
            signal = self.signal_generator.generate(
                actor=actor,
                tick=tick,
                timestamp=ts,
                rng=rng,
                commitment=commitment,
                customer=customer,
                channel=channel,
                trigger_kind=trigger_kind,
                signal_id=signal_id,
                peers=peers,
            )
            self._signals.append(signal)
            actor.record_signal(signal_id)

    def _peers_for(
        self, actor: ActorState, active_actors: list[ActorState]
    ) -> list[ActorState]:
        """Co-workers eligible for {peer} mentions: same manager + direct
        reports + the manager themselves. Limited to active actors. Returns
        empty list if no rich-profile manager chain is available."""
        manager_id = actor.persona.manager_id
        if manager_id is None and not any(a.persona.manager_id == actor.persona.actor_id for a in self._actor_states):
            return []
        out: list[ActorState] = []
        for a in active_actors:
            if a is actor:
                continue
            same_manager = (
                manager_id is not None
                and a.persona.manager_id == manager_id
            )
            is_my_manager = a.persona.actor_id == manager_id
            is_my_report = a.persona.manager_id == actor.persona.actor_id
            if same_manager or is_my_manager or is_my_report:
                out.append(a)
        return out

    def _maybe_create_new_commitments(
        self, rng: random.Random, current_date: datetime, tick: int
    ) -> None:
        if tick == 0:
            return
        for actor in self._actor_states:
            if not actor.active:
                continue
            # Generate at configured rate — modulated by current workload (satiation).
            base_rate = self.config.commitment_generation_rate
            modulated = base_rate * max(0.1, 1.0 - 0.15 * actor.current_workload)
            if rng.random() < modulated:
                self._create_commitment(rng, actor, current_date, tick=tick)

    def _maybe_emit_ground_truth(self, current_date: datetime) -> None:
        if self._gt_recorder.checkpoint_due(current_date):
            self._emit_ground_truth(current_date)

    def _emit_ground_truth(self, current_date: datetime) -> None:
        self._gt_recorder.emit(
            current_date=current_date,
            actors=self._actor_states,
            commitments=self._commitment_states,
            customers=self._customer_states,
            patterns=self._patterns,
            predictions_resolving=[],
        )

    def _build_meta(self, end_date: datetime) -> CorpusMeta:
        # Config hash: sha256 of normalized config JSON. Ensures reproducible identity.
        payload = self.config.model_dump_json()
        h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        return CorpusMeta(
            corpus_id=f"{self.config.company_id}-{h}",
            company_id=self.config.company_id,
            months_simulated=self.config.duration_months,
            seed=self.config.seed,
            config_hash=h,
            start_date=self.config.start_date,
            end_date=end_date,
        )


def _pick_trigger(
    rng: random.Random,
    commitment: CommitmentState | None,
    customer: CustomerState | None,
    actor: ActorState,
    tick: int,
) -> str:
    if customer is not None and customer.current_health in ("warning", "degraded", "critical"):
        return "customer"
    if commitment is None:
        return "customer" if customer is not None else "progress"
    if commitment.created_tick == tick:
        return "start"
    if commitment.slip_acknowledged and rng.random() < 0.8:
        return "slip"
    if commitment.true_progress >= 1.0 and rng.random() < 0.7:
        return "done"
    return "progress"


def _pick_channel(
    rng: random.Random, trigger_kind: str, actor: ActorState
) -> SourceChannel:
    # PR for engineers on start/progress/done; email for leadership on progress/slip;
    # ticket for customer escalations; calendar sparsely; doc occasionally.
    role = actor.persona.role
    if trigger_kind == "customer":
        return SourceChannel.ticket if rng.random() < 0.5 else SourceChannel.email
    if trigger_kind in ("start", "done") and role in ("eng", "senior-eng", "sre"):
        return SourceChannel.pr
    if trigger_kind == "slip" and role in ("pm", "exec", "cs-lead"):
        return SourceChannel.email
    if rng.random() < 0.1:
        return SourceChannel.doc
    if rng.random() < 0.05:
        return SourceChannel.calendar
    return SourceChannel.slack
