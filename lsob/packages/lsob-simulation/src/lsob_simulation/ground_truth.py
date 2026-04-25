"""Ground truth recorder — emits monthly snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from lsob_contracts import GroundTruth

from lsob_simulation.state import ActorState, CommitmentState, CustomerState


@dataclass
class PatternTruthEntry:
    pattern_id: str
    description: str
    scope: dict
    emergence_at: datetime
    detection_eligible_after: datetime


class GroundTruthRecorder:
    """Collects monthly GroundTruth snapshots for a running simulation."""

    def __init__(self, start_date: datetime, duration_months: int) -> None:
        self.start_date = start_date
        self.duration_months = duration_months
        self._snapshots: list[GroundTruth] = []
        # Next monthly checkpoint we will emit at.
        self._next_checkpoint_idx = 1

    @property
    def snapshots(self) -> list[GroundTruth]:
        return self._snapshots

    def checkpoint_due(self, current_date: datetime) -> bool:
        target = self._target_for(self._next_checkpoint_idx)
        return target is not None and current_date >= target

    def _target_for(self, month_idx: int) -> datetime | None:
        if month_idx > self.duration_months:
            return None
        # Monthly = start + 30 * idx days. Keeps arithmetic simple + deterministic.
        return self.start_date + timedelta(days=30 * month_idx)

    def emit(
        self,
        *,
        current_date: datetime,
        actors: Iterable[ActorState],
        commitments: Iterable[CommitmentState],
        customers: Iterable[CustomerState],
        patterns: Iterable[PatternTruthEntry],
        predictions_resolving: Iterable[dict],
    ) -> GroundTruth:
        gt = GroundTruth(
            timestamp=current_date,
            actors=[
                {
                    "id": a.persona.actor_id,
                    "name": a.persona.name,
                    "role": a.persona.role,
                    "reliability": a.persona.reliability_parameter,
                    "estimation_bias": a.persona.estimation_bias,
                    "mood": round(a.mood, 3),
                    "active": a.active,
                }
                for a in actors
            ],
            commitments=[
                {
                    "id": c.truth.commitment_id,
                    "owner": c.truth.owner_actor_id,
                    "true_complexity": c.truth.true_complexity,
                    "true_duration_days": c.truth.true_duration_days,
                    "asserted_duration_days": c.truth.asserted_duration_days,
                    "true_outcome": c.truth.true_outcome,
                    "resolution_event_at": (
                        c.truth.resolution_event_at.isoformat()
                        if c.truth.resolution_event_at
                        else None
                    ),
                    "true_progress": round(c.true_progress, 3),
                    "perceived_progress": round(c.perceived_progress, 3),
                    "resolved": c.resolved,
                }
                for c in commitments
            ],
            customers=[
                {
                    "id": cu.truth.customer_id,
                    "revenue_value": cu.truth.revenue_value,
                    "current_health": cu.current_health,
                    "served_by_commitments": cu.truth.served_by_commitments,
                    "trajectory": list(cu.health_history),
                }
                for cu in customers
            ],
            patterns=[
                {
                    "id": p.pattern_id,
                    "description": p.description,
                    "scope": p.scope,
                    "emergence_at": p.emergence_at.isoformat(),
                    "detection_eligible_after": p.detection_eligible_after.isoformat(),
                }
                for p in patterns
            ],
            predictions_that_will_resolve=list(predictions_resolving),
        )
        self._snapshots.append(gt)
        self._next_checkpoint_idx += 1
        return gt
