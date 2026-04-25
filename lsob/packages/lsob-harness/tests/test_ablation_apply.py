"""Tests for ``apply_ablation`` — happy + validation-failure paths."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest

from lsob_contracts import (
    AblationConfig,
    ActOp,
    AtRiskItem,
    AtRiskReport,
    Belief,
    BeliefQuery,
    ClaimOp,
    DiffOp,
    EntityRef,
    Signal,
    SUTConfig,
    Trigger,
)

from lsob_harness.ablation import (
    REGISTRY,
    AblationValidationError,
    apply_ablation,
)


@dataclass
class _RecordingMockSUT:
    """Mock SUT that records the applied ablation and honours it in probes."""

    name: str = "mock-recording"
    max_concurrent_ingestion: int = 1
    applied: list[AblationConfig] = field(default_factory=list)
    ablation: AblationConfig | None = None

    async def startup(self, config: SUTConfig) -> None:
        self.config = config

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self.applied.append(ablation)
        self.ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        return None

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        # Return an innocuous belief that does not advertise any feature.
        return [
            Belief(
                claim_id=f"claim-{uuid.uuid4().hex[:8]}",
                proposition="innocuous probe response",
                proposition_kind="status",
                asserted_confidence=0.4,
                last_updated=query.timestamp,
                entities=[query.entity_ref.id],
                evidence_signal_ids=[],
            )
        ]

    async def query_at_risk_at(self, ts: datetime) -> AtRiskReport:
        # Respect disable_bridge: return empty if disabled.
        if self.ablation and self.ablation.disable_bridge:
            return AtRiskReport(timestamp=ts, items=[])
        return AtRiskReport(timestamp=ts, items=[])

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        return DiffOp(
            diff_id=f"diff-{uuid.uuid4().hex[:8]}",
            produced_at=datetime.now(tz=timezone.utc),
            trigger_id=trigger.trigger_id,
            claim_ops=[
                ClaimOp(
                    op="upsert_claim",
                    claim_id=f"claim-{uuid.uuid4().hex[:8]}",
                    proposition="innocuous",
                    proposition_kind="status",
                    asserted_confidence=0.5,
                    evidence_signal_ids=[],
                    entities=["ablation-probe"],
                )
            ],
            act_ops=[
                ActOp(
                    op="transition",
                    entity_ref="ablation-probe",
                    to_state="reviewed",
                    reason="probe",
                )
            ],
            rationale="innocuous probe",
            metadata={},
        )

    async def shutdown(self) -> None:
        return None


@dataclass
class _LyingMockSUT(_RecordingMockSUT):
    """Mock that claims to honour ``apply_ablation`` but actually still runs it."""

    # Override default so dataclass inheritance is clean.
    name: str = "mock-lying"

    async def query_at_risk_at(self, ts: datetime) -> AtRiskReport:
        # Always emit a Bridge at-risk item, regardless of disable_bridge.
        return AtRiskReport(
            timestamp=ts,
            items=[
                AtRiskItem(
                    entity_ref=EntityRef(kind="commitment", id="still-alive"),
                    risk_score=0.9,
                    risk_kind="commitment_slip",
                    rationale="bridge computed this anyway",
                )
            ],
        )

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        base = await super().produce_diff_for_trigger(trigger)
        # Advertise calibration in metadata even when disable_calibration set.
        return base.model_copy(
            update={
                "metadata": {"calibration": "applied"},
                "rationale": "calibration pass still ran",
            }
        )


async def test_apply_ablation_happy_none() -> None:
    sut = _RecordingMockSUT()
    await sut.startup(SUTConfig(sut_name="mock", tenant_id=None, params={}))
    cfg = REGISTRY.get("none")
    await apply_ablation(sut, cfg)
    assert sut.applied == [cfg]
    assert sut.ablation == cfg


async def test_apply_ablation_happy_no_bridge_validates() -> None:
    sut = _RecordingMockSUT()
    await sut.startup(SUTConfig(sut_name="mock", tenant_id=None, params={}))
    cfg = REGISTRY.get("no-bridge")
    await apply_ablation(sut, cfg)
    assert sut.applied == [cfg]
    # Validation must have passed with no exception.


async def test_apply_ablation_happy_all_off_validates() -> None:
    sut = _RecordingMockSUT()
    await sut.startup(SUTConfig(sut_name="mock", tenant_id=None, params={}))
    cfg = REGISTRY.get("all-off")
    await apply_ablation(sut, cfg)
    assert sut.applied == [cfg]


async def test_apply_ablation_detects_still_active_bridge() -> None:
    sut = _LyingMockSUT()
    await sut.startup(SUTConfig(sut_name="mock-lying", tenant_id=None, params={}))
    cfg = REGISTRY.get("no-bridge")
    with pytest.raises(AblationValidationError) as exc_info:
        await apply_ablation(sut, cfg)
    assert "disable_bridge" in str(exc_info.value)


async def test_apply_ablation_detects_still_active_calibration() -> None:
    sut = _LyingMockSUT()
    await sut.startup(SUTConfig(sut_name="mock-lying", tenant_id=None, params={}))
    cfg = AblationConfig(name="no-calibration", disable_calibration=True)
    with pytest.raises(AblationValidationError) as exc_info:
        await apply_ablation(sut, cfg)
    assert "disable_calibration" in str(exc_info.value)


async def test_apply_ablation_recorded_even_when_validation_passes() -> None:
    sut = _RecordingMockSUT()
    await sut.startup(SUTConfig(sut_name="mock", tenant_id=None, params={}))
    cfg = AblationConfig(
        name="no-second-pass",
        disable_second_pass=True,
    )
    await apply_ablation(sut, cfg)
    assert sut.applied == [cfg]
    assert sut.ablation is cfg
