"""Minimal Phase 0.1 contract shapes. Phase 0.2 extends these."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


class EntityRef(_Base):
    kind: Literal["commitment", "customer", "actor", "pattern", "model"]
    id: str


class SourceChannel(str, Enum):
    slack = "slack"
    email = "email"
    pr = "pr"
    doc = "doc"
    calendar = "calendar"
    ticket = "ticket"


class Signal(_Base):
    signal_id: str
    source_channel: SourceChannel
    author_id: str
    content_text: str
    timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class GroundTruth(_Base):
    timestamp: datetime
    actors: list[dict[str, Any]] = Field(default_factory=list)
    commitments: list[dict[str, Any]] = Field(default_factory=list)
    customers: list[dict[str, Any]] = Field(default_factory=list)
    patterns: list[dict[str, Any]] = Field(default_factory=list)
    predictions_that_will_resolve: list[dict[str, Any]] = Field(default_factory=list)
    # Optional Layer-6 reference diffs. Each entry is a dict with keys:
    #   - trigger_id: str
    #   - trigger: dict compatible with Trigger (optional; if absent we
    #     synthesize a minimal Trigger from trigger_id).
    #   - diff: dict compatible with DiffOp.
    reference_diffs: list[dict[str, Any]] = Field(default_factory=list)


class CorpusMeta(_Base):
    corpus_id: str
    company_id: str
    months_simulated: int
    seed: int
    config_hash: str
    start_date: datetime
    end_date: datetime
    generator_version: str = "0.1.0"


class Corpus(_Base):
    meta: CorpusMeta
    signals: list[Signal] = Field(default_factory=list)
    ground_truth: list[GroundTruth] = Field(default_factory=list)


class EvalResult(_Base):
    layer_id: int
    metric_name: str
    value: float
    confidence_interval: tuple[float, float] | None = None
    breakdown_by: dict[str, Any] = Field(default_factory=dict)
    run_id: str | None = None


class Trigger(_Base):
    trigger_id: str
    kind: str
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime


class SUTConfig(_Base):
    sut_name: str
    tenant_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class SystemUnderTestSpec(_Base):
    """Declarative SUT descriptor. The Protocol interface lives in contracts.protocols."""

    name: str
    version: str
    config: SUTConfig


class AblationConfig(_Base):
    name: str = "none"
    disable_bridge: bool = False
    disable_calibration: bool = False
    disable_second_pass: bool = False
    disable_activation: bool = False
    disable_entity_resolver: bool = False
    disable_pattern_precipitation: bool = False
    disable_model_composition: bool = False

    def any_disabled(self) -> bool:
        return any(
            getattr(self, f)
            for f in type(self).model_fields
            if f.startswith("disable_")
        )


class JudgeCost(_Base):
    """Aggregate Anthropic usage + USD estimate for a run's judge calls."""

    input_tokens: int = 0
    output_tokens: int = 0
    estimated_usd: float = 0.0
    n_calls: int = 0


class RunManifest(_Base):
    run_id: str
    company: str
    months_simulated: int
    baseline: str
    ablation: AblationConfig
    seed: int
    git_sha: str
    started_at: datetime
    finished_at: datetime | None = None
    corpus_uri: str
    layers: list[int]
    judge_model: str | None = None
    judge_prompt_hash: str | None = None
    judge_cost: JudgeCost | None = None
