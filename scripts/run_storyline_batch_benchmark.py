#!/usr/bin/env python3
"""Run a planted-storyline benchmark for large-batch Think.

This is a value benchmark, not a raw load test. It asks whether a
20-30 signal batch helps the system discover durable company understanding:
composite situations, useful actions, relevant graph edges, low review debt,
and low internal amplification per useful outcome.

The Company Intelligence scorecard design is documented at:
docs/evaluation/company_intelligence_harness.md

Default `--mode build-only` writes the generated scenario and gold rubric
without touching Postgres or an LLM. Use `--mode run` for the real burn.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.migrations import apply_migrations_dir
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.app.gateway.db_bootstrap import _register_codecs
from services.reasoning.think.worker import ThinkWorker, WorkerConfig
from tests.real_llm.infrastructure.scenario_loader import Scenario, materialize

from scripts.run_1000_signal_model_layer_probe import (
    ACTOR_BY_FAMILY,
    CHANNEL_BY_FAMILY,
    TRUST_BY_FAMILY,
    _build_cached_provider,
    _fetch_distribution,
    _insert_extra_aliases,
    _render_markdown as _render_model_layer_markdown,
    build_scenario as build_base_scenario,
    collect_model_layer_report,
    drain_post_commit_actions,
    drain_topology_optimizer,
    enqueue_t1_for_observations,
    inject_generated_signals,
)

load_dotenv(REPO_ROOT / ".env", override=False)


@dataclass(frozen=True)
class StorylineSpec:
    id: str
    title: str
    thesis: str
    latent_pattern_groups: tuple[tuple[str, ...], ...]
    customers: tuple[str, ...]
    commitments: tuple[str, ...]
    goals: tuple[str, ...]
    decisions: tuple[str, ...]
    families: tuple[str, ...]
    expected_terms: tuple[str, ...]
    expected_actions: tuple[str, ...]
    expected_relationships: tuple[str, ...]
    risk_type: str


@dataclass
class StorylineScore:
    storyline_id: str
    title: str
    signal_count: int
    relevant_model_count: int
    evidence_supported_model_count: int
    keyword_hits: list[str]
    missing_keywords: list[str]
    situation_model_count: int
    recommendation_model_count: int
    scoped_edge_count: int
    edge_kind_hits: list[str]
    missing_edge_kinds: list[str]
    review_candidate_count: int
    accepted_candidate_count: int
    needs_review_candidate_count: int
    latent_pattern_score: float
    latent_pattern_model_count: int
    latent_pattern_evidence_supported_model_count: int
    latent_pattern_best_coverage: float
    latent_pattern_group_hits: list[str]
    missing_latent_pattern_groups: list[str]
    latent_pattern_model_ids: list[str]
    score: float
    inferred_bridge_model_count: int = 0
    inferred_bridge_transition_supported_model_count: int = 0
    inferred_bridge_future_confirmed_model_count: int = 0
    unsupported_bridge_specific_claim_count: int = 0
    bridge_epistemic_marker_hits: list[str] = field(default_factory=list)
    bridge_forbidden_detail_hits: list[str] = field(default_factory=list)
    thesis_judge_score: float | None = None
    thesis_judge_correct: bool | None = None
    thesis_judge_rationale: str | None = None
    thesis_judge_metadata: dict[str, Any] = field(default_factory=dict)
    calibration_samples: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


_LATENT_BRIDGE_STORYLINE_ID = "northstar_unobserved_discount_bridge"


_PRODUCT_VALUE_EVAL_KEYS: tuple[str, ...] = (
    "decision_impact",
    "memory_lifecycle",
    "prediction_lifecycle",
    "counterfactual_trap",
    "latent_bridge_inference",
    "compression_loss",
    "negative_learning",
    "question_policy",
    "customer_value",
)


_THESIS_JUDGE_NAME = "storyline_thesis_recovery"
_THESIS_JUDGE_AGREEMENT_SET = (
    REPO_ROOT
    / "benchmarks"
    / "fyralis_eval"
    / "storyline_thesis_judge_agreement.json"
)


STORYLINES: tuple[StorylineSpec, ...] = (
    StorylineSpec(
        id="atlas_renewal_risk",
        title="Atlas renewal risk is really security plus usage decay",
        thesis=(
            "Atlas looks like a support escalation, but the real pattern is "
            "security evidence missing while usage drops and procurement waits."
        ),
        latent_pattern_groups=(
            ("security", "audit", "soc2", "evidence"),
            ("usage", "drop", "decay", "telemetry"),
            ("procurement", "legal", "approval", "wait"),
            ("renewal", "risk", "escalation"),
        ),
        customers=("Atlas Retail Group",),
        commitments=(
            "Resolve Atlas Retail Group renewal blockers",
            "Publish SOC2 evidence room",
            "Deliver audit export v2",
        ),
        goals=("Protect enterprise renewal base", "Harden security review posture"),
        decisions=("Enterprise controls remain the top renewal lever",),
        families=(
            "customer_escalation",
            "usage_telemetry",
            "security_review",
            "legal_procurement",
            "support_ticket",
            "exec_decision",
        ),
        expected_terms=(
            "atlas",
            "renewal",
            "security",
            "usage",
            "procurement",
            "audit",
            "risk",
        ),
        expected_actions=("owner", "security packet", "renewal escalation"),
        expected_relationships=("supports", "early_warning_for", "explains"),
        risk_type="revenue",
    ),
    StorylineSpec(
        id="borealis_confidence_contradiction",
        title="Borealis confidence is contradicted by incident opacity",
        thesis=(
            "Borealis sales confidence is high, but incident opacity and stale "
            "dashboard state are undermining the executive sponsor."
        ),
        latent_pattern_groups=(
            ("confidence", "forecast", "sales"),
            ("incident", "opacity", "timeline"),
            ("stale", "dashboard", "lag"),
            ("sponsor", "trust", "undermin"),
        ),
        customers=("Borealis Bank",),
        commitments=(
            "Recover Borealis Bank executive confidence",
            "Build customer-visible incident timeline",
            "Stabilize streaming connector lag",
        ),
        goals=("Reduce incident-driven churn", "Protect enterprise renewal base"),
        decisions=("Use customer-facing timelines for repeat incidents",),
        families=(
            "sales_pipeline",
            "incident",
            "stale_replay",
            "calendar_meeting",
            "forecast_update",
            "contradiction",
        ),
        expected_terms=(
            "borealis",
            "confidence",
            "incident",
            "stale",
            "timeline",
            "contradiction",
            "forecast",
        ),
        expected_actions=("customer-visible timeline", "sponsor recovery"),
        expected_relationships=("weakens", "early_warning_for", "explains"),
        risk_type="trust",
    ),
    StorylineSpec(
        id="cobalt_security_packet",
        title="Cobalt security packet is the gating dependency",
        thesis=(
            "Cobalt's legal and security asks are not isolated paperwork; they "
            "gate procurement approval and should be tied to audit/SOC2 work."
        ),
        latent_pattern_groups=(
            ("legal", "procurement", "approval"),
            ("security", "packet", "review"),
            ("audit", "soc2", "evidence"),
            ("gate", "block", "dependency"),
        ),
        customers=("Cobalt Health Network",),
        commitments=(
            "Close Cobalt Health Network security packet",
            "Publish SOC2 evidence room",
            "Ship SAML group mapping",
        ),
        goals=("Harden security review posture", "Ship regulated enterprise controls"),
        decisions=("Enterprise controls remain the top renewal lever",),
        families=(
            "security_review",
            "legal_procurement",
            "compliance_regulatory",
            "finance_billing",
            "product_roadmap",
            "risk_digest",
        ),
        expected_terms=(
            "cobalt",
            "security",
            "soc2",
            "saml",
            "procurement",
            "audit",
            "approval",
        ),
        expected_actions=("security packet", "evidence room", "procurement approval"),
        expected_relationships=("blocks", "supports", "contributes_to_resolution"),
        risk_type="compliance",
    ),
    StorylineSpec(
        id="deltafleet_capacity_slip",
        title="DeltaFleet implementation slip is capacity, not customer apathy",
        thesis=(
            "DeltaFleet implementation risk is caused by capacity and handoff "
            "slippage, not by lack of customer interest."
        ),
        latent_pattern_groups=(
            ("implementation", "onboarding"),
            ("capacity", "hiring", "coverage"),
            ("handoff", "owner", "slip"),
            ("interest", "engaged", "not apathy"),
        ),
        customers=("DeltaFleet Logistics",),
        commitments=(
            "Unblock DeltaFleet Logistics implementation",
            "Rewrite onboarding health playbook",
        ),
        goals=("Improve implementation throughput",),
        decisions=("Revenue-at-risk beats activity volume",),
        families=(
            "implementation",
            "people_ops",
            "hiring_capacity",
            "support_ticket",
            "calendar_meeting",
            "risk_digest",
        ),
        expected_terms=(
            "deltafleet",
            "implementation",
            "capacity",
            "handoff",
            "onboarding",
            "slip",
            "throughput",
        ),
        expected_actions=("implementation owner", "capacity plan", "onboarding health"),
        expected_relationships=("explains", "blocks", "supports"),
        risk_type="execution",
    ),
    StorylineSpec(
        id="foundryworks_connector_reliability",
        title="FoundryWorks churn risk is repeat connector reliability",
        thesis=(
            "FoundryWorks is not a one-off outage; repeated connector reliability "
            "issues create churn risk and should affect incident policy."
        ),
        latent_pattern_groups=(
            ("repeat", "recurring", "again"),
            ("connector", "freshness", "reliability"),
            ("incident", "outage", "support"),
            ("churn", "renewal", "risk"),
        ),
        customers=("FoundryWorks Manufacturing",),
        commitments=(
            "Repair FoundryWorks Manufacturing connector reliability",
            "Stabilize streaming connector lag",
            "Clarify support severity language",
        ),
        goals=("Reduce incident-driven churn", "Stabilize data freshness"),
        decisions=("Treat data freshness as product quality",),
        families=(
            "incident",
            "support_ticket",
            "usage_telemetry",
            "engineering_pr",
            "stale_replay",
            "customer_escalation",
        ),
        expected_terms=(
            "foundryworks",
            "connector",
            "reliability",
            "repeat",
            "freshness",
            "incident",
            "churn",
        ),
        expected_actions=("incident timeline", "connector remediation"),
        expected_relationships=("early_warning_for", "supports", "weakens"),
        risk_type="product_quality",
    ),
    StorylineSpec(
        id="keystone_bespoke_drag",
        title="Keystone bespoke workflow threatens roadmap leverage",
        thesis=(
            "Keystone's custom workflow request looks like renewal pressure, but "
            "it conflicts with the decision to avoid unpriced bespoke work."
        ),
        latent_pattern_groups=(
            ("bespoke", "custom", "workflow"),
            ("renewal", "pressure", "deal"),
            ("unpriced", "price", "exception"),
            ("roadmap", "leverage", "reusable"),
        ),
        customers=("Keystone Robotics",),
        commitments=(
            "Contain Keystone Robotics custom workflow request",
            "Add permission audit trail to admin console",
        ),
        goals=("Constrain bespoke commitments", "Ship regulated enterprise controls"),
        decisions=("Do not accept unpriced bespoke workflow work",),
        families=(
            "product_roadmap",
            "sales_pipeline",
            "exec_decision",
            "finance_billing",
            "market_competitor",
            "risk_digest",
        ),
        expected_terms=(
            "keystone",
            "bespoke",
            "workflow",
            "roadmap",
            "unpriced",
            "leverage",
            "exception",
        ),
        expected_actions=("price exception", "reject bespoke", "reusable leverage"),
        expected_relationships=("weakens", "blocks", "explains"),
        risk_type="strategy",
    ),
    StorylineSpec(
        id="runway_enterprise_controls_tradeoff",
        title="Runway pressure and enterprise controls form one tradeoff",
        thesis=(
            "Cash runway, hiring capacity, and enterprise controls are one "
            "operating tradeoff, not separate finance/product updates."
        ),
        latent_pattern_groups=(
            ("runway", "cash", "board"),
            ("hiring", "capacity", "allocation"),
            ("enterprise", "controls", "compliance"),
            ("tradeoff", "resource", "priority"),
        ),
        customers=("Lumina Telecom", "Evergreen Energy", "HarborRail Transit"),
        commitments=(
            "Launch data residency controls",
            "Close Lumina Telecom audit exception",
            "Prepare Evergreen Energy data residency review",
            "Finish HarborRail Transit procurement evidence",
        ),
        goals=("Ship regulated enterprise controls", "Protect enterprise renewal base"),
        decisions=("Enterprise controls remain the top renewal lever",),
        families=(
            "cash_runway",
            "hiring_capacity",
            "board_update",
            "security_review",
            "compliance_regulatory",
            "forecast_update",
        ),
        expected_terms=(
            "runway",
            "enterprise",
            "controls",
            "hiring",
            "board",
            "renewal",
            "tradeoff",
        ),
        expected_actions=("board tradeoff", "hiring allocation", "enterprise controls"),
        expected_relationships=("explains", "supports", "early_warning_for"),
        risk_type="resource",
    ),
    StorylineSpec(
        id="alias_ambiguity_pollution",
        title="Alias ambiguity should not pollute customer memory",
        thesis=(
            "Ambiguous aliases across Atlas, Granite, and Northstar should be "
            "resolved before strong customer graph edges are written."
        ),
        latent_pattern_groups=(
            ("alias", "ambiguity", "ambiguous"),
            ("atlas", "granite", "northstar"),
            ("resolve", "review", "disambiguat"),
            ("graph", "edge", "mutation", "pollution"),
        ),
        customers=("Atlas Retail Group", "Granite Insurance", "Northstar Labs"),
        commitments=(
            "Rebuild Granite Insurance champion map",
            "Reprice Northstar Labs expansion package",
            "Resolve Atlas Retail Group renewal blockers",
        ),
        goals=("Improve expansion forecasting",),
        decisions=("Escalate ambiguous aliases before graph mutation",),
        families=(
            "alias_ambiguity",
            "forecast_update",
            "sales_pipeline",
            "contradiction",
            "noise",
            "exec_decision",
        ),
        expected_terms=(
            "alias",
            "ambiguity",
            "atlas",
            "granite",
            "northstar",
            "forecast",
            "resolve",
        ),
        expected_actions=("resolve alias", "avoid graph mutation"),
        expected_relationships=("needs_review", "weakens"),
        risk_type="memory_quality",
    ),
    StorylineSpec(
        id=_LATENT_BRIDGE_STORYLINE_ID,
        title="Northstar pricing shift implies an unobserved decision bridge",
        thesis=(
            "Northstar moves from blocked pricing to approved exception without "
            "a captured decision, so the system should infer a bounded off-sensor "
            "decision bridge without inventing specific unsupported details."
        ),
        latent_pattern_groups=(
            ("before", "after", "state", "transition"),
            ("unobserved", "inferred", "missing", "gap"),
            ("discount", "exception", "pricing", "policy"),
            ("confidence", "confirm", "uncertain", "indirect"),
        ),
        customers=("Northstar Labs",),
        commitments=("Reprice Northstar Labs expansion package",),
        goals=("Improve expansion forecasting",),
        decisions=("Forecast confidence requires evidence diversity",),
        families=(
            "forecast_update",
            "finance_billing",
            "sales_pipeline",
            "contradiction",
            "exec_decision",
            "calendar_meeting",
        ),
        expected_terms=(
            "northstar",
            "pricing",
            "discount",
            "exception",
            "before",
            "after",
            "inferred",
            "unobserved",
            "confidence",
        ),
        expected_actions=("mark inferred", "ask for confirmation", "bound confidence"),
        expected_relationships=("explains", "needs_review", "early_warning_for"),
        risk_type="epistemic_gap",
    ),
)


_STORY_EVIDENCE_FRAGMENTS: dict[str, tuple[str, ...]] = {
    "atlas_renewal_risk": (
        "Procurement moved the renewal packet to waiting status until audit export evidence is available.",
        "Admin usage is down again even though the support thread is receiving more attention.",
        "Security reviewers asked for SOC2 evidence before the renewal owner can unblock approval.",
        "The latest escalation is framed as support noise, but the blocker named by the buyer is evidence readiness.",
        "Finance is treating the renewal risk as material because procurement has not cleared the packet.",
        "The account team is asking whether usage decay and security review should share one owner.",
    ),
    "borealis_confidence_contradiction": (
        "The forecast remains confident while the sponsor asks why the incident timeline is still opaque.",
        "Dashboard status is stale again after the streaming lag incident was marked mostly resolved.",
        "The executive sponsor wants a customer-visible timeline before accepting the recovery plan.",
        "Sales notes still show expansion confidence, but support evidence points to trust erosion.",
        "A stale replay appeared in the account review and contradicted the optimistic forecast.",
        "The owner asked for one explanation that connects incident opacity, stale data, and sponsor confidence.",
    ),
    "cobalt_security_packet": (
        "Legal is waiting on procurement approval because the security packet is incomplete.",
        "The SOC2 evidence room and SAML group mapping are both referenced in the same review thread.",
        "Procurement says the approval path is blocked until audit evidence is attached.",
        "The customer asked whether compliance paperwork and product roadmap work share one dependency.",
        "Finance cannot clear the billing exception while the security review remains open.",
        "The risk digest calls the packet a gating dependency rather than isolated paperwork.",
    ),
    "deltafleet_capacity_slip": (
        "DeltaFleet stakeholders are still engaged, but the implementation owner handoff slipped again.",
        "The onboarding checklist is waiting on capacity coverage from the implementation team.",
        "Support volume is not the blocker; the missing owner handoff is delaying progress.",
        "Hiring capacity notes mention the same onboarding queue that appears in the customer meeting.",
        "The customer accepted the next milestone but asked for a clearer implementation owner.",
        "The risk update says throughput, not customer apathy, explains the slip.",
    ),
    "foundryworks_connector_reliability": (
        "The connector freshness incident recurred after the last support ticket was closed.",
        "Usage telemetry shows renewed risk whenever the connector reliability alert repeats.",
        "The engineering PR reduces lag but does not yet address the repeated customer-visible failure.",
        "Support wants the timeline to show this is not a one-off outage.",
        "The customer escalation ties connector reliability to churn risk in the renewal notes.",
        "The incident policy owner asked whether repeat freshness failures deserve a stronger rule.",
    ),
    "keystone_bespoke_drag": (
        "Keystone is pressing for a custom workflow without a priced exception.",
        "The renewal note frames bespoke work as urgent, but roadmap review calls for reusable leverage.",
        "Finance asked whether the workflow request should be rejected or converted into a price exception.",
        "The executive decision says unpriced bespoke work should not bypass the roadmap.",
        "A competitor mention is increasing deal pressure around the custom workflow.",
        "Product asked whether the request weakens the current enterprise controls roadmap.",
    ),
    "runway_enterprise_controls_tradeoff": (
        "The board update ties cash runway to which enterprise controls get staffed this quarter.",
        "Hiring capacity is being reallocated toward data residency and compliance work.",
        "The forecast update treats enterprise controls as the lever most connected to renewal protection.",
        "Finance asked for a single tradeoff view across runway, hiring, and compliance commitments.",
        "Security review owners need capacity allocation before audit exceptions can close.",
        "The operating review says these are not separate finance and product updates.",
    ),
    "alias_ambiguity_pollution": (
        "Atlas, Granite, and Northstar aliases appeared in the same forecast thread with conflicting references.",
        "The decision note says ambiguous customer names should be reviewed before graph mutation.",
        "Sales pressure is creating a tempting edge, but alias resolution is still incomplete.",
        "The forecast update mentions Atlas while the supporting account evidence points to Granite.",
        "A noisy executive thread asks for customer graph cleanup before any strong relationship is written.",
        "The memory-quality review says alias ambiguity could pollute durable customer context.",
    ),
}


def build_storyline_scenario(
    *,
    run_id: str,
    signals_per_storyline: int,
    noise_signals: int,
    future_validation_signals_per_storyline: int = 0,
    target_t1_batches: int = 0,
    foundation_namespace: str | None = None,
    horizon_start_batch: int = 0,
) -> tuple[Scenario, list[dict[str, Any]]]:
    if signals_per_storyline <= 0:
        raise ValueError("signals_per_storyline must be positive")
    if horizon_start_batch < 0:
        raise ValueError("horizon_start_batch must be >= 0")
    namespace = foundation_namespace or run_id
    if target_t1_batches > 0:
        return _build_long_horizon_storyline_scenario(
            run_id=run_id,
            foundation_namespace=namespace,
            signals_per_batch=signals_per_storyline,
            target_t1_batches=target_t1_batches,
            horizon_start_batch=horizon_start_batch,
        )
    future_validation_count = (
        len(STORYLINES) * max(0, future_validation_signals_per_storyline)
    )
    total_signals = (
        len(STORYLINES) * signals_per_storyline
        + max(0, noise_signals)
        + future_validation_count
    )
    scenario = build_base_scenario(total_signals, namespace=namespace)
    sequences: dict[str, list[dict[str, Any]]] = {}
    signal_index = 0
    gold: list[dict[str, Any]] = []
    for story in STORYLINES:
        signals = []
        for local_index in range(signals_per_storyline):
            signals.append(_make_story_signal(story, signal_index, local_index))
            signal_index += 1
        sequences[f"{story.id}_wave"] = signals
        gold.append(asdict(story))
    if future_validation_signals_per_storyline > 0:
        signals = []
        for story in STORYLINES:
            for local_index in range(future_validation_signals_per_storyline):
                signals.append(
                    _make_future_validation_signal(
                        story,
                        signal_index,
                        local_index,
                    )
                )
                signal_index += 1
        sequences["future_validation"] = signals
    if noise_signals > 0:
        signals = []
        for local_index in range(noise_signals):
            signals.append(_make_noise_signal(signal_index, local_index))
            signal_index += 1
        sequences["background_noise"] = signals
    scenario.signal_sequences = sequences
    scenario.expected_behaviors = [
        "Large T1 batches should create composite situations from cross-source evidence.",
        "Hidden storyline patterns should be compressed into concrete evidence-backed Models.",
        "Future validation signals should retrieve and update earlier compressed memory.",
        "Gold customer/commitment scopes should be represented without broad unscoped memory.",
        "The system should produce useful recommendations or decision pressure for high-risk storylines.",
        "Relationship candidates should not create unbounded review debt.",
        "Noise and alias ambiguity should not dominate durable model creation.",
        "Unobserved transition gaps should become bounded inferred Models, not fabricated facts.",
    ]
    scenario.raw = {
        **dict(scenario.raw or {}),
        "generated": True,
        "benchmark": "storyline_batch",
        "run_id": run_id,
        "foundation_namespace": namespace,
        "signals_per_storyline": signals_per_storyline,
        "future_validation_signals_per_storyline": (
            future_validation_signals_per_storyline
        ),
        "noise_signals": noise_signals,
        "storyline_gold": gold,
    }
    return scenario, gold


def _build_long_horizon_storyline_scenario(
    *,
    run_id: str,
    foundation_namespace: str,
    signals_per_batch: int,
    target_t1_batches: int,
    horizon_start_batch: int = 0,
) -> tuple[Scenario, list[dict[str, Any]]]:
    if target_t1_batches <= 0:
        raise ValueError("target_t1_batches must be positive")
    if signals_per_batch <= 0:
        raise ValueError("signals_per_batch must be positive")
    if horizon_start_batch < 0:
        raise ValueError("horizon_start_batch must be >= 0")
    total_signals = target_t1_batches * signals_per_batch
    scenario = build_base_scenario(total_signals, namespace=foundation_namespace)
    sequences: dict[str, list[dict[str, Any]]] = {}
    gold = [asdict(story) for story in STORYLINES]
    signal_index = horizon_start_batch * signals_per_batch
    story_offsets = {story.id: 0 for story in STORYLINES}
    future_offsets = {story.id: 0 for story in STORYLINES}
    noise_offset = 0
    horizon_end_batch = horizon_start_batch + target_t1_batches
    warmup_batches = min(len(STORYLINES) * 2, horizon_end_batch)

    for batch_index in range(horizon_start_batch):
        sequence_kind = _long_horizon_sequence_kind(batch_index, warmup_batches)
        if sequence_kind == "noise":
            noise_offset += signals_per_batch
        elif sequence_kind == "future_validation":
            for item_index in range(signals_per_batch):
                story = STORYLINES[(batch_index + item_index) % len(STORYLINES)]
                future_offsets[story.id] += 1
        else:
            story = STORYLINES[batch_index % len(STORYLINES)]
            story_offsets[story.id] += signals_per_batch

    for batch_index in range(horizon_start_batch, horizon_end_batch):
        sequence_kind = _long_horizon_sequence_kind(batch_index, warmup_batches)
        signals: list[dict[str, Any]] = []
        if sequence_kind == "noise":
            sequence_name = f"background_noise_wave_{batch_index + 1:03d}"
            for item_index in range(signals_per_batch):
                signal = _make_noise_signal(signal_index, noise_offset)
                _decorate_long_horizon_signal(
                    signal,
                    batch_index=batch_index,
                    sequence_kind=sequence_kind,
                )
                signals.append(signal)
                signal_index += 1
                noise_offset += 1
        elif sequence_kind == "future_validation":
            sequence_name = f"future_validation_wave_{batch_index + 1:03d}"
            for item_index in range(signals_per_batch):
                story = STORYLINES[(batch_index + item_index) % len(STORYLINES)]
                local_index = future_offsets[story.id]
                signal = _make_future_validation_signal(
                    story,
                    signal_index,
                    local_index,
                )
                _decorate_long_horizon_signal(
                    signal,
                    batch_index=batch_index,
                    sequence_kind=sequence_kind,
                )
                signals.append(signal)
                signal_index += 1
                future_offsets[story.id] += 1
        else:
            story = STORYLINES[batch_index % len(STORYLINES)]
            sequence_name = (
                f"{story.id}_horizon_wave_{batch_index + 1:03d}"
            )
            for _item_index in range(signals_per_batch):
                local_index = story_offsets[story.id]
                signal = _make_story_signal(story, signal_index, local_index)
                _decorate_long_horizon_signal(
                    signal,
                    batch_index=batch_index,
                    sequence_kind=sequence_kind,
                )
                signals.append(signal)
                signal_index += 1
                story_offsets[story.id] += 1
        sequences[sequence_name] = signals

    scenario.signal_sequences = sequences
    scenario.expected_behaviors = [
        "Two hundred T1 batches should preserve long-term memory health under repeated company change.",
        "Later waves should use compressed Models and graph context created by earlier waves.",
        "Future validation should update, confirm, or retire earlier inferred memory.",
        "Noise should remain bounded across a long horizon instead of accumulating into durable clutter.",
        "Unobserved transition gaps should become bounded inferred Models, not fabricated facts.",
    ]
    scenario.raw = {
        **dict(scenario.raw or {}),
        "generated": True,
        "benchmark": "storyline_batch",
        "scenario_mode": "long_horizon",
        "run_id": run_id,
        "foundation_namespace": foundation_namespace,
        "signals_per_batch": signals_per_batch,
        "target_t1_batches": target_t1_batches,
        "horizon_start_batch": horizon_start_batch,
        "horizon_end_batch": horizon_end_batch,
        "storyline_gold": gold,
    }
    return scenario, gold


def _long_horizon_sequence_kind(batch_index: int, warmup_batches: int) -> str:
    batch_number = batch_index + 1
    if batch_index >= warmup_batches and batch_number % 10 == 0:
        return "noise"
    if batch_index >= warmup_batches and batch_number % 5 == 0:
        return "future_validation"
    return "storyline"


def _decorate_long_horizon_signal(
    signal: dict[str, Any],
    *,
    batch_index: int,
    sequence_kind: str,
) -> None:
    content = signal.get("content_dict") or {}
    content["long_horizon"] = True
    content["horizon_wave_index"] = batch_index + 1
    content["horizon_day"] = (batch_index // 4) + 1
    content["horizon_sequence_kind"] = sequence_kind
    signal["content_dict"] = content
    signal["content"] = (
        f"{signal.get('content')} "
        f"Long-horizon context: company day {content['horizon_day']}, "
        f"T1 wave {content['horizon_wave_index']}."
    )
    content["text"] = signal["content"]


def _make_story_signal(
    story: StorylineSpec,
    signal_index: int,
    local_index: int,
) -> dict[str, Any]:
    if story.id == _LATENT_BRIDGE_STORYLINE_ID:
        return _make_latent_bridge_signal(story, signal_index, local_index)

    family = story.families[local_index % len(story.families)]
    customer = story.customers[local_index % len(story.customers)]
    commitment = story.commitments[local_index % len(story.commitments)]
    goal = story.goals[local_index % len(story.goals)]
    decision = story.decisions[local_index % len(story.decisions)]
    evidence_angle = [
        "customer source",
        "internal owner update",
        "product telemetry",
        "executive decision context",
        "finance or capacity pressure",
        "contradicting evidence",
    ][local_index % 6]
    keyword = story.expected_terms[local_index % len(story.expected_terms)]
    action = story.expected_actions[local_index % len(story.expected_actions)]
    relation = story.expected_relationships[
        local_index % len(story.expected_relationships)
    ]
    term_sentence = _term_sentence(keyword)
    action_sentence = _action_sentence(action, customer)
    relationship_sentence = _relationship_sentence(
        relation,
        customer=customer,
        commitment=commitment,
        goal=goal,
        decision=decision,
    )
    evidence_fragment = _evidence_fragment(story, family, local_index)
    text = (
        f"{customer}: {evidence_fragment} Evidence angle: {evidence_angle}. "
        f"{term_sentence} The update connects {commitment} with goal '{goal}' "
        f"and decision '{decision}'. {action_sentence} {relationship_sentence} "
        f"Local evidence {local_index + 1} of {story.risk_type} pressure."
    )
    content = {
        "text": text,
        "benchmark": "storyline_batch",
        "family": family,
        "signal_index": signal_index,
        "local_index": local_index,
        "customer_name": customer,
        "commitment_title": commitment,
        "goal_title": goal,
        "decision_title": decision,
        "risk_type": story.risk_type,
        "entity_names": {
            "customers": list(story.customers),
            "commitments": list(story.commitments),
            "goals": list(story.goals),
            "decisions": list(story.decisions),
        },
    }
    return {
        "channel": CHANNEL_BY_FAMILY.get(family, "slack:storyline-benchmark"),
        "actor": ACTOR_BY_FAMILY.get(family, "Maya Chen"),
        "delay_minutes": float(signal_index * 3),
        "content": text,
        "content_dict": content,
        "trust_tier": TRUST_BY_FAMILY.get(family, "inferential"),
        "external_id": f"storyline:{story.id}:{local_index:03d}",
    }


def _make_latent_bridge_signal(
    story: StorylineSpec,
    signal_index: int,
    local_index: int,
) -> dict[str, Any]:
    customer = story.customers[0]
    commitment = story.commitments[0]
    goal = story.goals[0]
    decision = story.decisions[0]
    phase_templates = (
        (
            "before_state",
            "Forecast checkpoint for Northstar Labs: the expansion package is "
            "still blocked because discount exception approval is absent.",
        ),
        (
            "before_state",
            "Finance note for Northstar Labs: pricing policy still says no "
            "nonstandard discount without a recorded approval artifact.",
        ),
        (
            "after_state",
            "Pipeline export for Northstar Labs now shows commit-stage expansion "
            "with exception pricing applied, but no approval record is attached.",
        ),
        (
            "after_state",
            "Billing review shows the nonstandard discount code active for "
            "Northstar Labs while the decision log still has no matching entry.",
        ),
        (
            "gap_review",
            "Ops review asks how Northstar Labs moved from blocked pricing to "
            "approved exception between checkpoints; the sensor trail has a gap.",
        ),
        (
            "gap_review",
            "Forecast review asks for a bounded inferred bridge between the "
            "before and after states, with confidence kept below confirmed fact.",
        ),
    )
    transition_phase, fragment = phase_templates[local_index % len(phase_templates)]
    keyword = story.expected_terms[local_index % len(story.expected_terms)]
    text = (
        f"{customer}: {fragment} The relevant term is {keyword}. "
        f"The update concerns {commitment}, goal '{goal}', and decision "
        f"'{decision}'. The system should preserve observed before/after facts "
        f"separately from any inferred off-sensor transition."
    )
    content = {
        "text": text,
        "benchmark": "storyline_batch",
        "family": story.families[local_index % len(story.families)],
        "signal_index": signal_index,
        "local_index": local_index,
        "transition_phase": transition_phase,
        "latent_bridge_event": True,
        "customer_name": customer,
        "commitment_title": commitment,
        "goal_title": goal,
        "decision_title": decision,
        "risk_type": story.risk_type,
        "entity_names": {
            "customers": list(story.customers),
            "commitments": list(story.commitments),
            "goals": list(story.goals),
            "decisions": list(story.decisions),
        },
    }
    family = str(content["family"])
    return {
        "channel": CHANNEL_BY_FAMILY.get(family, "slack:storyline-benchmark"),
        "actor": ACTOR_BY_FAMILY.get(family, "Maya Chen"),
        "delay_minutes": float(signal_index * 3),
        "content": text,
        "content_dict": content,
        "trust_tier": TRUST_BY_FAMILY.get(family, "inferential"),
        "external_id": f"storyline:{story.id}:{local_index:03d}",
    }


def _evidence_fragment(
    story: StorylineSpec,
    family: str,
    local_index: int,
) -> str:
    fragments = _STORY_EVIDENCE_FRAGMENTS.get(story.id)
    if fragments:
        return fragments[local_index % len(fragments)]
    family_text = family.replace("_", " ")
    return f"{family_text} evidence arrived for {story.customers[0]}."


def _term_sentence(term: str) -> str:
    normalized = term.strip()
    return f"The word on the thread is {normalized}."


def _action_sentence(action: str, customer: str) -> str:
    action = action.strip()
    if "owner" in action:
        return f"The team needs a named owner for {customer}."
    if "allocate" in action or "allocation" in action:
        return f"The next choice is allocation: {action}."
    if "reject" in action or "avoid" in action:
        return f"The safer move is to {action}."
    return f"The practical next step is {action}."


def _relationship_sentence(
    relation: str,
    *,
    customer: str,
    commitment: str,
    goal: str,
    decision: str,
) -> str:
    relation = relation.strip()
    if relation == "supports":
        return f"This supports {goal} through {commitment}."
    if relation == "early_warning_for":
        return f"This is an early warning for {goal} at {customer}."
    if relation == "explains":
        return f"It helps explain why {decision} matters for {customer}."
    if relation == "weakens":
        return f"It weakens confidence that {commitment} is enough on its own."
    if relation == "blocks":
        return f"It blocks progress unless {commitment} is handled."
    if relation == "contributes_to_resolution":
        return f"It contributes to resolving {commitment}."
    if relation == "needs_review":
        return f"It needs review before the graph should treat the link as durable."
    return f"It changes how {commitment} relates to {goal}."


def _make_future_validation_signal(
    story: StorylineSpec,
    signal_index: int,
    local_index: int,
) -> dict[str, Any]:
    if story.id == _LATENT_BRIDGE_STORYLINE_ID:
        return _make_latent_bridge_future_signal(story, signal_index, local_index)

    customer = story.customers[local_index % len(story.customers)]
    commitment = story.commitments[local_index % len(story.commitments)]
    goal = story.goals[local_index % len(story.goals)]
    decision = story.decisions[local_index % len(story.decisions)]
    relation = story.expected_relationships[
        (local_index + 1) % len(story.expected_relationships)
    ]
    keyword = story.expected_terms[(local_index + 2) % len(story.expected_terms)]
    relationship_text = _relationship_sentence(
        relation,
        customer=customer,
        commitment=commitment,
        goal=goal,
        decision=decision,
    )
    text = (
        f"Future validation for {customer}: later operating evidence confirms "
        f"that the earlier {story.risk_type} signals around {commitment} "
        f"changed the outcome of goal '{goal}' and decision '{decision}'. "
        f"The follow-up explicitly asks whether existing compressed memory "
        f"about {keyword} should be updated rather than restated as raw "
        f"observations. {relationship_text}"
    )
    content = {
        "text": text,
        "benchmark": "storyline_batch",
        "family": "future_validation",
        "phase": "future_validation",
        "future_validation_event": True,
        "signal_index": signal_index,
        "local_index": local_index,
        "customer_name": customer,
        "commitment_title": commitment,
        "goal_title": goal,
        "decision_title": decision,
        "risk_type": story.risk_type,
        "expected_relationship_hint": relation,
        "entity_names": {
            "customers": list(story.customers),
            "commitments": list(story.commitments),
            "goals": list(story.goals),
            "decisions": list(story.decisions),
        },
    }
    return {
        "channel": "ops:future-validation",
        "actor": "Maya Chen",
        "delay_minutes": float(signal_index * 3),
        "content": text,
        "content_dict": content,
        "trust_tier": "authoritative",
        "external_id": f"storyline:{story.id}:future:{local_index:03d}",
    }


def _make_latent_bridge_future_signal(
    story: StorylineSpec,
    signal_index: int,
    local_index: int,
) -> dict[str, Any]:
    customer = story.customers[0]
    commitment = story.commitments[0]
    goal = story.goals[0]
    decision = story.decisions[0]
    fragments = (
        "Future validation for Northstar Labs: the sponsor later confirmed an "
        "off-sensor hallway pricing conversation happened after the finance "
        "review and before exception pricing appeared in the pipeline.",
        "Future validation for Northstar Labs: finance confirmed the earlier "
        "missing transition should be upgraded from inferred bridge to confirmed "
        "external decision path, without adding details beyond the new evidence.",
        "Future validation for Northstar Labs: the account owner says the prior "
        "confidence should stay bounded because the approval artifact is still "
        "missing even though the off-sensor cause is now confirmed.",
    )
    text = (
        f"{fragments[local_index % len(fragments)]} Existing compressed memory "
        f"for {commitment}, goal '{goal}', and decision '{decision}' should be "
        f"updated instead of restating raw observations."
    )
    content = {
        "text": text,
        "benchmark": "storyline_batch",
        "family": "future_validation",
        "phase": "future_validation",
        "transition_phase": "future_confirmation",
        "future_validation_event": True,
        "latent_bridge_event": True,
        "signal_index": signal_index,
        "local_index": local_index,
        "customer_name": customer,
        "commitment_title": commitment,
        "goal_title": goal,
        "decision_title": decision,
        "risk_type": story.risk_type,
        "entity_names": {
            "customers": list(story.customers),
            "commitments": list(story.commitments),
            "goals": list(story.goals),
            "decisions": list(story.decisions),
        },
    }
    return {
        "channel": "ops:future-validation",
        "actor": "Maya Chen",
        "delay_minutes": float(signal_index * 3),
        "content": text,
        "content_dict": content,
        "trust_tier": "authoritative",
        "external_id": f"storyline:{story.id}:future:{local_index:03d}",
    }


def _make_noise_signal(signal_index: int, local_index: int) -> dict[str, Any]:
    text = (
        "General operational chatter: lunch logistics, duplicated dashboard "
        "links, and a non-actionable reminder. This should not dominate memory."
    )
    return {
        "channel": "slack:storyline-noise",
        "actor": "Ava Sinclair",
        "delay_minutes": float(signal_index * 3),
        "content": text,
        "content_dict": {
            "text": text,
            "benchmark": "storyline_batch",
            "family": "noise",
            "signal_index": signal_index,
            "local_index": local_index,
            "entity_names": {},
        },
        "trust_tier": "inferential",
        "external_id": f"storyline:noise:{local_index:03d}",
    }


def _read_json_obj(path: Path) -> dict[str, Any]:
    try:
        parsed = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _load_append_context(args: argparse.Namespace) -> dict[str, Any] | None:
    base_run_id = getattr(args, "append_to_run_id", None)
    if not base_run_id:
        return None
    base_report_dir = args.report_root / str(base_run_id)
    base_config = _read_json_obj(base_report_dir / "run_config.json")
    base_summary = _read_json_obj(base_report_dir / "storyline_scores.json")
    base_run_summary = _read_json_obj(base_report_dir / "run_summary.json")
    tenant_id = (
        getattr(args, "append_tenant_id", None)
        or base_summary.get("tenant_id")
        or base_run_summary.get("tenant_id")
    )
    if not tenant_id:
        raise SystemExit(
            "--append-to-run-id requires a tenant id in the base report, "
            "or an explicit --append-tenant-id"
        )
    horizon_start_batch = getattr(args, "horizon_start_batch", None)
    if horizon_start_batch is None:
        horizon_start_batch = base_config.get("target_t1_batches")
    if horizon_start_batch is None:
        signals = int(base_summary.get("signals") or 0)
        per_batch = max(1, int(getattr(args, "signals_per_storyline", 1) or 1))
        horizon_start_batch = signals // per_batch
    horizon_start_batch = int(horizon_start_batch)
    if horizon_start_batch < 0:
        raise SystemExit("--horizon-start-batch must be >= 0")
    return {
        "enabled": True,
        "base_run_id": str(base_run_id),
        "base_report_dir": str(base_report_dir),
        "tenant_id": str(tenant_id),
        "foundation_namespace": str(base_run_id),
        "horizon_start_batch": horizon_start_batch,
        "additional_t1_batches": int(args.target_t1_batches),
        "base_target_t1_batches": int(base_config.get("target_t1_batches") or 0),
        "base_signal_count": int(base_summary.get("signals") or 0),
        "base_seed_status": base_run_summary.get("seed_status") or {},
    }


async def _attach_existing_storyline_tenant_context(
    pool: asyncpg.Pool,
    scenario: Scenario,
    *,
    tenant_id: UUID,
) -> None:
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)",
            tenant_id,
        )
        if not exists:
            raise RuntimeError(f"append tenant does not exist: {tenant_id}")
        scenario.tenant_id = tenant_id
        scenario.base_time = await conn.fetchval(
            """
            SELECT occurred_at
            FROM observations
            WHERE tenant_id = $1
              AND source_channel = 'internal:scenario_loader'
            ORDER BY occurred_at ASC
            LIMIT 1
            """,
            tenant_id,
        ) or await conn.fetchval(
            """
            SELECT MIN(occurred_at)
            FROM observations
            WHERE tenant_id = $1
            """,
            tenant_id,
        ) or datetime.now(timezone.utc)

        actor_rows = await conn.fetch(
            """
            SELECT id, metadata->>'scenario_actor_name' AS name
            FROM actors
            WHERE tenant_id = $1
              AND metadata->>'scenario_actor_name' IS NOT NULL
            """,
            tenant_id,
        )
        scenario.actors = {
            str(row["name"]): row["id"]
            for row in actor_rows
            if row["name"] is not None
        }

        customer_names = [
            str(item["name"])
            for item in scenario.foundation.get("customers") or []
            if item.get("name")
        ]
        customer_rows = await conn.fetch(
            """
            SELECT id, identity, metadata->>'scenario_customer_name' AS scenario_name
            FROM resources
            WHERE tenant_id = $1
              AND kind = 'relational'
              AND (
                identity = ANY($2::text[])
                OR metadata->>'scenario_customer_name' = ANY($2::text[])
              )
            """,
            tenant_id,
            customer_names,
        )
        scenario.customers = {}
        for row in customer_rows:
            key = row["scenario_name"] or row["identity"]
            if key in customer_names:
                scenario.customers[str(key)] = row["id"]

        goal_titles = [
            str(item["title"])
            for item in scenario.foundation.get("goals") or []
            if item.get("title")
        ]
        commitment_titles = [
            str(item["title"])
            for item in scenario.foundation.get("commitments") or []
            if item.get("title")
        ]
        decision_titles = [
            str(item["title"])
            for item in scenario.foundation.get("decisions") or []
            if item.get("title")
        ]
        scenario.goals = await _fetch_title_id_map(conn, "goals", goal_titles, tenant_id)
        scenario.commitments = await _fetch_title_id_map(
            conn,
            "commitments",
            commitment_titles,
            tenant_id,
        )
        scenario.decisions = await _fetch_title_id_map(
            conn,
            "decisions",
            decision_titles,
            tenant_id,
        )

    missing = {
        "customers": sorted(set(customer_names) - set(scenario.customers)),
        "goals": sorted(set(goal_titles) - set(scenario.goals)),
        "commitments": sorted(set(commitment_titles) - set(scenario.commitments)),
        "decisions": sorted(set(decision_titles) - set(scenario.decisions)),
    }
    missing = {key: value for key, value in missing.items() if value}
    if missing:
        raise RuntimeError(
            "append tenant is missing expected storyline foundation ids: "
            + json.dumps(missing, sort_keys=True)
        )


async def _fetch_title_id_map(
    conn: asyncpg.Connection,
    table: str,
    titles: list[str],
    tenant_id: UUID,
) -> dict[str, UUID]:
    if not titles:
        return {}
    if table not in {"goals", "commitments", "decisions"}:
        raise ValueError(f"unsupported title map table: {table}")
    rows = await conn.fetch(
        f"""
        SELECT id, title
        FROM {table}
        WHERE tenant_id = $1
          AND title = ANY($2::text[])
        ORDER BY created_at ASC
        """,
        tenant_id,
        titles,
    )
    out: dict[str, UUID] = {}
    for row in rows:
        out.setdefault(str(row["title"]), row["id"])
    return out


async def _count_storyline_benchmark_observations(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'benchmark' = 'storyline_batch'
            """,
            tenant_id,
        )
    return int(value or 0)


async def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id
    if not run_id:
        if getattr(args, "append_to_run_id", None):
            run_id = (
                f"{args.append_to_run_id}-append-{args.target_t1_batches}-"
                f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
            )
        else:
            run_id = (
                "storyline-batch-"
                f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
            )
    report_dir = args.report_root / run_id
    append_context = _load_append_context(args)
    foundation_namespace = (
        str(append_context["foundation_namespace"]) if append_context else None
    )
    horizon_start_batch = (
        int(append_context["horizon_start_batch"]) if append_context else 0
    )
    scenario, gold = build_storyline_scenario(
        run_id=run_id,
        foundation_namespace=foundation_namespace,
        signals_per_storyline=args.signals_per_storyline,
        noise_signals=args.noise_signals,
        future_validation_signals_per_storyline=(
            args.future_validation_signals_per_storyline
        ),
        target_t1_batches=args.target_t1_batches,
        horizon_start_batch=horizon_start_batch,
    )
    run_config = _run_config(args, run_id, append_context=append_context)
    _write_build_artifacts(report_dir, scenario, gold, run_config)
    if args.mode == "build-only":
        return {
            "mode": args.mode,
            "run_id": run_id,
            "report_dir": str(report_dir),
            "signals": _signal_count(scenario),
            "storylines": len(STORYLINES),
        }

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set")
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
    )
    embedder = OllamaClient(OllamaConfig.from_env())
    started = time.monotonic()
    tenant_id: UUID | None = None
    observation_ids: list[UUID] = []
    waves: list[dict[str, Any]] = []
    seed_status: dict[str, Any] = {
        "requested_models": args.seed_models,
        "families": args.seed_families,
        "models": 0,
    }
    post_commit_status: dict[str, Any] = {}
    topology_status: dict[str, Any] = {"status": "skipped"}
    try:
        if not args.skip_migrations:
            async with pool.acquire() as conn:
                await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        if append_context:
            tenant_id = UUID(str(append_context["tenant_id"]))
            await _attach_existing_storyline_tenant_context(
                pool,
                scenario,
                tenant_id=tenant_id,
            )
            seed_status = {
                "requested_models": 0,
                "families": 0,
                "models": 0,
                "skipped": "append_to_existing_tenant",
                "base_seed_status": append_context.get("base_seed_status") or {},
            }
            print(
                f"tenant={tenant_id} run_id={run_id} "
                f"append_to={append_context['base_run_id']} "
                f"horizon_start_batch={append_context['horizon_start_batch']}",
                flush=True,
            )
        else:
            await materialize(scenario, pool=pool)
            if scenario.tenant_id is None:
                raise RuntimeError("scenario materialize did not set tenant_id")
            tenant_id = scenario.tenant_id
            print(f"tenant={tenant_id} run_id={run_id}", flush=True)
            await _insert_extra_aliases(scenario, alias_repo)

        if args.seed_models and not append_context:
            from scripts.run_incremental_feedback_loop_stress import _seed_company

            seeded = await _seed_company(
                pool,
                tenant_id=tenant_id,
                families=args.seed_families,
                total_models=args.seed_models,
            )
            seed_status = {
                "requested_models": args.seed_models,
                "families": args.seed_families,
                "models": seeded.total_models,
                "insert_ms": round(seeded.insert_ms, 3),
                "sidecars": seeded.sidecars,
            }
            print(f"seed_status={json.dumps(seed_status, sort_keys=True)}", flush=True)

        provider = _build_cached_provider()
        worker = ThinkWorker(
            pool,
            config=WorkerConfig(
                poll_batch=max(2, args.worker_poll_batch),
                max_concurrency_per_tenant=1,
                tenant_filter=tenant_id,
                worker_id=f"storyline-{run_id}",
                # Cost-plan §2.3 A/B arm: window=0 disables T1 batching so the
                # unbatched arm drains each event_arrival trigger as a single run.
                t1_batch_window_s=(
                    0.0 if args.unbatched_run else args.t1_batch_window_s
                ),
                t1_batch_min_size=args.t1_batch_min_size,
                t1_batch_max_size=args.t1_batch_max_size,
                downstream_batch_window_s=args.downstream_batch_window_s,
                downstream_batch_min_size=args.downstream_batch_min_size,
                t2_batch_max_size=args.t2_batch_max_size,
                t4_batch_max_size=args.t4_batch_max_size,
                run_timeout_s=args.run_timeout,
            ),
            llm_provider=provider,
            embedder=embedder,
        )

        all_sequences = list(scenario.signal_sequences.items())
        offset = 0
        for wave_index, (sequence_name, signals) in enumerate(all_sequences, start=1):
            if sequence_name.startswith("background_noise") and args.skip_noise_think:
                offset += len(signals)
                continue
            print(
                f"wave={wave_index} sequence={sequence_name} "
                f"signals={len(signals)}",
                flush=True,
            )
            batch_ids = await inject_generated_signals(
                scenario,
                pool=pool,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                run_id=run_id,
                progress_every=0,
                offset=offset,
                limit=len(signals),
            )
            offset += len(signals)
            observation_ids.extend(batch_ids)
            enqueued = await enqueue_t1_for_observations(
                pool,
                tenant_id=tenant_id,
                observation_ids=batch_ids,
                limit=len(batch_ids),
                run_id=run_id,
            )
            wave = {
                "wave": wave_index,
                "sequence": sequence_name,
                "signals": len(signals),
                "enqueued_t1": enqueued,
            }
            if args.unbatched_run:
                t1_batch = await _process_t1_unbatched(
                    pool,
                    worker,
                    tenant_id=tenant_id,
                    force_window_elapsed_s=args.t1_batch_window_s + 1.0,
                )
            else:
                t1_batch = await _process_one_t1_batch(
                    pool,
                    worker,
                    tenant_id=tenant_id,
                    force_window_elapsed_s=args.t1_batch_window_s + 1.0,
                )
            wave["t1_batch"] = t1_batch
            if args.downstream_steps_per_wave > 0:
                wave["downstream"] = await _drain_downstream_limited(
                    pool,
                    worker,
                    tenant_id=tenant_id,
                    steps=args.downstream_steps_per_wave,
                    force_window_elapsed_s=args.downstream_batch_window_s + 1.0,
                )
            wave["queue_counts"] = await _queue_counts(pool, tenant_id)
            waves.append(wave)
            _write_json(report_dir / "waves.json", waves)

        post_commit_status = await drain_post_commit_actions(
            pool,
            tenant_id=tenant_id,
            timeout_seconds=args.post_commit_timeout,
        )
        if not args.skip_topology_optimizer:
            topology_status = await drain_topology_optimizer(
                pool,
                tenant_id=tenant_id,
                timeout_seconds=args.topology_optimizer_timeout,
                batch_size=args.topology_optimizer_batch_size,
                lookback_hours=args.topology_optimizer_lookback_hours,
            )

        model_summary = await collect_model_layer_report(
            pool,
            tenant_id=tenant_id,
            run_id=run_id,
            report_dir=report_dir,
            scenario=scenario,
            observation_ids=observation_ids,
            think_status="storyline_batches_processed",
            run_config=run_config,
            seed_status=seed_status,
            processing_waves=waves,
            post_commit_status=post_commit_status,
            topology_optimizer_status=topology_status,
            elapsed_seconds=time.monotonic() - started,
        )
        model_summary["future_validation_events"] = (
            await _count_future_validation_events(pool, tenant_id=tenant_id)
        )
        model_summary["edge_lifecycle"] = await _collect_edge_lifecycle_report(
            pool,
            tenant_id=tenant_id,
        )
        if append_context:
            cumulative_signal_count = await _count_storyline_benchmark_observations(
                pool,
                tenant_id=tenant_id,
            )
            model_summary["append"] = {
                **append_context,
                "additional_signal_count": len(observation_ids),
                "cumulative_signal_count": cumulative_signal_count,
                "horizon_end_batch": horizon_start_batch + args.target_t1_batches,
            }
            model_summary["additional_signal_count"] = len(observation_ids)
            model_summary["run_observation_count"] = model_summary.get(
                "observation_count"
            )
            model_summary["signal_count"] = cumulative_signal_count
            (report_dir / "model_layer_summary.md").write_text(
                _render_model_layer_markdown(model_summary)
            )
        _write_json(report_dir / "run_summary.json", model_summary)
        scores = await score_storylines(
            pool,
            tenant_id=tenant_id,
            scenario=scenario,
            gold_specs=STORYLINES,
            enable_thesis_judge=args.enable_thesis_judge,
            thesis_judge_limit=args.thesis_judge_limit,
        )
        benchmark_summary = _benchmark_summary(
            model_summary=model_summary,
            storyline_scores=scores,
            waves=waves,
            elapsed_seconds=time.monotonic() - started,
        )
        _write_json(report_dir / "storyline_scores.json", benchmark_summary)
        (report_dir / "benchmark_summary.md").write_text(
            _render_benchmark_markdown(benchmark_summary)
        )
        print(f"report_dir={report_dir}", flush=True)
        return benchmark_summary
    finally:
        if args.cleanup and tenant_id is not None:
            from scripts.run_20000_model_4000_signal_company_probe import (
                _cleanup_probe_tenant,
            )

            cleanup = await _cleanup_probe_tenant(pool, tenant_id)
            print(f"cleanup={json.dumps(cleanup, sort_keys=True)}", flush=True)
        await pool.close()


async def _process_one_t1_batch(
    pool: asyncpg.Pool,
    worker: ThinkWorker,
    *,
    tenant_id: UUID,
    force_window_elapsed_s: float,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE think_trigger_queue
                SET enqueued_at = now() - ($2 || ' seconds')::interval
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND trigger_kind = 'T1'
                  AND trigger_subkind = 'event_arrival'
                """,
                tenant_id,
                str(max(0.0, force_window_elapsed_s)),
            )
            rows = await worker._create_t1_batch_rows(conn, available_slots=1)
    if len(rows) != 1:
        raise RuntimeError(f"expected one T1 batch row, got {len(rows)}")
    started = time.monotonic()
    await worker._dispatch_trigger(rows[0])
    run = await _run_for_trigger(pool, rows[0]["id"])
    payload = rows[0]["payload"] or {}
    return {
        "trigger_id": str(rows[0]["id"]),
        "member_count": len(payload.get("batch_member_trigger_ids") or []),
        "observation_count": len(payload.get("batch_observation_ids") or []),
        "elapsed_s": round(time.monotonic() - started, 3),
        "run": run,
    }


def _merge_numeric_tree(items: list[Any]) -> dict[str, Any]:
    """Deep-merge a list of `ops_applied`-shaped dicts: sum numbers, OR bools,
    recurse into dicts, last-wins for everything else. Lets the unbatched arm
    present an aggregate run the existing report pipeline can read unchanged."""
    dicts = [it for it in items if isinstance(it, dict)]
    if not dicts:
        return {}
    out: dict[str, Any] = {}
    keys: set[str] = set()
    for it in dicts:
        keys.update(it.keys())
    for k in keys:
        vals = [it[k] for it in dicts if k in it]
        if all(isinstance(v, bool) for v in vals):
            out[k] = any(vals)
        elif all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
            out[k] = sum(vals)
        elif all(isinstance(v, dict) for v in vals):
            out[k] = _merge_numeric_tree(vals)
        else:
            out[k] = vals[-1]
    return out


def _aggregate_runs(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Collapse N single-trigger runs into one wave-level run dict shaped like
    `_run_for_trigger`'s output, so `_wave_stats`/report code is unchanged.
    `status='success'` only when every single succeeded."""
    runs = [r for r in runs if r]
    if not runs:
        return None
    return {
        "id": runs[0].get("id"),
        "trigger_kind": "T1",
        "status": (
            "success" if all(r.get("status") == "success" for r in runs) else "error"
        ),
        "error": next((r.get("error") for r in runs if r.get("error")), None),
        "retrieval_model_count": sum(
            int(r.get("retrieval_model_count") or 0) for r in runs
        ),
        "retrieval_observation_count": sum(
            int(r.get("retrieval_observation_count") or 0) for r in runs
        ),
        "llm_latency_ms": sum(int(r.get("llm_latency_ms") or 0) for r in runs),
        "validation_error_count": sum(
            int(r.get("validation_error_count") or 0) for r in runs
        ),
        "ops_applied": _merge_numeric_tree([r.get("ops_applied") for r in runs]),
    }


async def _process_t1_unbatched(
    pool: asyncpg.Pool,
    worker: ThinkWorker,
    *,
    tenant_id: UUID,
    force_window_elapsed_s: float,
) -> dict[str, Any]:
    """Cost-plan §2.3 unbatched arm: drain the wave's pending T1 event_arrival
    triggers as individual single-trigger runs (T1 batching is disabled via
    window=0 on the worker config). Returns the same shape as
    `_process_one_t1_batch`, aggregated, plus a `singles` list and
    `unbatched=True` for downstream analysis."""
    async with pool.acquire() as conn:
        trigger_ids = [
            r["id"]
            for r in await conn.fetch(
                """
                SELECT id FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND trigger_kind = 'T1'
                  AND trigger_subkind = 'event_arrival'
                ORDER BY enqueued_at ASC
                """,
                tenant_id,
            )
        ]
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE think_trigger_queue
                SET enqueued_at = now() - ($2 || ' seconds')::interval
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND trigger_kind = 'T1'
                  AND trigger_subkind = 'event_arrival'
                """,
                tenant_id,
                str(max(0.0, force_window_elapsed_s)),
            )
    started = time.monotonic()
    guard = 0
    while trigger_ids:
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
                """
                SELECT COUNT(*)::bigint FROM think_trigger_queue
                WHERE id = ANY($1::uuid[]) AND completed_at IS NULL
                """,
                trigger_ids,
            )
        if int(pending or 0) == 0:
            break
        await worker._poll_and_dispatch()
        if worker._in_flight:
            await asyncio.gather(*list(worker._in_flight), return_exceptions=False)
        guard += 1
        if guard > len(trigger_ids) + 5:
            break
    singles: list[dict[str, Any]] = []
    for tid in trigger_ids:
        run = await _run_for_trigger(pool, tid)
        if run is not None:
            singles.append({"trigger_id": str(tid), "run": run})
    return {
        "unbatched": True,
        "trigger_id": str(trigger_ids[0]) if trigger_ids else None,
        "member_count": len(trigger_ids),
        "observation_count": len(trigger_ids),
        "elapsed_s": round(time.monotonic() - started, 3),
        "run": _aggregate_runs([s["run"] for s in singles]),
        "singles": singles,
    }


async def _drain_downstream_limited(
    pool: asyncpg.Pool,
    worker: ThinkWorker,
    *,
    tenant_id: UUID,
    steps: int,
    force_window_elapsed_s: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for step in range(max(0, steps)):
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND (
                    (trigger_kind = 'T2' AND trigger_subkind = 'belief_updated')
                    OR (trigger_kind = 'T4'
                        AND trigger_subkind = 'latent_relationship_candidate')
                  )
                """,
                tenant_id,
            )
            if int(pending or 0) == 0:
                break
            await conn.execute(
                """
                UPDATE think_trigger_queue
                SET enqueued_at = now() - ($2 || ' seconds')::interval
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND (
                    (trigger_kind = 'T2' AND trigger_subkind = 'belief_updated')
                    OR (trigger_kind = 'T4'
                        AND trigger_subkind = 'latent_relationship_candidate')
                  )
                """,
                tenant_id,
                str(max(0.0, force_window_elapsed_s)),
            )
        before = time.monotonic()
        await worker._poll_and_dispatch()
        if worker._in_flight:
            await asyncio.gather(*list(worker._in_flight), return_exceptions=False)
        out.append(
            {
                "step": step + 1,
                "elapsed_s": round(time.monotonic() - before, 3),
                "queue_counts": await _queue_counts(pool, tenant_id),
            }
        )
    return out


async def _run_for_trigger(pool: asyncpg.Pool, trigger_id: UUID) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, trigger_kind, status, error, retrieval_model_count,
                   retrieval_observation_count, llm_latency_ms,
                   validation_error_count, ops_applied
            FROM think_runs
            WHERE trigger_id = $1
            ORDER BY started_at DESC
            LIMIT 1
            """,
            trigger_id,
        )
    return _record_to_dict(row) if row else None


async def _count_future_validation_events(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'benchmark' = 'storyline_batch'
              AND content->>'phase' = 'future_validation'
            """,
            tenant_id,
        )
    return int(value or 0)


async def _collect_edge_lifecycle_report(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              COUNT(*)::bigint AS total_edges,
              COUNT(*) FILTER (WHERE status = 'active')::bigint AS active_edges,
              COUNT(*) FILTER (
                WHERE status != 'active' OR review_status = 'retired'
              )::bigint AS retired_or_inert_edges,
              COUNT(*) FILTER (WHERE review_status = 'accepted')::bigint
                AS accepted_edges,
              COUNT(*) FILTER (WHERE review_status = 'candidate')::bigint
                AS candidate_edges,
              COUNT(*) FILTER (WHERE review_status = 'needs_review')::bigint
                AS needs_review_edges,
              COUNT(*) FILTER (WHERE review_status = 'rejected')::bigint
                AS rejected_edges,
              COUNT(*) FILTER (WHERE confirmed_count > 1)::bigint
                AS reconfirmed_edges,
              COALESCE(SUM(GREATEST(confirmed_count - 1, 0)), 0)::bigint
                AS reconfirmation_events,
              COUNT(DISTINCT edge_kind)::bigint AS distinct_edge_kinds
            FROM model_edges
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        proposal_table = await conn.fetchval(
            "SELECT to_regclass('public.relationship_ontology_proposals')"
        )
        proposal_counts: dict[str, int] = {}
        accepted_edge_kind_distribution = await _fetch_distribution(
            conn,
            """
            SELECT edge_kind AS key, COUNT(*)::bigint AS value
            FROM model_edges
            WHERE tenant_id = $1
              AND status = 'active'
              AND review_status = 'accepted'
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        if proposal_table is not None:
            proposal_counts = await _fetch_distribution(
                conn,
                """
                SELECT status AS key, COUNT(*)::bigint AS value
                FROM relationship_ontology_proposals
                WHERE tenant_id = $1
                GROUP BY 1
                ORDER BY 2 DESC, 1 ASC
                """,
                tenant_id,
            )
    report = _record_to_dict(row) if row else {}
    report["accepted_edge_kind_distribution"] = accepted_edge_kind_distribution
    report["ontology_proposal_status_distribution"] = proposal_counts
    report["ontology_proposals"] = sum(proposal_counts.values())
    return report


async def _queue_counts(pool: asyncpg.Pool, tenant_id: UUID) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT trigger_kind || ':' || COALESCE(trigger_subkind, '') AS kind,
                   COUNT(*) FILTER (WHERE completed_at IS NULL)::int AS pending,
                   COUNT(*) FILTER (WHERE completed_at IS NOT NULL)::int AS done,
                   COUNT(*)::int AS total
            FROM think_trigger_queue
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 1
            """,
            tenant_id,
        )
    return [_record_to_dict(row) for row in rows]


async def score_storylines(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    scenario: Scenario,
    gold_specs: tuple[StorylineSpec, ...],
    enable_thesis_judge: bool = False,
    thesis_judge_limit: int = 0,
) -> list[StorylineScore]:
    async with pool.acquire() as conn:
        model_rows = await conn.fetch(
            """
            SELECT id, proposition_kind, proposition, "natural", scope_entities,
                   supporting_event_ids, status, confidence, confidence_at_assertion
            FROM models
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        edge_rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind, review_status
            FROM model_edges
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        candidate_rows = await conn.fetch(
            """
            SELECT member_model_ids, review_status, candidate_kind, edge_kind
            FROM relationship_candidates
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        observation_rows = await conn.fetch(
            """
            SELECT id, external_id, content
            FROM observations
            WHERE tenant_id = $1
              AND content->>'benchmark' = 'storyline_batch'
            """,
            tenant_id,
        )

    models = [_record_to_dict(row) for row in model_rows]
    edges = [_record_to_dict(row) for row in edge_rows]
    candidates = [_record_to_dict(row) for row in candidate_rows]
    observations = [_record_to_dict(row) for row in observation_rows]

    observations_by_story: dict[str, set[str]] = {}
    future_observations_by_story: dict[str, set[str]] = {}
    transition_phase_by_observation: dict[str, str] = {}
    for observation in observations:
        content = _json_obj(observation.get("content"))
        story_id = _story_id_from_external_id(observation.get("external_id"))
        if isinstance(story_id, str):
            observation_id = str(observation["id"])
            observations_by_story.setdefault(story_id, set()).add(observation_id)
            if content.get("phase") == "future_validation":
                future_observations_by_story.setdefault(story_id, set()).add(
                    observation_id
                )
            transition_phase = content.get("transition_phase")
            if isinstance(transition_phase, str):
                transition_phase_by_observation[observation_id] = transition_phase

    thesis_judge: Any | None = None
    if enable_thesis_judge:
        from benchmarks.fyralis_eval.judge import LLMAnswerJudge

        thesis_judge = LLMAnswerJudge(name=_THESIS_JUDGE_NAME)
    thesis_judged = 0

    scores: list[StorylineScore] = []
    for spec in gold_specs:
        scope_refs = _scope_refs_for_story(scenario, spec)
        story_observations = observations_by_story.get(spec.id, set())
        future_observations = future_observations_by_story.get(spec.id, set())
        relevant_models = [
            model for model in models
            if _model_matches_story(model, scope_refs, story_observations)
        ]
        relevant_model_ids = {str(model["id"]) for model in relevant_models}
        text_blob = "\n".join(_model_text(model) for model in relevant_models).lower()
        keyword_hits = [
            term for term in spec.expected_terms
            if term.lower() in text_blob
        ]
        missing_keywords = [
            term for term in spec.expected_terms
            if term.lower() not in text_blob
        ]
        evidence_supported = [
            model for model in relevant_models
            if set(map(str, model.get("supporting_event_ids") or []))
            & story_observations
        ]
        situation_count = sum(
            1 for model in relevant_models if _is_situation_model(model)
        )
        recommendation_count = sum(
            1 for model in relevant_models if _is_recommendation_model(model)
        )
        latent_pattern_assessments = [
            _latent_pattern_assessment(model, spec)
            for model in relevant_models
        ]
        best_assessment = max(
            latent_pattern_assessments,
            key=lambda assessment: assessment["coverage"],
            default={
                "coverage": 0.0,
                "hits": [],
                "missing": [
                    _latent_group_label(group)
                    for group in spec.latent_pattern_groups
                ],
            },
        )
        latent_pattern_models = [
            model for model, assessment in zip(
                relevant_models,
                latent_pattern_assessments,
                strict=False,
            )
            if assessment["coverage"] >= 0.6
            and (
                _is_situation_model(model)
                or _is_recommendation_model(model)
                or _is_concern_model(model)
            )
        ]
        latent_pattern_evidence_supported = [
            model for model in latent_pattern_models
            if set(map(str, model.get("supporting_event_ids") or []))
            & story_observations
        ]
        bridge_model_count = 0
        bridge_transition_supported_count = 0
        bridge_future_confirmed_count = 0
        unsupported_bridge_specific_claim_count = 0
        bridge_epistemic_marker_hits: set[str] = set()
        bridge_forbidden_detail_hits: set[str] = set()
        bridge_score = 0.0
        if spec.id == _LATENT_BRIDGE_STORYLINE_ID:
            for model in relevant_models:
                bridge_assessment = _latent_bridge_assessment(model)
                if bridge_assessment["coverage"] < 0.5:
                    continue
                bridge_model_count += 1
                support_ids = set(map(str, model.get("supporting_event_ids") or []))
                support_phases = {
                    transition_phase_by_observation.get(observation_id)
                    for observation_id in support_ids
                }
                transition_supported = (
                    "before_state" in support_phases
                    and (
                        "after_state" in support_phases
                        or "gap_review" in support_phases
                    )
                )
                future_confirmed = (
                    "future_confirmation" in support_phases
                    or bool(support_ids & future_observations)
                )
                if transition_supported:
                    bridge_transition_supported_count += 1
                if future_confirmed:
                    bridge_future_confirmed_count += 1
                bridge_epistemic_marker_hits.update(
                    bridge_assessment["epistemic_hits"]
                )
                bridge_forbidden_detail_hits.update(
                    bridge_assessment["forbidden_detail_hits"]
                )
                if (
                    bridge_assessment["forbidden_detail_hits"]
                    and not future_confirmed
                ):
                    unsupported_bridge_specific_claim_count += 1
            bridge_score = _clamp01(
                0.25 * (1.0 if bridge_model_count else 0.0)
                + 0.25 * (
                    _ratio(bridge_transition_supported_count, bridge_model_count)
                    if bridge_model_count else 0.0
                )
                + 0.20 * _clamp01(len(bridge_epistemic_marker_hits) / 3.0)
                + 0.15 * (
                    _ratio(bridge_future_confirmed_count, bridge_model_count)
                    if bridge_model_count else 0.0
                )
                + 0.15 * (
                    1.0
                    - _ratio(unsupported_bridge_specific_claim_count, bridge_model_count)
                    if bridge_model_count else 0.0
                )
            )
        scoped_edge_count = sum(
            1 for edge in edges
            if str(edge.get("source_model_id")) in relevant_model_ids
            or str(edge.get("target_model_id")) in relevant_model_ids
        )
        relevant_edge_kinds = {
            str(edge.get("edge_kind"))
            for edge in edges
            if (
                str(edge.get("source_model_id")) in relevant_model_ids
                or str(edge.get("target_model_id")) in relevant_model_ids
            )
            and edge.get("review_status") == "accepted"
            and edge.get("edge_kind")
        }
        expected_edge_kinds = {
            relation for relation in spec.expected_relationships
            if relation != "needs_review"
        }
        edge_kind_hits = sorted(expected_edge_kinds & relevant_edge_kinds)
        missing_edge_kinds = sorted(expected_edge_kinds - relevant_edge_kinds)
        review_candidates = [
            candidate for candidate in candidates
            if set(map(str, candidate.get("member_model_ids") or []))
            & relevant_model_ids
        ]
        accepted = sum(
            1 for candidate in review_candidates
            if candidate.get("review_status") == "accepted"
        )
        needs_review = sum(
            1 for candidate in review_candidates
            if candidate.get("review_status") == "needs_review"
        )
        keyword_score = (
            len(keyword_hits) / len(spec.expected_terms)
            if spec.expected_terms else 0.0
        )
        evidence_score = min(1.0, len(evidence_supported) / 3.0)
        situation_score = 1.0 if situation_count else 0.0
        recommendation_score = 1.0 if recommendation_count else 0.0
        edge_kind_score = _ratio(len(edge_kind_hits), len(expected_edge_kinds))
        edge_presence_score = 1.0 if scoped_edge_count else 0.0
        edge_score = 0.35 * edge_presence_score + 0.65 * edge_kind_score
        latent_pattern_best_coverage = float(best_assessment["coverage"])
        latent_pattern_score = (
            0.60 * latent_pattern_best_coverage
            + 0.25 * (1.0 if latent_pattern_models else 0.0)
            + 0.15 * (
                min(1.0, len(latent_pattern_evidence_supported) / 2.0)
                if latent_pattern_models else 0.0
            )
        )
        review_penalty = min(0.25, needs_review / 40.0)
        score = max(
            0.0,
            (
                0.25 * latent_pattern_score
                + 0.25 * keyword_score
                + 0.15 * evidence_score
                + 0.15 * situation_score
                + 0.10 * recommendation_score
                + 0.10 * edge_score
                - review_penalty
            ),
        )
        if spec.id == _LATENT_BRIDGE_STORYLINE_ID:
            score = max(score, bridge_score)
        calibration_samples = _storyline_calibration_samples(
            spec=spec,
            relevant_models=relevant_models,
            story_observations=story_observations,
            future_observations=future_observations,
        )
        notes: list[str] = []
        if not latent_pattern_models:
            notes.append(
                "No concrete model captured enough hidden-pattern facets."
            )
        elif not latent_pattern_evidence_supported:
            notes.append(
                "Latent-pattern model was not backed by storyline evidence."
            )
        if not situation_count:
            notes.append("No composite/situation model detected for storyline.")
        if not recommendation_count:
            notes.append("No recommendation/action model detected for storyline.")
        if missing_edge_kinds:
            notes.append(
                "Missing expected accepted edge kinds: "
                + ", ".join(missing_edge_kinds)
            )
        if needs_review > accepted * 3 and needs_review >= 5:
            notes.append("Review debt dominates accepted relationship candidates.")
        if spec.id == _LATENT_BRIDGE_STORYLINE_ID:
            if bridge_model_count == 0:
                notes.append(
                    "No bounded inferred bridge model detected for unobserved transition."
                )
            elif bridge_transition_supported_count == 0:
                notes.append(
                    "Bridge model was not supported by both before and after/gap states."
                )
            if unsupported_bridge_specific_claim_count:
                notes.append(
                    "Bridge model invented specific off-sensor details before validation."
                )
        thesis_judge_score: float | None = None
        thesis_judge_correct: bool | None = None
        thesis_judge_rationale: str | None = None
        thesis_judge_metadata: dict[str, Any] = {}
        if thesis_judge is not None and (
            thesis_judge_limit <= 0 or thesis_judged < thesis_judge_limit
        ):
            judge_result = await _judge_storyline_thesis(
                thesis_judge,
                tenant_id=tenant_id,
                spec=spec,
                relevant_models=relevant_models,
            )
            thesis_judged += 1
            thesis_judge_score = round(judge_result.score, 4)
            thesis_judge_correct = bool(judge_result.correct)
            thesis_judge_rationale = judge_result.rationale
            thesis_judge_metadata = dict(judge_result.metadata)
        scores.append(
            StorylineScore(
                storyline_id=spec.id,
                title=spec.title,
                signal_count=len(story_observations),
                relevant_model_count=len(relevant_models),
                evidence_supported_model_count=len(evidence_supported),
                keyword_hits=keyword_hits,
                missing_keywords=missing_keywords,
                situation_model_count=situation_count,
                recommendation_model_count=recommendation_count,
                scoped_edge_count=scoped_edge_count,
                edge_kind_hits=edge_kind_hits,
                missing_edge_kinds=missing_edge_kinds,
                review_candidate_count=len(review_candidates),
                accepted_candidate_count=accepted,
                needs_review_candidate_count=needs_review,
                latent_pattern_score=round(latent_pattern_score, 4),
                latent_pattern_model_count=len(latent_pattern_models),
                latent_pattern_evidence_supported_model_count=(
                    len(latent_pattern_evidence_supported)
                ),
                latent_pattern_best_coverage=round(latent_pattern_best_coverage, 4),
                latent_pattern_group_hits=list(best_assessment["hits"]),
                missing_latent_pattern_groups=list(best_assessment["missing"]),
                latent_pattern_model_ids=[
                    str(model["id"]) for model in latent_pattern_models[:5]
                ],
                score=round(score, 4),
                inferred_bridge_model_count=bridge_model_count,
                inferred_bridge_transition_supported_model_count=(
                    bridge_transition_supported_count
                ),
                inferred_bridge_future_confirmed_model_count=(
                    bridge_future_confirmed_count
                ),
                unsupported_bridge_specific_claim_count=(
                    unsupported_bridge_specific_claim_count
                ),
                bridge_epistemic_marker_hits=sorted(bridge_epistemic_marker_hits),
                bridge_forbidden_detail_hits=sorted(bridge_forbidden_detail_hits),
                thesis_judge_score=thesis_judge_score,
                thesis_judge_correct=thesis_judge_correct,
                thesis_judge_rationale=thesis_judge_rationale,
                thesis_judge_metadata=thesis_judge_metadata,
                calibration_samples=calibration_samples,
                notes=notes,
            )
        )
    return scores


async def _judge_storyline_thesis(
    judge: Any,
    *,
    tenant_id: UUID,
    spec: StorylineSpec,
    relevant_models: list[dict[str, Any]],
) -> Any:
    from benchmarks.adapters.base import BenchmarkQuery

    predicted_answer = _thesis_recovery_prediction_text(relevant_models)
    query = BenchmarkQuery(
        query_id=f"storyline-thesis:{spec.id}",
        tenant_id=str(tenant_id),
        query_text=(
            "Does this belief stream recover the benchmark storyline thesis? "
            "Credit the answer only when the main causal operating pattern is "
            "present; exact wording is not required."
        ),
        query_type="storyline_thesis_recovery",
        gold_answer=spec.thesis,
        metadata={
            "benchmark": "storyline_batch",
            "storyline_id": spec.id,
            "storyline_title": spec.title,
            "judge": _THESIS_JUDGE_NAME,
        },
    )
    return await judge.judge_async(
        query=query,
        expected_answer=spec.thesis,
        predicted_answer=predicted_answer,
    )


def _thesis_recovery_prediction_text(
    relevant_models: list[dict[str, Any]],
    *,
    max_models: int = 16,
    max_chars_per_model: int = 1200,
) -> str:
    excerpts: list[str] = []
    for index, model in enumerate(relevant_models[:max_models], start=1):
        text = " ".join(_model_text(model).split())
        if not text:
            continue
        excerpts.append(f"[{index}] {text[:max_chars_per_model]}")
    return "\n".join(excerpts) or "No relevant models were recovered."


def _scope_refs_for_story(
    scenario: Scenario,
    spec: StorylineSpec,
) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    for name in spec.customers:
        if name in scenario.customers:
            refs.add(("customer", str(scenario.customer_id(name))))
    for title in spec.commitments:
        if title in scenario.commitments:
            refs.add(("commitment", str(scenario.commitment_id(title))))
    for title in spec.goals:
        if title in scenario.goals:
            refs.add(("goal", str(scenario.goal_id(title))))
    for title in spec.decisions:
        if title in scenario.decisions:
            refs.add(("decision", str(scenario.decision_id(title))))
    return refs


def _model_matches_story(
    model: dict[str, Any],
    scope_refs: set[tuple[str, str]],
    story_observations: set[str],
) -> bool:
    supporting = set(map(str, model.get("supporting_event_ids") or []))
    if supporting & story_observations:
        return True
    for entity in _json_list(model.get("scope_entities")):
        if not isinstance(entity, dict):
            continue
        key = (str(entity.get("type")), str(entity.get("id")))
        if key in scope_refs:
            return True
    return False


def _model_text(model: dict[str, Any]) -> str:
    return (
        str(model.get("natural") or "")
        + "\n"
        + json.dumps(model.get("proposition") or {}, sort_keys=True, default=str)
    )


def _storyline_calibration_samples(
    *,
    spec: StorylineSpec,
    relevant_models: list[dict[str, Any]],
    story_observations: set[str],
    future_observations: set[str],
) -> list[dict[str, Any]]:
    if not future_observations:
        return []
    samples: list[dict[str, Any]] = []
    prior_observations = story_observations - future_observations
    for model in relevant_models:
        support_ids = set(map(str, model.get("supporting_event_ids") or []))
        if not (support_ids & prior_observations):
            continue
        confidence = _coerce_confidence(
            model.get("confidence_at_assertion", model.get("confidence"))
        )
        if confidence is None:
            continue
        assessment = _latent_pattern_assessment(model, spec)
        keyword_hits = [
            term for term in spec.expected_terms
            if term.lower() in _model_text(model).lower()
        ]
        future_touched = bool(support_ids & future_observations)
        outcome = 1.0 if (
            future_touched
            or (
                float(assessment["coverage"]) >= 0.6
                and len(keyword_hits) >= max(1, min(2, len(spec.expected_terms)))
            )
        ) else 0.0
        if spec.id == _LATENT_BRIDGE_STORYLINE_ID:
            bridge = _latent_bridge_assessment(model)
            if bridge["forbidden_detail_hits"] and not future_touched:
                outcome = 0.0
        samples.append({
            "storyline_id": spec.id,
            "model_id": str(model.get("id")),
            "confidence": round(confidence, 4),
            "outcome": outcome,
            "future_touched": future_touched,
            "basis": "future_validation_wave_proxy",
        })
    return samples


def _coerce_confidence(value: Any) -> float | None:
    try:
        return _clamp01(float(value))
    except (TypeError, ValueError):
        return None


def _is_situation_model(model: dict[str, Any]) -> bool:
    proposition = _json_obj(model.get("proposition"))
    text = _model_text(model).lower()
    return (
        proposition.get("claim_role") == "situation"
        or proposition.get("abstraction_level") == "composite"
        or any(term in text for term in ("composite", "situation", "tradeoff"))
    )


def _is_recommendation_model(model: dict[str, Any]) -> bool:
    proposition = _json_obj(model.get("proposition"))
    text = _model_text(model).lower()
    return (
        proposition.get("claim_role") == "recommendation"
        or any(term in text for term in ("recommend", "owner", "escalate", "allocate"))
    )


def _is_concern_model(model: dict[str, Any]) -> bool:
    proposition = _json_obj(model.get("proposition"))
    text = _model_text(model).lower()
    return (
        proposition.get("claim_role") in {"concern", "risk"}
        or any(term in text for term in ("risk", "concern", "blocker", "tradeoff"))
    )


def _latent_pattern_assessment(
    model: dict[str, Any],
    spec: StorylineSpec,
) -> dict[str, Any]:
    text = _model_text(model).lower()
    hits: list[str] = []
    missing: list[str] = []
    for group in spec.latent_pattern_groups:
        label = _latent_group_label(group)
        if any(term.lower() in text for term in group):
            hits.append(label)
        else:
            missing.append(label)
    coverage = (
        len(hits) / len(spec.latent_pattern_groups)
        if spec.latent_pattern_groups else 0.0
    )
    return {
        "coverage": coverage,
        "hits": hits,
        "missing": missing,
    }


def _latent_bridge_assessment(model: dict[str, Any]) -> dict[str, Any]:
    text = _model_text(model).lower()
    groups = {
        "state_transition": (
            "before",
            "after",
            "transition",
            "moved from",
            "changed from",
            "state",
            "checkpoint",
        ),
        "epistemic_gap": (
            "inferred",
            "likely",
            "unobserved",
            "off-sensor",
            "missing",
            "gap",
            "indirect",
            "not directly observed",
            "bounded",
            "uncertain",
            "confidence",
        ),
        "pricing_exception": (
            "northstar",
            "discount",
            "exception",
            "pricing",
            "approval",
            "policy",
        ),
    }
    hits = [
        label for label, terms in groups.items()
        if any(term in text for term in terms)
    ]
    epistemic_hits = [
        term for term in groups["epistemic_gap"]
        if term in text
    ]
    forbidden_detail_terms = (
        "hallway",
        "verbal approval",
        "pat ",
        "lena ",
        "sponsor confirmed",
    )
    forbidden_detail_hits = [
        term.strip() for term in forbidden_detail_terms
        if term in text
    ]
    return {
        "coverage": _ratio(len(hits), len(groups)),
        "hits": hits,
        "epistemic_hits": epistemic_hits,
        "forbidden_detail_hits": forbidden_detail_hits,
    }


def _latent_group_label(group: tuple[str, ...]) -> str:
    return "/".join(group)


def _storyline_calibration_report(
    storyline_scores: list[StorylineScore],
    *,
    bin_count: int = 10,
) -> dict[str, Any]:
    samples = [
        sample
        for score in storyline_scores
        for sample in score.calibration_samples
        if isinstance(sample, dict)
    ]
    if not samples:
        return {
            "source": "storyline_future_validation_proxy",
            "n": 0,
            "bin_count": bin_count,
            "expected_calibration_error": None,
            "bins": [],
            "note": (
                "No future-validation-backed calibration samples were available "
                "for this run."
            ),
        }
    bins: list[dict[str, Any]] = []
    total = len(samples)
    ece = 0.0
    for bin_index in range(bin_count):
        low = bin_index / bin_count
        high = (bin_index + 1) / bin_count
        if bin_index == bin_count - 1:
            bucket = [
                sample for sample in samples
                if low <= float(sample["confidence"]) <= high
            ]
        else:
            bucket = [
                sample for sample in samples
                if low <= float(sample["confidence"]) < high
            ]
        if not bucket:
            bins.append({
                "low": round(low, 4),
                "high": round(high, 4),
                "n": 0,
                "accuracy": None,
                "avg_confidence": None,
                "gap": None,
            })
            continue
        avg_conf = sum(float(sample["confidence"]) for sample in bucket) / len(bucket)
        accuracy = sum(float(sample["outcome"]) for sample in bucket) / len(bucket)
        gap = abs(accuracy - avg_conf)
        ece += (len(bucket) / total) * gap
        bins.append({
            "low": round(low, 4),
            "high": round(high, 4),
            "n": len(bucket),
            "accuracy": round(accuracy, 4),
            "avg_confidence": round(avg_conf, 4),
            "gap": round(gap, 4),
        })
    return {
        "source": "storyline_future_validation_proxy",
        "n": total,
        "bin_count": bin_count,
        "expected_calibration_error": round(ece, 4),
        "positive_outcomes": int(sum(float(sample["outcome"]) for sample in samples)),
        "negative_outcomes": int(total - sum(float(sample["outcome"]) for sample in samples)),
        "bins": bins,
        "note": (
            "ECE is computed only over Models supported by pre-validation "
            "storyline evidence, then checked against the run's "
            "future-validation waves. It is a benchmark proxy, not a production "
            "resolution-outcome audit."
        ),
    }


def _company_intelligence_scorecard(
    *,
    model_summary: dict[str, Any],
    storyline_scores: list[StorylineScore],
    waves: list[dict[str, Any]],
    retrieval_model_counts: list[int],
    retrieval_observation_counts: list[int],
    validation_errors: int,
) -> dict[str, Any]:
    """Score whether the run behaved like durable company intelligence."""
    total_signals = int(model_summary.get("signal_count") or 0)
    think_success = int(model_summary.get("think_runs_success") or 0)
    think_failed = int(model_summary.get("think_runs_failed") or 0)
    think_runs = think_success + think_failed
    pending_triggers = int(model_summary.get("pending_triggers") or 0)
    ops = _aggregate_wave_ops(waves)
    wave_stats = _wave_stats(waves)
    context_stats = _retrieval_context_stats(
        waves,
        retrieval_observation_counts=retrieval_observation_counts,
    )
    future_stats = _future_validation_stats(waves)
    graph_health = _json_obj(model_summary.get("graph_health"))
    context_distribution = _json_obj(model_summary.get("context_use_distribution"))
    context_contract = _json_obj(
        model_summary.get("context_use_relation_contract")
    )
    relationship_status = _json_obj(
        model_summary.get("relationship_candidate_status_distribution")
    )
    model_kind_distribution = _json_obj(model_summary.get("model_kind_distribution"))
    discovery_counts = _json_obj(model_summary.get("discovery_layer_counts"))
    topology_metrics = _json_obj(model_summary.get("topology_optimizer_metric_totals"))
    edge_lifecycle = _json_obj(model_summary.get("edge_lifecycle"))
    edge_ops_stats = _edge_ops_stats(waves)
    post_commit_status = _json_obj(model_summary.get("post_commit_status"))
    cost = _json_obj(model_summary.get("cost"))

    latent_avg = _avg([
        score.latent_pattern_score for score in storyline_scores
    ])
    concrete_latent_ratio = _ratio(
        sum(
            1 for score in storyline_scores
            if score.latent_pattern_evidence_supported_model_count > 0
        ),
        len(storyline_scores),
    )
    evidence_avg = _avg([
        min(1.0, score.evidence_supported_model_count / 3.0)
        for score in storyline_scores
    ])
    required_edge_kinds = {
        relation for story in STORYLINES
        for relation in story.expected_relationships
        if relation != "needs_review"
    }
    edge_distribution = _json_obj(model_summary.get("edge_kind_distribution"))
    accepted_edge_distribution = _json_obj(
        edge_lifecycle.get("accepted_edge_kind_distribution")
    ) or edge_distribution
    accepted_edge_coverage = _ratio(
        len(required_edge_kinds & set(accepted_edge_distribution)),
        len(required_edge_kinds),
    )
    precise_required_edge_kinds = required_edge_kinds - {"supports"}
    precise_edge_coverage = _ratio(
        len(precise_required_edge_kinds & set(accepted_edge_distribution)),
        len(precise_required_edge_kinds),
    )
    storyline_edge_kind_coverage = _avg([
        _ratio(
            len(score.edge_kind_hits),
            len(score.edge_kind_hits) + len(score.missing_edge_kinds),
        )
        for score in storyline_scores
    ])
    storyline_edge_presence = _ratio(
        sum(1 for score in storyline_scores if score.scoped_edge_count > 0),
        len(storyline_scores),
    )
    accepted_relationship_candidates = float(relationship_status.get("accepted") or 0)
    relationship_candidate_count = max(
        0,
        int(model_summary.get("relationship_candidates") or 0),
    )
    accepted_candidate_ratio = _ratio(
        accepted_relationship_candidates,
        relationship_candidate_count,
    )
    edge_reconfirmation_events = float(
        edge_lifecycle.get("reconfirmation_events") or 0.0
    )
    retired_or_inert_edges = float(edge_lifecycle.get("retired_or_inert_edges") or 0.0)
    lifecycle_signal_count = (
        float(edge_ops_stats.get("future_edge_ops") or 0.0)
        + float(edge_ops_stats.get("retire_ops") or 0.0)
        + edge_reconfirmation_events
        + retired_or_inert_edges
    )
    edge_lifecycle_score = _clamp01(
        lifecycle_signal_count
        / max(1.0, float(len(storyline_scores)))
    )
    supports_edges = float(edge_distribution.get("supports") or 0.0)
    total_edges = max(1.0, float(sum(edge_distribution.values()) or 0.0))
    generic_support_share = supports_edges / total_edges
    generic_overuse_penalty = (
        _clamp01((generic_support_share - 0.35) / 0.65)
        * (1.0 - precise_edge_coverage)
    )
    ontology_gap_ops = int(edge_ops_stats.get("ontology_gap_ops") or 0)
    missing_registered_edges = sorted(
        required_edge_kinds - set(accepted_edge_distribution)
    )
    ontology_proposals = int(edge_lifecycle.get("ontology_proposals") or 0)
    ontology_gap_discipline_score = 1.0
    if ontology_gap_ops and missing_registered_edges:
        ontology_gap_discipline_score = 0.6
    if ontology_gap_ops and ontology_proposals == 0:
        ontology_gap_discipline_score = min(ontology_gap_discipline_score, 0.7)

    model_inserts = int(ops["model_inserts"])
    model_updates = int(ops["model_updates"])
    durable_growth_per_signal = _ratio(model_inserts, total_signals)
    update_share = _ratio(model_updates, model_inserts + model_updates)
    compression_growth_score = 1.0 - _clamp01(
        (durable_growth_per_signal - 0.25) / 0.75
    )
    compression_update_score = _clamp01(update_share / 0.20)
    duplicate_group_count = int(graph_health.get("exact_duplicate_natural_groups") or 0)
    duplicate_penalty = _clamp01(duplicate_group_count / 500.0)

    useful_context = sum(
        int(context_distribution.get(key) or 0)
        for key in (
            "graph_context_used",
            "model_context_used",
            "observation_context_used",
            "justified_noop_context_used",
            "selected_context_accounted",
        )
    )
    unused_context = int(context_distribution.get("unused_selected_context") or 0)
    context_total = useful_context + unused_context
    context_use_score = _ratio(useful_context, context_total)
    graph_selected_runs = int(context_contract.get("graph_selected_runs") or 0)
    graph_relation_op_runs = int(
        context_contract.get("graph_relation_op_runs") or 0
    )
    graph_no_edge_rationale_runs = int(
        context_contract.get("graph_no_edge_rationale_runs") or 0
    )
    graph_relation_contract_satisfied_runs = int(
        context_contract.get("graph_relation_contract_satisfied_runs") or 0
    )
    graph_relation_contract_failed_runs = int(
        context_contract.get("graph_relation_contract_failed_runs") or 0
    )
    graph_relation_contract_score = (
        _ratio(graph_relation_contract_satisfied_runs, graph_selected_runs)
        if graph_selected_runs
        else 1.0
    )
    model_context_score = _ratio(
        int(context_distribution.get("graph_context_used") or 0)
        + int(context_distribution.get("model_context_used") or 0),
        context_total,
    )
    avg_retrieved_models = _avg(retrieval_model_counts)
    avg_retrieved_observations = _avg(retrieval_observation_counts)
    avg_historical_observations = float(
        context_stats.get("avg_historical_observations_per_t1_batch") or 0.0
    )
    historical_observation_leakage_score = 1.0 - _clamp01(
        max(0.0, avg_historical_observations - 4.0) / 12.0
    )
    retrieved_model_score = _clamp01(avg_retrieved_models / 20.0)

    recommendation_coverage = _ratio(
        sum(1 for score in storyline_scores if score.recommendation_model_count > 0),
        len(storyline_scores),
    )
    situation_coverage = _ratio(
        sum(1 for score in storyline_scores if score.situation_model_count > 0),
        len(storyline_scores),
    )
    useful_write_count = (
        int(ops["claim_ops"]) + int(ops["edge_ops"]) + int(ops["act_ops"])
    )
    useful_writes_per_storyline = _ratio(useful_write_count, len(storyline_scores))
    useful_write_score = _clamp01(useful_writes_per_storyline / 6.0)
    review_debt = int(relationship_status.get("needs_review") or 0)
    review_debt_per_signal = _ratio(review_debt, total_signals)
    review_debt_score = 1.0 - _clamp01(review_debt_per_signal / 0.25)

    temporal_proxy_score = _avg([
        _clamp01(model_updates / max(1, model_inserts + model_updates) / 0.20),
        _clamp01(int(ops["situation_model_updates"]) / max(1, len(storyline_scores))),
        _clamp01(float(topology_metrics.get("shortcut_creates_or_bumps") or 0) / 40.0),
        _clamp01(float(topology_metrics.get("affordance_reinforces") or 0) / 40.0),
    ])
    future_validation_events = int(
        model_summary.get("future_validation_events")
        or future_stats.get("signals")
        or 0
    )
    temporal_cap = 1.0 if future_validation_events else 0.55
    future_validation_memory_use_score = float(
        future_stats.get("model_or_graph_context_use_score") or 0.0
    )
    future_validation_update_score = _clamp01(
        float(future_stats.get("memory_touch_ops") or 0.0)
        / max(1.0, float(future_stats.get("batches") or 0.0))
    )
    future_validation_score = (
        _avg([
            float(future_stats.get("success_rate") or 0.0),
            future_validation_memory_use_score,
            future_validation_update_score,
        ])
        if future_validation_events else 0.0
    )
    temporal_evidence_score = (
        _avg([
            0.70 * future_validation_score + 0.30 * temporal_proxy_score,
        ])
        if future_validation_events
        else temporal_proxy_score
    )

    wave_success_score = _ratio(
        wave_stats["successful_t1_batches"],
        wave_stats["t1_batch_count"],
    )
    drain_score = 1.0 if pending_triggers == 0 else 0.0
    failure_score = 1.0 - _ratio(think_failed, think_runs)
    validation_score = 1.0 if validation_errors == 0 else 0.0
    timeout_score = 1.0 if wave_stats["timeout_like_t1_batches"] == 0 else 0.0
    noise_score = _noise_noop_score(waves)
    topology_missing_model_skips = (
        float(topology_metrics.get("shortcut_missing_model_skips") or 0.0)
        + float(topology_metrics.get("structural_missing_model_skips") or 0.0)
    )
    topology_integrity_score = 1.0 - _clamp01(topology_missing_model_skips / 10.0)

    think_runs_per_signal = _ratio(think_runs, total_signals)
    llm_calls_per_signal = _ratio(int(cost.get("llm_calls") or 0), total_signals)
    latency_score = 1.0 - _clamp01((wave_stats["max_t1_elapsed_s"] - 90.0) / 810.0)
    amplification_score = 1.0 - _clamp01(think_runs_per_signal / 0.20)
    llm_call_score = 1.0 - _clamp01(llm_calls_per_signal / 0.20)
    cost_per_signal = _ratio(float(cost.get("cost_usd") or 0.0), total_signals)
    cost_score = 1.0 - _clamp01(cost_per_signal / 0.01)

    dimensions = {
        "memory_truth": _dimension(
            score=_avg([
                0.55 * latent_avg + 0.20 * concrete_latent_ratio
                + 0.15 * evidence_avg + 0.10 * accepted_edge_coverage,
            ]),
            metrics={
                "average_latent_pattern_score": latent_avg,
                "concrete_latent_model_ratio": concrete_latent_ratio,
                "evidence_support_score": evidence_avg,
                "accepted_expected_edge_kind_coverage": accepted_edge_coverage,
            },
            findings=[
                "Measures whether hidden company truths became evidence-backed Models.",
                "Expected edge kinds with no accepted edge lower the truth score.",
            ],
        ),
        "compression": _dimension(
            score=_avg([
                0.50 * compression_growth_score
                + 0.25 * compression_update_score
                + 0.25 * (1.0 - duplicate_penalty),
            ]),
            metrics={
                "model_inserts": model_inserts,
                "model_updates": model_updates,
                "durable_growth_per_signal": durable_growth_per_signal,
                "update_share": update_share,
                "exact_duplicate_natural_groups": duplicate_group_count,
            },
            findings=[
                "Rewards preserving meaning with bounded durable memory growth.",
                "Rewards updates/absorption over unnecessary new model creation.",
            ],
        ),
        "retrieval_usefulness": _dimension(
            score=_avg([
                0.30 * context_use_score
                + 0.25 * model_context_score
                + 0.20 * historical_observation_leakage_score
                + 0.15 * retrieved_model_score
                + 0.10 * graph_relation_contract_score,
            ]),
            metrics={
                "context_use_score": context_use_score,
                "model_or_graph_context_use_score": model_context_score,
                "graph_relation_contract_score": (
                    graph_relation_contract_score
                ),
                "graph_selected_runs": graph_selected_runs,
                "graph_relation_contract_failed_runs": (
                    graph_relation_contract_failed_runs
                ),
                "avg_models_per_t1_batch": avg_retrieved_models,
                "avg_observations_per_t1_batch": avg_retrieved_observations,
                "avg_trigger_observations_per_t1_batch": float(
                    context_stats.get("avg_trigger_observations_per_t1_batch")
                    or 0.0
                ),
                "avg_historical_observations_per_t1_batch": (
                    avg_historical_observations
                ),
                "accounted_selected_context_count": int(
                    context_distribution.get("selected_context_accounted") or 0
                ),
                "unused_selected_context_count": unused_context,
            },
            findings=[
                "Rewards selected context that is actually referenced by reasoning.",
                "Penalizes falling back to raw observations as the dominant context.",
            ],
        ),
        "reasoning_value": _dimension(
            score=_avg([
                0.30 * situation_coverage
                + 0.25 * recommendation_coverage
                + 0.20 * useful_write_score
                + 0.15 * review_debt_score
                + 0.10 * validation_score,
            ]),
            metrics={
                "situation_coverage": situation_coverage,
                "recommendation_coverage": recommendation_coverage,
                "useful_writes_per_storyline": useful_writes_per_storyline,
                "review_debt_per_signal": review_debt_per_signal,
                "validation_error_count": validation_errors,
            },
            findings=[
                "Rewards durable situations, recommendations, and accepted graph work.",
                "Penalizes review debt and validation failures.",
            ],
        ),
        "edge_intelligence": _dimension(
            score=_avg([
                0.20 * accepted_edge_coverage
                + 0.18 * precise_edge_coverage
                + 0.13 * storyline_edge_kind_coverage
                + 0.09 * storyline_edge_presence
                + 0.10 * accepted_candidate_ratio
                + 0.10 * edge_lifecycle_score
                + 0.10 * graph_relation_contract_score
                + 0.05 * ontology_gap_discipline_score
                + 0.05 * (1.0 - generic_overuse_penalty),
            ]),
            metrics={
                "required_registered_edge_kind_coverage": accepted_edge_coverage,
                "precise_required_edge_kind_coverage": precise_edge_coverage,
                "storyline_edge_kind_coverage": storyline_edge_kind_coverage,
                "storyline_edge_presence": storyline_edge_presence,
                "accepted_relationship_candidate_ratio": accepted_candidate_ratio,
                "graph_relation_contract_score": (
                    graph_relation_contract_score
                ),
                "graph_selected_runs": graph_selected_runs,
                "graph_relation_op_runs": graph_relation_op_runs,
                "graph_no_edge_rationale_runs": graph_no_edge_rationale_runs,
                "graph_relation_contract_failed_runs": (
                    graph_relation_contract_failed_runs
                ),
                "edge_add_ops": int(edge_ops_stats.get("add_ops") or 0),
                "edge_retire_ops": int(edge_ops_stats.get("retire_ops") or 0),
                "future_validation_edge_ops": int(
                    edge_ops_stats.get("future_edge_ops") or 0
                ),
                "reconfirmation_events": edge_reconfirmation_events,
                "retired_or_inert_edges": retired_or_inert_edges,
                "generic_support_share": generic_support_share,
                "ontology_gap_ops": ontology_gap_ops,
                "ontology_proposals": ontology_proposals,
                "ontology_gap_discipline_score": ontology_gap_discipline_score,
            },
            findings=[
                "Separates existing registered edge usage from ontology-gap proposals.",
                "Rewards precise edge kinds, accepted candidates, and later edge evolution.",
            ],
        ),
        "temporal_improvement": _dimension(
            score=min(temporal_cap, temporal_evidence_score),
            metrics={
                "future_validation_events": future_validation_events,
                "score_cap_without_future_validation": temporal_cap,
                "future_validation_success_rate": float(
                    future_stats.get("success_rate") or 0.0
                ),
                "future_validation_model_or_graph_context_use_score": (
                    future_validation_memory_use_score
                ),
                "future_validation_accounted_context_score": float(
                    future_stats.get("accounted_context_score") or 0.0
                ),
                "future_validation_memory_touch_ops": float(
                    future_stats.get("memory_touch_ops") or 0.0
                ),
                "model_update_share": update_share,
                "situation_model_updates": int(ops["situation_model_updates"]),
                "shortcut_creates_or_bumps": float(
                    topology_metrics.get("shortcut_creates_or_bumps") or 0
                ),
                "affordance_reinforces": float(
                    topology_metrics.get("affordance_reinforces") or 0
                ),
            },
            findings=[
                "Current evidence is proxy-only unless future validation events exist.",
                "The ideal proof is earlier memory improving later retrieval and decisions.",
            ],
        ),
        "robustness": _dimension(
            score=_avg([
                0.25 * wave_success_score
                + 0.20 * drain_score
                + 0.20 * failure_score
                + 0.15 * validation_score
                + 0.10 * timeout_score
                + 0.05 * noise_score
                + 0.05 * topology_integrity_score,
            ]),
            metrics={
                "t1_batch_success_rate": wave_success_score,
                "timeout_like_t1_batches": wave_stats["timeout_like_t1_batches"],
                "pending_triggers": pending_triggers,
                "think_runs_failed": think_failed,
                "noise_noop_score": noise_score,
                "topology_missing_model_skips": topology_missing_model_skips,
                "topology_integrity_score": topology_integrity_score,
                "dead_lettered_post_commit_actions": int(
                    post_commit_status.get("dead_lettered") or 0
                ),
            },
            findings=[
                "Rewards drain, no failed Think runs, no validation errors, and clean noise handling.",
                "A timeout-like batch is a serious robustness miss even if a later retry succeeds.",
            ],
        ),
        "efficiency": _dimension(
            score=_avg([
                0.35 * amplification_score
                + 0.25 * llm_call_score
                + 0.25 * latency_score
                + 0.15 * cost_score,
            ]),
            metrics={
                "think_runs_per_signal": think_runs_per_signal,
                "llm_calls_per_signal": llm_calls_per_signal,
                "max_t1_elapsed_s": wave_stats["max_t1_elapsed_s"],
                "cost_per_signal_usd": cost_per_signal,
            },
            findings=[
                "Rewards calm processing: low trigger amplification, low calls, bounded latency.",
            ],
        ),
    }
    product_value_evals = _product_value_evals(
        model_summary=model_summary,
        storyline_scores=storyline_scores,
        dimensions=dimensions,
        ops=ops,
        graph_health=graph_health,
        context_distribution=context_distribution,
        model_kind_distribution=model_kind_distribution,
        discovery_counts=discovery_counts,
        topology_metrics=topology_metrics,
        future_stats=future_stats,
        edge_lifecycle=edge_lifecycle,
        recommendation_coverage=recommendation_coverage,
        situation_coverage=situation_coverage,
        accepted_edge_coverage=accepted_edge_coverage,
        precise_edge_coverage=precise_edge_coverage,
        latent_avg=latent_avg,
        concrete_latent_ratio=concrete_latent_ratio,
        evidence_avg=evidence_avg,
        update_share=update_share,
        durable_growth_per_signal=durable_growth_per_signal,
        model_context_score=model_context_score,
        context_use_score=context_use_score,
        historical_observation_leakage_score=historical_observation_leakage_score,
        review_debt_score=review_debt_score,
        noise_score=noise_score,
    )
    weights = {
        "memory_truth": 0.18,
        "compression": 0.12,
        "retrieval_usefulness": 0.14,
        "reasoning_value": 0.16,
        "edge_intelligence": 0.12,
        "temporal_improvement": 0.14,
        "robustness": 0.09,
        "efficiency": 0.05,
    }
    overall = round(
        sum(dimensions[name]["score"] * weight for name, weight in weights.items()),
        4,
    )
    proof_gaps = _company_intelligence_proof_gaps(
        model_summary=model_summary,
        dimensions=dimensions,
        wave_stats=wave_stats,
        ops=ops,
        required_edge_kinds=required_edge_kinds,
        edge_distribution=accepted_edge_distribution,
        model_kind_distribution=model_kind_distribution,
        discovery_counts=discovery_counts,
        future_stats=future_stats,
        edge_intelligence=dimensions["edge_intelligence"],
    )
    return {
        "overall_score": overall,
        "interpretation": _score_interpretation(overall),
        "dimension_weights": weights,
        "dimensions": dimensions,
        "proof_coverage": {
            "storylines": len(storyline_scores),
            "signals": total_signals,
            "t1_batches": wave_stats["t1_batch_count"],
            "successful_t1_batches": wave_stats["successful_t1_batches"],
            "future_validation_events": future_validation_events,
            "future_validation_batches": int(future_stats.get("batches") or 0),
            "future_validation_success_rate": float(
                future_stats.get("success_rate") or 0.0
            ),
            "future_validation_model_or_graph_context_use_score": (
                future_validation_memory_use_score
            ),
            "avg_historical_observations_per_t1_batch": (
                avg_historical_observations
            ),
            "latent_storylines_with_evidence_backed_model": sum(
                1 for score in storyline_scores
                if score.latent_pattern_evidence_supported_model_count > 0
            ),
            "required_edge_kinds": sorted(required_edge_kinds),
            "accepted_edge_kinds_observed": sorted(set(accepted_edge_distribution)),
            "missing_registered_edge_kinds": missing_registered_edges,
            "precise_required_edge_kind_coverage": precise_edge_coverage,
            "edge_lifecycle": edge_lifecycle,
            "edge_ops": edge_ops_stats,
            "context_use_relation_contract": context_contract,
            "prediction_models": int(model_kind_distribution.get("prediction") or 0),
            "resource_ops": int(ops["resource_ops"]),
            "ontology_gap_ops": int(ops["ontology_gap_ops"]),
            "negative_memory_inserts": float(
                topology_metrics.get("negative_memory_inserts") or 0
            ),
            "question_policy_updates": float(
                topology_metrics.get("question_policy_updates") or 0
            ),
            "shortcut_missing_model_skips": float(
                topology_metrics.get("shortcut_missing_model_skips") or 0
            ),
            "structural_missing_model_skips": float(
                topology_metrics.get("structural_missing_model_skips") or 0
            ),
            "product_value_eval_overall": product_value_evals["overall_score"],
            "product_value_eval_keys": list(_PRODUCT_VALUE_EVAL_KEYS),
        },
        "product_value_evals": product_value_evals,
        "proof_gaps": proof_gaps,
    }


def _aggregate_wave_ops(waves: list[dict[str, Any]]) -> dict[str, float]:
    totals: dict[str, float] = {
        "claim_ops": 0,
        "edge_ops": 0,
        "act_ops": 0,
        "resource_ops": 0,
        "ontology_gap_ops": 0,
        "state_changes": 0,
        "model_inserts": 0,
        "model_updates": 0,
        "model_archives": 0,
        "situation_model_inserts": 0,
        "situation_model_updates": 0,
        "situation_member_additions": 0,
        "near_duplicate_absorptions": 0,
        "evidence_attachments": 0,
    }
    for wave in waves:
        ops = (((wave.get("t1_batch") or {}).get("run") or {}).get("ops_applied") or {})
        totals["claim_ops"] += len(ops.get("claim_ops") or [])
        totals["edge_ops"] += len(ops.get("edge_ops") or [])
        totals["act_ops"] += len(ops.get("act_ops") or [])
        totals["resource_ops"] += len(ops.get("resource_ops") or [])
        totals["ontology_gap_ops"] += len(ops.get("ontology_gap_ops") or [])
        totals["state_changes"] += float(ops.get("state_changes_emitted") or 0)
        aggregation = _json_obj(ops.get("memory_aggregation"))
        for key in (
            "model_inserts",
            "model_updates",
            "model_archives",
            "situation_model_inserts",
            "situation_model_updates",
            "situation_member_additions",
            "near_duplicate_absorptions",
            "evidence_attachments",
        ):
            totals[key] += float(aggregation.get(key) or 0)
    return totals


def _edge_ops_stats(waves: list[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "add_ops": 0,
        "retire_ops": 0,
        "future_edge_ops": 0,
        "accepted_edge_ops": 0,
        "candidate_or_review_edge_ops": 0,
        "ontology_gap_ops": 0,
        "edge_kinds_from_ops": {},
    }
    edge_kinds: Counter[str] = Counter()
    for wave in waves:
        sequence = str(wave.get("sequence") or "")
        is_future = sequence.startswith("future_validation")
        run = ((wave.get("t1_batch") or {}).get("run") or {})
        ops = _json_obj(run.get("ops_applied"))
        edge_ops = ops.get("edge_ops") or []
        if not isinstance(edge_ops, list):
            edge_ops = []
        for edge_op in edge_ops:
            if not isinstance(edge_op, dict):
                continue
            op = str(edge_op.get("op") or "")
            kind = str(edge_op.get("edge_kind") or "")
            if op == "add":
                stats["add_ops"] += 1
            elif op == "retire":
                stats["retire_ops"] += 1
            if is_future:
                stats["future_edge_ops"] += 1
            if edge_op.get("review_status") == "accepted":
                stats["accepted_edge_ops"] += 1
            elif edge_op.get("review_status") in {"candidate", "needs_review"}:
                stats["candidate_or_review_edge_ops"] += 1
            if kind:
                edge_kinds[kind] += 1
        ontology_gap_ops = ops.get("ontology_gap_ops") or []
        if isinstance(ontology_gap_ops, list):
            stats["ontology_gap_ops"] += len(ontology_gap_ops)
    stats["edge_kinds_from_ops"] = dict(edge_kinds)
    return stats


def _wave_stats(waves: list[dict[str, Any]]) -> dict[str, Any]:
    elapsed_values: list[float] = []
    success = 0
    timeout_like = 0
    for wave in waves:
        batch = wave.get("t1_batch") or {}
        if not batch:
            continue
        elapsed = float(batch.get("elapsed_s") or 0.0)
        elapsed_values.append(elapsed)
        run = batch.get("run") or {}
        if run.get("status") == "success":
            success += 1
        if elapsed >= 300.0 and not run:
            timeout_like += 1
    return {
        "t1_batch_count": len(elapsed_values),
        "successful_t1_batches": success,
        "timeout_like_t1_batches": timeout_like,
        "max_t1_elapsed_s": max(elapsed_values) if elapsed_values else 0.0,
        "avg_t1_elapsed_s": _avg(elapsed_values),
    }


def _noise_noop_score(waves: list[dict[str, Any]]) -> float:
    noise_waves = [
        wave for wave in waves
        if str(wave.get("sequence") or "").lower() in {"background_noise", "noise"}
        or str(wave.get("sequence") or "").lower().startswith("background_noise")
    ]
    if not noise_waves:
        return 0.5
    scores: list[float] = []
    for wave in noise_waves:
        run = ((wave.get("t1_batch") or {}).get("run") or {})
        ops = run.get("ops_applied") or {}
        state_changes = int(ops.get("state_changes_emitted") or 0)
        grade = ((ops.get("context_use") or {}).get("context_use_grade") or "")
        scores.append(
            1.0 if state_changes == 0 and "noop" in str(grade).lower()
            else 0.0
        )
    return _avg(scores)


def _retrieval_context_stats(
    waves: list[dict[str, Any]],
    *,
    retrieval_observation_counts: list[int],
) -> dict[str, float]:
    trigger_counts: list[float] = []
    historical_counts: list[float] = []
    fallback_historical_counts: list[float] = []
    for index, wave in enumerate(waves):
        batch = wave.get("t1_batch") or {}
        run = batch.get("run") or {}
        ops = _json_obj(run.get("ops_applied"))
        context_use = _json_obj(ops.get("context_use"))
        if "selected_trigger_observation_count" in context_use:
            trigger_counts.append(
                float(context_use.get("selected_trigger_observation_count") or 0.0)
            )
        if "selected_historical_observation_count" in context_use:
            historical_counts.append(
                float(
                    context_use.get("selected_historical_observation_count") or 0.0
                )
            )

        retrieval_count = run.get("retrieval_observation_count")
        if retrieval_count is None and index < len(retrieval_observation_counts):
            retrieval_count = retrieval_observation_counts[index]
        try:
            retrieved = float(retrieval_count or 0.0)
            trigger_observations = float(batch.get("observation_count") or 0.0)
        except (TypeError, ValueError):
            continue
        fallback_historical_counts.append(max(0.0, retrieved - trigger_observations))

    return {
        "avg_trigger_observations_per_t1_batch": _avg(trigger_counts),
        "avg_historical_observations_per_t1_batch": _avg(
            historical_counts or fallback_historical_counts
        ),
    }


def _future_validation_stats(waves: list[dict[str, Any]]) -> dict[str, float]:
    future_waves = [
        wave for wave in waves
        if str(wave.get("sequence") or "").startswith("future_validation")
    ]
    if not future_waves:
        return {
            "signals": 0.0,
            "batches": 0.0,
            "success_rate": 0.0,
            "model_or_graph_context_use_score": 0.0,
            "accounted_context_score": 0.0,
            "memory_touch_ops": 0.0,
            "unused_selected_context": 0.0,
        }

    successes = 0
    memory_context_used = 0
    accounted_context = 0
    unused_context = 0
    memory_touch_ops = 0
    for wave in future_waves:
        run = ((wave.get("t1_batch") or {}).get("run") or {})
        if run.get("status") == "success":
            successes += 1
        ops = _json_obj(run.get("ops_applied"))
        context_use = _json_obj(ops.get("context_use"))
        grade = str(context_use.get("context_use_grade") or "")
        if grade in {
            "graph_context_used",
            "model_context_used",
        }:
            memory_context_used += 1
        if bool(context_use.get("selected_context_accounted_for")) or grade in {
            "graph_context_used",
            "model_context_used",
            "observation_context_used",
            "justified_noop_context_used",
            "selected_context_accounted",
        }:
            accounted_context += 1
        if grade == "unused_selected_context":
            unused_context += 1
        aggregation = _json_obj(ops.get("memory_aggregation"))
        memory_touch_ops += (
            len(ops.get("edge_ops") or [])
            + len(ops.get("act_ops") or [])
            + len(ops.get("resource_ops") or [])
            + len(ops.get("ontology_gap_ops") or [])
            + int(aggregation.get("model_updates") or 0)
            + int(aggregation.get("model_archives") or 0)
            + int(aggregation.get("evidence_attachments") or 0)
        )

    batches = len(future_waves)
    return {
        "signals": float(sum(int(wave.get("signals") or 0) for wave in future_waves)),
        "batches": float(batches),
        "success_rate": _ratio(successes, batches),
        "model_or_graph_context_use_score": _ratio(memory_context_used, batches),
        "accounted_context_score": _ratio(accounted_context, batches),
        "memory_touch_ops": float(memory_touch_ops),
        "unused_selected_context": float(unused_context),
    }


def _dimension(
    *,
    score: float,
    metrics: dict[str, Any],
    findings: list[str],
) -> dict[str, Any]:
    return {
        "score": round(_clamp01(score), 4),
        "metrics": _round_floats(metrics),
        "findings": findings,
    }


def _product_value_evals(
    *,
    model_summary: dict[str, Any],
    storyline_scores: list[StorylineScore],
    dimensions: dict[str, dict[str, Any]],
    ops: dict[str, float],
    graph_health: dict[str, Any],
    context_distribution: dict[str, Any],
    model_kind_distribution: dict[str, Any],
    discovery_counts: dict[str, Any],
    topology_metrics: dict[str, Any],
    future_stats: dict[str, float],
    edge_lifecycle: dict[str, Any],
    recommendation_coverage: float,
    situation_coverage: float,
    accepted_edge_coverage: float,
    precise_edge_coverage: float,
    latent_avg: float,
    concrete_latent_ratio: float,
    evidence_avg: float,
    update_share: float,
    durable_growth_per_signal: float,
    model_context_score: float,
    context_use_score: float,
    historical_observation_leakage_score: float,
    review_debt_score: float,
    noise_score: float,
) -> dict[str, Any]:
    """Score product-value proof paths orthogonally to pipeline health."""
    total_storylines = len(storyline_scores)
    total_storyline_floor = max(1, total_storylines)
    future_validation_events = int(
        model_summary.get("future_validation_events")
        or future_stats.get("signals")
        or 0
    )
    future_validation_success_rate = float(
        future_stats.get("success_rate") or 0.0
    )
    future_validation_memory_touch_ops = float(
        future_stats.get("memory_touch_ops") or 0.0
    )
    future_validation_batches = float(future_stats.get("batches") or 0.0)
    future_validation_memory_touch_score = _clamp01(
        future_validation_memory_touch_ops / max(1.0, future_validation_batches)
    )
    future_validation_context_score = float(
        future_stats.get("model_or_graph_context_use_score") or 0.0
    )

    act_ops = int(ops["act_ops"])
    resource_ops = int(ops["resource_ops"])
    model_inserts = int(ops["model_inserts"])
    model_updates = int(ops["model_updates"])
    model_archives = int(ops["model_archives"])
    evidence_attachments = int(ops["evidence_attachments"])
    near_duplicate_absorptions = int(ops["near_duplicate_absorptions"])
    exact_duplicate_groups = int(
        graph_health.get("exact_duplicate_natural_groups") or 0
    )

    useful_context = sum(
        int(context_distribution.get(key) or 0)
        for key in (
            "graph_context_used",
            "model_context_used",
            "observation_context_used",
            "justified_noop_context_used",
            "selected_context_accounted",
        )
    )
    unused_context = int(context_distribution.get("unused_selected_context") or 0)
    unused_context_avoidance_score = (
        1.0 - _ratio(unused_context, useful_context + unused_context)
        if useful_context or unused_context
        else 0.5
    )

    prediction_models = int(model_kind_distribution.get("prediction") or 0)
    negative_memory_count = int(discovery_counts.get("negative_memory") or 0)
    negative_memory_inserts = int(
        topology_metrics.get("negative_memory_inserts") or 0
    )
    negative_learning_events = negative_memory_count + negative_memory_inserts
    question_policy_count = int(discovery_counts.get("question_policy_stats") or 0)
    question_policy_updates = int(
        topology_metrics.get("question_policy_updates") or 0
    )
    question_policy_events = question_policy_count + question_policy_updates

    customer_scope_rows = _json_list(model_summary.get("top_customer_model_scopes"))
    customer_scope_count = len(customer_scope_rows)
    customer_scoped_models_from_rows = _named_count_total(customer_scope_rows)
    scope_distribution = _json_obj(model_summary.get("model_scope_entity_distribution"))
    customer_scoped_models = max(
        customer_scoped_models_from_rows,
        int(scope_distribution.get("customer") or 0),
    )
    unscoped_models = int(scope_distribution.get("<none>") or 0)
    customer_scope_share = _ratio(
        customer_scoped_models,
        customer_scoped_models + unscoped_models,
    )
    gold_customer_count = len({
        customer for story in STORYLINES for customer in story.customers
    })
    customer_scope_coverage = _ratio(customer_scope_count, gold_customer_count)

    alias_score = next(
        (
            score for score in storyline_scores
            if score.storyline_id == "alias_ambiguity_pollution"
        ),
        None,
    )
    alias_storyline_score = float(alias_score.score) if alias_score else 0.0
    alias_review_candidate_count = (
        int(alias_score.review_candidate_count) if alias_score else 0
    )
    alias_needs_review_count = (
        int(alias_score.needs_review_candidate_count) if alias_score else 0
    )
    alias_accepted_candidate_count = (
        int(alias_score.accepted_candidate_count) if alias_score else 0
    )
    alias_review_deferral_score = (
        _ratio(alias_needs_review_count, alias_review_candidate_count)
        if alias_review_candidate_count
        else (0.5 if alias_score else 0.0)
    )
    alias_strong_acceptance_pressure = _ratio(
        alias_accepted_candidate_count,
        alias_review_candidate_count,
    )
    bridge_story = next(
        (
            score for score in storyline_scores
            if score.storyline_id == _LATENT_BRIDGE_STORYLINE_ID
        ),
        None,
    )
    bridge_storyline_score = float(bridge_story.score) if bridge_story else 0.0
    bridge_model_count = (
        int(bridge_story.inferred_bridge_model_count) if bridge_story else 0
    )
    bridge_transition_supported_count = (
        int(bridge_story.inferred_bridge_transition_supported_model_count)
        if bridge_story else 0
    )
    bridge_future_confirmed_count = (
        int(bridge_story.inferred_bridge_future_confirmed_model_count)
        if bridge_story else 0
    )
    unsupported_bridge_specific_claims = (
        int(bridge_story.unsupported_bridge_specific_claim_count)
        if bridge_story else 0
    )
    bridge_epistemic_marker_count = (
        len(bridge_story.bridge_epistemic_marker_hits) if bridge_story else 0
    )
    bridge_presence_score = 1.0 if bridge_model_count else 0.0
    bridge_transition_support_score = _ratio(
        bridge_transition_supported_count,
        bridge_model_count,
    )
    bridge_future_confirmation_score = _ratio(
        bridge_future_confirmed_count,
        bridge_model_count,
    )
    bridge_epistemic_score = _clamp01(bridge_epistemic_marker_count / 3.0)
    bridge_no_fabrication_score = (
        1.0 - _ratio(unsupported_bridge_specific_claims, bridge_model_count)
        if bridge_model_count else 0.0
    )

    edge_lifecycle_events = (
        float(edge_lifecycle.get("reconfirmation_events") or 0.0)
        + float(edge_lifecycle.get("retired_or_inert_edges") or 0.0)
    )

    compression_dimension_score = float(
        (dimensions.get("compression") or {}).get("score") or 0.0
    )
    decision_action_score = _clamp01(_ratio(act_ops, total_storyline_floor))
    decision_resource_score = _clamp01(_ratio(resource_ops, total_storyline_floor))
    memory_archive_score = _clamp01(_ratio(model_archives, total_storyline_floor))
    evidence_attachment_score = _clamp01(
        _ratio(evidence_attachments, total_storyline_floor)
    )
    duplicate_health_score = 1.0 - _clamp01(exact_duplicate_groups / 500.0)
    duplicate_learning_score = max(
        duplicate_health_score,
        _clamp01(_ratio(near_duplicate_absorptions, total_storyline_floor)),
    )
    prediction_model_score = _clamp01(
        _ratio(prediction_models, total_storyline_floor)
    )
    prediction_resolution_proxy_score = (
        1.0
        if prediction_models and future_validation_events
        else 0.0
    )
    negative_memory_score = _clamp01(
        _ratio(negative_learning_events, total_storyline_floor)
    )
    question_policy_score = _clamp01(
        _ratio(question_policy_events, total_storyline_floor)
    )
    customer_account_health_score = _avg([
        customer_scope_coverage,
        recommendation_coverage,
        accepted_edge_coverage,
        future_validation_success_rate if future_validation_events else 0.0,
    ])

    evals = {
        "decision_impact": _dimension(
            score=(
                0.30 * recommendation_coverage
                + 0.20 * situation_coverage
                + 0.20 * decision_action_score
                + 0.10 * decision_resource_score
                + 0.10 * context_use_score
                + 0.10 * (
                    future_validation_success_rate
                    if future_validation_events else 0.0
                )
            ),
            metrics={
                "recommendation_coverage": recommendation_coverage,
                "situation_coverage": situation_coverage,
                "act_ops": act_ops,
                "act_ops_per_storyline": _ratio(act_ops, total_storyline_floor),
                "resource_ops": resource_ops,
                "resource_ops_per_storyline": _ratio(
                    resource_ops,
                    total_storyline_floor,
                ),
                "future_validation_success_rate": (
                    future_validation_success_rate
                    if future_validation_events else 0.0
                ),
                "context_use_score": context_use_score,
            },
            findings=[
                "Tests whether hidden understanding turns into concrete recommendations, actions, and resource decisions.",
                "Future validation shows whether those decisions stayed useful after the company changed.",
            ],
        ),
        "memory_lifecycle": _dimension(
            score=(
                0.25 * _clamp01(update_share / 0.25)
                + 0.20 * evidence_attachment_score
                + 0.20 * memory_archive_score
                + 0.20 * future_validation_memory_touch_score
                + 0.15 * duplicate_learning_score
            ),
            metrics={
                "model_inserts": model_inserts,
                "model_updates": model_updates,
                "update_share": update_share,
                "model_archives": model_archives,
                "evidence_attachments": evidence_attachments,
                "near_duplicate_absorptions": near_duplicate_absorptions,
                "exact_duplicate_natural_groups": exact_duplicate_groups,
                "future_validation_memory_touch_ops": (
                    future_validation_memory_touch_ops
                ),
            },
            findings=[
                "Tests whether memory is updated, evidenced, archived, and merged instead of only appended.",
                "The strongest proof is future evidence changing existing compressed memory.",
            ],
        ),
        "prediction_lifecycle": _dimension(
            score=(
                0.35 * prediction_model_score
                + 0.25 * prediction_resolution_proxy_score
                + 0.20 * (
                    future_validation_success_rate
                    if future_validation_events else 0.0
                )
                + 0.10 * future_validation_context_score
                + 0.10 * future_validation_memory_touch_score
            ),
            metrics={
                "prediction_models": prediction_models,
                "prediction_models_per_storyline": _ratio(
                    prediction_models,
                    total_storyline_floor,
                ),
                "future_validation_events": future_validation_events,
                "future_validation_success_rate": (
                    future_validation_success_rate
                    if future_validation_events else 0.0
                ),
                "future_validation_model_or_graph_context_use_score": (
                    future_validation_context_score
                ),
                "prediction_resolution_proxy_score": (
                    prediction_resolution_proxy_score
                ),
            },
            findings=[
                "Tests whether forecasts become durable Predictions and later evidence validates, updates, or retires them.",
                "The current harness uses future validation as a proxy until explicit prediction outcome records exist.",
            ],
        ),
        "counterfactual_trap": _dimension(
            score=(
                0.30 * noise_score
                + 0.30 * alias_storyline_score
                + 0.20 * alias_review_deferral_score
                + 0.10 * (1.0 - alias_strong_acceptance_pressure)
                + 0.10 * review_debt_score
            ),
            metrics={
                "noise_noop_score": noise_score,
                "alias_storyline_score": alias_storyline_score,
                "alias_review_candidate_count": alias_review_candidate_count,
                "alias_needs_review_candidate_count": alias_needs_review_count,
                "alias_accepted_candidate_count": alias_accepted_candidate_count,
                "alias_review_deferral_score": alias_review_deferral_score,
                "review_debt_score": review_debt_score,
            },
            findings=[
                "Tests whether the system resists tempting but wrong memory under noise, ambiguity, and contradictory evidence.",
                "Alias ambiguity should create review/deferral behavior before strong customer graph writes.",
            ],
        ),
        "latent_bridge_inference": _dimension(
            score=(
                0.20 * bridge_storyline_score
                + 0.20 * bridge_presence_score
                + 0.20 * bridge_transition_support_score
                + 0.15 * bridge_epistemic_score
                + 0.15 * bridge_future_confirmation_score
                + 0.10 * bridge_no_fabrication_score
            ),
            metrics={
                "bridge_storyline_score": bridge_storyline_score,
                "inferred_bridge_model_count": bridge_model_count,
                "transition_supported_bridge_model_count": (
                    bridge_transition_supported_count
                ),
                "future_confirmed_bridge_model_count": (
                    bridge_future_confirmed_count
                ),
                "unsupported_specific_claim_count": (
                    unsupported_bridge_specific_claims
                ),
                "bridge_epistemic_marker_count": bridge_epistemic_marker_count,
                "bridge_epistemic_marker_hits": (
                    bridge_story.bridge_epistemic_marker_hits
                    if bridge_story else []
                ),
                "bridge_forbidden_detail_hits": (
                    bridge_story.bridge_forbidden_detail_hits
                    if bridge_story else []
                ),
                "transition_support_score": bridge_transition_support_score,
                "future_confirmation_score": bridge_future_confirmation_score,
                "no_fabrication_score": bridge_no_fabrication_score,
            },
            findings=[
                "Tests whether irregular state transitions create bounded inferred bridge Models.",
                "Rewards indirect before/after support and later confirmation while penalizing invented specifics.",
            ],
        ),
        "compression_loss": _dimension(
            score=(
                0.25 * latent_avg
                + 0.20 * concrete_latent_ratio
                + 0.15 * evidence_avg
                + 0.15 * compression_dimension_score
                + 0.15 * model_context_score
                + 0.10 * historical_observation_leakage_score
            ),
            metrics={
                "average_latent_pattern_score": latent_avg,
                "concrete_latent_model_ratio": concrete_latent_ratio,
                "evidence_support_score": evidence_avg,
                "compression_dimension_score": compression_dimension_score,
                "durable_growth_per_signal": durable_growth_per_signal,
                "model_or_graph_context_use_score": model_context_score,
                "historical_observation_leakage_score": (
                    historical_observation_leakage_score
                ),
            },
            findings=[
                "Tests whether compressed Models preserve the hidden company pattern without needing raw observation replay.",
                "High compression is not valuable unless later retrieval uses the compressed form.",
            ],
        ),
        "negative_learning": _dimension(
            score=(
                0.45 * negative_memory_score
                + 0.25 * noise_score
                + 0.20 * unused_context_avoidance_score
                + 0.10 * _clamp01(
                    _ratio(negative_memory_inserts, total_storyline_floor)
                )
            ),
            metrics={
                "negative_memory_count": negative_memory_count,
                "negative_memory_inserts": negative_memory_inserts,
                "negative_learning_events": negative_learning_events,
                "noise_noop_score": noise_score,
                "unused_selected_context_count": unused_context,
                "unused_context_avoidance_score": unused_context_avoidance_score,
            },
            findings=[
                "Tests whether the system learns what not to retrieve, ask, or amplify.",
                "Noise no-op behavior helps, but durable negative memory is the stronger product proof.",
            ],
        ),
        "question_policy": _dimension(
            score=(
                0.55 * question_policy_score
                + 0.25 * context_use_score
                + 0.20 * unused_context_avoidance_score
            ),
            metrics={
                "question_policy_stats": question_policy_count,
                "question_policy_updates": question_policy_updates,
                "question_policy_events": question_policy_events,
                "context_use_score": context_use_score,
                "unused_selected_context_count": unused_context,
                "unused_context_avoidance_score": unused_context_avoidance_score,
            },
            findings=[
                "Tests whether the system learns when to ask, when not to ask, and which missing context matters.",
                "This should improve future context selection instead of producing repeated generic uncertainty.",
            ],
        ),
        "customer_value": _dimension(
            score=(
                0.25 * customer_scope_coverage
                + 0.15 * customer_scope_share
                + 0.20 * recommendation_coverage
                + 0.15 * customer_account_health_score
                + 0.15 * accepted_edge_coverage
                + 0.10 * precise_edge_coverage
            ),
            metrics={
                "gold_customer_count": gold_customer_count,
                "customer_scope_count": customer_scope_count,
                "customer_scope_coverage": customer_scope_coverage,
                "customer_scoped_models": customer_scoped_models,
                "unscoped_models": unscoped_models,
                "customer_scope_share": customer_scope_share,
                "recommendation_coverage": recommendation_coverage,
                "accepted_expected_edge_kind_coverage": accepted_edge_coverage,
                "precise_expected_edge_kind_coverage": precise_edge_coverage,
                "edge_lifecycle_events": edge_lifecycle_events,
                "customer_account_health_score": customer_account_health_score,
            },
            findings=[
                "Tests whether system value lands in account-health objects customers actually care about.",
                "Rewards scoped customer memory, recommendations, precise edges, and future validation.",
            ],
        ),
    }

    proof_gaps: list[str] = []
    if recommendation_coverage < 0.75 or act_ops == 0:
        proof_gaps.append(
            "Decision impact eval is weak: recommendations/actions did not cover most storylines."
        )
    if resource_ops == 0:
        proof_gaps.append(
            "Decision impact eval did not exercise resource or action-resource operations."
        )
    if model_archives == 0:
        proof_gaps.append(
            "Memory lifecycle eval did not exercise archival or stale-memory cleanup."
        )
    if evidence_attachments == 0:
        proof_gaps.append(
            "Memory lifecycle eval did not exercise evidence attachment behavior."
        )
    if prediction_models < max(1, total_storylines // 2):
        proof_gaps.append(
            "Prediction lifecycle eval has too few Prediction models for company-scale proof."
        )
    if not prediction_models or not future_validation_events:
        proof_gaps.append(
            "Prediction lifecycle eval lacks explicit outcome validation over time."
        )
    if alias_score is None:
        proof_gaps.append(
            "Counterfactual/trap eval did not include the alias ambiguity storyline."
        )
    elif alias_needs_review_count == 0 and alias_review_candidate_count > 0:
        proof_gaps.append(
            "Counterfactual/trap eval saw alias candidates but no explicit review deferral."
        )
    if noise_score < 0.75:
        proof_gaps.append(
            "Counterfactual/trap eval did not prove clean no-op behavior for noise."
        )
    if bridge_story is None:
        proof_gaps.append(
            "Latent bridge inference eval did not include the unobserved transition storyline."
        )
    elif bridge_model_count == 0:
        proof_gaps.append(
            "Latent bridge inference eval did not create a bounded inferred bridge model."
        )
    elif bridge_transition_supported_count == 0:
        proof_gaps.append(
            "Latent bridge inference eval did not support the bridge with both before and after/gap states."
        )
    if bridge_story is not None and bridge_epistemic_marker_count == 0:
        proof_gaps.append(
            "Latent bridge inference eval did not mark uncertainty or indirect inference."
        )
    if bridge_story is not None and unsupported_bridge_specific_claims > 0:
        proof_gaps.append(
            "Latent bridge inference eval found fabricated off-sensor details before validation."
        )
    if (
        bridge_story is not None
        and future_validation_events
        and bridge_future_confirmed_count == 0
    ):
        proof_gaps.append(
            "Latent bridge inference eval did not update the inferred bridge after future validation."
        )
    if latent_avg < 0.85 or concrete_latent_ratio < 0.75:
        proof_gaps.append(
            "Compression loss eval found hidden patterns that were not consistently preserved as concrete Models."
        )
    if model_context_score < 0.75:
        proof_gaps.append(
            "Compression loss eval found later reasoning was not mostly using compressed Model/graph context."
        )
    if negative_learning_events == 0:
        proof_gaps.append(
            "Negative learning eval did not create durable negative memory."
        )
    if question_policy_events == 0:
        proof_gaps.append(
            "Question policy eval did not exercise question-policy learning."
        )
    if customer_scope_count == 0 and customer_scoped_models == 0:
        proof_gaps.append(
            "Customer value eval did not prove customer-scoped account-health memory."
        )
    if precise_edge_coverage < 0.8:
        proof_gaps.append(
            "Customer value eval lacks enough precise edge semantics for high-confidence account health."
        )

    overall = round(
        _avg([
            float(evals[key]["score"])
            for key in _PRODUCT_VALUE_EVAL_KEYS
            if key in evals
        ]),
        4,
    )
    return {
        "overall_score": overall,
        "interpretation": _score_interpretation(overall),
        "evals": evals,
        "proof_gaps": proof_gaps,
    }


def _named_count_total(rows: list[Any]) -> int:
    total = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            total += int(row.get("value") or 0)
        except (TypeError, ValueError):
            continue
    return total


def _company_intelligence_proof_gaps(
    *,
    model_summary: dict[str, Any],
    dimensions: dict[str, dict[str, Any]],
    wave_stats: dict[str, Any],
    ops: dict[str, float],
    required_edge_kinds: set[str],
    edge_distribution: dict[str, Any],
    model_kind_distribution: dict[str, Any],
    discovery_counts: dict[str, Any],
    future_stats: dict[str, float],
    edge_intelligence: dict[str, Any],
) -> list[str]:
    gaps: list[str] = []
    if wave_stats["timeout_like_t1_batches"]:
        gaps.append("At least one T1 batch timed out before producing a Think run.")
    if dimensions["temporal_improvement"]["metrics"]["future_validation_events"] == 0:
        gaps.append(
            "No future validation events, so temporal improvement is proxy-scored."
        )
    elif float(future_stats.get("success_rate") or 0.0) < 1.0:
        gaps.append("At least one future validation batch did not complete cleanly.")
    elif float(future_stats.get("model_or_graph_context_use_score") or 0.0) == 0.0:
        gaps.append(
            "Future validation did not use compressed Model/graph context."
        )
    elif float(future_stats.get("memory_touch_ops") or 0.0) == 0.0:
        gaps.append(
            "Future validation used context but did not update, link, archive, "
            "or attach evidence to durable memory."
        )
    missing_edges = sorted(required_edge_kinds - set(edge_distribution))
    if missing_edges:
        gaps.append(
            "Expected registered edge kinds not observed as accepted durable edges: "
            + ", ".join(missing_edges)
        )
    edge_metrics = _json_obj(edge_intelligence.get("metrics"))
    if float(edge_metrics.get("precise_required_edge_kind_coverage") or 0.0) < 0.8:
        gaps.append(
            "Precise registered edge kinds are underused; check whether Think "
            "is collapsing blocks/weakens/explains/resolution edges into prose "
            "or generic support."
        )
    context_contract = _json_obj(model_summary.get("context_use_relation_contract"))
    graph_selected_runs = int(context_contract.get("graph_selected_runs") or 0)
    graph_relation_failed = int(
        context_contract.get("graph_relation_contract_failed_runs") or 0
    )
    graph_relation_ops = int(context_contract.get("graph_relation_op_runs") or 0)
    if graph_selected_runs and graph_relation_failed:
        gaps.append(
            "Graph-selected context failed the relationship contract in "
            f"{graph_relation_failed}/{graph_selected_runs} runs; relational "
            "context should become an edge, ontology-gap proposal, stronger "
            "model mutation, or explicit no-edge rationale."
        )
    if graph_selected_runs >= 3 and graph_relation_ops == 0:
        gaps.append(
            "Graph-selected context never produced durable relationship ops; "
            "verify Think is not leaving graph insight only in reasoning prose."
        )
    if float(edge_metrics.get("future_validation_edge_ops") or 0.0) == 0.0:
        gaps.append("Future validation did not evolve or reconfirm durable edges.")
    if (
        float(edge_metrics.get("ontology_gap_ops") or 0.0) > 0.0
        and missing_edges
    ):
        gaps.append(
            "Ontology-gap ops occurred while registered expected edge kinds "
            "were still missing; verify the system is not proposing new kinds "
            "where existing kinds fit."
        )
    if int(model_kind_distribution.get("prediction") or 0) < 5:
        gaps.append("Prediction memory is barely exercised.")
    if int(ops["resource_ops"]) == 0:
        gaps.append("Resource/action-resource operations are untested.")
    if int(ops["ontology_gap_ops"]) == 0:
        gaps.append("Ontology-gap write path is untested by this run.")
    if int(ops["model_archives"]) == 0:
        gaps.append("Model archival/staleness cleanup is untested.")
    if int(ops["evidence_attachments"]) == 0:
        gaps.append("Evidence attachment behavior is untested.")
    if float(discovery_counts.get("negative_memory") or 0) == 0:
        gaps.append("Negative memory behavior is untested.")
    if float(discovery_counts.get("question_policy_stats") or 0) == 0:
        gaps.append("Question-policy learning is untested.")
    topology_metrics = _json_obj(
        model_summary.get("topology_optimizer_metric_totals")
    )
    shortcut_skips = float(
        topology_metrics.get("shortcut_missing_model_skips") or 0
    )
    structural_skips = float(
        topology_metrics.get("structural_missing_model_skips") or 0
    )
    if shortcut_skips or structural_skips:
        gaps.append(
            "Topology optimizer skipped missing model references: "
            f"shortcuts={shortcut_skips:g}, structural_features={structural_skips:g}."
        )
    if int(model_summary.get("pending_triggers") or 0) != 0:
        gaps.append("Trigger queue did not drain.")
    return gaps


def _score_interpretation(score: float) -> str:
    if score >= 0.85:
        return "strong_company_intelligence"
    if score >= 0.70:
        return "promising_but_not_proven"
    if score >= 0.50:
        return "partial_system_proof"
    return "insufficient_company_intelligence_proof"


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _round_floats(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, dict):
        return {key: _round_floats(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_round_floats(item) for item in value]
    return value


def _benchmark_summary(
    *,
    model_summary: dict[str, Any],
    storyline_scores: list[StorylineScore],
    waves: list[dict[str, Any]],
    elapsed_seconds: float,
) -> dict[str, Any]:
    score_values = [score.score for score in storyline_scores]
    latent_pattern_scores = [
        score.latent_pattern_score for score in storyline_scores
    ]
    thesis_judge_scores = [
        float(score.thesis_judge_score)
        for score in storyline_scores
        if score.thesis_judge_score is not None
    ]
    calibration = _storyline_calibration_report(storyline_scores)
    concrete_latent_count = sum(
        1 for score in storyline_scores
        if score.latent_pattern_evidence_supported_model_count > 0
    )
    total_signals = int(model_summary.get("signal_count") or 0)
    think_runs = int(model_summary.get("think_runs_success") or 0) + int(
        model_summary.get("think_runs_failed") or 0
    )
    validation_errors = 0
    retrieval_model_counts: list[int] = []
    retrieval_observation_counts: list[int] = []
    for wave in waves:
        run = ((wave.get("t1_batch") or {}).get("run") or {})
        validation_errors += int(run.get("validation_error_count") or 0)
        if run.get("retrieval_model_count") is not None:
            retrieval_model_counts.append(int(run["retrieval_model_count"]))
        if run.get("retrieval_observation_count") is not None:
            retrieval_observation_counts.append(int(run["retrieval_observation_count"]))
    context_stats = _retrieval_context_stats(
        waves,
        retrieval_observation_counts=retrieval_observation_counts,
    )
    summary = {
        "run_id": model_summary.get("run_id"),
        "tenant_id": model_summary.get("tenant_id"),
        "append": model_summary.get("append"),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "signals": total_signals,
        "storyline_count": len(storyline_scores),
        "average_storyline_score": round(
            sum(score_values) / len(score_values), 4
        ) if score_values else 0.0,
        "min_storyline_score": min(score_values) if score_values else 0.0,
        "max_storyline_score": max(score_values) if score_values else 0.0,
        "storyline_scores": [asdict(score) for score in storyline_scores],
        "latent_pattern_fitness": {
            "average_latent_pattern_score": round(
                sum(latent_pattern_scores) / len(latent_pattern_scores),
                4,
            ) if latent_pattern_scores else 0.0,
            "storylines_with_concrete_latent_model": concrete_latent_count,
            "storylines_without_concrete_latent_model": (
                len(storyline_scores) - concrete_latent_count
            ),
            "average_best_pattern_coverage": round(
                sum(score.latent_pattern_best_coverage for score in storyline_scores)
                / len(storyline_scores),
                4,
            ) if storyline_scores else 0.0,
        },
        "thesis_recovery_judge": {
            "enabled": bool(thesis_judge_scores),
            "n": len(thesis_judge_scores),
            "average_score": _avg(thesis_judge_scores),
            "correct_count": sum(
                1 for score in storyline_scores
                if score.thesis_judge_correct is True
            ),
            "incorrect_count": sum(
                1 for score in storyline_scores
                if score.thesis_judge_correct is False
            ),
        },
        "calibration": calibration,
        "run_amplification": {
            "think_runs_per_signal": (
                round(think_runs / total_signals, 4) if total_signals else 0.0
            ),
            "pending_triggers": model_summary.get("pending_triggers"),
            "think_runs_success": model_summary.get("think_runs_success"),
            "think_runs_failed": model_summary.get("think_runs_failed"),
            "validation_error_count": validation_errors,
        },
        "retrieval_fitness_proxy": {
            "avg_models_per_t1_batch": _avg(retrieval_model_counts),
            "avg_observations_per_t1_batch": _avg(retrieval_observation_counts),
            "avg_trigger_observations_per_t1_batch": (
                context_stats["avg_trigger_observations_per_t1_batch"]
            ),
            "avg_historical_observations_per_t1_batch": (
                context_stats["avg_historical_observations_per_t1_batch"]
            ),
            "min_models_per_t1_batch": min(retrieval_model_counts)
            if retrieval_model_counts else 0,
            "max_models_per_t1_batch": max(retrieval_model_counts)
            if retrieval_model_counts else 0,
        },
        "memory_shape": {
            "active_models": model_summary.get("active_models"),
            "archived_models": model_summary.get("archived_models"),
            "model_edges": model_summary.get("model_edges"),
            "relationship_candidates": model_summary.get("relationship_candidates"),
            "relationship_candidate_status_distribution": model_summary.get(
                "relationship_candidate_status_distribution"
            ),
            "model_kind_distribution": model_summary.get("model_kind_distribution"),
            "context_use_distribution": model_summary.get("context_use_distribution"),
            "context_use_relation_contract": model_summary.get(
                "context_use_relation_contract"
            ),
        },
        "waves": waves,
    }
    summary["company_intelligence_scorecard"] = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=storyline_scores,
        waves=waves,
        retrieval_model_counts=retrieval_model_counts,
        retrieval_observation_counts=retrieval_observation_counts,
        validation_errors=validation_errors,
    )
    return summary


def _render_benchmark_markdown(summary: dict[str, Any]) -> str:
    append = summary.get("append") or {}
    lines = [
        "# Storyline Batch Benchmark",
        "",
        f"- Run: `{summary.get('run_id')}`",
        f"- Tenant: `{summary.get('tenant_id')}`",
        f"- Signals: {summary.get('signals')}",
        f"- Storylines: {summary.get('storyline_count')}",
        f"- Average storyline score: {summary.get('average_storyline_score')}",
        f"- Average latent pattern score: "
        f"{(summary.get('latent_pattern_fitness') or {}).get('average_latent_pattern_score')}",
        f"- Thesis judge: "
        f"{(summary.get('thesis_recovery_judge') or {}).get('average_score')} "
        f"(n={(summary.get('thesis_recovery_judge') or {}).get('n')})",
        f"- Calibration ECE: "
        f"{(summary.get('calibration') or {}).get('expected_calibration_error')} "
        f"(n={(summary.get('calibration') or {}).get('n')})",
        f"- Think runs per signal: "
        f"{(summary.get('run_amplification') or {}).get('think_runs_per_signal')}",
        f"- Pending triggers: "
        f"{(summary.get('run_amplification') or {}).get('pending_triggers')}",
    ]
    if append:
        lines.extend([
            f"- Append base run: `{append.get('base_run_id')}`",
            f"- Additional T1 batches: {append.get('additional_t1_batches')}",
            f"- Additional signals: {append.get('additional_signal_count')}",
            f"- Horizon batches: "
            f"{int(append.get('horizon_start_batch') or 0) + 1}-"
            f"{append.get('horizon_end_batch')}",
        ])
    lines.extend([
        "",
        "## Storyline Scores",
        "| Storyline | Score | Pattern | Pattern Models | Models | Situations | Recommendations | Edges | Edge Kinds Hit | Missing Edge Kinds | Review Debt | Missing Keywords |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |",
    ])
    for score in summary.get("storyline_scores") or []:
        lines.append(
            "| {title} | {score:.2f} | {pattern:.2f} | {pattern_models} | "
            "{models} | {situations} | {recommendations} | {edges} | "
            "{edge_hits} | {missing_edges} | {review} | {missing} |".format(
                title=score["title"],
                score=float(score["score"]),
                pattern=float(score.get("latent_pattern_score") or 0.0),
                pattern_models=score.get(
                    "latent_pattern_evidence_supported_model_count"
                ),
                models=score["relevant_model_count"],
                situations=score["situation_model_count"],
                recommendations=score["recommendation_model_count"],
                edges=score["scoped_edge_count"],
                edge_hits=", ".join(score.get("edge_kind_hits") or []) or "-",
                missing_edges=", ".join(score.get("missing_edge_kinds") or []) or "-",
                review=score["needs_review_candidate_count"],
                missing=", ".join(score["missing_keywords"][:5]) or "-",
            )
        )
    scorecard = summary.get("company_intelligence_scorecard") or {}
    lines.extend([
        "",
        "## Company Intelligence Scorecard",
        "",
        f"- Overall: {scorecard.get('overall_score')} "
        f"({scorecard.get('interpretation')})",
        "",
        "| Dimension | Score |",
        "| --- | ---: |",
    ])
    for name, dimension in (scorecard.get("dimensions") or {}).items():
        lines.append(
            f"| {name.replace('_', ' ').title()} | "
            f"{float(dimension.get('score') or 0.0):.2f} |"
        )
    product_value = scorecard.get("product_value_evals") or {}
    product_evals = product_value.get("evals") or {}
    lines.extend([
        "",
        "### Product Value Evals",
        "",
        f"- Overall: {product_value.get('overall_score')} "
        f"({product_value.get('interpretation')})",
        "",
        "| Eval | Score |",
        "| --- | ---: |",
    ])
    for name, evaluation in product_evals.items():
        lines.append(
            f"| {name.replace('_', ' ').title()} | "
            f"{float(evaluation.get('score') or 0.0):.2f} |"
        )
    product_gaps = product_value.get("proof_gaps") or []
    lines.extend([
        "",
        "#### Product Value Proof Gaps",
    ])
    if product_gaps:
        lines.extend([f"- {gap}" for gap in product_gaps])
    else:
        lines.append("- No product-value proof gaps detected by the current harness.")
    lines.extend([
        "",
        "### Proof Coverage",
        "```json",
        json.dumps(
            scorecard.get("proof_coverage") or {},
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "### Proof Gaps",
    ])
    proof_gaps = scorecard.get("proof_gaps") or []
    if proof_gaps:
        lines.extend([f"- {gap}" for gap in proof_gaps])
    else:
        lines.append("- No proof gaps detected by the current harness.")
    lines.extend([
        "",
        "## Latent Pattern Fitness",
        "```json",
        json.dumps(
            summary.get("latent_pattern_fitness") or {},
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Run Amplification",
        "```json",
        json.dumps(summary.get("run_amplification") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "## Calibration",
        "```json",
        json.dumps(summary.get("calibration") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "## Retrieval Fitness Proxy",
        "```json",
        json.dumps(
            summary.get("retrieval_fitness_proxy") or {},
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Memory Shape",
        "```json",
        json.dumps(summary.get("memory_shape") or {}, indent=2, sort_keys=True),
        "```",
        "",
    ])
    return "\n".join(lines)


def build_variance_report(report_root: Path, run_ids: list[str]) -> dict[str, Any]:
    run_summaries: list[dict[str, Any]] = []
    for run_id in run_ids:
        run_dir = report_root / run_id
        summary_path = run_dir / "storyline_scores.json"
        config_path = run_dir / "run_config.json"
        if not summary_path.exists():
            raise FileNotFoundError(f"Missing benchmark summary: {summary_path}")
        summary = json.loads(summary_path.read_text())
        config = (
            json.loads(config_path.read_text())
            if config_path.exists()
            else {}
        )
        run_summaries.append({
            "run_id": run_id,
            "signals": summary.get("signals"),
            "storyline_count": summary.get("storyline_count"),
            "elapsed_seconds": summary.get("elapsed_seconds"),
            "average_storyline_score": summary.get("average_storyline_score"),
            "company_intelligence_overall": (
                (summary.get("company_intelligence_scorecard") or {})
                .get("overall_score")
            ),
            "product_value_overall": (
                ((summary.get("company_intelligence_scorecard") or {})
                 .get("product_value_evals") or {})
                .get("overall_score")
            ),
            "thesis_recovery_judge": summary.get("thesis_recovery_judge") or {},
            "run_config": config,
            "cache_bypass_env": (
                config.get("cache_bypass_env")
                if isinstance(config, dict)
                else None
            ),
        })

    metric_names = (
        "average_storyline_score",
        "company_intelligence_overall",
        "product_value_overall",
    )
    metrics = {
        name: _variance_metric([
            run.get(name) for run in run_summaries
            if run.get(name) is not None
        ])
        for name in metric_names
    }
    thesis_scores = [
        (run.get("thesis_recovery_judge") or {}).get("average_score")
        for run in run_summaries
        if (run.get("thesis_recovery_judge") or {}).get("average_score") is not None
    ]
    metrics["thesis_recovery_judge_average_score"] = _variance_metric(thesis_scores)

    thesis_correct = sum(
        int((run.get("thesis_recovery_judge") or {}).get("correct_count") or 0)
        for run in run_summaries
    )
    thesis_incorrect = sum(
        int((run.get("thesis_recovery_judge") or {}).get("incorrect_count") or 0)
        for run in run_summaries
    )
    thesis_n = thesis_correct + thesis_incorrect
    thesis_ci = _wilson_interval(thesis_correct, thesis_n)

    return {
        "report_kind": "storyline_variance_band",
        "report_root": str(report_root),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_count": len(run_summaries),
        "run_ids": run_ids,
        "runs": run_summaries,
        "metrics": metrics,
        "judged_rates": {
            "thesis_recovery_correct_rate": {
                "n": thesis_n,
                "correct": thesis_correct,
                "incorrect": thesis_incorrect,
                "rate": _ratio(thesis_correct, thesis_n),
                "wilson_95_ci": thesis_ci,
            },
        },
        "standing_rule": (
            "Judged rates carry n and Wilson 95% confidence intervals; do not "
            "gate on deltas inside the interval."
        ),
        "cache_note": (
            "Use at least one cache-bypassed arm for run variance. Cache-on arms "
            "measure pipeline determinism more than model variance."
        ),
    }


def _variance_metric(values: list[Any]) -> dict[str, Any]:
    numeric_values = [float(value) for value in values if value is not None]
    if not numeric_values:
        return {
            "n": 0,
            "mean": 0.0,
            "min": 0.0,
            "max": 0.0,
            "stddev": 0.0,
            "values": [],
        }
    mean = sum(numeric_values) / len(numeric_values)
    variance = (
        sum((value - mean) ** 2 for value in numeric_values)
        / (len(numeric_values) - 1)
        if len(numeric_values) > 1
        else 0.0
    )
    return {
        "n": len(numeric_values),
        "mean": round(mean, 4),
        "min": round(min(numeric_values), 4),
        "max": round(max(numeric_values), 4),
        "stddev": round(math.sqrt(variance), 4),
        "values": [round(value, 4) for value in numeric_values],
    }


def _wilson_interval(successes: int, n: int, z: float = 1.96) -> dict[str, float]:
    if n <= 0:
        return {"low": 0.0, "high": 0.0}
    phat = successes / n
    denominator = 1.0 + z**2 / n
    center = (phat + z**2 / (2 * n)) / denominator
    margin = (
        z
        * math.sqrt((phat * (1.0 - phat) + z**2 / (4 * n)) / n)
        / denominator
    )
    return {
        "low": round(max(0.0, center - margin), 4),
        "high": round(min(1.0, center + margin), 4),
    }


def _render_variance_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Storyline Variance Band",
        "",
        f"- Runs: {report.get('run_count')}",
        f"- Run ids: {', '.join(report.get('run_ids') or [])}",
        f"- Rule: {report.get('standing_rule')}",
        f"- Cache note: {report.get('cache_note')}",
        "",
        "## Score Metrics",
        "| Metric | n | Mean | Min | Max | Stddev | Values |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for name, metric in (report.get("metrics") or {}).items():
        lines.append(
            "| {name} | {n} | {mean:.4f} | {min:.4f} | {max:.4f} | "
            "{stddev:.4f} | {values} |".format(
                name=name,
                n=int(metric.get("n") or 0),
                mean=float(metric.get("mean") or 0.0),
                min=float(metric.get("min") or 0.0),
                max=float(metric.get("max") or 0.0),
                stddev=float(metric.get("stddev") or 0.0),
                values=", ".join(map(str, metric.get("values") or [])) or "-",
            )
        )
    thesis_rate = (report.get("judged_rates") or {}).get(
        "thesis_recovery_correct_rate"
    ) or {}
    interval = thesis_rate.get("wilson_95_ci") or {}
    lines.extend([
        "",
        "## Judged Rates",
        "| Rate | n | Correct | Incorrect | Estimate | Wilson 95% CI |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
        "| thesis_recovery_correct_rate | {n} | {correct} | {incorrect} | "
        "{rate:.4f} | [{low:.4f}, {high:.4f}] |".format(
            n=int(thesis_rate.get("n") or 0),
            correct=int(thesis_rate.get("correct") or 0),
            incorrect=int(thesis_rate.get("incorrect") or 0),
            rate=float(thesis_rate.get("rate") or 0.0),
            low=float(interval.get("low") or 0.0),
            high=float(interval.get("high") or 0.0),
        ),
        "",
    ])
    return "\n".join(lines)


def run_variance_report(args: argparse.Namespace) -> dict[str, Any]:
    run_ids = list(args.variance_run_ids or [])
    report = build_variance_report(args.report_root, run_ids)
    output_id = args.run_id or (
        "storyline_variance_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    output_dir = args.report_root / output_id
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "variance_report.json", report)
    (output_dir / "variance_report.md").write_text(_render_variance_markdown(report))
    report["report_dir"] = str(output_dir)
    return report


def _write_build_artifacts(
    report_dir: Path,
    scenario: Scenario,
    gold: list[dict[str, Any]],
    run_config: dict[str, Any],
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    _write_json(report_dir / "storyline_gold.json", gold)
    _write_json(report_dir / "run_config.json", run_config)
    _write_jsonl(
        report_dir / "planned_signals.jsonl",
        [
            {
                "sequence": sequence,
                "index": index,
                "storyline_id": _story_id_from_external_id(
                    signal.get("external_id")
                ),
                "family": (signal.get("content_dict") or {}).get("family"),
                "customer": (signal.get("content_dict") or {}).get("customer_name"),
                "content": signal.get("content"),
            }
            for sequence, signals in scenario.signal_sequences.items()
            for index, signal in enumerate(signals)
        ],
    )
    (report_dir / "benchmark_plan.md").write_text(_render_plan_markdown(scenario))


def _render_plan_markdown(scenario: Scenario) -> str:
    counts = Counter(
        _story_id_from_external_id(signal.get("external_id")) or "<none>"
        for signals in scenario.signal_sequences.values()
        for signal in signals
    )
    lines = [
        "# Storyline Batch Benchmark Plan",
        "",
        f"- Signals: {_signal_count(scenario)}",
        f"- Sequences: {len(scenario.signal_sequences)}",
        "",
        "## Storyline Signal Counts",
        "| Storyline | Signals |",
        "| --- | ---: |",
    ]
    for key, value in sorted(counts.items()):
        lines.append(f"| {key} | {value} |")
    lines.extend([
        "",
        "## Expected Behaviors",
        *[f"- {item}" for item in scenario.expected_behaviors],
        "",
    ])
    return "\n".join(lines)


def _run_config(
    args: argparse.Namespace,
    run_id: str,
    *,
    append_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    keys = (
        "mode",
        "target_t1_batches",
        "signals_per_storyline",
        "future_validation_signals_per_storyline",
        "noise_signals",
        "seed_models",
        "seed_families",
        "t1_batch_window_s",
        "t1_batch_min_size",
        "t1_batch_max_size",
        "downstream_batch_window_s",
        "downstream_batch_min_size",
        "t2_batch_max_size",
        "t4_batch_max_size",
        "downstream_steps_per_wave",
        "skip_migrations",
        "skip_topology_optimizer",
        "enable_thesis_judge",
        "thesis_judge_limit",
    )
    config = {
        "run_id": run_id,
        **{key: getattr(args, key) for key in keys},
        "truth_gate": {
            "label_mapping": "external_id",
            "reasoner_visible_label_keys_removed": [
                "storyline_id",
                "storyline_title",
            ],
        },
        "thesis_judge": {
            "name": _THESIS_JUDGE_NAME,
            "enabled": bool(args.enable_thesis_judge),
            "limit": int(args.thesis_judge_limit),
            "identity": _judge_identity_from_env(),
            "agreement_set": _judge_agreement_set_metadata(),
        },
        "cache_bypass_env": {
            "LLM_CACHE_BYPASS": os.environ.get("LLM_CACHE_BYPASS"),
            "RUN_REAL_LLM": os.environ.get("RUN_REAL_LLM"),
        },
    }
    if append_context:
        config["append"] = append_context
    return config


def _signal_count(scenario: Scenario) -> int:
    return sum(len(signals) for signals in scenario.signal_sequences.values())


def _story_id_from_external_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) < 3:
        return None
    if parts[0] == "storyline":
        story_index = 0
    else:
        try:
            story_index = parts.index("storyline")
        except ValueError:
            return None
    if story_index + 1 >= len(parts):
        return None
    story_id = parts[story_index + 1].strip()
    return story_id or None


def _judge_identity_from_env() -> dict[str, Any]:
    try:
        from lib.llm.provider import LLMConfig

        config = LLMConfig.from_env()
    except Exception as exc:  # pragma: no cover - defensive config metadata.
        return {
            "provider": os.environ.get("LLM_PROVIDER", "anthropic").lower(),
            "model": os.environ.get("LLM_MODEL"),
            "config_error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "provider": config.provider,
        "model": config.model,
        "temperature": 0.0,
        "max_tokens": 512,
    }


def _judge_agreement_set_metadata() -> dict[str, Any]:
    if not _THESIS_JUDGE_AGREEMENT_SET.exists():
        return {"path": str(_THESIS_JUDGE_AGREEMENT_SET), "present": False}
    raw = _THESIS_JUDGE_AGREEMENT_SET.read_bytes()
    count: int | None = None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        count = len(parsed)
    return {
        "path": str(_THESIS_JUDGE_AGREEMENT_SET),
        "present": True,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "items": count,
    }


def _avg(values: list[Any]) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _record_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    return {key: _jsonable(row[key]) for key in row.keys()}


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True, default=str) for row in rows)
        + ("\n" if rows else "")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("build-only", "run", "variance-report"),
        default="build-only",
    )
    parser.add_argument(
        "--target-t1-batches",
        type=int,
        default=0,
        help=(
            "When > 0, build exactly this many T1 waves for a long-horizon "
            "company simulation. Each wave uses --signals-per-storyline signals."
        ),
    )
    parser.add_argument("--signals-per-storyline", type=int, default=25)
    parser.add_argument("--future-validation-signals-per-storyline", type=int, default=3)
    parser.add_argument("--noise-signals", type=int, default=25)
    parser.add_argument("--seed-models", type=int, default=15000)
    parser.add_argument("--seed-families", type=int, default=120)
    parser.add_argument("--t1-batch-window-s", type=float, default=0.1)
    parser.add_argument("--t1-batch-min-size", type=int, default=20)
    parser.add_argument("--t1-batch-max-size", type=int, default=30)
    parser.add_argument(
        "--unbatched-run",
        action="store_true",
        help=(
            "Cost-plan §2.3: process each wave's T1 event_arrival triggers as "
            "individual single-trigger runs (window=0, no T1 batching) instead "
            "of one coalesced batch. Run this and the batched default with the "
            "same scenario seed to A/B cost and CI, and to establish the "
            "same-config variance band before reading any batching delta."
        ),
    )
    parser.add_argument("--downstream-batch-window-s", type=float, default=1.0)
    parser.add_argument("--downstream-batch-min-size", type=int, default=2)
    parser.add_argument("--t2-batch-max-size", type=int, default=8)
    parser.add_argument("--t4-batch-max-size", type=int, default=4)
    parser.add_argument("--downstream-steps-per-wave", type=int, default=0)
    parser.add_argument("--worker-poll-batch", type=int, default=6)
    parser.add_argument("--run-timeout", type=float, default=900.0)
    parser.add_argument("--post-commit-timeout", type=int, default=600)
    parser.add_argument("--topology-optimizer-timeout", type=int, default=900)
    parser.add_argument("--topology-optimizer-batch-size", type=int, default=250)
    parser.add_argument("--topology-optimizer-lookback-hours", type=int, default=72)
    parser.add_argument("--skip-topology-optimizer", action="store_true")
    parser.add_argument("--skip-migrations", action="store_true")
    parser.add_argument("--skip-noise-think", action="store_true")
    parser.add_argument(
        "--enable-thesis-judge",
        action="store_true",
        help=(
            "After scoring, run the pinned LLM thesis-recovery judge over each "
            "storyline's relevant Models."
        ),
    )
    parser.add_argument(
        "--thesis-judge-limit",
        type=int,
        default=0,
        help="Maximum storylines to judge when enabled; 0 judges all storylines.",
    )
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--append-to-run-id",
        help=(
            "Append generated long-horizon batches to the tenant recorded by "
            "this prior run id under --report-root."
        ),
    )
    parser.add_argument(
        "--append-tenant-id",
        help=(
            "Tenant id to append to when the base report does not contain one."
        ),
    )
    parser.add_argument(
        "--horizon-start-batch",
        type=int,
        default=None,
        help=(
            "Absolute zero-based T1 batch offset for append generation. "
            "Defaults to the base run's target_t1_batches."
        ),
    )
    parser.add_argument(
        "--variance-run-ids",
        nargs="*",
        default=None,
        help=(
            "Run ids under --report-root to aggregate when --mode variance-report "
            "is selected."
        ),
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=REPO_ROOT / "tests" / "real_llm" / "reports" / "runs",
    )
    args = parser.parse_args()
    if args.target_t1_batches < 0:
        raise SystemExit("--target-t1-batches must be >= 0")
    if args.horizon_start_batch is not None and args.horizon_start_batch < 0:
        raise SystemExit("--horizon-start-batch must be >= 0")
    if args.signals_per_storyline < 1:
        raise SystemExit("--signals-per-storyline must be positive")
    if args.future_validation_signals_per_storyline < 0:
        raise SystemExit("--future-validation-signals-per-storyline must be >= 0")
    if args.thesis_judge_limit < 0:
        raise SystemExit("--thesis-judge-limit must be >= 0")
    if args.mode == "variance-report":
        if len(args.variance_run_ids or []) < 2:
            raise SystemExit(
                "--mode variance-report requires at least two --variance-run-ids"
            )
        return args
    if args.mode == "run":
        if args.append_to_run_id and args.target_t1_batches <= 0:
            raise SystemExit(
                "--append-to-run-id requires --target-t1-batches > 0"
            )
        if args.append_to_run_id and args.cleanup:
            raise SystemExit("--cleanup is not allowed with --append-to-run-id")
        if args.signals_per_storyline < args.t1_batch_min_size:
            raise SystemExit(
                "--signals-per-storyline must be >= --t1-batch-min-size in run mode"
            )
        if args.signals_per_storyline > args.t1_batch_max_size:
            raise SystemExit(
                "--signals-per-storyline must be <= --t1-batch-max-size in run mode"
            )
        if args.target_t1_batches == 0:
            future_validation_signals = (
                len(STORYLINES) * args.future_validation_signals_per_storyline
            )
            if future_validation_signals:
                if future_validation_signals < args.t1_batch_min_size:
                    raise SystemExit(
                        "future validation wave size must be >= --t1-batch-min-size "
                        "in run mode"
                    )
                if future_validation_signals > args.t1_batch_max_size:
                    raise SystemExit(
                        "future validation wave size must be <= --t1-batch-max-size "
                        "in run mode"
                    )
    return args


async def main() -> int:
    args = parse_args()
    if args.mode == "variance-report":
        summary = run_variance_report(args)
    else:
        summary = await run_benchmark(args)
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
