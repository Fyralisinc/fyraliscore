#!/usr/bin/env python3
"""Run a planted-storyline benchmark for large-batch Think.

This is a value benchmark, not a raw load test. It asks whether a
20-30 signal batch helps the system discover durable company understanding:
composite situations, useful actions, relevant graph edges, low review debt,
and low internal amplification per useful outcome.

The Company Intelligence scorecard design is documented at:
docs/evaluation/company_intelligence_harness.md

Default `--mode build-only` writes the generated scenario and gold rubric
without touching Postgres or an LLM. Use `--mode seed-only` to create a
persistent seeded tenant for repeated append validation runs. Use `--mode run`
for the real burn. Use `--mode rerender-report` to recompute a prior run's
scorecard from saved artifacts without touching Postgres or an LLM.
"""

from __future__ import annotations

# The benchmark mutates sys.path before importing repo modules so it can be run
# directly from the command line.
# ruff: noqa: E402

import argparse
import asyncio
import hashlib
import json
import math
import os
import re
import sys
import time
from collections import Counter
from dataclasses import MISSING, asdict, dataclass, field, fields as dataclass_fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
REAL_LLM_INFRA_ROOT = REPO_ROOT / "tests" / "real_llm" / "infrastructure"
sys.path.insert(0, str(REAL_LLM_INFRA_ROOT))


def _prefer_repo_tests_package() -> None:
    tests_module = sys.modules.get("tests")
    module_file = getattr(tests_module, "__file__", None)
    if module_file is None:
        return
    try:
        module_path = Path(module_file).resolve()
        scripts_tests = (REPO_ROOT / "scripts" / "tests").resolve()
    except OSError:
        return
    if (
        module_path == scripts_tests / "__init__.py"
        or scripts_tests in module_path.parents
    ):
        sys.modules.pop("tests", None)


_prefer_repo_tests_package()

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.migrations import apply_migrations_dir
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.app.gateway.db_bootstrap import _register_codecs
from services.reasoning.think.worker import ThinkWorker, WorkerConfig
from scenario_loader import Scenario, materialize

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

_POST_SEED_ANALYZE_TABLES = (
    "models",
    "model_scope_entities",
    "model_scope_actors",
    "model_sparse_terms",
    "model_answerability_index",
    "model_belief_addresses",
    "model_search_documents",
    "model_representation_feature_postings",
    "retrieval_affordance_profiles",
)
_SEED_PREFLIGHT_MIN_MODELS = 1_000
_SEED_PREFLIGHT_BLOAT_BYTES = 16 * 1024 * 1024
_SEED_PREFLIGHT_BLOAT_TABLES = (
    "models",
    "model_semantic_terms",
    "model_semantic_term_postings",
    "model_search_documents",
    "model_sparse_terms",
    "model_belief_addresses",
    "model_answerability_index",
    "model_representation_tag_postings",
    "model_representation_feature_postings",
    "model_scope_entities",
    "model_events",
    "retrieval_affordance_profiles",
)

_RETRIEVAL_PROBE_PRIMITIVES = (
    "GOAL_IMPACT",
    "COMMITMENT",
    "OWNERSHIP",
    "DEPENDENCY",
    "COUNTEREVIDENCE",
)

_RETRIEVAL_PROBE_STATIC_TERM_CASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("common_generic", ("customers", "customer_resource", "execution")),
    ("operational_common", ("capacity", "billing", "support")),
    ("focused_goal", ("goal", "impact", "commitment")),
    (
        "background_noise",
        (
            "general operational chatter",
            "lunch logistics",
            "duplicated dashboard links",
            "non-actionable reminder",
        ),
    ),
)

_T1_BATCH_TRANSIENT_FAILURE_MARKERS = (
    "connecttimeout",
    "readtimeout",
    "writetimeout",
    "pooltimeout",
    "timeout",
    "timed out",
    "temporarily unavailable",
    "connection reset",
    "connection aborted",
    "connection refused",
    "server disconnected",
    "service unavailable",
    "too many requests",
    "rate limit",
    "circuit_open",
    "http 429",
    "http 500",
    "http 502",
    "http 503",
    "http 504",
)

_RETRIEVAL_PROBE_POSITIVE_ANSWERABILITY_CASES = frozenset(
    {
        "common_generic",
        "operational_common",
        "top_sparse_terms",
        "top_answerability_terms",
    }
)
_RETRIEVAL_PROBE_POSITIVE_SCOPED_SPARSE_CASES = frozenset(
    {
        "common_generic",
        "operational_common",
        "top_sparse_terms",
    }
)
_RETRIEVAL_PROBE_SIDECAR_SPECS: tuple[dict[str, Any], ...] = (
    {
        "table": "model_scope_entities",
        "required": True,
        "min_active_model_ratio": 0.90,
        "min_distinct_entities": 3,
    },
    {
        "table": "model_sparse_terms",
        "required": True,
        "min_active_model_ratio": 0.98,
        "min_rows_per_active_model": 8,
        "min_distinct_terms": 16,
    },
    {
        "table": "model_answerability_index",
        "required": True,
        "min_active_model_ratio": 0.98,
        "min_distinct_terms": 16,
        "min_probe_primitives": 3,
    },
    {
        "table": "model_representation_feature_postings",
        "required": True,
        "min_active_model_ratio": 0.98,
        "min_distinct_features": 16,
        "min_feature_types": 4,
    },
    {
        "table": "model_scope_actors",
        "required": False,
        "min_active_model_ratio": 0.0,
    },
    {
        "table": "model_semantic_term_postings",
        "required": False,
        "min_active_model_ratio": 0.0,
    },
    {
        "table": "model_operational_role_postings",
        "required": False,
        "min_active_model_ratio": 0.0,
    },
    {
        "table": "model_representation_tag_postings",
        "required": False,
        "min_active_model_ratio": 0.0,
    },
)

_DOWNSTREAM_TRIGGER_DRAIN_PREDICATE = "trigger_kind IN ('T2', 'T3', 'T4')"
_BACKGROUND_MAINTENANCE_TRIGGER_KINDS = frozenset({"T4"})


def _trigger_kind_family(trigger_kind: str | None) -> str:
    return str(trigger_kind or "").split(":", 1)[0]


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
    relation_frame_count: int = 0
    accepted_relation_frame_count: int = 0
    relation_frame_kind_hits: list[str] = field(default_factory=list)
    relation_frame_projection_count: int = 0
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
    "experience_metabolism",
    "negative_learning",
    "question_policy",
    "customer_value",
)


_THESIS_JUDGE_NAME = "storyline_thesis_recovery"
_THESIS_JUDGE_AGREEMENT_SET = (
    REPO_ROOT / "benchmarks" / "fyralis_eval" / "storyline_thesis_judge_agreement.json"
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
    future_validation_count = len(STORYLINES) * max(
        0, future_validation_signals_per_storyline
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
    capability_offset = 0
    story_cursor = 0
    horizon_end_batch = horizon_start_batch + target_t1_batches
    warmup_batches = _long_horizon_warmup_batches(horizon_end_batch)

    for batch_index in range(horizon_start_batch):
        sequence_kind = _long_horizon_sequence_kind(batch_index, warmup_batches)
        if sequence_kind == "noise":
            noise_offset += signals_per_batch
        elif sequence_kind == "capability_probe":
            story_cursor, story_counts = _long_horizon_story_filler_plan(
                story_cursor=story_cursor,
                signals_per_batch=signals_per_batch,
                reserved_special_slots=1,
            )
            for story_id, count in story_counts.items():
                story_offsets[story_id] += count
            capability_offset += signals_per_batch - sum(story_counts.values())
        elif sequence_kind == "future_validation":
            for item_index in range(signals_per_batch):
                story = STORYLINES[(batch_index + item_index) % len(STORYLINES)]
                future_offsets[story.id] += 1
        else:
            story = STORYLINES[story_cursor % len(STORYLINES)]
            story_offsets[story.id] += signals_per_batch
            story_cursor += 1

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
        elif sequence_kind == "capability_probe":
            sequence_name = f"capability_probe_wave_{batch_index + 1:03d}"
            next_story_cursor, story_counts = _long_horizon_story_filler_plan(
                story_cursor=story_cursor,
                signals_per_batch=signals_per_batch,
                reserved_special_slots=1,
            )
            special_slots = signals_per_batch - sum(story_counts.values())
            for _item_index in range(special_slots):
                signal = _make_capability_probe_signal(
                    signal_index,
                    capability_offset,
                )
                _decorate_long_horizon_signal(
                    signal,
                    batch_index=batch_index,
                    sequence_kind=sequence_kind,
                )
                signals.append(signal)
                signal_index += 1
                capability_offset += 1
            for story_id, count in story_counts.items():
                story = _storyline_by_id(story_id)
                for _ in range(count):
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
            story_cursor = next_story_cursor
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
            story = STORYLINES[story_cursor % len(STORYLINES)]
            sequence_name = f"{story.id}_horizon_wave_{batch_index + 1:03d}"
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
            story_cursor += 1
        sequences[sequence_name] = signals

    scenario.signal_sequences = sequences
    scenario.expected_behaviors = [
        "Two hundred T1 batches should preserve long-term memory health under repeated company change.",
        "Later waves should use compressed Models and graph context created by earlier waves.",
        "Future validation should update, confirm, or retire earlier inferred memory.",
        "Noise should remain bounded across a long horizon instead of accumulating into durable clutter.",
        "Unobserved transition gaps should become bounded inferred Models, not fabricated facts.",
        "Capability probe waves should exercise prediction, resource, ontology-gap, archive, evidence, and question-policy lifecycle paths.",
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
        "warmup_batches": warmup_batches,
        "storyline_gold": gold,
    }
    return scenario, gold


def _storyline_by_id(story_id: str) -> StorylineSpec:
    for story in STORYLINES:
        if story.id == story_id:
            return story
    raise KeyError(story_id)


def _long_horizon_story_filler_plan(
    *,
    story_cursor: int,
    signals_per_batch: int,
    reserved_special_slots: int,
) -> tuple[int, dict[str, int]]:
    """Use spare probe-wave slots to finish the current storyline coverage cycle."""

    available_slots = max(0, signals_per_batch - max(0, reserved_special_slots))
    story_position = story_cursor % len(STORYLINES)
    if available_slots <= 0 or story_position == 0:
        return story_cursor, {}

    pending_stories = len(STORYLINES) - story_position
    counts: dict[str, int] = {}
    next_cursor = story_cursor
    remaining_slots = available_slots
    while pending_stories > 0 and remaining_slots > 0:
        story = STORYLINES[next_cursor % len(STORYLINES)]
        take = max(1, (remaining_slots + pending_stories - 1) // pending_stories)
        counts[story.id] = take
        remaining_slots -= take
        pending_stories -= 1
        next_cursor += 1
    return next_cursor, counts


def _long_horizon_warmup_batches(horizon_end_batch: int) -> int:
    if horizon_end_batch <= 0:
        return 0
    if horizon_end_batch <= 10:
        return min(4, max(0, horizon_end_batch - 1))
    return min(len(STORYLINES) * 2, max(0, horizon_end_batch - 5))


def _long_horizon_sequence_kind(batch_index: int, warmup_batches: int) -> str:
    batch_number = batch_index + 1
    if batch_index >= warmup_batches and batch_number % 10 == 0:
        return "noise"
    if batch_index >= warmup_batches and batch_number % 9 == 0:
        return "capability_probe"
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
        return "It needs review before the graph should treat the link as durable."
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


_ALL_CAPABILITY_PROBE_KINDS: tuple[str, ...] = (
    "prediction",
    "resource",
    "ontology_gap",
    "archive",
    "evidence_attachment",
    "question_policy",
)

_CAPABILITY_PROBE_KIND_GROUPS: tuple[tuple[str, ...], ...] = (
    _ALL_CAPABILITY_PROBE_KINDS,
    ("prediction",),
    ("resource",),
    ("ontology_gap",),
    ("archive",),
    ("evidence_attachment", "question_policy"),
)

_CAPABILITY_PROBE_TEXT: dict[str, str] = {
    "prediction": (
        "prediction lifecycle: create a Prediction model with an explicit "
        "evaluate_at deadline for Friday's enterprise-control launch decision."
    ),
    "resource": (
        "resource_ops lifecycle: create a constrained capacity Resource for "
        "security-review hours so action-resource planning is exercised."
    ),
    "ontology_gap": (
        "ontology_gap_ops lifecycle: propose a missing edge type for a "
        "regulatory exemption that gates launch progress more precisely than "
        "plain blocks."
    ),
    "archive": (
        "archive lifecycle: retire or archive stale memory when newer launch "
        "evidence supersedes an older assumption."
    ),
    "evidence_attachment": (
        "evidence attachment lifecycle: attach this low-durability repeated "
        "review feeling as evidence to existing memory instead of creating "
        "another durable Model."
    ),
    "question_policy": (
        "question_policy lifecycle: record whether a missing-context question "
        "would have helped, so future retrieval learns when to ask."
    ),
}


def _make_capability_probe_signal(
    signal_index: int,
    local_index: int,
) -> dict[str, Any]:
    kinds = _CAPABILITY_PROBE_KIND_GROUPS[
        local_index % len(_CAPABILITY_PROBE_KIND_GROUPS)
    ]
    kind_text = " ".join(_CAPABILITY_PROBE_TEXT[kind] for kind in kinds)
    kind_csv = ",".join(kinds)
    text = (
        "Capability probe for storyline_batch. "
        f"capability_probe=true capability_probe_kinds={kind_csv}. "
        f"{kind_text} "
        "Use the normal Think path and emit the concrete lifecycle write if "
        "the retrieved context provides a valid target."
    )
    content = {
        "text": text,
        "benchmark": "storyline_batch",
        "family": "capability_probe",
        "phase": "capability_probe",
        "capability_probe": True,
        "capability_probe_kind": kinds[0],
        "capability_probe_kinds": list(kinds),
        "signal_index": signal_index,
        "local_index": local_index,
        "entity_names": {},
    }
    return {
        "channel": "ops:capability-probe",
        "actor": "Maya Chen",
        "delay_minutes": float(signal_index * 3),
        "content": text,
        "content_dict": content,
        "trust_tier": "authoritative",
        "external_id": f"storyline:capability_probe:{local_index:03d}",
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


def _read_json_list(path: Path) -> list[Any]:
    try:
        parsed = json.loads(path.read_text())
    except FileNotFoundError:
        return []
    if not isinstance(parsed, list):
        return []
    return parsed


def _storyline_score_from_artifact(row: dict[str, Any]) -> StorylineScore:
    payload: dict[str, Any] = {}
    for field_info in dataclass_fields(StorylineScore):
        name = field_info.name
        if name in row:
            payload[name] = row[name]
            continue
        has_default = field_info.default is not MISSING
        has_default_factory = field_info.default_factory is not MISSING
        if has_default or has_default_factory:
            continue
        raise ValueError(f"storyline score artifact missing required field: {name}")
    return StorylineScore(**payload)


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
        scenario.base_time = (
            await conn.fetchval(
                """
            SELECT occurred_at
            FROM observations
            WHERE tenant_id = $1
              AND source_channel = 'internal:scenario_loader'
            ORDER BY occurred_at ASC
            LIMIT 1
            """,
                tenant_id,
            )
            or await conn.fetchval(
                """
            SELECT MIN(occurred_at)
            FROM observations
            WHERE tenant_id = $1
            """,
                tenant_id,
            )
            or datetime.now(timezone.utc)
        )

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
            str(row["name"]): row["id"] for row in actor_rows if row["name"] is not None
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
        scenario.goals = await _fetch_title_id_map(
            conn, "goals", goal_titles, tenant_id
        )
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


@dataclass(frozen=True)
class _BenchmarkInputs:
    report_dir: Path
    append_context: dict[str, Any] | None
    horizon_start_batch: int
    scenario: Scenario
    gold: Any
    run_config: dict[str, Any]


@dataclass(frozen=True)
class _BenchmarkRuntime:
    pool: asyncpg.Pool
    embedder: OllamaClient


@dataclass(frozen=True)
class _PreparedBenchmarkTenant:
    tenant_id: UUID
    actor_repo: ActorRepo
    alias_repo: EntityAliasRepo
    seed_status: dict[str, Any]


def _resolve_storyline_run_id(args: argparse.Namespace) -> str:
    run_id = args.run_id
    if run_id:
        return run_id
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if getattr(args, "mode", None) == "retrieval-probe":
        base = getattr(args, "append_to_run_id", None) or "tenant"
        return f"{base}-retrieval-probe-{timestamp}"
    if getattr(args, "append_to_run_id", None):
        return f"{args.append_to_run_id}-append-{args.target_t1_batches}-{timestamp}"
    return f"storyline-batch-{timestamp}"


def _build_storyline_benchmark_inputs(
    args: argparse.Namespace,
    *,
    run_id: str,
) -> _BenchmarkInputs:
    report_dir = args.report_root / run_id
    append_context = _load_append_context(args)
    foundation_namespace = (
        str(append_context["foundation_namespace"]) if append_context else None
    )
    horizon_start_batch = (
        int(append_context["horizon_start_batch"])
        if append_context
        else int(args.horizon_start_batch or 0)
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
    return _BenchmarkInputs(
        report_dir=report_dir,
        append_context=append_context,
        horizon_start_batch=horizon_start_batch,
        scenario=scenario,
        gold=gold,
        run_config=run_config,
    )


async def _open_storyline_benchmark_runtime(
    args: argparse.Namespace,
) -> _BenchmarkRuntime:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set")
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
    )
    return _BenchmarkRuntime(
        pool=pool,
        embedder=OllamaClient(OllamaConfig.from_env()),
    )


async def _prepare_storyline_benchmark_tenant(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    scenario: Scenario,
    append_context: dict[str, Any] | None,
    run_id: str,
    horizon_start_batch: int,
) -> _PreparedBenchmarkTenant:
    if not args.skip_migrations:
        async with pool.acquire() as conn:
            await apply_migrations_dir(
                conn,
                REPO_ROOT / "db" / "migrations",
                on_error="warn",
            )
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
            f"horizon_start_batch={horizon_start_batch}",
            flush=True,
        )
        return _PreparedBenchmarkTenant(
            tenant_id=tenant_id,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            seed_status=seed_status,
        )

    await materialize(scenario, pool=pool)
    if scenario.tenant_id is None:
        raise RuntimeError("scenario materialize did not set tenant_id")
    tenant_id = scenario.tenant_id
    print(f"tenant={tenant_id} run_id={run_id}", flush=True)
    await _insert_extra_aliases(scenario, alias_repo)
    seed_status = {
        "requested_models": args.seed_models,
        "families": args.seed_families,
        "models": 0,
    }
    return _PreparedBenchmarkTenant(
        tenant_id=tenant_id,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        seed_status=seed_status,
    )


async def _maybe_seed_storyline_models(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    append_context: dict[str, Any] | None,
    seed_status: dict[str, Any],
) -> dict[str, Any]:
    if not args.seed_models or append_context:
        return seed_status

    from scripts.run_incremental_feedback_loop_stress import _seed_company

    db_preflight = await _seed_database_preflight(
        pool,
        requested_models=args.seed_models,
        allow_failures=bool(getattr(args, "allow_seed_db_preflight_failures", False)),
    )
    seeded = await _seed_company(
        pool,
        tenant_id=tenant_id,
        families=args.seed_families,
        total_models=args.seed_models,
        suppress_legacy_postings=not bool(
            getattr(args, "keep_legacy_seed_postings", False)
        ),
    )
    analyze_status = await _analyze_post_seed_lookup_tables(pool)
    seeded_status = {
        "requested_models": args.seed_models,
        "families": args.seed_families,
        "models": seeded.total_models,
        "insert_ms": round(seeded.insert_ms, 3),
        "analyze": analyze_status,
        "db_preflight": db_preflight,
        "sidecars": seeded.sidecars,
        "timings": seeded.timings,
    }
    print(f"seed_status={json.dumps(seeded_status, sort_keys=True)}", flush=True)
    return seeded_status


async def _pre_first_wave_memory_snapshot(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> tuple[dict[str, int], dict[str, int]]:
    """Measure semantic memory and structural scaffolding before wave one."""

    semantic_tables = {
        "models": "models",
        "model_edges": "model_edges",
        "pattern_candidates": "pattern_candidates",
        "hypotheses": "sage_latent_gap_hypotheses",
    }
    scaffolding_tables = {
        "actors": "actors",
        "resources": "resources",
        "customers": "customers",
        "commitments": "commitments",
        "goals": "goals",
        "decisions": "decisions",
        "entity_aliases": "entity_aliases",
        "observations": "observations",
    }
    async with pool.acquire() as conn:
        async def _tenant_count(table: str) -> int:
            exists = await conn.fetchval(
                "SELECT to_regclass('public.' || $1) IS NOT NULL",
                table,
            )
            if not exists:
                return 0
            return int(
                await conn.fetchval(
                    f"SELECT count(*)::bigint FROM {table} WHERE tenant_id = $1",
                    tenant_id,
                )
                or 0
            )

        semantic = {
            name: await _tenant_count(table)
            for name, table in semantic_tables.items()
        }
        scaffolding = {
            "tenant": int(
                await conn.fetchval(
                    "SELECT count(*)::bigint FROM tenants WHERE id = $1",
                    tenant_id,
                )
                or 0
            ),
            **{
                name: await _tenant_count(table)
                for name, table in scaffolding_tables.items()
            },
        }
    return semantic, scaffolding


async def _seed_database_preflight(
    pool: asyncpg.Pool,
    *,
    requested_models: int,
    allow_failures: bool = False,
) -> dict[str, Any]:
    if requested_models < _SEED_PREFLIGHT_MIN_MODELS:
        return {
            "status": "skipped",
            "reason": "small_seed",
            "requested_models": requested_models,
            "min_models": _SEED_PREFLIGHT_MIN_MODELS,
        }

    async with pool.acquire() as conn:
        test_trigger_rows = await conn.fetch(
            """
            SELECT tgrelid::regclass::text AS table_name,
                   tgname AS trigger_name
            FROM pg_trigger
            WHERE NOT tgisinternal
              AND tgname = '_test_auto_register_tenant'
            ORDER BY tgrelid::regclass::text
            LIMIT 20
            """
        )
        test_trigger_count = int(
            await conn.fetchval(
                """
                SELECT count(*)::int
                FROM pg_trigger
                WHERE NOT tgisinternal
                  AND tgname = '_test_auto_register_tenant'
                """
            )
            or 0
        )
        bloat_rows = await conn.fetch(
            """
            SELECT c.relname AS table_name,
                   pg_total_relation_size(c.oid)::bigint AS total_bytes,
                   pg_relation_size(c.oid)::bigint AS heap_bytes,
                   pg_indexes_size(c.oid)::bigint AS index_bytes,
                   COALESCE(s.n_live_tup, 0)::bigint AS live_rows
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
              AND c.relname = ANY($1::text[])
              AND COALESCE(s.n_live_tup, 0) = 0
              AND pg_indexes_size(c.oid) >= $2
            ORDER BY pg_indexes_size(c.oid) DESC, c.relname
            """,
            list(_SEED_PREFLIGHT_BLOAT_TABLES),
            _SEED_PREFLIGHT_BLOAT_BYTES,
        )

    failures: list[str] = []
    warnings: list[str] = []
    if test_trigger_count:
        examples = [
            f"{row['table_name']}.{row['trigger_name']}"
            for row in test_trigger_rows[:5]
        ]
        failures.append(
            "seed database has test tenant auto-register triggers installed "
            f"(count={test_trigger_count}, examples={examples})"
        )
    bloat_tables = [
        {
            "table": str(row["table_name"]),
            "total_bytes": int(row["total_bytes"] or 0),
            "heap_bytes": int(row["heap_bytes"] or 0),
            "index_bytes": int(row["index_bytes"] or 0),
            "live_rows": int(row["live_rows"] or 0),
        }
        for row in bloat_rows
    ]
    if bloat_tables:
        failures.append(
            "seed database has large empty model-derived indexes; use a fresh "
            f"or reindexed DB before a large seed (tables="
            f"{[row['table'] for row in bloat_tables[:6]]})"
        )
    status = "passed" if not failures else "failed"
    result = {
        "status": status,
        "requested_models": requested_models,
        "test_auto_register_trigger_count": test_trigger_count,
        "test_auto_register_trigger_examples": [
            {
                "table": str(row["table_name"]),
                "trigger": str(row["trigger_name"]),
            }
            for row in test_trigger_rows
        ],
        "empty_index_bloat_threshold_bytes": _SEED_PREFLIGHT_BLOAT_BYTES,
        "empty_index_bloat_tables": bloat_tables,
        "failures": failures,
        "warnings": warnings,
    }
    if failures and not allow_failures:
        raise RuntimeError(
            "seed database preflight failed: "
            + "; ".join(failures)
            + " (override with --allow-seed-db-preflight-failures for exploratory runs)"
        )
    if failures:
        warnings.extend(failures)
    return result


async def _analyze_post_seed_lookup_tables(pool: asyncpg.Pool) -> dict[str, Any]:
    started = time.monotonic()
    analyzed: list[str] = []
    table_timings_ms: dict[str, float] = {}
    async with pool.acquire() as conn:
        for table in _POST_SEED_ANALYZE_TABLES:
            exists = await conn.fetchval("SELECT to_regclass($1)", f"public.{table}")
            if exists is None:
                continue
            table_started = time.monotonic()
            await conn.execute(f"ANALYZE {table}")
            table_timings_ms[table] = round(
                (time.monotonic() - table_started) * 1000,
                3,
            )
            analyzed.append(table)
    return {
        "tables": analyzed,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "table_timings_ms": table_timings_ms,
    }


def _build_storyline_worker(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    run_id: str,
    embedder: OllamaClient,
) -> ThinkWorker:
    provider = _build_cached_provider()
    return ThinkWorker(
        pool,
        config=WorkerConfig(
            poll_batch=max(2, args.worker_poll_batch),
            max_concurrency_per_tenant=1,
            tenant_filter=tenant_id,
            worker_id=f"storyline-{run_id}",
            # Cost-plan §2.3 A/B arm: window=0 disables T1 batching so the
            # unbatched arm drains each event_arrival trigger as a single run.
            t1_batch_window_s=(0.0 if args.unbatched_run else args.t1_batch_window_s),
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


async def _process_storyline_benchmark_waves(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    scenario: Scenario,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    worker: ThinkWorker,
    tenant_id: UUID,
    run_id: str,
    report_dir: Path,
) -> tuple[list[UUID], list[dict[str, Any]]]:
    observation_ids: list[UUID] = []
    waves: list[dict[str, Any]] = []
    offset = 0
    all_sequences = list(scenario.signal_sequences.items())
    for wave_index, (sequence_name, signals) in enumerate(all_sequences, start=1):
        if sequence_name.startswith("background_noise") and args.skip_noise_think:
            offset += len(signals)
            continue
        print(
            f"wave={wave_index} sequence={sequence_name} " f"signals={len(signals)}",
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
                retry_attempts=args.t1_batch_retry_attempts,
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
    return observation_ids, waves


async def _collect_storyline_benchmark_summary(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    scenario: Scenario,
    run_id: str,
    report_dir: Path,
    run_config: dict[str, Any],
    seed_status: dict[str, Any],
    observation_ids: list[UUID],
    waves: list[dict[str, Any]],
    post_commit_status: dict[str, Any],
    topology_status: dict[str, Any],
    adaptive_drain_status: dict[str, Any],
    append_context: dict[str, Any] | None,
    horizon_start_batch: int,
    semantic_memory_before_first_wave: dict[str, int],
    pre_first_wave_scaffolding: dict[str, int],
    started: float,
) -> dict[str, Any]:
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
    model_summary["adaptive_drain_status"] = adaptive_drain_status
    model_summary["semantic_memory_before_first_wave"] = dict(
        semantic_memory_before_first_wave
    )
    model_summary["pre_first_wave_scaffolding"] = dict(
        pre_first_wave_scaffolding
    )
    model_summary["future_validation_events"] = await _count_future_validation_events(
        pool, tenant_id=tenant_id
    )
    model_summary["capability_probe_counts"] = await _collect_capability_probe_counts(
        pool, tenant_id=tenant_id
    )
    model_summary["lifecycle_obligation_report"] = (
        await _collect_lifecycle_obligation_report(
            pool,
            tenant_id=tenant_id,
        )
    )
    model_summary["edge_lifecycle"] = await _collect_edge_lifecycle_report(
        pool,
        tenant_id=tenant_id,
    )
    model_summary["relation_frame_lifecycle"] = (
        await _collect_relation_frame_lifecycle_report(
            pool,
            tenant_id=tenant_id,
        )
    )
    model_summary["think_edge_ops_stats"] = await _collect_think_edge_ops_report(
        pool,
        tenant_id=tenant_id,
        future_trigger_ids=_future_wave_trigger_ids(waves),
    )
    model_summary["projection_metabolism"] = await _collect_projection_metabolism_report(
        pool,
        tenant_id=tenant_id,
    )
    model_summary[
        "question_planner_reflective_report"
    ] = await _collect_question_planner_reflective_report(
        pool,
        tenant_id=tenant_id,
    )
    model_summary["latency_breakdown"] = await _collect_latency_breakdown(
        pool,
        tenant_id=tenant_id,
        waves=waves,
    )
    model_summary["think_cost_profile"] = await _collect_think_cost_profile(
        pool,
        tenant_id=tenant_id,
    )
    model_summary[
        "post_commit_action_profile"
    ] = await _collect_post_commit_action_profile(
        pool,
        tenant_id=tenant_id,
    )
    model_summary["downstream_suppression"] = await _collect_downstream_suppression(
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
        model_summary["run_observation_count"] = model_summary.get("observation_count")
        model_summary["signal_count"] = cumulative_signal_count
        (report_dir / "model_layer_summary.md").write_text(
            _render_model_layer_markdown(model_summary)
        )
    _write_json(report_dir / "run_summary.json", model_summary)
    return model_summary


async def _write_storyline_benchmark_outputs(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    scenario: Scenario,
    model_summary: dict[str, Any],
    waves: list[dict[str, Any]],
    report_dir: Path,
    started: float,
) -> dict[str, Any]:
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
    _write_json(report_dir / "benchmark_summary.json", benchmark_summary)
    _write_json(report_dir / "storyline_scores.json", benchmark_summary)
    (report_dir / "benchmark_summary.md").write_text(
        _render_benchmark_markdown(benchmark_summary)
    )
    print(f"report_dir={report_dir}", flush=True)
    return benchmark_summary


async def _write_seed_only_outputs(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    scenario: Scenario,
    run_id: str,
    report_dir: Path,
    run_config: dict[str, Any],
    seed_status: dict[str, Any],
    started: float,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        active_model_count = int(
            await conn.fetchval(
                """
                SELECT count(*)::bigint
                FROM models
                WHERE tenant_id = $1
                  AND status = 'active'
                """,
                tenant_id,
            )
            or 0
        )
        observation_count = int(
            await conn.fetchval(
                """
                SELECT count(*)::bigint
                FROM observations
                WHERE tenant_id = $1
                """,
                tenant_id,
            )
            or 0
        )
    summary = {
        "mode": "seed-only",
        "tenant_id": str(tenant_id),
        "run_id": run_id,
        "seed_status": seed_status,
        "active_model_count": active_model_count,
        "observation_count": observation_count,
        "planned_signal_count": _signal_count(scenario),
        "processed_signal_count": 0,
        "append_ready": not bool(args.cleanup),
        "append_example": (
            "Use --mode run --append-to-run-id "
            f"{run_id} --target-t1-batches N without --cleanup."
        ),
        "run_config": run_config,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    _write_json(report_dir / "run_summary.json", summary)
    _write_json(report_dir / "benchmark_summary.json", summary)
    _write_json(report_dir / "storyline_scores.json", summary)
    (report_dir / "benchmark_summary.md").write_text(
        "\n".join(
            [
                "# Storyline Seed Baseline",
                "",
                f"- Run id: `{run_id}`",
                f"- Tenant: `{tenant_id}`",
                f"- Active models: {active_model_count}",
                f"- Observations: {observation_count}",
                f"- Append ready: {summary['append_ready']}",
                "",
                "This run intentionally skips signal injection, Think, scoring, "
                "adaptive drain, and cleanup unless `--cleanup` was requested.",
            ]
        )
    )
    print(f"report_dir={report_dir}", flush=True)
    return summary


def _retrieval_probe_tenant_id(args: argparse.Namespace) -> UUID:
    if getattr(args, "append_tenant_id", None):
        return UUID(str(args.append_tenant_id))
    append_context = _load_append_context(args)
    if append_context is None:
        raise SystemExit(
            "--mode retrieval-probe requires --append-to-run-id or --append-tenant-id"
        )
    return UUID(str(append_context["tenant_id"]))


async def _retrieval_probe_table_exists(
    conn: asyncpg.Connection,
    table: str,
) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1)", f"public.{table}"))


async def _retrieval_probe_column_exists(
    conn: asyncpg.Connection,
    *,
    table: str,
    column: str,
) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.columns
              WHERE table_schema = 'public'
                AND table_name = $1
                AND column_name = $2
            )
            """,
            table,
            column,
        )
    )


async def _retrieval_probe_sidecar_preflight(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    active_model_count = int(
        await conn.fetchval(
            """
            SELECT count(*)::bigint
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
            """,
            tenant_id,
        )
        or 0
    )
    failures: list[str] = []
    warnings: list[str] = []
    tables: dict[str, Any] = {}
    for spec in _RETRIEVAL_PROBE_SIDECAR_SPECS:
        table = str(spec["table"])
        required = bool(spec["required"])
        min_active_model_ratio = float(spec["min_active_model_ratio"])
        exists = await _retrieval_probe_table_exists(conn, table)
        table_status: dict[str, Any] = {
            "exists": exists,
            "required": required,
            "min_active_model_ratio": min_active_model_ratio,
        }
        tables[table] = table_status
        if not exists:
            message = f"retrieval sidecar table missing: {table}"
            if required:
                failures.append(message)
            else:
                warnings.append(message)
            continue

        has_status = await _retrieval_probe_column_exists(
            conn,
            table=table,
            column="status",
        )
        active_condition = "sidecar.status = 'active'" if has_status else "TRUE"

        row = await conn.fetchrow(
            f"""
            WITH active_models AS (
              SELECT id, tenant_id
              FROM models
              WHERE tenant_id = $1
                AND status = 'active'
            )
            SELECT
              (
                SELECT count(*)::bigint
                FROM {table} sidecar
                WHERE sidecar.tenant_id = $1
              ) AS row_count,
              (
                SELECT count(*)::bigint
                FROM {table} sidecar
                WHERE sidecar.tenant_id = $1
                  AND {active_condition}
              ) AS active_row_count,
              (
                SELECT count(*)::bigint
                FROM active_models model
                WHERE EXISTS (
                  SELECT 1
                  FROM {table} sidecar
                  WHERE sidecar.tenant_id = model.tenant_id
                    AND sidecar.model_id = model.id
                    AND {active_condition}
                )
              ) AS active_model_hit_count,
              (
                SELECT count(*)::bigint
                FROM {table} sidecar
                LEFT JOIN models model
                  ON model.tenant_id = sidecar.tenant_id
                 AND model.id = sidecar.model_id
                WHERE sidecar.tenant_id = $1
                  AND model.id IS NULL
              ) AS orphan_row_count
            """,
            tenant_id,
        )
        row_count = int((row or {}).get("row_count") or 0)
        active_row_count = int((row or {}).get("active_row_count") or 0)
        active_model_hit_count = int((row or {}).get("active_model_hit_count") or 0)
        orphan_row_count = int((row or {}).get("orphan_row_count") or 0)
        active_model_ratio = (
            active_model_hit_count / active_model_count
            if active_model_count > 0
            else 0.0
        )
        table_status.update(
            {
                "has_status": has_status,
                "row_count": row_count,
                "active_row_count": active_row_count,
                "active_model_hit_count": active_model_hit_count,
                "active_model_ratio": round(active_model_ratio, 4),
                "orphan_row_count": orphan_row_count,
            }
        )
        if int(spec.get("min_rows_per_active_model") or 0):
            min_rows = active_model_count * int(spec["min_rows_per_active_model"])
            table_status["min_active_rows"] = min_rows
            if required and active_row_count < min_rows:
                failures.append(
                    f"required retrieval sidecar row density too low: {table} "
                    f"{active_row_count} < {min_rows}"
                )
        if int(spec.get("min_distinct_entities") or 0):
            entity_count = int(
                await conn.fetchval(
                    f"""
                    SELECT count(DISTINCT entity_type || ':' || entity_id::text)::bigint
                    FROM {table} sidecar
                    WHERE sidecar.tenant_id = $1
                    """,
                    tenant_id,
                )
                or 0
            )
            table_status["distinct_entities"] = entity_count
            if required and entity_count < int(spec["min_distinct_entities"]):
                failures.append(
                    f"required retrieval sidecar entity variety too low: {table} "
                    f"{entity_count} < {int(spec['min_distinct_entities'])}"
                )
        if int(spec.get("min_distinct_terms") or 0):
            term_count = int(
                await conn.fetchval(
                    f"""
                    SELECT count(DISTINCT term)::bigint
                    FROM {table} sidecar
                    WHERE sidecar.tenant_id = $1
                      AND {active_condition}
                    """,
                    tenant_id,
                )
                or 0
            )
            table_status["distinct_terms"] = term_count
            if required and term_count < int(spec["min_distinct_terms"]):
                failures.append(
                    f"required retrieval sidecar term variety too low: {table} "
                    f"{term_count} < {int(spec['min_distinct_terms'])}"
                )
        if int(spec.get("min_probe_primitives") or 0):
            primitive_count = int(
                await conn.fetchval(
                    f"""
                    SELECT count(DISTINCT primitive)::bigint
                    FROM {table} sidecar
                    WHERE sidecar.tenant_id = $1
                      AND {active_condition}
                      AND primitive = ANY($2::text[])
                    """,
                    tenant_id,
                    list(_RETRIEVAL_PROBE_PRIMITIVES),
                )
                or 0
            )
            table_status["probe_primitive_count"] = primitive_count
            if required and primitive_count < int(spec["min_probe_primitives"]):
                failures.append(
                    f"required retrieval sidecar primitive variety too low: {table} "
                    f"{primitive_count} < {int(spec['min_probe_primitives'])}"
                )
        if int(spec.get("min_distinct_tags") or 0):
            tag_count = int(
                await conn.fetchval(
                    f"""
                    SELECT count(DISTINCT tag_type || ':' || tag)::bigint
                    FROM {table} sidecar
                    WHERE sidecar.tenant_id = $1
                      AND {active_condition}
                    """,
                    tenant_id,
                )
                or 0
            )
            table_status["distinct_tags"] = tag_count
            if required and tag_count < int(spec["min_distinct_tags"]):
                failures.append(
                    f"required retrieval sidecar tag variety too low: {table} "
                    f"{tag_count} < {int(spec['min_distinct_tags'])}"
                )
        if int(spec.get("min_distinct_features") or 0):
            feature_count = int(
                await conn.fetchval(
                    f"""
                    SELECT count(DISTINCT feature_type || ':' || feature)::bigint
                    FROM {table} sidecar
                    WHERE sidecar.tenant_id = $1
                      AND {active_condition}
                    """,
                    tenant_id,
                )
                or 0
            )
            table_status["distinct_features"] = feature_count
            if required and feature_count < int(spec["min_distinct_features"]):
                failures.append(
                    f"required retrieval sidecar feature variety too low: {table} "
                    f"{feature_count} < {int(spec['min_distinct_features'])}"
                )
        if int(spec.get("min_feature_types") or 0):
            feature_type_count = int(
                await conn.fetchval(
                    f"""
                    SELECT count(DISTINCT feature_type)::bigint
                    FROM {table} sidecar
                    WHERE sidecar.tenant_id = $1
                      AND {active_condition}
                    """,
                    tenant_id,
                )
                or 0
            )
            table_status["feature_type_count"] = feature_type_count
            if required and feature_type_count < int(spec["min_feature_types"]):
                failures.append(
                    f"required retrieval sidecar feature-type variety too low: {table} "
                    f"{feature_type_count} < {int(spec['min_feature_types'])}"
                )
        if required and active_model_count > 0:
            if active_row_count <= 0:
                failures.append(f"required retrieval sidecar has no active rows: {table}")
            if active_model_ratio < min_active_model_ratio:
                failures.append(
                    f"required retrieval sidecar coverage too low: {table} "
                    f"{active_model_ratio:.1%} < {min_active_model_ratio:.1%}"
                )
        if orphan_row_count > 0:
            message = (
                f"retrieval sidecar has orphan rows: {table} "
                f"orphan_rows={orphan_row_count}"
            )
            if required:
                failures.append(message)
            else:
                warnings.append(message)

    if active_model_count <= 0:
        failures.append("retrieval probe tenant has no active Models")
    return {
        "status": "passed" if not failures else "failed",
        "active_model_count": active_model_count,
        "tables": tables,
        "failures": failures,
        "warnings": warnings,
    }


async def _retrieval_probe_seed_pairs(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> tuple[list[dict[str, str]], list[tuple[str, UUID]]]:
    from services.platform.execution.retrieval_actions import focused_seed_entity_pairs

    if not await _retrieval_probe_table_exists(conn, "model_scope_entities"):
        return [], []
    rows = await conn.fetch(
        """
        SELECT entity_type, entity_id, count(*)::int AS model_count
        FROM model_scope_entities
        WHERE tenant_id = $1
        GROUP BY entity_type, entity_id
        ORDER BY model_count DESC, entity_type, entity_id
        LIMIT 3
        """,
        tenant_id,
    )
    raw_entities = [
        {"type": str(row["entity_type"]), "id": str(row["entity_id"])}
        for row in rows
        if row["entity_type"] and row["entity_id"]
    ]
    return raw_entities, focused_seed_entity_pairs(raw_entities)


async def _retrieval_probe_term_cases(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = [
        {"name": name, "terms": list(terms), "source": "static"}
        for name, terms in _RETRIEVAL_PROBE_STATIC_TERM_CASES
    ]

    async def add_top_terms(table: str, name: str) -> None:
        if not await _retrieval_probe_table_exists(conn, table):
            return
        rows = await conn.fetch(
            f"""
            SELECT term, count(*)::int AS df
            FROM {table}
            WHERE tenant_id = $1
              AND status = 'active'
            GROUP BY term
            ORDER BY df DESC, term
            LIMIT 4
            """,
            tenant_id,
        )
        terms = [str(row["term"]) for row in rows if row["term"]]
        if terms:
            cases.append(
                {
                    "name": name,
                    "terms": terms,
                    "source": table,
                    "dfs": {
                        str(row["term"]): int(row["df"] or 0)
                        for row in rows
                        if row["term"]
                    },
                }
            )

    await add_top_terms("model_sparse_terms", "top_sparse_terms")
    await add_top_terms("model_answerability_index", "top_answerability_terms")
    return cases


async def _time_retrieval_probe_call(
    *,
    label: str,
    max_ms: float,
    call: Any,
    min_rows: int = 0,
    max_rows: int | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    rows = await call
    elapsed_ms = round((time.monotonic() - started) * 1000, 3)
    row_count = len(rows or [])
    timed_out = bool(getattr(rows, "timed_out", False))
    latency_passed = elapsed_ms <= max_ms
    coverage_passed = row_count >= max(0, int(min_rows))
    max_row_count = None if max_rows is None else max(0, int(max_rows))
    excess_passed = max_row_count is None or row_count <= max_row_count
    timeout_passed = not timed_out
    return {
        "label": label,
        "elapsed_ms": elapsed_ms,
        "row_count": row_count,
        "min_rows": max(0, int(min_rows)),
        "max_rows": max_row_count,
        "timed_out": timed_out,
        "latency_passed": latency_passed,
        "coverage_passed": coverage_passed,
        "excess_passed": excess_passed,
        "timeout_passed": timeout_passed,
        "passed": (
            latency_passed and coverage_passed and excess_passed and timeout_passed
        ),
    }


async def _time_focused_action_probe_call(
    *,
    label: str,
    max_ms: float,
    call: Any,
    min_rows: int,
    min_sources: int,
) -> dict[str, Any]:
    started = time.monotonic()
    result = await call
    elapsed_ms = round((time.monotonic() - started) * 1000, 3)
    models = list(getattr(result, "models", []) or []) if result is not None else []
    notes = dict(getattr(result, "notes", {}) or {}) if result is not None else {}
    source_set: set[str] = set()
    for hit in notes.get("top_hits") or []:
        if not isinstance(hit, dict):
            continue
        raw_sources = hit.get("sources")
        if isinstance(raw_sources, list):
            source_set.update(str(source) for source in raw_sources)
    scan_timeouts = notes.get("scan_timeouts") if isinstance(notes, dict) else {}
    if not isinstance(scan_timeouts, dict):
        scan_timeouts = {}
    timed_out = any(bool(value) for value in scan_timeouts.values())
    latency_passed = elapsed_ms <= max_ms
    coverage_passed = len(models) >= max(0, int(min_rows))
    source_passed = len(source_set) >= max(0, int(min_sources))
    timeout_passed = not timed_out
    return {
        "label": label,
        "elapsed_ms": elapsed_ms,
        "row_count": len(models),
        "min_rows": max(0, int(min_rows)),
        "source_count": len(source_set),
        "source_set": sorted(source_set),
        "min_sources": max(0, int(min_sources)),
        "timed_out": timed_out,
        "latency_passed": latency_passed,
        "coverage_passed": coverage_passed,
        "source_passed": source_passed,
        "timeout_passed": timeout_passed,
        "passed": (
            latency_passed and coverage_passed and source_passed and timeout_passed
        ),
        "notes": {
            "answerability_hits": notes.get("answerability_hits"),
            "scoped_sparse_hits": notes.get("scoped_sparse_hits"),
            "direct_scope_hits": notes.get("direct_scope_hits"),
            "scan_timeouts": {
                str(key): bool(value) for key, value in scan_timeouts.items()
            },
            "bounded_lookup_timeout_count": int(
                notes.get("bounded_lookup_timeout_count") or 0
            ),
            "merged_hits": notes.get("merged_hits"),
            "returned_models": notes.get("returned_models"),
            "source_set": sorted(source_set),
        },
    }


async def _rollback_focused_action_probe(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    raw_seed_entities: list[dict[str, str]],
    terms: list[str],
    model_limit: int,
) -> Any:
    from services.platform.execution.config import InquiryConfig
    from services.platform.execution.retrieval_actions import execute_focused_index_action
    from services.platform.execution.types import RetrievalAction
    from services.reasoning.retrieval.primary import TriggerContext

    transaction = conn.transaction()
    await transaction.start()
    try:
        trigger = TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_entity_ids=list(raw_seed_entities),
            seed_natural_text=" ".join(terms),
            seed_occurred_at=datetime.now(timezone.utc),
        )
        action = RetrievalAction(
            question_id="Q_RETRIEVAL_PROBE",
            path="focused_index",
            target="focused_action_probe",
            query=" ".join(terms),
            filters={
                "primitive": "DEPENDENCY",
                "terms": terms,
                "seed_entities": list(raw_seed_entities),
            },
            budget=max(1, int(model_limit)),
        )
        return await execute_focused_index_action(
            action,
            trigger,
            conn,
            InquiryConfig(),
            model_limit=max(1, int(model_limit)),
        )
    finally:
        await transaction.rollback()


def _retrieval_probe_min_rows(
    *,
    path: str,
    case: dict[str, Any] | None = None,
    seed_pairs: list[tuple[str, UUID]] | None = None,
) -> int:
    if path == "focused_direct_scope":
        return 1 if seed_pairs else 0
    case_name = str((case or {}).get("name") or "")
    if path in {"focused_answerability", "sage_answerability"}:
        return 1 if case_name in _RETRIEVAL_PROBE_POSITIVE_ANSWERABILITY_CASES else 0
    if path == "focused_scope_sparse":
        return 1 if case_name in _RETRIEVAL_PROBE_POSITIVE_SCOPED_SPARSE_CASES else 0
    return 0


def _retrieval_probe_max_rows(
    *,
    path: str,
    case: dict[str, Any] | None = None,
) -> int | None:
    case_name = str((case or {}).get("name") or "")
    if path == "focused_scope_sparse" and case_name == "background_noise":
        return 0
    return None


async def _run_retrieval_hot_path_probe(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    max_ms: float,
    model_limit: int,
    require_scope: bool = True,
) -> dict[str, Any]:
    from services.platform.execution.retrieval_actions import (
        focused_answerability_index_scan,
        focused_direct_scope_scan,
        focused_scope_sparse_scan,
    )
    from services.reasoning.sage.reader import _fetch_answerability_index_matches

    raw_seed_entities, seed_pairs = await _retrieval_probe_seed_pairs(
        conn,
        tenant_id=tenant_id,
    )
    cases = await _retrieval_probe_term_cases(conn, tenant_id=tenant_id)
    results: list[dict[str, Any]] = []
    for case in cases:
        terms = list(case["terms"])
        focused_answerability = await _time_retrieval_probe_call(
            label=f"focused_answerability/{case['name']}",
            max_ms=max_ms,
            min_rows=_retrieval_probe_min_rows(
                path="focused_answerability",
                case=case,
            ),
            call=focused_answerability_index_scan(
                conn,
                tenant_id=tenant_id,
                primitives=_RETRIEVAL_PROBE_PRIMITIVES,
                terms=terms,
                seed_pairs=seed_pairs,
                limit=model_limit,
            ),
        )
        focused_answerability["case"] = case
        results.append(focused_answerability)

        sage_answerability = await _time_retrieval_probe_call(
            label=f"sage_answerability/{case['name']}",
            max_ms=max_ms,
            min_rows=_retrieval_probe_min_rows(
                path="sage_answerability",
                case=case,
            ),
            call=_fetch_answerability_index_matches(
                conn,
                tenant_id=tenant_id,
                primitive_values=list(_RETRIEVAL_PROBE_PRIMITIVES),
                terms=terms,
                limit=model_limit,
            ),
        )
        sage_answerability["case"] = case
        results.append(sage_answerability)

        if seed_pairs:
            scoped_sparse = await _time_retrieval_probe_call(
                label=f"focused_scope_sparse/{case['name']}",
                max_ms=max_ms,
                min_rows=_retrieval_probe_min_rows(
                    path="focused_scope_sparse",
                    case=case,
                    seed_pairs=seed_pairs,
                ),
                max_rows=_retrieval_probe_max_rows(
                    path="focused_scope_sparse",
                    case=case,
                ),
                call=focused_scope_sparse_scan(
                    conn,
                    tenant_id=tenant_id,
                    terms=terms,
                    seed_pairs=seed_pairs,
                    limit=model_limit,
                ),
            )
            scoped_sparse["case"] = case
            results.append(scoped_sparse)

    if seed_pairs:
        results.append(
            await _time_retrieval_probe_call(
                label="focused_direct_scope",
                max_ms=max_ms,
                min_rows=_retrieval_probe_min_rows(
                    path="focused_direct_scope",
                    seed_pairs=seed_pairs,
                ),
                call=focused_direct_scope_scan(
                    conn,
                    tenant_id=tenant_id,
                    seed_pairs=seed_pairs,
                    limit=model_limit,
                ),
            )
        )
        action_case = next(
            (case for case in cases if case.get("name") == "common_generic"),
            cases[0] if cases else None,
        )
        if action_case is not None:
            results.append(
                await _time_focused_action_probe_call(
                    label="focused_action/common_generic",
                    max_ms=max_ms,
                    min_rows=1,
                    min_sources=2,
                    call=_rollback_focused_action_probe(
                        conn,
                        tenant_id=tenant_id,
                        raw_seed_entities=raw_seed_entities,
                        terms=list(action_case["terms"]),
                        model_limit=model_limit,
                    ),
                )
            )

    failed = [result for result in results if not result["passed"]]
    timeout_paths = [
        str(result.get("label"))
        for result in results
        if bool(result.get("timed_out"))
    ]
    source_family_counts: dict[str, int] = {}
    for result in results:
        raw_sources = result.get("source_set")
        if not isinstance(raw_sources, list):
            raw_sources = (result.get("notes") or {}).get("source_set")
        if not isinstance(raw_sources, list):
            continue
        for source in raw_sources:
            key = str(source)
            source_family_counts[key] = source_family_counts.get(key, 0) + 1
    coverage_failures: list[str] = []
    if require_scope and not seed_pairs:
        coverage_failures.append(
            "no model_scope_entities seed pairs found; scoped retrieval paths "
            "were not exercised"
        )
    return {
        "status": "passed" if not failed and not coverage_failures else "failed",
        "tenant_id": str(tenant_id),
        "max_ms": max_ms,
        "model_limit": model_limit,
        "require_scope": require_scope,
        "seed_entities": raw_seed_entities,
        "seed_pair_count": len(seed_pairs),
        "case_count": len(cases),
        "results": results,
        "failures": failed,
        "coverage_failures": coverage_failures,
        "bounded_lookup_timeout_count": len(timeout_paths),
        "bounded_lookup_timeout_paths": timeout_paths,
        "source_family_counts": source_family_counts,
    }


def _render_retrieval_probe_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Retrieval Hot Path Probe",
        "",
        f"- Tenant: `{summary['tenant_id']}`",
        f"- Status: `{summary['status']}`",
        f"- Max allowed latency: {summary['max_ms']}ms",
        f"- Seed scope pairs: {summary['seed_pair_count']}",
        "",
        "| Path | ms | Rows | Sources | Status |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for result in summary.get("results", []):
        status = (
            "timeout"
            if result.get("timed_out")
            else ("pass" if result.get("passed") else "fail")
        )
        row_text = str(result["row_count"])
        min_rows = int(result.get("min_rows") or 0)
        if min_rows:
            row_text = f"{row_text} / min {min_rows}"
        max_rows = result.get("max_rows")
        if max_rows is not None:
            row_text = f"{row_text} / max {int(max_rows)}"
        source_text = "-"
        min_sources = int(result.get("min_sources") or 0)
        if "source_count" in result:
            source_text = str(result.get("source_count", 0))
            if min_sources:
                source_text = f"{source_text} / min {min_sources}"
            raw_source_set = result.get("source_set") or (
                result.get("notes") or {}
            ).get("source_set")
            if isinstance(raw_source_set, list) and raw_source_set:
                source_text = f"{source_text} ({', '.join(map(str, raw_source_set))})"
        lines.append(
            "| "
            f"{result['label']} | "
            f"{result['elapsed_ms']:.3f} | "
            f"{row_text} | "
            f"{source_text} | "
            f"{status} |"
        )
    timeout_paths = [
        str(item) for item in summary.get("bounded_lookup_timeout_paths") or []
    ]
    if timeout_paths:
        lines.extend(
            [
                "",
                "## Bounded Lookup Timeouts",
                *[f"- {item}" for item in timeout_paths],
            ]
        )
    readiness = summary.get("sidecar_readiness") or {}
    if isinstance(readiness, dict) and readiness:
        lines.extend(
            [
                "",
                "## Sidecar Readiness",
                "",
                f"- Status: `{readiness.get('status', 'unknown')}`",
                f"- Active Models: {readiness.get('active_model_count', 0)}",
                "",
                "| Surface | Active Rows | Active Models | Required | Status |",
                "| --- | ---: | ---: | --- | --- |",
            ]
        )
        readiness_failures = [str(item) for item in readiness.get("failures") or []]
        for table, item in sorted((readiness.get("tables") or {}).items()):
            if not isinstance(item, dict):
                continue
            table_status = "fail" if any(table in failure for failure in readiness_failures) else "pass"
            active_models = item.get("active_model_hit_count")
            ratio = item.get("active_model_ratio")
            active_model_text = "-"
            if active_models is not None:
                active_model_text = str(active_models)
                if ratio is not None:
                    active_model_text = f"{active_model_text} ({float(ratio):.1%})"
            active_rows = item.get("active_row_count", "-")
            lines.append(
                "| "
                f"{table} | "
                f"{active_rows} | "
                f"{active_model_text} | "
                f"{'yes' if item.get('required') else 'no'} | "
                f"{table_status} |"
            )
        if readiness_failures:
            lines.extend(["", "### Readiness Failures"])
            lines.extend(f"- {item}" for item in readiness_failures)
        readiness_warnings = [str(item) for item in readiness.get("warnings") or []]
        if readiness_warnings:
            lines.extend(["", "### Readiness Warnings"])
            lines.extend(f"- {item}" for item in readiness_warnings)
    if summary.get("failures"):
        lines.extend(
            [
                "",
                "## Failures",
                *[
                    (
                        f"- {item['label']} took {item['elapsed_ms']:.3f}ms "
                        f"and returned {item.get('row_count', 0)} rows"
                        + (
                            f" below min {item['min_rows']}"
                            if int(item.get("min_rows") or 0)
                            and not bool(item.get("coverage_passed", True))
                            else ""
                        )
                        + (
                            f"; rows {item.get('row_count', 0)} above max "
                            f"{item['max_rows']}"
                            if item.get("max_rows") is not None
                            and not bool(item.get("excess_passed", True))
                            else ""
                        )
                        + (
                            "; bounded lookup timed out"
                            if bool(item.get("timed_out"))
                            else ""
                        )
                        + (
                            f"; sources {item.get('source_count', 0)} below min "
                            f"{item['min_sources']}"
                            if int(item.get("min_sources") or 0)
                            and not bool(item.get("source_passed", True))
                            else ""
                        )
                    )
                    for item in summary["failures"]
                ],
            ]
        )
    if summary.get("coverage_failures"):
        lines.extend(
            [
                "",
                "## Coverage Failures",
                *[f"- {item}" for item in summary["coverage_failures"]],
            ]
        )
    return "\n".join(lines) + "\n"


async def run_retrieval_probe(args: argparse.Namespace) -> dict[str, Any]:
    run_id = _resolve_storyline_run_id(args)
    report_dir = args.report_root / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    tenant_id = _retrieval_probe_tenant_id(args)
    runtime = await _open_storyline_benchmark_runtime(args)
    try:
        if not args.skip_migrations:
            async with runtime.pool.acquire() as conn:
                await apply_migrations_dir(
                    conn,
                    REPO_ROOT / "db" / "migrations",
                    on_error="warn",
                )
        async with runtime.pool.acquire() as conn:
            model_count = int(
                await conn.fetchval(
                    """
                    SELECT count(*)::bigint
                    FROM models
                    WHERE tenant_id = $1
                    """,
                    tenant_id,
                )
                or 0
            )
            observation_count = int(
                await conn.fetchval(
                    """
                    SELECT count(*)::bigint
                    FROM observations
                    WHERE tenant_id = $1
                    """,
                    tenant_id,
                )
                or 0
            )
            if model_count <= 0 and observation_count <= 0:
                raise SystemExit(
                    "retrieval probe tenant has no Models or observations in the "
                    f"current database: {tenant_id}"
                )
            sidecar_readiness = await _retrieval_probe_sidecar_preflight(
                conn,
                tenant_id=tenant_id,
            )
            if sidecar_readiness["status"] == "passed":
                summary = await _run_retrieval_hot_path_probe(
                    conn,
                    tenant_id=tenant_id,
                    max_ms=float(args.retrieval_probe_max_ms),
                    model_limit=int(args.retrieval_probe_model_limit),
                    require_scope=not bool(args.retrieval_probe_allow_missing_scope),
                )
            else:
                summary = {
                    "status": "failed",
                    "tenant_id": str(tenant_id),
                    "max_ms": float(args.retrieval_probe_max_ms),
                    "model_limit": int(args.retrieval_probe_model_limit),
                    "require_scope": not bool(
                        args.retrieval_probe_allow_missing_scope
                    ),
                    "seed_entities": [],
                    "seed_pair_count": 0,
                    "case_count": 0,
                    "results": [],
                    "failures": [],
                    "coverage_failures": list(sidecar_readiness["failures"]),
                }
            summary["sidecar_readiness"] = sidecar_readiness
            summary["model_count"] = model_count
            summary["active_model_count"] = sidecar_readiness["active_model_count"]
            summary["observation_count"] = observation_count
    finally:
        await runtime.pool.close()
    summary["run_id"] = run_id
    summary["report_dir"] = str(report_dir)
    _write_json(report_dir / "retrieval_probe_summary.json", summary)
    _write_json(report_dir / "run_summary.json", summary)
    (report_dir / "retrieval_probe_summary.md").write_text(
        _render_retrieval_probe_markdown(summary)
    )
    if summary["status"] != "passed":
        summary["exit_code"] = 1
    return summary


async def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    run_id = _resolve_storyline_run_id(args)
    benchmark_inputs = _build_storyline_benchmark_inputs(args, run_id=run_id)
    if args.mode == "build-only":
        return _build_only_summary(args, run_id, benchmark_inputs)

    runtime = await _open_storyline_benchmark_runtime(args)
    started = time.monotonic()
    tenant_id: UUID | None = None
    try:
        prepared = await _prepare_storyline_benchmark_tenant(
            args,
            pool=runtime.pool,
            scenario=benchmark_inputs.scenario,
            append_context=benchmark_inputs.append_context,
            run_id=run_id,
            horizon_start_batch=benchmark_inputs.horizon_start_batch,
        )
        tenant_id = prepared.tenant_id
        seed_status = await _maybe_seed_storyline_models(
            args,
            pool=runtime.pool,
            tenant_id=tenant_id,
            append_context=benchmark_inputs.append_context,
            seed_status=prepared.seed_status,
        )
        (
            semantic_memory_before_first_wave,
            pre_first_wave_scaffolding,
        ) = await _pre_first_wave_memory_snapshot(
            runtime.pool,
            tenant_id=tenant_id,
        )
        print(
            "semantic_memory_before_first_wave="
            + json.dumps(semantic_memory_before_first_wave, sort_keys=True),
            flush=True,
        )
        print(
            "pre_first_wave_scaffolding="
            + json.dumps(pre_first_wave_scaffolding, sort_keys=True),
            flush=True,
        )
        if args.mode == "seed-only":
            return await _write_seed_only_outputs(
                args,
                pool=runtime.pool,
                tenant_id=tenant_id,
                scenario=benchmark_inputs.scenario,
                run_id=run_id,
                report_dir=benchmark_inputs.report_dir,
                run_config=benchmark_inputs.run_config,
                seed_status=seed_status,
                started=started,
            )
        worker = _build_storyline_worker(
            args,
            pool=runtime.pool,
            tenant_id=tenant_id,
            run_id=run_id,
            embedder=runtime.embedder,
        )
        observation_ids, waves = await _process_storyline_benchmark_waves(
            args,
            pool=runtime.pool,
            scenario=benchmark_inputs.scenario,
            actor_repo=prepared.actor_repo,
            alias_repo=prepared.alias_repo,
            embedder=runtime.embedder,
            worker=worker,
            tenant_id=tenant_id,
            run_id=run_id,
            report_dir=benchmark_inputs.report_dir,
        )
        adaptive_drain = await _drain_adaptive_work_to_quiescence(
            args,
            pool=runtime.pool,
            worker=worker,
            tenant_id=tenant_id,
        )
        post_commit_status = adaptive_drain["post_commit_status"]
        topology_status = adaptive_drain["topology_status"]
        model_summary = await _collect_storyline_benchmark_summary(
            args,
            pool=runtime.pool,
            tenant_id=tenant_id,
            scenario=benchmark_inputs.scenario,
            run_id=run_id,
            report_dir=benchmark_inputs.report_dir,
            run_config=benchmark_inputs.run_config,
            seed_status=seed_status,
            observation_ids=observation_ids,
            waves=waves,
            post_commit_status=post_commit_status,
            topology_status=topology_status,
            adaptive_drain_status=adaptive_drain,
            append_context=benchmark_inputs.append_context,
            horizon_start_batch=benchmark_inputs.horizon_start_batch,
            semantic_memory_before_first_wave=semantic_memory_before_first_wave,
            pre_first_wave_scaffolding=pre_first_wave_scaffolding,
            started=started,
        )
        return await _write_storyline_benchmark_outputs(
            args,
            pool=runtime.pool,
            tenant_id=tenant_id,
            scenario=benchmark_inputs.scenario,
            model_summary=model_summary,
            waves=waves,
            report_dir=benchmark_inputs.report_dir,
            started=started,
        )
    finally:
        await _cleanup_benchmark_tenant(args, runtime.pool, tenant_id)
        await runtime.pool.close()


def _build_only_summary(
    args: argparse.Namespace,
    run_id: str,
    benchmark_inputs: _BenchmarkInputs,
) -> dict[str, Any]:
    return {
        "mode": args.mode,
        "run_id": run_id,
        "report_dir": str(benchmark_inputs.report_dir),
        "signals": _signal_count(benchmark_inputs.scenario),
        "storylines": len(STORYLINES),
    }


async def _cleanup_benchmark_tenant(
    args: argparse.Namespace,
    pool: asyncpg.Pool,
    tenant_id: UUID | None,
) -> None:
    if not args.cleanup or tenant_id is None:
        return
    from scripts.run_20000_model_4000_signal_company_probe import (
        _cleanup_probe_tenant,
    )

    cleanup = await _cleanup_probe_tenant(pool, tenant_id)
    print(f"cleanup={json.dumps(cleanup, sort_keys=True)}", flush=True)


async def _process_one_t1_batch(
    pool: asyncpg.Pool,
    worker: ThinkWorker,
    *,
    tenant_id: UUID,
    force_window_elapsed_s: float,
    retry_attempts: int = 0,
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
    run, attempt_history = await _dispatch_t1_batch_with_retries(
        pool,
        worker,
        rows[0],
        retry_attempts=retry_attempts,
    )
    payload = rows[0]["payload"] or {}
    return {
        "trigger_id": str(rows[0]["id"]),
        "member_count": len(payload.get("batch_member_trigger_ids") or []),
        "observation_count": len(payload.get("batch_observation_ids") or []),
        "elapsed_s": round(time.monotonic() - started, 3),
        "retry_count": max(0, len(attempt_history) - 1),
        "attempt_history": attempt_history,
        "run": run,
    }


async def _dispatch_t1_batch_with_retries(
    pool: asyncpg.Pool,
    worker: ThinkWorker,
    row: asyncpg.Record | dict[str, Any],
    *,
    retry_attempts: int,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    trigger_id = row["id"]
    attempt_history: list[dict[str, Any]] = []
    current_row: asyncpg.Record | dict[str, Any] | None = row
    max_attempts = 1 + max(0, int(retry_attempts))

    for attempt_index in range(1, max_attempts + 1):
        if current_row is None:
            break
        attempt_started = time.monotonic()
        await worker._dispatch_trigger(current_row)
        run = await _run_for_trigger(pool, trigger_id)
        queue_state = await _t1_batch_queue_state(pool, trigger_id=trigger_id)
        retryable = _is_retryable_t1_batch_run(run)
        attempt_history.append(
            {
                "attempt": attempt_index,
                "elapsed_s": round(time.monotonic() - attempt_started, 3),
                "status": (run or {}).get("status"),
                "error": (run or {}).get("error"),
                "retryable": retryable,
                "queue_attempts": queue_state.get("attempts"),
                "queue_completed": queue_state.get("completed"),
            }
        )
        if run and run.get("status") == "success":
            final_run = dict(run)
            final_run["attempt_count"] = len(attempt_history)
            final_run["retry_count"] = max(0, len(attempt_history) - 1)
            final_run["recovered_after_retry"] = len(attempt_history) > 1
            final_run["attempt_history"] = attempt_history
            return final_run, attempt_history
        if not retryable or attempt_index >= max_attempts:
            final_run = dict(run) if run else None
            if final_run is not None:
                final_run["attempt_count"] = len(attempt_history)
                final_run["retry_count"] = max(0, len(attempt_history) - 1)
                final_run["recovered_after_retry"] = False
                final_run["attempt_history"] = attempt_history
            return final_run, attempt_history
        if queue_state.get("completed"):
            break
        current_row = await _lock_t1_batch_for_retry(
            pool,
            worker=worker,
            trigger_id=trigger_id,
        )

    run = await _run_for_trigger(pool, trigger_id)
    final_run = dict(run) if run else None
    if final_run is not None:
        final_run["attempt_count"] = len(attempt_history)
        final_run["retry_count"] = max(0, len(attempt_history) - 1)
        final_run["recovered_after_retry"] = False
        final_run["attempt_history"] = attempt_history
    return final_run, attempt_history


async def _lock_t1_batch_for_retry(
    pool: asyncpg.Pool,
    *,
    worker: ThinkWorker,
    trigger_id: UUID,
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await conn.fetchrow(
                """
                UPDATE think_trigger_queue
                SET locked_by = $2,
                    locked_at = now(),
                    scheduled_for = now()
                WHERE id = $1
                  AND completed_at IS NULL
                  AND attempts < $3
                  AND (
                    locked_by IS NULL
                    OR locked_by = $2
                    OR locked_at < now() - ($4 || ' seconds')::interval
                  )
                RETURNING id, tenant_id, trigger_kind, trigger_subkind,
                          observation_id, model_id, payload, attempts,
                          enqueued_at
                """,
                trigger_id,
                worker.config.worker_id,
                worker.config.trigger_max_attempts,
                str(worker.config.trigger_lock_timeout_s),
            )


async def _t1_batch_queue_state(
    pool: asyncpg.Pool,
    *,
    trigger_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT attempts, completed_at
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trigger_id,
        )
    if row is None:
        return {"attempts": None, "completed": True}
    return {
        "attempts": int(row["attempts"] or 0),
        "completed": row["completed_at"] is not None,
    }


def _is_retryable_t1_batch_run(run: dict[str, Any] | None) -> bool:
    if not run or run.get("status") == "success":
        return False
    error = str(run.get("error") or "")
    if not error:
        return False
    lowered = error.lower()
    return any(marker in lowered for marker in _T1_BATCH_TRANSIENT_FAILURE_MARKERS)


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
            trigger_rows = await conn.fetch(
                f"""
                SELECT id, trigger_kind, trigger_subkind, payload, attempts
                FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND {_DOWNSTREAM_TRIGGER_DRAIN_PREDICATE}
                ORDER BY enqueued_at ASC, id ASC
                """,
                tenant_id,
            )
            trigger_summaries = [
                _compact_downstream_trigger_summary(row) for row in trigger_rows
            ]
            if not trigger_summaries:
                break
            await conn.execute(
                f"""
                UPDATE think_trigger_queue
                SET enqueued_at = now() - ($2 || ' seconds')::interval
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND {_DOWNSTREAM_TRIGGER_DRAIN_PREDICATE}
                """,
                tenant_id,
                str(max(0.0, force_window_elapsed_s)),
            )
        before = time.monotonic()
        await worker._poll_and_dispatch()
        if worker._in_flight:
            await asyncio.gather(*list(worker._in_flight), return_exceptions=False)
        downstream_runs = await _downstream_run_summaries(pool, trigger_summaries)
        out.append(
            {
                "step": step + 1,
                "elapsed_s": round(time.monotonic() - before, 3),
                "pending_trigger_count_before_step": len(trigger_summaries),
                "pending_triggers_before_step": trigger_summaries[:50],
                "downstream_runs": downstream_runs,
                "queue_counts": await _queue_counts(pool, tenant_id),
            }
        )
    return out


def _compact_downstream_trigger_summary(row: Any) -> dict[str, Any]:
    payload = _json_obj(row["payload"])
    trigger_kind = str(row["trigger_kind"] or "")
    trigger_subkind = row["trigger_subkind"]
    return {
        "id": str(row["id"]),
        "trigger_kind": (
            f"{trigger_kind}:{trigger_subkind}" if trigger_subkind else trigger_kind
        ),
        "trigger_kind_family": _trigger_kind_family(trigger_kind),
        "trigger_subkind": trigger_subkind,
        "attempts": int(row["attempts"] or 0),
        "payload": _compact_downstream_trigger_payload(payload),
    }


def _compact_downstream_trigger_payload(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "repair_key",
        "repair_intent",
        "repair_source",
        "repair_residual_id",
        "residual_id",
        "residual_kind",
        "audit_warning_code",
        "open_question_key",
        "question_primitive",
        "cascade_depth",
        "source_trigger_kind",
        "source_trigger_subkind",
        "source_model_id",
        "model_id",
    )
    out = {key: _jsonable(payload[key]) for key in keys if payload.get(key) is not None}
    seed_signature = payload.get("seed_signature")
    if isinstance(seed_signature, dict):
        seed_out = {
            key: _jsonable(seed_signature[key])
            for key in keys
            if seed_signature.get(key) is not None
        }
        if seed_out:
            out["seed_signature"] = seed_out
    return out


async def _downstream_run_summaries(
    pool: asyncpg.Pool,
    trigger_summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for trigger in trigger_summaries:
        trigger_id = UUID(str(trigger["id"]))
        run = await _run_for_trigger(pool, trigger_id)
        if run is None:
            out.append({"trigger": trigger, "run": None, "ops_summary": {}})
            continue
        out.append(
            {
                "trigger": trigger,
                "run": run,
                "ops_summary": _compact_ops_applied(run.get("ops_applied")),
            }
        )
    return out


def _compact_ops_applied(value: Any) -> dict[str, Any]:
    ops = _json_obj(value)
    if not ops:
        return {}
    out: dict[str, Any] = {}
    for key in (
        "state_changes_emitted",
        "apply_dropped_op_count",
        "validation_error_count",
    ):
        if isinstance(ops.get(key), (int, float)) and not isinstance(ops.get(key), bool):
            out[key] = int(ops[key])
    dropped_errors = ops.get("apply_dropped_op_errors")
    if isinstance(dropped_errors, list):
        out["apply_dropped_op_errors"] = [_jsonable(item) for item in dropped_errors[:3]]
    applied_model_ids = ops.get("applied_model_ids")
    if isinstance(applied_model_ids, list):
        out["applied_model_count"] = len(applied_model_ids)
        out["applied_model_ids"] = [str(item) for item in applied_model_ids[:12]]
    for key in (
        "claim_ops",
        "relation_claim_ops",
        "relation_frame_ops",
        "edge_ops",
        "memory_lifecycle_ops",
        "open_question_ops",
        "act_ops",
        "resource_ops",
        "ontology_gap_ops",
        "formation_resolutions",
        "synthesis_decisions",
    ):
        value = ops.get(key)
        if isinstance(value, list):
            out[f"{key}_count"] = len(value)

    memory = _json_obj(ops.get("memory_aggregation"))
    if memory:
        out["memory_aggregation"] = {
            key: memory[key]
            for key in (
                "model_inserts",
                "model_updates",
                "model_archives",
                "evidence_attachments",
                "near_duplicate_absorptions",
                "situation_model_inserts",
                "situation_model_updates",
                "situation_member_additions",
                "new_model_pressure",
                "absorption_ratio",
            )
            if key in memory
        }
    context_use = _json_obj(ops.get("context_use"))
    if context_use:
        out["context_use"] = {
            key: context_use[key]
            for key in (
                "context_use_grade",
                "selected_context_reference_ratio",
                "model_context_used",
                "graph_context_used",
                "observation_context_used",
                "justified_noop_context_used",
                "reasoning_trace_context_used",
                "graph_relation_contract_satisfied",
            )
            if key in context_use
        }
    representation_audit = _json_obj(ops.get("representation_audit"))
    if representation_audit:
        out["representation_audit"] = {
            key: representation_audit[key]
            for key in (
                "budget_status",
                "claim_insert_count",
                "model_update_count",
                "edge_op_count",
                "relation_claim_count",
                "relation_frame_count",
                "evidence_attachment_count",
                "near_duplicate_absorption_count",
            )
            if key in representation_audit
        }
    mutation_summary = _json_obj(
        ops.get("mutation_compile_summary")
        or ops.get("mutation_compiler")
        or ops.get("compile_summary")
    )
    if mutation_summary:
        out["mutation_compile_summary"] = {
            key: mutation_summary[key]
            for key in (
                "accepted",
                "rejected",
                "blocked",
                "dropped",
                "repair_triggers",
                "repair_residuals",
            )
            if key in mutation_summary
        }
    repair_triggers = ops.get("representation_repair_triggers")
    if isinstance(repair_triggers, list):
        out["representation_repair_trigger_count"] = len(repair_triggers)
    return out


def _merge_numeric_status(
    total: dict[str, Any],
    status: dict[str, Any],
    *,
    metric_key: str | None = None,
) -> None:
    for key, value in status.items():
        if key == metric_key or isinstance(value, bool):
            continue
        if isinstance(value, int):
            total[key] = int(total.get(key) or 0) + value
        elif isinstance(value, float):
            total[key] = float(total.get(key) or 0.0) + value
    if metric_key:
        merged_metrics = total.setdefault(metric_key, {})
        metrics = status.get(metric_key) or {}
        if isinstance(metrics, dict):
            for key, value in metrics.items():
                if isinstance(value, bool):
                    continue
                if isinstance(value, int):
                    merged_metrics[key] = int(merged_metrics.get(key) or 0) + value
                elif isinstance(value, float):
                    merged_metrics[key] = float(merged_metrics.get(key) or 0.0) + value


async def _pending_trigger_count(pool: asyncpg.Pool, *, tenant_id: UUID) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
              AND batch_parent_id IS NULL
            """,
            tenant_id,
        )
    return int(value or 0)


async def _pending_post_commit_count(pool: asyncpg.Pool, *, tenant_id: UUID) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND processed_at IS NULL
              AND dead_lettered_at IS NULL
              AND scheduled_at <= now()
            """,
            tenant_id,
        )
    return int(value or 0)


async def _drain_adaptive_work_to_quiescence(
    args: argparse.Namespace,
    *,
    pool: asyncpg.Pool,
    worker: ThinkWorker,
    tenant_id: UUID,
) -> dict[str, Any]:
    post_commit_status: dict[str, Any] = {
        "processed": 0,
        "failed": 0,
        "dead_lettered": 0,
        "iterations": 0,
    }
    topology_status: dict[str, Any] = {
        "status": "skipped" if args.skip_topology_optimizer else "drained",
        "processed": 0,
        "completed": 0,
        "failed": 0,
        "iterations": 0,
        "metrics": {},
    }
    cycles: list[dict[str, Any]] = []
    max_cycles = max(1, int(args.adaptive_drain_cycles))
    for cycle in range(max_cycles):
        before_triggers = await _pending_trigger_count(pool, tenant_id=tenant_id)
        before_post_commit = await _pending_post_commit_count(pool, tenant_id=tenant_id)
        downstream_step_budget = max(0, int(args.adaptive_drain_steps_per_cycle))
        downstream = await _drain_downstream_limited(
            pool,
            worker,
            tenant_id=tenant_id,
            steps=downstream_step_budget,
            force_window_elapsed_s=args.downstream_batch_window_s + 1.0,
        )
        remaining_downstream_steps = max(0, downstream_step_budget - len(downstream))
        post_commit = await drain_post_commit_actions(
            pool,
            tenant_id=tenant_id,
            timeout_seconds=args.post_commit_timeout,
            batch_size=args.post_commit_batch_size,
            batch_timeout_seconds=args.post_commit_batch_timeout,
        )
        _merge_numeric_status(post_commit_status, post_commit)
        topology: dict[str, Any] = {
            "status": "skipped",
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "iterations": 0,
            "metrics": {},
        }
        if not args.skip_topology_optimizer:
            topology = await drain_topology_optimizer(
                pool,
                tenant_id=tenant_id,
                timeout_seconds=args.topology_optimizer_timeout,
                batch_size=args.topology_optimizer_batch_size,
                lookback_hours=args.topology_optimizer_lookback_hours,
            )
            _merge_numeric_status(topology_status, topology, metric_key="metrics")
            topology_status["status"] = topology.get("status", topology_status["status"])
        post_commit_downstream: list[dict[str, Any]] = []
        if remaining_downstream_steps > 0:
            post_commit_downstream = await _drain_downstream_limited(
                pool,
                worker,
                tenant_id=tenant_id,
                steps=remaining_downstream_steps,
                force_window_elapsed_s=args.downstream_batch_window_s + 1.0,
            )
        tail_post_commit: dict[str, Any] = {
            "processed": 0,
            "failed": 0,
            "dead_lettered": 0,
            "iterations": 0,
        }
        if post_commit_downstream and await _pending_post_commit_count(
            pool,
            tenant_id=tenant_id,
        ):
            tail_post_commit = await drain_post_commit_actions(
                pool,
                tenant_id=tenant_id,
                timeout_seconds=args.post_commit_timeout,
                batch_size=args.post_commit_batch_size,
                batch_timeout_seconds=args.post_commit_batch_timeout,
            )
            _merge_numeric_status(post_commit_status, tail_post_commit)
        after_triggers = await _pending_trigger_count(pool, tenant_id=tenant_id)
        after_post_commit = await _pending_post_commit_count(pool, tenant_id=tenant_id)
        cycle_post_commit_processed = int(post_commit.get("processed") or 0) + int(
            tail_post_commit.get("processed") or 0
        )
        cycle_status = {
            "cycle": cycle + 1,
            "before_triggers": before_triggers,
            "after_triggers": after_triggers,
            "before_post_commit": before_post_commit,
            "after_post_commit": after_post_commit,
            "downstream_steps": len(downstream) + len(post_commit_downstream),
            "downstream_steps_before_post_commit": len(downstream),
            "downstream_steps_after_post_commit": len(post_commit_downstream),
            "post_commit_processed": cycle_post_commit_processed,
            "post_commit_processed_before_downstream": int(
                post_commit.get("processed") or 0
            ),
            "post_commit_processed_after_downstream": int(
                tail_post_commit.get("processed") or 0
            ),
            "post_commit_timed_out": bool(
                post_commit.get("timed_out") or tail_post_commit.get("timed_out")
            ),
            "post_commit_pending": int(
                tail_post_commit.get("pending")
                or post_commit.get("pending")
                or after_post_commit
                or 0
            ),
            "topology_processed": int(topology.get("processed") or 0),
        }
        cycles.append(cycle_status)
        print(
            "adaptive_drain_cycle="
            f"{json.dumps(cycle_status, sort_keys=True)}",
            flush=True,
        )
        if cycle_status["post_commit_timed_out"]:
            break
        if after_triggers == 0 and after_post_commit == 0:
            break
        if (
            cycle > 0
            and before_triggers == after_triggers
            and before_post_commit == after_post_commit
            and cycle_post_commit_processed == 0
            and int(topology.get("processed") or 0) == 0
            and not downstream
            and not post_commit_downstream
        ):
            break
    return {
        "cycles": cycles,
        "post_commit_status": post_commit_status,
        "topology_status": topology_status,
    }


async def _run_for_trigger(
    pool: asyncpg.Pool, trigger_id: UUID
) -> dict[str, Any] | None:
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


async def _collect_capability_probe_counts(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT content
            FROM observations
            WHERE tenant_id = $1
              AND content->>'benchmark' = 'storyline_batch'
              AND content->>'phase' = 'capability_probe'
            """,
            tenant_id,
        )
    for row in rows:
        content = row["content"]
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                content = {}
        if not isinstance(content, dict):
            continue
        kinds = content.get("capability_probe_kinds")
        if isinstance(kinds, list):
            for kind in kinds:
                if isinstance(kind, str) and kind:
                    counts[kind] += 1
            continue
        kind = content.get("capability_probe_kind")
        if isinstance(kind, str) and kind:
            counts[kind] += 1
    return dict(counts)


_LIFECYCLE_OBLIGATION_PATTERNS: dict[str, re.Pattern[str]] = {
    "prediction": re.compile(
        r"\b("
        r"forecast|predict(?:ion|ed)?|expected|likely|eta|deadline|due|"
        r"target(?:ing)?|will\s+(?:ship|slip|delay|miss|deliver|merge|"
        r"deploy|launch|renew|churn|finish|complete|move|close|resolve|happen)|"
        r"by\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
        r"tomorrow|today|tonight|next\s+week|\d{1,2}(?::\d{2})?)"
        r")\b",
        re.IGNORECASE,
    ),
    "resource": re.compile(
        r"\b("
        r"capacity|bandwidth|budget|hours?|quota|limit|resource|availability|"
        r"staff(?:ing)?|headcount|constrained|overloaded|under[-\s]?resourced|"
        r"depleted|exhausted|no\s+room|down\s+to\s+\w+\s+hours?"
        r")\b",
        re.IGNORECASE,
    ),
    "question_policy": re.compile(
        r"\b("
        r"missing\s+context|needs?\s+clarification|unclear|unknown|ambiguous|"
        r"who\s+owns|which\s+owner|owner\s+(?:is\s+)?(?:unclear|unknown|missing)|"
        r"approval\s+owner|approver\s+(?:is\s+)?(?:unclear|unknown|missing)|"
        r"confirm\s+before|ask\s+before|source\s+of\s+truth"
        r")\b",
        re.IGNORECASE,
    ),
    "evidence_attachment": re.compile(
        r"\b("
        r"felt|feels|review|retro|feedback|concern|worried|rough|pushback|"
        r"complaint|friction|again|repeated|yesterday|today|sentiment"
        r")\b",
        re.IGNORECASE,
    ),
    "staleness_review": re.compile(
        r"\b("
        r"stale|outdated|obsolete|no\s+longer\s+true|superseded|replaced|"
        r"retire|archive|changed\s+since"
        r")\b",
        re.IGNORECASE,
    ),
    "ambiguity_review": re.compile(
        r"\b("
        r"alias|same\s+as|different\s+from|not\s+the\s+same|same\s+customer|"
        r"counterfactual|what\s+if|if\s+.+\s+had|ambiguit(?:y|ies)|ambiguous"
        r")\b",
        re.IGNORECASE,
    ),
}
_LIFECYCLE_TRACE_RE = re.compile(
    r"lifecycle_obligations:\s*injected\s+([^\n]+)",
    re.IGNORECASE,
)
_LIFECYCLE_KINDS = tuple(_LIFECYCLE_OBLIGATION_PATTERNS)


def _lifecycle_opportunity_counts_from_texts(texts: list[str]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for text in texts:
        if "capability_probe" in text.lower():
            continue
        for kind, pattern in _LIFECYCLE_OBLIGATION_PATTERNS.items():
            if pattern.search(text):
                counts[kind] += 1
    return {kind: int(counts.get(kind) or 0) for kind in _LIFECYCLE_KINDS}


def _lifecycle_injected_kinds_from_trace(trace: str) -> list[str]:
    kinds: list[str] = []
    for match in _LIFECYCLE_TRACE_RE.finditer(trace or ""):
        for raw_kind in match.group(1).split(","):
            kind = raw_kind.strip().lower()
            if kind in _LIFECYCLE_OBLIGATION_PATTERNS:
                kinds.append(kind)
    return kinds


def _lifecycle_conversion_rates(
    *,
    numerator: dict[str, int],
    denominator: dict[str, int],
) -> dict[str, float]:
    return {
        kind: _ratio(int(numerator.get(kind) or 0), int(denominator.get(kind) or 0))
        for kind in _LIFECYCLE_KINDS
    }


def _lifecycle_bottleneck_notes(
    *,
    opportunities: dict[str, int],
    injected: dict[str, int],
    persisted: dict[str, int],
) -> list[str]:
    notes: list[str] = []
    labels = {
        "prediction": "prediction lifecycle",
        "resource": "resource operations",
        "question_policy": "question-policy learning",
        "evidence_attachment": "evidence attachment",
        "staleness_review": "staleness review",
        "ambiguity_review": "alias/counterfactual ambiguity review",
    }
    for kind in _LIFECYCLE_KINDS:
        opportunity_count = int(opportunities.get(kind) or 0)
        injected_count = int(injected.get(kind) or 0)
        persisted_count = int(persisted.get(kind) or 0)
        label = labels[kind]
        if opportunity_count and not injected_count:
            notes.append(
                f"{label}: {opportunity_count} explicit opportunity signal(s), "
                "but Think injected no lifecycle obligation."
            )
        elif injected_count and not persisted_count:
            notes.append(
                f"{label}: {injected_count} obligation injection(s), but none "
                "persisted through validate/apply."
            )
        elif opportunity_count and persisted_count < injected_count:
            notes.append(
                f"{label}: {persisted_count}/{injected_count} injected "
                "obligations persisted; inspect validator/apply drops."
            )
    return notes


async def _collect_lifecycle_obligation_report(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    opportunity_counts: dict[str, int] = {kind: 0 for kind in _LIFECYCLE_KINDS}
    injected_counts: Counter[str] = Counter()
    persisted_counts: Counter[str] = Counter()
    trace_run_count = 0
    successful_think_runs = 0

    async with pool.acquire() as conn:
        observation_rows = await conn.fetch(
            """
            SELECT content
            FROM observations
            WHERE tenant_id = $1
              AND content->>'benchmark' = 'storyline_batch'
            """,
            tenant_id,
        )
        texts: list[str] = []
        for row in observation_rows:
            content = _json_obj(row["content"])
            texts.append(str(content.get("text") or ""))
        opportunity_counts = _lifecycle_opportunity_counts_from_texts(texts)

        think_rows = await conn.fetch(
            """
            SELECT ops_applied
            FROM think_runs
            WHERE tenant_id = $1
              AND status = 'success'
            """,
            tenant_id,
        )
        successful_think_runs = len(think_rows)
        for row in think_rows:
            ops = _json_obj(row["ops_applied"])
            trace = str(ops.get("reasoning_trace") or "")
            kinds = _lifecycle_injected_kinds_from_trace(trace)
            if not kinds:
                continue
            trace_run_count += 1
            injected_counts.update(kinds)

        prediction_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM models
            WHERE tenant_id = $1
              AND proposition->>'kind' = 'prediction'
              AND domain_tags @> ARRAY['lifecycle_obligation']::text[]
            """,
            tenant_id,
        )
        persisted_counts["prediction"] = int(prediction_count or 0)

        question_policy_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM models
            WHERE tenant_id = $1
              AND domain_tags @> ARRAY[
                    'question_policy',
                    'lifecycle_obligation'
                  ]::text[]
            """,
            tenant_id,
        )
        persisted_counts["question_policy"] = int(question_policy_count or 0)

        resource_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM resources
            WHERE tenant_id = $1
              AND metadata->>'source' = 'lifecycle_obligation'
            """,
            tenant_id,
        )
        persisted_counts["resource"] = int(resource_count or 0)

        if await _table_exists(conn, "model_signal_readings"):
            evidence_count = await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM model_signal_readings
                WHERE tenant_id = $1
                  AND detail->'proposition'->>'subject'
                    = 'lifecycle review evidence'
                """,
                tenant_id,
            )
            persisted_counts["evidence_attachment"] = int(evidence_count or 0)

        if await _table_exists(conn, "model_open_questions"):
            rows = await conn.fetch(
                """
                SELECT question_type, COUNT(*)::bigint AS count
                FROM model_open_questions
                WHERE tenant_id = $1
                  AND expected_resolution_signal->>'source'
                    = 'lifecycle_obligation'
                GROUP BY question_type
                """,
                tenant_id,
            )
            for row in rows:
                question_type = str(row["question_type"] or "")
                count = int(row["count"] or 0)
                if question_type == "temporal_status":
                    persisted_counts["staleness_review"] += count
                elif question_type == "contradiction_check":
                    persisted_counts["ambiguity_review"] += count

    persisted = {
        kind: int(persisted_counts.get(kind) or 0) for kind in _LIFECYCLE_KINDS
    }
    injected = {
        kind: int(injected_counts.get(kind) or 0) for kind in _LIFECYCLE_KINDS
    }
    return {
        "source": "storyline observations + successful think_runs + persisted lifecycle surfaces",
        "opportunities": {
            "total_signals": int(len(observation_rows)),
            "by_kind": opportunity_counts,
        },
        "injections": {
            "successful_think_runs": successful_think_runs,
            "runs_with_lifecycle_trace": trace_run_count,
            "by_kind": injected,
        },
        "persisted": {"by_kind": persisted},
        "conversion": {
            "opportunity_to_injection_by_kind": _lifecycle_conversion_rates(
                numerator=injected,
                denominator=opportunity_counts,
            ),
            "injection_to_persisted_by_kind": _lifecycle_conversion_rates(
                numerator=persisted,
                denominator=injected,
            ),
        },
        "bottlenecks": _lifecycle_bottleneck_notes(
            opportunities=opportunity_counts,
            injected=injected,
            persisted=persisted,
        ),
    }


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


async def _collect_relation_frame_lifecycle_report(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        if not await _table_exists(conn, "relation_instances"):
            return {
                "available": False,
                "total_relation_frames": 0,
                "accepted_relation_frames": 0,
                "projectable_relation_frames": 0,
                "relation_participants": 0,
                "relation_edge_projections": 0,
                "relation_frame_kind_distribution": {},
                "relation_frame_status_distribution": {},
                "relation_frame_write_policy_distribution": {},
                "relation_projection_kind_distribution": {},
            }
        frame_row = await conn.fetchrow(
            """
            SELECT
              COUNT(*)::bigint AS total_relation_frames,
              COUNT(*) FILTER (WHERE status = 'accepted')::bigint
                AS accepted_relation_frames,
              COUNT(*) FILTER (WHERE write_policy = 'project_edges')::bigint
                AS projectable_relation_frames,
              COUNT(*) FILTER (WHERE participant_binding_status = 'bound')::bigint
                AS bound_relation_frames,
              COUNT(*) FILTER (WHERE status IN ('candidate', 'needs_review'))::bigint
                AS open_relation_frames,
              COALESCE(AVG(confidence), 0)::float AS avg_relation_frame_confidence
            FROM relation_instances
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        participant_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM relation_participants
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        projection_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM relation_edge_projections
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        kind_distribution = await _fetch_distribution(
            conn,
            """
            SELECT relation_kind AS key, COUNT(*)::bigint AS value
            FROM relation_instances
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        status_distribution = await _fetch_distribution(
            conn,
            """
            SELECT status AS key, COUNT(*)::bigint AS value
            FROM relation_instances
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        write_policy_distribution = await _fetch_distribution(
            conn,
            """
            SELECT write_policy AS key, COUNT(*)::bigint AS value
            FROM relation_instances
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        projection_kind_distribution = await _fetch_distribution(
            conn,
            """
            SELECT edge_kind AS key, COUNT(*)::bigint AS value
            FROM relation_edge_projections
            WHERE tenant_id = $1
              AND status = 'active'
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
    report = _record_to_dict(frame_row)
    report["available"] = True
    report["relation_participants"] = int(participant_count or 0)
    report["relation_edge_projections"] = int(projection_count or 0)
    report["relation_frame_kind_distribution"] = kind_distribution
    report["relation_frame_status_distribution"] = status_distribution
    report["relation_frame_write_policy_distribution"] = write_policy_distribution
    report["relation_projection_kind_distribution"] = projection_kind_distribution
    return report


def _future_wave_trigger_ids(waves: list[dict[str, Any]]) -> set[str]:
    trigger_ids: set[str] = set()
    for wave in waves:
        if not str(wave.get("sequence") or "").startswith("future_validation"):
            continue
        t1_batch = wave.get("t1_batch") or {}
        if t1_batch.get("trigger_id"):
            trigger_ids.add(str(t1_batch["trigger_id"]))
        for downstream in wave.get("downstream") or []:
            if downstream.get("trigger_id"):
                trigger_ids.add(str(downstream["trigger_id"]))
    return trigger_ids


async def _collect_think_edge_ops_report(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    future_trigger_ids: set[str] | None = None,
) -> dict[str, Any]:
    stats = _empty_edge_ops_stats()
    trigger_kind_counts: Counter[str] = Counter()
    future_trigger_ids = future_trigger_ids or set()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT trigger_id, trigger_kind, ops_applied
            FROM think_runs
            WHERE tenant_id = $1
              AND status = 'success'
            ORDER BY started_at
            """,
            tenant_id,
        )
    for row in rows:
        trigger_kind = str(row["trigger_kind"] or "")
        trigger_kind_counts[trigger_kind] += 1
        _accumulate_edge_ops_stats(
            stats,
            _json_obj(row["ops_applied"]),
            is_future=str(row["trigger_id"]) in future_trigger_ids,
        )
    stats["think_run_count"] = int(sum(trigger_kind_counts.values()))
    stats["trigger_kind_counts"] = dict(trigger_kind_counts)
    stats["source"] = "all_successful_think_runs"
    return stats


async def _collect_question_planner_reflective_report(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        inquiry_table = await _table_exists(conn, "inquiry_sessions")
        if not inquiry_table:
            return {"available": False, "reason": "missing_inquiry_sessions"}

        planner_row = await conn.fetchrow(
            """
            WITH planning AS (
              SELECT s.id AS session_id, item AS note
              FROM inquiry_sessions s
              CROSS JOIN LATERAL jsonb_array_elements(
                CASE
                  WHEN jsonb_typeof(s.notes->'question_planning') = 'array'
                  THEN s.notes->'question_planning'
                  ELSE '[]'::jsonb
                END
              ) AS item
              WHERE s.tenant_id = $1
            )
            SELECT
              COUNT(DISTINCT session_id)::bigint AS inquiry_sessions,
              COUNT(*)::bigint AS planning_events,
              COALESCE(SUM(
                CASE WHEN note->>'mode' IN ('llm', 'llm_delta') THEN 1 ELSE 0 END
              ), 0)::bigint AS llm_planning_events,
              COALESCE(SUM(
                CASE WHEN note->>'mode' = 'deterministic_fallback' THEN 1 ELSE 0 END
              ), 0)::bigint AS deterministic_fallback_events,
              COALESCE(SUM(
                CASE WHEN note ? 'reflective_rules' THEN 1 ELSE 0 END
              ), 0)::bigint AS reflective_noted_events,
              COALESCE(SUM(
                CASE
                  WHEN (note->'reflective_rules'->>'loaded') ~ '^[0-9]+$'
                   AND (note->'reflective_rules'->>'loaded')::int > 0
                  THEN 1 ELSE 0
                END
              ), 0)::bigint AS reflective_loaded_events,
              COALESCE(SUM(
                CASE
                  WHEN (note->'reflective_rules'->>'applied')::boolean
                  THEN 1 ELSE 0
                END
              ), 0)::bigint AS reflective_applied_events,
              AVG(
                CASE
                  WHEN (note->>'candidate_count') ~ '^[0-9]+$'
                  THEN (note->>'candidate_count')::float
                  ELSE NULL
                END
              ) AS avg_candidate_count
            FROM planning
            """,
            tenant_id,
        )
        planner_modes = await _fetch_planner_mode_distribution(conn, tenant_id)
        action_rule_row = await _fetch_reflective_action_timing_summary(
            conn,
            tenant_id,
        )
        rule_lifecycle = await _fetch_reflective_rule_lifecycle(conn, tenant_id)
        replay_report = await _fetch_reflective_replay_report(conn, tenant_id)
        attribution_report = await _fetch_reflective_attribution_report(
            conn,
            tenant_id,
        )

    report = _record_to_dict(planner_row)
    report["available"] = True
    report["planner_mode_distribution"] = planner_modes
    report["reflective_action_timings"] = action_rule_row
    report["reflective_rule_lifecycle"] = rule_lifecycle
    report["reflective_replay"] = replay_report
    report["reflective_attribution"] = attribution_report
    report["runtime_config"] = _reflective_question_planner_env_config()
    return report


async def _fetch_planner_mode_distribution(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, int]:
    rows = await conn.fetch(
        """
        SELECT COALESCE(item->>'mode', '<missing>') AS key,
               COUNT(*)::bigint AS value
        FROM inquiry_sessions s
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(s.notes->'question_planning') = 'array'
            THEN s.notes->'question_planning'
            ELSE '[]'::jsonb
          END
        ) AS item
        WHERE s.tenant_id = $1
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        """,
        tenant_id,
    )
    return {str(row["key"]): int(row["value"] or 0) for row in rows}


async def _fetch_reflective_action_timing_summary(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        WITH actions AS (
          SELECT item AS note
          FROM inquiry_sessions s
          CROSS JOIN LATERAL jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(s.notes->'retrieval_action_timings') = 'array'
              THEN s.notes->'retrieval_action_timings'
              ELSE '[]'::jsonb
            END
          ) AS item
          WHERE s.tenant_id = $1
        )
        SELECT
          COUNT(*) FILTER (WHERE note ? 'reflective_rule_ids')::bigint
            AS rule_tagged_action_timings,
          COUNT(DISTINCT note->>'question_id')
            FILTER (WHERE note ? 'reflective_rule_ids')::bigint
            AS rule_tagged_questions,
          COUNT(DISTINCT note->>'path')
            FILTER (WHERE note ? 'reflective_rule_ids')::bigint
            AS rule_tagged_paths
        FROM actions
        """,
        tenant_id,
    )
    return _record_to_dict(row)


def _latency_ms_stats(values: list[Any]) -> dict[str, Any]:
    numeric = sorted(
        float(value)
        for value in values
        if value is not None and isinstance(value, (int, float)) and float(value) >= 0
    )
    if not numeric:
        return {
            "count": 0,
            "total_ms": 0.0,
            "min_ms": 0.0,
            "avg_ms": 0.0,
            "p50_ms": 0.0,
            "p90_ms": 0.0,
            "p95_ms": 0.0,
            "max_ms": 0.0,
        }

    def percentile(fraction: float) -> float:
        index = min(len(numeric) - 1, max(0, math.ceil(len(numeric) * fraction) - 1))
        return numeric[index]

    return {
        "count": len(numeric),
        "total_ms": round(sum(numeric), 3),
        "min_ms": round(numeric[0], 3),
        "avg_ms": round(sum(numeric) / len(numeric), 3),
        "p50_ms": round(percentile(0.50), 3),
        "p90_ms": round(percentile(0.90), 3),
        "p95_ms": round(percentile(0.95), 3),
        "max_ms": round(numeric[-1], 3),
    }


def _numeric_ms(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _summarize_think_stage_timings(
    ops_value: Any,
    stage_groups: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ops = _json_obj(ops_value)
    notes = _json_list(ops.get("think_stage_timings"))
    total_ms = 0.0
    llm_ms = 0.0
    non_llm_ms = 0.0
    stage_count = 0
    top_stage: str | None = None
    top_stage_ms = 0.0

    for note in notes:
        if not isinstance(note, dict):
            continue
        elapsed_ms = _numeric_ms(note.get("elapsed_ms"))
        if elapsed_ms is None:
            continue
        stage_count += 1
        total_ms += elapsed_ms
        is_llm = bool(note.get("is_llm"))
        if is_llm:
            llm_ms += elapsed_ms
        else:
            non_llm_ms += elapsed_ms
        stage = str(note.get("stage") or "<missing>")
        if elapsed_ms >= top_stage_ms:
            top_stage = stage
            top_stage_ms = elapsed_ms
        if stage_groups is not None:
            _accumulate_latency_note(
                stage_groups,
                stage,
                elapsed_ms,
                extra_count_key="llm_stage_count" if is_llm else "non_llm_stage_count",
            )

    return {
        "has_stage_timings": stage_count > 0,
        "stage_count": stage_count,
        "total_ms": round(total_ms, 3),
        "llm_ms": round(llm_ms, 3),
        "non_llm_ms": round(non_llm_ms, 3),
        "top_stage": top_stage,
        "top_stage_ms": round(top_stage_ms, 3) if top_stage else None,
    }


def _accumulate_latency_note(
    groups: dict[str, dict[str, Any]],
    key: str,
    elapsed_ms: float | None,
    *,
    work_ms: float | None = None,
    wait_ms: float | None = None,
    extra_count_key: str | None = None,
) -> None:
    if elapsed_ms is None:
        return
    group = groups.setdefault(
        key,
        {
            "count": 0,
            "elapsed_values": [],
            "elapsed_ms_total": 0.0,
            "work_ms_total": 0.0,
            "wait_ms_total": 0.0,
        },
    )
    group["count"] += 1
    group["elapsed_values"].append(elapsed_ms)
    group["elapsed_ms_total"] += elapsed_ms
    if work_ms is not None:
        group["work_ms_total"] += work_ms
    if wait_ms is not None:
        group["wait_ms_total"] += wait_ms
    if extra_count_key:
        group[extra_count_key] = int(group.get(extra_count_key) or 0) + 1


def _finalize_latency_groups(groups: dict[str, dict[str, Any]]) -> dict[str, Any]:
    finalized: dict[str, Any] = {}
    for key, raw in sorted(groups.items()):
        values = raw.pop("elapsed_values", [])
        finalized[key] = {
            **raw,
            "elapsed_ms_total": round(float(raw.get("elapsed_ms_total") or 0.0), 3),
            "work_ms_total": round(float(raw.get("work_ms_total") or 0.0), 3),
            "wait_ms_total": round(float(raw.get("wait_ms_total") or 0.0), 3),
            "elapsed_ms_stats": _latency_ms_stats(values),
        }
    return finalized


def _wave_latency_breakdown(waves: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    wall_values: list[float] = []
    llm_values: list[float] = []
    non_llm_values: list[float] = []
    stage_total_values: list[float] = []
    stage_llm_values: list[float] = []
    stage_non_llm_values: list[float] = []
    stage_groups: dict[str, dict[str, Any]] = {}
    waves_with_stage_timings = 0
    for wave in waves:
        batch = wave.get("t1_batch") or {}
        run = batch.get("run") or {}
        wall_ms = _numeric_ms(batch.get("elapsed_s"))
        wall_ms = wall_ms * 1000.0 if wall_ms is not None else None
        llm_ms = _numeric_ms(run.get("llm_latency_ms"))
        non_llm_ms = (
            max(0.0, wall_ms - llm_ms)
            if wall_ms is not None and llm_ms is not None
            else None
        )
        if wall_ms is not None:
            wall_values.append(wall_ms)
        if llm_ms is not None:
            llm_values.append(llm_ms)
        if non_llm_ms is not None:
            non_llm_values.append(non_llm_ms)
        stage_summary = _summarize_think_stage_timings(
            run.get("ops_applied"),
            stage_groups,
        )
        if stage_summary["has_stage_timings"]:
            waves_with_stage_timings += 1
            stage_total_values.append(float(stage_summary["total_ms"]))
            stage_llm_values.append(float(stage_summary["llm_ms"]))
            stage_non_llm_values.append(float(stage_summary["non_llm_ms"]))
        rows.append(
            {
                "wave": wave.get("wave"),
                "sequence": wave.get("sequence"),
                "status": run.get("status"),
                "trigger_id": batch.get("trigger_id"),
                "run_id": run.get("id"),
                "wall_ms": round(wall_ms, 3) if wall_ms is not None else None,
                "llm_ms": round(llm_ms, 3) if llm_ms is not None else None,
                "non_llm_residual_ms": (
                    round(non_llm_ms, 3) if non_llm_ms is not None else None
                ),
                "stage_timings_ms": stage_summary["total_ms"],
                "llm_stage_timings_ms": stage_summary["llm_ms"],
                "non_llm_stage_timings_ms": stage_summary["non_llm_ms"],
                "stage_timing_count": stage_summary["stage_count"],
                "top_stage": stage_summary["top_stage"],
                "top_stage_ms": stage_summary["top_stage_ms"],
                "retrieval_model_count": run.get("retrieval_model_count"),
                "retrieval_observation_count": run.get("retrieval_observation_count"),
                "validation_error_count": run.get("validation_error_count"),
                "error": run.get("error"),
            }
        )
    return {
        "t1_batches": len(rows),
        "successful_t1_batches": sum(1 for row in rows if row.get("status") == "success"),
        "failed_t1_batches": sum(1 for row in rows if row.get("status") == "failed"),
        "wall_ms": _latency_ms_stats(wall_values),
        "llm_ms": _latency_ms_stats(llm_values),
        "non_llm_residual_ms": _latency_ms_stats(non_llm_values),
        "waves_with_stage_timings": waves_with_stage_timings,
        "stage_timings_ms": _latency_ms_stats(stage_total_values),
        "llm_stage_timings_ms": _latency_ms_stats(stage_llm_values),
        "non_llm_stage_timings_ms": _latency_ms_stats(stage_non_llm_values),
        "stage_timings_by_stage": _finalize_latency_groups(stage_groups),
        "waves": rows,
    }


def _think_run_latency_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    elapsed_values: list[float] = []
    llm_values: list[float] = []
    non_llm_values: list[float] = []
    stage_total_values: list[float] = []
    stage_llm_values: list[float] = []
    stage_non_llm_values: list[float] = []
    stage_groups: dict[str, dict[str, Any]] = {}
    by_kind: dict[str, dict[str, Any]] = {}
    status_counts: Counter[str] = Counter()
    runs_with_stage_timings = 0
    for row in rows:
        status = str(row.get("status") or "<missing>")
        status_counts[status] += 1
        elapsed_ms = _numeric_ms(row.get("elapsed_ms"))
        llm_ms = _numeric_ms(row.get("llm_latency_ms"))
        non_llm_ms = (
            max(0.0, elapsed_ms - llm_ms)
            if elapsed_ms is not None and llm_ms is not None
            else None
        )
        if elapsed_ms is not None:
            elapsed_values.append(elapsed_ms)
        if llm_ms is not None:
            llm_values.append(llm_ms)
        if non_llm_ms is not None:
            non_llm_values.append(non_llm_ms)
        stage_summary = _summarize_think_stage_timings(
            row.get("ops_applied"),
            stage_groups,
        )
        if stage_summary["has_stage_timings"]:
            runs_with_stage_timings += 1
            stage_total_values.append(float(stage_summary["total_ms"]))
            stage_llm_values.append(float(stage_summary["llm_ms"]))
            stage_non_llm_values.append(float(stage_summary["non_llm_ms"]))
        kind = str(row.get("trigger_kind") or "<missing>")
        group = by_kind.setdefault(
            kind,
            {
                "count": 0,
                "elapsed_values": [],
                "llm_values": [],
                "non_llm_values": [],
                "stage_total_values": [],
                "stage_llm_values": [],
                "stage_non_llm_values": [],
                "runs_with_stage_timings": 0,
                "status_counts": Counter(),
            },
        )
        group["count"] += 1
        group["status_counts"][status] += 1
        if elapsed_ms is not None:
            group["elapsed_values"].append(elapsed_ms)
        if llm_ms is not None:
            group["llm_values"].append(llm_ms)
        if non_llm_ms is not None:
            group["non_llm_values"].append(non_llm_ms)
        if stage_summary["has_stage_timings"]:
            group["runs_with_stage_timings"] += 1
            group["stage_total_values"].append(float(stage_summary["total_ms"]))
            group["stage_llm_values"].append(float(stage_summary["llm_ms"]))
            group["stage_non_llm_values"].append(float(stage_summary["non_llm_ms"]))

    return {
        "run_count": len(rows),
        "status_counts": dict(status_counts),
        "elapsed_ms": _latency_ms_stats(elapsed_values),
        "llm_ms": _latency_ms_stats(llm_values),
        "non_llm_residual_ms": _latency_ms_stats(non_llm_values),
        "runs_with_stage_timings": runs_with_stage_timings,
        "stage_timings_ms": _latency_ms_stats(stage_total_values),
        "llm_stage_timings_ms": _latency_ms_stats(stage_llm_values),
        "non_llm_stage_timings_ms": _latency_ms_stats(stage_non_llm_values),
        "stage_timings_by_stage": _finalize_latency_groups(stage_groups),
        "by_trigger_kind": {
            kind: {
                "count": data["count"],
                "status_counts": dict(data["status_counts"]),
                "elapsed_ms": _latency_ms_stats(data["elapsed_values"]),
                "llm_ms": _latency_ms_stats(data["llm_values"]),
                "non_llm_residual_ms": _latency_ms_stats(data["non_llm_values"]),
                "runs_with_stage_timings": data["runs_with_stage_timings"],
                "stage_timings_ms": _latency_ms_stats(data["stage_total_values"]),
                "llm_stage_timings_ms": _latency_ms_stats(
                    data["stage_llm_values"]
                ),
                "non_llm_stage_timings_ms": _latency_ms_stats(
                    data["stage_non_llm_values"]
                ),
            }
            for kind, data in sorted(by_kind.items())
        },
    }


def _inquiry_latency_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    runtime_values: list[float] = []
    action_total_values: list[float] = []
    stage_total_values: list[float] = []
    unaccounted_values: list[float] = []
    work_unaccounted_values: list[float] = []
    runtime_totals: Counter[str] = Counter()
    stage_groups: dict[str, dict[str, Any]] = {}
    action_groups: dict[str, dict[str, Any]] = {}
    action_subtiming_groups: dict[str, dict[str, Any]] = {}
    question_planning_modes: Counter[str] = Counter()
    sessions_with_runtime = 0
    sessions_with_action_timings = 0
    sessions_with_stage_timings = 0

    for row in rows:
        notes = _json_obj(row.get("notes"))
        runtime = _json_obj(notes.get("retrieval_runtime"))
        if runtime:
            sessions_with_runtime += 1
            for key in (
                "total_ms",
                "retrieval_action_timings_ms_total",
                "retrieval_action_work_timings_ms_total",
                "retrieval_action_wait_timings_ms_total",
                "retrieval_stage_timings_ms_total",
                "measured_ms_total",
                "non_wait_measured_ms_total",
                "parallel_wait_overcount_ms",
                "unaccounted_ms",
                "work_unaccounted_ms",
            ):
                value = _numeric_ms(runtime.get(key))
                if value is not None:
                    runtime_totals[key] += value
            if (value := _numeric_ms(runtime.get("total_ms"))) is not None:
                runtime_values.append(value)
            if (
                value := _numeric_ms(runtime.get("retrieval_action_timings_ms_total"))
            ) is not None:
                action_total_values.append(value)
            if (
                value := _numeric_ms(runtime.get("retrieval_stage_timings_ms_total"))
            ) is not None:
                stage_total_values.append(value)
            if (value := _numeric_ms(runtime.get("unaccounted_ms"))) is not None:
                unaccounted_values.append(value)
            if (value := _numeric_ms(runtime.get("work_unaccounted_ms"))) is not None:
                work_unaccounted_values.append(value)

        action_timings = _json_list(notes.get("retrieval_action_timings"))
        if action_timings:
            sessions_with_action_timings += 1
        for note in action_timings:
            if not isinstance(note, dict):
                continue
            elapsed_ms = _numeric_ms(note.get("elapsed_ms"))
            work_ms = _numeric_ms(note.get("work_elapsed_ms"))
            wait_ms = _numeric_ms(note.get("wait_elapsed_ms"))
            path = str(note.get("path") or "<missing>")
            extra = "cache_hits" if note.get("cache_hit") else None
            _accumulate_latency_note(
                action_groups,
                path,
                elapsed_ms,
                work_ms=work_ms,
                wait_ms=wait_ms,
                extra_count_key=extra,
            )
            semantic_subtimings = _json_obj(
                note.get("semantic_substrate_timings_ms")
            )
            for subkey, raw_value in semantic_subtimings.items():
                value = _numeric_ms(raw_value)
                if value is None:
                    continue
                _accumulate_latency_note(
                    action_subtiming_groups,
                    f"{path}.{subkey}",
                    value,
                    work_ms=value,
                )
            temporal_subtimings = _json_obj(note.get("temporal_timings_ms"))
            for subkey, raw_value in temporal_subtimings.items():
                value = _numeric_ms(raw_value)
                if value is None:
                    continue
                _accumulate_latency_note(
                    action_subtiming_groups,
                    f"{path}.{subkey}",
                    value,
                    work_ms=value,
                )

        stage_timings = _json_list(notes.get("retrieval_stage_timings"))
        if stage_timings:
            sessions_with_stage_timings += 1
        for note in stage_timings:
            if not isinstance(note, dict):
                continue
            _accumulate_latency_note(
                stage_groups,
                str(note.get("stage") or "<missing>"),
                _numeric_ms(note.get("elapsed_ms")),
            )

        for note in _json_list(notes.get("question_planning")):
            if isinstance(note, dict):
                question_planning_modes[str(note.get("mode") or "<missing>")] += 1

    return {
        "session_count": len(rows),
        "sessions_with_runtime": sessions_with_runtime,
        "sessions_with_action_timings": sessions_with_action_timings,
        "sessions_with_stage_timings": sessions_with_stage_timings,
        "runtime_ms": _latency_ms_stats(runtime_values),
        "retrieval_action_total_ms": _latency_ms_stats(action_total_values),
        "retrieval_stage_total_ms": _latency_ms_stats(stage_total_values),
        "unaccounted_ms": _latency_ms_stats(unaccounted_values),
        "work_unaccounted_ms": _latency_ms_stats(work_unaccounted_values),
        "runtime_totals": {
            key: round(float(value), 3) for key, value in sorted(runtime_totals.items())
        },
        "action_timings_by_path": _finalize_latency_groups(action_groups),
        "action_subtimings_by_key": _finalize_latency_groups(action_subtiming_groups),
        "stage_timings_by_stage": _finalize_latency_groups(stage_groups),
        "question_planning_modes": dict(question_planning_modes),
    }


def _compose_latency_breakdown(
    *,
    waves: list[dict[str, Any]],
    think_rows: list[dict[str, Any]],
    inquiry_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    wave_report = _wave_latency_breakdown(waves)
    think_report = _think_run_latency_breakdown(think_rows)
    inquiry_report = _inquiry_latency_breakdown(inquiry_rows)
    t1_wall_total = float((wave_report["wall_ms"] or {}).get("total_ms") or 0.0)
    t1_llm_total = float((wave_report["llm_ms"] or {}).get("total_ms") or 0.0)
    t1_non_llm_total = float(
        (wave_report["non_llm_residual_ms"] or {}).get("total_ms") or 0.0
    )
    t1_non_llm_stage_total = float(
        (wave_report["non_llm_stage_timings_ms"] or {}).get("total_ms") or 0.0
    )
    t1_non_llm_unaccounted_total = max(
        0.0,
        t1_non_llm_total - t1_non_llm_stage_total,
    )
    t1_unclassified_total = max(
        0.0,
        t1_wall_total - t1_llm_total - t1_non_llm_total,
    )
    inquiry_runtime_total = float(
        (inquiry_report.get("runtime_totals") or {}).get("total_ms") or 0.0
    )
    return {
        "available": bool(waves or think_rows or inquiry_rows),
        "scope": "tenant_wide_consistent_with_model_layer_summary",
        "units": "milliseconds",
        "t1_wave_wall_clock": wave_report,
        "think_runs": think_report,
        "adaptive_inquiry": inquiry_report,
        "critical_path_summary": {
            "t1_wall_ms_total": round(t1_wall_total, 3),
            "t1_llm_ms_total": round(t1_llm_total, 3),
            "t1_non_llm_residual_ms_total": round(t1_non_llm_total, 3),
            "t1_measured_non_llm_stage_ms_total": round(t1_non_llm_stage_total, 3),
            "t1_non_llm_unaccounted_stage_ms_total": round(
                t1_non_llm_unaccounted_total,
                3,
            ),
            "t1_unclassified_or_failed_ms_total": round(t1_unclassified_total, 3),
            "adaptive_inquiry_runtime_ms_total": round(inquiry_runtime_total, 3),
            "adaptive_inquiry_share_of_t1_wall": round(
                _ratio(inquiry_runtime_total, t1_wall_total),
                4,
            ),
            "main_llm_share_of_t1_wall": round(
                _ratio(t1_llm_total, t1_wall_total),
                4,
            ),
            "non_main_llm_share_of_t1_wall": round(
                _ratio(t1_non_llm_total, t1_wall_total),
                4,
            ),
            "measured_non_llm_stage_share_of_non_llm_residual": round(
                _ratio(t1_non_llm_stage_total, t1_non_llm_total),
                4,
            ),
            "unclassified_or_failed_share_of_t1_wall": round(
                _ratio(t1_unclassified_total, t1_wall_total),
                4,
            ),
        },
        "instrumentation_notes": [
            "T1 wall-clock is measured by the benchmark around worker._dispatch_trigger.",
            "think_runs elapsed_ms is computed from started_at/ended_at in Postgres.",
            "llm_ms is the main Think llm_reason latency persisted on think_runs.",
            "Think internal stage timings come from think_runs.ops_applied.think_stage_timings on newly instrumented runs.",
            "Think stage timings classify main_llm_reason as LLM; all other stages explain the non-main-LLM residual.",
            "adaptive_inquiry timings come from inquiry_sessions.notes.",
            "Action timing totals can overcount parallel branches; use work/wait fields and wave wall-clock for critical-path reasoning.",
            "Post-commit drain is not part of T1 wave wall-clock; it is reported separately by post_commit_status and topology_optimizer_status.",
        ],
    }


async def _collect_latency_breakdown(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    waves: list[dict[str, Any]],
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        think_rows = [
            _record_to_dict(row)
            for row in await conn.fetch(
                """
                SELECT id, trigger_id, trigger_kind, status, error,
                       retrieval_model_count, retrieval_observation_count,
                       llm_latency_ms, validation_error_count,
                       ops_applied,
                       CASE
                         WHEN ended_at IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (ended_at - started_at)) * 1000.0
                         ELSE NULL
                       END AS elapsed_ms
                FROM think_runs
                WHERE tenant_id = $1
                ORDER BY started_at
                """,
                tenant_id,
            )
        ]
        inquiry_rows: list[dict[str, Any]] = []
        if await _table_exists(conn, "inquiry_sessions"):
            inquiry_rows = [
                _record_to_dict(row)
                for row in await conn.fetch(
                    """
                    SELECT id, route, status, stop_status, round_count,
                           question_count, evidence_count, context_packet, notes
                    FROM inquiry_sessions
                    WHERE tenant_id = $1
                    ORDER BY completed_at
                    """,
                    tenant_id,
                )
            ]
    return _compose_latency_breakdown(
        waves=waves,
        think_rows=think_rows,
        inquiry_rows=inquiry_rows,
    )


async def _collect_think_cost_profile(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        if not await _table_exists(conn, "think_run_costs"):
            return {"available": False}
        rows = await conn.fetch(
            """
            WITH cost_by_trigger AS (
              SELECT trigger_id,
                     COALESCE(sum(llm_calls_count), 0)::int AS llm_calls,
                     COALESCE(sum(llm_cost_usd), 0)::float8 AS cost_usd
              FROM think_run_costs
              WHERE tenant_id = $1
              GROUP BY trigger_id
            )
            SELECT
              r.trigger_kind,
              COALESCE(q.trigger_subkind, '') AS trigger_subkind,
              COUNT(*)::int AS runs,
              COALESCE(SUM(c.llm_calls), 0)::int AS llm_calls,
              COALESCE(SUM(c.cost_usd), 0)::float8 AS cost_usd
            FROM think_runs r
            LEFT JOIN think_trigger_queue q ON q.id = r.trigger_id
            LEFT JOIN cost_by_trigger c ON c.trigger_id = r.trigger_id
            WHERE r.tenant_id = $1
            GROUP BY r.trigger_kind, COALESCE(q.trigger_subkind, '')
            ORDER BY r.trigger_kind, COALESCE(q.trigger_subkind, '')
            """,
            tenant_id,
        )
        t4_rows = await conn.fetch(
            """
            WITH cost_by_trigger AS (
              SELECT trigger_id,
                     COALESCE(sum(llm_calls_count), 0)::int AS llm_calls,
                     COALESCE(sum(llm_input_tokens_total), 0)::bigint AS input_tokens,
                     COALESCE(sum(llm_output_tokens_total), 0)::bigint AS output_tokens,
                     COALESCE(sum(llm_cost_usd), 0)::float8 AS cost_usd
              FROM think_run_costs
              WHERE tenant_id = $1
              GROUP BY trigger_id
            )
            SELECT
              r.trigger_id,
              r.trigger_kind,
              r.status,
              r.ops_applied,
              COALESCE(q.trigger_subkind, '') AS trigger_subkind,
              q.payload,
              COALESCE(c.llm_calls, 0)::int AS llm_calls,
              COALESCE(c.input_tokens, 0)::bigint AS input_tokens,
              COALESCE(c.output_tokens, 0)::bigint AS output_tokens,
              COALESCE(c.cost_usd, 0)::float8 AS cost_usd
            FROM think_runs r
            LEFT JOIN think_trigger_queue q ON q.id = r.trigger_id
            LEFT JOIN cost_by_trigger c ON c.trigger_id = r.trigger_id
            WHERE r.tenant_id = $1
              AND split_part(r.trigger_kind, ':', 1) = 'T4'
            ORDER BY r.started_at ASC
            """,
            tenant_id,
        )
        audit_rows = await conn.fetch(
            """
            SELECT r.ops_applied
            FROM think_runs r
            WHERE r.tenant_id = $1
              AND split_part(r.trigger_kind, ':', 1) = 'T1'
              AND r.ops_applied ? 'representation_audit'
            ORDER BY r.started_at ASC
            """,
            tenant_id,
        )
    by_kind: dict[str, dict[str, Any]] = {}
    product_path = {"runs": 0, "llm_calls": 0, "cost_usd": 0.0}
    background = {"runs": 0, "llm_calls": 0, "cost_usd": 0.0}
    total = {"runs": 0, "llm_calls": 0, "cost_usd": 0.0}
    for row in rows:
        trigger_kind = str(row["trigger_kind"] or "")
        trigger_family = _trigger_kind_family(trigger_kind)
        trigger_subkind = str(row["trigger_subkind"] or "")
        key = f"{trigger_kind}:{trigger_subkind}"
        entry = {
            "trigger_kind": trigger_kind,
            "trigger_family": trigger_family,
            "trigger_subkind": trigger_subkind,
            "runs": int(row["runs"] or 0),
            "llm_calls": int(row["llm_calls"] or 0),
            "cost_usd": round(float(row["cost_usd"] or 0.0), 6),
        }
        by_kind[key] = entry
        target = (
            background
            if trigger_family in _BACKGROUND_MAINTENANCE_TRIGGER_KINDS
            else product_path
        )
        for bucket in (target, total):
            bucket["runs"] += entry["runs"]
            bucket["llm_calls"] += entry["llm_calls"]
            bucket["cost_usd"] += float(entry["cost_usd"])
    for bucket in (product_path, background, total):
        bucket["cost_usd"] = round(float(bucket["cost_usd"]), 6)
    return {
        "available": True,
        "efficiency_scope": "product_path_excludes_t4_background_maintenance",
        "background_maintenance_trigger_kinds": sorted(
            _BACKGROUND_MAINTENANCE_TRIGGER_KINDS
        ),
        "product_path": product_path,
        "background_maintenance": background,
        "total": total,
        "by_kind": by_kind,
        "t4_roi": _build_t4_roi_profile(t4_rows, audit_rows),
    }


_REPAIR_WARNING_CODES = frozenset(
    {
        "prediction_lifecycle_not_exercised",
        "truth_pressure_absent_for_contestable_memory",
        "missing_curiosity_coverage",
        "company_question_coverage_too_thin",
        "missing_source_coverage",
        "missing_discovered_pattern_coverage",
        "selected_raw_evidence_too_low",
        "selected_model_support_runaway",
    }
)


def _build_t4_roi_profile(
    t4_rows: list[Any],
    audit_rows: list[Any],
) -> dict[str, Any]:
    by_subkind: dict[str, dict[str, Any]] = {}
    total = {
        "runs": 0,
        "llm_calls": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "cost_usd": 0.0,
        "batch_runs": 0,
        "batched_member_count": 0,
        "estimated_calls_saved_by_batching": 0,
        "durable_outcome_runs": 0,
    }
    repair_warning_codes: Counter[str] = Counter()
    repair_intents: Counter[str] = Counter()
    open_question_searches = 0
    latent_candidate_reviews = 0

    for row in t4_rows:
        trigger_kind = str(_row_value(row, "trigger_kind", "") or "")
        if _trigger_kind_family(trigger_kind) != "T4":
            continue
        subkind = str(_row_value(row, "trigger_subkind", "") or "")
        payload = _json_obj(_row_value(row, "payload", {}))
        ops = _json_obj(_row_value(row, "ops_applied", {}))
        llm_calls = _intish(_row_value(row, "llm_calls", 0))
        input_tokens = _intish(_row_value(row, "input_tokens", 0))
        output_tokens = _intish(_row_value(row, "output_tokens", 0))
        cost_usd = float(_row_value(row, "cost_usd", 0.0) or 0.0)
        bucket = by_subkind.setdefault(
            subkind,
            {
                "runs": 0,
                "llm_calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cost_usd": 0.0,
                "batch_runs": 0,
                "batched_member_count": 0,
                "estimated_calls_saved_by_batching": 0,
                "durable_outcome_runs": 0,
            },
        )
        member_count = _batch_member_count(payload)
        is_batch = member_count > 1 or payload.get("batch") is True
        durable = _ops_has_durable_outcome(ops)
        for target in (bucket, total):
            target["runs"] += 1
            target["llm_calls"] += llm_calls
            target["input_tokens"] += input_tokens
            target["output_tokens"] += output_tokens
            target["cost_usd"] += cost_usd
            if is_batch:
                target["batch_runs"] += 1
                target["batched_member_count"] += member_count
                target["estimated_calls_saved_by_batching"] += max(0, member_count - 1)
            if durable:
                target["durable_outcome_runs"] += 1
        if subkind == "representation_repair":
            for item in _repair_items_from_payload(payload):
                warning = str(item.get("audit_warning_code") or "")
                intent = str(item.get("repair_intent") or "")
                if warning:
                    repair_warning_codes[warning] += 1
                if intent:
                    repair_intents[intent] += 1
        elif subkind == "open_question_search":
            open_question_searches += max(
                1,
                len(_json_list(payload.get("open_question_batch_items")))
                or len(_json_list(payload.get("open_question_keys"))),
            )
        elif subkind == "latent_relationship_candidate":
            latent_candidate_reviews += max(
                1,
                len(_json_list(payload.get("relationship_candidate_ids"))),
            )

    for bucket in [total, *by_subkind.values()]:
        bucket["cost_usd"] = round(float(bucket["cost_usd"]), 6)
    suppressed = _suppressed_noop_repair_profile(audit_rows)
    return {
        "available": True,
        "total": total,
        "by_subkind": by_subkind,
        "representation_repair": {
            "warning_codes": dict(sorted(repair_warning_codes.items())),
            "repair_intents": dict(sorted(repair_intents.items())),
        },
        "open_question_search": {
            "question_search_items": open_question_searches,
        },
        "latent_relationship_candidate": {
            "candidate_review_items": latent_candidate_reviews,
        },
        "suppressed_justified_noop_repairs": suppressed,
    }


def _suppressed_noop_repair_profile(rows: list[Any]) -> dict[str, Any]:
    audits = 0
    warnings = 0
    by_warning: Counter[str] = Counter()
    for row in rows:
        ops = _json_obj(_row_value(row, "ops_applied", {}))
        audit = _json_obj(ops.get("representation_audit"))
        if not audit:
            continue
        audit_warnings = [
            warning
            for warning in _json_list(audit.get("warnings"))
            if isinstance(warning, dict)
            and str(warning.get("code") or "") in _REPAIR_WARNING_CODES
        ]
        if not audit_warnings:
            continue
        if _json_list(ops.get("representation_repair_triggers")):
            continue
        metrics = _json_obj(audit.get("metrics"))
        grade = str(metrics.get("context_use_grade") or "")
        trace = str(metrics.get("reasoning_trace") or "").lower()
        no_state = _intish(metrics.get("state_changes_emitted")) <= 0
        no_adaptiveness = (
            _intish(audit.get("model_adaptiveness"))
            + _intish(audit.get("edge_adaptiveness"))
            <= 0
        )
        justified = grade in {"justified_noop_context_used", "noop_trace_accounted"}
        noisy = any(
            marker in trace
            for marker in ("discard_as_noise", "noise-only", "noise only", "noisy path")
        )
        if not (no_state and no_adaptiveness and (justified or noisy)):
            continue
        audits += 1
        warnings += len(audit_warnings)
        for warning in audit_warnings:
            by_warning[str(warning.get("code") or "unknown")] += 1
    return {
        "audit_runs": audits,
        "repair_warnings_suppressed": warnings,
        "by_warning_code": dict(sorted(by_warning.items())),
    }


def _repair_items_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = [
        item for item in _json_list(payload.get("repair_batch_items"))
        if isinstance(item, dict)
    ]
    if items:
        return items
    return [
        {
            "audit_warning_code": payload.get("audit_warning_code"),
            "repair_intent": payload.get("repair_intent"),
        }
    ]


def _batch_member_count(payload: dict[str, Any]) -> int:
    for key in (
        "batch_member_trigger_ids",
        "member_trigger_ids",
        "repair_batch_items",
        "open_question_batch_items",
        "relationship_candidate_ids",
    ):
        values = _json_list(payload.get(key))
        if values:
            return len(values)
    return 1


def _ops_has_durable_outcome(ops: dict[str, Any]) -> bool:
    if _intish(ops.get("state_changes_emitted")) > 0:
        return True
    for key in (
        "claim_ops",
        "memory_lifecycle_ops",
        "relation_claim_ops",
        "relation_frame_ops",
        "edge_ops",
        "ontology_gap_ops",
        "open_question_ops",
        "act_ops",
        "resource_ops",
    ):
        if _json_list(ops.get(key)):
            return True
    memory = _json_obj(ops.get("memory_aggregation"))
    return any(
        _intish(memory.get(key)) > 0
        for key in (
            "model_inserts",
            "model_updates",
            "model_archives",
            "evidence_attachments",
            "near_duplicate_absorptions",
        )
    )


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def _intish(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def _collect_post_commit_action_profile(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        if not await _table_exists(conn, "pending_post_commit_actions"):
            return {"available": False}
        by_kind_rows = await conn.fetch(
            """
            WITH profiled AS (
              SELECT
                action_kind,
                processed_at,
                dead_lettered_at,
                COALESCE(action_payload->>'source_trigger_kind', '') AS source_kind,
                COALESCE(action_payload->>'source_trigger_subkind', '') AS source_subkind,
                COALESCE(action_payload->>'selector', '') AS selector,
                COALESCE((action_payload->>'enqueue_think')::boolean, false)
                  AS enqueue_think,
                CASE
                  WHEN jsonb_typeof(action_payload->'model_ids') = 'array'
                  THEN jsonb_array_length(action_payload->'model_ids')
                  ELSE 0
                END AS model_id_count
              FROM pending_post_commit_actions
              WHERE tenant_id = $1
            )
            SELECT
              action_kind,
              COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE processed_at IS NOT NULL)::int AS processed,
              COUNT(*) FILTER (WHERE dead_lettered_at IS NOT NULL)::int AS dead_lettered,
              COALESCE(SUM(model_id_count), 0)::int AS model_ids_total,
              COALESCE(MAX(model_id_count), 0)::int AS max_model_ids,
              COUNT(*) FILTER (WHERE enqueue_think)::int AS enqueue_think_true,
              COUNT(*) FILTER (WHERE NOT enqueue_think)::int AS enqueue_think_false
            FROM profiled
            GROUP BY action_kind
            ORDER BY action_kind
            """,
            tenant_id,
        )
        source_rows = await conn.fetch(
            """
            SELECT
              action_kind,
              COALESCE(action_payload->>'source_trigger_kind', '') AS source_kind,
              COALESCE(action_payload->>'source_trigger_subkind', '') AS source_subkind,
              COALESCE(action_payload->>'selector', '') AS selector,
              COUNT(*)::int AS total,
              COALESCE(
                SUM(
                  CASE
                    WHEN jsonb_typeof(action_payload->'model_ids') = 'array'
                    THEN jsonb_array_length(action_payload->'model_ids')
                    ELSE 0
                  END
                ),
                0
              )::int AS model_ids_total,
              COUNT(*) FILTER (
                WHERE COALESCE((action_payload->>'enqueue_think')::boolean, false)
              )::int AS enqueue_think_true
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND action_kind IN ('discover_model_edges', 'materialize_projections')
            GROUP BY action_kind, source_kind, source_subkind, selector
            ORDER BY action_kind, total DESC, source_kind, source_subkind, selector
            """,
            tenant_id,
        )
    by_kind = {str(row["action_kind"]): _record_to_dict(row) for row in by_kind_rows}
    return {
        "available": True,
        "by_kind": by_kind,
        "source_profile": [_record_to_dict(row) for row in source_rows],
    }


async def _collect_downstream_suppression(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        if not await _table_exists(conn, "think_trigger_queue"):
            return {"available": False}
        reason_rows = await conn.fetch(
            """
            SELECT
              trigger_kind || ':' || COALESCE(trigger_subkind, '') AS trigger_kind,
              payload->>'auto_completed_reason' AS reason,
              COUNT(*)::int AS total
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NOT NULL
              AND payload ? 'auto_completed_reason'
            GROUP BY trigger_kind, trigger_subkind, reason
            ORDER BY total DESC, trigger_kind, reason
            """,
            tenant_id,
        )
        trigger_rows = await conn.fetch(
            """
            SELECT
              trigger_kind || ':' || COALESCE(trigger_subkind, '') AS trigger_kind,
              COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE payload ? 'auto_completed_reason')::int
                AS auto_completed,
              COUNT(*) FILTER (WHERE payload->>'batch' = 'true')::int AS batches,
              COUNT(*) FILTER (WHERE batch_parent_id IS NOT NULL)::int
                AS batched_members,
              COUNT(*) FILTER (WHERE completed_at IS NULL)::int AS pending
            FROM think_trigger_queue
            WHERE tenant_id = $1
            GROUP BY trigger_kind, trigger_subkind
            ORDER BY trigger_kind
            """,
            tenant_id,
        )
    reason_counts = [_record_to_dict(row) for row in reason_rows]
    return {
        "available": True,
        "auto_completed_total": sum(int(row["total"] or 0) for row in reason_counts),
        "auto_completed_by_reason": reason_counts,
        "trigger_profile": [_record_to_dict(row) for row in trigger_rows],
    }


async def _fetch_reflective_rule_lifecycle(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any]:
    if not await _table_exists(conn, "reflective_retrieval_rules"):
        return {"available": False}
    rows = await conn.fetch(
        """
        SELECT maturity AS key, COUNT(*)::bigint AS value
        FROM reflective_retrieval_rules
        WHERE tenant_id = $1
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        """,
        tenant_id,
    )
    row = await conn.fetchrow(
        """
        SELECT
          COUNT(*)::bigint AS total_rules,
          COALESCE(SUM(success_count), 0)::bigint AS successes,
          COALESCE(SUM(failure_count), 0)::bigint AS failures,
          AVG(utility_score) AS avg_utility,
          MAX(utility_score) AS max_utility
        FROM reflective_retrieval_rules
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    report = _record_to_dict(row)
    report["available"] = True
    report["maturity_distribution"] = {
        str(item["key"]): int(item["value"] or 0) for item in rows
    }
    return report


async def _fetch_reflective_replay_report(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any]:
    if not await _table_exists(conn, "reflective_rule_replay_runs"):
        return {"available": False}
    rows = await conn.fetch(
        """
        SELECT decision AS key, COUNT(*)::bigint AS value
        FROM reflective_rule_replay_runs
        WHERE tenant_id = $1
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        """,
        tenant_id,
    )
    row = await conn.fetchrow(
        """
        SELECT
          COUNT(*)::bigint AS replay_runs,
          AVG(utility_delta) AS avg_utility_delta,
          MAX(utility_delta) AS max_utility_delta,
          MIN(utility_delta) AS min_utility_delta
        FROM reflective_rule_replay_runs
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    report = _record_to_dict(row)
    report["available"] = True
    report["decision_distribution"] = {
        str(item["key"]): int(item["value"] or 0) for item in rows
    }
    return report


async def _fetch_reflective_attribution_report(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any]:
    if not await _table_exists(conn, "reflective_rule_attributions"):
        return {"available": False}
    row = await conn.fetchrow(
        """
        SELECT
          COUNT(*)::bigint AS attributions,
          COUNT(DISTINCT rule_id)::bigint AS attributed_rules,
          COUNT(DISTINCT inquiry_session_id)::bigint AS attributed_sessions,
          COALESCE(SUM(evidence_count), 0)::bigint AS evidence_count,
          COALESCE(SUM(selected_evidence_count), 0)::bigint
            AS selected_evidence_count,
          COALESCE(SUM(credit), 0.0) AS total_credit,
          COALESCE(SUM(cost), 0.0) AS total_cost,
          AVG(outcome_score) AS avg_outcome_score
        FROM reflective_rule_attributions
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    report = _record_to_dict(row)
    report["available"] = True
    return report


_REQUIRED_ENTITY_PROJECTION_FAMILIES = {
    "employees": "employee_profiles",
    "commitments": "commitments",
    "customers": "customers",
    "goals": "goals",
    "decisions": "decisions",
    "resources": "resources",
}


async def _collect_projection_metabolism_report(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        if not await _table_exists(conn, "projection_snapshots"):
            return {
                "available": False,
                "reason": "projection_snapshots_table_missing",
            }

        snapshot_rows = await conn.fetch(
            """
            SELECT
              projection_name,
              split_part(subject_key, ':', 1) AS subject_prefix,
              COUNT(*)::bigint AS snapshot_count,
              COUNT(DISTINCT subject_key)::bigint AS subject_count
            FROM projection_snapshots
            WHERE tenant_id = $1
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            tenant_id,
        )
        snapshots_by_family: Counter[str] = Counter()
        subject_prefix_matrix: dict[str, dict[str, int]] = {}
        for row in snapshot_rows:
            projection_name = str(row["projection_name"] or "")
            subject_prefix = str(row["subject_prefix"] or "")
            snapshot_count = int(row["snapshot_count"] or 0)
            snapshots_by_family[projection_name] += snapshot_count
            subject_prefix_matrix.setdefault(projection_name, {})[
                subject_prefix
            ] = int(row["subject_count"] or 0)

        total_snapshots = sum(snapshots_by_family.values())
        total_refresh_jobs = 0
        pending_refresh_jobs = 0
        failed_refresh_jobs = 0
        job_status_counts: dict[str, dict[str, int]] = {}
        max_refresh_subject: dict[str, Any] | None = None
        if await _table_exists(conn, "projection_refresh_jobs"):
            job_rows = await conn.fetch(
                """
                SELECT projection_name, status, COUNT(*)::bigint AS job_count
                FROM projection_refresh_jobs
                WHERE tenant_id = $1
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
                tenant_id,
            )
            for row in job_rows:
                projection_name = str(row["projection_name"] or "")
                status = str(row["status"] or "")
                count = int(row["job_count"] or 0)
                total_refresh_jobs += count
                if status in {"pending", "leased"}:
                    pending_refresh_jobs += count
                if status in {"failed", "dead_letter"}:
                    failed_refresh_jobs += count
                job_status_counts.setdefault(projection_name, {})[status] = count

            max_row = await conn.fetchrow(
                """
                SELECT projection_name, subject_key, COUNT(*)::bigint AS refresh_jobs
                FROM projection_refresh_jobs
                WHERE tenant_id = $1
                GROUP BY 1, 2
                ORDER BY refresh_jobs DESC, projection_name ASC, subject_key ASC
                LIMIT 1
                """,
                tenant_id,
            )
            if max_row is not None:
                max_refresh_subject = _record_to_dict(max_row)

        relation_projection_report = await _collect_relation_projection_report(
            conn,
            tenant_id=tenant_id,
        )

    entity_coverage: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for entity_name, projection_name in _REQUIRED_ENTITY_PROJECTION_FAMILIES.items():
        snapshot_count = int(snapshots_by_family.get(projection_name) or 0)
        covered = snapshot_count > 0
        if not covered:
            missing.append(entity_name)
        entity_coverage[entity_name] = {
            "projection_name": projection_name,
            "snapshot_count": snapshot_count,
            "covered": covered,
        }

    covered_count = len(_REQUIRED_ENTITY_PROJECTION_FAMILIES) - len(missing)
    jobs_to_snapshots_ratio = (
        round(total_refresh_jobs / total_snapshots, 4) if total_snapshots else None
    )
    status = (
        "ok"
        if not missing and pending_refresh_jobs == 0 and failed_refresh_jobs == 0
        else "watch"
    )
    return {
        "available": True,
        "status": status,
        "required_entity_projection_families": dict(
            _REQUIRED_ENTITY_PROJECTION_FAMILIES
        ),
        "entity_projection_coverage": entity_coverage,
        "entity_projection_coverage_ratio": round(
            covered_count / len(_REQUIRED_ENTITY_PROJECTION_FAMILIES),
            4,
        ),
        "missing_entity_projection_families": missing,
        "snapshot_count": total_snapshots,
        "snapshot_count_by_family": dict(sorted(snapshots_by_family.items())),
        "subject_prefix_matrix": subject_prefix_matrix,
        "refresh_job_count": total_refresh_jobs,
        "refresh_job_status_by_family": job_status_counts,
        "pending_refresh_jobs": pending_refresh_jobs,
        "failed_refresh_jobs": failed_refresh_jobs,
        "jobs_to_snapshots_ratio": jobs_to_snapshots_ratio,
        "max_refresh_subject": max_refresh_subject or {},
        "relation_projection_report": relation_projection_report,
    }


async def _collect_relation_projection_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    if not await _table_exists(conn, "relation_instances"):
        return {"available": False, "reason": "relation_instances_table_missing"}
    if not await _table_exists(conn, "relation_edge_projections"):
        return {"available": False, "reason": "relation_edge_projections_table_missing"}
    row = await conn.fetchrow(
        """
        SELECT
          COUNT(*) FILTER (
            WHERE ri.status = 'accepted'
              AND ri.write_policy = 'project_edges'
          )::bigint AS projectable_relation_frames,
          COUNT(DISTINCT rep.id) FILTER (
            WHERE rep.status = 'active'
          )::bigint AS active_relation_edge_projections,
          COUNT(DISTINCT rep.edge_kind) FILTER (
            WHERE rep.status = 'active'
          )::bigint AS active_projection_kind_count
        FROM relation_instances ri
        LEFT JOIN relation_edge_projections rep
          ON rep.tenant_id = ri.tenant_id
         AND rep.relation_id = ri.id
        WHERE ri.tenant_id = $1
        """,
        tenant_id,
    )
    out = _record_to_dict(row)
    out["available"] = True
    return out


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    found = await conn.fetchval(f"SELECT to_regclass('public.{table_name}')")
    return found is not None


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


@dataclass(frozen=True)
class _StorylineScoringData:
    models: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    relation_frames: list[dict[str, Any]]
    candidates: list[dict[str, Any]]
    observations: list[dict[str, Any]]


@dataclass(frozen=True)
class _StoryObservationIndex:
    observations_by_story: dict[str, set[str]]
    future_observations_by_story: dict[str, set[str]]
    transition_phase_by_observation: dict[str, str]


@dataclass(frozen=True)
class _LatentPatternScoring:
    score: float
    models: list[dict[str, Any]]
    evidence_supported_models: list[dict[str, Any]]
    best_assessment: dict[str, Any]


@dataclass(frozen=True)
class _BridgeScoring:
    score: float
    model_count: int
    transition_supported_count: int
    future_confirmed_count: int
    unsupported_specific_claim_count: int
    epistemic_marker_hits: set[str]
    forbidden_detail_hits: set[str]


@dataclass(frozen=True)
class _EdgeReviewScoring:
    scoped_edge_count: int
    edge_kind_hits: list[str]
    missing_edge_kinds: list[str]
    relation_frame_count: int
    accepted_relation_frame_count: int
    relation_frame_kind_hits: list[str]
    relation_frame_projection_count: int
    review_candidate_count: int
    accepted_count: int
    needs_review_count: int
    edge_score: float


@dataclass(frozen=True)
class _ThesisJudgeScoring:
    score: float | None = None
    correct: bool | None = None
    rationale: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    judged: bool = False


async def _fetch_storyline_scoring_data(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> _StorylineScoringData:
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
        if await _table_exists(conn, "relation_instances"):
            relation_frame_rows = await conn.fetch(
                """
                SELECT
                  ri.id,
                  ri.relation_kind,
                  ri.status,
                  ri.write_policy,
                  ri.participant_binding_status,
                  ri.evidence_model_ids,
                  COALESCE(
                    array_agg(DISTINCT rp.model_id)
                      FILTER (WHERE rp.model_id IS NOT NULL),
                    '{}'::uuid[]
                  ) AS participant_model_ids,
                  COALESCE(
                    array_agg(DISTINCT rep.edge_kind)
                      FILTER (
                        WHERE rep.edge_kind IS NOT NULL
                          AND rep.status = 'active'
                      ),
                    '{}'::text[]
                  ) AS projected_edge_kinds,
                  COUNT(DISTINCT rep.id)
                    FILTER (WHERE rep.status = 'active')
                    AS active_projection_count
                FROM relation_instances ri
                LEFT JOIN relation_participants rp
                  ON rp.tenant_id = ri.tenant_id
                 AND rp.relation_id = ri.id
                LEFT JOIN relation_edge_projections rep
                  ON rep.tenant_id = ri.tenant_id
                 AND rep.relation_id = ri.id
                WHERE ri.tenant_id = $1
                GROUP BY ri.id
                """,
                tenant_id,
            )
        else:
            relation_frame_rows = []
        observation_rows = await conn.fetch(
            """
            SELECT id, external_id, content
            FROM observations
            WHERE tenant_id = $1
              AND content->>'benchmark' = 'storyline_batch'
            """,
            tenant_id,
        )
    return _StorylineScoringData(
        models=[_record_to_dict(row) for row in model_rows],
        edges=[_record_to_dict(row) for row in edge_rows],
        relation_frames=[_record_to_dict(row) for row in relation_frame_rows],
        candidates=[_record_to_dict(row) for row in candidate_rows],
        observations=[_record_to_dict(row) for row in observation_rows],
    )


def _index_storyline_observations(
    observations: list[dict[str, Any]],
) -> _StoryObservationIndex:
    observations_by_story: dict[str, set[str]] = {}
    future_observations_by_story: dict[str, set[str]] = {}
    transition_phase_by_observation: dict[str, str] = {}
    for observation in observations:
        content = _json_obj(observation.get("content"))
        story_id = _story_id_from_external_id(observation.get("external_id"))
        if not isinstance(story_id, str):
            continue
        observation_id = str(observation["id"])
        observations_by_story.setdefault(story_id, set()).add(observation_id)
        if content.get("phase") == "future_validation":
            future_observations_by_story.setdefault(story_id, set()).add(observation_id)
        transition_phase = content.get("transition_phase")
        if isinstance(transition_phase, str):
            transition_phase_by_observation[observation_id] = transition_phase
    return _StoryObservationIndex(
        observations_by_story=observations_by_story,
        future_observations_by_story=future_observations_by_story,
        transition_phase_by_observation=transition_phase_by_observation,
    )


def _score_latent_patterns(
    *,
    spec: StorylineSpec,
    relevant_models: list[dict[str, Any]],
    story_observations: set[str],
) -> _LatentPatternScoring:
    assessments = [_latent_pattern_assessment(model, spec) for model in relevant_models]
    best_assessment = max(
        assessments,
        key=lambda assessment: assessment["coverage"],
        default={
            "coverage": 0.0,
            "hits": [],
            "missing": [
                _latent_group_label(group) for group in spec.latent_pattern_groups
            ],
        },
    )
    latent_models = [
        model
        for model, assessment in zip(relevant_models, assessments, strict=False)
        if assessment["coverage"] >= 0.6
        and (
            _is_situation_model(model)
            or _is_recommendation_model(model)
            or _is_concern_model(model)
        )
    ]
    evidence_supported = [
        model
        for model in latent_models
        if set(map(str, model.get("supporting_event_ids") or [])) & story_observations
    ]
    best_coverage = float(best_assessment["coverage"])
    score = (
        0.60 * best_coverage
        + 0.25 * (1.0 if latent_models else 0.0)
        + 0.15 * (min(1.0, len(evidence_supported) / 2.0) if latent_models else 0.0)
    )
    return _LatentPatternScoring(
        score=score,
        models=latent_models,
        evidence_supported_models=evidence_supported,
        best_assessment=best_assessment,
    )


def _score_latent_bridge(
    *,
    spec: StorylineSpec,
    relevant_models: list[dict[str, Any]],
    future_observations: set[str],
    transition_phase_by_observation: dict[str, str],
) -> _BridgeScoring:
    if spec.id != _LATENT_BRIDGE_STORYLINE_ID:
        return _BridgeScoring(0.0, 0, 0, 0, 0, set(), set())

    model_count = 0
    transition_supported_count = 0
    future_confirmed_count = 0
    unsupported_specific_claim_count = 0
    epistemic_marker_hits: set[str] = set()
    forbidden_detail_hits: set[str] = set()
    for model in relevant_models:
        assessment = _latent_bridge_assessment(model)
        if assessment["coverage"] < 0.5:
            continue
        model_count += 1
        support_ids = set(map(str, model.get("supporting_event_ids") or []))
        support_phases = _bridge_support_phases(
            model,
            support_ids=support_ids,
            transition_phase_by_observation=transition_phase_by_observation,
        )
        transition_supported = "before_state" in support_phases and (
            "after_state" in support_phases or "gap_review" in support_phases
        )
        future_confirmed = "future_confirmation" in support_phases or bool(
            support_ids & future_observations
        )
        if transition_supported:
            transition_supported_count += 1
        if future_confirmed:
            future_confirmed_count += 1
        epistemic_marker_hits.update(assessment["epistemic_hits"])
        forbidden_detail_hits.update(assessment["forbidden_detail_hits"])
        if assessment["forbidden_detail_hits"] and not future_confirmed:
            unsupported_specific_claim_count += 1

    score = _clamp01(
        0.25 * (1.0 if model_count else 0.0)
        + 0.25
        * (_ratio(transition_supported_count, model_count) if model_count else 0.0)
        + 0.20 * _clamp01(len(epistemic_marker_hits) / 3.0)
        + 0.15 * (_ratio(future_confirmed_count, model_count) if model_count else 0.0)
        + 0.15
        * (
            1.0 - _ratio(unsupported_specific_claim_count, model_count)
            if model_count
            else 0.0
        )
    )
    return _BridgeScoring(
        score=score,
        model_count=model_count,
        transition_supported_count=transition_supported_count,
        future_confirmed_count=future_confirmed_count,
        unsupported_specific_claim_count=unsupported_specific_claim_count,
        epistemic_marker_hits=epistemic_marker_hits,
        forbidden_detail_hits=forbidden_detail_hits,
    )


def _bridge_support_phases(
    model: dict[str, Any],
    *,
    support_ids: set[str],
    transition_phase_by_observation: dict[str, str],
) -> set[str]:
    phases = {
        transition_phase_by_observation.get(observation_id)
        for observation_id in support_ids
    }
    phases.discard(None)

    proposition = _json_obj(model.get("proposition"))
    transition_support = _json_obj(proposition.get("transition_support"))
    if not transition_support:
        transition_support = _json_obj(model.get("transition_support"))
    phase_fields = {
        "before_state": "before_state_event_ids",
        "after_state": "after_state_event_ids",
        "gap_review": "gap_review_event_ids",
    }
    for phase, support_field in phase_fields.items():
        if _json_list(transition_support.get(support_field)):
            phases.add(phase)
    return {str(phase) for phase in phases if phase}


def _score_storyline_edges_and_review(
    *,
    spec: StorylineSpec,
    edges: list[dict[str, Any]],
    relation_frames: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    relevant_model_ids: set[str],
) -> _EdgeReviewScoring:
    scoped_edge_count = sum(
        1
        for edge in edges
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
    relevant_frames = [
        frame
        for frame in relation_frames
        if (
            set(map(str, frame.get("participant_model_ids") or []))
            | set(map(str, frame.get("evidence_model_ids") or []))
        )
        & relevant_model_ids
    ]
    accepted_frames = [
        frame
        for frame in relevant_frames
        if frame.get("status") == "accepted"
    ]
    frame_projected_edge_kinds = {
        str(edge_kind)
        for frame in accepted_frames
        for edge_kind in frame.get("projected_edge_kinds") or []
        if edge_kind
    }
    frame_kind_hits = sorted(
        {
            str(frame.get("relation_kind"))
            for frame in accepted_frames
            if frame.get("relation_kind")
        }
    )
    expected_edge_kinds = {
        relation
        for relation in spec.expected_relationships
        if relation != "needs_review"
    }
    structural_edge_kinds = relevant_edge_kinds | frame_projected_edge_kinds
    edge_kind_hits = sorted(expected_edge_kinds & structural_edge_kinds)
    missing_edge_kinds = sorted(expected_edge_kinds - structural_edge_kinds)
    review_candidates = [
        candidate
        for candidate in candidates
        if set(map(str, candidate.get("member_model_ids") or [])) & relevant_model_ids
    ]
    accepted = sum(
        1
        for candidate in review_candidates
        if candidate.get("review_status") == "accepted"
    )
    needs_review = sum(
        1
        for candidate in review_candidates
        if candidate.get("review_status") == "needs_review"
    )
    edge_kind_score = _ratio(len(edge_kind_hits), len(expected_edge_kinds))
    relation_frame_projection_count = sum(
        int(frame.get("active_projection_count") or 0)
        for frame in relevant_frames
    )
    edge_presence_score = (
        1.0 if scoped_edge_count or relevant_frames else 0.0
    )
    return _EdgeReviewScoring(
        scoped_edge_count=scoped_edge_count,
        edge_kind_hits=edge_kind_hits,
        missing_edge_kinds=missing_edge_kinds,
        relation_frame_count=len(relevant_frames),
        accepted_relation_frame_count=len(accepted_frames),
        relation_frame_kind_hits=frame_kind_hits,
        relation_frame_projection_count=relation_frame_projection_count,
        review_candidate_count=len(review_candidates),
        accepted_count=accepted,
        needs_review_count=needs_review,
        edge_score=0.35 * edge_presence_score + 0.65 * edge_kind_score,
    )


def _storyline_score_notes(
    *,
    spec: StorylineSpec,
    latent: _LatentPatternScoring,
    situation_count: int,
    recommendation_count: int,
    edge_review: _EdgeReviewScoring,
    bridge: _BridgeScoring,
) -> list[str]:
    notes: list[str] = []
    if not latent.models:
        notes.append("No concrete model captured enough hidden-pattern facets.")
    elif not latent.evidence_supported_models:
        notes.append("Latent-pattern model was not backed by storyline evidence.")
    if not situation_count:
        notes.append("No composite/situation model detected for storyline.")
    if not recommendation_count:
        notes.append("No recommendation/action model detected for storyline.")
    if edge_review.missing_edge_kinds:
        notes.append(
            "Missing expected accepted edge kinds: "
            + ", ".join(edge_review.missing_edge_kinds)
        )
    if (
        edge_review.needs_review_count > edge_review.accepted_count * 3
        and edge_review.needs_review_count >= 5
    ):
        notes.append("Review debt dominates accepted relationship candidates.")
    if spec.id == _LATENT_BRIDGE_STORYLINE_ID:
        if bridge.model_count == 0:
            notes.append(
                "No bounded inferred bridge model detected for unobserved transition."
            )
        elif bridge.transition_supported_count == 0:
            notes.append(
                "Bridge model was not supported by both before and after/gap states."
            )
        if bridge.unsupported_specific_claim_count:
            notes.append(
                "Bridge model invented specific off-sensor details before validation."
            )
    return notes


async def _maybe_judge_storyline_thesis(
    thesis_judge: Any | None,
    *,
    tenant_id: UUID,
    spec: StorylineSpec,
    relevant_models: list[dict[str, Any]],
    should_judge: bool,
) -> _ThesisJudgeScoring:
    if thesis_judge is None or not should_judge:
        return _ThesisJudgeScoring()
    judge_result = await _judge_storyline_thesis(
        thesis_judge,
        tenant_id=tenant_id,
        spec=spec,
        relevant_models=relevant_models,
    )
    return _ThesisJudgeScoring(
        score=round(judge_result.score, 4),
        correct=bool(judge_result.correct),
        rationale=judge_result.rationale,
        metadata=dict(judge_result.metadata),
        judged=True,
    )


async def _score_one_storyline(
    *,
    tenant_id: UUID,
    scenario: Scenario,
    spec: StorylineSpec,
    data: _StorylineScoringData,
    observation_index: _StoryObservationIndex,
    thesis_judge: Any | None,
    should_judge_thesis: bool,
) -> tuple[StorylineScore, bool]:
    scope_refs = _scope_refs_for_story(scenario, spec)
    story_observations = observation_index.observations_by_story.get(spec.id, set())
    future_observations = observation_index.future_observations_by_story.get(
        spec.id,
        set(),
    )
    relevant_models = [
        model
        for model in data.models
        if _model_matches_story(model, scope_refs, story_observations)
    ]
    relevant_model_ids = {str(model["id"]) for model in relevant_models}
    text_blob = "\n".join(_model_text(model) for model in relevant_models).lower()
    keyword_hits = [term for term in spec.expected_terms if term.lower() in text_blob]
    missing_keywords = [
        term for term in spec.expected_terms if term.lower() not in text_blob
    ]
    evidence_supported = [
        model
        for model in relevant_models
        if set(map(str, model.get("supporting_event_ids") or [])) & story_observations
    ]
    situation_count = sum(1 for model in relevant_models if _is_situation_model(model))
    recommendation_count = sum(
        1 for model in relevant_models if _is_recommendation_model(model)
    )
    latent = _score_latent_patterns(
        spec=spec,
        relevant_models=relevant_models,
        story_observations=story_observations,
    )
    bridge = _score_latent_bridge(
        spec=spec,
        relevant_models=relevant_models,
        future_observations=future_observations,
        transition_phase_by_observation=(
            observation_index.transition_phase_by_observation
        ),
    )
    edge_review = _score_storyline_edges_and_review(
        spec=spec,
        edges=data.edges,
        relation_frames=data.relation_frames,
        candidates=data.candidates,
        relevant_model_ids=relevant_model_ids,
    )
    keyword_score = (
        len(keyword_hits) / len(spec.expected_terms) if spec.expected_terms else 0.0
    )
    evidence_score = min(1.0, len(evidence_supported) / 3.0)
    review_penalty = min(0.25, edge_review.needs_review_count / 40.0)
    score = max(
        0.0,
        (
            0.25 * latent.score
            + 0.25 * keyword_score
            + 0.15 * evidence_score
            + 0.15 * (1.0 if situation_count else 0.0)
            + 0.10 * (1.0 if recommendation_count else 0.0)
            + 0.10 * edge_review.edge_score
            - review_penalty
        ),
    )
    if spec.id == _LATENT_BRIDGE_STORYLINE_ID:
        score = max(score, bridge.score)
    calibration_samples = _storyline_calibration_samples(
        spec=spec,
        relevant_models=relevant_models,
        story_observations=story_observations,
        future_observations=future_observations,
    )
    thesis = await _maybe_judge_storyline_thesis(
        thesis_judge,
        tenant_id=tenant_id,
        spec=spec,
        relevant_models=relevant_models,
        should_judge=should_judge_thesis,
    )
    return (
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
            scoped_edge_count=edge_review.scoped_edge_count,
            edge_kind_hits=edge_review.edge_kind_hits,
            missing_edge_kinds=edge_review.missing_edge_kinds,
            relation_frame_count=edge_review.relation_frame_count,
            accepted_relation_frame_count=edge_review.accepted_relation_frame_count,
            relation_frame_kind_hits=edge_review.relation_frame_kind_hits,
            relation_frame_projection_count=(
                edge_review.relation_frame_projection_count
            ),
            review_candidate_count=edge_review.review_candidate_count,
            accepted_candidate_count=edge_review.accepted_count,
            needs_review_candidate_count=edge_review.needs_review_count,
            latent_pattern_score=round(latent.score, 4),
            latent_pattern_model_count=len(latent.models),
            latent_pattern_evidence_supported_model_count=(
                len(latent.evidence_supported_models)
            ),
            latent_pattern_best_coverage=round(
                float(latent.best_assessment["coverage"]),
                4,
            ),
            latent_pattern_group_hits=list(latent.best_assessment["hits"]),
            missing_latent_pattern_groups=list(latent.best_assessment["missing"]),
            latent_pattern_model_ids=[str(model["id"]) for model in latent.models[:5]],
            score=round(score, 4),
            inferred_bridge_model_count=bridge.model_count,
            inferred_bridge_transition_supported_model_count=(
                bridge.transition_supported_count
            ),
            inferred_bridge_future_confirmed_model_count=bridge.future_confirmed_count,
            unsupported_bridge_specific_claim_count=(
                bridge.unsupported_specific_claim_count
            ),
            bridge_epistemic_marker_hits=sorted(bridge.epistemic_marker_hits),
            bridge_forbidden_detail_hits=sorted(bridge.forbidden_detail_hits),
            thesis_judge_score=thesis.score,
            thesis_judge_correct=thesis.correct,
            thesis_judge_rationale=thesis.rationale,
            thesis_judge_metadata=thesis.metadata,
            calibration_samples=calibration_samples,
            notes=_storyline_score_notes(
                spec=spec,
                latent=latent,
                situation_count=situation_count,
                recommendation_count=recommendation_count,
                edge_review=edge_review,
                bridge=bridge,
            ),
        ),
        thesis.judged,
    )


async def score_storylines(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    scenario: Scenario,
    gold_specs: tuple[StorylineSpec, ...],
    enable_thesis_judge: bool = False,
    thesis_judge_limit: int = 0,
) -> list[StorylineScore]:
    data = await _fetch_storyline_scoring_data(pool, tenant_id=tenant_id)
    observation_index = _index_storyline_observations(data.observations)
    thesis_judge: Any | None = None
    if enable_thesis_judge:
        from benchmarks.fyralis_eval.judge import LLMAnswerJudge

        thesis_judge = LLMAnswerJudge(name=_THESIS_JUDGE_NAME)
    thesis_judged = 0
    scores: list[StorylineScore] = []
    for spec in gold_specs:
        score, judged = await _score_one_storyline(
            tenant_id=tenant_id,
            scenario=scenario,
            spec=spec,
            data=data,
            observation_index=observation_index,
            thesis_judge=thesis_judge,
            should_judge_thesis=(
                thesis_judge_limit <= 0 or thesis_judged < thesis_judge_limit
            ),
        )
        scores.append(score)
        if judged:
            thesis_judged += 1
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
    # DB return order is not semantic. Rank complete, evidenced explanatory
    # Models ahead of incidental scoped facts so the fixed excerpt budget
    # measures whether a causal thesis exists, not insertion-order luck.
    ranked = sorted(
        relevant_models,
        key=_thesis_model_priority,
        reverse=True,
    )
    excerpts: list[str] = []
    for index, model in enumerate(ranked[:max_models], start=1):
        text = " ".join(_model_text(model).split())
        if not text:
            continue
        excerpts.append(f"[{index}] {text[:max_chars_per_model]}")
    return "\n".join(excerpts) or "No relevant models were recovered."


def _thesis_model_priority(model: dict[str, Any]) -> tuple[float, int, float]:
    proposition = _json_obj(model.get("proposition"))
    role = str(proposition.get("claim_role") or "")
    text = _model_text(model).lower()
    role_score = {
        "situation": 5.0,
        "hypothesis": 4.5,
        "pattern": 4.0,
        "relation": 3.5,
        "concern": 2.0,
        "prediction": 1.5,
        "fact": 1.0,
    }.get(role, 0.5)
    causal_markers = (
        "because",
        "caused by",
        "due to",
        "driven by",
        "explains",
        "mechanism",
        "rather than",
        "not ",
        "but ",
        "therefore",
        "leading to",
        "results in",
        "shared_mechanism",
        "falsif",
    )
    causal_score = min(
        3.0,
        0.5 * sum(marker in text for marker in causal_markers),
    )
    support_count = len(set(map(str, model.get("supporting_event_ids") or [])))
    evidence_score = min(2.0, support_count / 2.0)
    confidence = _coerce_confidence(model.get("confidence")) or 0.0
    return role_score + causal_score + evidence_score, support_count, confidence


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
            term
            for term in spec.expected_terms
            if term.lower() in _model_text(model).lower()
        ]
        future_touched = bool(support_ids & future_observations)
        outcome = (
            1.0
            if (
                future_touched
                or (
                    float(assessment["coverage"]) >= 0.6
                    and len(keyword_hits) >= max(1, min(2, len(spec.expected_terms)))
                )
            )
            else 0.0
        )
        if spec.id == _LATENT_BRIDGE_STORYLINE_ID:
            bridge = _latent_bridge_assessment(model)
            if bridge["forbidden_detail_hits"] and not future_touched:
                outcome = 0.0
        samples.append(
            {
                "storyline_id": spec.id,
                "model_id": str(model.get("id")),
                "confidence": round(confidence, 4),
                "outcome": outcome,
                "future_touched": future_touched,
                "basis": "future_validation_wave_proxy",
            }
        )
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
    return proposition.get("claim_role") == "recommendation" or any(
        term in text for term in ("recommend", "owner", "escalate", "allocate")
    )


def _is_concern_model(model: dict[str, Any]) -> bool:
    proposition = _json_obj(model.get("proposition"))
    text = _model_text(model).lower()
    return proposition.get("claim_role") in {"concern", "risk"} or any(
        term in text for term in ("risk", "concern", "blocker", "tradeoff")
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
        if spec.latent_pattern_groups
        else 0.0
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
        label for label, terms in groups.items() if any(term in text for term in terms)
    ]
    epistemic_hits = [term for term in groups["epistemic_gap"] if term in text]
    forbidden_detail_terms = (
        "hallway",
        "verbal approval",
        "pat ",
        "lena ",
        "sponsor confirmed",
    )
    forbidden_detail_hits = [
        term.strip() for term in forbidden_detail_terms if term in text
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
            "brier_score": None,
            "signed_calibration_bias": None,
            "mean_confidence": None,
            "empirical_accuracy": None,
            "overconfident_sample_rate": None,
            "mean_overconfidence_exposure": None,
            "high_confidence_error_rate": None,
            "selective_accuracy_at_0_7": None,
            "selective_coverage_at_0_7": None,
            "bins": [],
            "note": (
                "No future-validation-backed calibration samples were available "
                "for this run."
            ),
        }
    bins: list[dict[str, Any]] = []
    total = len(samples)
    ece = 0.0
    confidences = [float(sample["confidence"]) for sample in samples]
    outcomes = [float(sample["outcome"]) for sample in samples]
    for bin_index in range(bin_count):
        low = bin_index / bin_count
        high = (bin_index + 1) / bin_count
        if bin_index == bin_count - 1:
            bucket = [
                sample
                for sample in samples
                if low <= float(sample["confidence"]) <= high
            ]
        else:
            bucket = [
                sample
                for sample in samples
                if low <= float(sample["confidence"]) < high
            ]
        if not bucket:
            bins.append(
                {
                    "low": round(low, 4),
                    "high": round(high, 4),
                    "n": 0,
                    "accuracy": None,
                    "avg_confidence": None,
                    "gap": None,
                }
            )
            continue
        avg_conf = sum(float(sample["confidence"]) for sample in bucket) / len(bucket)
        accuracy = sum(float(sample["outcome"]) for sample in bucket) / len(bucket)
        gap = abs(accuracy - avg_conf)
        ece += (len(bucket) / total) * gap
        bins.append(
            {
                "low": round(low, 4),
                "high": round(high, 4),
                "n": len(bucket),
                "accuracy": round(accuracy, 4),
                "avg_confidence": round(avg_conf, 4),
                "gap": round(gap, 4),
            }
        )
    mean_confidence = sum(confidences) / total
    empirical_accuracy = sum(outcomes) / total
    brier = sum(
        (confidence - outcome) ** 2
        for confidence, outcome in zip(confidences, outcomes, strict=True)
    ) / total
    positive_calibration_exposure = sum(
        (bucket["n"] / total)
        * max(0.0, bucket["avg_confidence"] - bucket["accuracy"])
        for bucket in bins
        if bucket["n"]
    )
    materially_overconfident_population = sum(
        bucket["n"]
        for bucket in bins
        if bucket["n"]
        and bucket["avg_confidence"] - bucket["accuracy"] >= 0.2
    )
    high_confidence = [
        (confidence, outcome)
        for confidence, outcome in zip(confidences, outcomes, strict=True)
        if confidence >= 0.7
    ]
    return {
        "source": "storyline_future_validation_proxy",
        "n": total,
        "bin_count": bin_count,
        "expected_calibration_error": round(ece, 4),
        "brier_score": round(brier, 4),
        "signed_calibration_bias": round(mean_confidence - empirical_accuracy, 4),
        "mean_confidence": round(mean_confidence, 4),
        "empirical_accuracy": round(empirical_accuracy, 4),
        "overconfident_sample_rate": round(
            materially_overconfident_population / total,
            4,
        ),
        "mean_overconfidence_exposure": round(
            positive_calibration_exposure,
            4,
        ),
        "high_confidence_error_rate": (
            round(
                sum(1.0 - outcome for _, outcome in high_confidence)
                / len(high_confidence),
                4,
            )
            if high_confidence
            else None
        ),
        "selective_accuracy_at_0_7": (
            round(
                sum(outcome for _, outcome in high_confidence)
                / len(high_confidence),
                4,
            )
            if high_confidence
            else None
        ),
        "selective_coverage_at_0_7": round(len(high_confidence) / total, 4),
        "positive_outcomes": int(sum(float(sample["outcome"]) for sample in samples)),
        "negative_outcomes": int(
            total - sum(float(sample["outcome"]) for sample in samples)
        ),
        "bins": bins,
        "note": (
            "ECE is computed only over Models supported by pre-validation "
            "storyline evidence, then checked against the run's "
            "future-validation waves. It is a benchmark proxy, not a production "
            "resolution-outcome audit. Signed bias distinguishes overconfidence "
            "(positive) from underconfidence (negative); Brier score preserves "
            "per-claim error magnitude. Overconfident sample rate counts claims "
            "that fall in calibration bins whose positive gap is at least 0.20, "
            "while mean overconfidence exposure is the population-weighted "
            "positive bin gap. Selective metrics "
            "expose whether high-confidence claims are actually safer to trust."
        ),
    }


def _memory_truth_dimension(
    *,
    latent_avg: float,
    concrete_latent_ratio: float,
    evidence_avg: float,
    accepted_edge_coverage: float,
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.55 * latent_avg
                + 0.20 * concrete_latent_ratio
                + 0.15 * evidence_avg
                + 0.10 * accepted_edge_coverage,
            ]
        ),
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
    )


def _compression_dimension(
    *,
    compression_growth_score: float,
    compression_update_score: float,
    duplicate_penalty: float,
    model_inserts: int,
    model_updates: int,
    durable_growth_per_signal: float,
    update_share: float,
    duplicate_group_count: int,
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.50 * compression_growth_score
                + 0.25 * compression_update_score
                + 0.25 * (1.0 - duplicate_penalty),
            ]
        ),
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
    )


def _retrieval_usefulness_dimension(
    *,
    context_use_score: float,
    model_context_score: float,
    historical_observation_leakage_score: float,
    retrieved_model_score: float,
    graph_relation_contract_score: float,
    graph_selected_runs: int,
    graph_relation_contract_failed_runs: int,
    avg_retrieved_models: float,
    avg_retrieved_observations: float,
    avg_trigger_observations: float,
    avg_historical_observations: float,
    accounted_selected_context_count: int,
    unused_context: int,
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.30 * context_use_score
                + 0.25 * model_context_score
                + 0.20 * historical_observation_leakage_score
                + 0.15 * retrieved_model_score
                + 0.10 * graph_relation_contract_score,
            ]
        ),
        metrics={
            "context_use_score": context_use_score,
            "model_or_graph_context_use_score": model_context_score,
            "graph_relation_contract_score": graph_relation_contract_score,
            "graph_selected_runs": graph_selected_runs,
            "graph_relation_contract_failed_runs": graph_relation_contract_failed_runs,
            "avg_models_per_t1_batch": avg_retrieved_models,
            "avg_observations_per_t1_batch": avg_retrieved_observations,
            "avg_trigger_observations_per_t1_batch": avg_trigger_observations,
            "avg_historical_observations_per_t1_batch": avg_historical_observations,
            "retrieval_budget_fit_score": retrieved_model_score,
            "accounted_selected_context_count": accounted_selected_context_count,
            "unused_selected_context_count": unused_context,
        },
        findings=[
            "Rewards selected context that is actually referenced by reasoning.",
            "Penalizes falling back to raw observations as the dominant context.",
        ],
    )


def _reasoning_value_dimension(
    *,
    situation_coverage: float,
    recommendation_coverage: float,
    useful_write_score: float,
    useful_writes_per_storyline: float,
    review_debt_score: float,
    review_debt_per_signal: float,
    validation_score: float,
    validation_errors: int,
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.30 * situation_coverage
                + 0.25 * recommendation_coverage
                + 0.20 * useful_write_score
                + 0.15 * review_debt_score
                + 0.10 * validation_score,
            ]
        ),
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
    )


def _edge_intelligence_dimension(
    *,
    accepted_edge_coverage: float,
    precise_edge_coverage: float,
    relation_frame_score: float,
    storyline_edge_kind_coverage: float,
    storyline_edge_presence: float,
    accepted_candidate_ratio: float,
    edge_lifecycle_score: float,
    graph_relation_contract_score: float,
    ontology_gap_discipline_score: float,
    generic_overuse_penalty: float,
    graph_selected_runs: int,
    graph_relation_op_runs: int,
    graph_no_edge_rationale_runs: int,
    graph_relation_contract_failed_runs: int,
    edge_ops_stats: dict[str, Any],
    edge_reconfirmation_events: float,
    retired_or_inert_edges: float,
    generic_support_share: float,
    ontology_gap_ops: int,
    ontology_proposals: int,
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.18 * accepted_edge_coverage
                + 0.16 * precise_edge_coverage
                + 0.10 * relation_frame_score
                + 0.11 * storyline_edge_kind_coverage
                + 0.08 * storyline_edge_presence
                + 0.09 * accepted_candidate_ratio
                + 0.09 * edge_lifecycle_score
                + 0.09 * graph_relation_contract_score
                + 0.05 * ontology_gap_discipline_score
                + 0.05 * (1.0 - generic_overuse_penalty),
            ]
        ),
        metrics={
            "required_registered_edge_kind_coverage": accepted_edge_coverage,
            "precise_required_edge_kind_coverage": precise_edge_coverage,
            "relation_frame_score": relation_frame_score,
            "storyline_edge_kind_coverage": storyline_edge_kind_coverage,
            "storyline_edge_presence": storyline_edge_presence,
            "accepted_relationship_candidate_ratio": accepted_candidate_ratio,
            "graph_relation_contract_score": graph_relation_contract_score,
            "graph_selected_runs": graph_selected_runs,
            "graph_relation_op_runs": graph_relation_op_runs,
            "graph_no_edge_rationale_runs": graph_no_edge_rationale_runs,
            "graph_relation_contract_failed_runs": graph_relation_contract_failed_runs,
            "edge_add_ops": int(edge_ops_stats.get("add_ops") or 0),
            "edge_retire_ops": int(edge_ops_stats.get("retire_ops") or 0),
            "future_validation_edge_ops": int(
                edge_ops_stats.get("future_edge_ops") or 0
            ),
            "relation_frame_ops": int(edge_ops_stats.get("relation_frame_ops") or 0),
            "accepted_relation_frame_ops": int(
                edge_ops_stats.get("accepted_relation_frame_ops") or 0
            ),
            "relation_frame_projected_edges_from_ops": int(
                edge_ops_stats.get("relation_frame_projected_edges") or 0
            ),
            "future_validation_relation_frame_ops": int(
                edge_ops_stats.get("future_relation_frame_ops") or 0
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
            "Rewards accepted N-ary relation frames when binary edges would lose role semantics.",
        ],
    )


def _temporal_improvement_dimension(
    *,
    temporal_cap: float,
    temporal_evidence_score: float,
    future_validation_events: int,
    future_validation_memory_use_score: float,
    future_stats: dict[str, Any],
    update_share: float,
    ops: dict[str, float],
    topology_metrics: dict[str, Any],
) -> dict[str, Any]:
    return _dimension(
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
    )


def _adaptive_lifecycle_dimension(
    *,
    feedback_learning_score: float,
    policy_feedback_score: float,
    negative_learning_score: float,
    relation_adaptation_score: float,
    temporal_closure_score: float,
    autonomous_maintenance_score: float,
    efficiency_guardrail_score: float,
    adaptive_feedback_events: float,
    question_policy_updates: float,
    negative_learning_events: float,
    future_relation_evolution_events: float,
    experience_loop_closed: float,
    experience_closure_score: float,
    experience_policy_effects: float,
    experience_evaluation_events: float,
    experience_future_behavior_levers: float,
    experience_direct_policy_effects: float,
    post_commit_processed: int,
    post_commit_dead_lettered: int,
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.20 * feedback_learning_score
                + 0.15 * policy_feedback_score
                + 0.10 * negative_learning_score
                + 0.20 * relation_adaptation_score
                + 0.20 * temporal_closure_score
                + 0.10 * autonomous_maintenance_score
                + 0.05 * efficiency_guardrail_score,
            ]
        ),
        metrics={
            "feedback_learning_score": feedback_learning_score,
            "policy_feedback_score": policy_feedback_score,
            "negative_learning_score": negative_learning_score,
            "relation_adaptation_score": relation_adaptation_score,
            "temporal_closure_score": temporal_closure_score,
            "autonomous_maintenance_score": autonomous_maintenance_score,
            "efficiency_guardrail_score": efficiency_guardrail_score,
            "adaptive_feedback_events": adaptive_feedback_events,
            "question_policy_updates": question_policy_updates,
            "negative_learning_events": negative_learning_events,
            "future_relation_evolution_events": future_relation_evolution_events,
            "experience_loop_closed": experience_loop_closed,
            "experience_closure_score": experience_closure_score,
            "experience_policy_effects": experience_policy_effects,
            "experience_evaluation_events": experience_evaluation_events,
            "experience_future_behavior_levers": experience_future_behavior_levers,
            "experience_direct_policy_effects": experience_direct_policy_effects,
            "post_commit_processed": post_commit_processed,
            "post_commit_dead_lettered": post_commit_dead_lettered,
        },
        findings=[
            "Measures closed-loop adaptation across retrieval, reasoning, writes, later validation, and autonomous maintenance.",
            "Scores zero for temporal closure until future validation proves later behavior changed.",
        ],
    )


def _robustness_dimension(
    *,
    wave_success_score: float,
    drain_score: float,
    failure_score: float,
    validation_score: float,
    timeout_score: float,
    noise_score: float,
    topology_integrity_score: float,
    wave_stats: dict[str, Any],
    pending_triggers: int,
    think_failed: int,
    topology_missing_model_skips: float,
    post_commit_status: dict[str, Any],
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.25 * wave_success_score
                + 0.20 * drain_score
                + 0.20 * failure_score
                + 0.15 * validation_score
                + 0.10 * timeout_score
                + 0.05 * noise_score
                + 0.05 * topology_integrity_score,
            ]
        ),
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
    )


def _efficiency_dimension(
    *,
    amplification_score: float,
    llm_call_score: float,
    latency_score: float,
    cost_score: float,
    think_runs_per_signal: float,
    llm_calls_per_signal: float,
    max_t1_elapsed_s: float,
    cost_per_signal: float,
    efficiency_scope: str,
    background_maintenance_runs: int,
    background_maintenance_llm_calls: int,
    background_maintenance_cost_usd: float,
    t4_roi: dict[str, Any],
) -> dict[str, Any]:
    return _dimension(
        score=_avg(
            [
                0.35 * amplification_score
                + 0.25 * llm_call_score
                + 0.25 * latency_score
                + 0.15 * cost_score,
            ]
        ),
        metrics={
            "think_runs_per_signal": think_runs_per_signal,
            "llm_calls_per_signal": llm_calls_per_signal,
            "max_t1_elapsed_s": max_t1_elapsed_s,
            "cost_per_signal_usd": cost_per_signal,
            "efficiency_scope": efficiency_scope,
            "background_maintenance_think_runs": background_maintenance_runs,
            "background_maintenance_llm_calls": background_maintenance_llm_calls,
            "background_maintenance_cost_usd": background_maintenance_cost_usd,
            "t4_roi": t4_roi,
        },
        findings=[
            "Rewards calm processing: low trigger amplification, low calls, bounded latency.",
            "Product-path efficiency excludes T4 background maintenance; maintenance overhead is reported separately.",
        ],
    )


def _company_scorecard_base_metrics(
    *,
    model_summary: dict[str, Any],
    waves: list[dict[str, Any]],
    retrieval_observation_counts: list[int],
) -> dict[str, Any]:
    think_success = int(model_summary.get("think_runs_success") or 0)
    think_failed = int(model_summary.get("think_runs_failed") or 0)
    return {
        "total_signals": int(model_summary.get("signal_count") or 0),
        "think_success": think_success,
        "think_failed": think_failed,
        "think_runs": think_success + think_failed,
        "pending_triggers": int(model_summary.get("pending_triggers") or 0),
        "ops": _aggregate_wave_ops(waves),
        "wave_stats": _wave_stats(waves),
        "context_stats": _retrieval_context_stats(
            waves,
            retrieval_observation_counts=retrieval_observation_counts,
        ),
        "future_stats": _future_validation_stats(waves),
        "capability_probe_counts": _json_obj(
            model_summary.get("capability_probe_counts")
        ),
        "lifecycle_obligation_report": _json_obj(
            model_summary.get("lifecycle_obligation_report")
        ),
        "graph_health": _json_obj(model_summary.get("graph_health")),
        "context_distribution": _json_obj(
            model_summary.get("context_use_distribution")
        ),
        "context_contract": _json_obj(
            model_summary.get("context_use_relation_contract")
        ),
        "relationship_status": _json_obj(
            model_summary.get("relationship_candidate_status_distribution")
        ),
        "model_kind_distribution": _json_obj(
            model_summary.get("model_kind_distribution")
        ),
        "discovery_counts": _json_obj(model_summary.get("discovery_layer_counts")),
        "topology_metrics": _json_obj(
            model_summary.get("topology_optimizer_metric_totals")
        ),
        "edge_lifecycle": _json_obj(model_summary.get("edge_lifecycle")),
        "relation_frame_lifecycle": _json_obj(
            model_summary.get("relation_frame_lifecycle")
        ),
        "edge_ops_stats": (
            _json_obj(model_summary.get("think_edge_ops_stats"))
            or _edge_ops_stats(waves)
        ),
        "post_commit_status": _json_obj(model_summary.get("post_commit_status")),
        "cost": _json_obj(model_summary.get("cost")),
        "think_cost_profile": _json_obj(model_summary.get("think_cost_profile")),
    }


def _company_storyline_edge_metrics(
    *,
    model_summary: dict[str, Any],
    storyline_scores: list[StorylineScore],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    required_edge_kinds = {
        relation
        for story in STORYLINES
        for relation in story.expected_relationships
        if relation != "needs_review"
    }
    edge_lifecycle = metrics["edge_lifecycle"]
    relation_frame_lifecycle = metrics["relation_frame_lifecycle"]
    edge_distribution = _json_obj(model_summary.get("edge_kind_distribution"))
    accepted_edge_distribution = (
        _json_obj(edge_lifecycle.get("accepted_edge_kind_distribution"))
        or edge_distribution
    )
    relation_projection_distribution = _json_obj(
        relation_frame_lifecycle.get("relation_projection_kind_distribution")
    )
    structural_edge_kind_distribution = {
        **accepted_edge_distribution,
        **{
            key: int(accepted_edge_distribution.get(key) or 0) + int(value or 0)
            for key, value in relation_projection_distribution.items()
        },
    }
    precise_required_edge_kinds = required_edge_kinds - {"supports"}
    precise_edge_coverage = _ratio(
        len(precise_required_edge_kinds & set(structural_edge_kind_distribution)),
        len(precise_required_edge_kinds),
    )
    accepted_relationship_candidates = float(
        metrics["relationship_status"].get("accepted") or 0
    )
    relationship_candidate_count = max(
        0,
        int(model_summary.get("relationship_candidates") or 0),
    )
    edge_reconfirmation_events = float(
        edge_lifecycle.get("reconfirmation_events") or 0.0
    )
    retired_or_inert_edges = float(edge_lifecycle.get("retired_or_inert_edges") or 0.0)
    edge_ops_stats = metrics["edge_ops_stats"]
    accepted_relation_frames = float(
        relation_frame_lifecycle.get("accepted_relation_frames") or 0.0
    )
    projectable_relation_frames = float(
        relation_frame_lifecycle.get("projectable_relation_frames") or 0.0
    )
    relation_frame_projections = float(
        relation_frame_lifecycle.get("relation_edge_projections") or 0.0
    )
    relation_frame_score = _clamp01(
        (
            accepted_relation_frames
            + 0.5 * projectable_relation_frames
            + 0.25 * relation_frame_projections
        )
        / max(1.0, float(len(storyline_scores)))
    )
    lifecycle_signal_count = (
        float(edge_ops_stats.get("future_edge_ops") or 0.0)
        + float(edge_ops_stats.get("future_relation_frame_ops") or 0.0)
        + float(edge_ops_stats.get("retire_ops") or 0.0)
        + edge_reconfirmation_events
        + retired_or_inert_edges
    )
    run_edge_distribution = _json_obj(edge_ops_stats.get("edge_kinds_from_ops"))
    support_distribution = run_edge_distribution or edge_distribution
    supports_edges = float(support_distribution.get("supports") or 0.0)
    total_edges = max(1.0, float(sum(support_distribution.values()) or 0.0))
    ontology_gap_ops = int(edge_ops_stats.get("ontology_gap_ops") or 0)
    ontology_proposals = int(edge_lifecycle.get("ontology_proposals") or 0)
    missing_registered_edges = sorted(
        required_edge_kinds - set(structural_edge_kind_distribution)
    )
    ontology_gap_discipline_score = 1.0
    if ontology_gap_ops and missing_registered_edges:
        ontology_gap_discipline_score = 0.6
    if ontology_gap_ops and ontology_proposals == 0:
        ontology_gap_discipline_score = min(ontology_gap_discipline_score, 0.7)
    generic_support_share = supports_edges / total_edges
    return {
        "latent_avg": _avg([score.latent_pattern_score for score in storyline_scores]),
        "concrete_latent_ratio": _ratio(
            sum(
                1
                for score in storyline_scores
                if score.latent_pattern_evidence_supported_model_count > 0
            ),
            len(storyline_scores),
        ),
        "evidence_avg": _avg(
            [
                min(1.0, score.evidence_supported_model_count / 3.0)
                for score in storyline_scores
            ]
        ),
        "required_edge_kinds": required_edge_kinds,
        "edge_distribution": edge_distribution,
        "run_edge_distribution": run_edge_distribution,
        "accepted_edge_distribution": accepted_edge_distribution,
        "structural_edge_kind_distribution": structural_edge_kind_distribution,
        "relation_projection_distribution": relation_projection_distribution,
        "accepted_edge_coverage": _ratio(
            len(required_edge_kinds & set(structural_edge_kind_distribution)),
            len(required_edge_kinds),
        ),
        "precise_edge_coverage": precise_edge_coverage,
        "relation_frame_score": relation_frame_score,
        "accepted_relation_frames": accepted_relation_frames,
        "projectable_relation_frames": projectable_relation_frames,
        "relation_frame_projections": relation_frame_projections,
        "relation_frame_kind_distribution": _json_obj(
            relation_frame_lifecycle.get("relation_frame_kind_distribution")
        ),
        "storyline_edge_kind_coverage": _avg(
            [
                _ratio(
                    len(score.edge_kind_hits),
                    len(score.edge_kind_hits) + len(score.missing_edge_kinds),
                )
                for score in storyline_scores
            ]
        ),
        "storyline_edge_presence": _ratio(
            sum(
                1
                for score in storyline_scores
                if score.scoped_edge_count > 0 or score.relation_frame_count > 0
            ),
            len(storyline_scores),
        ),
        "accepted_candidate_ratio": _ratio(
            accepted_relationship_candidates,
            relationship_candidate_count,
        ),
        "edge_reconfirmation_events": edge_reconfirmation_events,
        "retired_or_inert_edges": retired_or_inert_edges,
        "edge_lifecycle_score": _clamp01(
            lifecycle_signal_count / max(1.0, float(len(storyline_scores)))
        ),
        "generic_support_share": generic_support_share,
        "generic_support_share_basis": (
            "run_generated_edges" if run_edge_distribution else "full_graph"
        ),
        "generic_overuse_penalty": _clamp01((generic_support_share - 0.35) / 0.65)
        * (1.0 - precise_edge_coverage),
        "ontology_gap_ops": ontology_gap_ops,
        "missing_registered_edges": missing_registered_edges,
        "ontology_proposals": ontology_proposals,
        "ontology_gap_discipline_score": ontology_gap_discipline_score,
    }


def _company_memory_context_metrics(
    *,
    retrieval_model_counts: list[int],
    retrieval_observation_counts: list[int],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    ops = metrics["ops"]
    model_inserts = int(ops["model_inserts"])
    model_updates = int(ops["model_updates"])
    durable_growth_per_signal = _ratio(model_inserts, metrics["total_signals"])
    update_share = _ratio(model_updates, model_inserts + model_updates)
    context_distribution = metrics["context_distribution"]
    context_contract = metrics["context_contract"]
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
    graph_selected_runs = int(context_contract.get("graph_selected_runs") or 0)
    graph_relation_contract_satisfied_runs = int(
        context_contract.get("graph_relation_contract_satisfied_runs") or 0
    )
    avg_historical_observations = float(
        metrics["context_stats"].get("avg_historical_observations_per_t1_batch") or 0.0
    )
    avg_retrieved_models = _avg(retrieval_model_counts)
    retrieval_budget_fit_score = _retrieval_context_budget_fit_score(
        avg_retrieved_models
    )
    return {
        "model_inserts": model_inserts,
        "model_updates": model_updates,
        "durable_growth_per_signal": durable_growth_per_signal,
        "update_share": update_share,
        "compression_growth_score": 1.0
        - _clamp01((durable_growth_per_signal - 0.25) / 0.75),
        "compression_update_score": _clamp01(update_share / 0.20),
        "duplicate_group_count": int(
            metrics["graph_health"].get("exact_duplicate_natural_groups") or 0
        ),
        "context_use_score": _ratio(useful_context, context_total),
        "graph_selected_runs": graph_selected_runs,
        "graph_relation_op_runs": int(
            context_contract.get("graph_relation_op_runs") or 0
        ),
        "graph_no_edge_rationale_runs": int(
            context_contract.get("graph_no_edge_rationale_runs") or 0
        ),
        "graph_relation_contract_score": (
            _ratio(graph_relation_contract_satisfied_runs, graph_selected_runs)
            if graph_selected_runs
            else 1.0
        ),
        "graph_relation_contract_failed_runs": int(
            context_contract.get("graph_relation_contract_failed_runs") or 0
        ),
        "model_context_score": _ratio(
            int(context_distribution.get("graph_context_used") or 0)
            + int(context_distribution.get("model_context_used") or 0),
            context_total,
        ),
        "avg_retrieved_models": avg_retrieved_models,
        "avg_retrieved_observations": _avg(retrieval_observation_counts),
        "avg_historical_observations": avg_historical_observations,
        "historical_observation_leakage_score": 1.0
        - _clamp01(max(0.0, avg_historical_observations - 4.0) / 12.0),
        "retrieved_model_score": retrieval_budget_fit_score,
        "retrieval_budget_fit_score": retrieval_budget_fit_score,
        "unused_context": unused_context,
    }


def _combined_sage_experience_metrics(
    *,
    topology_metrics: dict[str, Any],
    ops: dict[str, float],
) -> dict[str, float]:
    def metric(source: dict[str, Any], key: str) -> float:
        try:
            return float(source.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    think_negative_memory_inserts = metric(ops, "negative_memory_inserts")
    think_question_policy_updates = metric(ops, "question_policy_updates")
    direct_policy_effects = (
        think_negative_memory_inserts + think_question_policy_updates
    )
    direct_evaluation_events = direct_policy_effects

    raw_topology_effects = {
        "affordance_policy": metric(topology_metrics, "affordance_reinforces")
        + metric(topology_metrics, "affordance_decays"),
        "shortcut_policy": metric(topology_metrics, "shortcut_creates_or_bumps")
        + metric(topology_metrics, "shortcut_decays"),
        "negative_memory": metric(topology_metrics, "negative_memory_inserts"),
        "question_policy": metric(topology_metrics, "question_policy_updates"),
        "region_summaries": metric(topology_metrics, "region_refreshes"),
        "structural_features": metric(topology_metrics, "structural_models_written")
        + metric(topology_metrics, "structural_edges_written"),
    }
    if think_negative_memory_inserts > 0:
        raw_topology_effects["negative_memory"] += think_negative_memory_inserts
    if think_question_policy_updates > 0:
        raw_topology_effects["question_policy"] += think_question_policy_updates

    derived_policy_effects = sum(raw_topology_effects.values())
    topology_policy_effects = metric(topology_metrics, "experience_policy_effects")
    policy_effects = max(topology_policy_effects, derived_policy_effects)
    evaluation_events = max(
        metric(topology_metrics, "experience_evaluation_events"),
        direct_evaluation_events,
    )
    outcome_events = max(
        metric(topology_metrics, "experience_outcome_events"),
        evaluation_events,
    )
    future_behavior_levers = max(
        metric(topology_metrics, "experience_future_behavior_levers"),
        float(sum(1 for value in raw_topology_effects.values() if value > 0)),
    )
    closure_score = max(
        metric(topology_metrics, "experience_closure_score"),
        (
            (0.20 if outcome_events > 0 else 0.0)
            + (0.30 if evaluation_events > 0 else 0.0)
            + (0.30 if policy_effects > 0 else 0.0)
            + (0.20 if future_behavior_levers > 0 else 0.0)
        ),
    )
    loop_closed = max(
        metric(topology_metrics, "experience_loop_closed"),
        1.0
        if (
            outcome_events > 0
            and evaluation_events > 0
            and policy_effects > 0
            and future_behavior_levers > 0
        )
        else 0.0,
    )
    return {
        "experience_outcome_events": outcome_events,
        "experience_evaluation_events": evaluation_events,
        "experience_policy_effects": policy_effects,
        "experience_future_behavior_levers": future_behavior_levers,
        "experience_closure_score": round(_clamp01(closure_score), 4),
        "experience_loop_closed": loop_closed,
        "experience_direct_policy_effects": direct_policy_effects,
    }


def _company_reasoning_temporal_operational_metrics(
    *,
    model_summary: dict[str, Any],
    storyline_scores: list[StorylineScore],
    validation_errors: int,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    ops = metrics["ops"]
    topology_metrics = metrics["topology_metrics"]
    wave_stats = metrics["wave_stats"]
    future_stats = metrics["future_stats"]
    recommendation_coverage = _ratio(
        sum(1 for score in storyline_scores if score.recommendation_model_count > 0),
        len(storyline_scores),
    )
    situation_coverage = _ratio(
        sum(1 for score in storyline_scores if score.situation_model_count > 0),
        len(storyline_scores),
    )
    useful_write_count = (
        int(ops["claim_ops"])
        + int(ops["memory_lifecycle_ops"])
        + int(ops["relation_claim_ops"])
        + int(ops["relation_frame_ops"])
        + int(ops["edge_ops"])
        + int(ops["act_ops"])
    )
    future_validation_events = int(
        model_summary.get("future_validation_events")
        or future_stats.get("signals")
        or 0
    )
    temporal_proxy_score = _avg(
        [
            _clamp01(
                metrics["model_updates"]
                / max(1, metrics["model_inserts"] + metrics["model_updates"])
                / 0.20
            ),
            _clamp01(
                int(ops["situation_model_updates"]) / max(1, len(storyline_scores))
            ),
            _clamp01(
                float(topology_metrics.get("shortcut_creates_or_bumps") or 0) / 40.0
            ),
            _clamp01(float(topology_metrics.get("affordance_reinforces") or 0) / 40.0),
        ]
    )
    future_validation_memory_use_score = float(
        future_stats.get("model_or_graph_context_use_score") or 0.0
    )
    future_validation_update_score = _clamp01(
        float(future_stats.get("memory_touch_ops") or 0.0)
        / max(1.0, float(future_stats.get("batches") or 0.0))
    )
    future_validation_score = (
        _avg(
            [
                float(future_stats.get("success_rate") or 0.0),
                future_validation_memory_use_score,
                future_validation_update_score,
            ]
        )
        if future_validation_events
        else 0.0
    )
    topology_missing_model_skips = float(
        topology_metrics.get("shortcut_missing_model_skips") or 0.0
    ) + float(topology_metrics.get("structural_missing_model_skips") or 0.0)
    cost_profile = metrics["think_cost_profile"]
    product_cost_profile = _json_obj(cost_profile.get("product_path"))
    background_cost_profile = _json_obj(cost_profile.get("background_maintenance"))
    t4_roi = _json_obj(cost_profile.get("t4_roi"))
    efficiency_scope = str(
        cost_profile.get("efficiency_scope")
        or "all_think_runs_legacy_cost_profile_unavailable"
    )
    efficiency_think_runs = int(
        product_cost_profile.get("runs") or 0
        if product_cost_profile
        else metrics["think_runs"]
    )
    efficiency_llm_calls = int(
        product_cost_profile.get("llm_calls") or 0
        if product_cost_profile
        else metrics["cost"].get("llm_calls") or 0
    )
    efficiency_cost_usd = float(
        product_cost_profile.get("cost_usd") or 0.0
        if product_cost_profile
        else metrics["cost"].get("cost_usd") or 0.0
    )
    think_runs_per_signal = _ratio(efficiency_think_runs, metrics["total_signals"])
    llm_calls_per_signal = _ratio(efficiency_llm_calls, metrics["total_signals"])
    negative_memory_rows = float(metrics["discovery_counts"].get("negative_memory") or 0)
    topology_negative_memory_inserts = float(
        topology_metrics.get("negative_memory_inserts") or 0
    )
    think_negative_memory_inserts = float(ops.get("negative_memory_inserts") or 0)
    negative_memory_write_events = (
        topology_negative_memory_inserts + think_negative_memory_inserts
    )
    negative_learning_events = max(negative_memory_rows, negative_memory_write_events)
    topology_question_policy_updates = float(
        topology_metrics.get("question_policy_updates") or 0
    )
    think_question_policy_updates = float(ops.get("question_policy_updates") or 0)
    question_policy_update_events = (
        topology_question_policy_updates + think_question_policy_updates
    )
    question_policy_stats = float(
        metrics["discovery_counts"].get("question_policy_stats") or 0
    )
    question_policy_events = max(question_policy_stats, question_policy_update_events)
    experience_metrics = _combined_sage_experience_metrics(
        topology_metrics=topology_metrics,
        ops=ops,
    )
    shortcut_events = float(topology_metrics.get("shortcut_creates_or_bumps") or 0)
    affordance_events = float(topology_metrics.get("affordance_reinforces") or 0)
    adaptive_feedback_events = (
        negative_learning_events
        + question_policy_events
        + shortcut_events
        + affordance_events
    )
    feedback_learning_score = _clamp01(
        adaptive_feedback_events / max(4.0, float(metrics["total_signals"]) * 0.10)
    )
    policy_feedback_score = 1.0 if (
        question_policy_update_events > 0 or question_policy_stats > 0
    ) else 0.0
    negative_learning_score = 1.0 if negative_learning_events > 0 else 0.0
    future_relation_evolution_events = float(
        metrics["edge_ops_stats"].get("future_edge_ops") or 0
    ) + float(metrics["edge_ops_stats"].get("future_relation_frame_ops") or 0)
    temporal_closure_score = (
        _avg(
            [
                float(future_stats.get("success_rate") or 0.0),
                future_validation_memory_use_score,
                future_validation_update_score,
                1.0 if future_relation_evolution_events > 0 else 0.0,
            ]
        )
        if future_validation_events
        else 0.0
    )
    relation_adaptation_score = _avg(
        [
            metrics["edge_lifecycle_score"],
            metrics["graph_relation_contract_score"],
            metrics["precise_edge_coverage"],
            metrics["relation_frame_score"],
        ]
    )
    post_commit_status = metrics["post_commit_status"]
    post_commit_processed = int(post_commit_status.get("processed") or 0)
    post_commit_dead_lettered = int(post_commit_status.get("dead_lettered") or 0)
    autonomous_maintenance_score = _avg(
        [
            1.0 if post_commit_processed > 0 else 0.0,
            1.0 if post_commit_dead_lettered == 0 else 0.0,
            0.0 if int(post_commit_status.get("timed_out") or 0) else 1.0,
            _clamp01(float(metrics["relationship_status"].get("accepted") or 0) / 3.0),
        ]
    )
    amplification_score = 1.0 - _clamp01(think_runs_per_signal / 0.20)
    llm_call_score = 1.0 - _clamp01(llm_calls_per_signal / 0.20)
    latency_score = 1.0 - _clamp01((wave_stats["max_t1_elapsed_s"] - 90.0) / 810.0)
    cost_per_signal = _ratio(efficiency_cost_usd, metrics["total_signals"])
    cost_score = 1.0 - _clamp01(cost_per_signal / 0.01)
    efficiency_guardrail_score = _avg(
        [amplification_score, llm_call_score, latency_score, cost_score]
    )
    return {
        "recommendation_coverage": recommendation_coverage,
        "situation_coverage": situation_coverage,
        "useful_writes_per_storyline": _ratio(
            useful_write_count,
            len(storyline_scores),
        ),
        "useful_write_score": _clamp01(
            _ratio(useful_write_count, len(storyline_scores)) / 6.0
        ),
        "review_debt_per_signal": _ratio(
            int(metrics["relationship_status"].get("needs_review") or 0),
            metrics["total_signals"],
        ),
        "review_debt_score": 1.0
        - _clamp01(
            _ratio(
                int(metrics["relationship_status"].get("needs_review") or 0),
                metrics["total_signals"],
            )
            / 0.25
        ),
        "temporal_cap": 1.0 if future_validation_events else 0.55,
        "future_validation_events": future_validation_events,
        "future_validation_memory_use_score": future_validation_memory_use_score,
        "temporal_evidence_score": (
            _avg([0.70 * future_validation_score + 0.30 * temporal_proxy_score])
            if future_validation_events
            else temporal_proxy_score
        ),
        "wave_success_score": _ratio(
            wave_stats["successful_t1_batches"],
            wave_stats["t1_batch_count"],
        ),
        "drain_score": 1.0 if metrics["pending_triggers"] == 0 else 0.0,
        "failure_score": 1.0 - _ratio(metrics["think_failed"], metrics["think_runs"]),
        "validation_score": 1.0 if validation_errors == 0 else 0.0,
        "timeout_score": 1.0 if wave_stats["timeout_like_t1_batches"] == 0 else 0.0,
        "noise_score": _noise_noop_score(metrics["waves"]),
        "topology_missing_model_skips": topology_missing_model_skips,
        "topology_integrity_score": 1.0 - _clamp01(topology_missing_model_skips / 10.0),
        "think_runs_per_signal": think_runs_per_signal,
        "llm_calls_per_signal": llm_calls_per_signal,
        "latency_score": latency_score,
        "amplification_score": amplification_score,
        "llm_call_score": llm_call_score,
        "cost_per_signal": cost_per_signal,
        "cost_score": cost_score,
        "efficiency_scope": efficiency_scope,
        "background_maintenance_runs": int(
            background_cost_profile.get("runs") or 0
        ),
        "background_maintenance_llm_calls": int(
            background_cost_profile.get("llm_calls") or 0
        ),
        "background_maintenance_cost_usd": float(
            background_cost_profile.get("cost_usd") or 0.0
        ),
        "t4_roi": t4_roi,
        "adaptive_feedback_events": adaptive_feedback_events,
        "adaptive_feedback_score": feedback_learning_score,
        "policy_feedback_score": policy_feedback_score,
        "negative_learning_events": negative_learning_events,
        "negative_memory_rows": negative_memory_rows,
        "negative_memory_inserts": negative_memory_write_events,
        "negative_memory_write_events": negative_memory_write_events,
        "think_negative_memory_inserts": think_negative_memory_inserts,
        "topology_negative_memory_inserts": topology_negative_memory_inserts,
        "negative_learning_score": negative_learning_score,
        "question_policy_updates": question_policy_update_events,
        "question_policy_update_events": question_policy_update_events,
        "question_policy_events": question_policy_events,
        "think_question_policy_updates": think_question_policy_updates,
        "topology_question_policy_updates": topology_question_policy_updates,
        **experience_metrics,
        "relation_adaptation_score": relation_adaptation_score,
        "temporal_closure_score": temporal_closure_score,
        "future_relation_evolution_events": future_relation_evolution_events,
        "post_commit_processed": post_commit_processed,
        "post_commit_dead_lettered": post_commit_dead_lettered,
        "autonomous_maintenance_score": autonomous_maintenance_score,
        "efficiency_guardrail_score": efficiency_guardrail_score,
    }


def _company_scorecard_dimensions(
    *,
    metrics: dict[str, Any],
    validation_errors: int,
) -> dict[str, dict[str, Any]]:
    duplicate_penalty = _clamp01(metrics["duplicate_group_count"] / 500.0)
    return {
        "memory_truth": _memory_truth_dimension(
            latent_avg=metrics["latent_avg"],
            concrete_latent_ratio=metrics["concrete_latent_ratio"],
            evidence_avg=metrics["evidence_avg"],
            accepted_edge_coverage=metrics["accepted_edge_coverage"],
        ),
        "compression": _compression_dimension(
            compression_growth_score=metrics["compression_growth_score"],
            compression_update_score=metrics["compression_update_score"],
            duplicate_penalty=duplicate_penalty,
            model_inserts=metrics["model_inserts"],
            model_updates=metrics["model_updates"],
            durable_growth_per_signal=metrics["durable_growth_per_signal"],
            update_share=metrics["update_share"],
            duplicate_group_count=metrics["duplicate_group_count"],
        ),
        "retrieval_usefulness": _retrieval_usefulness_dimension(
            context_use_score=metrics["context_use_score"],
            model_context_score=metrics["model_context_score"],
            historical_observation_leakage_score=metrics[
                "historical_observation_leakage_score"
            ],
            retrieved_model_score=metrics["retrieved_model_score"],
            graph_relation_contract_score=metrics["graph_relation_contract_score"],
            graph_selected_runs=metrics["graph_selected_runs"],
            graph_relation_contract_failed_runs=metrics[
                "graph_relation_contract_failed_runs"
            ],
            avg_retrieved_models=metrics["avg_retrieved_models"],
            avg_retrieved_observations=metrics["avg_retrieved_observations"],
            avg_trigger_observations=float(
                metrics["context_stats"].get("avg_trigger_observations_per_t1_batch")
                or 0.0
            ),
            avg_historical_observations=metrics["avg_historical_observations"],
            accounted_selected_context_count=int(
                metrics["context_distribution"].get("selected_context_accounted") or 0
            ),
            unused_context=metrics["unused_context"],
        ),
        "reasoning_value": _reasoning_value_dimension(
            situation_coverage=metrics["situation_coverage"],
            recommendation_coverage=metrics["recommendation_coverage"],
            useful_write_score=metrics["useful_write_score"],
            useful_writes_per_storyline=metrics["useful_writes_per_storyline"],
            review_debt_score=metrics["review_debt_score"],
            review_debt_per_signal=metrics["review_debt_per_signal"],
            validation_score=metrics["validation_score"],
            validation_errors=validation_errors,
        ),
        "edge_intelligence": _company_edge_intelligence_dimension(metrics),
        "temporal_improvement": _temporal_improvement_dimension(
            temporal_cap=metrics["temporal_cap"],
            temporal_evidence_score=metrics["temporal_evidence_score"],
            future_validation_events=metrics["future_validation_events"],
            future_validation_memory_use_score=metrics[
                "future_validation_memory_use_score"
            ],
            future_stats=metrics["future_stats"],
            update_share=metrics["update_share"],
            ops=metrics["ops"],
            topology_metrics=metrics["topology_metrics"],
        ),
        "adaptive_lifecycle": _adaptive_lifecycle_dimension(
            feedback_learning_score=metrics["adaptive_feedback_score"],
            policy_feedback_score=metrics["policy_feedback_score"],
            negative_learning_score=metrics["negative_learning_score"],
            relation_adaptation_score=metrics["relation_adaptation_score"],
            temporal_closure_score=metrics["temporal_closure_score"],
            autonomous_maintenance_score=metrics["autonomous_maintenance_score"],
            efficiency_guardrail_score=metrics["efficiency_guardrail_score"],
            adaptive_feedback_events=metrics["adaptive_feedback_events"],
            question_policy_updates=metrics["question_policy_update_events"],
            negative_learning_events=metrics["negative_learning_events"],
            future_relation_evolution_events=metrics[
                "future_relation_evolution_events"
            ],
            experience_loop_closed=metrics["experience_loop_closed"],
            experience_closure_score=metrics["experience_closure_score"],
            experience_policy_effects=metrics["experience_policy_effects"],
            experience_evaluation_events=metrics["experience_evaluation_events"],
            experience_future_behavior_levers=metrics[
                "experience_future_behavior_levers"
            ],
            experience_direct_policy_effects=metrics[
                "experience_direct_policy_effects"
            ],
            post_commit_processed=metrics["post_commit_processed"],
            post_commit_dead_lettered=metrics["post_commit_dead_lettered"],
        ),
        "robustness": _robustness_dimension(
            wave_success_score=metrics["wave_success_score"],
            drain_score=metrics["drain_score"],
            failure_score=metrics["failure_score"],
            validation_score=metrics["validation_score"],
            timeout_score=metrics["timeout_score"],
            noise_score=metrics["noise_score"],
            topology_integrity_score=metrics["topology_integrity_score"],
            wave_stats=metrics["wave_stats"],
            pending_triggers=metrics["pending_triggers"],
            think_failed=metrics["think_failed"],
            topology_missing_model_skips=metrics["topology_missing_model_skips"],
            post_commit_status=metrics["post_commit_status"],
        ),
        "efficiency": _efficiency_dimension(
            amplification_score=metrics["amplification_score"],
            llm_call_score=metrics["llm_call_score"],
            latency_score=metrics["latency_score"],
            cost_score=metrics["cost_score"],
            think_runs_per_signal=metrics["think_runs_per_signal"],
            llm_calls_per_signal=metrics["llm_calls_per_signal"],
            max_t1_elapsed_s=metrics["wave_stats"]["max_t1_elapsed_s"],
            cost_per_signal=metrics["cost_per_signal"],
            efficiency_scope=metrics["efficiency_scope"],
            background_maintenance_runs=metrics["background_maintenance_runs"],
            background_maintenance_llm_calls=metrics[
                "background_maintenance_llm_calls"
            ],
            background_maintenance_cost_usd=metrics[
                "background_maintenance_cost_usd"
            ],
            t4_roi=metrics["t4_roi"],
        ),
    }


def _company_edge_intelligence_dimension(
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return _edge_intelligence_dimension(
        accepted_edge_coverage=metrics["accepted_edge_coverage"],
        precise_edge_coverage=metrics["precise_edge_coverage"],
        relation_frame_score=metrics["relation_frame_score"],
        storyline_edge_kind_coverage=metrics["storyline_edge_kind_coverage"],
        storyline_edge_presence=metrics["storyline_edge_presence"],
        accepted_candidate_ratio=metrics["accepted_candidate_ratio"],
        edge_lifecycle_score=metrics["edge_lifecycle_score"],
        graph_relation_contract_score=metrics["graph_relation_contract_score"],
        ontology_gap_discipline_score=metrics["ontology_gap_discipline_score"],
        generic_overuse_penalty=metrics["generic_overuse_penalty"],
        graph_selected_runs=metrics["graph_selected_runs"],
        graph_relation_op_runs=metrics["graph_relation_op_runs"],
        graph_no_edge_rationale_runs=metrics["graph_no_edge_rationale_runs"],
        graph_relation_contract_failed_runs=metrics[
            "graph_relation_contract_failed_runs"
        ],
        edge_ops_stats=metrics["edge_ops_stats"],
        edge_reconfirmation_events=metrics["edge_reconfirmation_events"],
        retired_or_inert_edges=metrics["retired_or_inert_edges"],
        generic_support_share=metrics["generic_support_share"],
        ontology_gap_ops=metrics["ontology_gap_ops"],
        ontology_proposals=metrics["ontology_proposals"],
    )


def _company_scorecard_product_value_evals(
    *,
    model_summary: dict[str, Any],
    storyline_scores: list[StorylineScore],
    dimensions: dict[str, dict[str, Any]],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return _product_value_evals(
        model_summary=model_summary,
        storyline_scores=storyline_scores,
        dimensions=dimensions,
        ops=metrics["ops"],
        graph_health=metrics["graph_health"],
        context_distribution=metrics["context_distribution"],
        model_kind_distribution=metrics["model_kind_distribution"],
        discovery_counts=metrics["discovery_counts"],
        topology_metrics=metrics["topology_metrics"],
        future_stats=metrics["future_stats"],
        edge_lifecycle=metrics["edge_lifecycle"],
        recommendation_coverage=metrics["recommendation_coverage"],
        situation_coverage=metrics["situation_coverage"],
        accepted_edge_coverage=metrics["accepted_edge_coverage"],
        precise_edge_coverage=metrics["precise_edge_coverage"],
        relation_frame_score=metrics["relation_frame_score"],
        latent_avg=metrics["latent_avg"],
        concrete_latent_ratio=metrics["concrete_latent_ratio"],
        evidence_avg=metrics["evidence_avg"],
        update_share=metrics["update_share"],
        durable_growth_per_signal=metrics["durable_growth_per_signal"],
        model_context_score=metrics["model_context_score"],
        context_use_score=metrics["context_use_score"],
        historical_observation_leakage_score=metrics[
            "historical_observation_leakage_score"
        ],
        review_debt_score=metrics["review_debt_score"],
        noise_score=metrics["noise_score"],
    )


def _company_scorecard_weights() -> dict[str, float]:
    return {
        "memory_truth": 0.16,
        "compression": 0.10,
        "retrieval_usefulness": 0.12,
        "reasoning_value": 0.14,
        "edge_intelligence": 0.12,
        "temporal_improvement": 0.12,
        "adaptive_lifecycle": 0.14,
        "robustness": 0.07,
        "efficiency": 0.03,
    }


def _company_scorecard_proof_coverage(
    *,
    storyline_scores: list[StorylineScore],
    metrics: dict[str, Any],
    dimensions: dict[str, dict[str, Any]],
    product_value_evals: dict[str, Any],
) -> dict[str, Any]:
    return {
        "storylines": len(storyline_scores),
        "signals": metrics["total_signals"],
        "t1_batches": metrics["wave_stats"]["t1_batch_count"],
        "successful_t1_batches": metrics["wave_stats"]["successful_t1_batches"],
        "future_validation_events": metrics["future_validation_events"],
        "future_validation_batches": int(metrics["future_stats"].get("batches") or 0),
        "future_validation_success_rate": float(
            metrics["future_stats"].get("success_rate") or 0.0
        ),
        "future_validation_model_or_graph_context_use_score": metrics[
            "future_validation_memory_use_score"
        ],
        "avg_historical_observations_per_t1_batch": metrics[
            "avg_historical_observations"
        ],
        "avg_models_per_t1_batch": metrics["avg_retrieved_models"],
        "retrieval_budget_fit_score": metrics["retrieval_budget_fit_score"],
        "latent_storylines_with_evidence_backed_model": sum(
            1
            for score in storyline_scores
            if score.latent_pattern_evidence_supported_model_count > 0
        ),
        "required_edge_kinds": sorted(metrics["required_edge_kinds"]),
        "accepted_edge_kinds_observed": sorted(
            set(metrics["accepted_edge_distribution"])
        ),
        "structural_edge_kinds_observed": sorted(
            set(metrics["structural_edge_kind_distribution"])
        ),
        "missing_registered_edge_kinds": metrics["missing_registered_edges"],
        "precise_required_edge_kind_coverage": metrics["precise_edge_coverage"],
        "edge_lifecycle": metrics["edge_lifecycle"],
        "relation_frame_lifecycle": metrics["relation_frame_lifecycle"],
        "relation_frame_score": metrics["relation_frame_score"],
        "accepted_relation_frames": metrics["accepted_relation_frames"],
        "relation_frame_projections": metrics["relation_frame_projections"],
        "edge_ops": metrics["edge_ops_stats"],
        "context_use_relation_contract": metrics["context_contract"],
        "prediction_models": int(
            metrics["model_kind_distribution"].get("prediction") or 0
        ),
        "memory_lifecycle_ops": int(metrics["ops"]["memory_lifecycle_ops"]),
        "resource_ops": int(metrics["ops"]["resource_ops"]),
        "ontology_gap_ops": int(metrics["ops"]["ontology_gap_ops"]),
        "negative_memory_inserts": float(metrics["negative_memory_inserts"]),
        "think_negative_memory_inserts": float(
            metrics["think_negative_memory_inserts"]
        ),
        "topology_negative_memory_inserts": float(
            metrics["topology_negative_memory_inserts"]
        ),
        "question_policy_updates": float(metrics["question_policy_updates"]),
        "think_question_policy_updates": float(
            metrics["think_question_policy_updates"]
        ),
        "topology_question_policy_updates": float(
            metrics["topology_question_policy_updates"]
        ),
        "experience_loop_closed": float(metrics["experience_loop_closed"]),
        "experience_closure_score": float(metrics["experience_closure_score"]),
        "experience_policy_effects": float(metrics["experience_policy_effects"]),
        "experience_evaluation_events": float(
            metrics["experience_evaluation_events"]
        ),
        "experience_future_behavior_levers": float(
            metrics["experience_future_behavior_levers"]
        ),
        "experience_direct_policy_effects": float(
            metrics["experience_direct_policy_effects"]
        ),
        "adaptive_lifecycle": dimensions["adaptive_lifecycle"]["metrics"],
        "shortcut_missing_model_skips": float(
            metrics["topology_metrics"].get("shortcut_missing_model_skips") or 0
        ),
        "structural_missing_model_skips": float(
            metrics["topology_metrics"].get("structural_missing_model_skips") or 0
        ),
        "product_value_eval_overall": product_value_evals["overall_score"],
        "product_value_eval_keys": list(_PRODUCT_VALUE_EVAL_KEYS),
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
    metrics = _company_scorecard_base_metrics(
        model_summary=model_summary,
        waves=waves,
        retrieval_observation_counts=retrieval_observation_counts,
    )
    metrics["waves"] = waves
    metrics.update(
        _company_storyline_edge_metrics(
            model_summary=model_summary,
            storyline_scores=storyline_scores,
            metrics=metrics,
        )
    )
    metrics.update(
        _company_memory_context_metrics(
            retrieval_model_counts=retrieval_model_counts,
            retrieval_observation_counts=retrieval_observation_counts,
            metrics=metrics,
        )
    )
    metrics.update(
        _company_reasoning_temporal_operational_metrics(
            model_summary=model_summary,
            storyline_scores=storyline_scores,
            validation_errors=validation_errors,
            metrics=metrics,
        )
    )
    dimensions = _company_scorecard_dimensions(
        metrics=metrics,
        validation_errors=validation_errors,
    )
    product_value_evals = _company_scorecard_product_value_evals(
        model_summary=model_summary,
        storyline_scores=storyline_scores,
        dimensions=dimensions,
        metrics=metrics,
    )
    weights = _company_scorecard_weights()
    overall = round(sum(dimensions[name]["score"] * weight for name, weight in weights.items()), 4)
    proof_gaps = _company_intelligence_proof_gaps(
        model_summary=model_summary,
        dimensions=dimensions,
        wave_stats=metrics["wave_stats"],
        ops=metrics["ops"],
        required_edge_kinds=metrics["required_edge_kinds"],
        edge_distribution=metrics["structural_edge_kind_distribution"],
        model_kind_distribution=metrics["model_kind_distribution"],
        discovery_counts=metrics["discovery_counts"],
        future_stats=metrics["future_stats"],
        edge_intelligence=dimensions["edge_intelligence"],
        capability_probe_counts=metrics["capability_probe_counts"],
    )
    return {
        "overall_score": overall,
        "interpretation": _score_interpretation(overall),
        "dimension_weights": weights,
        "dimensions": dimensions,
        "proof_coverage": _company_scorecard_proof_coverage(
            storyline_scores=storyline_scores,
            metrics=metrics,
            dimensions=dimensions,
            product_value_evals=product_value_evals,
        ),
        "product_value_evals": product_value_evals,
        "proof_gaps": proof_gaps,
    }


def _aggregate_wave_ops(waves: list[dict[str, Any]]) -> dict[str, float]:
    totals: dict[str, float] = {
        "claim_ops": 0,
        "memory_lifecycle_ops": 0,
        "relation_claim_ops": 0,
        "relation_frame_ops": 0,
        "relation_frame_projected_edges": 0,
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
        "negative_memory_inserts": 0,
        "question_policy_updates": 0,
    }
    for wave in waves:
        ops = ((wave.get("t1_batch") or {}).get("run") or {}).get("ops_applied") or {}
        totals["claim_ops"] += len(ops.get("claim_ops") or [])
        totals["memory_lifecycle_ops"] += len(
            ops.get("memory_lifecycle_ops") or []
        )
        relation_frame_ops = ops.get("relation_frame_ops") or []
        totals["relation_claim_ops"] += len(ops.get("relation_claim_ops") or [])
        totals["relation_frame_ops"] += len(relation_frame_ops)
        totals["relation_frame_projected_edges"] += sum(
            int(op.get("projected_edge_count") or 0)
            for op in relation_frame_ops
            if isinstance(op, dict)
        )
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
        totals["negative_memory_inserts"] += float(
            ops.get("negative_memory_inserts")
            or len(ops.get("negative_memory_ops") or [])
            or 0
        )
        totals["question_policy_updates"] += float(
            ops.get("question_policy_updates") or 0
        )
    return totals


def _empty_edge_ops_stats() -> dict[str, Any]:
    return {
        "add_ops": 0,
        "retire_ops": 0,
        "future_edge_ops": 0,
        "accepted_edge_ops": 0,
        "candidate_or_review_edge_ops": 0,
        "relation_frame_ops": 0,
        "accepted_relation_frame_ops": 0,
        "projectable_relation_frame_ops": 0,
        "relation_frame_projected_edges": 0,
        "future_relation_frame_ops": 0,
        "ontology_gap_ops": 0,
        "edge_kinds_from_ops": {},
        "relation_frame_kinds_from_ops": {},
    }


def _accumulate_edge_ops_stats(
    stats: dict[str, Any],
    ops: dict[str, Any],
    *,
    is_future: bool,
) -> None:
    edge_kinds: Counter[str] = Counter()
    edge_kinds.update(_json_obj(stats.get("edge_kinds_from_ops")))
    relation_frame_kinds: Counter[str] = Counter()
    relation_frame_kinds.update(_json_obj(stats.get("relation_frame_kinds_from_ops")))
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
    relation_frame_ops = ops.get("relation_frame_ops") or []
    if not isinstance(relation_frame_ops, list):
        relation_frame_ops = []
    for frame_op in relation_frame_ops:
        if not isinstance(frame_op, dict):
            continue
        stats["relation_frame_ops"] += 1
        if frame_op.get("status") == "accepted":
            stats["accepted_relation_frame_ops"] += 1
        if frame_op.get("write_policy") == "project_edges":
            stats["projectable_relation_frame_ops"] += 1
        if is_future:
            stats["future_relation_frame_ops"] += 1
        try:
            stats["relation_frame_projected_edges"] += int(
                frame_op.get("projected_edge_count") or 0
            )
        except (TypeError, ValueError):
            pass
        relation_kind = str(frame_op.get("relation_kind") or "")
        if relation_kind:
            relation_frame_kinds[relation_kind] += 1
    ontology_gap_ops = ops.get("ontology_gap_ops") or []
    if isinstance(ontology_gap_ops, list):
        stats["ontology_gap_ops"] += len(ontology_gap_ops)
    stats["edge_kinds_from_ops"] = dict(edge_kinds)
    stats["relation_frame_kinds_from_ops"] = dict(relation_frame_kinds)


def _edge_ops_stats(waves: list[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Any] = _empty_edge_ops_stats()
    for wave in waves:
        sequence = str(wave.get("sequence") or "")
        is_future = sequence.startswith("future_validation")
        run = (wave.get("t1_batch") or {}).get("run") or {}
        ops = _json_obj(run.get("ops_applied"))
        _accumulate_edge_ops_stats(stats, ops, is_future=is_future)
    stats["source"] = "t1_wave_runs"
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
        wave
        for wave in waves
        if str(wave.get("sequence") or "").lower() in {"background_noise", "noise"}
        or str(wave.get("sequence") or "").lower().startswith("background_noise")
    ]
    if not noise_waves:
        return 0.5
    scores: list[float] = []
    for wave in noise_waves:
        run = (wave.get("t1_batch") or {}).get("run") or {}
        ops = _json_obj(run.get("ops_applied"))
        state_changes = int(ops.get("state_changes_emitted") or 0)
        mutating_ops = sum(
            len(ops.get(key) or [])
            for key in (
                "claim_ops",
                "memory_lifecycle_ops",
                "relation_claim_ops",
                "relation_frame_ops",
                "edge_ops",
                "act_ops",
                "resource_ops",
                "ontology_gap_ops",
                "new_predictions",
            )
        )
        trace = str(ops.get("reasoning_trace") or "").lower()
        synthesis = json.dumps(
            ops.get("synthesis_decisions") or [],
            sort_keys=True,
            default=str,
        ).lower()
        explicit_noop = any(
            marker in f"{trace}\n{synthesis}"
            for marker in (
                "packet_obligations_skipped:explicit_noop",
                "discard_as_noise",
                "empty diff",
                "no durable diff",
                "no durable write",
            )
        )
        validation_errors = int(run.get("validation_error_count") or 0)
        scores.append(
            1.0
            if (
                state_changes == 0
                and mutating_ops == 0
                and validation_errors == 0
                and explicit_noop
            )
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
                float(context_use.get("selected_historical_observation_count") or 0.0)
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
        wave
        for wave in waves
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
        run = (wave.get("t1_batch") or {}).get("run") or {}
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
            + len(ops.get("memory_lifecycle_ops") or [])
            + len(ops.get("relation_claim_ops") or [])
            + len(ops.get("relation_frame_ops") or [])
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


def _product_value_proof_gaps(
    *,
    recommendation_coverage: float,
    act_ops: int,
    resource_ops: int,
    model_archives: int,
    evidence_attachments: int,
    prediction_models: int,
    future_validation_events: int,
    total_storylines: int,
    alias_score: StorylineScore | None,
    alias_needs_review_count: int,
    alias_review_candidate_count: int,
    noise_score: float,
    bridge_story: StorylineScore | None,
    bridge_model_count: int,
    bridge_transition_supported_count: int,
    bridge_epistemic_marker_count: int,
    unsupported_bridge_specific_claims: int,
    bridge_future_confirmed_count: int,
    latent_avg: float,
    concrete_latent_ratio: float,
    model_context_score: float,
    experience_loop_closed: float,
    experience_closure_score: float,
    experience_future_behavior_levers: float,
    negative_learning_events: int,
    question_policy_events: int,
    customer_scope_count: int,
    customer_scoped_models: int,
    precise_edge_coverage: float,
    relation_frame_score: float,
    capability_probe_counts: dict[str, Any],
    lifecycle_obligation_report: dict[str, Any],
) -> list[str]:
    proof_gaps: list[str] = []
    def probed(kind: str) -> bool:
        try:
            return int(capability_probe_counts.get(kind) or 0) > 0
        except (TypeError, ValueError):
            return False

    lifecycle_opportunities = _json_obj(
        _json_obj(lifecycle_obligation_report.get("opportunities")).get("by_kind")
    )
    lifecycle_injections = _json_obj(
        _json_obj(lifecycle_obligation_report.get("injections")).get("by_kind")
    )
    lifecycle_persisted = _json_obj(
        _json_obj(lifecycle_obligation_report.get("persisted")).get("by_kind")
    )

    def lifecycle_count(bucket: dict[str, Any], kind: str) -> int:
        try:
            return int(bucket.get(kind) or 0)
        except (TypeError, ValueError):
            return 0

    if recommendation_coverage < 0.75 or act_ops == 0:
        proof_gaps.append(
            "Decision impact eval is weak: recommendations/actions did not cover most storylines."
        )
    if resource_ops == 0:
        if probed("resource"):
            proof_gaps.append(
                "Decision impact eval ran a resource/action-resource probe but produced no resource ops."
            )
        elif lifecycle_count(lifecycle_injections, "resource"):
            proof_gaps.append(
                "Decision impact eval detected resource obligations, but no "
                "resource ops persisted; inspect lifecycle_obligation_report "
                "resource conversion."
            )
        elif lifecycle_count(lifecycle_opportunities, "resource"):
            proof_gaps.append(
                "Decision impact eval had explicit resource opportunities, but "
                "Think injected no resource lifecycle obligation."
            )
        else:
            proof_gaps.append(
                "Decision impact eval did not exercise resource or action-resource operations."
            )
    if model_archives == 0:
        if probed("archive"):
            proof_gaps.append(
                "Memory lifecycle eval ran an archive probe but produced no archival cleanup."
            )
        elif lifecycle_count(lifecycle_persisted, "staleness_review"):
            proof_gaps.append(
                "Memory lifecycle eval persisted stale-memory review obligations "
                "but did not resolve them into archival cleanup."
            )
        elif lifecycle_count(lifecycle_opportunities, "staleness_review"):
            proof_gaps.append(
                "Memory lifecycle eval saw stale-memory opportunities, but no "
                "staleness review obligation persisted."
            )
        else:
            proof_gaps.append(
                "Memory lifecycle eval did not exercise archival or stale-memory cleanup."
            )
    if evidence_attachments == 0:
        if probed("evidence_attachment"):
            proof_gaps.append(
                "Memory lifecycle eval ran an evidence probe but produced no evidence attachment."
            )
        elif lifecycle_count(lifecycle_injections, "evidence_attachment"):
            proof_gaps.append(
                "Memory lifecycle eval injected evidence-attachment obligations "
                "but no evidence attachment survived apply."
            )
        elif lifecycle_count(lifecycle_opportunities, "evidence_attachment"):
            proof_gaps.append(
                "Memory lifecycle eval had evidence-attachment opportunities, "
                "but Think injected no evidence obligation."
            )
        else:
            proof_gaps.append(
                "Memory lifecycle eval did not exercise evidence attachment behavior."
            )
    if prediction_models < max(1, total_storylines // 2):
        if probed("prediction"):
            proof_gaps.append(
                "Prediction lifecycle eval ran a prediction probe but still has too few Prediction models for company-scale proof."
            )
        elif lifecycle_count(lifecycle_persisted, "prediction"):
            proof_gaps.append(
                "Prediction lifecycle obligations produced Prediction models, "
                "but volume is still thin for company-scale proof."
            )
        else:
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
    elif (
        alias_score is not None
        and alias_needs_review_count == 0
        and lifecycle_count(lifecycle_persisted, "ambiguity_review")
    ):
        proof_gaps.append(
            "Counterfactual/trap eval persisted ambiguity review obligations, "
            "but storyline review debt still needs closure."
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
    if experience_loop_closed < 1.0:
        proof_gaps.append(
            "SAGE experience metabolism eval did not prove outcomes became future-behavior policy."
        )
    elif experience_closure_score < 0.8 or experience_future_behavior_levers < 2:
        proof_gaps.append(
            "SAGE experience metabolism eval closed a loop, but policy leverage was still thin."
        )
    if negative_learning_events == 0:
        proof_gaps.append(
            "Negative learning eval did not create durable negative memory."
        )
    if question_policy_events == 0:
        if probed("question_policy"):
            proof_gaps.append(
                "Question policy eval ran a probe but did not produce policy stats or updates."
            )
        elif lifecycle_count(lifecycle_persisted, "question_policy"):
            proof_gaps.append(
                "Question policy lifecycle obligations persisted, but SAGE "
                "policy stats/updates did not credit them."
            )
        elif lifecycle_count(lifecycle_opportunities, "question_policy"):
            proof_gaps.append(
                "Question policy opportunities existed, but no policy-learning "
                "obligation persisted."
            )
        else:
            proof_gaps.append(
                "Question policy eval did not exercise question-policy learning."
            )
    if customer_scope_count == 0 and customer_scoped_models == 0:
        proof_gaps.append(
            "Customer value eval did not prove customer-scoped account-health memory."
        )
    if precise_edge_coverage < 0.8 and relation_frame_score < 0.5:
        proof_gaps.append(
            "Customer value eval lacks enough precise edge or relation-frame semantics for high-confidence account health."
        )
    return proof_gaps


def _product_value_future_metrics(
    *,
    model_summary: dict[str, Any],
    future_stats: dict[str, float],
) -> dict[str, Any]:
    future_validation_events = int(
        model_summary.get("future_validation_events")
        or future_stats.get("signals")
        or 0
    )
    future_validation_success_rate = float(future_stats.get("success_rate") or 0.0)
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
    return {
        "future_validation_events": future_validation_events,
        "future_validation_success_rate": future_validation_success_rate,
        "future_validation_memory_touch_ops": future_validation_memory_touch_ops,
        "future_validation_batches": future_validation_batches,
        "future_validation_memory_touch_score": (future_validation_memory_touch_score),
        "future_validation_context_score": future_validation_context_score,
    }


def _product_value_operation_context_metrics(
    *,
    ops: dict[str, float],
    graph_health: dict[str, Any],
    context_distribution: dict[str, Any],
) -> dict[str, Any]:
    act_ops = int(ops["act_ops"])
    memory_lifecycle_ops = int(ops["memory_lifecycle_ops"])
    relation_claim_ops = int(ops["relation_claim_ops"])
    relation_frame_ops = int(ops["relation_frame_ops"])
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
    return {
        "act_ops": act_ops,
        "memory_lifecycle_ops": memory_lifecycle_ops,
        "relation_claim_ops": relation_claim_ops,
        "relation_frame_ops": relation_frame_ops,
        "resource_ops": resource_ops,
        "model_inserts": model_inserts,
        "model_updates": model_updates,
        "model_archives": model_archives,
        "evidence_attachments": evidence_attachments,
        "near_duplicate_absorptions": near_duplicate_absorptions,
        "exact_duplicate_groups": exact_duplicate_groups,
        "useful_context": useful_context,
        "unused_context": unused_context,
        "unused_context_avoidance_score": unused_context_avoidance_score,
    }


def _product_value_learning_scope_metrics(
    *,
    model_summary: dict[str, Any],
    model_kind_distribution: dict[str, Any],
    discovery_counts: dict[str, Any],
    topology_metrics: dict[str, Any],
    ops: dict[str, float],
) -> dict[str, Any]:
    prediction_models = int(model_kind_distribution.get("prediction") or 0)
    negative_memory_count = int(discovery_counts.get("negative_memory") or 0)
    topology_negative_memory_inserts = int(
        topology_metrics.get("negative_memory_inserts") or 0
    )
    think_negative_memory_inserts = int(ops.get("negative_memory_inserts") or 0)
    negative_memory_inserts = (
        topology_negative_memory_inserts + think_negative_memory_inserts
    )
    negative_learning_events = max(negative_memory_count, negative_memory_inserts)
    question_policy_count = int(discovery_counts.get("question_policy_stats") or 0)
    topology_question_policy_updates = int(
        topology_metrics.get("question_policy_updates") or 0
    )
    think_question_policy_updates = int(ops.get("question_policy_updates") or 0)
    question_policy_updates = (
        topology_question_policy_updates + think_question_policy_updates
    )
    question_policy_events = max(question_policy_count, question_policy_updates)
    experience_metrics = _combined_sage_experience_metrics(
        topology_metrics=topology_metrics,
        ops=ops,
    )
    capability_probe_counts = _json_obj(model_summary.get("capability_probe_counts"))

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
    gold_customer_count = len(
        {customer for story in STORYLINES for customer in story.customers}
    )
    customer_scope_coverage = _ratio(customer_scope_count, gold_customer_count)
    return {
        "prediction_models": prediction_models,
        "negative_memory_count": negative_memory_count,
        "negative_memory_inserts": negative_memory_inserts,
        "think_negative_memory_inserts": think_negative_memory_inserts,
        "topology_negative_memory_inserts": topology_negative_memory_inserts,
        "negative_learning_events": negative_learning_events,
        "question_policy_count": question_policy_count,
        "question_policy_updates": question_policy_updates,
        "think_question_policy_updates": think_question_policy_updates,
        "topology_question_policy_updates": topology_question_policy_updates,
        "question_policy_events": question_policy_events,
        "question_policy_probe_count": int(
            capability_probe_counts.get("question_policy") or 0
        ),
        **experience_metrics,
        "experience_policy_effect_score": _clamp01(
            _ratio(
                experience_metrics["experience_policy_effects"],
                max(1, len(STORYLINES)),
            )
        ),
        "experience_evaluation_score": _clamp01(
            _ratio(
                experience_metrics["experience_evaluation_events"],
                max(1, len(STORYLINES)),
            )
        ),
        "experience_future_behavior_score": _clamp01(
            _ratio(experience_metrics["experience_future_behavior_levers"], 4)
        ),
        "customer_scope_rows": customer_scope_rows,
        "customer_scope_count": customer_scope_count,
        "customer_scoped_models_from_rows": customer_scoped_models_from_rows,
        "scope_distribution": scope_distribution,
        "customer_scoped_models": customer_scoped_models,
        "unscoped_models": unscoped_models,
        "customer_scope_share": customer_scope_share,
        "gold_customer_count": gold_customer_count,
        "customer_scope_coverage": customer_scope_coverage,
    }


def _product_value_alias_bridge_metrics(
    storyline_scores: list[StorylineScore],
) -> dict[str, Any]:
    alias_score = next(
        (
            score
            for score in storyline_scores
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
    alias_deferred_candidate_count = max(
        0,
        alias_review_candidate_count - alias_accepted_candidate_count,
    )
    alias_review_deferral_score = (
        _ratio(alias_deferred_candidate_count, alias_review_candidate_count)
        if alias_review_candidate_count
        else (0.5 if alias_score else 0.0)
    )
    alias_strong_acceptance_pressure = _ratio(
        alias_accepted_candidate_count,
        alias_review_candidate_count,
    )
    bridge_story = next(
        (
            score
            for score in storyline_scores
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
        if bridge_story
        else 0
    )
    bridge_future_confirmed_count = (
        int(bridge_story.inferred_bridge_future_confirmed_model_count)
        if bridge_story
        else 0
    )
    unsupported_bridge_specific_claims = (
        int(bridge_story.unsupported_bridge_specific_claim_count) if bridge_story else 0
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
        if bridge_model_count
        else 0.0
    )
    return {
        "alias_score": alias_score,
        "alias_storyline_score": alias_storyline_score,
        "alias_review_candidate_count": alias_review_candidate_count,
        "alias_needs_review_count": alias_needs_review_count,
        "alias_deferred_candidate_count": alias_deferred_candidate_count,
        "alias_accepted_candidate_count": alias_accepted_candidate_count,
        "alias_review_deferral_score": alias_review_deferral_score,
        "alias_strong_acceptance_pressure": alias_strong_acceptance_pressure,
        "bridge_story": bridge_story,
        "bridge_storyline_score": bridge_storyline_score,
        "bridge_model_count": bridge_model_count,
        "bridge_transition_supported_count": bridge_transition_supported_count,
        "bridge_future_confirmed_count": bridge_future_confirmed_count,
        "unsupported_bridge_specific_claims": unsupported_bridge_specific_claims,
        "bridge_epistemic_marker_count": bridge_epistemic_marker_count,
        "bridge_presence_score": bridge_presence_score,
        "bridge_transition_support_score": bridge_transition_support_score,
        "bridge_future_confirmation_score": bridge_future_confirmation_score,
        "bridge_epistemic_score": bridge_epistemic_score,
        "bridge_no_fabrication_score": bridge_no_fabrication_score,
    }


def _product_value_score_metrics(
    *,
    metrics: dict[str, Any],
    dimensions: dict[str, dict[str, Any]],
    edge_lifecycle: dict[str, Any],
    recommendation_coverage: float,
    accepted_edge_coverage: float,
) -> dict[str, Any]:
    edge_lifecycle_events = float(
        edge_lifecycle.get("reconfirmation_events") or 0.0
    ) + float(edge_lifecycle.get("retired_or_inert_edges") or 0.0)

    compression_dimension_score = float(
        (dimensions.get("compression") or {}).get("score") or 0.0
    )
    total_storyline_floor = metrics["total_storyline_floor"]
    decision_action_score = _clamp01(_ratio(metrics["act_ops"], total_storyline_floor))
    decision_resource_score = _clamp01(
        _ratio(metrics["resource_ops"], total_storyline_floor)
    )
    memory_archive_score = _clamp01(
        _ratio(metrics["model_archives"], total_storyline_floor)
    )
    evidence_attachment_score = _clamp01(
        _ratio(metrics["evidence_attachments"], total_storyline_floor)
    )
    duplicate_health_score = 1.0 - _clamp01(metrics["exact_duplicate_groups"] / 500.0)
    duplicate_learning_score = max(
        duplicate_health_score,
        _clamp01(_ratio(metrics["near_duplicate_absorptions"], total_storyline_floor)),
    )
    prediction_model_score = _clamp01(
        _ratio(metrics["prediction_models"], total_storyline_floor)
    )
    prediction_resolution_proxy_score = (
        1.0
        if metrics["prediction_models"] and metrics["future_validation_events"]
        else 0.0
    )
    negative_memory_score = _clamp01(
        _ratio(metrics["negative_learning_events"], total_storyline_floor)
    )
    question_policy_score = _clamp01(
        _ratio(metrics["question_policy_events"], total_storyline_floor)
    )
    customer_account_health_score = _avg(
        [
            metrics["customer_scope_coverage"],
            recommendation_coverage,
            accepted_edge_coverage,
            (
                metrics["future_validation_success_rate"]
                if metrics["future_validation_events"]
                else 0.0
            ),
        ]
    )
    return {
        "edge_lifecycle_events": edge_lifecycle_events,
        "compression_dimension_score": compression_dimension_score,
        "decision_action_score": decision_action_score,
        "decision_resource_score": decision_resource_score,
        "memory_archive_score": memory_archive_score,
        "evidence_attachment_score": evidence_attachment_score,
        "duplicate_health_score": duplicate_health_score,
        "duplicate_learning_score": duplicate_learning_score,
        "prediction_model_score": prediction_model_score,
        "prediction_resolution_proxy_score": prediction_resolution_proxy_score,
        "negative_memory_score": negative_memory_score,
        "question_policy_score": question_policy_score,
        "customer_account_health_score": customer_account_health_score,
    }


def _product_value_metrics(
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
    relation_frame_score: float,
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
    metrics = {
        "total_storylines": len(storyline_scores),
        "capability_probe_counts": _json_obj(
            model_summary.get("capability_probe_counts")
        ),
        "lifecycle_obligation_report": _json_obj(
            model_summary.get("lifecycle_obligation_report")
        ),
        "recommendation_coverage": recommendation_coverage,
        "situation_coverage": situation_coverage,
        "accepted_edge_coverage": accepted_edge_coverage,
        "precise_edge_coverage": precise_edge_coverage,
        "relation_frame_score": relation_frame_score,
        "latent_avg": latent_avg,
        "concrete_latent_ratio": concrete_latent_ratio,
        "evidence_avg": evidence_avg,
        "update_share": update_share,
        "durable_growth_per_signal": durable_growth_per_signal,
        "model_context_score": model_context_score,
        "context_use_score": context_use_score,
        "historical_observation_leakage_score": historical_observation_leakage_score,
        "review_debt_score": review_debt_score,
        "noise_score": noise_score,
    }
    metrics["total_storyline_floor"] = max(1, metrics["total_storylines"])
    metrics.update(
        _product_value_future_metrics(
            model_summary=model_summary,
            future_stats=future_stats,
        )
    )
    metrics.update(
        _product_value_operation_context_metrics(
            ops=ops,
            graph_health=graph_health,
            context_distribution=context_distribution,
        )
    )
    metrics.update(
        _product_value_learning_scope_metrics(
            model_summary=model_summary,
            model_kind_distribution=model_kind_distribution,
            discovery_counts=discovery_counts,
            topology_metrics=topology_metrics,
            ops=ops,
        )
    )
    metrics.update(_product_value_alias_bridge_metrics(storyline_scores))
    metrics.update(
        _product_value_score_metrics(
            metrics=metrics,
            dimensions=dimensions,
            edge_lifecycle=edge_lifecycle,
            recommendation_coverage=recommendation_coverage,
            accepted_edge_coverage=accepted_edge_coverage,
        )
    )
    return {
        key: value
        for key, value in metrics.items()
        if key not in {"customer_scope_rows", "scope_distribution"}
    }


def _product_value_decision_memory_prediction_evals(
    metrics: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        "decision_impact": _dimension(
            score=(
                0.30 * metrics["recommendation_coverage"]
                + 0.20 * metrics["situation_coverage"]
                + 0.20 * metrics["decision_action_score"]
                + 0.10 * metrics["decision_resource_score"]
                + 0.10 * metrics["context_use_score"]
                + 0.10
                * (
                    metrics["future_validation_success_rate"]
                    if metrics["future_validation_events"]
                    else 0.0
                )
            ),
            metrics={
                "recommendation_coverage": metrics["recommendation_coverage"],
                "situation_coverage": metrics["situation_coverage"],
                "act_ops": metrics["act_ops"],
                "act_ops_per_storyline": _ratio(
                    metrics["act_ops"],
                    metrics["total_storyline_floor"],
                ),
                "resource_ops": metrics["resource_ops"],
                "resource_ops_per_storyline": _ratio(
                    metrics["resource_ops"],
                    metrics["total_storyline_floor"],
                ),
                "future_validation_success_rate": (
                    metrics["future_validation_success_rate"]
                    if metrics["future_validation_events"]
                    else 0.0
                ),
                "context_use_score": metrics["context_use_score"],
            },
            findings=[
                "Tests whether hidden understanding turns into concrete recommendations, actions, and resource decisions.",
                "Future validation shows whether those decisions stayed useful after the company changed.",
            ],
        ),
        "memory_lifecycle": _dimension(
            score=(
                0.25 * _clamp01(metrics["update_share"] / 0.25)
                + 0.20 * metrics["evidence_attachment_score"]
                + 0.20 * metrics["memory_archive_score"]
                + 0.20 * metrics["future_validation_memory_touch_score"]
                + 0.15 * metrics["duplicate_learning_score"]
            ),
            metrics={
                "model_inserts": metrics["model_inserts"],
                "model_updates": metrics["model_updates"],
                "update_share": metrics["update_share"],
                "model_archives": metrics["model_archives"],
                "evidence_attachments": metrics["evidence_attachments"],
                "near_duplicate_absorptions": metrics["near_duplicate_absorptions"],
                "exact_duplicate_natural_groups": metrics["exact_duplicate_groups"],
                "future_validation_memory_touch_ops": (
                    metrics["future_validation_memory_touch_ops"]
                ),
            },
            findings=[
                "Tests whether memory is updated, evidenced, archived, and merged instead of only appended.",
                "The strongest proof is future evidence changing existing compressed memory.",
            ],
        ),
        "prediction_lifecycle": _dimension(
            score=(
                0.35 * metrics["prediction_model_score"]
                + 0.25 * metrics["prediction_resolution_proxy_score"]
                + 0.20
                * (
                    metrics["future_validation_success_rate"]
                    if metrics["future_validation_events"]
                    else 0.0
                )
                + 0.10 * metrics["future_validation_context_score"]
                + 0.10 * metrics["future_validation_memory_touch_score"]
            ),
            metrics={
                "prediction_models": metrics["prediction_models"],
                "prediction_models_per_storyline": _ratio(
                    metrics["prediction_models"],
                    metrics["total_storyline_floor"],
                ),
                "future_validation_events": metrics["future_validation_events"],
                "future_validation_success_rate": (
                    metrics["future_validation_success_rate"]
                    if metrics["future_validation_events"]
                    else 0.0
                ),
                "future_validation_model_or_graph_context_use_score": (
                    metrics["future_validation_context_score"]
                ),
                "prediction_resolution_proxy_score": (
                    metrics["prediction_resolution_proxy_score"]
                ),
            },
            findings=[
                "Tests whether forecasts become durable Predictions and later evidence validates, updates, or retires them.",
                "The current harness uses future validation as a proxy until explicit prediction outcome records exist.",
            ],
        ),
    }


def _product_value_counterfactual_bridge_compression_evals(
    metrics: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    bridge_story = metrics["bridge_story"]
    return {
        "counterfactual_trap": _dimension(
            score=(
                0.30 * metrics["noise_score"]
                + 0.30 * metrics["alias_storyline_score"]
                + 0.20 * metrics["alias_review_deferral_score"]
                + 0.10 * (1.0 - metrics["alias_strong_acceptance_pressure"])
                + 0.10 * metrics["review_debt_score"]
            ),
            metrics={
                "noise_noop_score": metrics["noise_score"],
                "alias_storyline_score": metrics["alias_storyline_score"],
                "alias_review_candidate_count": (
                    metrics["alias_review_candidate_count"]
                ),
                "alias_needs_review_candidate_count": (
                    metrics["alias_needs_review_count"]
                ),
                "alias_deferred_candidate_count": (
                    metrics["alias_deferred_candidate_count"]
                ),
                "alias_accepted_candidate_count": (
                    metrics["alias_accepted_candidate_count"]
                ),
                "alias_review_deferral_score": metrics["alias_review_deferral_score"],
                "review_debt_score": metrics["review_debt_score"],
            },
            findings=[
                "Tests whether the system resists tempting but wrong memory under noise, ambiguity, and contradictory evidence.",
                "Alias ambiguity should create review/deferral behavior before strong customer graph writes.",
            ],
        ),
        "latent_bridge_inference": _dimension(
            score=(
                0.20 * metrics["bridge_storyline_score"]
                + 0.20 * metrics["bridge_presence_score"]
                + 0.20 * metrics["bridge_transition_support_score"]
                + 0.15 * metrics["bridge_epistemic_score"]
                + 0.15 * metrics["bridge_future_confirmation_score"]
                + 0.10 * metrics["bridge_no_fabrication_score"]
            ),
            metrics={
                "bridge_storyline_score": metrics["bridge_storyline_score"],
                "inferred_bridge_model_count": metrics["bridge_model_count"],
                "transition_supported_bridge_model_count": (
                    metrics["bridge_transition_supported_count"]
                ),
                "future_confirmed_bridge_model_count": (
                    metrics["bridge_future_confirmed_count"]
                ),
                "unsupported_specific_claim_count": (
                    metrics["unsupported_bridge_specific_claims"]
                ),
                "bridge_epistemic_marker_count": (
                    metrics["bridge_epistemic_marker_count"]
                ),
                "bridge_epistemic_marker_hits": (
                    bridge_story.bridge_epistemic_marker_hits if bridge_story else []
                ),
                "bridge_forbidden_detail_hits": (
                    bridge_story.bridge_forbidden_detail_hits if bridge_story else []
                ),
                "transition_support_score": metrics["bridge_transition_support_score"],
                "future_confirmation_score": (
                    metrics["bridge_future_confirmation_score"]
                ),
                "no_fabrication_score": metrics["bridge_no_fabrication_score"],
            },
            findings=[
                "Tests whether irregular state transitions create bounded inferred bridge Models.",
                "Rewards indirect before/after support and later confirmation while penalizing invented specifics.",
            ],
        ),
        "compression_loss": _dimension(
            score=(
                0.25 * metrics["latent_avg"]
                + 0.20 * metrics["concrete_latent_ratio"]
                + 0.15 * metrics["evidence_avg"]
                + 0.15 * metrics["compression_dimension_score"]
                + 0.15 * metrics["model_context_score"]
                + 0.10 * metrics["historical_observation_leakage_score"]
            ),
            metrics={
                "average_latent_pattern_score": metrics["latent_avg"],
                "concrete_latent_model_ratio": metrics["concrete_latent_ratio"],
                "evidence_support_score": metrics["evidence_avg"],
                "compression_dimension_score": metrics["compression_dimension_score"],
                "durable_growth_per_signal": metrics["durable_growth_per_signal"],
                "model_or_graph_context_use_score": metrics["model_context_score"],
                "historical_observation_leakage_score": (
                    metrics["historical_observation_leakage_score"]
                ),
            },
            findings=[
                "Tests whether compressed Models preserve the hidden company pattern without needing raw observation replay.",
                "High compression is not valuable unless later retrieval uses the compressed form.",
            ],
        ),
    }


def _product_value_learning_customer_evals(
    metrics: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        "experience_metabolism": _dimension(
            score=(
                0.45 * metrics["experience_closure_score"]
                + 0.20 * metrics["experience_policy_effect_score"]
                + 0.20 * metrics["experience_future_behavior_score"]
                + 0.15 * metrics["experience_evaluation_score"]
            ),
            metrics={
                "experience_closure_score": metrics["experience_closure_score"],
                "experience_loop_closed": metrics["experience_loop_closed"],
                "experience_policy_effects": metrics["experience_policy_effects"],
                "experience_evaluation_events": (
                    metrics["experience_evaluation_events"]
                ),
                "experience_future_behavior_levers": (
                    metrics["experience_future_behavior_levers"]
                ),
                "experience_direct_policy_effects": (
                    metrics["experience_direct_policy_effects"]
                ),
                "experience_policy_effect_score": (
                    metrics["experience_policy_effect_score"]
                ),
                "experience_future_behavior_score": (
                    metrics["experience_future_behavior_score"]
                ),
                "experience_evaluation_score": (
                    metrics["experience_evaluation_score"]
                ),
            },
            findings=[
                "Tests whether SAGE turned observed outcomes into future-behavior policy.",
                "Scattered adaptive activity does not count unless the experience loop closes.",
            ],
        ),
        "negative_learning": _dimension(
            score=(
                0.45 * metrics["negative_memory_score"]
                + 0.25 * metrics["noise_score"]
                + 0.20 * metrics["unused_context_avoidance_score"]
                + 0.10
                * _clamp01(
                    _ratio(
                        metrics["negative_memory_inserts"],
                        metrics["total_storyline_floor"],
                    )
                )
            ),
            metrics={
                "negative_memory_count": metrics["negative_memory_count"],
                "negative_memory_inserts": metrics["negative_memory_inserts"],
                "think_negative_memory_inserts": (
                    metrics["think_negative_memory_inserts"]
                ),
                "topology_negative_memory_inserts": (
                    metrics["topology_negative_memory_inserts"]
                ),
                "negative_learning_events": metrics["negative_learning_events"],
                "noise_noop_score": metrics["noise_score"],
                "unused_selected_context_count": metrics["unused_context"],
                "unused_context_avoidance_score": (
                    metrics["unused_context_avoidance_score"]
                ),
            },
            findings=[
                "Tests whether the system learns what not to retrieve, ask, or amplify.",
                "Noise no-op behavior helps, but durable negative memory is the stronger product proof.",
            ],
        ),
        "question_policy": _dimension(
            score=(
                0.55 * metrics["question_policy_score"]
                + 0.25 * metrics["context_use_score"]
                + 0.20 * metrics["unused_context_avoidance_score"]
            ),
            metrics={
                "question_policy_stats": metrics["question_policy_count"],
                "question_policy_updates": metrics["question_policy_updates"],
                "think_question_policy_updates": (
                    metrics["think_question_policy_updates"]
                ),
                "topology_question_policy_updates": (
                    metrics["topology_question_policy_updates"]
                ),
                "question_policy_events": metrics["question_policy_events"],
                "question_policy_probe_count": metrics["question_policy_probe_count"],
                "context_use_score": metrics["context_use_score"],
                "unused_selected_context_count": metrics["unused_context"],
                "unused_context_avoidance_score": (
                    metrics["unused_context_avoidance_score"]
                ),
            },
            findings=[
                "Tests whether the system learns when to ask, when not to ask, and which missing context matters.",
                "This should improve future context selection instead of producing repeated generic uncertainty.",
            ],
        ),
        "customer_value": _dimension(
            score=(
                0.25 * metrics["customer_scope_coverage"]
                + 0.15 * metrics["customer_scope_share"]
                + 0.20 * metrics["recommendation_coverage"]
                + 0.15 * metrics["customer_account_health_score"]
                + 0.12 * metrics["accepted_edge_coverage"]
                + 0.08 * metrics["precise_edge_coverage"]
                + 0.05 * metrics["relation_frame_score"]
            ),
            metrics={
                "gold_customer_count": metrics["gold_customer_count"],
                "customer_scope_count": metrics["customer_scope_count"],
                "customer_scope_coverage": metrics["customer_scope_coverage"],
                "customer_scoped_models": metrics["customer_scoped_models"],
                "unscoped_models": metrics["unscoped_models"],
                "customer_scope_share": metrics["customer_scope_share"],
                "recommendation_coverage": metrics["recommendation_coverage"],
                "accepted_expected_edge_kind_coverage": (
                    metrics["accepted_edge_coverage"]
                ),
                "precise_expected_edge_kind_coverage": (
                    metrics["precise_edge_coverage"]
                ),
                "relation_frame_score": metrics["relation_frame_score"],
                "relation_frame_ops": metrics["relation_frame_ops"],
                "edge_lifecycle_events": metrics["edge_lifecycle_events"],
                "customer_account_health_score": (
                    metrics["customer_account_health_score"]
                ),
            },
            findings=[
                "Tests whether system value lands in account-health objects customers actually care about.",
                "Rewards scoped customer memory, recommendations, precise edges, and future validation.",
            ],
        ),
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
    relation_frame_score: float,
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
    metrics = _product_value_metrics(
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
        relation_frame_score=relation_frame_score,
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
    evals = {
        **_product_value_decision_memory_prediction_evals(metrics),
        **_product_value_counterfactual_bridge_compression_evals(metrics),
        **_product_value_learning_customer_evals(metrics),
    }
    proof_gaps = _product_value_proof_gaps_from_metrics(metrics)

    overall = round(
        _avg(
            [
                float(evals[key]["score"])
                for key in _PRODUCT_VALUE_EVAL_KEYS
                if key in evals
            ]
        ),
        4,
    )
    return {
        "overall_score": overall,
        "interpretation": _score_interpretation(overall),
        "evals": evals,
        "proof_gaps": proof_gaps,
    }


def _product_value_proof_gaps_from_metrics(metrics: dict[str, Any]) -> list[str]:
    return _product_value_proof_gaps(
        recommendation_coverage=metrics["recommendation_coverage"],
        act_ops=metrics["act_ops"],
        resource_ops=metrics["resource_ops"],
        model_archives=metrics["model_archives"],
        evidence_attachments=metrics["evidence_attachments"],
        prediction_models=metrics["prediction_models"],
        future_validation_events=metrics["future_validation_events"],
        total_storylines=metrics["total_storylines"],
        alias_score=metrics["alias_score"],
        alias_needs_review_count=metrics["alias_needs_review_count"],
        alias_review_candidate_count=metrics["alias_review_candidate_count"],
        noise_score=metrics["noise_score"],
        bridge_story=metrics["bridge_story"],
        bridge_model_count=metrics["bridge_model_count"],
        bridge_transition_supported_count=metrics["bridge_transition_supported_count"],
        bridge_epistemic_marker_count=metrics["bridge_epistemic_marker_count"],
        unsupported_bridge_specific_claims=metrics["unsupported_bridge_specific_claims"],
        bridge_future_confirmed_count=metrics["bridge_future_confirmed_count"],
        latent_avg=metrics["latent_avg"],
        concrete_latent_ratio=metrics["concrete_latent_ratio"],
        model_context_score=metrics["model_context_score"],
        experience_loop_closed=metrics["experience_loop_closed"],
        experience_closure_score=metrics["experience_closure_score"],
        experience_future_behavior_levers=metrics["experience_future_behavior_levers"],
        negative_learning_events=metrics["negative_learning_events"],
        question_policy_events=metrics["question_policy_events"],
        customer_scope_count=metrics["customer_scope_count"],
        customer_scoped_models=metrics["customer_scoped_models"],
        precise_edge_coverage=metrics["precise_edge_coverage"],
        relation_frame_score=metrics["relation_frame_score"],
        capability_probe_counts=metrics["capability_probe_counts"],
        lifecycle_obligation_report=metrics["lifecycle_obligation_report"],
    )


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
    capability_probe_counts: dict[str, Any],
) -> list[str]:
    gaps: list[str] = []
    def probed(kind: str) -> bool:
        try:
            return int(capability_probe_counts.get(kind) or 0) > 0
        except (TypeError, ValueError):
            return False

    if wave_stats["timeout_like_t1_batches"]:
        gaps.append("At least one T1 batch timed out before producing a Think run.")
    if dimensions["temporal_improvement"]["metrics"]["future_validation_events"] == 0:
        gaps.append(
            "No future validation events, so temporal improvement is proxy-scored."
        )
    elif float(future_stats.get("success_rate") or 0.0) < 1.0:
        gaps.append("At least one future validation batch did not complete cleanly.")
    elif float(future_stats.get("model_or_graph_context_use_score") or 0.0) == 0.0:
        gaps.append("Future validation did not use compressed Model/graph context.")
    elif float(future_stats.get("memory_touch_ops") or 0.0) == 0.0:
        gaps.append(
            "Future validation used context but did not update, link, archive, "
            "or attach evidence to durable memory."
        )
    missing_edges = sorted(required_edge_kinds - set(edge_distribution))
    if missing_edges:
        gaps.append(
            "Expected registered edge kinds not observed as accepted durable "
            "edges or projected relation-frame structure: "
            + ", ".join(missing_edges)
        )
    edge_metrics = _json_obj(edge_intelligence.get("metrics"))
    if float(edge_metrics.get("precise_required_edge_kind_coverage") or 0.0) < 0.8:
        gaps.append(
            "Precise registered edge kinds are underused; check whether Think "
            "is collapsing blocks/weakens/explains/resolution edges into prose "
            "or generic support."
        )
    if float(edge_metrics.get("relation_frame_score") or 0.0) == 0.0:
        gaps.append(
            "N-ary relation frames were not exercised; cross-model connection "
            "quality is only proven for binary edges/candidates."
        )
    retrieval_metrics = _json_obj(
        dimensions.get("retrieval_usefulness", {}).get("metrics")
    )
    avg_models = float(retrieval_metrics.get("avg_models_per_t1_batch") or 0.0)
    retrieval_budget_fit = float(
        retrieval_metrics.get("retrieval_budget_fit_score") or 0.0
    )
    if avg_models > 16.0 and retrieval_budget_fit < 0.8:
        gaps.append(
            "Selected Model context is above the efficient batch budget; "
            "retrieval should prove usefulness or compact before Think."
        )
    elif 0.0 < avg_models < 6.0 and retrieval_budget_fit < 0.8:
        gaps.append(
            "Selected Model context is too sparse for reliable cross-model "
            "reasoning in batch mode."
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
    if (
        float(edge_metrics.get("future_validation_edge_ops") or 0.0) == 0.0
        and float(edge_metrics.get("future_validation_relation_frame_ops") or 0.0)
        == 0.0
    ):
        gaps.append(
            "Future validation did not evolve binary edges or N-ary relation frames."
        )
    if float(edge_metrics.get("ontology_gap_ops") or 0.0) > 0.0 and missing_edges:
        gaps.append(
            "Ontology-gap ops occurred while registered expected edge kinds "
            "were still missing; verify the system is not proposing new kinds "
            "where existing kinds fit."
        )
    if int(model_kind_distribution.get("prediction") or 0) < 5:
        if probed("prediction"):
            gaps.append("Prediction probe ran, but prediction memory is still thin.")
        else:
            gaps.append("Prediction memory is barely exercised.")
    if int(ops["resource_ops"]) == 0:
        if probed("resource"):
            gaps.append(
                "Resource/action-resource probe ran, but no resource ops survived."
            )
        else:
            gaps.append("Resource/action-resource operations are untested.")
    if int(ops["ontology_gap_ops"]) == 0:
        if probed("ontology_gap"):
            gaps.append("Ontology-gap probe ran, but no ontology-gap op survived.")
        else:
            gaps.append("Ontology-gap write path is untested by this run.")
    if int(ops["model_archives"]) == 0:
        if probed("archive"):
            gaps.append("Archive probe ran, but no model archive survived.")
        else:
            gaps.append("Model archival/staleness cleanup is untested.")
    if int(ops["evidence_attachments"]) == 0:
        if probed("evidence_attachment"):
            gaps.append("Evidence probe ran, but no evidence attachment survived.")
        else:
            gaps.append("Evidence attachment behavior is untested.")
    if float(discovery_counts.get("negative_memory") or 0) == 0:
        gaps.append("Negative memory behavior is untested.")
    if float(discovery_counts.get("question_policy_stats") or 0) == 0:
        if probed("question_policy"):
            gaps.append(
                "Question-policy probe ran, but no policy stats were updated."
            )
        else:
            gaps.append("Question-policy learning is untested.")
    adaptive_metrics = _json_obj(
        dimensions.get("adaptive_lifecycle", {}).get("metrics")
    )
    if float(adaptive_metrics.get("feedback_learning_score") or 0.0) < 0.5:
        gaps.append(
            "Adaptive feedback is weak: retrieval/writer outcomes are not "
            "creating enough durable shortcut, affordance, policy, or negative "
            "learning signals."
        )
    if float(adaptive_metrics.get("temporal_closure_score") or 0.0) < 0.5:
        gaps.append(
            "Adaptive temporal closure is weak: future validation has not "
            "proven that prior memory changes later retrieval, writes, or edge "
            "evolution."
        )
    if float(adaptive_metrics.get("autonomous_maintenance_score") or 0.0) < 0.5:
        gaps.append(
            "Autonomous maintenance is weak: post-commit/background work is "
            "not yet proving reliable self-maintenance for this run."
        )
    topology_metrics = _json_obj(model_summary.get("topology_optimizer_metric_totals"))
    shortcut_skips = float(topology_metrics.get("shortcut_missing_model_skips") or 0)
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


def _retrieval_context_budget_fit_score(
    avg_models_per_t1_batch: float,
    *,
    useful_floor: float = 6.0,
    useful_ceiling: float = 16.0,
    hard_ceiling: float = 28.0,
) -> float:
    """Reward enough selected Models without rewarding context bloat."""
    avg_models = max(0.0, float(avg_models_per_t1_batch))
    if avg_models <= 0.0:
        return 0.0
    if avg_models < useful_floor:
        return _clamp01(avg_models / useful_floor)
    if avg_models <= useful_ceiling:
        return 1.0
    return 1.0 - _clamp01(
        (avg_models - useful_ceiling)
        / max(1.0, hard_ceiling - useful_ceiling)
    )


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
    latent_pattern_scores = [score.latent_pattern_score for score in storyline_scores]
    thesis_judge_scores = [
        float(score.thesis_judge_score)
        for score in storyline_scores
        if score.thesis_judge_score is not None
    ]
    calibration = _storyline_calibration_report(storyline_scores)
    concrete_latent_count = sum(
        1
        for score in storyline_scores
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
        run = (wave.get("t1_batch") or {}).get("run") or {}
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
        "average_storyline_score": round(sum(score_values) / len(score_values), 4)
        if score_values
        else 0.0,
        "min_storyline_score": min(score_values) if score_values else 0.0,
        "max_storyline_score": max(score_values) if score_values else 0.0,
        "storyline_scores": [asdict(score) for score in storyline_scores],
        "latent_pattern_fitness": {
            "average_latent_pattern_score": round(
                sum(latent_pattern_scores) / len(latent_pattern_scores),
                4,
            )
            if latent_pattern_scores
            else 0.0,
            "storylines_with_concrete_latent_model": concrete_latent_count,
            "storylines_without_concrete_latent_model": (
                len(storyline_scores) - concrete_latent_count
            ),
            "average_best_pattern_coverage": round(
                sum(score.latent_pattern_best_coverage for score in storyline_scores)
                / len(storyline_scores),
                4,
            )
            if storyline_scores
            else 0.0,
        },
        "thesis_recovery_judge": {
            "enabled": bool(thesis_judge_scores),
            "n": len(thesis_judge_scores),
            "average_score": _avg(thesis_judge_scores),
            "correct_count": sum(
                1 for score in storyline_scores if score.thesis_judge_correct is True
            ),
            "incorrect_count": sum(
                1 for score in storyline_scores if score.thesis_judge_correct is False
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
        "run_health": {
            "min_efficiency_score": (
                _json_obj(model_summary.get("run_config")).get(
                    "min_efficiency_score"
                )
            ),
            "max_background_maintenance_llm_calls": (
                _json_obj(model_summary.get("run_config")).get(
                    "max_background_maintenance_llm_calls"
                )
            ),
            "background_maintenance_llm_calls": _json_obj(
                _json_obj(model_summary.get("think_cost_profile")).get(
                    "background_maintenance"
                )
            ).get("llm_calls"),
            "pending_triggers": model_summary.get("pending_triggers"),
            "pending_post_commit_actions": model_summary.get(
                "pending_post_commit_actions"
            ),
            "dead_lettered_post_commit_actions": model_summary.get(
                "dead_lettered_post_commit_actions"
            ),
            "failed_post_commit_actions": (
                _json_obj(model_summary.get("post_commit_status")).get("failed")
            ),
            "failed_topology_optimizer_runs": (
                _json_obj(model_summary.get("topology_optimizer_status")).get("failed")
            ),
            "think_runs_success": model_summary.get("think_runs_success"),
            "think_runs_failed": model_summary.get("think_runs_failed"),
        },
        "t1_retry": _t1_retry_report(waves),
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
            if retrieval_model_counts
            else 0,
            "max_models_per_t1_batch": max(retrieval_model_counts)
            if retrieval_model_counts
            else 0,
        },
        "memory_shape": {
            "active_models": model_summary.get("active_models"),
            "archived_models": model_summary.get("archived_models"),
            "model_edges": model_summary.get("model_edges"),
            "projection_metabolism": model_summary.get("projection_metabolism"),
            "relation_frame_lifecycle": model_summary.get(
                "relation_frame_lifecycle"
            ),
            "relationship_candidates": model_summary.get("relationship_candidates"),
            "relationship_candidate_lifecycle": model_summary.get(
                "relationship_candidate_lifecycle"
            ),
            "relationship_candidates_from_topology": model_summary.get(
                "relationship_candidates_from_topology",
                model_summary.get("latent_topology_candidates"),
            ),
            "relationship_candidate_status_distribution": model_summary.get(
                "relationship_candidate_status_distribution"
            ),
            "model_kind_distribution": model_summary.get("model_kind_distribution"),
            "context_use_distribution": model_summary.get("context_use_distribution"),
            "context_use_relation_contract": model_summary.get(
                "context_use_relation_contract"
            ),
        },
        "question_planner_reflective_report": model_summary.get(
            "question_planner_reflective_report"
        )
        or {},
        "latency_breakdown": model_summary.get("latency_breakdown") or {},
        "post_commit_action_profile": model_summary.get(
            "post_commit_action_profile"
        )
        or {},
        "projection_metabolism": model_summary.get("projection_metabolism") or {},
        "downstream_suppression": model_summary.get("downstream_suppression") or {},
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
    required_failures = _benchmark_required_run_failures(summary)
    summary["required_run_failures"] = required_failures
    summary["status"] = "failed" if required_failures else "passed"
    return summary


def _fmt_latency_ms(value: Any) -> str:
    number = _numeric_ms(value)
    if number is None:
        return "-"
    if number >= 1000.0:
        return f"{number / 1000.0:.3f}s"
    return f"{number:.1f}ms"


def _fmt_ratio(value: Any) -> str:
    try:
        return f"{float(value) * 100.0:.1f}%"
    except (TypeError, ValueError):
        return "-"


def _top_latency_groups(
    groups: dict[str, Any],
    *,
    limit: int = 8,
) -> list[tuple[str, Any]]:
    return sorted(
        groups.items(),
        key=lambda item: float((item[1] or {}).get("elapsed_ms_total") or 0.0),
        reverse=True,
    )[:limit]


def _t1_retry_report(waves: list[dict[str, Any]]) -> dict[str, Any]:
    recovered = 0
    exhausted = 0
    retry_attempts = 0
    retry_waves: list[dict[str, Any]] = []
    for wave in waves:
        batch = wave.get("t1_batch") or {}
        retry_count = int(batch.get("retry_count") or 0)
        if retry_count <= 0:
            continue
        run = batch.get("run") or {}
        retry_attempts += retry_count
        if run.get("status") == "success":
            recovered += 1
        else:
            exhausted += 1
        retry_waves.append(
            {
                "wave": wave.get("wave"),
                "sequence": wave.get("sequence"),
                "trigger_id": batch.get("trigger_id"),
                "retry_count": retry_count,
                "final_status": run.get("status"),
                "final_error": run.get("error"),
            }
        )
    return {
        "retry_attempts": retry_attempts,
        "recovered_t1_batches": recovered,
        "exhausted_t1_batches": exhausted,
        "retry_waves": retry_waves,
    }


def _benchmark_required_run_failures(summary: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    for wave in summary.get("waves") or []:
        batch = wave.get("t1_batch") or {}
        if not batch:
            continue
        run = batch.get("run") or {}
        if run.get("status") == "success":
            continue
        label = wave.get("sequence") or f"wave {wave.get('wave')}"
        trigger_id = batch.get("trigger_id") or "<unknown>"
        error = run.get("error") or "no successful Think run recorded"
        failures.append(
            f"required T1 batch failed: {label} trigger={trigger_id} error={error}"
        )
    health = summary.get("run_health") or {}
    pending_triggers = int(health.get("pending_triggers") or 0)
    if pending_triggers:
        failures.append(f"trigger queue did not drain: pending={pending_triggers}")
    pending_post_commit = int(health.get("pending_post_commit_actions") or 0)
    if pending_post_commit:
        failures.append(
            f"post-commit queue did not drain: pending={pending_post_commit}"
        )
    dead_lettered = int(health.get("dead_lettered_post_commit_actions") or 0)
    if dead_lettered:
        failures.append(
            f"post-commit actions dead-lettered: dead_lettered={dead_lettered}"
        )
    failed_post_commit = int(health.get("failed_post_commit_actions") or 0)
    if failed_post_commit:
        failures.append(f"post-commit actions failed: failed={failed_post_commit}")
    failed_topology = int(health.get("failed_topology_optimizer_runs") or 0)
    if failed_topology:
        failures.append(f"topology optimizer failed: failed={failed_topology}")
    min_efficiency = health.get("min_efficiency_score")
    if min_efficiency is not None:
        min_efficiency_score = float(min_efficiency or 0.0)
        efficiency = (
            (summary.get("company_intelligence_scorecard") or {})
            .get("dimensions", {})
            .get("efficiency", {})
            .get("score")
        )
        if efficiency is not None and float(efficiency) < min_efficiency_score:
            failures.append(
                "efficiency score below required floor: "
                f"{float(efficiency):.4f} < {min_efficiency_score:.4f}"
            )
    max_background_llm = health.get("max_background_maintenance_llm_calls")
    if max_background_llm is not None:
        max_background_llm_calls = int(max_background_llm)
        if max_background_llm_calls >= 0:
            actual_background_llm_calls = int(
                health.get("background_maintenance_llm_calls") or 0
            )
            if actual_background_llm_calls > max_background_llm_calls:
                failures.append(
                    "background maintenance LLM calls above required ceiling: "
                    f"{actual_background_llm_calls} > "
                    f"{max_background_llm_calls}"
                )
    return failures


def _render_benchmark_markdown(summary: dict[str, Any]) -> str:
    append = summary.get("append") or {}
    latency = summary.get("latency_breakdown") or {}
    critical_latency = latency.get("critical_path_summary") or {}
    t1_latency = latency.get("t1_wave_wall_clock") or {}
    think_latency = latency.get("think_runs") or {}
    inquiry_latency = latency.get("adaptive_inquiry") or {}
    lines = [
        "# Storyline Batch Benchmark",
        "",
        f"- Run: `{summary.get('run_id')}`",
        f"- Status: `{summary.get('status', 'unknown')}`",
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
    planner_report = summary.get("question_planner_reflective_report") or {}
    if planner_report:
        lifecycle = planner_report.get("reflective_rule_lifecycle") or {}
        replay = planner_report.get("reflective_replay") or {}
        attribution = planner_report.get("reflective_attribution") or {}
        lines.extend(
            [
                f"- Question planner events: {planner_report.get('planning_events')}",
                f"- Reflective active rules: "
                f"{(lifecycle.get('maturity_distribution') or {}).get('active', 0)}",
                f"- Reflective replay runs: {replay.get('replay_runs')}",
                f"- Reflective attributions: {attribution.get('attributions')}",
            ]
        )
    if append:
        lines.extend(
            [
                f"- Append base run: `{append.get('base_run_id')}`",
                f"- Additional T1 batches: {append.get('additional_t1_batches')}",
                f"- Additional signals: {append.get('additional_signal_count')}",
                f"- Horizon batches: "
                f"{int(append.get('horizon_start_batch') or 0) + 1}-"
                f"{append.get('horizon_end_batch')}",
            ]
        )
    rerender = summary.get("rerender") or {}
    if rerender:
        lines.extend(
            [
                f"- Rerender source: `{rerender.get('source_run_id')}`",
                f"- Artifact-only rerender: {rerender.get('artifact_only')}",
                f"- Product-value delta: {rerender.get('product_value_delta')}",
            ]
        )
    readiness = summary.get("rerun_readiness") or {}
    if readiness:
        lines.extend(
            [
                "",
                "## Rerun Readiness",
                "",
                f"- Ready for fresh 10-batch: "
                f"{readiness.get('ready_for_fresh_10batch')}",
                f"- Recommended next step: "
                f"`{readiness.get('recommended_next_step')}`",
                f"- Product-value overall: "
                f"{readiness.get('product_value_overall')} "
                f"(floor {readiness.get('min_product_value')})",
                f"- Eval floor: {readiness.get('min_eval_score')}",
            ]
        )
        low_eval_scores = readiness.get("low_eval_scores") or {}
        if low_eval_scores:
            lines.extend(
                [
                    "",
                    "Low eval scores:",
                    *[
                        f"- {key}: {value}"
                        for key, value in sorted(low_eval_scores.items())
                    ],
                ]
            )
        gate_failures = readiness.get("gate_failures") or []
        if gate_failures:
            lines.extend(
                [
                    "",
                    "Gate failures:",
                    *[f"- {item}" for item in gate_failures],
                ]
            )
        targeted_db_command = readiness.get("targeted_db_proof_command")
        optional_canary_command = readiness.get("optional_small_canary_command")
        if targeted_db_command or optional_canary_command:
            lines.extend(["", "Targeted proof commands:"])
            if targeted_db_command:
                lines.extend(["", "```bash", str(targeted_db_command), "```"])
            if optional_canary_command:
                lines.extend(["", "Optional health canary:", "", "```bash"])
                lines.extend([str(optional_canary_command), "```"])
            if readiness.get("small_canary_scope_note"):
                lines.extend(["", str(readiness.get("small_canary_scope_note"))])
    if summary.get("required_run_failures"):
        lines.extend(
            [
                "",
                "## Required Run Failures",
                "",
                *[f"- {item}" for item in summary["required_run_failures"]],
            ]
        )
    retry_report = summary.get("t1_retry") or {}
    if retry_report:
        lines.extend(
            [
                "",
                "## T1 Retry Recovery",
                "",
                "```json",
                json.dumps(retry_report, indent=2, sort_keys=True),
                "```",
            ]
        )
    lines.extend(
        [
            "",
            "## Question Planner And Reflective Rules",
            "",
            "```json",
            json.dumps(planner_report, indent=2, sort_keys=True, default=str),
            "```",
            "",
            "## Latency Breakdown",
            "",
            "| Critical Path Metric | Value |",
            "| --- | ---: |",
            "| T1 wall-clock total | "
            f"{_fmt_latency_ms(critical_latency.get('t1_wall_ms_total'))} |",
            "| Main Think LLM total | "
            f"{_fmt_latency_ms(critical_latency.get('t1_llm_ms_total'))} |",
            "| Non-main-LLM residual total | "
            f"{_fmt_latency_ms(critical_latency.get('t1_non_llm_residual_ms_total'))} |",
            "| Measured T1 internal non-main stage total | "
            f"{_fmt_latency_ms(critical_latency.get('t1_measured_non_llm_stage_ms_total'))} |",
            "| Unaccounted T1 non-main residual | "
            f"{_fmt_latency_ms(critical_latency.get('t1_non_llm_unaccounted_stage_ms_total'))} |",
            "| Unclassified or failed T1 wall total | "
            f"{_fmt_latency_ms(critical_latency.get('t1_unclassified_or_failed_ms_total'))} |",
            "| Adaptive inquiry runtime total | "
            f"{_fmt_latency_ms(critical_latency.get('adaptive_inquiry_runtime_ms_total'))} |",
            "| Main Think LLM share of T1 wall | "
            f"{_fmt_ratio(critical_latency.get('main_llm_share_of_t1_wall'))} |",
            "| Non-main-LLM share of T1 wall | "
            f"{_fmt_ratio(critical_latency.get('non_main_llm_share_of_t1_wall'))} |",
            "| Measured internal share of non-main residual | "
            f"{_fmt_ratio(critical_latency.get('measured_non_llm_stage_share_of_non_llm_residual'))} |",
            "| Unclassified or failed share of T1 wall | "
            f"{_fmt_ratio(critical_latency.get('unclassified_or_failed_share_of_t1_wall'))} |",
            "| Adaptive inquiry share of T1 wall | "
            f"{_fmt_ratio(critical_latency.get('adaptive_inquiry_share_of_t1_wall'))} |",
            "",
            "### T1 Waves",
            "",
            "| Wave | Sequence | Status | Wall | Main LLM | Non-main Residual | Measured Non-main | Top Stage | Models | Observations | Error |",
            "| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |",
        ]
    )
    for wave in t1_latency.get("waves") or []:
        lines.append(
            "| {wave} | {sequence} | {status} | {wall} | {llm} | {residual} | "
            "{measured} | {top_stage} | {models} | {observations} | {error} |".format(
                wave=wave.get("wave") or "-",
                sequence=wave.get("sequence") or "-",
                status=wave.get("status") or "-",
                wall=_fmt_latency_ms(wave.get("wall_ms")),
                llm=_fmt_latency_ms(wave.get("llm_ms")),
                residual=_fmt_latency_ms(wave.get("non_llm_residual_ms")),
                measured=_fmt_latency_ms(wave.get("non_llm_stage_timings_ms")),
                top_stage=wave.get("top_stage") or "-",
                models=wave.get("retrieval_model_count")
                if wave.get("retrieval_model_count") is not None
                else "-",
                observations=wave.get("retrieval_observation_count")
                if wave.get("retrieval_observation_count") is not None
                else "-",
                error=wave.get("error") or "-",
            )
        )
    lines.extend(
        [
            "",
            "### Think Internal Stages",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| Think runs | {think_latency.get('run_count') or 0} |",
            "| Runs with stage timings | "
            f"{think_latency.get('runs_with_stage_timings') or 0} |",
            "| Stage timing total | "
            f"{_fmt_latency_ms((think_latency.get('stage_timings_ms') or {}).get('total_ms'))} |",
            "| Non-main stage timing total | "
            f"{_fmt_latency_ms((think_latency.get('non_llm_stage_timings_ms') or {}).get('total_ms'))} |",
            "| Main LLM stage timing total | "
            f"{_fmt_latency_ms((think_latency.get('llm_stage_timings_ms') or {}).get('total_ms'))} |",
            "",
            "### Top Think Internal Stages",
            "",
            "| Stage | Count | Non-main Count | Main LLM Count | Total | P95 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for stage, data in _top_latency_groups(
        think_latency.get("stage_timings_by_stage") or {}
    ):
        stats = (data or {}).get("elapsed_ms_stats") or {}
        lines.append(
            f"| {stage} | {data.get('count') or 0} | "
            f"{data.get('non_llm_stage_count') or 0} | "
            f"{data.get('llm_stage_count') or 0} | "
            f"{_fmt_latency_ms(data.get('elapsed_ms_total'))} | "
            f"{_fmt_latency_ms(stats.get('p95_ms'))} |"
        )
    lines.extend(
        [
            "",
            "### Adaptive Inquiry Runtime",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| Sessions | {inquiry_latency.get('session_count') or 0} |",
            "| Sessions with runtime | "
            f"{inquiry_latency.get('sessions_with_runtime') or 0} |",
            "| Runtime total | "
            f"{_fmt_latency_ms((inquiry_latency.get('runtime_ms') or {}).get('total_ms'))} |",
            "| Runtime p95 | "
            f"{_fmt_latency_ms((inquiry_latency.get('runtime_ms') or {}).get('p95_ms'))} |",
            "| Action timing total | "
            f"{_fmt_latency_ms((inquiry_latency.get('retrieval_action_total_ms') or {}).get('total_ms'))} |",
            "| Stage timing total | "
            f"{_fmt_latency_ms((inquiry_latency.get('retrieval_stage_total_ms') or {}).get('total_ms'))} |",
            "| Unaccounted total | "
            f"{_fmt_latency_ms((inquiry_latency.get('unaccounted_ms') or {}).get('total_ms'))} |",
            "",
            "### Top Inquiry Action Paths",
            "",
            "| Path | Count | Total | Work | Wait | P95 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for path, data in _top_latency_groups(
        inquiry_latency.get("action_timings_by_path") or {}
    ):
        stats = (data or {}).get("elapsed_ms_stats") or {}
        lines.append(
            f"| {path} | {data.get('count') or 0} | "
            f"{_fmt_latency_ms(data.get('elapsed_ms_total'))} | "
            f"{_fmt_latency_ms(data.get('work_ms_total'))} | "
            f"{_fmt_latency_ms(data.get('wait_ms_total'))} | "
            f"{_fmt_latency_ms(stats.get('p95_ms'))} |"
        )
    lines.extend(
        [
            "",
            "### Top Inquiry Stages",
            "",
            "| Stage | Count | Total | P95 |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for stage, data in _top_latency_groups(
        inquiry_latency.get("stage_timings_by_stage") or {}
    ):
        stats = (data or {}).get("elapsed_ms_stats") or {}
        lines.append(
            f"| {stage} | {data.get('count') or 0} | "
            f"{_fmt_latency_ms(data.get('elapsed_ms_total'))} | "
            f"{_fmt_latency_ms(stats.get('p95_ms'))} |"
        )
    lines.extend(
        [
            "",
            "### Instrumentation Notes",
        ]
    )
    latency_notes = latency.get("instrumentation_notes") or []
    if latency_notes:
        lines.extend([f"- {note}" for note in latency_notes])
    else:
        lines.append("- No latency instrumentation notes were emitted.")
    lines.extend(
        _render_downstream_budget_markdown(
            summary.get("post_commit_action_profile") or {},
            summary.get("downstream_suppression") or {},
        )
    )
    lines.extend(
        _render_projection_metabolism_markdown(
            summary.get("projection_metabolism") or {}
        )
    )
    lines.extend(
        [
            "",
            "## Storyline Scores",
            "| Storyline | Score | Pattern | Pattern Models | Models | Situations | Recommendations | Edges | Frames | Edge Kinds Hit | Missing Edge Kinds | Review Debt | Missing Keywords |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |",
        ]
    )
    for score in summary.get("storyline_scores") or []:
        lines.append(
            "| {title} | {score:.2f} | {pattern:.2f} | {pattern_models} | "
            "{models} | {situations} | {recommendations} | {edges} | {frames} | "
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
                frames=score.get("relation_frame_count") or 0,
                edge_hits=", ".join(score.get("edge_kind_hits") or []) or "-",
                missing_edges=", ".join(score.get("missing_edge_kinds") or []) or "-",
                review=score["needs_review_candidate_count"],
                missing=", ".join(score["missing_keywords"][:5]) or "-",
            )
        )
    scorecard = summary.get("company_intelligence_scorecard") or {}
    lines.extend(
        [
            "",
            "## Company Intelligence Scorecard",
            "",
            f"- Overall: {scorecard.get('overall_score')} "
            f"({scorecard.get('interpretation')})",
            "",
            "| Dimension | Score |",
            "| --- | ---: |",
        ]
    )
    for name, dimension in (scorecard.get("dimensions") or {}).items():
        lines.append(
            f"| {name.replace('_', ' ').title()} | "
            f"{float(dimension.get('score') or 0.0):.2f} |"
        )
    product_value = scorecard.get("product_value_evals") or {}
    product_evals = product_value.get("evals") or {}
    lines.extend(
        [
            "",
            "### Product Value Evals",
            "",
            f"- Overall: {product_value.get('overall_score')} "
            f"({product_value.get('interpretation')})",
            "",
            "| Eval | Score |",
            "| --- | ---: |",
        ]
    )
    for name, evaluation in product_evals.items():
        lines.append(
            f"| {name.replace('_', ' ').title()} | "
            f"{float(evaluation.get('score') or 0.0):.2f} |"
        )
    product_gaps = product_value.get("proof_gaps") or []
    lines.extend(
        [
            "",
            "#### Product Value Proof Gaps",
        ]
    )
    if product_gaps:
        lines.extend([f"- {gap}" for gap in product_gaps])
    else:
        lines.append("- No product-value proof gaps detected by the current harness.")
    lines.extend(
        [
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
        ]
    )
    proof_gaps = scorecard.get("proof_gaps") or []
    if proof_gaps:
        lines.extend([f"- {gap}" for gap in proof_gaps])
    else:
        lines.append("- No proof gaps detected by the current harness.")
    lines.extend(
        [
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
            json.dumps(
                summary.get("run_amplification") or {}, indent=2, sort_keys=True
            ),
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
        ]
    )
    return "\n".join(lines)


def _render_projection_metabolism_markdown(
    projection_report: dict[str, Any],
) -> list[str]:
    if not projection_report:
        return []
    if projection_report.get("available") is False:
        return [
            "",
            "## Projection Metabolism",
            "",
            f"- Projection metrics unavailable: {projection_report.get('reason') or 'unknown'}.",
        ]
    coverage = projection_report.get("entity_projection_coverage") or {}
    lines = [
        "",
        "## Projection Metabolism",
        "",
        f"- Status: `{projection_report.get('status') or 'unknown'}`",
        f"- Entity coverage: {projection_report.get('entity_projection_coverage_ratio')}",
        f"- Refresh jobs / snapshots: {projection_report.get('jobs_to_snapshots_ratio')}",
        "",
        "| Entity Surface | Projection | Snapshots | Covered |",
        "| --- | --- | ---: | --- |",
    ]
    for entity_name, row in sorted(coverage.items()):
        lines.append(
            f"| {entity_name} | {row.get('projection_name')} | "
            f"{row.get('snapshot_count', 0)} | {bool(row.get('covered'))} |"
        )
    missing = projection_report.get("missing_entity_projection_families") or []
    if missing:
        lines.extend(["", "Missing entity surfaces: " + ", ".join(map(str, missing))])
    return lines


def _render_downstream_budget_markdown(
    post_commit_profile: dict[str, Any],
    downstream_suppression: dict[str, Any],
) -> list[str]:
    if not post_commit_profile and not downstream_suppression:
        return []
    lines = [
        "",
        "## Downstream Budget",
        "",
    ]
    by_kind = post_commit_profile.get("by_kind") or {}
    if by_kind:
        lines.extend(
            [
                "### Post-Commit Actions",
                "",
                "| Action | Total | Processed | Model IDs | Enqueue Think | No Think |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for action, row in sorted(by_kind.items()):
            lines.append(
                f"| {action} | {row.get('total', 0)} | "
                f"{row.get('processed', 0)} | {row.get('model_ids_total', 0)} | "
                f"{row.get('enqueue_think_true', 0)} | "
                f"{row.get('enqueue_think_false', 0)} |"
            )

    source_profile = post_commit_profile.get("source_profile") or []
    if source_profile:
        lines.extend(
            [
                "",
                "### Edge/Projection Source Profile",
                "",
                "| Action | Source | Selector | Rows | Model IDs | Enqueue Think |",
                "| --- | --- | --- | ---: | ---: | ---: |",
            ]
        )
        for row in source_profile[:12]:
            source = ":".join(
                part
                for part in [
                    str(row.get("source_kind") or "-"),
                    str(row.get("source_subkind") or ""),
                ]
                if part
            )
            lines.append(
                f"| {row.get('action_kind')} | {source} | "
                f"{row.get('selector') or '-'} | {row.get('total', 0)} | "
                f"{row.get('model_ids_total', 0)} | "
                f"{row.get('enqueue_think_true', 0)} |"
            )

    suppression_rows = downstream_suppression.get("auto_completed_by_reason") or []
    if suppression_rows:
        lines.extend(
            [
                "",
                "### Auto-Completed Triggers",
                "",
                "| Trigger | Reason | Count |",
                "| --- | --- | ---: |",
            ]
        )
        for row in suppression_rows[:12]:
            lines.append(
                f"| {row.get('trigger_kind')} | {row.get('reason')} | "
                f"{row.get('total', 0)} |"
            )

    trigger_profile = downstream_suppression.get("trigger_profile") or []
    if trigger_profile:
        lines.extend(
            [
                "",
                "### Trigger Profile",
                "",
                "| Trigger | Total | Auto-Completed | Batches | Batched Members | Pending |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in trigger_profile:
            lines.append(
                f"| {row.get('trigger_kind')} | {row.get('total', 0)} | "
                f"{row.get('auto_completed', 0)} | {row.get('batches', 0)} | "
                f"{row.get('batched_members', 0)} | {row.get('pending', 0)} |"
            )
    return lines


def build_variance_report(report_root: Path, run_ids: list[str]) -> dict[str, Any]:
    run_summaries: list[dict[str, Any]] = []
    for run_id in run_ids:
        run_dir = report_root / run_id
        summary_path = run_dir / "storyline_scores.json"
        config_path = run_dir / "run_config.json"
        if not summary_path.exists():
            raise FileNotFoundError(f"Missing benchmark summary: {summary_path}")
        summary = json.loads(summary_path.read_text())
        config = json.loads(config_path.read_text()) if config_path.exists() else {}
        run_summaries.append(
            {
                "run_id": run_id,
                "signals": summary.get("signals"),
                "storyline_count": summary.get("storyline_count"),
                "elapsed_seconds": summary.get("elapsed_seconds"),
                "average_storyline_score": summary.get("average_storyline_score"),
                "company_intelligence_overall": (
                    (summary.get("company_intelligence_scorecard") or {}).get(
                        "overall_score"
                    )
                ),
                "product_value_overall": (
                    (
                        (summary.get("company_intelligence_scorecard") or {}).get(
                            "product_value_evals"
                        )
                        or {}
                    ).get("overall_score")
                ),
                "thesis_recovery_judge": summary.get("thesis_recovery_judge") or {},
                "run_config": config,
                "cache_bypass_env": (
                    config.get("cache_bypass_env") if isinstance(config, dict) else None
                ),
            }
        )

    metric_names = (
        "average_storyline_score",
        "company_intelligence_overall",
        "product_value_overall",
    )
    metrics = {
        name: _variance_metric(
            [run.get(name) for run in run_summaries if run.get(name) is not None]
        )
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
        sum((value - mean) ** 2 for value in numeric_values) / (len(numeric_values) - 1)
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
    margin = z * math.sqrt((phat * (1.0 - phat) + z**2 / (4 * n)) / n) / denominator
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
    lines.extend(
        [
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
        ]
    )
    return "\n".join(lines)


def run_variance_report(args: argparse.Namespace) -> dict[str, Any]:
    run_ids = list(args.variance_run_ids or [])
    report = build_variance_report(args.report_root, run_ids)
    output_id = args.run_id or (
        "storyline_variance_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    output_dir = args.report_root / output_id
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "variance_report.json", report)
    (output_dir / "variance_report.md").write_text(_render_variance_markdown(report))
    report["report_dir"] = str(output_dir)
    return report


def _nested_float(payload: dict[str, Any], path: tuple[str, ...]) -> float | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    if current is None:
        return None
    try:
        return float(current)
    except (TypeError, ValueError):
        return None


_LIVE_ONLY_PRODUCT_GAP_MARKERS = (
    "noise",
    "negative memory",
    "question policy",
    "latent bridge",
    "compression loss",
    "decision impact",
    "sage experience",
    "experience metabolism",
)


def _rerun_readiness_report(
    summary: dict[str, Any],
    *,
    min_product_value: float,
    min_eval_score: float,
) -> dict[str, Any]:
    scorecard = _json_obj(summary.get("company_intelligence_scorecard"))
    product_value = _json_obj(scorecard.get("product_value_evals"))
    evals = _json_obj(product_value.get("evals"))
    eval_scores: dict[str, float] = {}
    for key, payload in evals.items():
        if not isinstance(payload, dict):
            continue
        try:
            eval_scores[str(key)] = float(payload.get("score") or 0.0)
        except (TypeError, ValueError):
            eval_scores[str(key)] = 0.0
    product_overall = float(product_value.get("overall_score") or 0.0)
    low_eval_scores = {
        key: round(score, 4)
        for key, score in sorted(eval_scores.items())
        if score < min_eval_score
    }
    proof_gaps = [str(gap) for gap in _json_list(product_value.get("proof_gaps"))]
    live_only_gaps = [
        gap
        for gap in proof_gaps
        if any(marker in gap.lower() for marker in _LIVE_ONLY_PRODUCT_GAP_MARKERS)
    ]
    gate_failures: list[str] = []
    if summary.get("status") != "passed":
        gate_failures.append("source run status is not passed")
    required_failures = _json_list(summary.get("required_run_failures"))
    if required_failures:
        gate_failures.append("source run has required-run failures")
    if product_overall < min_product_value:
        gate_failures.append(
            "product-value overall below rerun floor: "
            f"{product_overall:.4f} < {min_product_value:.4f}"
        )
    if low_eval_scores:
        gate_failures.append(
            "product evals below rerun floor: "
            + ", ".join(f"{key}={value:.4f}" for key, value in low_eval_scores.items())
        )
    if summary.get("rerender", {}).get("artifact_only") and live_only_gaps:
        gate_failures.append(
            "artifact-only rerender still has live-only proof gaps; run a smaller "
            "DB/canary proof before spending a fresh 10-batch"
        )

    source_run_id = str(
        _json_obj(summary.get("rerender")).get("source_run_id")
        or summary.get("run_id")
        or "storyline-run"
    )
    targeted_db_command = (
        "DATABASE_URL=<postgres-url> .venv/bin/python -m pytest "
        "services/reasoning/think/tests/test_reason.py::"
        "test_think_noise_only_t1_fast_path_skips_retrieval_and_llm "
        "services/reasoning/think/tests/test_applier.py::"
        "test_question_policy_probe_feedback_reaches_policy_stats "
        "services/reasoning/think/tests/test_applier.py::"
        "test_noise_noop_negative_memory_emits_sage_experience_event "
        "-q --tb=short"
    )
    capability_noise_canary_command = (
        "RUN_REAL_LLM=1 LLM_PROVIDER=codex CODEX_TRANSPORT=cli "
        ".venv/bin/python scripts/run_storyline_batch_benchmark.py "
        f"--mode run --run-id {source_run_id}-capability-noise-canary "
        "--target-t1-batches 2 --horizon-start-batch 8 "
        "--signals-per-storyline 20 "
        "--future-validation-signals-per-storyline 3 --noise-signals 5 "
        "--min-efficiency-score 0.5 "
        "--max-background-maintenance-llm-calls 4"
    )
    return {
        "ready_for_fresh_10batch": not gate_failures,
        "gate_failures": gate_failures,
        "min_product_value": min_product_value,
        "min_eval_score": min_eval_score,
        "product_value_overall": round(product_overall, 4),
        "low_eval_scores": low_eval_scores,
        "live_only_product_proof_gaps": live_only_gaps,
        "recommended_next_step": (
            "fresh_10batch"
            if not gate_failures
            else "targeted_db_or_capability_noise_canary_before_10batch"
        ),
        "targeted_db_proof_command": targeted_db_command,
        "optional_small_canary_command": capability_noise_canary_command,
        "small_canary_scope_note": (
            "This 2-batch canary starts at horizon batch 8 to exercise "
            "capability_probe_wave_009 and background_noise_wave_010. It is a "
            "live health smoke for probe/noise routing, not a replacement for "
            "the targeted DB assertions or fresh 10-batch product proof."
        ),
    }


def rerender_existing_report(args: argparse.Namespace) -> dict[str, Any]:
    source_run_id = str(args.append_to_run_id or "")
    if not source_run_id:
        raise SystemExit("--mode rerender-report requires --append-to-run-id")
    source_dir = args.report_root / source_run_id
    model_summary = _read_json_obj(source_dir / "run_summary.json")
    if not model_summary:
        raise SystemExit(f"missing source run_summary.json for {source_run_id}")

    prior_summary = _read_json_obj(source_dir / "benchmark_summary.json")
    score_rows = prior_summary.get("storyline_scores")
    if not isinstance(score_rows, list):
        score_rows = _read_json_obj(source_dir / "storyline_scores.json").get(
            "storyline_scores"
        )
    if not isinstance(score_rows, list) or not score_rows:
        raise SystemExit(f"missing source storyline_scores for {source_run_id}")
    scores = [
        _storyline_score_from_artifact(row)
        for row in score_rows
        if isinstance(row, dict)
    ]
    if len(scores) != len(score_rows):
        raise SystemExit(f"invalid source storyline_scores for {source_run_id}")

    waves = _read_json_list(source_dir / "waves.json")
    if not waves:
        prior_waves = prior_summary.get("waves")
        waves = prior_waves if isinstance(prior_waves, list) else []

    elapsed_seconds = float(
        model_summary.get("elapsed_seconds")
        or prior_summary.get("elapsed_seconds")
        or 0.0
    )
    summary = _benchmark_summary(
        model_summary=model_summary,
        storyline_scores=scores,
        waves=waves,
        elapsed_seconds=elapsed_seconds,
    )

    output_id = args.run_id or (
        f"{source_run_id}-rerender-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    output_dir = args.report_root / output_id
    if output_dir.resolve() == source_dir.resolve():
        raise SystemExit("--run-id for rerender-report must differ from source run id")
    output_dir.mkdir(parents=True, exist_ok=True)

    source_company = _nested_float(
        prior_summary, ("company_intelligence_scorecard", "overall_score")
    )
    new_company = _nested_float(
        summary, ("company_intelligence_scorecard", "overall_score")
    )
    source_product = _nested_float(
        prior_summary,
        ("company_intelligence_scorecard", "product_value_evals", "overall_score"),
    )
    new_product = _nested_float(
        summary,
        ("company_intelligence_scorecard", "product_value_evals", "overall_score"),
    )
    summary["run_id"] = output_id
    summary["rerender"] = {
        "source_run_id": source_run_id,
        "source_report_dir": str(source_dir),
        "artifact_only": True,
        "postgres_used": False,
        "llm_used": False,
        "source_status": prior_summary.get("status"),
        "source_company_intelligence_overall": source_company,
        "rerendered_company_intelligence_overall": new_company,
        "company_intelligence_delta": (
            round(new_company - source_company, 4)
            if source_company is not None and new_company is not None
            else None
        ),
        "source_product_value_overall": source_product,
        "rerendered_product_value_overall": new_product,
        "product_value_delta": (
            round(new_product - source_product, 4)
            if source_product is not None and new_product is not None
            else None
        ),
    }
    summary["rerun_readiness"] = _rerun_readiness_report(
        summary,
        min_product_value=float(getattr(args, "rerun_min_product_value", 0.75)),
        min_eval_score=float(getattr(args, "rerun_min_product_eval_score", 0.6)),
    )
    run_config = _read_json_obj(source_dir / "run_config.json")
    run_config["mode"] = "rerender-report"
    run_config["run_id"] = output_id
    run_config["source_run_id"] = source_run_id
    _write_json(output_dir / "run_config.json", run_config)
    _write_json(output_dir / "benchmark_summary.json", summary)
    _write_json(output_dir / "storyline_scores.json", summary)
    (output_dir / "benchmark_summary.md").write_text(
        _render_benchmark_markdown(summary)
    )
    summary["report_dir"] = str(output_dir)
    return summary


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
                "storyline_id": _story_id_from_external_id(signal.get("external_id")),
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
    lines.extend(
        [
            "",
            "## Expected Behaviors",
            *[f"- {item}" for item in scenario.expected_behaviors],
            "",
        ]
    )
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
        "horizon_start_batch",
        "signals_per_storyline",
        "future_validation_signals_per_storyline",
        "noise_signals",
        "seed_models",
        "seed_families",
        "t1_batch_window_s",
        "t1_batch_min_size",
        "t1_batch_max_size",
        "t1_batch_retry_attempts",
        "downstream_batch_window_s",
        "downstream_batch_min_size",
        "t2_batch_max_size",
        "t4_batch_max_size",
        "downstream_steps_per_wave",
        "adaptive_drain_cycles",
        "adaptive_drain_steps_per_cycle",
        "post_commit_batch_size",
        "post_commit_batch_timeout",
        "skip_migrations",
        "skip_topology_optimizer",
        "allow_degraded_exit_zero",
        "min_efficiency_score",
        "max_background_maintenance_llm_calls",
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
        "question_planner_reflective_config": (
            _reflective_question_planner_env_config()
        ),
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


def _reflective_question_planner_env_config() -> dict[str, Any]:
    keys = (
        "INQUIRY_LLM_QUESTION_PLANNING_ENABLED",
        "INQUIRY_LLM_QUESTION_MAX_TOKENS",
        "INQUIRY_CODEX_QUESTION_MODEL",
        "INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS",
        "INQUIRY_CODEX_QUESTION_MAX_RETRIES",
        "INQUIRY_CODEX_COMPACT_QUESTION_SCHEMA",
        "INQUIRY_REFLECTIVE_RULES_ENABLED",
        "INQUIRY_REFLECTIVE_RULES_SHADOW_ONLY",
        "INQUIRY_REFLECTIVE_RULE_LIMIT",
        "INQUIRY_REFLECTIVE_RULE_MATCH_THRESHOLD",
        "INQUIRY_REFLECTIVE_RULE_SCORE_BOOST",
        "INQUIRY_REFLECTIVE_RULE_ATTRIBUTION_ENABLED",
        "INQUIRY_REFLECTIVE_RULE_LEARNING_ENABLED",
        "INQUIRY_REFLECTIVE_RULE_MAX_PROPOSALS",
        "INQUIRY_REFLECTIVE_RULE_PROMOTION_MIN_DELTA",
        "INQUIRY_REFLECTIVE_RULE_QUARANTINE_FAILURES",
        "INQUIRY_REFLECTIVE_RULE_QUARANTINE_UTILITY",
    )
    return {key: os.environ.get(key) for key in keys}


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
        choices=(
            "build-only",
            "seed-only",
            "retrieval-probe",
            "rerender-report",
            "run",
            "variance-report",
        ),
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
    parser.add_argument(
        "--future-validation-signals-per-storyline", type=int, default=3
    )
    parser.add_argument("--noise-signals", type=int, default=25)
    parser.add_argument("--seed-models", type=int, default=5000)
    parser.add_argument("--seed-families", type=int, default=100)
    parser.add_argument(
        "--keep-legacy-seed-postings",
        action="store_true",
        help=(
            "During synthetic seed, keep legacy compatibility posting triggers "
            "for model_semantic_term_postings and model_representation_tag_postings. "
            "Default is to seed only the current retrieval surfaces."
        ),
    )
    parser.add_argument(
        "--allow-seed-db-preflight-failures",
        action="store_true",
        help=(
            "Allow large seed runs to continue even when the DB preflight detects "
            "test-only triggers or large empty model-derived indexes."
        ),
    )
    parser.add_argument("--t1-batch-window-s", type=float, default=0.1)
    parser.add_argument("--t1-batch-min-size", type=int, default=20)
    parser.add_argument("--t1-batch-max-size", type=int, default=30)
    parser.add_argument(
        "--t1-batch-retry-attempts",
        type=int,
        default=1,
        help=(
            "Additional attempts for a required T1 batch when the first Think "
            "run fails with a retryable provider/transport error. The worker's "
            "trigger_max_attempts remains the hard dead-letter cap."
        ),
    )
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
    parser.add_argument(
        "--downstream-steps-per-wave",
        type=int,
        default=4,
        help=(
            "Bounded downstream T2/T3/T4 drain after each T1 wave. Keep this "
            "nonzero by default so production-readiness reports measure real "
            "backlog rather than intentionally skipped downstream work."
        ),
    )
    parser.add_argument(
        "--adaptive-drain-cycles",
        type=int,
        default=3,
        help=(
            "After all waves, cycle downstream Think drains, post-commit work, and "
            "topology optimization until adaptive work reaches quiescence or this "
            "cycle cap is reached."
        ),
    )
    parser.add_argument(
        "--adaptive-drain-steps-per-cycle",
        type=int,
        default=12,
        help=(
            "Maximum T2/T3/T4 Think dispatch steps to run in each final adaptive "
            "drain cycle."
        ),
    )
    parser.add_argument("--worker-poll-batch", type=int, default=6)
    parser.add_argument("--run-timeout", type=float, default=900.0)
    parser.add_argument("--post-commit-timeout", type=int, default=600)
    parser.add_argument(
        "--post-commit-batch-size",
        type=int,
        default=25,
        help=(
            "Maximum durable post-commit actions to process in one transaction. "
            "Keep this modest so final adaptive drain has visible progress and "
            "cannot pin the benchmark behind one giant batch."
        ),
    )
    parser.add_argument(
        "--post-commit-batch-timeout",
        type=float,
        default=60.0,
        help=(
            "Maximum seconds allowed for a single post-commit batch during final "
            "adaptive drain."
        ),
    )
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
    parser.add_argument(
        "--allow-degraded-exit-zero",
        action="store_true",
        help=(
            "For exploratory benchmark runs only: write a failed/degraded "
            "summary but still return process exit 0 in --mode run."
        ),
    )
    parser.add_argument(
        "--min-efficiency-score",
        type=float,
        default=0.5,
        help=(
            "Minimum Company Intelligence efficiency dimension required for "
            "--mode run to pass. Use 0 to disable the efficiency gate."
        ),
    )
    parser.add_argument(
        "--max-background-maintenance-llm-calls",
        type=int,
        default=-1,
        help=(
            "Maximum allowed T4/background-maintenance LLM calls for --mode run. "
            "Use -1 to disable the overhead gate."
        ),
    )
    parser.add_argument(
        "--rerun-min-product-value",
        type=float,
        default=0.75,
        help=(
            "Minimum artifact-only product-value overall score for the "
            "rerender-report readiness recommendation to allow a fresh 10-batch."
        ),
    )
    parser.add_argument(
        "--rerun-min-product-eval-score",
        type=float,
        default=0.6,
        help=(
            "Minimum per-product-eval score for the rerender-report readiness "
            "recommendation to allow a fresh 10-batch."
        ),
    )
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument(
        "--retrieval-probe-max-ms",
        type=float,
        default=1000.0,
        help=(
            "Maximum allowed latency for each retrieval hot-path probe query. "
            "Used only with --mode retrieval-probe."
        ),
    )
    parser.add_argument(
        "--retrieval-probe-model-limit",
        type=int,
        default=16,
        help="Model limit passed to each retrieval hot-path probe query.",
    )
    parser.add_argument(
        "--retrieval-probe-allow-missing-scope",
        action="store_true",
        help=(
            "Allow retrieval-probe to pass when the tenant has no scope sidecars. "
            "Use only for tiny local smoke tests; full retrieval gates should "
            "exercise scoped paths."
        ),
    )
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
        help=("Tenant id to append to when the base report does not contain one."),
    )
    parser.add_argument(
        "--horizon-start-batch",
        type=int,
        default=None,
        help=(
            "Absolute zero-based T1 batch offset for long-horizon generation. "
            "Append runs default to the base run's target_t1_batches."
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
    if args.retrieval_probe_max_ms <= 0:
        raise SystemExit("--retrieval-probe-max-ms must be > 0")
    if args.retrieval_probe_model_limit < 1:
        raise SystemExit("--retrieval-probe-model-limit must be positive")
    if args.adaptive_drain_cycles < 1:
        raise SystemExit("--adaptive-drain-cycles must be >= 1")
    if args.adaptive_drain_steps_per_cycle < 0:
        raise SystemExit("--adaptive-drain-steps-per-cycle must be >= 0")
    if args.post_commit_batch_size < 1:
        raise SystemExit("--post-commit-batch-size must be >= 1")
    if args.post_commit_batch_timeout <= 0:
        raise SystemExit("--post-commit-batch-timeout must be > 0")
    if args.t1_batch_retry_attempts < 0:
        raise SystemExit("--t1-batch-retry-attempts must be >= 0")
    if args.min_efficiency_score < 0.0 or args.min_efficiency_score > 1.0:
        raise SystemExit("--min-efficiency-score must be between 0 and 1")
    if args.max_background_maintenance_llm_calls < -1:
        raise SystemExit("--max-background-maintenance-llm-calls must be >= -1")
    if args.rerun_min_product_value < 0.0 or args.rerun_min_product_value > 1.0:
        raise SystemExit("--rerun-min-product-value must be between 0 and 1")
    if (
        args.rerun_min_product_eval_score < 0.0
        or args.rerun_min_product_eval_score > 1.0
    ):
        raise SystemExit("--rerun-min-product-eval-score must be between 0 and 1")
    if args.mode == "variance-report":
        if len(args.variance_run_ids or []) < 2:
            raise SystemExit(
                "--mode variance-report requires at least two --variance-run-ids"
            )
        return args
    if args.mode == "rerender-report":
        if not args.append_to_run_id:
            raise SystemExit("--mode rerender-report requires --append-to-run-id")
        if args.target_t1_batches != 0:
            raise SystemExit("--mode rerender-report requires --target-t1-batches 0")
        if args.cleanup:
            raise SystemExit("--cleanup is not allowed with rerender-report")
        return args
    if args.mode == "seed-only":
        if args.append_to_run_id:
            raise SystemExit("--mode seed-only cannot be combined with append mode")
        if args.target_t1_batches != 0:
            raise SystemExit("--mode seed-only requires --target-t1-batches 0")
    if args.mode == "retrieval-probe":
        if not args.append_to_run_id and not args.append_tenant_id:
            raise SystemExit(
                "--mode retrieval-probe requires --append-to-run-id "
                "or --append-tenant-id"
            )
        if args.target_t1_batches != 0:
            raise SystemExit("--mode retrieval-probe requires --target-t1-batches 0")
        if args.cleanup:
            raise SystemExit("--cleanup is not allowed with retrieval-probe")
    if args.mode == "run":
        if args.append_to_run_id and args.target_t1_batches <= 0:
            raise SystemExit("--append-to-run-id requires --target-t1-batches > 0")
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
    elif args.mode == "rerender-report":
        summary = rerender_existing_report(args)
    elif args.mode == "retrieval-probe":
        summary = await run_retrieval_probe(args)
    else:
        summary = await run_benchmark(args)
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    if args.mode == "rerender-report" and summary.get("status") != "passed":
        return 1
    if args.mode == "retrieval-probe" and summary.get("status") != "passed":
        return 1
    if (
        args.mode == "run"
        and not args.allow_degraded_exit_zero
        and summary.get("status") != "passed"
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
