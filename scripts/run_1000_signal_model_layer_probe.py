#!/usr/bin/env python3
"""Generate and run a configurable single-company model-layer probe.

This script is intentionally outside pytest: the goal is a durable scale
artifact that can be inspected later, not an ephemeral fixture rollback.

Default behavior:
  * create one synthetic tenant/company foundation,
  * inject thousands of diverse signals through production ingestion,
  * enqueue a configurable number of T1 triggers for Think,
  * run the live Think worker until drain or timeout,
  * export model-layer shape reports to tests/real_llm/reports/runs/.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.llm.provider import (
    LLMConfig,
    LLMConfigError,
    build_provider,
    set_response_cache,
    _codex_transport,
)
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.app.gateway.db_bootstrap import _register_codecs
from services.ingest.synthetic.core import SyntheticSignal, inject
from services.reasoning.think.post_commit import WorkerStats, process_batch
from services.workers.sage_topology_optimizer.worker import (
    run_once as run_topology_optimizer_once,
)
from tests.real_llm.infrastructure.durability_flow import run_think_until_drain
from tests.real_llm.infrastructure.response_cache import LLMResponseCache
from tests.real_llm.infrastructure.scenario_loader import (
    Scenario,
    _resolve_actor_ref,
    materialize,
)


SCENARIO_ID = "mega_single_company_e2e"
COMPANY_NAME = "AsterGrid Systems"

load_dotenv(REPO_ROOT / ".env", override=False)


COMPANY_PROFILE: dict[str, Any] = {
    "company_name": COMPANY_NAME,
    "operating_space": (
        "Enterprise operational-intelligence software for regulated B2B "
        "companies whose leaders need customer, product, security, revenue, "
        "and execution signals reconciled into reliable memory."
    ),
    "product": {
        "name": "AsterGrid MemoryOps",
        "category": "AI-native operating system for enterprise execution",
        "stage": "Series B, post-product-market fit, scaling enterprise deployments",
        "core_modules": [
            "signal ingestion",
            "organizational memory graph",
            "customer-risk bridge",
            "executive decision cockpit",
            "regulated-enterprise audit trail",
        ],
    },
    "financials": {
        "cash_in_hand_usd": 18_400_000,
        "monthly_burn_usd": 1_250_000,
        "runway_months": 14.7,
        "arr_usd": 12_800_000,
        "pipeline_usd": 9_600_000,
        "renewal_base_usd": 7_900_000,
    },
    "stage_constraints": [
        "enterprise controls must ship before renewal season",
        "implementation quality is limiting expansion",
        "support volume is rising faster than headcount",
        "data freshness incidents threaten trust in the product narrative",
    ],
    "employees_by_department": {
        "engineering": 42,
        "product_design": 12,
        "customer_success": 18,
        "sales": 16,
        "marketing": 7,
        "support": 11,
        "security_compliance": 6,
        "data_platform": 8,
        "finance_ops": 5,
        "people": 4,
        "executive": 5,
    },
    "board_priorities": [
        "protect renewal base",
        "prove AI memory produces hidden organizational insight",
        "extend runway without starving enterprise commitments",
        "reduce founder-mediated decision load",
    ],
}

DEPARTMENTS = list(COMPANY_PROFILE["employees_by_department"])


CUSTOMER_ALIASES: dict[str, list[str]] = {
    "Atlas Retail Group": ["ARG", "Atlas Retail", "Atlas"],
    "Borealis Bank": ["BBK", "Borealis"],
    "Cobalt Health Network": ["CHN", "Cobalt Health"],
    "DeltaFleet Logistics": ["DFL", "DeltaFleet"],
    "Evergreen Energy": ["EGE", "Evergreen"],
    "FoundryWorks Manufacturing": ["FWM", "FoundryWorks"],
    "Granite Insurance": ["GIC", "Granite"],
    "HarborRail Transit": ["HRT", "HarborRail"],
    "IonPay": ["Ion", "IPay"],
    "Jupiter Foods": ["JFO", "Jupiter"],
    "Keystone Robotics": ["KRB", "Keystone"],
    "Lumina Telecom": ["LTC", "Lumina"],
    "Meridian Public Sector": ["MPS", "Meridian"],
    "Northstar Labs": ["NSL", "Northstar"],
}

CUSTOMERS = list(CUSTOMER_ALIASES)

ACTORS: list[dict[str, Any]] = [
    {
        "name": "Maya Chen",
        "role": "CEO",
        "email": "maya@astergrid.example",
        "slack": "slack:maya",
        "email_alias": "email:maya@astergrid.example",
        "nexus_attested": True,
    },
    {
        "name": "Jon Bell",
        "role": "CTO",
        "email": "jon@astergrid.example",
        "slack": "slack:jon",
        "github": "github:jbell",
        "email_alias": "email:jon@astergrid.example",
        "nexus_attested": True,
    },
    {
        "name": "Priya Rao",
        "role": "VP Product",
        "email": "priya@astergrid.example",
        "slack": "slack:priya",
        "linear": "linear:priya",
        "email_alias": "email:priya@astergrid.example",
    },
    {
        "name": "Elena Voss",
        "role": "VP Customer Success",
        "email": "elena@astergrid.example",
        "slack": "slack:elena",
        "email_alias": "email:elena@astergrid.example",
    },
    {
        "name": "Marcus Li",
        "role": "CRO",
        "email": "marcus@astergrid.example",
        "slack": "slack:marcus",
        "email_alias": "email:marcus@astergrid.example",
    },
    {
        "name": "Rina Patel",
        "role": "SRE Lead",
        "email": "rina@astergrid.example",
        "slack": "slack:rina",
        "github": "github:rina-p",
        "email_alias": "email:rina@astergrid.example",
    },
    {
        "name": "Diego Alvarez",
        "role": "Security Lead",
        "email": "diego@astergrid.example",
        "slack": "slack:diego",
        "github": "github:dalvarez",
        "email_alias": "email:diego@astergrid.example",
    },
    {
        "name": "Nadia Brooks",
        "role": "Data Platform Lead",
        "email": "nadia@astergrid.example",
        "slack": "slack:nadia",
        "github": "github:nbrooks",
        "email_alias": "email:nadia@astergrid.example",
    },
    {
        "name": "Theo Martin",
        "role": "Support Director",
        "email": "theo@astergrid.example",
        "slack": "slack:theo",
        "email_alias": "email:theo@astergrid.example",
    },
    {
        "name": "Olivia Grant",
        "role": "Finance Lead",
        "email": "olivia@astergrid.example",
        "slack": "slack:olivia",
        "email_alias": "email:olivia@astergrid.example",
    },
    {
        "name": "Samir Haddad",
        "role": "Legal Counsel",
        "email": "samir@astergrid.example",
        "slack": "slack:samir",
        "email_alias": "email:samir@astergrid.example",
    },
    {
        "name": "Talia Morgan",
        "role": "Solutions Architect",
        "email": "talia@astergrid.example",
        "slack": "slack:talia",
        "email_alias": "email:talia@astergrid.example",
    },
    {
        "name": "Iris Wu",
        "role": "Account Executive",
        "email": "iris@astergrid.example",
        "slack": "slack:iris",
        "email_alias": "email:iris@astergrid.example",
    },
    {
        "name": "Noah Stone",
        "role": "Engineering Manager",
        "email": "noah@astergrid.example",
        "slack": "slack:noah",
        "github": "github:nstone",
        "linear": "linear:noah",
        "email_alias": "email:noah@astergrid.example",
    },
    {
        "name": "Ava Sinclair",
        "role": "Marketing Lead",
        "email": "ava@astergrid.example",
        "slack": "slack:ava",
        "email_alias": "email:ava@astergrid.example",
    },
    {
        "name": "Owen Park",
        "role": "Implementation Lead",
        "email": "owen@astergrid.example",
        "slack": "slack:owen",
        "email_alias": "email:owen@astergrid.example",
    },
    {
        "name": "Lena Ortiz",
        "role": "RevOps Lead",
        "email": "lena@astergrid.example",
        "slack": "slack:lena",
        "email_alias": "email:lena@astergrid.example",
    },
    {
        "name": "Ben Foster",
        "role": "QA Lead",
        "email": "ben@astergrid.example",
        "slack": "slack:ben",
        "github": "github:bfoster",
        "email_alias": "email:ben@astergrid.example",
    },
    {
        "name": "Hana Kim",
        "role": "Customer Success Manager",
        "email": "hana@astergrid.example",
        "slack": "slack:hana",
        "email_alias": "email:hana@astergrid.example",
    },
    {
        "name": "External Atlas Sponsor",
        "role": "Customer sponsor",
        "email": "sponsor@atlas.example",
        "email_alias": "email:sponsor@atlas.example",
    },
]


GOALS: list[dict[str, Any]] = [
    {
        "title": "Protect enterprise renewal base",
        "altitude": "strategic",
        "description": "Keep mission-critical customers healthy through renewal season.",
        "success_criteria": {"at_risk_arr_under_usd": 2_000_000},
        "target_days_from_start": 90,
    },
    {
        "title": "Make retrieval-grade memory reliable",
        "altitude": "strategic",
        "description": "Improve the signal-to-model path so hidden connections are surfaced.",
        "success_criteria": {"manual_research_hours_saved": 120},
        "target_days_from_start": 75,
    },
    {
        "title": "Ship regulated enterprise controls",
        "altitude": "strategic",
        "description": "Deliver audit, SAML, residency, and permission controls.",
        "success_criteria": {"tier1_blockers": 0},
        "target_days_from_start": 60,
    },
    {
        "title": "Reduce incident-driven churn",
        "altitude": "operational",
        "parent": "Protect enterprise renewal base",
        "description": "Cut repeat-severity incidents and postmortem leakage.",
        "success_criteria": {"repeat_p1_incidents": 0},
        "target_days_from_start": 45,
    },
    {
        "title": "Stabilize data freshness",
        "altitude": "operational",
        "parent": "Make retrieval-grade memory reliable",
        "description": "Make connectors, embeddings, and state-change propagation predictable.",
        "success_criteria": {"freshness_slo_minutes": 15},
        "target_days_from_start": 30,
    },
    {
        "title": "Improve expansion forecasting",
        "altitude": "operational",
        "parent": "Protect enterprise renewal base",
        "description": "Separate genuine expansion intent from noisy account chatter.",
        "success_criteria": {"forecast_slippage_percent": 15},
        "target_days_from_start": 80,
    },
    {
        "title": "Tighten support escalation loops",
        "altitude": "operational",
        "parent": "Reduce incident-driven churn",
        "description": "Close customer escalations with traceable owner and date.",
        "success_criteria": {"unowned_escalations": 0},
        "target_days_from_start": 25,
    },
    {
        "title": "Harden security review posture",
        "altitude": "operational",
        "parent": "Ship regulated enterprise controls",
        "description": "Make security evidence accessible before procurement asks.",
        "success_criteria": {"security_review_days": 5},
        "target_days_from_start": 50,
    },
    {
        "title": "Constrain bespoke commitments",
        "altitude": "operational",
        "description": "Avoid one-off work that weakens roadmap leverage.",
        "success_criteria": {"bespoke_work_percent": 20},
        "target_days_from_start": 65,
    },
    {
        "title": "Improve implementation throughput",
        "altitude": "operational",
        "description": "Move new customers from kickoff to useful usage faster.",
        "success_criteria": {"median_go_live_days": 21},
        "target_days_from_start": 70,
    },
]


COMMITMENTS: list[dict[str, Any]] = [
    {
        "title": "Deliver audit export v2",
        "owner": "Priya Rao",
        "state": "active",
        "due_days_from_start": 21,
        "priority": 1,
        "contributes_to_goal": ["Ship regulated enterprise controls"],
    },
    {
        "title": "Ship SAML group mapping",
        "owner": "Diego Alvarez",
        "state": "active",
        "due_days_from_start": 18,
        "priority": 1,
        "contributes_to_goal": ["Ship regulated enterprise controls"],
    },
    {
        "title": "Launch data residency controls",
        "owner": "Nadia Brooks",
        "state": "active",
        "due_days_from_start": 35,
        "priority": 1,
        "contributes_to_goal": ["Ship regulated enterprise controls"],
    },
    {
        "title": "Stabilize streaming connector lag",
        "owner": "Rina Patel",
        "state": "active",
        "due_days_from_start": 14,
        "priority": 1,
        "contributes_to_goal": ["Stabilize data freshness"],
    },
    {
        "title": "Resolve Atlas Retail Group renewal blockers",
        "owner": "Elena Voss",
        "state": "active",
        "due_days_from_start": 30,
        "priority": 1,
        "contributes_to_goal": ["Protect enterprise renewal base"],
    },
    {
        "title": "Recover Borealis Bank executive confidence",
        "owner": "Maya Chen",
        "state": "active",
        "due_days_from_start": 25,
        "priority": 1,
        "contributes_to_goal": ["Protect enterprise renewal base"],
    },
    {
        "title": "Close Cobalt Health Network security packet",
        "owner": "Diego Alvarez",
        "state": "active",
        "due_days_from_start": 17,
        "priority": 2,
        "contributes_to_goal": ["Harden security review posture"],
    },
    {
        "title": "Unblock DeltaFleet Logistics implementation",
        "owner": "Owen Park",
        "state": "active",
        "due_days_from_start": 20,
        "priority": 2,
        "contributes_to_goal": ["Improve implementation throughput"],
    },
    {
        "title": "Prepare Evergreen Energy data residency review",
        "owner": "Samir Haddad",
        "state": "active",
        "due_days_from_start": 29,
        "priority": 2,
        "contributes_to_goal": ["Ship regulated enterprise controls"],
    },
    {
        "title": "Repair FoundryWorks Manufacturing connector reliability",
        "owner": "Rina Patel",
        "state": "active",
        "due_days_from_start": 16,
        "priority": 1,
        "contributes_to_goal": ["Reduce incident-driven churn"],
    },
    {
        "title": "Rebuild Granite Insurance champion map",
        "owner": "Hana Kim",
        "state": "active",
        "due_days_from_start": 22,
        "priority": 3,
        "contributes_to_goal": ["Improve expansion forecasting"],
    },
    {
        "title": "Finish HarborRail Transit procurement evidence",
        "owner": "Samir Haddad",
        "state": "active",
        "due_days_from_start": 28,
        "priority": 2,
        "contributes_to_goal": ["Harden security review posture"],
    },
    {
        "title": "Validate IonPay latency remediation",
        "owner": "Ben Foster",
        "state": "active",
        "due_days_from_start": 12,
        "priority": 2,
        "contributes_to_goal": ["Reduce incident-driven churn"],
    },
    {
        "title": "Scope Jupiter Foods expansion forecast",
        "owner": "Marcus Li",
        "state": "active",
        "due_days_from_start": 33,
        "priority": 3,
        "contributes_to_goal": ["Improve expansion forecasting"],
    },
    {
        "title": "Contain Keystone Robotics custom workflow request",
        "owner": "Priya Rao",
        "state": "active",
        "due_days_from_start": 26,
        "priority": 3,
        "contributes_to_goal": ["Constrain bespoke commitments"],
    },
    {
        "title": "Close Lumina Telecom audit exception",
        "owner": "Diego Alvarez",
        "state": "active",
        "due_days_from_start": 19,
        "priority": 2,
        "contributes_to_goal": ["Ship regulated enterprise controls"],
    },
    {
        "title": "Prepare Meridian Public Sector launch plan",
        "owner": "Owen Park",
        "state": "active",
        "due_days_from_start": 32,
        "priority": 2,
        "contributes_to_goal": ["Improve implementation throughput"],
    },
    {
        "title": "Reprice Northstar Labs expansion package",
        "owner": "Olivia Grant",
        "state": "active",
        "due_days_from_start": 40,
        "priority": 4,
        "contributes_to_goal": ["Improve expansion forecasting"],
    },
    {
        "title": "Implement model graph edge quality telemetry",
        "owner": "Nadia Brooks",
        "state": "active",
        "due_days_from_start": 27,
        "priority": 1,
        "contributes_to_goal": ["Make retrieval-grade memory reliable"],
    },
    {
        "title": "Deploy stale replay guardrails",
        "owner": "Jon Bell",
        "state": "active",
        "due_days_from_start": 23,
        "priority": 2,
        "contributes_to_goal": ["Make retrieval-grade memory reliable"],
    },
    {
        "title": "Create executive risk digest",
        "owner": "Maya Chen",
        "state": "proposed",
        "due_days_from_start": 15,
        "priority": 3,
        "contributes_to_goal": ["Protect enterprise renewal base"],
    },
    {
        "title": "Refactor Salesforce duplicate account merge",
        "owner": "Lena Ortiz",
        "state": "active",
        "due_days_from_start": 24,
        "priority": 3,
        "contributes_to_goal": ["Improve expansion forecasting"],
    },
    {
        "title": "Publish SOC2 evidence room",
        "owner": "Diego Alvarez",
        "state": "active",
        "due_days_from_start": 10,
        "priority": 1,
        "contributes_to_goal": ["Harden security review posture"],
    },
    {
        "title": "Rewrite onboarding health playbook",
        "owner": "Elena Voss",
        "state": "active",
        "due_days_from_start": 38,
        "priority": 4,
        "contributes_to_goal": ["Improve implementation throughput"],
    },
    {
        "title": "Add permission audit trail to admin console",
        "owner": "Noah Stone",
        "state": "active",
        "due_days_from_start": 34,
        "priority": 2,
        "contributes_to_goal": ["Ship regulated enterprise controls"],
    },
    {
        "title": "Backfill contract metadata from billing",
        "owner": "Olivia Grant",
        "state": "active",
        "due_days_from_start": 36,
        "priority": 4,
        "contributes_to_goal": ["Make retrieval-grade memory reliable"],
    },
    {
        "title": "Clarify support severity language",
        "owner": "Theo Martin",
        "state": "active",
        "due_days_from_start": 13,
        "priority": 3,
        "contributes_to_goal": ["Tighten support escalation loops"],
    },
    {
        "title": "Build customer-visible incident timeline",
        "owner": "Rina Patel",
        "state": "active",
        "due_days_from_start": 31,
        "priority": 2,
        "contributes_to_goal": ["Reduce incident-driven churn"],
    },
]


DECISIONS: list[dict[str, Any]] = [
    {
        "title": "Enterprise controls remain the top renewal lever",
        "state": "active",
        "decision_text": "Prioritize audit export, SAML mapping, data residency, and permission audit work before exploratory AI UX work.",
        "rationale": "Renewal and procurement evidence repeatedly dominate expansion and churn risk.",
        "revisit_triggers": {"if_at_risk_arr_under_usd": 1_000_000},
    },
    {
        "title": "Do not accept unpriced bespoke workflow work",
        "state": "active",
        "decision_text": "Custom workflow requests need explicit ARR upside or reusable platform leverage.",
        "rationale": "Bespoke commitments have been hiding behind renewal pressure.",
    },
    {
        "title": "Use customer-facing timelines for repeat incidents",
        "state": "active",
        "decision_text": "Every repeat P1 or P2 incident gets a customer-visible timeline and owner.",
        "rationale": "Incident opacity is a stronger churn driver than the incident itself.",
    },
    {
        "title": "Treat data freshness as product quality",
        "state": "active",
        "decision_text": "Connector lag over the SLO is product risk, not internal ops noise.",
        "rationale": "Delayed memory creates false confidence in customer-facing workflows.",
    },
    {
        "title": "Escalate ambiguous aliases before graph mutation",
        "state": "active",
        "decision_text": "Ambiguous customer aliases should be resolved before strong model graph edges are written.",
        "rationale": "A wrong alias creates high-confidence retrieval pollution.",
    },
    {
        "title": "Prefer factual executive digest over narrative summary",
        "state": "drafted",
        "decision_text": "The executive digest should show linked evidence and deltas, not prose alone.",
        "rationale": "Leaders need inspectable connections under pressure.",
    },
    {
        "title": "Revenue-at-risk beats activity volume",
        "state": "active",
        "decision_text": "Prioritize escalations by ARR and commitment criticality, not message volume.",
        "rationale": "Noisy small accounts should not drown quiet strategic risk.",
    },
    {
        "title": "Bridge views should expose contested memory",
        "state": "drafted",
        "decision_text": "Customer and model pages should show contradiction and contestability, not hide it.",
        "rationale": "False cleanliness makes the system feel premium but less useful.",
    },
    {
        "title": "Forecast confidence requires evidence diversity",
        "state": "active",
        "decision_text": "Expansion forecasts need customer source, internal source, and product usage evidence.",
        "rationale": "Single-source enthusiasm has produced false positives.",
    },
    {
        "title": "Keep model graph edges first-class",
        "state": "active",
        "decision_text": "Hidden non-obvious connections should be represented as model_edges, not buried in model text.",
        "rationale": "Retrieval needs traversable structure, not just better summaries.",
    },
]


FAMILIES = [
    "customer_escalation",
    "incident",
    "support_ticket",
    "sales_pipeline",
    "security_review",
    "legal_procurement",
    "product_roadmap",
    "engineering_pr",
    "calendar_meeting",
    "finance_billing",
    "usage_telemetry",
    "implementation",
    "exec_decision",
    "alias_ambiguity",
    "contradiction",
    "stale_replay",
    "market_competitor",
    "forecast_update",
    "risk_digest",
    "cash_runway",
    "hiring_capacity",
    "board_update",
    "partner_integration",
    "compliance_regulatory",
    "people_ops",
    "noise",
]


CHANNEL_BY_FAMILY = {
    "customer_escalation": "slack:mega-customer-escalations",
    "incident": "pagerduty:mega-incidents",
    "support_ticket": "support:zendesk-mega",
    "sales_pipeline": "salesforce:mega-accounts",
    "security_review": "email:mega-security",
    "legal_procurement": "email:mega-legal",
    "product_roadmap": "linear:mega-roadmap",
    "engineering_pr": "github:astergrid/core",
    "calendar_meeting": "calendar:mega-exec",
    "finance_billing": "email:mega-finance",
    "usage_telemetry": "datadog:mega-product-usage",
    "implementation": "slack:mega-implementation",
    "exec_decision": "slack:mega-exec",
    "alias_ambiguity": "slack:mega-aliases",
    "contradiction": "slack:mega-contradictions",
    "stale_replay": "email:mega-replays",
    "market_competitor": "email:mega-market",
    "forecast_update": "salesforce:mega-forecast",
    "risk_digest": "slack:mega-risk",
    "cash_runway": "email:mega-finance",
    "hiring_capacity": "greenhouse:mega-recruiting",
    "board_update": "calendar:mega-board",
    "partner_integration": "slack:mega-partners",
    "compliance_regulatory": "email:mega-compliance",
    "people_ops": "email:mega-people",
    "noise": "slack:mega-general",
}


ACTOR_BY_FAMILY = {
    "customer_escalation": "Elena Voss",
    "incident": "Rina Patel",
    "support_ticket": "Theo Martin",
    "sales_pipeline": "Marcus Li",
    "security_review": "Diego Alvarez",
    "legal_procurement": "Samir Haddad",
    "product_roadmap": "Priya Rao",
    "engineering_pr": "Noah Stone",
    "calendar_meeting": "Maya Chen",
    "finance_billing": "Olivia Grant",
    "usage_telemetry": "Nadia Brooks",
    "implementation": "Owen Park",
    "exec_decision": "Maya Chen",
    "alias_ambiguity": "Lena Ortiz",
    "contradiction": "Jon Bell",
    "stale_replay": "Ben Foster",
    "market_competitor": "Ava Sinclair",
    "forecast_update": "Iris Wu",
    "risk_digest": "Hana Kim",
    "cash_runway": "Olivia Grant",
    "hiring_capacity": "Jon Bell",
    "board_update": "Maya Chen",
    "partner_integration": "Talia Morgan",
    "compliance_regulatory": "Diego Alvarez",
    "people_ops": "Hana Kim",
    "noise": "Ava Sinclair",
}


TRUST_BY_FAMILY = {
    "customer_escalation": "authoritative_external",
    "incident": "authoritative",
    "support_ticket": "authoritative_external",
    "sales_pipeline": "authoritative",
    "security_review": "authoritative",
    "legal_procurement": "authoritative",
    "product_roadmap": "authoritative",
    "engineering_pr": "authoritative",
    "calendar_meeting": "authoritative",
    "finance_billing": "authoritative",
    "usage_telemetry": "authoritative",
    "implementation": "inferential",
    "exec_decision": "authoritative",
    "alias_ambiguity": "inferential",
    "contradiction": "inferential",
    "stale_replay": "inferential",
    "market_competitor": "inferential",
    "forecast_update": "authoritative",
    "risk_digest": "inferential",
    "cash_runway": "authoritative",
    "hiring_capacity": "authoritative",
    "board_update": "authoritative",
    "partner_integration": "inferential",
    "compliance_regulatory": "authoritative",
    "people_ops": "authoritative",
    "noise": "inferential",
}


def build_scenario(signal_count: int, *, namespace: str) -> Scenario:
    foundation = {
        "company_profile": COMPANY_PROFILE,
        "actors": _namespaced_actors(namespace),
        "customers": _build_customers(),
        "goals": GOALS,
        "commitments": COMMITMENTS,
        "decisions": DECISIONS,
        "customer_commitments": _build_customer_commitments(),
    }
    signal_sequences = _build_signal_sequences(signal_count)
    return Scenario(
        scenario_id=SCENARIO_ID,
        name=f"{COMPANY_NAME} mega signal probe",
        description=(
            "One-company corpus for stress-testing ingestion, retrieval, "
            "Think, and model graph shape under production-like chaos."
        ),
        foundation=foundation,
        signal_sequences=signal_sequences,
        expected_behaviors=[
            "All generated signals inject through production ingestion.",
            "Think creates scoped models instead of unscoped global memory.",
            "Customer, commitment, incident, and decision evidence remains connected.",
            "Noisy and stale signals do not dominate active model creation.",
            "Financial and department-capacity context appears in derived memory.",
            "The topology layer surfaces hidden cross-functional relationships.",
        ],
        raw={
            "generated": True,
            "signal_count": signal_count,
            "company_profile": COMPANY_PROFILE,
        },
    )


def _namespaced_actors(namespace: str) -> list[dict[str, Any]]:
    slug = "".join(ch if ch.isalnum() else "-" for ch in namespace.lower()).strip("-")
    slug = slug[:48] or "mega"
    actors: list[dict[str, Any]] = []
    for actor in ACTORS:
        copy = dict(actor)
        email = copy.get("email")
        if isinstance(email, str) and "@" in email:
            local, domain = email.split("@", 1)
            copy["email"] = f"{local}+{slug}@{domain}"
        for field_name in ("slack", "github", "email_alias", "linear"):
            value = copy.get(field_name)
            if not isinstance(value, str) or ":" not in value:
                continue
            channel, ref = value.split(":", 1)
            copy[field_name] = f"{channel}:{slug}-{ref}"
        actors.append(copy)
    return actors


def _build_customers() -> list[dict[str, Any]]:
    health_cycle = [
        "healthy",
        "at_risk",
        "watch",
        "healthy",
        "watch",
        "at_risk",
        "healthy",
    ]
    base_arr = [
        1_850_000,
        2_600_000,
        1_400_000,
        920_000,
        1_100_000,
        760_000,
        1_300_000,
        880_000,
        690_000,
        540_000,
        1_050_000,
        2_200_000,
        1_720_000,
        430_000,
    ]
    return [
        {
            "name": customer,
            "description": f"{customer} is an enterprise customer of {COMPANY_NAME}.",
            "arr_usd": base_arr[i],
            "health": health_cycle[i % len(health_cycle)],
            "contract_start_days_ago": 30 + i * 18,
        }
        for i, customer in enumerate(CUSTOMERS)
    ]


def _build_customer_commitments() -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    customer_specific = [
        ("Atlas Retail Group", "Resolve Atlas Retail Group renewal blockers"),
        ("Borealis Bank", "Recover Borealis Bank executive confidence"),
        ("Cobalt Health Network", "Close Cobalt Health Network security packet"),
        ("DeltaFleet Logistics", "Unblock DeltaFleet Logistics implementation"),
        ("Evergreen Energy", "Prepare Evergreen Energy data residency review"),
        (
            "FoundryWorks Manufacturing",
            "Repair FoundryWorks Manufacturing connector reliability",
        ),
        ("Granite Insurance", "Rebuild Granite Insurance champion map"),
        ("HarborRail Transit", "Finish HarborRail Transit procurement evidence"),
        ("IonPay", "Validate IonPay latency remediation"),
        ("Jupiter Foods", "Scope Jupiter Foods expansion forecast"),
        (
            "Keystone Robotics",
            "Contain Keystone Robotics custom workflow request",
        ),
        ("Lumina Telecom", "Close Lumina Telecom audit exception"),
        ("Meridian Public Sector", "Prepare Meridian Public Sector launch plan"),
        ("Northstar Labs", "Reprice Northstar Labs expansion package"),
    ]
    shared = [
        "Deliver audit export v2",
        "Ship SAML group mapping",
        "Launch data residency controls",
        "Stabilize streaming connector lag",
        "Publish SOC2 evidence room",
    ]
    for i, (customer, commitment) in enumerate(customer_specific):
        links.append(
            {
                "customer": customer,
                "commitment": commitment,
                "criticality": "must_have" if i < 4 else "high",
                "revenue_at_risk_usd": 300_000 + i * 55_000,
            }
        )
        shared_commitment = shared[i % len(shared)]
        links.append(
            {
                "customer": customer,
                "commitment": shared_commitment,
                "criticality": "high" if i % 3 == 0 else "medium",
                "revenue_at_risk_usd": 120_000 + i * 30_000,
            }
        )
    return links


def _build_signal_sequences(signal_count: int) -> dict[str, list[dict[str, Any]]]:
    sequences: dict[str, list[dict[str, Any]]] = {}
    for index in range(signal_count):
        window = index // 50
        seq_name = f"window_{window + 1:02d}_mixed_operations"
        sequences.setdefault(seq_name, []).append(_make_signal(index, window))
    return sequences


def _make_signal(index: int, window: int) -> dict[str, Any]:
    family = FAMILIES[index % len(FAMILIES)]
    customer = CUSTOMERS[(index * 7 + window) % len(CUSTOMERS)]
    secondary = CUSTOMERS[(index * 11 + 3) % len(CUSTOMERS)]
    alias = CUSTOMER_ALIASES[customer][index % len(CUSTOMER_ALIASES[customer])]
    commitment = _commitment_for_customer(customer, index)
    related_goal = GOALS[index % len(GOALS)]["title"]
    decision = DECISIONS[index % len(DECISIONS)]["title"]
    department = DEPARTMENTS[(index * 5 + window) % len(DEPARTMENTS)]
    department_headcount = int(
        COMPANY_PROFILE["employees_by_department"][department]
    )
    severity = ["P0", "P1", "P2", "P3"][index % 4]
    arr = 250_000 + (index % 13) * 75_000
    cash_impact = 25_000 + (index % 29) * 18_000
    week = 1 + index // 140
    actor = ACTOR_BY_FAMILY[family]
    channel = CHANNEL_BY_FAMILY[family]
    trust = TRUST_BY_FAMILY[family]
    delay = float(index * 6 + (index % 3))
    text = _signal_text(
        index=index,
        family=family,
        customer=customer,
        secondary=secondary,
        alias=alias,
        commitment=commitment,
        goal=related_goal,
        decision=decision,
        department=department,
        department_headcount=department_headcount,
        severity=severity,
        arr=arr,
        cash_impact=cash_impact,
        week=week,
    )
    content_dict = {
        "text": text,
        "company": COMPANY_NAME,
        "family": family,
        "signal_index": index,
        "week": week,
        "customer_name": customer,
        "customer_alias": alias,
        "secondary_customer_name": secondary,
        "commitment_title": commitment,
        "goal_title": related_goal,
        "decision_title": decision,
        "department": department,
        "department_headcount": department_headcount,
        "severity": severity,
        "arr_usd": arr,
        "cash_impact_usd": cash_impact,
        "cash_in_hand_usd": COMPANY_PROFILE["financials"]["cash_in_hand_usd"],
        "runway_months": COMPANY_PROFILE["financials"]["runway_months"],
        "product_stage": COMPANY_PROFILE["product"]["stage"],
        "entity_names": {
            "customers": [customer, secondary] if index % 9 == 0 else [customer],
            "commitments": [commitment],
            "goals": [related_goal],
            "decisions": [decision],
        },
    }
    signal: dict[str, Any] = {
        "channel": channel,
        "actor": actor,
        "delay_minutes": delay,
        "content": text,
        "content_dict": content_dict,
        "trust_tier": trust,
        "external_id": f"{SCENARIO_ID}:{index:04d}",
    }
    index_in_sequence = index % 50
    if index_in_sequence > 0 and index % 17 == 0:
        signal["thread_of"] = index_in_sequence - 1
    return signal


def _commitment_for_customer(customer: str, index: int) -> str:
    for commitment in COMMITMENTS:
        title = commitment["title"]
        if customer in title:
            return title
    return COMMITMENTS[index % len(COMMITMENTS)]["title"]


def _signal_text(
    *,
    index: int,
    family: str,
    customer: str,
    secondary: str,
    alias: str,
    commitment: str,
    goal: str,
    decision: str,
    department: str,
    department_headcount: int,
    severity: str,
    arr: int,
    cash_impact: int,
    week: int,
) -> str:
    financials = COMPANY_PROFILE["financials"]
    product = COMPANY_PROFILE["product"]
    base = (
        f"[{COMPANY_NAME} signal {index:04d} week {week}] "
        f"{customer} ({alias}) relates to {commitment}. "
        f"Company context: {product['name']} is at {product['stage']} with "
        f"${financials['cash_in_hand_usd']:,} cash, "
        f"{financials['runway_months']} months runway, and "
        f"{department_headcount} people in {department}. "
    )
    tails = {
        "customer_escalation": (
            f"External sponsor says {severity} renewal confidence is dropping; "
            f"blocked evidence now puts roughly ${arr:,} ARR at risk. "
            f"They referenced {decision} and asked for a named owner today."
        ),
        "incident": (
            f"{severity} incident repeats in the ingestion freshness path. "
            f"{customer} saw delayed dashboard state, while {secondary} is a possible "
            "adjacent blast-radius account. Postmortem needs a falsifier for repeat risk."
        ),
        "support_ticket": (
            f"Ticket says {alias} hit a permission edge case after SAML setup. "
            "Support cannot tell whether this is onboarding confusion or a product gap."
        ),
        "sales_pipeline": (
            f"Salesforce changed {customer} renewal stage and expansion amount by ${arr:,}. "
            f"The champion is warm but procurement cites {goal} as the hard blocker."
        ),
        "security_review": (
            f"Security review asks for SOC2, audit export, SAML mapping, and data residency "
            f"evidence before {alias} will approve the renewal packet."
        ),
        "legal_procurement": (
            f"Procurement redline from {customer}: liability cap and data processing terms "
            "are acceptable only if the audit trail is customer-visible."
        ),
        "product_roadmap": (
            f"Roadmap comment says {commitment} is reusable for {customer}, but a bespoke "
            f"variant requested by {alias} may conflict with {decision}."
        ),
        "engineering_pr": (
            f"GitHub PR links connector lag remediation to {customer}; tests mention stale "
            "replay guardrails, graph edge telemetry, and customer-visible incident timelines."
        ),
        "calendar_meeting": (
            f"Exec meeting notes: {customer} renewal review, {secondary} adjacent risk, "
            f"and decision '{decision}' need one consolidated model trail."
        ),
        "finance_billing": (
            f"Billing note: {customer} has an invoice dispute and asks whether the "
            f"${arr:,} credit should be tied to the {commitment} remediation."
        ),
        "usage_telemetry": (
            f"Telemetry shows {customer} active seats changed {3 + index % 17}% while "
            "workflow completion dropped; this may contradict sales confidence."
        ),
        "implementation": (
            f"Implementation status for {customer}: integration owner changed, kickoff "
            "dependency slipped, and onboarding health should be downgraded unless evidence improves."
        ),
        "exec_decision": (
            f"Leadership says {decision} still stands for {customer}; exceptions require "
            "clear reusable leverage and explicit revenue-at-risk."
        ),
        "alias_ambiguity": (
            f"Ambiguous mention: '{alias}' could refer to {customer}, but a note also mentions "
            f"{secondary}. Resolve before creating a strong customer edge."
        ),
        "contradiction": (
            f"Contradiction: customer success marks {customer} at risk, while sales marks "
            f"{alias} as expansion-qualified. Need evidence diversity before forecast confidence rises."
        ),
        "stale_replay": (
            f"Stale replay detected for {customer}: an old connector alert repeats after "
            "remediation. Treat as low-confidence unless confirmed by fresh telemetry."
        ),
        "market_competitor": (
            f"Competitor displacement note: {customer} compared AsterGrid to a vendor with "
            "faster audit evidence but weaker workflow memory."
        ),
        "forecast_update": (
            f"Forecast update: {customer} moved renewal probability by {index % 23 + 5} points; "
            f"expansion depends on closing {commitment}."
        ),
        "risk_digest": (
            f"Risk digest ties {customer}, {secondary}, {goal}, and {commitment}; "
            "the non-obvious connection is that procurement anxiety and incident opacity are merging."
        ),
        "cash_runway": (
            f"Finance update: {department} spend changed by ${cash_impact:,}; "
            f"runway sensitivity now connects {commitment}, renewal timing, "
            "and whether enterprise controls can ship before the next board meeting."
        ),
        "hiring_capacity": (
            f"Capacity plan says {department} has {department_headcount} people but "
            f"needs {2 + index % 5} more to protect {goal}. Recruiting tradeoff "
            "could slow customer-facing remediation if cash burn stays fixed."
        ),
        "board_update": (
            f"Board prep asks whether {decision} is still valid given {customer} "
            f"risk, ${arr:,} ARR exposure, current runway, and product-stage pressure."
        ),
        "partner_integration": (
            f"Partner integration note: {customer} depends on an ecosystem connector "
            f"owned by {department}; the delay may quietly weaken {secondary}'s "
            "implementation confidence too."
        ),
        "compliance_regulatory": (
            f"Regulatory update: audit evidence and data residency controls for "
            f"{customer} now affect procurement, legal review, and {department} "
            "capacity in the same operating loop."
        ),
        "people_ops": (
            f"People ops signal: {department} attrition risk is rising after repeated "
            f"{severity} escalation load. This may explain slips on {commitment} "
            "and should not be treated as isolated morale noise."
        ),
        "noise": (
            f"General chatter mentions {alias} in passing with lunch logistics and no actionable "
            "customer evidence. This should not dominate model creation."
        ),
    }
    return base + tails[family]


def _resolve_entities_hint(scenario: Scenario, signal_def: dict[str, Any]) -> list[dict[str, str]]:
    entity_names = dict((signal_def.get("content_dict") or {}).get("entity_names") or {})
    hints: list[dict[str, str]] = []
    for name in entity_names.get("customers") or []:
        if name in scenario.customers:
            hints.append({"type": "customer", "id": str(scenario.customer_id(name))})
    for title in entity_names.get("commitments") or []:
        if title in scenario.commitments:
            hints.append({"type": "commitment", "id": str(scenario.commitment_id(title))})
    for title in entity_names.get("goals") or []:
        if title in scenario.goals:
            hints.append({"type": "goal", "id": str(scenario.goal_id(title))})
    for title in entity_names.get("decisions") or []:
        if title in scenario.decisions:
            hints.append({"type": "decision", "id": str(scenario.decision_id(title))})
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for hint in hints:
        key = (hint["type"], hint["id"])
        if key not in seen:
            seen.add(key)
            deduped.append(hint)
    return deduped


async def _insert_extra_aliases(scenario: Scenario, alias_repo: EntityAliasRepo) -> None:
    assert scenario.tenant_id is not None
    for customer, aliases in CUSTOMER_ALIASES.items():
        ref = {"type": "customer", "id": str(scenario.customer_id(customer))}
        for alias in aliases:
            await alias_repo.insert_alias(
                phrase=alias,
                resolved_entity_ref=ref,
                source="manual",
                confidence=0.92,
                tenant_id=scenario.tenant_id,
                extra_metadata={"scenario_id": scenario.scenario_id},
            )


async def inject_generated_signals(
    scenario: Scenario,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    run_id: str,
    progress_every: int,
    offset: int = 0,
    limit: int | None = None,
) -> list[UUID]:
    if scenario.tenant_id is None:
        raise RuntimeError("scenario must be materialized")
    if offset < 0:
        raise ValueError("offset cannot be negative")
    base = scenario.base_time or datetime.now(timezone.utc)
    observation_ids: list[UUID] = []
    all_signals = [
        signal for sequence in scenario.signal_sequences.values() for signal in sequence
    ]
    selected_signals = (
        all_signals[offset:] if limit is None else all_signals[offset:offset + limit]
    )
    started = time.monotonic()
    for index, signal_def in enumerate(selected_signals, start=offset + 1):
        content_text = str(signal_def.get("content") or signal_def.get("text") or "")
        content_dict = dict(signal_def.get("content_dict") or {})
        content_dict.setdefault("text", content_text)
        occurred_at = base + timedelta(minutes=float(signal_def.get("delay_minutes", 0)))
        signal = SyntheticSignal(
            source_channel=str(signal_def["channel"]),
            content_text=content_text,
            content=content_dict,
            occurred_at=occurred_at,
            source_actor_ref=_resolve_actor_ref(signal_def.get("actor"), scenario),
            external_id=f"{run_id}:{signal_def.get('external_id') or index}",
            entities_hint=_resolve_entities_hint(scenario, signal_def),
            trust_tier=signal_def.get("trust_tier"),
            kind=signal_def.get("kind", "signal"),
            scenario_id=scenario.scenario_id,
            run_id=run_id,
        )
        result = await inject(
            signal,
            scenario.tenant_id,
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            skip_t1_enqueue=True,
        )
        observation_ids.append(result.observation.id)
        if progress_every and index % progress_every == 0:
            elapsed = time.monotonic() - started
            print(
                f"injected {index}/{len(all_signals)} signals "
                f"({elapsed:.1f}s elapsed)",
                flush=True,
            )
    return observation_ids


async def enqueue_t1_for_observations(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_ids: list[UUID],
    limit: int,
    run_id: str,
) -> int:
    selected = observation_ids[:limit]
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, source_channel, kind, trust_tier, occurred_at, content_text,
                   entities_mentioned, actor_id
            FROM observations
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            ORDER BY occurred_at ASC, id ASC
            """,
            tenant_id,
            selected,
        )
        for row in rows:
            payload = {
                "source_channel": row["source_channel"],
                "kind": row["kind"],
                "trust_tier": row["trust_tier"],
                "seed_occurred_at": row["occurred_at"].isoformat(),
                "seed_natural_text": (row["content_text"] or "")[:2000],
                "seed_entity_ids": row["entities_mentioned"] or [],
                "scope_actors": [str(row["actor_id"])] if row["actor_id"] else [],
                "mega_probe": {"run_id": run_id},
            }
            await conn.execute(
                """
                INSERT INTO think_trigger_queue (
                    id, tenant_id, trigger_kind, trigger_subkind,
                    observation_id, model_id, payload
                ) VALUES (
                    $1, $2, 'T1', 'event_arrival', $3, NULL, $4::jsonb
                )
                """,
                uuid7(),
                tenant_id,
                row["id"],
                json.dumps(payload, default=str),
            )
    return len(rows)


async def drain_post_commit_actions(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    timeout_seconds: int,
    batch_size: int = 250,
) -> dict[str, int]:
    """Drain durable post-commit actions for this tenant."""
    deadline = time.monotonic() + timeout_seconds
    stats = WorkerStats()
    while True:
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
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
        if int(pending or 0) == 0:
            return {
                "processed": stats.processed,
                "failed": stats.failed,
                "dead_lettered": stats.dead_lettered,
                "iterations": stats.iterations,
            }
        if time.monotonic() >= deadline:
            return {
                "processed": stats.processed,
                "failed": stats.failed,
                "dead_lettered": stats.dead_lettered,
                "iterations": stats.iterations,
                "timed_out": 1,
            }
        await process_batch(
            pool,
            limit=batch_size,
            stats=stats,
            tenant_id=tenant_id,
        )
        await asyncio.sleep(0.05)


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


async def drain_topology_optimizer(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    timeout_seconds: int,
    batch_size: int = 250,
    lookback_hours: int = 72,
) -> dict[str, Any]:
    """Run topology optimization until newly completed sessions are consumed."""
    if timeout_seconds <= 0:
        return {
            "status": "skipped",
            "reason": "timeout_seconds<=0",
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "iterations": 0,
            "metrics": {},
        }

    deadline = time.monotonic() + timeout_seconds
    totals: dict[str, Any] = {
        "status": "drained",
        "processed": 0,
        "completed": 0,
        "failed": 0,
        "iterations": 0,
        "metrics": {},
    }
    while True:
        if time.monotonic() >= deadline:
            totals["status"] = "timeout"
            totals["timed_out"] = 1
            return totals

        report = await run_topology_optimizer_once(
            pool,
            tenant_id=tenant_id,
            lookback_hours=lookback_hours,
            limit=batch_size,
        )
        totals["iterations"] = int(totals["iterations"]) + 1
        totals["processed"] = int(totals["processed"]) + report.processed
        totals["completed"] = int(totals["completed"]) + report.completed
        totals["failed"] = int(totals["failed"]) + report.failed

        metrics = totals.setdefault("metrics", {})
        for session in report.sessions:
            for key, value in (session.metrics or {}).items():
                if isinstance(value, bool):
                    continue
                if isinstance(value, int):
                    metrics[key] = int(metrics.get(key) or 0) + value
                elif isinstance(value, float):
                    metrics[key] = float(metrics.get(key) or 0.0) + value

        if report.processed == 0:
            return totals
        await asyncio.sleep(0.05)


async def collect_model_layer_report(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    run_id: str,
    report_dir: Path,
    scenario: Scenario,
    observation_ids: list[UUID],
    think_status: str,
    run_config: dict[str, Any],
    seed_status: dict[str, Any] | None,
    processing_waves: list[dict[str, Any]],
    post_commit_status: dict[str, Any],
    topology_optimizer_status: dict[str, Any],
    elapsed_seconds: float,
) -> dict[str, Any]:
    report_dir.mkdir(parents=True, exist_ok=True)
    async with pool.acquire() as conn:
        summary = {
            "tenant_id": str(tenant_id),
            "run_id": run_id,
            "scenario_id": scenario.scenario_id,
            "company_profile": scenario.raw.get("company_profile") or {},
            "run_config": run_config,
            "seed_status": seed_status or {},
            "signal_count": len(observation_ids),
            "think_status": think_status,
            "processing_waves": processing_waves,
            "post_commit_status": post_commit_status,
            "topology_optimizer_status": topology_optimizer_status,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "observation_count": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM observations
                WHERE tenant_id = $1
                  AND content->>'run_id' = $2
                """,
                tenant_id,
                run_id,
            ),
            "observations_with_entities": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM observations
                WHERE tenant_id = $1
                  AND content->>'run_id' = $2
                  AND jsonb_typeof(entities_mentioned) = 'array'
                  AND jsonb_array_length(entities_mentioned) > 0
                """,
                tenant_id,
                run_id,
            ),
            "active_models": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM models WHERE tenant_id = $1 AND status = 'active'",
                tenant_id,
            ),
            "archived_models": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM models WHERE tenant_id = $1 AND status = 'archived'",
                tenant_id,
            ),
            "model_edges": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM model_edges WHERE tenant_id = $1",
                tenant_id,
            ),
            "active_model_edges": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM model_edges WHERE tenant_id = $1 AND status = 'active'",
                tenant_id,
            ),
            "relationship_candidates": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM relationship_candidates WHERE tenant_id = $1",
                tenant_id,
            ),
            "latent_topology_candidates": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM relationship_candidates
                WHERE tenant_id = $1
                  AND source = 'latent_topology'
                """,
                tenant_id,
            ),
            "relationship_candidate_think_triggers": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND trigger_kind = 'T4'
                  AND trigger_subkind = 'latent_relationship_candidate'
                """,
                tenant_id,
            ),
            "model_scope_entity_sidecars": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM model_scope_entities WHERE tenant_id = $1",
                tenant_id,
            ),
            "model_scope_actor_sidecars": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM model_scope_actors WHERE tenant_id = $1",
                tenant_id,
            ),
            "state_changes": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM observations
                WHERE tenant_id = $1
                  AND kind = 'state_change'
                """,
                tenant_id,
            ),
            "think_runs_success": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM think_runs WHERE tenant_id = $1 AND status = 'success'",
                tenant_id,
            ),
            "think_runs_failed": await conn.fetchval(
                "SELECT COUNT(*)::bigint FROM think_runs WHERE tenant_id = $1 AND status = 'failed'",
                tenant_id,
            ),
            "pending_triggers": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                """,
                tenant_id,
            ),
            "pending_post_commit_actions": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM pending_post_commit_actions
                WHERE tenant_id = $1
                  AND processed_at IS NULL
                """,
                tenant_id,
            ),
            "dead_lettered_post_commit_actions": await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM pending_post_commit_actions
                WHERE tenant_id = $1
                  AND dead_lettered_at IS NOT NULL
                """,
                tenant_id,
            ),
        }
        summary["channel_family_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT split_part(source_channel, ':', 1) AS key, COUNT(*)::bigint AS value
            FROM observations
            WHERE tenant_id = $1
              AND content->>'run_id' = $2
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
            run_id,
        )
        summary["signal_family_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT content->>'family' AS key, COUNT(*)::bigint AS value
            FROM observations
            WHERE tenant_id = $1
              AND content->>'run_id' = $2
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
            run_id,
        )
        summary["trust_tier_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT trust_tier AS key, COUNT(*)::bigint AS value
            FROM observations
            WHERE tenant_id = $1
              AND content->>'run_id' = $2
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
            run_id,
        )
        summary["model_kind_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT COALESCE(proposition_kind, '<none>') AS key, COUNT(*)::bigint AS value
            FROM models
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["model_status_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT status AS key, COUNT(*)::bigint AS value
            FROM models
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["model_scope_entity_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT COALESCE(e.value->>'type', '<none>') AS key, COUNT(DISTINCT m.id)::bigint AS value
            FROM models m
            LEFT JOIN LATERAL jsonb_array_elements(COALESCE(m.scope_entities, '[]'::jsonb)) e(value) ON true
            WHERE m.tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["edge_kind_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT edge_kind AS key, COUNT(*)::bigint AS value
            FROM model_edges
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["edge_review_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT review_status AS key, COUNT(*)::bigint AS value
            FROM model_edges
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["relationship_candidate_kind_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT candidate_kind AS key, COUNT(*)::bigint AS value
            FROM relationship_candidates
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["relationship_candidate_status_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT review_status AS key, COUNT(*)::bigint AS value
            FROM relationship_candidates
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["topology_object_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT COALESCE(metadata->'topology'->>'object_type', '<none>') AS key,
                   COUNT(*)::bigint AS value
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND source = 'latent_topology'
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["topology_optimizer_run_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT status AS key, COUNT(*)::bigint AS value
            FROM sage_topology_optimizer_runs
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        summary["context_use_distribution"] = await _fetch_distribution(
            conn,
            """
            SELECT COALESCE(ops_applied->'context_use'->>'context_use_grade', '<none>') AS key,
                   COUNT(*)::bigint AS value
            FROM think_runs
            WHERE tenant_id = $1
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC
            """,
            tenant_id,
        )
        row = await conn.fetchrow(
            """
            WITH contexts AS (
              SELECT ops_applied->'context_use' AS context
              FROM think_runs
              WHERE tenant_id = $1
                AND status = 'success'
                AND ops_applied ? 'context_use'
            )
            SELECT
              COUNT(*)::bigint AS context_use_runs,
              COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(context->>'graph_selected_model_count', '')::int, 0) > 0
              )::bigint AS graph_selected_runs,
              COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(context->>'graph_relation_op_count', '')::int, 0) > 0
              )::bigint AS graph_relation_op_runs,
              COUNT(*) FILTER (
                WHERE COALESCE((context->>'graph_no_edge_rationale_present')::boolean, false)
              )::bigint AS graph_no_edge_rationale_runs,
              COUNT(*) FILTER (
                WHERE COALESCE((context->>'graph_selected_without_relation_ops')::boolean, false)
              )::bigint AS graph_selected_without_relation_ops_runs,
              COUNT(*) FILTER (
                WHERE COALESCE((context->>'graph_relation_contract_satisfied')::boolean, false)
              )::bigint AS graph_relation_contract_satisfied_runs,
              COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(context->>'graph_selected_model_count', '')::int, 0) > 0
                  AND NOT COALESCE((context->>'graph_relation_contract_satisfied')::boolean, false)
              )::bigint AS graph_relation_contract_failed_runs
            FROM contexts
            """,
            tenant_id,
        )
        summary["context_use_relation_contract"] = (
            _record_to_dict(row) if row is not None else {}
        )
        summary["discovery_layer_counts"] = await _fetch_discovery_layer_counts(
            conn,
            tenant_id,
        )
        summary["topology_optimizer_metric_totals"] = (
            await _fetch_topology_optimizer_metric_totals(conn, tenant_id)
        )
        summary["top_customer_model_scopes"] = await _fetch_named_counts(
            conn,
            """
            SELECT r.identity AS name, COUNT(DISTINCT m.id)::bigint AS value
            FROM models m
            JOIN LATERAL jsonb_array_elements(COALESCE(m.scope_entities, '[]'::jsonb)) e(value) ON true
            JOIN resources r
              ON r.tenant_id = m.tenant_id
             AND r.id::text = e.value->>'id'
             AND e.value->>'type' = 'customer'
            WHERE m.tenant_id = $1
            GROUP BY r.identity
            ORDER BY 2 DESC, 1 ASC
            LIMIT 20
            """,
            tenant_id,
        )
        summary["cost"] = await _fetch_cost(conn, tenant_id)
        model_rows = await conn.fetch(
            """
            SELECT id, proposition_kind, status, confidence, activation,
                   "natural", scope_entities, scope_actors,
                   array_length(supporting_event_ids, 1) AS supporting_events,
                   array_length(supporting_model_ids, 1) AS supporting_models,
                   created_at
            FROM models
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            """,
            tenant_id,
        )
        edge_rows = await conn.fetch(
            """
            SELECT id, source_model_id, target_model_id, edge_kind, status,
                   review_status, confidence, explanation, created_at
            FROM model_edges
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            """,
            tenant_id,
        )

    summary["graph_health"] = _compute_graph_health(model_rows, edge_rows)
    _write_json(report_dir / "run_summary.json", summary)
    _write_jsonl(report_dir / "models.jsonl", [_record_to_dict(row) for row in model_rows])
    _write_jsonl(report_dir / "model_edges.jsonl", [_record_to_dict(row) for row in edge_rows])
    _write_jsonl(
        report_dir / "signal_manifest.jsonl",
        [
            {
                "index": index,
                "observation_id": str(observation_id),
                "family": (signal.get("content_dict") or {}).get("family"),
                "customer": (signal.get("content_dict") or {}).get("customer_name"),
                "channel": signal["channel"],
                "trust_tier": signal.get("trust_tier"),
                "content": signal["content"],
            }
            for index, (observation_id, signal) in enumerate(
                zip(
                    observation_ids,
                    [
                        s
                        for sequence in scenario.signal_sequences.values()
                        for s in sequence
                    ],
                    strict=False,
                )
            )
        ],
    )
    (report_dir / "model_layer_summary.md").write_text(_render_markdown(summary))
    return summary


async def _fetch_distribution(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
) -> dict[str, int]:
    rows = await conn.fetch(query, *args)
    return {str(row["key"]): int(row["value"] or 0) for row in rows}


async def _fetch_named_counts(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(query, *args)
    return [{"name": row["name"], "count": int(row["value"] or 0)} for row in rows]


async def _fetch_cost(conn: asyncpg.Connection, tenant_id: UUID) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT COUNT(*)::bigint AS rows,
               COALESCE(SUM(llm_calls_count), 0)::bigint AS llm_calls,
               COALESCE(SUM(llm_input_tokens_total), 0)::bigint AS input_tokens,
               COALESCE(SUM(llm_output_tokens_total), 0)::bigint AS output_tokens,
               COALESCE(SUM(llm_cost_usd), 0)::numeric AS cost_usd
        FROM think_run_costs
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    if row is None:
        return {}
    data = _record_to_dict(row)
    data["cost_usd"] = float(data.get("cost_usd") or 0)
    return data


async def _fetch_discovery_layer_counts(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, int]:
    rows = await conn.fetch(
        """
        SELECT 'affordance_profiles' AS name, COUNT(*)::bigint AS count
          FROM retrieval_affordance_profiles WHERE tenant_id = $1
        UNION ALL
        SELECT 'reinforced_affordance_profiles', COUNT(*)::bigint
          FROM retrieval_affordance_profiles
         WHERE tenant_id = $1 AND utility_score > 0
        UNION ALL
        SELECT 'contextual_affordance_profiles', COUNT(*)::bigint
          FROM retrieval_affordance_profiles
         WHERE tenant_id = $1
           AND jsonb_typeof(activation_signatures->'entities') = 'array'
           AND jsonb_array_length(activation_signatures->'entities') > 0
        UNION ALL
        SELECT 'discovery_shortcuts', COUNT(*)::bigint
          FROM discovery_shortcuts WHERE tenant_id = $1
        UNION ALL
        SELECT 'negative_memory', COUNT(*)::bigint
          FROM negative_memory WHERE tenant_id = $1
        UNION ALL
        SELECT 'question_policy_stats', COUNT(*)::bigint
          FROM sage_question_policy_stats WHERE tenant_id = $1
        UNION ALL
        SELECT 'reader_decision_attributions', COUNT(*)::bigint
          FROM sage_reader_decision_attributions WHERE tenant_id = $1
        """,
        tenant_id,
    )
    return {str(row["name"]): int(row["count"] or 0) for row in rows}


async def _fetch_topology_optimizer_metric_totals(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT
          COUNT(*)::bigint AS rows,
          COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed,
          COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed,
          COALESCE(SUM((metrics->>'affordance_reinforces')::numeric), 0)::numeric
            AS affordance_reinforces,
          COALESCE(SUM((metrics->>'affordance_decays')::numeric), 0)::numeric
            AS affordance_decays,
          COALESCE(SUM((metrics->>'shortcut_creates_or_bumps')::numeric), 0)::numeric
            AS shortcut_creates_or_bumps,
          COALESCE(SUM((metrics->>'shortcut_decays')::numeric), 0)::numeric
            AS shortcut_decays,
          COALESCE(SUM((metrics->>'negative_memory_inserts')::numeric), 0)::numeric
            AS negative_memory_inserts,
          COALESCE(SUM((metrics->>'region_refreshes')::numeric), 0)::numeric
            AS region_refreshes,
          COALESCE(SUM((metrics->>'question_policy_updates')::numeric), 0)::numeric
            AS question_policy_updates,
          COALESCE(SUM((metrics->>'canonical_merge_candidates')::numeric), 0)::numeric
            AS canonical_merge_candidates,
          COALESCE(SUM((metrics->>'canonical_split_candidates')::numeric), 0)::numeric
            AS canonical_split_candidates,
          COALESCE(SUM((metrics->>'canonical_promote_candidates')::numeric), 0)::numeric
            AS canonical_promote_candidates,
          COALESCE(SUM((metrics->>'canonical_demote_candidates')::numeric), 0)::numeric
            AS canonical_demote_candidates,
          COALESCE(SUM((metrics->>'shortcut_missing_model_skips')::numeric), 0)::numeric
            AS shortcut_missing_model_skips,
          COALESCE(SUM((metrics->>'structural_missing_model_skips')::numeric), 0)::numeric
            AS structural_missing_model_skips
        FROM sage_topology_optimizer_runs
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    return _record_to_dict(row) if row is not None else {}


def _record_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    return {key: _jsonable(row[key]) for key in row.keys()}


def _compute_graph_health(
    model_rows: list[asyncpg.Record],
    edge_rows: list[asyncpg.Record],
) -> dict[str, Any]:
    active_model_ids = {
        row["id"] for row in model_rows if str(row["status"]) == "active"
    }
    active_edges = [
        row for row in edge_rows
        if str(row["status"]) == "active"
        and row["source_model_id"] in active_model_ids
        and row["target_model_id"] in active_model_ids
    ]
    degree: Counter[UUID] = Counter()
    adjacency: dict[UUID, set[UUID]] = {mid: set() for mid in active_model_ids}
    edge_keys: Counter[tuple[UUID, UUID, str]] = Counter()
    soft_kinds = {"co_occurs_with", "same_issue_as", "analogous_to"}
    actionable_kinds = {
        "blocks",
        "causes",
        "explains",
        "predicts",
        "contradicts",
        "weakens",
        "early_warning_for",
        "enables",
        "contributes_to_resolution",
    }
    self_edges = 0
    orphan_edges = 0
    for row in edge_rows:
        source = row["source_model_id"]
        target = row["target_model_id"]
        kind = str(row["edge_kind"])
        if str(row["status"]) != "active":
            continue
        if source == target:
            self_edges += 1
        if source not in active_model_ids or target not in active_model_ids:
            orphan_edges += 1
        edge_keys[(source, target, kind)] += 1
        if source in active_model_ids and target in active_model_ids:
            degree[source] += 1
            degree[target] += 1
            adjacency[source].add(target)
            adjacency[target].add(source)

    seen: set[UUID] = set()
    component_sizes: list[int] = []
    for mid in active_model_ids:
        if mid in seen:
            continue
        stack = [mid]
        seen.add(mid)
        size = 0
        while stack:
            current = stack.pop()
            size += 1
            for neighbor in adjacency.get(current, set()):
                if neighbor not in seen:
                    seen.add(neighbor)
                    stack.append(neighbor)
        component_sizes.append(size)
    component_sizes.sort(reverse=True)

    naturals = Counter(
        " ".join(str(row["natural"] or "").lower().split())
        for row in model_rows
        if str(row["status"]) == "active" and str(row["natural"] or "").strip()
    )
    exact_duplicate_groups = [
        {"natural": natural[:240], "count": count}
        for natural, count in naturals.most_common()
        if count > 1
    ]
    isolated_count = sum(1 for mid in active_model_ids if degree[mid] == 0)
    edge_kind_counts = Counter(str(row["edge_kind"]) for row in active_edges)
    soft_edge_count = sum(edge_kind_counts[kind] for kind in soft_kinds)
    actionable_edge_count = sum(edge_kind_counts[kind] for kind in actionable_kinds)
    active_model_count = len(active_model_ids)
    active_edge_count = len(active_edges)
    duplicate_edge_count = sum(count - 1 for count in edge_keys.values() if count > 1)
    degree_values = [degree[mid] for mid in active_model_ids]

    def _ratio(numerator: int, denominator: int) -> float:
        return round(numerator / denominator, 4) if denominator else 0.0

    return {
        "active_model_count": active_model_count,
        "active_edge_count": active_edge_count,
        "component_count": len(component_sizes),
        "largest_component_size": component_sizes[0] if component_sizes else 0,
        "largest_component_ratio": _ratio(
            component_sizes[0] if component_sizes else 0,
            active_model_count,
        ),
        "isolated_model_count": isolated_count,
        "isolated_model_ratio": _ratio(isolated_count, active_model_count),
        "average_degree": (
            round(sum(degree_values) / len(degree_values), 4)
            if degree_values else 0.0
        ),
        "max_degree": max(degree_values) if degree_values else 0,
        "soft_edge_count": soft_edge_count,
        "soft_edge_ratio": _ratio(soft_edge_count, active_edge_count),
        "actionable_edge_count": actionable_edge_count,
        "actionable_edge_ratio": _ratio(actionable_edge_count, active_edge_count),
        "self_edge_count": self_edges,
        "orphan_edge_count": orphan_edges,
        "duplicate_directed_edge_count": duplicate_edge_count,
        "exact_duplicate_natural_groups": len(exact_duplicate_groups),
        "top_exact_duplicate_naturals": exact_duplicate_groups[:10],
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True))


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n")


def _render_markdown(summary: dict[str, Any]) -> str:
    profile = summary.get("company_profile") or {}
    financials = profile.get("financials") or {}
    employee_count = sum(
        int(v) for v in (profile.get("employees_by_department") or {}).values()
    )
    lines = [
        "# Company-Scale Signal Probe",
        "",
        f"- Tenant: `{summary['tenant_id']}`",
        f"- Run: `{summary['run_id']}`",
        f"- Company: {profile.get('company_name', COMPANY_NAME)}",
        f"- Operating space: {profile.get('operating_space', '')}",
        f"- Cash in hand: ${int(financials.get('cash_in_hand_usd') or 0):,}",
        f"- Runway months: {financials.get('runway_months', '<unknown>')}",
        f"- Employees modeled: {employee_count}",
        f"- Initial seed models: {(summary.get('seed_status') or {}).get('models', 0)}",
        f"- Think status: `{summary['think_status']}`",
        f"- Signals injected: {summary['signal_count']}",
        f"- Observations stored: {summary['observation_count']}",
        f"- Observations with entities: {summary['observations_with_entities']}",
        f"- Successful Think runs: {summary['think_runs_success']}",
        f"- Failed Think runs: {summary['think_runs_failed']}",
        f"- Pending triggers: {summary['pending_triggers']}",
        f"- Pending post-commit actions: {summary.get('pending_post_commit_actions', 0)}",
        f"- Dead-lettered post-commit actions: {summary.get('dead_lettered_post_commit_actions', 0)}",
        f"- Active models: {summary['active_models']}",
        f"- Archived models: {summary['archived_models']}",
        f"- Model edges: {summary['model_edges']}",
        f"- Relationship candidates: {summary.get('relationship_candidates', 0)}",
        f"- Latent topology candidates: {summary.get('latent_topology_candidates', 0)}",
        f"- Topology optimizer runs: {summary.get('topology_optimizer_run_distribution', {})}",
        f"- Scope entity sidecars: {summary.get('model_scope_entity_sidecars', 0)}",
        f"- State changes: {summary['state_changes']}",
        f"- Elapsed seconds: {summary['elapsed_seconds']}",
        "",
        "## Self-Evolution",
        "```json",
        json.dumps(
            {
                "seed_status": summary.get("seed_status") or {},
                "run_config": summary.get("run_config") or {},
                "post_commit_status": summary.get("post_commit_status") or {},
                "topology_optimizer_status": (
                    summary.get("topology_optimizer_status") or {}
                ),
                "topology_optimizer_run_distribution": (
                    summary.get("topology_optimizer_run_distribution") or {}
                ),
                "topology_optimizer_metric_totals": (
                    summary.get("topology_optimizer_metric_totals") or {}
                ),
                "discovery_layer_counts": (
                    summary.get("discovery_layer_counts") or {}
                ),
                "processing_waves": summary.get("processing_waves") or [],
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Model Kinds",
        _table(summary.get("model_kind_distribution") or {}),
        "",
        "## Context Use",
        _table(summary.get("context_use_distribution") or {}),
        "",
        "## Edge Kinds",
        _table(summary.get("edge_kind_distribution") or {}),
        "",
        "## Relationship Candidates",
        _table(summary.get("relationship_candidate_kind_distribution") or {}),
        "",
        "## Topology Objects",
        _table(summary.get("topology_object_distribution") or {}),
        "",
        "## Graph Health",
        "```json",
        json.dumps(summary.get("graph_health") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "## Top Customer Scopes",
        _named_table(summary.get("top_customer_model_scopes") or []),
        "",
        "## Cost",
        "```json",
        json.dumps(summary.get("cost") or {}, indent=2, sort_keys=True),
        "```",
        "",
    ]
    return "\n".join(lines)


def _table(dist: dict[str, int]) -> str:
    if not dist:
        return "_No rows._"
    lines = ["| Key | Count |", "| --- | ---: |"]
    for key, value in dist.items():
        lines.append(f"| {key} | {value} |")
    return "\n".join(lines)


def _named_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "_No rows._"
    lines = ["| Name | Count |", "| --- | ---: |"]
    for row in rows:
        lines.append(f"| {row['name']} | {row['count']} |")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=int, default=3000)
    parser.add_argument(
        "--think-limit",
        type=int,
        default=200,
        help=(
            "Number of injected signals to enqueue for live Think. Use --signals "
            "for a full LLM burn."
        ),
    )
    parser.add_argument("--think-timeout", type=int, default=3600)
    parser.add_argument("--post-commit-timeout", type=int, default=600)
    parser.add_argument(
        "--signal-batch-size",
        type=int,
        default=250,
        help=(
            "Inject/process signals in waves so learned model-layer changes from "
            "earlier waves can affect later waves."
        ),
    )
    parser.add_argument(
        "--seed-models",
        type=int,
        default=0,
        help="Optional starting model-layer size to seed before signals arrive.",
    )
    parser.add_argument("--seed-families", type=int, default=80)
    parser.add_argument("--topology-optimizer-timeout", type=int, default=900)
    parser.add_argument("--topology-optimizer-batch-size", type=int, default=250)
    parser.add_argument("--topology-optimizer-lookback-hours", type=int, default=72)
    parser.add_argument("--skip-topology-optimizer", action="store_true")
    parser.add_argument(
        "--skip-migrations",
        action="store_true",
        help="Skip db/migrations replay when the target database is already current.",
    )
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--report-root",
        type=Path,
        default=REPO_ROOT / "tests" / "real_llm" / "reports" / "runs",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    if args.signals <= 0:
        raise SystemExit("--signals must be positive")
    if args.think_limit < 0:
        raise SystemExit("--think-limit cannot be negative")
    if args.think_limit > args.signals:
        raise SystemExit("--think-limit cannot exceed --signals")
    if args.signal_batch_size <= 0:
        raise SystemExit("--signal-batch-size must be positive")
    if args.seed_models < 0:
        raise SystemExit("--seed-models cannot be negative")
    if args.seed_families <= 0:
        raise SystemExit("--seed-families must be positive")
    if args.topology_optimizer_batch_size <= 0:
        raise SystemExit("--topology-optimizer-batch-size must be positive")

    run_id = args.run_id or f"company-e2e-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    report_dir = args.report_root / f"model-layer-{run_id}"
    print(f"building {args.signals}-signal scenario for {COMPANY_NAME}", flush=True)
    scenario = build_scenario(args.signals, namespace=run_id)

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
    think_status = "not_run"
    seed_status: dict[str, Any] = {
        "requested_models": args.seed_models,
        "families": args.seed_families,
        "models": 0,
    }
    processing_waves: list[dict[str, Any]] = []
    post_commit_status: dict[str, Any] = {
        "processed": 0,
        "failed": 0,
        "dead_lettered": 0,
        "iterations": 0,
    }
    topology_optimizer_status: dict[str, Any] = {
        "status": "skipped" if args.skip_topology_optimizer else "not_run",
        "processed": 0,
        "completed": 0,
        "failed": 0,
        "iterations": 0,
        "metrics": {},
    }
    try:
        if args.skip_migrations:
            print("skipping migrations because --skip-migrations was set", flush=True)
        else:
            async with pool.acquire() as conn:
                await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")

        await materialize(scenario, pool=pool)
        assert scenario.tenant_id is not None
        print(f"tenant={scenario.tenant_id} run_id={run_id}", flush=True)

        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        await _insert_extra_aliases(scenario, alias_repo)

        if args.seed_models:
            from scripts.run_incremental_feedback_loop_stress import _seed_company

            print(
                f"seeding {args.seed_models} starting models "
                f"across {args.seed_families} families",
                flush=True,
            )
            seeded = await _seed_company(
                pool,
                tenant_id=scenario.tenant_id,
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

        observation_ids: list[UUID] = []

        remaining_think = args.think_limit
        provider = _build_cached_provider() if args.think_limit else None
        batch_size = min(args.signal_batch_size, args.signals)
        for offset in range(0, args.signals, batch_size):
            current_batch_size = min(batch_size, args.signals - offset)
            wave_index = len(processing_waves) + 1
            wave: dict[str, Any] = {
                "wave": wave_index,
                "signal_offset": offset,
                "signal_start": offset + 1,
                "signal_end": offset + current_batch_size,
            }
            print(
                f"wave={wave_index} injecting signals "
                f"{offset + 1}-{offset + current_batch_size}",
                flush=True,
            )
            batch_ids = await inject_generated_signals(
                scenario,
                pool=pool,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                run_id=run_id,
                progress_every=args.progress_every,
                offset=offset,
                limit=current_batch_size,
            )
            observation_ids.extend(batch_ids)
            wave["injected"] = len(batch_ids)
            wave["cumulative_signals"] = len(observation_ids)

            if provider is not None and remaining_think > 0:
                think_count = min(len(batch_ids), remaining_think)
                enqueued = await enqueue_t1_for_observations(
                    pool,
                    tenant_id=scenario.tenant_id,
                    observation_ids=batch_ids,
                    limit=think_count,
                    run_id=run_id,
                )
                remaining_think -= enqueued
                wave["enqueued_t1"] = enqueued
                print(
                    f"wave={wave_index} enqueued {enqueued} T1 triggers "
                    f"(remaining_think={remaining_think})",
                    flush=True,
                )

                try:
                    await run_think_until_drain(
                        scenario.tenant_id,
                        pool=pool,
                        provider=provider,
                        timeout_seconds=args.think_timeout,
                    )
                    think_status = "drained"
                    wave["think_status"] = "drained"
                except TimeoutError as exc:
                    think_status = f"timeout: {exc}"
                    wave["think_status"] = think_status
                    print(think_status, flush=True)

                current_post_commit = await drain_post_commit_actions(
                    pool,
                    tenant_id=scenario.tenant_id,
                    timeout_seconds=args.post_commit_timeout,
                )
                wave["post_commit_status"] = current_post_commit
                _merge_numeric_status(post_commit_status, current_post_commit)
                print(
                    "post_commit="
                    f"{json.dumps(current_post_commit, sort_keys=True)}",
                    flush=True,
                )

                if not args.skip_topology_optimizer:
                    current_topology = await drain_topology_optimizer(
                        pool,
                        tenant_id=scenario.tenant_id,
                        timeout_seconds=args.topology_optimizer_timeout,
                        batch_size=args.topology_optimizer_batch_size,
                        lookback_hours=args.topology_optimizer_lookback_hours,
                    )
                    wave["topology_optimizer_status"] = current_topology
                    _merge_numeric_status(
                        topology_optimizer_status,
                        current_topology,
                        metric_key="metrics",
                    )
                    if current_topology.get("status") == "timeout":
                        topology_optimizer_status["status"] = "timeout"
                    elif topology_optimizer_status.get("status") != "timeout":
                        topology_optimizer_status["status"] = "drained"
                    print(
                        "topology_optimizer="
                        f"{json.dumps(current_topology, sort_keys=True)}",
                        flush=True,
                    )
            else:
                wave["enqueued_t1"] = 0
                wave["think_status"] = "not_run"

            processing_waves.append(wave)
            if str(wave.get("think_status") or "").startswith("timeout:"):
                break

        print(f"injected_total={len(observation_ids)}", flush=True)

        summary = await collect_model_layer_report(
            pool,
            tenant_id=scenario.tenant_id,
            run_id=run_id,
            report_dir=report_dir,
            scenario=scenario,
            observation_ids=observation_ids,
            think_status=think_status,
            run_config={
                "signals": args.signals,
                "think_limit": args.think_limit,
                "signal_batch_size": args.signal_batch_size,
                "seed_models": args.seed_models,
                "seed_families": args.seed_families,
                "skip_migrations": args.skip_migrations,
                "skip_topology_optimizer": args.skip_topology_optimizer,
                "topology_optimizer_batch_size": args.topology_optimizer_batch_size,
                "topology_optimizer_timeout": args.topology_optimizer_timeout,
                "topology_optimizer_lookback_hours": (
                    args.topology_optimizer_lookback_hours
                ),
            },
            seed_status=seed_status,
            processing_waves=processing_waves,
            post_commit_status=post_commit_status,
            topology_optimizer_status=topology_optimizer_status,
            elapsed_seconds=time.monotonic() - started,
        )
        _write_json(report_dir / "run_summary.json", summary)
        (report_dir / "model_layer_summary.md").write_text(_render_markdown(summary))
        print(f"report_dir={report_dir}", flush=True)
        print(
            json.dumps(
                {
                    "tenant_id": summary["tenant_id"],
                    "run_id": summary["run_id"],
                    "signals": summary["signal_count"],
                    "active_models": summary["active_models"],
                    "model_edges": summary["model_edges"],
                    "relationship_candidates": summary["relationship_candidates"],
                    "latent_topology_candidates": summary["latent_topology_candidates"],
                    "seed_status": seed_status,
                    "processing_waves": len(processing_waves),
                    "think_runs_success": summary["think_runs_success"],
                    "think_runs_failed": summary["think_runs_failed"],
                    "pending_triggers": summary["pending_triggers"],
                    "pending_post_commit_actions": summary["pending_post_commit_actions"],
                    "dead_lettered_post_commit_actions": summary[
                        "dead_lettered_post_commit_actions"
                    ],
                    "post_commit_status": post_commit_status,
                    "topology_optimizer_status": topology_optimizer_status,
                    "discovery_layer_counts": summary.get("discovery_layer_counts"),
                    "topology_optimizer_metric_totals": summary.get(
                        "topology_optimizer_metric_totals"
                    ),
                    "cost": summary["cost"],
                },
                indent=2,
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        await embedder.close()
        await pool.close()
    return 0


def _build_cached_provider():
    cache = LLMResponseCache(
        cache_dir=REPO_ROOT / "tests" / "real_llm" / "cache",
        current_epoch=LLMResponseCache.current_epoch(),
    )
    set_response_cache(cache)
    try:
        cfg = LLMConfig.from_env()
    except LLMConfigError as exc:
        raise SystemExit(f"LLM provider is not configured: {exc}") from exc
    if not cfg.api_key:
        if cfg.provider == "codex" and _codex_transport() in {"app-server", "cli"}:
            raise SystemExit(
                "Codex local auth is not set; run `codex login` or set CODEX_AUTH_FILE"
            )
        raise SystemExit("LLM API key is not set")
    return build_provider(cfg)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
