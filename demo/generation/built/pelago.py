"""Pelago — spec-loaded demo bundle.

Series A B2B SaaS revenue-intelligence platform. The single source of
truth is `demo/generation/specs/pelago.yaml`; this module reads the spec
and the LSOB corpus shards under `corpora/pelago/shards/` and emits
the SQL snapshot at `demo/snapshots/pelago-v1.sql`.

Re-running produces the same SQL because every UUID is
`uuid5(DEMO_NS, "pelago|<kind>|<key>")` and the signal sample is a
seeded selection from the deterministic corpus.

Usage:
  python -m demo.generation.built.pelago             # validate only
  python -m demo.generation.built.pelago --emit      # write SQL snapshot
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any

from demo.generation.built._helpers import (
    days_from_now,
    did,
    find_signals_containing,
)
from demo.generation.schemas import (
    EntityMention,
    GeneratedActor,
    GeneratedBundle,
    GeneratedCommitment,
    GeneratedCustomer,
    GeneratedDecision,
    GeneratedGoal,
    GeneratedModel,
    GeneratedRecommendation,
    GeneratedResource,
    GeneratedResourceDeployment,
    GeneratedSignal,
    TargetActRef,
)
from demo.generation.spec_io import load_spec
from demo.generation.sql_emit import write_sql
from demo.generation.validate import validate_bundle


COMPANY = "pelago"
SPEC_PATH = Path("demo/generation/specs/pelago.yaml")
SHARDS_DIR = Path("corpora/pelago/shards")
GT_DIR = Path("corpora/pelago/ground_truth")
SYNTH_MODELS_PATH = Path("corpora/pelago/synthesis/models.json")
DEFAULT_OUT = Path("demo/snapshots/pelago-v1.sql")
SIGNAL_SAMPLE_TARGET = 250
SIGNAL_SAMPLE_SEED = 17
RATE_COMMITMENTS_TARGET = 110         # to land near the spec's commitment_count: 140


# --- LSOB → demo channel mapping -----------------------------------------

_CHANNEL_MAP = {
    "slack": "slack:message",
    "email": "email:message",
    "pr": "github:event",
    "doc": "doc:edit",
    "calendar": "calendar:event",
    "ticket": "ticket:opened",
}


# =====================================================================
# Bundle builders
# =====================================================================

def _actors_from_spec(sim_dict: dict[str, Any]) -> list[GeneratedActor]:
    out: list[GeneratedActor] = []
    for ap in sim_dict.get("actor_profiles", []):
        out.append(GeneratedActor(
            id=did(COMPANY, "actor", ap["actor_id"]),
            name=ap["name"],
            role=ap["role"],
            manager_id=(did(COMPANY, "actor", ap["manager_id"])
                        if ap.get("manager_id") else None),
            personality_brief=ap.get("brief", ""),
            email=ap.get("email"),
        ))
    return out


def _customers_from_spec(sim_dict: dict[str, Any]) -> list[GeneratedCustomer]:
    out: list[GeneratedCustomer] = []
    health_map = {
        "healthy": "healthy",
        "warning": "watching",
        "degraded": "at_risk",
        "critical": "escalating",
        "churned": "escalating",
    }
    for cp in sim_dict.get("customer_profiles", []):
        out.append(GeneratedCustomer(
            id=did(COMPANY, "customer", cp["customer_id"]),
            company_name=cp["company_name"],
            arr_usd=float(cp["arr_usd"]),
            segment=cp["segment"],
            current_health=health_map.get(cp.get("initial_health", "healthy"), "healthy"),
            primary_contacts=[
                did(COMPANY, "actor", aid)
                for aid in cp.get("primary_contact_actor_ids", [])
            ],
        ))
    return out


def _goals_from_spec(sim_dict: dict[str, Any]) -> list[GeneratedGoal]:
    out: list[GeneratedGoal] = []
    for g in sim_dict.get("goals", []):
        out.append(GeneratedGoal(
            id=did(COMPANY, "goal", g["goal_id"]),
            title=g["title"],
            description=g.get("description", ""),
            owner_id=did(COMPANY, "actor", g["owner_actor_id"]),
            target_date=days_from_now(g["target_offset_days"])
                if g.get("target_offset_days") else None,
            parent_goal_id=(did(COMPANY, "goal", g["parent_goal_id"])
                            if g.get("parent_goal_id") else None),
            altitude=g.get("altitude", "operational"),
        ))
    return out


def _decisions_from_spec(sim_dict: dict[str, Any]) -> list[GeneratedDecision]:
    out: list[GeneratedDecision] = []
    for d in sim_dict.get("decisions", []):
        out.append(GeneratedDecision(
            id=did(COMPANY, "decision", d["decision_id"]),
            title=d["title"],
            decision_text=d["decision_text"],
            rationale=d.get("rationale", ""),
            scope={"company_id": COMPANY},
            revisit_triggers=[],
        ))
    return out


def _commitment_state_from_outcome(outcome: str | None) -> str:
    if outcome in ("will_be_cancelled", "cancelled"):
        return "closed"
    if outcome == "will_slip":
        return "at_risk"
    if outcome in ("succeeded", "slipped_but_completed"):
        return "done"
    return "active"


def _commitments_from_spec(sim_dict: dict[str, Any]) -> list[GeneratedCommitment]:
    out: list[GeneratedCommitment] = []
    for s in sim_dict.get("commitment_seeds", []):
        goal_id = s.get("goal_id")
        # Constrain by the decision(s) that govern this commitment's
        # goal — the seeded commit ↔ goal mapping is already explicit
        # in the spec; goal ↔ decision mapping comes from
        # _GOAL_TO_DECISIONS so seeded and rate-gen commits use the
        # same source of truth.
        decision_ids: list[str] = []
        if goal_id:
            for dk in _GOAL_TO_DECISIONS.get(goal_id, []):
                decision_ids.append(did(COMPANY, "decision", dk))
        out.append(GeneratedCommitment(
            id=did(COMPANY, "commitment", s["commitment_id"]),
            title=s["title"],
            owner_id=did(COMPANY, "actor", s["owner_actor_id"]),
            contributors=[],
            state=_commitment_state_from_outcome(s.get("intended_outcome")),
            due_date=days_from_now(s.get("created_offset_days", 0)
                                   + s["asserted_duration_days"]),
            contributes_to_goal_id=(did(COMPANY, "goal", goal_id)
                                    if goal_id else None),
            depends_on=[],
            constrained_by_decision_ids=decision_ids,
            served_by_customer_id=(did(COMPANY, "customer", s["customer_id"])
                                   if s.get("customer_id") else None),
        ))
    return out


# Rate-gen commits don't carry titles, goals, or customer links in the
# spec. We derive each from the corpus: title from owner role + family,
# goal from a role-family heuristic, customer from the most-frequent
# customer_ref in the commit's signals, decision from the implied goal.

# Per-role-family pool of realistic-looking commit titles. The chosen
# title is a deterministic pick keyed on commit_id hash so re-runs
# produce the same bundle. None of these embed owner name or
# complexity — that metadata lives on the actor / commit row.
_ROLE_FAMILY_TITLE_POOL: dict[str, list[str]] = {
    "engineering": [
        "API surface refactor",
        "Worker queue stabilization",
        "Auth middleware rewrite",
        "Backend service integration",
        "Cron job hardening",
        "Dependency graph cleanup",
        "Internal SDK iteration",
        "Rate-limit handler",
        "Webhook dispatch hardening",
        "Idempotency key rollout",
    ],
    "data_ml": [
        "Ingestion connector iteration",
        "Snowflake migration shard",
        "BigQuery dual-write removal",
        "Schema reconciliation pass",
        "Data quality regression sweep",
        "Forecast feature engineering",
        "Conversation-AI training loop",
        "ICP scoring feature spike",
        "Per-tenant warehouse cutover",
    ],
    "sales": [
        "Pipeline review and forecast update",
        "Mid-market deal-cycle thread",
        "QBR preparation",
        "Outbound sequence iteration",
        "Renewal pre-discussion",
        "Annual contract negotiation",
        "Procurement follow-up",
    ],
    "customer_success": [
        "Account onboarding",
        "Renewal preparation",
        "Health-check cycle",
        "Sync coordination",
        "Save-play execution",
        "Implementation handoff",
        "Customer escalation triage",
    ],
    "product": [
        "Roadmap thread",
        "Customer requirements doc",
        "Spec review and lock",
        "Launch readiness checklist",
    ],
    "design": [
        "Design system iteration",
        "Dashboard mockup pass",
        "User-testing notes synthesis",
    ],
    "exec": [
        "Executive coordination thread",
        "Cross-functional unblock",
        "Board update preparation",
    ],
    "founder": [
        "Founder-driven thread",
        "Board update preparation",
        "Strategic narrative draft",
    ],
    "marketing": [
        "Launch coordination",
        "Content calendar push",
        "Campaign analytics review",
    ],
    "finance": [
        "Burn forecast update",
        "Vendor renewal cycle",
        "Headcount plan refresh",
    ],
    "people": [
        "Recruiting pipeline review",
        "Closing-call coordination",
        "Sourcing batch outreach",
    ],
}

# Map (role_family) → primary goal_id (the simulator's seeded goal ids)
_ROLE_FAMILY_TO_GOAL = {
    "engineering": "G-2-multi-crm",
    "data_ml":     "G-4-incident-halve",
    "sales":       "G-1-arr-target",
    "customer_success": "G-3-renewal-90",
    "product":     "G-5-conv-ai-v1",
    "design":      "G-5-conv-ai-v1",
    "exec":        "G-1-arr-target",
    "founder":     "G-1-arr-target",
    "marketing":   "G-1-arr-target",
    "finance":     "G-1-arr-target",
    "people":      "G-6-vp-eng-successor",
}

# Map (goal_id) → list of decision_ids that constrain commits under that goal.
# Decision IDs come from demo/generation/specs/pelago.yaml `decisions:`.
_GOAL_TO_DECISIONS = {
    "G-1-arr-target":   ["D-4-uk-ae", "D-5-pricing-model"],
    "G-2-multi-crm":    ["D-1-crm-in-house", "D-3-snowflake"],
    "G-4-incident-halve": ["D-3-snowflake"],
    "G-5-conv-ai-v1":   ["D-2-conv-ai-first"],
}


def _per_commit_signal_stats() -> dict[str, dict[str, Any]]:
    """One pass over the corpus: count signals per commitment, track the
    most-referenced customer, the first/last touch dates."""
    stats: dict[str, dict[str, Any]] = {}
    if not SHARDS_DIR.is_dir():
        return stats
    for sf in sorted(SHARDS_DIR.glob("day-*.jsonl")):
        for raw in sf.read_text().splitlines():
            if not raw.strip():
                continue
            s = json.loads(raw)
            m = s.get("metadata") or {}
            cref = m.get("commitment_ref")
            if not cref:
                continue
            kref = m.get("customer_ref")
            ts = s["timestamp"]
            entry = stats.setdefault(cref, {
                "signal_count": 0,
                "customers": {},
                "first_ts": None,
                "last_ts": None,
            })
            entry["signal_count"] += 1
            if kref:
                entry["customers"][kref] = entry["customers"].get(kref, 0) + 1
            if entry["first_ts"] is None or ts < entry["first_ts"]:
                entry["first_ts"] = ts
            if entry["last_ts"] is None or ts > entry["last_ts"]:
                entry["last_ts"] = ts
    return stats


def _rate_generated_commitments_from_gt(
    actor_ids: set[str],
    customer_ids: set[str],
    seeded_ids: set[str],
    sim_dict: dict[str, Any],
    target_count: int = RATE_COMMITMENTS_TARGET,
) -> list[GeneratedCommitment]:
    """Build rate-gen commitments enriched with derived titles + goals
    + customer links + decision links. Only includes commits with
    sustained signal traffic so the bundle reflects real operational
    tempo, not empty placeholders."""
    gt_files = sorted(GT_DIR.glob("snapshot-month-*.json"))
    if not gt_files:
        return []
    gt = json.loads(gt_files[-1].read_text())
    rate_commits = [c for c in gt.get("commitments", []) if c["id"] not in seeded_ids]

    # Index actor metadata by id for role/role_family lookup
    actor_profile_by_id = {
        ap["actor_id"]: ap for ap in sim_dict.get("actor_profiles", [])
    }

    sig_stats = _per_commit_signal_stats()
    # Filter to rate-gen with meaningful traffic, sort by signal count desc
    enriched: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for c in rate_commits:
        st = sig_stats.get(c["id"])
        if not st or st["signal_count"] < 25:
            continue
        owner = c.get("owner")
        if owner not in actor_profile_by_id:
            continue
        owner_uuid = did(COMPANY, "actor", owner)
        if owner_uuid not in actor_ids:
            continue
        enriched.append((c, st))
    enriched.sort(key=lambda pair: pair[1]["signal_count"], reverse=True)

    out: list[GeneratedCommitment] = []
    # Track titles already used so the bundle doesn't end up with
    # six identical "Snowflake migration shard" rows — we cycle through
    # the pool deterministically.
    title_use_count: dict[str, int] = {}
    for idx, (c, st) in enumerate(enriched):
        if len(out) >= target_count:
            break
        owner = c["owner"]
        owner_uuid = did(COMPANY, "actor", owner)
        owner_profile = actor_profile_by_id[owner]
        family = owner_profile.get("role_family", "engineering")

        # Most-referenced customer if signals overwhelmingly cite one
        served_customer_uuid: str | None = None
        served_customer_label: str | None = None
        if st["customers"]:
            top_cust, top_n = max(st["customers"].items(), key=lambda x: x[1])
            if top_n / max(1, st["signal_count"]) >= 0.4:
                cuuid = did(COMPANY, "customer", top_cust)
                if cuuid in customer_ids:
                    served_customer_uuid = cuuid
                    served_customer_label = top_cust

        # Goal heuristic from role_family
        goal_key = _ROLE_FAMILY_TO_GOAL.get(family)
        goal_uuid = did(COMPANY, "goal", goal_key) if goal_key else None

        # Decision links from goal
        decision_uuids: list[str] = []
        if goal_key:
            for dk in _GOAL_TO_DECISIONS.get(goal_key, []):
                decision_uuids.append(did(COMPANY, "decision", dk))

        # Title: pick from the role-family pool deterministically by
        # commit_id hash. When the customer is clearly identified,
        # prefix with the customer name. Owner name and complexity
        # are NEVER in the title — they live as separate columns.
        pool = _ROLE_FAMILY_TITLE_POOL.get(family) or ["Operational thread"]
        # Deterministic stable pick (independent of iteration order)
        title_idx = (sum(ord(c) for c in c["id"])) % len(pool)
        base_title = pool[title_idx]
        if served_customer_label:
            customer_pretty = served_customer_label.replace("cust-", "").replace("_", " ").title()
            title = f"{customer_pretty}: {base_title.lower()}"
        else:
            title = base_title

        outcome = c.get("true_outcome")
        if outcome in ("succeeded", "slipped_but_completed"):
            state = "done"
        elif outcome == "cancelled":
            state = "closed"
        elif outcome == "will_slip" and not c.get("resolved"):
            state = "at_risk"
        else:
            state = "active"

        out.append(GeneratedCommitment(
            id=did(COMPANY, "commitment", c["id"]),
            title=title,
            owner_id=owner_uuid,
            contributors=[],
            state=state,
            due_date=None,
            contributes_to_goal_id=goal_uuid,
            depends_on=[],
            constrained_by_decision_ids=decision_uuids,
            served_by_customer_id=served_customer_uuid,
        ))
    return out


# =====================================================================
# Resources — capacity pools (human pods, financial, technical platforms)
# =====================================================================
#
# Why Pelago has these particular resources:
#
# - Human pods are the unit at which the company actually thinks about
#   capacity. Engineering is the famously over-stretched pod (Salesforce
#   sync fires, VP-Eng departure m5); Data & ML carries the Snowflake
#   migration + forecast model + conv-AI; Sales/CS run the customer
#   surface; Founder & exec is a thin pool that gets eaten by 1:1s and
#   board prep. Mapping commitments → pod via owner role_family lets
#   the aggregate view show "where the work lives" at a glance.
#
# - Financial pools are the long-horizon constraints any Series-A CEO
#   tracks: cash runway is the existential clock; GTM budget is the
#   ARR-growth lever. Linking commits to either pool surfaces "what
#   strategic spend is each motion drawing on?".
#
# - Technical platforms are infrastructure surfaces that have real
#   maintenance load. Salesforce-sync + Production-API platforms
#   compete for the same backend engineers. Snowflake is the warehouse
#   that the entire data org standardizes on. ML training compute is
#   the single pool feeding all model work; if it's underutilized the
#   conv-AI roadmap can absorb it without a procurement cycle.
#
# Capacity values are tuned so the aggregate view shows the spec's
# narrative tensions: engineering near 100%, ML compute under-allocated,
# Salesforce sync platform constrained.

_POD_BY_ROLE_FAMILY = {
    "engineering":      "pod-engineering",
    "data_ml":          "pod-data-ml",
    "sales":            "pod-sales",
    "customer_success": "pod-cs",
    "product":          "pod-product-design",
    "design":           "pod-product-design",
    "exec":             "pod-exec",
    "founder":          "pod-exec",
    "marketing":        "pod-ops",
    "finance":          "pod-ops",
    "people":           "pod-ops",
}

# Tech-resource keyword routes: any commitment whose title or owner
# role implicates one of these areas pulls from the corresponding
# infrastructure pool. Each match attaches a small unit-quantity
# deployment alongside the human-pod deployment so the same commit
# shows up on both surfaces in the resource view.
_TECH_KEYWORD_ROUTES = [
    # (resource_identity, [keywords], unit_per_commit)
    ("infra-salesforce-sync",
     ["salesforce", "sync", "connector", "crm", "hubspot", "pipedrive"], 1.0),
    ("infra-snowflake",
     ["snowflake", "warehouse", "bigquery", "dbt", "schema", "ingestion"], 60.0),
    ("infra-ml-compute",
     ["forecast", "conv-ai", "conversation-ai", "icp", "ml ", "ml-", "training", "model", "scoring"], 30.0),
    ("infra-api-platform",
     ["api", "rate", "webhook", "auth", "queue", "worker", "idempotency", "service"], 0.8),
]


def _capacity_resources_for_pelago() -> list[GeneratedResource]:
    """The 13-resource capacity surface for Pelago: 6 human pods,
    2 financial pools, 4 technical platforms, 1 founder bandwidth."""
    return [
        # ---- Human pods --------------------------------------------------
        GeneratedResource(
            id=did(COMPANY, "resource", "pod-engineering"),
            kind="human", identity="pod:engineering",
            label="Engineering pod",
            description="Backend + frontend engineering capacity. Owns Salesforce sync, API platform, and dashboard surface.",
            capacity=7.0, unit="FTE",
            utilization_state="constrained",
            metadata={"category": "human_pod", "members": ["maya", "wei", "marcus", "elena", "sam", "yuki", "aisha"]},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "pod-data-ml"),
            kind="human", identity="pod:data_ml",
            label="Data & ML pod",
            description="Data engineering + ML engineering capacity. Owns Snowflake, forecast model, conv-AI, ICP scoring.",
            capacity=6.0, unit="FTE",
            utilization_state="deployed",
            metadata={"category": "human_pod", "members": ["nikita", "hannah", "dimitri", "felix", "naomi", "ravi"]},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "pod-sales"),
            kind="human", identity="pod:sales",
            label="Sales pod (AE + SE)",
            description="Account executives and sales engineers. Pipeline coverage, deal cycles, technical demos.",
            capacity=6.0, unit="FTE",
            utilization_state="deployed",
            metadata={"category": "human_pod", "members": ["liam", "sophie", "marco", "tess", "jordan", "adrienne"]},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "pod-cs"),
            kind="human", identity="pod:customer_success",
            label="Customer Success pod",
            description="CS managers and implementation engineers. Onboarding, renewals, save-plays.",
            capacity=5.0, unit="FTE",
            utilization_state="constrained",
            metadata={"category": "human_pod", "members": ["eve", "adam", "robin", "carlos", "hema"]},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "pod-product-design"),
            kind="human", identity="pod:product_design",
            label="Product & Design pod",
            description="Two PMs and the sole designer. Roadmap stewardship, dashboard v2, onboarding flows.",
            capacity=3.0, unit="FTE",
            utilization_state="deployed",
            metadata={"category": "human_pod", "members": ["caleb", "brielle", "theo"]},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "pod-exec"),
            kind="human", identity="pod:exec",
            label="Founder & exec pool",
            description="Diana (CEO), Sanjay (CTO), Tom (VP Eng), Owen (VP Sales), Priya (Head of CS).",
            capacity=5.0, unit="FTE",
            utilization_state="constrained",
            metadata={"category": "human_pod", "members": ["diana", "sanjay", "tom", "owen", "priya"]},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "pod-ops"),
            kind="human", identity="pod:ops_marketing_finance",
            label="Ops, Marketing & Finance",
            description="Solo functions: marketing (Quinn), finance (Beatriz), recruiting (Olu).",
            capacity=3.0, unit="FTE",
            utilization_state="deployed",
            metadata={"category": "human_pod", "members": ["quinn", "beatriz", "olu"]},
        ),
        # ---- Financial pools --------------------------------------------
        GeneratedResource(
            id=did(COMPANY, "resource", "cash-runway"),
            kind="financial", identity="cash:runway",
            label="Cash runway",
            description="Capital available post-Series A close ($14M). Burn-rate scaled monthly.",
            capacity=14_000_000.0, unit="USD",
            utilization_state="deployed",
            controllability="owned",
            temporal_character="time_limited",
            metadata={"category": "capital", "monthly_burn_target": 740_000},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "gtm-budget"),
            kind="financial", identity="cash:gtm_budget",
            label="GTM budget (annual)",
            description="Sales + marketing programmatic spend (events, demand-gen, AE ramp, partnerships).",
            capacity=2_400_000.0, unit="USD",
            utilization_state="deployed",
            controllability="owned",
            temporal_character="time_limited",
            metadata={"category": "capital", "fiscal_year": "2026"},
        ),
        # ---- Technical platforms ----------------------------------------
        GeneratedResource(
            id=did(COMPANY, "resource", "infra-salesforce-sync"),
            kind="technical", identity="infra:salesforce_sync",
            label="Salesforce sync platform",
            description="The CRM ingestion surface. Maintenance + anchor-customer fires draw heavily on this pool.",
            capacity=10.0, unit="engineer-weeks/quarter",
            utilization_state="constrained",
            metadata={"category": "platform", "owner_pod": "pod-engineering"},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "infra-snowflake"),
            kind="technical", identity="infra:snowflake_warehouse",
            label="Snowflake warehouse",
            description="Per-tenant warehouse. Forecast inputs, BigQuery cutover, dbt pipeline.",
            capacity=1500.0, unit="credits/month",
            utilization_state="deployed",
            metadata={"category": "platform", "owner_pod": "pod-data-ml"},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "infra-ml-compute"),
            kind="technical", identity="infra:ml_compute",
            label="ML training compute",
            description="GPU pool feeding forecast model retraining, conv-AI experiments, ICP scoring research.",
            capacity=600.0, unit="GPU-hours/month",
            utilization_state="available",
            metadata={"category": "platform", "owner_pod": "pod-data-ml"},
        ),
        GeneratedResource(
            id=did(COMPANY, "resource", "infra-api-platform"),
            kind="technical", identity="infra:api_platform",
            label="Production API platform",
            description="Customer-facing API surface — auth, rate limiting, webhook dispatch, idempotency.",
            capacity=8.0, unit="engineer-weeks/quarter",
            utilization_state="deployed",
            metadata={"category": "platform", "owner_pod": "pod-engineering"},
        ),
    ]


def _deployment_quantity_for_state(state: str, on_pod: bool) -> float:
    """How much of a pod's FTE pool a single commitment claims, by state.

    Tuned so a typical 130-commit corpus distributes across pods at
    realistic utilization levels (engineering near 100%, founder/exec
    over 100%, others 70-85%)."""
    if state == "active":
        return 0.30 if on_pod else 0.0
    if state == "at_risk":
        return 0.45 if on_pod else 0.0
    if state == "blocked":
        return 0.25 if on_pod else 0.0
    if state == "done":
        return 0.05 if on_pod else 0.0
    return 0.0


def _resource_deployments_for_pelago(
    commitments: list[GeneratedCommitment],
    actors: list[GeneratedActor],
    sim_dict: dict[str, Any],
    customers: list[GeneratedCustomer],
) -> list[GeneratedResourceDeployment]:
    """Materialize the commitment → resource bridge.

    Each commitment claims a slice of:
    - its owner's pod (always)
    - Customer Success pod (if it serves a customer and the owner isn't
      already in CS — these are the cross-pod handoff hours)
    - one or more technical platforms (if title/owner role implicates
      it via _TECH_KEYWORD_ROUTES)
    - a per-commitment dollar pull on the relevant financial pool
      (cash runway for any active commit; GTM budget for sales-led work)
    """
    role_family_by_actor = {
        ap["actor_id"]: ap.get("role_family", "engineering")
        for ap in sim_dict.get("actor_profiles", [])
    }
    role_by_actor = {
        ap["actor_id"]: ap.get("role", "engineer")
        for ap in sim_dict.get("actor_profiles", [])
    }
    actor_uuid_to_sim_id = {
        did(COMPANY, "actor", ap["actor_id"]): ap["actor_id"]
        for ap in sim_dict.get("actor_profiles", [])
    }
    customer_uuid_set = {c.id for c in customers}

    deployments: list[GeneratedResourceDeployment] = []

    for c in commitments:
        sim_actor_id = actor_uuid_to_sim_id.get(c.owner_id)
        if not sim_actor_id:
            continue
        role_family = role_family_by_actor.get(sim_actor_id, "engineering")
        role = role_by_actor.get(sim_actor_id, "engineer")
        pod_key = _POD_BY_ROLE_FAMILY.get(role_family, "pod-engineering")
        pod_resource_id = did(COMPANY, "resource", pod_key)

        pod_qty = _deployment_quantity_for_state(c.state, on_pod=True)
        if pod_qty > 0:
            deployments.append(GeneratedResourceDeployment(
                resource_id=pod_resource_id,
                commitment_id=c.id,
                deployed_quantity=pod_qty,
            ))

        # Cross-pod CS handoff for customer-bearing commits where the
        # owner isn't already on the CS pod.
        if c.served_by_customer_id and c.served_by_customer_id in customer_uuid_set:
            if pod_key != "pod-cs" and c.state in ("active", "at_risk", "blocked"):
                deployments.append(GeneratedResourceDeployment(
                    resource_id=did(COMPANY, "resource", "pod-cs"),
                    commitment_id=c.id,
                    deployed_quantity=0.10,
                ))

        # Technical platform routes by title keyword. A single commit can
        # consume up to 2 platforms if multiple keywords match (e.g.,
        # "Snowflake migration shard" → snowflake; "API rate-limit" → api).
        title_lc = (c.title or "").lower()
        matched_tech = 0
        for resource_key, keywords, unit_per_commit in _TECH_KEYWORD_ROUTES:
            if matched_tech >= 2:
                break
            if any(k in title_lc for k in keywords):
                # Scale by state — done/closed only contribute residual load.
                scale = 1.0 if c.state in ("active", "at_risk") else 0.4
                deployments.append(GeneratedResourceDeployment(
                    resource_id=did(COMPANY, "resource", resource_key),
                    commitment_id=c.id,
                    deployed_quantity=unit_per_commit * scale,
                ))
                matched_tech += 1

        # Financial pool: every active commit pulls a sliver of cash
        # runway (proxied by per-commit burn share). Sales-led work
        # additionally pulls GTM budget.
        if c.state in ("active", "at_risk", "blocked"):
            deployments.append(GeneratedResourceDeployment(
                resource_id=did(COMPANY, "resource", "cash-runway"),
                commitment_id=c.id,
                deployed_quantity=22_000.0,    # ~$22k of ARR-supporting burn per active commit
            ))
            if role_family == "sales" or role == "vp_sales":
                deployments.append(GeneratedResourceDeployment(
                    resource_id=did(COMPANY, "resource", "gtm-budget"),
                    commitment_id=c.id,
                    deployed_quantity=14_000.0,
                ))

    return deployments


# =====================================================================
# Signals — sample from the corpus shards
# =====================================================================

def _read_shard_signals() -> list[dict[str, Any]]:
    """Read signals from the corpus shards. Each entry is a raw dict
    matching the LSOB Signal model."""
    if not SHARDS_DIR.is_dir():
        raise FileNotFoundError(
            f"corpus shards not found at {SHARDS_DIR}; run "
            f"`uv run lsob-simulation run --config {SPEC_PATH} --shards corpora/pelago/` first"
        )
    out: list[dict[str, Any]] = []
    for sf in sorted(SHARDS_DIR.glob("day-*.jsonl")):
        for raw in sf.read_text().splitlines():
            if raw.strip():
                out.append(json.loads(raw))
    return out


def _sample_signals(
    raw_signals: list[dict[str, Any]],
    target: int = SIGNAL_SAMPLE_TARGET,
) -> list[dict[str, Any]]:
    """Sample ~target signals deterministically. Always include signals
    relevant to the 7 target recommendations (we'll let `find_signals_containing`
    pick supporting_observation_ids from this same sample). Spread the
    rest evenly across actors, channels, and time."""
    rng = random.Random(SIGNAL_SAMPLE_SEED)
    n = len(raw_signals)
    if n <= target:
        return raw_signals

    # Anchor a small "must-include" set with signals that mention the key
    # recommendation themes — guarantees the recommendations have evidence.
    keywords = [
        "Beacon Analytics", "Northvale", "Conduit Software",
        "Salesforce sync", "VP Eng", "ICP scoring", "conversation-AI",
        "Tilt", "data-warehouse", "1:1", "QBR", "renewal",
        "Strand Labs",
    ]
    must_include: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for s in raw_signals:
        text = s.get("content_text", "")
        if any(k in text for k in keywords):
            must_include.append(s)
            seen_ids.add(s["signal_id"])
        if len(must_include) >= 60:
            break

    remaining_budget = target - len(must_include)
    candidates = [s for s in raw_signals if s["signal_id"] not in seen_ids]

    # Stratify by author so each actor surfaces in the bundle.
    by_author: dict[str, list[dict[str, Any]]] = {}
    for s in candidates:
        by_author.setdefault(s["author_id"], []).append(s)
    per_author = max(1, remaining_budget // max(1, len(by_author)))
    sampled: list[dict[str, Any]] = []
    for author_id, sigs in sorted(by_author.items()):
        rng.shuffle(sigs)
        sampled.extend(sigs[:per_author])

    # Top up if under-target via a random sample of leftovers.
    if len(sampled) < remaining_budget:
        leftover_ids = {s["signal_id"] for s in sampled}
        leftovers = [s for s in candidates if s["signal_id"] not in leftover_ids]
        rng.shuffle(leftovers)
        sampled.extend(leftovers[: remaining_budget - len(sampled)])

    out = must_include + sampled[:remaining_budget]
    out.sort(key=lambda s: s["timestamp"])
    return out


def _signals_to_generated(
    sampled: list[dict[str, Any]],
    actors: list[GeneratedActor],
    customers: list[GeneratedCustomer],
    commitments: list[GeneratedCommitment],
) -> list[GeneratedSignal]:
    actor_id_set = {a.id for a in actors}
    customer_id_set = {c.id for c in customers}
    commitment_id_set = {c.id for c in commitments}

    out: list[GeneratedSignal] = []
    for s in sampled:
        author_uuid = did(COMPANY, "actor", s["author_id"])
        if author_uuid not in actor_id_set:
            continue  # skip signals authored by an actor not in the spec
        channel_raw = s["source_channel"]
        channel = _CHANNEL_MAP.get(channel_raw, channel_raw)
        mentions: list[EntityMention] = []
        meta = s.get("metadata", {}) or {}
        if cref := meta.get("commitment_ref"):
            cuuid = did(COMPANY, "commitment", cref)
            if cuuid in commitment_id_set:
                mentions.append(EntityMention(type="commitment", id=cuuid))
        if cust := meta.get("customer_ref"):
            cuuid = did(COMPANY, "customer", cust)
            if cuuid in customer_id_set:
                mentions.append(EntityMention(type="customer", id=cuuid))
        out.append(GeneratedSignal(
            id=did(COMPANY, "signal", s["signal_id"]),
            source_channel=channel,
            source_ref=f"sim-{s['signal_id']}",
            author_id=author_uuid,
            occurred_at=s["timestamp"],
            content_text=s["content_text"],
            entities_mentioned=mentions,
        ))
    return out


# =====================================================================
# Models — loaded from the synthesis store
# =====================================================================

# Sim entity ids (e.g. "cust-beacon", "C-sf-stabilize", "G-2-multi-crm",
# "D-2-conv-ai-first") get remapped to demo UUIDs via did(). Actor ids
# in the synthesis store are bare names ("diana", "maya") matching the
# spec's actor_profiles[].actor_id, so the same did() call covers them.

_VALID_MODEL_KINDS = {
    "state", "relation", "prediction", "pattern", "pattern_instance",
    "capability_assessment", "hypothesis", "concern",
    "market_assessment", "environmental_trend",
}


def _remap_scope_entity(ent: dict[str, Any]) -> dict[str, Any] | None:
    """Synth scope entity → demo scope entity (UUIDs).

    Returns None if the entity references a sim id that doesn't exist
    in the demo bundle (e.g., a rate-generated commitment we didn't
    sample in)."""
    t = ent.get("type")
    sid = ent.get("id")
    if not t or not sid:
        return None
    if t in ("commitment", "customer", "goal", "decision"):
        return {"type": t, "id": did(COMPANY, t, sid)}
    return None


def _models_from_synthesis(
    actors: list[GeneratedActor],
    customers: list[GeneratedCustomer],
    commitments: list[GeneratedCommitment],
    goals: list[GeneratedGoal],
    decisions: list[GeneratedDecision],
    signals: list[GeneratedSignal],
) -> list[GeneratedModel]:
    """Load the curated synthesis store and remap to demo UUIDs."""
    if not SYNTH_MODELS_PATH.is_file():
        return []
    raw = json.loads(SYNTH_MODELS_PATH.read_text()).get("models") or {}

    actor_id_set = {a.id for a in actors}
    commit_id_set = {c.id for c in commitments}
    customer_id_set = {c.id for c in customers}
    goal_id_set = {g.id for g in goals}
    decision_id_set = {d.id for d in decisions}
    bundle_entity_ids = (
        commit_id_set | customer_id_set | goal_id_set | decision_id_set
    )
    # Map the corpus signal_id → bundle signal UUID, but only for the
    # subset we sampled into the bundle.
    signal_id_set = {s.id for s in signals}

    # First pass: compute new model UUIDs so supporting_model_ids can be
    # rewritten in the second pass.
    new_model_id: dict[str, str] = {}
    for orig_id, model in raw.items():
        kind = model.get("kind")
        if kind not in _VALID_MODEL_KINDS:
            continue  # e.g. recommendation kind — handled by GeneratedRecommendation
        new_model_id[orig_id] = did(COMPANY, "model", orig_id)

    out: list[GeneratedModel] = []
    for orig_id, model in raw.items():
        kind = model.get("kind")
        if kind not in _VALID_MODEL_KINDS:
            continue
        new_id = new_model_id[orig_id]

        # Remap scope_actor_ids → demo UUIDs, drop unknown actors
        actor_uuids = [
            did(COMPANY, "actor", a) for a in model.get("scope_actor_ids") or []
        ]
        actor_uuids = [a for a in actor_uuids if a in actor_id_set]

        # Remap scope_entities → demo {type,id}, drop entities not in bundle
        scope_ents: list[dict[str, Any]] = []
        for ent in model.get("scope_entities") or []:
            mapped = _remap_scope_entity(ent)
            if mapped and mapped["id"] in bundle_entity_ids:
                scope_ents.append(mapped)

        # An unscoped Model is invisible to the system — skip
        if not actor_uuids and not scope_ents:
            continue

        # Remap supporting_observation_ids: only keep refs to sampled signals
        supp_obs: list[str] = []
        for sig_id in model.get("supporting_observation_ids") or []:
            sig_uuid = did(COMPANY, "signal", sig_id)
            if sig_uuid in signal_id_set:
                supp_obs.append(sig_uuid)

        # Remap supporting_model_ids: only keep refs to other emitted models
        supp_models = [
            new_model_id[m] for m in model.get("supporting_model_ids") or []
            if m in new_model_id
        ]

        # Falsifier passthrough — synth store sometimes uses a string,
        # sometimes a {condition, threshold, observable_via} dict. The
        # GeneratedModel schema accepts a dict-or-None.
        falsifier = model.get("falsifier")
        if isinstance(falsifier, str):
            falsifier = {"observable_via": falsifier}

        # Confidence bounds [0.05, 0.95]
        conf = float(model.get("confidence", 0.7))
        conf = max(0.05, min(0.95, conf))

        # Predictions must have evaluate_at. The synthesis store has it
        # at top level on most, inside proposition on a few, missing on
        # the rest — fall back to scope_temporal["as_of"] or a sentinel.
        evaluate_at = model.get("evaluate_at")
        prop = model.get("proposition") or {}
        if not evaluate_at and isinstance(prop, dict):
            evaluate_at = prop.get("evaluate_at")
        if kind == "prediction" and not evaluate_at:
            scope_t = model.get("scope_temporal") or {}
            evaluate_at = scope_t.get("as_of") or "2026-12-31T00:00:00Z"

        out.append(GeneratedModel(
            id=new_id,
            kind=kind,
            natural=(model.get("natural") or "").strip()[:500] or "synthesized model",
            proposition=prop,
            confidence=conf,
            scope_actor_ids=actor_uuids,
            scope_entities=scope_ents,
            scope_temporal=model.get("scope_temporal") or {"window": "current"},
            falsifier=falsifier,
            supporting_observation_ids=supp_obs,
            supporting_model_ids=supp_models,
            evaluate_at=evaluate_at,
        ))
    return out


# =====================================================================
# Recommendations
# =====================================================================

def _recommendations_from_spec(
    extras: dict[str, Any],
    actors: list[GeneratedActor],
    commitments: list[GeneratedCommitment],
    goals: list[GeneratedGoal],
    decisions: list[GeneratedDecision],
    signals: list[GeneratedSignal],
    sim_dict: dict[str, Any],
) -> list[GeneratedRecommendation]:
    ceo_id = did(COMPANY, "actor", sim_dict["company_metadata"]["ceo_actor_id"])

    # Map kinds to which target Act they should point at.
    spec_recs = list(extras.get("recommendations", []))
    out: list[GeneratedRecommendation] = []

    # Helper: by id lookups.
    by_commitment_seed_id = {
        s["commitment_id"]: did(COMPANY, "commitment", s["commitment_id"])
        for s in sim_dict.get("commitment_seeds", [])
    }
    by_goal_id = {
        g["goal_id"]: did(COMPANY, "goal", g["goal_id"])
        for g in sim_dict.get("goals", [])
    }

    # Map each spec recommendation to a target. Hand-picked since the spec's
    # 7 recs have specific narratives.
    targets: list[tuple[str, str, list[str]]] = [
        # (target_type, target_id, signal-text keywords for evidence)
        ("commitment", by_commitment_seed_id["C-sf-stabilize"],   ["Salesforce sync", "Beacon"]),
        ("goal",       by_goal_id["G-2-multi-crm"],               ["Conduit", "Salesforce sync", "escalat"]),
        ("goal",       by_goal_id["G-6-vp-eng-successor"],        ["VP Eng", "successor", "leadership"]),
        ("commitment", by_commitment_seed_id["C-conv-ai-v1"],     ["conversation-AI", "ICP scoring"]),
        ("commitment", by_commitment_seed_id["C-strand-onboard"], ["Strand", "Conduit", "drift", "QBR"]),
        ("decision",   did(COMPANY, "decision", "D-3-snowflake"), ["Snowflake", "data-warehouse", "1:1"]),
        ("commitment", by_commitment_seed_id["C-tilt-save-play"], ["Tilt", "renewal"]),
    ]

    for rec_idx, (rec, (target_type, target_id, evidence_keywords)) in enumerate(zip(spec_recs, targets)):
        rec_id = did(COMPANY, "rec", f"r_{rec_idx:02d}")
        evidence = find_signals_containing(signals, *evidence_keywords, limit=4)
        out.append(GeneratedRecommendation(
            id=rec_id,
            proposition_text=rec["proposition"],
            target_act_ref=TargetActRef(type=target_type, id=target_id),
            proposed_change={"operation": "review", "payload": {"kind": rec["kind"]}},
            expected_impact_usd=float(rec.get("impact_usd", 0)),
            supporting_observation_ids=evidence,
            supporting_model_ids=[],
            target_actor_id=ceo_id,
        ))
    return out


# =====================================================================
# Top-level
# =====================================================================

def build_bundle() -> tuple[GeneratedBundle, dict[str, Any]]:
    sim_dict, extras = load_spec(SPEC_PATH)

    actors = _actors_from_spec(sim_dict)
    customers = _customers_from_spec(sim_dict)
    goals = _goals_from_spec(sim_dict)
    decisions = _decisions_from_spec(sim_dict)
    seeded_commits = _commitments_from_spec(sim_dict)
    actor_id_set = {a.id for a in actors}
    customer_id_set = {c.id for c in customers}
    seeded_sim_ids = {s["commitment_id"] for s in sim_dict.get("commitment_seeds", [])}
    rate_commits = _rate_generated_commitments_from_gt(
        actor_id_set, customer_id_set, seeded_sim_ids, sim_dict,
    )
    commitments = seeded_commits + rate_commits

    raw_signals = _read_shard_signals()
    sampled = _sample_signals(raw_signals)
    signals = _signals_to_generated(sampled, actors, customers, commitments)

    recommendations = _recommendations_from_spec(
        extras, actors, commitments, goals, decisions, signals, sim_dict,
    )
    models = _models_from_synthesis(
        actors, customers, commitments, goals, decisions, signals,
    )

    # Capacity resources + commitment → resource deployments. The 13
    # capacity resources are static for Pelago; deployments are derived
    # from each commitment's owner role_family + title keywords.
    resources = _capacity_resources_for_pelago()
    resource_deployments = _resource_deployments_for_pelago(
        commitments, actors, sim_dict, customers,
    )

    bundle = GeneratedBundle(
        company_id=COMPANY,
        ceo_actor_id=did(COMPANY, "actor", sim_dict["company_metadata"]["ceo_actor_id"]),
        actors=actors,
        customers=customers,
        goals=goals,
        decisions=decisions,
        commitments=commitments,
        signals=signals,
        models=models,
        recommendations=recommendations,
        resources=resources,
        resource_deployments=resource_deployments,
    )
    # Pull validator-target counts from the extras for validate_bundle.
    spec_for_validation = {
        "company_id": COMPANY,
        "actor_count":          len(actors),
        "customer_count":       len(customers),
        "goal_count":           extras.get("goal_count", len(goals)),
        "decision_count":       extras.get("decision_count", len(decisions)),
        "commitment_count":     extras.get("commitment_count", len(commitments)),
        "recommendation_count": extras.get("recommendation_count", len(recommendations)),
    }
    return bundle, spec_for_validation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emit", action="store_true",
                        help="Write the SQL snapshot")
    parser.add_argument("--out", default=str(DEFAULT_OUT),
                        help="Output path for the SQL snapshot")
    parser.add_argument("--compress", action="store_true",
                        help="Zstd-compress the SQL snapshot (.sql.zst)")
    args = parser.parse_args()

    print(f"Building Pelago bundle...")
    bundle, spec_for_validation = build_bundle()
    print(f"  actors:           {len(bundle.actors)}")
    print(f"  customers:        {len(bundle.customers)}")
    print(f"  goals:            {len(bundle.goals)}")
    print(f"  decisions:        {len(bundle.decisions)}")
    print(f"  commitments:      {len(bundle.commitments)}")
    print(f"  signals:          {len(bundle.signals)}")
    print(f"  models:           {len(bundle.models)}")
    print(f"  recommendations:  {len(bundle.recommendations)}")
    print(f"  resources:        {len(bundle.resources)}")
    print(f"  deployments:      {len(bundle.resource_deployments)}")

    print("Validating...")
    errors = validate_bundle(bundle, spec=spec_for_validation)
    if errors:
        print(f"  {len(errors)} validation error(s):", file=sys.stderr)
        for e in errors[:20]:
            print(f"    - {e}", file=sys.stderr)
        return 1
    print("  OK")

    if args.emit:
        out_path = Path(args.out)
        written = write_sql(bundle, out_path, compress=args.compress)
        print(f"Wrote SQL snapshot to {written}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
