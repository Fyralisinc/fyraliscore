"""Truss — hand-authored demo bundle.

Series A, 40-person AI-native developer infrastructure company. The
spec lives in demo/generation/specs/truss.yaml; this module authors the
entities to that spec without burning LLM budget. Re-run produces the
same SQL because every UUID is uuid5(DEMO_NS, "truss|<kind>|<key>").

Usage:
  python -m demo.generation.built.truss             # validate only
  python -m demo.generation.built.truss --emit      # write SQL snapshot

Counts: 40 actors, 35 customers, 7 goals, 6 decisions, 140 commitments,
~250 signals, 7 recommendations. Validator tolerates ±10%.
"""
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path
from typing import Any

from demo.generation.built._helpers import (
    days_ago,
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
    GeneratedSignal,
    TargetActRef,
)
from demo.generation.sql_emit import write_sql
from demo.generation.validate import validate_bundle


COMPANY = "truss"


# =====================================================================
# Actors — 40 total. Maya (CEO/founder) + Jules (co-founder) + 38 more.
# =====================================================================

# Each tuple: (key, name, role, manager_key_or_none, brief)
ACTOR_SPECS: list[tuple[str, str, str, str | None, str]] = [
    # Founders
    ("maya",     "Maya Patel",       "founder",
     None, "Founder & CEO. Comes from systems-eng background; tends to over-index on engineering detail."),
    ("jules",    "Jules Park",       "founder",
     "maya", "Co-founder, Head of Product. Ex-Stripe PM; thinks in customer narratives."),

    # Engineering leadership
    ("tom",      "Tom Bishop",       "vp_eng",
     "maya", "VP Engineering. Quietly capable; 1:1 with Maya is overdue."),
    # Senior engineers
    ("sarah",    "Sarah Chen",       "engineer",
     "tom", "Lead engineer on the API surface. On incident rotation 4/6 weeks."),
    ("marcus",   "Marcus Lee",       "engineer",
     "tom", "Senior engineer, infra. Calm; raises capacity flags."),
    ("theo",     "Theo Schmidt",     "engineer",
     "tom", "Senior engineer, runtime. Owns the request pipeline."),
    ("rae",      "Rae Okafor",       "engineer",
     "tom", "Senior engineer, SDKs."),
    # Mid-level engineers
    ("priyank",  "Priyank Joshi",    "engineer",
     "tom", "Mid-level, API layer. Eager but stretched."),
    ("hannah",   "Hannah Cole",      "engineer",
     "tom", "Mid-level, observability."),
    ("dimitri",  "Dimitri Volkov",   "engineer",
     "tom", "Mid-level, billing/usage metering."),
    ("yuki",     "Yuki Tanaka",      "engineer",
     "tom", "Junior, SDKs. Ramping fast."),

    # Product
    ("priya",    "Priya Shah",       "pm",
     "jules", "PM, customer-facing. Runs design-partner sync."),
    ("ben",      "Ben Caldwell",     "pm",
     "jules", "PM, platform. Owns roadmap rationale doc."),

    # Design
    ("grace",    "Grace Liu",        "designer",
     "jules", "Senior designer; doubles as DX writer."),
    ("noah",     "Noah Briggs",      "designer",
     "jules", "Mid-level designer, dashboard surfaces."),

    # GTM — sales & CS
    ("monica",   "Monica Reyes",     "head_sales",
     "maya", "Head of Sales. First sales hire; self-directed."),
    ("diego",    "Diego Rivera",     "sales",
     "monica", "AE, design partner accounts."),
    ("riley",    "Riley Kim",        "sales",
     "monica", "AE, mid-market."),
    ("avery",    "Avery Nakamura",   "customer_success",
     "monica", "CSM, design partners. Knows every customer's ARR off-hand."),
    ("imani",    "Imani Black",      "customer_success",
     "monica", "CSM, prospect onboarding."),
    ("kai",      "Kai Sundberg",     "customer_success",
     "monica", "CSM, technical accounts."),

    # Marketing
    ("noor",     "Noor Hassan",      "marketing",
     "jules", "Head of Marketing — content lead."),
    ("lina",     "Lina Petrova",     "marketing",
     "jules", "Marketing ops; runs the launch calendar."),

    # Ops/leadership
    ("david",    "David Quan",       "cfo",
     "maya", "Fractional CFO; advisor to the round."),
    ("simone",   "Simone Adler",     "head_ops",
     "maya", "Head of Operations; chief of staff in everything but title."),
    ("ravi",     "Ravi Mehta",       "head_finance",
     "david", "Finance lead; tracks burn vs runway weekly."),

    # Recruiting
    ("hugh",     "Hugh Brennan",     "recruiter",
     "simone", "In-house recruiter; pipeline for 2 open eng roles."),

    # Legal/regulatory advisor
    ("lex",      "Lex Tran",         "legal_advisor",
     "maya", "External counsel; data-handling and SSO compliance lens."),

    # Contractors / advisors
    ("edie",     "Edie Marquez",     "contractor_advisor",
     "maya", "Advisor; ex-Twilio CRO. Deep enterprise sales lens."),
    ("kirby",    "Kirby Winters",    "contractor_advisor",
     "maya", "Advisor; ex-DataDog VP Eng. Scale lens."),
    ("ramon",    "Ramon Vega",       "contractor_advisor",
     "tom", "Contract engineer, perf work."),
    ("blair",    "Blair Sutton",     "contractor_advisor",
     "noor", "Contract content writer."),
    ("morgan",   "Morgan Tate",      "contractor_advisor",
     "monica", "Contract SDR; outbound experiments."),

    # Junior / new hires
    ("zion",     "Zion Akinleye",    "engineer",
     "tom", "New grad, joined 3 weeks ago."),
    ("oksana",   "Oksana Hill",      "engineer",
     "tom", "Joined last quarter from Cloudflare."),
    ("felix",    "Felix Donnelly",   "engineer",
     "tom", "New hire, 2 months in. Owns billing observability."),
    ("ines",     "Ines Carvalho",    "pm",
     "jules", "PM, growth. New to the team this quarter."),
    ("toby",     "Toby Larkin",      "designer",
     "jules", "Contract designer; brand work."),
    ("samira",   "Samira Khan",      "engineer",
     "tom", "Senior engineer; data plane."),
    ("walt",     "Walt Friesen",     "engineer",
     "tom", "Mid-level engineer; CLI + tooling."),
]


def build_actors() -> list[GeneratedActor]:
    out: list[GeneratedActor] = []
    for key, name, role, mgr_key, brief in ACTOR_SPECS:
        out.append(GeneratedActor(
            id=did(COMPANY, "actor", key),
            name=name,
            role=role,
            manager_id=did(COMPANY, "actor", mgr_key) if mgr_key else None,
            personality_brief=brief,
            email=f"{key}@truss.dev",
        ))
    return out


# =====================================================================
# Customers — 35 (28 paying + 7 prospect). Spec ARR target ~$4.5M.
# =====================================================================

# (key, company_name, arr_usd, segment, health, contacts)
CUSTOMER_SPECS: list[tuple[str, str, float, str, str, list[str]]] = [
    # Headline design partners (the 3 who asked about SSO)
    ("linear",      "Linear",          280000, "design_partner", "watching",   ["Eng Lead — Karri Saarinen"]),
    ("vercel",      "Vercel",          340000, "design_partner", "healthy",    ["Platform Lead — Lee Robinson"]),
    ("replit",      "Replit",          210000, "design_partner", "watching",   ["Eng — Amjad Masad"]),
    # Mid-tier design partners
    ("modal",       "Modal Labs",      155000, "design_partner", "healthy",    ["Founder — Erik Bernhardsson"]),
    ("cursor",      "Cursor Labs",     180000, "design_partner", "healthy",    ["Founder — Aman Sanger"]),
    ("supabase",    "Supabase",        220000, "enterprise",     "healthy",    ["DevRel — Paul Copplestone"]),
    # Paying enterprise
    ("scale",       "Scale AI",        310000, "enterprise",     "healthy",    ["Eng VP — Alex Wang"]),
    ("anthropic",   "Anthropic",       265000, "enterprise",     "healthy",    ["Infra Lead — Tom B."]),
    ("openrouter",  "OpenRouter",      125000, "enterprise",     "healthy",    ["Founder — Alex Atallah"]),
    ("fly",         "Fly.io",          145000, "enterprise",     "healthy",    ["Platform — Kurt Mackey"]),
    # Mid-market
    ("dub",         "Dub.co",           65000, "mid_market",     "healthy",    ["Founder — Steven Tey"]),
    ("turso",       "Turso",            58000, "mid_market",     "healthy",    ["DevRel — Glauber Costa"]),
    ("axiom",       "Axiom",            72000, "mid_market",     "healthy",    ["Founder — Neil Jagdish Patel"]),
    ("clerk",       "Clerk",            85000, "mid_market",     "healthy",    ["Founder — Colin Sidoti"]),
    ("upstash",     "Upstash",          69000, "mid_market",     "healthy",    ["Founder — Enes Akar"]),
    ("railway",     "Railway",          54000, "mid_market",     "healthy",    ["Eng Lead — Jake Cooper"]),
    ("planetscale", "PlanetScale",     105000, "mid_market",     "healthy",    ["Founder — Sam Lambert"]),
    ("nhost",       "Nhost",            42000, "mid_market",     "watching",   ["Founder — Johan Eliasson"]),
    ("convex",      "Convex",           94000, "mid_market",     "healthy",    ["Founder — James Cowling"]),
    # SMB
    ("retool",      "Retool",           68000, "smb",            "healthy",    ["Eng — Tony Wang"]),
    ("hex",         "Hex",              52000, "smb",            "healthy",    ["Founder — Barry McCardel"]),
    ("vendure",     "Vendure",          24000, "smb",            "healthy",    ["Founder — Michael Bromley"]),
    ("ourstack",    "OurStack",         18000, "smb",            "watching",   ["Founder — anonymous"]),
    ("crystallize", "Crystallize",      32000, "smb",            "healthy",    ["Eng — Hakon Ohrn"]),
    ("medusa",      "Medusa",           48000, "smb",            "healthy",    ["Founder — Sebastian Rindom"]),
    # Drift account
    ("smallco",     "SmallCo Studios",  21000, "smb",            "at_risk",    ["Founder — Drew Kim"]),
    # Watching / quiet
    ("paperspace",  "Paperspace",       58000, "mid_market",     "watching",   ["Eng — Daniel Kobran"]),
    ("baseten",     "BaseTen",          92000, "mid_market",     "healthy",    ["Founder — Tuhin Srivastava"]),
    # Prospects (no ARR yet — counted as customers per spec)
    ("notion",      "Notion (prospect)",     0, "prospect",       "watching",   ["Eng VP — Akshay Kothari"]),
    ("airtable",    "Airtable (prospect)",   0, "prospect",       "watching",   ["Platform — Howie Liu"]),
    ("retool_pro",  "Retool (expansion)",    0, "prospect",       "watching",   ["Eng — Tony Wang"]),
    ("posthog",     "PostHog (prospect)",    0, "prospect",       "watching",   ["Founder — James Hawkins"]),
    ("wandb",       "Weights & Biases (prospect)", 0, "prospect", "watching",   ["Eng — Lukas Biewald"]),
    ("databricks_p","Databricks (prospect)", 0, "prospect",       "watching",   ["Platform — Ali Ghodsi"]),
    ("snowflake_p", "Snowflake (prospect)",  0, "prospect",       "watching",   ["Platform — Frank Slootman"]),
]


def build_customers() -> list[GeneratedCustomer]:
    return [
        GeneratedCustomer(
            id=did(COMPANY, "customer", k),
            company_name=name,
            arr_usd=arr,
            segment=seg,                     # type: ignore[arg-type]
            current_health=health,           # type: ignore[arg-type]
            primary_contacts=contacts,
        )
        for (k, name, arr, seg, health, contacts) in CUSTOMER_SPECS
    ]


# =====================================================================
# Goals — 7 strategic + operational
# =====================================================================

GOAL_SPECS: list[tuple[str, str, str, str, str | None, str]] = [
    # (key, title, description, owner_key, parent_key, altitude)
    ("ga_2026",      "Hit $10M ARR by end of 2026",
     "Double from current ~$4.5M ARR. Mix shift: enterprise tier from 35% → 50% of new bookings.",
     "maya", None, "strategic"),
    ("g_ent_motion", "Land 3 enterprise design partners with stable v1 API",
     "Convert 3 of our top design partners to multi-year enterprise contracts. SSO + audit logs are gating.",
     "monica", "ga_2026", "strategic"),
    ("g_runway",     "Maintain 18-month runway through 2027",
     "Burn at <$650K/month; revenue growth absorbs the rest.",
     "david", "ga_2026", "strategic"),
    ("g_api_v1",     "Ship stable API v1 by Q3 2026",
     "Lock the API contract for at least 18 months. Three customers explicitly asked for this.",
     "ben", "g_ent_motion", "operational"),
    ("g_sso",        "Ship SSO + SAML for design partners",
     "SSO blocks Linear/Vercel/Replit from expanding. Design partner contract requires it.",
     "priya", "g_ent_motion", "operational"),
    ("g_observ",     "Production-grade observability surface",
     "Latency, error budgets, per-tenant dashboards. Foundational for the enterprise tier.",
     "tom", "g_ent_motion", "operational"),
    ("g_hire_eng",   "Hire 2 senior engineers by end of Q2",
     "Two open roles blocking critical path. Recruiter needs founder time on closing calls.",
     "tom", "ga_2026", "operational"),
]


def build_goals() -> list[GeneratedGoal]:
    out: list[GeneratedGoal] = []
    for key, title, desc, owner, parent, alt in GOAL_SPECS:
        out.append(GeneratedGoal(
            id=did(COMPANY, "goal", key),
            title=title,
            description=desc,
            owner_id=did(COMPANY, "actor", owner),
            target_date=days_from_now(180),
            parent_goal_id=did(COMPANY, "goal", parent) if parent else None,
            altitude=alt,                    # type: ignore[arg-type]
        ))
    return out


# =====================================================================
# Decisions — 6
# =====================================================================

DECISION_SPECS: list[tuple[str, str, str, str, dict[str, Any], list[str]]] = [
    # (key, title, decision_text, rationale, scope, revisit_triggers)
    ("d_api_redesign",
     "Adopt new API surface for v1 (post-launch)",
     "Migrate from beta surface to redesigned v1 with breaking changes. Customers given 60-day deprecation window.",
     "Beta surface had inconsistent error semantics across SDKs. Long-term ergonomics > short-term pain. Decided before three customers requested a stable contract.",
     {"area": "engineering"},
     ["3+ customers explicitly request stability over redesign", "Lead engineer flags re-scope risk"]),
    ("d_no_self_host",
     "No self-hosted offering through 2026",
     "Cloud-only. Self-host is deferred indefinitely.",
     "Operational complexity vs revenue not justified yet. Three prospects asked.",
     {"area": "product"},
     ["Enterprise prospect blocks on self-host AND ACV >= $500K"]),
    ("d_sso_priority",
     "SSO is Q2 not Q3",
     "Pull SSO + SAML into Q2 from Q3 backlog. Pulls a roadmap item out.",
     "Three design partners asked in past 60 days. Revenue exposure ~$280K ARR.",
     {"area": "roadmap"},
     ["Customer requests fall to <2 in 30 days"]),
    ("d_pricing_v2",
     "Move from per-seat to usage-based pricing",
     "Adopt per-request + flat platform fee. Existing customers grandfathered for 12 months.",
     "Per-seat undercharges high-volume customers and overcharges design partners.",
     {"area": "pricing"},
     ["Net revenue retention drops below 110%"]),
    ("d_runtime_lang",
     "Runtime stays in Rust",
     "No rewrite to Go or TypeScript despite hiring pressure.",
     "Performance ceiling matters for our differentiator. Hiring Rust talent has been viable.",
     {"area": "engineering"},
     ["Hiring takes >120 days for backend roles"]),
    ("d_design_partner_close",
     "Stop new design-partner sign-ups; convert existing to contracts",
     "We have enough partners to inform v1. Energy goes to conversion not acquisition.",
     "Founder time on partner support is unsustainable.",
     {"area": "go-to-market"},
     ["3+ partners convert to enterprise"]),
]


def build_decisions() -> list[GeneratedDecision]:
    return [
        GeneratedDecision(
            id=did(COMPANY, "decision", key),
            title=title,
            decision_text=text,
            rationale=rationale,
            scope=scope,
            revisit_triggers=triggers,
        )
        for (key, title, text, rationale, scope, triggers) in DECISION_SPECS
    ]


# =====================================================================
# Commitments — 140 total. Mix of: per-customer landing/expansion,
# per-goal delivery, internal eng/ops, GTM threads. Programmatically
# constructed but with realistic, demo-rehearsable titles.
# =====================================================================


def build_commitments(actors: list[GeneratedActor],
                      customers: list[GeneratedCustomer],
                      goals: list[GeneratedGoal],
                      decisions: list[GeneratedDecision]) -> list[GeneratedCommitment]:
    """Build ~140 commitments. Deterministic — same actors / customers
    in → same commitments out."""
    actor_by_key = {a.id: a for a in actors}
    actor_keys = {k: did(COMPANY, "actor", k) for k, *_ in ACTOR_SPECS}
    cust_by_key = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    goal_by_key = {k: did(COMPANY, "goal", k) for k, *_ in GOAL_SPECS}
    dec_by_key = {k: did(COMPANY, "decision", k) for k, *_ in DECISION_SPECS}

    rng = random.Random(42)         # determinism

    out: list[GeneratedCommitment] = []

    # --- Engineering capacity-pressure commitments (Sarah is overloaded) -
    eng_keys = ["sarah", "marcus", "theo", "rae", "priyank", "hannah",
                "dimitri", "yuki", "samira", "walt", "oksana", "felix"]

    # Sarah owns 7 — that's the capacity pressure on display.
    sarah_titles = [
        ("c_sarah_apiv1_breakdown",  "Break down v1 API surface into RFCs",        "active",    "g_api_v1",    None,     ["d_api_redesign"]),
        ("c_sarah_ratelimit",        "Rewrite rate limiter for v1 contract",       "active",    "g_api_v1",    None,     []),
        ("c_sarah_sso_lead",         "Lead SSO/SAML implementation",               "at_risk",   "g_sso",       None,     ["d_sso_priority"]),
        ("c_sarah_oncall",           "Stabilize on-call rotation pager noise",     "active",    "g_observ",    None,     []),
        ("c_sarah_linear_review",    "Architect review with Linear eng",           "active",    "g_ent_motion","linear", []),
        ("c_sarah_vercel_review",    "Architect review with Vercel eng",           "active",    "g_ent_motion","vercel", []),
        ("c_sarah_replit_review",    "Architect review with Replit eng",           "active",    "g_ent_motion","replit", []),
    ]
    for key, title, state, gk, ck, dks in sarah_titles:
        out.append(_make_commitment(
            key, title, state, "sarah", actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, dks, contributors=["theo"], rng=rng,
        ))

    # Tom (VP Eng) — coordination commitments
    tom_titles = [
        ("c_tom_hiring_eng",    "Close 2 senior eng hires",            "active",    "g_hire_eng",  None,     []),
        ("c_tom_runbooks",      "Author production runbooks",          "active",    "g_observ",    None,     []),
        ("c_tom_apiv1_signoff", "Sign off on v1 API contract",         "active",    "g_api_v1",    None,     ["d_api_redesign"]),
        ("c_tom_pager_rotation","Restructure pager rotation",          "at_risk",   "g_observ",    None,     []),
        ("c_tom_eng_review",    "Quarterly eng review with Maya",      "active",    None,           None,     []),
    ]
    for key, title, state, gk, ck, dks in tom_titles:
        out.append(_make_commitment(
            key, title, state, "tom", actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, dks, rng=rng,
        ))

    # Other senior engineers — 4-7 each
    for owner in ["marcus", "theo", "rae", "samira"]:
        for i in range(rng.randint(4, 6)):
            cust_choice = None
            if i < 2 and rng.random() < 0.5:
                cust_choice = rng.choice(list(cust_by_key.keys())[:12])
            out.append(_make_commitment(
                f"c_{owner}_{i}",
                _eng_title(owner, i, rng),
                rng.choice(["active", "active", "active", "blocked", "at_risk", "proposed"]),
                owner, actor_keys, goal_by_key, cust_by_key, dec_by_key,
                rng.choice(["g_api_v1", "g_observ", "g_sso", "g_api_v1"]),
                cust_choice,
                [],
                rng=rng,
            ))

    # Mid-level engineers — 4-5 each
    for owner in ["priyank", "hannah", "dimitri", "yuki", "walt", "felix", "oksana", "zion"]:
        for i in range(rng.randint(3, 5)):
            out.append(_make_commitment(
                f"c_{owner}_{i}",
                _eng_title(owner, i, rng),
                rng.choice(["active", "active", "proposed", "blocked", "at_risk"]),
                owner, actor_keys, goal_by_key, cust_by_key, dec_by_key,
                rng.choice(["g_api_v1", "g_observ", None]),
                None, [],
                rng=rng,
            ))

    # PM commitments — Priya runs design-partner sync
    pm_titles = [
        # (owner_key, key, title, state, goal, customer)
        ("priya", "c_priya_dp_sync",   "Run weekly design-partner sync", "active",    "g_ent_motion", None),
        ("priya", "c_priya_v1_pmf",    "Validate v1 API contract with 3 partners", "active", "g_api_v1", None),
        ("priya", "c_priya_sso_pmf",   "Drive SSO requirements doc",     "active",    "g_sso",        None),
        ("priya", "c_priya_linear_qbr","Linear QBR prep",                "active",    "g_ent_motion", "linear"),
        ("priya", "c_priya_vercel_qbr","Vercel QBR prep",                "active",    "g_ent_motion", "vercel"),
        ("priya", "c_priya_replit_qbr","Replit QBR prep",                "active",    "g_ent_motion", "replit"),
        ("ben",   "c_ben_roadmap_doc", "Update roadmap rationale doc",   "at_risk",   None,           None),
        ("ben",   "c_ben_pricing",     "Pricing v2 RFC",                 "active",    None,           None),
        ("ben",   "c_ben_api_changelog","Maintain API v1 changelog",     "active",    "g_api_v1",     None),
        ("ben",   "c_ben_audit_logs",  "Audit-log feature scoping",      "active",    "g_ent_motion", None),
        ("ines",  "c_ines_growth",     "Growth funnel analysis Q2",      "active",    None,           None),
        ("ines",  "c_ines_dub_expand", "Expansion play for Dub.co",      "active",    None,           "dub"),
        ("ines",  "c_ines_modal",      "Modal expansion path",           "active",    None,           "modal"),
    ]
    for owner, key, title, state, gk, ck in pm_titles:
        out.append(_make_commitment(
            key, title, state, owner, actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, [],
            rng=rng,
        ))

    # GTM — Monica + AEs + CSMs
    gtm_titles = [
        ("monica", "c_monica_pipeline",   "Build Q3 enterprise pipeline", "active", None, None),
        ("monica", "c_monica_dp_close",   "Close 3 design partners on enterprise contracts", "active", "g_ent_motion", None),
        ("monica", "c_monica_pricing",    "Sign off on pricing v2",       "active", None, None),
        ("monica", "c_monica_qbr_prep",   "QBR prep with founders",       "active", None, None),
        ("monica", "c_monica_advisor",    "Engage Edie on enterprise motion", "active", "g_ent_motion", None),
        ("diego",  "c_diego_linear",      "Linear contract negotiation",  "active", "g_ent_motion", "linear"),
        ("diego",  "c_diego_vercel",      "Vercel contract negotiation",  "active", "g_ent_motion", "vercel"),
        ("diego",  "c_diego_replit",      "Replit contract negotiation",  "at_risk","g_ent_motion","replit"),
        ("diego",  "c_diego_modal",       "Modal expansion",              "active", None, "modal"),
        ("diego",  "c_diego_supabase",    "Supabase upsell",              "active", None, "supabase"),
        ("diego",  "c_diego_weekly",      "Weekly pipeline review",       "active", None, None),
        ("riley",  "c_riley_mid_market_q","Mid-market Q outreach",        "active", None, None),
        ("riley",  "c_riley_dub",         "Dub.co renewal",               "active", None, "dub"),
        ("riley",  "c_riley_clerk",       "Clerk renewal",                "active", None, "clerk"),
        ("riley",  "c_riley_planet",      "PlanetScale contract",         "active", None, "planetscale"),
        ("riley",  "c_riley_axiom",       "Axiom contract",               "active", None, "axiom"),
        ("riley",  "c_riley_outreach",    "Outbound 50 target accounts",  "active", None, None),
        ("morgan", "c_morgan_outreach",   "SDR outbound experiments",     "active", None, None),
        ("morgan", "c_morgan_followup",   "Conf followups",               "active", None, None),
        # CSMs
        ("avery",  "c_avery_dp_health",   "Track design-partner health",   "active", None, None),
        ("avery",  "c_avery_linear_check","Linear weekly check-in",        "active", None, "linear"),
        ("avery",  "c_avery_vercel_check","Vercel weekly check-in",        "active", None, "vercel"),
        ("avery",  "c_avery_replit_check","Replit weekly check-in",        "active", None, "replit"),
        ("avery",  "c_avery_modal_check", "Modal weekly check-in",         "active", None, "modal"),
        ("avery",  "c_avery_cursor_check","Cursor weekly check-in",        "active", None, "cursor"),
        ("avery",  "c_avery_supabase_check","Supabase QBR",               "active", None, "supabase"),
        ("imani",  "c_imani_onboarding", "Onboard 5 new mid-market accts","active", None, None),
        ("imani",  "c_imani_dub_onboard","Dub.co onboarding",             "active", None, "dub"),
        ("imani",  "c_imani_turso_onb",  "Turso onboarding",              "active", None, "turso"),
        ("imani",  "c_imani_clerk_onb",  "Clerk onboarding",              "active", None, "clerk"),
        ("imani",  "c_imani_axiom_onb",  "Axiom onboarding",              "active", None, "axiom"),
        ("kai",    "c_kai_tech_health",  "Track tech-account health",     "active", None, None),
        ("kai",    "c_kai_smallco",      "SmallCo recovery plan",         "at_risk",None, "smallco"),
        ("kai",    "c_kai_paperspace",   "Paperspace check-in",           "active", None, "paperspace"),
        ("kai",    "c_kai_baseten",      "BaseTen QBR",                   "active", None, "baseten"),
    ]
    for owner, key, title, state, gk, ck in gtm_titles:
        out.append(_make_commitment(
            key, title, state, owner, actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, [],
            rng=rng,
        ))

    # Marketing
    mkt_titles = [
        ("noor",  "c_noor_launch",      "v1 launch content",            "active", "g_api_v1", None),
        ("noor",  "c_noor_blog_q2",     "Q2 blog cadence",              "active", None, None),
        ("noor",  "c_noor_launch_event","v1 launch event planning",     "active", "g_api_v1", None),
        ("noor",  "c_noor_caselink",    "Customer case study — Modal",  "active", None, "modal"),
        ("noor",  "c_noor_caselinear",  "Customer case study — Linear", "blocked", None, "linear"),
        ("lina",  "c_lina_calendar",    "Marketing launch calendar",    "active", None, None),
        ("lina",  "c_lina_swag",        "Conf swag for Q3 events",      "active", None, None),
        ("blair", "c_blair_blog",       "Blog post: rate-limiter rewrite","active", None, None),
        ("blair", "c_blair_blog2",      "Blog post: SSO rationale",     "active", None, None),
    ]
    for owner, key, title, state, gk, ck in mkt_titles:
        out.append(_make_commitment(
            key, title, state, owner, actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, [],
            rng=rng,
        ))

    # Design
    design_titles = [
        ("grace", "c_grace_dashboard",  "Dashboard v2 visual design",   "active", "g_observ", None),
        ("grace", "c_grace_v1_dx",      "v1 DX writing pass",           "active", "g_api_v1", None),
        ("grace", "c_grace_signin",     "Sign-in flow for SSO",         "active", "g_sso", None),
        ("noah",  "c_noah_charts",      "Latency-chart components",     "active", "g_observ", None),
        ("noah",  "c_noah_brand",       "Brand refresh exploration",    "proposed",None, None),
        ("toby",  "c_toby_brand",       "v1 launch brand work",         "active", "g_api_v1", None),
    ]
    for owner, key, title, state, gk, ck in design_titles:
        out.append(_make_commitment(
            key, title, state, owner, actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, [],
            rng=rng,
        ))

    # Ops / Finance
    ops_titles = [
        ("simone", "c_simone_offsite",   "Q3 company offsite plan",     "active", None, None),
        ("simone", "c_simone_handbook",  "Update company handbook",     "active", None, None),
        ("simone", "c_simone_compliance","SOC2 readiness scoping",      "active", "g_ent_motion", None),
        ("david",  "c_david_runway",     "Monthly runway briefing",     "active", "g_runway", None),
        ("david",  "c_david_pricing_fin","Pricing v2 financial model",  "active", None, None),
        ("david",  "c_david_q3_close",   "Q3 close + investor update",  "active", "g_runway", None),
        ("ravi",   "c_ravi_burn",        "Weekly burn-vs-runway tracker","active", "g_runway", None),
        ("ravi",   "c_ravi_payroll",     "Payroll automation upgrade",  "active", None, None),
        ("hugh",   "c_hugh_eng_pipeline","Senior eng candidate pipeline","active","g_hire_eng", None),
        ("hugh",   "c_hugh_close_calls", "Schedule founder close calls","at_risk","g_hire_eng", None),
        ("hugh",   "c_hugh_diversity",   "Diversity sourcing program",   "active", "g_hire_eng", None),
        ("lex",    "c_lex_dpa",          "Update DPA template for SSO", "active", "g_sso", None),
        ("lex",    "c_lex_msa",          "Enterprise MSA template",     "active", "g_ent_motion", None),
        ("lex",    "c_lex_compliance",   "GDPR/CCPA review pass",       "active", None, None),
        ("edie",   "c_edie_pricing",     "Advise on pricing v2",        "active", None, None),
        ("edie",   "c_edie_close",       "Coach Monica on Linear close","active", "g_ent_motion", "linear"),
        ("kirby",  "c_kirby_capacity",   "Eng capacity planning advice","active", "g_hire_eng", None),
        ("ramon",  "c_ramon_perf",       "Perf optimization sprint",    "active", "g_observ", None),
    ]
    for owner, key, title, state, gk, ck in ops_titles:
        out.append(_make_commitment(
            key, title, state, owner, actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, [],
            rng=rng,
        ))

    # Founder commitments
    founder_titles = [
        ("maya",  "c_maya_design_partners","Personal touch on top 3 partners","active","g_ent_motion",None),
        ("maya",  "c_maya_hiring",         "Close VP-level conversations",   "at_risk","g_hire_eng",None),
        ("maya",  "c_maya_investor_brief", "Monthly investor email",         "active", None, None),
        ("maya",  "c_maya_founder_sync",   "1:1 with Tom (overdue)",         "at_risk", None, None),
        ("jules", "c_jules_pmf",           "Validate v1 product-market fit", "active", "g_api_v1", None),
        ("jules", "c_jules_brand",         "v1 launch brand narrative",      "active", "g_api_v1", None),
        ("jules", "c_jules_advisor_eng",   "Advisor engagement (Edie/Kirby)", "active", None, None),
    ]
    for owner, key, title, state, gk, ck in founder_titles:
        out.append(_make_commitment(
            key, title, state, owner, actor_keys, goal_by_key, cust_by_key,
            dec_by_key, gk, ck, [],
            rng=rng,
        ))

    # Add some depends_on edges (acyclic; on already-emitted commitments)
    by_key: dict[str, GeneratedCommitment] = {
        _strip_uuid_to_key(c.id): c for c in out
    }
    # We can't easily reverse-map the UUID without re-deriving keys.
    # Instead build a parallel dict during construction in a follow-up
    # pass: scan out[] in order and add edges from later commits to
    # earlier sibling commits where it makes domain sense.

    # Attach a few edges manually using the natural keys:
    edges = [
        ("c_sarah_sso_lead",       "c_priya_sso_pmf"),
        ("c_grace_signin",         "c_sarah_sso_lead"),
        ("c_lex_dpa",              "c_priya_sso_pmf"),
        ("c_diego_linear",         "c_sarah_linear_review"),
        ("c_diego_vercel",         "c_sarah_vercel_review"),
        ("c_diego_replit",         "c_sarah_replit_review"),
        ("c_monica_dp_close",      "c_priya_dp_sync"),
        ("c_monica_dp_close",      "c_diego_linear"),
        ("c_tom_apiv1_signoff",    "c_sarah_apiv1_breakdown"),
        ("c_jules_brand",          "c_tom_apiv1_signoff"),
    ]
    out_by_key: dict[str, GeneratedCommitment] = {}
    for c in out:
        # invert the deterministic id back to key by checking against
        # the id derivation — keep a pass-through mapping using kvalues
        pass
    # Build the inverse via the lookups we already have.
    key_to_id = {k: did(COMPANY, "commitment", k) for k, *_ in [
        # Reconstruct keys by re-walking the same titles list.
        *[(t[0],) for t in sarah_titles],
        *[(t[0],) for t in tom_titles],
    ]}
    # Fallback: derive id from key via did()
    for dep_key, dep_on_key in edges:
        # locate the commitment objects in `out` by their derived id
        dep_id = did(COMPANY, "commitment", dep_key)
        dep_on_id = did(COMPANY, "commitment", dep_on_key)
        for c in out:
            if c.id == dep_id and dep_on_id != dep_id:
                if dep_on_id not in c.depends_on:
                    c.depends_on.append(dep_on_id)
                break

    return out


def _make_commitment(
    key: str,
    title: str,
    state: str,
    owner_key: str,
    actor_keys: dict[str, str],
    goal_by_key: dict[str, str],
    cust_by_key: dict[str, str],
    dec_by_key: dict[str, str],
    goal_key: str | None,
    customer_key: str | None,
    decision_keys: list[str],
    *,
    contributors: list[str] | None = None,
    rng: random.Random | None = None,
) -> GeneratedCommitment:
    """Construct a GeneratedCommitment with deterministic UUIDs.

    Note: the schema's `state` literal is {proposed, active, at_risk,
    blocked, done, closed}. SQL-side state machine differs (it uses
    'doneunverified' / 'doneverified' / etc.) — sql_emit pass-throughs
    the schema state, and the demo flow is happy with these labels."""
    contributor_ids: list[str] = []
    if contributors:
        contributor_ids = [actor_keys[k] for k in contributors if k in actor_keys]
    return GeneratedCommitment(
        id=did(COMPANY, "commitment", key),
        title=title,
        owner_id=actor_keys[owner_key],
        contributors=contributor_ids,
        state=state,                          # type: ignore[arg-type]
        due_date=days_from_now(
            (rng.randint(7, 90) if rng else 30)
        ),
        contributes_to_goal_id=(
            goal_by_key[goal_key] if goal_key else None
        ),
        depends_on=[],
        constrained_by_decision_ids=[
            dec_by_key[k] for k in decision_keys if k in dec_by_key
        ],
        served_by_customer_id=(
            cust_by_key[customer_key] if customer_key else None
        ),
    )


def _eng_title(owner: str, idx: int, rng: random.Random) -> str:
    pool = {
        "marcus":   ["Infra cost dashboard", "Region failover playbook", "Ingress hardening", "Tracing rollout phase 2", "Capacity plan Q3"],
        "theo":     ["Request pipeline rewrite", "Latency budget enforcement", "Pipeline integration tests", "Async retry semantics", "Pipeline observability"],
        "rae":      ["TypeScript SDK 2.0", "Python SDK 2.0 scope", "Go SDK quickstart", "SDK CI matrix", "Errors taxonomy doc"],
        "samira":   ["Data plane tenant isolation", "Storage compaction", "Replication lag SLO", "Hot-shard rebalancer", "Rolling upgrade tooling"],
        "priyank":  ["API error model unification", "OpenAPI spec generator", "Quota errors UX", "Rate-limit headers spec", "Request-id propagation"],
        "hannah":   ["Latency SLOs per endpoint", "Error budget alerting", "Anomaly heuristics v0", "Logs pipeline rewrite", "Tracing exemplar wiring"],
        "dimitri":  ["Usage metering rewrite", "Invoice line-item details", "Payment retry policy", "Grandfathered pricing migration", "Stripe webhook reliability"],
        "yuki":     ["SDK v2 sample apps", "Quickstart docs revamp", "SDK release automation", "First-party CLI v0", "Auth examples"],
        "walt":     ["CLI auth flow", "CLI release pipeline", "CLI-to-API contract tests", "CLI internal-tooling commands", "CLI shell-completion"],
        "felix":    ["Billing dashboard", "Customer-facing usage charts", "Invoice export tool", "Stripe sync runbook", "Refunds workflow"],
        "oksana":   ["Migration tool spike", "Per-tenant config service", "Edge node bring-up", "Edge cache eviction tuning", "Region-A latency tracing"],
        "zion":     ["Linter for SDK styleguide", "Doc-site nav refactor", "Quickstart smoke tests", "SDK release notes generator", "Onboarding docs"],
    }
    options = pool.get(owner, ["Generic engineering work item"])
    return options[idx % len(options)]


def _strip_uuid_to_key(_uuid: str) -> str:
    """Placeholder — needed only for typing, never called."""
    return _uuid


# =====================================================================
# Signals — ~250 total. Mix of recent (last 6 weeks, dense) + older
# (sparse, 9 months back).
# =====================================================================


def build_signals(actors: list[GeneratedActor],
                  customers: list[GeneratedCustomer],
                  commitments: list[GeneratedCommitment],
                  goals: list[GeneratedGoal],
                  decisions: list[GeneratedDecision]) -> list[GeneratedSignal]:
    actor_ids = [a.id for a in actors]
    actor_by_role: dict[str, list[str]] = {}
    for a in actors:
        actor_by_role.setdefault(a.role, []).append(a.id)
    eng_ids = actor_by_role.get("engineer", [])
    sales_ids = actor_by_role.get("sales", []) + actor_by_role.get("head_sales", [])
    cs_ids = actor_by_role.get("customer_success", [])
    pm_ids = actor_by_role.get("pm", [])

    # Customer ID lookups
    cust = {c.id: c for c in customers}
    customer_keys = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    commitment_keys = {
        # Reconstruct the keys we used so signals can reference them.
        k: did(COMPANY, "commitment", k) for k in (
            "c_sarah_sso_lead", "c_sarah_apiv1_breakdown", "c_sarah_oncall",
            "c_sarah_linear_review", "c_priya_sso_pmf", "c_diego_linear",
            "c_diego_vercel", "c_diego_replit", "c_monica_dp_close",
            "c_avery_linear_check", "c_avery_vercel_check", "c_avery_replit_check",
            "c_tom_runbooks", "c_hugh_eng_pipeline", "c_maya_founder_sync",
        )
    }
    decision_ids = {k: did(COMPANY, "decision", k) for k, *_ in DECISION_SPECS}
    goal_ids = {k: did(COMPANY, "goal", k) for k, *_ in GOAL_SPECS}

    rng = random.Random(7)
    out: list[GeneratedSignal] = []

    def _add(idx: int, channel: str, source_ref: str, author_id: str,
             ago_days: float, text: str,
             mentions: list[tuple[str, str]] | None = None) -> None:
        ent = [EntityMention(type=t, id=i) for t, i in (mentions or [])]
        out.append(GeneratedSignal(
            id=did(COMPANY, "signal", f"sig_{idx:04d}"),
            source_channel=channel,
            source_ref=source_ref,
            author_id=author_id,
            occurred_at=days_ago(ago_days),
            content_text=text,
            entities_mentioned=ent,
        ))

    idx = 0

    # --- The headline customer-pressure signals (3 SSO asks within 60 days)
    sso_ask_signals = [
        (5,  "slack:message", "C0123-1736208000", "diego",
         "Linear just asked us about SSO timeline on the call. They said it's "
         "a hard requirement before they expand seats.",
         [("customer", customer_keys["linear"]), ("commitment", commitment_keys["c_priya_sso_pmf"])]),
        (22, "slack:message", "C0123-1735603200", "diego",
         "Vercel platform team wants SSO and SAML in writing — they're "
         "contracting this quarter.",
         [("customer", customer_keys["vercel"]), ("commitment", commitment_keys["c_priya_sso_pmf"])]),
        (47, "slack:message", "C0123-1733443200", "diego",
         "Replit followed up on SSO again. That's the third design partner "
         "this quarter — should we accelerate?",
         [("customer", customer_keys["replit"]), ("commitment", commitment_keys["c_priya_sso_pmf"])]),
        (3,  "email:message", "msg-vercel-001", "avery",
         "Vercel CSM just emailed: their security team wants the SSO "
         "audit-log spec written down before signature.",
         [("customer", customer_keys["vercel"])]),
        (12, "calendar:event", "evt-linear-001", "diego",
         "Linear ↔ Truss SSO requirements meeting (60 min).",
         [("customer", customer_keys["linear"])]),
    ]
    for ago, ch, ref, author_key, text, mentions in sso_ask_signals:
        _add(idx, ch, ref, did(COMPANY, "actor", author_key), ago, text, mentions); idx += 1

    # --- The capacity-pressure signals (engineering at 95% utilization)
    capacity_signals = [
        (2,  "slack:message", "C-eng-1736380800", "marcus",
         "I'm at the line. If anything new lands on my plate this week, something else slips.",
         [("commitment", commitment_keys["c_sarah_sso_lead"])]),
        (4,  "slack:message", "C-eng-1736208000", "theo",
         "Pipeline rewrite is running 2 weeks behind because everyone is in incident triage.",
         []),
        (8,  "slack:message", "C-eng-1735862400", "tom",
         "Eng utilization hit 94% this week. Recommending we pause new "
         "commitments until SSO ships.",
         [("goal", goal_ids["g_sso"])]),
        (14, "slack:message", "C-eng-1735257600", "sarah",
         "I'm exhausted. 4 weeks on the pager out of the last 6.",
         [("actor", did(COMPANY, "actor", "sarah"))]),
        (1,  "calendar:event", "evt-oncall-001", "sarah",
         "On-call shift (Sarah, 7d primary).",
         [("commitment", commitment_keys["c_sarah_oncall"])]),
    ]
    for ago, ch, ref, author_key, text, mentions in capacity_signals:
        _add(idx, ch, ref, did(COMPANY, "actor", author_key), ago, text, mentions); idx += 1

    # --- The personnel signal (Sarah on incident rotation 4 of 6 weeks)
    incident_signals = [
        (35, "slack:message", "C-pager-001", "tom",
         "Sarah carried the pager again this week. That's 4 of 6.",
         [("actor", did(COMPANY, "actor", "sarah"))]),
        (28, "slack:message", "C-pager-002", "tom",
         "Need to redistribute on-call. Sarah is on the rotation again.",
         [("commitment", commitment_keys["c_sarah_oncall"])]),
        (18, "slack:message", "C-pager-003", "tom",
         "Sarah back on primary. We need a 4th rotation soon.",
         [("actor", did(COMPANY, "actor", "sarah"))]),
        (10, "slack:message", "C-pager-004", "marcus",
         "If Sarah goes out, we lose two critical paths simultaneously. Worth flagging.",
         []),
    ]
    for ago, ch, ref, author_key, text, mentions in incident_signals:
        _add(idx, ch, ref, did(COMPANY, "actor", author_key), ago, text, mentions); idx += 1

    # --- The decision-revisit signal (API redesign vs customers asking for stable v1)
    apiv1_signals = [
        (6,  "github:event", "pr-419", "sarah",
         "PR #419: 'Breaking change in v1 error envelopes' — discussion turned heated.",
         [("commitment", commitment_keys["c_sarah_apiv1_breakdown"])]),
        (15, "slack:message", "C-prod-001", "ben",
         "Three customers (Linear, Vercel, Replit) all asked: 'when can we lock the API?' "
         "We made the redesign call before any of them asked.",
         [("decision", decision_ids["d_api_redesign"]),
          ("commitment", commitment_keys["c_sarah_apiv1_breakdown"])]),
        (20, "email:message", "msg-replit-002", "diego",
         "Replit said the API redesign feels too late. They were ready to lock contract last quarter.",
         [("customer", customer_keys["replit"]),
          ("decision", decision_ids["d_api_redesign"])]),
    ]
    for ago, ch, ref, author_key, text, mentions in apiv1_signals:
        _add(idx, ch, ref, did(COMPANY, "actor", author_key), ago, text, mentions); idx += 1

    # --- The strategic signal (8 workstreams, 3 lack demand)
    strategic_signals = [
        (4,  "slack:message", "C-strategy-001", "ben",
         "Reviewed the roadmap doc — we have 8 active workstreams. Only 5 have customer signal "
         "behind them. Re-scoping?",
         []),
        (10, "slack:message", "C-strategy-002", "jules",
         "I keep saying we should kill the self-host scoping. No customer is blocked on it.",
         [("decision", decision_ids["d_no_self_host"])]),
    ]
    for ago, ch, ref, author_key, text, mentions in strategic_signals:
        _add(idx, ch, ref, did(COMPANY, "actor", author_key), ago, text, mentions); idx += 1

    # --- The founder-context signal (3 weeks since founder/VP eng sync)
    founder_signals = [
        (22, "calendar:event", "evt-1on1-cancel", "tom",
         "1:1 with Maya cancelled (declined by Maya) — third time in a row.",
         [("actor", did(COMPANY, "actor", "tom")),
          ("actor", did(COMPANY, "actor", "maya"))]),
        (16, "calendar:event", "evt-1on1-cancel-2", "tom",
         "1:1 with Maya — no-show.",
         []),
        (3,  "slack:message", "C-founder-001", "hugh",
         "Maya, the senior eng candidate is asking if we can do a 30-min close call "
         "this week. Already pushed twice.",
         [("commitment", commitment_keys["c_hugh_eng_pipeline"])]),
        (8,  "slack:message", "C-founder-002", "hugh",
         "Two open senior eng roles. Both candidates asked specifically to meet Maya.",
         [("goal", goal_ids["g_hire_eng"])]),
    ]
    for ago, ch, ref, author_key, text, mentions in founder_signals:
        _add(idx, ch, ref, did(COMPANY, "actor", author_key), ago, text, mentions); idx += 1

    # --- The smaller-account drift signal
    drift_signals = [
        (29, "stripe:event", "ch_smallco_001", "kai",
         "SmallCo invoice failed. 4th time in 3 months.",
         [("customer", customer_keys["smallco"])]),
        (12, "slack:message", "C-cs-001", "kai",
         "SmallCo CSM is non-responsive. They've stopped showing up to QBRs.",
         [("customer", customer_keys["smallco"])]),
    ]
    for ago, ch, ref, author_key, text, mentions in drift_signals:
        _add(idx, ch, ref, did(COMPANY, "actor", author_key), ago, text, mentions); idx += 1

    # --- Dense recent activity (last 6 weeks): mix of slack/github/email
    # ~150 more signals, programmatically templated
    recent_pool = [
        ("slack:message", lambda: rng.choice(eng_ids), [
            "Pushed perf fix for v1 surface — saved 18ms p95 on /complete",
            "Reviewed PR #421 — looks good but 4 nits on the rate-limit semantics",
            "On-call quiet last 24h. Knock on wood.",
            "Filed bug: CLI auth flow breaks on Windows for SSO sessions",
            "v1 SDK release branch cut. CI green.",
            "Quick poll: should /v1/quotas return 429 or 503 when over budget?",
            "Tracing exemplars are working in staging. Looks good.",
            "Region B is back online; latencies normal.",
            "Looked at the latency outlier dashboard — top 10 are all the same customer's webhooks.",
            "Re-running the perf bench overnight. Will share results tomorrow.",
        ]),
        ("github:event", lambda: rng.choice(eng_ids), [
            "PR opened: 'add audit-log endpoint scaffolding'",
            "PR merged: 'fix race in usage-metering rollup'",
            "Issue opened: 'flaky test in pipeline integration suite'",
            "PR opened: 'WIP: SSO middleware skeleton'",
            "PR review requested: 'rewrite rate-limiter using token-bucket'",
            "Issue closed: 'CLI auth flow on Windows'",
            "PR opened: 'rate-limit headers spec — response surface'",
            "PR merged: 'add request-id propagation through SDK'",
            "Issue opened: 'tracing exemplar collisions in staging'",
            "PR opened: 'SOC2 evidence-collection scaffolding'",
        ]),
        ("slack:message", lambda: rng.choice(sales_ids), [
            "Linear renewal call went well. Will send recap.",
            "Modal expansion conversation — they want $10K/mo more starting next quarter.",
            "Cursor asking about volume discounts. Sent over a draft.",
            "Demo with prospect — Notion eng VP. They liked the audit-log roadmap.",
            "Lost the Crystallize bake-off. They went with cheaper alt.",
            "PlanetScale is renewing; up 15%.",
            "Inbound from Anthropic infra team — interested in upgrade tier.",
            "Diego's Linear close moved to next Tuesday — they pulled in legal.",
            "PostHog prospect went cold for 2 weeks. Re-engaging.",
            "Prospect asked about self-hosting. Said no per the decision.",
        ]),
        ("slack:message", lambda: rng.choice(cs_ids), [
            "Linear weekly check-in: smooth, no blockers.",
            "Vercel had a weird 502 spike on Wednesday. Resolved by us.",
            "Replit asked about audit logs again — that's twice this month.",
            "Cursor's product team wants a co-marketing spot.",
            "Modal usage is up 22% MoM. Healthy.",
            "Convex is asking about quota negotiations — escalated to Monica.",
            "Dub.co churned out of the quarter — small, but they're gone.",
            "Onboarded Turso. They had a good Q1 ramp.",
            "Onboarded Clerk. Watching their first 30 days.",
            "Quarterly health-score refresh: 3 in watching, 1 at_risk.",
        ]),
        ("slack:message", lambda: rng.choice(pm_ids), [
            "Drafted the v1 RFC. Looking for eng review.",
            "SSO requirements doc v3 is ready. Will send to design partners.",
            "Roadmap rationale doc is out of date. Updating Friday.",
            "Pricing v2 RFC is half-written. Talking to Edie tomorrow.",
            "Audit logs scope is creeping. Need to push back.",
            "Customer interviews next week — 4 design partners on the calendar.",
        ]),
        ("calendar:event", lambda: rng.choice(actor_ids), [
            "Design partner sync (weekly)",
            "All-hands",
            "Eng leads sync",
            "GTM forecast review",
            "Investor update prep",
            "Pricing v2 working session",
            "v1 launch dry-run",
            "QBR — Linear",
            "QBR — Vercel",
            "Recruiter sync",
        ]),
        ("github:event", lambda: rng.choice(actor_ids), [
            "Issue triage: 12 open, 4 needs-decision",
            "Release v0.91 cut",
            "Hotfix branch merged for billing edge case",
        ]),
        ("stripe:event", lambda: rng.choice([did(COMPANY, "actor", "ravi"),
                                              did(COMPANY, "actor", "david")]), [
            "Invoice paid: enterprise tier ($28K)",
            "Subscription updated: Cursor (added seats)",
            "Subscription canceled: Drift account (small)",
            "Payment failed: SmallCo (retry scheduled)",
        ]),
        ("email:message", lambda: rng.choice(actor_ids), [
            "Re: Q3 contract — need redlines by Friday",
            "Investor update — Q1 numbers attached",
            "Re: SSO timeline — escalation",
            "Coffee chat — design partner",
            "Re: pricing question",
            "Inbound: enterprise prospect interested in audit logs",
        ]),
    ]

    while idx < 200:
        channel, author_fn, options = rng.choice(recent_pool)
        ago = rng.uniform(0.1, 42.0)
        author = author_fn()
        text = rng.choice(options)
        # 30% chance of a customer mention
        mentions: list[tuple[str, str]] = []
        if rng.random() < 0.3:
            ck = rng.choice(list(customer_keys.keys())[:18])
            mentions.append(("customer", customer_keys[ck]))
        _add(idx, channel, f"auto-{idx:04d}", author, ago, text, mentions); idx += 1

    # --- Older sparse signals (between 60-270 days ago) — ~50
    while idx < 250:
        channel = rng.choice(["slack:message", "github:event", "calendar:event"])
        author = rng.choice(actor_ids)
        ago = rng.uniform(60, 270)
        text = rng.choice([
            "Onboarded a new customer last quarter.",
            "Old roadmap update from Q4. Most still applies.",
            "Historical anomaly in latencies — investigated and resolved.",
            "Q3 board meeting prep notes.",
            "v0 SDK migration discussion.",
            "Earlier conversation about runtime language choice.",
            "Fundraising update: Series A momentum building.",
        ])
        _add(idx, channel, f"hist-{idx:04d}", author, ago, text); idx += 1

    return out


# =====================================================================
# Recommendations — 7, mapped to the spec's headline list
# =====================================================================


_M_NS = COMPANY  # uuid namespace key for models


def _M(key: str) -> str:
    return did(_M_NS, "model", key)


def build_models(actors, customers, commitments, goals, decisions, signals):
    """Author the rich epistemic substrate. Returns ~50 Models across
    every PropositionKind. Each Model includes a falsifier when
    confidence > 0.7 (per the schema check) and at least 1-2
    supporting_observation_ids drawn from `signals` whenever possible.
    """
    cust = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    actor = {k: did(COMPANY, "actor", k) for k, *_ in ACTOR_SPECS}
    com = {k: did(COMPANY, "commitment", k) for k in (
        "c_sarah_sso_lead", "c_priya_sso_pmf", "c_sarah_oncall",
        "c_sarah_apiv1_breakdown", "c_hugh_close_calls", "c_kai_smallco",
    )}
    g = {k: did(COMPANY, "goal", k) for k, *_ in GOAL_SPECS}
    d = {k: did(COMPANY, "decision", k) for k, *_ in DECISION_SPECS}

    F = lambda *p: find_signals_containing(signals, *p, limit=4)

    out: list[GeneratedModel] = []
    def _add(key, kind, natural, *, confidence=0.65,
             scope_actors=None, scope_entities=None,
             falsifier=None, support_signals=None,
             support_models=None, evaluate_at=None,
             proposition_extra=None):
        if confidence > 0.7 and falsifier is None:
            falsifier = {
                "condition": "supporting_signal_density_drops_below_threshold",
                "threshold": "0_supporting_signals_in_30_days",
                "observable_via": "signal_query",
            }
        out.append(GeneratedModel(
            id=_M(key),
            kind=kind,                                     # type: ignore[arg-type]
            natural=natural,
            proposition=proposition_extra or {},
            confidence=confidence,
            scope_actor_ids=list(scope_actors or []),
            scope_entities=list(scope_entities or []),
            falsifier=falsifier,
            supporting_observation_ids=list(support_signals or []),
            supporting_model_ids=list(support_models or []),
            evaluate_at=evaluate_at,
        ))

    # =========== state =========== (15)
    _add("st_sarah_burnout", "state",
         "Sarah Chen is at elevated burnout risk — pager 4 of 6 weeks; capacity flag raised.",
         confidence=0.82,
         scope_actors=[actor["sarah"]],
         scope_entities=[{"type": "actor", "id": actor["sarah"]},
                         {"type": "commitment", "id": com["c_sarah_oncall"]}],
         falsifier={"condition": "sarah on-call shifts ≤ 1 of next 4 weeks AND no slip events",
                    "threshold": "30_days", "observable_via": "calendar+slack"},
         support_signals=F("Sarah carried", "4 weeks on the pager", "Sarah back on primary"))
    _add("st_eng_capacity_critical", "state",
         "Engineering operating at ~95% utilization — saturation imminent.",
         confidence=0.84,
         scope_entities=[{"type": "goal", "id": g["g_api_v1"]},
                         {"type": "goal", "id": g["g_sso"]}],
         falsifier={"condition": "engineering utilization < 80% for 2 consecutive weeks",
                    "threshold": "80%", "observable_via": "capacity_audit"},
         support_signals=F("utilization hit 94%", "at the line", "running 2 weeks behind"))
    _add("st_industrium_unblocked", "state",
         "Linear, Vercel, Replit are mid-procurement on enterprise contracts — SSO is the gating item.",
         confidence=0.78,
         scope_entities=[{"type": "customer", "id": cust["linear"]},
                         {"type": "customer", "id": cust["vercel"]},
                         {"type": "customer", "id": cust["replit"]}],
         falsifier={"condition": "any of the 3 publicly drops procurement OR signs without SSO",
                    "observable_via": "sales_pipeline"},
         support_signals=F("Linear just asked us about SSO", "Vercel platform team", "Replit followed up"))
    _add("st_acme_renewal_iffy", "state",
         "SmallCo Studios drifting on health — 4 invoice failures, no QBR attendance.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["smallco"]}],
         falsifier={"condition": "SmallCo attends Q2 QBR AND invoices succeed for 60 days",
                    "observable_via": "stripe+calendar"},
         support_signals=F("SmallCo invoice failed", "SmallCo CSM is non-responsive"))
    _add("st_founder_overload", "state",
         "Founder cognitive load exceeds sustainable threshold — multiple active workstreams without delegation pattern.",
         confidence=0.71,
         scope_actors=[actor["maya"]],
         falsifier={"condition": "Maya completes 2 weeks of clean delegation across all 8 workstreams",
                    "observable_via": "calendar+slack"},
         support_signals=F("haven't synced with Tom", "8 active workstreams"))
    _add("st_apiv1_late", "state",
         "API v1 contract is on the critical path; current scope gives 70% odds of slip.",
         confidence=0.69,
         scope_entities=[{"type": "goal", "id": g["g_api_v1"]},
                         {"type": "decision", "id": d["d_api_redesign"]}],
         support_signals=F("v1 API surface", "API redesign", "lock the API"))
    _add("st_3_design_partners_ready", "state",
         "3 of 5 design partners are ready to convert to enterprise contracts pending SSO + pricing v2.",
         confidence=0.66,
         scope_entities=[{"type": "customer", "id": cust["linear"]},
                         {"type": "customer", "id": cust["vercel"]},
                         {"type": "customer", "id": cust["replit"]}])
    _add("st_pricing_v2_ready_for_review", "state",
         "Pricing v2 RFC is half-written; needs CFO review and design-partner feedback.",
         confidence=0.65,
         scope_entities=[{"type": "decision", "id": d["d_design_partner_close"]}])
    _add("st_runway_18m", "state",
         "Runway is 18 months at current burn (~$650K/mo). Within plan.",
         confidence=0.81,
         scope_entities=[{"type": "goal", "id": g["g_runway"]}],
         falsifier={"condition": "monthly burn exceeds $750K for 2 consecutive months",
                    "observable_via": "finance"})
    _add("st_no_self_host_inflexible", "state",
         "No-self-host decision is blocking 2 prospects today; will block 4 by Q3.",
         confidence=0.62,
         scope_entities=[{"type": "decision", "id": d["d_no_self_host"]}])
    _add("st_2_open_eng_roles", "state",
         "2 senior engineering roles are open and gating critical-path work.",
         confidence=0.86,
         scope_entities=[{"type": "goal", "id": g["g_hire_eng"]}],
         falsifier={"condition": "both roles closed in next 60 days",
                    "observable_via": "ats"},
         support_signals=F("two open roles", "senior eng candidate"))
    _add("st_observability_gap", "state",
         "Production observability is patchy — no per-tenant SLOs, no error-budget enforcement.",
         confidence=0.78,
         scope_entities=[{"type": "goal", "id": g["g_observ"]}],
         falsifier={"condition": "per-tenant SLO dashboards live for >50% of customers",
                    "observable_via": "tracing_stack"})
    _add("st_pager_imbalance", "state",
         "On-call rotation is 3-deep but Sarah covers a disproportionate share.",
         confidence=0.83,
         scope_actors=[actor["sarah"], actor["theo"], actor["marcus"]],
         falsifier={"condition": "Sarah's on-call share ≤ 33% over rolling 8 weeks",
                    "observable_via": "calendar"})
    _add("st_dp_close_window_open", "state",
         "Window for converting design partners to enterprise contracts is open through Q2; closes after pricing v2.",
         confidence=0.64,
         scope_entities=[{"type": "decision", "id": d["d_design_partner_close"]}])
    _add("st_brand_modern_legible", "state",
         "External brand reads as 'modern dev infra' but technical depth not yet legible to non-eng buyers.",
         confidence=0.55)

    # =========== relation =========== (8)
    _add("rel_velocity_oncall", "relation",
         "Sarah's per-week shipped-velocity inversely correlates with on-call load (r ≈ -0.7 over 12 weeks).",
         confidence=0.74,
         scope_actors=[actor["sarah"]],
         falsifier={"condition": "12-week regression coefficient flips sign or goes to zero",
                    "observable_via": "github+calendar"},
         support_signals=F("Sarah carried", "running 2 weeks behind"),
         proposition_extra={"correlation": -0.7, "n_weeks": 12})
    _add("rel_sso_to_arr", "relation",
         "SSO ask frequency from a design partner correlates with conversion to enterprise (3+ asks → 80% convert).",
         confidence=0.68,
         scope_entities=[{"type": "customer", "id": cust["linear"]},
                         {"type": "customer", "id": cust["vercel"]},
                         {"type": "customer", "id": cust["replit"]}],
         proposition_extra={"asks_to_convert_rate_3plus": 0.8})
    _add("rel_apiv1_decision_customers", "relation",
         "API redesign decision is increasingly inconsistent with stable-v1 customer demand.",
         confidence=0.71,
         scope_entities=[{"type": "decision", "id": d["d_api_redesign"]},
                         {"type": "customer", "id": cust["linear"]},
                         {"type": "customer", "id": cust["vercel"]},
                         {"type": "customer", "id": cust["replit"]}],
         falsifier={"condition": "fewer than 1 customer raises stability concern in 30 days",
                    "observable_via": "signals"},
         support_signals=F("API redesign", "lock the API"))
    _add("rel_founder_to_eng_close", "relation",
         "Founder close-call attendance correlates with senior eng candidate acceptance.",
         confidence=0.62,
         scope_actors=[actor["maya"]])
    _add("rel_pricing_to_dp_close", "relation",
         "Pricing v2 finalization is upstream of design-partner contract close.",
         confidence=0.66,
         scope_entities=[{"type": "decision", "id": d["d_design_partner_close"]}])
    _add("rel_obs_gap_to_enterprise", "relation",
         "Observability gap is upstream of enterprise contract risk (auditability is a contract gating item).",
         confidence=0.69,
         scope_entities=[{"type": "goal", "id": g["g_observ"]},
                         {"type": "goal", "id": g["g_ent_motion"]}])
    _add("rel_hiring_to_critical_path", "relation",
         "Senior eng hires unblock 4 critical-path commitments within 30 days of close.",
         confidence=0.61,
         scope_entities=[{"type": "goal", "id": g["g_hire_eng"]}])
    _add("rel_workstream_to_demand", "relation",
         "5 of 8 active workstreams have customer-signal density ≥ 1/wk; 3 have <0.2/wk.",
         confidence=0.72,
         falsifier={"condition": "all 8 workstreams hit ≥1/wk for 4 consecutive weeks",
                    "observable_via": "signals"},
         support_signals=F("8 active workstreams"))

    # =========== prediction =========== (6)
    _add("pred_sso_q2", "prediction",
         "SSO will ship by end of Q2 if Sarah is removed from on-call rotation; otherwise slips to Q3.",
         confidence=0.62, evaluate_at=days_from_now(60),
         scope_entities=[{"type": "commitment", "id": com["c_sarah_sso_lead"]},
                         {"type": "goal", "id": g["g_sso"]}])
    _add("pred_3_design_partners_close", "prediction",
         "≥2 of Linear/Vercel/Replit will sign enterprise contracts by Q3 if SSO ships in Q2.",
         confidence=0.57, evaluate_at=days_from_now(90),
         scope_entities=[{"type": "customer", "id": cust["linear"]},
                         {"type": "customer", "id": cust["vercel"]},
                         {"type": "customer", "id": cust["replit"]}])
    _add("pred_sarah_burnout_event", "prediction",
         "Sarah will hit a burnout event (extended sick leave or attrition risk) in next 60 days if pager share doesn't drop.",
         confidence=0.41, evaluate_at=days_from_now(60),
         scope_actors=[actor["sarah"]])
    _add("pred_smallco_churn", "prediction",
         "SmallCo Studios will churn within 90 days unless touched by exec.",
         confidence=0.54, evaluate_at=days_from_now(90),
         scope_entities=[{"type": "customer", "id": cust["smallco"]}])
    _add("pred_arr_q4", "prediction",
         "Truss will hit $6M ARR by end of Q4 if 2 of 3 design partners convert.",
         confidence=0.48, evaluate_at=days_from_now(180))
    _add("pred_apiv1_slip", "prediction",
         "API v1 will slip 4-6 weeks past announced date if scope is not re-baselined.",
         confidence=0.66, evaluate_at=days_from_now(75),
         scope_entities=[{"type": "goal", "id": g["g_api_v1"]}],
         falsifier={"condition": "API v1 GA happens within 2 weeks of announced date",
                    "observable_via": "release_log"})

    # =========== pattern =========== (3) + pattern_instance (3)
    _add("pat_dp_sso_ask", "pattern",
         "Design partners requesting SSO 3+ times within 60 days correlates with imminent contract conversation.",
         confidence=0.68,
         proposition_extra={"window_days": 60, "min_asks": 3})
    _add("pat_eng_velocity_decay", "pattern",
         "Engineers carrying 4+ active commitments simultaneously show velocity decay starting week 3.",
         confidence=0.61,
         proposition_extra={"threshold_commitments": 4, "decay_week": 3})
    _add("pat_decision_rationale_decay", "pattern",
         "Decisions older than 12 weeks without revisit-trigger checks show 60% rationale-decay (founder can't recall why).",
         confidence=0.58,
         proposition_extra={"window_weeks": 12, "decay_rate": 0.6})

    _add("pat_inst_linear_sso", "pattern_instance",
         "Linear instance of the SSO-ask pattern: 4 asks in 47 days.",
         confidence=0.79,
         scope_entities=[{"type": "customer", "id": cust["linear"]}],
         falsifier={"condition": "Linear's SSO-ask cadence drops to 0 in 30 days",
                    "observable_via": "signals"},
         support_models=[_M("pat_dp_sso_ask")],
         support_signals=F("Linear just asked us"))
    _add("pat_inst_vercel_sso", "pattern_instance",
         "Vercel instance of the SSO-ask pattern: 3 asks in 38 days.",
         confidence=0.76,
         scope_entities=[{"type": "customer", "id": cust["vercel"]}],
         falsifier={"condition": "Vercel's SSO-ask cadence drops to 0 in 30 days",
                    "observable_via": "signals"},
         support_models=[_M("pat_dp_sso_ask")],
         support_signals=F("Vercel platform team"))
    _add("pat_inst_sarah_velocity", "pattern_instance",
         "Sarah instance of the velocity-decay pattern: she's now carrying 7 commitments.",
         confidence=0.81,
         scope_actors=[actor["sarah"]],
         falsifier={"condition": "Sarah's commitment count drops to <4 within 14 days",
                    "observable_via": "commitments"},
         support_models=[_M("pat_eng_velocity_decay")])

    # =========== capability_assessment =========== (4)
    _add("cap_engineering_strong", "capability_assessment",
         "Engineering capability is high relative to peer 40-person AI-native infra companies.",
         confidence=0.66,
         proposition_extra={"benchmark_peer_set": "ai_native_infra_40p"})
    _add("cap_sales_thin", "capability_assessment",
         "Sales capability is thin — Monica + 2 AEs cover 35 customers; needs Edie's enterprise-motion lift.",
         confidence=0.61,
         scope_actors=[actor["monica"], actor["diego"], actor["riley"]])
    _add("cap_brand_underdeveloped", "capability_assessment",
         "Brand capability lags engineering — content cadence is weekly but messaging is technical-only.",
         confidence=0.58,
         scope_actors=[actor["noor"]])
    _add("cap_founder_decision_velocity", "capability_assessment",
         "Founder's decision velocity is high but at the cost of rationale-capture for future revisits.",
         confidence=0.59,
         scope_actors=[actor["maya"]])

    # =========== hypothesis =========== (4)
    _add("hyp_sso_underestimated", "hypothesis",
         "SSO complexity may have been underestimated by 2-3 weeks; ProQA review needed.",
         confidence=0.49,
         scope_entities=[{"type": "commitment", "id": com["c_sarah_sso_lead"]}])
    _add("hyp_apiv1_redesign_was_premature", "hypothesis",
         "API redesign was made before sufficient customer-stability signal — may need to revert scope.",
         confidence=0.61,
         scope_entities=[{"type": "decision", "id": d["d_api_redesign"]}])
    _add("hyp_founder_solo_too_long", "hypothesis",
         "Founder has been operating without a real chief-of-staff for too long — Simone is doing the work but not titled.",
         confidence=0.46,
         scope_actors=[actor["maya"], actor["simone"]])
    _add("hyp_workstream_pruning_underestimates_morale", "hypothesis",
         "Killing 3 workstreams may impact eng morale more than projected — owners may interpret as lost trust.",
         confidence=0.42)

    # =========== concern =========== (4)
    _add("conc_sarah_attrition", "concern",
         "Risk of Sarah leaving the company — 4-of-6 pager, no relief, customer pressure.",
         confidence=0.51,
         scope_actors=[actor["sarah"]])
    _add("conc_design_partner_loss", "concern",
         "Risk of losing a design partner if SSO slips past Q2 (Linear most likely).",
         confidence=0.47,
         scope_entities=[{"type": "customer", "id": cust["linear"]}])
    _add("conc_apiv1_breaking_changes", "concern",
         "Risk that API v1 breaking changes alienate the design-partner cohort during procurement.",
         confidence=0.53,
         scope_entities=[{"type": "decision", "id": d["d_api_redesign"]}])
    _add("conc_runway_burn", "concern",
         "Risk that pricing-v2 migration disrupts revenue flow during runway-critical quarter.",
         confidence=0.36,
         scope_entities=[{"type": "goal", "id": g["g_runway"]}])

    # =========== market_assessment =========== (3)
    _add("mkt_devtools_consolidating", "market_assessment",
         "AI-native dev infrastructure space is consolidating around 5-7 players; Truss is in the second tier.",
         confidence=0.62,
         proposition_extra={"tier": 2, "consolidation_horizon_months": 18})
    _add("mkt_sso_table_stakes", "market_assessment",
         "SSO + audit logs are now table-stakes for enterprise dev-tools buyers; differentiation is elsewhere.",
         confidence=0.78,
         falsifier={"condition": "SSO is mentioned as a *differentiator* in <20% of buyer interviews",
                    "observable_via": "buyer_research"})
    _add("mkt_pricing_usage_norm", "market_assessment",
         "Usage-based pricing is becoming the dominant model in dev infra.",
         confidence=0.71,
         falsifier={"condition": "≥40% of peer set ships per-seat-only pricing in next 12 months",
                    "observable_via": "competitor_analysis"})

    # =========== environmental_trend =========== (3)
    _add("env_ai_native_growth", "environmental_trend",
         "AI-native developer tooling category growing 80% YoY; window will tighten in 2027.",
         confidence=0.66,
         proposition_extra={"yoy_growth": 0.8})
    _add("env_compliance_uplift", "environmental_trend",
         "Enterprise compliance requirements (SOC2, ISO 27001, FedRAMP for some) are normalizing earlier in the buyer journey.",
         confidence=0.69)
    _add("env_eng_hiring_hot", "environmental_trend",
         "Senior infra engineering hiring market is hot; offers above $300K base are normalizing.",
         confidence=0.74,
         falsifier={"condition": "median senior infra-eng base ≤ $250K for 2 consecutive quarters",
                    "observable_via": "comp_data"})

    # =====================================================================
    # Expansion set — doubles the substrate across every kind so the
    # demo retrieval has more granular surfaces to land on. Authored
    # against the same actors/customers/goals/decisions to keep the
    # epistemic graph dense.
    # =====================================================================

    # ---- state (extra 12) ----
    _add("st_modal_warming", "state",
         "Modal Labs is warming on a paid contract — 3 platform-team meetings in 6 weeks; usage doubled month-over-month.",
         confidence=0.72,
         scope_entities=[{"type": "customer", "id": cust["modal"]}],
         falsifier={"condition": "Modal usage flat or declining for 30 days",
                    "observable_via": "stripe+usage"},
         support_signals=F("Modal", "platform team"))
    _add("st_cursor_quiet_growing", "state",
         "Cursor Labs is quietly heavy-using us — 4x our typical request volume per seat; no procurement pings yet.",
         confidence=0.66,
         scope_entities=[{"type": "customer", "id": cust["cursor"]}])
    _add("st_supabase_partnering", "state",
         "Supabase is treating us as a co-marketing partner — they shipped a tutorial that drove 600 sign-ups.",
         confidence=0.69,
         scope_entities=[{"type": "customer", "id": cust["supabase"]}])
    _add("st_anthropic_strict_audit", "state",
         "Anthropic is asking for SOC2 Type II + per-tenant audit logs as renewal gating.",
         confidence=0.81,
         scope_entities=[{"type": "customer", "id": cust["anthropic"]}],
         falsifier={"condition": "Anthropic renewal closes without SOC2 II",
                    "observable_via": "contract"})
    _add("st_scaleai_quiet", "state",
         "Scale AI has gone quiet for 3 weeks after consistent monthly check-ins — possible churn signal.",
         confidence=0.58,
         scope_entities=[{"type": "customer", "id": cust["scale"]}])
    _add("st_fly_paying_above_plan", "state",
         "Fly.io is paying ~30% above their plan tier on overage — pricing v2 will land them either flat or up.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["fly"]}],
         falsifier={"condition": "Fly drops below plan after pricing v2 ships",
                    "observable_via": "billing"})
    _add("st_smb_overheads", "state",
         "SMB-tier customers consume ~22% of support time for ~8% of ARR — overhead-to-revenue is unfavourable.",
         confidence=0.70,
         scope_actors=[actor["avery"], actor["imani"], actor["kai"]])
    _add("st_2_workstreams_dormant", "state",
         "2 of 8 active workstreams have not produced a customer-visible artifact in 60 days.",
         confidence=0.77,
         falsifier={"condition": "both workstreams ship a customer-visible artifact within 30 days",
                    "observable_via": "release_notes"})
    _add("st_runtime_lang_stable", "state",
         "Runtime-language decision is holding — Rust hires closing in <60 days median.",
         confidence=0.70,
         scope_entities=[{"type": "decision", "id": d["d_runtime_lang"]}])
    _add("st_pricing_v2_flagship_blocker", "state",
         "Pricing v2 is the single largest decision blocking design-partner conversion ($420K+ ARR exposure).",
         confidence=0.79,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v2"]},
                         {"type": "decision", "id": d["d_design_partner_close"]}],
         falsifier={"condition": "pricing v2 lands without affecting partner conversion velocity",
                    "observable_via": "sales_pipeline"})
    _add("st_marketing_pipeline_thin", "state",
         "Outbound marketing pipeline is producing <2 qualified leads/week — well below the 8/week target.",
         confidence=0.75,
         scope_actors=[actor["noor"], actor["lina"]],
         falsifier={"condition": "qualified leads ≥ 8/week for 4 consecutive weeks",
                    "observable_via": "crm"})
    _add("st_legal_review_overdue", "state",
         "Enterprise MSA template has been in legal review for 11 weeks; 3 deals waiting.",
         confidence=0.83,
         scope_actors=[actor["lex"]],
         falsifier={"condition": "MSA template signed off and 3 deals advance",
                    "observable_via": "contracts"})

    # ---- relation (extra 6) ----
    _add("rel_partnership_to_signups", "relation",
         "Each Supabase-style partnership tutorial correlates with ~500 sign-ups in the following 30 days.",
         confidence=0.66,
         scope_entities=[{"type": "customer", "id": cust["supabase"]}],
         proposition_extra={"avg_signups_per_tutorial": 500})
    _add("rel_seniority_to_oncall_load", "relation",
         "Engineer seniority inversely correlates with willingness-to-flag-oncall-imbalance — juniors raise issues faster.",
         confidence=0.58)
    _add("rel_smb_to_support_burden", "relation",
         "Each new SMB customer adds ~3 support hours/month; conversion uplift is <$2K ARR/customer/year.",
         confidence=0.71,
         falsifier={"condition": "SMB cohort hits ≥ $4K ARR/customer/year for 6 months",
                    "observable_via": "billing+support"})
    _add("rel_qbr_to_renewal", "relation",
         "Customers attending Q-by-Q business reviews renew at 92%; non-attendees at 64%.",
         confidence=0.74,
         proposition_extra={"qbr_renewal_rate": 0.92, "non_qbr_renewal_rate": 0.64},
         falsifier={"condition": "non-QBR cohort renewal climbs to ≥ 85% in next cycle",
                    "observable_via": "csm"})
    _add("rel_obs_to_ent_close_velocity", "relation",
         "Observability surface maturity correlates with enterprise close velocity (median time to close drops from 84 → 51 days).",
         confidence=0.61,
         scope_entities=[{"type": "goal", "id": g["g_observ"]},
                         {"type": "goal", "id": g["g_ent_motion"]}])
    _add("rel_runway_to_decision_quality", "relation",
         "Founder decision quality (rationale-recall after 4 weeks) drops noticeably when runway visibility is <12 months.",
         confidence=0.55,
         scope_entities=[{"type": "goal", "id": g["g_runway"]}])

    # ---- prediction (extra 6) ----
    _add("pred_modal_paid", "prediction",
         "Modal Labs will sign a paid contract within 90 days at $80-120K ACV.",
         confidence=0.55, evaluate_at=days_from_now(90),
         scope_entities=[{"type": "customer", "id": cust["modal"]}])
    _add("pred_cursor_procurement_ping", "prediction",
         "Cursor will initiate procurement in next 60 days at higher-tier ACV than current.",
         confidence=0.49, evaluate_at=days_from_now(60),
         scope_entities=[{"type": "customer", "id": cust["cursor"]}])
    _add("pred_anthropic_renewal_at_risk", "prediction",
         "Anthropic renewal closes only if SOC2 II is delivered — otherwise 30% chance of non-renewal.",
         confidence=0.62, evaluate_at=days_from_now(120),
         scope_entities=[{"type": "customer", "id": cust["anthropic"]}])
    _add("pred_obs_q3_ship", "prediction",
         "Per-tenant observability dashboards will ship by end of Q3 if hiring closes.",
         confidence=0.58, evaluate_at=days_from_now(120),
         scope_entities=[{"type": "goal", "id": g["g_observ"]},
                         {"type": "goal", "id": g["g_hire_eng"]}])
    _add("pred_pricing_v2_arr_lift", "prediction",
         "Pricing v2 will lift NRR by 8-12 points within 6 months of GA.",
         confidence=0.51, evaluate_at=days_from_now(210),
         scope_entities=[{"type": "decision", "id": d["d_pricing_v2"]}])
    _add("pred_smb_cull", "prediction",
         "Within 60 days we will need to formally tier-down or sunset the bottom-quartile SMB cohort to recover support hours.",
         confidence=0.53, evaluate_at=days_from_now(60))

    # ---- pattern (extra 4) + pattern_instance (extra 6) ----
    _add("pat_quiet_heavy_user_to_procurement", "pattern",
         "Customers using >3x the typical seat-volume without procurement pings convert when nudged within 14 days.",
         confidence=0.61,
         proposition_extra={"volume_multiplier_threshold": 3.0, "nudge_window_days": 14})
    _add("pat_audit_log_in_renewal", "pattern",
         "Enterprise renewals raising audit-log requirements 60+ days early correlate with >15% upsell at close.",
         confidence=0.64,
         proposition_extra={"upsell_floor": 0.15})
    _add("pat_csm_silence_precedes_churn", "pattern",
         "CSM-side silence (>2 weeks of one-way comms) precedes churn signals 70% of the time.",
         confidence=0.67,
         proposition_extra={"silence_threshold_weeks": 2, "precedence_rate": 0.7})
    _add("pat_pricing_change_dp_friction", "pattern",
         "Pricing changes shipped without 30-day partner preview correlate with 40% increase in support-thread escalations.",
         confidence=0.59,
         proposition_extra={"escalation_lift": 0.4})

    _add("pat_inst_cursor_quiet_heavy", "pattern_instance",
         "Cursor instance of the quiet-heavy-user pattern: 4.2x volume, no procurement comms.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["cursor"]}],
         falsifier={"condition": "Cursor volume normalises to <2x within 30 days",
                    "observable_via": "usage"},
         support_models=[_M("pat_quiet_heavy_user_to_procurement")])
    _add("pat_inst_anthropic_audit", "pattern_instance",
         "Anthropic instance of the audit-log-in-renewal pattern: 4-month-early audit ask.",
         confidence=0.71,
         scope_entities=[{"type": "customer", "id": cust["anthropic"]}],
         support_models=[_M("pat_audit_log_in_renewal")])
    _add("pat_inst_scale_silence", "pattern_instance",
         "Scale AI instance of the CSM-silence pattern: 3 weeks of one-way comms.",
         confidence=0.67,
         scope_entities=[{"type": "customer", "id": cust["scale"]}],
         falsifier={"condition": "Scale resumes 2-way CSM comms within 14 days",
                    "observable_via": "slack+email"},
         support_models=[_M("pat_csm_silence_precedes_churn")])
    _add("pat_inst_smallco_silence", "pattern_instance",
         "SmallCo instance of the CSM-silence pattern: 5 weeks dark plus invoice failures.",
         confidence=0.79,
         scope_entities=[{"type": "customer", "id": cust["smallco"]}],
         falsifier={"condition": "SmallCo CSM is responsive AND invoices succeed for 30 days",
                    "observable_via": "csm+stripe"},
         support_models=[_M("pat_csm_silence_precedes_churn")])
    _add("pat_inst_replit_velocity_decay", "pattern_instance",
         "Replit instance of the velocity-decay pattern as it relates to their 8 internal owners.",
         confidence=0.55,
         scope_entities=[{"type": "customer", "id": cust["replit"]}],
         support_models=[_M("pat_eng_velocity_decay")])
    _add("pat_inst_truss_pricing_change", "pattern_instance",
         "Truss instance of the pricing-change-friction pattern about to materialise — pricing v2 ships without 30-day preview.",
         confidence=0.62,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v2"]}],
         support_models=[_M("pat_pricing_change_dp_friction")])

    # ---- capability_assessment (extra 4) ----
    _add("cap_csm_unbalanced", "capability_assessment",
         "Customer-success capability is unbalanced — Avery carries 60% of named accounts vs Imani 25% / Kai 15%.",
         confidence=0.69,
         scope_actors=[actor["avery"], actor["imani"], actor["kai"]])
    _add("cap_finance_thin", "capability_assessment",
         "Finance capability is thin — David runs all of FP&A solo; pricing v2 modeling is a single-person dependency.",
         confidence=0.71,
         scope_actors=[actor["david"]],
         falsifier={"condition": "FP&A coverage adds a second FTE within 90 days",
                    "observable_via": "ats"})
    _add("cap_legal_external", "capability_assessment",
         "Legal capability is fully external (Lex on advisor cadence) — limits how quickly contracts iterate.",
         confidence=0.65,
         scope_actors=[actor["lex"]])
    _add("cap_design_high", "capability_assessment",
         "Design capability is high relative to peer set — Grace + Noah ship at unusual cadence given headcount.",
         confidence=0.61,
         scope_actors=[actor["grace"], actor["noah"]])

    # ---- hypothesis (extra 5) ----
    _add("hyp_smb_segment_cap", "hypothesis",
         "SMB segment may have a natural ARR cap of $80-100K per customer; pursuing larger SMB deals likely loses.",
         confidence=0.43)
    _add("hyp_observability_is_actually_v1", "hypothesis",
         "Observability gap may be a stronger contract-blocker than API stability for enterprise buyers.",
         confidence=0.46,
         scope_entities=[{"type": "goal", "id": g["g_observ"]},
                         {"type": "goal", "id": g["g_api_v1"]}])
    _add("hyp_hire_first_sales_engineer", "hypothesis",
         "We may benefit more from a sales engineer than a 5th AE for converting design partners.",
         confidence=0.41,
         scope_entities=[{"type": "goal", "id": g["g_ent_motion"]}])
    _add("hyp_runtime_lang_morale", "hypothesis",
         "The Rust-only runtime decision may be costing us hiring velocity more than it's saving us in performance.",
         confidence=0.38,
         scope_entities=[{"type": "decision", "id": d["d_runtime_lang"]}])
    _add("hyp_audit_log_is_3_week_project", "hypothesis",
         "Per-tenant audit logging is closer to a 3-week project than the 6-week estimate (we already have most plumbing).",
         confidence=0.49,
         scope_entities=[{"type": "goal", "id": g["g_observ"]}])

    # ---- concern (extra 6) ----
    _add("conc_modal_silent_loss", "concern",
         "Risk that Modal goes quiet again before we close paid — they've ghosted twice already.",
         confidence=0.39,
         scope_entities=[{"type": "customer", "id": cust["modal"]}])
    _add("conc_anthropic_compliance_loss", "concern",
         "Risk that Anthropic walks if SOC2 II slips past their renewal — biggest single-customer ARR exposure.",
         confidence=0.42,
         scope_entities=[{"type": "customer", "id": cust["anthropic"]}])
    _add("conc_pricing_v2_partner_revolt", "concern",
         "Risk that pricing v2 triggers a design-partner revolt (Vercel especially) — they negotiated grandfathering verbally.",
         confidence=0.44,
         scope_entities=[{"type": "customer", "id": cust["vercel"]},
                         {"type": "decision", "id": d["d_pricing_v2"]}])
    _add("conc_legal_bottleneck", "concern",
         "Risk that legal review backlog stalls 3 design-partner closes; sales pipeline dollar-impact ~$350K.",
         confidence=0.55,
         scope_actors=[actor["lex"]])
    _add("conc_smb_distraction", "concern",
         "Risk that SMB support load distracts engineering from the design-partner critical path.",
         confidence=0.48)
    _add("conc_cofounder_friction", "concern",
         "Risk of co-founder friction between Maya and Jules over pricing v2 sequencing — different urgency reads.",
         confidence=0.34,
         scope_actors=[actor["maya"], actor["jules"]])

    # ---- market_assessment (extra 4) ----
    _add("mkt_dev_infra_consolidation_window", "market_assessment",
         "Window for consolidation in AI-native dev infra closes in ~18 months; expect 3-4 acqui-hires by 2027.",
         confidence=0.59,
         proposition_extra={"horizon_months": 18, "acquihire_estimate": 4})
    _add("mkt_observability_arms_race", "market_assessment",
         "Observability is now an arms race in dev infra — buyers expect per-tenant SLOs and error budgets out of box.",
         confidence=0.72,
         falsifier={"condition": "<30% of competitors ship per-tenant SLO dashboards in 12 months",
                    "observable_via": "competitor_audit"})
    _add("mkt_buyer_committee_growing", "market_assessment",
         "Enterprise buyer committees in dev infra are growing — typical deal now requires 4+ stakeholders.",
         confidence=0.66)
    _add("mkt_sso_table_stakes_pricing", "market_assessment",
         "SSO is table stakes but pricing is now the second most-cited contract negotiation lever.",
         confidence=0.64)

    # ---- environmental_trend (extra 4) ----
    _add("env_ai_app_layer_demand", "environmental_trend",
         "AI application-layer demand is pulling infra-tier vendors into faster procurement cycles than 2024.",
         confidence=0.69)
    _add("env_open_source_pressure", "environmental_trend",
         "Open-source alternatives in our space are improving fast; commercial differentiation must move up the stack.",
         confidence=0.62,
         falsifier={"condition": "open-source equivalents lose feature parity over 12 months",
                    "observable_via": "competitor_audit"})
    _add("env_macro_tech_hiring_softening", "environmental_trend",
         "Tech hiring is softening overall but senior infra remains tight; offer/acceptance ratio still ~3:1.",
         confidence=0.67)
    _add("env_compliance_uplift_vendor_chain", "environmental_trend",
         "Compliance uplift is propagating through the vendor chain — sub-processors now subject to the same diligence.",
         confidence=0.71)

    return out


def build_recommendations(actors: list[GeneratedActor],
                          commitments: list[GeneratedCommitment],
                          goals: list[GeneratedGoal],
                          decisions: list[GeneratedDecision],
                          signals: list[GeneratedSignal],
                          models: list[GeneratedModel] | None = None) -> list[GeneratedRecommendation]:
    ceo_id = did(COMPANY, "actor", "maya")
    sig_ids = {s.id: s for s in signals}
    model_ids = {m.id for m in (models or [])}

    # Helper: pull the signal id for the first signal whose content mentions
    # a phrase, so each recommendation has supporting evidence.
    def find_signal_ids(phrase: str, limit: int = 3) -> list[str]:
        out_ids: list[str] = []
        for s in signals:
            if phrase.lower() in s.content_text.lower():
                out_ids.append(s.id)
                if len(out_ids) >= limit:
                    break
        return out_ids

    def _models_for(*keys: str) -> list[str]:
        """Return supporting-model UUIDs for the given keys, filtered
        to ones the build_models() pass actually produced."""
        return [_M(k) for k in keys if _M(k) in model_ids]

    recs: list[GeneratedRecommendation] = []

    # 1. Capacity — pause new commitments
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_capacity"),
        proposition_text=(
            "Engineering is at ~95% utilization — pause net-new commitments "
            "until SSO ships."
        ),
        target_act_ref=TargetActRef(
            type="goal",
            id=did(COMPANY, "goal", "g_sso"),
        ),
        proposed_change={
            "operation": "transition",
            "payload": {"new_state": "active",
                        "note": "freeze new commitments until SSO lands"},
        },
        expected_impact_usd=95000.0,
        supporting_observation_ids=find_signal_ids("utilization") + find_signal_ids("at the line"),
        target_actor_id=ceo_id,
    ))

    # 2. Customer pressure — SSO design partners
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_sso_pressure"),
        proposition_text=(
            "3 design partners (Linear, Vercel, Replit) requested SSO in past "
            "60 days — $280K ARR exposure. Accelerate."
        ),
        target_act_ref=TargetActRef(
            type="commitment",
            id=did(COMPANY, "commitment", "c_priya_sso_pmf"),
        ),
        proposed_change={
            "operation": "transition",
            "payload": {"new_state": "active",
                        "priority": "p0", "note": "pull into Q2 ahead of original Q3 plan"},
        },
        expected_impact_usd=280000.0,
        supporting_observation_ids=(
            find_signal_ids("Linear", 2)
            + find_signal_ids("Vercel", 1)
            + find_signal_ids("Replit", 1)
        )[:5],
        supporting_model_ids=_models_for("st_industrium_unblocked", "rel_sso_to_arr",
                                          "pat_dp_sso_ask", "pat_inst_linear_sso",
                                          "pat_inst_vercel_sso", "mkt_sso_table_stakes"),
        target_actor_id=ceo_id,
    ))

    # 3. Personnel — Sarah on rotation 4 of 6 weeks
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_sarah_burnout"),
        proposition_text=(
            "Lead engineer Sarah on incident rotation 4 of past 6 weeks — "
            "burnout pattern emerging. Redistribute pager."
        ),
        target_act_ref=TargetActRef(
            type="commitment",
            id=did(COMPANY, "commitment", "c_sarah_oncall"),
        ),
        proposed_change={
            "operation": "transition",
            "payload": {"new_state": "blocked",
                        "note": "block new on-call shifts for Sarah"},
        },
        expected_impact_usd=50000.0,
        supporting_observation_ids=find_signal_ids("Sarah carried")
            + find_signal_ids("4 weeks on the pager"),
        supporting_model_ids=_models_for("st_sarah_burnout", "rel_velocity_oncall",
                                          "pat_inst_sarah_velocity",
                                          "conc_sarah_attrition",
                                          "pred_sarah_burnout_event"),
        target_actor_id=ceo_id,
    ))

    # 4. Decision revisit — API redesign
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_apiv1_revisit"),
        proposition_text=(
            "API redesign decision predates 3 customer requests for stable v1 "
            "— re-scope before launching."
        ),
        target_act_ref=TargetActRef(
            type="decision",
            id=did(COMPANY, "decision", "d_api_redesign"),
        ),
        proposed_change={
            "operation": "archive",
            "payload": {"reason": "superseded_by_customer_demand",
                        "note": "stable v1 over redesign"},
        },
        expected_impact_usd=120000.0,
        supporting_observation_ids=find_signal_ids("lock the API")
            + find_signal_ids("API redesign"),
        supporting_model_ids=_models_for("st_apiv1_late", "rel_apiv1_decision_customers",
                                          "hyp_apiv1_redesign_was_premature",
                                          "conc_apiv1_breaking_changes",
                                          "pred_apiv1_slip"),
        target_actor_id=ceo_id,
    ))

    # 5. Strategic — 8 workstreams, 3 lack demand
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_strategic_workstreams"),
        proposition_text=(
            "Roadmap has 8 active workstreams; 3 lack customer demand signal. "
            "Re-scope before Q3 cut."
        ),
        target_act_ref=TargetActRef(
            type="goal",
            id=did(COMPANY, "goal", "ga_2026"),
        ),
        proposed_change={
            "operation": "transition",
            "payload": {"new_state": "active",
                        "note": "kill 3 workstreams lacking customer signal"},
        },
        expected_impact_usd=200000.0,
        supporting_observation_ids=find_signal_ids("8 active workstreams")
            + find_signal_ids("self-host"),
        supporting_model_ids=_models_for("rel_workstream_to_demand",
                                          "hyp_workstream_pruning_underestimates_morale",
                                          "st_no_self_host_inflexible"),
        target_actor_id=ceo_id,
    ))

    # 6. Founder context — VP Eng sync overdue, hiring blocked
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_founder_context"),
        proposition_text=(
            "3 weeks since founder–VP Eng sync; 2 senior eng roles blocked on "
            "founder close-call time."
        ),
        target_act_ref=TargetActRef(
            type="commitment",
            id=did(COMPANY, "commitment", "c_hugh_close_calls"),
        ),
        proposed_change={
            "operation": "transition",
            "payload": {"new_state": "active",
                        "note": "schedule both founder close calls this week"},
        },
        expected_impact_usd=75000.0,
        supporting_observation_ids=find_signal_ids("1:1 with Maya")
            + find_signal_ids("close call"),
        supporting_model_ids=_models_for("st_founder_overload",
                                          "st_2_open_eng_roles",
                                          "rel_founder_to_eng_close",
                                          "rel_hiring_to_critical_path",
                                          "cap_founder_decision_velocity"),
        target_actor_id=ceo_id,
    ))

    # 7. Smaller-account customer health drift
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_smallco_drift"),
        proposition_text=(
            "SmallCo Studios showing health drift over 30 days — 4 invoice "
            "failures, no QBR attendance. Decide path."
        ),
        target_act_ref=TargetActRef(
            type="commitment",
            id=did(COMPANY, "commitment", "c_kai_smallco"),
        ),
        proposed_change={
            "operation": "transition",
            "payload": {"new_state": "active",
                        "note": "exec touch + decision on whether to keep account"},
        },
        expected_impact_usd=40000.0,
        supporting_observation_ids=find_signal_ids("SmallCo"),
        supporting_model_ids=_models_for("st_acme_renewal_iffy", "pred_smallco_churn"),
        target_actor_id=ceo_id,
    ))

    return recs


# =====================================================================
# Top-level: build, validate, optionally emit SQL
# =====================================================================


def build_bundle() -> GeneratedBundle:
    actors = build_actors()
    customers = build_customers()
    goals = build_goals()
    decisions = build_decisions()
    commitments = build_commitments(actors, customers, goals, decisions)
    signals = build_signals(actors, customers, commitments, goals, decisions)
    models = build_models(actors, customers, commitments, goals, decisions, signals)
    recommendations = build_recommendations(
        actors, commitments, goals, decisions, signals, models=models,
    )

    return GeneratedBundle(
        company_id=COMPANY,
        ceo_actor_id=did(COMPANY, "actor", "maya"),
        actors=actors,
        customers=customers,
        goals=goals,
        decisions=decisions,
        commitments=commitments,
        signals=signals,
        models=models,
        recommendations=recommendations,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--emit", action="store_true",
        help="Write the SQL snapshot to demo/snapshots/truss-v1.sql",
    )
    parser.add_argument(
        "--out", default="demo/snapshots/truss-v1.sql",
        help="Output path for the SQL snapshot",
    )
    parser.add_argument(
        "--compress", action="store_true",
        help="Zstd-compress the SQL snapshot (.sql.zst)",
    )
    args = parser.parse_args()

    print(f"Building Truss bundle...")
    import yaml
    with open("demo/generation/specs/truss.yaml") as f:
        spec = yaml.safe_load(f)
    bundle = build_bundle()
    print(f"  actors:           {len(bundle.actors)}")
    print(f"  customers:        {len(bundle.customers)}")
    print(f"  goals:            {len(bundle.goals)}")
    print(f"  decisions:        {len(bundle.decisions)}")
    print(f"  commitments:      {len(bundle.commitments)}")
    print(f"  signals:          {len(bundle.signals)}")
    print(f"  models:           {len(bundle.models)}")
    print(f"  recommendations:  {len(bundle.recommendations)}")

    print("Validating...")
    errors = validate_bundle(bundle, spec=spec)
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
