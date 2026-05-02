"""Northwind Software — hand-authored demo bundle.

Series B SaaS, 180 employees, $14M ARR, growing 80% YoY. Building a
modern HR platform for mid-market companies. Past founder-overload;
the product earns its keep on a normal Tuesday.

Spec: demo/generation/specs/northwind.yaml.

Counts target ~180 actors, ~50 customers, 9 goals, 8 decisions, 250
commitments, ~400 signals, 7 recommendations. We trim actor count to
~60 (the synthetic fallback was 13 — even 60 is enough density to
back the substrate without authoring 180 lines by hand). Validator
tolerance is ±10%.
"""
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

import yaml

from demo.generation.built._helpers import (
    days_ago, days_from_now, did, find_signals_containing,
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


COMPANY = "northwind"


# =====================================================================
# Actors — 60. CEO Jordan + leadership + functional teams.
# =====================================================================

ACTOR_SPECS = [
    # Leadership
    ("jordan",      "Jordan Reyes",       "ceo",       None,        "CEO. Past founder-overload phase."),
    ("alex",        "Alex Yamamoto",      "coo",       "jordan",    "COO; runs operating cadence."),
    ("priya_n",     "Priya Vasquez",      "vp_eng",    "jordan",    "VP Engineering; coordinates 3 eng pods."),
    ("chen",        "Chen Watari",        "vp_product","jordan",    "VP Product."),
    ("morgan_n",    "Morgan Bellamy",     "cro",       "jordan",    "CRO. Owns the $14M ARR number."),
    ("dani",        "Dani Olsson",        "vp_cs",     "jordan",    "VP Customer Success."),
    ("hector",      "Hector Almeida",     "vp_marketing","jordan",  "VP Marketing."),
    ("sasha",       "Sasha Konstantinov", "cfo",       "jordan",    "CFO. Focused on burn-vs-runway."),
    ("priti",       "Priti Mahajan",      "head_people", "jordan",  "Head of People."),
    # Engineering pods (3 leads + senior engs + mid)
    ("kai_n",       "Kai Tateishi",       "em",        "priya_n",   "EM, Platform pod."),
    ("zara",        "Zara Roosevelt",     "em",        "priya_n",   "EM, App pod. Has gone 6 wks without 1:1s."),
    ("luca",        "Luca Romani",        "em",        "priya_n",   "EM, Integrations pod."),
    *[(f"eng{i}", n, "engineer", lead, "Engineer.") for i, (n, lead) in enumerate([
        ("Henrik Steiner",        "kai_n"),
        ("Beatriz Carmona",       "kai_n"),
        ("Tariq Mansour",         "kai_n"),
        ("Esther Krasner",        "kai_n"),
        ("Owen Sandoval",         "kai_n"),
        ("Mira Antoniou",         "zara"),
        ("Rafael Baylon",         "zara"),
        ("Sienna Brodie",         "zara"),
        ("Jonas Lindqvist",       "zara"),
        ("Aileen Bautista",       "zara"),
        ("Rhys Walford",          "zara"),
        ("Cosima Bertrand",       "luca"),
        ("Manon Kowalski",        "luca"),
        ("Yasin Demir",           "luca"),
        ("Frida Engström",        "luca"),
        ("Tom Quispe",             "kai_n"),
        ("Rina Ozaki",             "zara"),
    ])],
    # Product / Design
    ("ben_n",       "Ben Eckstrom",       "pm",        "chen",      "PM, Platform."),
    ("mei",         "Mei Tanigawa",       "pm",        "chen",      "PM, Apps."),
    ("oren",        "Oren Pavlovsky",     "pm",        "chen",      "PM, Integrations."),
    ("ava",         "Ava Kindt",          "designer",  "chen",      "Lead designer."),
    ("nik",         "Nik Robles",         "designer",  "chen",      "Designer."),
    # Sales
    ("monica_n",    "Monica Faulkner",    "head_sales","morgan_n",  "Head of Enterprise Sales."),
    ("damon",       "Damon Whitlock",     "ae",        "monica_n",  "AE, enterprise."),
    ("priscilla",   "Priscilla Aoki",     "ae",        "monica_n",  "AE, enterprise."),
    ("victor_n",    "Victor Castaneda",   "ae",        "monica_n",  "AE, mid-market."),
    ("nadia_n",     "Nadia Brennan",      "ae",        "monica_n",  "AE, mid-market."),
    ("isaac",       "Isaac Aldenberg",    "ae",        "monica_n",  "AE, mid-market."),
    # CS
    ("avery_n",     "Avery Tomson",       "csm",       "dani",      "Senior CSM, top accounts."),
    ("kai_csm",     "Kai Hjelmeland",     "csm",       "dani",      "CSM."),
    ("juno",        "Juno Bertonelli",    "csm",       "dani",      "CSM. Direct report of Zara who has missed 1:1s."),
    ("perla",       "Perla Saldivar",     "csm",       "dani",      "CSM."),
    ("riku",        "Riku Saavedra",      "csm",       "dani",      "CSM."),
    # Marketing
    ("noor_n",      "Noor Lindenberg",    "marketing", "hector",    "Content lead."),
    ("yuri",        "Yuri Hashemi",       "marketing", "hector",    "Field marketing."),
    ("lila",        "Lila Cardoso",       "marketing", "hector",    "Demand gen."),
    # Ops
    ("simon",       "Simon Heyerdahl",    "ops",       "alex",      "Head of Operations."),
    ("emil_n",      "Emil Patrocinio",    "ops",       "alex",      "Ops manager, GTM systems."),
    ("greta",       "Greta Hoffmann",     "ops",       "alex",      "Revenue operations."),
    # Finance
    ("ravi_n",      "Ravi Bachchan",      "finance",   "sasha",     "Finance manager."),
    ("dora",        "Dora Kingsley",      "finance",   "sasha",     "Accounting."),
    # People
    ("fern",        "Fern Eulalia",       "people",    "priti",     "Recruiter."),
    ("malou",       "Malou Andersen",     "people",    "priti",     "People ops."),
    ("kev",         "Kev Salinger",       "people",    "priti",     "Talent ops."),
    # Legal
    ("rita",        "Rita Sundberg",      "legal",     "jordan",    "GC."),
    # Advisors
    ("edie_n",      "Edie Marquez",       "advisor",   "jordan",    "Board advisor."),
    ("renée",       "Renée Beaumont",     "advisor",   "jordan",    "Board observer."),
]


def build_actors() -> list[GeneratedActor]:
    out: list[GeneratedActor] = []
    for entry in ACTOR_SPECS:
        key, name, role, mgr, brief = entry
        out.append(GeneratedActor(
            id=did(COMPANY, "actor", key),
            name=name, role=role,
            manager_id=did(COMPANY, "actor", mgr) if mgr else None,
            personality_brief=brief,
            email=f"{key}@northwind.io",
        ))
    return out


# =====================================================================
# Customers — 50 paying. ARR ~$14M target.
# =====================================================================

CUSTOMER_SPECS = [
    # Top tier — 4 enterprise accounts
    ("acme_corp",     "Acme Corp",                   840000, "enterprise",  "watching",   ["CIO — Aaron Lazar"]),
    ("wayfair",       "Wayfair",                    1200000, "enterprise",  "healthy",    ["Head of People — Pat Trumbull"]),
    ("notion",        "Notion",                      720000, "enterprise",  "healthy",    ["People Ops — Akshay Kothari"]),
    ("drift",         "Drift",                       540000, "enterprise",  "healthy",    ["VP People — Devon Tatum"]),
    # Mid-market 25
    ("pendo",         "Pendo",                       320000, "mid_market",  "healthy",    ["HR Lead — Kim Laden"]),
    ("hex_n",         "Hex",                         180000, "mid_market",  "healthy",    ["Ops — Cynthia Ng"]),
    ("retool_n",      "Retool",                      210000, "mid_market",  "healthy",    ["People — Xavier Chen"]),
    ("brex",          "Brex",                        390000, "mid_market",  "healthy",    ["People — Anna Brody"]),
    ("ramp",          "Ramp",                        420000, "mid_market",  "healthy",    ["People — Tomás Ortiz"]),
    ("airtable_n",    "Airtable",                    280000, "mid_market",  "healthy",    ["VP People — Sasha Yang"]),
    ("dropbox",       "Dropbox",                     475000, "enterprise",  "healthy",    ["Head HR — Alex Bly"]),
    ("zoom",          "Zoom",                        380000, "mid_market",  "healthy",    ["People — Dru Westphal"]),
    ("envoy",         "Envoy",                       195000, "mid_market",  "healthy",    ["Head People — Kit Vance"]),
    ("calm",          "Calm",                        165000, "mid_market",  "healthy",    ["HR — Tia Bredahl"]),
    ("github",        "GitHub",                      610000, "enterprise",  "healthy",    ["VP People — Ana Iorio"]),
    ("stripe",        "Stripe",                      540000, "enterprise",  "healthy",    ["People — Greta Sigurd"]),
    ("twilio",        "Twilio",                      355000, "mid_market",  "healthy",    ["People — Boon Yi"]),
    ("hashicorp",     "HashiCorp",                   295000, "mid_market",  "healthy",    ["Head People — Joel Whyte"]),
    ("plaid",         "Plaid",                       275000, "mid_market",  "healthy",    ["People Ops — Mavis Gore"]),
    ("amplitude",     "Amplitude",                   245000, "mid_market",  "healthy",    ["Head People — Caleb Trent"]),
    ("databricks",    "Databricks",                  490000, "enterprise",  "healthy",    ["People — Renske Hofman"]),
    ("snowflake",     "Snowflake",                   620000, "enterprise",  "healthy",    ["People — Hua Lin"]),
    ("segment",       "Segment",                     230000, "mid_market",  "healthy",    ["Ops — Kira Salava"]),
    ("intercom",      "Intercom",                    285000, "mid_market",  "healthy",    ["People — Mert Yilmaz"]),
    ("doordash",      "DoorDash",                    430000, "enterprise",  "healthy",    ["People — Owen Bekky"]),
    ("instacart",     "Instacart",                   415000, "enterprise",  "healthy",    ["People — Mia Devine"]),
    ("rivian",        "Rivian",                      305000, "mid_market",  "healthy",    ["HR — Phebe Coleman"]),
    ("airbnb",        "Airbnb",                      540000, "enterprise",  "healthy",    ["People — Wren Lopata"]),
    ("uber",          "Uber",                        635000, "enterprise",  "healthy",    ["People — Ezra Boyle"]),
    ("lyft",          "Lyft",                        320000, "mid_market",  "healthy",    ["People — Iva Gorska"]),
    # SMB tier
    ("xero",          "Xero",                        110000, "smb",         "healthy",    ["Ops — Pia Linde"]),
    ("freshdesk",     "Freshworks",                   90000, "smb",         "healthy",    ["People — Vid Ellis"]),
    ("hubspot",       "HubSpot",                     145000, "mid_market",  "healthy",    ["People — Yara Volkov"]),
    ("klaviyo",       "Klaviyo",                     185000, "mid_market",  "healthy",    ["People — Trip Bayardo"]),
    ("shopify",       "Shopify",                     520000, "enterprise",  "healthy",    ["People — Aria Knapp"]),
    ("zapier",        "Zapier",                      225000, "mid_market",  "healthy",    ["People — Jude Lehrer"]),
    ("squarespace",   "Squarespace",                 175000, "mid_market",  "healthy",    ["People — Kit Fletcher"]),
    ("etsy",          "Etsy",                        390000, "mid_market",  "healthy",    ["People — Ace Vermillion"]),
    ("toast",         "Toast",                       210000, "mid_market",  "healthy",    ["People — Reza Tabari"]),
    ("appfolio",      "AppFolio",                    150000, "mid_market",  "healthy",    ["People — Cami Salt"]),
    ("workday_p",     "Workday (prospect)",               0, "prospect",    "watching",   ["VP People — Ros Boa"]),
    ("salesforce_p",  "Salesforce (prospect)",            0, "prospect",    "watching",   ["People — Demi Bond"]),
    ("oracle_p",      "Oracle (prospect)",                0, "prospect",    "watching",   ["People — Lin Caro"]),
    # Drift accounts (small, watching)
    ("opal",          "Opal Logistics",               75000, "smb",         "watching",   ["HR — Sumi Aldea"]),
    ("crystal",       "Crystal Pharmacy",             52000, "smb",         "watching",   ["HR — Dave Bohem"]),
    ("riverstone",    "Riverstone Capital",           65000, "smb",         "watching",   ["People — Theo Rine"]),
    ("northstar",     "Northstar Manufacturing",      48000, "smb",         "at_risk",    ["HR — Ines Coombs"]),
    # Recently churned -> at_risk
    ("oldworld",      "OldWorld Foods",               38000, "smb",         "at_risk",    ["HR — Una Kovac"]),
    ("paragon",       "Paragon Health",              115000, "mid_market",  "watching",   ["People — Iris Yelo"]),
    ("emberton",      "Emberton Industries",          42000, "smb",         "watching",   ["HR — Gus Zay"]),
]


def build_customers() -> list[GeneratedCustomer]:
    return [
        GeneratedCustomer(
            id=did(COMPANY, "customer", k),
            company_name=name,
            arr_usd=arr,
            segment=seg,                         # type: ignore[arg-type]
            current_health=health,               # type: ignore[arg-type]
            primary_contacts=contacts,
        )
        for k, name, arr, seg, health, contacts in CUSTOMER_SPECS
    ]


# =====================================================================
# Goals — 9
# =====================================================================

GOAL_SPECS = [
    ("g_30m",       "Reach $30M ARR by end of 2027",
     "Double from $14M while expanding into enterprise tier.",
     "jordan", None, "strategic"),
    ("g_q3_pipeline",  "Land 8 enterprise customers by EOY",
     "Expansion into Fortune 500 — needs SAML, audit logs, SOC2 type II.",
     "monica_n", "g_30m", "strategic"),
    ("g_renewal",   "Maintain >115% NRR through 2026",
     "Net revenue retention is the lifeblood of valuation.",
     "dani", "g_30m", "strategic"),
    ("g_saml",      "Ship SAML/SSO + audit-log feature for enterprise",
     "Customer pressure: 3 customers requested in past 60 days.",
     "ben_n", "g_q3_pipeline", "operational"),
    ("g_pricing",   "Migrate pricing v2 (per-seat → per-employee)",
     "Closer alignment with customers' own ARR. NRR exposure.",
     "ben_n", "g_renewal", "operational"),
    ("g_q3_eng",    "Re-balance engineering capacity (91% util)",
     "Engineering team showing capacity strain.",
     "priya_n", None, "operational"),
    ("g_postgres",  "Re-evaluate Postgres-only architecture",
     "14-month-old decision. Conditions have changed: 3 customers >$500K want low-latency reads.",
     "kai_n", "g_q3_pipeline", "operational"),
    ("g_qbr_cad",   "Standardize QBR cadence across enterprise accounts",
     "Inconsistent today; some accounts go 90 days without QBR.",
     "dani", "g_renewal", "operational"),
    ("g_brand",     "Position as the modern HR platform of choice",
     "Brand work + analyst relations.",
     "hector", "g_30m", "operational"),
]


def build_goals() -> list[GeneratedGoal]:
    return [
        GeneratedGoal(
            id=did(COMPANY, "goal", k),
            title=t, description=d,
            owner_id=did(COMPANY, "actor", o),
            target_date=days_from_now(180),
            parent_goal_id=did(COMPANY, "goal", p) if p else None,
            altitude=alt,                            # type: ignore[arg-type]
        )
        for k, t, d, o, p, alt in GOAL_SPECS
    ]


# =====================================================================
# Decisions — 8
# =====================================================================

DECISION_SPECS = [
    ("d_postgres_only",
     "Postgres-only architecture (14 months ago)",
     "All customer data lives in Postgres. No Redis, no Elasticsearch.",
     "Operational simplicity. Hiring + ops cost. Was right at our scale 14 months ago.",
     {"area": "engineering"},
     ["3+ customers above $500K request low-latency search", "p99 read latency exceeds 500ms"]),
    ("d_pricing_v1",
     "Per-seat pricing v1",
     "Per-seat with 100-seat minimum.",
     "Aligns with peer set. Customers like predictability.",
     {"area": "pricing"},
     ["NRR drops below 110%", "Customer requests >5 per quarter to change"]),
    ("d_no_self_host_n",
     "No self-host — cloud only",
     "We don't ship a self-hosted version.",
     "Operational complexity vs revenue.",
     {"area": "product"},
     ["Enterprise prospect blocks on self-host AND ACV >= $1M"]),
    ("d_dont_pursue_oracle",
     "Don't pursue the Oracle migration motion",
     "We'll stay focused on greenfield mid-market and enterprise.",
     "Migrations from Oracle take 18+ months; we don't have the muscle.",
     {"area": "go-to-market"},
     ["3+ inbound Oracle migration leads in a quarter"]),
    ("d_brand_modern",
     "Lead brand on 'modern' vs 'enterprise'",
     "We position against legacy HR vendors as the modern alternative.",
     "Differentiates us in the analyst grid.",
     {"area": "marketing"},
     ["Customer interviews indicate 'modern' is no longer differentiating"]),
    ("d_us_first",
     "US-first market focus through 2026",
     "International expansion deferred to 2027.",
     "Compliance overhead (GDPR, regional data residency) is heavy.",
     {"area": "go-to-market"},
     ["3+ customer asks for EU presence with ACV >= $300K"]),
    ("d_cs_pod_model",
     "Pod-based CS team structure",
     "Each CSM owns 8-12 accounts; managers oversee 5-6 CSMs.",
     "Better customer outcomes than book-of-business model.",
     {"area": "customer_success"},
     ["NRR drops; CSM utilization >85% sustained"]),
    ("d_q3_offsite_skip",
     "Skip the company offsite this quarter",
     "Cost vs benefit didn't pencil.",
     "Burn is tight; team morale is fine.",
     {"area": "operations"},
     ["Engagement scores drop below benchmark"]),
]


def build_decisions() -> list[GeneratedDecision]:
    return [
        GeneratedDecision(
            id=did(COMPANY, "decision", k),
            title=t, decision_text=dt, rationale=ra,
            scope=sc, revisit_triggers=rt,
        )
        for k, t, dt, ra, sc, rt in DECISION_SPECS
    ]


# =====================================================================
# Commitments — programmatic ~250
# =====================================================================

def build_commitments(actors, customers, goals, decisions):
    actor_keys = {k: did(COMPANY, "actor", k) for k, *_ in ACTOR_SPECS}
    cust_keys = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    goal_keys = {k: did(COMPANY, "goal", k) for k, *_ in GOAL_SPECS}
    dec_keys = {k: did(COMPANY, "decision", k) for k, *_ in DECISION_SPECS}
    rng = random.Random(11)

    out = []

    def _c(key, title, owner, state="active", goal=None, customer=None,
           decisions=None, contributors=None):
        out.append(GeneratedCommitment(
            id=did(COMPANY, "commitment", key),
            title=title,
            owner_id=actor_keys[owner],
            contributors=[actor_keys[c] for c in (contributors or []) if c in actor_keys],
            state=state,                             # type: ignore[arg-type]
            due_date=days_from_now(rng.randint(7, 120)),
            contributes_to_goal_id=goal_keys[goal] if goal else None,
            depends_on=[],
            constrained_by_decision_ids=[dec_keys[d] for d in (decisions or []) if d in dec_keys],
            served_by_customer_id=cust_keys[customer] if customer else None,
        ))

    # SAML/SSO push (the customer-pressure recommendation)
    _c("c_saml_lead",       "Lead SAML/SSO implementation",       "kai_n",     "active", "g_saml", contributors=["eng0", "eng1"])
    _c("c_saml_audit_logs", "Implement audit-log feature",        "kai_n",     "active", "g_saml")
    _c("c_saml_review_acme","Acme SSO requirements review",       "ben_n",     "active", "g_saml", customer="acme_corp")
    _c("c_saml_review_wf",  "Wayfair SSO requirements review",    "ben_n",     "active", "g_saml", customer="wayfair")
    _c("c_saml_review_drift","Drift SSO requirements review",     "ben_n",     "active", "g_saml", customer="drift")
    _c("c_saml_dpa",        "DPA template update for SSO",        "rita",      "active", "g_saml")

    # Postgres revisit (decision-revisit recommendation)
    _c("c_pg_eval",         "Evaluate Postgres-only fit for $500K+ accounts", "kai_n", "active", "g_postgres", decisions=["d_postgres_only"])
    _c("c_pg_spike",        "Spike: read replicas + materialized views",      "eng2",  "active", "g_postgres", decisions=["d_postgres_only"])
    _c("c_pg_dropbox",      "Dropbox latency-budget review",                  "eng3",  "active", "g_postgres", customer="dropbox")
    _c("c_pg_snowflake",    "Snowflake architecture conversation",            "eng3",  "active", "g_postgres", customer="snowflake")

    # Capacity reallocation (capacity recommendation)
    _c("c_cap_audit",       "Engineering capacity audit",          "priya_n",   "active", "g_q3_eng")
    _c("c_cap_redistribute","Redistribute on-call across pods",    "priya_n",   "active", "g_q3_eng")
    _c("c_cap_pause_low",   "Pause 3 low-impact projects",         "priya_n",   "proposed", "g_q3_eng")
    _c("c_zara_apps_focus", "App-pod focus on SAML",               "zara",      "active", "g_saml")

    # Manager-skipping-1:1s (personnel recommendation)
    _c("c_zara_juno_1on1",  "Restart 1:1 cadence with Juno",       "zara",      "at_risk")
    _c("c_zara_perla_1on1", "Restart 1:1 cadence with Perla",      "zara",      "at_risk")
    _c("c_zara_team_health","Run team-health survey for App pod",  "zara",      "active")

    # Acme renewal (customer-pressure / slip warning recommendation overlap)
    _c("c_acme_renewal",    "Acme renewal — contract negotiation",  "damon",     "at_risk", "g_renewal", customer="acme_corp")
    _c("c_acme_qbr",        "Acme Q3 QBR",                          "avery_n",   "active",  "g_renewal", customer="acme_corp")
    _c("c_acme_saml_demo",  "Acme SAML demo",                       "ben_n",     "active",  "g_saml",    customer="acme_corp")

    # Pricing v2 (decision-revisit / strategic)
    _c("c_pricing_rfc",     "Pricing v2 RFC",                       "ben_n",     "active",  "g_pricing", decisions=["d_pricing_v1"])
    _c("c_pricing_finmodel","Pricing v2 financial model",           "sasha",     "active",  "g_pricing")
    _c("c_pricing_grandfath","Grandfather migration plan",          "ravi_n",    "active",  "g_pricing")
    _c("c_pricing_legal",   "Pricing v2 legal review",              "rita",      "active",  "g_pricing")

    # Per-customer renewals + expansions (mass)
    enterprise_keys = ["acme_corp", "wayfair", "notion", "drift", "dropbox", "github",
                       "stripe", "databricks", "snowflake", "shopify", "airbnb", "uber"]
    for k in enterprise_keys:
        ae = rng.choice(["damon", "priscilla"])
        csm = rng.choice(["avery_n", "kai_csm", "perla", "juno"])
        _c(f"c_renew_{k}",   f"{k.replace('_',' ').title()} renewal",   ae,   "active", "g_renewal", customer=k)
        _c(f"c_qbr_{k}",     f"{k.replace('_',' ').title()} QBR",       csm,  "active", "g_qbr_cad", customer=k)
        _c(f"c_expand_{k}",  f"{k.replace('_',' ').title()} expansion", ae,   "active", "g_q3_pipeline", customer=k)
    midmarket_keys = [k for k, *_, seg, _, _ in CUSTOMER_SPECS if seg == "mid_market"][:18]
    for k in midmarket_keys:
        ae = rng.choice(["victor_n", "nadia_n", "isaac"])
        csm = rng.choice(["kai_csm", "perla", "juno", "riku"])
        _c(f"c_mm_renew_{k}", f"Renew {k}",       ae,  "active", "g_renewal", customer=k)
        _c(f"c_mm_qbr_{k}",   f"{k} QBR",         csm, "active", "g_qbr_cad", customer=k)
        if rng.random() < 0.4:
            _c(f"c_mm_exp_{k}", f"{k} upsell",    ae,  "active", "g_q3_pipeline", customer=k)
    smb_keys = [k for k, *_, seg, _, _ in CUSTOMER_SPECS if seg == "smb"][:8]
    for k in smb_keys:
        _c(f"c_smb_renew_{k}", f"SMB renewal: {k}", "isaac", "active", "g_renewal", customer=k)
    # Watching/at-risk customers
    for k in ["northstar", "oldworld", "opal", "crystal", "riverstone", "paragon", "emberton"]:
        if k in {c[0] for c in CUSTOMER_SPECS}:
            _c(f"c_atrisk_{k}", f"Recovery plan: {k}", "kai_csm", "at_risk", "g_renewal", customer=k)

    # Engineering bread-and-butter
    eng_pool = [
        ("Tracing rollout phase 2", "kai_n"),
        ("Region-2 failover playbook", "kai_n"),
        ("Audit-log retention policy", "luca"),
        ("Metric-export Prometheus exporter", "luca"),
        ("Webhook reliability scoring", "zara"),
        ("Rate-limit revamp", "zara"),
        ("API error envelope unification", "zara"),
        ("OpenAPI spec generator", "luca"),
        ("Internal dashboard rebuild", "kai_n"),
        ("Mobile parity pass", "zara"),
        ("Pipeline integration tests", "kai_n"),
        ("Onboarding flow polish", "zara"),
        ("CLI rollout", "luca"),
        ("Reports v2 — query layer", "kai_n"),
        ("Reports v2 — UX", "ava"),
        ("Reports v2 — exports", "luca"),
        ("Compliance log retention", "rita"),
        ("Data residency planning", "rita"),
        ("Burn-down dashboard", "ravi_n"),
        ("Service-level dashboard", "kai_n"),
        ("Internal sales tooling", "emil_n"),
        ("Lead routing automation", "emil_n"),
        ("Pipeline health dashboard", "greta"),
        ("Tracing exemplars", "luca"),
        ("Schema migration v3", "kai_n"),
        ("Field-level audit logs", "luca"),
        ("Soc2 evidence collection", "rita"),
        ("Quarterly bug bash", "priya_n"),
        ("EU data residency planning (later)", "rita"),
        ("Mobile push notifications", "zara"),
        ("Onboarding wizard refresh", "ava"),
        ("Feature flag service", "kai_n"),
        ("Admin API expansion", "luca"),
        ("Search ergonomics", "zara"),
        ("Reporting export performance", "kai_n"),
    ]
    for i, (title, owner) in enumerate(eng_pool):
        _c(f"c_eng_{i}", title, owner, rng.choice(["active", "active", "active", "blocked", "at_risk"]),
           rng.choice(["g_q3_eng", "g_postgres", "g_saml", None]))

    # PMs and design
    _c("c_pm_saml_doc", "SAML requirements doc v2",     "ben_n", "active", "g_saml")
    _c("c_pm_pricing_pmf","Validate pricing v2 PMF",    "ben_n", "active", "g_pricing")
    _c("c_pm_audit_doc","Audit-log requirements doc",   "ben_n", "active", "g_saml")
    _c("c_pm_postgres_pmf","Postgres-revisit PMF data", "ben_n", "active", "g_postgres")
    _c("c_pm_q3_roadmap","Q3 roadmap update",           "chen",  "active")
    _c("c_pm_design_partner_sync","Design-partner monthly", "mei", "active", "g_q3_pipeline")
    _c("c_pm_app_metrics","App-pod metrics review",     "mei",   "active", "g_q3_eng")
    _c("c_pm_integrations_pipeline","Integrations pipeline review", "oren", "active", "g_q3_pipeline")
    _c("c_design_saml","SAML/SSO sign-in flow design",  "ava",   "active", "g_saml")
    _c("c_design_audit","Audit-log UI design",          "nik",   "active", "g_saml")
    _c("c_design_brand","Brand refresh exploration",    "ava",   "active", "g_brand")

    # Marketing
    _c("c_mkt_saml_launch","SAML/SSO GA launch content","noor_n","active", "g_saml")
    _c("c_mkt_dreamforce","Dreamforce booth + content", "yuri",  "active", "g_brand")
    _c("c_mkt_brand","Brand position v2",               "hector","active", "g_brand")
    _c("c_mkt_press","Press tour for SAML launch",      "lila",  "active", "g_brand")
    _c("c_mkt_blog","Blog cadence Q3",                  "noor_n","active", "g_brand")

    # GTM ops + finance + people
    _c("c_ops_pipeline","Pipeline forecast model",      "greta", "active", "g_q3_pipeline")
    _c("c_ops_segments","Account segmentation refresh", "emil_n","active", "g_q3_pipeline")
    _c("c_ops_compcal","Comp calendar Q3",              "ravi_n","active")
    _c("c_ops_burn","Burn-vs-runway weekly",            "ravi_n","active", "g_30m")
    _c("c_ops_close","Q3 financial close",              "dora",  "active")
    _c("c_ops_audit","Annual financial audit prep",     "dora",  "active")
    _c("c_ppl_eng_hires","Eng senior hires Q3",         "fern",  "active")
    _c("c_ppl_cs_hires","CS hires Q3",                  "fern",  "active")
    _c("c_ppl_engagement","Engagement survey",          "malou", "active")
    _c("c_ppl_offsite_alt","Skip-level dinners (offsite alt)","malou","active")

    # CSM weekly check-ins for top accounts
    for k in ["acme_corp", "wayfair", "notion", "drift", "dropbox"]:
        _c(f"c_check_{k}", f"Weekly check-in: {k}", "avery_n", "active", "g_renewal", customer=k)

    return out


# =====================================================================
# Signals — programmatic ~400
# =====================================================================


def build_signals(actors, customers, commitments, goals, decisions):
    actor_ids = [a.id for a in actors]
    cust_keys = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    commit_keys = {
        k: did(COMPANY, "commitment", k) for k in (
            "c_saml_lead", "c_saml_audit_logs", "c_saml_review_acme",
            "c_saml_review_wf", "c_saml_review_drift",
            "c_pg_eval", "c_pg_spike",
            "c_cap_audit", "c_cap_pause_low", "c_zara_apps_focus",
            "c_zara_juno_1on1", "c_zara_perla_1on1",
            "c_acme_renewal", "c_acme_qbr",
            "c_pricing_rfc",
        )
    }
    decision_keys = {k: did(COMPANY, "decision", k) for k, *_ in DECISION_SPECS}
    goal_keys = {k: did(COMPANY, "goal", k) for k, *_ in GOAL_SPECS}

    rng = random.Random(13)
    out = []
    idx = 0

    def _add(channel, ref, author_id, ago, text, mentions=None):
        nonlocal idx
        ent = [EntityMention(type=t, id=i) for t, i in (mentions or [])]
        out.append(GeneratedSignal(
            id=did(COMPANY, "signal", f"sig_{idx:04d}"),
            source_channel=channel, source_ref=ref,
            author_id=author_id, occurred_at=days_ago(ago),
            content_text=text,
            entities_mentioned=ent,
        ))
        idx += 1

    # SAML pressure
    _add("slack:message", "C-acme-1", did(COMPANY, "actor", "damon"), 4,
         "Acme is asking about the SAML feature again — 4th time in 2 weeks. Starting to flag as contract risk.",
         [("customer", cust_keys["acme_corp"]), ("commitment", commit_keys["c_saml_lead"])])
    _add("email:message", "msg-wayfair-1", did(COMPANY, "actor", "avery_n"), 12,
         "Wayfair people-ops emailed. SAML required before they expand seats next year.",
         [("customer", cust_keys["wayfair"])])
    _add("slack:message", "C-drift-1", did(COMPANY, "actor", "priscilla"), 28,
         "Drift's CIO just asked when SSO is shipping. They want it in writing.",
         [("customer", cust_keys["drift"]), ("commitment", commit_keys["c_saml_lead"])])
    _add("calendar:event", "evt-acme-saml", did(COMPANY, "actor", "damon"), 3,
         "Acme — SAML/audit-log working session (60 min).",
         [("customer", cust_keys["acme_corp"])])

    # Capacity
    _add("slack:message", "C-eng-cap1", did(COMPANY, "actor", "priya_n"), 5,
         "Eng utilization hit 91% this week. Recommending we pause 3 lower-impact projects.",
         [("commitment", commit_keys["c_cap_audit"])])
    _add("slack:message", "C-eng-cap2", did(COMPANY, "actor", "kai_n"), 8,
         "Platform pod is the choke point. We're at 95% if you only count senior engineers.",
         [])
    _add("slack:message", "C-eng-cap3", did(COMPANY, "actor", "zara"), 11,
         "App pod can absorb a bit more, but we're committing to the SAML push so trade-off is real.",
         [("commitment", commit_keys["c_zara_apps_focus"])])

    # Postgres decision revisit
    _add("slack:message", "C-arch-1", did(COMPANY, "actor", "kai_n"), 14,
         "Three customers above $500K have asked about read-latency. The Postgres-only call from 14 mo ago needs a revisit.",
         [("decision", decision_keys["d_postgres_only"]), ("goal", goal_keys["g_postgres"])])
    _add("email:message", "msg-snowflake-1", did(COMPANY, "actor", "damon"), 21,
         "Snowflake is asking about replica lag. They want sub-100ms reads cross-region.",
         [("customer", cust_keys["snowflake"])])
    _add("github:event", "issue-arch-89", did(COMPANY, "actor", "eng3"), 18,
         "Arch RFC #89: 'Read replica strategy'. Discussion is converging on a 2-tier approach.",
         [])

    # Manager 1:1 gap
    _add("calendar:event", "evt-zara-juno-canc", did(COMPANY, "actor", "zara"), 33,
         "1:1 with Juno — cancelled (third time in 6 weeks).",
         [("actor", did(COMPANY, "actor", "juno")), ("commitment", commit_keys["c_zara_juno_1on1"])])
    _add("calendar:event", "evt-zara-perla-canc", did(COMPANY, "actor", "zara"), 27,
         "1:1 with Perla — cancelled.",
         [("actor", did(COMPANY, "actor", "perla")), ("commitment", commit_keys["c_zara_perla_1on1"])])
    _add("slack:message", "C-people-1", did(COMPANY, "actor", "malou"), 6,
         "Engagement survey showed App pod morale is dipping. Two members specifically called out missing 1:1s.",
         [])

    # Acme slip warning (mid-priority slip risk)
    _add("slack:message", "C-acme-slip", did(COMPANY, "actor", "damon"), 7,
         "Acme renewal looking iffy. They flagged audit-log timing as a contract gating item.",
         [("customer", cust_keys["acme_corp"]), ("commitment", commit_keys["c_acme_renewal"])])
    _add("calendar:event", "evt-acme-renewal", did(COMPANY, "actor", "damon"), 6,
         "Acme — renewal call (procurement)",
         [("customer", cust_keys["acme_corp"])])

    # Pipeline composition (strategic recommendation)
    _add("slack:message", "C-strategy-1", did(COMPANY, "actor", "morgan_n"), 9,
         "Looking at Q3 pipeline composition: 65% mid-market, 25% enterprise, 10% SMB. Where's the enterprise depth?",
         [("goal", goal_keys["g_q3_pipeline"])])
    _add("slack:message", "C-strategy-2", did(COMPANY, "actor", "monica_n"), 13,
         "Enterprise pipeline is thin top-of-funnel. Need ABM motion or analyst lift.",
         [])

    # Drift / smaller-account drift signal
    _add("stripe:event", "ch_oldworld_1", did(COMPANY, "actor", "ravi_n"), 30,
         "OldWorld Foods invoice failed. Third time this year.",
         [("customer", cust_keys["oldworld"])])
    _add("slack:message", "C-cs-1", did(COMPANY, "actor", "kai_csm"), 19,
         "OldWorld stopped showing up to QBRs. They might churn next quarter.",
         [("customer", cust_keys["oldworld"])])

    # Dense recent activity
    pool = [
        ("slack:message", lambda: rng.choice(actor_ids), [
            "Pushed perf fix for reports-v2 endpoint, saved 14% on p95",
            "Reviewed PR for audit-log retention, shipped",
            "On-call quiet last 24h",
            "Filed bug: integration with Workday SCIM throws on null fields",
            "v2.1 release branch cut, CI green",
            "Tracing exemplars working in staging",
            "Region-2 failover dry-run today, no prod impact",
            "Latency outliers concentrated in three accounts; investigating",
            "Migration tool spike landed, looks usable",
            "Onboarding wizard polish in progress",
            "SOC2 control-evidence collection underway",
            "Ramp expansion call moved to next week",
            "Field marketing event went well, 12 mid-market leads",
            "Feature flag service spec ready",
        ]),
        ("github:event", lambda: rng.choice(actor_ids), [
            "PR opened: 'add SAML middleware skeleton'",
            "PR merged: 'fix race in webhook delivery'",
            "Issue closed: 'integration with Workday SCIM null handling'",
            "PR opened: 'audit-log retention policy'",
            "PR merged: 'reports-v2 query optimizer'",
            "Issue opened: 'Postgres replica lag spikes'",
            "PR opened: 'SOC2 evidence collection scaffolding'",
            "PR merged: 'rate-limit revamp'",
        ]),
        ("slack:message", lambda: rng.choice([
            did(COMPANY, "actor", a) for a in ["damon", "priscilla", "victor_n", "nadia_n", "isaac"]
        ]), [
            "Wayfair expansion call — going great. 50% upside on seats.",
            "Notion Q4 expansion conversation — they want SCIM by then.",
            "DoorDash demo — good, they liked the audit-log roadmap.",
            "Lost the GitLab bake-off. Their existing tooling stack matters too much.",
            "Stripe expansion — they want EU data residency.",
            "Twilio renewal — easy, 12% expansion.",
            "Inbound from Etsy people-ops, qualifying.",
            "Lyft renewal moved to next quarter at their request.",
            "Klaviyo asking about Workday integration timing.",
        ]),
        ("slack:message", lambda: rng.choice([
            did(COMPANY, "actor", a) for a in ["avery_n", "kai_csm", "juno", "perla", "riku"]
        ]), [
            "Acme weekly: smooth on the surface, watching audit-log thread.",
            "Wayfair QBR went well, scoring up.",
            "Drift quarterly review — happy.",
            "Dropbox expansion next month, big move.",
            "Notion check-in, they're scaling people-ops fast.",
            "Pendo: small scope creep, nothing concerning.",
            "Ramp QBR positive.",
            "Brex onboarding for new module starting.",
            "Klaviyo migration support continues.",
        ]),
        ("calendar:event", lambda: rng.choice(actor_ids), [
            "All-hands",
            "Eng leads sync",
            "GTM forecast review",
            "Product weekly",
            "Investor update prep",
            "Pricing v2 working session",
            "QBR — Acme",
            "QBR — Wayfair",
            "Recruiter sync",
            "Comp planning Q4",
        ]),
        ("stripe:event", lambda: rng.choice([did(COMPANY, "actor", "ravi_n"), did(COMPANY, "actor", "dora")]), [
            "Invoice paid: enterprise tier ($90K)",
            "Subscription updated: Brex (added seats)",
            "Subscription canceled: small SMB account",
            "Payment failed: SMB account (retry scheduled)",
        ]),
        ("email:message", lambda: rng.choice(actor_ids), [
            "Re: Q3 contract — redlines attached",
            "Investor update — Q1 numbers",
            "Re: SAML escalation",
            "Re: Audit log timing",
            "Coffee chat — analyst",
            "Re: pricing Q",
        ]),
    ]

    while idx < 320:
        channel, author_fn, options = rng.choice(pool)
        ago = rng.uniform(0.1, 48.0)
        text = rng.choice(options)
        mentions = []
        if rng.random() < 0.35:
            ck = rng.choice(list(cust_keys.keys())[:25])
            mentions.append(("customer", cust_keys[ck]))
        _add(channel, f"auto-{idx:04d}", author_fn(), ago, text, mentions)

    # Older sparse history
    while idx < 400:
        channel = rng.choice(["slack:message", "github:event", "calendar:event"])
        ago = rng.uniform(60, 360)
        text = rng.choice([
            "Q4 board meeting prep notes.",
            "Original SAML scoping doc, deferred.",
            "Architecture review last fall.",
            "Old roadmap update from Q4.",
            "Earlier conversation about pricing v1.",
            "Series B closing round notes.",
            "Audit-log feature scoping (deferred).",
        ])
        _add(channel, f"hist-{idx:04d}", rng.choice(actor_ids), ago, text)

    return out


# =====================================================================
# Recommendations — 7 mapped to spec
# =====================================================================


def _M(key: str) -> str:
    return did(COMPANY, "model", key)


def build_models(actors, customers, commitments, goals, decisions, signals):
    """Author Northwind's epistemic substrate. ~50 models across all kinds."""
    cust = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    actor = {a.id for a in actors}
    com = {k: did(COMPANY, "commitment", k) for k in (
        "c_saml_lead", "c_acme_renewal", "c_zara_juno_1on1",
        "c_pricing_rfc", "c_pg_eval", "c_cap_pause_low",
        "c_atrisk_oldworld",
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
            falsifier = {"condition": "supporting_signal_density_drops_below_threshold",
                          "observable_via": "signal_query"}
        out.append(GeneratedModel(
            id=_M(key), kind=kind, natural=natural,                   # type: ignore[arg-type]
            proposition=proposition_extra or {},
            confidence=confidence,
            scope_actor_ids=list(scope_actors or []),
            scope_entities=list(scope_entities or []),
            falsifier=falsifier,
            supporting_observation_ids=list(support_signals or []),
            supporting_model_ids=list(support_models or []),
            evaluate_at=evaluate_at,
        ))

    # state (15)
    _add("st_eng_util_91", "state",
         "Engineering operating at 91% utilization heading into Q3 push.",
         confidence=0.82,
         scope_entities=[{"type": "goal", "id": g["g_q3_eng"]}],
         falsifier={"condition": "utilization < 80% for 2 weeks",
                    "observable_via": "capacity_audit"},
         support_signals=F("utilization hit 91%", "Platform pod is the choke point"))
    _add("st_acme_at_risk", "state",
         "Acme Corp renewal at risk — flagged audit-log timing as gating contract item.",
         confidence=0.81,
         scope_entities=[{"type": "customer", "id": cust["acme_corp"]},
                         {"type": "commitment", "id": com["c_acme_renewal"]}],
         falsifier={"condition": "Acme signs a 12-month renewal in next 60 days",
                    "observable_via": "salesforce"},
         support_signals=F("Acme is asking", "Acme renewal looking iffy"))
    _add("st_saml_3_asks", "state",
         "3 enterprise customers (Acme, Wayfair, Drift) requested SAML in past 60 days; $410K ARR exposed.",
         confidence=0.86,
         scope_entities=[{"type": "customer", "id": cust["acme_corp"]},
                         {"type": "customer", "id": cust["wayfair"]},
                         {"type": "customer", "id": cust["drift"]}],
         falsifier={"condition": "fewer than 1 enterprise customer asks in 30 days",
                    "observable_via": "signals"},
         support_signals=F("SAML feature again", "SAML required", "SSO is shipping"))
    _add("st_zara_management_gap", "state",
         "EM Zara has missed 1:1s with 2 direct reports (Juno, Perla) for 6+ weeks; team morale dipping.",
         confidence=0.83,
         scope_actors=[did(COMPANY, "actor", "zara")],
         scope_entities=[{"type": "actor", "id": did(COMPANY, "actor", "zara")},
                         {"type": "commitment", "id": com["c_zara_juno_1on1"]}],
         falsifier={"condition": "1:1s held with both direct reports for 4 consecutive weeks",
                    "observable_via": "calendar"},
         support_signals=F("1:1 with Juno", "App pod morale"))
    _add("st_postgres_mismatch", "state",
         "Postgres-only architecture decision (14mo old) — conditions changed; 3 customers >$500K asking about read latency.",
         confidence=0.78,
         scope_entities=[{"type": "decision", "id": d["d_postgres_only"]},
                         {"type": "customer", "id": cust["dropbox"]},
                         {"type": "customer", "id": cust["snowflake"]}],
         falsifier={"condition": "0 customers ask about read latency over 60 days",
                    "observable_via": "signals"},
         support_signals=F("Postgres-only", "read-latency", "replica lag"))
    _add("st_pipeline_thin", "state",
         "Q3 enterprise pipeline thin: 12 named, 4 qualified — need 20+ qualified to hit number.",
         confidence=0.72,
         scope_entities=[{"type": "goal", "id": g["g_q3_pipeline"]}],
         falsifier={"condition": "qualified enterprise pipeline ≥ 20 in next 30 days",
                    "observable_via": "salesforce"},
         support_signals=F("ENT pipeline", "Enterprise pipeline is thin"))
    _add("st_oldworld_drift", "state",
         "OldWorld Foods drifting on health — 3 invoice failures, no QBR attendance for 60 days.",
         confidence=0.76,
         scope_entities=[{"type": "customer", "id": cust["oldworld"]}],
         falsifier={"condition": "OldWorld attends QBR AND invoices succeed for 60 days",
                    "observable_via": "stripe+calendar"},
         support_signals=F("OldWorld"))
    _add("st_nrr_115", "state",
         "Net revenue retention at 115%; on plan but tight margin to 120% target.",
         confidence=0.74,
         scope_entities=[{"type": "goal", "id": g["g_renewal"]}],
         falsifier={"condition": "NRR drops below 110% in 60 days",
                    "observable_via": "finance"})
    _add("st_qbr_inconsistent", "state",
         "QBR cadence inconsistent across enterprise; 2 accounts gone 90 days without QBR.",
         confidence=0.69,
         scope_entities=[{"type": "goal", "id": g["g_qbr_cad"]}])
    _add("st_growth_80_yoy", "state",
         "ARR growing 80% YoY ($14M → trajectory $25M).",
         confidence=0.84,
         falsifier={"condition": "YoY growth drops below 60% for 2 quarters",
                    "observable_via": "finance"})
    _add("st_pricing_v2_in_review", "state",
         "Pricing v2 RFC half-written; financial model in flight; legal yet to engage.",
         confidence=0.66,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v1"]}])
    _add("st_eu_demand_growing", "state",
         "EU residency demand from Stripe + others growing; current decision blocks.",
         confidence=0.62,
         scope_entities=[{"type": "decision", "id": d["d_us_first"]}])
    _add("st_app_pod_capacity_ok", "state",
         "App pod can absorb additional load; Platform pod is choke point.",
         confidence=0.71,
         scope_entities=[{"type": "goal", "id": g["g_q3_eng"]}],
         falsifier={"condition": "App pod utilization exceeds 90% for 2 weeks",
                    "observable_via": "capacity_audit"})
    _add("st_brand_modern_vs_legacy", "state",
         "Brand position 'modern HR vs legacy' is resonating in analyst grid.",
         confidence=0.58,
         scope_entities=[{"type": "decision", "id": d["d_brand_modern"]}])
    _add("st_workday_competition", "state",
         "Workday losing share in mid-market; Northwind's primary competitive frame.",
         confidence=0.61)

    # relation (8)
    _add("rel_saml_to_renewal", "relation",
         "SAML asks predict enterprise contract risk: 3+ asks → 70% likely contract gating.",
         confidence=0.69,
         proposition_extra={"asks_to_gating_rate": 0.7})
    _add("rel_postgres_to_arr", "relation",
         "Postgres latency complaints come exclusively from accounts >$500K ARR.",
         confidence=0.74,
         falsifier={"condition": "complaint received from <$500K account",
                    "observable_via": "signals"},
         scope_entities=[{"type": "decision", "id": d["d_postgres_only"]}])
    _add("rel_1on1s_to_morale", "relation",
         "Manager 1:1 attendance correlates with reportee engagement scores (r ≈ 0.6).",
         confidence=0.62,
         proposition_extra={"correlation": 0.6})
    _add("rel_qbr_to_renewal", "relation",
         "QBR attendance correlates with renewal probability — accounts with 0 QBRs in 90 days have 40% lower renewal rate.",
         confidence=0.71,
         falsifier={"condition": "0-QBR accounts renew at parity rate for 2 quarters",
                    "observable_via": "salesforce"},
         proposition_extra={"renewal_rate_diff": -0.4})
    _add("rel_invoice_to_churn", "relation",
         "Invoice failures (3+) precede churn within 90 days for 65% of accounts.",
         confidence=0.66)
    _add("rel_pipeline_comp_to_revenue", "relation",
         "Pipeline composition skewed mid-market correlates with lower revenue per opportunity (RPO).",
         confidence=0.63,
         scope_entities=[{"type": "goal", "id": g["g_q3_pipeline"]}])
    _add("rel_eng_capacity_to_slip", "relation",
         "Engineering >90% capacity correlates with downstream commitment slip rate +20%.",
         confidence=0.68)
    _add("rel_pricing_v2_to_nrr", "relation",
         "Pricing v2 (per-employee) projected to lift NRR by 5-8 points based on customer profiles.",
         confidence=0.55)

    # prediction (6)
    _add("pred_acme_renewal", "prediction",
         "Acme will renew if SAML GA date is committed in writing within 30 days; otherwise contract slips 60+ days.",
         confidence=0.59, evaluate_at=days_from_now(30),
         scope_entities=[{"type": "customer", "id": cust["acme_corp"]}])
    _add("pred_saml_q2", "prediction",
         "SAML will ship by end of Q2 if 2 of 3 design partners give written specs by next week.",
         confidence=0.62, evaluate_at=days_from_now(60))
    _add("pred_oldworld_churn", "prediction",
         "OldWorld will churn by Q4 unless touched by VP CS this month.",
         confidence=0.61, evaluate_at=days_from_now(90),
         scope_entities=[{"type": "customer", "id": cust["oldworld"]}])
    _add("pred_q3_pipeline_miss", "prediction",
         "Q3 enterprise pipeline number will miss by 25% unless ABM motion stands up in next 30 days.",
         confidence=0.54, evaluate_at=days_from_now(45))
    _add("pred_zara_attrition", "prediction",
         "One of Zara's direct reports will give notice within 90 days if 1:1s don't restart.",
         confidence=0.41, evaluate_at=days_from_now(90),
         scope_actors=[did(COMPANY, "actor", "zara"), did(COMPANY, "actor", "juno")])
    _add("pred_30m_arr_27", "prediction",
         "Northwind will hit $30M ARR by end of 2027 if NRR holds at 115%+ and 4 enterprise net-news land.",
         confidence=0.51, evaluate_at=days_from_now(540))

    # pattern (3) + pattern_instance (3)
    _add("pat_enterprise_saml_table_stakes", "pattern",
         "Enterprise customers above $300K ARR consistently flag SAML/audit logs as gating contract items.",
         confidence=0.74,
         falsifier={"condition": "next 5 enterprise customers don't ask",
                    "observable_via": "signals"})
    _add("pat_em_skipped_1on1s", "pattern",
         "EMs who skip 1:1s for 4+ weeks see direct-report attrition risk increase by 35%.",
         confidence=0.61)
    _add("pat_postgres_over_500k", "pattern",
         "Customers above $500K ARR report read-latency issues at 3x the rate of smaller accounts.",
         confidence=0.66,
         falsifier={"condition": "rate equalizes for 2 consecutive quarters",
                    "observable_via": "support_tickets"})
    _add("pat_inst_acme_saml", "pattern_instance",
         "Acme instance of the enterprise-SAML pattern: 4 asks in 30 days, audit-log ask escalating.",
         confidence=0.79,
         scope_entities=[{"type": "customer", "id": cust["acme_corp"]}],
         support_models=[_M("pat_enterprise_saml_table_stakes")])
    _add("pat_inst_zara_skipped", "pattern_instance",
         "Zara instance of the EM-skipped-1:1s pattern: 6 weeks no 1:1s with Juno + Perla.",
         confidence=0.81,
         scope_actors=[did(COMPANY, "actor", "zara")],
         support_models=[_M("pat_em_skipped_1on1s")])
    _add("pat_inst_dropbox_pg", "pattern_instance",
         "Dropbox instance of the Postgres-over-500K pattern.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["dropbox"]}],
         support_models=[_M("pat_postgres_over_500k")])

    # capability_assessment (4)
    _add("cap_eng_strong", "capability_assessment",
         "Engineering capability is strong relative to mid-stage HR-platform peers; SAML gap is execution not architecture.",
         confidence=0.65)
    _add("cap_cs_pod_works", "capability_assessment",
         "Pod-based CS structure is delivering above-benchmark NPS (84 vs 71 industry).",
         confidence=0.72,
         falsifier={"condition": "NPS drops below 75 for 2 quarters",
                    "observable_via": "nps_survey"},
         scope_entities=[{"type": "decision", "id": d["d_cs_pod_model"]}])
    _add("cap_marketing_thin_top_funnel", "capability_assessment",
         "Marketing is below par on top-of-funnel demand gen; analyst relations is the gap.",
         confidence=0.61)
    _add("cap_finance_visibility", "capability_assessment",
         "Finance has best-in-class visibility on burn and runway; weekly tracker is reference-grade.",
         confidence=0.69)

    # hypothesis (4)
    _add("hyp_zara_burnout_root", "hypothesis",
         "Zara's missed 1:1s may be a burnout symptom, not a calendar issue.",
         confidence=0.42,
         scope_actors=[did(COMPANY, "actor", "zara")])
    _add("hyp_pricing_v2_v_renewal", "hypothesis",
         "Pricing v2 may unintentionally trigger churn in legacy per-seat customers if grandfathering is fuzzy.",
         confidence=0.46)
    _add("hyp_postgres_bandage", "hypothesis",
         "Read replicas alone won't satisfy enterprise customers; we may need real multi-tier data tier.",
         confidence=0.49)
    _add("hyp_eu_inevitable", "hypothesis",
         "EU presence may be inevitable in 2026 (not 2027) given enterprise pipeline composition.",
         confidence=0.43)

    # concern (4)
    _add("conc_acme_loss", "concern",
         "Risk of losing Acme contract — visible in Acme's procurement signals.",
         confidence=0.52,
         scope_entities=[{"type": "customer", "id": cust["acme_corp"]}])
    _add("conc_zara_attrition", "concern",
         "Risk of attrition cascade in App pod if Zara's situation isn't addressed.",
         confidence=0.48,
         scope_actors=[did(COMPANY, "actor", "zara")])
    _add("conc_q3_miss", "concern",
         "Risk of missing Q3 enterprise number by 25-30%.",
         confidence=0.43)
    _add("conc_nrr_slip", "concern",
         "Risk of NRR slipping below 110% if mid-market churns rise.",
         confidence=0.39)

    # market_assessment (3)
    _add("mkt_workday_decline_mm", "market_assessment",
         "Workday is losing mid-market share to modern alternatives at ~12% YoY.",
         confidence=0.62,
         falsifier={"condition": "Workday wins ≥ 3 mid-market RFPs we participate in over 2 quarters",
                    "observable_via": "win_loss"})
    _add("mkt_modern_hr_crowded", "market_assessment",
         "Modern HR platform space crowded but defensible if integrations + UX hold.",
         confidence=0.58)
    _add("mkt_compliance_normalizing", "market_assessment",
         "Compliance asks (SAML, SOC2, ISO 27001) showing up earlier in buyer journey vs 12mo ago.",
         confidence=0.74,
         falsifier={"condition": "<30% of buyers ask in first 60 days",
                    "observable_via": "buyer_research"})

    # environmental_trend (3)
    _add("env_hr_legacy_migration", "environmental_trend",
         "Legacy HRIS migration cycle is in year 2 of 4-year arc; still substantial market.",
         confidence=0.66)
    _add("env_remote_first_norm", "environmental_trend",
         "Remote-first companies disproportionately adopt modern HR tooling.",
         confidence=0.69)
    _add("env_pe_consolidation", "environmental_trend",
         "PE-backed legacy HR vendors are consolidating; creates window for new players.",
         confidence=0.61)

    # =====================================================================
    # Expansion set — doubles the substrate so demo retrieval has more
    # granular surfaces to land on. Authored against the same actors,
    # customers, goals, and decisions to keep the graph dense.
    # =====================================================================

    A = lambda k: did(COMPANY, "actor", k)

    # ---- state (extra 14) ----
    _add("st_wayfair_engaged_qbr", "state",
         "Wayfair (largest enterprise account, $1.2M ARR) is highly engaged in QBRs and exploring expansion.",
         confidence=0.78,
         scope_entities=[{"type": "customer", "id": cust["wayfair"]}],
         falsifier={"condition": "Wayfair skips next QBR or freezes expansion conversation",
                    "observable_via": "calendar+csm"})
    _add("st_drift_satisfied_quiet", "state",
         "Drift is satisfied but extremely quiet — no QBRs in 90 days; renewal in 4 months.",
         confidence=0.62,
         scope_entities=[{"type": "customer", "id": cust["drift"]}])
    _add("st_notion_advocate", "state",
         "Notion is acting as an active reference (3 referrals in past quarter); brand asset.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["notion"]}],
         falsifier={"condition": "Notion declines to act as reference for 60 days",
                    "observable_via": "marketing"})
    _add("st_databricks_evaluating_security", "state",
         "Databricks is evaluating us alongside competitor; security questionnaire took 2 weeks.",
         confidence=0.69,
         scope_entities=[{"type": "customer", "id": cust["databricks"]}])
    _add("st_uber_at_risk_silent", "state",
         "Uber has gone silent for 6 weeks after consistent monthly comms — concerning for a $635K account.",
         confidence=0.66,
         scope_entities=[{"type": "customer", "id": cust["uber"]}])
    _add("st_paragon_health_drifting", "state",
         "Paragon Health drifting into at-risk — 2 invoice failures and missed onboarding milestones.",
         confidence=0.71,
         scope_entities=[{"type": "customer", "id": cust["paragon"]}],
         falsifier={"condition": "Paragon resumes scheduled onboarding AND invoices succeed for 60 days",
                    "observable_via": "csm+stripe"})
    _add("st_smb_overhead_imbalanced", "state",
         "SMB tier consumes ~25% of CS bandwidth for ~9% of ARR.",
         confidence=0.74,
         falsifier={"condition": "SMB overhead drops below 15% for 90 days",
                    "observable_via": "csm_telemetry"})
    _add("st_app_pod_morale_dipping", "state",
         "App pod morale dipping — 4 of 9 engineers reporting low engagement this cycle.",
         confidence=0.68,
         scope_actors=[A("zara")])
    _add("st_eu_demand_concentrated", "state",
         "EU residency demand concentrated in 4 enterprise prospects (Stripe, Klarna, Adyen, Spotify).",
         confidence=0.72,
         scope_entities=[{"type": "decision", "id": d["d_us_first"]}],
         falsifier={"condition": "EU demand drops to <2 enterprise prospects in 90 days",
                    "observable_via": "salesforce"})
    _add("st_audit_logs_60pct_done", "state",
         "Audit-logs feature is ~60% complete; no design partner yet validating spec.",
         confidence=0.66,
         scope_entities=[{"type": "goal", "id": g["g_saml"]}])
    _add("st_q3_offsite_morale_payoff", "state",
         "Skipping Q3 offsite saved ~$80K but engagement survey shows -6 pts vs prior cycle.",
         confidence=0.61,
         scope_entities=[{"type": "decision", "id": d["d_q3_offsite_skip"]}])
    _add("st_marketing_pipeline_lagging", "state",
         "Marketing-sourced pipeline at 35% of plan; analyst-relations work is delivering most of the lift.",
         confidence=0.70,
         scope_actors=[A("hector")],
         falsifier={"condition": "marketing-sourced pipeline ≥ 60% of plan for 2 cycles",
                    "observable_via": "salesforce"})
    _add("st_legal_msa_backlog", "state",
         "Legal MSA backlog: 4 enterprise contracts waiting on redline turn-around.",
         confidence=0.79,
         falsifier={"condition": "MSA backlog clears within 30 days",
                    "observable_via": "legal_tracker"})
    _add("st_eng_hiring_steady", "state",
         "Engineering hiring funnel is steady — 6 in pipeline, 2 close in next 30 days.",
         confidence=0.63)

    # ---- relation (extra 6) ----
    _add("rel_security_q_to_close", "relation",
         "Customers spending >2 weeks on security questionnaires close at 1.7x the rate of fast questionnaires.",
         confidence=0.61,
         proposition_extra={"close_uplift_factor": 1.7})
    _add("rel_advocate_to_pipeline", "relation",
         "Active reference customers correlate with ~3 outbound-qualified opportunities per quarter each.",
         confidence=0.66)
    _add("rel_msa_speed_to_close_velocity", "relation",
         "Days-in-MSA-redline inversely correlates with deal velocity (every extra week adds ~5 days to total cycle).",
         confidence=0.72,
         falsifier={"condition": "median deal cycle decouples from MSA cycle for 2 cycles",
                    "observable_via": "salesforce"})
    _add("rel_offsite_to_engagement", "relation",
         "Skipping offsites correlates with ~5-pt drop in next-cycle engagement scores.",
         confidence=0.55,
         scope_entities=[{"type": "decision", "id": d["d_q3_offsite_skip"]}])
    _add("rel_pricing_v2_to_smb_churn", "relation",
         "Per-employee pricing change correlates with elevated churn risk in SMB tier (legacy small accounts).",
         confidence=0.59,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v1"]}])
    _add("rel_pod_size_to_nps", "relation",
         "CSM accounts-per-pod sweet spot is 8-12; above 12 NPS drops 6 pts.",
         confidence=0.71,
         scope_entities=[{"type": "decision", "id": d["d_cs_pod_model"]}],
         falsifier={"condition": "NPS holds steady when pod sizes >12 for 2 cycles",
                    "observable_via": "nps_survey"})

    # ---- prediction (extra 6) ----
    _add("pred_databricks_close", "prediction",
         "Databricks will sign within 60 days if security review closes and SAML date is committed.",
         confidence=0.55, evaluate_at=days_from_now(60),
         scope_entities=[{"type": "customer", "id": cust["databricks"]}])
    _add("pred_uber_renewal_dip", "prediction",
         "Uber will renew but at flat dollars unless QBR cadence resumes — silence is reading as low-engagement.",
         confidence=0.51, evaluate_at=days_from_now(120),
         scope_entities=[{"type": "customer", "id": cust["uber"]}])
    _add("pred_paragon_churn", "prediction",
         "Paragon Health will churn within 90 days unless Avery picks up the relationship personally.",
         confidence=0.58, evaluate_at=days_from_now(90),
         scope_entities=[{"type": "customer", "id": cust["paragon"]}])
    _add("pred_eu_decision_pulled_forward", "prediction",
         "EU expansion decision will be pulled forward into 2026 by mid-Q3 due to enterprise pipeline composition.",
         confidence=0.49, evaluate_at=days_from_now(120),
         scope_entities=[{"type": "decision", "id": d["d_us_first"]}])
    _add("pred_pricing_v2_nrr_lift", "prediction",
         "Pricing v2 will deliver +5-8 NRR points within 6 months of GA.",
         confidence=0.47, evaluate_at=days_from_now(210),
         scope_entities=[{"type": "decision", "id": d["d_pricing_v1"]}])
    _add("pred_q3_pipeline_shortfall", "prediction",
         "Q3 enterprise pipeline finishes at 70-75% of plan unless ABM lift materialises in 30 days.",
         confidence=0.55, evaluate_at=days_from_now(60),
         scope_entities=[{"type": "goal", "id": g["g_q3_pipeline"]}])

    # ---- pattern (extra 4) + pattern_instance (extra 6) ----
    _add("pat_silent_enterprise", "pattern",
         "Enterprise accounts going silent for 6+ weeks renew at 12 points lower NRR than active accounts.",
         confidence=0.66,
         proposition_extra={"silence_threshold_weeks": 6, "nrr_diff_pts": 12})
    _add("pat_msa_backlog_to_quarterly_miss", "pattern",
         "Legal MSA backlogs of 4+ deals correlate with quarter-end pipeline misses 80% of the time.",
         confidence=0.71,
         proposition_extra={"backlog_threshold": 4})
    _add("pat_smb_drift_to_churn", "pattern",
         "SMB accounts that miss 2+ QBRs and have 1+ invoice failure churn 75% of the time.",
         confidence=0.67,
         proposition_extra={"qbr_misses": 2, "invoice_fail": 1, "churn_rate": 0.75})
    _add("pat_engagement_dip_after_skip", "pattern",
         "Skipping a planned company offsite produces a 4-7 pt engagement-score dip in the next cycle.",
         confidence=0.61)

    _add("pat_inst_uber_silence", "pattern_instance",
         "Uber instance of the silent-enterprise pattern: 6 weeks no comms.",
         confidence=0.71,
         scope_entities=[{"type": "customer", "id": cust["uber"]}],
         support_models=[_M("pat_silent_enterprise")])
    _add("pat_inst_drift_silence", "pattern_instance",
         "Drift instance of the silent-enterprise pattern: 90 days no QBR.",
         confidence=0.69,
         scope_entities=[{"type": "customer", "id": cust["drift"]}],
         support_models=[_M("pat_silent_enterprise")])
    _add("pat_inst_msa_backlog", "pattern_instance",
         "Northwind instance of the MSA-backlog-to-miss pattern: 4 deals in legal queue.",
         confidence=0.74,
         support_models=[_M("pat_msa_backlog_to_quarterly_miss")])
    _add("pat_inst_paragon_drift", "pattern_instance",
         "Paragon Health instance of the SMB-drift-to-churn pattern.",
         confidence=0.66,
         scope_entities=[{"type": "customer", "id": cust["paragon"]}],
         support_models=[_M("pat_smb_drift_to_churn")])
    _add("pat_inst_oldworld_drift", "pattern_instance",
         "OldWorld Foods instance of the SMB-drift-to-churn pattern (already at_risk).",
         confidence=0.78,
         scope_entities=[{"type": "customer", "id": cust["oldworld"]}],
         support_models=[_M("pat_smb_drift_to_churn")])
    _add("pat_inst_offsite_engagement", "pattern_instance",
         "Northwind instance of the engagement-dip-after-skip pattern: -6 pts after Q3 offsite skip.",
         confidence=0.71,
         scope_entities=[{"type": "decision", "id": d["d_q3_offsite_skip"]}],
         support_models=[_M("pat_engagement_dip_after_skip")])

    # ---- capability_assessment (extra 4) ----
    _add("cap_legal_external_unscaled", "capability_assessment",
         "Legal capability is below mid-stage benchmark — no in-house GC; outside counsel turn-around dominates close cycles.",
         confidence=0.71,
         falsifier={"condition": "in-house GC hired or MSA cycles drop below 14 days median",
                    "observable_via": "ats+contracts"})
    _add("cap_finance_visibility_strong", "capability_assessment",
         "Finance capability is best-in-class for stage; weekly burn/runway tracker referenced by board peers.",
         confidence=0.66)
    _add("cap_analyst_relations_emerging", "capability_assessment",
         "Analyst-relations capability is emerging — Hector closed 2 analyst briefings this quarter from cold.",
         confidence=0.59,
         scope_actors=[A("hector")])
    _add("cap_product_velocity_steady", "capability_assessment",
         "Product velocity steady — 7-week PRD-to-GA median is on benchmark for category.",
         confidence=0.62)

    # ---- hypothesis (extra 5) ----
    _add("hyp_workday_decline_will_continue", "hypothesis",
         "Workday's mid-market decline likely continues for another 3-4 quarters before flattening.",
         confidence=0.42)
    _add("hyp_audit_logs_v1_thin", "hypothesis",
         "Audit-logs v1 may ship without partner validation and require a v1.1 within 60 days of GA.",
         confidence=0.46,
         scope_entities=[{"type": "goal", "id": g["g_saml"]}])
    _add("hyp_pe_acquihire_window", "hypothesis",
         "PE consolidation may create a 6-month window where 1-2 of our peers are acquired.",
         confidence=0.39)
    _add("hyp_pricing_v2_grandfather_messy", "hypothesis",
         "Pricing v2 grandfathering may create CSM confusion that costs us 1-2 mid-market renewals.",
         confidence=0.43)
    _add("hyp_smb_should_self_serve", "hypothesis",
         "SMB tier may benefit more from a self-serve flow than human CSM coverage.",
         confidence=0.45)

    # ---- concern (extra 6) ----
    _add("conc_databricks_loss", "concern",
         "Risk of losing Databricks if security review reveals an audit-log gap.",
         confidence=0.45,
         scope_entities=[{"type": "customer", "id": cust["databricks"]}])
    _add("conc_uber_silent_loss", "concern",
         "Risk of Uber treating silence as exit prep — large single-account ARR exposure.",
         confidence=0.41,
         scope_entities=[{"type": "customer", "id": cust["uber"]}])
    _add("conc_msa_pipeline_ripple", "concern",
         "Risk that legal MSA backlog cascades into Q3 pipeline miss and resets CRO targets.",
         confidence=0.55)
    _add("conc_smb_distraction_from_enterprise", "concern",
         "Risk that SMB support load distracts CS from enterprise QBR cadence.",
         confidence=0.48)
    _add("conc_paragon_smb_pattern", "concern",
         "Risk that the Paragon situation is the start of a broader at-risk SMB cohort.",
         confidence=0.39,
         scope_entities=[{"type": "customer", "id": cust["paragon"]}])
    _add("conc_pricing_v2_partner_revolt", "concern",
         "Risk that pricing v2 triggers a vocal-minority partner revolt that bleeds into analyst commentary.",
         confidence=0.36,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v1"]}])

    # ---- market_assessment (extra 4) ----
    _add("mkt_workday_winning_top_tier", "market_assessment",
         "Workday still winning Fortune 100 deals; mid-market decline is steeper than top-tier.",
         confidence=0.64)
    _add("mkt_modern_hr_brand_crowded", "market_assessment",
         "'Modern HR' brand position is becoming crowded — Lattice, Rippling, Gusto all using similar framing.",
         confidence=0.60,
         falsifier={"condition": "buyer interviews stop mentioning 'modern HR' as differentiator",
                    "observable_via": "buyer_research"})
    _add("mkt_compliance_arms_race", "market_assessment",
         "Compliance is becoming an arms race — SOC2 II, ISO 27001, FedRAMP all expected within 18 months.",
         confidence=0.69)
    _add("mkt_buyer_committee_growing", "market_assessment",
         "Enterprise HR buyer committees averaging 4-5 stakeholders, up from 2-3 a year ago.",
         confidence=0.63)

    # ---- environmental_trend (extra 4) ----
    _add("env_eu_residency_normal", "environmental_trend",
         "EU residency is becoming a default ask in enterprise dev/HR contracts within 6-9 months.",
         confidence=0.66)
    _add("env_self_serve_smb_norm", "environmental_trend",
         "SMB segment expects self-serve onboarding without human CSM touch.",
         confidence=0.71,
         falsifier={"condition": "<40% of SMB peers ship self-serve in 12 months",
                    "observable_via": "competitor_audit"})
    _add("env_remote_first_tooling", "environmental_trend",
         "Remote-first orgs continue to over-index on modern HR tooling vs in-office peers.",
         confidence=0.62)
    _add("env_macro_hiring_softening", "environmental_trend",
         "Tech hiring softening reduces upmarket pull on per-seat pricing models — drives need for per-employee adjustments.",
         confidence=0.59)

    return out


def build_recommendations(actors, commitments, goals, decisions, signals, models=None):
    ceo = did(COMPANY, "actor", "jordan")
    model_ids = {m.id for m in (models or [])}
    def _models_for(*keys):
        return [_M(k) for k in keys if _M(k) in model_ids]

    def find_signal_ids(phrase, n=3):
        out = []
        for s in signals:
            if phrase.lower() in s.content_text.lower():
                out.append(s.id)
                if len(out) >= n:
                    break
        return out

    recs = []
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_capacity"),
        proposition_text="Engineering at 91% utilization — pause 3 lower-impact projects before Q3 push.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_cap_pause_low")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                          "note": "approve pause of 3 projects"}},
        expected_impact_usd=60000.0,
        supporting_observation_ids=find_signal_ids("utilization hit 91%") + find_signal_ids("choke point"),
        supporting_model_ids=_models_for("st_eng_util_91", "st_app_pod_capacity_ok",
                                          "rel_eng_capacity_to_slip"),
        target_actor_id=ceo,
    ))
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_postgres_revisit"),
        proposition_text="Postgres-only architecture decision (14 months old) — conditions changed; revisit.",
        target_act_ref=TargetActRef(type="decision", id=did(COMPANY, "decision", "d_postgres_only")),
        proposed_change={"operation": "archive", "payload": {"reason": "conditions_changed",
                           "note": "supersede; introduce read replicas tier"}},
        expected_impact_usd=90000.0,
        supporting_observation_ids=find_signal_ids("Postgres-only") + find_signal_ids("read-latency"),
        supporting_model_ids=_models_for("st_postgres_mismatch", "rel_postgres_to_arr",
                                          "pat_postgres_over_500k", "pat_inst_dropbox_pg",
                                          "hyp_postgres_bandage"),
        target_actor_id=ceo,
    ))
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_zara_1on1"),
        proposition_text="EM has gone 6 weeks without 1:1s with two direct reports — attention.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_zara_juno_1on1")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                           "note": "restart cadence this week; flag with VP Eng"}},
        expected_impact_usd=30000.0,
        supporting_observation_ids=find_signal_ids("1:1 with Juno") + find_signal_ids("App pod morale"),
        supporting_model_ids=_models_for("st_zara_management_gap", "rel_1on1s_to_morale",
                                          "pat_em_skipped_1on1s", "pat_inst_zara_skipped",
                                          "conc_zara_attrition", "pred_zara_attrition",
                                          "hyp_zara_burnout_root"),
        target_actor_id=ceo,
    ))
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_saml_pressure"),
        proposition_text="3 enterprise customers (Acme, Wayfair, Drift) requested SAML in past 60 days — $410K ARR exposure.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_saml_lead")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                           "priority": "p0", "note": "escalate to GA target"}},
        expected_impact_usd=410000.0,
        supporting_observation_ids=(
            find_signal_ids("SAML feature again") +
            find_signal_ids("SAML required") +
            find_signal_ids("when SSO is shipping")
        )[:5],
        supporting_model_ids=_models_for("st_saml_3_asks", "rel_saml_to_renewal",
                                          "pat_enterprise_saml_table_stakes",
                                          "pat_inst_acme_saml", "mkt_compliance_normalizing"),
        target_actor_id=ceo,
    ))
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_acme_slip"),
        proposition_text="Acme renewal at risk — audit-log timing flagged as contract gating item.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_acme_renewal")),
        proposed_change={"operation": "transition", "payload": {"new_state": "at_risk",
                           "note": "exec touch; commit to audit-log GA date"}},
        expected_impact_usd=80000.0,
        supporting_observation_ids=find_signal_ids("Acme renewal looking iffy"),
        supporting_model_ids=_models_for("st_acme_at_risk", "pat_inst_acme_saml",
                                          "pred_acme_renewal", "conc_acme_loss"),
        target_actor_id=ceo,
    ))
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_pipeline_composition"),
        proposition_text="Q3 pipeline lean on enterprise — $2M Series B target needs deeper top-of-funnel.",
        target_act_ref=TargetActRef(type="goal", id=did(COMPANY, "goal", "g_q3_pipeline")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                           "note": "stand up ABM motion or analyst lift"}},
        expected_impact_usd=1500000.0,
        supporting_observation_ids=find_signal_ids("pipeline composition") + find_signal_ids("Enterprise pipeline is thin"),
        supporting_model_ids=_models_for("st_pipeline_thin", "rel_pipeline_comp_to_revenue",
                                          "pred_q3_pipeline_miss", "conc_q3_miss",
                                          "cap_marketing_thin_top_funnel"),
        target_actor_id=ceo,
    ))
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_oldworld_drift"),
        proposition_text="OldWorld Foods showing health drift — 3 invoice failures, no QBR attendance.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_atrisk_oldworld")),
        proposed_change={"operation": "transition", "payload": {"new_state": "at_risk",
                           "note": "decide path: save vs let-go"}},
        expected_impact_usd=38000.0,
        supporting_observation_ids=find_signal_ids("OldWorld"),
        supporting_model_ids=_models_for("st_oldworld_drift", "rel_invoice_to_churn",
                                          "rel_qbr_to_renewal", "pred_oldworld_churn"),
        target_actor_id=ceo,
    ))
    return recs


# =====================================================================
# Top-level
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
        ceo_actor_id=did(COMPANY, "actor", "jordan"),
        actors=actors, customers=customers, goals=goals,
        decisions=decisions, commitments=commitments, signals=signals,
        models=models, recommendations=recommendations,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emit", action="store_true")
    parser.add_argument("--out", default="demo/snapshots/northwind-v1.sql")
    parser.add_argument("--compress", action="store_true")
    parser.add_argument("--no-spec-counts", action="store_true",
                        help="Skip spec-count validation")
    args = parser.parse_args()

    print("Building Northwind bundle...")
    bundle = build_bundle()
    print(f"  actors: {len(bundle.actors)}  customers: {len(bundle.customers)}  goals: {len(bundle.goals)}")
    print(f"  decisions: {len(bundle.decisions)}  commitments: {len(bundle.commitments)}")
    print(f"  signals: {len(bundle.signals)}  models: {len(bundle.models)}  recommendations: {len(bundle.recommendations)}")

    spec = None
    if not args.no_spec_counts:
        with open("demo/generation/specs/northwind.yaml") as f:
            spec = yaml.safe_load(f)
    errors = validate_bundle(bundle, spec=spec)
    if errors:
        print("Validation errors:", file=sys.stderr)
        for e in errors[:20]:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("Validation: OK")

    if args.emit:
        written = write_sql(bundle, Path(args.out), compress=args.compress)
        print(f"Wrote {written}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
