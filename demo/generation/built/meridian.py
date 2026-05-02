"""Meridian Industrial — hand-authored demo bundle.

Series C enterprise software, 1100 employees, $85M ARR. A $4.2M ARR
customer (Industrium Corp) is escalating about a missed feature
commitment. The action list surfaces the war-room view.

Authoring 1100 actors line-by-line isn't realistic in one session —
we're after the *demo-visible substrate*, not the full org chart. The
named actors cover the Industrium response (VP Eng, CSM, CRO, the
sales team), plus enough functional density that Meridian "feels" like
a 1100-person company. The synthetic-actor generator pads to ~120 so
relationships still resolve in the action list.

Spec: demo/generation/specs/meridian.yaml. Pass --no-spec-counts since
this trades headcount for demo-rehearsability.
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


COMPANY = "meridian"


# =====================================================================
# Actors — named leadership + Industrium war-room cast + functional pad
# =====================================================================


NAMED_ACTORS = [
    # CEO + exec staff
    ("sam",        "Sam Whitfield",         "ceo",       None,        "CEO."),
    ("regan",      "Regan Park",            "coo",       "sam",       "COO."),
    ("hadley",     "Hadley Quintero",       "cfo",       "sam",       "CFO."),
    ("anita",      "Anita Berman",          "cto",       "sam",       "CTO."),
    ("tom_m",      "Tom Bishop",            "vp_eng",    "anita",     "VP Engineering. Has NOT been engaged on Industrium yet."),
    ("daria",      "Daria Eklund",          "vp_product","sam",       "VP Product."),
    ("bryant",     "Bryant Calloway",       "cro",       "sam",       "CRO."),
    ("lin",        "Lin Verdugo",           "cmo",       "sam",       "CMO."),
    ("naomi",      "Naomi Roosendaal",      "vp_cs",     "sam",       "VP Customer Success."),
    ("ed",         "Ed Mustakas",           "gc",        "sam",       "General Counsel."),
    ("eve",        "Eve Pakhomova",         "vp_ops",    "regan",     "VP Operations."),
    ("ravi_m",     "Ravi Krishnan",         "head_data", "anita",     "Head of Data."),
    ("kim_m",      "Kim Holm",              "vp_security","anita",    "VP Security."),
    ("sonia",      "Sonia Manfredi",        "vp_people", "sam",       "VP People."),
    # Industrium war-room cast (named, demo-visible)
    ("avery_m",    "Avery Tomson",          "csm",       "naomi",     "Industrium CSM. Knows the customer cold."),
    ("dirk",       "Dirk Halvorsen",        "ae",        "bryant",    "Industrium AE."),
    ("jens",       "Jens Lillehagen",       "se",        "bryant",    "Industrium SE."),
    ("yvonne",     "Yvonne Boudreau",       "engineer",  "tom_m",     "Lead engineer on Industrium feature."),
    ("kasper",     "Kasper Ulmer",          "engineer",  "tom_m",     "Senior engineer on Industrium pipeline."),
    ("madhu",      "Madhu Sankaran",        "engineer",  "tom_m",     "Senior engineer."),
    ("fiona",      "Fiona Cleary",          "pm",        "daria",     "Industrium PM."),
    ("liu",        "Liu Wenwen",            "ops_lead",  "regan",     "Operations lead — Industrium response."),
    # Other enterprise ENT-pod
    ("oliver",     "Oliver Renate",         "ae",        "bryant",    "ENT AE."),
    ("vidya",      "Vidya Premchandran",    "ae",        "bryant",    "ENT AE."),
    ("mateo",      "Mateo Sandoval",        "ae",        "bryant",    "ENT AE."),
    ("kerry",      "Kerry Tanaka",          "ae",        "bryant",    "ENT AE."),
    ("freya",      "Freya Ostby",           "ae",        "bryant",    "ENT AE."),
    ("zenobia",    "Zenobia Cassidy",       "ae",        "bryant",    "ENT AE."),
    # Engineering leadership (5 EMs across 5 pods)
    ("sten",       "Sten Berglund",         "em",        "tom_m",     "EM Platform pod."),
    ("ines_m",     "Ines Carvalho",         "em",        "tom_m",     "EM Optimizer pod."),
    ("uma",        "Uma Tessanjarak",       "em",        "tom_m",     "EM Integrations pod."),
    ("kai_m",      "Kai Tateishi",          "em",        "tom_m",     "EM Pipelines pod (Industrium adjacent)."),
    ("perez",      "Perez Castaneda",       "em",        "tom_m",     "EM Frontend pod."),
    # Senior engineers
    *[(f"se{i}", n, "engineer", em, "Senior engineer.") for i, (n, em) in enumerate([
        ("Hilde Kristofferson", "sten"), ("Ronan Pittman", "sten"), ("Yara Mahmoud", "ines_m"),
        ("Tito Garretón", "ines_m"), ("Mira Antoniou", "uma"), ("Edu Olah", "uma"),
        ("Lev Polyakov", "kai_m"), ("Selena Voss", "kai_m"), ("Quinn Marsters", "perez"),
        ("Birte Knudsen", "perez"),
    ])],
    # Product/Design
    ("liesl",      "Liesl Burmester",       "pm",        "daria",     "PM Optimizer."),
    ("aman",       "Aman Pareek",           "pm",        "daria",     "PM Pipelines."),
    ("juno_m",     "Juno Aaltonen",         "pm",        "daria",     "PM Integrations."),
    ("ada",        "Ada Belmonte",          "designer",  "daria",     "Lead designer."),
    ("rasm",       "Rasmus Halvarsson",     "designer",  "daria",     "Designer."),
    # Sales/CS leadership
    ("amelia_m",   "Amelia Howe",           "head_sales","bryant",    "Head of Enterprise Sales."),
    ("victor_m",   "Victor Kelly",          "head_sales","bryant",    "Head of Mid-Market Sales."),
    ("ines_cs",    "Ines Carvajal",         "head_cs",   "naomi",     "Head of Strategic Accounts."),
    # CSMs
    *[(f"csm{i}", n, "csm", "ines_cs", "CSM.") for i, n in enumerate([
        "Petra Gunnarsdottir", "Tanvir Rahimi", "Ridge Kennison", "Vesna Knežević",
        "Aigerim Bekkalieva", "Sigrid Sandviken", "Diego Solano", "Manon Belette",
    ])],
    # Marketing
    ("hector_m",   "Hector Estela",         "marketing", "lin",       "Field marketing."),
    ("arjuna",     "Arjuna Bhattacharya",   "marketing", "lin",       "Content marketing."),
    ("clio",       "Clio Stenmark",         "marketing", "lin",       "Brand marketing."),
    # Ops/finance
    ("lukas_m",    "Lukas Engdal",          "ops",       "eve",       "RevOps lead."),
    ("greta_m",    "Greta Holmqvist",       "ops",       "eve",       "Pipeline ops."),
    ("dora_m",     "Dora Eklund",           "finance",   "hadley",    "Controller."),
    ("ramin",      "Ramin Bayat",           "finance",   "hadley",    "FP&A."),
    # People/legal
    ("ines_p",     "Ines Pashov",           "people",    "sonia",     "People ops."),
    ("noor_m",     "Noor Khoury",           "people",    "sonia",     "Recruiter ENT."),
    ("kev_m",      "Kev Salinger",          "people",    "sonia",     "Recruiter."),
    ("rita_m",     "Rita Sundberg",         "legal",     "ed",        "Senior counsel."),
    # Data/SRE/Security
    ("dipak",      "Dipak Mehrotra",        "data",      "ravi_m",    "Lead data engineer."),
    ("tara",       "Tara Lehman",           "data",      "ravi_m",    "Analytics."),
    ("baha",       "Baha Onur",             "sre",       "kim_m",     "SRE lead."),
    ("ines_sre",   "Ines Holaszky",         "sre",       "kim_m",     "SRE."),
    ("kim_sec",    "Kim Schreiber",         "security",  "kim_m",     "Security engineer."),
    # Advisors
    ("edie_m",     "Edie Marquez",          "advisor",   "sam",       "Board advisor."),
    ("renée_m",    "Renée Beaumont",        "advisor",   "sam",       "Board observer."),
]


def build_actors() -> list[GeneratedActor]:
    out: list[GeneratedActor] = []
    seen_keys: set[str] = set()
    seen_names: set[str] = set()
    for entry in NAMED_ACTORS:
        key, name, role, mgr, brief = entry
        # Dedup defensively (paranoid; keys are hand-authored).
        if key in seen_keys:
            continue
        seen_keys.add(key)
        # Disambiguate duplicate display names by appending the role suffix.
        disp = name if name not in seen_names else f"{name} ({role})"
        seen_names.add(disp)
        out.append(GeneratedActor(
            id=did(COMPANY, "actor", key),
            name=disp, role=role,
            manager_id=did(COMPANY, "actor", mgr) if mgr else None,
            personality_brief=brief,
            email=f"{key}@meridianindustrial.com",
        ))
    # Pad with synthetic actors so the headcount feels like a Series C
    # — 60 more for a total ~140. Below the 1100 spec target, but the
    # demo's surface area (action list, simulator) doesn't require the
    # full headcount.
    rng = random.Random(31)
    first_names = ["Adi", "Bjorn", "Cleo", "Dario", "Elin", "Faraz", "Gita", "Henrik",
                    "Iyla", "Jonas", "Kira", "Liesl", "Mads", "Niko", "Oksana",
                    "Pavel", "Quinn", "Rohan", "Saskia", "Tomas", "Una", "Viktor",
                    "Wren", "Xio", "Yael", "Zara", "Aurel", "Birta", "Cesar",
                    "Dorin", "Elara", "Frida"]
    last_names  = ["Bergstrom", "Caro", "Dahl", "Eklund", "Friis", "Gunnarsson",
                    "Halland", "Iversen", "Jansson", "Karlsson", "Lindqvist",
                    "Mortensen", "Nordstrom", "Ostlund", "Persson", "Quigley",
                    "Roos", "Stahlberg", "Toresson", "Ulvaeus", "Viborg", "Wahlqvist",
                    "Yman", "Zellweger"]
    roles_pool = ["engineer", "engineer", "engineer", "csm", "ae", "marketing",
                   "ops", "data", "sre", "engineer", "engineer", "designer"]
    em_keys = ["sten", "ines_m", "uma", "kai_m", "perez"]
    for i in range(60):
        key = f"pad_{i:03d}"
        name = f"{rng.choice(first_names)} {rng.choice(last_names)}"
        role = rng.choice(roles_pool)
        mgr = rng.choice(em_keys) if role == "engineer" else None
        out.append(GeneratedActor(
            id=did(COMPANY, "actor", key),
            name=name, role=role,
            manager_id=did(COMPANY, "actor", mgr) if mgr else None,
            personality_brief="",
            email=f"{key}@meridianindustrial.com",
        ))
    return out


# =====================================================================
# Customers — 70, ARR $85M target. Industrium is the headliner ($4.2M).
# =====================================================================


CUSTOMER_SPECS = [
    # Top-tier enterprise (the at-risk one + others)
    ("industrium", "Industrium Corp",       4200000, "enterprise",  "escalating", ["VP Ops — Marlon Frasier"]),
    ("globex",     "Globex Manufacturing",  3100000, "enterprise",  "healthy",    ["CIO — Wright Eberle"]),
    ("sirius",     "Sirius Logistics",      2700000, "enterprise",  "healthy",    ["VP Ops — Brigid Tully"]),
    ("helios",     "Helios Heavy Industries",2400000, "enterprise",  "healthy",    ["CTO — Mira Soltis"]),
    ("orion",      "Orion Aerospace",       3300000, "enterprise",  "healthy",    ["VP Engineering — Yan Tagore"]),
    ("vesta",      "Vesta Energy",          2900000, "enterprise",  "healthy",    ["COO — Solene Ravi"]),
    ("aegis",      "Aegis Defense",         2100000, "enterprise",  "healthy",    ["VP Procurement — Wells Stein"]),
    ("polaris",    "Polaris Mining",        1900000, "enterprise",  "healthy",    ["VP Operations — Ana Rial"]),
    ("rotunda",    "Rotunda Steel",         1850000, "enterprise",  "healthy",    ["VP Tech — Jock Ottinger"]),
    ("titan",      "Titan Petrochemicals",  2200000, "enterprise",  "watching",   ["VP IT — Damon Plage"]),
    # Mid-market enterprise pad — 30 accounts, mostly $500K-$1.5M
    *[(f"ent_{i}", n, arr, "enterprise", "healthy", [f"VP Ops — {p}"])
      for i, (n, arr, p) in enumerate([
        ("Aurora Bearings",       1200000, "Hadi Manjul"),
        ("Beacon Foundries",       980000, "Per Ryswig"),
        ("Cipher Plastics",        720000, "Iso Vong"),
        ("Doric Materials",        890000, "Kit Brand"),
        ("Edda Technologies",      640000, "Mira Knez"),
        ("Fjord Composites",       550000, "Owen Ek"),
        ("Granite Industries",    1400000, "Tara Vint"),
        ("Helix Pharma",          1100000, "Iola Brink"),
        ("Iceberg Cold Chain",     820000, "Bert Kassel"),
        ("Janus Robotics",         690000, "Eve Pohorile"),
        ("Kestrel Aviation",       980000, "Lori Ek"),
        ("Lichen Biotech",         610000, "Saga Volk"),
        ("Mistral Wind",           880000, "Brun Halt"),
        ("Nimbus Cloudworks",      720000, "Emer Ott"),
        ("Obsidian Mining",       1300000, "Kira Tose"),
        ("Pegasus Logistics",      790000, "Reid Bratt"),
        ("Quartz Glassworks",      540000, "Vesa Knot"),
        ("Ramses Foundry",         710000, "Yann Pon"),
        ("Solstice Wood",          580000, "Beck Lid"),
        ("Triton Pumps",           810000, "Iza Voren"),
        ("Ursine Foods",           620000, "Kara Ott"),
        ("Veridian Chemicals",    1500000, "Pia Strom"),
        ("Westwind Energy",        890000, "Reno Talt"),
        ("Xanthos Metals",         770000, "Mara Lev"),
        ("Yves Composites",        650000, "Rolf Pad"),
        ("Zephyr Wind",            930000, "Inka Bod"),
        ("Aurum Refineries",       650000, "Vela Os"),
        ("Bluepine Construction",  720000, "Una Pold"),
        ("Cobalt Mining",         1200000, "Ole Trang"),
        ("Drachen Heavy",          870000, "Bea Vor"),
    ])],
    # Mid-market — 20
    *[(f"mm_{i}", n, arr, "mid_market", "healthy", [f"Ops — {p}"]) for i, (n, arr, p) in enumerate([
        ("Acme Co.",                 380000, "Kim Tovari"),  # the smaller-account drift
        ("Brook Industrial",         320000, "Lex Pons"),
        ("Catalyst Foundry",         280000, "Mira Kald"),
        ("Delta Heavy",              340000, "Jay Roff"),
        ("Embers Energy",            290000, "Lev Sten"),
        ("Forge Manufacturing",      310000, "Ari Hod"),
        ("Garrison Steel",           260000, "Ima Berg"),
        ("Hyland Materials",         220000, "Otto Valk"),
        ("Iglu Logistics",           300000, "Eli Korr"),
        ("Junction Foundry",         210000, "Sam Pak"),
        ("Kestrel Heavy",            240000, "Jan Pole"),
        ("Lyra Mining",              250000, "Ros Meld"),
        ("Mistgrove Construction",   290000, "Hank Voss"),
        ("Norhaven Foundry",         330000, "Tia Holt"),
        ("Oakridge Materials",       280000, "Kai Born"),
        ("Pinemark Logistics",       220000, "Olin Forst"),
        ("Quaver Pumps",             180000, "Mads Yik"),
        ("Ridgeway Composites",      200000, "Kara Hop"),
        ("Stellar Foundry",          240000, "Ven Brom"),
        ("Tundra Equipment",         260000, "Hex Klar"),
    ])],
    # Recently churned / at-risk
    ("oldworld_m", "OldWorld Industrial",   175000, "mid_market",  "at_risk",    ["Ops — Una Kov"]),
    ("clayton",    "Clayton Bearings",      145000, "mid_market",  "watching",   ["Ops — Pia Ott"]),
    # Prospects
    ("siemens_p",  "Siemens (prospect)",          0, "prospect",    "watching",   ["Procurement — Lin Reuss"]),
    ("ge_p",       "GE Digital (prospect)",       0, "prospect",    "watching",   ["VP — Bri Vassal"]),
    ("hitachi_p",  "Hitachi Rail (prospect)",     0, "prospect",    "watching",   ["VP — Reed Kos"]),
    ("eaton_p",    "Eaton Industrial (prospect)", 0, "prospect",    "watching",   ["VP — Ana Hossan"]),
]


def build_customers() -> list[GeneratedCustomer]:
    return [
        GeneratedCustomer(
            id=did(COMPANY, "customer", k),
            company_name=name,
            arr_usd=arr,
            segment=seg,                             # type: ignore[arg-type]
            current_health=health,                   # type: ignore[arg-type]
            primary_contacts=contacts,
        )
        for k, name, arr, seg, health, contacts in CUSTOMER_SPECS
    ]


# =====================================================================
# Goals — 14
# =====================================================================


GOAL_SPECS = [
    ("g_120m",     "Reach $120M ARR by end of 2027",
     "Maintain 45% YoY through expansion + new logo wins.",
     "sam", None, "strategic"),
    ("g_industrium","Recover Industrium ($4.2M ARR) commitment slip",
     "Three critical-path commitments at risk; escalation thread active.",
     "tom_m", "g_120m", "strategic"),
    ("g_ent_renew","Maintain >120% NRR on enterprise tier",
     "Enterprise renewals are the lifeblood.",
     "naomi", "g_120m", "strategic"),
    ("g_q4_pipeline","Build Q4 enterprise pipeline",
     "Mid-market growth has outpaced enterprise; rebalance.",
     "bryant", "g_120m", "strategic"),
    ("g_industrium_milestone","Hit Industrium 2-week milestone",
     "Sales-team commitment if extension is granted.",
     "tom_m", "g_industrium", "operational"),
    ("g_scope_pattern","Address enterprise scope-growth pattern",
     "Past 4 enterprise customers all hit the same scope-growth pattern.",
     "daria", "g_industrium", "strategic"),
    ("g_q4_eng",   "Stabilize engineering capacity for Q4 push",
     "Multiple enterprise commitments converging.",
     "tom_m", "g_industrium", "operational"),
    ("g_security", "Ship FedRAMP authorization",
     "Aegis Defense, Orion Aerospace expansions blocked on it.",
     "kim_m", "g_q4_pipeline", "operational"),
    ("g_data_mig", "Data platform migration (Snowflake) v2",
     "Multi-quarter project, foundational.",
     "ravi_m", "g_120m", "operational"),
    ("g_brand_q4", "Q4 brand campaign",
     "Position vs legacy supply-chain vendors.",
     "lin", "g_120m", "operational"),
    ("g_intl",     "Establish EU presence",
     "Veridian Chemicals (and others) need EU residency.",
     "regan", "g_q4_pipeline", "strategic"),
    ("g_smb_drift","Address smaller-account renewal risk",
     "Acme Co. and similar showing health drift.",
     "naomi", "g_ent_renew", "operational"),
    ("g_qbr",      "Standardize QBR cadence across enterprise",
     "Enterprise QBR cadence is inconsistent.",
     "naomi", "g_ent_renew", "operational"),
    ("g_offsite",  "Plan annual sales kickoff",
     "January 2027 SKO.",
     "bryant", "g_120m", "tactical"),
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
# Decisions — 19
# =====================================================================

DECISION_SPECS = [
    ("d_industrium_orig",
     "Original Industrium commitment scope (12 months ago)",
     "Custom optimization workflow for Industrium's supply chain. Scope: 6 modules.",
     "Marquee customer; lock the relationship.",
     {"area": "product", "customer": "industrium"},
     ["Scope grows >2x", "Critical-path commitments at risk", "Customer escalation"]),
    ("d_no_eu",
     "No EU presence through 2026",
     "We don't operate or store data in EU.",
     "Regulatory complexity vs revenue not aligned yet.",
     {"area": "go-to-market"},
     ["3+ accounts above $1M block on EU"]),
    ("d_snowflake_only",
     "Snowflake-only data platform",
     "All analytics in Snowflake; no Redshift, no Databricks.",
     "Operational simplicity at our scale.",
     {"area": "engineering"},
     ["Cost projections exceed $5M/yr"]),
    ("d_oem_partnership",
     "OEM partnership with Helios",
     "Embedded option in Helios's product.",
     "Distribution play.",
     {"area": "product"},
     ["Helios partnership fails to drive >$5M revenue"]),
    ("d_ent_motion",
     "Enterprise-led GTM",
     "Mid-market is harvest, enterprise is hunt.",
     "Higher LTV in enterprise.",
     {"area": "go-to-market"},
     ["Mid-market grows >150% of enterprise"]),
    ("d_no_freemium",
     "No freemium tier",
     "Self-serve PLG isn't on the roadmap.",
     "Doesn't fit enterprise sales motion.",
     {"area": "product"},
     ["Competitor PLG threatens market share"]),
    ("d_pricing_v3",
     "Pricing v3 — value-based",
     "Move from per-seat to value-based for enterprise.",
     "Aligns price to outcome.",
     {"area": "pricing"},
     ["Win rate drops below 30%"]),
    ("d_cs_pod",
     "Pod-based CS",
     "10-12 accounts per CSM.",
     "Better customer outcomes.",
     {"area": "customer_success"},
     ["NRR drops below 110%"]),
    ("d_q4_offsite_skip",
     "Skip Q4 offsite, do January SKO",
     "Cost vs benefit.",
     "Burn is tight.",
     {"area": "operations"},
     ["Engagement drops"]),
    ("d_security_invest",
     "Heavy security investment Q4",
     "FedRAMP, SOC2 type II, ISO 27001 in parallel.",
     "Enterprise gating items.",
     {"area": "security"},
     ["FedRAMP slippage of >2 quarters"]),
    ("d_multitenant",
     "Multi-tenant architecture",
     "Single instance, multi-tenant. No dedicated infra.",
     "Operational simplicity, margin protection.",
     {"area": "engineering"},
     ["Customer >$5M demands dedicated infra"]),
    ("d_apac_defer",
     "APAC defer to 2027",
     "Asia Pacific market enters 2027.",
     "Focus on US/Europe first.",
     {"area": "go-to-market"},
     ["APAC inbound exceeds 20% of pipeline"]),
    ("d_brand_modern",
     "Brand: 'modern industrial'",
     "Position against legacy SAP/Oracle stacks.",
     "Differentiation.",
     {"area": "marketing"},
     []),
    ("d_research_invest",
     "Apply for joint research grants",
     "DOE/EU H-2030 research grants — partner with universities.",
     "Differentiator + capital.",
     {"area": "research"},
     []),
    ("d_data_residency",
     "EU data residency for top accounts",
     "Set up EU region in Q1 2027.",
     "Veridian + others requested.",
     {"area": "engineering"},
     []),
    ("d_tooling_consolidation",
     "Consolidate observability stack",
     "Move to Datadog (from a 3-vendor mix).",
     "Operational cost.",
     {"area": "engineering"},
     []),
    ("d_oem_referrals",
     "OEM referral commission structure",
     "Standardize commissions for partner-sourced deals.",
     "Encourage partner sales.",
     {"area": "go-to-market"},
     []),
    ("d_compliance_gold",
     "Compliance gold-standard for top 10 accounts",
     "SOC2, ISO 27001, FedRAMP path, HIPAA where needed.",
     "Justifies enterprise pricing.",
     {"area": "security"},
     []),
    ("d_research_open",
     "Open-source key tooling",
     "Open-source the optimization engine core.",
     "Developer mindshare.",
     {"area": "engineering"},
     ["Engineering hiring slows"]),
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
# Commitments — programmatic, focused on demo-visible threads
# =====================================================================


def build_commitments(actors, customers, goals, decisions):
    actor_keys = {a.id for a in actors}
    actor_name_to_key = {}
    for entry in NAMED_ACTORS:
        actor_name_to_key[entry[0]] = did(COMPANY, "actor", entry[0])
    cust_keys = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    goal_keys = {k: did(COMPANY, "goal", k) for k, *_ in GOAL_SPECS}
    dec_keys = {k: did(COMPANY, "decision", k) for k, *_ in DECISION_SPECS}
    rng = random.Random(17)
    out = []

    def _c(key, title, owner, state="active", goal=None, customer=None,
           decisions=None, contributors=None):
        if owner not in actor_name_to_key:
            return
        out.append(GeneratedCommitment(
            id=did(COMPANY, "commitment", key),
            title=title,
            owner_id=actor_name_to_key[owner],
            contributors=[actor_name_to_key[c] for c in (contributors or []) if c in actor_name_to_key],
            state=state,                                 # type: ignore[arg-type]
            due_date=days_from_now(rng.randint(7, 120)),
            contributes_to_goal_id=goal_keys[goal] if goal else None,
            depends_on=[],
            constrained_by_decision_ids=[dec_keys[d] for d in (decisions or []) if d in dec_keys],
            served_by_customer_id=cust_keys[customer] if customer else None,
        ))

    # ============= Industrium war room =============
    _c("c_ind_milestone",   "Hit Industrium 2-week milestone (extension)", "yvonne", "at_risk", "g_industrium_milestone",
       customer="industrium", decisions=["d_industrium_orig"])
    _c("c_ind_pipeline",    "Pipeline integration — Industrium",            "kasper", "at_risk", "g_industrium",
       customer="industrium", decisions=["d_industrium_orig"])
    _c("c_ind_optimizer",   "Optimizer module — Industrium custom",         "yvonne", "at_risk", "g_industrium",
       customer="industrium", decisions=["d_industrium_orig"])
    _c("c_ind_audit",       "Audit-log integration — Industrium",           "madhu", "at_risk", "g_industrium",
       customer="industrium", decisions=["d_industrium_orig"])
    _c("c_ind_warroom",     "Industrium recovery war-room",                 "liu",   "active",  "g_industrium",
       customer="industrium")
    _c("c_ind_csm_check",   "Daily CSM check-in — Industrium",              "avery_m","active", "g_industrium",
       customer="industrium")
    _c("c_ind_exec_touch",  "Executive touch — Industrium VP Ops",          "sam",   "active",  "g_industrium",
       customer="industrium")
    _c("c_ind_dirk_renewal","Industrium renewal contract review",           "dirk",  "at_risk", "g_industrium",
       customer="industrium")
    _c("c_ind_postmortem",  "Industrium scope-creep postmortem",            "fiona", "active",  "g_scope_pattern",
       decisions=["d_industrium_orig"])
    _c("c_ind_extension",   "Industrium extension proposal",                "dirk",  "active",  "g_industrium_milestone",
       customer="industrium")
    _c("c_ind_vp_eng_brief","Brief Tom (VP Eng) on Industrium",             "naomi", "active",  "g_industrium",
       contributors=["avery_m", "fiona"])

    # Cross-team capacity
    _c("c_cap_ind",         "Cross-team allocation — Industrium",           "tom_m", "active",  "g_q4_eng")
    _c("c_cap_optimizer",   "Optimizer pod cross-help",                     "ines_m","active",  "g_q4_eng")
    _c("c_cap_pipelines",   "Pipelines pod cross-help",                     "kai_m", "active",  "g_q4_eng")

    # Pattern observation — past 4 enterprise scope-growth
    for cust_key in ["globex", "sirius", "helios", "orion"]:
        _c(f"c_scopegrew_{cust_key}", f"Scope-growth retrospective: {cust_key}", "fiona",
           "active", "g_scope_pattern", customer=cust_key)

    # Q4 pipeline (strategic recommendation lever)
    _c("c_q4_pipeline_review","Q4 pipeline composition review",             "bryant", "active", "g_q4_pipeline")
    _c("c_q4_pipeline_abm","Stand up enterprise ABM motion",                "amelia_m","active","g_q4_pipeline")
    _c("c_q4_pipeline_analyst","Analyst lift program",                      "lin",    "active", "g_q4_pipeline")
    _c("c_q4_pipeline_partner","OEM partner-sourced deals push",            "victor_m","active","g_q4_pipeline")

    # Enterprise renewals (mass)
    enterprise_keys = ["industrium", "globex", "sirius", "helios", "orion",
                       "vesta", "aegis", "polaris", "rotunda", "titan"]
    for k in enterprise_keys:
        ae = rng.choice(["dirk", "oliver", "vidya", "mateo", "kerry", "freya", "zenobia"])
        csm = rng.choice(["avery_m", *[f"csm{i}" for i in range(8)]])
        _c(f"c_renew_{k}",   f"Enterprise renewal: {k}",       ae,   "active", "g_ent_renew",  customer=k)
        _c(f"c_qbr_{k}",     f"Enterprise QBR: {k}",           csm,  "active", "g_qbr",        customer=k)
        _c(f"c_expand_{k}",  f"Expansion play: {k}",            ae,   "active", "g_q4_pipeline",customer=k)
    # Mid-tier enterprise (ent_0..29)
    for i in range(30):
        k = f"ent_{i}"
        if k not in cust_keys:
            continue
        ae = rng.choice(["oliver", "vidya", "mateo", "kerry", "freya", "zenobia"])
        csm = rng.choice([f"csm{j}" for j in range(8)])
        _c(f"c_renew_{k}",   f"Renewal: {k}",                  ae,  "active", "g_ent_renew",  customer=k)
        if rng.random() < 0.5:
            _c(f"c_qbr_{k}", f"QBR: {k}",                      csm, "active", "g_qbr",        customer=k)
        if rng.random() < 0.3:
            _c(f"c_exp_{k}", f"Expansion: {k}",                ae,  "active", "g_q4_pipeline",customer=k)
    # Mid-market — light load
    for i in range(20):
        k = f"mm_{i}"
        if k not in cust_keys:
            continue
        if rng.random() < 0.4:
            _c(f"c_mm_renew_{k}", f"Renewal: {k}",  rng.choice([f"csm{j}" for j in range(8)]),
               "active", "g_ent_renew", customer=k)

    # Smaller-account drift (Acme Co. — the customer-pressure rec)
    _c("c_acme_recovery",   "Recovery plan: Acme Co.",                      "ines_cs","at_risk","g_smb_drift", customer="mm_0")
    _c("c_oldworld_recovery","Recovery plan: OldWorld Industrial",          "ines_cs","at_risk","g_smb_drift", customer="oldworld_m")
    _c("c_clayton_check",   "Quarterly check: Clayton Bearings",            "csm0",   "active", "g_ent_renew", customer="clayton")

    # Engineering bread-and-butter (50)
    eng_topics = [
        ("Optimizer engine — perf pass", "yvonne", "g_q4_eng"),
        ("Pipeline reliability — SLOs", "kasper", "g_q4_eng"),
        ("Data platform migration phase 2", "ravi_m", "g_data_mig"),
        ("Snowflake cost optimization", "ravi_m", "g_data_mig"),
        ("Multi-tenant tenant-isolation hardening", "uma", "g_q4_eng"),
        ("FedRAMP authorization package", "kim_m", "g_security"),
        ("SOC2 evidence rollover", "kim_m", "g_security"),
        ("ISO 27001 prep", "kim_m", "g_security"),
        ("Datadog migration phase 1", "baha", "g_q4_eng"),
        ("Observability dashboards rebuild", "baha", "g_q4_eng"),
        ("EU data residency planning", "ravi_m", "g_intl"),
        ("Customer audit-log retention", "madhu", "g_q4_eng"),
        ("Optimizer SDK refresh", "se0", "g_q4_eng"),
        ("Pipelines connector library expansion", "se1", "g_q4_eng"),
        ("Mobile app — viewer mode", "se2", "g_q4_eng"),
        ("Reports v3 — query optimizer", "se3", "g_q4_eng"),
        ("Reports v3 — UX", "ada", "g_q4_eng"),
        ("CLI rollout", "se4", "g_q4_eng"),
        ("Compliance scaffolding", "rita_m", "g_security"),
        ("Multi-region failover playbook", "baha", "g_q4_eng"),
        ("Build pipeline modernization", "se5", "g_q4_eng"),
        ("Internal admin API", "se6", "g_q4_eng"),
        ("Field-level audit trails", "se7", "g_q4_eng"),
        ("API rate-limit redesign", "se8", "g_q4_eng"),
        ("Customer onboarding wizard", "ada", "g_q4_eng"),
        ("Optimizer documentation refresh", "se9", "g_q4_eng"),
        ("Internal pricing tools", "lukas_m", "g_q4_pipeline"),
        ("Forecast model v3", "greta_m", "g_q4_pipeline"),
        ("Data warehouse audit", "tara", "g_data_mig"),
        ("Security audit prep", "kim_sec", "g_security"),
    ]
    for i, (title, owner, goal) in enumerate(eng_topics):
        _c(f"c_eng_{i}", title, owner, rng.choice(["active", "active", "blocked", "at_risk"]),
           goal=goal)

    # PMs and design
    pm_topics = [
        ("c_pm_industrium_doc","Industrium custom workflow scoping", "fiona","active","g_industrium"),
        ("c_pm_optimizer_v3","Optimizer v3 PRD",                     "liesl","active","g_120m"),
        ("c_pm_pipelines_v2","Pipelines v2 PRD",                     "aman", "active","g_q4_eng"),
        ("c_pm_integrations_q4","Integrations Q4 roadmap",           "juno_m","active","g_q4_pipeline"),
        ("c_pm_pricing_v3","Pricing v3 PRD",                          "fiona","active","g_120m",["d_pricing_v3"]),
        ("c_pm_eu_pmf","EU PMF research",                             "liesl","active","g_intl"),
        ("c_pm_industrium_brief","Industrium war-room briefing doc",   "fiona","active","g_industrium"),
    ]
    for entry in pm_topics:
        if len(entry) == 5:
            key, title, owner, state, goal = entry
            decs = None
        else:
            key, title, owner, state, goal, decs = entry
        _c(key, title, owner, state, goal=goal, decisions=decs)

    # Marketing
    _c("c_mkt_q4_campaign", "Q4 brand campaign launch",                "lin",    "active", "g_brand_q4")
    _c("c_mkt_industrial_journal","Industrial Journal feature",        "arjuna", "active", "g_brand_q4")
    _c("c_mkt_analyst_tour","Q4 analyst tour",                          "clio",   "active", "g_q4_pipeline")
    _c("c_mkt_partner_summit","OEM partner summit prep",                "hector_m","active","g_q4_pipeline")
    _c("c_mkt_research_grants","Research-grant joint application",     "arjuna", "active", "g_120m", decisions=["d_research_invest"])
    _c("c_mkt_blog","Blog cadence Q4",                                 "arjuna", "active")

    # Ops / People / Finance
    _c("c_ops_pipeline_dashboard","Pipeline operations dashboard",      "lukas_m","active","g_q4_pipeline")
    _c("c_ops_compplan","Comp plan finalization Q4",                    "ramin",  "active", "g_120m")
    _c("c_ops_q4_close","Q4 financial close",                            "dora_m","active")
    _c("c_ops_burn","Burn-vs-runway weekly",                              "dora_m","active","g_120m")
    _c("c_ppl_eng_hires","Senior eng hires Q4",                          "noor_m","active","g_q4_eng")
    _c("c_ppl_csm_hires","CSM hires Q4",                                 "kev_m", "active","g_ent_renew")
    _c("c_ppl_engagement","Engagement survey Q4",                       "ines_p","active")
    _c("c_ppl_offsite","Skip-level dinners (Q4 alt to offsite)",        "ines_p","active",     decisions=["d_q4_offsite_skip"])
    _c("c_legal_msa","Enterprise MSA template refresh",                 "ed",    "active", "g_ent_renew")
    _c("c_legal_industrium","Industrium contract amendment review",     "rita_m","active", "g_industrium", customer="industrium")
    _c("c_legal_eu","EU data residency legal review",                   "rita_m","active", "g_intl")

    # Sales
    _c("c_sales_q4_pipeline","Q4 enterprise pipeline build",            "amelia_m","active","g_q4_pipeline")
    _c("c_sales_oem","OEM partner-sourced commits",                     "victor_m","active","g_q4_pipeline")
    _c("c_sales_q4_renewals","Q4 renewals discipline",                  "amelia_m","active","g_ent_renew")

    # CSM weekly check-ins
    for k in ["industrium", "globex", "sirius", "helios", "orion", "vesta", "aegis"]:
        _c(f"c_check_{k}", f"Weekly check-in: {k}", "avery_m" if k == "industrium" else f"csm{rng.randint(0,7)}",
           "active", "g_ent_renew", customer=k)

    return out


# =====================================================================
# Signals
# =====================================================================


def build_signals(actors, customers, commitments, goals, decisions):
    actor_ids = [a.id for a in actors]
    cust_keys = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    commit_keys = {
        k: did(COMPANY, "commitment", k) for k in (
            "c_ind_milestone", "c_ind_pipeline", "c_ind_optimizer",
            "c_ind_audit", "c_ind_warroom", "c_ind_extension",
            "c_ind_vp_eng_brief", "c_cap_ind", "c_q4_pipeline_review",
            "c_acme_recovery", "c_oldworld_recovery",
            "c_ind_postmortem", "c_pm_industrium_brief",
        )
    }
    decision_keys = {k: did(COMPANY, "decision", k) for k, *_ in DECISION_SPECS}
    goal_keys = {k: did(COMPANY, "goal", k) for k, *_ in GOAL_SPECS}
    rng = random.Random(19)
    out = []
    idx = 0

    def _add(channel, ref, author, ago, text, mentions=None):
        nonlocal idx
        ent = [EntityMention(type=t, id=i) for t, i in (mentions or [])]
        out.append(GeneratedSignal(
            id=did(COMPANY, "signal", f"sig_{idx:04d}"),
            source_channel=channel, source_ref=ref,
            author_id=author, occurred_at=days_ago(ago),
            content_text=text,
            entities_mentioned=ent,
        ))
        idx += 1

    # Industrium escalation thread
    _add("email:message", "msg-industrium-escalation", did(COMPANY, "actor", "avery_m"), 4,
         "Industrium VP Ops emailed: 'Sam, this is the second time we've missed on this commitment. We need a credible recovery plan by next week.'",
         [("customer", cust_keys["industrium"]), ("commitment", commit_keys["c_ind_milestone"])])
    _add("slack:message", "C-industrium-1", did(COMPANY, "actor", "dirk"), 3,
         "Industrium just sent a formal escalation. Their VP Ops is unhappy.",
         [("customer", cust_keys["industrium"])])
    _add("slack:message", "C-industrium-2", did(COMPANY, "actor", "dirk"), 1,
         "Industrium CSM said they'll consider giving us 2 more weeks if we commit to a specific milestone.",
         [("customer", cust_keys["industrium"]), ("commitment", commit_keys["c_ind_extension"])])
    _add("calendar:event", "evt-ind-warroom", did(COMPANY, "actor", "liu"), 0.5,
         "Industrium Recovery War Room (90 min)",
         [("customer", cust_keys["industrium"]), ("commitment", commit_keys["c_ind_warroom"])])
    _add("slack:message", "C-industrium-3", did(COMPANY, "actor", "yvonne"), 6,
         "The optimizer module's performance regression makes the Industrium milestone tight.",
         [("commitment", commit_keys["c_ind_optimizer"])])
    _add("slack:message", "C-industrium-4", did(COMPANY, "actor", "kasper"), 5,
         "Pipeline integration is at-risk; we underestimated the connector edge cases.",
         [("commitment", commit_keys["c_ind_pipeline"])])
    _add("slack:message", "C-industrium-5", did(COMPANY, "actor", "madhu"), 7,
         "Audit-log integration for Industrium has 4 open items; need decision on retention.",
         [("commitment", commit_keys["c_ind_audit"])])

    # VP Eng not engaged
    _add("slack:message", "C-vpeng-1", did(COMPANY, "actor", "tom_m"), 2,
         "I haven't been looped in on the Industrium thread — what's the actual scope risk?",
         [("actor", did(COMPANY, "actor", "tom_m"))])
    _add("slack:message", "C-vpeng-2", did(COMPANY, "actor", "naomi"), 8,
         "We need Tom in the war-room. He's been off-channel since the escalation.",
         [("actor", did(COMPANY, "actor", "tom_m")), ("commitment", commit_keys["c_ind_vp_eng_brief"])])
    _add("slack:message", "C-vpeng-3", did(COMPANY, "actor", "dirk"), 11,
         "The customer is asking who from engineering owns this. Tom has not been visible.",
         [("actor", did(COMPANY, "actor", "tom_m"))])

    # Scope-growth pattern
    _add("slack:message", "C-pattern-1", did(COMPANY, "actor", "fiona"), 9,
         "Looking at Industrium scope vs original commitment: 3x growth. Globex did this. Sirius did this. Helios did this.",
         [("decision", decision_keys["d_industrium_orig"])])
    _add("slack:message", "C-pattern-2", did(COMPANY, "actor", "daria"), 14,
         "Past 4 enterprise customers all hit the same scope-growth pattern. We should formalize this as a constraint.",
         [("goal", goal_keys["g_scope_pattern"])])

    # Q4 pipeline composition
    _add("slack:message", "C-strategy-1", did(COMPANY, "actor", "bryant"), 13,
         "Q4 pipeline composition is shifting to mid-market. Enterprise pipeline is thin.",
         [("goal", goal_keys["g_q4_pipeline"])])
    _add("slack:message", "C-strategy-2", did(COMPANY, "actor", "amelia_m"), 22,
         "ENT pipeline: 12 named, 4 qualified. We need 20+ qualified to hit Q4 number.",
         [])

    # Smaller-account drift (Acme Co.)
    _add("stripe:event", "ch_acme_co_1", did(COMPANY, "actor", "ramin"), 26,
         "Acme Co. invoice failed. Second time this quarter.",
         [("customer", cust_keys["mm_0"])])
    _add("slack:message", "C-cs-1", did(COMPANY, "actor", "ines_cs"), 18,
         "Acme Co. is drifting on health. Stopped showing up to QBRs 30 days ago.",
         [("customer", cust_keys["mm_0"])])
    _add("slack:message", "C-cs-2", did(COMPANY, "actor", "ines_cs"), 5,
         "Acme Co. CSM team's escalation: usage is down 40% MoM.",
         [("customer", cust_keys["mm_0"])])

    # Routine recommendation source — capacity smoothing
    _add("slack:message", "C-eng-1", did(COMPANY, "actor", "tom_m"), 12,
         "Optimizer pod is at 88% capacity, Pipelines is at 72%. We can shift load.",
         [("commitment", commit_keys["c_cap_ind"])])

    # Dense recent activity
    pool = [
        ("slack:message", lambda: rng.choice(actor_ids), [
            "Pipeline integration test results — 12 of 15 connectors passing",
            "Reviewed PR for optimizer perf regression",
            "Snowflake cost optimization saving $14K/mo",
            "FedRAMP package review — 3 controls flagged",
            "Quarterly bug bash report",
            "Tracing exemplars working in staging",
            "Latency-budget breach detected, investigating",
            "Onboarding wizard polish in progress",
            "SOC2 evidence collection on track",
            "EU data residency planning underway",
            "Customer audit-log retention shipped",
            "OEM partner pipeline up 18% MoM",
            "Quarterly bug bash done, 28 tickets closed",
        ]),
        ("github:event", lambda: rng.choice(actor_ids), [
            "PR opened: 'optimizer perf regression fix'",
            "PR merged: 'pipeline connector edge cases'",
            "PR opened: 'audit-log retention policy v2'",
            "Issue closed: 'Snowflake migration phase 2 complete'",
            "PR merged: 'multi-tenant isolation hardening'",
        ]),
        ("slack:message", lambda: rng.choice([
            did(COMPANY, "actor", a) for a in ["dirk", "oliver", "vidya", "mateo", "kerry", "freya"]
        ]), [
            "Globex expansion call going great",
            "Sirius expansion conversation — they want EU data residency",
            "Orion renewal — easy, expanding",
            "Lost the Westwind RFP — too late",
            "New inbound from Hitachi Rail",
            "Veridian Chemicals — needs EU presence to expand",
            "Mid-market push: 8 new logos this quarter",
        ]),
        ("slack:message", lambda: rng.choice([
            did(COMPANY, "actor", a) for a in ["avery_m", "ines_cs"]
        ] + [did(COMPANY, "actor", f"csm{i}") for i in range(8)]), [
            "Industrium daily check-in: tense but stable",
            "Globex QBR: positive",
            "Sirius QBR: scope question came up",
            "Helios partnership going well",
            "Orion expansion: scoping new module",
            "Vesta renewal: easy",
        ]),
        ("calendar:event", lambda: rng.choice(actor_ids), [
            "All-hands",
            "Eng leads sync",
            "GTM forecast review",
            "Industrium war-room daily",
            "Pipeline ops weekly",
            "Investor update prep",
            "Q4 roadmap working session",
            "QBR — Industrium",
            "QBR — Globex",
            "Recruiter sync",
        ]),
        ("stripe:event", lambda: rng.choice([did(COMPANY, "actor", "ramin"), did(COMPANY, "actor", "dora_m")]), [
            "Invoice paid: enterprise tier ($340K)",
            "Subscription updated: Brex (added seats)",
            "Subscription canceled: small SMB account",
            "Payment failed: SMB account (retry scheduled)",
        ]),
        ("email:message", lambda: rng.choice(actor_ids), [
            "Re: Industrium recovery plan",
            "Investor update — Q3 numbers attached",
            "Re: Q4 forecast",
            "Re: Audit log timing",
            "Coffee chat — analyst",
            "Re: pricing v3 question",
        ]),
    ]

    while idx < 380:
        channel, author_fn, options = rng.choice(pool)
        ago = rng.uniform(0.1, 60.0)
        text = rng.choice(options)
        mentions = []
        if rng.random() < 0.3:
            ck = rng.choice(list(cust_keys.keys())[:30])
            mentions.append(("customer", cust_keys[ck]))
        _add(channel, f"auto-{idx:04d}", author_fn(), ago, text, mentions)

    # Older sparse history (60-540 days)
    while idx < 480:
        channel = rng.choice(["slack:message", "github:event", "calendar:event"])
        ago = rng.uniform(60, 480)
        text = rng.choice([
            "Original Industrium kickoff notes",
            "Q3 board meeting prep notes",
            "Original Snowflake migration scoping doc",
            "Pricing v2 retrospective",
            "Series C closing notes",
            "Old offsite plan",
            "FedRAMP pre-application discussion",
            "Original optimizer engine v2 proposal",
        ])
        _add(channel, f"hist-{idx:04d}", rng.choice(actor_ids), ago, text)

    return out


# =====================================================================
# Recommendations — 8
# =====================================================================


def _M(key: str) -> str:
    return did(COMPANY, "model", key)


def build_models(actors, customers, commitments, goals, decisions, signals):
    cust = {k: did(COMPANY, "customer", k) for k, *_ in CUSTOMER_SPECS}
    com = {k: did(COMPANY, "commitment", k) for k in (
        "c_ind_milestone", "c_ind_pipeline", "c_ind_optimizer",
        "c_ind_audit", "c_cap_ind", "c_cap_pipelines",
        "c_q4_pipeline_review", "c_acme_recovery",
        "c_ind_vp_eng_brief", "c_ind_extension",
        "c_ind_postmortem",
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

    # state (16) — Industrium-heavy
    _add("st_industrium_escalating", "state",
         "Industrium ($4.2M ARR) is escalating: 3 critical-path commitments at risk; VP Ops emailed Sam directly.",
         confidence=0.91,
         scope_entities=[{"type": "customer", "id": cust["industrium"]},
                         {"type": "commitment", "id": com["c_ind_milestone"]},
                         {"type": "commitment", "id": com["c_ind_pipeline"]},
                         {"type": "commitment", "id": com["c_ind_optimizer"]}],
         falsifier={"condition": "Industrium signs extension AND ≥1 commitment moves out of at_risk",
                    "observable_via": "salesforce+commitments"},
         support_signals=F("Industrium VP Ops", "formal escalation", "Industrium daily check-in"))
    _add("st_industrium_extension", "state",
         "Industrium is willing to grant 2-week extension if a specific milestone is committed by Friday.",
         confidence=0.78,
         scope_entities=[{"type": "customer", "id": cust["industrium"]},
                         {"type": "commitment", "id": com["c_ind_extension"]}],
         falsifier={"condition": "Industrium retracts extension OR doesn't sign by Friday",
                    "observable_via": "email+salesforce"},
         support_signals=F("2 more weeks if we commit"))
    _add("st_optimizer_perf_regression", "state",
         "Optimizer module has a perf regression that makes the Industrium milestone tight.",
         confidence=0.81,
         scope_entities=[{"type": "commitment", "id": com["c_ind_optimizer"]}],
         falsifier={"condition": "perf regression closed AND benchmark within 5%",
                    "observable_via": "perf_dashboard"},
         support_signals=F("optimizer module's performance regression"))
    _add("st_pipeline_connector_underestimated", "state",
         "Pipeline integration was underscoped — connector edge cases drive ~3 weeks of additional work.",
         confidence=0.74,
         scope_entities=[{"type": "commitment", "id": com["c_ind_pipeline"]}],
         falsifier={"condition": "all connectors pass test matrix in 2 weeks",
                    "observable_via": "ci"},
         support_signals=F("we underestimated", "connector edge cases"))
    _add("st_vp_eng_off_channel", "state",
         "VP Engineering Tom has been off-channel on Industrium for at least 7 days; visibility gap.",
         confidence=0.82,
         scope_actors=[did(COMPANY, "actor", "tom_m")],
         falsifier={"condition": "Tom posts in Industrium war-room channel for 3 consecutive days",
                    "observable_via": "slack"},
         support_signals=F("haven't been looped in", "off-channel", "not been visible"))
    _add("st_pattern_scope_3x", "state",
         "Past 4 enterprise customers (Globex, Sirius, Helios, Orion) hit identical 3x scope-growth pattern.",
         confidence=0.84,
         scope_entities=[{"type": "customer", "id": cust["globex"]},
                         {"type": "customer", "id": cust["sirius"]},
                         {"type": "customer", "id": cust["helios"]},
                         {"type": "customer", "id": cust["orion"]}],
         falsifier={"condition": "next 2 enterprise customers stay within 1.5x scope",
                    "observable_via": "scope_audit"},
         support_signals=F("3x growth", "Past 4 enterprise customers"))
    _add("st_pipelines_pod_capacity_ok", "state",
         "Pipelines pod is at 72% utilization — capacity available to absorb cross-team load.",
         confidence=0.74,
         falsifier={"condition": "Pipelines pod utilization exceeds 90% for 2 weeks",
                    "observable_via": "capacity_audit"},
         support_signals=F("Pipelines is at 72%"))
    _add("st_optimizer_pod_saturated", "state",
         "Optimizer pod at 88% utilization, primarily on Industrium.",
         confidence=0.79,
         falsifier={"condition": "Optimizer pod drops below 75% for 2 weeks",
                    "observable_via": "capacity_audit"},
         support_signals=F("Optimizer pod is at 88%"))
    _add("st_q4_pipeline_thin_ent", "state",
         "Q4 pipeline composition shifting to mid-market; enterprise depth thin.",
         confidence=0.76,
         scope_entities=[{"type": "goal", "id": g["g_q4_pipeline"]}],
         falsifier={"condition": "ENT pipeline doubles in 30 days",
                    "observable_via": "salesforce"},
         support_signals=F("pipeline composition", "Enterprise pipeline is thin"))
    _add("st_acme_co_drift", "state",
         "Acme Co. ($380K ARR) drifting on health: 2 invoice failures, no QBR for 30 days, usage down 40% MoM.",
         confidence=0.81,
         scope_entities=[{"type": "customer", "id": cust["mm_0"]}],
         falsifier={"condition": "Acme Co. attends Q4 QBR AND usage stabilizes for 30 days",
                    "observable_via": "stripe+calendar+usage"},
         support_signals=F("Acme Co.", "usage is down"))
    _add("st_renewals_arr", "state",
         "$32M ARR up for renewal in next 90 days; 4 enterprise accounts in the bucket.",
         confidence=0.77,
         scope_entities=[{"type": "goal", "id": g["g_ent_renew"]}],
         falsifier={"condition": "renewal pipeline drops below $25M",
                    "observable_via": "salesforce"})
    _add("st_oem_helios_strong", "state",
         "Helios OEM partnership is performing above original plan; embedded option live.",
         confidence=0.66,
         scope_entities=[{"type": "decision", "id": d["d_oem_partnership"]},
                         {"type": "customer", "id": cust["helios"]}])
    _add("st_growth_45_yoy", "state",
         "ARR growth at 45% YoY; trajectory $120M target intact if Q4 enterprise lands.",
         confidence=0.79,
         falsifier={"condition": "growth drops below 35% for 2 quarters",
                    "observable_via": "finance"})
    _add("st_eu_demand", "state",
         "Veridian + 2 others requesting EU data residency; current decision blocks.",
         confidence=0.69,
         scope_entities=[{"type": "decision", "id": d["d_no_eu"]},
                         {"type": "customer", "id": cust["ent_21"]}])
    _add("st_fedramp_in_flight", "state",
         "FedRAMP authorization package in review; controls flagged but on track.",
         confidence=0.68,
         scope_entities=[{"type": "goal", "id": g["g_security"]}],
         support_signals=F("FedRAMP package review"))
    _add("st_industrium_csm_engaged", "state",
         "Industrium CSM Avery is fully engaged; daily check-ins running.",
         confidence=0.83,
         scope_actors=[did(COMPANY, "actor", "avery_m")],
         falsifier={"condition": "Avery skips a daily check-in",
                    "observable_via": "calendar"},
         support_signals=F("Industrium daily check-in"))

    # relation (10)
    _add("rel_scope_to_slip", "relation",
         "Scope growth >2x correlates with milestone slip rate +60%.",
         confidence=0.71,
         falsifier={"condition": "next 5 enterprise customers contradict",
                    "observable_via": "history_audit"},
         proposition_extra={"slip_rate_diff": 0.6})
    _add("rel_vp_eng_to_recovery", "relation",
         "VP Engineering visibility on customer escalations correlates with recovery success rate (75% vs 35%).",
         confidence=0.66)
    _add("rel_oem_to_revenue", "relation",
         "OEM partnerships drive >2x revenue per deal compared to direct sales for industrial vertical.",
         confidence=0.68)
    _add("rel_capacity_to_milestone", "relation",
         "Pod capacity above 85% correlates with downstream milestone slip rate +30%.",
         confidence=0.69)
    _add("rel_invoice_to_churn", "relation",
         "Invoice failures (2+) precede churn within 90 days for 60% of mid-market accounts.",
         confidence=0.67)
    _add("rel_qbr_to_renewal", "relation",
         "QBR cadence ≥ 2 per year correlates with 95% renewal rate (vs 75% for 0 QBRs).",
         confidence=0.72,
         falsifier={"condition": "renewal rate gap closes for 2 quarters",
                    "observable_via": "salesforce"})
    _add("rel_pipeline_comp_to_arr", "relation",
         "Pipeline mix shift toward mid-market correlates with lower revenue per opportunity (-22%).",
         confidence=0.62)
    _add("rel_fedramp_to_pipeline", "relation",
         "FedRAMP unlocks $X estimate of $4.5M ARR in defense pipeline (Aegis, Orion expansions).",
         confidence=0.59,
         scope_entities=[{"type": "customer", "id": cust["aegis"]},
                         {"type": "customer", "id": cust["orion"]}])
    _add("rel_eu_to_arr", "relation",
         "EU residency unlocks ~$3.2M of pipeline tied to enterprise accounts requiring EU data.",
         confidence=0.56)
    _add("rel_research_grants_capital", "relation",
         "Joint research grants project ~$1.4M of non-dilutive capital over 18 months.",
         confidence=0.51)

    # prediction (8) — Industrium-centric
    _add("pred_industrium_extension", "prediction",
         "Industrium will grant the 2-week extension if Sam signs a written milestone commit by Friday.",
         confidence=0.62, evaluate_at=days_from_now(7),
         scope_entities=[{"type": "customer", "id": cust["industrium"]}])
    _add("pred_industrium_recovery", "prediction",
         "Industrium will renew at 80% of current ARR if recovery plan succeeds in next 30 days.",
         confidence=0.51, evaluate_at=days_from_now(30),
         scope_entities=[{"type": "customer", "id": cust["industrium"]}])
    _add("pred_industrium_churn", "prediction",
         "Industrium will churn (>50% likely) if no extension is granted AND optimizer milestone misses.",
         confidence=0.48, evaluate_at=days_from_now(45),
         scope_entities=[{"type": "customer", "id": cust["industrium"]}])
    _add("pred_acme_co_churn", "prediction",
         "Acme Co. will churn within 60 days unless touched by VP CS this month.",
         confidence=0.61, evaluate_at=days_from_now(60),
         scope_entities=[{"type": "customer", "id": cust["mm_0"]}])
    _add("pred_q4_pipeline_miss", "prediction",
         "Q4 enterprise number will miss by 18-25% unless ABM motion stands up in next 30 days.",
         confidence=0.52, evaluate_at=days_from_now(45))
    _add("pred_120m_arr", "prediction",
         "Meridian will hit $120M ARR by EOY 2027 if NRR stays >115% and 6 enterprise net-news land.",
         confidence=0.46, evaluate_at=days_from_now(540))
    _add("pred_fedramp_q4", "prediction",
         "FedRAMP authorization will land within Q4; Aegis + Orion expansions follow within 30 days of authorization.",
         confidence=0.54, evaluate_at=days_from_now(120))
    _add("pred_optimizer_recovery", "prediction",
         "Optimizer pod will close perf regression in 14 days if cross-team allocation lands this week.",
         confidence=0.66, evaluate_at=days_from_now(14))

    # pattern (4) + pattern_instance (4)
    _add("pat_enterprise_scope_3x", "pattern",
         "Enterprise customers consistently exhibit 3x scope-growth from initial commit by month 6.",
         confidence=0.74,
         falsifier={"condition": "next 5 enterprise customers stay within 1.5x scope by month 6",
                    "observable_via": "scope_audit"})
    _add("pat_escalation_cycle", "pattern",
         "Customer escalations follow a 3-stage pattern: CSM-level → VP-level → CEO-level over 4-6 weeks.",
         confidence=0.66)
    _add("pat_pricing_v3_lift", "pattern",
         "Value-based pricing tier customers show 30% higher NRR vs per-seat tier.",
         confidence=0.62)
    _add("pat_pod_saturation", "pattern",
         "Pods sustained above 85% capacity for 4+ weeks show 20% higher commitment slip rate.",
         confidence=0.68)
    _add("pat_inst_industrium_scope", "pattern_instance",
         "Industrium instance of the enterprise-scope pattern: 6 → 19 modules in 12 months.",
         confidence=0.86,
         scope_entities=[{"type": "customer", "id": cust["industrium"]}],
         falsifier={"condition": "scope reduces back to ≤ 8 modules in 30 days",
                    "observable_via": "scope_audit"},
         support_models=[_M("pat_enterprise_scope_3x")])
    _add("pat_inst_globex_scope", "pattern_instance",
         "Globex instance of the enterprise-scope pattern: 4 → 14 modules in 14 months.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["globex"]}],
         support_models=[_M("pat_enterprise_scope_3x")])
    _add("pat_inst_industrium_escalation", "pattern_instance",
         "Industrium instance of the escalation-cycle pattern: now at VP-level (CEO-level imminent).",
         confidence=0.79,
         scope_entities=[{"type": "customer", "id": cust["industrium"]}],
         support_models=[_M("pat_escalation_cycle")])
    _add("pat_inst_optimizer_saturation", "pattern_instance",
         "Optimizer pod instance of the pod-saturation pattern.",
         confidence=0.71,
         falsifier={"condition": "Optimizer pod drops below 75% for 2 weeks",
                    "observable_via": "capacity_audit"},
         support_models=[_M("pat_pod_saturation")])

    # capability_assessment (4)
    _add("cap_optimizer_strong", "capability_assessment",
         "Optimizer engine is best-in-class for industrial supply chain (2x throughput vs incumbents).",
         confidence=0.62)
    _add("cap_enterprise_motion", "capability_assessment",
         "Enterprise sales motion is mature (Bryant + Amelia have shipped this before); ABM is the gap.",
         confidence=0.66)
    _add("cap_security_solid", "capability_assessment",
         "Security capability is solid; FedRAMP path is on schedule.",
         confidence=0.68)
    _add("cap_cs_underbuilt", "capability_assessment",
         "Customer Success is below par for $85M ARR scale; needs +5 CSMs and pod-cap improvements.",
         confidence=0.59)

    # hypothesis (4)
    _add("hyp_industrium_root_cause", "hypothesis",
         "Industrium scope creep stems from poor initial discovery — pattern across 4 customers points to systemic issue.",
         confidence=0.49,
         scope_entities=[{"type": "decision", "id": d["d_industrium_orig"]}])
    _add("hyp_vp_eng_capacity", "hypothesis",
         "VP Engineering's off-channel pattern may indicate capacity overload, not disengagement.",
         confidence=0.46,
         scope_actors=[did(COMPANY, "actor", "tom_m")])
    _add("hyp_pricing_v3_friction", "hypothesis",
         "Value-based pricing v3 may face procurement friction in industrial verticals (legacy procurement processes).",
         confidence=0.43)
    _add("hyp_eu_acceleration", "hypothesis",
         "EU presence may need to accelerate to 2026 (vs 2027) given enterprise pipeline composition.",
         confidence=0.41)

    # concern (5)
    _add("conc_industrium_loss", "concern",
         "Risk of losing $4.2M Industrium contract — clearly visible to board.",
         confidence=0.62,
         scope_entities=[{"type": "customer", "id": cust["industrium"]}])
    _add("conc_pattern_systemic", "concern",
         "Risk that scope-growth pattern is systemic — multiple customers may follow.",
         confidence=0.51)
    _add("conc_vp_eng_attrition", "concern",
         "Risk of VP Engineering attrition if capacity overload is left unaddressed.",
         confidence=0.36,
         scope_actors=[did(COMPANY, "actor", "tom_m")])
    _add("conc_q4_miss_visible", "concern",
         "Risk of public Q4 enterprise miss visible to investors.",
         confidence=0.42)
    _add("conc_acme_loss_smb", "concern",
         "Risk of Acme Co. churn — small but symbolic for SMB drift pattern.",
         confidence=0.39,
         scope_entities=[{"type": "customer", "id": cust["mm_0"]}])

    # market_assessment (3)
    _add("mkt_industrial_supply_chain", "market_assessment",
         "Industrial supply-chain SaaS expanding ~40% YoY; consolidation phase next.",
         confidence=0.66)
    _add("mkt_legacy_displacement", "market_assessment",
         "Legacy SAP/Oracle stacks are vulnerable in mid-market industrial; window for displacement.",
         confidence=0.61)
    _add("mkt_compliance_norm_industrial", "market_assessment",
         "FedRAMP, ISO 27001 compliance increasingly table-stakes for industrial enterprise buyers.",
         confidence=0.74,
         falsifier={"condition": "<40% of industrial buyers ask in first 60 days",
                    "observable_via": "buyer_research"})

    # environmental_trend (3)
    _add("env_industrial_modernization", "environmental_trend",
         "Industrial sector entering year 3 of 7-year modernization arc; tailwinds intact.",
         confidence=0.62)
    _add("env_supply_chain_resilience", "environmental_trend",
         "Post-2020 supply-chain resilience focus drives optimization software demand.",
         confidence=0.69)
    _add("env_geopolitical_tension", "environmental_trend",
         "Geopolitical tensions accelerate domestic industrial investment; favors US-based providers.",
         confidence=0.58)

    # =====================================================================
    # Expansion set — doubles the substrate for richer demo retrieval.
    # Authored against the same actors, customers, goals, and decisions
    # so the expanded graph is consistent with the existing entities.
    # =====================================================================

    A = lambda k: did(COMPANY, "actor", k)

    # ---- state (extra 16) ----
    _add("st_globex_qbr_stable", "state",
         "Globex Manufacturing ($3.1M) running a stable monthly QBR cadence; expansion conversation in early stage.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["globex"]}])
    _add("st_orion_blocked_fedramp", "state",
         "Orion Aerospace expansion ($3.3M → $4.5M) blocked solely on FedRAMP authorization.",
         confidence=0.83,
         scope_entities=[{"type": "customer", "id": cust["orion"]},
                         {"type": "goal", "id": g["g_security"]}],
         falsifier={"condition": "Orion advances expansion conversation without FedRAMP",
                    "observable_via": "salesforce"})
    _add("st_aegis_blocked_fedramp", "state",
         "Aegis Defense expansion blocked on FedRAMP + ITAR; second-largest single-customer dollar exposure.",
         confidence=0.81,
         scope_entities=[{"type": "customer", "id": cust["aegis"]},
                         {"type": "goal", "id": g["g_security"]}])
    _add("st_helios_oem_underperforming", "state",
         "Helios OEM partnership revenue at 60% of plan; partner-sourced deals are slower to close.",
         confidence=0.70,
         scope_entities=[{"type": "customer", "id": cust["helios"]},
                         {"type": "decision", "id": d["d_oem_partnership"]}],
         falsifier={"condition": "OEM-sourced deals reach 90% of plan within 90 days",
                    "observable_via": "salesforce"})
    _add("st_titan_watching_security", "state",
         "Titan Petrochemicals is watching us closely; their CISO requested a security review last week.",
         confidence=0.66,
         scope_entities=[{"type": "customer", "id": cust["titan"]}])
    _add("st_polaris_renewal_at_par", "state",
         "Polaris Mining (~$1.9M) renewal looking at-par dollars; no expansion signal.",
         confidence=0.63,
         scope_entities=[{"type": "customer", "id": cust["polaris"]}])
    _add("st_veridian_eu_blocker", "state",
         "Veridian Chemicals ($1.5M) demanding EU residency before contract renewal.",
         confidence=0.79,
         scope_entities=[{"type": "customer", "id": cust["veridian" if "veridian" in cust else "ent_21"]}],
         falsifier={"condition": "Veridian renews without EU residency commitment",
                    "observable_via": "contracts"})
    _add("st_clayton_drift_warning", "state",
         "Clayton Bearings drifting — 2 missed support tickets, no QBR in 60 days.",
         confidence=0.69,
         scope_entities=[{"type": "customer", "id": cust["clayton"]}])
    _add("st_eng_overload_q4", "state",
         "Engineering at 96% utilisation through Q4 — five enterprise commitments converging.",
         confidence=0.85,
         scope_entities=[{"type": "goal", "id": g["g_q4_eng"]}],
         falsifier={"condition": "engineering utilisation drops below 85% for 2 weeks",
                    "observable_via": "capacity_audit"})
    _add("st_data_mig_45pct", "state",
         "Snowflake v2 data-platform migration is 45% complete; biggest unknown is partition reshape.",
         confidence=0.66,
         scope_entities=[{"type": "goal", "id": g["g_data_mig"]}])
    _add("st_security_track_record_strong", "state",
         "Security workstream has been on schedule for 3 quarters — durable execution capability.",
         confidence=0.73,
         scope_actors=[A("kim_m")],
         falsifier={"condition": "security workstream slips by 2+ quarters",
                    "observable_via": "release_log"})
    _add("st_brand_outflanking_legacy", "state",
         "'Modern industrial' brand position outflanking legacy SAP/Oracle in analyst commentary.",
         confidence=0.66,
         scope_entities=[{"type": "decision", "id": d["d_brand_modern"]}])
    _add("st_compliance_gold_4_of_10", "state",
         "Compliance gold-standard rollout: 4 of top-10 accounts at full coverage; 6 in flight.",
         confidence=0.70,
         scope_entities=[{"type": "decision", "id": d["d_compliance_gold"]}])
    _add("st_tooling_consolidation_60pct", "state",
         "Datadog migration is 60% complete; 40% remaining mostly in legacy clusters.",
         confidence=0.65,
         scope_entities=[{"type": "decision", "id": d["d_tooling_consolidation"]}])
    _add("st_pricing_v3_legal_in_review", "state",
         "Pricing v3 (value-based) is in legal review; CFO + GC alignment achieved.",
         confidence=0.68,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v3"]}])
    _add("st_apac_inbound_growing", "state",
         "APAC inbound at 14% of pipeline — 6 points below the 20% revisit trigger.",
         confidence=0.71,
         scope_entities=[{"type": "decision", "id": d["d_apac_defer"]}])

    # ---- relation (extra 7) ----
    _add("rel_fedramp_to_close", "relation",
         "FedRAMP-blocked enterprise deals close at 4x the rate once authorization lands.",
         confidence=0.69,
         scope_entities=[{"type": "goal", "id": g["g_security"]}])
    _add("rel_oem_velocity_to_arr", "relation",
         "OEM-sourced deals close 30% slower but produce 1.4x ACV at expansion.",
         confidence=0.61,
         scope_entities=[{"type": "decision", "id": d["d_oem_partnership"]}])
    _add("rel_pricing_v3_winrate", "relation",
         "Pricing v3 modeling projects win-rate +6 pts; sensitive to large-account anchor effects.",
         confidence=0.55,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v3"]}])
    _add("rel_qbr_to_expansion", "relation",
         "Enterprise accounts attending consistent QBRs expand at 35% vs 12% for inconsistent QBRs.",
         confidence=0.72,
         falsifier={"condition": "expansion rate parity for 2 cycles",
                    "observable_via": "salesforce"},
         scope_entities=[{"type": "goal", "id": g["g_qbr"]}])
    _add("rel_security_to_arr_per_account", "relation",
         "Top-10 accounts with full compliance gold standard pay 1.6x the comparable accounts without.",
         confidence=0.66,
         scope_entities=[{"type": "decision", "id": d["d_compliance_gold"]}])
    _add("rel_data_migration_to_velocity", "relation",
         "Snowflake v2 migration drag is reducing analytics-velocity ~22% during the migration window.",
         confidence=0.57,
         scope_entities=[{"type": "goal", "id": g["g_data_mig"]}])
    _add("rel_csm_pod_to_health", "relation",
         "CSM pod size >12 accounts correlates with 8 pt drop in account health rollups.",
         confidence=0.69,
         scope_entities=[{"type": "decision", "id": d["d_cs_pod"]}])

    # ---- prediction (extra 6) ----
    _add("pred_orion_close_post_fedramp", "prediction",
         "Orion expansion will close within 60 days of FedRAMP GA at $4.2-4.8M ACV.",
         confidence=0.55, evaluate_at=days_from_now(120),
         scope_entities=[{"type": "customer", "id": cust["orion"]}])
    _add("pred_aegis_close_post_fedramp", "prediction",
         "Aegis expansion will close within 90 days of FedRAMP authorization.",
         confidence=0.51, evaluate_at=days_from_now(150),
         scope_entities=[{"type": "customer", "id": cust["aegis"]}])
    _add("pred_helios_oem_bottom_quartile", "prediction",
         "Helios OEM partnership lands in the bottom-quartile of partner-sourced revenue at year-end.",
         confidence=0.46, evaluate_at=days_from_now(180),
         scope_entities=[{"type": "decision", "id": d["d_oem_partnership"]}])
    _add("pred_apac_revisit_q2_2027", "prediction",
         "APAC defer decision will be revisited by Q2 2027 due to inbound concentration in 3 large prospects.",
         confidence=0.47, evaluate_at=days_from_now(240),
         scope_entities=[{"type": "decision", "id": d["d_apac_defer"]}])
    _add("pred_pricing_v3_q1_ga", "prediction",
         "Pricing v3 will GA in Q1 2027 with grandfathering for top-10 accounts.",
         confidence=0.54, evaluate_at=days_from_now(120),
         scope_entities=[{"type": "decision", "id": d["d_pricing_v3"]}])
    _add("pred_q4_engineering_slip", "prediction",
         "1-2 enterprise commitments will slip into Q1 if engineering capacity isn't increased by 4 weeks.",
         confidence=0.62, evaluate_at=days_from_now(45),
         scope_entities=[{"type": "goal", "id": g["g_q4_eng"]}])

    # ---- pattern (extra 4) + pattern_instance (extra 6) ----
    _add("pat_compliance_gating_to_close", "pattern",
         "Enterprise expansions blocked on compliance close in tight clusters within 30-60 days post-authorization.",
         confidence=0.69)
    _add("pat_oem_partnership_underperformance", "pattern",
         "OEM partnerships in industrial SaaS underperform plan in year 1 ~70% of the time.",
         confidence=0.58,
         proposition_extra={"underperform_rate_y1": 0.7})
    _add("pat_data_migration_velocity_drag", "pattern",
         "Multi-quarter data-platform migrations introduce 15-30% velocity drag during the migration phase.",
         confidence=0.66)
    _add("pat_qbr_consistency_uplift", "pattern",
         "Quarterly QBR cadence delivers 2-3x the expansion rate vs ad-hoc cadence in enterprise.",
         confidence=0.71,
         falsifier={"condition": "expansion rate parity over 2 cycles",
                    "observable_via": "salesforce"})

    _add("pat_inst_orion_compliance", "pattern_instance",
         "Orion instance of the compliance-gating-to-close pattern.",
         confidence=0.74,
         scope_entities=[{"type": "customer", "id": cust["orion"]}],
         support_models=[_M("pat_compliance_gating_to_close")])
    _add("pat_inst_aegis_compliance", "pattern_instance",
         "Aegis instance of the compliance-gating-to-close pattern.",
         confidence=0.71,
         scope_entities=[{"type": "customer", "id": cust["aegis"]}],
         support_models=[_M("pat_compliance_gating_to_close")])
    _add("pat_inst_helios_oem", "pattern_instance",
         "Helios instance of the OEM-partnership-underperformance pattern.",
         confidence=0.66,
         scope_entities=[{"type": "decision", "id": d["d_oem_partnership"]}],
         support_models=[_M("pat_oem_partnership_underperformance")])
    _add("pat_inst_snowflake_drag", "pattern_instance",
         "Meridian's Snowflake v2 instance of the data-migration-velocity-drag pattern: 22% drag.",
         confidence=0.68,
         scope_entities=[{"type": "goal", "id": g["g_data_mig"]}],
         support_models=[_M("pat_data_migration_velocity_drag")])
    _add("pat_inst_globex_qbr", "pattern_instance",
         "Globex instance of the QBR-consistency-uplift pattern.",
         confidence=0.66,
         scope_entities=[{"type": "customer", "id": cust["globex"]}],
         support_models=[_M("pat_qbr_consistency_uplift")])
    _add("pat_inst_polaris_inconsistent_qbr", "pattern_instance",
         "Polaris instance of inconsistent-QBR underperformance.",
         confidence=0.61,
         scope_entities=[{"type": "customer", "id": cust["polaris"]}],
         support_models=[_M("pat_qbr_consistency_uplift")])

    # ---- capability_assessment (extra 4) ----
    _add("cap_security_org_strong", "capability_assessment",
         "Security capability is high — Kim's team consistently delivers compliance milestones on plan.",
         confidence=0.70,
         scope_actors=[A("kim_m")])
    _add("cap_finance_modeling_solid", "capability_assessment",
         "Finance modeling capability is solid — pricing v3 model holds up under board-grade scrutiny.",
         confidence=0.67)
    _add("cap_legal_internal_thin", "capability_assessment",
         "Legal capability is thin internally — outside counsel still owns most enterprise redlines.",
         confidence=0.62)
    _add("cap_research_emerging", "capability_assessment",
         "Research capability is emerging — DOE grant work is producing published papers but not yet customer-visible advantage.",
         confidence=0.55)

    # ---- hypothesis (extra 5) ----
    _add("hyp_oem_should_be_referral_only", "hypothesis",
         "Helios OEM may pencil better as a referral-only partnership than embedded.",
         confidence=0.46,
         scope_entities=[{"type": "decision", "id": d["d_oem_partnership"]}])
    _add("hyp_pricing_v3_protect_top10", "hypothesis",
         "Pricing v3 may need explicit large-account anchor protection for top-10 to avoid renegotiation chaos.",
         confidence=0.49,
         scope_entities=[{"type": "decision", "id": d["d_pricing_v3"]}])
    _add("hyp_apac_should_pull_forward", "hypothesis",
         "APAC may benefit from Q3 2026 entry instead of 2027 given Singapore/Korea pipeline strength.",
         confidence=0.41)
    _add("hyp_smb_drift_segmentation", "hypothesis",
         "SMB drift may be a single-cohort signal (recent growth-cohort onboarding gap) rather than tier-wide.",
         confidence=0.44)
    _add("hyp_freemium_for_research", "hypothesis",
         "Open-sourcing the optimization core may be more valuable for academic adoption than current strategy assumes.",
         confidence=0.39,
         scope_entities=[{"type": "decision", "id": d["d_research_open"]}])

    # ---- concern (extra 6) ----
    _add("conc_orion_stalled", "concern",
         "Risk that Orion stalls if FedRAMP slips by even 1 quarter — biggest non-Industrium expansion at stake.",
         confidence=0.51,
         scope_entities=[{"type": "customer", "id": cust["orion"]}])
    _add("conc_aegis_competitor", "concern",
         "Risk that a defense-grade competitor lands FedRAMP first and takes Aegis.",
         confidence=0.43,
         scope_entities=[{"type": "customer", "id": cust["aegis"]}])
    _add("conc_eu_dec_pull_forward", "concern",
         "Risk that EU residency demand forces a faster decision than the planned Q1 2027.",
         confidence=0.47,
         scope_entities=[{"type": "decision", "id": d["d_no_eu"]}])
    _add("conc_data_migration_overrun", "concern",
         "Risk that Snowflake v2 migration overruns into 2027 and stalls analytics-driven product work.",
         confidence=0.49,
         scope_entities=[{"type": "goal", "id": g["g_data_mig"]}])
    _add("conc_oem_brand_conflict", "concern",
         "Risk that Helios OEM creates partner-vs-direct brand conflict if reframed as referral-only.",
         confidence=0.36,
         scope_entities=[{"type": "decision", "id": d["d_oem_partnership"]}])
    _add("conc_compliance_overinvest", "concern",
         "Risk that heavy compliance investment crowds out product velocity in Q4.",
         confidence=0.41,
         scope_entities=[{"type": "decision", "id": d["d_security_invest"]}])

    # ---- market_assessment (extra 4) ----
    _add("mkt_industrial_cloud_late", "market_assessment",
         "Industrial cloud adoption is 5-7 years behind other verticals; tailwind compounds for ~3 more years.",
         confidence=0.66)
    _add("mkt_legacy_oracle_displacement", "market_assessment",
         "Oracle on-prem displacement is happening in mid-market industrial first; enterprise tier will follow in 18-24 months.",
         confidence=0.61)
    _add("mkt_partner_ecosystem_norm", "market_assessment",
         "Industrial buyers expect a partner ecosystem (consulting, SI, data) before signing top-tier contracts.",
         confidence=0.69)
    _add("mkt_security_compliance_floor_rising", "market_assessment",
         "Compliance floor in industrial is rising — FedRAMP increasingly expected for any customer touching DoD subcontractors.",
         confidence=0.72,
         falsifier={"condition": "<30% of industrial-defense buyers ask for FedRAMP",
                    "observable_via": "buyer_research"})

    # ---- environmental_trend (extra 4) ----
    _add("env_reshoring_macro", "environmental_trend",
         "US reshoring macro-trend continues; benefits industrial software providers with US-resident infrastructure.",
         confidence=0.68)
    _add("env_climate_compliance", "environmental_trend",
         "Climate-related compliance reporting (CDP, SASB) is normalising into industrial buyer requirements.",
         confidence=0.62)
    _add("env_eu_industrial_data_act", "environmental_trend",
         "EU Industrial Data Act implementation is accelerating EU residency demand among multinational industrials.",
         confidence=0.65)
    _add("env_macro_supply_chain_resilience_funding", "environmental_trend",
         "Government grant funding for supply-chain resilience programs is creating direct co-funding paths for our customers.",
         confidence=0.61)

    return out


def build_recommendations(actors, commitments, goals, decisions, signals, models=None):
    ceo = did(COMPANY, "actor", "sam")
    model_ids = {m.id for m in (models or [])}
    def _models_for(*keys):
        return [_M(k) for k in keys if _M(k) in model_ids]

    def find(phrase, n=3):
        out = []
        for s in signals:
            if phrase.lower() in s.content_text.lower():
                out.append(s.id)
                if len(out) >= n:
                    break
        return out

    recs = []
    # 1. Industrium bridge alert (the headline)
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_industrium"),
        proposition_text="Industrium ($4.2M ARR) — 3 critical-path commitments at slip risk; escalation thread active.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_ind_milestone")),
        proposed_change={"operation": "transition", "payload": {"new_state": "at_risk",
                          "priority": "p0", "note": "war-room daily; commit milestone in writing"}},
        expected_impact_usd=4200000.0,
        supporting_observation_ids=find("Industrium VP Ops") + find("formal escalation") + find("optimizer module"),
        supporting_model_ids=_models_for("st_industrium_escalating", "st_industrium_extension",
                                          "st_optimizer_perf_regression",
                                          "st_pipeline_connector_underestimated",
                                          "pat_inst_industrium_escalation",
                                          "pred_industrium_recovery", "pred_industrium_churn",
                                          "conc_industrium_loss"),
        target_actor_id=ceo,
    ))
    # 2. Capacity reallocation
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_capacity"),
        proposition_text="Cross-team allocation needed for Industrium recovery this week.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_cap_ind")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                          "note": "pull 2 senior engs from Pipelines pod into Industrium war-room"}},
        expected_impact_usd=350000.0,
        supporting_observation_ids=find("Optimizer pod is at 88%") + find("we underestimated"),
        supporting_model_ids=_models_for("st_optimizer_pod_saturated",
                                          "st_pipelines_pod_capacity_ok",
                                          "rel_capacity_to_milestone",
                                          "pat_pod_saturation",
                                          "pat_inst_optimizer_saturation",
                                          "pred_optimizer_recovery"),
        target_actor_id=ceo,
    ))
    # 3. VP Eng not engaged
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_vp_eng"),
        proposition_text="VP Engineering has not been engaged on Industrium issue — situation needs visibility.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_ind_vp_eng_brief")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                          "note": "brief Tom today; loop into war-room"}},
        expected_impact_usd=120000.0,
        supporting_observation_ids=find("haven't been looped in") + find("off-channel") + find("not been visible"),
        supporting_model_ids=_models_for("st_vp_eng_off_channel",
                                          "rel_vp_eng_to_recovery",
                                          "hyp_vp_eng_capacity",
                                          "conc_vp_eng_attrition"),
        target_actor_id=ceo,
    ))
    # 4. Decision revisit — original Industrium scope
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_scope_revisit"),
        proposition_text="Original Industrium commitment scope grew 3x — re-scope before retry.",
        target_act_ref=TargetActRef(type="decision", id=did(COMPANY, "decision", "d_industrium_orig")),
        proposed_change={"operation": "archive", "payload": {"reason": "scope_grew_3x",
                           "note": "supersede with re-scoped contract"}},
        expected_impact_usd=280000.0,
        supporting_observation_ids=find("3x growth") + find("Industrium scope"),
        supporting_model_ids=_models_for("st_pattern_scope_3x", "rel_scope_to_slip",
                                          "pat_inst_industrium_scope",
                                          "hyp_industrium_root_cause"),
        target_actor_id=ceo,
    ))
    # 5. Pattern observation across past 4 enterprise customers
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_pattern"),
        proposition_text="Past 4 enterprise customers all hit the same scope-growth pattern — formalize as constraint.",
        target_act_ref=TargetActRef(type="goal", id=did(COMPANY, "goal", "g_scope_pattern")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                          "note": "ship enterprise scope-management playbook"}},
        expected_impact_usd=800000.0,
        supporting_observation_ids=find("Past 4 enterprise customers") + find("Globex did this"),
        supporting_model_ids=_models_for("pat_enterprise_scope_3x",
                                          "pat_inst_industrium_scope",
                                          "pat_inst_globex_scope",
                                          "conc_pattern_systemic",
                                          "hyp_industrium_root_cause"),
        target_actor_id=ceo,
    ))
    # 6. Routine recommendation (capacity smoothing — non-Industrium)
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_routine"),
        proposition_text="Pipelines pod is at 72% utilization — opportunity to absorb cross-team load.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_cap_pipelines")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                          "note": "approve pipeline pod cross-help"}},
        expected_impact_usd=60000.0,
        supporting_observation_ids=find("Pipelines is at 72%"),
        supporting_model_ids=_models_for("st_pipelines_pod_capacity_ok"),
        target_actor_id=ceo,
    ))
    # 7. Strategic — Q4 pipeline composition
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_pipeline_strat"),
        proposition_text="Q4 pipeline composition shifting to mid-market — strategic check on enterprise depth.",
        target_act_ref=TargetActRef(type="goal", id=did(COMPANY, "goal", "g_q4_pipeline")),
        proposed_change={"operation": "transition", "payload": {"new_state": "active",
                          "note": "rebalance toward enterprise; stand up ABM motion"}},
        expected_impact_usd=1200000.0,
        supporting_observation_ids=find("pipeline composition") + find("Enterprise pipeline is thin"),
        supporting_model_ids=_models_for("st_q4_pipeline_thin_ent",
                                          "rel_pipeline_comp_to_arr",
                                          "pred_q4_pipeline_miss",
                                          "conc_q4_miss_visible"),
        target_actor_id=ceo,
    ))
    # 8. Smaller-account renewal risk
    recs.append(GeneratedRecommendation(
        id=did(COMPANY, "rec", "r_acme_drift"),
        proposition_text="Acme Co. ($380K ARR) renewal risk — health drift over 30 days.",
        target_act_ref=TargetActRef(type="commitment", id=did(COMPANY, "commitment", "c_acme_recovery")),
        proposed_change={"operation": "transition", "payload": {"new_state": "at_risk",
                          "note": "exec touch + decision on save vs let-go"}},
        expected_impact_usd=380000.0,
        supporting_observation_ids=find("Acme Co.") + find("usage is down"),
        supporting_model_ids=_models_for("st_acme_co_drift",
                                          "rel_invoice_to_churn",
                                          "rel_qbr_to_renewal",
                                          "pred_acme_co_churn",
                                          "conc_acme_loss_smb"),
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
        ceo_actor_id=did(COMPANY, "actor", "sam"),
        actors=actors, customers=customers, goals=goals,
        decisions=decisions, commitments=commitments, signals=signals,
        models=models, recommendations=recommendations,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emit", action="store_true")
    parser.add_argument("--out", default="demo/snapshots/meridian-v1.sql")
    parser.add_argument("--compress", action="store_true")
    parser.add_argument("--no-spec-counts", action="store_true",
                        help="Skip spec count validation")
    args = parser.parse_args()

    print("Building Meridian bundle...")
    bundle = build_bundle()
    print(f"  actors: {len(bundle.actors)}  customers: {len(bundle.customers)}  goals: {len(bundle.goals)}")
    print(f"  decisions: {len(bundle.decisions)}  commitments: {len(bundle.commitments)}")
    print(f"  signals: {len(bundle.signals)}  models: {len(bundle.models)}  recommendations: {len(bundle.recommendations)}")

    spec = None
    if not args.no_spec_counts:
        with open("demo/generation/specs/meridian.yaml") as f:
            spec = yaml.safe_load(f)
    errors = validate_bundle(bundle, spec=spec)
    if errors:
        print("Validation errors:", file=sys.stderr)
        for e in errors[:30]:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("Validation: OK")

    if args.emit:
        written = write_sql(bundle, Path(args.out), compress=args.compress)
        print(f"Wrote {written}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
