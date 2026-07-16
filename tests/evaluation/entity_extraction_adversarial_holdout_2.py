"""Second independently authored adversarial holdout for persisted entity surfaces.

This corpus intentionally carries no unresolved-phrase hints and no contextual
messages.  Gold spans are derived from unique literal surfaces and then frozen
by ``FROZEN_CORPUS_SHA256`` over canonical JSON.
"""

from __future__ import annotations

import hashlib
import json

from lib.evaluation.entity_extraction_gold import GoldMention, GoldSignal


# Each tuple is (id, source, text, [(surface, type, canonical referent), ...]).
# Batches are deliberately genuine groups of ten, not repeated text templates.
_CASES = (
    # Batch 01: Slack-native forms, punctuation, and plausible non-entities.
    ("h2-001", "slack:message", "<@U04K9|zoë> asked Kestrel-7 to hold.", (("<@U04K9|zoë>", "person", "person:zoe"), ("Kestrel-7", "project", "project:kestrel-7"))),
    ("h2-002", "slack:message", "Route this through <#C91OPS|war-room>; no customer is named.", (("<#C91OPS|war-room>", "channel", "channel:war-room"),)),
    ("h2-003", "slack:message", "The words Important Update are only a heading.", ()),
    ("h2-004", "slack:message", "@mira.k owns the Élan Migration, not Friday.", (("mira.k", "person", "person:mira-k"), ("Élan Migration", "project", "project:elan-migration"))),
    ("h2-005", "slack:message", "Please page <!subteam^S88SEC|security-oncall> for SEC-204.", (("<!subteam^S88SEC|security-oncall>", "team", "team:security-oncall"), ("SEC-204", "issue", "issue:SEC-204"))),
    ("h2-006", "slack:message", "“Customer Success” describes a function here, not an account.", ()),
    ("h2-007", "slack:message", "CFO revenue joined the call with Blue Yonder GmbH.", (("CFO revenue", "role", "role:cfo-revenue"), ("Blue Yonder GmbH", "customer", "customer:blue-yonder"))),
    ("h2-008", "slack:message", "A.B.C. reviewed the contract ‘Málaga-Prime’. ", (("A.B.C.", "team", "team:abc"), ("Málaga-Prime", "contract", "contract:malaga-prime"))),
    ("h2-009", "slack:message", "Today Research And Development met; those are ordinary words.", ()),
    ("h2-010", "slack:message", "#launch-room tracks Ωmega Service while @li-wei coordinates.", (("launch-room", "channel", "channel:launch-room"), ("Ωmega Service", "service", "service:omega"), ("li-wei", "person", "person:li-wei"))),

    # Batch 02: Jira keys, product names, generic capitalized prose.
    ("h2-011", "jira:issue", "OPS-7 duplicates OPS_7001 in the Qilin rollout.", (("OPS-7", "issue", "issue:OPS-7"), ("OPS_7001", "incident", "incident:OPS_7001"), ("Qilin", "project", "project:qilin"))),
    ("h2-012", "jira:issue", "Title: Improve Overall Quality Before Release", ()),
    ("h2-013", "jira:issue", "Blocked by Data&AI Platform Europe and the API team.", (("Data&AI Platform Europe", "team", "team:data-ai-europe"), ("the API team", "team", "team:api"))),
    ("h2-014", "jira:issue", "ACME-000042 affects tenant org_42, but org_42 is opaque lowercase data.", (("ACME-000042", "issue", "issue:ACME-000042"),)),
    ("h2-015", "jira:issue", "Renée D’Arcy reproduced it in São-Paulo Sandbox.", (("Renée D’Arcy", "person", "person:renee-darcy"), ("São-Paulo Sandbox", "environment", "environment:sao-paulo"))),
    ("h2-016", "jira:issue", "Acceptance Criteria: Fast, Safe, Simple.", ()),
    ("h2-017", "jira:issue", "The incident ‘午夜-β’ supersedes SRE_991.", (("午夜-β", "incident", "incident:midnight-beta"), ("SRE_991", "incident", "incident:SRE_991"))),
    ("h2-018", "jira:issue", "VP security approved Project of the Dawn.", (("VP security", "role", "role:vp-security"), ("Project of the Dawn", "project", "project:dawn"))),
    ("h2-019", "jira:issue", "JSON says {\"status\": \"Blocked\", \"owner\": \"Unknown\"}.", ()),
    ("h2-020", "jira:issue", "North-Star/2 hands off to CRM.2026 after FIN-81.", (("North-Star", "project", "project:north-star"), ("CRM.2026", "system", "system:crm-2026"), ("FIN-81", "issue", "issue:FIN-81"))),

    # Batch 03: Email quoting, signatures, addresses, and named accounts.
    ("h2-021", "email:message", "Re: renewal for Lumen & Finch — approved by Anaïs Nin.", (("Lumen & Finch", "customer", "customer:lumen-finch"), ("Anaïs Nin", "person", "person:anais-nin"))),
    ("h2-022", "email:message", "Dear Team, Please Review The Attached Document.", ()),
    ("h2-023", "email:message", "The account \"Crème Brûlée Co\" moved to EMEA-3.", (("Crème Brûlée Co", "customer", "customer:creme-brulee"), ("EMEA-3", "region", "region:EMEA-3"))),
    ("h2-024", "email:message", "> Nimbus Gateway failed\nThat quoted system remains the subject.", (("Nimbus Gateway", "service", "service:nimbus-gateway"),)),
    ("h2-025", "email:message", "From: NOREPLY <noreply@example.test>; this is transport metadata.", ()),
    ("h2-026", "email:message", "Could Dr. Łukasz Żak brief the Board of Meridian?", (("Dr", "role", "role:doctor"), ("Łukasz Żak", "person", "person:lukasz-zak"), ("Board of Meridian", "team", "team:board-meridian"))),
    ("h2-027", "email:message", "We renewed customer ‘株式会社みらい’ under CUST_88.", (("株式会社みらい", "customer", "customer:mirai"), ("CUST_88", "customer_id", "customer:CUST_88"))),
    ("h2-028", "email:message", "Monday Morning Summary: Everything Is Green.", ()),
    ("h2-029", "email:message", "[External] O'Brien-Rao Partners needs the DPA workflow.", (("O'Brien-Rao Partners", "customer", "customer:obrien-rao"), ("the DPA workflow", "workflow", "workflow:dpa"))),
    ("h2-030", "email:message", "Nested quote: “the project ‘Orchid/Delta’” is paused.", (("Orchid/Delta", "project", "project:orchid-delta"),)),

    # Batch 04: Slack plain references and intentional ambiguity.
    ("h2-031", "slack:message", "@qa_bot-2 filed BUG-9 against Zeta.Cloud.", (("qa_bot-2", "person", "person:qa-bot-2"), ("BUG-9", "issue", "issue:BUG-9"), ("Zeta.Cloud", "service", "service:zeta-cloud"))),
    ("h2-032", "slack:message", "Can Someone Update This?", ()),
    ("h2-033", "slack:message", "The Atlas customer accepted role ‘Design Partner’. ", (("Atlas customer", "customer", "customer:atlas"), ("Design Partner", "role", "role:design-partner"))),
    ("h2-034", "slack:message", "CTO and CEO are titles, but CTO engineering is the routed role.", (("CTO", "role", "role:cto"), ("CEO", "role", "role:ceo"), ("CTO engineering", "role", "role:cto-engineering"))),
    ("h2-035", "slack:message", "Ship with R&D North America; exclude the phrase Very High Priority.", (("R&D North America", "team", "team:rd-na"),)),
    ("h2-036", "slack:message", "<@U777> said `Alpha Service` is literal code, not a reference.", (("<@U777>", "person", "person:U777"),)),
    ("h2-037", "slack:message", "The workflow is stuck; the owner is absent.", (("The workflow", "workflow", None),)),
    ("h2-038", "slack:message", "PÉGASE_12 belongs to Münchner Rück AG.", (("Münchner Rück AG", "customer", "customer:munich-re"),)),
    ("h2-039", "slack:message", "FYI: Good News Everyone is a greeting, not an organization.", ()),
    ("h2-040", "slack:message", "Ask #proj-éclair about Éclair-Next and Q4.", (("proj-éclair", "channel", "channel:proj-eclair"), ("Éclair-Next", "project", "project:eclair-next"), ("Q4", "period", "period:q4"))),

    # Batch 05: Jira markup and boundary traps.
    ("h2-041", "jira:issue", "[~accountid:abc123] assigned IAM-404 to Helios Team.", (("IAM-404", "issue", "issue:IAM-404"), ("Helios Team", "team", "team:helios"))),
    ("h2-042", "jira:issue", "h2. Current State and Desired State", ()),
    ("h2-043", "jira:issue", "The service \"支付-Gateway_v2\" calls U.S.A.", (("支付-Gateway_v2", "service", "service:payment-gateway-v2"), ("U.S.A.", "region", "region:usa"))),
    ("h2-044", "jira:issue", "Do not parse abc-123 or _OPS_9_; parse CORE-123456789012.", (("CORE-123456789012", "issue", "issue:CORE-123456789012"),)),
    ("h2-045", "jira:issue", "Owner changed from Мария Иванова to Team Québec.", (("Мария Иванова", "person", "person:maria-ivanova"), ("Team Québec", "team", "team:quebec"))),
    ("h2-046", "jira:issue", "Expected Result / Actual Result / Steps To Reproduce", ()),
    ("h2-047", "jira:issue", "Epic Nebula's child is DEV-17, not the word Child.", (("Nebula's", "project", "project:nebula"), ("DEV-17", "issue", "issue:DEV-17"))),
    ("h2-048", "jira:issue", "Escalate to SVP operations for the Phoenix launch.", (("SVP operations", "role", "role:svp-operations"), ("the Phoenix launch", "launch", "launch:phoenix"))),
    ("h2-049", "jira:issue", "Inline code `Customer Alpha` must remain data, not mention evidence.", ()),
    ("h2-050", "jira:issue", "GRC_2, GRC-2, and G.R.C. denote three distinct records.", (("GRC_2", "control", "control:GRC_2"), ("GRC-2", "issue", "issue:GRC-2"), ("G.R.C.", "team", "team:grc"))),

    # Batch 06: Email typography, aliases, and false headings.
    ("h2-051", "email:message", "İpek Şahin introduced Æther Works to Project Möbius.", (("İpek Şahin", "person", "person:ipek-sahin"), ("Æther Works", "customer", "customer:aether"), ("Project Möbius", "project", "project:mobius"))),
    ("h2-052", "email:message", "Subject: Action Required Immediately", ()),
    ("h2-053", "email:message", "Account (New): Fünf Sterne KG; owner: José-Luis.", (("Fünf Sterne KG", "customer", "customer:fuenf-sterne"), ("José-Luis", "person", "person:jose-luis"))),
    ("h2-054", "email:message", "The decision “Go/No-Go 27” replaced DEC_19.", (("Go/No-Go 27", "decision", "decision:go-no-go-27"), ("DEC_19", "decision", "decision:DEC_19"))),
    ("h2-055", "email:message", "Thanks, Best Regards, and Kind Regards are closings.", ()),
    ("h2-056", "email:message", "CC: 王小明; ask Sakura Bank 株式会社 for evidence.", (("王小明", "person", "person:wang-xiaoming"), ("Sakura Bank 株式会社", "customer", "customer:sakura-bank"))),
    ("h2-057", "email:message", "The renewal for ACME (Europe) is owned by RevOps-EMEA.", (("ACME", "customer", "customer:acme-europe"), ("RevOps-EMEA", "team", "team:revops-emea"))),
    ("h2-058", "email:message", "Quoted phrase 'Mission Critical' is an adjective, not a system.", ()),
    ("h2-059", "email:message", "Please route case CS-8 to the same owner.", (("CS-8", "case", "case:CS-8"), ("same owner", "person", None))),
    ("h2-060", "email:message", "N.A.S.A. and ESA met at Site-β.", (("N.A.S.A.", "organization", "organization:nasa"), ("ESA", "organization", "organization:esa"), ("Site-β", "location", "location:site-beta"))),

    # Batch 07: Slack Unicode and markup collisions.
    ("h2-061", "slack:message", "<!here> notify is broadcast markup, while <#C2|ops> is a channel.", (("<#C2|ops>", "channel", "channel:ops"),)),
    ("h2-062", "slack:message", "I Think We Should Wait Until Tomorrow.", ()),
    ("h2-063", "slack:message", "@søren paired with Nguyễn Thị Minh on Café-42.", (("søren", "person", "person:soren"), ("Nguyễn Thị Minh", "person", "person:nguyen-minh"), ("Café-42", "project", "project:cafe-42"))),
    ("h2-064", "slack:message", "The customer 'A/B Testing Ltd.' rejected PLAN_6.", (("A/B Testing Ltd.", "customer", "customer:ab-testing"), ("PLAN_6", "plan", "plan:PLAN_6"))),
    ("h2-065", "slack:message", "VP product works with VP Productization; they are separate roles.", (("VP product", "role", "role:vp-product"), ("VP", "role", "role:vp"), ("Productization", "team", "team:productization"))),
    ("h2-066", "slack:message", "`#secret-room` is code; #public-room is a real reference.", (("public-room", "channel", "channel:public-room"),)),
    ("h2-067", "slack:message", "The Modified Service is descriptive, but the service is contextual.", (("the service", "service", None),)),
    ("h2-068", "slack:message", "Nørd&Co Research Group acquired Δelta Systems.", (("Nørd&Co Research Group", "organization", "organization:nordco"), ("Δelta Systems", "customer", "customer:delta-systems"))),
    ("h2-069", "slack:message", "OK Great Perfect Done — none names an entity.", ()),
    ("h2-070", "slack:message", "INC-1/INC-2 means two incidents separated by slash.", (("INC-1", "incident", "incident:INC-1"), ("INC-2", "incident", "incident:INC-2"))),

    # Batch 08: mixed email/Jira final adversarial set.
    ("h2-071", "email:message", "Legal entity d/b/a Northwind-X signed CONTRACT_51.", (("Northwind-X", "customer", "customer:northwind-x"), ("CONTRACT_51", "contract", "contract:CONTRACT_51"))),
    ("h2-072", "jira:issue", "Sprint Goal: Make Search Better For Everyone", ()),
    ("h2-073", "email:message", "The team “People & Culture — APAC” hired Léa.", (("People & Culture — APAC", "team", "team:people-culture-apac"), ("Léa", "person", "person:lea"))),
    ("h2-074", "jira:issue", "Fix affects com.Example.Widget but not Example sentence text.", (("Example.Widget", "system", "system:example-widget"),)),
    ("h2-075", "email:message", "Forwarded message from THE OFFICE OF GENERAL COUNSEL.", ()),
    ("h2-076", "jira:issue", "Release train Жар-Птица depends on REL_20260717.", (("Жар-Птица", "release", "release:firebird"), ("REL_20260717", "release_id", "release:REL_20260717"))),
    ("h2-077", "email:message", "Meet Jean‑Luc Picard at Résumé.io; note the nonbreaking hyphen.", (("Jean", "person", "person:jean-luc-picard"), ("Luc Picard", "person", "person:jean-luc-picard"), ("Résumé.io", "customer", "customer:resume-io"))),
    ("h2-078", "jira:issue", "The candidate \"Null\" is a named hiring record, unlike null.", (("Null", "candidate", "candidate:null"),)),
    ("h2-079", "email:message", "Quarterly Business Review and Executive Summary are section labels.", ()),
    ("h2-080", "jira:issue", "ops-1️⃣ is decorated text; plain OPS-1 is the issue.", (("OPS-1", "issue", "issue:OPS-1"),)),
)


def _build() -> tuple[tuple[GoldSignal, ...], tuple[GoldMention, ...]]:
    signals: list[GoldSignal] = []
    mentions: list[GoldMention] = []
    for index, (signal_id, source, text, annotations) in enumerate(_CASES):
        signals.append(GoldSignal(signal_id=signal_id, batch_id=f"h2-b{index // 10 + 1:02d}", source_type=source, text=text, slack_context="standalone" if source == "slack:message" else "not_slack"))
        search_from = 0
        for ordinal, (surface, entity_type, referent) in enumerate(annotations, 1):
            try:
                start = text.index(surface, search_from)
            except ValueError as exc:
                raise ValueError(f"{signal_id}: missing ordered surface {surface!r}") from exc
            end = start + len(surface)
            search_from = end
            mentions.append(GoldMention(mention_id=f"{signal_id}-m{ordinal}", signal_id=signal_id, start=start, end=end, entity_type=entity_type, canonical_referent=referent))
    return tuple(signals), tuple(mentions)


SIGNALS, GOLD_MENTIONS = _build()


def corpus_sha256() -> str:
    payload = {"signals": [item.model_dump(mode="json") for item in SIGNALS], "gold_mentions": [item.model_dump(mode="json") for item in GOLD_MENTIONS]}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


# Set only after all cases and exact spans have been frozen, before prediction.
FROZEN_CORPUS_SHA256 = "8b082d2600f4f2739a4165539425f96f6f14294e56549a66b9e28b7f3ec64e91"
