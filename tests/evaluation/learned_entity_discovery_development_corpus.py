"""Mutable development corpus for learned batched entity discovery.

This is prompt-development material, not a sealed holdout.  Its annotations may
be inspected while changing prompts and extraction policy, so results on it are
training feedback only and MUST NOT be reported as generalization evidence.

The fixture starts after ingestion with normalized, persisted signal text.  It
contains four genuine ten-signal batches and evaluator-owned exact spans/types.
Canonical linking is deliberately outside this extraction-development corpus.
"""

from __future__ import annotations

import json
from uuid import UUID, uuid5

DEVELOPMENT_ONLY = True
EVIDENCE_CLASS = "development_feedback_only_not_generalization_evidence"
NAMESPACE = UUID("b9f0d49d-7b2a-42d1-a349-31d406a91890")

# source, Slack stratum, normalized text, (literal surface, ontology type).
# The texts intentionally differ from both frozen learned-discovery holdouts.
_ROWS = (
    # Batch 1: completeness, minimal boundaries, and delayed Slack context.
    ("slack", "thread_reply_delayed", "[reply +31h] Nadia Okonkwo says Team Lark still owns Project Paper Kite and CMT-K9.", (("Nadia Okonkwo", "person"), ("Team Lark", "team"), ("Project Paper Kite", "project"), ("CMT-K9", "commitment"))),
    ("slack", "cross_thread_reference", "Quoting #renewals/204: Verdant Railway chose LoomOS; Decision PEAR-4 is pending.", (("Verdant Railway", "customer"), ("LoomOS", "product"), ("Decision PEAR-4", "decision"))),
    ("jira", "not_slack", "BK-2041 assigns Imani Clarke to the Silver Current workstream on svc-river-02.", (("BK-2041", "resource"), ("Imani Clarke", "person"), ("Silver Current", "workstream"), ("svc-river-02", "system"))),
    ("email", "not_slack", "Subject: objective update\nGoal VELA-8 now belongs to Juniper Forecasting.", (("Goal VELA-8", "goal"), ("Juniper Forecasting", "team"))),
    ("slack", "temporal_sequence", "[Day 2→Day 9] Aruna Mehta moved the Cobalt Passage project behind Gate G-17.", (("Aruna Mehta", "person"), ("Cobalt Passage", "project"), ("Gate G-17", "resource"))),
    ("slack", "thread_reply", "the owner mentioned earlier can handle it after lunch", ()),
    ("jira", "not_slack", "Priority: urgent | state: reopened | owner: unassigned | estimate: unknown", ()),
    ("email", "not_slack", "This automated footer contains no customer or project name.", ()),
    ("slack", "standalone", "@channel tomorrow at 15:00 works; add a reaction if unavailable", ()),
    ("slack", "channel_followup", "same decision as yesterday, except the unnamed option was removed", ()),

    # Batch 2: Unicode, codes, punctuation, and dense multi-mention signals.
    ("slack", "cross_channel_temporal", "[copied from #field at T+5h] Équipe Sûreté asked Clínica del Lago to test `π-router@v4`.", (("Équipe Sûreté", "team"), ("Clínica del Lago", "customer"), ("π-router@v4", "system"))),
    ("jira", "not_slack", "ΔPRJ/77: Zoë van Dijk linked the Crème Brûlée launch to Dataset Ω::12.", (("ΔPRJ/77", "project"), ("Zoë van Dijk", "person"), ("Crème Brûlée", "workstream"), ("Dataset Ω::12", "resource"))),
    ("email", "not_slack", "Łukasz Żmuda confirmed Commitment Ł-22 for Żuraw Analytics and Północ CRM.", (("Łukasz Żmuda", "person"), ("Commitment Ł-22", "commitment"), ("Żuraw Analytics", "customer"), ("Północ CRM", "product"))),
    ("slack", "thread_reply_delayed", "[下一天 07:10] 王芳 approved Decision 火-6 for Project 星河.", (("王芳", "person"), ("Decision 火-6", "decision"), ("Project 星河", "project"))),
    ("slack", "temporal_sequence", "[week 4] Daði Jónsson closed Objective Þ-3 for Reykjavík Signals.", (("Daði Jónsson", "person"), ("Objective Þ-3", "goal"), ("Reykjavík Signals", "product"))),
    ("jira", "not_slack", "Trace: foo.bar():71 | request=0af3 | status=failed | retries=3", ()),
    ("email", "not_slack", "Auto-response: this mailbox is checked only during ordinary office hours.", ()),
    ("slack", "standalone", "`if (ready) return true;` is an example, not a named system", ()),
    ("slack", "thread_reply", "they renamed it, but nobody included either the old or new name", ()),
    ("slack", "channel_followup", "2027-02-03T11:22Z is only the timestamp from the prior message", ()),

    # Batch 3: minimal exact boundaries and Slack's distributed semantics.
    ("slack", "thread_reply_delayed", "[18h later, same incident] Dr. Salma Haddad transferred Aurora Relay to Cedar Response.", (("Dr. Salma Haddad", "person"), ("Aurora Relay", "system"), ("Cedar Response", "team"))),
    ("slack", "cross_thread_reference", "Thread 771 records North Pier Foods under the product called MicaDesk, not ‘the dashboard’.", (("North Pier Foods", "customer"), ("MicaDesk", "product"))),
    ("jira", "not_slack", "Workstream Kestrel North depends on DEC-Z31 and Resource EU#408.", (("Kestrel North", "workstream"), ("DEC-Z31", "decision"), ("Resource EU#408", "resource"))),
    ("email", "not_slack", "Marta de la Cruz introduced Blue Quarry Bank to Initiative Sundial.", (("Marta de la Cruz", "person"), ("Blue Quarry Bank", "customer"), ("Initiative Sundial", "project"))),
    ("slack", "cross_channel_temporal", "[from #strategy, three days old] Goal MAPLE-2 commits Horizon Guild to Commitment C/88.", (("Goal MAPLE-2", "goal"), ("Horizon Guild", "team"), ("Commitment C/88", "commitment"))),
    ("slack", "standalone", "the engineering team and account lead are discussing a possible codename", ()),
    ("jira", "not_slack", "Acceptance criteria: observable, recoverable, documented, and cheap to operate", ()),
    ("email", "not_slack", "----- forwarded content removed -----\nPlease ask the previous sender.", ()),
    ("slack", "thread_reply", "not Project — I mean the generic project field in the form", ()),
    ("slack", "channel_followup", "✅ agreed; nothing here gives the proposal a proper name", ()),

    # Batch 4: repeated contextual pressure, multilingual names, hard negatives.
    ("slack", "temporal_sequence", "[T0→T+48h] Nguyễn An renamed Project Red Lantern to Crimson Passage.", (("Nguyễn An", "person"), ("Project Red Lantern", "project"), ("Crimson Passage", "project"))),
    ("slack", "cross_thread_reference", "#legal/991 says Société Mistral accepted Contract CT-λ9 for Cloud Loom.", (("Société Mistral", "customer"), ("Contract CT-λ9", "resource"), ("Cloud Loom", "product"))),
    ("jira", "not_slack", "INC-К7 moved from Nimbus Watch to Команда Восток, according to Мария Орлова.", (("INC-К7", "resource"), ("Nimbus Watch", "system"), ("Команда Восток", "team"), ("Мария Орлова", "person"))),
    ("email", "not_slack", "Amara Ndlovu approved Decision BAOBAB-5 for Objective SUN-21 and the Solar Thread workstream.", (("Amara Ndlovu", "person"), ("Decision BAOBAB-5", "decision"), ("Objective SUN-21", "goal"), ("Solar Thread", "workstream"))),
    ("slack", "thread_reply_delayed", "[Sunday +26h] Tidal Works promised Commitment WAVE-14 to Koru Medical.", (("Tidal Works", "team"), ("Commitment WAVE-14", "commitment"), ("Koru Medical", "customer"))),
    ("slack", "standalone", "the string customer_id is a schema field and does not name a customer", ()),
    ("jira", "not_slack", "Environment: dev | browser: unspecified | reproduction: intermittent", ()),
    ("email", "not_slack", "Confidentiality notice: delete this message if it reached you by mistake.", ()),
    ("slack", "thread_reply", "I meant the second unnamed service, not the first unnamed one", ()),
    ("slack", "channel_followup", "Friday is a date, the manager is a role, and neither is a company entity", ()),
)


def _materialize() -> tuple[dict, ...]:
    rows: list[dict] = []
    for index, (source, slack_context, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"development-signal-{index:03d}"))
        gold = []
        for mention_index, (surface, entity_type) in enumerate(mentions, 1):
            if text.count(surface) != 1:
                raise AssertionError((index, surface, text.count(surface)))
            start = text.index(surface)
            gold.append({
                "mention_id": f"dev-m-{index:03d}-{mention_index}",
                "start": start,
                "end": start + len(surface),
                "surface": surface,
                "entity_type": entity_type,
                "canonical_referent": None,
            })
        rows.append({
            "signal_id": signal_id,
            "batch_id": f"development-batch-{((index - 1) // 10) + 1}",
            "source_type": source,
            "slack_context": slack_context,
            "text": text,
            "gold": gold,
        })
    return tuple(rows)


DEVELOPMENT_CORPUS = _materialize()


def offline_structured_response(batch_id: str) -> dict:
    """Return evaluator gold in provider-schema form for harness development.

    This is deliberately a gold-replaying fake response, not a model prediction.
    """

    rows = [row for row in DEVELOPMENT_CORPUS if row["batch_id"] == batch_id]
    if not rows:
        raise KeyError(batch_id)
    return {"mentions": [
        {
            "signal_id": row["signal_id"],
            "surface": mention["surface"],
            "span_start": mention["start"],
            "span_end": mention["end"],
            "entity_type": mention["entity_type"],
            "confidence": 0.99,
            "abstain": False,
        }
        for row in rows for mention in row["gold"]
    ]}


def canonical_development_bytes() -> bytes:
    """Stable bytes for detecting accidental fixture drift, never for sealing."""

    return json.dumps(
        DEVELOPMENT_CORPUS, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
