"""Precommitted broad v4 holdout for batched company-entity discovery.

This population is disjoint from all earlier entity-discovery corpora.  It
mixes source formats, repeated aliases, explicit cross-batch recurrence,
Slack-style unresolved references, type-confusable identifiers, Unicode, and
hard transport/schema negatives.  Freeze this file before its one allowed
provider execution; never edit it after that execution.

The benchmark scores literal mention discovery and role-grounded typing only.
Alias linking and implicit-pronoun resolution are deliberately not claimed.
"""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid5

NAMESPACE = UUID("26b22dc5-79fc-4eed-a54a-34f751a41ccb")
ONTOLOGY_TYPES = frozenset({
    "person", "customer", "product", "project", "team", "system",
    "commitment", "decision", "goal", "resource", "workstream",
})

ONE_SHOT_EVIDENCE_METADATA = {
    "benchmark": "learned-entity-discovery-quality-v4",
    "evidence_class": "precommitted_untouched_broad_holdout",
    "sealed_before_first_provider_call": True,
    "provider_execution_count_at_seal": 0,
    "evidence_status": "not_executed",
    "allowed_provider_executions": 1,
    "split_policy": "organization_entity_time_text_disjoint_from_v1_v2_v3_and_development",
    "time_window": "2034-01-01/2034-12-31",
    "canonical_link_claim_permitted": False,
    "implicit_reference_resolution_claim_permitted": False,
}

# source, Slack context, text, (complete designation, ontology type).
# Every genuine ten-signal batch contains five positives and five negatives.
_ROWS = (
    # Batch 1: first appearances and noisy Slack coordinates.
    ("slack", "thread_reply_delayed", "[2034-01-08 +31h] Renée Okafor says Team Copper Aurora owns the Meridian Drift workstream and Commitment MD-14.", (("Renée Okafor", "person"), ("Team Copper Aurora", "team"), ("Meridian Drift workstream", "workstream"), ("Commitment MD-14", "commitment"))),
    ("jira", "not_slack", "Goal FERN-204 requires Resource EU::504 and the system LatticePort for Project Quiet Delta.", (("Goal FERN-204", "goal"), ("Resource EU::504", "resource"), ("system LatticePort", "system"), ("Project Quiet Delta", "project"))),
    ("email", "not_slack", "customer Serein Foods selected the product AlderPay after Decision AP-31.", (("customer Serein Foods", "customer"), ("product AlderPay", "product"), ("Decision AP-31", "decision"))),
    ("slack", "thread_reply", "[2034-01-09] she approved that one; the name is not present in this reply", ()),
    ("jira", "not_slack", "GET /entities/MD-14?expand=owner returned 404; request_id=workstream-77.", ()),
    ("email", "not_slack", "Message-ID: <fern-204@example.invalid>; X-Project: Quiet-Delta; this is routing metadata.", ()),
    ("slack", "cross_channel_temporal", "[2034-01-12 copied from #sales at T+5h] Guild Winter Saffron introduced customer Tern Vale to product MosaicRun.", (("Guild Winter Saffron", "team"), ("customer Tern Vale", "customer"), ("product MosaicRun", "product"))),
    ("jira", "not_slack", "Assignee: manager | team: platform | objective: improve reliability", ()),
    ("email", "not_slack", "Mateo Živković assigned system CedarRelay to the Umber Lake workstream.", (("Mateo Živković", "person"), ("system CedarRelay", "system"), ("Umber Lake workstream", "workstream"))),
    ("slack", "standalone", "[2034-01-14] `Team`, `customer_id`, and Project.class are schema examples only.", ()),

    # Batch 2: explicit aliases and same-name/different-role pressure.
    ("slack", "temporal_sequence", "[2034-03-01→2034-03-21] Project Paper Lantern, called P-Lantern internally, moved to Team Quartz Finch.", (("Project Paper Lantern", "project"), ("P-Lantern", "project"), ("Team Quartz Finch", "team"))),
    ("jira", "not_slack", "Decision RILL-8 binds Commitment RC-88 to customer New Moon Freight and product KeelNote.", (("Decision RILL-8", "decision"), ("Commitment RC-88", "commitment"), ("customer New Moon Freight", "customer"), ("product KeelNote", "product"))),
    ("email", "not_slack", "Engineer Laila Haddad delivered Resource ME◇72 to the Juniper Treaty workstream for system HushMesh.", (("Laila Haddad", "person"), ("Resource ME◇72", "resource"), ("Juniper Treaty workstream", "workstream"), ("system HushMesh", "system"))),
    ("slack", "cross_thread_reference", "[2034-03-24] thread 88 says the short name from last week still applies", ()),
    ("jira", "not_slack", "P-Lantern is a display string in this schema example, not a company entity.", ()),
    ("email", "not_slack", "Calendar update: Quartz room moved to 14:00; no project name was supplied.", ()),
    ("slack", "channel_followup", "[2034-04-02] same customer as Monday; do not guess from channel membership", ()),
    ("jira", "not_slack", "Project Harbor Glass is sponsored by Émile N'Dour and delivered by the Harbor Glass team.", (("Project Harbor Glass", "project"), ("Émile N'Dour", "person"), ("Harbor Glass team", "team"))),
    ("email", "not_slack", "The customer Copper Aurora approved Goal CA-90; the account is explicitly external here.", (("customer Copper Aurora", "customer"), ("Goal CA-90", "goal"))),
    ("slack", "standalone", "[2034-04-07] @P-Lantern-bot and #harbor-glass are routing coordinates, not identities.", ()),

    # Batch 3: explicit recurrence across batches and code morphology traps.
    ("slack", "cross_thread_reference", "[2034-06-03] Team Copper Aurora renewed Commitment MD-14 for the Meridian Drift workstream.", (("Team Copper Aurora", "team"), ("Commitment MD-14", "commitment"), ("Meridian Drift workstream", "workstream"))),
    ("jira", "not_slack", "Workstream Granite Petal depends on system TIDE-22, Resource US#410, and Commitment GP-6.", (("Workstream Granite Petal", "workstream"), ("system TIDE-22", "system"), ("Resource US#410", "resource"), ("Commitment GP-6", "commitment"))),
    ("email", "not_slack", "Amina El-Sayed introduced customer Pale River Health to product VerityLeaf under Goal VL-10.", (("Amina El-Sayed", "person"), ("customer Pale River Health", "customer"), ("product VerityLeaf", "product"), ("Goal VL-10", "goal"))),
    ("slack", "thread_reply_delayed", "[2034-06-04 +26h] they renamed it again but omitted both names", ()),
    ("jira", "not_slack", "TIDE-22 is a test datum here, not a deployed system or business entity.", ()),
    ("email", "not_slack", "Confidentiality footer: Commitment means an ordinary English word in this notice.", ()),
    ("slack", "cross_channel_temporal", "[2034-06-12 imported from #field at T+9h] customer Willow Arc asked Team Indigo Kite to deploy system FinchGate.", (("customer Willow Arc", "customer"), ("Team Indigo Kite", "team"), ("system FinchGate", "system"))),
    ("jira", "not_slack", "Stack trace: Goal.py:204; pod=decision-8; namespace=project-system.", ()),
    ("email", "not_slack", "Óscar Ibáñez signed Commitment OI-4 for Project Rain Archive after Decision RA-2.", (("Óscar Ibáñez", "person"), ("Commitment OI-4", "commitment"), ("Project Rain Archive", "project"), ("Decision RA-2", "decision"))),
    ("slack", "standalone", "[2034-06-18] 11:30 UTC, T+2h, 0xCA90, and GP-6.log are literals in a debug example.", ()),

    # Batch 4: renames, reused lexical heads, and explicit boundary markers.
    ("slack", "temporal_sequence", "[2034-09-02→2034-09-19] Dr. Anaïs Kovač renamed Project Silver Orchard to Project Ember Orchard.", (("Dr. Anaïs Kovač", "person"), ("Project Silver Orchard", "project"), ("Project Ember Orchard", "project"))),
    ("jira", "not_slack", "Goal DUSK-71 requires product ThreadMint, system Orchard, and Resource NZ::29 for customer Orchard Mutual.", (("Goal DUSK-71", "goal"), ("product ThreadMint", "product"), ("system Orchard", "system"), ("Resource NZ::29", "resource"), ("customer Orchard Mutual", "customer"))),
    ("email", "not_slack", "Decision FLINT-19 assigns the Salt Meadow workstream to Team Orchard under Commitment SO-3.", (("Decision FLINT-19", "decision"), ("Salt Meadow workstream", "workstream"), ("Team Orchard", "team"), ("Commitment SO-3", "commitment"))),
    ("slack", "channel_followup", "[2034-09-21] the project, system, customer, and team above all share a word; this message names none", ()),
    ("jira", "not_slack", "URL=https://orchard.invalid/api/project; token=$TEAM_ORCHARD; build=goal-71.", ()),
    ("email", "not_slack", "Auto-reply: contact the project team through the shared mailbox.", ()),
    ("slack", "thread_reply_delayed", "[2034-10-01 +41h] Prof. Daria Mensah says customer Blue Forge Works adopted product SoftHarbor.", (("Prof. Daria Mensah", "person"), ("customer Blue Forge Works", "customer"), ("product SoftHarbor", "product"))),
    ("jira", "not_slack", "No entity designation was supplied; project-123 is placeholder syntax only.", ()),
    ("email", "not_slack", "Resource EU-Ψ-12 supports Project Cloud Barrow for Team Fallow North.", (("Resource EU-Ψ-12", "resource"), ("Project Cloud Barrow", "project"), ("Team Fallow North", "team"))),
    ("slack", "cross_thread_reference", "[2034-10-05] #archive/MD-14 and `/api/Copper-Aurora` are locators, not company identities.", ()),
)


def _freeze() -> tuple[dict, ...]:
    frozen = []
    for index, (source, slack_context, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"v4-signal-{index:03d}"))
        gold = []
        for mention_index, (surface, entity_type) in enumerate(mentions, 1):
            if text.count(surface) != 1:
                raise AssertionError((index, surface, text.count(surface)))
            start = text.index(surface)
            gold.append({
                "mention_id": f"v4-m-{index:03d}-{mention_index}",
                "start": start, "end": start + len(surface), "surface": surface,
                "entity_type": entity_type, "canonical_referent": None,
            })
        frozen.append({
            "signal_id": signal_id,
            "batch_id": f"v4-batch-{((index - 1) // 10) + 1}",
            "source_type": source, "slack_context": slack_context,
            "text": text, "gold": gold,
        })
    return tuple(frozen)


FROZEN_CORPUS_V4 = _freeze()


def canonical_bytes_v4() -> bytes:
    return json.dumps(
        {"metadata": ONE_SHOT_EVIDENCE_METADATA, "corpus": FROZEN_CORPUS_V4},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


FROZEN_SHA256_V4 = "6bcee8bd5c47ed0dc7e9c20ff1f2606f2ceb93c363261d3cc52f2355183aa6ea"


def computed_sha256_v4() -> str:
    return hashlib.sha256(canonical_bytes_v4()).hexdigest()
