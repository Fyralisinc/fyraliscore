"""Sealed v3 holdout for learned company-entity mention discovery.

This fixture is organization-, entity-, time-, and text-disjoint from the v1,
v2, and mutable development corpora.  It was sealed before any provider call.
Do not edit the corpus, metadata, or digest after the first provider execution.
Canonical linking remains outside this exact-span/type benchmark.
"""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid5

NAMESPACE = UUID("df8bbb48-99db-4df2-b903-79aa7f761b61")
ONTOLOGY_TYPES = frozenset({
    "person", "customer", "product", "project", "team", "system",
    "commitment", "decision", "goal", "resource", "workstream",
})

ONE_SHOT_EVIDENCE_METADATA = {
    "benchmark": "learned-entity-discovery-quality-v3",
    "evidence_class": "sealed_untouched_holdout",
    "sealed_before_first_provider_call": True,
    "provider_execution_count_at_seal": 0,
    "evidence_status": "not_executed",
    "allowed_provider_executions": 1,
    "split_policy": "organization_entity_time_text_disjoint_from_v1_v2_and_development",
    "time_window": "2031-01-01/2032-12-31",
    "canonical_link_claim_permitted": False,
}

# source, Slack context, text, (complete designation, ontology type).
# Every batch contains five positives and five hard negatives.
_ROWS = (
    # Batch 1
    ("slack", "thread_reply_delayed", "[2031-01-14 +22h] Priya Seneviratne confirmed Team Moonwake owns Project Ivory Current and Commitment MC-41.", (("Priya Seneviratne", "person"), ("Team Moonwake", "team"), ("Project Ivory Current", "project"), ("Commitment MC-41", "commitment"))),
    ("jira", "not_slack", "RUNE-310: the Obsidian Meadow workstream moves Resource RM::73 into the Helioframe system.", (("RUNE-310", "goal"), ("Obsidian Meadow workstream", "workstream"), ("Resource RM::73", "resource"), ("Helioframe system", "system"))),
    ("email", "not_slack", "Subject: 2031 renewal\nSelkie Maritime selected the product BrightLedger after Decision BL-9.", (("Selkie Maritime", "customer"), ("product BrightLedger", "product"), ("Decision BL-9", "decision"))),
    ("slack", "cross_thread_reference", "[2031-02-03] #delivery/618 says the owner and account lead will decide later.", ()),
    ("jira", "not_slack", "GET /v3/accounts?limit=50 returned 429; request_id=ab91; environment=stage-7.", ()),
    ("email", "not_slack", "Message-ID: <20310204.771@example.invalid>\nThis mailbox is unattended.", ()),
    ("slack", "cross_channel_temporal", "[2031-02-11 copied from #council at T+7h] Guild Amberglass asked Northstar Orchards to trial the product VellumArc.", (("Guild Amberglass", "team"), ("Northstar Orchards", "customer"), ("product VellumArc", "product"))),
    ("jira", "not_slack", "Acceptance criteria: retryable, measurable, reversible, and documented.", ()),
    ("email", "not_slack", "Marisol Echevarría assigned the system CloudHarbor X to Project Saffron Meridian.", (("Marisol Echevarría", "person"), ("system CloudHarbor X", "system"), ("Project Saffron Meridian", "project"))),
    ("slack", "thread_reply", "[2031-02-19] yes, that unnamed option from above—not the other one", ()),

    # Batch 2
    ("slack", "temporal_sequence", "[2031-04-01→2031-04-18] Kwame Adusei moved Goal ORBIT-52 to the Cinder Atlas workstream.", (("Kwame Adusei", "person"), ("Goal ORBIT-52", "goal"), ("Cinder Atlas workstream", "workstream"))),
    ("jira", "not_slack", "Decision IRIS-204 binds Commitment LANTERN-8 to customer Fjord & Fable and product NacreFlow.", (("Decision IRIS-204", "decision"), ("Commitment LANTERN-8", "commitment"), ("customer Fjord & Fable", "customer"), ("product NacreFlow", "product"))),
    ("email", "not_slack", "Engineer Hanae Mori delivered Resource JP◇611 to Team Cedar Comet for the system KairoMesh.", (("Hanae Mori", "person"), ("Resource JP◇611", "resource"), ("Team Cedar Comet", "team"), ("system KairoMesh", "system"))),
    ("slack", "standalone", "[2031-04-22] `customer_ref`, `/ops/replay`, and trace-883 are syntax, not names.", ()),
    ("jira", "not_slack", "Assignee: none | component: platform | sprint: 31 | resolution: unresolved", ()),
    ("email", "not_slack", "Calendar update: the recurring meeting moved by fifteen minutes.", ()),
    ("slack", "channel_followup", "[2031-05-02] same unnamed plan as Monday, with the third bullet removed", ()),
    ("jira", "not_slack", "Project Glass Tern is sponsored by Élodie Marchal and executed by the Signal Loom team.", (("Project Glass Tern", "project"), ("Élodie Marchal", "person"), ("Signal Loom team", "team"))),
    ("email", "not_slack", "The customer Red Clay Transit approved Goal PATH-6 for the product PalisadeIQ.", (("customer Red Clay Transit", "customer"), ("Goal PATH-6", "goal"), ("product PalisadeIQ", "product"))),
    ("slack", "thread_reply", "[2031-05-09] @here and #general are routing tokens; neither names a company object.", ()),

    # Batch 3
    ("slack", "cross_thread_reference", "[2032-01-07] Thread 904 quotes Decision QUILL-17: Project Lunar Kelp transfers to Team Foxglove Circuit.", (("Decision QUILL-17", "decision"), ("Project Lunar Kelp", "project"), ("Team Foxglove Circuit", "team"))),
    ("jira", "not_slack", "Workstream Granite Echo depends on system Riverlock-Prime, Resource CA#902, and Commitment GE-5.", (("Workstream Granite Echo", "workstream"), ("system Riverlock-Prime", "system"), ("Resource CA#902", "resource"), ("Commitment GE-5", "commitment"))),
    ("email", "not_slack", "Sofía Quispe introduced customer Marimba Health to the product WrenSuite under Goal SOL-11.", (("Sofía Quispe", "person"), ("customer Marimba Health", "customer"), ("product WrenSuite", "product"), ("Goal SOL-11", "goal"))),
    ("slack", "thread_reply_delayed", "[2032-01-12 +35h] they changed the second value from six to seven", ()),
    ("jira", "not_slack", "Stack trace: worker.ts:117; path=/internal/flush; pod=worker-5; status=failed.", ()),
    ("email", "not_slack", "Confidentiality footer: discard this message if received in error.", ()),
    ("slack", "cross_channel_temporal", "[2032-01-21 imported from #field at T+11h] customer Oriel Water asked Team Juniper Bell to deploy the system PrismDock.", (("customer Oriel Water", "customer"), ("Team Juniper Bell", "team"), ("system PrismDock", "system"))),
    ("jira", "not_slack", "Severity: medium; browser: current; reproduction rate: intermittent.", ()),
    ("email", "not_slack", "Nkem Chukwu signed Commitment NEBULA-3 for Project Bronze Estuary and Decision BE-44.", (("Nkem Chukwu", "person"), ("Commitment NEBULA-3", "commitment"), ("Project Bronze Estuary", "project"), ("Decision BE-44", "decision"))),
    ("slack", "standalone", "[2032-02-02] 10:30 UTC, T+4h, and 0x91fe are temporal or transport literals.", ()),

    # Batch 4
    ("slack", "temporal_sequence", "[2032-08-03→2032-08-27] Aroha Te Rangi renamed Project Silver Koru to Project Dawn Fern.", (("Aroha Te Rangi", "person"), ("Project Silver Koru", "project"), ("Project Dawn Fern", "project"))),
    ("jira", "not_slack", "Goal CYPRESS-70 requires the product EmberMint, system TernGrid, and Resource NZ::18 for customer Kestrel Mutual.", (("Goal CYPRESS-70", "goal"), ("product EmberMint", "product"), ("system TernGrid", "system"), ("Resource NZ::18", "resource"), ("customer Kestrel Mutual", "customer"))),
    ("email", "not_slack", "Decision SABLE-12 assigns the Morrow Reef workstream to Team Alpine Thread under Commitment AT-90.", (("Decision SABLE-12", "decision"), ("Morrow Reef workstream", "workstream"), ("Team Alpine Thread", "team"), ("Commitment AT-90", "commitment"))),
    ("slack", "channel_followup", "[2032-09-01] the role called manager is generic, and Friday is only a date", ()),
    ("jira", "not_slack", "URL=https://invalid.test/api/v9; build=nightly-44; token=$REDACTED.", ()),
    ("email", "not_slack", "Auto-reply: away until next week; contact the shared inbox for urgent requests.", ()),
    ("slack", "thread_reply_delayed", "[2032-09-18 +29h] Dr. Irena Vuković says customer Blue Heron Foundry adopted the product Mossline.", (("Dr. Irena Vuković", "person"), ("customer Blue Heron Foundry", "customer"), ("product Mossline", "product"))),
    ("jira", "not_slack", "No entity designation was supplied; the description remains intentionally blank.", ()),
    ("email", "not_slack", "Resource EU-Λ-77 supports Project Opal Wind for Team Dovetail North.", (("Resource EU-Λ-77", "resource"), ("Project Opal Wind", "project"), ("Team Dovetail North", "team"))),
    ("slack", "cross_thread_reference", "[2032-10-05] #archive/77 is a locator and `/api/owner` is a path, not an identity.", ()),
)


def _freeze() -> tuple[dict, ...]:
    frozen = []
    for index, (source, slack_context, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"v3-signal-{index:03d}"))
        gold = []
        for mention_index, (surface, entity_type) in enumerate(mentions, 1):
            if text.count(surface) != 1:
                raise AssertionError((index, surface, text.count(surface)))
            start = text.index(surface)
            gold.append({
                "mention_id": f"v3-m-{index:03d}-{mention_index}",
                "start": start, "end": start + len(surface), "surface": surface,
                "entity_type": entity_type, "canonical_referent": None,
            })
        frozen.append({
            "signal_id": signal_id,
            "batch_id": f"v3-batch-{((index - 1) // 10) + 1}",
            "source_type": source, "slack_context": slack_context,
            "text": text, "gold": gold,
        })
    return tuple(frozen)


FROZEN_CORPUS_V3 = _freeze()
FROZEN_SHA256_V3 = "e6d5821399403feeac727253f791a8bb0d98d1c42232376c3b30305f00a43bc4"


def canonical_bytes_v3() -> bytes:
    return json.dumps(
        {"metadata": ONE_SHOT_EVIDENCE_METADATA, "corpus": FROZEN_CORPUS_V3},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def computed_sha256_v3() -> str:
    return hashlib.sha256(canonical_bytes_v3()).hexdigest()
