"""Frozen current-runtime entity holdout for one execution only.

This corpus is organization, entity, time, and text disjoint from the v1-v4
and development populations. It targets the same-batch semantic-isolation
failure class, Slack cross-signal ambiguity, explicit source-local aliases,
weak person/system/project slices, role-grounded typing, and hard negatives.

The corpus must be committed before the pre-call receipt is created. Never
edit it after the receipt or provider execution exists.
"""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid5

NAMESPACE = UUID("d786a3cd-83c8-4484-a60a-38387d04c5bc")

METADATA = {
    "benchmark": "learned-entity-current-runtime-holdout-v5",
    "evidence_class": "precommitted_disjoint_current_runtime_one_shot",
    "allowed_executions": 1,
    "provider_execution_count_at_freeze": 0,
    "batch_only": True,
    "batch_count": 3,
    "signals_per_batch": 8,
    "time_window": "2037-01-01/2037-12-31",
    "split_policy": "organization_entity_time_text_disjoint_from_v1_v2_v3_v4_and_development",
    "canonical_link_claim_permitted": False,
    "implicit_reference_resolution_claim_permitted": False,
}

# source, Slack context, text, complete literal designation and ontology type.
# Each batch has four positives and four hard-negative signals.
_ROWS = (
    # Batch 1: the exact semantic-isolation class. The same literal is a real
    # entity in one focal signal and explicitly non-entity metadata in another.
    ("slack", "thread_reply", "[2037-01-11 #delivery] Dr. Imani Voss moved Project Sable Comet onto system ReedVault.", (("Dr. Imani Voss", "person"), ("Project Sable Comet", "project"), ("system ReedVault", "system"))),
    ("jira", "not_slack", "Project Sable Comet is a schema string here, not a company entity.", ()),
    ("email", "not_slack", "Engineer Pavel Ito assigned system LumenSpoke to Project Moss Circuit.", (("Pavel Ito", "person"), ("system LumenSpoke", "system"), ("Project Moss Circuit", "project"))),
    ("slack", "cross_thread_reference", "[2037-01-13 thread 417] she moved that one; neither literal name appears in this reply.", ()),
    ("slack", "channel_followup", "[2037-01-14 #ops] Team Topaz Heron owns the Kestrel Bloom workstream under Commitment KB-17.", (("Team Topaz Heron", "team"), ("Kestrel Bloom workstream", "workstream"), ("Commitment KB-17", "commitment"))),
    ("jira", "not_slack", "GET /project/Sable-Comet?system=ReedVault returned 404; these are URL parameters.", ()),
    ("email", "not_slack", "customer Northstar Loom adopted product BrambleDesk after Decision BD-42.", (("customer Northstar Loom", "customer"), ("product BrambleDesk", "product"), ("Decision BD-42", "decision"))),
    ("slack", "standalone", "[2037-01-16] `person`, `system`, and `Project.class` are ontology examples only.", ()),

    # Batch 2: explicit aliases and source/transport designators. Aliases are
    # scored as written occurrences; canonical linking is outside this claim.
    ("slack", "temporal_sequence", "[2037-05-02→2037-05-19] Project Ivory Current, called I-Current internally, moved to system DeltaCairn.", (("Project Ivory Current", "project"), ("I-Current", "project"), ("system DeltaCairn", "system"))),
    ("email", "not_slack", "Director Zoë Banerjee approved Goal QUILL-73 for customer Marrow & Finch.", (("Director Zoë Banerjee", "person"), ("Goal QUILL-73", "goal"), ("customer Marrow & Finch", "customer"))),
    ("jira", "not_slack", "Resource AP::771 blocks Project Violet Inlet and system EmberQueue.", (("Resource AP::771", "resource"), ("Project Violet Inlet", "project"), ("system EmberQueue", "system"))),
    ("slack", "cross_channel_temporal", "[2037-05-23 copied from #source-integrations] @I-Current-bot and #delta-cairn are source coordinates only.", ()),
    ("email", "not_slack", "Message-ID: <quill-73@example.invalid>; X-System: DeltaCairn; routing metadata only.", ()),
    ("slack", "thread_reply_delayed", "[2037-05-26 +37h] the director approved it; do not infer a person or project from history.", ()),
    ("jira", "not_slack", "I-Current is a display string in this schema example, not a business entity.", ()),
    ("slack", "standalone", "[2037-05-28] Prof. Samir Qureshi says Team Cobalt Wren supports product FableGrid.", (("Prof. Samir Qureshi", "person"), ("Team Cobalt Wren", "team"), ("product FableGrid", "product"))),

    # Batch 3: same lexical head across roles, weak-type recurrence, code traps,
    # and Slack messages whose context is deliberately insufficient.
    ("slack", "cross_thread_reference", "[2037-09-04 #field] Ana-María Dube deployed system Juniper to Project Juniper Passage.", (("Ana-María Dube", "person"), ("system Juniper", "system"), ("Project Juniper Passage", "project"))),
    ("jira", "not_slack", "customer Juniper Works renewed Commitment JW-9 for the Juniper Passage workstream.", (("customer Juniper Works", "customer"), ("Commitment JW-9", "commitment"), ("Juniper Passage workstream", "workstream"))),
    ("email", "not_slack", "Decision TERN-6 assigns Resource EU/Σ/44 to Team Silver Nacre.", (("Decision TERN-6", "decision"), ("Resource EU/Σ/44", "resource"), ("Team Silver Nacre", "team"))),
    ("slack", "thread_reply", "[2037-09-06] system Juniper is technical here; customer Alder Works is explicitly a different referent.", (("system Juniper", "system"), ("customer Alder Works", "customer"))),
    ("jira", "not_slack", "namespace=project-juniper; pod=system-juniper; owner=person-unknown.", ()),
    ("email", "not_slack", "The words project, system, customer, and person are generic labels in this notice.", ()),
    ("slack", "channel_followup", "[2037-09-09 #field] they renewed it after the deployment; no literal designation is present.", ()),
    ("slack", "standalone", "[2037-09-10] `/api/Juniper`, #juniper-passage, JW-9.log, and @silver-nacre are locators or debug literals.", ()),
)


def _freeze() -> tuple[dict, ...]:
    rows = []
    for index, (source, slack_context, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"v5-signal-{index:03d}"))
        gold = []
        for mention_index, (surface, entity_type) in enumerate(mentions, 1):
            if text.count(surface) != 1:
                raise AssertionError((index, surface, text.count(surface)))
            start = text.index(surface)
            gold.append({
                "mention_id": f"v5-m-{index:03d}-{mention_index}",
                "start": start,
                "end": start + len(surface),
                "surface": surface,
                "entity_type": entity_type,
            })
        rows.append({
            "signal_id": signal_id,
            "batch_id": f"v5-batch-{((index - 1) // 8) + 1}",
            "source_type": source,
            "slack_context": slack_context,
            "text": text,
            "gold": gold,
        })
    return tuple(rows)


FROZEN_CORPUS_V5 = _freeze()


def canonical_bytes_v5() -> bytes:
    return json.dumps(
        {"metadata": METADATA, "corpus": FROZEN_CORPUS_V5},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


FROZEN_SHA256_V5 = "859dfbe64788b07a20721d03cdfac5d673daa6e47ec76b0a744152ae6a0de8ff"


def computed_sha256_v5() -> str:
    return hashlib.sha256(canonical_bytes_v5()).hexdigest()
