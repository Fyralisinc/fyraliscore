"""Sealed untouched holdout v2 for full boundaries and code-type ambiguity."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid5

NAMESPACE = UUID("193f8904-9014-4ca2-9afb-b09fd738b780")
VERSION = "boundary-type-untouched-holdout-v2"
ONE_SHOT_METADATA = {
    "evidence_class": "sealed_untouched_holdout",
    "sealed_before_first_provider_call": True,
    "allowed_provider_executions": 1,
    "provider_execution_count_at_seal": 0,
    "sources": ["slack", "email", "jira"],
    "connector_scope": "excluded_starts_from_persisted_text",
}

# Full written designations include an attached leading or literal workstream
# suffix. Bare code morphology without a stated role is deliberately negative.
_ROWS = (
    ("slack", "[reply +13h] Amber Shoal workstream waits on Gate GX-14.", (("Amber Shoal workstream", "workstream"), ("Gate GX-14", "resource"))),
    ("email", "System Quartz Ferry serves customer Halcyon Foods.", (("System Quartz Ferry", "system"), ("customer Halcyon Foods", "customer"))),
    ("jira", "Workstream Birch Current is owned by Team Meridian.", (("Workstream Birch Current", "workstream"), ("Team Meridian", "team"))),
    ("slack", "Decision SABLE-9 moved Project Lumen Path behind Commitment CP-7.", (("Decision SABLE-9", "decision"), ("Project Lumen Path", "project"), ("Commitment CP-7", "commitment"))),
    ("email", "Product CoralDesk supports Goal NORTH-6.", (("Product CoralDesk", "product"), ("Goal NORTH-6", "goal"))),
    ("jira", "KITE-771 appears in a log; no ticket, goal, or decision role is stated.", ()),
    ("slack", "`deploy(target_id)` and request AB-22 are syntax examples only.", ()),
    ("email", "The migration plan and launch notes use no proper designation.", ()),
    ("jira", "channel=#ops | thread=339 | timestamp=2034-04-08T10:00Z", ()),
    ("slack", "the unnamed system and project owner will respond later", ()),
    ("slack", "[thread +2d] Copper Vale workstream uses system svc-harbor-7.", (("Copper Vale workstream", "workstream"), ("system svc-harbor-7", "system"))),
    ("email", "Contract CN-88 names Customer Palisade Health as buyer.", (("Contract CN-88", "resource"), ("Customer Palisade Health", "customer"))),
    ("jira", "Workstream Nimbus Crossing depends on Dataset DS-41.", (("Workstream Nimbus Crossing", "workstream"), ("Dataset DS-41", "resource"))),
    ("slack", "Engineer Laila Mensah approved Decision OAK-3.", (("Engineer Laila Mensah", "person"), ("Decision OAK-3", "decision"))),
    ("email", "Project Glass Orchard ships through Product EmberSuite.", (("Project Glass Orchard", "project"), ("Product EmberSuite", "product"))),
    ("jira", "TRACE-900 is a correlation value, not a declared business object.", ()),
    ("slack", "Goal review discussed ZX-12, whose role remains unstated.", ()),
    ("email", "Quarterly Rollout status is a generic heading, not a named stream.", ()),
    ("jira", "function migrate_v2() returned code 17 at line 80", ()),
    ("slack", "@channel tomorrow is a transport instruction", ()),
    ("slack", "[cross-thread] Workstream Tidal Bridge carries Commitment WAVE-5.", (("Workstream Tidal Bridge", "workstream"), ("Commitment WAVE-5", "commitment"))),
    ("email", "System Pine Relay is maintained by Team Astrolabe.", (("System Pine Relay", "system"), ("Team Astrolabe", "team"))),
    ("jira", "Ivory Channel workstream is tracked by Resource RC-19.", (("Ivory Channel workstream", "workstream"), ("Resource RC-19", "resource"))),
    ("slack", "Goal VANTAGE-4 guides Project Quiet Lantern.", (("Goal VANTAGE-4", "goal"), ("Project Quiet Lantern", "project"))),
    ("email", "Person Mateo Silva introduced Product DriftNote.", (("Person Mateo Silva", "person"), ("Product DriftNote", "product"))),
    ("jira", "ID-404 is shown without an entity role.", ()),
    ("slack", "the string customer_ref is a schema field", ()),
    ("email", "Migration planning starts soon; nothing has a proper name.", ()),
    ("jira", "GET /v2/workstreams returned 200", ()),
    ("slack", "thread 72 says the unnamed team agreed", ()),
)


def _materialize() -> tuple[dict, ...]:
    rows = []
    for index, (source, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"holdout-v2-{index:02d}"))
        gold = []
        for j, (surface, entity_type) in enumerate(mentions, 1):
            assert text.count(surface) == 1
            start = text.index(surface)
            gold.append({"mention_id": f"bt-h2-{index:02d}-{j}", "start": start,
                "end": start + len(surface), "surface": surface,
                "entity_type": entity_type, "canonical_referent": None})
        rows.append({"signal_id": signal_id,
            "batch_id": f"boundary-type-holdout-v2-batch-{((index-1)//10)+1}",
            "source_type": source, "text": text, "gold": gold})
    return tuple(rows)


FROZEN_CORPUS_V2 = _materialize()


def computed_sha256_v2() -> str:
    raw = json.dumps(FROZEN_CORPUS_V2, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


FROZEN_SHA256_V2 = "95aab10194d5e3e5e10fdadb3abd62f0a8e32c0a003ecb7ad07c1f3fc39bc0aa"
