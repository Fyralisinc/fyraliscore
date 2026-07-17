"""Versioned mutable corpus for boundary and code-type policy development.

This is inspected development feedback, never sealed/generalization evidence.
It starts from normalized persisted Slack, email, and Jira text; connectors are
outside its scope.
"""

from __future__ import annotations

from uuid import UUID, uuid5

DEVELOPMENT_ONLY = True
EVIDENCE_CLASS = "development_feedback_only_not_generalization_evidence"
VERSION = "boundary-type-development-v1"
NAMESPACE = UUID("f6ab3d2b-774c-49e0-a9f0-b96e97c26b73")

# source, text, ((surface, type), ...). Two genuine mixed-source batches.
_ROWS = (
    ("slack", "[reply +9h] Cinder Atlas workstream is blocked by Gate Q-17.", (("Cinder Atlas workstream", "workstream"), ("Gate Q-17", "resource"))),
    ("email", "The Obsidian Meadow migration moves through system lake-router-4.", (("Obsidian Meadow migration", "workstream"), ("lake-router-4", "system"))),
    ("jira", "Workstream Kestrel North depends on Goal ORBIT-52.", (("Kestrel North", "workstream"), ("Goal ORBIT-52", "goal"))),
    ("slack", "Silver Current rollout remains owned by Team Lark.", (("Silver Current rollout", "workstream"), ("Team Lark", "team"))),
    ("email", "The named transition Cedar Passage starts Monday.", (("Cedar Passage", "workstream"),)),
    ("jira", "RUNE-310 blocked delivery; its record type is not stated.", (("RUNE-310", "other"),)),
    ("slack", "Goal VELA-8 moved; TRACE-44 is only a request identifier.", (("Goal VELA-8", "goal"),)),
    ("email", "Decision PEAR-4 supersedes the old option.", (("Decision PEAR-4", "decision"),)),
    ("jira", "Quarterly Migration launch notes are generic planning text; request_id=0af3.", ()),
    ("slack", "#delivery and thread 881 are transport coordinates.", ()),
    ("slack", "[thread follow-up] Morrow Reef workstream now uses svc-tide-02.", (("Morrow Reef workstream", "workstream"), ("svc-tide-02", "system"))),
    ("email", "Crimson Harbor launch is waiting on Contract CT-91.", (("Crimson Harbor launch", "workstream"), ("Contract CT-91", "resource"))),
    ("jira", "Project Paper Kite contains the Blue Lantern migration.", (("Project Paper Kite", "project"), ("Blue Lantern migration", "workstream"))),
    ("slack", "Aurora Relay transition belongs to Horizon Guild.", (("Aurora Relay transition", "workstream"), ("Horizon Guild", "team"))),
    ("email", "Workstream Solar Thread is separate from product Cloud Loom.", (("Solar Thread", "workstream"), ("Cloud Loom", "product"))),
    ("jira", "Goal review moved; AX-19 is present but has no declared role.", (("AX-19", "other"),)),
    ("slack", "system Riverlock-Prime received traffic.", (("Riverlock-Prime", "system"),)),
    ("email", "Commitment MC-41 remains active.", (("Commitment MC-41", "commitment"),)),
    ("jira", "customer_id and project_ref are schema fields, not entities.", ()),
    ("slack", "Yesterday and the unnamed owner are not company entities.", ()),
)


def _materialize() -> tuple[dict, ...]:
    rows = []
    for index, (source, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"signal-{index:02d}"))
        gold = []
        for mention_index, (surface, entity_type) in enumerate(mentions, 1):
            assert text.count(surface) == 1
            start = text.index(surface)
            gold.append({
                "mention_id": f"bt-v1-{index:02d}-{mention_index}",
                "start": start, "end": start + len(surface), "surface": surface,
                "entity_type": entity_type, "canonical_referent": None,
            })
        rows.append({
            "signal_id": signal_id,
            "batch_id": f"boundary-type-v1-batch-{1 if index <= 10 else 2}",
            "source_type": source, "text": text, "gold": gold,
        })
    return tuple(rows)


DEVELOPMENT_CORPUS = _materialize()
