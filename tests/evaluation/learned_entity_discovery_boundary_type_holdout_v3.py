"""Sealed small holdout v3 for boundary/type and explicit-meta negatives."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid5

VERSION = "boundary-type-untouched-holdout-v3"
NAMESPACE = UUID("bcb77db8-d523-49ab-8840-a39b9e43040d")
ONE_SHOT_METADATA = {"evidence_class": "sealed_untouched_holdout",
    "sealed_before_first_provider_call": True, "allowed_provider_executions": 1,
    "provider_execution_count_at_seal": 0, "batch_size": 10,
    "connector_scope": "excluded_starts_from_persisted_text"}

_ROWS = (
    ("slack", "[reply +6h] Jasper Delta workstream waits on Request RQ-81.", (("Jasper Delta workstream", "workstream"), ("Request RQ-81", "resource"))),
    ("email", "Workstream Opal Causeway uses System svc-orchid-8.", (("Workstream Opal Causeway", "workstream"), ("System svc-orchid-8", "system"))),
    ("jira", "Project Fallow Star contains the Mariner Shift workstream.", (("Project Fallow Star", "project"), ("Mariner Shift workstream", "workstream"))),
    ("slack", "Customer Willow Transit approved Contract CX-42.", (("Customer Willow Transit", "customer"), ("Contract CX-42", "resource"))),
    ("email", "Request LM-30 is an approved customer request owned by Team Saffron.", (("Request LM-30", "resource"), ("Team Saffron", "team"))),
    ("jira", "`request LM-30` and deploy(target) are code examples only.", ()),
    ("slack", "the string customer_ref is a schema field, not a company entity", ()),
    ("email", "QZ-71 is shown without any stated business role.", ()),
    ("jira", "Quarterly Migration is a generic report heading, not a named stream.", ()),
    ("slack", "#ops, thread 19, and tomorrow are transport context only", ()),
)


def _materialize() -> tuple[dict, ...]:
    rows = []
    for index, (source, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"holdout-v3-{index:02d}"))
        gold = []
        for j, (surface, entity_type) in enumerate(mentions, 1):
            assert text.count(surface) == 1
            start = text.index(surface)
            gold.append({"mention_id": f"bt-h3-{index:02d}-{j}", "start": start,
                "end": start + len(surface), "surface": surface,
                "entity_type": entity_type, "canonical_referent": None})
        rows.append({"signal_id": signal_id, "batch_id": "boundary-type-holdout-v3-batch-1",
            "source_type": source, "text": text, "gold": gold})
    return tuple(rows)


FROZEN_CORPUS_V3 = _materialize()


def computed_sha256_v3() -> str:
    return hashlib.sha256(json.dumps(FROZEN_CORPUS_V3, ensure_ascii=False,
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()


FROZEN_SHA256_V3 = "b3e6be475c8d6a2bd22a27aefbfb66858491560264cc1fd987a0e3bf68a10953"
