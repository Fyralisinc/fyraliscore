"""Assemble source-agnostic routing signals from settled perception records."""

from __future__ import annotations

import json
import re
from typing import Any

import asyncpg

from .intake import PerceptionOutboxRow
from .routing import RoutingSignal, anchor_is_strong, canonical_ref, lexical_terms


_TOPIC_WORDS = re.compile(
    r"\b(audit|launch|incident|goal|project|initiative|migration|release|review)\b",
    re.IGNORECASE,
)
_PRIMARY_ORDER = {
    "audit": 0, "workstream": 1, "incident": 2, "goal": 3, "project": 4,
    "initiative": 5, "milestone": 6, "customer": 7, "service": 8,
    "software_system": 9, "repository": 10, "work_item": 11,
    "topic_phrase": 12,
}


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


def _normalize_phrase(value: str) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", value.lower()))


def _dedupe_refs(values: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    by_key = {canonical_ref(value): value for value in values if value}
    return tuple(by_key[key] for key in sorted(by_key))


class EpisodeSignalAssembler:
    async def assemble(
        self, item: PerceptionOutboxRow, *, conn: asyncpg.Connection
    ) -> RoutingSignal:
        row = await conn.fetchrow(
            """
            SELECT o.ingested_at, o.content, o.content_text, o.entities_mentioned,
                   e.source, e.installation_scope, e.thread_id, e.parent_ref,
                   e.container_ref, e.source_object_type, e.source_object_id,
                   s.manifest,
                   k.claim_ids AS settled_claim_ids,
                   k.snapshot_hash AS settled_knowledge_snapshot_hash,
                   k.claim_set_hash AS settled_claim_set_hash
              FROM observations o
              JOIN source_evidence e
                ON e.tenant_id=o.tenant_id AND e.id=o.evidence_id
              JOIN identity_resolution_snapshots s
                ON s.tenant_id=o.tenant_id AND s.id=$4
              JOIN perception_knowledge_snapshots k
                ON k.tenant_id=o.tenant_id AND k.id=$6
               AND k.observation_id=o.id
               AND k.observation_occurred_at=o.occurred_at
               AND k.evidence_id=o.evidence_id
               AND k.identity_snapshot_id=s.id
              WHERE o.tenant_id=$1 AND o.id=$2 AND o.occurred_at=$3
               AND o.evidence_id=$5
            """,
            item.tenant_id, item.observation_id, item.observation_occurred_at,
            item.identity_snapshot_id, item.evidence_id,
            item.knowledge_snapshot_id,
        )
        if row is None:
            raise ValueError("episode intake lineage is stale or incomplete")
        content = _json(row["content"])
        hints = _json(row["entities_mentioned"])
        manifest = _json(row["manifest"])

        anchors: list[dict[str, Any]] = []
        participants: list[dict[str, Any]] = []
        for hint in hints if isinstance(hints, list) else ():
            if not isinstance(hint, dict) or not hint.get("type") or hint.get("id") in (None, ""):
                continue
            ref = {"type": str(hint["type"]), "id": str(hint["id"])}
            if ref["type"] in {"actor", "person", "person_name"}:
                participants.append(ref)
            else:
                anchors.append(ref)

        assertion_ids = []
        mention_ids = []
        for snapshot_item in manifest.get("items", []):
            if not isinstance(snapshot_item, dict):
                continue
            if snapshot_item.get("assertion_id"):
                assertion_ids.append(snapshot_item["assertion_id"])
            if snapshot_item.get("mention_id"):
                mention_ids.append(snapshot_item["mention_id"])
            selected = snapshot_item.get("selected_ref")
            if not isinstance(selected, dict):
                continue
            if selected.get("type") in {"actor", "person"}:
                participants.append(selected)
            elif selected.get("type") != "source_reference":
                anchors.append(selected)

        explicit_topics = content.get("_episode_topics", []) if isinstance(content, dict) else []
        for phrase in explicit_topics if isinstance(explicit_topics, list) else ():
            if isinstance(phrase, str) and _normalize_phrase(phrase):
                anchors.append({"type": "topic_phrase", "id": _normalize_phrase(phrase)})

        unresolved = content.get("_unresolved_phrases", []) if isinstance(content, dict) else []
        for phrase in unresolved if isinstance(unresolved, list) else ():
            if isinstance(phrase, str) and _TOPIC_WORDS.search(phrase):
                anchors.append({"type": "topic_phrase", "id": _normalize_phrase(phrase)})

        if (
            row["settled_knowledge_snapshot_hash"] != item.knowledge_snapshot_hash
            or row["settled_claim_set_hash"] != item.claim_set_hash
        ):
            raise ValueError("episode intake knowledge snapshot lineage is stale")
        settled_claim_ids = tuple(row["settled_claim_ids"])
        claim_rows = await conn.fetch(
            """
            SELECT id, subject_ref, predicate FROM perception_claims
             WHERE tenant_id=$1 AND id=ANY($2::uuid[])
             ORDER BY id
            """,
            item.tenant_id, list(settled_claim_ids),
        )
        if len(claim_rows) != len(settled_claim_ids):
            raise ValueError("knowledge snapshot references missing claims")
        claim_ids = tuple(record["id"] for record in claim_rows)
        predicates = tuple(sorted({str(record["predicate"]) for record in claim_rows}))
        for claim in claim_rows:
            subject = _json(claim["subject_ref"])
            if isinstance(subject, dict) and subject:
                if subject.get("type") in {"actor", "person"}:
                    participants.append(subject)
                else:
                    anchors.append(subject)

        structure_keys: list[str] = []
        if row["thread_id"]:
            structure_keys.append(
                f"{row['source']}:{row['installation_scope']}:thread:{row['thread_id']}"
            )
        for name in ("parent_ref", "container_ref"):
            ref = _json(row[name])
            if ref:
                structure_keys.append(
                    f"{row['source']}:{row['installation_scope']}:{name}:"
                    + json.dumps(ref, sort_keys=True, separators=(",", ":"))
                )
        if structure_keys:
            anchors.extend(
                {"type": "source_structure", "id": value} for value in structure_keys
            )

        refs = _dedupe_refs(anchors)
        strong = sorted(
            (ref for ref in refs if anchor_is_strong(ref)),
            key=lambda ref: (_PRIMARY_ORDER.get(str(ref.get("type")), 100), canonical_ref(ref)),
        )
        primary = strong[0] if strong else {
            "type": "observation_seed", "id": str(item.observation_id)
        }
        if not refs:
            refs = (primary,)
        label = str(primary.get("id") or "episode").replace("-", " ").strip()
        return RoutingSignal(
            tenant_id=item.tenant_id,
            observation_id=item.observation_id,
            evidence_id=item.evidence_id,
            identity_snapshot_id=item.identity_snapshot_id,
            knowledge_snapshot_id=item.knowledge_snapshot_id,
            knowledge_snapshot_hash=item.knowledge_snapshot_hash,
            claim_set_hash=item.claim_set_hash,
            occurred_at=item.observation_occurred_at,
            ingested_at=row["ingested_at"],
            source=str(row["source"]),
            installation_scope=str(row["installation_scope"]),
            content_text=str(row["content_text"]),
            primary_anchor=primary,
            anchor_refs=refs,
            participant_refs=_dedupe_refs(participants),
            claim_ids=claim_ids,
            identity_assertion_ids=tuple(sorted(set(assertion_ids))),
            claim_predicates=predicates,
            lexical_terms=lexical_terms(str(row["content_text"])),
            structure_keys=tuple(sorted(set(structure_keys))),
            topic_label=label,
            explicit_topic=bool(explicit_topics),
        )


__all__ = ["EpisodeSignalAssembler"]
