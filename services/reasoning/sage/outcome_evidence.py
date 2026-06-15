"""Evidence outcome event emission for the Sage OutcomeEvaluator."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from services.reasoning.sage.inquiry_traces.repo import OutcomeEventsRepo


@dataclass(frozen=True, slots=True)
class EvidenceOutcomeStats:
    events_emitted: int
    used_evidence_ids: list[UUID]
    omitted_evidence_ids: list[UUID]
    counterevidence_retrieved: int
    counterevidence_in_packet: int
    duplicate_evidence: int


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def evidence_in_packet(
    *,
    source_ref: str,
    source_ref_id: Any,
    packet_strings: set[str],
) -> bool:
    if source_ref and source_ref in packet_strings:
        return True
    if source_ref_id is not None:
        sref = str(source_ref_id)
        if sref in packet_strings:
            return True
    return False


async def emit_evidence_outcome_events(
    *,
    events_repo: OutcomeEventsRepo,
    conn: asyncpg.Connection,
    inquiry_session_id: UUID,
    evidence_items: list[asyncpg.Record],
    omitted_rows: list[asyncpg.Record],
    packet_strings: set[str],
    existing_keys: set[tuple[str, str]],
) -> EvidenceOutcomeStats:
    events_emitted = 0
    used_evidence_ids: list[UUID] = []
    omitted_evidence_ids: list[UUID] = []
    counterevidence_retrieved = 0
    counterevidence_in_packet = 0
    retrieved_source_refs: dict[str, int] = {}

    for item in evidence_items:
        source_ref = str(item["source_ref"])
        retrieved_source_refs[source_ref] = retrieved_source_refs.get(source_ref, 0) + 1
        contradicts = _coerce_list(item.get("contradicts_hypotheses"))
        weakens = _coerce_list(item.get("weakens_hypotheses"))
        is_counterevidence = bool(contradicts) or bool(weakens)
        if is_counterevidence:
            counterevidence_retrieved += 1
        evidence_uuid = item["id"]
        in_packet = evidence_in_packet(
            source_ref=source_ref,
            source_ref_id=item.get("source_ref_id"),
            packet_strings=packet_strings,
        )
        if in_packet:
            used_evidence_ids.append(evidence_uuid)
            if is_counterevidence:
                counterevidence_in_packet += 1
            key = ("retrieved_evidence_used_in_packet", str(evidence_uuid))
            if key not in existing_keys:
                await events_repo.append(
                    inquiry_session_id,
                    "retrieved_evidence_used_in_packet",
                    {
                        "evidence_id": str(evidence_uuid),
                        "source_type": item["source_type"],
                        "source_ref": source_ref,
                    },
                    conn=conn,
                )
                existing_keys.add(key)
                events_emitted += 1
        else:
            omitted_evidence_ids.append(evidence_uuid)
            key = ("retrieved_evidence_omitted", str(evidence_uuid))
            if key not in existing_keys:
                await events_repo.append(
                    inquiry_session_id,
                    "retrieved_evidence_omitted",
                    {
                        "evidence_id": str(evidence_uuid),
                        "source_type": item["source_type"],
                        "source_ref": source_ref,
                        "omission_source": "evidence_items_diff",
                    },
                    conn=conn,
                )
                existing_keys.add(key)
                events_emitted += 1

    seen_omitted_refs = {str(item["source_ref"]) for item in evidence_items}
    for orow in omitted_rows:
        source_ref = str(orow["source_ref"])
        if source_ref in seen_omitted_refs:
            continue
        seen_omitted_refs.add(source_ref)
        key = ("retrieved_evidence_omitted", f"omitted_row:{orow['id']}")
        if key not in existing_keys:
            await events_repo.append(
                inquiry_session_id,
                "retrieved_evidence_omitted",
                {
                    "omitted_evidence_id": str(orow["id"]),
                    "source_type": orow["source_type"],
                    "source_ref": source_ref,
                    "omission_reason": orow["omission_reason"],
                    "omission_source": "omitted_evidence_table",
                },
                conn=conn,
            )
            existing_keys.add(key)
            events_emitted += 1

    duplicate_evidence = sum(
        count - 1 for count in retrieved_source_refs.values() if count > 1
    )
    return EvidenceOutcomeStats(
        events_emitted=events_emitted,
        used_evidence_ids=used_evidence_ids,
        omitted_evidence_ids=omitted_evidence_ids,
        counterevidence_retrieved=counterevidence_retrieved,
        counterevidence_in_packet=counterevidence_in_packet,
        duplicate_evidence=duplicate_evidence,
    )


__all__ = [
    "EvidenceOutcomeStats",
    "emit_evidence_outcome_events",
    "evidence_in_packet",
]
