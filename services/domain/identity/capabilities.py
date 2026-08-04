"""Source-grounded identity capabilities.

This registry is intentionally conservative: it describes identity evidence
the current connectors actually emit, not every entity a company may contain.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


SourceFamily = Literal[
    "communication",
    "knowledge",
    "work",
    "meeting",
    "operations",
    "people",
    "finance",
]
SemanticMaturity = Literal["rich", "generic"]
Admission = Literal["canonical", "conditional", "contextual_only"]


class SourceIdentityCapability(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: str
    family: SourceFamily
    semantic_maturity: SemanticMaturity
    native_reference_types: tuple[str, ...]
    canonical_candidates: tuple[str, ...] = ()


def _cap(
    source: str,
    family: SourceFamily,
    maturity: SemanticMaturity,
    refs: tuple[str, ...],
    candidates: tuple[str, ...] = (),
) -> SourceIdentityCapability:
    return SourceIdentityCapability(
        source=source,
        family=family,
        semantic_maturity=maturity,
        native_reference_types=refs,
        canonical_candidates=candidates,
    )


SOURCE_IDENTITY_CAPABILITIES: dict[str, SourceIdentityCapability] = {
    item.source: item
    for item in (
        _cap("slack", "communication", "rich", ("message", "user", "channel", "thread", "url"), ("person",)),
        _cap("gmail", "communication", "generic", ("message", "thread", "email_address"), ("person",)),
        _cap("discord", "communication", "generic", ("message", "user", "channel"), ("person",)),
        _cap("telegram", "communication", "generic", ("message", "user", "chat"), ("person",)),
        _cap("signal", "communication", "generic", ("message", "user", "conversation"), ("person",)),
        _cap("whatsapp", "communication", "rich", ("message", "user", "status"), ("person",)),
        _cap("notion", "knowledge", "rich", ("page", "block", "comment", "database", "user"), ("person", "document", "work_item")),
        _cap("google_drive", "knowledge", "generic", ("file",), ("document",)),
        _cap("miro", "knowledge", "generic", ("item", "board"), ("document",)),
        _cap("figma", "knowledge", "generic", ("event", "file", "version"), ("document",)),
        _cap("jira", "work", "generic", ("issue",), ("work_item",)),
        _cap("github", "work", "generic", ("event",), ("work_item", "repository")),
        _cap("google_calendar", "meeting", "generic", ("event", "email_address"), ("meeting", "person")),
        _cap("fireflies", "meeting", "generic", ("transcript",), ("meeting",)),
        _cap("aws", "operations", "generic", ("event",), ("software_system",)),
        _cap("grafana", "operations", "generic", ("annotation",), ("software_system",)),
        _cap("hibob", "people", "generic", ("employee", "object"), ("person", "team")),
        _cap("gusto", "people", "generic", ("object",), ("person",)),
        _cap("deel", "people", "generic", ("payment", "contract"), ("person",)),
        _cap("ashby", "people", "generic", ("object", "candidate"), ("person",)),
        _cap("linkedin", "people", "generic", ("object",), ("person", "organization")),
        _cap("mercury", "finance", "generic", ("transaction",), ("external_party",)),
        _cap("quickbooks", "finance", "generic", ("object",), ("external_party",)),
        _cap("brex", "finance", "generic", ("transaction",), ("external_party",)),
        _cap("ramp", "finance", "generic", ("transaction",), ("external_party",)),
        _cap("carta", "finance", "generic", ("object",), ("organization",)),
    )
}


_ADMISSION: dict[str, Admission] = {
    "person": "canonical",
    "document": "conditional",
    "meeting": "conditional",
    "work_item": "conditional",
    "external_party": "conditional",
    "organization": "conditional",
    "repository": "conditional",
    "audit": "contextual_only",
    "goal": "contextual_only",
    "project": "contextual_only",
    "team": "contextual_only",
    "software_system": "contextual_only",
    "topic": "contextual_only",
}


_HINT_REFERENCE_KIND = {
    "slack_user": "principal",
    "notion_user": "principal",
    "whatsapp_user": "principal",
    "email_address": "principal",
    "person_name": "principal",
    "slack_channel": "container",
    "notion_database": "container",
    "notion_page": "artifact",
    "notion_block": "artifact",
    "notion_comment": "artifact",
    "linear_issue": "work_record",
    "linear_project": "work_record",
    "linear_comment": "artifact",
    "linear_team": "container",
    "meeting_topic": "scheduled_event",
    "url": "url",
}


def capability_for(source: str) -> SourceIdentityCapability:
    try:
        return SOURCE_IDENTITY_CAPABILITIES[source]
    except KeyError as exc:
        raise ValueError(f"source {source!r} has no declared identity capability") from exc


def canonical_admission(entity_type: str) -> Admission:
    return _ADMISSION.get(entity_type, "contextual_only")


def reference_kind_for_hint(hint_type: str) -> str | None:
    return _HINT_REFERENCE_KIND.get(hint_type)


def capability_snapshot() -> dict[str, object]:
    return {
        "schema_version": 1,
        "sources": {
            source: capability.model_dump(mode="json")
            for source, capability in sorted(SOURCE_IDENTITY_CAPABILITIES.items())
        },
        "canonical_admission": dict(sorted(_ADMISSION.items())),
    }


__all__ = [
    "SOURCE_IDENTITY_CAPABILITIES",
    "SourceIdentityCapability",
    "canonical_admission",
    "capability_for",
    "capability_snapshot",
    "reference_kind_for_hint",
]
