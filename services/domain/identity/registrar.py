"""Convert observation-level identity hints into provenance-bound mentions."""

from __future__ import annotations

from typing import Any

import asyncpg

from lib.shared.types import ObservationRow, SourceEvidenceRow

from .capabilities import reference_kind_for_hint
from .foundation import EntityMentionCreate, EntityMentionRow, SourceReferenceCreate
from .foundation_repo import EntityMentionRepository, SourceReferenceRepository


_EXPECTED_BY_REFERENCE_KIND = {
    "principal": ("person",),
    "artifact": ("document",),
    "work_record": ("work_item",),
    "scheduled_event": ("meeting",),
}


def _native_type(hint_type: str) -> str:
    for prefix in ("slack_", "notion_", "whatsapp_", "linear_"):
        if hint_type.startswith(prefix):
            return hint_type.removeprefix(prefix)
    return hint_type


class ObservationMentionRegistrar:
    def __init__(
        self,
        *,
        sources: SourceReferenceRepository | None = None,
        mentions: EntityMentionRepository | None = None,
    ) -> None:
        self._sources = sources or SourceReferenceRepository()
        self._mentions = mentions or EntityMentionRepository()

    async def register(
        self,
        observation: ObservationRow,
        evidence: SourceEvidenceRow,
        *,
        conn: asyncpg.Connection,
    ) -> list[EntityMentionRow]:
        values: list[EntityMentionCreate] = []
        if observation.source_actor_ref:
            source_ref = await self._sources.register(
                SourceReferenceCreate(
                    tenant_id=observation.tenant_id,
                    connector_installation_id=evidence.connector_installation_id,
                    installation_scope=evidence.installation_scope,
                    source=evidence.source,
                    native_type="user",
                    native_id=observation.source_actor_ref,
                    reference_kind="principal",
                    attributes={
                        "source_channel": evidence.source,
                        "source_actor_ref": observation.source_actor_ref,
                    },
                    evidence_id=evidence.id,
                    valid_from=evidence.valid_from,
                    valid_to=evidence.valid_to,
                    status="deleted" if evidence.operation == "delete" else "active",
                ),
                conn=conn,
            )
            context: dict[str, Any] = {}
            if observation.actor_id is not None:
                context["provided_candidate_ref"] = {
                    "type": "actor",
                    "id": str(observation.actor_id),
                }
            values.append(
                self._mention(
                    observation,
                    evidence,
                    text=observation.source_actor_ref,
                    mention_kind="source_actor",
                    expected_types=("person",),
                    source_reference_id=source_ref.id,
                    context=context,
                )
            )

        for raw_hint in observation.entities_mentioned:
            if not isinstance(raw_hint, dict):
                continue
            hint_type = str(raw_hint.get("type") or "")
            hint_id = raw_hint.get("id")
            if not hint_type or hint_id in (None, ""):
                continue
            text = str(hint_id)
            reference_kind = reference_kind_for_hint(hint_type)
            if reference_kind and hint_type != "person_name":
                source_ref = await self._sources.register(
                    SourceReferenceCreate(
                        tenant_id=observation.tenant_id,
                        connector_installation_id=evidence.connector_installation_id,
                        installation_scope=evidence.installation_scope,
                        source=evidence.source,
                        native_type=_native_type(hint_type),
                        native_id=text,
                        reference_kind=reference_kind,  # type: ignore[arg-type]
                        attributes={"source_hint": raw_hint},
                        evidence_id=evidence.id,
                        valid_from=evidence.valid_from,
                        valid_to=evidence.valid_to,
                        status=(
                            "deleted" if evidence.operation == "delete" else "active"
                        ),
                    ),
                    conn=conn,
                )
                values.append(
                    self._mention(
                        observation,
                        evidence,
                        text=text,
                        mention_kind="structured_reference",
                        expected_types=_EXPECTED_BY_REFERENCE_KIND.get(
                            reference_kind, ()
                        ),
                        source_reference_id=source_ref.id,
                        context={"source_hint": raw_hint},
                    )
                )
            else:
                expected = ("person",) if hint_type in {"actor", "person_name"} else (hint_type,)
                context = {"source_hint": raw_hint}
                if hint_type != "person_name":
                    context["provided_candidate_ref"] = raw_hint
                values.append(
                    self._mention(
                        observation,
                        evidence,
                        text=text,
                        mention_kind="structured_reference",
                        expected_types=expected,
                        source_reference_id=None,
                        context=context,
                    )
                )

        unresolved = observation.content.get("_unresolved_phrases", [])
        for phrase in unresolved if isinstance(unresolved, list) else ():
            if isinstance(phrase, str) and phrase.strip():
                values.append(
                    self._mention(
                        observation,
                        evidence,
                        text=phrase.strip(),
                        mention_kind="text",
                        expected_types=(),
                        source_reference_id=None,
                        context={"origin": "ingestion_candidate_phrase"},
                    )
                )

        result: list[EntityMentionRow] = []
        seen: set[str] = set()
        for value in values:
            if value.computed_mention_key in seen:
                continue
            seen.add(value.computed_mention_key)
            result.append(await self._mentions.register(value, conn=conn))
        return result

    @staticmethod
    def _mention(
        observation: ObservationRow,
        evidence: SourceEvidenceRow,
        *,
        text: str,
        mention_kind: str,
        expected_types: tuple[str, ...],
        source_reference_id: Any,
        context: dict[str, Any],
    ) -> EntityMentionCreate:
        return EntityMentionCreate(
            tenant_id=observation.tenant_id,
            observation_id=observation.id,
            observation_occurred_at=observation.occurred_at,
            evidence_id=evidence.id,
            source_reference_id=source_reference_id,
            mention_kind=mention_kind,  # type: ignore[arg-type]
            text=text,
            expected_types=expected_types,
            context=context,
        )


__all__ = ["ObservationMentionRegistrar"]
