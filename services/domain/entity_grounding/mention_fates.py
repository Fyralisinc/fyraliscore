"""Close mention-detection fates at the observation persistence boundary."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from services.domain.conversation_context.repo import GroundingAnnotationAppender
from services.domain.entity_aliases.repo import normalize_phrase
from services.domain.entity_grounding.episode import prepare_context_selection
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection


@dataclass(frozen=True)
class MentionFateCoverage:
    eligible_opportunities: int
    committed_fates: int
    existing_fates: int

    @property
    def covered_opportunities(self) -> int:
        return self.committed_fates + self.existing_fates

    @property
    def coverage(self) -> float | None:
        if self.eligible_opportunities == 0:
            return None
        return self.covered_opportunities / self.eligible_opportunities


def _unique_phrases(phrases: Iterable[str]) -> tuple[str, ...]:
    unique: list[str] = []
    seen: set[str] = set()
    for phrase in phrases:
        if not isinstance(phrase, str):
            continue
        rendered = phrase.strip()
        normalized = normalize_phrase(rendered)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(rendered)
    return tuple(unique)


def _source_space(source_channel: str, content: dict[str, Any]) -> str:
    for key in ("channel", "channel_id", "conversation_id", "thread_id", "space_id"):
        value = content.get(key)
        if isinstance(value, str) and value.strip():
            return f"{source_channel}:{value.strip()}"
    return source_channel


async def ensure_observation_mention_fates(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    observation_id: UUID,
    occurred_at: datetime,
    source_channel: str,
    content: dict[str, Any],
    content_text: str,
    phrases: Iterable[str],
    now: datetime | None = None,
) -> MentionFateCoverage:
    """Ensure one immutable current detection fate per ingestion opportunity."""

    opportunities = _unique_phrases(phrases)
    if not opportunities:
        return MentionFateCoverage(0, 0, 0)

    prepared_at = now or datetime.now(timezone.utc)
    appender = GroundingAnnotationAppender()
    committed = 0
    existing = 0
    source_space = _source_space(source_channel, content)

    for phrase in opportunities:
        context_command, context_outcome = prepare_context_selection(
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
            occurred_at=occurred_at,
            source_channel=source_channel,
            source_space=source_space,
            topology_incomplete=False,
            boundary_hypotheses=(),
            context_observations=(),
            selection_dependency_refs=(),
            now=prepared_at,
            focal_content_text=content_text,
        )
        detection_command = prepare_entity_mention_detection(
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
            content_text=content_text,
            source_channel=source_channel,
            context_command=context_command,
            context_outcome=context_outcome,
            now=prepared_at,
        )
        if await conn.fetchval(
            """
            SELECT EXISTS (
              SELECT 1 FROM entity_mention_detection_heads
              WHERE tenant_id=$1 AND detection_key=$2
            )
            """,
            tenant_id,
            detection_command.detection_key,
        ):
            existing += 1
            continue

        await appender.apply_context(
            conn=conn,
            command=context_command,
            now=prepared_at,
        )
        await appender.apply_mention_detection(
            conn=conn,
            command=detection_command,
            now=prepared_at,
        )
        committed += 1

    return MentionFateCoverage(
        eligible_opportunities=len(opportunities),
        committed_fates=committed,
        existing_fates=existing,
    )


async def ensure_persisted_observation_mention_fates(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    observation_ids: Iterable[UUID],
    now: datetime | None = None,
) -> MentionFateCoverage:
    """Close mention fates for an already-persisted observation batch."""

    ids = tuple(dict.fromkeys(observation_ids))
    if not ids:
        return MentionFateCoverage(0, 0, 0)
    rows = await conn.fetch(
        """
        SELECT id, occurred_at, source_channel, content, content_text
        FROM observations
        WHERE tenant_id=$1 AND id=ANY($2::uuid[])
        ORDER BY occurred_at, id
        """,
        tenant_id,
        list(ids),
    )
    eligible = 0
    committed = 0
    existing = 0
    for row in rows:
        content = row["content"]
        if isinstance(content, str):
            content = json.loads(content)
        if not isinstance(content, dict):
            continue
        phrases = content.get("_unresolved_phrases")
        if not isinstance(phrases, list):
            continue
        coverage = await ensure_observation_mention_fates(
            conn=conn,
            tenant_id=tenant_id,
            observation_id=row["id"],
            occurred_at=row["occurred_at"],
            source_channel=row["source_channel"],
            content=content,
            content_text=row["content_text"] or "",
            phrases=phrases,
            now=now,
        )
        eligible += coverage.eligible_opportunities
        committed += coverage.committed_fates
        existing += coverage.existing_fates
    return MentionFateCoverage(eligible, committed, existing)


__all__ = [
    "MentionFateCoverage",
    "ensure_observation_mention_fates",
    "ensure_persisted_observation_mention_fates",
]
