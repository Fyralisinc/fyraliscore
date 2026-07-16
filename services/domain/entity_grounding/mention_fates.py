"""Close mention-detection fates at the observation persistence boundary."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from lib.entity_mention_detection import extract_bootstrap_mention_opportunities
from lib.shared.entity_phrases import phrase_requires_context
from services.domain.conversation_context.slack_source_structure import (
    SlackSourceObservation,
    project_slack_source_structure,
)
from services.domain.conversation_context.repo import GroundingAnnotationAppender
from services.domain.entity_aliases.repo import normalize_phrase
from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection


_MAX_DERIVED_OPPORTUNITIES = 50
_SLACK_CONTEXT_SURFACE_RE = re.compile(
    r"(?<!\w)(?:it|this|that|these|those|they|them)(?!\w)",
    flags=re.IGNORECASE,
)
_SLACK_TEMPORAL_CONTEXT_WINDOW = timedelta(minutes=30)


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
    context_observations: tuple[ContextObservationInput, ...] = (),
    boundary_hypotheses: tuple[dict[str, Any], ...] = (),
    topology_incomplete: bool = False,
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
            topology_incomplete=topology_incomplete,
            boundary_hypotheses=boundary_hypotheses,
            context_observations=context_observations,
            selection_dependency_refs=tuple(
                f"observation:{item.observation_id}"
                for item in context_observations
            ),
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
    prepared_rows = [_prepare_persisted_row(row) for row in rows]
    slack_context = _slack_batch_context(prepared_rows, tenant_id=tenant_id)
    eligible = 0
    committed = 0
    existing = 0
    for row in prepared_rows:
        phrases = _persisted_mention_opportunities(
            content=row["content"],
            content_text=row["content_text"],
            source_channel=row["source_channel"],
            has_structural_context=bool(slack_context.get(row["id"])),
        )
        context_observations = slack_context.get(row["id"], ())
        coverage = await ensure_observation_mention_fates(
            conn=conn,
            tenant_id=tenant_id,
            observation_id=row["id"],
            occurred_at=row["occurred_at"],
            source_channel=row["source_channel"],
            content=row["content"],
            content_text=row["content_text"],
            phrases=phrases,
            context_observations=context_observations,
            boundary_hypotheses=(
                ({"kind": "slack_batch_boundary", "status": "provisional"},)
                if row["source_channel"] == "slack:message"
                else ()
            ),
            topology_incomplete=(
                row["source_channel"] == "slack:message"
                and not any(
                    item.inclusion_layer == "source_topology"
                    for item in context_observations
                )
            ),
            now=now,
        )
        eligible += coverage.eligible_opportunities
        committed += coverage.committed_fates
        existing += coverage.existing_fates
    return MentionFateCoverage(eligible, committed, existing)


def _prepare_persisted_row(row: Any) -> dict[str, Any]:
    content = row["content"]
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            content = {}
    return {
        "id": row["id"],
        "occurred_at": row["occurred_at"],
        "source_channel": str(row["source_channel"]),
        "content": content if isinstance(content, dict) else {},
        "content_text": str(row["content_text"] or ""),
    }


def _persisted_mention_opportunities(
    *,
    content: dict[str, Any],
    content_text: str,
    source_channel: str,
    has_structural_context: bool,
) -> tuple[str, ...]:
    """Recover exact candidate surfaces without mutating persisted evidence.

    Explicit ingestion hints remain authoritative opportunities.  The shared
    bootstrap locator fills omissions common in simulated and older persisted
    rows.  Context-dependent Slack pronouns are admitted only when the batch
    contains source-topology evidence; their identity remains unresolved.
    """

    phrases = content.get("_unresolved_phrases")
    opportunities: list[str] = list(phrases) if isinstance(phrases, list) else []
    derived = extract_bootstrap_mention_opportunities(
        content_text,
        max_opportunities=_MAX_DERIVED_OPPORTUNITIES,
    )
    opportunities.extend(
        phrase
        for phrase in derived
        if source_channel != "slack:message"
        or has_structural_context
        or not phrase_requires_context(phrase)
    )
    if source_channel == "slack:message" and has_structural_context:
        opportunities.extend(
            match.group(0) for match in _SLACK_CONTEXT_SURFACE_RE.finditer(content_text)
        )
    return _unique_phrases(opportunities)[:_MAX_DERIVED_OPPORTUNITIES]


def _slack_batch_context(
    rows: list[dict[str, Any]],
    *,
    tenant_id: UUID,
) -> dict[UUID, tuple[ContextObservationInput, ...]]:
    slack_rows = [row for row in rows if row["source_channel"] == "slack:message"]
    if not slack_rows:
        return {}
    structure = project_slack_source_structure(
        tuple(
            SlackSourceObservation(
                tenant_id=tenant_id,
                event_revision_id=f"observation:{row['id']}:v1",
                occurred_at=row["occurred_at"],
                content_text=row["content_text"],
                content=row["content"],
            )
            for row in slack_rows
        )
    )
    by_revision = {
        f"observation:{row['id']}:v1": row for row in slack_rows
    }
    output: dict[UUID, tuple[ContextObservationInput, ...]] = {}
    for focal in slack_rows:
        focal_revision = f"observation:{focal['id']}:v1"
        connected = set(structure.connected_revision_ids(focal_revision, max_hops=2))
        candidates: list[ContextObservationInput] = []
        for other_revision, other in by_revision.items():
            if (
                other["id"] == focal["id"]
                or other["occurred_at"] > focal["occurred_at"]
            ):
                continue
            is_structural = other_revision in connected
            is_temporal = (
                not is_structural
                and _source_space(other["source_channel"], other["content"])
                == _source_space(focal["source_channel"], focal["content"])
                and focal["occurred_at"] - other["occurred_at"]
                <= _SLACK_TEMPORAL_CONTEXT_WINDOW
            )
            if not is_structural and not is_temporal:
                continue
            candidates.append(
                ContextObservationInput(
                    observation_id=other["id"],
                    occurred_at=other["occurred_at"],
                    source_channel=other["source_channel"],
                    source_space=_source_space(
                        other["source_channel"], other["content"]
                    ),
                    inclusion_layer=(
                        "source_topology" if is_structural else "temporal_candidate"
                    ),
                    inclusion_reasons=(
                        ("projected Slack thread/reply topology",)
                        if is_structural
                        else ("same Slack source space within temporal window",)
                    ),
                    content_text=other["content_text"],
                    topology_edge_ids=(
                        structure.incident_edge_ids(other_revision)
                        if is_structural
                        else ()
                    ),
                )
            )
        output[focal["id"]] = tuple(
            sorted(
                candidates,
                key=lambda item: (item.occurred_at, str(item.observation_id)),
            )
        )
    return output


__all__ = [
    "MentionFateCoverage",
    "ensure_observation_mention_fates",
    "ensure_persisted_observation_mention_fates",
]
