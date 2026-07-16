"""services/workers/entity_resolver/context.py — context bundle builder.

For each unresolved phrase on an Observation, the resolver worker needs an
as-known, source-structured candidate context. Slack's semantic boundary is
not a fixed message count or the entire connector channel: actual channel,
thread/reply/edit topology and then bounded temporal alternatives are kept
separate so proximity cannot impersonate corroboration.

Inputs:
    - observation_id (UUID)
    - phrase (str)
    - tenant_id (UUID)
    - asyncpg.Pool (or Connection)

Outputs: `ResolverContext` — a small dataclass with:
    - recent_observations: authorized source-topology and temporal candidates
      from the actual source channel, never all Slack observations
    - recent_aliases: list of aliases already seen for the phrase
      (useful to LLM as "we've seen this before")

The context is intentionally small — LLM budget is 2K tokens per
spec §15 "Context budget".
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.conversation_context import (
    CommitInterpretationContextCommand,
    ContextSelectionOutcome,
)
from lib.contracts.entity_mentions import CommitEntityMentionDetectionCommand
from services.domain.conversation_context.slack_source_structure import (
    SlackSourceObservation,
    SlackSourceStructure,
    project_slack_source_structure,
)
from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    candidate_id_for_ref,
    estimate_context_tokens,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection


_DEFAULT_RECENT_OBS = 20
_DEFAULT_SCOPED_MODELS = 10


@dataclass
class RecentObservation:
    id: UUID
    occurred_at: Any   # datetime
    source_channel: str
    content_text: str
    entities_mentioned: list[dict[str, Any]]
    inclusion_layer: str
    inclusion_reasons: list[str]
    source_content: dict[str, Any] = field(default_factory=dict)
    topology_edge_ids: tuple[str, ...] = ()


@dataclass
class ScopedModel:
    id: UUID
    natural: str
    confidence: float
    scope_entities: list[dict[str, Any]]


@dataclass
class RecentAlias:
    alias_id: UUID
    alias_text: str
    resolved_entity_ref: dict[str, Any]
    confidence: float
    source: str
    identity_basis_class: str | None
    identity_basis_ref: str | None
    adjudication_state: str | None = None
    resolution_scope: str | None = None
    autonomous_replay_eligible: bool = False
    replay_lineage_valid: bool = False
    adjudication_answer_digest: str | None = None
    clarification_request_id: str | None = None
    canonical_target_valid: bool = False


@dataclass
class KnownEntityCandidate:
    alias_id: UUID
    alias_text: str
    resolved_entity_ref: dict[str, Any]
    confidence: float
    source: str
    identity_basis_class: str | None
    identity_basis_ref: str | None
    adjudication_state: str | None = None
    resolution_scope: str | None = None
    canonical_target_valid: bool = False


@dataclass
class ResolverContext:
    observation_id: UUID
    phrase: str
    tenant_id: UUID
    recent_observations: list[RecentObservation] = field(default_factory=list)
    scoped_models: list[ScopedModel] = field(default_factory=list)
    recent_aliases: list[RecentAlias] = field(default_factory=list)
    known_entity_candidates: list[KnownEntityCandidate] = field(default_factory=list)
    source_entities_mentioned: list[dict[str, Any]] = field(default_factory=list)
    source_channel: str = ""
    source_space: str = ""
    content_text: str = ""
    evidence_cutoff: Any | None = None
    topology_incomplete: bool = False
    boundary_hypotheses: list[dict[str, Any]] = field(default_factory=list)
    selection_dependencies: list[str] = field(default_factory=list)
    context_selection_command: CommitInterpretationContextCommand | None = None
    context_selection_outcome: ContextSelectionOutcome | None = None
    mention_detection_command: CommitEntityMentionDetectionCommand | None = None

    def to_prompt_blob(self) -> str:
        """Compact JSON-ish rendering for the LLM user message."""
        selected_event_ids = (
            {
                item.event_revision_id
                for item in self.context_selection_outcome.snapshot.selected_items
            }
            if self.context_selection_outcome is not None
            else None
        )
        selected_recent = [
            item
            for item in self.recent_observations
            if selected_event_ids is None
            or f"observation:{item.id}:v1" in selected_event_ids
        ]
        out: dict[str, Any] = {
            "phrase": self.phrase,
            "source_channel": self.source_channel,
            "source_space": self.source_space,
            "evidence_cutoff": self.evidence_cutoff,
            "topology_incomplete": self.topology_incomplete,
            "boundary_hypotheses": self.boundary_hypotheses,
            "selection_dependencies": self.selection_dependencies,
            "context_selection": (
                {
                    "snapshot_id": self.context_selection_outcome.snapshot.snapshot_id,
                    "snapshot_hash": self.context_selection_outcome.snapshot.snapshot_content_hash,
                    "disposition": self.context_selection_outcome.disposition.value,
                    "omissions": list(
                        self.context_selection_outcome.snapshot.sufficiency_verdict.omissions
                    ),
                    "unresolved_references": list(
                        self.context_selection_outcome.snapshot.sufficiency_verdict.unresolved_references
                    ),
                }
                if self.context_selection_outcome is not None
                else None
            ),
            "mention_detection": (
                {
                    "detection_id": str(
                        self.mention_detection_command.detection.detection_id
                    ),
                    "fate": self.mention_detection_command.detection.fate.value,
                    "mention": (
                        self.mention_detection_command.detection.mention.model_dump(
                            mode="json"
                        )
                        if self.mention_detection_command.detection.mention is not None
                        else None
                    ),
                    "semantic_limit": (
                        "source-anchored mention only; it carries no entity identity "
                        "or entity-type authority"
                    ),
                }
                if self.mention_detection_command is not None
                else None
            ),
            "source_content_excerpt": self.content_text[:500],
            "source_entities_mentioned": [
                {
                    "candidate_id": candidate_id_for_ref(ref),
                    "entity_ref": ref,
                    "semantic_limit": (
                        "already-admitted source sidecar; candidate navigation, "
                        "not new identity evidence"
                    ),
                }
                for ref in self.source_entities_mentioned[:10]
                if isinstance(ref, dict) and ref.get("id") and ref.get("type")
            ],
            "recent_observations": [
                {
                    "channel": o.source_channel,
                    "text": o.content_text[:200],
                    "entities": o.entities_mentioned[:5],
                    "occurred_at": o.occurred_at,
                    "inclusion_layer": o.inclusion_layer,
                    "inclusion_reasons": o.inclusion_reasons,
                }
                for o in selected_recent[:10]
            ],
            "evidence_law": (
                "context proximity, prior model output, and repeated aliases are "
                "navigation or candidate features, never independent identity evidence"
            ),
            "prior_alias_matches": [
                {
                    "alias": a.alias_text,
                    "candidate_id": candidate_id_for_ref(a.resolved_entity_ref),
                    "entity_ref": a.resolved_entity_ref,
                    "alias_origin": a.source,
                    "governed_identity_basis_class": a.identity_basis_class,
                    "governed_identity_basis_ref": a.identity_basis_ref,
                    "adjudication_state": a.adjudication_state,
                    "resolution_scope": a.resolution_scope,
                    "autonomous_replay_eligible": a.autonomous_replay_eligible,
                    "replay_lineage_valid": a.replay_lineage_valid,
                    "adjudication_answer_digest": a.adjudication_answer_digest,
                    "clarification_request_id": a.clarification_request_id,
                    "canonical_target_valid": a.canonical_target_valid,
                    "semantic_limit": (
                        "candidate navigation; automatic admission additionally "
                        "requires independently governed identity lineage"
                    ),
                }
                for a in self.recent_aliases
            ],
            "known_entity_candidates": [
                {
                    "alias": c.alias_text,
                    "candidate_id": candidate_id_for_ref(c.resolved_entity_ref),
                    "entity_ref": c.resolved_entity_ref,
                    "confidence": c.confidence,
                    "alias_origin": c.source,
                    "governed_identity_basis_class": c.identity_basis_class,
                    "governed_identity_basis_ref": c.identity_basis_ref,
                    "adjudication_state": c.adjudication_state,
                    "resolution_scope": c.resolution_scope,
                    "canonical_target_valid": c.canonical_target_valid,
                    "semantic_limit": (
                        "candidate navigation; automatic admission additionally "
                        "requires independently governed identity lineage"
                    ),
                }
                for c in self.known_entity_candidates[:30]
            ],
        }
        return json.dumps(out, default=str, separators=(",", ":"))


async def build_context(
    *,
    pool: asyncpg.Pool | asyncpg.Connection,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    recent_n: int = _DEFAULT_RECENT_OBS,
    scoped_models_n: int = _DEFAULT_SCOPED_MODELS,
    known_entities_n: int = 30,
) -> ResolverContext:
    """Assemble the context bundle used by the resolver LLM prompt.

    Accepts either a pool or a connection (tests pin one connection
    per test transaction — see the observations conftest pattern).
    """
    conn_owned: asyncpg.Connection | None = None
    if isinstance(pool, asyncpg.Connection):
        conn = pool
    else:
        conn_owned = await pool.acquire()
        conn = conn_owned

    try:
        # Source observation — need source_channel, content_text,
        # occurred_at for the context-window query.
        src = await conn.fetchrow(
            """
            SELECT id, source_channel, content_text, content, occurred_at,
                   entities_mentioned
            FROM observations
            WHERE id = $1 AND tenant_id = $2
            """,
            observation_id,
            tenant_id,
        )
        if src is None:
            return ResolverContext(
                observation_id=observation_id,
                phrase=phrase,
                tenant_id=tenant_id,
            )

        source_channel = src["source_channel"]
        content_text = str(src["content_text"] or "")
        occurred_at = src["occurred_at"]
        source_content = _parse_jsonb(src["content"]) or {}
        source_space = str(
            source_content.get("channel")
            or source_content.get("project_id")
            or source_content.get("repository")
            or source_channel
        )
        source_entities_mentioned = (
            _parse_jsonb(src["entities_mentioned"]) or []
        )

        del scoped_models_n  # prior Models cannot be identity corroboration
        structural_rows, temporal_rows = await _load_context_candidates(
            conn=conn,
            tenant_id=tenant_id,
            observation_id=observation_id,
            source_channel=source_channel,
            source_space=source_space,
            source_content=source_content,
            occurred_at=occurred_at,
            limit=recent_n,
        )
        source_structure = _project_slack_context_structure(
            tenant_id=tenant_id,
            source_channel=source_channel,
            observation_id=observation_id,
            occurred_at=occurred_at,
            content_text=content_text,
            source_content=source_content,
            context_rows=tuple(
                row for row, _, _ in (*structural_rows, *temporal_rows)
            ),
        )
        selected: list[RecentObservation] = []
        seen_observations: set[UUID] = set()
        for row, layer, reasons in (*structural_rows, *temporal_rows):
            if row["id"] in seen_observations or len(selected) >= recent_n:
                continue
            seen_observations.add(row["id"])
            selected.append(
                RecentObservation(
                    id=row["id"],
                    occurred_at=row["occurred_at"],
                    source_channel=row["source_channel"],
                    content_text=row["content_text"],
                    entities_mentioned=_parse_jsonb(row["entities_mentioned"]) or [],
                    inclusion_layer=layer,
                    inclusion_reasons=reasons,
                    source_content=_parse_jsonb(row["content"]) or {},
                    topology_edge_ids=(
                        source_structure.incident_edge_ids(
                            f"observation:{row['id']}:v1"
                        )
                        if layer == "source_topology"
                        else ()
                    ),
                )
            )
        recent_observations = selected
        scoped_models: list[ScopedModel] = []

        # Prior aliases for the exact phrase (if any).
        alias_rows = await conn.fetch(
            """
            SELECT alias.id, alias.alias_text, alias.resolved_entity_ref,
                   alias.confidence,
                   COALESCE(
                     alias.entity_metadata ->> 'source', 'unknown'
                   ) AS source,
                   alias.entity_metadata
                     ->> 'identity_basis_class' AS identity_basis_class,
                   alias.entity_metadata
                     ->> 'identity_basis_ref' AS identity_basis_ref,
                   alias.entity_metadata
                     ->> 'adjudication_state' AS adjudication_state,
                   alias.entity_metadata
                     ->> 'resolution_scope' AS resolution_scope,
                   COALESCE(
                     alias.entity_metadata
                       ->> 'autonomous_replay_eligible' = 'true',
                     FALSE
                   ) AS autonomous_replay_eligible,
                   alias.entity_metadata
                     ->> 'adjudication_answer_digest'
                     AS adjudication_answer_digest,
                   alias.entity_metadata
                     ->> 'clarification_request_id'
                     AS clarification_request_id,
                   CASE
                     WHEN COALESCE(
                       alias.resolved_entity_ref ->> 'version', '1'
                     ) <> '1' THEN FALSE
                     WHEN alias.resolved_entity_ref ->> 'type' = 'actor'
                     THEN EXISTS (
                       SELECT 1 FROM actors target
                       WHERE target.tenant_id=alias.tenant_id
                         AND target.id::text
                             = alias.resolved_entity_ref ->> 'id'
                         AND target.status='active'
                     )
                     WHEN alias.resolved_entity_ref ->> 'type'
                          IN ('resource', 'customer')
                     THEN EXISTS (
                       SELECT 1 FROM resources target
                       WHERE target.tenant_id=alias.tenant_id
                         AND target.id::text
                             = alias.resolved_entity_ref ->> 'id'
                         AND target.archived_at IS NULL
                         AND (
                           alias.resolved_entity_ref ->> 'type' <> 'customer'
                           OR target.metadata ->> 'semantic_kind' = 'customer'
                         )
                     )
                     ELSE FALSE
                   END AS canonical_target_valid,
                   EXISTS (
                     SELECT 1
                     FROM clarification_requests clarification
                     JOIN grounding_traces predecessor
                       ON predecessor.tenant_id=alias.tenant_id
                      AND predecessor.id::text = alias.entity_metadata
                        -> 'grounding_feedback_lineage'
                        ->> 'grounding_trace_id'
                     JOIN grounding_traces successor
                       ON successor.tenant_id=predecessor.tenant_id
                      AND successor.trace
                        ->> 'supersedes_grounding_trace_id' = predecessor.id::text
                     WHERE clarification.tenant_id=alias.tenant_id
                       AND clarification.id::text = alias.entity_metadata
                         ->> 'clarification_request_id'
                       AND clarification.status='answered'
                       AND clarification.source_observation_id=alias.source_event_id
                       AND successor.trace ->> 'adjudication_ref'
                           = 'clarification-request:' || clarification.id::text
                       AND successor.selected_referent=alias.resolved_entity_ref
                   ) AS replay_lineage_valid
            FROM entity_aliases alias
            WHERE alias.tenant_id = $1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')
                  = regexp_replace(lower($2::text), '\\s+', ' ', 'g')
            ORDER BY confidence DESC
            """,
            tenant_id,
            phrase,
        )
        recent_aliases = [
            RecentAlias(
                alias_id=r["id"],
                alias_text=r["alias_text"],
                resolved_entity_ref=_parse_jsonb(r["resolved_entity_ref"]) or {},
                confidence=float(r["confidence"]),
                source=r["source"],
                identity_basis_class=r["identity_basis_class"],
                identity_basis_ref=r["identity_basis_ref"],
                adjudication_state=r["adjudication_state"],
                resolution_scope=r["resolution_scope"],
                autonomous_replay_eligible=bool(
                    r["autonomous_replay_eligible"]
                ),
                replay_lineage_valid=bool(r["replay_lineage_valid"]),
                adjudication_answer_digest=r["adjudication_answer_digest"],
                clarification_request_id=r["clarification_request_id"],
                canonical_target_valid=bool(r["canonical_target_valid"]),
            )
            for r in alias_rows
        ]

        candidate_rows = await conn.fetch(
            """
            SELECT id, alias_text, resolved_entity_ref, confidence,
                   COALESCE(entity_metadata ->> 'source', 'unknown') AS source,
                   entity_metadata ->> 'identity_basis_class' AS identity_basis_class,
                   entity_metadata ->> 'identity_basis_ref' AS identity_basis_ref,
                   entity_metadata ->> 'adjudication_state' AS adjudication_state,
                   entity_metadata ->> 'resolution_scope' AS resolution_scope,
                   CASE
                     WHEN COALESCE(
                       alias.resolved_entity_ref ->> 'version', '1'
                     ) <> '1' THEN FALSE
                     WHEN alias.resolved_entity_ref ->> 'type' = 'actor'
                     THEN EXISTS (
                       SELECT 1 FROM actors target
                       WHERE target.tenant_id=alias.tenant_id
                         AND target.id::text
                             = alias.resolved_entity_ref ->> 'id'
                         AND target.status='active'
                     )
                     WHEN alias.resolved_entity_ref ->> 'type'
                          IN ('resource', 'customer')
                     THEN EXISTS (
                       SELECT 1 FROM resources target
                       WHERE target.tenant_id=alias.tenant_id
                         AND target.id::text
                             = alias.resolved_entity_ref ->> 'id'
                         AND target.archived_at IS NULL
                         AND (
                           alias.resolved_entity_ref ->> 'type' <> 'customer'
                           OR target.metadata ->> 'semantic_kind' = 'customer'
                         )
                     )
                     ELSE FALSE
                   END AS canonical_target_valid
            FROM entity_aliases alias
            WHERE alias.tenant_id = $1
            ORDER BY confidence DESC, confirmed_count DESC, last_used_at DESC
            LIMIT 200
            """,
            tenant_id,
        )
        known_entity_candidates = _rank_known_entity_candidates(
            phrase=phrase,
            content_text=content_text,
            rows=candidate_rows,
            limit=known_entities_n,
        )

        context = ResolverContext(
            observation_id=observation_id,
            phrase=phrase,
            tenant_id=tenant_id,
            source_channel=source_channel,
            source_space=source_space,
            content_text=content_text,
            evidence_cutoff=occurred_at,
            topology_incomplete=(
                source_channel == "slack:message"
                and not isinstance(source_content.get("channel"), str)
            ),
            boundary_hypotheses=_boundary_hypotheses(
                source_content=source_content,
                structural_count=len(structural_rows),
                temporal_count=len(temporal_rows),
            ),
            selection_dependencies=[
                f"observation:{row.id}@{row.occurred_at.isoformat()}"
                for row in recent_observations
            ],
            source_entities_mentioned=source_entities_mentioned,
            recent_observations=recent_observations,
            scoped_models=scoped_models,
            recent_aliases=recent_aliases,
            known_entity_candidates=known_entity_candidates,
        )
        prepared_at = datetime.now(timezone.utc)
        selection_command, selection_outcome = prepare_context_selection(
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
            occurred_at=occurred_at,
            source_channel=source_channel,
            source_space=source_space,
            topology_incomplete=context.topology_incomplete,
            boundary_hypotheses=tuple(context.boundary_hypotheses),
            context_observations=tuple(
                ContextObservationInput(
                    observation_id=item.id,
                    occurred_at=item.occurred_at,
                    source_channel=item.source_channel,
                    source_space=source_space,
                    inclusion_layer=item.inclusion_layer,
                    inclusion_reasons=tuple(item.inclusion_reasons),
                    content_text=item.content_text,
                    token_count=estimate_context_tokens(item.content_text),
                    topology_edge_ids=item.topology_edge_ids,
                )
                for item in recent_observations
            ),
            selection_dependency_refs=tuple(context.selection_dependencies),
            now=prepared_at,
            focal_content_text=content_text,
            governed_exact_alias_available=any(
                alias.autonomous_replay_eligible
                and alias.replay_lineage_valid
                and alias.canonical_target_valid
                and alias.resolution_scope == "tenant_global_exact"
                for alias in recent_aliases
            ),
        )
        context.context_selection_command = selection_command
        context.context_selection_outcome = selection_outcome
        context.mention_detection_command = prepare_entity_mention_detection(
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
            content_text=content_text,
            source_channel=source_channel,
            context_command=selection_command,
            context_outcome=selection_outcome,
            now=prepared_at,
        )
        return context
    finally:
        if conn_owned is not None:
            await pool.release(conn_owned)


async def _load_context_candidates(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    observation_id: UUID,
    source_channel: str,
    source_space: str,
    source_content: dict[str, Any],
    occurred_at: Any,
    limit: int,
) -> tuple[list[tuple[Any, str, list[str]]], list[tuple[Any, str, list[str]]]]:
    """Load source-structural and temporal alternatives without conflating them."""

    common = """
        SELECT id, occurred_at, source_channel, content_text,
               entities_mentioned, content
        FROM observations
        WHERE tenant_id = $1
          AND source_channel = $2
          AND occurred_at <= $3
          AND id <> $4
    """
    source_space_clause = ""
    args: list[Any] = [tenant_id, source_channel, occurred_at, observation_id]
    if source_channel == "slack:message":
        source_space_clause = " AND content ->> 'channel' = $5"
        args.append(source_space)

    temporal = await conn.fetch(
        common
        + source_space_clause
        + " ORDER BY occurred_at DESC, id DESC LIMIT $%d" % (len(args) + 1),
        *args,
        max(limit * 3, limit),
    )
    temporal_rows = [
        (row, "temporal_candidate", ["same exact source space", "as-known cutoff"])
        for row in temporal
    ]

    structural_rows: list[tuple[Any, str, list[str]]] = []
    if source_channel == "slack:message":
        thread_root = (
            source_content.get("thread_ts")
            or source_content.get("original_ts")
            or source_content.get("ts")
        )
        source_ts = source_content.get("ts")
        if isinstance(thread_root, str) and thread_root:
            structural = await conn.fetch(
                common
                + source_space_clause
                + """
                  AND (
                    content ->> 'ts' = $6
                    OR content ->> 'thread_ts' = $6
                    OR content ->> 'original_ts' = $6
                    OR ($7::text IS NOT NULL AND content ->> 'original_ts' = $7)
                  )
                  ORDER BY occurred_at ASC, id ASC
                  LIMIT $8
                """,
                *args,
                thread_root,
                source_ts if isinstance(source_ts, str) else None,
                max(limit * 3, limit),
            )
            structural_rows = [
                (
                    row,
                    "source_topology",
                    ["same Slack channel", "thread/reply/edit lineage"],
                )
                for row in structural
            ]
    return structural_rows, temporal_rows


def _project_slack_context_structure(
    *,
    tenant_id: UUID,
    source_channel: str,
    observation_id: UUID,
    occurred_at: datetime,
    content_text: str,
    source_content: dict[str, Any],
    context_rows: tuple[Any, ...],
) -> SlackSourceStructure:
    if (
        source_channel != "slack:message"
        or not isinstance(source_content.get("channel"), str)
    ):
        return SlackSourceStructure((), (), ())
    sources = [
        SlackSourceObservation(
            tenant_id=tenant_id,
            event_revision_id=f"observation:{observation_id}:v1",
            occurred_at=occurred_at,
            content_text=content_text,
            content=source_content,
        )
    ]
    seen = {observation_id}
    for row in context_rows:
        if row["id"] in seen:
            continue
        seen.add(row["id"])
        sources.append(
            SlackSourceObservation(
                tenant_id=tenant_id,
                event_revision_id=f"observation:{row['id']}:v1",
                occurred_at=row["occurred_at"],
                content_text=str(row["content_text"] or ""),
                content=_parse_jsonb(row["content"]) or {},
            )
        )
    return project_slack_source_structure(tuple(sources))


def _boundary_hypotheses(
    *,
    source_content: dict[str, Any],
    structural_count: int,
    temporal_count: int,
) -> list[dict[str, Any]]:
    hypotheses: list[dict[str, Any]] = []
    if source_content.get("thread_ts") or source_content.get("original_ts"):
        hypotheses.append(
            {
                "kind": "source_topology",
                "candidate_count": structural_count,
                "limits": "thread membership is useful structure, not proof of one topic",
            }
        )
    hypotheses.append(
        {
            "kind": "same_source_space_temporal",
            "candidate_count": temporal_count,
            "limits": "temporal proximity is a candidate boundary, not corroboration",
        }
    )
    return hypotheses


def _parse_jsonb(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode()
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return None
    return v


def _rank_known_entity_candidates(
    *,
    phrase: str,
    content_text: str,
    rows: list[Any],
    limit: int,
) -> list[KnownEntityCandidate]:
    """Return a small, ranked alias candidate set for the resolver prompt.

    This is intentionally lexical and cheap. It gives the LLM real canonical
    IDs to choose from without turning every vague phrase into a large RAG call.
    """
    phrase_norm = _norm(phrase)
    phrase_compact = _compact(phrase)
    content_norm = _norm(content_text)
    phrase_tokens = set(_tokens(phrase))

    scored: list[tuple[float, int, KnownEntityCandidate]] = []
    for idx, row in enumerate(rows):
        alias_text = row["alias_text"]
        alias_norm = _norm(alias_text)
        alias_compact = _compact(alias_text)
        alias_tokens = set(_tokens(alias_text))
        ref = _parse_jsonb(row["resolved_entity_ref"]) or {}
        confidence = float(row["confidence"])

        score = confidence
        if phrase_norm and phrase_norm == alias_norm:
            score += 100.0
        elif phrase_norm and (
            phrase_norm in alias_norm or alias_norm in phrase_norm
        ):
            score += 20.0

        overlap = phrase_tokens & alias_tokens
        score += 5.0 * len(overlap)

        acronym = "".join(tok[0] for tok in _tokens(alias_text))
        if phrase_compact and acronym and phrase_compact == acronym:
            score += 35.0
        elif phrase_compact and acronym and (
            phrase_compact.startswith(acronym)
            or acronym.startswith(phrase_compact)
        ):
            score += 12.0

        if phrase_compact and _is_subsequence(phrase_compact, alias_compact):
            score += 10.0

        if alias_norm and alias_norm in content_norm:
            score += 25.0

        if ref.get("type") == "customer":
            score += 2.0

        scored.append((
            score,
            -idx,
            KnownEntityCandidate(
                alias_id=row["id"],
                alias_text=alias_text,
                resolved_entity_ref=ref,
                confidence=confidence,
                source=row["source"],
                identity_basis_class=row["identity_basis_class"],
                identity_basis_ref=row["identity_basis_ref"],
                adjudication_state=row["adjudication_state"],
                resolution_scope=row["resolution_scope"],
                canonical_target_valid=bool(row["canonical_target_valid"]),
            ),
        ))

    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [candidate for _, _, candidate in scored[:limit]]


def _norm(value: str) -> str:
    return " ".join(str(value or "").casefold().split())


def _compact(value: str) -> str:
    return "".join(ch for ch in _norm(value) if ch.isalnum())


def _tokens(value: str) -> list[str]:
    return [
        token
        for token in _norm(value).replace("-", " ").split()
        if token and any(ch.isalpha() for ch in token)
    ]


def _is_subsequence(needle: str, haystack: str) -> bool:
    if not needle:
        return False
    pos = 0
    for ch in haystack:
        if pos < len(needle) and needle[pos] == ch:
            pos += 1
    return pos == len(needle)


__all__ = [
    "RecentObservation",
    "ScopedModel",
    "RecentAlias",
    "KnownEntityCandidate",
    "ResolverContext",
    "build_context",
]
