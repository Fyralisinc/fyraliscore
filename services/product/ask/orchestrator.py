"""Ask Fyralis orchestration over the Synthesis layer."""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow, ObservationRow
from services.platform.access_control.audit import record_override_if_needed
from services.platform.access_control.checks import can_read
from services.domain.models.repo import _SELECT_COLS_SQL as _MODEL_SELECT_COLS_SQL
from services.domain.models.repo import _hydrate_row as _hydrate_model_row
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.reader import SynthesisReader, SynthesisReaderResult
from services.reasoning.synthesis.query_understanding import extract_query_alternatives
from services.reasoning.synthesis.state_contract import StateSource, compile_state_contract

from .intent import classify_intent
from .schemas import (
    AskAnswerPayload,
    AskEvidenceItem,
    AskMode,
    AskRelatedNode,
    AskScope,
    AskSession,
    AskSessionCreateRequest,
    AskTurnRequest,
    AskTurnResponse,
)
from .store import AskStore


_log = structlog.get_logger(__name__)


class ConnProvider(Protocol):
    def __call__(self) -> Any: ...


@dataclass(slots=True)
class RetrievalPacket:
    models: list[ModelRow]
    observations: list[ObservationRow]
    evidence: list[AskEvidenceItem]
    omitted: list[AskEvidenceItem]
    debug: dict[str, Any]
    state_contract: dict[str, Any] = field(default_factory=dict)


class AskOrchestrator:
    def __init__(
        self,
        *,
        store: AskStore,
        conn_provider: ConnProvider,
        reader: SynthesisReader | None = None,
    ) -> None:
        self._store = store
        self._conn_provider = conn_provider
        self._reader = reader or SynthesisReader()

    async def create_session(
        self,
        *,
        tenant_id: UUID,
        viewer_id: UUID,
        body: AskSessionCreateRequest,
    ) -> AskSession:
        _, mode = classify_intent("", body.initial_scope)
        return await self._store.create_session(
            tenant_id=tenant_id,
            viewer_id=viewer_id,
            scope=body.initial_scope,
            source_route=body.source_route,
            source_object_id=body.source_object_id,
            source_object_type=body.source_object_type,
            mode=mode,
        )

    async def answer_turn(
        self,
        *,
        tenant_id: UUID,
        viewer_id: UUID,
        session_id: UUID,
        body: AskTurnRequest,
    ) -> AskTurnResponse:
        query = body.query.strip()
        if not query:
            raise ValueError("query must be non-empty")
        session = await self._store.get_session(session_id, tenant_id=tenant_id)
        if session is None:
            raise LookupError("ask session not found")
        scope = body.scope or session.current_scope
        if body.scope is not None:
            await self._store.update_scope(session_id, scope=scope)
            session = session.model_copy(update={"current_scope": scope})

        intent, planned_mode = classify_intent(query, scope)
        mode: AskMode = body.requested_mode or planned_mode
        started = time.perf_counter()
        user_message_id = await self._store.add_message(
            session_id=session_id,
            role="user",
            content=query,
            structured_payload={"scope": scope.model_dump(mode="json"), "mode": mode},
        )
        retrieval_run_id = await self._store.add_retrieval_run(
            session_id=session_id,
            message_id=user_message_id,
            intent=intent,
            retrieval_plan={
                "priority": [
                    "synthesis_nodes",
                    "synthesis_relationships",
                    "attached_evidence",
                    "recent_observations",
                ],
                "scope": scope.model_dump(mode="json"),
                "mode": mode,
            },
            mode=mode,
            status="running",
            latency_ms=None,
        )

        try:
            packet = await self._retrieve_packet(
                tenant_id=tenant_id,
                viewer_id=viewer_id,
                query=query,
                scope=scope,
                mode=mode,
                intent=intent,
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            await self._store.update_retrieval_run_status(
                retrieval_run_id,
                status="completed",
                latency_ms=latency_ms,
            )
            await self._store.add_evidence_items(
                retrieval_run_id,
                [*packet.evidence, *packet.omitted],
            )

            payload = _compose_answer(
                query=query,
                scope=scope,
                mode=mode,
                intent=intent,
                packet=packet,
            )
            assistant_message_id = await self._store.add_message(
                session_id=session_id,
                role="assistant",
                content=payload.answer,
                structured_payload=payload.model_dump(mode="json"),
            )
            answer_id = await self._store.add_answer(
                session_id=session_id,
                message_id=assistant_message_id,
                retrieval_run_id=retrieval_run_id,
                payload=payload,
                mode=mode,
                scope=scope,
                latency_ms=latency_ms,
            )
            if _should_propose_state_change(query, intent, packet):
                change = await self._store.add_proposed_state_change(
                    tenant_id=tenant_id,
                    answer_id=answer_id,
                    proposed_op=_build_proposed_op(query, scope, packet),
                )
                payload.possible_state_change = change
                await self._store.update_answer_payload(answer_id, payload)
                await self._store.add_message(
                    session_id=session_id,
                    role="system",
                    content="Proposed state change created for validation.",
                    structured_payload=change.model_dump(mode="json"),
                )
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            await self._store.update_retrieval_run_status(
                retrieval_run_id,
                status="failed",
                latency_ms=latency_ms,
                error=f"{type(exc).__name__}: {exc}",
            )
            await self._store.add_message(
                session_id=session_id,
                role="system",
                content="Ask turn failed before an answer could be composed.",
                structured_payload={
                    "retrieval_run_id": str(retrieval_run_id),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            raise

        return AskTurnResponse(
            session=session,
            message_id=assistant_message_id,
            answer_id=answer_id,
            retrieval_run_id=retrieval_run_id,
            mode=mode,
            intent=intent,
            latency_ms=latency_ms,
            payload=payload,
        )

    async def expand_evidence(
        self,
        *,
        tenant_id: UUID,
        retrieval_run_id: UUID,
    ) -> tuple[list[AskEvidenceItem], list[AskEvidenceItem]]:
        return await self._store.list_evidence(
            retrieval_run_id,
            tenant_id=tenant_id,
        )

    async def act_on_proposed_change(
        self,
        *,
        tenant_id: UUID,
        change_id: UUID,
        action: str,
        note: str | None,
        delegate_to: str | None,
    ):
        return await self._store.act_on_proposed_change(
            tenant_id=tenant_id,
            change_id=change_id,
            action=action,
            note=note,
            delegate_to=delegate_to,
        )

    async def add_feedback(
        self,
        *,
        session_id: UUID,
        answer_id: UUID | None,
        viewer_id: UUID,
        feedback_type: str,
        payload: dict[str, Any],
    ) -> UUID:
        return await self._store.add_feedback(
            session_id=session_id,
            answer_id=answer_id,
            viewer_id=viewer_id,
            feedback_type=feedback_type,
            payload=payload,
        )

    async def _retrieve_packet(
        self,
        *,
        tenant_id: UUID,
        viewer_id: UUID,
        query: str,
        scope: AskScope,
        mode: AskMode,
        intent: str,
    ) -> RetrievalPacket:
        async with self._conn_provider() as conn:
            trigger = _trigger_for_scope(tenant_id, query, scope, mode)
            reader_result: SynthesisReaderResult | None = None
            try:
                reader_result = await self._reader.read(
                    conn=conn,
                    tenant_id=tenant_id,
                    trigger=trigger,
                    question_id=str(uuid7()),
                    question=query,
                    question_primitive=intent,
                )
            except Exception as exc:  # noqa: BLE001
                _log.warning("ask.sage_reader_failed", error=str(exc))

            models = list(reader_result.models) if reader_result else []
            observations = list(reader_result.observations) if reader_result else []
            omitted_pairs = list(reader_result.omitted_projection) if reader_result else []
            debug = dict(reader_result.debug or {}) if reader_result else {"reader": "failed"}

            if not models:
                models = await _fallback_models(conn, tenant_id, scope, query)
            if not observations:
                observations = await _fallback_observations(conn, tenant_id, scope)

            models = await _filter_models(conn, tenant_id, viewer_id, models)
            observations = await _filter_observations(conn, tenant_id, viewer_id, observations)
            evidence = _evidence_from_reader(reader_result, models, observations)
            evidence = [
                *_composed_evidence_from_observations(query, intent, observations),
                *evidence,
            ]
            evidence = _rank_evidence_for_packet(query, intent, evidence)
            omitted = [
                AskEvidenceItem(
                    id=uuid7(),
                    source_ref=_try_uuid(mid),
                    source_kind="omitted_model",
                    summary=f"Evidence projection omitted model {mid}: {reason}.",
                    strength="unknown",
                    omitted_reason=reason,
                    raw_payload={"source": "sage_projection"},
                )
                for mid, reason in omitted_pairs
            ]
            state_contract = compile_state_contract(
                query,
                _state_sources_for_packet(models, observations, evidence),
            ).to_dict()
            debug = {
                **debug,
                "state_contract": {
                    "required_slots": state_contract["required_slots"],
                    "covered_slots": state_contract["covered_slots"],
                    "missing_slots": state_contract["missing_slots"],
                    "premise_status": state_contract["premise_check"]["status"],
                },
            }
            return RetrievalPacket(
                models=models,
                observations=observations,
                evidence=evidence,
                omitted=omitted,
                debug=debug,
                state_contract=state_contract,
            )


def _trigger_for_scope(
    tenant_id: UUID,
    query: str,
    scope: AskScope,
    mode: AskMode,
) -> TriggerContext:
    model_ids = list(scope.root_node_ids)
    return TriggerContext(
        kind="T4" if mode in {"deep_inquiry", "background_review"} else "T1",
        tenant_id=tenant_id,
        seed_natural_text=f"{scope.label}: {query}",
        seed_entity_ids=[{"id": str(eid)} for eid in scope.related_entity_ids],
        member_model_ids=model_ids,
        seed_signature={
            "ask_scope_type": scope.type,
            "ask_scope_label": scope.label,
            "root_node_ids": [str(mid) for mid in model_ids],
        },
        subkind="ask_fyralis",
        max_hops=2,
    )


async def _fallback_models(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    scope: AskScope,
    query: str,
) -> list[ModelRow]:
    if scope.root_node_ids:
        rows = await conn.fetch(
            f"""
            SELECT {_MODEL_SELECT_COLS_SQL}
            FROM models
            WHERE tenant_id = $1 AND status = 'active' AND id = ANY($2::uuid[])
            ORDER BY activation DESC, created_at DESC
            LIMIT 24
            """,
            tenant_id,
            scope.root_node_ids,
        )
    else:
        terms = [t for t in _terms(query)[:4] if len(t) >= 3]
        if terms:
            conditions = " OR ".join(
                f'"natural" ILIKE ${idx}'
                for idx in range(3, 3 + len(terms))
            )
            rows = await conn.fetch(
                f"""
                SELECT {_MODEL_SELECT_COLS_SQL}
                FROM models
                WHERE tenant_id = $1 AND status = 'active' AND ({conditions})
                ORDER BY activation DESC, created_at DESC
                LIMIT $2
                """,
                tenant_id,
                24,
                *[f"%{term}%" for term in terms],
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT {_MODEL_SELECT_COLS_SQL}
                FROM models
                WHERE tenant_id = $1 AND status = 'active'
                ORDER BY activation DESC, created_at DESC
                LIMIT 24
                """,
                tenant_id,
            )
    return [_hydrate_model_row(row) for row in rows]


async def _fallback_observations(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    scope: AskScope,
) -> list[ObservationRow]:
    rows = await conn.fetch(
        """
        SELECT id, tenant_id, occurred_at, ingested_at, kind,
               source_channel, source_actor_ref, actor_id,
               content, content_text, embedding, embedding_pending,
               trust_tier, external_id, cause_id, sequence_num,
               entities_mentioned
        FROM observations
        WHERE tenant_id = $1
        ORDER BY occurred_at DESC
        LIMIT $2
        """,
        tenant_id,
        12 if scope.type == "whole_company" else 6,
    )
    return [ObservationRow.model_validate(dict(row)) for row in rows]


async def _filter_models(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    viewer_id: UUID,
    models: list[ModelRow],
) -> list[ModelRow]:
    visible: list[ModelRow] = []
    for model in models:
        decision = await can_read(
            viewer_id,
            {
                "kind": "model",
                "id": model.id,
                "tenant_id": model.tenant_id,
                "visible_to_subjects": model.visible_to_subjects,
                "scope_actors": model.scope_actors,
                "scope_entities": model.scope_entities,
            },
            conn=conn,
            tenant_id=tenant_id,
        )
        await record_override_if_needed(
            decision,
            actor_id=viewer_id,
            entity_type="model",
            entity_id=model.id,
            conn=conn,
            tenant_id=tenant_id,
        )
        if decision.allowed:
            visible.append(model)
    return visible


async def _filter_observations(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    viewer_id: UUID,
    observations: list[ObservationRow],
) -> list[ObservationRow]:
    visible: list[ObservationRow] = []
    for obs in observations:
        decision = await can_read(
            viewer_id,
            {
                "kind": "observation",
                "id": obs.id,
                "tenant_id": obs.tenant_id,
                "actor_id": obs.actor_id,
                "source_channel": obs.source_channel,
                "entities_mentioned": obs.entities_mentioned,
                "source_actor_ref": obs.source_actor_ref,
            },
            conn=conn,
            tenant_id=tenant_id,
        )
        await record_override_if_needed(
            decision,
            actor_id=viewer_id,
            entity_type="observation",
            entity_id=obs.id,
            conn=conn,
            tenant_id=tenant_id,
        )
        if decision.allowed:
            visible.append(obs)
    return visible


def _evidence_from_reader(
    reader_result: SynthesisReaderResult | None,
    models: list[ModelRow],
    observations: list[ObservationRow],
) -> list[AskEvidenceItem]:
    items: list[AskEvidenceItem] = []
    obs_ids = {o.id for o in observations}
    if reader_result:
        for projected in reader_result.projected_evidence:
            ref = _try_uuid(projected.get("evidence_id") or projected.get("source_ref"))
            kind = str(projected.get("evidence_kind") or "evidence")
            if kind == "observation" and ref not in obs_ids:
                continue
            summary = str(
                projected.get("summary")
                or projected.get("text")
                or projected.get("evidence_text")
                or "Projected evidence from Synthesis."
            )
            role = str(projected.get("role") or projected.get("evidence_role") or "")
            items.append(
                AskEvidenceItem(
                    id=uuid7(),
                    source_ref=ref,
                    source_kind=kind,
                    summary=_clip(summary, 360),
                    strength="counterevidence" if "counter" in role else "supporting",
                    supports_answer="counter" not in role,
                    is_counterevidence="counter" in role,
                    token_estimate=max(1, len(summary) // 4),
                    raw_payload=projected,
                )
            )
    if not items:
        for model in models[:6]:
            items.append(
                AskEvidenceItem(
                    id=uuid7(),
                    source_ref=model.id,
                    source_kind="model",
                    summary=_clip(_model_text(model), 360),
                    strength="supporting",
                    supports_answer=True,
                    token_estimate=max(1, len(_model_text(model)) // 4),
                    raw_payload={"confidence": model.confidence, "activation": model.activation},
                )
            )
        for obs in observations[:4]:
            items.append(
                AskEvidenceItem(
                    id=uuid7(),
                    source_ref=obs.id,
                    source_kind="observation",
                    summary=_clip(obs.content_text, 360),
                    strength="contextual",
                    supports_answer=True,
                    token_estimate=max(1, len(obs.content_text) // 4),
                    raw_payload={"source_channel": obs.source_channel, "occurred_at": obs.occurred_at.isoformat()},
                )
            )
    return items


def _rank_evidence_for_packet(
    query: str,
    intent: str,
    evidence: list[AskEvidenceItem],
) -> list[AskEvidenceItem]:
    if len(evidence) <= 1:
        return evidence

    alternatives = extract_query_alternatives(query)
    desired_roles = _ask_desired_roles(query, intent)
    selected: list[AskEvidenceItem] = []
    selected_ids: set[UUID] = set()

    def add(item: AskEvidenceItem) -> None:
        if item.id in selected_ids:
            return
        selected.append(item)
        selected_ids.add(item.id)

    if alternatives:
        for alternative in alternatives[:8]:
            best = _best_evidence_for_alternative(
                alternative,
                evidence,
                excluded_ids=selected_ids,
                desired_roles=desired_roles,
            )
            if best is not None:
                best.raw_payload = {
                    **best.raw_payload,
                    "packet_selection_reason": "alternative_coverage",
                    "covered_alternative": alternative,
                }
                add(best)

    ranked = sorted(
        evidence,
        key=lambda item: _packet_evidence_score(query, desired_roles, item),
        reverse=True,
    )

    family_counts: dict[str, int] = {}
    deferred: list[AskEvidenceItem] = []
    for item in ranked:
        if item.id in selected_ids:
            continue
        family = _evidence_family_key(item)
        count = family_counts.get(family, 0)
        family_cap = 3 if item.source_kind == "composed_chain" else 2
        if family and count >= family_cap:
            deferred.append(item)
            continue
        add(item)
        if family:
            family_counts[family] = count + 1

    for item in deferred:
        add(item)

    return selected


def _best_evidence_for_alternative(
    alternative: str,
    evidence: list[AskEvidenceItem],
    *,
    excluded_ids: set[UUID],
    desired_roles: set[str],
) -> AskEvidenceItem | None:
    alt_terms = set(_terms(alternative))
    if not alt_terms:
        return None
    candidates: list[tuple[float, AskEvidenceItem]] = []
    for item in evidence:
        if item.id in excluded_ids:
            continue
        summary_terms = set(_terms(item.summary))
        overlap = len(alt_terms & summary_terms)
        if overlap <= 0 and alternative.casefold() not in item.summary.casefold():
            continue
        score = _packet_evidence_score("", desired_roles, item)
        score += 0.18 + min(0.18, 0.05 * overlap)
        candidates.append((score, item))
    if not candidates:
        return None
    candidates.sort(key=lambda pair: pair[0], reverse=True)
    return candidates[0][1]


def _packet_evidence_score(
    query: str,
    desired_roles: set[str],
    item: AskEvidenceItem,
) -> float:
    score = 0.0
    if item.source_kind == "composed_chain":
        score += 0.34
    if item.strength == "decisive":
        score += 0.24
    elif item.strength == "supporting":
        score += 0.15
    elif item.strength == "counterevidence":
        score += 0.18
    if item.supports_answer:
        score += 0.08
    roles = _ask_evidence_roles(item)
    score += min(0.30, 0.075 * len(roles & desired_roles))
    query_terms = set(_terms(query))
    if query_terms:
        score += min(0.16, 0.025 * len(query_terms & set(_terms(item.summary))))
    raw_score = item.raw_payload.get("score")
    try:
        score += min(0.12, max(0.0, float(raw_score)) * 0.08)
    except (TypeError, ValueError):
        pass
    return score


def _evidence_family_key(item: AskEvidenceItem) -> str:
    payload = item.raw_payload if isinstance(item.raw_payload, dict) else {}
    for key in (
        "node_id",
        "source_model_id",
        "fyralis_model_id",
        "trajectory_id",
        "session_id",
        "case_id",
    ):
        value = payload.get(key)
        if value:
            return f"{key}:{value}"
    if item.source_ref is not None:
        return f"{item.source_kind}:{item.source_ref}"
    return item.source_kind


def _state_sources_for_packet(
    models: list[ModelRow],
    observations: list[ObservationRow],
    evidence: list[AskEvidenceItem],
) -> list[StateSource]:
    sources: list[StateSource] = []
    for model in models[:48]:
        sources.append(
            StateSource(
                source_kind="model",
                source_ref=f"model:{model.id}",
                text=_model_text(model),
                occurred_at=model.created_at,
                confidence=float(model.confidence or 0.5),
                metadata={
                    "activation": model.activation,
                    "claim_role": model.claim_role,
                    "status": model.status,
                },
            )
        )
    for obs in observations[:80]:
        sources.append(
            StateSource(
                source_kind="observation",
                source_ref=f"observation:{obs.id}",
                text=obs.content_text or str(obs.content or ""),
                occurred_at=obs.occurred_at,
                confidence=_trust_confidence(obs.trust_tier),
                metadata={
                    "source_channel": obs.source_channel,
                    "kind": obs.kind,
                    "sequence_num": obs.sequence_num,
                },
            )
        )
    for item in evidence[:80]:
        ref = item.source_ref or item.id
        sources.append(
            StateSource(
                source_kind=item.source_kind,
                source_ref=f"{item.source_kind}:{ref}",
                text=item.summary,
                confidence=_evidence_confidence(item),
                metadata=item.raw_payload,
            )
        )
    return sources


def _trust_confidence(trust_tier: str | None) -> float:
    return {
        "authoritative": 0.92,
        "reputable": 0.78,
        "model": 0.62,
        "low": 0.38,
    }.get(str(trust_tier or "").casefold(), 0.55)


def _evidence_confidence(item: AskEvidenceItem) -> float:
    if item.strength == "decisive":
        return 0.9
    if item.strength == "counterevidence":
        return 0.86
    if item.strength == "supporting":
        return 0.74
    if item.strength == "weak":
        return 0.42
    return 0.58


def _composed_evidence_from_observations(
    query: str,
    intent: str,
    observations: list[ObservationRow],
) -> list[AskEvidenceItem]:
    if len(observations) < 2 or not _ask_needs_composition(query, intent):
        return []
    desired_roles = _ask_desired_roles(query, intent)
    selected: list[ObservationRow] = []
    selected_ids: set[UUID] = set()
    covered: set[str] = set()
    for obs in sorted(
        observations,
        key=lambda item: _ask_observation_score(query, desired_roles, item),
        reverse=True,
    ):
        if obs.id in selected_ids:
            continue
        roles = _ask_observation_roles(obs)
        new_roles = (roles & desired_roles) - covered
        if not new_roles and len(selected) >= 2:
            continue
        selected.append(obs)
        selected_ids.add(obs.id)
        covered.update(roles & desired_roles)
        if len(selected) >= 5 or (len(selected) >= 3 and covered >= desired_roles):
            break
    if len(selected) < 2 or len(covered) < 2:
        return []

    selected.sort(key=lambda item: (item.occurred_at, item.sequence_num or 0, str(item.id)))
    lines = [
        "Composed evidence chain from accessible Synthesis observations:",
    ]
    for obs in selected:
        role_text = ",".join(sorted(_ask_observation_roles(obs) & desired_roles)) or "context"
        lines.append(
            "- "
            f"{obs.occurred_at.isoformat()} | roles={role_text} | "
            f"{_clip(obs.content_text, 260)}"
        )
    summary = "\n".join(lines)
    return [
        AskEvidenceItem(
            id=uuid7(),
            source_ref=None,
            source_kind="composed_chain",
            summary=_clip(summary, 900),
            strength="supporting",
            supports_answer=True,
            token_estimate=max(1, len(summary) // 4),
            raw_payload={
                "source": "ask_query_local_composition",
                "source_observation_ids": [str(obs.id) for obs in selected],
                "covered_roles": sorted(covered),
            },
        )
    ]


def _ask_needs_composition(query: str, intent: str) -> bool:
    text = f"{query} {intent}".casefold()
    return any(
        marker in text
        for marker in (
            "after",
            "because",
            "before",
            "causal",
            "caused",
            "during",
            "final",
            "led to",
            "mental model",
            "root cause",
            "solution",
            "triggered",
            "why",
        )
    ) or len(extract_query_alternatives(query)) >= 2 or any(
        marker in text
        for marker in (
            "compare",
            "exact",
            "how many",
            "largest",
            "least",
            "number",
            "price",
            "quantity",
            "which",
        )
    )


def _ask_desired_roles(query: str, intent: str) -> set[str]:
    text = f"{query} {intent}".casefold()
    roles = {"cause", "transition", "outcome"}
    if any(marker in text for marker in ("mental model", "assume", "believe", "thought")):
        roles.update({"viewpoint", "decision"})
    if any(marker in text for marker in ("who", "owner", "responsible", "lead")):
        roles.add("owner")
    if any(marker in text for marker in ("root cause", "postmortem", "final")):
        roles.add("diagnosis")
    if any(marker in text for marker in ("final", "solution", "resolved", "replacement")):
        roles.update({"decision", "final_outcome"})
    if any(marker in text for marker in ("after", "as of", "before", "during", "latest", "last", "final")):
        roles.add("temporal_anchor")
    if any(
        marker in text
        for marker in (
            "checkbox",
            "compare",
            "exact",
            "how many",
            "largest",
            "least",
            "number",
            "price",
            "quantity",
            "value",
            "which",
        )
    ):
        roles.add("exact_value")
    if len(extract_query_alternatives(query)) >= 2:
        roles.add("alternative_coverage")
    return roles


def _ask_observation_score(
    query: str,
    desired_roles: set[str],
    obs: ObservationRow,
) -> float:
    roles = _ask_observation_roles(obs)
    score = 0.4 * len(roles & desired_roles)
    content = (obs.content_text or "").casefold()
    text = query.casefold()
    if "mental model" in text and "diagnosis" in roles and "viewpoint" not in roles:
        score -= 0.35
    if any(marker in text for marker in ("during", "at the time")) and any(
        marker in content for marker in ("postmortem", "post-mortem", "final")
    ):
        score -= 0.2
    if any(marker in text for marker in ("final", "solution", "resolved")):
        if "decision" in roles:
            score += 0.25
        if "final_outcome" in roles:
            score += 0.22
        elif "outcome" in roles:
            score += 0.08
    return score


def _ask_observation_roles(obs: ObservationRow) -> set[str]:
    content = (obs.content_text or "").casefold()
    roles: set[str] = set()
    if obs.occurred_at is not None or obs.sequence_num is not None:
        roles.add("temporal_anchor")
    if any(marker in content for marker in ("because", "caused", "due to", "root cause", "blocked")):
        roles.add("cause")
    if any(marker in content for marker in ("after", "before", "changed", "led to", "shifted")):
        roles.add("transition")
    if any(
        marker in content
        for marker in (
            "completed",
            "deployed",
            "impact",
            "replacement",
            "resolved",
            "resolution",
            "risk",
            "rollback",
            "solution",
        )
    ):
        roles.add("outcome")
    if any(
        marker in content
        for marker in (
            "completed",
            "deployed",
            "final solution",
            "replacement shipped",
            "resolved",
            "resolution:",
            "solution shipped",
        )
    ):
        roles.add("final_outcome")
    if any(marker in content for marker in ("postmortem", "post-mortem", "final", "root cause")):
        roles.add("diagnosis")
    if any(marker in content for marker in ("assumed", "believed", "expected", "thought", "understood")):
        roles.add("viewpoint")
    if any(
        marker in content
        for marker in (
            "chose",
            "decided",
            "decision",
            "implemented",
            "proposed",
            "replacement",
            "split",
            "workaround",
        )
    ):
        roles.add("decision")
    if any(marker in content for marker in ("owner", "assigned", "responsible", "lead")):
        roles.add("owner")
    if re.search(r"\$\d+(?:\.\d+)?|\b\d+(?:\.\d+)?\b", content) or any(
        marker in content
        for marker in (
            "checkbox choice",
            "count =",
            "field",
            "largest",
            "least",
            "price",
            "quantity",
            "status",
            "value",
        )
    ):
        roles.add("exact_value")
    return roles


def _compose_answer(
    *,
    query: str,
    scope: AskScope,
    mode: AskMode,
    intent: str,
    packet: RetrievalPacket,
) -> AskAnswerPayload:
    top_models = packet.models[:5]
    evidence = packet.evidence[:8]
    state_contract = packet.state_contract or {}
    state_facts = _select_state_facts_for_payload(state_contract)
    premise_check = dict(state_contract.get("premise_check") or {})
    counter = [
        *_state_contract_counterevidence(state_contract),
        *[item.summary for item in evidence if item.is_counterevidence],
    ][:4]
    requirements = _ask_answer_requirements(query, intent)
    sufficiency = _ask_packet_sufficiency(query, requirements, evidence)
    state_contract_answer = _state_contract_answer(
        query,
        scope,
        state_contract,
        top_models,
    )
    evidence_shaped_answer = _evidence_shaped_answer(
        query,
        scope,
        top_models,
        evidence,
        sufficiency=sufficiency,
    )
    if state_contract_answer:
        answer = state_contract_answer
    elif evidence_shaped_answer:
        answer = evidence_shaped_answer
    elif top_models:
        lead = _model_text(top_models[0])
        answer = (
            f"In {scope.label}, Fyralis' current Synthesis read points to: "
            f"{_clip(lead, 420)}"
        )
    elif evidence:
        answer = (
            f"I found relevant evidence in {scope.label}, but no strong active "
            f"Synthesis node currently dominates the answer."
        )
    else:
        answer = (
            f"I do not have enough accessible Synthesis state in {scope.label} "
            f"to answer this with confidence."
        )
    confidence = _confidence(top_models, evidence, len(packet.omitted), mode)
    confidence = _confidence_after_sufficiency(confidence, sufficiency)
    confidence = _confidence_after_state_contract(confidence, state_contract)
    why = [
        _clip(_model_text(model), 180)
        for model in top_models[:3]
    ] or [
        _clip(item.summary, 180)
        for item in evidence[:3]
    ]
    impact = _impact_lines(query, top_models, scope)
    unknowns = []
    if _requires_external_tool_surface(query) and not _packet_has_external_tool_result(packet):
        unknowns.append(
            "This question appears to require repository, filesystem, ticket, or tool inspection beyond the compact Synthesis packet."
        )
    if packet.omitted:
        unknowns.append(
            f"{len(packet.omitted)} evidence item(s) were omitted from the compact packet; expand evidence to inspect them."
        )
    unknowns.extend(_state_contract_unknowns(state_contract))
    if sufficiency["missing_roles"]:
        unknowns.append(
            "The compact packet is missing expected answer roles: "
            + ", ".join(sufficiency["missing_roles"])
            + "."
        )
    if confidence < 0.55:
        unknowns.append("Accessible Synthesis support is thin or contradictory.")
    if not unknowns:
        unknowns.append("No major unknowns surfaced in the bounded Synthesis read.")
    actions = _recommended_actions(intent, confidence, bool(packet.omitted))
    return AskAnswerPayload(
        answer=answer,
        confidence=confidence,
        premise_check=premise_check,
        state_facts=state_facts,
        why=why,
        counterevidence=counter or ["No direct counterevidence survived the current packet."],
        impact=impact,
        recommended_actions=actions,
        unknowns=unknowns,
        related_nodes=[
            AskRelatedNode(
                id=model.id,
                label=_clip(_model_text(model), 120),
                confidence=model.confidence,
                activation=model.activation,
                role="supporting",
            )
            for model in top_models
        ],
        evidence=evidence,
        omitted_evidence_count=len(packet.omitted),
    )


def _evidence_shaped_answer(
    query: str,
    scope: AskScope,
    models: list[ModelRow],
    evidence: list[AskEvidenceItem],
    *,
    sufficiency: dict[str, Any],
) -> str | None:
    if not evidence:
        return None
    if not _query_needs_evidence_shaped_answer(query):
        return None
    best = _best_answer_evidence(query, evidence)
    if best is None:
        return None
    if sufficiency["requires_finality"] and sufficiency["missing_roles"]:
        return None
    prefix = f"In {scope.label}, the strongest accessible evidence says: "
    if best.source_kind == "composed_chain":
        prefix = f"In {scope.label}, the accessible evidence chain says: "
    model_context = ""
    if models:
        model_context = f" Current Synthesis read: {_clip(_model_text(models[0]), 180)}"
    answer_chars = 760 if best.source_kind == "composed_chain" else 520
    return prefix + _clip(best.summary, answer_chars) + model_context


def _select_state_facts_for_payload(
    state_contract: dict[str, Any],
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    facts = [dict(fact) for fact in state_contract.get("facts") or []]
    if not facts:
        return []
    slot_order = [
        *[str(slot) for slot in state_contract.get("required_slots") or []],
        "premise_challenge",
        "dynamic_state",
        "current_stage",
        "current_blocker",
        "current_owner",
        "temporal_anchor",
        "exact_value",
        "workflow_missing_step",
        "recurring_gotcha",
    ]
    selected: list[dict[str, Any]] = []
    selected_ids: set[int] = set()
    for slot in _dedupe_lines(slot_order):
        slot_facts = [
            (idx, fact)
            for idx, fact in enumerate(facts)
            if fact.get("slot") == slot and idx not in selected_ids
        ]
        if not slot_facts:
            continue
        idx, fact = max(
            slot_facts,
            key=lambda item: (
                float(item[1].get("confidence") or 0.0),
                str(item[1].get("status") or "") in {"current", "changed", "expanded"},
            ),
        )
        selected.append(fact)
        selected_ids.add(idx)
        if len(selected) >= limit:
            return selected
    for idx, fact in enumerate(facts):
        if idx in selected_ids:
            continue
        selected.append(fact)
        if len(selected) >= limit:
            break
    return selected


def _state_contract_answer(
    query: str,
    scope: AskScope,
    state_contract: dict[str, Any],
    models: list[ModelRow],
) -> str | None:
    facts = list(state_contract.get("facts") or [])
    premise = dict(state_contract.get("premise_check") or {})
    status = str(premise.get("status") or "not_checked")
    required_slots = set(str(slot) for slot in state_contract.get("required_slots") or [])
    if "current_owner" in required_slots:
        owner_facts = [fact for fact in facts if fact.get("slot") == "current_owner"]
        if owner_facts:
            owner = owner_facts[0]
            if owner.get("status") in {"missing", "unsupported"}:
                return (
                    f"No explicit owner is represented in Synthesis for {scope.label}. "
                    f"{_clip(str(owner.get('value') or ''), 420)}"
                )
            return (
                f"In {scope.label}, the represented owner is "
                f"{_clip(str(owner.get('value') or ''), 420)}."
            )

    if status in {"stale_or_incomplete", "unsupported"}:
        opening = (
            "That premise is incomplete."
            if status == "stale_or_incomplete"
            else "That premise is unsupported."
        )
        lines = _state_fact_lines(
            facts,
            (
                "premise_challenge",
                "dynamic_state",
                "current_stage",
                "current_blocker",
                "current_owner",
                "workflow_missing_step",
                "recurring_gotcha",
            ),
            limit=4,
        )
        if not lines:
            lines = [str(item) for item in premise.get("corrections") or []][:3]
        detail = " ".join(lines) or "The compact Synthesis read found counterevidence to the framing."
        model_context = ""
        if models:
            model_context = f" Current Synthesis read: {_clip(_model_text(models[0]), 180)}"
        return f"{opening} In {scope.label}, {_clip(detail, 620)}{model_context}"

    answerable_state_slots = {
        "current_blocker",
        "current_stage",
        "dynamic_state",
        "exact_value",
        "workflow_missing_step",
        "recurring_gotcha",
    }
    if required_slots & answerable_state_slots:
        lines = _state_fact_lines(
            facts,
            (
                "dynamic_state",
                "current_stage",
                "current_blocker",
                "exact_value",
                "temporal_anchor",
                "workflow_missing_step",
                "recurring_gotcha",
            ),
            limit=4,
        )
        if lines:
            return f"In {scope.label}, Fyralis' state read is: {_clip(' '.join(lines), 700)}"
    return None


def _state_fact_lines(
    facts: list[dict[str, Any]],
    slots: tuple[str, ...],
    *,
    limit: int,
) -> list[str]:
    labels = {
        "current_blocker": "Blocker",
        "current_owner": "Owner",
        "current_stage": "Stage",
        "dynamic_state": "State change",
        "exact_value": "Exact value",
        "premise_challenge": "Premise check",
        "recurring_gotcha": "Recurring trap",
        "temporal_anchor": "Observed at",
        "workflow_missing_step": "Workflow gap",
    }
    out: list[str] = []
    seen: set[str] = set()
    for slot in slots:
        for fact in facts:
            if fact.get("slot") != slot:
                continue
            value = _clip(str(fact.get("value") or ""), 180)
            key = f"{slot}:{value.casefold()}"
            if not value or key in seen:
                continue
            seen.add(key)
            out.append(f"{labels.get(slot, slot)}: {value}.")
            if len(out) >= limit:
                return out
    return out


def _state_contract_counterevidence(state_contract: dict[str, Any]) -> list[str]:
    premise = dict(state_contract.get("premise_check") or {})
    corrections = [str(item) for item in premise.get("corrections") or []]
    facts = [
        str(fact.get("value"))
        for fact in state_contract.get("facts") or []
        if fact.get("slot") == "premise_challenge"
    ]
    return _dedupe_lines([*corrections, *facts])[:4]


def _state_contract_unknowns(state_contract: dict[str, Any]) -> list[str]:
    unknowns: list[str] = []
    premise = dict(state_contract.get("premise_check") or {})
    status = str(premise.get("status") or "not_checked")
    reason = str(premise.get("reason") or "")
    if status in {"stale_or_incomplete", "unsupported", "unknown"} and reason:
        unknowns.append(reason)
    missing = [
        str(slot)
        for slot in state_contract.get("missing_slots") or []
        if str(slot) != "premise_challenge"
    ]
    if missing:
        unknowns.append(
            "The compact Synthesis packet is missing state slots: "
            + ", ".join(missing)
            + "."
        )
    return unknowns


def _confidence_after_state_contract(
    confidence: float,
    state_contract: dict[str, Any],
) -> float:
    premise = dict(state_contract.get("premise_check") or {})
    status = str(premise.get("status") or "not_checked")
    missing_count = len(state_contract.get("missing_slots") or [])
    penalty = 0.0
    if status == "unknown":
        penalty += 0.1
    elif status == "unsupported":
        penalty += 0.04
    elif status == "stale_or_incomplete":
        penalty += 0.02
    penalty += min(0.16, 0.04 * missing_count)
    return round(max(0.05, confidence - penalty), 2)


def _dedupe_lines(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        clean = _clip(str(line or ""), 360)
        key = clean.casefold()
        if not clean or key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out


def _ask_answer_requirements(query: str, intent: str) -> list[dict[str, Any]]:
    text = f"{query} {intent}".casefold()
    requirements: list[dict[str, Any]] = []

    def add(kind: str, roles: set[str]) -> None:
        if any(item["kind"] == kind for item in requirements):
            return
        requirements.append({"kind": kind, "roles": sorted(roles)})

    if any(marker in text for marker in ("why", "cause", "caused", "root cause", "mechanism")):
        causal_roles = {"cause"}
        if any(marker in text for marker in ("what did it cause", "led to", "outcome", "result", "impact")):
            causal_roles.add("outcome")
        add("causal_mechanism", causal_roles)
    if any(marker in text for marker in ("after", "before", "during", "first")):
        add("temporal_scope", {"temporal_anchor"})
    if any(
        marker in text
        for marker in (
            "as of",
            "current",
            "final",
            "last",
            "latest",
            "most recent",
            "previous",
        )
    ):
        add("temporal_scope", {"temporal_anchor"})
    if any(
        marker in text
        for marker in (
            "checkbox",
            "count",
            "exact",
            "false",
            "how many",
            "largest",
            "least",
            "number",
            "price",
            "quantity",
            "true",
            "value",
        )
    ):
        add("exact_value", {"exact_value"})
    if len(extract_query_alternatives(query)) >= 2:
        add("alternatives", {"alternative_coverage"})
    if any(marker in text for marker in ("final", "solution", "resolved", "replacement")):
        add("finality", {"decision", "final_outcome"})
    if any(marker in text for marker in ("who", "owner", "responsible", "lead")):
        add("actor", {"owner"})
    return requirements


def _ask_packet_sufficiency(
    query: str,
    requirements: list[dict[str, Any]],
    evidence: list[AskEvidenceItem],
) -> dict[str, Any]:
    required_roles: set[str] = set()
    for requirement in requirements:
        raw_roles = requirement.get("roles")
        if isinstance(raw_roles, list):
            required_roles.update(str(role) for role in raw_roles)
    covered_roles: set[str] = set()
    for item in evidence:
        raw_roles = item.raw_payload.get("covered_roles")
        if isinstance(raw_roles, list):
            covered_roles.update(str(role) for role in raw_roles)
        covered_roles.update(_ask_evidence_roles(item))
        if item.source_kind == "composed_chain":
            covered_roles.add("composed_chain")
    alternatives = extract_query_alternatives(query)
    if len(alternatives) >= 2:
        covered_alternatives = _covered_alternatives(alternatives, evidence)
        if len(covered_alternatives) >= min(2, len(alternatives)):
            covered_roles.add("alternative_coverage")
    return {
        "required_roles": sorted(required_roles),
        "covered_roles": sorted(covered_roles),
        "missing_roles": sorted(required_roles - covered_roles),
        "requires_finality": any(item.get("kind") == "finality" for item in requirements),
    }


def _ask_evidence_roles(item: AskEvidenceItem) -> set[str]:
    roles = _ask_summary_roles(item.summary)
    payload = item.raw_payload if isinstance(item.raw_payload, dict) else {}
    if payload.get("occurred_at") or payload.get("source_observation_ids"):
        roles.add("temporal_anchor")
    if payload.get("covered_alternative") or payload.get("packet_selection_reason") == "alternative_coverage":
        roles.add("alternative_coverage")
    reason = str(payload.get("reason") or payload.get("sage_projection_reason") or "")
    if "counter" in reason:
        roles.add("counterevidence")
    if "support" in reason:
        roles.add("support")
    return roles


def _covered_alternatives(
    alternatives: tuple[str, ...],
    evidence: list[AskEvidenceItem],
) -> set[str]:
    covered: set[str] = set()
    for item in evidence:
        payload = item.raw_payload if isinstance(item.raw_payload, dict) else {}
        raw = payload.get("covered_alternative")
        if raw:
            covered.add(str(raw).casefold())
            continue
        summary = item.summary.casefold()
        for alternative in alternatives:
            alt = alternative.casefold()
            alt_terms = set(_terms(alternative))
            if alt in summary or alt_terms & set(_terms(item.summary)):
                covered.add(alt)
    return covered


def _ask_summary_roles(summary: str) -> set[str]:
    content = summary.casefold()
    roles: set[str] = set()
    if any(marker in content for marker in ("because", "caused", "due to", "root cause", "blocked")):
        roles.add("cause")
    if any(marker in content for marker in ("after", "before", "changed", "led to", "split")):
        roles.add("transition")
    if any(marker in content for marker in ("decision", "decided", "proposed", "replacement", "split")):
        roles.add("decision")
    if any(marker in content for marker in ("completed", "deployed", "resolved", "solution", "impact")):
        roles.add("outcome")
    if any(
        marker in content
        for marker in (
            "completed",
            "deployed",
            "final solution",
            "replacement shipped",
            "resolved",
            "resolution:",
            "solution shipped",
        )
    ):
        roles.add("final_outcome")
    if any(marker in content for marker in ("owner", "responsible", "lead", "sender:")):
        roles.add("owner")
    if any(marker in content for marker in ("timestamp", "event", "202")):
        roles.add("temporal_anchor")
    if re.search(r"\$\d+(?:\.\d+)?|\b\d+(?:\.\d+)?\b", content):
        roles.add("exact_value")
    if any(
        marker in content
        for marker in (
            "checkbox choice",
            "count =",
            "field",
            "largest",
            "least",
            "price",
            "quantity",
            "status",
            "value",
        )
    ):
        roles.add("exact_value")
    return roles


def _confidence_after_sufficiency(confidence: float, sufficiency: dict[str, Any]) -> float:
    missing = len(sufficiency.get("missing_roles") or [])
    if missing <= 0:
        return confidence
    penalty = min(0.28, 0.09 * missing)
    return round(max(0.05, confidence - penalty), 2)


def _query_needs_evidence_shaped_answer(query: str) -> bool:
    text = query.casefold()
    return any(
        marker in text
        for marker in (
            "after",
            "before",
            "caused",
            "during",
            "evidence",
            "exact",
            "how many",
            "mechanism",
            "metric",
            "owner",
            "reason",
            "root cause",
            "specific",
            "what was",
            "which",
            "who",
            "why",
        )
    )


def _best_answer_evidence(
    query: str,
    evidence: list[AskEvidenceItem],
) -> AskEvidenceItem | None:
    query_terms = set(_terms(query))
    ranked = []
    for index, item in enumerate(evidence):
        summary_terms = set(_terms(item.summary))
        score = 0.0
        if item.supports_answer:
            score += 0.4
        if item.source_kind == "composed_chain":
            score += 0.25
        if item.strength == "decisive":
            score += 0.2
        elif item.strength == "supporting":
            score += 0.12
        if query_terms:
            score += min(0.25, 0.03 * len(query_terms & summary_terms))
        score -= 0.015 * index
        ranked.append((score, -index, item))
    if not ranked:
        return None
    ranked.sort(reverse=True)
    best = ranked[0][2]
    return best if ranked[0][0] > 0.0 else None


def _requires_external_tool_surface(query: str) -> bool:
    text = query.casefold()
    return any(
        marker in text
        for marker in (
            "clone the repository",
            "examine the source",
            "filesystem",
            "git history",
            "inspect the repository",
            "line number",
            "repository code",
            "ticket descriptions",
        )
    )


def _packet_has_external_tool_result(packet: RetrievalPacket) -> bool:
    artifact_markers = (
        "commit",
        "file",
        "function",
        "line",
        "pull request",
        "repository",
        ".py",
        ".ts",
        ".tsx",
    )
    for item in packet.evidence:
        payload_text = " ".join(
            str(value)
            for value in (
                item.source_kind,
                item.summary,
                item.raw_payload.get("source"),
                item.raw_payload.get("retrieval_system"),
            )
        ).casefold()
        if any(marker in payload_text for marker in artifact_markers):
            return True
    return False


def _should_propose_state_change(query: str, intent: str, packet: RetrievalPacket) -> bool:
    q = query.casefold()
    gap_terms = (
        "missing", "stale", "contradict", "wrong", "outdated",
        "hidden blocker", "not captured", "should update",
    )
    premise_status = str(
        (packet.state_contract.get("premise_check") or {}).get("status") or ""
    )
    return intent == "state_gap_inquiry" or any(term in q for term in gap_terms) or any(
        item.is_counterevidence for item in packet.evidence
    ) or premise_status in {"stale_or_incomplete", "unsupported"}


def _build_proposed_op(query: str, scope: AskScope, packet: RetrievalPacket) -> dict[str, Any]:
    return {
        "op": "validate_synthesis_gap",
        "query": query,
        "scope": scope.model_dump(mode="json"),
        "evidence_refs": [
            {"kind": item.source_kind, "id": str(item.source_ref)}
            for item in packet.evidence[:8]
            if item.source_ref is not None
        ],
        "candidate_model_ids": [str(model.id) for model in packet.models[:8]],
        "validation_required": True,
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }


def _confidence(
    models: list[ModelRow],
    evidence: list[AskEvidenceItem],
    omitted_count: int,
    mode: AskMode,
) -> float:
    if not models and not evidence:
        return 0.18
    model_component = (
        sum(float(m.confidence or 0.5) for m in models[:5]) / max(1, min(len(models), 5))
        if models else 0.42
    )
    evidence_component = min(0.2, len([e for e in evidence if e.supports_answer]) * 0.035)
    omission_penalty = min(0.18, omitted_count * 0.025)
    mode_penalty = 0.05 if mode == "background_review" else 0.0
    return round(max(0.05, min(0.95, model_component + evidence_component - omission_penalty - mode_penalty)), 2)


def _impact_lines(query: str, models: list[ModelRow], scope: AskScope) -> list[str]:
    if not models:
        return [
            "Decision impact is limited until Fyralis has stronger accessible Synthesis support."
        ]
    q = query.casefold()
    if "risk" in q or "block" in q:
        return [
            f"This may change prioritization inside {scope.label}.",
            "Treat the top related nodes as the current risk boundary until deeper review says otherwise.",
        ]
    return [
        f"This answer is scoped to {scope.label}; broader conclusions need wider scope."
    ]


def _recommended_actions(intent: str, confidence: float, has_omissions: bool) -> list[str]:
    actions = []
    if confidence < 0.55:
        actions.append("Ask for deeper review before changing canonical state.")
    if has_omissions:
        actions.append("Expand evidence and inspect omitted items.")
    if intent == "state_gap_inquiry":
        actions.append("Review the proposed state change through validation.")
    if not actions:
        actions.append("Use this as the current Synthesis read; no mutation is needed.")
    return actions


def _model_text(model: ModelRow) -> str:
    natural = getattr(model, "natural", "") or ""
    if natural:
        return str(natural)
    proposition = getattr(model, "proposition", None)
    return str(proposition or model.id)


def _terms(text: str) -> list[str]:
    return [
        part.strip(".,:;!?()[]{}\"'")
        for part in text.casefold().split()
        if len(part.strip(".,:;!?()[]{}\"'")) >= 3
    ]


def _clip(text: str, limit: int) -> str:
    clean = " ".join(str(text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


def _try_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None
