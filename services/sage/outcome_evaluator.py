"""services.sage.outcome_evaluator — Phase 13 Outcome Evaluator (writer-side).

Implements the "Outcome Evaluator" half of Phase 13 of the SAGE-inspired
self-evolution architecture. See:
  * fyralis-sage-synthesis-self-evolution.md §5.5 (component charter)
  * fyralis-sage-synthesis-self-evolution.md §15 (outcome events + labels)
  * fyralis-sage-synthesis-self-evolution.md §17.1 (reward features)
  * fyralis-sage-synthesis-self-evolution.md Phase 13 (acceptance criteria)

The evaluator is the bridge between the inquiry/think pipeline and the
Topology Optimizer. Given a completed `inquiry_sessions` row + its
associated `think_runs` row + the validated diff captured on
`think_runs.ops_applied`, it derives:

  1. A stream of typed events appended to `inquiry_outcome_events` via
     `OutcomeEventsRepo.append`. The event_type vocabulary is the
     `OUTCOME_EVENT_TYPES` frozenset declared in
     services/sage/inquiry_traces/types.py.

  2. A dense `reward_features` map (doc §17.1) that the synthesis-writer
     reward function (Phase 17) will consume. Every feature is a float
     in [0.0, 2.0]; the few features that have no v1 signal are placed
     at a documented sentinel value (0.0) with a TODO.

  3. A persisted `outcome_quality_assessed` event plus a returned
     `OutcomeQualitySignal`. This is the anti-proxy layer: it separates
     writer failure, missing evidence, dropped counterevidence, packet
     bloat, and low-value leakage so later optimizers reinforce the
     actual bottleneck rather than a vague success/failure scalar.

The evaluator is read-mostly: it never writes to the canonical truth
layer (models / model_edges / acts / resources). Its only writes are
INSERTs into `inquiry_outcome_events`. The Topology Optimizer (the
other Phase 13 half, built in parallel) is the one that reads these
events back and applies discovery-layer updates.

Idempotency
-----------
`evaluate()` is idempotent at the event-type-x-key level: a re-run
loads the events already emitted for the session, deduplicates against
the payload "key" we emit for each event type (e.g. `evidence_id` for
`retrieved_evidence_used_in_packet`, `model_id` for
`node_used_in_valid_diff`, `(source_model_id, target_model_id,
edge_kind)` for `path_used_in_valid_diff`), and only appends new ones.
The reward features are recomputed fresh every call — they are derived
from current state, not cumulative.

Design notes
------------
* No LLM, no embeddings, no async fanout. Pure SQL + Python.
* The evaluator accepts an optional `outcome_events_repo=` so call
  sites that already own a repo (e.g. an inquiry executor wrapper) can
  share it. When unset we construct one bound to the same tenant_id.
* The optional `conn=` on `evaluate()` lets callers participate in an
  outer transaction; if `None`, we acquire from the pool.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import CompanyOSError
from services.sage.inquiry_traces.repo import OutcomeEventsRepo


_log = structlog.get_logger(__name__)


class OutcomeEvaluatorError(CompanyOSError):
    default_code = "outcome_evaluator_error"


# ---------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OutcomeQualitySignal:
    """Dense quality diagnosis for one inquiry/writer outcome.

    `objective_alignment_score` is deliberately not a truth score. It is
    a utility-layer estimate of whether this run produced the kind of
    outcome the reader/writer flywheel should reinforce.
    """

    objective_alignment_score: float
    primary_bottleneck: str
    failure_modes: tuple[str, ...]
    quality_axes: dict[str, float]
    evidence_counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class InquiryOutcomeSummary:
    """Summary returned by `OutcomeEvaluator.evaluate()`.

    `events_emitted` counts only the events appended on THIS call
    (idempotent re-runs report 0 even though aggregate event count
    grows on the first call). `events_by_type` is the full per-type
    count after this call so callers don't have to re-aggregate.
    """

    inquiry_session_id: UUID
    think_run_id: UUID | None
    events_emitted: int
    events_by_type: dict[str, int]
    useful_node_ids: tuple[UUID, ...]
    noisy_path_signatures: tuple[dict, ...]
    missing_anchor_signatures: tuple[dict, ...]
    reward_features: dict[str, float]
    quality_signal: OutcomeQualitySignal


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------


_MISSING_EVIDENCE_TOKENS = (
    "missing evidence",
    "no evidence",
    "insufficient evidence",
    "evidence missing",
    "evidence not found",
    "evidence_required",
)

_BAD_REFERENCE_TOKENS = (
    "unknown model",
    "bad model_id",
    "invalid model_id",
    "unknown edge",
    "missing model",
    "invalid reference",
    "bad reference",
    "unknown reference",
    "model not found",
    "edge not found",
)

LOW_VALUE_READER_DECISION_LIMIT = 12

_CONTEXT_USED_GRADES = frozenset({
    "graph_context_used",
    "model_context_used",
    "observation_context_used",
    "justified_noop_context_used",
})


def _coerce_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


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


def _try_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _clamp(value: float, lo: float, hi: float) -> float:
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def _walk_strings(value: Any) -> list[str]:
    """Flatten nested dict/list into all leaf strings (for packet membership).

    Used for the "is this evidence source_ref present in the packet?"
    cross-check. The context_packet shape is not stable (it carries
    sufficiency verdicts, tier groups, omission ledgers, etc.) so we
    treat it as an opaque bag and scan for the source_ref token. This
    is a heuristic; the packet may later expose a typed `evidence_ids`
    field, at which point we can short-circuit.
    """
    out: list[str] = []
    stack: list[Any] = [value]
    while stack:
        cur = stack.pop()
        if cur is None:
            continue
        if isinstance(cur, str):
            out.append(cur)
        elif isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, (list, tuple)):
            stack.extend(cur)
    return out


def _signature_context_from_packet(
    packet: dict[str, Any],
    session: asyncpg.Record,
) -> dict[str, Any]:
    source_metadata = _coerce_obj(packet.get("source_metadata"))
    signal_type = (
        source_metadata.get("trigger_kind")
        or source_metadata.get("route")
        or session.get("route")
    )
    entities: list[str] = []
    for raw in _coerce_list(packet.get("resolved_entities")):
        if isinstance(raw, dict):
            value = raw.get("id") or raw.get("name") or raw.get("type")
        else:
            value = raw
        text = str(value).strip() if value is not None else ""
        if text and text not in entities:
            entities.append(text)
    primitives: list[str] = []
    for raw_q in _coerce_list(packet.get("question_path")):
        q = raw_q if isinstance(raw_q, dict) else {}
        primitive = q.get("primitive")
        if primitive is not None:
            p = str(primitive).strip().upper()
            if p and p not in primitives:
                primitives.append(p)
    primitive = primitives[0] if primitives else None
    signature = {
        k: v for k, v in {
            "signal_type": signal_type,
            "entities": entities[:12],
            "question_primitive": primitive,
        }.items() if v
    }
    return {
        "signal_type": signal_type,
        "entities": entities[:12],
        "question_primitive": primitive,
        "signature": signature,
    }


def _selected_context_was_used(context_use: dict[str, Any]) -> bool:
    """Return whether Think says selected reader context contributed."""
    if context_use.get("selected_context_used") is True:
        return True
    grade = str(context_use.get("context_use_grade") or "").strip()
    return grade in _CONTEXT_USED_GRADES


def _entity_texts(value: Any) -> list[str]:
    entities: list[str] = []
    for raw in _coerce_list(value):
        if isinstance(raw, dict):
            item = raw.get("id") or raw.get("name") or raw.get("type")
        else:
            item = raw
        text = str(item).strip() if item is not None else ""
        if text and text not in entities:
            entities.append(text)
    return entities


def _signature_from_reader_attribution(
    attribution: asyncpg.Record,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    signal_type = str(
        attribution["signal_type"] or fallback.get("signal_type") or ""
    ).strip()
    primitive = str(
        attribution["question_primitive"]
        or fallback.get("question_primitive")
        or ""
    ).strip().upper()
    entities = _entity_texts(attribution["entities"])
    if not entities:
        entities = _entity_texts(fallback.get("entities"))
    return {
        k: v for k, v in {
            "signal_type": signal_type or None,
            "entities": entities[:12],
            "question_primitive": primitive or None,
        }.items() if v
    }


def _collect_model_ids_from_ops(ops_applied: dict[str, Any]) -> list[UUID]:
    """Pull every Model id referenced anywhere in the applied diff.

    Covers:
      * claim_ops.model_id and claim_ops.entry.id (insert path)
      * edge_ops.source_model_id / target_model_id
      * act_ops.confidence_basis
      * act_ops.entity.model_id (when present)
      * resource_ops payloads that reference a model_id
    """
    out: list[UUID] = []
    seen: set[UUID] = set()

    def _add(value: Any) -> None:
        mid = _try_uuid(value)
        if mid is not None and mid not in seen:
            seen.add(mid)
            out.append(mid)

    for op in _coerce_list(ops_applied.get("claim_ops")):
        op = op if isinstance(op, dict) else {}
        _add(op.get("model_id"))
        entry = op.get("entry") if isinstance(op.get("entry"), dict) else {}
        _add(entry.get("id"))
    for op in _coerce_list(ops_applied.get("edge_ops")):
        op = op if isinstance(op, dict) else {}
        _add(op.get("source_model_id"))
        _add(op.get("target_model_id"))
        for ev in _coerce_list(op.get("evidence_model_ids")):
            _add(ev)
    for op in _coerce_list(ops_applied.get("act_ops")):
        op = op if isinstance(op, dict) else {}
        _add(op.get("confidence_basis"))
        ent = op.get("entity") if isinstance(op.get("entity"), dict) else {}
        _add(ent.get("model_id"))
    for op in _coerce_list(ops_applied.get("resource_ops")):
        op = op if isinstance(op, dict) else {}
        payload = op.get("payload") if isinstance(op.get("payload"), dict) else {}
        _add(payload.get("model_id"))
    return out


def _collect_act_op_counts(ops_applied: dict[str, Any]) -> tuple[int, int]:
    """Return (applied_act_ops_count, total_act_ops_proposed).

    v1 has no separate "proposed" telemetry surface; we conservatively
    treat dropped_op_count as the proposal overhead and applied as
    total minus dropped. Spec §17.1 expects the ratio as a proxy.
    """
    applied = len(_coerce_list(ops_applied.get("act_ops")))
    dropped = int(ops_applied.get("dropped_op_count") or 0)
    proposed = applied + dropped
    return applied, proposed


# ---------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------


class OutcomeEvaluator:
    """Phase 13 Outcome Evaluator — emits typed outcome events + reward features.

    Construction is tenant-bound (mirroring the trace repos in
    services/sage/inquiry_traces/repo.py). Pass `outcome_events_repo=`
    to share a repo with the caller's existing wiring; otherwise we
    build one over the same pool + tenant_id.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None,
        tenant_id: UUID,
        outcome_events_repo: OutcomeEventsRepo | None = None,
    ) -> None:
        self._pool = pool
        self.tenant_id = tenant_id
        self._events_repo = outcome_events_repo or OutcomeEventsRepo(
            pool, tenant_id=tenant_id,
        )

    # -----------------------------------------------------------------
    # Public entrypoint
    # -----------------------------------------------------------------

    async def evaluate(
        self,
        *,
        inquiry_session_id: UUID,
        conn: asyncpg.Connection | None = None,
    ) -> InquiryOutcomeSummary:
        if conn is not None:
            return await self._evaluate(inquiry_session_id, conn)
        if self._pool is None:
            raise OutcomeEvaluatorError(
                "evaluator constructed without a pool; pass conn= on every call",
            )
        async with self._pool.acquire() as owned:
            return await self._evaluate(inquiry_session_id, owned)

    # -----------------------------------------------------------------
    # Core implementation
    # -----------------------------------------------------------------

    async def _evaluate(
        self,
        inquiry_session_id: UUID,
        conn: asyncpg.Connection,
    ) -> InquiryOutcomeSummary:
        session = await self._load_session(conn, inquiry_session_id)
        if session is None:
            raise OutcomeEvaluatorError(
                f"inquiry_session {inquiry_session_id} not found for tenant",
            )

        packet = _coerce_obj(session["context_packet"])
        signature_context = _signature_context_from_packet(packet, session)
        packet_strings = set(_walk_strings(packet))
        packet_tokens_raw = (
            _coerce_obj(packet.get("budget")).get("estimated_tokens_used")
            or packet.get("estimated_tokens")
            or 0
        )
        try:
            packet_tokens = float(packet_tokens_raw)
        except (TypeError, ValueError):
            packet_tokens = 0.0

        evidence_items = await self._load_evidence_items(conn, inquiry_session_id)
        omitted_rows = await self._load_omitted_evidence(conn, inquiry_session_id)

        think_run_id = _try_uuid(session.get("think_run_id"))
        think_run = (
            await self._load_think_run(conn, think_run_id)
            if think_run_id is not None
            else None
        )
        ops_applied = _coerce_obj(think_run["ops_applied"]) if think_run else {}
        context_use = _coerce_obj(ops_applied.get("context_use"))
        selected_context_used = _selected_context_was_used(context_use)
        run_status = (think_run or {}).get("status") or ""
        run_error = ((think_run or {}).get("error") or "").lower()

        # Existing events for idempotency (cheap one-shot scan).
        existing_events = await self._load_existing_events(
            conn, inquiry_session_id,
        )
        existing_keys = _build_existing_keys(existing_events)

        events_emitted = 0
        used_evidence_ids: list[UUID] = []
        omitted_evidence_ids: list[UUID] = []
        used_node_ids: list[UUID] = []
        noisy_paths: list[dict[str, Any]] = []
        missing_anchors: list[dict[str, Any]] = []
        counterevidence_retrieved = 0
        counterevidence_in_packet = 0
        duplicate_evidence = 0
        retrieved_source_refs: dict[str, int] = {}

        # ------------------------------------------------------------------
        # retrieved_evidence_used_in_packet / retrieved_evidence_omitted
        # ------------------------------------------------------------------
        for item in evidence_items:
            source_ref = str(item["source_ref"])
            retrieved_source_refs[source_ref] = (
                retrieved_source_refs.get(source_ref, 0) + 1
            )
            contradicts = _coerce_list(item.get("contradicts_hypotheses"))
            weakens = _coerce_list(item.get("weakens_hypotheses"))
            is_counterevidence = bool(contradicts) or bool(weakens)
            if is_counterevidence:
                counterevidence_retrieved += 1
            evidence_uuid = item["id"]
            in_packet = self._evidence_in_packet(
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
                    await self._events_repo.append(
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
                    await self._events_repo.append(
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

        # Cross-check with omitted_evidence rows (additional omissions
        # the retrieval pathway already labelled). We dedupe by source_ref
        # so we don't double-emit for items also in inquiry_evidence_items.
        seen_omitted_refs = {str(item["source_ref"]) for item in evidence_items}
        for orow in omitted_rows:
            source_ref = str(orow["source_ref"])
            if source_ref in seen_omitted_refs:
                continue
            seen_omitted_refs.add(source_ref)
            key = ("retrieved_evidence_omitted", f"omitted_row:{orow['id']}")
            if key not in existing_keys:
                await self._events_repo.append(
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

        # TODO(Phase 14+): omitted_evidence_later_requested requires a
        # cross-session signal (the omitted source_ref re-surfacing in a
        # future plan). Out of scope for v1.

        for ref, count in retrieved_source_refs.items():
            if count > 1:
                duplicate_evidence += count - 1

        # ------------------------------------------------------------------
        # node_used_in_valid_diff / path_used_in_valid_diff
        # ------------------------------------------------------------------
        diff_is_valid = run_status == "success"
        diff_model_ids: list[UUID] = []
        if think_run is not None and diff_is_valid:
            diff_model_ids = _collect_model_ids_from_ops(ops_applied)
            attribution_rows = await self._load_reader_decision_attributions(
                conn, inquiry_session_id, diff_model_ids,
            )
            attributions_by_model: dict[UUID, list[asyncpg.Record]] = {}
            for row in attribution_rows:
                attributions_by_model.setdefault(row["model_id"], []).append(row)
            for mid in diff_model_ids:
                used_node_ids.append(mid)
                key = ("node_used_in_valid_diff", str(mid))
                if key not in existing_keys:
                    await self._events_repo.append(
                        inquiry_session_id,
                        "node_used_in_valid_diff",
                            {
                                "model_id": str(mid),
                                "think_run_id": str(think_run["id"]),
                                **signature_context,
                            },
                            conn=conn,
                        )
                    existing_keys.add(key)
                    events_emitted += 1
                for attribution in attributions_by_model.get(mid, []):
                    attr_id = attribution["id"]
                    credit_score = _reader_decision_credit_score(attribution)
                    attribution_signature = _signature_from_reader_attribution(
                        attribution,
                        signature_context,
                    )
                    decision_key = (
                        "reader_decision_used_in_valid_diff",
                        str(attr_id),
                    )
                    if decision_key not in existing_keys:
                        payload = {
                            **signature_context,
                            "signature": attribution_signature,
                            "attribution_id": str(attr_id),
                            "model_id": str(mid),
                            "think_run_id": str(think_run["id"]),
                            "question_id": attribution["question_id"],
                            "question_primitive": attribution_signature.get(
                                "question_primitive"
                            ) or attribution["question_primitive"],
                            "signal_type": attribution_signature.get("signal_type")
                            or attribution["signal_type"],
                            "entities": attribution_signature.get("entities")
                            or _coerce_list(attribution["entities"]),
                            "selected": bool(attribution["selected"]),
                            "selection_rank": attribution["selection_rank"],
                            "activation_score": float(
                                attribution["activation_score"] or 0.0
                            ),
                            "activation_reasons": _coerce_list(
                                attribution["activation_reasons"]
                            ),
                            "source_breakdown": _coerce_obj(
                                attribution["source_breakdown"]
                            ),
                            "retrieval_actions": _coerce_list(
                                attribution["retrieval_actions"]
                            ),
                            "projected_evidence_refs": _coerce_list(
                                attribution["projected_evidence_refs"]
                            ),
                            "evidence_in_packet_count": int(
                                attribution["evidence_in_packet_count"] or 0
                            ),
                            "expected_value": float(
                                attribution["expected_value"] or 0.0
                            ),
                            "expected_cost": float(
                                attribution["expected_cost"] or 0.0
                            ),
                            "credit_score": credit_score,
                        }
                        await self._events_repo.append(
                            inquiry_session_id,
                            "reader_decision_used_in_valid_diff",
                            payload,
                            conn=conn,
                        )
                        await self._credit_reader_decision_attribution(
                            conn,
                            attribution_id=attr_id,
                            credit_score=credit_score,
                        )
                        existing_keys.add(decision_key)
                        events_emitted += 1

            if len(diff_model_ids) >= 2:
                edges = await self._load_edges_between(conn, diff_model_ids)
                for edge in edges:
                    sig = {
                        "source_model_id": str(edge["source_model_id"]),
                        "target_model_id": str(edge["target_model_id"]),
                        "edge_kind": edge["edge_kind"],
                    }
                    key = (
                        "path_used_in_valid_diff",
                        f"{sig['source_model_id']}|{sig['target_model_id']}|"
                        f"{sig['edge_kind']}",
                    )
                    if key not in existing_keys:
                        await self._events_repo.append(
                            inquiry_session_id,
                            "path_used_in_valid_diff",
                            {
                                **sig,
                                "think_run_id": str(think_run["id"]),
                                **signature_context,
                            },
                            conn=conn,
                        )
                        existing_keys.add(key)
                        events_emitted += 1

        # A successful no-op is valuable only when Think can show that
        # selected context was actually used. If the reader selected and
        # packetized evidence but the writer neither changed nor referenced
        # any Model, emit negative credit at the same attribution grain as
        # positive reader-decision credit. This is what lets future reads
        # learn to abstain from irrelevant no-answer routes.
        low_value_selected_reader = (
            think_run is not None
            and diff_is_valid
            and not diff_model_ids
            and len(evidence_items) >= 3
            and not selected_context_used
        )
        if low_value_selected_reader:
            attribution_rows = await self._load_reader_decision_attributions(
                conn,
                inquiry_session_id,
                None,
                selected_only=True,
            )
            for attribution in attribution_rows[:LOW_VALUE_READER_DECISION_LIMIT]:
                attr_id = attribution["id"]
                decision_key = ("reader_decision_low_value", str(attr_id))
                if decision_key in existing_keys:
                    continue
                attribution_signature = _signature_from_reader_attribution(
                    attribution,
                    signature_context,
                )
                payload = {
                    **signature_context,
                    "signature": attribution_signature,
                    "attribution_id": str(attr_id),
                    "model_id": str(attribution["model_id"]),
                    "think_run_id": str(think_run["id"]),
                    "question_id": attribution["question_id"],
                    "question_primitive": attribution_signature.get(
                        "question_primitive"
                    ) or attribution["question_primitive"],
                    "signal_type": attribution_signature.get("signal_type")
                    or attribution["signal_type"],
                    "entities": attribution_signature.get("entities")
                    or _coerce_list(attribution["entities"]),
                    "selected": bool(attribution["selected"]),
                    "selection_rank": attribution["selection_rank"],
                    "activation_score": float(
                        attribution["activation_score"] or 0.0
                    ),
                    "activation_reasons": _coerce_list(
                        attribution["activation_reasons"]
                    ),
                    "source_breakdown": _coerce_obj(
                        attribution["source_breakdown"]
                    ),
                    "retrieval_actions": _coerce_list(
                        attribution["retrieval_actions"]
                    ),
                    "projected_evidence_refs": _coerce_list(
                        attribution["projected_evidence_refs"]
                    ),
                    "evidence_in_packet_count": int(
                        attribution["evidence_in_packet_count"] or 0
                    ),
                    "reason": "selected_reader_context_unused_by_valid_noop",
                    "retrieved_evidence_count": len(evidence_items),
                    "used_evidence_count": len(used_evidence_ids),
                    "context_use_grade": context_use.get("context_use_grade"),
                }
                await self._events_repo.append(
                    inquiry_session_id,
                    "reader_decision_low_value",
                    payload,
                    conn=conn,
                )
                existing_keys.add(decision_key)
                events_emitted += 1

        # ------------------------------------------------------------------
        # validation_failed_due_to_missing_evidence
        # validation_failed_due_to_bad_reference
        # ------------------------------------------------------------------
        validation_failure = think_run is not None and run_status in (
            "failed", "partial",
        )
        # think_runs in this repo never sets status='partial' (only
        # running/success/failed/skipped_idempotent), but we honour
        # 'partial' so a future migration that extends the enum doesn't
        # require evaluator code changes.
        if validation_failure and run_error:
            if any(tok in run_error for tok in _MISSING_EVIDENCE_TOKENS):
                anchor = {
                    "think_run_id": str(think_run["id"]),
                    "error_excerpt": run_error[:240],
                }
                missing_anchors.append(anchor)
                key = (
                    "validation_failed_due_to_missing_evidence",
                    str(think_run["id"]),
                )
                if key not in existing_keys:
                    await self._events_repo.append(
                        inquiry_session_id,
                        "validation_failed_due_to_missing_evidence",
                        anchor,
                        conn=conn,
                    )
                    existing_keys.add(key)
                    events_emitted += 1
            if any(tok in run_error for tok in _BAD_REFERENCE_TOKENS):
                payload = {
                    "think_run_id": str(think_run["id"]),
                    "error_excerpt": run_error[:240],
                }
                key = (
                    "validation_failed_due_to_bad_reference",
                    str(think_run["id"]),
                )
                if key not in existing_keys:
                    await self._events_repo.append(
                        inquiry_session_id,
                        "validation_failed_due_to_bad_reference",
                        payload,
                        conn=conn,
                    )
                    existing_keys.add(key)
                    events_emitted += 1

        # TODO(Phase 14+): user_accepted_node / user_contested_node /
        # model_later_confirmed / model_later_falsified /
        # recommendation_acted_on / recommendation_ignored require a
        # user-feedback table that does not exist in v1. Topology
        # Optimizer reads will treat absence as zero signal.

        # ------------------------------------------------------------------
        # Reward features (doc §17.1)
        # ------------------------------------------------------------------
        retrieved_count = len(evidence_items)
        used_count = len(used_evidence_ids)
        packet_node_count = self._count_packet_nodes(packet)
        applied_acts, proposed_acts = _collect_act_op_counts(ops_applied)
        added_nodes = sum(
            1
            for op in _coerce_list(ops_applied.get("claim_ops"))
            if isinstance(op, dict) and op.get("op") == "insert"
        )
        merged_nodes = sum(
            1
            for op in _coerce_list(ops_applied.get("claim_ops"))
            if isinstance(op, dict)
            and op.get("op") == "archive"
            and (op.get("reason") or "").startswith("superseded")
        )
        used_path_count = len(used_node_ids)
        noisy_path_count = sum(
            1
            for orow in omitted_rows
            if orow.get("omission_reason") in ("generic_hub", "redundant")
        )
        if noisy_path_count:
            noisy_paths.append(
                {"count": noisy_path_count, "from": "omitted_evidence"},
            )

        if run_status == "success":
            diff_deducibility = 1.0
        elif run_status == "partial":
            diff_deducibility = 0.5
        else:
            diff_deducibility = 0.0

        reward_features: dict[str, float] = {
            "evidence_coverage": _clamp(
                used_count / max(retrieved_count, 1), 0.0, 1.0,
            ),
            "diff_deducibility": diff_deducibility,
            "compression_gain": _clamp(
                used_count / max(packet_node_count, 1), 0.0, 2.0,
            ),
            "prediction_falsification_value": 0.0,  # TODO Phase 14+
            "action_value": _clamp(
                applied_acts / max(proposed_acts, 1), 0.0, 1.0,
            ),
            "counterevidence_preservation": _clamp(
                counterevidence_in_packet / max(counterevidence_retrieved, 1),
                0.0,
                1.0,
            ),
            "graph_bloat": float(added_nodes - merged_nodes),
            "redundancy": _clamp(
                duplicate_evidence / max(retrieved_count, 1), 0.0, 1.0,
            ),
            "noise_introduced": _clamp(
                noisy_path_count / max(used_path_count, 1), 0.0, 2.0,
            ),
            "token_cost": _clamp(packet_tokens / 30000.0, 0.0, 2.0),
            "permission_risk": 0.0,  # TODO Phase 14+
        }

        quality_signal = _build_quality_signal(
            session_stop_status=str(session.get("stop_status") or ""),
            think_run_present=think_run is not None,
            run_status=run_status,
            run_error=run_error,
            reward_features=reward_features,
            retrieved_count=retrieved_count,
            used_count=used_count,
            omitted_count=len(omitted_evidence_ids),
            packet_node_count=packet_node_count,
            counterevidence_retrieved=counterevidence_retrieved,
            counterevidence_in_packet=counterevidence_in_packet,
            duplicate_evidence=duplicate_evidence,
            noisy_path_count=noisy_path_count,
            used_path_count=used_path_count,
            selected_context_used=selected_context_used,
            applied_acts=applied_acts,
            proposed_acts=proposed_acts,
        )
        quality_key = ("outcome_quality_assessed", "quality")
        if quality_key not in existing_keys:
            await self._events_repo.append(
                inquiry_session_id,
                "outcome_quality_assessed",
                _quality_signal_payload(quality_signal),
                conn=conn,
            )
            existing_keys.add(quality_key)
            events_emitted += 1

        # Final per-type aggregation (post-emit) so callers don't need to
        # re-query. Cheap because we already have the existing_events.
        events_by_type = await self._events_repo.aggregate_by_type(
            inquiry_session_id, conn=conn,
        )

        _log.debug(
            "outcome_evaluator.summary",
            inquiry_session_id=str(inquiry_session_id),
            think_run_id=str(think_run_id) if think_run_id else None,
            events_emitted=events_emitted,
            event_types=sorted(events_by_type.keys()),
        )

        return InquiryOutcomeSummary(
            inquiry_session_id=inquiry_session_id,
            think_run_id=think_run_id,
            events_emitted=events_emitted,
            events_by_type=events_by_type,
            useful_node_ids=tuple(used_node_ids),
            noisy_path_signatures=tuple(noisy_paths),
            missing_anchor_signatures=tuple(missing_anchors),
            reward_features=reward_features,
            quality_signal=quality_signal,
        )

    # -----------------------------------------------------------------
    # SQL loaders (read-only)
    # -----------------------------------------------------------------

    async def _load_session(
        self, conn: asyncpg.Connection, session_id: UUID,
    ) -> asyncpg.Record | None:
        return await conn.fetchrow(
            """
            SELECT id, tenant_id, status, stop_status, context_packet,
                   think_run_id, route_decision_id
            FROM inquiry_sessions
            WHERE tenant_id = $1 AND id = $2
            """,
            self.tenant_id,
            session_id,
        )

    async def _load_evidence_items(
        self, conn: asyncpg.Connection, session_id: UUID,
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT id, source_type, source_ref, source_ref_id,
                   contradicts_hypotheses, weakens_hypotheses,
                   supports_hypotheses, token_estimate
            FROM inquiry_evidence_items
            WHERE tenant_id = $1 AND session_id = $2
            ORDER BY created_at ASC, id ASC
            """,
            self.tenant_id,
            session_id,
        )

    async def _load_reader_decision_attributions(
        self,
        conn: asyncpg.Connection,
        session_id: UUID,
        model_ids: list[UUID] | None,
        *,
        selected_only: bool = False,
    ) -> list[asyncpg.Record]:
        if model_ids is not None and not model_ids:
            return []
        table_name = await conn.fetchval(
            "SELECT to_regclass('public.sage_reader_decision_attributions')"
        )
        if table_name is None:
            return []
        filters = [
            "tenant_id = $1",
            "inquiry_session_id = $2",
        ]
        params: list[Any] = [self.tenant_id, session_id]
        if model_ids is not None:
            params.append(model_ids)
            filters.append(f"model_id = ANY(${len(params)}::uuid[])")
        if selected_only:
            filters.append("selected = TRUE")
        return list(await conn.fetch(
            f"""
            SELECT id, tenant_id, inquiry_session_id,
                   question_id, question_primitive, question,
                   question_score, expected_value, expected_cost,
                   signal_type, entities, model_id,
                   selected, selection_rank, activation_score,
                   activation_reasons, source_breakdown, retrieval_actions,
                   projected_evidence_refs, evidence_in_packet_count
            FROM sage_reader_decision_attributions
            WHERE {' AND '.join(filters)}
            ORDER BY selected DESC, activation_score DESC, created_at ASC
            """,
            *params,
        ))

    async def _credit_reader_decision_attribution(
        self,
        conn: asyncpg.Connection,
        *,
        attribution_id: UUID,
        credit_score: float,
    ) -> None:
        await conn.execute(
            """
            UPDATE sage_reader_decision_attributions
               SET writer_used = TRUE,
                   writer_credit_score = GREATEST(writer_credit_score, $3),
                   credited_at = COALESCE(credited_at, now()),
                   updated_at = now()
             WHERE tenant_id = $1
               AND id = $2
            """,
            self.tenant_id,
            attribution_id,
            float(credit_score),
        )

    async def _load_omitted_evidence(
        self, conn: asyncpg.Connection, session_id: UUID,
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT id, source_type, source_ref, source_ref_id,
                   omission_reason, retrieval_paths
            FROM omitted_evidence
            WHERE tenant_id = $1 AND inquiry_session_id = $2
            ORDER BY created_at ASC, id ASC
            """,
            self.tenant_id,
            session_id,
        )

    async def _load_think_run(
        self, conn: asyncpg.Connection, run_id: UUID,
    ) -> asyncpg.Record | None:
        return await conn.fetchrow(
            """
            SELECT id, tenant_id, status, error, ops_applied,
                   trigger_id, trigger_kind, validation_error_count
            FROM think_runs
            WHERE tenant_id = $1 AND id = $2
            """,
            self.tenant_id,
            run_id,
        )

    async def _load_edges_between(
        self, conn: asyncpg.Connection, model_ids: list[UUID],
    ) -> list[asyncpg.Record]:
        if len(model_ids) < 2:
            return []
        return await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = ANY($2::uuid[])
              AND target_model_id = ANY($2::uuid[])
              AND status = 'active'
            """,
            self.tenant_id,
            model_ids,
        )

    async def _load_existing_events(
        self, conn: asyncpg.Connection, session_id: UUID,
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT event_type, payload
            FROM inquiry_outcome_events
            WHERE tenant_id = $1 AND inquiry_session_id = $2
            """,
            self.tenant_id,
            session_id,
        )

    # -----------------------------------------------------------------
    # Helpers (instance — packet shape is up to us to interpret)
    # -----------------------------------------------------------------

    def _evidence_in_packet(
        self,
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

    def _count_packet_nodes(self, packet: dict[str, Any]) -> int:
        """Estimate how many distinct Models/nodes the packet references.

        v1 heuristic: count UUID-shaped strings inside the packet body
        and treat that as the upper bound on referenced nodes. This is a
        proxy because the packet may inline more than just Models — the
        reward signal cares about relative compression, not absolute
        count, so noise washes out across many sessions.
        """
        n = 0
        for s in _walk_strings(packet):
            if _try_uuid(s) is not None:
                n += 1
        return n


# ---------------------------------------------------------------------
# Module helpers (free functions)
# ---------------------------------------------------------------------


def _build_existing_keys(events: list[asyncpg.Record]) -> set[tuple[str, str]]:
    """Build the idempotency key set from previously-emitted events.

    The key shape mirrors what `_evaluate` uses when deciding whether
    to append. Keep these two in lockstep.
    """
    keys: set[tuple[str, str]] = set()
    for row in events:
        etype = row["event_type"]
        payload = _coerce_obj(row["payload"])
        if etype in (
            "retrieved_evidence_used_in_packet",
            "retrieved_evidence_omitted",
        ):
            eid = payload.get("evidence_id")
            if eid:
                keys.add((etype, str(eid)))
            oeid = payload.get("omitted_evidence_id")
            if oeid:
                keys.add((etype, f"omitted_row:{oeid}"))
        elif etype == "node_used_in_valid_diff":
            mid = payload.get("model_id")
            if mid:
                keys.add((etype, str(mid)))
        elif etype == "path_used_in_valid_diff":
            src = payload.get("source_model_id")
            tgt = payload.get("target_model_id")
            kind = payload.get("edge_kind")
            if src and tgt and kind:
                keys.add((etype, f"{src}|{tgt}|{kind}"))
        elif etype == "reader_decision_used_in_valid_diff":
            attr_id = payload.get("attribution_id")
            if attr_id:
                keys.add((etype, str(attr_id)))
        elif etype == "reader_decision_low_value":
            attr_id = payload.get("attribution_id")
            if attr_id:
                keys.add((etype, str(attr_id)))
        elif etype == "outcome_quality_assessed":
            keys.add((etype, "quality"))
        elif etype in (
            "validation_failed_due_to_missing_evidence",
            "validation_failed_due_to_bad_reference",
        ):
            run_id = payload.get("think_run_id")
            if run_id:
                keys.add((etype, str(run_id)))
    return keys


def _reader_decision_credit_score(row: asyncpg.Record) -> float:
    activation = float(row["activation_score"] or 0.0)
    expected_value = float(row["expected_value"] or 0.0)
    expected_cost = float(row["expected_cost"] or 0.0)
    selected_bonus = 0.35 if bool(row["selected"]) else 0.0
    packet_bonus = min(0.25, 0.06 * int(row["evidence_in_packet_count"] or 0))
    credit = (
        0.35
        + 0.35 * activation
        + 0.30 * expected_value
        + selected_bonus
        + packet_bonus
        - 0.20 * expected_cost
    )
    return _clamp(credit, 0.05, 2.0)


def _answerability_score(stop_status: str) -> float:
    status = stop_status.strip().lower()
    if status == "sufficient_for_reasoning":
        return 1.0
    if status in {"completed", "success"}:
        return 0.8
    if status in {"budget_exhausted", "max_questions_reached"}:
        return 0.45
    if status in {"insufficient_evidence", "no_evidence", "failed"}:
        return 0.2
    return 0.6 if status else 0.5


def _has_token(text: str, tokens: tuple[str, ...]) -> bool:
    return any(tok in text for tok in tokens)


def _build_quality_signal(
    *,
    session_stop_status: str,
    think_run_present: bool,
    run_status: str,
    run_error: str,
    reward_features: dict[str, float],
    retrieved_count: int,
    used_count: int,
    omitted_count: int,
    packet_node_count: int,
    counterevidence_retrieved: int,
    counterevidence_in_packet: int,
    duplicate_evidence: int,
    noisy_path_count: int,
    used_path_count: int,
    selected_context_used: bool,
    applied_acts: int,
    proposed_acts: int,
) -> OutcomeQualitySignal:
    """Classify the dominant quality bottleneck for one SAGE run.

    This deliberately composes multiple axes instead of turning every
    success into positive credit. A valid diff with no counterevidence
    preservation or a bloated low-density packet should not teach the
    reader/writer loop that the retrieval policy was globally good.
    """
    evidence_coverage = reward_features["evidence_coverage"]
    diff_deducibility = reward_features["diff_deducibility"]
    redundancy = reward_features["redundancy"]
    token_cost = reward_features["token_cost"]
    action_value = reward_features["action_value"]
    graph_bloat = max(0.0, reward_features["graph_bloat"])
    noise = reward_features["noise_introduced"]

    if counterevidence_retrieved > 0:
        counter_retention = _clamp(
            counterevidence_in_packet / counterevidence_retrieved,
            0.0,
            1.0,
        )
    else:
        # Neutral: absence of retrieved counterevidence is not, by
        # itself, proof the reader failed to seek falsifiers.
        counter_retention = 1.0

    low_value_leak = _clamp(
        (0.42 * redundancy)
        + (0.22 * _clamp(noise / 2.0, 0.0, 1.0))
        + (0.22 * _clamp(token_cost / 2.0, 0.0, 1.0))
        + (0.14 * _clamp(graph_bloat / 10.0, 0.0, 1.0)),
        0.0,
        1.0,
    )
    packet_value_density = 1.0 - low_value_leak
    reference_integrity = (
        0.0 if _has_token(run_error, _BAD_REFERENCE_TOKENS) else 1.0
    )
    answerability = _answerability_score(session_stop_status)

    axes = {
        "diff_deducibility": _clamp(diff_deducibility, 0.0, 1.0),
        "evidence_coverage": _clamp(evidence_coverage, 0.0, 1.0),
        "counterevidence_retention": counter_retention,
        "reference_integrity": reference_integrity,
        "packet_value_density": packet_value_density,
        "answerability": answerability,
        "selected_context_used": 1.0 if selected_context_used else 0.0,
        "low_value_leak": low_value_leak,
    }

    failure_modes: list[str] = []
    if not think_run_present:
        failure_modes.append("no_writer_outcome")
    if run_status and run_status != "success":
        failure_modes.append("no_valid_diff")
    if _has_token(run_error, _MISSING_EVIDENCE_TOKENS):
        failure_modes.append("missing_evidence")
    if reference_integrity < 1.0:
        failure_modes.append("bad_reference")
    if counterevidence_retrieved > 0 and counter_retention < 0.5:
        failure_modes.append("counterevidence_dropped")
    if retrieved_count >= 3 and evidence_coverage < 0.35:
        failure_modes.append("low_evidence_coverage")
    if (
        run_status == "success"
        and retrieved_count >= 3
        and used_path_count == 0
        and not selected_context_used
    ):
        failure_modes.append("unused_selected_context")
    if redundancy >= 0.20:
        failure_modes.append("duplicate_low_value_evidence")
    if token_cost >= 0.70 and evidence_coverage < 0.50:
        failure_modes.append("packet_bloat")
    if used_path_count > 0 and noisy_path_count > used_path_count:
        failure_modes.append("noisy_path_overselection")
    if proposed_acts > 0 and applied_acts == 0 and action_value < 0.5:
        failure_modes.append("writer_action_drop")

    score = (
        (0.32 * axes["diff_deducibility"])
        + (0.18 * axes["evidence_coverage"])
        + (0.16 * axes["counterevidence_retention"])
        + (0.13 * axes["reference_integrity"])
        + (0.11 * axes["packet_value_density"])
        + (0.10 * axes["answerability"])
    )
    if "missing_evidence" in failure_modes:
        score *= 0.65
    if "bad_reference" in failure_modes:
        score *= 0.55
    if "no_writer_outcome" in failure_modes:
        score *= 0.45
    if "unused_selected_context" in failure_modes:
        score *= 0.72
    score = _clamp(score, 0.0, 1.0)

    priority = (
        "bad_reference",
        "missing_evidence",
        "counterevidence_dropped",
        "no_valid_diff",
        "low_evidence_coverage",
        "unused_selected_context",
        "packet_bloat",
        "duplicate_low_value_evidence",
        "noisy_path_overselection",
        "writer_action_drop",
        "no_writer_outcome",
    )
    primary = "none"
    for mode in priority:
        if mode in failure_modes:
            primary = mode
            break

    return OutcomeQualitySignal(
        objective_alignment_score=score,
        primary_bottleneck=primary,
        failure_modes=tuple(failure_modes),
        quality_axes=axes,
        evidence_counts={
            "retrieved": retrieved_count,
            "used": used_count,
            "omitted": omitted_count,
            "packet_nodes": packet_node_count,
            "counterevidence_retrieved": counterevidence_retrieved,
            "counterevidence_in_packet": counterevidence_in_packet,
            "duplicate_evidence": duplicate_evidence,
            "noisy_paths": noisy_path_count,
        },
    )


def _quality_signal_payload(signal: OutcomeQualitySignal) -> dict[str, Any]:
    return {
        "objective_alignment_score": signal.objective_alignment_score,
        "primary_bottleneck": signal.primary_bottleneck,
        "failure_modes": list(signal.failure_modes),
        "quality_axes": signal.quality_axes,
        "evidence_counts": signal.evidence_counts,
    }


__all__ = [
    "InquiryOutcomeSummary",
    "OutcomeQualitySignal",
    "OutcomeEvaluator",
    "OutcomeEvaluatorError",
]
