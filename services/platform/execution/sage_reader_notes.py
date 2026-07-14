"""SAGE reader diagnostics, gates, and retrieval timing summaries."""

from __future__ import annotations

from collections import Counter
from typing import Any, Literal

from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext

from .evidence_utils import compact as _compact
from .types import InquiryQuestion


def record_sage_reader_notes(
    notes: dict[str, Any],
    question: InquiryQuestion,
    result: RetrievalResult,
) -> None:
    read_note = (result.notes or {}).get("sage_reader")
    if not isinstance(read_note, dict):
        return
    qid = question.question_id
    notes.setdefault("questions", {})[qid] = read_note
    signature = read_note.get("signature")
    if isinstance(signature, dict) and signature not in notes["signatures"]:
        notes["signatures"].append(signature)
    for mid in read_note.get("selected_model_ids", []) or []:
        if mid not in notes["selected_model_ids"]:
            notes["selected_model_ids"].append(mid)
    notes["projected_evidence_count"] = int(
        notes.get("projected_evidence_count") or 0
    ) + int(read_note.get("projected_evidence_count") or 0)
    notes["activation_trace_count"] = int(
        notes.get("activation_trace_count") or 0
    ) + int(read_note.get("activation_trace_count") or 0)


def compact_inquiry_notes_for_persistence(
    notes: dict[str, Any],
    *,
    persist_full_sage_reader_notes: bool,
) -> dict[str, Any]:
    if persist_full_sage_reader_notes:
        return notes
    compact = dict(notes)
    if isinstance(compact.get("context_packet"), dict):
        compact["context_packet"] = {"stored_in_context_packet_column": True}
    sage_notes = compact.get("sage_reader")
    if isinstance(sage_notes, dict):
        compact["sage_reader"] = compact_sage_reader_notes_for_persistence(sage_notes)
    compact["persist_compaction"] = {
        "sage_reader_full_notes": False,
        "context_packet_stored_once": True,
    }
    return compact


def compact_sage_reader_notes_for_persistence(
    sage_notes: dict[str, Any],
) -> dict[str, Any]:
    compact = dict(sage_notes)
    questions = sage_notes.get("questions")
    if isinstance(questions, dict):
        compact["questions"] = {
            str(qid): compact_sage_question_note_for_persistence(qnote)
            for qid, qnote in questions.items()
            if isinstance(qnote, dict)
        }
    compact["persist_compacted"] = True
    compact["omitted_payloads"] = [
        "questions.*.activations",
        "questions.*.debug.gate_scores",
        "questions.*.debug.activation_reasons",
    ]
    return compact


def compact_sage_question_note_for_persistence(
    qnote: dict[str, Any],
) -> dict[str, Any]:
    activations = qnote.get("activations")
    activation_count = (
        len(activations)
        if isinstance(activations, list)
        else int(qnote.get("activation_trace_count") or 0)
    )
    selected_model_ids = [str(mid) for mid in (qnote.get("selected_model_ids") or [])]
    return {
        "question_id": qnote.get("question_id"),
        "question_primitive": qnote.get("question_primitive"),
        "signature": qnote.get("signature"),
        "selected_model_ids": selected_model_ids,
        "selected_model_count": len(selected_model_ids),
        "projected_evidence_count": int(qnote.get("projected_evidence_count") or 0),
        "activation_trace_count": activation_count,
        "debug": compact_sage_reader_debug_for_persistence(qnote.get("debug")),
        "activations_stored_in": "sage_reader_activations",
    }


def compact_sage_reader_debug_for_persistence(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    selector = raw.get("selector") if isinstance(raw.get("selector"), dict) else {}
    intents = raw.get("intents") if isinstance(raw.get("intents"), list) else []
    gate_scores = raw.get("gate_scores")
    activation_reasons = raw.get("activation_reasons")
    return {
        "stage_timings_ms": raw.get("stage_timings_ms") or {},
        "learned_read_plan": raw.get("learned_read_plan") or {},
        "projection_budget": raw.get("projection_budget") or {},
        "projection_coverage": raw.get("projection_coverage") or {},
        "candidate_pool": raw.get("candidate_pool") or {},
        "row_cache": raw.get("row_cache") or {},
        "cue_extraction": raw.get("cue_extraction") or {},
        "intents": [
            {
                "intent": item.get("intent"),
                "paths": item.get("paths"),
                "expected_cost": item.get("expected_cost"),
                "expected_value": item.get("expected_value"),
                "target": _compact(item.get("target"), 180),
            }
            for item in intents
            if isinstance(item, dict)
        ],
        "selector": {
            "selected_node_count": len(selector.get("selected_nodes") or []),
            "selected_edge_count": len(selector.get("selected_edges") or []),
            "bridge_node_count": len(selector.get("bridge_nodes") or []),
            "coverage_metrics": selector.get("coverage_metrics") or {},
        },
        "gate_score_count": (len(gate_scores) if isinstance(gate_scores, dict) else 0),
        "activation_reason_count": (
            len(activation_reasons)
            if isinstance(activation_reasons, (dict, list))
            else 0
        ),
    }


def sage_reader_total_ms(result: RetrievalResult) -> int | None:
    read_note = (result.notes or {}).get("sage_reader") or {}
    if not isinstance(read_note, dict):
        return None
    debug = read_note.get("debug") or {}
    if not isinstance(debug, dict):
        return None
    timings = debug.get("stage_timings_ms") or {}
    if not isinstance(timings, dict):
        return None
    try:
        return int(timings.get("reader_total_ms"))
    except (TypeError, ValueError):
        return None


def sage_reader_action_gate(
    result: RetrievalResult,
    *,
    gate_broad_actions: bool = True,
) -> tuple[Literal["all", "broad"] | None, str | None]:
    if not gate_broad_actions:
        return None, None
    plan = sage_reader_plan_from_result(result)
    if not plan:
        return None, None
    mode = str(plan.get("mode") or "")
    if sage_reader_plan_hard_abstained(plan):
        return "all", "sage_reader_negative_memory_abstain"
    if not bool(plan.get("gate_broad_actions")):
        return None, None
    if bool(plan.get("skip_broad_discovery")) and mode in {
        "focused",
        "guarded_negative_memory",
        "rerank",
    }:
        return "broad", f"sage_reader_{mode}_broad_gate"
    read_note = (result.notes or {}).get("sage_reader") or {}
    if (
        mode == "rerank"
        and len(result.models) >= 1
        and int(read_note.get("projected_evidence_count") or 0) >= 1
    ):
        return "broad", "sage_reader_rerank_sufficient_evidence"
    return None, None


def sage_reader_controller_summary(
    notes: dict[str, Any],
    *,
    trigger: TriggerContext,
) -> dict[str, Any]:
    raw_questions = notes.get("questions")
    questions = raw_questions if isinstance(raw_questions, dict) else {}
    question_summaries: dict[str, dict[str, Any]] = {}
    hard_abstain_count = 0
    skipped_broad_count = 0
    selected_model_count = 0
    for qid, read_note in questions.items():
        if not isinstance(read_note, dict):
            continue
        plan = sage_reader_plan_from_read_note(read_note)
        selected_ids = read_note.get("selected_model_ids") or []
        selected_count = len(selected_ids) if isinstance(selected_ids, list) else 0
        selected_model_count += selected_count
        hard_abstained = sage_reader_plan_hard_abstained(plan)
        if hard_abstained:
            hard_abstain_count += 1
        if bool(plan.get("skip_broad_discovery")):
            skipped_broad_count += 1
        question_summaries[str(qid)] = {
            "mode": plan.get("mode"),
            "confidence": plan.get("confidence"),
            "abstain_early": bool(plan.get("abstain_early")),
            "skip_broad_discovery": bool(plan.get("skip_broad_discovery")),
            "gate_broad_actions": bool(plan.get("gate_broad_actions")),
            "selected_model_count": selected_count,
            "hard_abstained": hard_abstained,
        }

    question_count = len(question_summaries)
    explicit_anchor = trigger_has_explicit_model_anchor(trigger)
    global_negative_route_gate = (
        question_count > 0
        and hard_abstain_count == question_count
        and selected_model_count == 0
        and not explicit_anchor
    )
    return {
        "used": question_count > 0,
        "question_count": question_count,
        "hard_abstain_count": hard_abstain_count,
        "skipped_broad_count": skipped_broad_count,
        "selected_model_count": selected_model_count,
        "explicit_model_anchor": explicit_anchor,
        "global_negative_route_gate": global_negative_route_gate,
        "questions": question_summaries,
    }


def sage_reader_plan_from_result(result: RetrievalResult) -> dict[str, Any]:
    read_note = (result.notes or {}).get("sage_reader") or {}
    if not isinstance(read_note, dict):
        return {}
    return sage_reader_plan_from_read_note(read_note)


def sage_reader_plan_from_read_note(read_note: dict[str, Any]) -> dict[str, Any]:
    debug = read_note.get("debug") or {}
    if not isinstance(debug, dict):
        return {}
    plan = debug.get("learned_read_plan") or {}
    return plan if isinstance(plan, dict) else {}


def sage_reader_plan_hard_abstained(plan: dict[str, Any]) -> bool:
    return str(plan.get("mode") or "") == "abstain" and bool(plan.get("abstain_early"))


def trigger_has_explicit_model_anchor(trigger: TriggerContext) -> bool:
    return trigger.model_id is not None or bool(trigger.member_model_ids)


def sage_only_retrieval_results(
    results: list[RetrievalResult],
) -> list[RetrievalResult]:
    return [
        result
        for result in results
        if "sage_reader" in set((result.notes or {}).get("pathways_run", []) or [])
        or any(pr.source_pathway == "SAGE" for pr in result.pathway_results)
    ]


def action_cache_summary(action_timings: list[dict[str, Any]]) -> dict[str, Any]:
    hits = sum(1 for note in action_timings if note.get("cache_hit"))
    in_flight_waits = sum(
        1
        for note in action_timings
        if note.get("in_flight_wait") or note.get("timing_kind") == "in_flight_wait"
    )
    misses = sum(
        1
        for note in action_timings
        if not note.get("cache_hit") and note.get("path") != "sage_reader"
    )
    elapsed_by_path: Counter[str] = Counter()
    work_elapsed_by_path: Counter[str] = Counter()
    wait_elapsed_by_path: Counter[str] = Counter()
    cached_by_path: Counter[str] = Counter()
    for note in action_timings:
        path = str(note.get("path") or "")
        if path:
            elapsed_ms = int(note.get("elapsed_ms") or 0)
            elapsed_by_path[path] += elapsed_ms
            if (
                note.get("in_flight_wait")
                or note.get("timing_kind") == "in_flight_wait"
            ):
                wait_elapsed_by_path[path] += elapsed_ms
            else:
                work_elapsed_by_path[path] += elapsed_ms
            if note.get("cache_hit"):
                cached_by_path[path] += 1
    return {
        "hits": hits,
        "in_flight_waits": in_flight_waits,
        "misses": misses,
        "elapsed_ms_by_path": dict(sorted(elapsed_by_path.items())),
        "work_elapsed_ms_by_path": dict(sorted(work_elapsed_by_path.items())),
        "wait_elapsed_ms_by_path": dict(sorted(wait_elapsed_by_path.items())),
        "cache_hits_by_path": dict(sorted(cached_by_path.items())),
    }
