"""Retrieval action cache and scope-binding helpers."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any
from uuid import UUID

from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext

from .config import InquiryConfig
from .routing import trigger_text
from .types import RetrievalAction


def seed_action_cache_from_baseline(
    action_cache: dict[tuple[Any, ...], PathwayResult],
    baseline: RetrievalResult,
    trigger: TriggerContext,
    cfg: InquiryConfig,
) -> dict[str, Any]:
    """Reuse baseline graph reads for question actions when the scope matches."""
    notes: dict[str, Any] = {
        "seeded": 0,
        "paths": [],
        "skipped": [],
    }
    by_source = {result.source_pathway: result for result in baseline.pathway_results}

    if int(getattr(trigger, "max_hops", 0) or 0) == int(cfg.structural_max_hops):
        source = by_source.get("A")
        if source is not None:
            action = RetrievalAction("Q0", "structural", "baseline_structural")
            key = retrieval_action_cache_key(action, trigger, cfg)
            action_cache[key] = clone_pathway_result(
                source,
                model_limit=min(action.budget, cfg.action_model_budget_limit),
                cap_models_by_activation=True,
                note="baseline_A",
            )
            notes["seeded"] += 1
            notes["paths"].append("structural:A")
    else:
        notes["skipped"].append("structural_hop_mismatch")

    g_hops = min(max(int(getattr(trigger, "max_hops", 0) or 0), 0), 3)
    if g_hops == int(cfg.model_edge_max_hops):
        source = by_source.get("G")
        if source is not None:
            action = RetrievalAction(
                "Q0",
                "model_edge",
                "baseline_model_edges",
                budget=cfg.action_model_budget_limit,
            )
            key = retrieval_action_cache_key(action, trigger, cfg)
            action_cache[key] = clone_pathway_result(
                source,
                model_limit=cfg.action_model_budget_limit,
                note="baseline_G",
            )
            notes["seeded"] += 1
            notes["paths"].append("model_edge:G")
    else:
        notes["skipped"].append("model_edge_hop_mismatch")

    return notes


def clone_pathway_result(
    result: PathwayResult,
    *,
    model_limit: int | None = None,
    observation_limit: int | None = None,
    cap_models_by_activation: bool = False,
    note: str,
) -> PathwayResult:
    models = list(result.models)
    if model_limit is not None:
        limit = max(0, int(model_limit))
        if cap_models_by_activation:
            models = sorted(
                models,
                key=lambda model: (
                    -float(getattr(model, "activation", 0.0) or 0.0),
                    str(getattr(model, "id", "")),
                ),
            )
        models = models[:limit]
    observations = list(result.observations)
    if observation_limit is not None:
        observations = observations[: max(0, int(observation_limit))]
    notes = dict(result.notes or {})
    notes["cache_seeded_from"] = note
    notes["models_after_cache_seed_cap"] = len(models)
    if observation_limit is not None:
        notes["observations_after_cache_seed_cap"] = len(observations)
    return PathwayResult(
        models=models,
        observations=observations,
        acts={key: list(value) for key, value in (result.acts or {}).items()},
        resources=list(result.resources),
        source_pathway=result.source_pathway,
        notes=notes,
    )


def retrieval_action_cache_key(
    action: RetrievalAction,
    trigger: TriggerContext,
    cfg: InquiryConfig,
) -> tuple[Any, ...]:
    model_budget = min(
        max(1, int(action.budget)), max(1, int(cfg.action_model_budget_limit))
    )
    observation_budget = min(
        max(1, int(action.budget)),
        max(1, int(cfg.action_observation_budget_limit)),
    )
    scope_actors = tuple(sorted(str(actor) for actor in (trigger.scope_actors or [])))
    scope_entities = stable_cache_value(action_seed_entities(action, trigger))
    seed_model_ids = tuple(sorted(str(mid) for mid in action_seed_model_ids(action)))
    if action.path == "structural":
        return (
            "structural",
            cfg.structural_max_hops,
            model_budget,
            scope_actors,
            scope_entities,
        )
    if action.path == "focused_index":
        return (
            "focused_index",
            model_budget,
            scope_actors,
            scope_entities,
            str(action.filters.get("primitive") or ""),
            stable_cache_value(action.filters.get("terms") or []),
        )
    if action.path == "temporal":
        return (
            "temporal",
            str(trigger.seed_occurred_at),
            int(action.filters.get("window_days") or cfg.temporal_window_days),
            model_budget,
            observation_budget,
            scope_actors,
            scope_entities,
        )
    if action.path == "model_edge":
        return (
            "model_edge",
            cfg.model_edge_max_hops,
            model_budget,
            str(trigger.model_id or ""),
            seed_model_ids,
            scope_actors,
            scope_entities,
        )
    if action.path == "pattern":
        return (
            "pattern",
            model_budget,
            stable_cache_value(trigger.seed_signature or {}),
        )
    return (
        action.path,
        action.target,
        action.query or trigger_text(trigger),
        model_budget,
        observation_budget,
        scope_actors,
        scope_entities,
    )


def action_seed_entities(
    action: RetrievalAction,
    trigger: TriggerContext,
) -> list[dict[str, Any]]:
    raw = action.filters.get("seed_entities")
    if isinstance(raw, list):
        out = [item for item in raw if isinstance(item, dict)]
        if out:
            return out
    return list(trigger.seed_entity_ids or [])


def action_seed_model_ids(action: RetrievalAction) -> list[UUID]:
    out: list[UUID] = []
    raw = action.filters.get("seed_model_ids")
    if not isinstance(raw, list):
        return out
    for value in raw:
        try:
            out.append(UUID(str(value)))
        except (TypeError, ValueError):
            continue
    return out


def stable_cache_value(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def bind_action_to_previous_results(
    action: RetrievalAction,
    trigger: TriggerContext,
    prior_results: list[PathwayResult],
) -> RetrievalAction:
    if not action.filters.get("_bind_previous_scope") or not prior_results:
        return action
    seed_entities = dedupe_seed_entities(
        [
            *action_seed_entities(action, trigger),
            *seed_entities_from_pathway_results(prior_results),
        ]
    )[:24]
    seed_model_ids = [
        str(model_id)
        for model_id in seed_model_ids_from_pathway_results(prior_results)[:24]
    ]
    filters = dict(action.filters)
    if seed_entities:
        filters["seed_entities"] = seed_entities
    if seed_model_ids:
        filters["seed_model_ids"] = seed_model_ids
    filters["_bound_scope"] = {
        "model_count": len(seed_model_ids),
        "entity_count": len(seed_entities),
    }
    return replace(action, filters=filters)


def seed_model_ids_from_pathway_results(
    results: list[PathwayResult],
) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for result in results:
        for model in result.models:
            mid = model.id
            if mid not in seen:
                seen.add(mid)
                out.append(mid)
    return out


def seed_entities_from_pathway_results(
    results: list[PathwayResult],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for result in results:
        for model in result.models[:24]:
            raw_entities = getattr(model, "scope_entities", None) or []
            if isinstance(raw_entities, list):
                out.extend(item for item in raw_entities if isinstance(item, dict))
        for resource in result.resources[:12]:
            rid = getattr(resource, "id", None)
            if rid is not None:
                out.append({"type": "resource", "id": str(rid)})
        for key, entity_type in (
            ("commitments", "commitment"),
            ("goals", "goal"),
            ("decisions", "decision"),
        ):
            for act in (result.acts or {}).get(key, [])[:12]:
                aid = getattr(act, "id", None)
                if aid is not None:
                    out.append({"type": entity_type, "id": str(aid)})
    return dedupe_seed_entities(out)


def dedupe_seed_entities(
    entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        etype = str(entity.get("type") or "").strip()
        eid = str(entity.get("id") or "").strip()
        if not etype or not eid:
            continue
        key = (etype.casefold(), eid)
        if key in seen:
            continue
        seen.add(key)
        out.append({"type": etype, "id": eid})
    return out


__all__ = [
    "action_seed_entities",
    "action_seed_model_ids",
    "bind_action_to_previous_results",
    "clone_pathway_result",
    "dedupe_seed_entities",
    "retrieval_action_cache_key",
    "seed_action_cache_from_baseline",
    "seed_entities_from_pathway_results",
    "seed_model_ids_from_pathway_results",
    "stable_cache_value",
]
