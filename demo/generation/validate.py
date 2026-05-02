"""Validate a generated entity bundle for internal consistency.

Returns a list of error strings. Empty list = valid. The CLI in
generate.py exits non-zero when any validation error is reported.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from demo.generation.schemas import GeneratedBundle


# Tolerance: spec counts within ±10% per DEMO-BUILD-PLAN Step 3.
COUNT_TOLERANCE = 0.10


def validate_bundle(
    bundle: GeneratedBundle,
    spec: dict | None = None,
) -> list[str]:
    """Run all validators. Returns ordered error messages."""
    errors: list[str] = []
    errors.extend(_check_unique_ids(bundle))
    errors.extend(_check_actor_graph(bundle))
    errors.extend(_check_goal_tree(bundle))
    errors.extend(_check_commitment_refs(bundle))
    errors.extend(_check_signal_refs(bundle))
    errors.extend(_check_model_refs(bundle))
    errors.extend(_check_recommendation_refs(bundle))
    if spec is not None:
        errors.extend(_check_counts(bundle, spec))
    return errors


# ---------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------


def _check_unique_ids(b: GeneratedBundle) -> list[str]:
    errs: list[str] = []
    for label, ids in (
        ("actor", [a.id for a in b.actors]),
        ("customer", [c.id for c in b.customers]),
        ("goal", [g.id for g in b.goals]),
        ("decision", [d.id for d in b.decisions]),
        ("commitment", [c.id for c in b.commitments]),
        ("signal", [s.id for s in b.signals]),
        ("recommendation", [r.id for r in b.recommendations]),
    ):
        dupes = _duplicates(ids)
        if dupes:
            errs.append(f"{label}: duplicate ids {sorted(dupes)[:5]}")
    return errs


def _check_actor_graph(b: GeneratedBundle) -> list[str]:
    errs: list[str] = []
    actor_ids = {a.id for a in b.actors}
    if not actor_ids:
        return errs
    for a in b.actors:
        if a.manager_id is None:
            continue
        if a.manager_id not in actor_ids:
            errs.append(f"actor {a.id}: manager_id {a.manager_id} unresolved")
    edges = {a.id: a.manager_id for a in b.actors if a.manager_id}
    if _has_cycle(edges):
        errs.append("actor reporting graph has a cycle")
    return errs


def _check_goal_tree(b: GeneratedBundle) -> list[str]:
    errs: list[str] = []
    actor_ids = {a.id for a in b.actors}
    goal_ids = {g.id for g in b.goals}
    for g in b.goals:
        if g.owner_id and g.owner_id not in actor_ids:
            errs.append(f"goal {g.id}: owner_id {g.owner_id} unresolved")
        if g.parent_goal_id and g.parent_goal_id not in goal_ids:
            errs.append(f"goal {g.id}: parent_goal_id {g.parent_goal_id} unresolved")
    edges = {g.id: g.parent_goal_id for g in b.goals if g.parent_goal_id}
    if _has_cycle(edges):
        errs.append("goal tree has a cycle")
    return errs


def _check_commitment_refs(b: GeneratedBundle) -> list[str]:
    errs: list[str] = []
    actor_ids = {a.id for a in b.actors}
    goal_ids = {g.id for g in b.goals}
    customer_ids = {c.id for c in b.customers}
    decision_ids = {d.id for d in b.decisions}
    commit_ids = {c.id for c in b.commitments}

    # depends_on edges as multimap.
    depends: dict[str, list[str]] = defaultdict(list)
    for c in b.commitments:
        if c.owner_id not in actor_ids:
            errs.append(f"commitment {c.id}: owner_id {c.owner_id} unresolved")
        for cid in c.contributors:
            if cid not in actor_ids:
                errs.append(f"commitment {c.id}: contributor {cid} unresolved")
        if c.contributes_to_goal_id and c.contributes_to_goal_id not in goal_ids:
            errs.append(
                f"commitment {c.id}: contributes_to_goal_id "
                f"{c.contributes_to_goal_id} unresolved"
            )
        if c.served_by_customer_id and c.served_by_customer_id not in customer_ids:
            errs.append(
                f"commitment {c.id}: served_by_customer_id "
                f"{c.served_by_customer_id} unresolved"
            )
        for did in c.constrained_by_decision_ids:
            if did not in decision_ids:
                errs.append(
                    f"commitment {c.id}: constrained_by decision {did} unresolved"
                )
        for dep in c.depends_on:
            if dep not in commit_ids:
                errs.append(f"commitment {c.id}: depends_on {dep} unresolved")
            else:
                depends[c.id].append(dep)

    if _has_cycle_multi(depends):
        errs.append("commitment depends_on graph has a cycle")
    return errs


def _check_signal_refs(b: GeneratedBundle) -> list[str]:
    errs: list[str] = []
    actor_ids = {a.id for a in b.actors}
    type_buckets = {
        "actor": actor_ids,
        "customer": {c.id for c in b.customers},
        "commitment": {c.id for c in b.commitments},
        "decision": {d.id for d in b.decisions},
        "goal": {g.id for g in b.goals},
    }
    for s in b.signals:
        if s.author_id not in actor_ids:
            errs.append(f"signal {s.id}: author_id {s.author_id} unresolved")
        for ent in s.entities_mentioned:
            bucket = type_buckets.get(ent.type)
            if bucket is None or ent.id not in bucket:
                errs.append(
                    f"signal {s.id}: mentioned {ent.type}/{ent.id} unresolved"
                )
    return errs


def _check_model_refs(b: GeneratedBundle) -> list[str]:
    """Non-recommendation Models. Validates supporting refs + scope_actors
    + scope_entities + per-kind requirements (predictions need
    evaluate_at; high-confidence models need a falsifier)."""
    errs: list[str] = []
    actor_ids = {a.id for a in b.actors}
    signal_ids = {s.id for s in b.signals}
    model_ids = {m.id for m in b.models}
    type_buckets = {
        "actor": actor_ids,
        "customer": {c.id for c in b.customers},
        "commitment": {c.id for c in b.commitments},
        "decision": {d.id for d in b.decisions},
        "goal": {g.id for g in b.goals},
    }
    for m in b.models:
        if not (0.05 <= m.confidence <= 0.95):
            errs.append(f"model {m.id}: confidence {m.confidence} outside [0.05, 0.95]")
        if m.confidence > 0.7 and m.falsifier is None:
            errs.append(
                f"model {m.id}: confidence {m.confidence} > 0.7 requires falsifier"
            )
        if m.kind == "prediction" and not m.evaluate_at:
            errs.append(f"model {m.id}: prediction kind requires evaluate_at")
        for aid in m.scope_actor_ids:
            if aid not in actor_ids:
                errs.append(f"model {m.id}: scope_actor_id {aid} unresolved")
        for ent in m.scope_entities:
            if not isinstance(ent, dict):
                continue
            t, eid = ent.get("type"), ent.get("id")
            bucket = type_buckets.get(t)
            if bucket is None or eid not in bucket:
                errs.append(f"model {m.id}: scope_entity {t}/{eid} unresolved")
        for sid in m.supporting_observation_ids:
            if sid not in signal_ids:
                errs.append(f"model {m.id}: supporting_observation {sid} unresolved")
        for mid in m.supporting_model_ids:
            if mid not in model_ids:
                errs.append(f"model {m.id}: supporting_model {mid} unresolved")
    return errs


def _check_recommendation_refs(b: GeneratedBundle) -> list[str]:
    errs: list[str] = []
    signal_ids = {s.id for s in b.signals}
    actor_ids = {a.id for a in b.actors}
    # Recommendations may cite either non-recommendation models or
    # other recommendations as `supporting_model_ids`.
    referenceable_models = {m.id for m in b.models} | {r.id for r in b.recommendations}
    target_buckets = {
        "actor": actor_ids,
        "commitment": {c.id for c in b.commitments},
        "goal": {g.id for g in b.goals},
        "decision": {d.id for d in b.decisions},
    }
    for r in b.recommendations:
        if r.target_actor_id not in actor_ids:
            errs.append(
                f"recommendation {r.id}: target_actor_id "
                f"{r.target_actor_id} unresolved"
            )
        bucket = target_buckets.get(r.target_act_ref.type)
        if bucket is None or r.target_act_ref.id not in bucket:
            errs.append(
                f"recommendation {r.id}: target_act_ref "
                f"{r.target_act_ref.type}/{r.target_act_ref.id} unresolved"
            )
        for sid in r.supporting_observation_ids:
            if sid not in signal_ids:
                errs.append(
                    f"recommendation {r.id}: supporting_observation {sid} unresolved"
                )
        for mid in r.supporting_model_ids:
            if mid not in referenceable_models:
                errs.append(
                    f"recommendation {r.id}: supporting_model {mid} unresolved"
                )
    return errs


def _check_counts(b: GeneratedBundle, spec: dict) -> list[str]:
    errs: list[str] = []
    pairs = [
        ("actors", len(b.actors), int(spec.get("actor_count", 0))),
        ("customers", len(b.customers), int(spec.get("customer_count", 0))),
        ("goals", len(b.goals), int(spec.get("goal_count", 0))),
        ("decisions", len(b.decisions), int(spec.get("decision_count", 0))),
        ("commitments", len(b.commitments), int(spec.get("commitment_count", 0))),
        (
            "recommendations",
            len(b.recommendations),
            int(spec.get("recommendation_count", 0)),
        ),
    ]
    for label, actual, target in pairs:
        if target == 0:
            continue
        low = target * (1 - COUNT_TOLERANCE)
        high = target * (1 + COUNT_TOLERANCE)
        if not (low <= actual <= high):
            errs.append(
                f"count {label}: {actual} outside ±{int(COUNT_TOLERANCE * 100)}% "
                f"of spec {target}"
            )
    return errs


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _duplicates(seq: Iterable[str]) -> set[str]:
    seen: set[str] = set()
    dupes: set[str] = set()
    for x in seq:
        if x in seen:
            dupes.add(x)
        seen.add(x)
    return dupes


def _has_cycle(edges: dict[str, str]) -> bool:
    """edges: child -> parent. Returns True if any cycle exists."""
    color: dict[str, int] = {}     # 0=white, 1=gray, 2=black
    def visit(n: str) -> bool:
        c = color.get(n, 0)
        if c == 1:
            return True
        if c == 2:
            return False
        color[n] = 1
        nxt = edges.get(n)
        if nxt is not None and visit(nxt):
            return True
        color[n] = 2
        return False
    return any(visit(node) for node in list(edges.keys()))


def _has_cycle_multi(edges: dict[str, list[str]]) -> bool:
    """Multi-edge variant for the commitment dependency graph."""
    color: dict[str, int] = {}
    def visit(n: str) -> bool:
        c = color.get(n, 0)
        if c == 1:
            return True
        if c == 2:
            return False
        color[n] = 1
        for nxt in edges.get(n, []):
            if visit(nxt):
                return True
        color[n] = 2
        return False
    return any(visit(node) for node in list(edges.keys()))


__all__ = ["validate_bundle", "COUNT_TOLERANCE"]
