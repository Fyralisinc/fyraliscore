"""services/workers/anomaly_processor/detectors.py — six detectors.

One function per anomaly kind per BUILD-PLAN Prompt 4.B and
ARCHITECTURE-FINAL.md §18. Each returns a list of `AnomalyCandidate`
dataclass instances.

All SQL runs on a caller-supplied `asyncpg.Connection` so detectors
can be composed inside the worker's poll cycle under one pool
acquisition per cycle. Every query is tenant-scoped; cross-tenant
anomaly detection is forbidden by design.

Time windows
------------
Detectors that have a temporal component take a `window` timedelta.
The worker supplies sensible defaults per the spec:

- contestation_cluster: 30 min
- silent_disagreement: 7 days (lookback for actor activity)
- external_signal_anomaly: 60 min
- commitment_drift: 28 days (4 weeks)
- activation_decay / resource_overcommit: no temporal window; current state

Region
------
`region_entity_ids` is the list of `(entity_kind, entity_id)` tuples
that the T3 trigger should lock. The worker computes `region_hash`
from this list via `debounce.compute_region_hash`. We keep the hash
out of the detectors to keep them pure.

The ``payload`` field is free-form context the worker passes into the
T3 trigger payload verbatim (JSONB). Put only JSON-serialisable
primitives here — UUIDs are stringified at enqueue time.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg


# ---------------------------------------------------------------------
# Shared types
# ---------------------------------------------------------------------


ANOMALY_KINDS = (
    "contestation_cluster",
    "silent_disagreement",
    "activation_decay_anomaly",
    "external_signal_anomaly",
    "commitment_drift",
    "resource_overcommit",
)


@dataclass
class AnomalyCandidate:
    """
    Output shape of every detector. The worker turns this into a
    T3 trigger payload (if significance >= threshold and not
    debounced) or a row in `signal_memory_fabric` (if below).

    Fields:
    - kind — one of ANOMALY_KINDS.
    - entity_type — the primary entity the anomaly is about (e.g.
      'commitment', 'resource', 'model'). Used for region key.
    - entity_id — the primary entity's UUID.
    - tenant_id — tenant scope.
    - region_entity_ids — list of [{entity_kind, entity_id}] dicts
      describing the full touched region. The first entry is almost
      always `{entity_kind: entity_type, entity_id: entity_id}`; the
      detector may add more (e.g. contestation_cluster adds all the
      contested Model ids).
    - significance — raw base significance 0.0-1.0. `compute_significance`
      will modulate this by critical-path / customer / trust-tier
      weights.
    - triggering_observation_ids — Observations (or state_change rows)
      that triggered the detection. The worker copies these into the
      T3 payload so Think can replay the underlying signals.
    - payload — extra context the detector wants to hand to Think.
    - trust_tiers — the trust tiers of the triggering signals,
      used by `compute_significance` for the trust-tier modulator.
    """
    kind: str
    entity_type: str
    entity_id: UUID
    tenant_id: UUID
    region_entity_ids: list[dict[str, Any]]
    significance: float
    triggering_observation_ids: list[UUID] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    trust_tiers: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _jsonb(v: Any) -> str:
    """Tolerant JSONB encoder — UUIDs become strings."""
    return json.dumps(v, default=str, sort_keys=True)


def _parse_content(raw: Any) -> dict[str, Any]:
    """asyncpg returns JSONB as str or dict depending on codec. Normalise."""
    if raw is None:
        return {}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    if isinstance(raw, dict):
        return raw
    return {}


def _parse_current_value(raw: Any) -> dict[str, Any]:
    return _parse_content(raw)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =====================================================================
# 1. contestation_cluster
# =====================================================================


async def detect_contestation_cluster(
    tenant_id: UUID,
    window: timedelta,
    conn: asyncpg.Connection,
    *,
    min_cluster_size: int = 3,
    reference_time: datetime | None = None,
) -> list[AnomalyCandidate]:
    """
    N contestations of Models with similar scope/proposition within
    a time window. Spec §18 "Contestation cluster — multiple
    contestations in same region".

    Implementation:
    - Read Observations where kind='contestation' and
      occurred_at >= now - window.
    - Group by `content.contested_model_id`. Each contested model
      with >= `min_cluster_size` contestations in-window fires.
    - Region = {model_id, all contesters' actor ids}.
    """
    ref = reference_time or _utc_now()
    start = ref - window

    rows = await conn.fetch(
        """
        SELECT id, actor_id, occurred_at, content, trust_tier
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'contestation'
          AND occurred_at >= $2
          AND occurred_at <= $3
        ORDER BY occurred_at ASC
        """,
        tenant_id, start, ref,
    )

    by_model: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        content = _parse_content(r["content"])
        model_id_str = content.get("contested_model_id")
        if not model_id_str:
            continue
        by_model.setdefault(model_id_str, []).append({
            "obs_id": r["id"],
            "actor_id": r["actor_id"],
            "occurred_at": r["occurred_at"],
            "trust_tier": r["trust_tier"],
        })

    out: list[AnomalyCandidate] = []
    for model_id_str, contestations in by_model.items():
        if len(contestations) < min_cluster_size:
            continue
        try:
            model_uuid = UUID(model_id_str)
        except (ValueError, TypeError):
            continue
        triggering_ids = [c["obs_id"] for c in contestations]
        contester_actor_ids = [
            c["actor_id"] for c in contestations if c["actor_id"] is not None
        ]
        trust_tiers = [c["trust_tier"] for c in contestations if c["trust_tier"]]

        region: list[dict[str, Any]] = [
            {"entity_kind": "model", "entity_id": str(model_uuid)},
        ]
        for a in contester_actor_ids:
            region.append({"entity_kind": "actor", "entity_id": str(a)})

        # Base significance scales with cluster size. Spec leaves the
        # exact formula to the significance module — here we give a
        # first-pass baseline that `compute_significance` modulates.
        base = min(0.9, 0.3 + 0.1 * len(contestations))

        out.append(AnomalyCandidate(
            kind="contestation_cluster",
            entity_type="model",
            entity_id=model_uuid,
            tenant_id=tenant_id,
            region_entity_ids=region,
            significance=base,
            triggering_observation_ids=triggering_ids,
            payload={
                "contested_model_id": str(model_uuid),
                "cluster_size": len(contestations),
                "contester_actor_ids": [str(a) for a in contester_actor_ids],
            },
            trust_tiers=trust_tiers,
        ))
    return out


# =====================================================================
# 2. silent_disagreement
# =====================================================================


async def detect_silent_disagreement(
    tenant_id: UUID,
    window: timedelta,
    conn: asyncpg.Connection,
    *,
    activity_min_count: int = 3,
    confidence_rise_min: float = 0.1,
    reference_time: datetime | None = None,
) -> list[AnomalyCandidate]:
    """
    Model scoped to Actor A, confidence rising, but A is active in
    ingestion observations (not state_change) yet never appears in
    the Model's supporting_event_ids. Spec §11.

    Heuristic:
    - For each active Model with non-empty scope_actors:
      - Check confidence_at_assertion < confidence AND
        (confidence - confidence_at_assertion) >= confidence_rise_min.
      - For each actor in scope_actors, check the actor has
        >= activity_min_count non-state-change observations in window.
      - If no observation in window by that actor shows up in the
        Model's supporting_event_ids, flag.

    Keeps the query simple by doing the final filter in Python — the
    Model count per tenant is small; the hot path is the per-actor
    observation count which is indexed on (actor_id, occurred_at).
    """
    ref = reference_time or _utc_now()
    start = ref - window

    models = await conn.fetch(
        """
        SELECT id, scope_actors, confidence, confidence_at_assertion,
               supporting_event_ids, proposition
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND array_length(scope_actors, 1) >= 1
        """,
        tenant_id,
    )

    out: list[AnomalyCandidate] = []
    for m in models:
        conf = float(m["confidence"])
        prior = float(m["confidence_at_assertion"])
        if (conf - prior) < confidence_rise_min:
            continue
        scope_actors = list(m["scope_actors"] or [])
        support_ids = list(m["supporting_event_ids"] or [])

        for actor_id in scope_actors:
            # Actor's non-state-change observations in window.
            actor_obs = await conn.fetch(
                """
                SELECT id, trust_tier
                FROM observations
                WHERE tenant_id = $1
                  AND actor_id = $2
                  AND occurred_at >= $3
                  AND occurred_at <= $4
                  AND kind != 'state_change'
                """,
                tenant_id, actor_id, start, ref,
            )
            if len(actor_obs) < activity_min_count:
                continue
            # If any of the actor's observations is in supporting_event_ids,
            # the actor has contributed — not silent.
            actor_obs_ids = {r["id"] for r in actor_obs}
            if actor_obs_ids.intersection(set(support_ids)):
                continue
            # Flag: actor active, scope includes them, Model confidence
            # rose, but no support from them.
            region = [
                {"entity_kind": "model", "entity_id": str(m["id"])},
                {"entity_kind": "actor", "entity_id": str(actor_id)},
            ]
            out.append(AnomalyCandidate(
                kind="silent_disagreement",
                entity_type="model",
                entity_id=m["id"],
                tenant_id=tenant_id,
                region_entity_ids=region,
                significance=0.5 + min(0.3, (conf - prior) * 0.5),
                triggering_observation_ids=[r["id"] for r in actor_obs],
                payload={
                    "model_id": str(m["id"]),
                    "silent_actor_id": str(actor_id),
                    "confidence_rise": conf - prior,
                    "activity_count": len(actor_obs),
                },
                trust_tiers=[r["trust_tier"] for r in actor_obs if r["trust_tier"]],
            ))
    return out


# =====================================================================
# 3. activation_decay_anomaly
# =====================================================================


async def detect_activation_decay_anomaly(
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    activation_floor: float = 0.8,
    cohort_p90_max: float = 0.6,
) -> list[AnomalyCandidate]:
    """
    Models retrieved way more than their cohort's p90. Spec §18
    "Activation anomaly".

    A Model is anomalous when:
    - Its activation is >= `activation_floor` AND
    - The cohort's 90th-percentile activation is <= `cohort_p90_max`.

    Cohort = active Models of the same `proposition_kind` in the same
    tenant. If a tenant has a single active Model of the kind, there's
    no cohort, skip (the "way more than its neighbors" test fails trivially).
    """
    # Fetch all active models per tenant, grouped by proposition_kind.
    rows = await conn.fetch(
        """
        SELECT id, proposition_kind, activation
        FROM models
        WHERE tenant_id = $1 AND status = 'active'
        """,
        tenant_id,
    )

    # Group into cohorts.
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        k = r["proposition_kind"]
        if k is None:
            continue
        by_kind.setdefault(k, []).append({
            "id": r["id"],
            "activation": float(r["activation"]),
        })

    out: list[AnomalyCandidate] = []
    for kind_str, cohort in by_kind.items():
        if len(cohort) < 3:
            # No statistically meaningful cohort.
            continue
        activations = sorted(c["activation"] for c in cohort)
        # Simple p90 — nearest-rank.
        p90_idx = max(0, int(0.9 * (len(activations) - 1)))
        p90 = activations[p90_idx]
        if p90 > cohort_p90_max:
            # Cohort itself is hot — no single outlier stands out.
            continue
        for c in cohort:
            if c["activation"] < activation_floor:
                continue
            # Only flag if this model's activation is substantially
            # above the cohort p90 (at least 2x OR 0.3 absolute gap).
            if c["activation"] < p90 * 2 and (c["activation"] - p90) < 0.3:
                continue
            region = [{"entity_kind": "model", "entity_id": str(c["id"])}]
            out.append(AnomalyCandidate(
                kind="activation_decay_anomaly",
                entity_type="model",
                entity_id=c["id"],
                tenant_id=tenant_id,
                region_entity_ids=region,
                significance=min(0.9, 0.4 + (c["activation"] - p90)),
                triggering_observation_ids=[],
                payload={
                    "model_id": str(c["id"]),
                    "activation": c["activation"],
                    "cohort_p90": p90,
                    "cohort_size": len(cohort),
                    "proposition_kind": kind_str,
                },
                trust_tiers=[],
            ))
    return out


# =====================================================================
# 4. external_signal_anomaly
# =====================================================================


async def detect_external_signal_anomaly(
    tenant_id: UUID,
    window: timedelta,
    conn: asyncpg.Connection,
    *,
    min_burst_size: int = 3,
    reference_time: datetime | None = None,
) -> list[AnomalyCandidate]:
    """
    Burst of `authoritative_external` or `reputable` observations
    on a single entity within a time window. Spec §18 "External shock".

    Implementation:
    - Load Observations with trust_tier IN ('authoritative_external',
      'reputable') in window.
    - Group by each entity in `entities_mentioned`.
    - Fire when count >= min_burst_size.
    """
    ref = reference_time or _utc_now()
    start = ref - window

    rows = await conn.fetch(
        """
        SELECT id, trust_tier, entities_mentioned, occurred_at
        FROM observations
        WHERE tenant_id = $1
          AND trust_tier IN ('authoritative_external', 'reputable')
          AND occurred_at >= $2
          AND occurred_at <= $3
        """,
        tenant_id, start, ref,
    )

    # entity_key -> list of (obs_id, trust_tier)
    by_entity: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        entities = _parse_content(r["entities_mentioned"])
        if isinstance(entities, dict):
            # Normalise single dict to list for convenience.
            entities = [entities]
        if not isinstance(entities, list):
            continue
        for e in entities:
            if not isinstance(e, dict):
                continue
            kind = e.get("type") or e.get("entity_kind") or e.get("kind")
            eid = e.get("id") or e.get("entity_id")
            if not kind or not eid:
                continue
            key = (str(kind), str(eid))
            by_entity.setdefault(key, []).append({
                "obs_id": r["id"],
                "trust_tier": r["trust_tier"],
            })

    out: list[AnomalyCandidate] = []
    for (entity_kind, entity_id_str), items in by_entity.items():
        if len(items) < min_burst_size:
            continue
        # entity_id may not be a UUID (external refs); try to parse,
        # fallback to the string key. Region_ids uses strings.
        try:
            entity_uuid = UUID(entity_id_str)
        except (ValueError, TypeError):
            # Non-UUID entity id (external ref) — synthesize via UUID5.
            # We still return a valid UUID for entity_id because the
            # caller always expects one. We use a deterministic
            # namespace so the same entity maps to the same UUID.
            import uuid as _uuid
            entity_uuid = _uuid.uuid5(_uuid.NAMESPACE_URL,
                                      f"{entity_kind}:{entity_id_str}")
        region = [{"entity_kind": entity_kind, "entity_id": entity_id_str}]
        trust_tiers = [i["trust_tier"] for i in items]
        # Base score from §18 `external_event` branch.
        has_auth_external = any(
            t == "authoritative_external" for t in trust_tiers
        )
        base = 0.5 + min(0.4, 0.1 * len(items))
        if not has_auth_external:
            base *= 0.8  # reputable-only bursts are slightly less sharp
        out.append(AnomalyCandidate(
            kind="external_signal_anomaly",
            entity_type=entity_kind,
            entity_id=entity_uuid,
            tenant_id=tenant_id,
            region_entity_ids=region,
            significance=min(0.95, base),
            triggering_observation_ids=[i["obs_id"] for i in items],
            payload={
                "entity_kind": entity_kind,
                "entity_ref": entity_id_str,
                "burst_size": len(items),
            },
            trust_tiers=trust_tiers,
        ))
    return out


# =====================================================================
# 5. commitment_drift
# =====================================================================


async def detect_commitment_drift(
    tenant_id: UUID,
    window: timedelta,
    conn: asyncpg.Connection,
    *,
    min_events: int = 3,
    reference_time: datetime | None = None,
) -> list[AnomalyCandidate]:
    """
    Repeated `due_date` extensions or `owner_id` reassignments on the
    same Commitment within N weeks. Spec §18 "Commitment drift".

    Drift source:
    - state_change Observations with `content.state_change_kind` in
      {'due_date_extended', 'owner_reassigned', 'commitment_due_date_changed',
       'commitment_owner_changed'}.
    - content.entity_id is the Commitment id.

    Sub-threshold-by-default: a single state_change doesn't fire; the
    Memory Fabric catches those. `min_events` (default 3) gates the
    real detection.
    """
    ref = reference_time or _utc_now()
    start = ref - window

    drift_kinds = [
        "due_date_extended",
        "owner_reassigned",
        "commitment_due_date_changed",
        "commitment_owner_changed",
    ]

    rows = await conn.fetch(
        """
        SELECT id, content, occurred_at, trust_tier
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'state_change'
          AND occurred_at >= $2
          AND occurred_at <= $3
          AND content->>'state_change_kind' = ANY($4::text[])
        """,
        tenant_id, start, ref, drift_kinds,
    )

    by_commit: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        content = _parse_content(r["content"])
        entity_id_str = content.get("entity_id")
        entity_kind = content.get("entity_kind")
        if not entity_id_str:
            continue
        # Only commitment-scoped drift counts.
        if entity_kind not in (None, "commitment"):
            continue
        by_commit.setdefault(entity_id_str, []).append({
            "obs_id": r["id"],
            "state_change_kind": content.get("state_change_kind"),
            "occurred_at": r["occurred_at"],
            "trust_tier": r["trust_tier"],
        })

    out: list[AnomalyCandidate] = []
    for cid_str, events in by_commit.items():
        if len(events) < min_events:
            continue
        try:
            cid = UUID(cid_str)
        except (ValueError, TypeError):
            continue
        # Confirm the commitment still exists (and fetch critical-path /
        # customer hints for `compute_significance`).
        cr = await conn.fetchrow(
            """
            SELECT id, tenant_id, external_counterparty_ref
            FROM commitments
            WHERE id = $1 AND tenant_id = $2
            """,
            cid, tenant_id,
        )
        if cr is None:
            continue
        region = [{"entity_kind": "commitment", "entity_id": str(cid)}]
        base = 0.5 + min(0.3, 0.05 * len(events))
        out.append(AnomalyCandidate(
            kind="commitment_drift",
            entity_type="commitment",
            entity_id=cid,
            tenant_id=tenant_id,
            region_entity_ids=region,
            significance=min(0.9, base),
            triggering_observation_ids=[e["obs_id"] for e in events],
            payload={
                "commitment_id": str(cid),
                "drift_event_count": len(events),
                "kinds_seen": sorted(
                    {e["state_change_kind"] for e in events}
                ),
            },
            trust_tiers=[e["trust_tier"] for e in events if e["trust_tier"]],
        ))
    return out


# =====================================================================
# 6. resource_overcommit
# =====================================================================


async def detect_resource_overcommit(
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    utilization_threshold: float = 0.95,
) -> list[AnomalyCandidate]:
    """
    Capacity resources whose cumulative deploys exceed (utilization_threshold)
    of total_units. Spec §18 "Resource threshold anomaly".

    Uses the same math as `services/resources/bridge.capability_at_risk`
    so the two modules agree on what counts as over-committed. Bridge
    returns the pair (resource, deploying_commitments); we return a
    candidate keyed on the resource.
    """
    rows = await conn.fetch(
        """
        SELECT id, current_value
        FROM resources
        WHERE tenant_id = $1
          AND kind = 'capacity'
          AND archived_at IS NULL
        """,
        tenant_id,
    )

    out: list[AnomalyCandidate] = []
    for r in rows:
        cv = _parse_current_value(r["current_value"])
        total = float(cv.get("total_units", 0) or 0)
        deployed = float(cv.get("deployed_units", 0) or 0)
        if total <= 0:
            continue
        utilization = deployed / total
        if utilization < utilization_threshold:
            continue
        # Fetch deploying commitments for the trigger payload + region.
        deploy_rows = await conn.fetch(
            """
            SELECT commitment_id
            FROM resource_deployments
            WHERE resource_id = $1 AND released_at IS NULL
            """,
            r["id"],
        )
        deploying_commitment_ids = [row["commitment_id"] for row in deploy_rows]
        region: list[dict[str, Any]] = [
            {"entity_kind": "resource", "entity_id": str(r["id"])},
        ]
        for cid in deploying_commitment_ids:
            region.append({
                "entity_kind": "commitment",
                "entity_id": str(cid),
            })
        # Spec §18 base=0.8 at >0.95 util.
        base = 0.8 if utilization > 0.95 else 0.5
        out.append(AnomalyCandidate(
            kind="resource_overcommit",
            entity_type="resource",
            entity_id=r["id"],
            tenant_id=tenant_id,
            region_entity_ids=region,
            significance=base,
            triggering_observation_ids=[],
            payload={
                "resource_id": str(r["id"]),
                "total_units": total,
                "deployed_units": deployed,
                "utilization": utilization,
                "deploying_commitments": [str(c) for c in deploying_commitment_ids],
            },
            trust_tiers=[],
        ))
    return out


__all__ = [
    "ANOMALY_KINDS",
    "AnomalyCandidate",
    "detect_activation_decay_anomaly",
    "detect_commitment_drift",
    "detect_contestation_cluster",
    "detect_external_signal_anomaly",
    "detect_resource_overcommit",
    "detect_silent_disagreement",
]
