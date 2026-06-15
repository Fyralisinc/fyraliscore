"""Artifact drawer and Structure overlay query builders."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

# ---------------------------------------------------------------------
# Artifact lookup — per-type fetch + relationship queries powering the
# artifact drawer. Each kind composes a few short SELECTs and assembles
# a structured `sections` list (field-grid, narrative, link-list).
# ---------------------------------------------------------------------


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _ago(ts: Any, *, now: datetime | None = None) -> str:
    """Human-friendly relative timestamp ("3 days ago", "2 hr ago")."""
    if ts is None or not hasattr(ts, "tzinfo"):
        return "—"
    now = now or datetime.now(timezone.utc)
    delta = now - ts
    secs = int(delta.total_seconds())
    if secs < 60:
        return "just now"
    if secs < 3600:
        return f"{secs // 60} min ago"
    if secs < 86400:
        return f"{secs // 3600} hr ago"
    days = secs // 86400
    if days == 1:
        return "yesterday"
    if days < 30:
        return f"{days} days ago"
    months = days // 30
    if months < 12:
        return f"{months} mo ago"
    return f"{days // 365} yr ago"


def _trim(s: str | None, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


@dataclass(frozen=True, slots=True)
class _CommitmentOverlayRows:
    commitment: Any
    owner: Any | None
    goals: list[Any]
    customers: list[Any]
    contributors: list[Any]
    consumed_resources: list[Any]
    decisions: list[Any]
    pattern_models: list[Any]
    state_changes: list[Any]


async def fetch_commitment_overlay(
    cid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    """Build the Structure-overlay payload for a single commitment:
    the commitment row + its contributing goals + its customer link +
    its owner / contributors. Used by both the focus-by-id endpoint
    and the recent-commitments list endpoint."""
    rows = await _fetch_commitment_overlay_rows(cid, tenant_id, conn)
    if rows is None:
        return None

    activity_payload, substrate_insight = await _build_commitment_activity_payload(
        rows.state_changes,
        tenant_id,
        conn,
    )
    customers_payload, customer_id_str, customer_label = _build_customer_payload(
        rows.customers
    )
    resources_payload = _build_consumed_resources_payload(rows.consumed_resources)
    learnings_payload = await _build_commitment_learnings_payload(
        rows.pattern_models,
        tenant_id,
        conn,
    )

    commitment_payload = _build_commitment_overlay_payload(
        rows=rows,
        customer_id_str=customer_id_str,
        customer_label=customer_label,
        resources_payload=resources_payload,
        substrate_insight=substrate_insight,
        activity_payload=activity_payload,
        learnings_payload=learnings_payload,
    )
    return {
        "commitment": commitment_payload,
        "goals": _build_goal_overlay_payload(rows.goals),
        "people": _build_people_overlay_payload(rows.owner, rows.contributors),
        "customers": customers_payload,
        "decisions": _build_decision_overlay_payload(rows.decisions),
        "resources": resources_payload,
    }


async def _fetch_commitment_overlay_rows(
    cid: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> _CommitmentOverlayRows | None:
    crow = await conn.fetchrow(
        "SELECT id, title, state, owner_id, due_date, priority, "
        "       is_maintenance "
        "FROM commitments WHERE id = $1 AND tenant_id = $2",
        cid, tenant_id,
    )
    if crow is None:
        return None

    owner_row = None
    if crow["owner_id"] is not None:
        owner_row = await conn.fetchrow(
            "SELECT id, display_name FROM actors "
            "WHERE id = $1 AND tenant_id = $2",
            crow["owner_id"], tenant_id,
        )

    goal_rows = list(await conn.fetch(
        "SELECT g.id, g.title, g.altitude, g.parent_goal_id FROM goals g "
        "JOIN contributes_to ct ON ct.goal_id = g.id "
        "WHERE ct.commitment_id = $1 AND g.tenant_id = $2",
        cid, tenant_id,
    ))

    customer_rows = list(await conn.fetch(
        "SELECT r.id, r.identity, r.metadata FROM resources r "
        "JOIN customer_commitments cc ON cc.customer_resource_id = r.id "
        "WHERE cc.commitment_id = $1 AND r.tenant_id = $2",
        cid, tenant_id,
    ))

    contributor_rows = list(await conn.fetch(
        "SELECT a.id, a.display_name FROM actors a "
        "JOIN commitment_contributors cc ON cc.actor_id = a.id "
        "WHERE cc.commitment_id = $1 AND a.tenant_id = $2",
        cid, tenant_id,
    ))

    # Capacity resources consumed by this commitment. We exclude the
    # `relational` kind so customer rows (also stored in `resources`)
    # don't double-count as capacity resources in the graph.
    consumed_resource_rows = list(await conn.fetch(
        "SELECT r.id, r.kind, r.identity, r.description, r.current_value, "
        "       r.utilization_state, r.metadata, "
        "       rd.deployed_quantity "
        "FROM resources r "
        "JOIN resource_deployments rd ON rd.resource_id = r.id "
        "WHERE rd.commitment_id = $1 "
        "  AND rd.released_at IS NULL "
        "  AND r.tenant_id = $2 "
        "  AND r.kind IN ('human', 'financial', 'technical', 'time') "
        "ORDER BY r.kind, r.identity",
        cid, tenant_id,
    ))

    decision_rows = list(await conn.fetch(
        "SELECT d.id, d.title, d.decision_text, d.rationale, d.state "
        "FROM decisions d "
        "JOIN constrained_by cb ON cb.decision_id = d.id "
        "WHERE cb.commitment_id = $1 AND d.tenant_id = $2",
        cid, tenant_id,
    ))

    # Models scoped to this commitment — surfaced as learned-pattern
    # bundles on the commitment card. Filter by scope_entities @>
    # [{type=commitment, id=cid}] using JSONB containment, then pick
    # top 6 by confidence so the card stays scannable.
    pattern_model_rows = list(await conn.fetch(
        """
        SELECT id, "natural", proposition, confidence, falsifier,
               proposition_kind AS kind,
               supporting_event_ids, evidential_weight,
               created_at
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND scope_entities @> $2::jsonb
        ORDER BY confidence DESC NULLS LAST, created_at DESC
        LIMIT 6
        """,
        tenant_id,
        json.dumps([{"type": "commitment", "id": str(cid)}]),
    ))

    # State-change history: most recent transition + the originating
    # signal that caused it. Used to render "why this is at risk" on
    # the Structure detail card.
    state_change_rows = list(await conn.fetch(
        """
        SELECT id, occurred_at, cause_id, content
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'state_change'
          AND content->>'entity_kind' = 'commitment'
          AND content->>'entity_id' = $2::text
        ORDER BY occurred_at DESC
        LIMIT 5
        """,
        tenant_id, str(cid),
    ))

    return _CommitmentOverlayRows(
        commitment=crow,
        owner=owner_row,
        goals=goal_rows,
        customers=customer_rows,
        contributors=contributor_rows,
        consumed_resources=consumed_resource_rows,
        decisions=decision_rows,
        pattern_models=pattern_model_rows,
        state_changes=state_change_rows,
    )


async def _build_commitment_activity_payload(
    state_change_rows: list[Any],
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> tuple[list[dict[str, Any]], str | None]:
    activity_payload: list[dict[str, Any]] = []
    substrate_insight: str | None = None
    seen_cause_ids: set[UUID] = set()
    for sc in state_change_rows:
        sc_content = sc["content"]
        if isinstance(sc_content, str):
            try:
                sc_content = json.loads(sc_content)
            except json.JSONDecodeError:
                sc_content = {}
        elif not isinstance(sc_content, dict):
            sc_content = {}
        from_state = sc_content.get("from_state")
        to_state = sc_content.get("to_state")
        sc_date = sc["occurred_at"].date().isoformat()
        if from_state and to_state:
            activity_payload.append({
                "date": sc_date,
                "desc": f"transitioned {from_state} → {to_state}",
            })
        cause_id = sc["cause_id"]
        if cause_id is None or cause_id in seen_cause_ids:
            continue
        seen_cause_ids.add(cause_id)
        cause_row = await conn.fetchrow(
            "SELECT source_channel, content_text, occurred_at, "
            "       actor_id "
            "FROM observations "
            "WHERE id = $1 AND tenant_id = $2",
            cause_id, tenant_id,
        )
        if cause_row is None:
            continue
        text = (cause_row["content_text"] or "").strip()
        if not text:
            continue
        actor_label: str | None = None
        if cause_row["actor_id"] is not None:
            actor_lookup = await conn.fetchrow(
                "SELECT display_name FROM actors "
                "WHERE id = $1 AND tenant_id = $2",
                cause_row["actor_id"], tenant_id,
            )
            if actor_lookup is not None:
                actor_label = actor_lookup["display_name"]
        cause_date = cause_row["occurred_at"].date().isoformat()
        ch = cause_row["source_channel"] or "signal"
        truncated = text if len(text) <= 240 else text[:237] + "…"
        attribution = (
            f"{actor_label} via {ch}" if actor_label else ch
        )
        activity_payload.append({
            "date": cause_date,
            "desc": f"{attribution}: {truncated}",
        })
        # First (most recent) cause becomes the substrate insight —
        # the headline reason this commitment is in its current state.
        if substrate_insight is None and from_state and to_state:
            substrate_insight = (
                f"Moved to {to_state} after {attribution.lower()}: "
                f"\u201c{truncated}\u201d"
            )
    return activity_payload, substrate_insight


def _build_customer_payload(
    customer_rows: list[Any],
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    customer_id_str: str | None = None
    customer_label: str | None = None
    if customer_rows:
        cr = customer_rows[0]
        customer_id_str = str(cr["id"])
        md = _json_obj(cr["metadata"])
        customer_label = md.get("display_name") or cr["identity"] or "Customer"

    customers_payload: list[dict[str, Any]] = []
    if customer_id_str and customer_label:
        customers_payload.append({
            "id": customer_id_str,
            "label": customer_label,
        })
    return customers_payload, customer_id_str, customer_label


def _build_status_label(state: str) -> str:
    status_label = "on-track"
    if state == "blocked":
        status_label = "blocked"
    elif state == "paused":
        status_label = "at-risk"
    return status_label


def _build_goal_overlay_payload(goal_rows: list[Any]) -> list[dict[str, Any]]:
    goals_payload: list[dict[str, Any]] = []
    for g in goal_rows:
        altitude = (
            g["altitude"] if g["altitude"] in ("strategic", "operational")
            else "operational"
        )
        goals_payload.append({
            "id": str(g["id"]),
            "label": g["title"],
            "altitude": altitude,
            "parent_goal_id": (
                str(g["parent_goal_id"]) if g["parent_goal_id"] else None
            ),
        })
    return goals_payload


def _build_people_overlay_payload(
    owner_row: Any | None,
    contributor_rows: list[Any],
) -> list[dict[str, Any]]:
    people_payload: list[dict[str, Any]] = []
    seen_actor_ids: set[str] = set()
    if owner_row is not None:
        people_payload.append({
            "id": str(owner_row["id"]),
            "label": owner_row["display_name"],
            "role": "Owner",
        })
        seen_actor_ids.add(str(owner_row["id"]))
    for c in contributor_rows:
        cid_str = str(c["id"])
        if cid_str in seen_actor_ids:
            continue
        seen_actor_ids.add(cid_str)
        people_payload.append({
            "id": cid_str,
            "label": c["display_name"],
            "role": "Contributor",
        })
    return people_payload


def _build_decision_overlay_payload(decision_rows: list[Any]) -> list[dict[str, Any]]:
    decisions_payload: list[dict[str, Any]] = []
    for d in decision_rows:
        decisions_payload.append({
            "id": str(d["id"]),
            "label": d["title"],
            "state": d["state"] if d["state"] in (
                "in-force", "drifting", "revisited",
            ) else "in-force",
        })
    return decisions_payload


def _build_consumed_resources_payload(
    consumed_resource_rows: list[Any],
) -> list[dict[str, Any]]:
    # Resources consumed by this commitment — used by the right quadrant
    # of the relational graph and the commitment side-panel "Resources"
    # block. Each entry carries the deployed quantity in the resource's
    # native unit (FTE, USD, engineer-weeks, GPU-hours).
    resources_payload: list[dict[str, Any]] = []
    for rr in consumed_resource_rows:
        cv = _json_obj(rr["current_value"])
        md = _json_obj(rr["metadata"])
        dq = _json_obj(rr["deployed_quantity"])
        resources_payload.append({
            "id": str(rr["id"]),
            "label": cv.get("label") or md.get("label") or rr["identity"] or "Resource",
            "kind": rr["kind"],
            "unit": cv.get("unit"),
            "deployed_quantity": dq.get("value"),
        })
    return resources_payload


async def _build_commitment_learnings_payload(
    pattern_model_rows: list[Any],
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> list[dict[str, Any]]:
    # Build LearnedPattern bundles from the scoped models. Each model's
    # natural-language statement becomes the pattern statement;
    # supporting_event_ids resolve to short evidence snippets via a
    # bounded observation lookup (cap 3 per model so we don't blow up
    # the response).
    learnings_payload: list[dict[str, Any]] = []
    for m in pattern_model_rows:
        statement = (m["natural"] or "").strip()
        if not statement:
            continue
        evidence_payload: list[dict[str, Any]] = []
        ev_ids = m["supporting_event_ids"] or []
        # ev_ids is a list of UUIDs (or strings). Cap at 3.
        ev_lookup_ids = [eid for eid in list(ev_ids)[:3]]
        if ev_lookup_ids:
            ev_rows = await conn.fetch(
                "SELECT id, occurred_at, content_text FROM observations "
                "WHERE tenant_id = $1 AND id = ANY($2::uuid[])",
                tenant_id, [
                    UUID(str(x)) if not isinstance(x, UUID) else x
                    for x in ev_lookup_ids
                ],
            )
            for er in ev_rows:
                t = (er["content_text"] or "").strip()
                if not t:
                    continue
                evidence_payload.append({
                    "when": er["occurred_at"].date().isoformat(),
                    "text": t if len(t) <= 180 else t[:177] + "…",
                })
        learnings_payload.append({
            "id": str(m["id"]),
            "statement": statement if len(statement) <= 240 else statement[:237] + "…",
            "strength": float(m["confidence"] or 0.5),
            "evidence": evidence_payload,
        })
    return learnings_payload


def _build_commitment_overlay_payload(
    *,
    rows: _CommitmentOverlayRows,
    customer_id_str: str | None,
    customer_label: str | None,
    resources_payload: list[dict[str, Any]],
    substrate_insight: str | None,
    activity_payload: list[dict[str, Any]],
    learnings_payload: list[dict[str, Any]],
) -> dict[str, Any]:
    crow = rows.commitment
    owner_row = rows.owner
    owner_id_str = str(owner_row["id"]) if owner_row else None
    owner_label = owner_row["display_name"] if owner_row else None

    return {
        "id": str(crow["id"]),
        "label": crow["title"],
        "owner": owner_id_str,
        "owner_display": owner_label,
        "due_date": (
            crow["due_date"].date().isoformat()
            if crow["due_date"] is not None else None
        ),
        "status": _build_status_label(crow["state"]),
        "priority": (
            "high" if (crow["priority"] or 5) <= 3
            else "low" if (crow["priority"] or 5) >= 8
            else "standard"
        ),
        "customer": customer_id_str,
        "customer_label": customer_label,
        "edges": {
            "contributes_to": [str(g["id"]) for g in rows.goals],
            "constrained_by": [str(d["id"]) for d in rows.decisions],
            "consumes": [r["id"] for r in resources_payload],
            "contributors": [str(c["id"]) for c in rows.contributors],
        },
        # Per-commit slice of every consumed resource (label, unit,
        # deployed_quantity in the resource's native unit). Lets the
        # commitment focus view show "Engineering pod · 0.4 FTE"
        # without a second roundtrip to fetch resource metadata.
        "consumed_resources": resources_payload,
        "substrate_insight": substrate_insight,
        "activity": activity_payload,
        "learnings": learnings_payload,
    }


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {}
    if not isinstance(value, dict):
        return {}
    return value


async def fetch_artifact(
    kind: str, aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    """Dispatch to per-kind builder. Each builder does its own queries
    and returns the assembled drawer payload."""
    builders = {
        "actor": _build_actor_drawer,
        "commitment": _build_commitment_drawer,
        "goal": _build_goal_drawer,
        "decision": _build_decision_drawer,
        "resource": _build_resource_drawer,
        "observation": _build_observation_drawer,
        "model": _build_model_drawer,
    }
    builder = builders.get(kind)
    if builder is None:
        return None
    return await builder(aid, tenant_id, conn)


# ----- actor ---------------------------------------------------------


async def _build_actor_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, display_name, type, email, created_at, last_seen_at "
        "FROM actors WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    owns = await conn.fetch(
        "SELECT id, title, state, last_state_change_at FROM commitments "
        "WHERE tenant_id = $1 AND owner_id = $2 AND terminal_at IS NULL "
        "ORDER BY last_state_change_at DESC LIMIT 5",
        tenant_id, aid,
    )
    owns_count = await conn.fetchval(
        "SELECT count(*) FROM commitments "
        "WHERE tenant_id = $1 AND owner_id = $2 AND terminal_at IS NULL",
        tenant_id, aid,
    ) or 0

    recent = await conn.fetch(
        "SELECT id, source_channel, occurred_at, content_text "
        "FROM observations WHERE tenant_id = $1 AND actor_id = $2 "
        "ORDER BY occurred_at DESC LIMIT 5",
        tenant_id, aid,
    )

    last_seen = row["last_seen_at"] or row["created_at"]
    summary_bits: list[str] = []
    if owns_count:
        summary_bits.append(f"owns {owns_count} active commitment{'s' if owns_count != 1 else ''}")
    if last_seen:
        summary_bits.append(f"last seen {_ago(last_seen)}")
    summary = " · ".join(summary_bits) or None

    sections: list[dict[str, Any]] = [
        {
            "kind": "fields",
            "title": "At a glance",
            "rows": [
                {"label": "Type", "value": row["type"] or "—"},
                {"label": "Email", "value": row["email"] or "—"},
                {"label": "Joined", "value": _ago(row["created_at"])},
                {"label": "Last seen", "value": _ago(row["last_seen_at"])},
            ],
        }
    ]
    if owns:
        sections.append({
            "kind": "links",
            "title": f"Owns ({owns_count})",
            "items": [
                {
                    "type": "commitment", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": f"{c['state']} · updated {_ago(c['last_state_change_at'])}",
                }
                for c in owns
            ],
        })
    if recent:
        sections.append({
            "kind": "links",
            "title": "Recent activity",
            "items": [
                {
                    "type": "observation", "id": str(o["id"]),
                    "primary": _trim(o["content_text"], 120),
                    "secondary": f"{o['source_channel'] or 'signal'} · {_ago(o['occurred_at'])}",
                }
                for o in recent
            ],
        })
    return {
        "type": "actor",
        "id": str(row["id"]),
        "title": row["display_name"],
        "subtitle": f"actor · {row['type'] or 'unknown'}",
        "summary": summary,
        "sections": sections,
    }


# ----- commitment ----------------------------------------------------


async def _build_commitment_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT c.id, c.title, c.state, c.description, c.created_at, "
        "c.last_state_change_at, c.terminal_at, c.due_date, "
        "c.owner_id, a.display_name AS owner_name "
        "FROM commitments c LEFT JOIN actors a ON a.id = c.owner_id "
        "WHERE c.id = $1 AND c.tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    contributors = await conn.fetch(
        "SELECT cc.actor_id, a.display_name, a.type "
        "FROM commitment_contributors cc "
        "JOIN actors a ON a.id = cc.actor_id "
        "WHERE cc.commitment_id = $1 AND a.tenant_id = $2 "
        "ORDER BY a.display_name LIMIT 10",
        aid, tenant_id,
    )

    # Recent state-change observations referencing this commitment
    recent_obs = await conn.fetch(
        """
        SELECT id, source_channel, occurred_at, content_text
        FROM observations
        WHERE tenant_id = $1
          AND entities_mentioned @> jsonb_build_array(
              jsonb_build_object('type','commitment','id',$2::text)
          )
        ORDER BY occurred_at DESC LIMIT 5
        """,
        tenant_id, str(aid),
    )

    # Models that reference this commitment via scope_entities
    related_models = await conn.fetch(
        """
        SELECT id, "natural", confidence, proposition_kind
        FROM models
        WHERE tenant_id = $1 AND status = 'active'
          AND scope_entities @> jsonb_build_array(
              jsonb_build_object('type','commitment','id',$2::text)
          )
        ORDER BY confidence DESC LIMIT 5
        """,
        tenant_id, str(aid),
    )

    state = row["state"] or "unknown"
    days_in_state = 0
    if row["last_state_change_at"]:
        days_in_state = max(
            0,
            int((datetime.now(timezone.utc) - row["last_state_change_at"]).total_seconds() // 86400),
        )
    summary_bits = [f"in <strong>{state}</strong> for {days_in_state}d"]
    if row["owner_name"]:
        summary_bits.append(f"owned by {row['owner_name']}")
    if row["due_date"]:
        summary_bits.append(f"due {_iso(row['due_date'])[:10] if _iso(row['due_date']) else ''}")

    sections: list[dict[str, Any]] = []

    fields_rows: list[dict[str, str]] = [
        {"label": "State", "value": state},
        {"label": "Owner", "value": row["owner_name"] or "—"},
        {"label": "Created", "value": _ago(row["created_at"])},
        {"label": "Last move", "value": _ago(row["last_state_change_at"])},
    ]
    if row["due_date"]:
        fields_rows.append({"label": "Due", "value": _iso(row["due_date"]) or "—"})
    sections.append({"kind": "fields", "title": "At a glance", "rows": fields_rows})

    if row["description"]:
        sections.append({
            "kind": "narrative",
            "title": "Acceptance",
            "body": row["description"],
        })

    if row["owner_id"]:
        # Show owner as a single link so the user can drill into them
        owner_items: list[dict[str, Any]] = [{
            "type": "actor", "id": str(row["owner_id"]),
            "primary": row["owner_name"] or "Owner",
            "secondary": "owner",
        }]
        for c in contributors:
            if c["actor_id"] == row["owner_id"]:
                continue
            owner_items.append({
                "type": "actor", "id": str(c["actor_id"]),
                "primary": c["display_name"], "secondary": "contributor",
            })
        sections.append({
            "kind": "links",
            "title": f"People ({len(owner_items)})",
            "items": owner_items,
        })

    if related_models:
        sections.append({
            "kind": "links",
            "title": "Why it exists",
            "items": [
                {
                    "type": "model", "id": str(m["id"]),
                    "primary": _trim(m["natural"], 140),
                    "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                    "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
                }
                for m in related_models
            ],
        })

    if recent_obs:
        sections.append({
            "kind": "links",
            "title": f"Recent mentions ({len(recent_obs)})",
            "items": [
                {
                    "type": "observation", "id": str(o["id"]),
                    "primary": _trim(o["content_text"], 120),
                    "secondary": f"{o['source_channel'] or 'signal'} · {_ago(o['occurred_at'])}",
                }
                for o in recent_obs
            ],
        })

    return {
        "type": "commitment",
        "id": str(row["id"]),
        "title": row["title"],
        "subtitle": f"commitment · {state}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- goal ----------------------------------------------------------


async def _build_goal_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, title, state, description, altitude, target_date, "
        "parent_goal_id, cached_health, "
        "created_at, last_state_change_at "
        "FROM goals WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    parent_row = None
    if row["parent_goal_id"]:
        parent_row = await conn.fetchrow(
            "SELECT id, title, state FROM goals WHERE id = $1 AND tenant_id = $2",
            row["parent_goal_id"], tenant_id,
        )
    children = await conn.fetch(
        "SELECT id, title, state, cached_health FROM goals "
        "WHERE tenant_id = $1 AND parent_goal_id = $2 AND archived_at IS NULL "
        "ORDER BY created_at LIMIT 8",
        tenant_id, aid,
    )
    contrib = await conn.fetch(
        """
        SELECT c.id, c.title, c.state, c.last_state_change_at
        FROM commitments c
        JOIN contributes_to ct ON ct.commitment_id = c.id
        WHERE ct.goal_id = $1 AND c.tenant_id = $2 AND c.terminal_at IS NULL
        ORDER BY c.last_state_change_at DESC LIMIT 8
        """,
        aid, tenant_id,
    )

    summary_bits: list[str] = []
    if row["altitude"]:
        summary_bits.append(row["altitude"])
    if row["cached_health"]:
        summary_bits.append(row["cached_health"])
    if children:
        summary_bits.append(f"{len(children)} sub-goal{'s' if len(children) != 1 else ''}")
    if contrib:
        summary_bits.append(f"{len(contrib)} contributing commitment{'s' if len(contrib) != 1 else ''}")

    fields_rows = [
        {"label": "State", "value": row["state"] or "—"},
        {"label": "Altitude", "value": row["altitude"] or "—"},
        {"label": "Health", "value": row["cached_health"] or "—"},
    ]
    if row["target_date"]:
        fields_rows.append({"label": "Target", "value": _iso(row["target_date"]) or "—"})

    sections: list[dict[str, Any]] = [
        {"kind": "fields", "title": "At a glance", "rows": fields_rows},
    ]
    if row["description"]:
        sections.append({"kind": "narrative", "title": "Description", "body": row["description"]})
    if parent_row:
        sections.append({
            "kind": "links",
            "title": "Parent goal",
            "items": [{
                "type": "goal", "id": str(parent_row["id"]),
                "primary": parent_row["title"], "secondary": parent_row["state"] or "",
            }],
        })
    if children:
        sections.append({
            "kind": "links",
            "title": f"Sub-goals ({len(children)})",
            "items": [
                {
                    "type": "goal", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": c["state"] or "",
                    "meta": c["cached_health"] or None,
                }
                for c in children
            ],
        })
    if contrib:
        sections.append({
            "kind": "links",
            "title": f"Contributing commitments ({len(contrib)})",
            "items": [
                {
                    "type": "commitment", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": f"{c['state']} · {_ago(c['last_state_change_at'])}",
                }
                for c in contrib
            ],
        })
    return {
        "type": "goal",
        "id": str(row["id"]),
        "title": row["title"],
        "subtitle": f"goal · {row['state'] or 'unknown'}",
        "summary": " · ".join(summary_bits) or None,
        "sections": sections,
    }


# ----- decision -------------------------------------------------------


async def _build_decision_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, title, state, decision_text, rationale, "
        "created_at, last_state_change_at "
        "FROM decisions WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    constrained = await conn.fetch(
        """
        SELECT c.id, c.title, c.state, c.last_state_change_at
        FROM commitments c
        JOIN constrained_by cb ON cb.commitment_id = c.id
        WHERE cb.decision_id = $1 AND c.tenant_id = $2 AND c.terminal_at IS NULL
        ORDER BY c.last_state_change_at DESC LIMIT 8
        """,
        aid, tenant_id,
    )

    related_models = await conn.fetch(
        """
        SELECT id, "natural", confidence, proposition_kind
        FROM models
        WHERE tenant_id = $1 AND status = 'active'
          AND scope_entities @> jsonb_build_array(
              jsonb_build_object('type','decision','id',$2::text)
          )
        ORDER BY confidence DESC LIMIT 5
        """,
        tenant_id, str(aid),
    )

    days_since_change = (
        max(0, int((datetime.now(timezone.utc) - row["last_state_change_at"]).total_seconds() // 86400))
        if row["last_state_change_at"] else None
    )
    summary_bits = [f"<strong>{row['state'] or 'drafted'}</strong>"]
    if days_since_change is not None:
        summary_bits.append(f"unchanged for {days_since_change}d")
    if constrained:
        summary_bits.append(f"constrains {len(constrained)} commitment{'s' if len(constrained) != 1 else ''}")

    sections: list[dict[str, Any]] = [
        {
            "kind": "fields",
            "title": "At a glance",
            "rows": [
                {"label": "State", "value": row["state"] or "—"},
                {"label": "Created", "value": _ago(row["created_at"])},
                {"label": "Last move", "value": _ago(row["last_state_change_at"])},
            ],
        }
    ]
    if row["decision_text"]:
        sections.append({"kind": "narrative", "title": "Decision", "body": row["decision_text"]})
    if row["rationale"]:
        sections.append({"kind": "narrative", "title": "Rationale", "body": row["rationale"]})
    if constrained:
        sections.append({
            "kind": "links",
            "title": f"Constrains ({len(constrained)})",
            "items": [
                {
                    "type": "commitment", "id": str(c["id"]),
                    "primary": c["title"],
                    "secondary": f"{c['state']} · {_ago(c['last_state_change_at'])}",
                }
                for c in constrained
            ],
        })
    if related_models:
        sections.append({
            "kind": "links",
            "title": "Reasoning that cites this",
            "items": [
                {
                    "type": "model", "id": str(m["id"]),
                    "primary": _trim(m["natural"], 140),
                    "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                    "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
                }
                for m in related_models
            ],
        })
    return {
        "type": "decision",
        "id": str(row["id"]),
        "title": row["title"],
        "subtitle": f"decision · {row['state'] or 'unknown'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- resource -------------------------------------------------------


async def _build_resource_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, kind, identity, description, current_value, "
        "utilization_state, controllability, metadata, "
        "created_at, last_updated_at "
        "FROM resources WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None
    cv = row["current_value"]
    if isinstance(cv, str):
        try:
            cv = json.loads(cv)
        except json.JSONDecodeError:
            cv = None
    if not isinstance(cv, dict):
        cv = {}
    md = row["metadata"]
    if isinstance(md, str):
        try:
            md = json.loads(md)
        except json.JSONDecodeError:
            md = {}
    if not isinstance(md, dict):
        md = {}

    is_capacity_kind = row["kind"] in ("human", "financial", "technical", "time")
    label = cv.get("label") or md.get("label") or row["identity"] or "Resource"
    capacity = cv.get("capacity")
    unit = cv.get("unit") or ""
    legacy_value = cv.get("value")  # customer rows etc.

    # Aggregate deployed quantity if this is a capacity resource.
    total_deployed = 0.0
    deployments_count = 0
    util_pct = 0.0
    if is_capacity_kind:
        agg = await conn.fetchrow(
            "SELECT COALESCE(SUM((deployed_quantity->>'value')::float), 0) AS total, "
            "       COUNT(*) AS n "
            "FROM resource_deployments rd "
            "JOIN commitments c ON c.id = rd.commitment_id "
            "WHERE rd.resource_id = $1 "
            "  AND rd.released_at IS NULL "
            "  AND c.tenant_id = $2 "
            "  AND c.terminal_at IS NULL",
            aid, tenant_id,
        )
        total_deployed = float(agg["total"] or 0.0)
        deployments_count = int(agg["n"] or 0)
        if isinstance(capacity, (int, float)) and capacity > 0:
            util_pct = total_deployed / float(capacity) * 100.0

    if is_capacity_kind and isinstance(capacity, (int, float)):
        capacity_str = f"{_fmt_quantity(capacity, unit)}"
        deployed_str = f"{_fmt_quantity(total_deployed, unit)}"
        util_str = f"{util_pct:.0f}% utilized"
    elif legacy_value is not None:
        capacity_str = f"{legacy_value} {unit}".strip()
        deployed_str = "—"
        util_str = "—"
    else:
        capacity_str = "—"
        deployed_str = "—"
        util_str = "—"

    summary_bits: list[str] = [row["kind"] or "resource"]
    if is_capacity_kind:
        summary_bits.append(util_str)
    elif row["utilization_state"]:
        summary_bits.append(row["utilization_state"])

    fields_rows: list[dict[str, Any]] = [
        {"label": "Kind", "value": row["kind"] or "—"},
    ]
    if is_capacity_kind:
        fields_rows.extend([
            {"label": "Capacity", "value": capacity_str},
            {"label": "Deployed", "value": deployed_str},
            {"label": "Utilization", "value": util_str},
            {"label": "Active commitments", "value": str(deployments_count)},
        ])
    else:
        fields_rows.extend([
            {"label": "Current", "value": capacity_str},
            {"label": "Utilization", "value": row["utilization_state"] or "—"},
        ])
    fields_rows.append({"label": "Control", "value": row["controllability"] or "—"})
    fields_rows.append({"label": "Updated", "value": _ago(row["last_updated_at"])})

    sections: list[dict[str, Any]] = [
        {"kind": "fields", "title": "At a glance", "rows": fields_rows},
    ]
    if row["description"]:
        sections.append({
            "kind": "narrative",
            "title": "Description",
            "body": row["description"],
        })

    if is_capacity_kind:
        consumers = await conn.fetch(
            "SELECT c.id, c.title, c.state, "
            "       (rd.deployed_quantity->>'value')::float AS qty, "
            "       a.display_name AS owner_name "
            "FROM resource_deployments rd "
            "JOIN commitments c ON c.id = rd.commitment_id "
            "LEFT JOIN actors a ON a.id = c.owner_id "
            "WHERE rd.resource_id = $1 "
            "  AND rd.released_at IS NULL "
            "  AND c.tenant_id = $2 "
            "  AND c.terminal_at IS NULL "
            "ORDER BY (rd.deployed_quantity->>'value')::float DESC NULLS LAST "
            "LIMIT 8",
            aid, tenant_id,
        )
        items: list[dict[str, Any]] = []
        for cr in consumers:
            qty = float(cr["qty"] or 0.0)
            secondary = cr["owner_name"] or ""
            meta_str = (
                f"{_fmt_quantity(qty, unit)}" if unit else f"{qty:.2g}"
            )
            if cr["state"]:
                meta_str = f"{meta_str} · {cr['state']}"
            items.append({
                "type": "commitment",
                "id": str(cr["id"]),
                "primary": cr["title"] or "(untitled)",
                "secondary": secondary,
                "meta": meta_str,
            })
        sections.append({
            "kind": "links",
            "title": "Top consumers",
            "items": items,
            "empty_text": "No active commitments are drawing on this resource.",
        })

    return {
        "type": "resource",
        "id": str(row["id"]),
        "title": label,
        "subtitle": f"resource · {row['kind'] or 'unknown'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


def _fmt_quantity(value: float, unit: str) -> str:
    """Pretty-format a quantity in its unit. Cash gets dollar formatting,
    FTE gets one decimal, engineer-weeks/credits/GPU-hours get integer
    rounding."""
    u = (unit or "").lower()
    if "usd" in u:
        if value >= 1_000_000:
            return f"${value / 1_000_000:.1f}M"
        if value >= 1_000:
            return f"${value / 1_000:.0f}k"
        return f"${value:.0f}"
    if "fte" in u:
        return f"{value:.1f} FTE"
    if not unit:
        return f"{value:.2f}"
    return f"{value:.0f} {unit}"


# ----- observation ----------------------------------------------------


async def _build_observation_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT id, kind, source_channel, occurred_at, content_text, "
        "actor_id, trust_tier, entities_mentioned, source_actor_ref "
        "FROM observations WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    actor_link = None
    if row["actor_id"]:
        a = await conn.fetchrow(
            "SELECT id, display_name, type FROM actors "
            "WHERE id = $1 AND tenant_id = $2",
            row["actor_id"], tenant_id,
        )
        if a:
            actor_link = {
                "type": "actor", "id": str(a["id"]),
                "primary": a["display_name"],
                "secondary": a["type"] or "actor",
            }

    # Models that count this observation among their supporting events.
    using_models = await conn.fetch(
        """
        SELECT id, "natural", confidence, proposition_kind
        FROM models
        WHERE tenant_id = $1 AND status = 'active'
          AND $2 = ANY (supporting_event_ids)
        ORDER BY confidence DESC LIMIT 5
        """,
        tenant_id, aid,
    )

    # entities_mentioned is a jsonb array of {type,id}; resolve ids → titles
    mentioned: list[dict[str, Any]] = []
    em = row["entities_mentioned"]
    if isinstance(em, str):
        try:
            em = json.loads(em)
        except json.JSONDecodeError:
            em = []
    if isinstance(em, list):
        for ent in em[:8]:
            if not isinstance(ent, dict):
                continue
            etype = ent.get("type")
            eid = ent.get("id")
            if not etype or not eid:
                continue
            try:
                e_uuid = UUID(str(eid))
            except (ValueError, TypeError):
                continue
            title = await _resolve_entity_title(etype, e_uuid, tenant_id, conn)
            if title:
                mentioned.append({
                    "type": etype, "id": str(e_uuid),
                    "primary": title, "secondary": etype,
                })

    summary_bits = [
        row["source_channel"] or row["kind"] or "signal",
        _ago(row["occurred_at"]),
    ]
    if row["trust_tier"]:
        summary_bits.append(f"trust: {row['trust_tier']}")

    sections: list[dict[str, Any]] = [
        {
            "kind": "fields",
            "title": "At a glance",
            "rows": [
                {"label": "Channel", "value": row["source_channel"] or "—"},
                {"label": "Kind", "value": row["kind"] or "—"},
                {"label": "Trust", "value": row["trust_tier"] or "—"},
                {"label": "Source", "value": row["source_actor_ref"] or "—"},
                {"label": "Occurred", "value": _ago(row["occurred_at"])},
            ],
        },
        {
            "kind": "narrative",
            "title": "Content",
            "body": row["content_text"] or "",
        },
    ]
    if actor_link:
        sections.append({
            "kind": "links", "title": "From", "items": [actor_link],
        })
    if mentioned:
        sections.append({
            "kind": "links",
            "title": f"Mentions ({len(mentioned)})",
            "items": mentioned,
        })
    if using_models:
        sections.append({
            "kind": "links",
            "title": f"Used in {len(using_models)} model{'s' if len(using_models) != 1 else ''}",
            "items": [
                {
                    "type": "model", "id": str(m["id"]),
                    "primary": _trim(m["natural"], 140),
                    "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                    "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
                }
                for m in using_models
            ],
        })
    return {
        "type": "observation",
        "id": str(row["id"]),
        "title": _trim(row["content_text"], 140),
        "subtitle": f"evidence · {row['source_channel'] or row['kind'] or 'signal'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- model ----------------------------------------------------------


async def _build_model_drawer(
    aid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        'SELECT id, "natural", proposition_kind, confidence, status, '
        "supporting_event_ids, supporting_model_ids, "
        "scope_actors, scope_entities, falsifier, "
        "confirmed_count, contested_count, "
        "created_at, resolved_at "
        "FROM models WHERE id = $1 AND tenant_id = $2",
        aid, tenant_id,
    )
    if row is None:
        return None

    pk = (row["proposition_kind"] or "model").replace("_", " ")
    conf = float(row["confidence"] or 0.0)
    conf_pct = int(round(conf * 100))

    # Top supporting observations
    sup_obs: list[dict[str, Any]] = []
    if row["supporting_event_ids"]:
        rows_obs = await conn.fetch(
            "SELECT id, source_channel, occurred_at, content_text "
            "FROM observations WHERE id = ANY($1::uuid[]) AND tenant_id = $2 "
            "ORDER BY occurred_at DESC LIMIT 5",
            list(row["supporting_event_ids"]), tenant_id,
        )
        for o in rows_obs:
            sup_obs.append({
                "type": "observation", "id": str(o["id"]),
                "primary": _trim(o["content_text"], 120),
                "secondary": f"{o['source_channel'] or 'signal'} · {_ago(o['occurred_at'])}",
            })

    # Top supporting models
    sup_models: list[dict[str, Any]] = []
    if row["supporting_model_ids"]:
        rows_m = await conn.fetch(
            'SELECT id, "natural", confidence, proposition_kind '
            "FROM models WHERE id = ANY($1::uuid[]) AND tenant_id = $2 "
            "ORDER BY confidence DESC LIMIT 5",
            list(row["supporting_model_ids"]), tenant_id,
        )
        for m in rows_m:
            sup_models.append({
                "type": "model", "id": str(m["id"]),
                "primary": _trim(m["natural"], 140),
                "secondary": (m["proposition_kind"] or "model").replace("_", " "),
                "meta": f"{int(round(float(m['confidence'] or 0.0) * 100))}%",
            })

    # Falsifier as a narrative
    falsifier_body: str | None = None
    fals = row["falsifier"]
    if isinstance(fals, str):
        try:
            fals = json.loads(fals)
        except json.JSONDecodeError:
            fals = None
    if isinstance(fals, dict):
        if fals.get("text"):
            falsifier_body = str(fals["text"])
        elif fals.get("description"):
            falsifier_body = str(fals["description"])

    # Scope actors → links
    actor_links: list[dict[str, Any]] = []
    if row["scope_actors"]:
        rows_a = await conn.fetch(
            "SELECT id, display_name, type FROM actors "
            "WHERE id = ANY($1::uuid[]) AND tenant_id = $2 LIMIT 6",
            list(row["scope_actors"]), tenant_id,
        )
        for a in rows_a:
            actor_links.append({
                "type": "actor", "id": str(a["id"]),
                "primary": a["display_name"],
                "secondary": a["type"] or "actor",
            })

    # Scope entities → links
    entity_links: list[dict[str, Any]] = []
    se = row["scope_entities"]
    if isinstance(se, str):
        try:
            se = json.loads(se)
        except json.JSONDecodeError:
            se = []
    if isinstance(se, list):
        for ent in se[:6]:
            if not isinstance(ent, dict):
                continue
            etype = ent.get("type")
            eid = ent.get("id")
            if not etype or not eid:
                continue
            try:
                e_uuid = UUID(str(eid))
            except (ValueError, TypeError):
                continue
            title = await _resolve_entity_title(etype, e_uuid, tenant_id, conn)
            if title:
                entity_links.append({
                    "type": etype, "id": str(e_uuid),
                    "primary": title, "secondary": etype,
                })

    confirmed = int(row["confirmed_count"] or 0)
    contested = int(row["contested_count"] or 0)
    summary_bits = [
        f"{conf_pct}% confident",
        pk,
        f"{len(sup_obs)} signal{'s' if len(sup_obs) != 1 else ''}",
    ]
    if confirmed or contested:
        summary_bits.append(f"{confirmed}↑ {contested}↓")

    fields_rows = [
        {"label": "Kind", "value": pk},
        {"label": "Confidence", "value": f"{conf_pct}%"},
        {"label": "Status", "value": row["status"] or "—"},
        {"label": "Confirmed", "value": str(confirmed)},
        {"label": "Contested", "value": str(contested)},
        {"label": "Created", "value": _ago(row["created_at"])},
    ]
    if row["resolved_at"]:
        fields_rows.append({"label": "Resolved", "value": _ago(row["resolved_at"])})

    sections: list[dict[str, Any]] = [
        {"kind": "fields", "title": "At a glance", "rows": fields_rows},
        {"kind": "narrative", "title": "What it claims", "body": row["natural"] or ""},
    ]
    if falsifier_body:
        sections.append({
            "kind": "narrative", "title": "What would falsify it",
            "body": falsifier_body,
        })
    if entity_links:
        sections.append({
            "kind": "links",
            "title": f"About ({len(entity_links)})",
            "items": entity_links,
        })
    if actor_links:
        sections.append({
            "kind": "links",
            "title": f"Subjects ({len(actor_links)})",
            "items": actor_links,
        })
    if sup_obs:
        sections.append({
            "kind": "links",
            "title": f"Built from ({len(sup_obs)} signal{'s' if len(sup_obs) != 1 else ''})",
            "items": sup_obs,
        })
    if sup_models:
        sections.append({
            "kind": "links",
            "title": f"Built on ({len(sup_models)} other model{'s' if len(sup_models) != 1 else ''})",
            "items": sup_models,
        })

    return {
        "type": "model",
        "id": str(row["id"]),
        "title": row["natural"] or "(no natural rendering)",
        "subtitle": f"{pk} · {row['status'] or 'unknown'}",
        "summary": " · ".join(summary_bits),
        "sections": sections,
    }


# ----- entity title resolver -----------------------------------------


_TITLE_SQL_BY_TYPE: dict[str, str] = {
    "actor":      "SELECT display_name AS title FROM actors WHERE id = $1 AND tenant_id = $2",
    "commitment": "SELECT title FROM commitments WHERE id = $1 AND tenant_id = $2",
    "goal":       "SELECT title FROM goals WHERE id = $1 AND tenant_id = $2",
    "decision":   "SELECT title FROM decisions WHERE id = $1 AND tenant_id = $2",
    "resource":   "SELECT identity AS title FROM resources WHERE id = $1 AND tenant_id = $2",
    "observation":"SELECT left(content_text, 100) AS title FROM observations WHERE id = $1 AND tenant_id = $2",
    "model":      'SELECT left("natural", 100) AS title FROM models WHERE id = $1 AND tenant_id = $2',
}


async def _resolve_entity_title(
    kind: str, eid: UUID, tenant_id: UUID, conn: asyncpg.Connection,
) -> str | None:
    sql = _TITLE_SQL_BY_TYPE.get(kind)
    if sql is None:
        return None
    try:
        row = await conn.fetchrow(sql, eid, tenant_id)
    except Exception:
        return None
    return (row["title"] if row else None) or None
