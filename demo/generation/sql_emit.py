"""Emit a SQL snapshot file from a validated GeneratedBundle.

Targets `services/demo/snapshot.py:load_snapshot` — uses the placeholder
tenant id `00000000-0000-0000-0000-000000000000`, which the loader
substitutes for the real tenant_id at load time. Inserts are emitted in
dependency order with `ON CONFLICT (id) DO NOTHING` for idempotency.

The output is plain SQL; if `zstandard` is available and the caller
asks for compression, the file is written as `.sql.zst`.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from demo.generation.schemas import (
    GeneratedBundle,
    GeneratedCommitment,
    GeneratedDecision,
    GeneratedGoal,
    GeneratedModel,
    GeneratedRecommendation,
)


PLACEHOLDER_TENANT_ID = "00000000-0000-0000-0000-000000000000"


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


def emit_sql(bundle: GeneratedBundle) -> str:
    """Render the entity bundle into one SQL string.

    The emitted SQL has no BEGIN/COMMIT — the loader (services/demo/
    snapshot.py:load_snapshot) runs inside its own transaction, and
    nesting BEGIN/COMMIT silently ends the outer transaction in
    asyncpg's multi-statement execute path."""
    parts: list[str] = []
    parts.append("-- Auto-generated demo snapshot. Do not edit by hand.")
    parts.append(f"-- company_id={bundle.company_id}")
    parts.append(f"-- ceo_actor_id={bundle.ceo_actor_id}")
    parts.append("")

    # Ensure observation partitions exist for the signal date range.
    # Bundle authors can place signals up to 9 months in the past per
    # the DEMO-BUILD-PLAN spec; foundation only creates current-quarter
    # forward partitions, so we backfill on demand.
    parts.append(_ensure_observation_partitions_sql(bundle))
    parts.append("")

    # 1) actors
    parts.append("-- actors")
    for a in bundle.actors:
        parts.append(_actor_insert(a, bundle.ceo_actor_id))

    # 2) seed observation — every Model needs born_from_event_id; every
    # Resource is created_by_event_id; goals/decisions need
    # created_by_event_id.
    seed_obs_id = _seed_obs_id(bundle.company_id)
    seed_actor_id = bundle.ceo_actor_id
    parts.append("")
    parts.append("-- seed observation")
    parts.append(_seed_observation_insert(seed_obs_id, seed_actor_id, bundle.company_id))

    # 3) resources (customers)
    parts.append("")
    parts.append("-- resources (customers)")
    for c in bundle.customers:
        parts.append(_resource_insert(c, seed_obs_id))

    # 4) observations (signals)
    parts.append("")
    parts.append("-- observations (signals)")
    for s in bundle.signals:
        parts.append(_signal_insert(s))

    # 5) goals
    parts.append("")
    parts.append("-- goals")
    for g in bundle.goals:
        parts.append(_goal_insert(g, seed_obs_id))

    # 6) decisions
    parts.append("")
    parts.append("-- decisions")
    for d in bundle.decisions:
        parts.append(_decision_insert(d, seed_obs_id))

    # 7) commitments + edges
    parts.append("")
    parts.append("-- commitments")
    for c in bundle.commitments:
        parts.append(_commitment_insert(c, seed_obs_id))
    for c in bundle.commitments:
        for actor_id in c.contributors:
            parts.append(_contributor_insert(c.id, actor_id))
        if c.contributes_to_goal_id:
            parts.append(_contributes_to_insert(c.id, c.contributes_to_goal_id))
        for dep in c.depends_on:
            parts.append(_depends_on_insert(c.id, dep))
        for did in c.constrained_by_decision_ids:
            parts.append(_constrained_by_insert(c.id, did))
        if c.served_by_customer_id:
            parts.append(_customer_commitment_insert(c.served_by_customer_id, c.id))

    # 8) models — diverse epistemic substrate (state / relation /
    # prediction / pattern / capability_assessment / hypothesis /
    # concern / market_assessment / environmental_trend) plus
    # recommendations. Recommendations are emitted last so their
    # supporting_model_ids can reference earlier models.
    parts.append("")
    parts.append("-- models (epistemic substrate)")
    for m in bundle.models:
        parts.append(_model_insert(m, seed_obs_id))
    parts.append("")
    parts.append("-- models (recommendations)")
    for r in bundle.recommendations:
        parts.append(_recommendation_insert(r, seed_obs_id))

    parts.append("")
    return "\n".join(parts) + "\n"


def write_sql(
    bundle: GeneratedBundle,
    out_path: Path,
    *,
    compress: bool = False,
) -> Path:
    """Write the SQL to disk; optionally zstd-compress.

    Returns the actual path written (which may have .zst appended).
    """
    sql = emit_sql(bundle)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if compress:
        try:
            import zstandard as zstd
        except ImportError as e:
            raise RuntimeError(
                "compress=True requires `pip install zstandard`"
            ) from e
        comp = zstd.ZstdCompressor(level=10)
        target = out_path.with_suffix(out_path.suffix + ".zst")
        target.write_bytes(comp.compress(sql.encode("utf-8")))
        return target
    out_path.write_text(sql, encoding="utf-8")
    return out_path


# ---------------------------------------------------------------------
# Per-entity SQL builders. All identifiers are quoted via _q to avoid
# escape-injection from generated content.
# ---------------------------------------------------------------------


def _ensure_observation_partitions_sql(bundle: GeneratedBundle) -> str:
    """Emit a DO-block that creates monthly partitions of `observations`
    covering the earliest signal in the bundle through one month past
    the seed observation. Idempotent (CREATE TABLE IF NOT EXISTS)."""
    if not bundle.signals:
        return "-- (no signal-date partitions needed)"
    earliest = None
    for s in bundle.signals:
        try:
            dt = datetime.fromisoformat(s.occurred_at.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if earliest is None or dt < earliest:
            earliest = dt
    if earliest is None:
        return "-- (no parseable signal dates)"
    # Walk month-by-month from `earliest` to (now + 1 month) and emit
    # CREATE TABLE IF NOT EXISTS PARTITION OF for each. Same shape as
    # 0001_foundation's bootstrap loop.
    earliest_month = datetime(earliest.year, earliest.month, 1, tzinfo=timezone.utc)
    end_target = datetime.now(timezone.utc) + timedelta(days=31)
    end_month = datetime(end_target.year, end_target.month, 1, tzinfo=timezone.utc)
    return (
        "DO $$\n"
        "DECLARE\n"
        "  start_date DATE;\n"
        "  end_date DATE;\n"
        "  partition_name TEXT;\n"
        "BEGIN\n"
        f"  start_date := DATE '{earliest_month.date().isoformat()}';\n"
        f"  WHILE start_date <= DATE '{end_month.date().isoformat()}' LOOP\n"
        "    end_date := (start_date + INTERVAL '1 month')::DATE;\n"
        "    partition_name := format('observations_%s', TO_CHAR(start_date, 'YYYY_MM'));\n"
        "    EXECUTE format(\n"
        "      'CREATE TABLE IF NOT EXISTS %I PARTITION OF observations "
        "FOR VALUES FROM (%L) TO (%L)',\n"
        "      partition_name, start_date, end_date\n"
        "    );\n"
        "    partition_name := format('resource_transactions_%s', TO_CHAR(start_date, 'YYYY_MM'));\n"
        "    EXECUTE format(\n"
        "      'CREATE TABLE IF NOT EXISTS %I PARTITION OF resource_transactions "
        "FOR VALUES FROM (%L) TO (%L)',\n"
        "      partition_name, start_date, end_date\n"
        "    );\n"
        "    start_date := end_date;\n"
        "  END LOOP;\n"
        "END $$;"
    )


def _q(value) -> str:
    """Quote a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict) or isinstance(value, list):
        return "'" + json.dumps(value).replace("'", "''") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def _seed_obs_id(company_id: str) -> str:
    """Deterministic UUID-shaped id for the seed observation."""
    # uuid v8-ish; deterministic based on company_id so re-emitting
    # produces the same id and ON CONFLICT keeps it idempotent.
    h = hash(company_id) & 0xFFFFFFFF
    return f"00000000-0000-7d24-8000-{h:012x}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


_ROLE_DISPLAY_TITLE = {
    "founder": "Founder & CEO",
    "cto": "CTO",
    "vp_eng": "VP Engineering",
    "vp_sales": "VP Sales",
    "head_cs": "Head of Customer Success",
    "head_sales": "Head of Sales",
    "head_ops": "Head of Operations",
    "head_finance": "Head of Finance",
    "cfo": "CFO",
    "engineer": "Engineer",
    "data_engineer": "Data Engineer",
    "ml_engineer": "ML Engineer",
    "pm": "Product Manager",
    "designer": "Designer",
    "ae": "Account Executive",
    "se": "Sales Engineer",
    "cs_manager": "Customer Success Manager",
    "customer_success": "Customer Success",
    "sales": "Sales",
    "implementation": "Implementation Engineer",
    "marketing": "Marketing",
    "finance_ops": "Finance / Ops",
    "recruiter": "Recruiter",
    "fractional_legal": "Fractional Legal Counsel",
}


def _actor_insert(a, ceo_id: str) -> str:
    is_ceo = a.id == ceo_id
    title = _ROLE_DISPLAY_TITLE.get(
        a.role,
        a.role.replace("_", " ").title() if a.role else "Team member",
    )
    metadata = {
        "role": a.role,
        "personality_brief": a.personality_brief,
        "is_ceo": is_ceo,
        "title": title,
    }
    if is_ceo:
        metadata["title"] = "Founder & CEO"
    email = a.email or f"{a.name.split()[0].lower()}@example.com"
    return (
        "INSERT INTO actors (id, tenant_id, type, display_name, email, status, "
        "metadata, created_at, last_seen_at) VALUES ("
        f"{_q(a.id)}, {_q(PLACEHOLDER_TENANT_ID)}, 'human_internal', "
        f"{_q(a.name)}, {_q(email)}, 'active', {_q(metadata)}::jsonb, "
        f"{_q(_now())}, {_q(_now())}) ON CONFLICT (id) DO NOTHING;"
    )


def _seed_observation_insert(obs_id: str, actor_id: str, company_id: str) -> str:
    embedding = "[" + ",".join(["0"] * 768) + "]"
    return (
        "INSERT INTO observations (id, tenant_id, occurred_at, ingested_at, kind, "
        "source_channel, source_actor_ref, actor_id, content, content_text, "
        "embedding, trust_tier, external_id) VALUES ("
        f"{_q(obs_id)}, {_q(PLACEHOLDER_TENANT_ID)}, {_q(_now())}, {_q(_now())}, "
        f"'signal', 'system:demo_seed', {_q(actor_id[:12])}, {_q(actor_id)}, "
        f"{_q({'event': 'demo_seed'})}::jsonb, "
        f"{_q(f'Demo seed for {company_id}')}, "
        f"'{embedding}'::vector, 'authoritative', {_q('seed-' + obs_id)}) "
        "ON CONFLICT (id, occurred_at) DO NOTHING;"
    )


def _resource_insert(c, seed_obs_id: str) -> str:
    current_value = {"arr_usd": c.arr_usd}
    metadata = {
        "segment": c.segment,
        "current_health": c.current_health,
        "primary_contacts": c.primary_contacts,
        "source": "demo_generation",
    }
    return (
        "INSERT INTO resources (id, tenant_id, kind, identity, description, "
        "current_value, utilization_state, controllability, temporal_character, "
        "metadata, created_at, last_updated_by_event_id) VALUES ("
        f"{_q(c.id)}, {_q(PLACEHOLDER_TENANT_ID)}, 'relational', "
        f"{_q('customer:' + c.company_name.lower().replace(' ', '_'))}, "
        f"{_q(c.company_name + ' — paying customer')}, "
        f"{_q(current_value)}::jsonb, 'deployed', 'owned', 'time_limited', "
        f"{_q(metadata)}::jsonb, {_q(_now())}, {_q(seed_obs_id)}) "
        "ON CONFLICT (id) DO NOTHING;"
    )


def _signal_insert(s) -> str:
    """Per-signal observation insert.

    `ON CONFLICT (id, occurred_at)` (not the bare ON CONFLICT DO
    NOTHING) so a re-load with the same UUIDs and timestamps no-ops on
    the PK rather than tripping the UNIQUE (source_channel, external_id,
    occurred_at) constraint as well — the latter would silently drop
    rows whose external_id collides across snapshot loads. The remapped
    UUID at load time + suffixing external_id with the signal id keeps
    rows distinct between tenants.
    """
    embedding = "[" + ",".join(["0"] * 768) + "]"
    entities = [{"type": e.type, "id": e.id} for e in s.entities_mentioned]
    content = {"text": s.content_text, "channel": s.source_channel}
    # Append the signal's UUID-suffix to external_id so the unique
    # (source_channel, external_id, occurred_at) constraint can't
    # collide with another tenant that loaded the same snapshot.
    external_id_unique = f"{s.source_ref}:{s.id}"
    return (
        "INSERT INTO observations (id, tenant_id, occurred_at, ingested_at, kind, "
        "source_channel, source_actor_ref, actor_id, content, content_text, "
        "embedding, trust_tier, external_id, entities_mentioned) VALUES ("
        f"{_q(s.id)}, {_q(PLACEHOLDER_TENANT_ID)}, {_q(s.occurred_at)}, "
        f"{_q(s.occurred_at)}, 'signal', {_q(s.source_channel)}, "
        f"{_q(s.author_id[:12])}, {_q(s.author_id)}, {_q(content)}::jsonb, "
        f"{_q(s.content_text)}, '{embedding}'::vector, 'derived', "
        f"{_q(external_id_unique)}, {_q(entities)}::jsonb) "
        "ON CONFLICT (id, occurred_at) DO NOTHING;"
    )


def _goal_insert(g: GeneratedGoal, seed_obs_id: str) -> str:
    return (
        "INSERT INTO goals (id, tenant_id, title, description, state, target_date, "
        "parent_goal_id, altitude, created_by_event_id) VALUES ("
        f"{_q(g.id)}, {_q(PLACEHOLDER_TENANT_ID)}, {_q(g.title)}, "
        f"{_q(g.description)}, 'active', {_q(g.target_date)}, "
        f"{_q(g.parent_goal_id)}, {_q(g.altitude)}, {_q(seed_obs_id)}) "
        "ON CONFLICT (id) DO NOTHING;"
    )


def _decision_insert(d: GeneratedDecision, seed_obs_id: str) -> str:
    return (
        "INSERT INTO decisions (id, tenant_id, title, decision_text, rationale, "
        "state, scope, revisit_triggers, created_by_event_id) VALUES ("
        f"{_q(d.id)}, {_q(PLACEHOLDER_TENANT_ID)}, {_q(d.title)}, "
        f"{_q(d.decision_text)}, {_q(d.rationale)}, 'active', "
        f"{_q(d.scope)}::jsonb, {_q(d.revisit_triggers)}::jsonb, "
        f"{_q(seed_obs_id)}) ON CONFLICT (id) DO NOTHING;"
    )


def _commitment_insert(c: GeneratedCommitment, seed_obs_id: str) -> str:
    return (
        "INSERT INTO commitments (id, tenant_id, title, state, owner_id, "
        "due_date, created_by_event_id) VALUES ("
        f"{_q(c.id)}, {_q(PLACEHOLDER_TENANT_ID)}, {_q(c.title)}, "
        f"{_q(c.state)}, {_q(c.owner_id)}, {_q(c.due_date)}, "
        f"{_q(seed_obs_id)}) ON CONFLICT (id) DO NOTHING;"
    )


def _contributor_insert(commitment_id: str, actor_id: str) -> str:
    return (
        "INSERT INTO commitment_contributors (commitment_id, actor_id) VALUES ("
        f"{_q(commitment_id)}, {_q(actor_id)}) "
        "ON CONFLICT DO NOTHING;"
    )


def _contributes_to_insert(commitment_id: str, goal_id: str) -> str:
    return (
        "INSERT INTO contributes_to (commitment_id, goal_id) VALUES ("
        f"{_q(commitment_id)}, {_q(goal_id)}) ON CONFLICT DO NOTHING;"
    )


def _depends_on_insert(dep_cid: str, dep_on_cid: str) -> str:
    return (
        "INSERT INTO depends_on (dependent_commitment_id, dependency_commitment_id) "
        f"VALUES ({_q(dep_cid)}, {_q(dep_on_cid)}) ON CONFLICT DO NOTHING;"
    )


def _constrained_by_insert(commitment_id: str, decision_id: str) -> str:
    return (
        "INSERT INTO constrained_by (commitment_id, decision_id) VALUES ("
        f"{_q(commitment_id)}, {_q(decision_id)}) ON CONFLICT DO NOTHING;"
    )


def _customer_commitment_insert(customer_id: str, commitment_id: str) -> str:
    return (
        "INSERT INTO customer_commitments (customer_resource_id, commitment_id) "
        f"VALUES ({_q(customer_id)}, {_q(commitment_id)}) "
        "ON CONFLICT DO NOTHING;"
    )


def _model_insert(m: GeneratedModel, seed_obs_id: str) -> str:
    """Emit a single non-recommendation Model insert.

    The proposition column is JSON. We merge `m.proposition` (caller-
    supplied structured detail) with `kind` and `natural` so the row
    survives the schema-validator and the proposition_kind generated
    column resolves to the right value."""
    embedding = "[" + ",".join(["0"] * 768) + "]"
    proposition = {**m.proposition, "kind": m.kind, "natural": m.natural}
    scope_entities = m.scope_entities or []
    scope_actors_arr = (
        "ARRAY[" + ",".join(_q(a) for a in m.scope_actor_ids) + "]::uuid[]"
        if m.scope_actor_ids else "ARRAY[]::uuid[]"
    )
    falsifier_sql = (
        f"{_q(m.falsifier)}::jsonb" if m.falsifier is not None else "NULL"
    )
    evaluate_at_sql = _q(m.evaluate_at) if m.evaluate_at else "NULL"
    supp_evt_arr = (
        "ARRAY[" + ",".join(_q(s) for s in m.supporting_observation_ids) + "]::uuid[]"
        if m.supporting_observation_ids else "ARRAY[]::uuid[]"
    )
    supp_mod_arr = (
        "ARRAY[" + ",".join(_q(s) for s in m.supporting_model_ids) + "]::uuid[]"
        if m.supporting_model_ids else "ARRAY[]::uuid[]"
    )
    return (
        "INSERT INTO models (id, tenant_id, born_from_event_id, proposition, "
        "\"natural\", embedding, scope_actors, scope_entities, scope_temporal, "
        "confidence, activation, confidence_at_assertion, falsifier, "
        "supporting_event_ids, supporting_model_ids, "
        "status, created_at, evaluate_at, visible_to_subjects) VALUES ("
        f"{_q(m.id)}, {_q(PLACEHOLDER_TENANT_ID)}, {_q(seed_obs_id)}, "
        f"{_q(proposition)}::jsonb, {_q(m.natural)}, "
        f"'{embedding}'::vector, {scope_actors_arr}, "
        f"{_q(scope_entities)}::jsonb, {_q(m.scope_temporal)}::jsonb, "
        f"{m.confidence}, 1.0, {m.confidence}, {falsifier_sql}, "
        f"{supp_evt_arr}, {supp_mod_arr}, "
        f"'active', {_q(_now())}, {evaluate_at_sql}, TRUE) "
        "ON CONFLICT (id) DO NOTHING;"
    )


def _recommendation_insert(r: GeneratedRecommendation, seed_obs_id: str) -> str:
    embedding = "[" + ",".join(["0"] * 768) + "]"
    proposition = {
        "kind": "recommendation",
        "natural": r.proposition_text,
        "target_actor_id": r.target_actor_id,
        "target_act_ref": {"type": r.target_act_ref.type, "id": r.target_act_ref.id},
        "proposed_change": r.proposed_change,
        "expected_impact": r.expected_impact_usd,
        "supporting_observation_ids": r.supporting_observation_ids,
        "supporting_model_ids": r.supporting_model_ids,
    }
    supp_evt_arr = (
        "ARRAY[" + ",".join(_q(s) for s in r.supporting_observation_ids) + "]::uuid[]"
        if r.supporting_observation_ids else "ARRAY[]::uuid[]"
    )
    supp_mod_arr = (
        "ARRAY[" + ",".join(_q(s) for s in r.supporting_model_ids) + "]::uuid[]"
        if r.supporting_model_ids else "ARRAY[]::uuid[]"
    )
    return (
        "INSERT INTO models (id, tenant_id, born_from_event_id, proposition, "
        "\"natural\", embedding, scope_temporal, confidence, activation, "
        "confidence_at_assertion, supporting_event_ids, supporting_model_ids, "
        "status, created_at, visible_to_subjects) VALUES ("
        f"{_q(r.id)}, {_q(PLACEHOLDER_TENANT_ID)}, {_q(seed_obs_id)}, "
        f"{_q(proposition)}::jsonb, {_q(r.proposition_text)}, "
        f"'{embedding}'::vector, {_q({'window': 'current'})}::jsonb, "
        "0.78, 1.0, 0.78, "
        f"{supp_evt_arr}, {supp_mod_arr}, 'active', "
        f"{_q(_now())}, TRUE) ON CONFLICT (id) DO NOTHING;"
    )


__all__ = ["emit_sql", "write_sql", "PLACEHOLDER_TENANT_ID"]
