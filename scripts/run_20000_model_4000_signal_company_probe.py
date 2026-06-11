#!/usr/bin/env python3
"""Run a single-company 20k-model / 4k-signal incremental systems probe.

This runner defaults to an LLM-free hot path. A literal full Think drain for
4,000 signals can cascade into tens of thousands of LLM runs; this script
instead stresses the durable model layer that decides whether the next signal
can retrieve, reuse, and learn from what prior signals changed. Use
`--question-planning-mode codex-low` to exercise the same Codex provider
abstraction Think uses for bounded retrieval-question planning.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.llm.provider import (
    LLMConfig,
    LLMConfigError,
    LLMProvider,
    _codex_transport,
    build_provider,
    close_codex_app_server_client,
)
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from lib.shared.types import ModelCreate
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.platform.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.platform.execution.question_planning_provider import (
    question_planning_provider_metadata,
    select_question_planning_provider,
)
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.models.repo import ModelsRepo, pgvector_pool_init
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.outcome_evaluator import OutcomeEvaluator
from services.reasoning.sage.topology_optimizer import TopologyOptimizer

from run_1000_signal_model_layer_probe import (  # noqa: E402
    COMPANY_NAME,
    COMMITMENTS,
    CUSTOMERS,
    DECISIONS,
    FAMILIES,
    GOALS,
    _insert_extra_aliases,
    build_scenario,
    inject_generated_signals,
)
from run_incremental_feedback_loop_stress import (  # noqa: E402
    _attach_writer_outcome,
    _embedding,
    _learned_layer_counts,
    _percentile,
)
from tests.real_llm.infrastructure.scenario_loader import materialize  # noqa: E402


load_dotenv(REPO_ROOT / ".env", override=False)

REPORT_ROOT = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"
LOCAL_DATABASE_URL = "postgresql://company_os:company_os@localhost:5432/company_os"
LOCAL_DB_HOSTS = {"", "localhost", "127.0.0.1", "::1"}
LOCAL_COMPACTION_EXTRA_TABLES = {
    "audit_events",
    "inquiry_evidence_items",
    "inquiry_outcome_events",
    "inquiry_question_runs",
    "inquiry_sessions",
    "omitted_evidence",
    "retrieval_affordance_profiles",
    "retrieval_plans",
    "sage_reader_activations",
    "sage_reader_decision_attributions",
    "think_run_artifacts",
    "think_runs",
}


def _json_default(value: Any) -> str:
    return str(value)


def _quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _status_row_count(status: str) -> int:
    try:
        return int(str(status).rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        return 0


def _database_url_is_local(database_url: str) -> bool:
    parsed = urlparse(database_url)
    return (parsed.hostname or "") in LOCAL_DB_HOSTS


def _cleanup_enabled(args: argparse.Namespace) -> bool:
    if args.cleanup_after_run is not None:
        return bool(args.cleanup_after_run)
    return _database_url_is_local(str(args.database_url))


async def _tenant_scoped_tables(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a
          ON a.attrelid = c.oid
         AND a.attname = 'tenant_id'
         AND NOT a.attisdropped
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relname <> 'tenants'
        ORDER BY c.relname ASC
        """
    )
    return [str(row["relname"]) for row in rows]


async def _cleanup_unscoped_probe_links(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, int]:
    statements = {
        "contributes_to": """
            DELETE FROM contributes_to
            WHERE commitment_id IN (
                    SELECT id FROM commitments WHERE tenant_id = $1
                )
               OR goal_id IN (
                    SELECT id FROM goals WHERE tenant_id = $1
                )
        """,
        "constrained_by": """
            DELETE FROM constrained_by
            WHERE commitment_id IN (
                    SELECT id FROM commitments WHERE tenant_id = $1
                )
               OR decision_id IN (
                    SELECT id FROM decisions WHERE tenant_id = $1
                )
        """,
        "commitment_contributors": """
            DELETE FROM commitment_contributors
            WHERE commitment_id IN (
                    SELECT id FROM commitments WHERE tenant_id = $1
                )
               OR actor_id IN (
                    SELECT id FROM actors WHERE tenant_id = $1
                )
        """,
        "resource_deployments": """
            DELETE FROM resource_deployments
            WHERE commitment_id IN (
                    SELECT id FROM commitments WHERE tenant_id = $1
                )
               OR resource_id IN (
                    SELECT id FROM resources WHERE tenant_id = $1
                )
        """,
        "actor_identity_mappings": """
            DELETE FROM actor_identity_mappings
            WHERE actor_id IN (
                    SELECT id FROM actors WHERE tenant_id = $1
                )
        """,
    }
    deleted: dict[str, int] = {}
    for table, sql in statements.items():
        try:
            count = _status_row_count(await conn.execute(sql, tenant_id))
        except (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError):
            continue
        deleted[table] = count
    return deleted


async def _cleanup_probe_tenant(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    max_passes: int = 8,
) -> dict[str, Any]:
    """Delete rows for a generated probe tenant.

    The large probes create intentionally disposable tenants. Cleanup uses
    tenant_id-discovered tables instead of a static list so newly added trace
    tables do not silently bloat local databases after stress runs.
    """

    async with pool.acquire() as conn:
        tables = await _tenant_scoped_tables(conn)
        remaining = set(tables)
        deleted_by_table: dict[str, int] = {}
        blocked: dict[str, str] = {}
        passes = 0
        for pass_index in range(max(1, max_passes)):
            passes = pass_index + 1
            blocked.clear()
            progressed = False
            for table, deleted in (
                await _cleanup_unscoped_probe_links(conn, tenant_id)
            ).items():
                deleted_by_table[table] = deleted_by_table.get(table, 0) + deleted
                progressed = progressed or deleted > 0
            for table in list(remaining):
                try:
                    status = await conn.execute(
                        f"DELETE FROM {_quote_ident(table)} WHERE tenant_id = $1",
                        tenant_id,
                    )
                except asyncpg.UndefinedTableError:
                    remaining.discard(table)
                    progressed = True
                    continue
                except asyncpg.ForeignKeyViolationError as exc:
                    blocked[table] = str(exc).splitlines()[0]
                    continue
                deleted = _status_row_count(status)
                deleted_by_table[table] = deleted_by_table.get(table, 0) + deleted
                remaining.discard(table)
                progressed = True
            if not remaining:
                break
            if not progressed:
                break

        tenant_deleted = 0
        tenant_delete_error: str | None = None
        try:
            tenant_deleted = _status_row_count(
                await conn.execute("DELETE FROM tenants WHERE id = $1", tenant_id)
            )
        except asyncpg.ForeignKeyViolationError as exc:
            tenant_delete_error = str(exc).splitlines()[0]

        positive_deletes = {
            table: count
            for table, count in deleted_by_table.items()
            if count > 0
        }
        return {
            "tenant_id": str(tenant_id),
            "passes": passes,
            "tables_seen": len(tables),
            "tables_cleaned": len(deleted_by_table),
            "rows_deleted": sum(deleted_by_table.values()) + tenant_deleted,
            "tenant_deleted": bool(tenant_deleted),
            "blocked_tables": blocked,
            "tenant_delete_error": tenant_delete_error,
            "deleted_tables": sorted(positive_deletes),
            "top_deleted_tables": dict(
                sorted(
                    positive_deletes.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:12]
            ),
        }


async def _compact_local_cleanup_tables(
    pool: asyncpg.Pool,
    cleanup_status: dict[str, Any],
) -> dict[str, Any]:
    """Physically shrink local tables after deleting a generated probe tenant."""

    requested_tables = set(cleanup_status.get("deleted_tables") or [])
    requested_tables.update(LOCAL_COMPACTION_EXTRA_TABLES)
    if not requested_tables:
        return {
            "tables_requested": 0,
            "tables_compacted": 0,
            "compacted_tables": [],
            "failed_tables": {},
        }

    async with pool.acquire() as conn:
        existing_rows = await conn.fetch(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r', 'p')
              AND c.relname = ANY($1::text[])
            """,
            sorted(requested_tables),
        )
        existing_tables = {str(row["relname"]) for row in existing_rows}
        compacted: list[str] = []
        failed: dict[str, str] = {}
        for table in sorted(existing_tables):
            try:
                await conn.execute(
                    f"VACUUM (FULL, ANALYZE) {_quote_ident(table)}"
                )
            except (
                asyncpg.InsufficientPrivilegeError,
                asyncpg.UndefinedTableError,
                asyncpg.PostgresError,
            ) as exc:
                failed[table] = str(exc).splitlines()[0]
                continue
            compacted.append(table)

    return {
        "tables_requested": len(requested_tables),
        "tables_compacted": len(compacted),
        "compacted_tables": compacted[:24],
        "failed_tables": failed,
    }


def _channel_prefix(channel: str | None) -> str:
    text = str(channel or "unknown")
    return text.split(":", 1)[0]


def _signal_family(row: asyncpg.Record) -> str:
    content = row["content"] if isinstance(row["content"], dict) else {}
    return str(content.get("family") or "unknown")


def _coerce_embedding(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip("[]")
        if not stripped:
            return None
        return [float(part) for part in stripped.split(",")]
    return [float(v) for v in value]


async def _ensure_migrations(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await pgvector_pool_init(conn)
        await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")


async def _analyze_probe_tables(pool: asyncpg.Pool) -> dict[str, Any]:
    tables = (
        "models",
        "model_scope_entities",
        "model_scope_actors",
        "model_edges",
        "model_edge_structural_features",
        "observations",
        "actors",
        "resources",
        "goals",
        "commitments",
        "decisions",
        "contributes_to",
        "customer_commitments",
        "entity_aliases",
        "retrieval_affordance_profiles",
        "retrieval_plans",
    )
    started = time.perf_counter()
    async with pool.acquire() as conn:
        existing: list[str] = []
        for table in tables:
            exists = await conn.fetchval(
                "SELECT to_regclass($1)",
                f"public.{table}",
            )
            if exists is not None:
                existing.append(table)
        if existing:
            await conn.execute(
                "ANALYZE " + ", ".join(f"public.{table}" for table in existing)
            )
    return {
        "tables": len(existing),
        "table_names": existing,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def _belief_prop(
    *,
    about: str,
    nature: str,
    family: str,
    claim_role: str,
    abstraction_level: str = "atomic",
    polarity: str = "negative",
) -> dict[str, Any]:
    return {
        "kind": "belief",
        "about": about,
        "nature": nature,
        "claim": nature,
        "summary": nature,
        "assessment": nature,
        "modality": "observed",
        "polarity": polarity,
        "time_mode": "current",
        "claim_role": claim_role,
        "domain_tags": [family, "company_probe", "astergrid"],
        "abstraction_level": abstraction_level,
    }


async def _supporting_observation_id(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> UUID:
    row = await conn.fetchrow(
        """
        SELECT id
        FROM observations
        WHERE tenant_id = $1
        ORDER BY occurred_at ASC, id ASC
        LIMIT 1
        """,
        tenant_id,
    )
    if row is None:
        raise RuntimeError("scenario materialization did not create observations")
    return row["id"]


def _commitment_for_customer(customer: str, index: int) -> str:
    for commitment in COMMITMENTS:
        title = str(commitment["title"])
        if customer in title:
            return title
    return str(COMMITMENTS[index % len(COMMITMENTS)]["title"])


def _coherent_scope(
    scenario: Any,
    *,
    customer: str,
    commitment: str,
    goal: str,
    decision: str,
) -> list[dict[str, str]]:
    scope: list[dict[str, str]] = []
    if customer in scenario.customers:
        scope.append({"type": "customer", "id": str(scenario.customer_id(customer))})
    if commitment in scenario.commitments:
        scope.append({
            "type": "commitment",
            "id": str(scenario.commitment_id(commitment)),
        })
    if goal in scenario.goals:
        scope.append({"type": "goal", "id": str(scenario.goal_id(goal))})
    if decision in scenario.decisions:
        scope.append({"type": "decision", "id": str(scenario.decision_id(decision))})
    return scope


def _coherent_model_draft(
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
    embedding_key: str,
    scope_entities: list[dict[str, str]],
    family: str,
    claim_role: str,
    confidence: float,
    supporting_model_ids: list[UUID] | None = None,
) -> ModelCreate:
    return ModelCreate(
        id=uuid7(),
        tenant_id=tenant_id,
        born_from_event_id=born_from_event_id,
        proposition=_belief_prop(
            about=scope_entities[0]["id"] if scope_entities else family,
            nature=natural,
            family=family,
            claim_role=claim_role,
            abstraction_level="atomic",
            polarity="neutral" if family == "noise" else "negative",
        ),
        natural=natural,
        embedding=_embedding(embedding_key),
        scope_actors=[],
        scope_entities=scope_entities,
        scope_temporal={"valid_from": "now", "valid_until": None},
        confidence=confidence,
        confidence_at_assertion=confidence,
        supporting_event_ids=[born_from_event_id],
        supporting_model_ids=list(supporting_model_ids or []),
        domain_tags=[family, "astergrid", "company_probe"],
    )


async def _seed_coherent_company_models(
    pool: asyncpg.Pool,
    *,
    scenario: Any,
    tenant_id: UUID,
    total_models: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    async with pool.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            await conn.execute("SET CONSTRAINTS ALL DEFERRED")
            born_from_event_id = await _supporting_observation_id(conn, tenant_id)
            drafts: list[ModelCreate] = []
            anchor_ids: list[UUID] = []
            for index in range(total_models):
                family = FAMILIES[index % len(FAMILIES)]
                customer = CUSTOMERS[(index * 7 + index // 50) % len(CUSTOMERS)]
                secondary = CUSTOMERS[(index * 11 + 3) % len(CUSTOMERS)]
                commitment = _commitment_for_customer(customer, index)
                goal = str(GOALS[index % len(GOALS)]["title"])
                decision = str(DECISIONS[index % len(DECISIONS)]["title"])
                scope = _coherent_scope(
                    scenario,
                    customer=customer,
                    commitment=commitment,
                    goal=goal,
                    decision=decision,
                )
                if index % 9 == 0 and secondary in scenario.customers:
                    scope.append({
                        "type": "customer",
                        "id": str(scenario.customer_id(secondary)),
                    })
                family_cycle = index // len(FAMILIES)
                natural = (
                    f"AsterGrid belief {index:05d}: {customer} has a {family} "
                    f"operating signal tied to '{commitment}'. The durable "
                    f"memory is that {goal} and decision '{decision}' should "
                    "shape retrieval before treating the next workplace update "
                    "as isolated noise."
                )
                if family in {"risk_digest", "partner_integration", "board_update"}:
                    natural += (
                        f" Hidden connection: {secondary} may share the same "
                        "underlying dependency through enterprise controls."
                    )
                if family == "noise":
                    natural = (
                        f"AsterGrid weak-signal belief {index:05d}: chatter "
                        f"mentioning {customer} should stay low-priority unless "
                        "it gains a commitment, customer-risk, or owner-change cue."
                    )
                supporting = [anchor_ids[-1]] if anchor_ids and index % 5 == 0 else []
                draft = _coherent_model_draft(
                    tenant_id=tenant_id,
                    born_from_event_id=born_from_event_id,
                    natural=natural,
                    embedding_key=f"astergrid:{family}:{customer}:{family_cycle}",
                    scope_entities=scope,
                    family=family,
                    claim_role="fact" if family == "noise" else "concern",
                    confidence=0.55 + ((index % 5) * 0.025),
                    supporting_model_ids=supporting,
                )
                drafts.append(draft)
                if index % 20 == 0:
                    assert draft.id is not None
                    anchor_ids.append(draft.id)

            repo = ModelsRepo(pool, embedder=None, run_topology_on_insert=False)
            await repo.insert_many(
                drafts,
                conn=conn,
                apply_confidence_calibration=False,
            )
            sidecars = {
                "models": int(await conn.fetchval(
                    "SELECT COUNT(*) FROM models WHERE tenant_id = $1",
                    tenant_id,
                ) or 0),
                "model_edges": int(await conn.fetchval(
                    "SELECT COUNT(*) FROM model_edges WHERE tenant_id = $1",
                    tenant_id,
                ) or 0),
                "model_scope_entities": int(await conn.fetchval(
                    "SELECT COUNT(*) FROM model_scope_entities WHERE tenant_id = $1",
                    tenant_id,
                ) or 0),
                "model_scope_actors": int(await conn.fetchval(
                    "SELECT COUNT(*) FROM model_scope_actors WHERE tenant_id = $1",
                    tenant_id,
                ) or 0),
            }
    return {
        "requested_models": total_models,
        "families": len(FAMILIES),
        "models": len(drafts),
        "insert_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "sidecars": sidecars,
        "mode": "coherent_astergrid_seed",
    }


async def _fetch_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_id: UUID,
) -> asyncpg.Record:
    row = await conn.fetchrow(
        """
        SELECT id, tenant_id, source_channel, kind, trust_tier, occurred_at,
               content_text, content, entities_mentioned, actor_id, embedding
        FROM observations
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        observation_id,
    )
    if row is None:
        raise RuntimeError(f"observation not found: {observation_id}")
    return row


def _entity_hints(row: asyncpg.Record) -> list[dict[str, Any]]:
    raw = row["entities_mentioned"] or []
    return [item for item in raw if isinstance(item, dict)]


async def _expected_scoped_model_ids(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    hints: list[dict[str, Any]],
    limit: int,
) -> list[UUID]:
    pairs: list[tuple[str, UUID]] = []
    seen: set[tuple[str, UUID]] = set()
    for hint in hints:
        raw_type = hint.get("type")
        raw_id = hint.get("id")
        if raw_type is None or raw_id is None:
            continue
        try:
            pair = (str(raw_type), UUID(str(raw_id)))
        except (TypeError, ValueError):
            continue
        if pair not in seen:
            seen.add(pair)
            pairs.append(pair)
    if not pairs:
        return []
    types = [pair[0] for pair in pairs]
    ids = [pair[1] for pair in pairs]
    rows = await conn.fetch(
        """
        WITH hints AS (
          SELECT *
          FROM unnest($2::text[], $3::uuid[]) AS h(entity_type, entity_id)
        )
        SELECT m.id, COUNT(*) AS overlap
        FROM hints h
        JOIN model_scope_entities mse
          ON mse.tenant_id = $1
         AND mse.entity_type = h.entity_type
         AND mse.entity_id = h.entity_id
        JOIN models m
          ON m.tenant_id = mse.tenant_id
         AND m.id = mse.model_id
         AND m.status = 'active'
        GROUP BY m.id
        ORDER BY overlap DESC, m.id ASC
        LIMIT $4
        """,
        tenant_id,
        types,
        ids,
        limit,
    )
    return [row["id"] for row in rows]


def _trigger_from_observation(row: asyncpg.Record) -> TriggerContext:
    actor_id = row["actor_id"]
    return TriggerContext(
        kind="T1",
        tenant_id=row["tenant_id"],
        observation_id=row["id"],
        seed_entity_ids=_entity_hints(row),
        seed_natural_text=str(row["content_text"] or ""),
        seed_occurred_at=row["occurred_at"],
        scope_actors=[actor_id] if actor_id else [],
        precomputed_seed_vector=_coerce_embedding(row["embedding"]),
        semantic_k=48,
        temporal_window=timedelta(days=45),
        max_hops=2,
    )


class _ScriptedBeliefDeltaProvider(LLMProvider):
    """Deterministic structured provider for large retrieval probes.

    The local environment may not have a live LLM key, but the new flow we
    need to stress is downstream of the provider response. This provider emits
    belief_deltas only, forcing production code to expand delta uncertainty
    slots into retrieval questions.
    """

    def __init__(self) -> None:
        super().__init__(
            LLMConfig(
                provider="anthropic",
                api_key="scripted",
                model="scripted-belief-delta-compiler",
            )
        )
        self.calls = 0

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        self.calls += 1
        try:
            payload = json.loads(user)
        except json.JSONDecodeError:
            payload = {}
        signal = payload.get("signal") if isinstance(payload, dict) else {}
        text = str((signal or {}).get("text") or "")
        entities = (signal or {}).get("seed_entities") or []
        entity_labels = _scripted_entity_labels(entities)
        deltas = _scripted_belief_deltas(text, entity_labels)
        return json.dumps({
            "rationale": "scripted structured belief-delta compiler",
            "belief_deltas": deltas,
            "questions": [],
        })


def _scripted_entity_labels(entities: list[Any]) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    for entity in entities[:8]:
        if not isinstance(entity, dict):
            continue
        raw = entity.get("label") or entity.get("name") or entity.get("id")
        if raw is None:
            continue
        label = str(raw).strip()
        if not label or _looks_like_uuid(label):
            continue
        key = label.casefold()
        if key in seen:
            continue
        seen.add(key)
        labels.append(label[:80])
    return labels


def _looks_like_uuid(value: str) -> bool:
    try:
        UUID(str(value))
    except (TypeError, ValueError):
        return False
    return True


def _scripted_focus_sentences(text: str) -> list[str]:
    clean = " ".join((text or "").split())
    if not clean:
        return []
    clean = clean.partition("Company context:")[2] or clean
    sentences = [
        item.strip(" '\"`.,;:()[]{}")
        for item in clean.replace("?", ".").replace("!", ".").split(".")
    ]
    keywords = (
        "approve",
        "approval",
        "at risk",
        "blocked",
        "blocker",
        "capacity",
        "cannot",
        "conflict",
        "delayed",
        "edge case",
        "incident",
        "owner",
        "procurement",
        "redline",
        "renewal",
        "repeats",
        "review",
        "risk",
        "saml",
        "security",
        "stale",
    )
    ranked: list[tuple[int, str]] = []
    for sentence in sentences:
        lower = sentence.casefold()
        if not sentence or "post-product-market fit" in lower or "months runway" in lower:
            continue
        score = sum(1 for keyword in keywords if keyword in lower)
        if "$" in sentence or "arr" in lower:
            score += 1
        if score:
            ranked.append((score, sentence))
    ranked.sort(key=lambda item: (item[0], len(item[1])), reverse=True)
    return [sentence for _, sentence in ranked[:3]]


def _scripted_belief_deltas(text: str, entity_labels: list[str]) -> list[dict[str, Any]]:
    lower = text.casefold()
    subject = ", ".join(entity_labels[:3]) or "the signal scope"
    focuses = _scripted_focus_sentences(text)
    if not focuses:
        focuses = [text[:180] or "the signal may require a model update"]
    deltas: list[dict[str, Any]] = []
    primary = focuses[0][:220]
    slots = [
        "what evidence would weaken this interpretation",
        "who owns the next action",
    ]
    evidence = ["recent workplace signal", "related active commitments"]
    if any(word in lower for word in ("block", "depends", "cannot", "critical")):
        slots.insert(0, "whether this blocker is on the critical path")
        evidence.append("dependency or commitment graph")
    if any(word in lower for word in ("owner", "who", "assigned")):
        slots.append("who is accountable for the affected commitment")
        evidence.append("owner or decision record")
    if any(word in lower for word in ("renewal", "arr", "$", "revenue", "customer")):
        slots.append("which customer goal or revenue path is at risk")
        evidence.append("goal or customer resource record")
    if any(word in lower for word in ("repeat", "recur", "again", "stale")):
        slots.append("whether this pattern has appeared before")
        evidence.append("similar prior observations")
    if any(word in lower for word in ("policy", "capacity", "quota", "security", "legal")):
        slots.append("which resource, policy, or capacity constraint is binding")
        evidence.append("constraint or resource evidence")
    deltas.append({
        "delta_id": "D_PRIMARY",
        "claim_atom": primary,
        "delta_type": "update",
        "affected_entities": [subject],
        "uncertainty_slots": slots[:6],
        "evidence_needed": evidence[:6],
        "impact_if_true": "high" if any(word in lower for word in ("p0", "at risk", "$", "cannot", "blocked")) else "medium",
        "confidence": 0.68,
    })
    if len(focuses) > 1:
        deltas.append({
            "delta_id": "D_ALT",
            "claim_atom": focuses[1][:220],
            "delta_type": "weaken" if "stale" in lower or "conflict" in lower else "update",
            "affected_entities": [subject],
            "uncertainty_slots": [
                "what evidence distinguishes this from the leading interpretation",
                "what evidence would weaken this alternate belief",
            ],
            "evidence_needed": ["counterevidence", "nearby existing models"],
            "impact_if_true": "medium",
            "confidence": 0.52,
        })
    return deltas


def _provider_for_mode(mode: str) -> LLMProvider | None:
    if mode == "deterministic":
        return None
    if mode == "scripted-belief-delta":
        return _ScriptedBeliefDeltaProvider()
    if mode == "live-env":
        try:
            cfg = LLMConfig.from_env()
        except LLMConfigError as exc:
            raise SystemExit(f"LLM provider is not configured: {exc}") from exc
        if not cfg.api_key:
            raise SystemExit(
                "LLM auth is not configured for --question-planning-mode live-env"
            )
        _align_question_planning_timeout(cfg)
        return build_provider(cfg)
    if mode == "codex-low":
        os.environ["LLM_PROVIDER"] = "codex"
        os.environ["CODEX_REASONING_EFFORT"] = "low"
        try:
            cfg = LLMConfig.from_env()
        except LLMConfigError as exc:
            raise SystemExit(f"Codex provider is not configured: {exc}") from exc
        if not cfg.api_key:
            raise SystemExit(
                "Codex auth is not configured; run `codex login` or set "
                "CODEX_API_KEY/OPENAI_API_KEY/LLM_API_KEY"
            )
        _align_question_planning_timeout(cfg)
        return build_provider(cfg)
    return None


def _align_question_planning_timeout(cfg: LLMConfig) -> None:
    if os.environ.get("INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS"):
        return
    os.environ["INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS"] = str(
        max(30, int(cfg.timeout_s))
    )


def _provider_metadata(
    provider: LLMProvider | None,
    *,
    question_planning_mode: str,
) -> dict[str, Any]:
    planning_provider = (
        select_question_planning_provider(provider)
        if question_planning_mode != "deterministic"
        else None
    )
    metadata: dict[str, Any] = {
        "question_planning_mode": question_planning_mode,
        "llm_question_planning_enabled": planning_provider is not None,
    }
    if planning_provider is None:
        return metadata
    planning_metadata = question_planning_provider_metadata(planning_provider)
    metadata.update({
        **planning_metadata,
        "llm_timeout_s": planning_provider.config.timeout_s,
        "llm_max_retries": planning_provider.config.max_retries,
        "inquiry_llm_question_timeout_s": float(
            os.environ.get("INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS") or 0.0
        ),
    })
    if provider is not None and provider is not planning_provider:
        metadata.update({
            "base_llm_provider": provider.config.provider,
            "base_llm_model": provider.config.model,
            "base_llm_reasoning_effort": provider.config.reasoning_effort,
        })
    if planning_provider.config.provider == "codex":
        metadata["codex_transport"] = _codex_transport()
        metadata["codex_reasoning_effort"] = planning_provider.config.reasoning_effort
    return metadata


def _inquiry_config(question_planning_mode: str = "deterministic") -> InquiryConfig:
    return InquiryConfig(
        max_rounds=1,
        questions_per_round=3,
        evidence_reservoir_limit=260,
        fast_path_evidence_limit=48,
        candidate_model_limit=180,
        result_model_limit=56,
        action_model_budget_limit=40,
        action_observation_budget_limit=28,
        relevance_min_material_models=3,
        temporal_window_days=45,
        semantic_budget=48,
        structural_max_hops=2,
        structural_read_fanout_enabled=True,
        structural_read_fanout_min_seeds=12,
        structural_read_fanout_chunk_size=6,
        model_edge_max_hops=2,
        llm_question_planning_enabled=question_planning_mode != "deterministic",
        sage_reader_enabled=True,
        persist=True,
    )


def _selected_ids(result: Any) -> list[UUID]:
    return [model.id for model in result.retrieval_result.models]


def _evidence_source_prefixes(result: Any) -> set[str]:
    prefixes: set[str] = set()
    for obs in result.retrieval_result.observations:
        prefixes.add(_channel_prefix(getattr(obs, "source_channel", None)))
    return prefixes


def _quality_failure_modes(
    *,
    row: asyncpg.Record,
    expected_ids: list[UUID],
    selected_ids: list[UUID],
    evidence_count: int,
    retrieval_ms: float,
    evidence_sources: set[str],
    outcome_modes: list[str],
) -> list[str]:
    modes = set(outcome_modes)
    family = _signal_family(row)
    if not _entity_hints(row):
        modes.add("no_entity_hints")
    if not expected_ids:
        modes.add("no_expected_scoped_models")
    elif not set(expected_ids).intersection(selected_ids):
        modes.add("expected_scope_miss")
    if not selected_ids:
        modes.add("empty_model_selection")
    if evidence_count == 0:
        modes.add("empty_evidence_packet")
    if len(selected_ids) >= 52:
        modes.add("wide_model_selection")
    if retrieval_ms >= 3500.0:
        modes.add("retrieval_latency_slo_miss")
    if family == "noise" and len(selected_ids) > 8:
        modes.add("weak_signal_overselected")
    if len(evidence_sources) <= 1 and family not in {"noise", "stale_replay"}:
        modes.add("single_source_context")
    return sorted(modes)


def _value_tags(
    *,
    row: asyncpg.Record,
    expected_hit: bool,
    selected_count: int,
    evidence_sources: set[str],
    optimizer: Any,
    notes: dict[str, Any],
) -> list[str]:
    tags: set[str] = set()
    family = _signal_family(row)
    if expected_hit:
        tags.add("scoped_memory_hit")
    if family == "noise" and selected_count <= 8:
        tags.add("noise_suppression")
    if len(evidence_sources) >= 2:
        tags.add("cross_source_context")
    pathways = set((notes.get("pathways_run") or []))
    if "G" in pathways or "model_edge" in pathways:
        tags.add("hidden_graph_path")
    if (
        optimizer.affordance_reinforces
        or optimizer.shortcut_creates_or_bumps
        or optimizer.negative_memory_inserts
    ):
        tags.add("feedback_learning")
    return sorted(tags)


def _elapsed_total(notes: list[dict[str, Any]]) -> int:
    total = 0
    for note in notes:
        try:
            total += int(note.get("elapsed_ms") or 0)
        except (TypeError, ValueError):
            continue
    return total


def _top_elapsed_notes(
    notes: list[dict[str, Any]],
    *,
    key: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for note in notes:
        try:
            elapsed_ms = int(note.get("elapsed_ms") or 0)
        except (TypeError, ValueError):
            continue
        item = {
            key: note.get(key),
            "elapsed_ms": elapsed_ms,
            "question_id": note.get("question_id"),
            "models": note.get("models"),
            "observations": note.get("observations"),
        }
        for extra_key in (
            "acts",
            "resources",
            "skipped",
            "reason",
            "pathway_notes",
        ):
            if note.get(extra_key) is not None:
                item[extra_key] = note.get(extra_key)
        ranked.append(item)
    ranked.sort(key=lambda item: int(item["elapsed_ms"]), reverse=True)
    return ranked[:limit]


def _sage_reader_questions(notes: dict[str, Any]) -> list[dict[str, Any]]:
    sage = notes.get("sage_reader") or {}
    questions = sage.get("questions") if isinstance(sage, dict) else {}
    if not isinstance(questions, dict):
        return []
    return [item for item in questions.values() if isinstance(item, dict)]


def _sage_stage_timing_summary(
    notes: dict[str, Any],
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    totals: Counter[str] = Counter()
    maximums: dict[str, int] = {}
    modes: Counter[str] = Counter()
    for question in _sage_reader_questions(notes):
        debug = question.get("debug") or {}
        if not isinstance(debug, dict):
            continue
        plan = debug.get("learned_read_plan") or {}
        if isinstance(plan, dict):
            modes[str(plan.get("mode") or "unknown")] += 1
        timings = debug.get("stage_timings_ms") or {}
        if not isinstance(timings, dict):
            continue
        for stage, raw_value in timings.items():
            try:
                value = int(raw_value or 0)
            except (TypeError, ValueError):
                continue
            totals[str(stage)] += value
            maximums[str(stage)] = max(value, maximums.get(str(stage), 0))
    return dict(sorted(totals.items())), dict(sorted(maximums.items())), dict(modes)


def _primary_pathway_timings_top(
    stage_timings: list[dict[str, Any]],
    *,
    limit: int = 8,
) -> list[dict[str, Any]]:
    for note in stage_timings:
        if note.get("stage") != "primary_retrieve":
            continue
        timings = [
            item for item in (note.get("primary_pathway_timings") or [])
            if isinstance(item, dict)
        ]
        return _top_elapsed_notes(timings, key="stage", limit=limit)
    return []


async def _process_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    step_index: int,
    expected_limit: int,
    question_planning_mode: str,
    llm_provider: LLMProvider | None,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            obs = await _fetch_observation(
                conn,
                tenant_id=tenant_id,
                observation_id=observation_id,
            )
            expected_ids = await _expected_scoped_model_ids(
                conn,
                tenant_id=tenant_id,
                hints=_entity_hints(obs),
                limit=expected_limit,
            )
            trigger = _trigger_from_observation(obs)
            started = time.perf_counter()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                embedder=None,
                llm_provider=llm_provider,
                read_pool=pool,
                mode="deep",
                top_n=180,
                config=_inquiry_config(question_planning_mode),
            )
            retrieval_ms = (time.perf_counter() - started) * 1000.0
            selected_ids = _selected_ids(result)
            selected_expected = [
                model_id for model_id in selected_ids if model_id in set(expected_ids)
            ]
            expected_overlap = len(selected_expected)
            expected_precision = expected_overlap / max(len(selected_ids), 1)
            expected_recall = expected_overlap / max(len(expected_ids), 1)
            used_model_ids = selected_expected[:4] or selected_ids[:4]
            writer = await _attach_writer_outcome(
                conn,
                tenant_id=tenant_id,
                result=result,
                trigger=trigger,
                used_model_ids=used_model_ids,
                expected_model_ids=expected_ids,
            )
            outcome = await OutcomeEvaluator(
                pool=None,
                tenant_id=tenant_id,
            ).evaluate(
                inquiry_session_id=result.session_id,
                conn=conn,
            )
            optimizer = await TopologyOptimizer(
                pool=None,
                tenant_id=tenant_id,
            ).optimize(
                inquiry_session_id=result.session_id,
                trigger_event=(
                    "validated_synthesis_diff_applied"
                    if writer["status"] == "success"
                    else "reasoning_diff_failed_validation"
                ),
                conn=conn,
            )
            learned_counts = await _learned_layer_counts(conn, tenant_id)

    notes = result.notes or {}
    action_timings = [
        item for item in (notes.get("retrieval_action_timings") or [])
        if isinstance(item, dict)
    ]
    stage_timings = [
        item for item in (notes.get("retrieval_stage_timings") or [])
        if isinstance(item, dict)
    ]
    sage_stage_total, sage_stage_max, sage_plan_modes = _sage_stage_timing_summary(notes)
    sage_reader_notes = notes.get("sage_reader") or {}
    question_planning = [
        item for item in (notes.get("question_planning") or [])
        if isinstance(item, dict)
    ]
    first_planning = question_planning[0] if question_planning else {}
    evidence_sources = _evidence_source_prefixes(result)
    expected_hit = bool(set(expected_ids).intersection(selected_ids))
    context_packet_stats = _context_packet_stats(result.context_packet)
    context_packet_quality = _context_packet_quality_score(
        expected_precision=expected_precision,
        expected_recall=expected_recall,
        evidence_count=len(result.evidence_cards),
        evidence_source_count=len(evidence_sources),
        context_packet_stats=context_packet_stats,
    )
    failure_modes = _quality_failure_modes(
        row=obs,
        expected_ids=expected_ids,
        selected_ids=selected_ids,
        evidence_count=len(result.evidence_cards),
        retrieval_ms=retrieval_ms,
        evidence_sources=evidence_sources,
        outcome_modes=list(outcome.quality_signal.failure_modes),
    )
    row = {
        "case_index": step_index,
        "observation_id": str(observation_id),
        "source_channel": obs["source_channel"],
        "source_prefix": _channel_prefix(obs["source_channel"]),
        "family": _signal_family(obs),
        "trust_tier": obs["trust_tier"],
        "expected_scoped_models": len(expected_ids),
        "expected_scope_hit": expected_hit,
        "expected_scope_overlap": expected_overlap,
        "expected_scope_precision": round(expected_precision, 6),
        "expected_scope_recall": round(expected_recall, 6),
        "selected_count": len(selected_ids),
        "evidence_count": len(result.evidence_cards),
        "evidence_source_prefixes": sorted(evidence_sources),
        "context_packet_quality_score": context_packet_quality,
        "context_packet_stats": context_packet_stats,
        "question_planning_mode": question_planning_mode,
        "question_planning": question_planning,
        "question_mode": first_planning.get("mode"),
        "belief_delta_count": first_planning.get("belief_delta_count", 0),
        "belief_delta_question_count": first_planning.get(
            "belief_delta_question_count", 0
        ),
        "llm_candidate_count": first_planning.get("llm_candidate_count", 0),
        "questions": [
            {
                "question_id": question.question_id,
                "primitive": question.primitive,
                "question": question.question,
                "score": round(float(question.score), 4),
            }
            for question in result.questions
        ],
        "retrieval_ms": round(retrieval_ms, 3),
        "retrieval_runtime": notes.get("retrieval_runtime") or {},
        "retrieval_stage_timings_ms_total": _elapsed_total(stage_timings),
        "retrieval_stage_timings_top": _top_elapsed_notes(
            stage_timings,
            key="stage",
        ),
        "retrieval_primary_pathway_timings_top": _primary_pathway_timings_top(
            stage_timings,
        ),
        "retrieval_action_timings_ms_total": _elapsed_total(action_timings),
        "retrieval_action_timings_top": _top_elapsed_notes(
            action_timings,
            key="path",
        ),
        "sage_reader_batches": (
            sage_reader_notes.get("batches") if isinstance(sage_reader_notes, dict) else []
        ) or [],
        "sage_substrate": (
            sage_reader_notes.get("substrate")
            if isinstance(sage_reader_notes, dict) else {}
        ) or {},
        "sage_plan_modes": sage_plan_modes,
        "sage_stage_timings_ms_total": sage_stage_total,
        "sage_stage_timings_ms_max": sage_stage_max,
        "writer_status": writer["status"],
        "outcome_events": outcome.events_by_type,
        "quality_bottleneck": outcome.quality_signal.primary_bottleneck,
        "quality_failure_modes": failure_modes,
        "value_tags": _value_tags(
            row=obs,
            expected_hit=expected_hit,
            selected_count=len(selected_ids),
            evidence_sources=evidence_sources,
            optimizer=optimizer,
            notes=notes,
        ),
        "optimizer": {
            "affordance_reinforces": optimizer.affordance_reinforces,
            "affordance_decays": optimizer.affordance_decays,
            "shortcut_creates_or_bumps": optimizer.shortcut_creates_or_bumps,
            "shortcut_decays": optimizer.shortcut_decays,
            "negative_memory_inserts": optimizer.negative_memory_inserts,
            "question_policy_updates": optimizer.question_policy_updates,
            "canonical_merge_candidates": len(optimizer.canonical_merge_candidates),
            "canonical_split_candidates": len(optimizer.canonical_split_candidates),
            "canonical_promote_candidates": len(optimizer.canonical_promote_candidates),
            "canonical_demote_candidates": len(optimizer.canonical_demote_candidates),
            "metrics": optimizer.metrics,
        },
        "learned_counts": learned_counts,
        "pathways_run": notes.get("pathways_run") or [],
    }
    print(json.dumps(row, sort_keys=True, default=_json_default), flush=True)
    return row


def _context_packet_stats(packet: dict[str, Any]) -> dict[str, int]:
    tiers = packet.get("tiers") if isinstance(packet, dict) else {}
    tiers = tiers if isinstance(tiers, dict) else {}
    return {
        "decisive_evidence": len(tiers.get("decisive_evidence") or []),
        "supporting_evidence_groups": len(
            tiers.get("supporting_evidence_groups") or []
        ),
        "background_summaries": len(tiers.get("background_summaries") or []),
        "omission_ledger": len(tiers.get("omission_ledger") or []),
        "important_unknowns": len(packet.get("important_unknowns") or [])
        if isinstance(packet, dict) else 0,
        "candidate_state_changes": len(packet.get("candidate_state_changes") or [])
        if isinstance(packet, dict) else 0,
        "question_path": len(packet.get("question_path") or [])
        if isinstance(packet, dict) else 0,
        "question_answers": len(packet.get("question_answers") or [])
        if isinstance(packet, dict) else 0,
    }


def _context_packet_quality_score(
    *,
    expected_precision: float,
    expected_recall: float,
    evidence_count: int,
    evidence_source_count: int,
    context_packet_stats: dict[str, int],
) -> float:
    evidence_score = min(float(evidence_count) / 18.0, 1.0)
    source_score = min(float(evidence_source_count) / 3.0, 1.0)
    answer_score = min(
        float(context_packet_stats.get("question_answers") or 0) / 3.0,
        1.0,
    )
    unknown_penalty = min(
        float(context_packet_stats.get("important_unknowns") or 0) / 8.0,
        1.0,
    )
    state_score = 1.0 if context_packet_stats.get("candidate_state_changes") else 0.5
    score = (
        0.24 * expected_precision
        + 0.24 * expected_recall
        + 0.16 * evidence_score
        + 0.14 * source_score
        + 0.12 * answer_score
        + 0.10 * state_score
        - 0.08 * unknown_penalty
    )
    return round(max(0.0, min(1.0, score)), 6)


async def _fetch_distribution(
    conn: asyncpg.Connection,
    sql: str,
    *args: Any,
) -> dict[str, int]:
    rows = await conn.fetch(sql, *args)
    return {str(row["key"]): int(row["value"]) for row in rows}


async def _model_layer_health(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        await pgvector_pool_init(conn)
        active_models = int(await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id = $1 AND status = 'active'",
            tenant_id,
        ) or 0)
        rows = await conn.fetch(
            """
            SELECT
              COUNT(*) FILTER (
                WHERE COALESCE(proposition->>'abstraction_level', 'atomic') = 'atomic'
              )::int AS atomic_models,
              COUNT(*) FILTER (
                WHERE jsonb_array_length(COALESCE(scope_entities, '[]'::jsonb)) = 0
              )::int AS unscoped_models,
              AVG(length("natural"))::float AS avg_natural_len,
              percentile_cont(0.95) WITHIN GROUP (ORDER BY length("natural"))::float
                AS p95_natural_len
            FROM models
            WHERE tenant_id = $1 AND status = 'active'
            """,
            tenant_id,
        )
        shape = dict(rows[0]) if rows else {}
        return {
            "active_models": active_models,
            "archived_models": int(await conn.fetchval(
                "SELECT COUNT(*) FROM models WHERE tenant_id = $1 AND status = 'archived'",
                tenant_id,
            ) or 0),
            "model_edges": int(await conn.fetchval(
                "SELECT COUNT(*) FROM model_edges WHERE tenant_id = $1",
                tenant_id,
            ) or 0),
            "relationship_candidates": int(await conn.fetchval(
                "SELECT COUNT(*) FROM relationship_candidates WHERE tenant_id = $1",
                tenant_id,
            ) or 0),
            "latent_topology_candidates": int(await conn.fetchval(
                """
                SELECT COUNT(*) FROM relationship_candidates
                WHERE tenant_id = $1 AND source = 'latent_topology'
                """,
                tenant_id,
            ) or 0),
            "model_scope_entity_sidecars": int(await conn.fetchval(
                "SELECT COUNT(*) FROM model_scope_entities WHERE tenant_id = $1",
                tenant_id,
            ) or 0),
            "model_scope_actor_sidecars": int(await conn.fetchval(
                "SELECT COUNT(*) FROM model_scope_actors WHERE tenant_id = $1",
                tenant_id,
            ) or 0),
            "atomic_models": int(shape.get("atomic_models") or 0),
            "atomic_model_ratio": (
                int(shape.get("atomic_models") or 0) / max(active_models, 1)
            ),
            "unscoped_models": int(shape.get("unscoped_models") or 0),
            "unscoped_model_ratio": (
                int(shape.get("unscoped_models") or 0) / max(active_models, 1)
            ),
            "avg_natural_len": round(float(shape.get("avg_natural_len") or 0.0), 3),
            "p95_natural_len": round(float(shape.get("p95_natural_len") or 0.0), 3),
            "model_kind_distribution": await _fetch_distribution(
                conn,
                """
                SELECT COALESCE(proposition_kind, '<none>') AS key,
                       COUNT(*)::int AS value
                FROM models
                WHERE tenant_id = $1
                GROUP BY 1
                ORDER BY 2 DESC, 1 ASC
                """,
                tenant_id,
            ),
            "edge_kind_distribution": await _fetch_distribution(
                conn,
                """
                SELECT edge_kind AS key, COUNT(*)::int AS value
                FROM model_edges
                WHERE tenant_id = $1
                GROUP BY 1
                ORDER BY 2 DESC, 1 ASC
                """,
                tenant_id,
            ),
        }


def _learning_total(counts: dict[str, Any]) -> int:
    return sum(
        int(counts.get(key) or 0)
        for key in (
            "contextual_affordance_profiles",
            "discovery_shortcuts",
            "negative_memory",
            "reinforced_affordance_profiles",
        )
    )


def _learning_pressure(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {"quarters": [], "late_trace_pressure": False}
    quarter_size = max(1, len(results) // 4)
    quarters = []
    late_pressure = False
    for idx, start in enumerate(range(0, len(results), quarter_size), start=1):
        chunk = results[start:start + quarter_size]
        first = chunk[0].get("learned_counts") or {}
        last = chunk[-1].get("learned_counts") or {}
        attr_delta = int(last.get("reader_decision_attributions") or 0) - int(
            first.get("reader_decision_attributions") or 0
        )
        learning_delta = _learning_total(last) - _learning_total(first)
        ratio = attr_delta / max(learning_delta, 1)
        pressure = idx > 1 and attr_delta >= 1000 and ratio >= 5000
        late_pressure = late_pressure or pressure
        quarters.append({
            "quarter": idx,
            "case_start": chunk[0]["case_index"],
            "case_end": chunk[-1]["case_index"],
            "reader_decision_attribution_delta": attr_delta,
            "compact_learning_delta": learning_delta,
            "attributions_per_new_learning_signal": round(ratio, 6),
            "trace_pressure": pressure,
        })
    return {"quarters": quarters, "late_trace_pressure": late_pressure}


def _summarize_patterns(results: list[dict[str, Any]]) -> dict[str, Any]:
    by_failure = Counter()
    by_family_failure: dict[str, Counter[str]] = defaultdict(Counter)
    by_source_failure: dict[str, Counter[str]] = defaultdict(Counter)
    value_tags = Counter()
    family_cases = Counter()
    family_misses = Counter()
    for row in results:
        family = str(row.get("family") or "unknown")
        source = str(row.get("source_prefix") or "unknown")
        family_cases[family] += 1
        if not row.get("expected_scope_hit"):
            family_misses[family] += 1
        for tag in row.get("value_tags") or []:
            value_tags[str(tag)] += 1
        for mode in row.get("quality_failure_modes") or []:
            mode = str(mode)
            by_failure[mode] += 1
            by_family_failure[family][mode] += 1
            by_source_failure[source][mode] += 1
    worst_families = []
    for family, total in family_cases.items():
        misses = family_misses[family]
        worst_families.append({
            "family": family,
            "cases": total,
            "expected_scope_misses": misses,
            "expected_scope_miss_rate": misses / max(total, 1),
            "top_failure_modes": dict(by_family_failure[family].most_common(5)),
        })
    worst_families.sort(
        key=lambda row: (row["expected_scope_miss_rate"], row["cases"]),
        reverse=True,
    )
    return {
        "failure_mode_totals": dict(by_failure.most_common()),
        "value_tag_totals": dict(value_tags.most_common()),
        "worst_families": worst_families[:12],
        "worst_sources": {
            source: dict(counter.most_common(5))
            for source, counter in sorted(
                by_source_failure.items(),
                key=lambda item: sum(item[1].values()),
                reverse=True,
            )[:12]
        },
    }


def _readiness(summary: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if summary["expected_scope_hit_rate"] < 0.98:
        blockers.append("expected_scope_hit_rate")
    if summary["retrieval_ms"]["p95"] > 3500.0:
        blockers.append("retrieval_p95_slo")
    if summary["model_layer_health"]["atomic_model_ratio"] < 0.98:
        blockers.append("atomic_belief_ratio")
    if summary["model_layer_health"]["unscoped_model_ratio"] > 0.02:
        blockers.append("unscoped_model_ratio")
    if summary["learning_pressure"].get("late_trace_pressure"):
        blockers.append("late_trace_pressure")
    if summary["failure_cases"] / max(summary["cases"], 1) > 0.05:
        blockers.append("failure_case_rate")
    tier = "customer_beta" if not blockers else "design_partner_controlled"
    if {"expected_scope_hit_rate", "retrieval_p95_slo"} & set(blockers):
        tier = "internal_dogfood"
    return {
        "tier": tier,
        "blockers": blockers,
        "customer_value": [
            "retrieves scoped company memory from heterogeneous workplace signals",
            "uses prior retrieval outcomes to tune affordances and reader policy",
            "keeps source-specific noise visible instead of blending it into one narrative",
            "surfaces graph/topology candidates for hidden cross-functional links",
        ],
    }


async def _summarize(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    run_id: str,
    results: list[dict[str, Any]],
    seed_status: dict[str, Any],
    elapsed_seconds: float,
) -> dict[str, Any]:
    retrieval = [float(row.get("retrieval_ms") or 0.0) for row in results]
    residual = [
        float((row.get("retrieval_runtime") or {}).get("unaccounted_ms") or 0.0)
        for row in results
    ]
    stage_total = [
        float(row.get("retrieval_stage_timings_ms_total") or 0.0)
        for row in results
    ]
    action_total = [
        float(row.get("retrieval_action_timings_ms_total") or 0.0)
        for row in results
    ]
    precision = [
        float(row.get("expected_scope_precision") or 0.0)
        for row in results
    ]
    recall = [
        float(row.get("expected_scope_recall") or 0.0)
        for row in results
    ]
    context_quality = [
        float(row.get("context_packet_quality_score") or 0.0)
        for row in results
    ]
    questions_per_signal = [
        float(len(row.get("questions") or []))
        for row in results
    ]
    belief_delta_counts = [
        float(row.get("belief_delta_count") or 0.0)
        for row in results
    ]
    belief_delta_question_counts = [
        float(row.get("belief_delta_question_count") or 0.0)
        for row in results
    ]
    llm_candidate_counts = [
        float(row.get("llm_candidate_count") or 0.0)
        for row in results
    ]
    question_planning_modes = Counter(
        str(row.get("question_planning_mode") or "unknown")
        for row in results
    )
    question_modes = Counter(
        str(row.get("question_mode") or "none")
        for row in results
    )
    question_primitives: Counter[str] = Counter()
    context_stat_values: dict[str, list[float]] = defaultdict(list)
    for row in results:
        for question in row.get("questions") or []:
            if isinstance(question, dict):
                question_primitives[str(question.get("primitive") or "unknown")] += 1
        for key, raw_value in (row.get("context_packet_stats") or {}).items():
            try:
                context_stat_values[str(key)].append(float(raw_value or 0.0))
            except (TypeError, ValueError):
                continue
    sage_stage_total: Counter[str] = Counter()
    sage_stage_max: dict[str, int] = {}
    sage_plan_modes: Counter[str] = Counter()
    sage_batch_wall: list[float] = []
    for row in results:
        for stage, raw_value in (row.get("sage_stage_timings_ms_total") or {}).items():
            try:
                sage_stage_total[str(stage)] += int(raw_value or 0)
            except (TypeError, ValueError):
                continue
        for stage, raw_value in (row.get("sage_stage_timings_ms_max") or {}).items():
            try:
                value = int(raw_value or 0)
            except (TypeError, ValueError):
                continue
            sage_stage_max[str(stage)] = max(value, sage_stage_max.get(str(stage), 0))
        for mode, raw_value in (row.get("sage_plan_modes") or {}).items():
            try:
                sage_plan_modes[str(mode)] += int(raw_value or 0)
            except (TypeError, ValueError):
                continue
        for batch in row.get("sage_reader_batches") or []:
            if not isinstance(batch, dict):
                continue
            try:
                sage_batch_wall.append(float(batch.get("elapsed_ms") or 0.0))
            except (TypeError, ValueError):
                continue
    expected_rows = [row for row in results if int(row.get("expected_scoped_models") or 0) > 0]
    expected_hits = [row for row in expected_rows if row.get("expected_scope_hit")]
    failure_rows = [row for row in results if row.get("quality_failure_modes")]
    health = await _model_layer_health(pool, tenant_id)
    summary = {
        "tenant_id": str(tenant_id),
        "run_id": run_id,
        "company": COMPANY_NAME,
        "seed_status": seed_status,
        "cases": len(results),
        "expected_cases": len(expected_rows),
        "expected_scope_hits": len(expected_hits),
        "expected_scope_hit_rate": len(expected_hits) / max(len(expected_rows), 1),
        "expected_scope_precision": _metric_stats(precision),
        "expected_scope_recall": _metric_stats(recall),
        "context_packet_quality_score": _metric_stats(context_quality),
        "context_packet_stats_mean": {
            key: statistics.fmean(values) if values else 0.0
            for key, values in sorted(context_stat_values.items())
        },
        "question_planning_summary": {
            "planning_modes": dict(question_planning_modes.most_common()),
            "question_modes": dict(question_modes.most_common()),
            "question_primitives": dict(question_primitives.most_common()),
            "questions_per_signal": _metric_stats(questions_per_signal),
            "belief_delta_count": _metric_stats(belief_delta_counts),
            "belief_delta_question_count": _metric_stats(
                belief_delta_question_counts
            ),
            "llm_candidate_count": _metric_stats(llm_candidate_counts),
        },
        "failure_cases": len(failure_rows),
        "failure_case_rate": len(failure_rows) / max(len(results), 1),
        "retrieval_ms": {
            "min": min(retrieval) if retrieval else 0.0,
            "mean": statistics.fmean(retrieval) if retrieval else 0.0,
            "median": statistics.median(retrieval) if retrieval else 0.0,
            "p95": _percentile(retrieval, 0.95),
            "p99": _percentile(retrieval, 0.99),
            "max": max(retrieval) if retrieval else 0.0,
        },
        "retrieval_action_timings_ms": {
            "mean": statistics.fmean(action_total) if action_total else 0.0,
            "p95": _percentile(action_total, 0.95),
            "max": max(action_total) if action_total else 0.0,
        },
        "retrieval_stage_timings_ms": {
            "mean": statistics.fmean(stage_total) if stage_total else 0.0,
            "p95": _percentile(stage_total, 0.95),
            "max": max(stage_total) if stage_total else 0.0,
        },
        "retrieval_unaccounted_ms": {
            "mean": statistics.fmean(residual) if residual else 0.0,
            "p95": _percentile(residual, 0.95),
            "max": max(residual) if residual else 0.0,
        },
        "sage_reader_batch_wall_ms": {
            "mean": statistics.fmean(sage_batch_wall) if sage_batch_wall else 0.0,
            "p95": _percentile(sage_batch_wall, 0.95),
            "max": max(sage_batch_wall) if sage_batch_wall else 0.0,
        },
        "sage_plan_modes": dict(sorted(sage_plan_modes.items())),
        "sage_stage_timings_ms_total": dict(sorted(sage_stage_total.items())),
        "sage_stage_timings_ms_max": dict(sorted(sage_stage_max.items())),
        "selected_count": {
            "mean": statistics.fmean([int(r.get("selected_count") or 0) for r in results])
            if results else 0.0,
            "p95": _percentile([int(r.get("selected_count") or 0) for r in results], 0.95),
            "max": max([int(r.get("selected_count") or 0) for r in results] or [0]),
        },
        "evidence_count": {
            "mean": statistics.fmean([int(r.get("evidence_count") or 0) for r in results])
            if results else 0.0,
            "p95": _percentile([int(r.get("evidence_count") or 0) for r in results], 0.95),
            "max": max([int(r.get("evidence_count") or 0) for r in results] or [0]),
        },
        "source_distribution": dict(Counter(str(r.get("source_prefix")) for r in results)),
        "family_distribution": dict(Counter(str(r.get("family")) for r in results)),
        "final_learned_counts": results[-1].get("learned_counts") if results else {},
        "learning_pressure": _learning_pressure(results),
        "pattern_analysis": _summarize_patterns(results),
        "primary_pathway_timings_ms": _primary_pathway_summary(results),
        "model_layer_health": health,
        "elapsed_seconds": round(elapsed_seconds, 3),
    }
    summary["readiness"] = _readiness(summary)
    summary["structural_findings"] = _structural_findings(summary)
    return summary


def _structural_findings(summary: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    health = summary["model_layer_health"]
    patterns = summary["pattern_analysis"]
    if summary["expected_scope_hit_rate"] < 0.98:
        findings.append(
            "Scoped retrieval is not yet reliable enough: expected entity-scoped "
            f"models were hit {summary['expected_scope_hit_rate']:.3f} of the time."
        )
    if summary["retrieval_ms"]["p95"] > 3500.0:
        findings.append(
            "Retrieval tail latency is over the 3.5s architecture SLO; broad "
            "signals and large entity neighborhoods need tighter candidate caps "
            "or better precomputed routing."
        )
    if health["atomic_model_ratio"] < 0.98:
        findings.append(
            "The active model layer contains non-atomic belief shapes. The system "
            "needs stronger proposition splitting before reorganization can stay safe."
        )
    if health["unscoped_model_ratio"] > 0.02:
        findings.append(
            "Too many active models lack scope entities; unscoped beliefs become "
            "retrieval gravity wells in a realistic company corpus."
        )
    if patterns["failure_mode_totals"].get("single_source_context", 0):
        findings.append(
            "Several signals retrieved context from only one source family; hidden "
            "insights need deliberate cross-source bridging, not just semantic recall."
        )
    if patterns["failure_mode_totals"].get("weak_signal_overselected", 0):
        findings.append(
            "Weak/noisy workspace chatter is still selecting too many models in "
            "some cases, which can make low-value activity look operationally important."
        )
    if summary["learning_pressure"].get("late_trace_pressure"):
        findings.append(
            "Late-run feedback is dominated by reader attribution trace growth; "
            "compact utility structures are not keeping up with audit exhaust."
        )
    if not findings:
        findings.append(
            "No blocking structural pattern was detected by this deterministic "
            "probe; remaining risk is the LLM Think cascade measured separately."
        )
    return findings


def _primary_pathway_summary(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in results:
        for item in row.get("retrieval_primary_pathway_timings_top") or []:
            if not isinstance(item, dict):
                continue
            stage = str(item.get("stage") or "unknown")
            try:
                elapsed_ms = float(item.get("elapsed_ms") or 0.0)
            except (TypeError, ValueError):
                continue
            buckets[stage].append(elapsed_ms)
    out: dict[str, dict[str, Any]] = {}
    for stage, values in sorted(
        buckets.items(),
        key=lambda pair: max(pair[1]) if pair[1] else 0.0,
        reverse=True,
    ):
        out[stage] = {
            "count": len(values),
            "mean": statistics.fmean(values) if values else 0.0,
            "p95": _percentile(values, 0.95),
            "max": max(values) if values else 0.0,
        }
    return out


def _metric_stats(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "min": min(values) if values else 0.0,
        "mean": statistics.fmean(values) if values else 0.0,
        "median": statistics.median(values) if values else 0.0,
        "p95": _percentile(values, 0.95),
        "max": max(values) if values else 0.0,
    }


def _render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# 20k Model / 4k Signal Company Probe",
        "",
        f"- Run id: `{summary['run_id']}`",
        f"- Tenant: `{summary['tenant_id']}`",
        f"- Company: {summary['company']}",
        f"- Seeded models: {summary['seed_status'].get('models', 0)}",
        f"- Signals processed incrementally: {summary['cases']}",
        f"- Expected scoped hit rate: {summary['expected_scope_hit_rate']:.3f}",
        f"- Scoped precision mean/p95: "
        f"{summary['expected_scope_precision']['mean']:.3f} / "
        f"{summary['expected_scope_precision']['p95']:.3f}",
        f"- Scoped recall mean/p95: "
        f"{summary['expected_scope_recall']['mean']:.3f} / "
        f"{summary['expected_scope_recall']['p95']:.3f}",
        f"- Context packet quality mean/p95: "
        f"{summary['context_packet_quality_score']['mean']:.3f} / "
        f"{summary['context_packet_quality_score']['p95']:.3f}",
        f"- Question modes: "
        f"{summary['question_planning_summary']['question_modes']}",
        f"- Failure case rate: {summary['failure_case_rate']:.3f}",
        f"- Retrieval mean/p95/p99/max ms: "
        f"{summary['retrieval_ms']['mean']:.1f} / "
        f"{summary['retrieval_ms']['p95']:.1f} / "
        f"{summary['retrieval_ms']['p99']:.1f} / "
        f"{summary['retrieval_ms']['max']:.1f}",
        f"- Retrieval action/stage/unaccounted mean ms: "
        f"{summary['retrieval_action_timings_ms']['mean']:.1f} / "
        f"{summary['retrieval_stage_timings_ms']['mean']:.1f} / "
        f"{summary['retrieval_unaccounted_ms']['mean']:.1f}",
        f"- Readiness: `{summary['readiness']['tier']}`",
        f"- Blockers: {', '.join(summary['readiness']['blockers']) or 'none'}",
        "",
        "## Where The System Provides Value",
        "",
    ]
    for item in summary["readiness"]["customer_value"]:
        lines.append(f"- {item}")
    if summary.get("primary_pathway_timings_ms"):
        lines.extend(["", "## Primary Retrieval Substage Timings", ""])
        for stage, stats in summary["primary_pathway_timings_ms"].items():
            lines.append(
                f"- {stage}: mean={stats['mean']:.1f}ms, "
                f"p95={stats['p95']:.1f}ms, max={stats['max']:.1f}ms, "
                f"count={stats['count']}"
            )
    lines.extend(["", "## Model Layer Health", ""])
    for key in (
        "active_models",
        "archived_models",
        "model_edges",
        "relationship_candidates",
        "latent_topology_candidates",
        "atomic_model_ratio",
        "unscoped_model_ratio",
        "avg_natural_len",
        "p95_natural_len",
    ):
        lines.append(f"- {key}: {summary['model_layer_health'].get(key)}")
    lines.extend(["", "## Learned Layer Counts", ""])
    for key, value in sorted((summary.get("final_learned_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Failure Pattern Analysis", ""])
    for key, value in (summary["pattern_analysis"]["failure_mode_totals"] or {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Worst Families", ""])
    for row in summary["pattern_analysis"]["worst_families"]:
        lines.append(
            f"- {row['family']}: miss_rate={row['expected_scope_miss_rate']:.3f}, "
            f"cases={row['cases']}, failures={row['top_failure_modes']}"
        )
    lines.extend(["", "## Learning Pressure", ""])
    for row in summary["learning_pressure"]["quarters"]:
        lines.append(
            f"- q{row['quarter']} cases {row['case_start']}-{row['case_end']}: "
            f"attr_delta={row['reader_decision_attribution_delta']}, "
            f"compact_learning_delta={row['compact_learning_delta']}, "
            f"ratio={row['attributions_per_new_learning_signal']}, "
            f"pressure={row['trace_pressure']}"
        )
    lines.extend(["", "## Structural Findings", ""])
    for finding in summary["structural_findings"]:
        lines.append(f"- {finding}")
    return "\n".join(lines) + "\n"


def _write_reports(
    *,
    report_dir: Path,
    summary: dict[str, Any],
    results: list[dict[str, Any]],
) -> tuple[Path, Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "run_summary.json"
    jsonl_path = report_dir / "signal_results.jsonl"
    md_path = report_dir / "pattern_analysis.md"
    json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=_json_default),
        encoding="utf-8",
    )
    jsonl_path.write_text(
        "".join(
            json.dumps(row, sort_keys=True, default=_json_default) + "\n"
            for row in results
        ),
        encoding="utf-8",
    )
    md_path.write_text(_render_markdown(summary), encoding="utf-8")
    return json_path, jsonl_path, md_path


async def run(args: argparse.Namespace) -> int:
    run_id = args.run_id or datetime.now(timezone.utc).strftime(
        "company-scale-%Y%m%dT%H%M%SZ"
    )
    report_dir = args.report_root / f"model-layer-{run_id}"
    scenario = build_scenario(args.signals, namespace=run_id)
    llm_provider = _provider_for_mode(args.question_planning_mode)
    provider_metadata = _provider_metadata(
        llm_provider,
        question_planning_mode=args.question_planning_mode,
    )
    pool = await asyncpg.create_pool(
        args.database_url,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
        command_timeout=300,
    )
    embedder = OllamaClient(OllamaConfig.from_env())
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    tenant_id: UUID | None = None
    cleanup_after_run = _cleanup_enabled(args)
    try:
        print(
            json.dumps(
                {"event": "question_planning_config", **provider_metadata},
                sort_keys=True,
            ),
            flush=True,
        )
        if not args.skip_migrations:
            await _ensure_migrations(pool)
        await materialize(scenario, pool=pool)
        assert scenario.tenant_id is not None
        tenant_id = scenario.tenant_id
        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        await _insert_extra_aliases(scenario, alias_repo)

        print(
            json.dumps({
                "event": "seed_start",
                "tenant_id": str(tenant_id),
                "models": args.models,
                "families": args.seed_families,
            }, sort_keys=True),
            flush=True,
        )
        seed_status = await _seed_coherent_company_models(
            pool,
            scenario=scenario,
            tenant_id=tenant_id,
            total_models=args.models,
        )
        seed_status["question_planning"] = provider_metadata
        print(json.dumps({"event": "seed_complete", **seed_status}, sort_keys=True), flush=True)
        analyze_status = await _analyze_probe_tables(pool)
        seed_status["post_seed_analyze"] = analyze_status
        print(
            json.dumps(
                {"event": "post_seed_analyze_complete", **analyze_status},
                sort_keys=True,
            ),
            flush=True,
        )

        for index in range(args.signals):
            observation_ids = await inject_generated_signals(
                scenario,
                pool=pool,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                run_id=run_id,
                progress_every=0,
                offset=index,
                limit=1,
            )
            row = await _process_observation(
                pool,
                tenant_id=tenant_id,
                observation_id=observation_ids[0],
                step_index=index + 1,
                expected_limit=args.expected_model_limit,
                question_planning_mode=args.question_planning_mode,
                llm_provider=llm_provider,
            )
            results.append(row)
            if args.progress_every and (index + 1) % args.progress_every == 0:
                elapsed = time.monotonic() - started
                print(
                    json.dumps({
                        "event": "progress",
                        "processed": index + 1,
                        "signals": args.signals,
                        "elapsed_seconds": round(elapsed, 3),
                        "mean_seconds_per_signal": round(elapsed / (index + 1), 6),
                    }, sort_keys=True),
                    flush=True,
                )

        summary = await _summarize(
            pool=pool,
            tenant_id=tenant_id,
            run_id=run_id,
            results=results,
            seed_status=seed_status,
            elapsed_seconds=time.monotonic() - started,
        )
        json_path, jsonl_path, md_path = _write_reports(
            report_dir=report_dir,
            summary=summary,
            results=results,
        )
        print(
            json.dumps({
                "event": "run_complete",
                "report_dir": str(report_dir),
                "summary_json": str(json_path),
                "results_jsonl": str(jsonl_path),
                "analysis_md": str(md_path),
                "summary": summary,
            }, sort_keys=True, default=_json_default),
            flush=True,
        )
        return 0
    finally:
        try:
            if tenant_id is not None and cleanup_after_run:
                cleanup_status = await _cleanup_probe_tenant(pool, tenant_id)
                cleanup_event = (
                    "cleanup_incomplete"
                    if cleanup_status["blocked_tables"]
                    or cleanup_status["tenant_delete_error"]
                    or not cleanup_status["tenant_deleted"]
                    else "cleanup_complete"
                )
                print(
                    json.dumps({
                        "event": cleanup_event,
                        "run_id": run_id,
                        "report_dir": str(report_dir),
                        **cleanup_status,
                    }, sort_keys=True, default=_json_default),
                    flush=True,
                )
                if (
                    args.compact_db_after_cleanup
                    and _database_url_is_local(str(args.database_url))
                    and cleanup_status["rows_deleted"] > 0
                ):
                    compaction_status = await _compact_local_cleanup_tables(
                        pool,
                        cleanup_status,
                    )
                    print(
                        json.dumps({
                            "event": (
                                "cleanup_compaction_incomplete"
                                if compaction_status["failed_tables"]
                                else "cleanup_compaction_complete"
                            ),
                            "run_id": run_id,
                            "report_dir": str(report_dir),
                            **compaction_status,
                        }, sort_keys=True, default=_json_default),
                        flush=True,
                    )
            elif tenant_id is not None:
                print(
                    json.dumps({
                        "event": "cleanup_skipped",
                        "run_id": run_id,
                        "tenant_id": str(tenant_id),
                        "reason": (
                            "--keep-run-data"
                            if args.cleanup_after_run is False
                            else "non_local_database_url"
                        ),
                    }, sort_keys=True),
                    flush=True,
                )
        finally:
            if llm_provider is not None and llm_provider.config.provider == "codex":
                await close_codex_app_server_client()
            await embedder.close()
            await pool.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL") or LOCAL_DATABASE_URL,
    )
    parser.add_argument("--models", type=int, default=20_000)
    parser.add_argument("--signals", type=int, default=4_000)
    parser.add_argument("--seed-families", type=int, default=160)
    parser.add_argument("--expected-model-limit", type=int, default=48)
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--skip-migrations", action="store_true")
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument(
        "--question-planning-mode",
        choices=(
            "deterministic",
            "codex-low",
            "live-env",
            "scripted-belief-delta",
        ),
        default="deterministic",
        help=(
            "Question planner for inquiry retrieval. `deterministic` is the "
            "baseline; `codex-low` uses the same Codex provider abstraction "
            "as Think with CODEX_REASONING_EFFORT=low."
        ),
    )
    parser.add_argument(
        "--skip-db-compaction",
        dest="compact_db_after_cleanup",
        action="store_false",
        default=True,
        help=(
            "Skip local VACUUM FULL after cleanup. By default, local large "
            "runs delete generated rows and physically compact affected "
            "trace/model tables."
        ),
    )
    cleanup = parser.add_mutually_exclusive_group()
    cleanup.add_argument(
        "--cleanup-after-run",
        dest="cleanup_after_run",
        action="store_true",
        default=None,
        help=(
            "Delete the generated tenant after the report is written. "
            "This is the default for local database URLs."
        ),
    )
    cleanup.add_argument(
        "--keep-run-data",
        dest="cleanup_after_run",
        action="store_false",
        help="Keep the generated tenant rows after the run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.models <= 0:
        raise SystemExit("--models must be positive")
    if args.signals <= 0:
        raise SystemExit("--signals must be positive")
    if args.seed_families <= 0:
        raise SystemExit("--seed-families must be positive")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
