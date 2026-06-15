"""Scenario YAML loader and DB materializer for real-LLM tests."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import yaml

from lib.embeddings.ollama import OllamaClient
from lib.shared.ids import uuid7
from services.domain.acts import commitments as commitments_svc
from services.domain.acts import decisions as decisions_svc
from services.domain.acts import goals as goals_svc
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.resources import customer_commitments as customer_commitments_svc
from services.domain.resources import repo as resources_repo
from services.ingest.synthetic.core import SyntheticSignal, inject


_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCENARIOS_DIR = _REPO_ROOT / "tests" / "real_llm" / "scenarios"


@dataclass
class Scenario:
    """In-memory scenario: raw YAML plus resolved IDs after materialize()."""

    scenario_id: str
    name: str
    description: str
    foundation: dict[str, Any]
    signal_sequences: dict[str, list[dict[str, Any]]]
    expected_behaviors: list[str]
    raw: dict[str, Any] = field(default_factory=dict)

    tenant_id: UUID | None = None
    bootstrap_observation_id: UUID | None = None
    base_time: datetime | None = None

    actors: dict[str, UUID] = field(default_factory=dict)
    customers: dict[str, UUID] = field(default_factory=dict)
    goals: dict[str, UUID] = field(default_factory=dict)
    commitments: dict[str, UUID] = field(default_factory=dict)
    decisions: dict[str, UUID] = field(default_factory=dict)

    def actor_id(self, name: str) -> UUID:
        try:
            return self.actors[name]
        except KeyError as e:
            raise KeyError(
                f"actor {name!r} not in scenario {self.scenario_id!r}; "
                f"known: {sorted(self.actors)}"
            ) from e

    def customer_id(self, name: str) -> UUID:
        try:
            return self.customers[name]
        except KeyError as e:
            raise KeyError(
                f"customer {name!r} not in scenario {self.scenario_id!r}; "
                f"known: {sorted(self.customers)}"
            ) from e

    def goal_id(self, title: str) -> UUID:
        try:
            return self.goals[title]
        except KeyError as e:
            raise KeyError(
                f"goal {title!r} not in scenario {self.scenario_id!r}; "
                f"known: {sorted(self.goals)}"
            ) from e

    def commitment_id(self, title: str) -> UUID:
        try:
            return self.commitments[title]
        except KeyError as e:
            raise KeyError(
                f"commitment {title!r} not in scenario {self.scenario_id!r}; "
                f"known: {sorted(self.commitments)}"
            ) from e

    def decision_id(self, title: str) -> UUID:
        try:
            return self.decisions[title]
        except KeyError as e:
            raise KeyError(
                f"decision {title!r} not in scenario {self.scenario_id!r}; "
                f"known: {sorted(self.decisions)}"
            ) from e

    def get_sequence(self, name: str) -> list[dict[str, Any]]:
        try:
            return self.signal_sequences[name]
        except KeyError as e:
            raise KeyError(
                f"signal_sequence {name!r} not in scenario "
                f"{self.scenario_id!r}; known: {sorted(self.signal_sequences)}"
            ) from e


def load_scenario(scenario_id: str) -> Scenario:
    """Load a scenario YAML file by its scenario_id (matches `*_<id>.yaml`)."""
    matches = sorted(_SCENARIOS_DIR.glob(f"*_{scenario_id}.yaml"))
    if not matches:
        # Fall back to exact-name match for callers that pass a leading number.
        exact = _SCENARIOS_DIR / f"{scenario_id}.yaml"
        if exact.exists():
            matches = [exact]
    if not matches:
        raise FileNotFoundError(
            f"scenario {scenario_id!r} not found under {_SCENARIOS_DIR}"
        )
    if len(matches) > 1:
        raise ValueError(
            f"ambiguous scenario_id {scenario_id!r}: matched {matches}"
        )
    raw = yaml.safe_load(matches[0].read_text()) or {}
    sequences_raw = raw.get("signal_sequences") or {}
    sequences: dict[str, list[dict[str, Any]]] = {}
    for seq_name, seq_body in sequences_raw.items():
        if isinstance(seq_body, dict):
            sequences[seq_name] = list(seq_body.get("signals") or [])
        else:
            sequences[seq_name] = list(seq_body or [])
    return Scenario(
        scenario_id=raw.get("scenario_id", scenario_id),
        name=raw.get("scenario_name", raw.get("name", scenario_id)),
        description=raw.get("description", ""),
        foundation=raw.get("foundation") or {},
        signal_sequences=sequences,
        expected_behaviors=list(raw.get("expected_behaviors") or []),
        raw=raw,
    )


async def materialize(scenario: Scenario, *, pool: asyncpg.Pool) -> None:
    """Create tenant, bootstrap observation, and all foundation entities atomically."""
    tenant_id = uuid7()
    scenario.tenant_id = tenant_id
    base_time = datetime.now(timezone.utc)
    scenario.base_time = base_time

    foundation = scenario.foundation
    actors_def, customers_def, goals_def, commitments_def, decisions_def = (
        _foundation_entity_defs(foundation)
    )
    actor_repo = ActorRepo(pool)
    alias_repo = EntityAliasRepo(pool)
    pending_aliases: list[tuple[str, dict[str, Any], float]] = []
    linked_customer_commitments: set[tuple[UUID, UUID]] = set()

    await _create_scenario_tenant(pool, scenario=scenario, tenant_id=tenant_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            bootstrap_id = await _insert_bootstrap_observation(
                conn,
                scenario=scenario,
                tenant_id=tenant_id,
            )
            scenario.bootstrap_observation_id = bootstrap_id
            await _materialize_actors(
                scenario,
                actors_def=actors_def,
                actor_repo=actor_repo,
                tenant_id=tenant_id,
            )
            await _materialize_customers(
                scenario,
                customers_def=customers_def,
                pending_aliases=pending_aliases,
                tenant_id=tenant_id,
                bootstrap_id=bootstrap_id,
                base_time=base_time,
                conn=conn,
            )
            await _materialize_goals(
                scenario,
                goals_def=goals_def,
                pending_aliases=pending_aliases,
                tenant_id=tenant_id,
                bootstrap_id=bootstrap_id,
                base_time=base_time,
                conn=conn,
            )
            await _materialize_commitments(
                scenario,
                commitments_def=commitments_def,
                pending_aliases=pending_aliases,
                tenant_id=tenant_id,
                bootstrap_id=bootstrap_id,
                base_time=base_time,
                conn=conn,
            )
            await _materialize_decisions(
                scenario,
                decisions_def=decisions_def,
                pending_aliases=pending_aliases,
                tenant_id=tenant_id,
                bootstrap_id=bootstrap_id,
                conn=conn,
            )
            await _materialize_customer_commitment_links(
                scenario,
                customer_commitment_links=foundation.get("customer_commitments") or [],
                linked_customer_commitments=linked_customer_commitments,
                tenant_id=tenant_id,
                conn=conn,
            )
            await _infer_customer_commitment_links(
                scenario,
                commitments_def=commitments_def,
                linked_customer_commitments=linked_customer_commitments,
                tenant_id=tenant_id,
                conn=conn,
            )

    await _insert_pending_aliases(
        scenario,
        alias_repo=alias_repo,
        pending_aliases=pending_aliases,
        tenant_id=tenant_id,
    )


def _foundation_entity_defs(
    foundation: dict[str, Any],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    return (
        foundation.get("actors") or [],
        foundation.get("customers") or [],
        foundation.get("goals") or [],
        foundation.get("commitments") or [],
        foundation.get("decisions") or [],
    )


async def _create_scenario_tenant(
    pool: asyncpg.Pool,
    *,
    scenario: Scenario,
    tenant_id: UUID,
) -> None:
    # ActorRepo and a few foundation services write through their own pool
    # connections, so the tenant must be committed before the transaction below.
    await pool.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, $2, FALSE)
        ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        f"real-llm:{scenario.scenario_id}",
    )


async def _insert_bootstrap_observation(
    conn: asyncpg.Connection,
    *,
    scenario: Scenario,
    tenant_id: UUID,
) -> UUID:
    bootstrap_id = uuid7()
    now = datetime.now(timezone.utc)
    bootstrap_content = {
        "synthetic": True,
        "scenario_id": scenario.scenario_id,
        "purpose": "scenario_loader_bootstrap",
    }
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, ingested_at, kind,
          source_channel, content, content_text, trust_tier,
          entities_mentioned
        ) VALUES (
          $1, $2, $3, $3, 'signal',
          'internal:scenario_loader', $4::jsonb, $5, 'authoritative',
          $6::jsonb
        )
        """,
        bootstrap_id,
        tenant_id,
        now,
        json.dumps(bootstrap_content),
        f"scenario {scenario.scenario_id} bootstrap",
        json.dumps([]),
    )
    return bootstrap_id


async def _materialize_actors(
    scenario: Scenario,
    *,
    actors_def: list[dict[str, Any]],
    actor_repo: ActorRepo,
    tenant_id: UUID,
) -> None:
    for actor_def in actors_def:
        name = actor_def["name"]
        row = await actor_repo.create_actor(
            email=actor_def.get("email"),
            display_name=name,
            type=actor_def.get("kind", "human_internal"),
            tenant_id=tenant_id,
            nexus_attested=bool(actor_def.get("nexus_attested", False)),
            metadata={
                "role": actor_def.get("role"),
                "scenario_actor_name": name,
            },
        )
        scenario.actors[name] = row.id
        await _add_actor_identity_mappings(actor_repo, actor_def, actor_id=row.id)


async def _add_actor_identity_mappings(
    actor_repo: ActorRepo,
    actor_def: dict[str, Any],
    *,
    actor_id: UUID,
) -> None:
    for alias_field in ("slack", "github", "email_alias", "linear"):
        raw_ref = actor_def.get(alias_field)
        if not raw_ref or ":" not in raw_ref:
            continue
        channel, _, ref = raw_ref.partition(":")
        await actor_repo.add_identity_mapping(
            actor_id=actor_id,
            source_channel=channel,
            source_actor_ref=ref,
        )


async def _materialize_customers(
    scenario: Scenario,
    *,
    customers_def: list[dict[str, Any]],
    pending_aliases: list[tuple[str, dict[str, Any], float]],
    tenant_id: UUID,
    bootstrap_id: UUID,
    base_time: datetime,
    conn: asyncpg.Connection,
) -> None:
    for customer_def in customers_def:
        name = customer_def["name"]
        contract_start_days_ago = int(customer_def.get("contract_start_days_ago", 0))
        contract_start = (base_time - timedelta(days=contract_start_days_ago)).isoformat()
        resource = await resources_repo.create(
            kind="relational",
            identity=name,
            description=customer_def.get("description", f"Customer: {name}"),
            current_value={
                "arr_usd": customer_def.get("arr_usd"),
                "health": customer_def.get("health", "healthy"),
                "contract_start": contract_start,
            },
            metadata={"scenario_customer_name": name},
            created_by_event_id=bootstrap_id,
            tenant_id=tenant_id,
            conn=conn,
        )
        scenario.customers[name] = resource.id
        pending_aliases.extend(
            _entity_aliases_for_name(
                name,
                {"type": "customer", "id": str(resource.id)},
                confidence=0.98,
            )
        )


async def _materialize_goals(
    scenario: Scenario,
    *,
    goals_def: list[dict[str, Any]],
    pending_aliases: list[tuple[str, dict[str, Any], float]],
    tenant_id: UUID,
    bootstrap_id: UUID,
    base_time: datetime,
    conn: asyncpg.Connection,
) -> None:
    for goal_def in _order_goals_by_parent(goals_def):
        title = goal_def["title"]
        parent_id = _resolve_goal_parent_id(scenario, goal_def)
        target_date: datetime | None = None
        if "target_days_from_start" in goal_def:
            target_date = base_time + timedelta(
                days=int(goal_def["target_days_from_start"])
            )
        row = await goals_svc.create(
            title=title,
            description=goal_def.get("description"),
            parent_goal_id=parent_id,
            altitude=goal_def.get("altitude", "operational"),
            success_criteria=goal_def.get("success_criteria"),
            target_date=target_date,
            created_by_event_id=bootstrap_id,
            tenant_id=tenant_id,
            conn=conn,
        )
        scenario.goals[title] = row.id
        pending_aliases.append((title, {"type": "goal", "id": str(row.id)}, 0.95))


def _resolve_goal_parent_id(
    scenario: Scenario,
    goal_def: dict[str, Any],
) -> UUID | None:
    parent_title = goal_def.get("parent")
    if not parent_title:
        return None
    parent_id = scenario.goals.get(parent_title)
    if parent_id is None:
        raise ValueError(
            f"goal {goal_def['title']!r} references unknown parent {parent_title!r}"
        )
    return parent_id


async def _materialize_commitments(
    scenario: Scenario,
    *,
    commitments_def: list[dict[str, Any]],
    pending_aliases: list[tuple[str, dict[str, Any], float]],
    tenant_id: UUID,
    bootstrap_id: UUID,
    base_time: datetime,
    conn: asyncpg.Connection,
) -> None:
    for commitment_def in commitments_def:
        title = commitment_def["title"]
        owner_id = _resolve_commitment_owner_id(scenario, commitment_def)
        due_date: datetime | None = None
        if "due_days_from_start" in commitment_def:
            due_date = base_time + timedelta(
                days=int(commitment_def["due_days_from_start"])
            )
        contributes = _resolve_commitment_goal_refs(scenario, commitment_def)
        state_val = commitment_def.get("state", "proposed")
        row = await commitments_svc.create(
            title=title,
            description=commitment_def.get("description"),
            initial_state=state_val,
            owner_id=owner_id,
            due_date=due_date,
            ambition_level=commitment_def.get("ambition_level", "base"),
            priority=int(commitment_def.get("priority", 5)),
            success_criteria=commitment_def.get("success_criteria"),
            contributes_to_goal_ids=contributes,
            estimated_capacity=_commitment_estimated_capacity(
                commitment_def,
                state_val=state_val,
                contributes=contributes,
            ),
            created_by_event_id=bootstrap_id,
            tenant_id=tenant_id,
            conn=conn,
        )
        scenario.commitments[title] = row.id
        pending_aliases.append(
            (title, {"type": "commitment", "id": str(row.id)}, 0.95)
        )


def _resolve_commitment_owner_id(
    scenario: Scenario,
    commitment_def: dict[str, Any],
) -> UUID | None:
    owner_name = commitment_def.get("owner")
    owner_id = scenario.actors.get(owner_name) if owner_name else None
    if owner_name and owner_id is None:
        raise ValueError(
            f"commitment {commitment_def['title']!r} references unknown owner "
            f"{owner_name!r}"
        )
    return owner_id


def _resolve_commitment_goal_refs(
    scenario: Scenario,
    commitment_def: dict[str, Any],
) -> list[UUID | tuple[UUID, bool]]:
    contributes_raw = commitment_def.get("contributes_to_goal")
    if contributes_raw is None:
        contributes_list: list[Any] = []
    elif isinstance(contributes_raw, list):
        contributes_list = contributes_raw
    else:
        contributes_list = [contributes_raw]

    contributes: list[UUID | tuple[UUID, bool]] = []
    for entry in contributes_list:
        if isinstance(entry, dict):
            contributes.append(
                (scenario.goal_id(entry["title"]), bool(entry.get("critical_path", False)))
            )
        else:
            contributes.append(scenario.goal_id(str(entry)))
    return contributes


def _commitment_estimated_capacity(
    commitment_def: dict[str, Any],
    *,
    state_val: str,
    contributes: list[UUID | tuple[UUID, bool]],
) -> Any:
    estimated_capacity = commitment_def.get("estimated_capacity")
    non_terminal_non_proposed = state_val not in (
        "proposed",
        "doneverified",
        "closed",
    )
    if non_terminal_non_proposed and not contributes and estimated_capacity is None:
        return {"maintenance": True}
    return estimated_capacity


async def _materialize_decisions(
    scenario: Scenario,
    *,
    decisions_def: list[dict[str, Any]],
    pending_aliases: list[tuple[str, dict[str, Any], float]],
    tenant_id: UUID,
    bootstrap_id: UUID,
    conn: asyncpg.Connection,
) -> None:
    for decision_def in decisions_def:
        title = decision_def["title"]
        row = await decisions_svc.create(
            title=title,
            decision_text=decision_def.get(
                "decision_text",
                decision_def.get("text", title),
            ),
            rationale=decision_def.get("rationale"),
            state=decision_def.get("state", "drafted"),
            scope=decision_def.get("scope"),
            revisit_triggers=decision_def.get("revisit_triggers"),
            created_by_event_id=bootstrap_id,
            tenant_id=tenant_id,
            conn=conn,
        )
        scenario.decisions[title] = row.id
        pending_aliases.append((title, {"type": "decision", "id": str(row.id)}, 0.95))


async def _materialize_customer_commitment_links(
    scenario: Scenario,
    *,
    customer_commitment_links: list[dict[str, Any]],
    linked_customer_commitments: set[tuple[UUID, UUID]],
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> None:
    for link_def in customer_commitment_links:
        customer_id = scenario.customer_id(link_def["customer"])
        commitment_id = scenario.commitment_id(link_def["commitment"])
        await customer_commitments_svc.link_commitment(
            customer_id,
            commitment_id,
            tenant_id=tenant_id,
            relationship_kind=link_def.get("relationship_kind", "delivers"),
            revenue_at_risk_usd=link_def.get("revenue_at_risk_usd"),
            criticality=link_def.get("criticality", "medium"),
            served_description=link_def.get("served_description"),
            conn=conn,
        )
        linked_customer_commitments.add((customer_id, commitment_id))


async def _infer_customer_commitment_links(
    scenario: Scenario,
    *,
    commitments_def: list[dict[str, Any]],
    linked_customer_commitments: set[tuple[UUID, UUID]],
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> None:
    for customer_name, customer_id in scenario.customers.items():
        aliases = _alias_phrases_for_name(customer_name)
        for commitment_def in commitments_def:
            title = commitment_def["title"]
            commitment_id = scenario.commitment_id(title)
            if (customer_id, commitment_id) in linked_customer_commitments:
                continue
            searchable = " ".join(
                str(part or "") for part in (title, commitment_def.get("description"))
            ).casefold()
            if not any(alias.casefold() in searchable for alias in aliases):
                continue
            await customer_commitments_svc.link_commitment(
                customer_id,
                commitment_id,
                tenant_id=tenant_id,
                relationship_kind="impacts",
                criticality="high",
                served_description=f"Inferred from scenario commitment title: {title}",
                conn=conn,
            )
            linked_customer_commitments.add((customer_id, commitment_id))


async def _insert_pending_aliases(
    scenario: Scenario,
    *,
    alias_repo: EntityAliasRepo,
    pending_aliases: list[tuple[str, dict[str, Any], float]],
    tenant_id: UUID,
) -> None:
    for phrase, ref, confidence in _dedupe_aliases(pending_aliases):
        await alias_repo.insert_alias(
            phrase=phrase,
            resolved_entity_ref=ref,
            source="manual",
            confidence=confidence,
            tenant_id=tenant_id,
            extra_metadata={"scenario_id": scenario.scenario_id},
        )


def _order_goals_by_parent(
    goals_def: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Topological-ish sort: goals with no parent first, then dependents."""
    by_title = {g["title"]: g for g in goals_def}
    ordered: list[dict[str, Any]] = []
    visited: set[str] = set()

    def visit(g: dict[str, Any], stack: tuple[str, ...]) -> None:
        title = g["title"]
        if title in visited:
            return
        if title in stack:
            raise ValueError(
                f"cycle in goal parent chain: {' -> '.join(stack + (title,))}"
            )
        parent = g.get("parent")
        if parent and parent in by_title:
            visit(by_title[parent], stack + (title,))
        visited.add(title)
        ordered.append(g)

    for g in goals_def:
        visit(g, ())
    return ordered


def _entity_aliases_for_name(
    name: str,
    ref: dict[str, Any],
    *,
    confidence: float,
) -> list[tuple[str, dict[str, Any], float]]:
    aliases = [(phrase, ref, confidence) for phrase in _alias_phrases_for_name(name)]
    if aliases:
        aliases[0] = (aliases[0][0], aliases[0][1], confidence)
    if len(aliases) > 1:
        aliases[1] = (aliases[1][0], aliases[1][1], confidence - 0.03)
    return aliases


def _alias_phrases_for_name(name: str) -> list[str]:
    aliases = [name]
    suffixes = (" inc", " corp", " llc", " ltd", " incorporated")
    folded = name.casefold()
    for suffix in suffixes:
        if folded.endswith(suffix):
            aliases.append(name[: -len(suffix)].strip())
            break
    return [a for a in aliases if a.strip()]


def _dedupe_aliases(
    aliases: list[tuple[str, dict[str, Any], float]],
) -> list[tuple[str, dict[str, Any], float]]:
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, dict[str, Any], float]] = []
    for phrase, ref, confidence in aliases:
        cleaned = phrase.strip()
        if not cleaned:
            continue
        key = (cleaned.casefold(), json.dumps(ref, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        result.append((cleaned, ref, confidence))
    return result


async def inject_sequence(
    scenario: Scenario,
    sequence_name: str,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    time_compression: float = 0.0,
    run_id: str | None = None,
) -> list[UUID]:
    """Inject a named signal sequence; returns the resulting observation IDs."""
    if scenario.tenant_id is None:
        raise RuntimeError(
            "Scenario must be materialized before inject_sequence()"
        )
    sequence = scenario.get_sequence(sequence_name)
    base = scenario.base_time or datetime.now(timezone.utc)
    cumulative_minutes = 0.0
    obs_ids: list[UUID] = []
    prev_delay = 0.0
    for index, signal_def in enumerate(sequence):
        delay_minutes = float(signal_def.get("delay_minutes", 0))
        wall_sleep_seconds = max(
            0.0, (delay_minutes - prev_delay) * 60.0 * time_compression
        )
        if wall_sleep_seconds > 0:
            await asyncio.sleep(wall_sleep_seconds)
        prev_delay = delay_minutes
        cumulative_minutes = delay_minutes
        occurred_at = base + timedelta(minutes=cumulative_minutes)

        channel = signal_def["channel"]
        actor_ref = _resolve_actor_ref(signal_def.get("actor"), scenario)
        content_text = signal_def.get("content") or signal_def.get("text") or ""
        content_dict = dict(signal_def.get("content_dict") or {})
        content_dict.setdefault("text", content_text)
        if "thread_of" in signal_def:
            content_dict["thread_of_index"] = int(signal_def["thread_of"])

        external_id = signal_def.get(
            "external_id",
            f"{scenario.scenario_id}:{sequence_name}:{index}:{uuid7()}",
        )
        signal = SyntheticSignal(
            source_channel=channel,
            content_text=content_text,
            content=content_dict,
            occurred_at=occurred_at,
            source_actor_ref=actor_ref,
            external_id=external_id,
            entities_hint=list(signal_def.get("entities_hint") or []),
            trust_tier=signal_def.get("trust_tier"),
            kind=signal_def.get("kind", "signal"),
            scenario_id=scenario.scenario_id,
            run_id=run_id,
        )
        result = await inject(
            signal,
            scenario.tenant_id,
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
        )
        obs_ids.append(result.observation.id)
    return obs_ids


def _resolve_actor_ref(raw: Any, scenario: Scenario) -> str | None:
    """Translate a YAML 'actor' field into a `<channel>:<ref>` string."""
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    if ":" in raw:
        # Already in `<channel>:<ref>` form (e.g. `external:globex.contact`).
        return raw
    actor_def_list = scenario.foundation.get("actors") or []
    for actor_def in actor_def_list:
        if actor_def.get("name") != raw:
            continue
        for field_name in ("slack", "github", "email_alias", "linear"):
            ref = actor_def.get(field_name)
            if ref and ":" in ref:
                return ref
    return None


__all__ = [
    "Scenario",
    "load_scenario",
    "materialize",
    "inject_sequence",
]
