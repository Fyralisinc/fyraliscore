#!/usr/bin/env python3
"""Run large-memory real-LLM situation formation checks.

Each case:
  * materializes a realistic company tenant,
  * seeds a large active Model corpus across the four-stance grammar,
  * injects one compound signal through production ingestion,
  * processes the real T1 Think path with the configured live provider,
  * verifies a queryable situation Model and composition sidecar.

Reports land under tests/real_llm/reports/runs/large-situation-*.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("LLM_CACHE_BYPASS", "1")

import asyncpg
from dotenv import load_dotenv

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.db_bootstrap import _register_codecs
from services.synthetic.core import SyntheticSignal, inject
from services.think.text_embedding import deterministic_text_embedding
from tests.real_llm.infrastructure.scenario_loader import materialize

from scripts.run_1000_signal_model_layer_probe import (
    _build_cached_provider,
    build_scenario,
)
from scripts.run_100_signal_real_llm_e2e import (
    run_signal_t1_triggers_until_complete,
)


load_dotenv(REPO_ROOT / ".env", override=False)


CASE_DEFINITIONS = [
    {
        "name": "renewal_compliance_pressure",
        "customer": "HarborRail Transit",
        "text": (
            "HarborRail procurement evidence is delayed, sponsor confidence "
            "is dropping, ARR renewal is at risk, SOC2 audit review is "
            "blocked, and the implementation team is overloaded."
        ),
    },
    {
        "name": "capacity_trust_pressure",
        "customer": "Borealis Bank",
        "text": (
            "Borealis Bank escalation volume is rising, the migration owner "
            "is out next week, trust in the timeline is weakening, security "
            "questionnaire answers are incomplete, and expansion approval is "
            "now waiting on delivery proof."
        ),
    },
    {
        "name": "market_execution_pressure",
        "customer": "Lumina Telecom",
        "text": (
            "Lumina Telecom is comparing a competitor, platform latency is "
            "visible in the pilot, the sales champion is asking for an "
            "executive plan, implementation milestones are slipping, and the "
            "commercial review may pause without a reliability story."
        ),
    },
    {
        "name": "resource_decision_pressure",
        "customer": "Granite Insurance",
        "text": (
            "Granite Insurance needs a go/no-go decision, the data residency "
            "workstream lacks a reviewer, support backlog is consuming the "
            "same engineers, the renewal discount is unresolved, and legal "
            "approval depends on evidence arriving this week."
        ),
    },
    {
        "name": "multi_customer_revenue_pressure",
        "customer": "Atlas Retail Group",
        "related_customers": ["DeltaFleet Logistics", "Cobalt Health Network"],
        "text": (
            "Atlas Retail Group renewal is at risk, DeltaFleet is reporting "
            "the same data freshness issue, Cobalt Health wants a mitigation "
            "plan, support capacity is saturated, and three expansion deals "
            "now depend on the same reliability fix."
        ),
    },
    {
        "name": "contradictory_churn_signal",
        "customer": "FoundryWorks Manufacturing",
        "text": (
            "FoundryWorks Manufacturing is marked healthy in the QBR deck, "
            "but their plant manager says connector reliability is still "
            "blocking rollout, the sponsor is questioning the success plan, "
            "and finance is holding the expansion invoice."
        ),
    },
    {
        "name": "ambiguous_alias_pressure",
        "customer": "Northstar Labs",
        "related_customers": ["Borealis Bank"],
        "text": (
            "The bank account note is ambiguous between Northstar and Borealis, but "
            "the note says Northstar Labs has the open security exception, the "
            "migration owner changed twice, the renewal committee wants proof, "
            "and support is asking whether this is the same identity-mapping issue."
        ),
    },
    {
        "name": "stale_replay_vs_new_risk",
        "customer": "IonPay",
        "text": (
            "IonPay received a stale replay of last month's latency alert, "
            "but today's pager data shows a new payment reconciliation delay, "
            "the champion is asking for an incident timeline, and procurement "
            "will not approve expansion until the fresh issue is separated."
        ),
    },
    {
        "name": "exec_decision_reversal_pressure",
        "customer": "Jupiter Foods",
        "text": (
            "Jupiter Foods was excluded from custom workflow work by the "
            "standardization decision, but the CRO now wants an exception, "
            "implementation says it will displace the audit export, and legal "
            "needs a decision before renewal pricing expires."
        ),
    },
    {
        "name": "security_packet_dependency_chain",
        "customer": "Cobalt Health Network",
        "text": (
            "Cobalt Health Network's security packet is missing HIPAA evidence, "
            "the mitigation owner is overloaded, the same packet blocks their "
            "go-live and the Atlas reliability fix narrative, and the customer "
            "is asking for an executive escalation path."
        ),
    },
    {
        "name": "public_sector_compliance_freeze",
        "customer": "Meridian Public Sector",
        "text": (
            "Meridian Public Sector has frozen launch because data residency "
            "controls lack an audit owner, procurement requires a signed "
            "access-control memo, the federal champion is traveling, and the "
            "launch plan now depends on legal and platform in the same week."
        ),
    },
    {
        "name": "custom_workflow_capacity_trap",
        "customer": "Keystone Robotics",
        "text": (
            "Keystone Robotics wants a bespoke workflow, the product decision "
            "says no exceptions, sales is promising it in the expansion plan, "
            "and the platform team says it would consume the same reviewers "
            "needed for SAML group mapping."
        ),
    },
    {
        "name": "forecast_quality_and_revenue_slip",
        "customer": "Northstar Labs",
        "text": (
            "Northstar Labs expansion probability increased in Salesforce, "
            "but usage telemetry fell, the forecast owner cannot explain the "
            "gap, the renewal package is being repriced, and finance wants a "
            "source-backed confidence update before board reporting."
        ),
    },
    {
        "name": "ecosystem_partner_blocker",
        "customer": "Evergreen Energy",
        "text": (
            "Evergreen Energy's data residency review now depends on an "
            "ecosystem connector partner, the partner API is rate-limiting, "
            "the support backlog is using the same integration lead, and the "
            "customer wants a customer-visible timeline by Friday."
        ),
    },
    {
        "name": "repeat_incident_customer_timeline",
        "customer": "DeltaFleet Logistics",
        "text": (
            "DeltaFleet Logistics reported a repeat data freshness incident, "
            "the customer-visible timeline is missing owner names, Atlas is "
            "seeing the same symptom, support is saturated, and the incident "
            "response decision may need to be revisited."
        ),
        "related_customers": ["Atlas Retail Group"],
    },
    {
        "name": "board_metric_conflict",
        "customer": "Borealis Bank",
        "text": (
            "Borealis Bank looks green in the board renewal rollup, but the "
            "migration owner reports three unresolved escalations, the "
            "expansion committee is waiting for delivery proof, and the trust "
            "recovery goal is no longer aligned with the dashboard status."
        ),
    },
    {
        "name": "legal_redline_and_support_overlap",
        "customer": "Granite Insurance",
        "text": (
            "Granite Insurance accepted the liability cap only if audit trails "
            "are customer-visible, but support backlog is consuming the audit "
            "engineers, the renewal discount is unresolved, and legal approval "
            "is now tied to the same Friday evidence package."
        ),
    },
    {
        "name": "multi_team_owner_gap",
        "customer": "Lumina Telecom",
        "text": (
            "Lumina Telecom's commercial review needs a reliability story, "
            "platform owns latency, implementation owns milestones, sales owns "
            "the executive plan, and no single owner is accountable for the "
            "combined competitor displacement risk."
        ),
    },
    {
        "name": "pricing_exception_policy_pressure",
        "customer": "Atlas Retail Group",
        "text": (
            "Atlas Retail Group needs a pricing exception, the policy says "
            "revenue-at-risk beats activity volume, customer success says the "
            "renewal is fragile, finance wants margin protection, and product "
            "says the reliability fix is still the binding constraint."
        ),
    },
    {
        "name": "silent_capacity_degradation",
        "customer": "HarborRail Transit",
        "text": (
            "HarborRail Transit has no new escalation email, but cycle time on "
            "procurement evidence doubled, the SOC2 reviewer is split across "
            "three accounts, sponsor confidence is drifting down, and the "
            "implementation team is absorbing unplanned support work."
        ),
    },
    {
        "name": "cash_burn_vs_customer_commitment",
        "customer": "IonPay",
        "text": (
            "IonPay needs payment reconciliation fixes before expansion, but "
            "leadership is freezing nonessential spend, the same engineers are "
            "assigned to burn reduction work, and the customer commitment may "
            "miss unless the resource decision changes."
        ),
    },
    {
        "name": "cross_customer_pattern_without_single_owner",
        "customer": "Cobalt Health Network",
        "related_customers": ["Atlas Retail Group", "DeltaFleet Logistics"],
        "text": (
            "Cobalt Health, Atlas, and DeltaFleet all reference freshness or "
            "audit evidence failures, each team thinks their account is "
            "isolated, support capacity is saturated, and there is no shared "
            "owner for the emerging customer-trust pattern."
        ),
    },
]


@dataclass(frozen=True)
class RunnerConfig:
    case_count: int = 20
    seed_models_per_case: int = 2000
    think_timeout: int = 900
    post_commit_timeout: int = 300
    pool_max_size: int = 8
    run_id: str | None = None
    report_root: Path = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _seed_proposition(index: int, customer: str) -> dict[str, Any]:
    mod = index % 8
    if mod == 0:
        return {
            "kind": "belief",
            "claim_role": "concern",
            "polarity": "negative",
            "about": customer,
            "nature": f"{customer} operational risk signal #{index}",
            "raised_by": "large-situation-seed",
        }
    if mod == 1:
        return {
            "kind": "belief",
            "claim_role": "fact",
            "subject": customer,
            "assertion": f"{customer} execution status signal #{index}",
        }
    if mod == 2:
        return {
            "kind": "prediction",
            "expected": f"{customer} renewal pressure will move by checkpoint #{index}",
            "resolution": "Check CRM renewal state and implementation status.",
        }
    if mod == 3:
        return {
            "kind": "belief",
            "claim_role": "pattern",
            "abstraction_level": "pattern",
            "observed_tendency": (
                f"{customer} risk signals recur when evidence and capacity collide"
            ),
        }
    if mod == 4:
        return {
            "kind": "belief",
            "claim_role": "capability",
            "subject": "implementation team",
            "assessment": f"capacity surface for {customer} is constrained #{index}",
        }
    if mod == 5:
        return {
            "kind": "belief",
            "claim_role": "hypothesis",
            "hypothesis_text": (
                f"{customer} risk is mediated by shared delivery bottleneck #{index}"
            ),
        }
    if mod == 6:
        return {
            "kind": "belief",
            "claim_role": "relation",
            "abstraction_level": "relationship",
            "subject": customer,
            "relation": "depends_on",
            "object": f"evidence stream #{index % 17}",
        }
    return {
        "kind": "norm",
        "claim_role": "recommendation",
        "target_actor_id": str(uuid7()),
        "proposed_change": {
            "operation": "create",
            "payload": {"title": f"Review {customer} pressure #{index}"},
        },
        "qualitative_impact": "Improves operational response quality.",
    }


async def _seed_large_model_set(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    customer: str,
    scope_entities: list[dict[str, str]],
    count: int,
) -> list[UUID]:
    model_ids: list[UUID] = []
    rows: list[tuple[Any, ...]] = []
    for index in range(count):
        model_id = uuid7()
        natural = (
            f"{customer} large-memory seed model {index}: "
            "renewal, compliance, delivery, trust, capacity, and execution "
            "signals provide background context."
        )
        prop = _seed_proposition(index, customer)
        rows.append(
            (
                model_id,
                tenant_id,
                observation_id,
                _json(prop),
                natural,
                deterministic_text_embedding(natural),
                _json(scope_entities),
                0.42 + ((index % 40) / 100.0),
                0.42 + ((index % 40) / 100.0),
            )
        )
        model_ids.append(model_id)

    await conn.executemany(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_actors, scope_entities, scope_temporal,
          confidence, activation, falsifier, signal_readings,
          reading_contestable, supporting_event_ids, supporting_model_ids,
          evidential_weight, status, confidence_at_assertion,
          activation_coefficient
        ) VALUES (
          $1, $2, $3, $4::jsonb, $5, $6,
          '{}'::uuid[], $7::jsonb,
          '{"kind":"open_ended"}'::jsonb,
          $8, 1.0, NULL, '[]'::jsonb, TRUE,
          ARRAY[$3]::uuid[], '{}'::uuid[], 0.5, 'active', $9, 1.0
        )
        """,
        rows,
    )
    return model_ids


def _case_scope_entities(case: dict[str, Any], scenario: Any) -> list[dict[str, str]]:
    """Return real materialized customer Resource refs for a case."""
    entities: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    customer_names = [case["customer"], *(case.get("related_customers") or [])]
    for name in customer_names:
        try:
            ref = {"type": "customer", "id": str(scenario.customer_id(name))}
        except KeyError:
            continue
        key = (ref["type"], ref["id"])
        if key in seen:
            continue
        seen.add(key)
        entities.append(ref)
    return entities


async def _insert_seed_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    run_id: str,
    case_name: str,
) -> UUID:
    obs_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, embedding, embedding_pending,
          trust_tier, external_id, entities_mentioned
        ) VALUES (
          $1, $2, now(), 'signal', 'large-situation-seed',
          $3::jsonb, $4, NULL, TRUE,
          'authoritative', $5, '[]'::jsonb
        )
        """,
        obs_id,
        tenant_id,
        _json({"run_id": run_id, "case": case_name}),
        f"Seed observation for {case_name}",
        f"{run_id}:{case_name}:seed",
    )
    return obs_id


async def _inject_case_signal(
    case: dict[str, Any],
    *,
    scenario: Any,
    scope_entities: list[dict[str, str]],
    pool: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    run_id: str,
) -> UUID:
    assert scenario.tenant_id is not None
    occurred_at = (scenario.base_time or datetime.now(timezone.utc)) + timedelta(days=7)
    signal = SyntheticSignal(
        source_channel="slack",
        content_text=case["text"],
        content={
            "text": case["text"],
            "run_id": run_id,
            "case": case["name"],
            "customer_name": case["customer"],
            "family": "large_situation",
        },
        occurred_at=occurred_at,
        source_actor_ref="slack:elena",
        external_id=f"{run_id}:{case['name']}:live-signal",
        entities_hint=scope_entities,
        trust_tier="authoritative",
        kind="signal",
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
        skip_t1_enqueue=False,
    )
    return result.observation.id


async def _collect_case_result(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    seed_model_count: int,
    scope_entity_count: int,
) -> dict[str, Any]:
    active_models = int(await conn.fetchval(
        "SELECT COUNT(*)::bigint FROM models WHERE tenant_id = $1 AND status = 'active'",
        tenant_id,
    ) or 0)
    trigger_row = await conn.fetchrow(
        """
        SELECT q.id, q.completed_at, q.attempts, r.status AS run_status,
               r.error AS run_error, r.retrieval_model_count,
               r.retrieval_observation_count, r.llm_latency_ms,
               r.validation_error_count, r.started_at, r.ended_at,
               EXTRACT(EPOCH FROM (r.ended_at - r.started_at)) * 1000 AS run_elapsed_ms,
               r.ops_applied
        FROM think_trigger_queue q
        LEFT JOIN LATERAL (
          SELECT *
          FROM think_runs r
          WHERE r.trigger_id = q.id
          ORDER BY r.started_at DESC
          LIMIT 1
        ) r ON true
        WHERE q.tenant_id = $1
          AND q.observation_id = $2
          AND q.trigger_kind = 'T1'
        ORDER BY q.enqueued_at DESC
        LIMIT 1
        """,
        tenant_id,
        observation_id,
    )
    situation_rows = await conn.fetch(
        """
        SELECT id, proposition, proposition_kind, claim_role, abstraction_level,
               time_mode, modality, polarity, domain_tags
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND claim_role = 'situation'
        ORDER BY created_at DESC
        """,
        tenant_id,
    )
    valid_situations: list[dict[str, Any]] = []
    for row in situation_rows:
        prop = row["proposition"]
        if not isinstance(prop, dict):
            prop = json.loads(prop)
        members = prop.get("member_model_ids") or []
        sidecar_count = int(await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM model_composition_members
            WHERE tenant_id = $1 AND composite_model_id = $2
            """,
            tenant_id,
            row["id"],
        ) or 0)
        if (
            row["proposition_kind"] == "belief"
            and row["claim_role"] == "situation"
            and row["abstraction_level"] == "composite"
            and len(members) >= 2
            and sidecar_count >= 2
            and prop.get("pressure_type")
            and prop.get("shared_mechanism")
            and prop.get("judgment_change")
            and prop.get("open_falsifier")
        ):
            valid_situations.append(
                {
                    "id": str(row["id"]),
                    "member_count": len(members),
                    "sidecar_count": sidecar_count,
                    "pressure_type": prop.get("pressure_type"),
                    "domain_tags": list(row["domain_tags"] or []),
                    "natural": prop.get("summary") or prop.get("situation"),
                }
            )

    ops = (trigger_row and trigger_row["ops_applied"]) or {}
    if not isinstance(ops, dict):
        ops = {}
    return {
        "tenant_id": str(tenant_id),
        "observation_id": str(observation_id),
        "seed_model_count": seed_model_count,
        "scope_entity_count": scope_entity_count,
        "active_models": active_models,
        "trigger_completed": bool(trigger_row and trigger_row["completed_at"]),
        "trigger_attempts": int((trigger_row and trigger_row["attempts"]) or 0),
        "run_status": (trigger_row and trigger_row["run_status"]),
        "run_error": (trigger_row and trigger_row["run_error"]),
        "retrieval_model_count": int(
            (trigger_row and trigger_row["retrieval_model_count"]) or 0
        ),
        "retrieval_observation_count": int(
            (trigger_row and trigger_row["retrieval_observation_count"]) or 0
        ),
        "llm_latency_ms": int((trigger_row and trigger_row["llm_latency_ms"]) or 0),
        "validation_error_count": int(
            (trigger_row and trigger_row["validation_error_count"]) or 0
        ),
        "run_elapsed_ms": int((trigger_row and trigger_row["run_elapsed_ms"]) or 0),
        "split_summary": ops.get("split_summary") or {},
        "quality_summary": ops.get("quality_summary") or {},
        "reconcile_summary": ops.get("reconcile_summary") or {},
        "memory_aggregation": ops.get("memory_aggregation") or {},
        "valid_situation_count": len(valid_situations),
        "valid_situations": valid_situations[:5],
    }


async def run(config: RunnerConfig) -> dict[str, Any]:
    run_id = config.run_id or (
        "large-situation-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    report_dir = config.report_root / f"large-situation-{run_id}"
    report_dir.mkdir(parents=True, exist_ok=True)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=config.pool_max_size,
        init=_register_codecs,
    )
    embedder = OllamaClient(OllamaConfig.from_env())
    provider = _build_cached_provider()
    started = time.monotonic()
    results: list[dict[str, Any]] = []

    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")

        for case_index, case in enumerate(CASE_DEFINITIONS[: config.case_count], start=1):
            # build_scenario namespaces actor identity mappings, but that
            # helper truncates the namespace slug. Keep the case discriminator
            # at the front so multi-case runs do not collide on global
            # (source_channel, source_actor_ref) identity keys.
            case_run_id = f"case-{case_index}-{case['name']}:{run_id}"
            print(
                f"[case {case_index}/{config.case_count}] materializing {case['name']}",
                flush=True,
            )
            scenario = build_scenario(1, namespace=case_run_id)
            await materialize(scenario, pool=pool)
            if scenario.tenant_id is None:
                raise RuntimeError("scenario materialization did not set tenant_id")

            actor_repo = ActorRepo(pool)
            alias_repo = EntityAliasRepo(pool)
            scope_entities = _case_scope_entities(case, scenario)
            if not scope_entities:
                raise RuntimeError(
                    f"case {case['name']} did not resolve any customer scope entities"
                )

            async with pool.acquire() as conn:
                seed_obs = await _insert_seed_observation(
                    conn,
                    tenant_id=scenario.tenant_id,
                    run_id=run_id,
                    case_name=case["name"],
                )
                await _seed_large_model_set(
                    conn,
                    tenant_id=scenario.tenant_id,
                    observation_id=seed_obs,
                    customer=case["customer"],
                    scope_entities=scope_entities,
                    count=config.seed_models_per_case,
                )
                seeded_count = int(await conn.fetchval(
                    """
                    SELECT COUNT(*)::bigint
                    FROM models
                    WHERE tenant_id = $1 AND status = 'active'
                    """,
                    scenario.tenant_id,
                ) or 0)
            print(
                f"[case {case_index}] seeded active_models={seeded_count}",
                flush=True,
            )

            observation_id = await _inject_case_signal(
                case,
                scenario=scenario,
                scope_entities=scope_entities,
                pool=pool,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                run_id=run_id,
            )
            await run_signal_t1_triggers_until_complete(
                scenario.tenant_id,
                pool=pool,
                provider=provider,
                observation_ids=[observation_id],
                timeout_seconds=config.think_timeout,
            )
            async with pool.acquire() as conn:
                result = await _collect_case_result(
                    conn,
                    tenant_id=scenario.tenant_id,
                    observation_id=observation_id,
                    seed_model_count=config.seed_models_per_case,
                    scope_entity_count=len(scope_entities),
                )
            result.update({"case_index": case_index, "case_name": case["name"]})
            results.append(result)
            print(
                "[case {idx}] run_status={status} active_models={models} "
                "situations={situations} retrieved={retrieved}".format(
                    idx=case_index,
                    status=result["run_status"],
                    models=result["active_models"],
                    situations=result["valid_situation_count"],
                    retrieved=result["retrieval_model_count"],
                ),
                flush=True,
            )

        failures: list[str] = []
        for result in results:
            label = f"case {result['case_index']} {result['case_name']}"
            if result["active_models"] < config.seed_models_per_case:
                failures.append(
                    f"{label}: active_models {result['active_models']} < "
                    f"{config.seed_models_per_case}"
                )
            if result["run_status"] != "success":
                failures.append(f"{label}: run_status={result['run_status']} error={result['run_error']}")
            if not result["trigger_completed"]:
                failures.append(f"{label}: trigger did not complete")
            if result["valid_situation_count"] < 1:
                failures.append(f"{label}: no valid queryable situation model")

        summary = {
            "run_id": run_id,
            "case_count": len(results),
            "seed_models_per_case": config.seed_models_per_case,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "passes": not failures,
            "failures": failures,
            "results": results,
        }
        (report_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
        (report_dir / "summary.md").write_text(_render_markdown(summary))
        print(f"report_dir={report_dir}", flush=True)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        if failures:
            raise AssertionError("; ".join(failures))
        return summary
    finally:
        await embedder.close()
        await pool.close()


def _render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Large Situation Real-LLM E2E",
        "",
        f"- Run id: `{summary['run_id']}`",
        f"- Cases: {summary['case_count']}",
        f"- Seed Models per case: {summary['seed_models_per_case']}",
        f"- Passed: {summary['passes']}",
        f"- Elapsed seconds: {summary['elapsed_seconds']}",
        "",
        "| Case | Active Models | Retrieved | Valid Situations | New Model Pressure | Absorption | Status |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for result in summary["results"]:
        memory = result.get("memory_aggregation") or {}
        lines.append(
            "| {case} | {models} | {retrieved} | {situations} | {pressure:.2f} | {absorption:.2f} | {status} |".format(
                case=result["case_name"],
                models=result["active_models"],
                retrieved=result["retrieval_model_count"],
                situations=result["valid_situation_count"],
                pressure=float(memory.get("new_model_pressure") or 0.0),
                absorption=float(memory.get("absorption_ratio") or 0.0),
                status=result["run_status"],
            )
        )
    if summary["failures"]:
        lines.extend(["", "## Failures", ""])
        lines.extend(f"- {failure}" for failure in summary["failures"])
    lines.extend(["", "## Bottleneck Signals", ""])
    retrieval_counts = [
        int(result.get("retrieval_model_count") or 0)
        for result in summary["results"]
    ]
    elapsed = [
        int(result.get("run_elapsed_ms") or 0)
        for result in summary["results"]
    ]
    validation_drops = sum(
        int(result.get("validation_error_count") or 0)
        for result in summary["results"]
    )
    if retrieval_counts:
        lines.append(f"- Retrieval min/max Models: {min(retrieval_counts)} / {max(retrieval_counts)}")
        sparse = sum(1 for count in retrieval_counts if count == 0)
        lines.append(f"- Sparse retrieval cases: {sparse}/{len(retrieval_counts)}")
    if elapsed:
        lines.append(f"- Think elapsed ms min/max: {min(elapsed)} / {max(elapsed)}")
    lines.append(f"- Validation drops: {validation_drops}")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=int, default=20)
    parser.add_argument("--seed-models", type=int, default=2000)
    parser.add_argument("--think-timeout", type=int, default=900)
    parser.add_argument("--post-commit-timeout", type=int, default=300)
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    if args.cases < 1 or args.cases > len(CASE_DEFINITIONS):
        raise SystemExit(f"--cases must be between 1 and {len(CASE_DEFINITIONS)}")
    if args.seed_models < 1000:
        raise SystemExit("--seed-models must be at least 1000")
    await run(
        RunnerConfig(
            case_count=args.cases,
            seed_models_per_case=args.seed_models,
            think_timeout=args.think_timeout,
            post_commit_timeout=args.post_commit_timeout,
            pool_max_size=args.pool_max_size,
            run_id=args.run_id,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
