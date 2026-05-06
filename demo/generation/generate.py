"""Orchestrator: read spec → run pipeline → emit SQL snapshot.

Default behavior (no `--execute`) is dry-run: prints the planned LLM
calls and an estimated cost, then exits. Only `--execute` actually
invokes the LLM. This guards against accidental burn — the framework
is reproducibly callable but money only flows when the operator
explicitly opts in.

Pipeline (per DEMO-BUILD-PLAN Step 2):

  1. actors          (1 call)
  2. customers       (1 call)
  3. goals           (1 call)
  4. decisions       (1 call)
  5. commitments     (ceil(commitment_count / 30) batched calls)
  6. signals         (1 call per channel per week, sparse fill for older history)
  7. recommendations (1 call per recommendation entry in the spec)
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from demo.generation.cache import FileCache
from demo.generation.schemas import (
    ActorBatch, CommitmentBatch, CustomerBatch, DecisionBatch,
    GeneratedBundle, GeneratedRecommendation, GoalBatch, SignalBatch,
)
from demo.generation.sql_emit import write_sql
from demo.generation.validate import validate_bundle


SPEC_DIR = Path(__file__).resolve().parent / "specs"
PROMPT_DIR = Path(__file__).resolve().parent / "prompts"
SNAPSHOT_DIR = Path(__file__).resolve().parents[1] / "snapshots"

# Channels to generate signals on. Density per channel governs the
# total signal budget — see _plan_signals.
SIGNAL_CHANNELS = ["slack", "github", "email", "calendar"]

# Recent-window density: signals per channel per week for the last 6 weeks.
RECENT_SIGNALS_PER_CALL = 25
RECENT_WEEKS = 6
# Older history: fewer calls, sparser content.
OLDER_HISTORY_CALLS = 3

# Heuristic token budget per call kind (used only for dry-run cost estimate).
COST_ESTIMATES_USD = {
    "actors": 0.30,
    "customers": 0.20,
    "goals": 0.15,
    "decisions": 0.15,
    "commitment_batch": 0.40,
    "signal_batch": 0.25,
    "recommendation": 0.20,
}


# ---------------------------------------------------------------------
# Spec loader
# ---------------------------------------------------------------------


def load_spec(company: str) -> dict[str, Any]:
    import yaml      # PyYAML, listed under [project.optional-dependencies].dev
    p = SPEC_DIR / f"{company}.yaml"
    if not p.exists():
        raise FileNotFoundError(f"no spec found for company={company!r}: {p}")
    with open(p, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_prompt(name: str) -> str:
    return (PROMPT_DIR / f"{name}.md").read_text(encoding="utf-8")


def render_prompt(template: str, context: dict[str, Any]) -> str:
    """Tiny `{{ var }}` substitution. Avoids a Jinja2 dep — the prompt
    templates only need flat key replacement."""
    out = template
    for key, val in context.items():
        token = "{{ " + key + " }}"
        out = out.replace(token, str(val))
    return out


# ---------------------------------------------------------------------
# Plan: enumerate calls + estimated cost without invoking the LLM
# ---------------------------------------------------------------------


@dataclass
class CallPlan:
    name: str
    schema_name: str
    cost_kind: str
    estimated_cost_usd: float


def build_plan(spec: dict[str, Any]) -> list[CallPlan]:
    plan: list[CallPlan] = []
    plan.append(CallPlan("actors", "ActorBatch", "actors",
                         COST_ESTIMATES_USD["actors"]))
    plan.append(CallPlan("customers", "CustomerBatch", "customers",
                         COST_ESTIMATES_USD["customers"]))
    plan.append(CallPlan("goals", "GoalBatch", "goals",
                         COST_ESTIMATES_USD["goals"]))
    plan.append(CallPlan("decisions", "DecisionBatch", "decisions",
                         COST_ESTIMATES_USD["decisions"]))

    n_commit_batches = max(1, math.ceil(spec["commitment_count"] / 30))
    for i in range(n_commit_batches):
        plan.append(CallPlan(
            f"commitments[{i + 1}/{n_commit_batches}]",
            "CommitmentBatch", "commitment_batch",
            COST_ESTIMATES_USD["commitment_batch"],
        ))

    # Recent signals: one call per channel per week of the recent window.
    for week_idx in range(RECENT_WEEKS):
        for channel in SIGNAL_CHANNELS:
            plan.append(CallPlan(
                f"signals[{channel}, recent w-{week_idx}]",
                "SignalBatch", "signal_batch",
                COST_ESTIMATES_USD["signal_batch"],
            ))
    # Older history: a small number of sparse fills.
    for i in range(OLDER_HISTORY_CALLS):
        plan.append(CallPlan(
            f"signals[older fill {i + 1}/{OLDER_HISTORY_CALLS}]",
            "SignalBatch", "signal_batch",
            COST_ESTIMATES_USD["signal_batch"],
        ))

    for i, rec in enumerate(spec.get("recommendations", [])):
        plan.append(CallPlan(
            f"recommendation[{i + 1}: {rec['kind']}]",
            "GeneratedRecommendation", "recommendation",
            COST_ESTIMATES_USD["recommendation"],
        ))
    return plan


def print_plan(spec: dict[str, Any], plan: list[CallPlan]) -> None:
    print(f"=== Plan for {spec['company_id']} ({spec['company_name']}) ===")
    print(f"actors={spec['actor_count']} customers={spec['customer_count']} "
          f"goals={spec['goal_count']} decisions={spec['decision_count']} "
          f"commitments={spec['commitment_count']} "
          f"recommendations={spec['recommendation_count']}")
    print()
    for i, c in enumerate(plan):
        print(f"  [{i + 1:>3}] {c.name:<48} -> {c.schema_name:<22} "
              f"~${c.estimated_cost_usd:.2f}")
    total = sum(c.estimated_cost_usd for c in plan)
    print()
    print(f"Total calls: {len(plan)}    Estimated cost: ~${total:.2f}")
    print()
    print("Dry-run only. Pass --execute to run the LLM pipeline.")


# ---------------------------------------------------------------------
# LLM execution path. Wrapped behind --execute. Imported lazily so the
# dry-run doesn't trigger any provider/SDK import.
# ---------------------------------------------------------------------


async def execute_pipeline(
    spec: dict[str, Any],
    *,
    cache: FileCache,
) -> GeneratedBundle:
    from lib.llm.provider import build_provider

    provider = build_provider()
    bundle = GeneratedBundle(
        company_id=spec["company_id"],
        ceo_actor_id=str(uuid.uuid4()),       # filled in once actors come back
    )

    # Convenience: render+call wrapper that goes through the cache.
    async def call(name: str, schema, prompt_template: str, ctx: dict) -> Any:
        system = (
            "You are generating realistic, internally-consistent state "
            "for a synthetic SaaS company used in a sales demo. "
            "Output strict JSON matching the schema. No prose outside JSON."
        )
        user = render_prompt(prompt_template, ctx)
        # Cache key: prompt+model+schema. Same hash → same response.
        cached = cache.get(
            system=system, user=user, model=provider.config.model,
            schema_name=schema.__name__,
        )
        if cached is not None:
            return schema.model_validate(cached)
        out = await provider.structured(
            system=system, user=user, schema=schema, temperature=0.3,
        )
        cache.put(
            system=system, user=user, model=provider.config.model,
            schema_name=schema.__name__,
            value=out.model_dump(mode="json"),
        )
        return out

    actors_prompt = load_prompt("actors")
    customers_prompt = load_prompt("customers")
    goals_prompt = load_prompt("goals")
    decisions_prompt = load_prompt("decisions")
    commitments_prompt = load_prompt("commitments")
    signals_prompt = load_prompt("signals")
    recommendations_prompt = load_prompt("recommendations")

    # 1. actors
    role_mix_yaml = "\n".join(
        f"  {k}: {v}" for k, v in spec.get("role_mix", {}).items()
    )
    actor_ctx = {
        **spec, "role_mix_yaml": role_mix_yaml,
    }
    actor_batch: ActorBatch = await call(
        "actors", ActorBatch, actors_prompt, actor_ctx,
    )
    bundle.actors = actor_batch.items
    # Resolve CEO id by matching name.
    for a in bundle.actors:
        if a.name == spec["ceo_name"]:
            bundle.ceo_actor_id = a.id
            break

    # 2. customers
    customer_batch: CustomerBatch = await call(
        "customers", CustomerBatch, customers_prompt, spec,
    )
    bundle.customers = customer_batch.items

    # 3. goals
    goal_ctx = {**spec, "actor_ids": [a.id for a in bundle.actors]}
    goal_batch: GoalBatch = await call(
        "goals", GoalBatch, goals_prompt, goal_ctx,
    )
    bundle.goals = goal_batch.items

    # 4. decisions
    decision_batch: DecisionBatch = await call(
        "decisions", DecisionBatch, decisions_prompt, spec,
    )
    bundle.decisions = decision_batch.items

    # 5. commitments — batched 30/call so prior batches' ids feed forward.
    n_batches = max(1, math.ceil(spec["commitment_count"] / 30))
    for i in range(n_batches):
        ctx = {
            **spec,
            "batch_index": i + 1,
            "batch_total": n_batches,
            "batch_size": min(30, spec["commitment_count"] - i * 30),
            "prior_commitment_ids": [c.id for c in bundle.commitments],
        }
        cb: CommitmentBatch = await call(
            f"commitments[{i + 1}]", CommitmentBatch,
            commitments_prompt, ctx,
        )
        bundle.commitments.extend(cb.items)

    # 6. signals — channel × week for the recent window, plus sparse older.
    now = datetime.now(timezone.utc)
    for week_idx in range(RECENT_WEEKS):
        week_end = now - timedelta(days=7 * week_idx)
        week_start = week_end - timedelta(days=7)
        for channel in SIGNAL_CHANNELS:
            ctx = {
                **spec,
                "channel": channel,
                "week_index": week_idx,
                "week_start_iso": week_start.isoformat(),
                "week_end_iso": week_end.isoformat(),
                "signals_per_call": RECENT_SIGNALS_PER_CALL,
            }
            sb: SignalBatch = await call(
                f"signals[{channel},w{week_idx}]", SignalBatch,
                signals_prompt, ctx,
            )
            bundle.signals.extend(sb.items)
    # Older history (sparse fill — leave to operator iteration).

    # 7. recommendations — one call per spec entry.
    for i, rec_spec in enumerate(spec.get("recommendations", [])):
        ctx = {
            **spec,
            "rec_kind": rec_spec["kind"],
            "rec_proposition": rec_spec["proposition"],
            "rec_impact_usd": rec_spec["impact_usd"],
            "ceo_actor_id": bundle.ceo_actor_id,
        }
        rec: GeneratedRecommendation = await call(
            f"rec[{i + 1}]", GeneratedRecommendation,
            recommendations_prompt, ctx,
        )
        bundle.recommendations.append(rec)

    return bundle


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def _cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m demo.generation.generate")
    p.add_argument("--company", required=True,
                   choices=["pelago"])
    p.add_argument("--execute", action="store_true",
                   help="Actually invoke the LLM. Without this flag, "
                        "the script prints a plan + estimated cost and exits.")
    p.add_argument("--out", type=Path, default=None,
                   help="Output SQL path. Defaults to "
                        "demo/snapshots/<company>-v1.sql")
    p.add_argument("--compress", action="store_true",
                   help="Write .sql.zst (requires zstandard).")
    p.add_argument("--bundle-out", type=Path, default=None,
                   help="Optionally also write the validated entity "
                        "bundle as JSON for inspection.")
    return p


async def main_async(argv: list[str] | None = None) -> int:
    args = _cli().parse_args(argv)
    spec = load_spec(args.company)
    plan = build_plan(spec)

    if not args.execute:
        print_plan(spec, plan)
        return 0

    cache = FileCache()
    print(f"Executing pipeline for {args.company} "
          f"(~{len(plan)} LLM calls)…", file=sys.stderr)
    bundle = await execute_pipeline(spec, cache=cache)

    errors = validate_bundle(bundle, spec)
    if errors:
        print("VALIDATION FAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 2

    out = args.out or (SNAPSHOT_DIR / f"{args.company}-v1.sql")
    written = write_sql(bundle, out, compress=args.compress)
    print(f"snapshot written: {written}")

    if args.bundle_out:
        args.bundle_out.write_text(
            bundle.model_dump_json(indent=2), encoding="utf-8",
        )
        print(f"bundle JSON written: {args.bundle_out}")

    return 0


def main() -> None:
    sys.exit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
