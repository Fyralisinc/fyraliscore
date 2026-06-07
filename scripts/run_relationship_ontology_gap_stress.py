#!/usr/bin/env python3
"""Stress the model-layer relationship ontology against high-value links.

This runner is deliberately deterministic and database-free. The existing
retrieval stress tests prove the system can still find expected Models through
lexical, scope, and learned affordance routes. This probe asks a different
question: when the valuable insight is a relationship whose semantics do not
fit the current edge registry, where does it first get lost?
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from lib.shared.edge_registry import EDGE_REGISTRY, EdgeRegistryError, get_spec
from lib.shared.ids import uuid7
from services.relationships.candidates import (
    JudgmentScores,
    TOPOLOGY_EMITTABLE_EDGE_KINDS,
    candidate_rules,
    make_edge_type_candidate,
)


REPORT_DIR = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"
EDGE_KIND_SHAPE_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


@dataclass(frozen=True, slots=True)
class RelationshipStressCase:
    key: str
    desired_kind: str
    relation_summary: str
    value_driver: str
    nearest_existing: str | None
    dropped_dimensions: tuple[str, ...]
    severity: int
    expected_stage: str


CASES: tuple[RelationshipStressCase, ...] = (
    RelationshipStressCase(
        key="evidential_support",
        desired_kind="supports",
        relation_summary="A claim materially supports another claim.",
        value_driver="Preserve provenance and confidence propagation.",
        nearest_existing="supports",
        dropped_dimensions=(),
        severity=2,
        expected_stage="accepted_exact",
    ),
    RelationshipStressCase(
        key="direct_contradiction",
        desired_kind="contradicts",
        relation_summary="Two claims cannot both be true.",
        value_driver="Prevent stale or mutually inconsistent memory from coexisting.",
        nearest_existing="contradicts",
        dropped_dimensions=(),
        severity=3,
        expected_stage="accepted_exact",
    ),
    RelationshipStressCase(
        key="blocker_dependency",
        desired_kind="blocks",
        relation_summary="A constraint prevents another claim/outcome from becoming true.",
        value_driver="Expose the smallest intervention surface.",
        nearest_existing="blocks",
        dropped_dimensions=(),
        severity=3,
        expected_stage="accepted_exact",
    ),
    RelationshipStressCase(
        key="capability_enabler",
        desired_kind="enables",
        relation_summary="One capability makes another outcome possible.",
        value_driver="Find leverage points that unlock many downstream items.",
        nearest_existing="enables",
        dropped_dimensions=(),
        severity=3,
        expected_stage="accepted_exact",
    ),
    RelationshipStressCase(
        key="causal_mechanism",
        desired_kind="causes",
        relation_summary="A mechanism makes another state happen.",
        value_driver="Support causal reasoning and intervention design.",
        nearest_existing="causes",
        dropped_dimensions=(),
        severity=4,
        expected_stage="accepted_exact",
    ),
    RelationshipStressCase(
        key="pattern_instance",
        desired_kind="instance_of",
        relation_summary="A specific case instantiates a durable pattern.",
        value_driver="Let the system generalize across a large corpus.",
        nearest_existing="instance_of",
        dropped_dimensions=(),
        severity=3,
        expected_stage="accepted_exact",
    ),
    RelationshipStressCase(
        key="replacement",
        desired_kind="superseded_by",
        relation_summary="A newer claim replaces an older one.",
        value_driver="Keep memory current without deleting history.",
        nearest_existing="superseded_by",
        dropped_dimensions=(),
        severity=3,
        expected_stage="accepted_exact",
    ),
    RelationshipStressCase(
        key="accountability_gap",
        desired_kind="accountable_for",
        relation_summary="A Model says an actor/team is accountable for resolving another Model.",
        value_driver="Surface ownerless or misowned work before it becomes a blocker.",
        nearest_existing="supports",
        dropped_dimensions=("actor accountability", "ownership status", "escalation route"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="decision_gate",
        desired_kind="gated_by_decision",
        relation_summary="A Model cannot progress until a specific decision is made.",
        value_driver="Turn hidden authority requirements into an explicit path to action.",
        nearest_existing="blocks",
        dropped_dimensions=("authority surface", "decision dependency", "approval state"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="reinforcing_loop",
        desired_kind="reinforces",
        relation_summary="Two pressures amplify each other as a loop.",
        value_driver="Detect compounding risk humans miss when reading items independently.",
        nearest_existing="causes",
        dropped_dimensions=("loop directionality", "mutual amplification", "runaway dynamic"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="dampening_effect",
        desired_kind="dampens",
        relation_summary="One intervention reduces another pressure without falsifying it.",
        value_driver="Separate mitigations from counterevidence.",
        nearest_existing="weakens",
        dropped_dimensions=("operational mitigation", "residual truth", "intervention surface"),
        severity=4,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="tradeoff_tension",
        desired_kind="trades_off_with",
        relation_summary="Improving one Model predictably worsens another.",
        value_driver="Reveal strategic tensions instead of treating them as contradictions.",
        nearest_existing="contradicts",
        dropped_dimensions=("both can be true", "optimization frontier", "choice cost"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="risk_transfer",
        desired_kind="transfers_risk_to",
        relation_summary="Resolving one risk moves the exposure to another owner/scope.",
        value_driver="Prevent local optimization from hiding system-level risk.",
        nearest_existing="causes",
        dropped_dimensions=("risk movement", "recipient scope", "second-order cost"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="assumption_dependency",
        desired_kind="depends_on_assumption",
        relation_summary="A forecast or plan only holds if another uncertain premise holds.",
        value_driver="Make fragility inspectable before plans harden around it.",
        nearest_existing="supports",
        dropped_dimensions=("assumption status", "conditional truth", "fragility"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="evidence_proxy",
        desired_kind="proxy_for",
        relation_summary="One weak signal is a proxy for a harder-to-observe state.",
        value_driver="Use indirect evidence without overclaiming causality.",
        nearest_existing="early_warning_for",
        dropped_dimensions=("proxy validity", "measurement gap", "not necessarily future"),
        severity=4,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="scope_contains",
        desired_kind="contains_scope",
        relation_summary="A broader situation contains a narrower local issue.",
        value_driver="Navigate between portfolio, account, and local operational detail.",
        nearest_existing="same_issue_as",
        dropped_dimensions=("hierarchy", "containment", "roll-up semantics"),
        severity=4,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="priority_conflict",
        desired_kind="competes_for_priority_with",
        relation_summary="Two valid Models compete for the same limited attention or capacity.",
        value_driver="Help the user allocate scarce focus across true concerns.",
        nearest_existing="alternative_to",
        dropped_dimensions=("resource contention", "capacity limit", "both valid"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="latency_lag",
        desired_kind="lags",
        relation_summary="One metric/state responds after another with a predictable delay.",
        value_driver="Improve diagnosis by separating leading and lagging indicators.",
        nearest_existing="predicts",
        dropped_dimensions=("delay shape", "lagging indicator", "temporal offset"),
        severity=4,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="leverage_amplifier",
        desired_kind="amplifies_leverage_of",
        relation_summary="One Model makes an intervention on another much more valuable.",
        value_driver="Find high-return sequences hidden in a large graph.",
        nearest_existing="enables",
        dropped_dimensions=("marginal value", "sequence leverage", "intervention ordering"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="customer_precedent",
        desired_kind="sets_precedent_for",
        relation_summary="A customer-specific case should shape future treatment of similar cases.",
        value_driver="Turn one-off learning into reusable judgment without overgeneralizing.",
        nearest_existing="analogous_to",
        dropped_dimensions=("normative precedent", "future policy", "scope of reuse"),
        severity=4,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="attention_shadow",
        desired_kind="obscures",
        relation_summary="A loud Model makes a quieter but higher-value Model less likely to be noticed.",
        value_driver="Find hidden high-value risks masked by obvious activity.",
        nearest_existing=None,
        dropped_dimensions=("attention competition", "salience distortion", "visibility risk"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
    RelationshipStressCase(
        key="ontology_gap_signal",
        desired_kind="proposed_edge_type",
        relation_summary="The system notices a valuable relation that needs a new edge kind.",
        value_driver="Let the ontology evolve from repeated evidence instead of code edits.",
        nearest_existing=None,
        dropped_dimensions=("new type proposal", "evidence examples", "promotion workflow"),
        severity=5,
        expected_stage="captured_as_edge_type_candidate",
    ),
)


def _registry_status(kind: str) -> dict[str, Any]:
    try:
        spec = get_spec(kind)
    except EdgeRegistryError as exc:
        return {"known": False, "error": str(exc)}
    return {
        "known": True,
        "directed": spec.is_directed,
        "weight_required": spec.weight_required,
        "weight_allowed": spec.weight_allowed,
        "cycle_scope": sorted(spec.cycle_scope or []),
        "enabled_for_writes": spec.enabled_for_writes,
    }


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _case_result(case: RelationshipStressCase) -> dict[str, Any]:
    registry = _registry_status(case.desired_kind)
    strict_shape_allowed = bool(EDGE_KIND_SHAPE_RE.match(case.desired_kind))
    topology_allowed = case.desired_kind in TOPOLOGY_EMITTABLE_EDGE_KINDS
    rule_allowed = case.desired_kind in candidate_rules()
    fallback_known = (
        case.nearest_existing in EDGE_REGISTRY if case.nearest_existing else False
    )
    needs_new_type = not bool(registry["known"])
    can_emit_exact_anywhere = bool(
        registry["known"] and strict_shape_allowed and (topology_allowed or rule_allowed)
    )
    coerced = needs_new_type and bool(case.nearest_existing)
    silently_dropped = needs_new_type and not case.nearest_existing
    ontology_gap_captured = False
    edge_type_candidate_error = None
    edge_type_candidate_payload: dict[str, Any] | None = None
    if needs_new_type:
        try:
            candidate = make_edge_type_candidate(
                tenant_id=uuid7(),
                proposed_edge_kind=case.desired_kind,
                description=case.relation_summary,
                relationship_summary=case.value_driver,
                parent_kind=case.nearest_existing if fallback_known else None,
                nearest_existing_kind=case.nearest_existing if fallback_known else None,
                dropped_dimensions=case.dropped_dimensions,
                scores=JudgmentScores(
                    impact=min(1.0, case.severity / 5.0),
                    uncertainty=0.75,
                    urgency=0.65,
                    authority_required=0.70,
                    actionability=0.80,
                    novelty=1.0,
                    confidence=0.60,
                ),
            )
            edge_type_candidate_payload = _json_safe(candidate.to_record())
            ontology_gap_captured = True
            coerced = False
            silently_dropped = False
        except Exception as exc:  # noqa: BLE001
            edge_type_candidate_error = f"{type(exc).__name__}: {exc}"
    if needs_new_type:
        failure_stage = (
            "captured_as_edge_type_candidate"
            if ontology_gap_captured
            else (
                "no_candidate_lane"
                if case.nearest_existing is None
                else "runtime_rejects_until_promotion"
            )
        )
    elif not topology_allowed and not rule_allowed:
        failure_stage = "llm_only_no_deterministic_candidate"
    else:
        failure_stage = "exact_supported"
    return {
        "key": case.key,
        "desired_kind": case.desired_kind,
        "relation_summary": case.relation_summary,
        "value_driver": case.value_driver,
        "severity": case.severity,
        "nearest_existing": case.nearest_existing,
        "dropped_dimensions": list(case.dropped_dimensions),
        "registry": registry,
        "strict_schema_shape_allowed": strict_shape_allowed,
        "topology_allowed": topology_allowed,
        "candidate_rule_allowed": rule_allowed,
        "fallback_known": fallback_known,
        "needs_new_type": needs_new_type,
        "can_emit_exact_anywhere": can_emit_exact_anywhere,
        "coerced_to_existing": coerced,
        "would_coerce_without_gap_lane": needs_new_type and bool(case.nearest_existing),
        "silently_dropped": silently_dropped,
        "ontology_gap_captured": ontology_gap_captured,
        "edge_type_candidate_error": edge_type_candidate_error,
        "edge_type_candidate_payload": edge_type_candidate_payload,
        "expected_stage": case.expected_stage,
        "observed_failure_stage": failure_stage,
    }


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    high_value = [r for r in results if int(r["severity"]) >= 4]
    gaps = [r for r in results if r["needs_new_type"]]
    severe_gaps = [r for r in gaps if int(r["severity"]) >= 4]
    exact_registry = [r for r in results if r["registry"]["known"]]
    strict_shape = [r for r in results if r["strict_schema_shape_allowed"]]
    topology = [r for r in results if r["topology_allowed"]]
    rules = [r for r in results if r["candidate_rule_allowed"]]
    coerced = [r for r in results if r["coerced_to_existing"]]
    would_coerce = [r for r in results if r["would_coerce_without_gap_lane"]]
    dropped = [r for r in results if r["silently_dropped"]]
    captured_gaps = [r for r in gaps if r["ontology_gap_captured"]]
    llm_only = [
        r
        for r in results
        if r["registry"]["known"]
        and r["strict_schema_shape_allowed"]
        and not r["topology_allowed"]
        and not r["candidate_rule_allowed"]
    ]
    severity_total = sum(int(r["severity"]) for r in results)
    lost_severity = sum(
        int(r["severity"])
        for r in results
        if r["needs_new_type"] and not r["ontology_gap_captured"]
    )
    return {
        "cases": total,
        "high_value_cases": len(high_value),
        "registry_exact_coverage": _ratio(len(exact_registry), total),
        "strict_schema_shape_coverage": _ratio(len(strict_shape), total),
        "topology_exact_coverage": _ratio(len(topology), total),
        "candidate_rule_exact_coverage": _ratio(len(rules), total),
        "new_type_required_count": len(gaps),
        "new_type_required_ratio": _ratio(len(gaps), total),
        "high_value_new_type_required_count": len(severe_gaps),
        "coercion_count": len(coerced),
        "coercion_ratio_among_gaps": _ratio(len(coerced), len(gaps)),
        "would_coerce_without_gap_lane_count": len(would_coerce),
        "silent_drop_count": len(dropped),
        "llm_only_registered_count": len(llm_only),
        "ontology_gap_capture_rate": _ratio(len(captured_gaps), len(gaps)),
        "severity_weighted_lost_value_ratio": _ratio(lost_severity, severity_total),
        "registered_edge_kinds": sorted(EDGE_REGISTRY.keys()),
        "strict_schema_edge_kind_shape": EDGE_KIND_SHAPE_RE.pattern,
        "topology_emittable_edge_kinds": sorted(TOPOLOGY_EMITTABLE_EDGE_KINDS),
        "candidate_rule_edge_kinds": sorted(candidate_rules().keys()),
        "breakpoints": [
            "Candidate storage can represent proposed edge types as edge_type rows.",
            "Topology can emit edge_type candidates for recognized ontology-gap patterns.",
            "Strict Think schema can express dynamic snake_case edge kinds.",
            "Validator/runtime reject unknown dynamic edge kinds until promotion.",
            "Strict Think schema can emit ontology_gap_ops into relationship candidates.",
            "Accepted ontology proposals give dynamic edge kinds fallback runtime semantics.",
            "Deterministic candidate rules cover only a subset of registered edge kinds.",
            "Topology ontology-gap coverage is still deterministic and pattern-based.",
        ],
    }


def _write_markdown(
    *,
    run_id: str,
    summary: dict[str, Any],
    results: list[dict[str, Any]],
    path: Path,
) -> None:
    lines = [
        "# Relationship Ontology Gap Stress",
        "",
        f"- Run id: `{run_id}`",
        f"- Cases: {summary['cases']}",
        f"- Registry exact coverage: {summary['registry_exact_coverage']:.3f}",
        f"- Strict schema shape coverage: {summary['strict_schema_shape_coverage']:.3f}",
        f"- Topology exact coverage: {summary['topology_exact_coverage']:.3f}",
        f"- Candidate-rule exact coverage: {summary['candidate_rule_exact_coverage']:.3f}",
        f"- New type required: {summary['new_type_required_count']} "
        f"({summary['new_type_required_ratio']:.3f})",
        f"- High-value new type required: {summary['high_value_new_type_required_count']}",
        f"- Ontology-gap capture rate: {summary['ontology_gap_capture_rate']:.3f}",
        f"- Severity-weighted lost value ratio: "
        f"{summary['severity_weighted_lost_value_ratio']:.3f}",
        "",
        "## Breakpoints",
        "",
    ]
    for item in summary["breakpoints"]:
        lines.append(f"- {item}")
    lines.extend(["", "## Case Results", ""])
    lines.append(
        "| Case | Desired kind | Stage | Fallback | Dropped dimensions | Severity |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for result in results:
        dims = ", ".join(result["dropped_dimensions"]) or "-"
        fallback = result["nearest_existing"] or "-"
        lines.append(
            f"| {result['key']} | `{result['desired_kind']}` | "
            f"{result['observed_failure_stage']} | `{fallback}` | "
            f"{dims} | {result['severity']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_id: str) -> dict[str, Any]:
    results = [_case_result(case) for case in CASES]
    summary = _summarize(results)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "summary": summary,
        "results": results,
    }
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORT_DIR / f"relationship-ontology-gap-stress-{run_id}.json"
    md_path = REPORT_DIR / f"relationship-ontology-gap-stress-{run_id}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(run_id=run_id, summary=summary, results=results, path=md_path)
    payload["json_report"] = str(json_path)
    payload["markdown_report"] = str(md_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a deterministic relationship ontology gap stress probe.",
    )
    parser.add_argument(
        "--run-id",
        default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
    )
    args = parser.parse_args()
    payload = run(args.run_id)
    print(
        "RELATIONSHIP_ONTOLOGY_GAP_STRESS_SUMMARY "
        + json.dumps(payload["summary"], sort_keys=True)
    )
    print(
        "RELATIONSHIP_ONTOLOGY_GAP_STRESS_REPORT "
        + json.dumps({
            "json": payload["json_report"],
            "markdown": payload["markdown_report"],
        }, sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
