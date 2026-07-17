#!/usr/bin/env python3
"""Join saved large-simulation evidence into one precise evaluation report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.large_company_simulation import (
    PROFILES,
    evaluate_large_company_simulation,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report_dir = args.report_dir.resolve()
    benchmark_path = _first_existing(
        report_dir / "benchmark_summary.json",
        report_dir / "storyline_scores.json",
    )
    if benchmark_path is None:
        raise SystemExit(
            f"{report_dir} has no benchmark_summary.json or storyline_scores.json"
        )
    run_path = report_dir / "run_summary.json"
    if not run_path.exists():
        raise SystemExit(f"{report_dir} has no run_summary.json")
    run_config_path = report_dir / "run_config.json"
    if not run_config_path.exists():
        raise SystemExit(f"{report_dir} has no run_config.json")

    vitals_path = args.vitals or _first_existing(
        report_dir / "vitals" / "vitals_scorecard.json",
        report_dir / "vitals_scorecard.json",
    )
    assurance_path = args.assurance or _first_existing(
        report_dir / "company_learning_assurance_summary.json",
        report_dir / "vitals" / "company_learning_assurance_summary.json",
    )
    entity_evidence_path = args.entity_evidence or _first_existing(
        report_dir / "objective_entity_evidence.json",
        report_dir / "vitals" / "objective_entity_evidence.json",
    )
    company_learning_evidence_path = args.company_learning_evidence or _first_existing(
        report_dir / "objective_company_learning_evidence.json",
        report_dir / "vitals" / "objective_company_learning_evidence.json",
    )
    report = evaluate_large_company_simulation(
        benchmark=_read_json(benchmark_path),
        run_summary=_read_json(run_path),
        vitals=_read_json(vitals_path) if vitals_path else None,
        assurance=_read_json(assurance_path) if assurance_path else None,
        run_config=_read_json(run_config_path),
        profile_name=args.profile,
        entity_evidence=(
            _read_json(entity_evidence_path) if entity_evidence_path else None
        ),
        company_learning_evidence=(
            _read_json(company_learning_evidence_path)
            if company_learning_evidence_path else None
        ),
    )
    report["artifact_inputs"] = {
        "benchmark": str(benchmark_path),
        "run_summary": str(run_path),
        "run_config": str(run_config_path),
        "vitals": str(vitals_path) if vitals_path else None,
        "assurance": str(assurance_path) if assurance_path else None,
        "entity_evidence": (
            str(entity_evidence_path) if entity_evidence_path else None
        ),
        "company_learning_evidence": (
            str(company_learning_evidence_path)
            if company_learning_evidence_path else None
        ),
    }
    output_dir = (args.output_dir or report_dir / "large_simulation_gate").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "large_company_simulation_evaluation.json"
    markdown_path = output_dir / "large_company_simulation_evaluation.md"
    json_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")
    print(f"json_report={json_path}")
    print(f"markdown_report={markdown_path}")
    print(
        f"status={report['status']} score={report['overall_score']:.4f} "
        f"coverage={report['evidence_coverage']:.4f} "
        f"hard_failures={len(report['hard_failures'])} "
        f"proof_gaps={len(report['proof_gaps'])}"
    )
    if args.fail_on_not_credible and report["status"] == "not_credible":
        return 1
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument(
        "--profile", choices=tuple(PROFILES), default="authoritative-45"
    )
    parser.add_argument("--vitals", type=Path)
    parser.add_argument("--assurance", type=Path)
    parser.add_argument("--entity-evidence", type=Path)
    parser.add_argument("--company-learning-evidence", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--fail-on-not-credible", action="store_true")
    return parser.parse_args(argv)


def _first_existing(*paths: Path) -> Path | None:
    return next((path for path in paths if path.exists()), None)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"{path} must contain a JSON object")
    return payload


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Large Company Simulation Evaluation",
        "",
        f"- Run: `{report.get('run_id')}`",
        f"- Profile: `{report['profile']}`",
        f"- Status: **{report['status']}**",
        f"- Overall measured quality: **{report['overall_score']:.1%}**",
        f"- Evidence coverage: **{report['evidence_coverage']:.1%}**",
        f"- Interpretation: {report['interpretation']}",
        "",
        "## Dimension Map",
        "",
        "| Dimension | Weight | Score | Coverage |",
        "| --- | ---: | ---: | ---: |",
    ]
    weights = {
        "hidden_pattern_recovery": 0.25,
        "temporal_improvement": 0.15,
        "entity_model_quality": 0.20,
        "learning_correction_lift": 0.20,
        "operational_drain": 0.10,
        "proof_completeness": 0.10,
    }
    for name, payload in report["dimensions"].items():
        lines.append(
            f"| {name.replace('_', ' ').title()} | {weights[name]:.0%} | "
            f"{payload['score']:.1%} | {payload['coverage']:.1%} |"
        )
    lines.extend(["", "## Profile Scale", ""])
    for name, payload in report["scale"].items():
        lines.append(
            f"- {name.replace('_', ' ').title()}: "
            f"{payload['observed']}/{payload['required']} "
            f"({payload['coverage']:.1%})"
        )
    lines.extend(["", "## Authoritative Run Contract", "", "```json"])
    lines.append(json.dumps(report["run_contract"], indent=2, sort_keys=True))
    lines.extend(["```", "", "## Retrieval Evolution Postmortem", "", "```json"])
    lines.append(
        json.dumps(report["retrieval_evolution"], indent=2, sort_keys=True)
    )
    lines.append("```")
    lines.extend(["", "## Current Bounded Company-Learning Evidence", "", "```json"])
    lines.append(json.dumps(report.get("current_bounded_company_learning") or {},
                            indent=2, sort_keys=True))
    lines.append("```")
    lines.extend(["", "## Hidden Pattern Recovery", "", "```json"])
    lines.append(
        json.dumps(
            report["dimensions"]["hidden_pattern_recovery"]["metrics"],
            indent=2,
            sort_keys=True,
        )
    )
    lines.extend(["```", "", "## Hard Failures", ""])
    lines.extend(
        f"- {item}" for item in report["hard_failures"]
    )
    if not report["hard_failures"]:
        lines.append("- None observed.")
    lines.extend(["", "## Proof Gaps", ""])
    lines.extend(f"- {item}" for item in report["proof_gaps"])
    if not report["proof_gaps"]:
        lines.append("- None reported.")
    lines.extend(["", "## Proof Boundaries", ""])
    lines.extend(f"- {item}" for item in report.get("proof_boundaries") or [])
    if not report.get("proof_boundaries"):
        lines.append("- None reported.")
    lines.extend(["", "## Claims Supported by This Run", ""])
    lines.extend(f"- {item}" for item in report["claims_supported"])
    if not report["claims_supported"]:
        lines.append("- No broad claim cleared both quality and coverage floors.")
    lines.extend(["", "## Claims This Run Does Not Support", ""])
    lines.extend(f"- {item}" for item in report["claims_not_supported"])
    lines.extend(["", "## Full Metric Detail", "", "```json"])
    lines.append(json.dumps(report["dimensions"], indent=2, sort_keys=True))
    lines.extend(["```", ""])
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
