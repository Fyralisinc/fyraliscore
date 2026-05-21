"""Markdown validation-run reports (A29 / Decision 6).

Each run writes a human-readable report to `docs/validation/path_i/`.
The report is the operator-facing artifact: what ran, what passed, the
fixture-realism gate, the consumer-rc annotations (ticket #45), and the
per-source observation counts vs fixtures.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

# Consumer subprocesses whose non-zero SIGTERM rc is the documented
# ticket #45 gap (Decision 11). Anything OTHER than these codes — and
# anything non-zero on a NON-consumer service — is a real failure.
_CONSUMER_SERVICES = ("normalizer", "observation_writer")
_ACCEPTED_CONSUMER_RC = (0, -9, -15)


@dataclass
class SourceResult:
    source: str
    tenants: int
    expected_observations: int
    actual_observations: int

    @property
    def ok(self) -> bool:
        return self.actual_observations == self.expected_observations


@dataclass
class AssertionResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class RunReport:
    run_name: str
    run_number: int
    tenant_count: int
    started_at: dt.datetime
    wall_seconds: float
    preflight_lines: list[str] = field(default_factory=list)
    cleanup_line: str = ""
    source_results: list[SourceResult] = field(default_factory=list)
    subprocess_returncodes: dict[str, int] = field(default_factory=dict)
    assertions: list[AssertionResult] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    # M-Validate-Live (A30): free-form live-phase + coverage lines.
    live_lines: list[str] = field(default_factory=list)
    coverage_rows: list[tuple[str, str, str, str, str, str]] = field(
        default_factory=list)
    # Explicit verdict overrides the binary pass/fail when set
    # (READY / PARTIAL / NOT_READY) — Run 2 may be PARTIAL under FLAKY.
    verdict: str | None = None

    def rc_violations(self) -> list[str]:
        """Return rc entries that are real failures under Decision 11."""
        bad: list[str] = []
        for name, rc in self.subprocess_returncodes.items():
            if name in _CONSUMER_SERVICES:
                if rc not in _ACCEPTED_CONSUMER_RC:
                    bad.append(f"{name}={rc}")
            elif rc != 0:
                bad.append(f"{name}={rc}")
        return bad

    @property
    def passed(self) -> bool:
        return (
            all(s.ok for s in self.source_results)
            and all(a.passed for a in self.assertions)
            and not self.rc_violations()
        )


def _rc_annotation(name: str, rc: int) -> str:
    if name in _CONSUMER_SERVICES and rc in (-9, -15):
        return " — expected per ticket #45 (consumer graceful-shutdown)"
    if name in _CONSUMER_SERVICES and rc == 0:
        return " — clean (ticket #45 resolved)"
    if rc != 0:
        return " — **UNEXPECTED (real failure)**"
    return ""


def render(report: RunReport) -> str:
    r = report
    if r.verdict is not None:
        status = {"READY": "READY ✅", "PARTIAL": "PARTIAL ⚠️",
                  "NOT_READY": "NOT_READY ❌"}.get(r.verdict, r.verdict)
    else:
        status = "PASS ✅" if r.passed else "FAIL ❌"
    lines: list[str] = []
    lines.append(f"# Validation Run {r.run_number} — {r.run_name}")
    lines.append("")
    lines.append(f"**Status:** {status}")
    lines.append(f"**Started:** {r.started_at.isoformat()}")
    lines.append(f"**Wall time:** {r.wall_seconds:.1f}s")
    lines.append(f"**Tenants:** {r.tenant_count}")
    lines.append("")

    lines.append("## Pre-flight (fixture realism — Decision 12)")
    lines.append("")
    for ln in r.preflight_lines:
        lines.append(f"- {ln}")
    lines.append("")

    lines.append("## State reset (Decision 10)")
    lines.append("")
    lines.append(f"- {r.cleanup_line}")
    lines.append("")

    lines.append("## Per-source observation counts")
    lines.append("")
    lines.append("| Source | Tenants | Expected | Actual | Result |")
    lines.append("|---|---|---|---|---|")
    for s in r.source_results:
        mark = "✅" if s.ok else "❌"
        lines.append(
            f"| {s.source} | {s.tenants} | {s.expected_observations} | "
            f"{s.actual_observations} | {mark} |"
        )
    lines.append("")

    if r.live_lines:
        lines.append("## Live phase (A30)")
        lines.append("")
        for ln in r.live_lines:
            lines.append(f"- {ln}")
        lines.append("")

    if r.coverage_rows:
        lines.append("## Per-source × per-dimension coverage")
        lines.append("")
        lines.append(
            "| Source | Backfill | Live | Cross-path dedup | "
            "Signature gate | Replay idempotency |")
        lines.append("|---|---|---|---|---|---|")
        for row in r.coverage_rows:
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

    lines.append("## Assertions")
    lines.append("")
    for a in r.assertions:
        mark = "✅" if a.passed else "❌"
        detail = f" — {a.detail}" if a.detail else ""
        lines.append(f"- {mark} `{a.name}`{detail}")
    lines.append("")

    lines.append("## Subprocess exit codes (Decision 11)")
    lines.append("")
    for name, rc in r.subprocess_returncodes.items():
        lines.append(f"- `{name}`: rc={rc}{_rc_annotation(name, rc)}")
    lines.append("")

    if r.notes:
        lines.append("## Notes")
        lines.append("")
        for n in r.notes:
            lines.append(f"- {n}")
        lines.append("")

    return "\n".join(lines) + "\n"


def write_report(
    report: RunReport,
    out_dir: str | Path = "docs/validation/path_i",
) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"run{report.run_number}_report.md"
    path.write_text(render(report))
    return path
