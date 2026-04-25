"""Environment / installation diagnostics for ``lsob doctor``."""

from __future__ import annotations

import importlib
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class CheckResult:
    name: str
    ok: bool
    required: bool
    message: str = ""


@dataclass
class DoctorReport:
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks if c.required)

    def exit_code(self) -> int:
        return 0 if self.ok else 1


def _check_workspace_installed() -> CheckResult:
    try:
        importlib.import_module("lsob_contracts")
        return CheckResult("workspace-installed", True, True, "lsob-contracts importable")
    except Exception as e:
        return CheckResult(
            "workspace-installed", False, True, f"cannot import lsob_contracts: {e}"
        )


def _check_anthropic_key() -> CheckResult:
    present = bool(os.environ.get("ANTHROPIC_API_KEY"))
    return CheckResult(
        "anthropic-api-key",
        ok=present,
        required=False,
        message="ANTHROPIC_API_KEY set" if present else "not set (judge runs will fail)",
    )


def _check_docker() -> CheckResult:
    if shutil.which("docker") is None:
        return CheckResult(
            "docker-daemon", False, False, "docker CLI not on PATH"
        )
    try:
        out = subprocess.run(
            ["docker", "info"], capture_output=True, text=True, timeout=5.0
        )
        if out.returncode == 0:
            return CheckResult("docker-daemon", True, False, "docker info succeeded")
        return CheckResult(
            "docker-daemon", False, False, f"docker info failed rc={out.returncode}"
        )
    except Exception as e:
        return CheckResult("docker-daemon", False, False, f"docker info error: {e}")


def _check_fixtures(fixtures_root: Path) -> CheckResult:
    if not fixtures_root.exists():
        return CheckResult(
            "fixtures", False, True, f"fixtures dir not found: {fixtures_root}"
        )
    matches = sorted(fixtures_root.glob("mini_corpus_*.json"))
    if not matches:
        return CheckResult(
            "fixtures", False, True, f"no mini_corpus_*.json under {fixtures_root}"
        )
    return CheckResult(
        "fixtures", True, True, f"found {len(matches)} mini corpus fixtures"
    )


def _check_baselines() -> CheckResult:
    from lsob_harness.registry import _load_baseline_registry  # local to avoid cycle

    registry = _load_baseline_registry()
    if registry is None:
        return CheckResult(
            "baselines",
            ok=False,
            required=False,
            message="baselines package not installed",
        )
    try:
        lister = getattr(registry, "list", None) or getattr(registry, "list_names", None)
        names = list(lister()) if callable(lister) else []
        failures: list[str] = []
        from lsob_contracts import SUTConfig  # local import to keep top light

        for name in names:
            try:
                registry.construct(name, SUTConfig(sut_name=name))
            except Exception as e:  # noqa: BLE001
                failures.append(f"{name}: {e}")
        if failures:
            return CheckResult(
                "baselines",
                False,
                False,
                f"{len(failures)} baselines failed to construct: {failures}",
            )
        return CheckResult(
            "baselines", True, False, f"{len(names)} baselines constructable"
        )
    except Exception as e:  # pragma: no cover
        return CheckResult("baselines", False, False, f"error enumerating baselines: {e}")


def run_doctor(
    *,
    workspace_root: Path | None = None,
    fixtures_root: Path | None = None,
) -> DoctorReport:
    if workspace_root is None:
        workspace_root = Path.cwd()
    if fixtures_root is None:
        fixtures_root = workspace_root / "fixtures"

    report = DoctorReport()
    report.checks.append(_check_workspace_installed())
    report.checks.append(_check_anthropic_key())
    report.checks.append(_check_docker())
    report.checks.append(_check_fixtures(fixtures_root))
    report.checks.append(_check_baselines())
    return report
