from __future__ import annotations

import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

FIXTURE_PATHS = (
    "scripts/bootstrap_integration_test.py",
    "scripts/sandbox_*.py",
    "scripts/report_think_representation_health.py",
    "services/ingest/synthetic",
    "tests/e2e",
    "tests/real_llm/scenarios",
)
PRODUCTION_PATHS = ("services", "lib", "scripts")

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")

ALLOWED_EMAIL_DOMAINS = {
    "example.com",
    "example.net",
    "example.org",
}
ALLOWED_EMAIL_SUFFIXES = (
    ".example",
    ".test",
    ".invalid",
    ".iam.gserviceaccount.com",
)

BLOCKED_LITERAL_PATTERNS = {
    "legacy customer-specific token": re.compile(r"\balpen(labs)?\b", re.I),
    "personal email provider": re.compile(r"@[A-Za-z0-9.-]*(gmail|yahoo|hotmail)\.com\b", re.I),
    "non-reserved acme domain": re.compile(r"(?<![A-Za-z0-9.-])acme\.com(?![A-Za-z0-9.-])", re.I),
    "non-reserved x domain": re.compile(r"(?<![A-Za-z0-9.-])x\.com(?![A-Za-z0-9.-])", re.I),
    "non-reserved y domain": re.compile(r"(?<![A-Za-z0-9.-])y\.com(?![A-Za-z0-9.-])", re.I),
    "non-reserved y1 domain": re.compile(r"(?<![A-Za-z0-9.-])y1\.com(?![A-Za-z0-9.-])", re.I),
    "legacy sample startup domain": re.compile(r"\bfernweave\.io\b", re.I),
}


def _tracked_files(paths: tuple[str, ...]) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", *paths],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [REPO_ROOT / line for line in result.stdout.splitlines() if line]


def _tracked_fixture_files() -> list[Path]:
    return _tracked_files(FIXTURE_PATHS)


def _tracked_production_files() -> list[Path]:
    files: list[Path] = []
    for path in _tracked_files(PRODUCTION_PATHS):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if "/tests/" in rel or rel.startswith("scripts/tests/"):
            continue
        files.append(path)
    return files


def _is_allowed_email_domain(domain: str) -> bool:
    normalized = domain.lower()
    return (
        normalized in ALLOWED_EMAIL_DOMAINS
        or any(normalized.endswith(suffix) for suffix in ALLOWED_EMAIL_SUFFIXES)
    )


def test_tracked_fixture_emails_use_reserved_domains() -> None:
    violations: list[str] = []
    for path in _tracked_fixture_files():
        text = path.read_text(errors="ignore")
        for match in EMAIL_RE.finditer(text):
            domain = match.group(1)
            if not _is_allowed_email_domain(domain):
                rel = path.relative_to(REPO_ROOT)
                violations.append(f"{rel}: {match.group(0)}")

    assert not violations, "Fixture/demo emails must use reserved domains:\n" + "\n".join(
        violations[:50]
    )


def test_tracked_fixtures_do_not_reference_legacy_customer_data() -> None:
    violations: list[str] = []
    for path in _tracked_fixture_files():
        text = path.read_text(errors="ignore")
        for label, pattern in BLOCKED_LITERAL_PATTERNS.items():
            if pattern.search(text):
                rel = path.relative_to(REPO_ROOT)
                violations.append(f"{rel}: {label}")

    assert not violations, "Fixture/demo files contain customer-shaped data:\n" + "\n".join(
        violations[:50]
    )


def test_production_code_does_not_reference_legacy_customer_tokens() -> None:
    pattern = BLOCKED_LITERAL_PATTERNS["legacy customer-specific token"]
    violations: list[str] = []
    for path in _tracked_production_files():
        if pattern.search(path.read_text(errors="ignore")):
            violations.append(path.relative_to(REPO_ROOT).as_posix())

    assert not violations, "Production code contains legacy customer tokens:\n" + "\n".join(
        violations[:50]
    )


def test_integration_bootstrap_does_not_print_bearer_tokens() -> None:
    text = (REPO_ROOT / "scripts/bootstrap_integration_test.py").read_text(
        encoding="utf-8"
    )

    assert "create_session" not in text
    assert "Bearer    :" not in text
    assert "session bearer" not in text.lower()
