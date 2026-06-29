from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_TEMPLATE = REPO_ROOT / ".env.production.example"
INTEGRATION_ROOT = REPO_ROOT / "services" / "ingest" / "integrations"

CLIENT_RATE_LIMIT_ENV_RE = re.compile(
    r"os\.environ\.get\(\s*[\"']([A-Z0-9]+_RL_(?:MAX_ATTEMPTS|MAX_SLEEP_SEC))[\"']"
)
ENV_KEY_RE = re.compile(r"^\s*#?\s*([A-Z0-9_]+)\s*=")

REQUIRED_SOURCE_RATE_LIMIT_KEYS = {
    "REDIS_URL",
    "SHARD_FETCH_RATE_LIMIT",
    "SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC",
    "SLACK_API_TIER",
}


def _source_client_rate_limit_envs() -> set[str]:
    names: set[str] = set()
    for path in sorted(INTEGRATION_ROOT.glob("*/client.py")):
        names.update(CLIENT_RATE_LIMIT_ENV_RE.findall(path.read_text(encoding="utf-8")))
    return names


def _production_env_template_keys() -> set[str]:
    keys: set[str] = set()
    for line in ENV_TEMPLATE.read_text(encoding="utf-8").splitlines():
        match = ENV_KEY_RE.match(line)
        if match:
            keys.add(match.group(1))
    return keys


def test_source_api_retry_budget_envs_are_documented_for_production() -> None:
    expected = _source_client_rate_limit_envs() | REQUIRED_SOURCE_RATE_LIMIT_KEYS

    missing = sorted(expected - _production_env_template_keys())

    assert missing == []
