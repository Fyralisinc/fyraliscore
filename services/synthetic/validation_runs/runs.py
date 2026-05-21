"""Run definitions (A29).

This spine ships **Run 1 (E2E)** — the clean-path backfill validation
across all four sources. Runs 2 (fault injection) and 3 (50-tenant
concurrency) are DEFERRED to the M-Validate-Live work-unit (ticket #47);
their config builders will live here alongside Run 1.

Per-source expected observation counts (clean, single-channel/repo
fixtures, no reshare):
  - gmail   : `messages`                         observations.
  - slack   : `messages_per_channel`             (channels=1).
  - discord : `messages_per_channel`             (channels=1 → fully
              sampled; M6.6's 5% sampling rounds max(1, .) to 1).
  - github  : `events_per_repo * 2`              (issues + pull_requests
              event types, repos=1).
"""
from __future__ import annotations

from services.synthetic.backfill_harness.scenarios import BackfillScenario


# Balanced fixture sizes — small + deterministic so counts are exact.
_GMAIL_MESSAGES = 5
_SLACK_MESSAGES = 5
_DISCORD_MESSAGES = 5
_GITHUB_EVENTS_PER_REPO = 3  # × 2 event types = 6 observations


def _gmail(slug: str) -> BackfillScenario:
    return BackfillScenario(
        tenant_slug=slug, source="gmail",
        fixture_params={"email": f"{slug}@val.example", "messages": _GMAIL_MESSAGES},
        expected_observation_count=_GMAIL_MESSAGES,
    )


def _slack(slug: str) -> BackfillScenario:
    return BackfillScenario(
        tenant_slug=slug, source="slack",
        fixture_params={"team_id": f"T_{slug}", "channels": 1,
                        "messages_per_channel": _SLACK_MESSAGES},
        expected_observation_count=_SLACK_MESSAGES,
    )


def _discord(slug: str) -> BackfillScenario:
    return BackfillScenario(
        tenant_slug=slug, source="discord",
        fixture_params={"guild_id": f"G_{slug}", "channels": 1,
                        "messages_per_channel": _DISCORD_MESSAGES},
        expected_observation_count=_DISCORD_MESSAGES,
    )


def _github(slug: str) -> BackfillScenario:
    return BackfillScenario(
        tenant_slug=slug, source="github",
        fixture_params={"org_or_user": slug, "repos": 1,
                        "events_per_repo": _GITHUB_EVENTS_PER_REPO},
        expected_observation_count=_GITHUB_EVENTS_PER_REPO * 2,
    )


_BUILDERS = {"gmail": _gmail, "slack": _slack,
             "discord": _discord, "github": _github}


def run1_scenarios(tenants_per_source: int = 4) -> list[BackfillScenario]:
    """Run 1 (E2E): `tenants_per_source` tenants per source.

    Default 4 → 16 tenants total (Decision 3). Each tenant backfills one
    source end-to-end; the suite proves observation production + parity
    across all four sources concurrently.
    """
    scenarios: list[BackfillScenario] = []
    for source, builder in _BUILDERS.items():
        for i in range(tenants_per_source):
            scenarios.append(builder(f"val-{source}-{i}"))
    return scenarios
