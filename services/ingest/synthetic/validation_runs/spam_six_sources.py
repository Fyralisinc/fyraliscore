"""Scoped 6-source synthetic spam — the local pre-wiring ingestion check.

Drives EXACTLY 200 backfill observations per source for the six sources we are
about to wire with real credentials —

    slack, jira, notion, github, discord, telegram

— = 1200 backfill observations, PLUS a small live-ingress smoke per source,
through the REAL subprocess + Kafka data plane (oauth_poller → tenant_onboarding
→ source_onboarding → shard_fetch → normalizer → observation_writer), with the
raw tier on in-process moto-S3.

It reuses the proven `run_all_sources` orchestration verbatim (the all-source
overlap gate), narrowed to these six sources and dialed to 200 backfill
observations/tenant via per-source fixture params. The per-source counts are the
audited 200-exact knobs:

    slack    : 1 channel  × 200 messages                      = 200
    github   : 1 repo     × 100 events × 2 (issues+PRs)       = 200
    discord  : 1 channel  × 200 messages (channels MUST be 1) = 200
    jira     : 1 project  × 200 issues                        = 200
    notion   : 1 database × 200 pages                         = 200
    telegram : 1 dialog   × 200 messages                      = 200

Run against a PERSISTENT database (DATABASE_URL). The harness migrates +
truncates it at start, but leaves every landed row in place afterward for
cross-verification.

    DATABASE_URL=postgresql://company_os:company_os@localhost:5434/fyralis_signal_check \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 COMPANY_OS_ENV=test \
    OBS_EMBEDDING_MODE=cutover TENANTS_PER_SOURCE=1 LIVE_PER_TENANT=5 \
    ./.venv/bin/python -m services.ingest.synthetic.validation_runs.spam_six_sources
"""
from __future__ import annotations

import asyncio
import logging
import os

import services.ingest.synthetic.validation_runs.run_all_sources as R

SIX = ["slack", "jira", "notion", "github", "discord", "telegram"]
PER_SOURCE_BACKFILL = 200


def _params(source: str, slug: str) -> dict:
    """Audited fixture params yielding EXACTLY 200 backfill observations/tenant.

    The install identifier embeds `slug` (team_id / site_host / workspace_id /
    guild_id / telegram seed) so the tenant-scoped observations dedup never
    collapses two tenants' synthetic ids — belt-and-suspenders at 1 tenant/src.
    """
    if source == "slack":
        return {"team_id": f"T_{slug}", "channels": 1,
                "messages_per_channel": PER_SOURCE_BACKFILL, "page_size": 10}
    if source == "github":
        # EVENT_TYPES = (issues, pull_requests) → 2 obs/event; 100 × 2 = 200.
        # per_page MUST be ≥ events_per_repo (capped at the fetcher's
        # _DEFAULT_PER_PAGE=100): the github backfill fetcher drains ONE page
        # per shard-fetch call (cursor re-queue model) and the synthetic harness
        # does not re-queue beyond page 1, so with the default per_page=30 only
        # 30/type (60 total) land. A single 100-event page per type → full 200.
        return {"org_or_user": slug.replace("-", ""), "repos": 1,
                "events_per_repo": PER_SOURCE_BACKFILL // 2, "per_page": 100}
    if source == "discord":
        # channels MUST be 1: planner samples k=max(1,int(channels*0.05)); at
        # channels=1 the single channel is always fully sampled (200→200).
        return {"guild_id": f"G_{slug}", "channels": 1,
                "messages_per_channel": PER_SOURCE_BACKFILL}
    if source == "jira":
        return {"site_host": f"{slug}.atlassian.net", "projects": 1,
                "issues_per_project": PER_SOURCE_BACKFILL,
                "transitions_per_issue": 0, "comments_per_issue": 0}
    if source == "notion":
        return {"workspace_id": f"x3-{slug}-notion", "databases": 1,
                "pages_per_database": PER_SOURCE_BACKFILL, "loose_pages": 0,
                "blocks_per_page": 0, "comments_per_item": 0}
    if source == "telegram":
        return {"dialogs": 1, "messages_per_dialog": PER_SOURCE_BACKFILL,
                "seed": slug}
    raise ValueError(f"unsupported source: {source!r}")


def _install_overrides() -> None:
    """Narrow run_all_sources' module-level dispatch tables to the six sources
    and dial each to 200 backfill observations. run_all_sources resolves all of
    these as module globals at call time, so rebinding them here takes effect."""
    R.SOURCES = list(SIX)
    R._EXPECTED = {s: PER_SOURCE_BACKFILL for s in SIX}
    R._EXPECTED_LIVE_STATUS = {s: R._EXPECTED_LIVE_STATUS[s] for s in SIX}
    # Of the six, only jira rides the generic HMAC webhook driver; slack/github
    # use their dedicated webhook drivers, notion its webhook, discord/telegram
    # are gateway direct-dispatch.
    R._HMAC_SOURCES = tuple(s for s in R._HMAC_SOURCES if s in SIX)
    R._scen_params = _params

    # run_all_sources calls run_preflight(pool) over ALL sources (its own
    # _SOURCE_SPECS list, not narrowed by R.SOURCES). The realism gate fails
    # fast on any source whose fixture base date is outside the partition
    # window (e.g. gusto @ 2025-05). Narrow it to the six we actually run.
    _orig_preflight = R.run_preflight
    from services.ingest.synthetic.validation_runs.preflight import _SOURCE_SPECS
    _preflightable = [s for s in SIX if s in _SOURCE_SPECS]  # jira/notion/telegram have no spec

    async def _narrowed_preflight(pool, sources=None):
        return await _orig_preflight(pool, sources=_preflightable)

    R.run_preflight = _narrowed_preflight

    # Surface any non-clean subprocess stderr (run_all_sources keeps only the
    # return codes). The `reconciler rc=1` is otherwise opaque.
    from services.ingest.synthetic.backfill_harness import harness as _H
    _orig_teardown = _H.BackfillHarness._teardown_services

    def _teardown_with_dump(self):
        stderrs = _orig_teardown(self)
        for name, tail in stderrs.items():
            if tail and any(m in tail for m in ("Traceback", "Error", "error", "raise")):
                print(f"\n===== SUBPROCESS STDERR[{name}] (tail) =====\n{tail[-2500:]}\n")
        return stderrs

    _H.BackfillHarness._teardown_services = _teardown_with_dump


def main() -> int:
    logging.basicConfig(level=logging.WARNING)
    _install_overrides()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    tps = int(os.environ.get("TENANTS_PER_SOURCE", "1"))
    live = int(os.environ.get("LIVE_PER_TENANT", "5"))
    drain = float(os.environ.get("DRAIN_TIMEOUT_S", "600"))
    report = asyncio.run(R.run_all_sources(
        bootstrap_servers=bootstrap, tenants_per_source=tps,
        live_per_tenant=live, drain_timeout_s=drain))
    from services.ingest.synthetic.validation_runs.reports import render
    print(render(report))
    try:
        from services.ingest.synthetic.validation_runs.reports import write_report
        path = write_report(report)
        print(f"report written: {path}")
    except Exception as exc:  # noqa: BLE001 — never let report-write mask the run
        print(f"(report-write skipped: {exc!r})")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
