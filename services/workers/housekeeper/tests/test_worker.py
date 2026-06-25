from __future__ import annotations

from datetime import timedelta

import pytest

from services.workers.housekeeper.worker import (
    build_housekeeper_descriptors,
    run_once_all,
)
from services.workers.maintenance.scheduler import JobDescriptor


def test_housekeeper_default_registry_keeps_expensive_jobs_disabled(monkeypatch):
    monkeypatch.delenv("HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS", raising=False)
    descriptors = build_housekeeper_descriptors()
    enabled = {d.name for d in descriptors if d.enabled}
    disabled = {d.name for d in descriptors if not d.enabled}

    assert {
        "deadline_resolver",
        "obligation_due_sweep",
        "hourly_decay",
        "archive_decayed",
        "access_matview_refresh",
        "relationship_maintenance",
        "think_run_artifact_retention",
        "backup_recovery_metrics",
        "db_activity_metrics",
        "calibration_updater",
        "edge_drift",
    } <= enabled
    assert {
        "topology_sweeper",
        "precipitation",
        "relationship_ontology_proposals",
        "sage_structural_features",
    } <= disabled


def test_housekeeper_expensive_registry_can_be_enabled():
    descriptors = build_housekeeper_descriptors(include_expensive=True)
    assert all(d.enabled for d in descriptors)


def test_housekeeper_specific_expensive_flag(monkeypatch):
    monkeypatch.setenv("HOUSEKEEPER_ENABLE_PRECIPITATION", "true")
    descriptors = build_housekeeper_descriptors(include_expensive=False)
    states = {d.name: d.enabled for d in descriptors}

    assert states["precipitation"] is True
    assert states["topology_sweeper"] is False


@pytest.mark.asyncio
async def test_run_once_all_runs_selected_enabled_jobs_only():
    calls: list[str] = []

    async def job(_pool):
        return None

    descriptors = [
        JobDescriptor("a", job, timedelta(seconds=60), enabled=True),
        JobDescriptor("b", job, timedelta(seconds=60), enabled=False),
        JobDescriptor("c", job, timedelta(seconds=60), enabled=True),
    ]

    class FakeScheduler:
        def __init__(self, *, pool, descriptors):
            self.descriptors = descriptors

        async def run_job_now(self, name: str) -> None:
            calls.append(name)

        def stats(self):
            return {d.name: {"runs": int(d.name in calls)} for d in self.descriptors}

    report = await run_once_all(
        object(),
        descriptors=descriptors,
        job_names=["a", "b"],
        scheduler_factory=FakeScheduler,
    )

    assert calls == ["a"]
    assert report.completed == 1
    assert report.failed == 0
    assert report.scheduler_stats["a"]["runs"] == 1
    assert report.scheduler_stats["b"]["runs"] == 0
