"""Launch-scope policy for packages under services/workers.

The process manifest tracks runnable processes. This module tracks the package
decision behind each worker implementation so a package cannot quietly sit in
the tree without being selected, scheduled, or explicitly flag-gated.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


WorkerLaunchStatus = Literal[
    "production_process",
    "housekeeper_default",
    "housekeeper_flag_gated",
    "dogfood_only",
]


@dataclass(frozen=True)
class WorkerLaunchPolicy:
    package: str
    status: WorkerLaunchStatus
    runtime_process: str | None = None
    housekeeper_jobs: tuple[str, ...] = ()
    enable_flags: tuple[str, ...] = ()
    notes: str = ""


_POLICIES: tuple[WorkerLaunchPolicy, ...] = (
    WorkerLaunchPolicy(
        package="anomaly_processor",
        status="production_process",
        runtime_process="anomaly_processor_worker",
        notes="Launch-critical anomaly-to-T3 path.",
    ),
    WorkerLaunchPolicy(
        package="entity_resolver",
        status="production_process",
        runtime_process="entity_resolver_worker",
        notes="Launch-critical deferred alias resolution path.",
    ),
    WorkerLaunchPolicy(
        package="housekeeper",
        status="production_process",
        runtime_process="housekeeper_worker",
        notes="Scheduler host for low-frequency lifecycle jobs.",
    ),
    WorkerLaunchPolicy(
        package="relationship_ontology_proposals",
        status="production_process",
        runtime_process="relationship_ontology_proposals_worker",
        notes="Dedicated proposal worker; housekeeper fallback remains flag-gated.",
    ),
    WorkerLaunchPolicy(
        package="sage_structural_features",
        status="production_process",
        runtime_process="sage_structural_features_worker",
        notes="Dedicated SAGE structural feature worker.",
    ),
    WorkerLaunchPolicy(
        package="sage_topology_optimizer",
        status="production_process",
        runtime_process="sage_topology_optimizer_worker",
        notes="Dedicated SAGE topology optimizer worker.",
    ),
    WorkerLaunchPolicy(
        package="calibration_updater",
        status="housekeeper_default",
        housekeeper_jobs=("calibration_updater",),
        notes="Weekly calibration offset refresh.",
    ),
    WorkerLaunchPolicy(
        package="deadline_resolver",
        status="housekeeper_default",
        housekeeper_jobs=("deadline_resolver",),
        notes="Prediction deadline to T2 trigger sweep.",
    ),
    WorkerLaunchPolicy(
        package="edge_drift",
        status="housekeeper_default",
        housekeeper_jobs=("edge_drift",),
        notes="Typed-edge parity sampling.",
    ),
    WorkerLaunchPolicy(
        package="maintenance",
        status="housekeeper_default",
        housekeeper_jobs=(
            "hourly_decay",
            "archive_decayed",
            "access_matview_refresh",
            "relationship_maintenance",
            "think_run_artifact_retention",
            "backup_recovery_metrics",
            "db_activity_metrics",
        ),
        notes="Default lifecycle and operational maintenance jobs.",
    ),
    WorkerLaunchPolicy(
        package="precipitation",
        status="housekeeper_flag_gated",
        housekeeper_jobs=("precipitation",),
        enable_flags=(
            "HOUSEKEEPER_ENABLE_PRECIPITATION",
            "HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS",
        ),
        notes="Embedding-heavy pattern formation; disabled by default.",
    ),
    WorkerLaunchPolicy(
        package="topology_sweeper",
        status="housekeeper_flag_gated",
        housekeeper_jobs=("topology_sweeper",),
        enable_flags=(
            "HOUSEKEEPER_ENABLE_TOPOLOGY_SWEEPER",
            "HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS",
        ),
        notes="Dogfood launcher exists; production housekeeper job is disabled by default.",
    ),
)


def worker_launch_policies() -> tuple[WorkerLaunchPolicy, ...]:
    return _POLICIES


def worker_launch_policy_by_package() -> dict[str, WorkerLaunchPolicy]:
    return {policy.package: policy for policy in _POLICIES}


__all__ = [
    "WorkerLaunchPolicy",
    "WorkerLaunchStatus",
    "worker_launch_policies",
    "worker_launch_policy_by_package",
]
