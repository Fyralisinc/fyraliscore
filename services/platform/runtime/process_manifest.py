"""Canonical runtime process manifest.

The application has several runtime launch surfaces: local dogfood
scripts, docker compose, and tests. This module keeps the process names
and commands in one typed list so new workers do not quietly appear in
one surface and disappear from another.
"""
from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import Literal


RuntimeMode = Literal["dogfood", "production"]


@dataclass(frozen=True)
class RuntimeProcess:
    name: str
    family: str
    command: tuple[str, ...] | None
    modes: frozenset[RuntimeMode]
    description: str
    compose_service: str | None = None
    log_file: str | None = None
    cwd: str = "."
    has_healthcheck: bool = False
    singleton: bool = False

    def compose_command(self) -> str | None:
        if self.command is None:
            return None
        return shlex.join(self.command)


def _proc(
    name: str,
    family: str,
    command: tuple[str, ...] | None,
    modes: tuple[RuntimeMode, ...],
    description: str,
    *,
    compose_service: str | None = None,
    log_file: str | None = None,
    cwd: str = ".",
    has_healthcheck: bool = False,
    singleton: bool = False,
) -> RuntimeProcess:
    return RuntimeProcess(
        name=name,
        family=family,
        command=command,
        modes=frozenset(modes),
        description=description,
        compose_service=compose_service,
        log_file=log_file,
        cwd=cwd,
        has_healthcheck=has_healthcheck,
        singleton=singleton,
    )


_PROCESSES: tuple[RuntimeProcess, ...] = (
    _proc(
        "gateway",
        "app",
        None,
        ("dogfood", "production"),
        "FastAPI gateway and in-process schedulers.",
        compose_service="gateway",
        log_file="gateway.log",
        has_healthcheck=True,
    ),
    _proc(
        "think_worker",
        "reasoning",
        ("python", "scripts/run_think_worker.py"),
        ("dogfood", "production"),
        "Claim reasoning worker.",
        compose_service="think_worker",
        log_file="think_worker.log",
        has_healthcheck=True,
    ),
    _proc(
        "post_commit_worker",
        "reasoning",
        ("python", "scripts/run_post_commit_worker.py"),
        ("dogfood", "production"),
        "Post-commit cascade and follow-up worker.",
        compose_service="post_commit_worker",
        log_file="post_commit_worker.log",
        has_healthcheck=True,
    ),
    _proc(
        "anomaly_processor_worker",
        "reasoning",
        ("python", "scripts/run_anomaly_processor_worker.py"),
        ("production",),
        "Detects anomaly candidates, records signal fabric, and enqueues T3 triggers.",
        compose_service="anomaly_processor_worker",
        log_file="anomaly_processor_worker.log",
        has_healthcheck=True,
    ),
    _proc(
        "entity_resolver_worker",
        "reasoning",
        ("python", "scripts/run_entity_resolver_worker.py"),
        ("production",),
        "Resolves deferred entity aliases from observations and re-enqueues material context.",
        compose_service="entity_resolver_worker",
        log_file="entity_resolver_worker.log",
        has_healthcheck=True,
    ),
    _proc(
        "schema_drift_monitor",
        "operations",
        ("python", "scripts/run_schema_drift_monitor.py"),
        ("production",),
        "Continuously checks live schema and RLS coverage against the schema lock.",
        compose_service="schema_drift_monitor",
        log_file="schema_drift_monitor.log",
        has_healthcheck=True,
        singleton=True,
    ),
    _proc(
        "topology_sweeper",
        "reasoning",
        ("python", "scripts/run_topology_sweeper.py"),
        ("dogfood",),
        "Local latent relationship-field refresh loop.",
        log_file="topology_sweeper.log",
    ),
    _proc(
        "ui",
        "app",
        ("npm", "run", "dev"),
        ("dogfood",),
        "Frontend UI.",
        log_file="ui.log",
        cwd="ui",
    ),
    _proc(
        "oauth_poller",
        "ingest-workflow",
        ("python", "-m", "services.ingest.ingestion.workflows.oauth_poller"),
        ("production",),
        "OAuth install polling workflow.",
        compose_service="oauth_poller",
        has_healthcheck=True,
    ),
    _proc(
        "tenant_onboarding",
        "ingest-workflow",
        ("python", "-m", "services.ingest.ingestion.workflows.tenant_onboarding"),
        ("production",),
        "Tenant onboarding workflow.",
        compose_service="tenant_onboarding",
        has_healthcheck=True,
    ),
    _proc(
        "source_onboarding",
        "ingest-workflow",
        ("python", "-m", "services.ingest.ingestion.workflows.source_onboarding"),
        ("production",),
        "Source onboarding workflow.",
        compose_service="source_onboarding",
        has_healthcheck=True,
    ),
    _proc(
        "shard_fetch",
        "ingest-workflow",
        ("python", "-m", "services.ingest.ingestion.workflows.shard_fetch"),
        ("production",),
        "Backfill shard fetch workflow.",
        compose_service="shard_fetch",
        has_healthcheck=True,
    ),
    _proc(
        "reconciler",
        "ingest-workflow",
        ("python", "-m", "services.ingest.ingestion.workflows.reconciler"),
        ("production",),
        "Onboarding reconciliation workflow.",
        compose_service="reconciler",
        has_healthcheck=True,
    ),
    _proc(
        "extension_workers",
        "extensions",
        ("python", "-m", "lib.extensions.run_workers"),
        ("production",),
        "Extension-contributed background worker supervisor.",
        compose_service="extension_workers",
        has_healthcheck=True,
        singleton=True,
    ),
    _proc(
        "feels_onboarded_monitor",
        "ingest-workflow",
        (
            "python",
            "-m",
            "services.ingest.ingestion.workflows.feels_onboarded_monitor",
        ),
        ("production",),
        "Onboarding progress monitor.",
        compose_service="feels_onboarded_monitor",
        has_healthcheck=True,
        singleton=True,
    ),
    _proc(
        "periodic_reconciler",
        "ingest-workflow",
        ("python", "-m", "services.ingest.ingestion.workflows.periodic_reconciler"),
        ("production",),
        "Steady-state ingestion gap reconciler.",
        compose_service="periodic_reconciler",
        has_healthcheck=True,
    ),
    _proc(
        "normalizer",
        "ingest-consumer",
        ("python", "-m", "services.ingest.ingestion.normalizer.worker"),
        ("production",),
        "Raw-envelope normalizer.",
        compose_service="normalizer",
        has_healthcheck=True,
    ),
    _proc(
        "observation_writer",
        "ingest-consumer",
        ("python", "-m", "services.ingest.ingestion.writers.observation_writer"),
        ("production",),
        "Normalized observation writer.",
        compose_service="observation_writer",
        has_healthcheck=True,
    ),
    _proc(
        "dlq_writer",
        "ingest-consumer",
        (
            "python",
            "-m",
            "services.ingest.ingestion.writers.dlq_writer.dlq_writer",
        ),
        ("production",),
        "Dead-letter writer.",
        compose_service="dlq_writer",
        has_healthcheck=True,
    ),
    _proc(
        "summarization_worker",
        "ingest-consumer",
        (
            "python",
            "-m",
            "services.ingest.ingestion.writers.summarization_worker.summarization_worker",
        ),
        ("production",),
        "Kafka large-document summarization worker.",
        compose_service="summarization_worker",
        has_healthcheck=True,
    ),
    _proc(
        "summarization_batch_worker",
        "ingest-consumer",
        (
            "python",
            "-m",
            "services.ingest.ingestion.writers.summarization_batch_worker",
        ),
        ("production",),
        "OpenAI Batch API worker for backfill document summarization.",
        compose_service="summarization_batch_worker",
        has_healthcheck=True,
    ),
    _proc(
        "embedding_worker",
        "ingest-consumer",
        (
            "python",
            "-m",
            "services.ingest.ingestion.writers.embedding_worker.embedding_worker",
        ),
        ("production",),
        "Kafka embedding worker.",
        compose_service="embedding_worker",
        has_healthcheck=True,
    ),
    _proc(
        "embedding_backlog",
        "ingest-recovery",
        ("python", "-m", "services.ingest.ingestion.recovery.embedding_backlog"),
        ("production",),
        "DB-scanning embedding backlog drainer.",
        compose_service="embedding_backlog",
        has_healthcheck=True,
    ),
    _proc(
        "circuit_breaker",
        "ingest-recovery",
        ("python", "-m", "services.ingest.ingestion.feature_flags"),
        ("production",),
        "Ingestion Kafka cutover circuit breaker.",
        compose_service="circuit_breaker",
        has_healthcheck=True,
        singleton=True,
    ),
    _proc(
        "discord_gateway_worker",
        "live-source",
        ("python", "scripts/run_discord_gateway_worker.py"),
        ("production",),
        "Discord gateway session worker.",
        compose_service="discord_gateway_worker",
        has_healthcheck=True,
        singleton=True,
    ),
    _proc(
        "telegram_gateway_worker",
        "live-source",
        ("python", "scripts/run_telegram_gateway_worker.py"),
        ("production",),
        "Telegram MTProto gateway session worker.",
        compose_service="telegram_gateway_worker",
        has_healthcheck=True,
        singleton=True,
    ),
    _proc(
        "signal_gateway_worker",
        "live-source",
        ("python", "scripts/run_signal_gateway_worker.py"),
        ("production",),
        "Signal linked-device (signal-cli JSON-RPC) gateway session worker.",
        compose_service="signal_gateway_worker",
        has_healthcheck=True,
        singleton=True,
    ),
    _proc(
        "gmail_watch_scheduler",
        "live-source",
        ("python", "scripts/run_gmail_watch_scheduler.py"),
        ("production",),
        "Gmail watch renewal scheduler.",
        compose_service="gmail_watch_scheduler",
        has_healthcheck=True,
    ),
    _proc(
        "gmail_history_poller",
        "live-source",
        ("python", "scripts/run_gmail_history_poller.py"),
        ("production",),
        "Gmail history poller.",
        compose_service="gmail_history_poller",
        has_healthcheck=True,
    ),
    _proc(
        "google_calendar_live_poller",
        "live-source",
        ("python", "scripts/run_google_calendar_live_poller.py"),
        ("production",),
        "Google Calendar live poller.",
        compose_service="google_calendar_live_poller",
        has_healthcheck=True,
    ),
    _proc(
        "google_drive_live_poller",
        "live-source",
        ("python", "scripts/run_google_drive_live_poller.py"),
        ("production",),
        "Google Drive live poller.",
        compose_service="google_drive_live_poller",
        has_healthcheck=True,
    ),
    _proc(
        "google_calendar_watch_scheduler",
        "live-source",
        ("python", "scripts/run_google_calendar_watch_scheduler.py"),
        ("production",),
        "Google Calendar watch renewal scheduler.",
        compose_service="google_calendar_watch_scheduler",
        has_healthcheck=True,
    ),
    _proc(
        "google_drive_watch_scheduler",
        "live-source",
        ("python", "scripts/run_google_drive_watch_scheduler.py"),
        ("production",),
        "Google Drive watch renewal scheduler.",
        compose_service="google_drive_watch_scheduler",
        has_healthcheck=True,
    ),
    _proc(
        "sage_structural_features_worker",
        "reasoning",
        ("python", "scripts/run_sage_structural_features_worker.py"),
        ("production",),
        "SAGE structural feature refresh worker.",
        compose_service="sage_structural_features_worker",
        has_healthcheck=True,
    ),
    _proc(
        "sage_topology_optimizer_worker",
        "reasoning",
        ("python", "scripts/run_sage_topology_optimizer_worker.py"),
        ("production",),
        "SAGE topology optimizer worker.",
        compose_service="sage_topology_optimizer_worker",
        has_healthcheck=True,
    ),
    _proc(
        "housekeeper_worker",
        "reasoning",
        ("python", "scripts/run_housekeeper_worker.py"),
        ("production",),
        "Scheduled lifecycle and maintenance job worker.",
        compose_service="housekeeper_worker",
        has_healthcheck=True,
    ),
    _proc(
        "relationship_ontology_proposals_worker",
        "reasoning",
        ("python", "scripts/run_relationship_ontology_proposals_worker.py"),
        ("production",),
        "Relationship ontology proposal worker.",
        compose_service="relationship_ontology_proposals_worker",
        has_healthcheck=True,
    ),
)


def all_processes() -> tuple[RuntimeProcess, ...]:
    return _PROCESSES


def processes_for(mode: RuntimeMode) -> tuple[RuntimeProcess, ...]:
    return tuple(p for p in _PROCESSES if mode in p.modes)


def dogfood_processes() -> tuple[RuntimeProcess, ...]:
    return processes_for("dogfood")


def production_processes() -> tuple[RuntimeProcess, ...]:
    return processes_for("production")


def process_by_name(name: str) -> RuntimeProcess:
    for process in _PROCESSES:
        if process.name == name:
            return process
    raise KeyError(name)


__all__ = [
    "RuntimeProcess",
    "all_processes",
    "dogfood_processes",
    "process_by_name",
    "processes_for",
    "production_processes",
]
