"""services/workers/maintenance — Wave 4-D background maintenance.

Three worker modules + an in-process scheduler.

* `daily.py` — decay + archival + alias cleanup + orphan detection +
  think_runs + region_lock_log cleanup. The hourly decay runs every
  hour; the other jobs run once per day.
* `weekly.py` — relationship maintenance + calibration updater +
  partition extension + memory-fabric decay + contestation aggregation.
* `monthly.py` — vacuum analyze, cold-partition migration notes,
  activation histogram, uncontested-high-confidence Model report.
* `scheduler.py` — simple in-process asyncio scheduler that runs each
  job on its own interval, with a `pg_advisory_lock` single-instance
  guard per job.

Spec references:
- ARCHITECTURE-FINAL.md §8 "Background maintenance workers" (lines
  2476-2539).
- BUILD-PLAN §5 Prompt 4.D (Wave 4-D deliverables).
- SCHEMA-LOCK.md S1.* (observations — orphan scan), S2.1 (activation
  decay), S6.1 (entity_aliases cleanup), W2.A2 (think_trigger_queue),
  W3.Q4 (think_region_lock_log cleanup), A1 (activation_coefficient).
"""

from services.workers.maintenance.daily import (  # noqa: F401
    DailyReport,
    hourly_decay_job,
    archive_decayed_job,
    entity_alias_cleanup,
    orphan_detection,
    think_runs_cleanup,
    region_lock_log_cleanup,
    run_daily,
)
from services.workers.maintenance.weekly import (  # noqa: F401
    WeeklyReport,
    extend_partitions_job,
    signal_memory_fabric_decay,
    contestation_aggregation_report,
    run_weekly,
)
from services.workers.maintenance.monthly import (  # noqa: F401
    MonthlyReport,
    vacuum_analyze_foundation,
    activation_histogram_report,
    uncontested_high_confidence_report,
    run_monthly,
)
from services.workers.maintenance.scheduler import (  # noqa: F401
    JobDescriptor,
    MaintenanceScheduler,
    advisory_lock_key,
)

__all__ = [
    # daily
    "DailyReport",
    "hourly_decay_job",
    "archive_decayed_job",
    "entity_alias_cleanup",
    "orphan_detection",
    "think_runs_cleanup",
    "region_lock_log_cleanup",
    "run_daily",
    # weekly
    "WeeklyReport",
    "extend_partitions_job",
    "signal_memory_fabric_decay",
    "contestation_aggregation_report",
    "run_weekly",
    # monthly
    "MonthlyReport",
    "vacuum_analyze_foundation",
    "activation_histogram_report",
    "uncontested_high_confidence_report",
    "run_monthly",
    # scheduler
    "JobDescriptor",
    "MaintenanceScheduler",
    "advisory_lock_key",
]
