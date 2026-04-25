"""services/observations — observation store (Wave 1-A).

See BUILD-PLAN.md §2 Prompt 1.A, SCHEMA-LOCK.md S1.1 / S1.2,
ARCHITECTURE-FINAL.md §1.

Public API:
- repo.ObservationRepository — CRUD + retrieval over the partitioned
  `observations` parent table.
- events.emit_observations_new — post-commit NOTIFY helper.
- state_change.emit_state_change — shared helper for other services to
  record internal state_change observations inside their own
  transaction.
- partitions.ensure_partitions / ensure_next_n_months — monthly
  partition creator run at startup and from the Wave-4-D cron.
"""
from __future__ import annotations
