"""services/ingestion/writers — Postgres-side writers.

Per ingestion LLD §5.2-§5.4.

In M2.4 this package ships ONE writer:
  - `observation_writer.py` — Path B no-op writer. Consumes
    `ingestion.normalized`, logs each event, appends to an in-process
    shadow log. NO DB INSERT during M2 (the inline path remains the
    source of truth; the shadow path's correctness is asserted via
    set-equality between the inline observations table and this
    writer's shadow log — see test_e2e_shadow.py).

M3 will land:
  - `observation_writer.py` (rewrite) — batched INSERT into
    observations.
  - `embedding_worker.py` — Ollama + UPDATE.
  - `dlq_writer.py` — UPSERTs into ingestion_failures.

The M2.4 writer's consumer interface (group_id, topic, message
shape) is stable so the M3 rewrite can swap the recording function
for an INSERT without touching the dev compose stack or runbook.
"""

__all__: list[str] = []
