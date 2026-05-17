"""services/ingestion/normalizer — M2.3 no-op normalizer worker pool.

Per ingestion LLD §5.2 + M2 work-order §M2.3.

The normalizer is the second stage of the M2 raw → normalized →
write pipeline:

    ingestion.raw (envelopes) → normalizer → ingestion.normalized
                                              (drafts; M2.4 logs,
                                               M3+ persists)

Path B contract: this worker package MUST NOT touch the database.
The static + runtime proofs live in
`services/ingestion/normalizer/tests/test_worker_no_db_access.py`.
"""

__all__: list[str] = []
