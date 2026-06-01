"""Composed validation runs (M-Validate spine).

A standalone, operator-invokable runner that composes the synthetic
backfill path (M6.7) into observation-production validation across all
four sources. Run with:

    python -m services.synthetic.validation_runs.runner --run=1

This package (the "spine") delivers:

  - `preflight`  — fixture-realism behavioral gate (A29 / Decision 12).
  - `cleanup`    — Kafka topic + moto bucket reset between runs (D10).
  - `moto_lifecycle` — runner-owned moto-server (D9).
  - `assertions` — cross-path external_id uniqueness + zero-partition-
    missing, layered on the backfill_harness assertions (D5).
  - `reports`    — markdown run reports in `docs/validation/path_i/` (D6).
  - `runner`     — orchestrates Run 1 (E2E) backfill across 4 sources
    with the consumer-drain wait (D4) and consumer-rc policy (D11).

The LIVE phase (4 in-process generators) and Runs 2 (fault) + 3
(concurrency) are DEFERRED to the M-Validate-Live work-unit
(ticket #47) — see A29's deferral sub-section.
"""
from __future__ import annotations
