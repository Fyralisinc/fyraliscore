"""Adversarial test suite for the synthesis layer.

Goal: deliberately try to break the substrate. Pass/fail counts are
secondary; the deliverable is TRIAGE.md.

Categories:
    1. Linguistic adversarial inputs       (cases_linguistic)
    2. Boundary / degenerate inputs        (cases_boundary)
    3. Sequencing & ordering               (cases_sequencing)
    4. Reconciliation pressure             (cases_reconciliation_pressure)
    5. Falsifier adversarial               (cases_falsifier_adversarial)
    6. Cascade & propagation pressure      (cases_cascade_pressure)
    7. Concurrency / race conditions       (concurrency_harness)
    8. Failure injection                   (failure_injection_harness)
    9. Multi-tenant isolation pressure     (cases_multitenant)
   10. Slow-burn / accumulation drift      (slow_burn_harness)

See COVERAGE_GAPS.md for the gap analysis these scenarios target,
and TRIAGE.md for the latest run's findings.
"""
