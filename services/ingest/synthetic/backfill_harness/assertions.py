"""Properties-based assertions on `HarnessResult`.

Per A22: assertions verify framework guarantees rather than exact
fixture data — robust to fixture evolution. Each assertion raises
`PropertyViolation` with operator-facing context on failure.

Invariants checked:
  1. assert_all_complete                — every installation reached
                                          tenant_onboarding_completed.
  2. assert_no_duplicate_observations   — per tenant, no duplicate
                                          observation external_ids.
  3. assert_cursor_monotonic_per_shard  — each shard's cursor advanced
                                          monotonically across pages.
  4. assert_completion_emitted_per_tenant — exactly-once completion
                                          per installation run.
  5. assert_observation_count_matches_fixture — total observation count
                                          per tenant equals the sum of its
                                          installation fixtures.
  6. assert_sibling_installation_identity — sibling installs have distinct
                                          triggers, rows, and onboarding runs.
  7. assert_reshare_cycles_completed    — when a scenario triggers
                                          reshare, the state machine
                                          cycles completed → in_progress
                                          → completed.
"""
from __future__ import annotations

from typing import Any

from services.ingest.synthetic.backfill_harness.harness import (
    HarnessResult,
    TenantOutcome,
)


class PropertyViolation(AssertionError):
    """Raised when a property-based assertion fails. The message
    carries enough context for the operator to investigate."""


def _by_tenant(
    outcomes: list[TenantOutcome],
) -> dict[object, list[TenantOutcome]]:
    grouped: dict[object, list[TenantOutcome]] = {}
    for outcome in outcomes:
        grouped.setdefault(outcome.tenant_id, []).append(outcome)
    return grouped


def assert_all_complete(result: HarnessResult) -> None:
    """Every installation reached tenant_onboarding_completed (clean or
    failed terminal state). Installations stuck in 'in_progress' are
    violations: the harness didn't wait long enough OR the framework
    has a bug."""
    incomplete = [
        t.identity_label
        for t in result.outcomes
        if not t.completion_observed
    ]
    if incomplete:
        raise PropertyViolation(
            f"{len(incomplete)} installation(s) did NOT reach completion within "
            f"the harness deadline: {incomplete}. Either the deadline was "
            f"too short OR the M6 chain stalled — check oauth_poller, "
            f"shard_fetch, reconciler subprocess stderr in the result."
        )


def assert_no_duplicate_observations(result: HarnessResult) -> None:
    """Per tenant, no duplicate observation external_ids. The framework
    contract: writer dedup via `observations.external_id` UNIQUE."""
    for tenant_outcomes in _by_tenant(result.outcomes).values():
        representative = tenant_outcomes[0]
        ext_ids = [o.get("external_id") for o in representative.observations
                   if o.get("external_id") is not None]
        if len(ext_ids) != len(set(ext_ids)):
            duplicates = [
                eid for eid in set(ext_ids)
                if ext_ids.count(eid) > 1
            ][:5]
            raise PropertyViolation(
                f"Tenant {representative.scenario.tenant_slug}: "
                f"duplicate observations found (first 5): {duplicates}. "
                f"Writer dedup broken OR fetcher emitted same record twice."
            )


def assert_cursor_monotonic_per_shard(result: HarnessResult) -> None:
    """Each shard's cursor pages_fetched advanced monotonically (no
    regression). Stored in workflow_states under the shard_fetch
    workflow_kind."""
    for t in result.outcomes:
        for shard_id, states in t.cursor_history.items():
            pages = [
                int(s.get("pages_fetched", 0)) for s in states
            ]
            if pages != sorted(pages):
                raise PropertyViolation(
                    f"Tenant {t.scenario.tenant_slug} shard {shard_id}: "
                    f"cursor pages_fetched regressed (non-monotonic): "
                    f"{pages}. Cursor advance invariant broken."
                )


def assert_completion_emitted_per_tenant(result: HarnessResult) -> None:
    """Exactly one completion signal per exact installation onboarding run."""
    for t in result.outcomes:
        n = t.completion_signal_count
        if n != 1:
            raise PropertyViolation(
                f"Installation {t.identity_label}: expected exactly 1 "
                f"tenant_onboarding_completed signal in Bridge inbox; "
                f"got {n}. Idempotency-key dedup broken OR signal "
                f"emitted from the wrong context."
            )


def assert_observation_count_matches_fixture(
    result: HarnessResult, *, tolerance: float = 0.0,
) -> None:
    """Total observation count per tenant equals all sibling fixture counts.

    `tolerance` allows a small fractional deviation (e.g., 0.1 for 10%)
    for sources with intentionally fuzzy fixture counts.
    """
    for tenant_outcomes in _by_tenant(result.outcomes).values():
        expected_counts = [
            outcome.scenario.expected_observation_count
            for outcome in tenant_outcomes
        ]
        if not any(expected_counts):
            continue  # Scenario didn't specify; skip the assertion.
        expected = sum(expected_counts)
        actual = len(tenant_outcomes[0].observations)
        deviation = abs(actual - expected)
        max_allowed = expected * tolerance
        if deviation > max_allowed:
            raise PropertyViolation(
                f"Tenant {tenant_outcomes[0].scenario.tenant_slug}: "
                f"expected {expected} observations (±{max_allowed:.1f}), "
                f"got {actual}. Either the fetcher dropped records OR "
                f"the fixture generator output drifted from expected."
            )


def assert_sibling_installation_identity(
    result: HarnessResult,
    *,
    installations_per_tenant: int,
) -> None:
    """Prove sibling scenarios remained exact through onboarding.

    This intentionally checks only durable identities: one shared tenant UUID,
    and distinct installation rows, triggers, and onboarding runs. It does not
    infer per-installation attribution from tenant-scoped Observation rows.
    """

    for tenant_outcomes in _by_tenant(result.outcomes).values():
        tenant_slug = tenant_outcomes[0].scenario.tenant_slug
        if len(tenant_outcomes) != installations_per_tenant:
            raise PropertyViolation(
                f"Tenant {tenant_slug}: expected "
                f"{installations_per_tenant} installation outcomes, got "
                f"{len(tenant_outcomes)}"
            )
        keys = [
            outcome.scenario.resolved_installation_key
            for outcome in tenant_outcomes
        ]
        identity_fields = {
            "installation keys": keys,
            "installation rows": [
                outcome.installation_row_id for outcome in tenant_outcomes
            ],
            "triggers": [outcome.trigger_id for outcome in tenant_outcomes],
            "onboarding runs": [
                outcome.onboarding_run_id for outcome in tenant_outcomes
            ],
        }
        for label, values in identity_fields.items():
            if any(value is None for value in values) or (
                len(set(values)) != installations_per_tenant
            ):
                raise PropertyViolation(
                    f"Tenant {tenant_slug}: sibling {label} are missing or "
                    f"ambiguous: {values!r}"
                )


def assert_reshare_cycles_completed(result: HarnessResult) -> None:
    """For tenants whose scenarios triggered reshare (fixture has
    history_events for Gmail, etc.), assert the state machine cycled:

        completed → in_progress (reshare) → completed (clean)

    Read from `source_onboarding_runs.reconciliation_pass_count`:
    non-zero means at least one reshare cycle ran AND completed."""
    for t in result.outcomes:
        if not t.expected_reshare:
            continue
        if t.reconciliation_pass_count < 1:
            raise PropertyViolation(
                f"Tenant {t.scenario.tenant_slug}: scenario configured "
                f"to trigger reshare, but reconciliation_pass_count == "
                f"{t.reconciliation_pass_count}. Reconciler's gap-fill "
                f"path may have been skipped — check reconciler stderr "
                f"and shard states."
            )


def _summarize(outcomes: list[TenantOutcome]) -> dict[str, Any]:
    """Diagnostic summary across all outcomes — useful in CI logs."""
    return {
        "total_tenants": len({outcome.tenant_id for outcome in outcomes}),
        "total_installations": len(outcomes),
        "completed": sum(1 for o in outcomes if o.completion_observed),
        "total_observations": sum(len(o.observations) for o in outcomes),
        "total_reshare_passes": sum(
            o.reconciliation_pass_count for o in outcomes
        ),
    }
