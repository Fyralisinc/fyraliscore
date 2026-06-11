"""services/ingest/integrations/metrics_export.py — per-source counters → text.

Pins one family per collector shape so an internal rename in a source's
metrics module fails loudly here instead of silently dropping that
source from the scrape:

  * mercury — flat snapshot() shape → integration_requests_total
  * slack   — install-shaped (_install_outcomes) → integration_install_total
  * github  — richer FR-017 names kept verbatim (github_webhook_*)

Counters are recorded only through each module's public record_*
helpers; the fixture resets all three modules before AND after each
test because the counters are process-global module state.
"""
from __future__ import annotations

import pytest

from services.ingest.integrations.github import metrics as github
from services.ingest.integrations.mercury import metrics as mercury
from services.ingest.integrations.metrics_export import (
    render_integration_metrics,
)
from services.ingest.integrations.slack import metrics as slack


@pytest.fixture(autouse=True)
def _reset_source_counters():
    mercury._reset_for_tests()
    slack.reset()
    github.reset()
    yield
    mercury._reset_for_tests()
    slack.reset()
    github.reset()


def test_mercury_request_normalized_to_integration_requests_total():
    mercury.record_request("ok")
    text = render_integration_metrics()
    assert 'integration_requests_total{source="mercury",outcome="ok"} 1' in text
    assert "# TYPE integration_requests_total counter" in text


def test_slack_install_normalized_to_integration_install_total():
    slack.record_install_outcome("success")
    text = render_integration_metrics()
    assert (
        'integration_install_total{source="slack",outcome="success"} 1'
    ) in text
    assert "# TYPE integration_install_total counter" in text


def test_github_counters_keep_their_fr017_names():
    github.record_webhook_received()
    text = render_integration_metrics()
    # GitHub families are NOT collapsed into the integration_* namespace.
    assert "github_webhook_received_total 1" in text
    assert "# TYPE github_webhook_received_total counter" in text
