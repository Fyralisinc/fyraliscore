"""Contract-driven per-source counters → Prometheus text.

Pins one family per source-owned exporter shape so an internal rename fails
loudly instead of silently dropping a source from the scrape:

  * mercury — flat snapshot() shape → integration_requests_total
  * slack   — install state → integration_install_total
  * github  — richer FR-017 names kept verbatim (github_webhook_*)

Counters are recorded only through each module's public record_*
helpers; the fixture resets the modules before AND after each
test because the counters are process-global module state.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from services.ingest.integrations.discord.gateway import metrics as discord_gateway
from services.ingest.integrations.github import metrics as github
from services.ingest.integrations.mercury import metrics as mercury
from services.ingest.integrations.metrics_contract import MetricSample
from services.ingest.integrations import metrics_export
from services.ingest.source_contract.catalog import (
    SOURCE_DEFINITIONS,
    source_definition,
)
from services.ingest.source_contract.runtime import resolve_callable_reference
from services.ingest.integrations.slack import metrics as slack


@pytest.fixture(autouse=True)
def _reset_source_counters():
    mercury._reset_for_tests()
    slack.reset()
    github.reset()
    discord_gateway.reset()
    yield
    mercury._reset_for_tests()
    slack.reset()
    github.reset()
    discord_gateway.reset()


def test_mercury_request_normalized_to_integration_requests_total():
    mercury.record_request("ok")
    text = metrics_export.render_integration_metrics()
    assert 'integration_requests_total{source="mercury",outcome="ok"} 1' in text
    assert "# TYPE integration_requests_total counter" in text


def test_slack_install_normalized_to_integration_install_total():
    slack.record_install_outcome("success")
    text = metrics_export.render_integration_metrics()
    assert (
        'integration_install_total{source="slack",outcome="success"} 1'
    ) in text
    assert "# TYPE integration_install_total counter" in text


def test_github_counters_keep_their_fr017_names():
    github.record_webhook_received()
    text = metrics_export.render_integration_metrics()
    # GitHub families are NOT collapsed into the integration_* namespace.
    assert "github_webhook_received_total 1" in text
    assert "# TYPE github_webhook_received_total counter" in text


def test_source_owned_exporters_preserve_duration_and_gateway_shapes():
    slack.observe_install_duration(0.25)
    slack.observe_install_duration(0.75)
    discord_gateway.inc("discord_gateway_dispatch_total", event="MESSAGE_CREATE")
    discord_gateway.set_gauge(
        "discord_gateway_connection_state",
        1,
        state="ready",
    )

    text = metrics_export.render_integration_metrics()

    assert (
        'integration_install_duration_p95_seconds{source="slack"} 0.75'
    ) in text
    assert (
        'discord_gateway_dispatch_total{event="MESSAGE_CREATE"} 1'
    ) in text
    assert (
        'discord_gateway_connection_state{state="ready"} 1'
    ) in text


def test_every_catalog_metrics_binding_resolves_and_returns_samples():
    bindings = 0
    for source in SOURCE_DEFINITIONS:
        for reference in source.metrics_export_bindings:
            bindings += 1
            exporter = resolve_callable_reference(reference)
            samples = tuple(exporter(source.source_id))
            assert all(isinstance(sample, MetricSample) for sample in samples)

    assert bindings > 0


def test_catalog_binding_controls_collector_membership(monkeypatch):
    source = replace(
        source_definition("slack"),
        metrics_export_bindings=("tests.fake_metrics:export",),
    )

    def resolve(reference: str):
        assert reference == "tests.fake_metrics:export"

        def export(source_id: str):
            return (
                MetricSample(
                    name="contract_owned_total",
                    kind="counter",
                    labels=(("source", source_id),),
                    value=3,
                ),
            )

        return export

    monkeypatch.setattr(metrics_export, "SOURCE_DEFINITIONS", (source,))
    monkeypatch.setattr(metrics_export, "resolve_callable_reference", resolve)

    assert metrics_export.render_integration_metrics() == (
        "# TYPE contract_owned_total counter\n"
        'contract_owned_total{source="slack"} 3\n'
    )


def test_missing_optional_metrics_module_does_not_break_scrape(monkeypatch):
    source = replace(
        source_definition("slack"),
        metrics_export_bindings=("tests.missing_metrics:export",),
    )

    def fail_to_resolve(_reference: str):
        raise ImportError("optional integration is not installed")

    monkeypatch.setattr(metrics_export, "SOURCE_DEFINITIONS", (source,))
    monkeypatch.setattr(
        metrics_export,
        "resolve_callable_reference",
        fail_to_resolve,
    )

    assert metrics_export.render_integration_metrics() == ""
