from __future__ import annotations

import re
from pathlib import Path

import yaml  # type: ignore[import-untyped]


ROOT = Path(__file__).resolve().parents[4]


def _load_yaml(path: str) -> dict:
    with open(ROOT / path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _alert_rules() -> list[dict]:
    config = _load_yaml("observability/grafana/provisioning/alerting/alert-rules.yml")
    return [
        rule
        for group in config["groups"]
        for rule in group.get("rules", [])
    ]


def _recording_rules() -> dict[str, str]:
    recording = _load_yaml("observability/prometheus/rules/recording.yml")
    return {
        rule["record"]: rule["expr"]
        for group in recording["groups"]
        for rule in group.get("rules", [])
        if "record" in rule
    }


def test_grafana_alert_provisioning_covers_core_operational_failures() -> None:
    rules = _alert_rules()
    titles = {rule["title"] for rule in rules}

    assert {
        "WorkerHeartbeatStale",
        "WorkerScrapeDown",
        "InfraScrapeDown",
        "DLQDepthHigh",
        "ConsumerLagHigh",
        "SignatureFailureSpike",
        "EmbedFailureRatioHigh",
        "ThinkQueueBackpressure",
        "ThinkStaleLocks",
        "ThinkRetryExhausted",
        "DBPoolSaturated",
        "BackupRecoveryUnhealthy",
        "SchemaRLSDriftDetected",
        "ProductSLOBurnHigh",
        "LLMSpendBurnRateHigh",
    } <= titles

    for rule in rules:
        assert rule["data"][0]["datasourceUid"] == "fyralis-prom"
        assert rule["labels"]["severity"] in {"warning", "critical"}
        assert rule["annotations"]["summary"]


def test_worker_heartbeat_alert_uses_shared_recording_rule() -> None:
    rules = _alert_rules()
    by_title = {rule["title"]: rule for rule in rules}
    heartbeat_alert = by_title["WorkerHeartbeatStale"]
    assert heartbeat_alert["data"][0]["model"]["expr"] == (
        "fyralis:worker_heartbeat_age_seconds"
    )

    recording_rules = _recording_rules()
    heartbeat_expr = recording_rules["fyralis:worker_heartbeat_age_seconds"]
    assert "ingestion_heartbeat_age_seconds" in heartbeat_expr
    assert "worker_heartbeat_age_seconds" in heartbeat_expr
    assert "max by (worker)" in heartbeat_expr


def test_product_workflow_slo_recording_rules_use_bounded_workflow_metrics() -> None:
    recording_rules = _recording_rules()
    assert {
        "fyralis:product_workflow_request_rate:5m",
        "fyralis:product_workflow_error_ratio:5m",
        "fyralis:product_workflow_latency_p95_seconds:5m",
        "fyralis:product_workflow_error_budget_burn:5m",
        "fyralis:product_workflow_latency_budget_burn:5m",
    } <= set(recording_rules)

    assert "product_workflow_requests_total" in recording_rules[
        "fyralis:product_workflow_request_rate:5m"
    ]
    assert "product_workflow_requests_total{status_class=\"5xx\"}" in (
        recording_rules["fyralis:product_workflow_error_ratio:5m"]
    )
    assert "product_workflow_request_duration_seconds_bucket" in (
        recording_rules["fyralis:product_workflow_latency_p95_seconds:5m"]
    )
    assert "http_requests_total" not in recording_rules[
        "fyralis:product_workflow_request_rate:5m"
    ]


def test_product_dashboard_route_regex_covers_user_surfaces() -> None:
    product_dashboard = _load_yaml(
        "observability/grafana/dashboards/product-workflow-health.json"
    )
    variable = {
        item["name"]: item
        for item in product_dashboard["templating"]["list"]
    }["product_route_re"]
    route_re = re.compile(variable["query"])
    included = [
        "/v1/today",
        "/v1/ask/sessions/{session_id}/messages",
        "/v1/recommendations/{recommendation_id}/act",
        "/v1/forecasts/detail/{forecast_id}",
        "/v1/decision_deltas/{delta_id}/accept",
        "/v1/history/summary",
        "/v1/resolution_threads/{thread_id}/evaluate",
        "/v1/model/{node_id}/trace",
        "/v1/cards/{card_id}/probe",
        "/map/snapshot",
        "/model/items/{item_id}/trace",
        "/today/deltas/{delta_id}/apply",
        "/view/ceo/home",
        "/dashboard/revenue-at-risk",
        "/integrations/gmail/status",
        "/finance/{source}/status",
        "/rendering/card",
        "/models",
    ]
    excluded = [
        "/webhooks/slack",
        "/debug/whatsapp",
        "/internal/synthesis-reader/read",
        "/healthz",
        "/readyz",
        "/metrics",
        "/ingest/slack",
        "/v1/spec/forecasts",
    ]

    for route in included:
        assert route_re.fullmatch(route), route
    for route in excluded:
        assert not route_re.fullmatch(route), route


def test_schema_rls_drift_alert_uses_monitor_metrics() -> None:
    rules = _alert_rules()
    by_title = {rule["title"]: rule for rule in rules}
    drift_alert = by_title["SchemaRLSDriftDetected"]

    expr = drift_alert["data"][0]["model"]["expr"]
    assert 'schema_drift_check_status{status=~"drift|error"}' in expr
    assert "schema_drift_findings" in expr
    assert drift_alert["labels"]["severity"] == "critical"


def test_product_slo_burn_alert_uses_product_burn_recording_rules() -> None:
    rules = _alert_rules()
    by_title = {rule["title"]: rule for rule in rules}
    slo_alert = by_title["ProductSLOBurnHigh"]

    expr = slo_alert["data"][0]["model"]["expr"]
    assert "fyralis:product_workflow_error_budget_burn:5m" in expr
    assert "fyralis:product_workflow_latency_budget_burn:5m" in expr
    assert slo_alert["labels"]["severity"] == "warning"


def test_grafana_dashboard_provisioning_covers_core_dashboards() -> None:
    dashboard_dir = ROOT / "observability/grafana/dashboards"
    dashboards = {path.name for path in dashboard_dir.glob("*.json")}

    assert {
        "system-health.json",
        "ingestion-funnel.json",
        "webhook-ingress.json",
        "embeddings-ollama.json",
        "reasoning-llm-cost.json",
        "data-plane-infra.json",
        "product-workflow-health.json",
    } <= dashboards

    provider = _load_yaml("observability/grafana/provisioning/dashboards/dashboards.yml")
    providers = provider["providers"]
    assert providers[0]["disableDeletion"] is True
    assert providers[0]["allowUiUpdates"] is False

    product_dashboard = _load_yaml(
        "observability/grafana/dashboards/product-workflow-health.json"
    )
    panel_exprs = {
        target["expr"]
        for panel in product_dashboard["panels"]
        for target in panel.get("targets", [])
    }
    assert "fyralis:product_workflow_request_rate:5m" in panel_exprs
    assert "fyralis:product_workflow_error_ratio:5m" in panel_exprs
    assert "fyralis:product_workflow_latency_p95_seconds:5m" in panel_exprs
    assert "fyralis:product_workflow_error_budget_burn:5m" in panel_exprs
    assert "fyralis:product_workflow_latency_budget_burn:5m" in panel_exprs
