#!/usr/bin/env python3
"""Self-test for the WS-BOUNDARY OTel Collector (Invariant I1).

Validates the boundary config WITHOUT requiring the otelcol binary:

  1. YAML parses (otel-collector-config.yaml, tier_policy.yaml,
     prometheus_remote_write_overlay.yml).
  2. Structural asserts: the redaction allowlist filter, the label-drop
     transform, the identity `resource` processor, and the
     prometheusremotewrite exporter all exist and are wired into the T1
     metrics pipeline. The collector does NOT set X-Scope-OrgID.
  3. Behavioral asserts: a sample PII-ish label is dropped by the rule set and
     an allowlisted metric family is kept — simulated against the actual rules
     parsed from the config (not a hand-copy).

If `otelcol-contrib` is on PATH, also run `otelcol-contrib validate`.

Exit 0 = pass. Run:
    /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python selftest.py
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = os.path.join(HERE, "otel-collector-config.yaml")
TIER = os.path.join(HERE, "tier_policy.yaml")
OVERLAY = os.path.join(HERE, "prometheus_remote_write_overlay.yml")

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))


def load(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# 1. YAML parses
# ---------------------------------------------------------------------------
try:
    cfg = load(CFG)
    check("otel-collector-config.yaml parses as YAML", isinstance(cfg, dict))
except Exception as e:  # noqa: BLE001
    check("otel-collector-config.yaml parses as YAML", False, repr(e))
    cfg = {}

for label, path in [("tier_policy.yaml", TIER), ("remote_write overlay", OVERLAY)]:
    try:
        load(path)
        check(f"{label} parses as YAML", True)
    except Exception as e:  # noqa: BLE001
        check(f"{label} parses as YAML", False, repr(e))

receivers = cfg.get("receivers", {}) or {}
processors = cfg.get("processors", {}) or {}
exporters = cfg.get("exporters", {}) or {}
service = cfg.get("service", {}) or {}
pipelines = (service.get("pipelines", {}) or {})
metrics_pipe = pipelines.get("metrics", {}) or {}

# ---------------------------------------------------------------------------
# 2. Structural asserts
# ---------------------------------------------------------------------------
check("prometheus receiver scrapes the data plane", "prometheus" in receivers)

# scrape targets reference the worker :9300 metrics endpoint
scrape_text = yaml.safe_dump(receivers)
check("scrape targets include worker :9300", ":9300" in scrape_text)

check("filter/allowlist processor exists", "filter/allowlist" in processors)
check("transform/redact-labels processor exists", "transform/redact-labels" in processors)
check("resource (identity) processor exists", "resource" in processors)
check("prometheusremotewrite exporter exists", "prometheusremotewrite" in exporters)

# identity attributes present on the resource processor
res_attrs = {a.get("key") for a in (processors.get("resource", {}) or {}).get("attributes", [])}
for ident in ("tenant_id", "deployment_id", "region"):
    check(f"resource adds identity label '{ident}'", ident in res_attrs)

# pipeline wiring: the redaction + identity processors are actually in the
# metrics pipeline (a processor that exists but isn't wired enforces nothing).
mp = metrics_pipe.get("processors", []) or []
check("metrics pipeline wires filter/allowlist", "filter/allowlist" in mp)
check("metrics pipeline wires transform/redact-labels", "transform/redact-labels" in mp)
check("metrics pipeline wires resource (identity)", "resource" in mp)
check(
    "metrics pipeline exports via prometheusremotewrite",
    "prometheusremotewrite" in (metrics_pipe.get("exporters", []) or []),
)

# I1 by absence: no logs/traces pipeline at the default tier
check("no logs pipeline at T1 (logs cannot egress)", "logs" not in pipelines)
check("no traces pipeline at T1 (traces cannot egress)", "traces" not in pipelines)

# remote-write goes through the auth proxy, and the collector does NOT set X-Scope-OrgID
prw = exporters.get("prometheusremotewrite", {}) or {}
endpoint = str(prw.get("endpoint", ""))
check("remote-write endpoint targets the auth proxy", "AUTH_PROXY" in endpoint or "/api/v1/push" in endpoint)
check(
    "collector does NOT set X-Scope-OrgID (proxy injects it)",
    "X-Scope-OrgID" not in yaml.safe_dump(prw),
)
check(
    "remote-write retries during a CP outage (I3)",
    bool((prw.get("retry_on_failure", {}) or {}).get("enabled")),
)

# ---------------------------------------------------------------------------
# 3. Behavioral asserts — simulate the rules parsed from the config
# ---------------------------------------------------------------------------
# 3a. Label drop: parse the delete_key(...) keys out of transform/redact-labels
redact = processors.get("transform/redact-labels", {}) or {}
delete_keys: set[str] = set()
for block in redact.get("metric_statements", []) or []:
    for stmt in block.get("statements", []) or []:
        m = re.search(r'delete_key\(attributes,\s*"([^"]+)"\)', stmt)
        if m:
            delete_keys.add(m.group(1))

PII_SAMPLES = ["email", "installation_id", "user_id", "signal_id", "url", "content"]
for lbl in PII_SAMPLES:
    check(f"PII/high-card label '{lbl}' is dropped by rule set", lbl in delete_keys)

ENUM_KEPT = ["worker", "source", "provider", "state", "job"]
for lbl in ENUM_KEPT:
    check(f"bounded-enum label '{lbl}' is NOT dropped", lbl not in delete_keys)

# 3b. Family allowlist: extract the allowlist expression and simulate keep/drop.
allow = processors.get("filter/allowlist", {}) or {}
metric_exprs = (allow.get("metrics", {}) or {}).get("metric", []) or []
allow_text = " ".join(str(e) for e in metric_exprs)

# Build a python-evaluable predicate that mirrors the OTTL allowlist: a metric
# is KEPT iff one of the name conditions is true. We translate the OTTL
# `name == "x"` and `IsMatch(name, "re")` clauses into a single regex set.
exact_names = set(re.findall(r'name\s*==\s*"([^"]+)"', allow_text))
regexes = re.findall(r'IsMatch\(name,\s*"([^"]+)"\)', allow_text)
compiled = [re.compile(r) for r in regexes]


def kept(metric_name: str) -> bool:
    if metric_name in exact_names:
        return True
    return any(rx.match(metric_name) for rx in compiled)


# allowlisted families MUST be kept
KEEP_CASES = [
    "up",
    "fyralis_schema_version",
    "fyralis_oauth_token_refresh_failures_total",
    "fyralis_llm_breaker_state",
    "fyralis_dlq_unresolved",
    "fyralis_think_queue_pending",
    "fyralis_embedding_backlog_pending",
    "fyralis_db_pool_in_use",
    "worker_heartbeat_age_seconds",
    "kafka_consumergroup_lag_sum",
    "webhook_verification_failures_total",
]
for mname in KEEP_CASES:
    check(f"allowlisted family '{mname}' is KEPT", kept(mname))

# non-allowlisted, high-card families MUST be dropped
DROP_CASES = [
    "signal_processing_latency_seconds",
    "per_user_action_total",
    "raw_payload_bytes",
    "observation_detail_gauge",
]
for mname in DROP_CASES:
    check(f"non-allowlisted family '{mname}' is DROPPED", not kept(mname))

# the golden G1–G7 fleet families are all represented in the allowlist text
for fam in [
    "fyralis_schema_version",   # G1
    "fyralis_oauth_token",      # G2
    "fyralis_llm_breaker",      # G3
    "fyralis_worker_expected_present",  # G5
    "fyralis_dlq_unresolved",
    "fyralis_think_queue_pending",
    "fyralis_embedding_backlog_pending",
]:
    check(f"allowlist references fleet family '{fam}'", fam in allow_text)

# ---------------------------------------------------------------------------
# Optional: real otelcol validate if the binary is available
# ---------------------------------------------------------------------------
OTEL_IMAGE = "otel/opentelemetry-collector-contrib:0.103.1"
VALIDATE_ENV = {
    "FYRALIS_TENANT_ID": "selftest",
    "FYRALIS_DEPLOYMENT_ID": "selftest-0000",
    "FYRALIS_REGION": "us-east-1",
    "FYRALIS_TELEMETRY_TIER": "T1",
    "FYRALIS_AUTH_PROXY_URL": "https://auth-proxy.local:8443",
    "FYRALIS_AUTH_PROXY_GRPC": "auth-proxy.local:4317",
}

otelcol = shutil.which("otelcol-contrib") or shutil.which("otelcol")
docker = shutil.which("docker")
if otelcol:
    env = {**os.environ, **VALIDATE_ENV}
    proc = subprocess.run(
        [otelcol, "validate", "--config", CFG],
        capture_output=True, text=True, env=env,
    )
    check("otelcol validate (native binary)", proc.returncode == 0, proc.stderr.strip()[:400])
elif docker:
    # Validate the T1 config against the REAL collector schema via docker.
    docker_env = []
    for k, v in VALIDATE_ENV.items():
        docker_env += ["-e", f"{k}={v}"]
    proc = subprocess.run(
        ["docker", "run", "--rm", *docker_env,
         "-v", f"{CFG}:/etc/otelcol/config.yaml:ro",
         OTEL_IMAGE, "validate", "--config=/etc/otelcol/config.yaml"],
        capture_output=True, text=True, timeout=180,
    )
    check("otelcol-contrib validate (docker, T1)", proc.returncode == 0,
          (proc.stderr or proc.stdout).strip()[:400])
else:
    results.append(("SKIP", "otelcol validate (no native binary / docker)", ""))

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
width = max(len(n) for _, n, _ in results)
failed = 0
for status, name, detail in results:
    line = f"[{status:4}] {name.ljust(width)}"
    if detail and status == FAIL:
        line += f"  -> {detail}"
    print(line)
    if status == FAIL:
        failed += 1

total = sum(1 for s, _, _ in results if s in (PASS, FAIL))
print("-" * (width + 12))
print(f"{total - failed}/{total} checks passed; {failed} failed")
sys.exit(1 if failed else 0)
