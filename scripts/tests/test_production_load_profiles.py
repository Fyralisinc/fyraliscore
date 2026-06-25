from __future__ import annotations

import json
import subprocess
import sys

from services.platform.performance.load_profiles import build_load_plan, cutover_env


def test_beta_load_plan_matches_documented_launch_targets() -> None:
    plan = build_load_plan("beta")

    assert plan["profile"] == "beta"
    dataset = plan["synthetic_dataset"]
    assert dataset["users"] == 50
    assert dataset["historical_observations"] == 250_000
    assert dataset["active_models"] == 50_000
    assert dataset["relationship_edges"] == 500_000
    assert dataset["enabled_sources"] == (
        "slack",
        "github",
        "gmail",
        "google_calendar",
        "notion",
        "jira",
    )
    rates = plan["load_rates"]
    assert rates["peak_webhook_qps"] == 9
    assert rates["new_observation_avg_qps"] == 0.289352


def test_ga_scaled_load_plan_preserves_shape_with_smoke_counts() -> None:
    plan = build_load_plan("ga", scale=0.01, duration_s=300)

    dataset = plan["synthetic_dataset"]
    assert dataset["users"] == 3
    assert dataset["historical_observations"] == 20_000
    assert dataset["active_models"] == 5_000
    assert len(dataset["enabled_sources"]) == 12
    m_load = plan["generators"]["m_load_cutover"]
    assert m_load["qps"] == 1
    assert m_load["duration_s"] == 300
    assert m_load["provider_weights"] == {"slack": 0.75, "github": 0.25}


def test_cutover_env_renders_m_load_variables() -> None:
    plan = build_load_plan("beta", duration_s=600)

    env = cutover_env(plan)

    assert env == {
        "CUTOVER_DRYRUN_QPS": "9",
        "CUTOVER_DRYRUN_DURATION_S": "600",
        "CUTOVER_DRYRUN_TENANTS": "1",
        "CUTOVER_DRYRUN_PROVIDER_WEIGHTS": "slack=0.75,github=0.25",
    }


def test_plan_production_load_profile_cli_outputs_json() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/plan_production_load_profile.py",
            "beta",
            "--scale",
            "0.1",
            "--duration-s",
            "60",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)

    assert payload["profile"] == "beta"
    assert payload["duration_s"] == 60
