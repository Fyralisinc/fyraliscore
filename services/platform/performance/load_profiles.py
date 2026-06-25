"""Executable launch-size load profiles for Fyralis production readiness."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Literal


ProfileName = Literal["beta", "ga"]

MB = 1024 * 1024
GB = 1024 * 1024 * 1024

BETA_SOURCE_MIX = (
    "slack",
    "github",
    "gmail",
    "google_calendar",
    "notion",
    "jira",
)
GA_SOURCE_MIX = (
    "slack",
    "github",
    "gmail",
    "google_calendar",
    "google_drive",
    "notion",
    "jira",
    "quickbooks",
    "ramp",
    "mercury",
    "aws",
    "figma",
)


@dataclass(frozen=True, slots=True)
class TenantLoadProfile:
    name: ProfileName
    users: int
    enabled_sources: int
    active_source_installs: int
    historical_backfill_days: int
    historical_observations: int
    new_observations_per_day: int
    peak_webhook_events_per_minute: int
    largest_blob_bytes: int
    daily_blob_ingest_bytes: int
    active_models: int
    relationship_edges: int
    think_triggers_per_day: int
    ask_requests_per_day: int
    today_requests_per_day: int
    recommendation_actions_per_day: int
    source_lifecycle_actions_per_day: int
    think_llm_spend_budget_usd_per_day: float
    think_llm_token_budget_per_day: int
    think_llm_request_budget_per_day: int
    embedding_spend_budget_usd_per_day: float
    object_storage_growth_gb_per_month: int
    postgres_storage_growth_gb_per_month: int
    source_mix: tuple[str, ...]


PROFILES: dict[ProfileName, TenantLoadProfile] = {
    "beta": TenantLoadProfile(
        name="beta",
        users=50,
        enabled_sources=6,
        active_source_installs=10,
        historical_backfill_days=180,
        historical_observations=250_000,
        new_observations_per_day=25_000,
        peak_webhook_events_per_minute=500,
        largest_blob_bytes=25 * MB,
        daily_blob_ingest_bytes=10 * GB,
        active_models=50_000,
        relationship_edges=500_000,
        think_triggers_per_day=5_000,
        ask_requests_per_day=1_000,
        today_requests_per_day=2_500,
        recommendation_actions_per_day=250,
        source_lifecycle_actions_per_day=10,
        think_llm_spend_budget_usd_per_day=25.0,
        think_llm_token_budget_per_day=5_000_000,
        think_llm_request_budget_per_day=5_000,
        embedding_spend_budget_usd_per_day=10.0,
        object_storage_growth_gb_per_month=300,
        postgres_storage_growth_gb_per_month=50,
        source_mix=BETA_SOURCE_MIX,
    ),
    "ga": TenantLoadProfile(
        name="ga",
        users=250,
        enabled_sources=12,
        active_source_installs=30,
        historical_backfill_days=365,
        historical_observations=2_000_000,
        new_observations_per_day=150_000,
        peak_webhook_events_per_minute=2_500,
        largest_blob_bytes=100 * MB,
        daily_blob_ingest_bytes=100 * GB,
        active_models=500_000,
        relationship_edges=5_000_000,
        think_triggers_per_day=50_000,
        ask_requests_per_day=10_000,
        today_requests_per_day=25_000,
        recommendation_actions_per_day=2_500,
        source_lifecycle_actions_per_day=100,
        think_llm_spend_budget_usd_per_day=100.0,
        think_llm_token_budget_per_day=25_000_000,
        think_llm_request_budget_per_day=50_000,
        embedding_spend_budget_usd_per_day=50.0,
        object_storage_growth_gb_per_month=3_000,
        postgres_storage_growth_gb_per_month=500,
        source_mix=GA_SOURCE_MIX,
    ),
}


def _scaled_count(value: int, scale: float) -> int:
    return max(1, int(math.ceil(value * scale)))


def _rate_per_second(value_per_day: int, scale: float) -> float:
    return round((value_per_day * scale) / 86_400, 6)


def _qps_from_events_per_minute(value: int, scale: float) -> int:
    return max(1, int(math.ceil((value * scale) / 60)))


def build_load_plan(
    profile_name: ProfileName,
    *,
    scale: float = 1.0,
    duration_s: int = 3600,
) -> dict[str, object]:
    if scale <= 0:
        raise ValueError("scale must be positive")
    if duration_s <= 0:
        raise ValueError("duration_s must be positive")

    profile = PROFILES[profile_name]
    peak_webhook_qps = _qps_from_events_per_minute(
        profile.peak_webhook_events_per_minute,
        scale,
    )
    webhook_providers = ("slack", "github")
    webhook_weights = {"slack": 0.75, "github": 0.25}

    return {
        "profile": profile.name,
        "scale": scale,
        "duration_s": duration_s,
        "tenant_profile": {
            **asdict(profile),
            "largest_blob_mb": round(profile.largest_blob_bytes / MB, 3),
            "daily_blob_ingest_gb": round(profile.daily_blob_ingest_bytes / GB, 3),
        },
        "synthetic_dataset": {
            "users": _scaled_count(profile.users, scale),
            "enabled_sources": profile.source_mix[: profile.enabled_sources],
            "active_source_installs": _scaled_count(
                profile.active_source_installs,
                scale,
            ),
            "historical_backfill_days": profile.historical_backfill_days,
            "historical_observations": _scaled_count(
                profile.historical_observations,
                scale,
            ),
            "new_observations_per_day": _scaled_count(
                profile.new_observations_per_day,
                scale,
            ),
            "active_models": _scaled_count(profile.active_models, scale),
            "relationship_edges": _scaled_count(profile.relationship_edges, scale),
            "largest_blob_bytes": profile.largest_blob_bytes,
            "daily_blob_ingest_bytes": _scaled_count(
                profile.daily_blob_ingest_bytes,
                scale,
            ),
            "daily_blob_count_at_largest_size": _scaled_count(
                math.ceil(profile.daily_blob_ingest_bytes / profile.largest_blob_bytes),
                scale,
            ),
        },
        "load_rates": {
            "new_observation_avg_qps": _rate_per_second(
                profile.new_observations_per_day,
                scale,
            ),
            "peak_webhook_qps": peak_webhook_qps,
            "think_trigger_avg_qps": _rate_per_second(
                profile.think_triggers_per_day,
                scale,
            ),
            "ask_avg_qps": _rate_per_second(profile.ask_requests_per_day, scale),
            "today_avg_qps": _rate_per_second(profile.today_requests_per_day, scale),
            "recommendation_action_avg_qps": _rate_per_second(
                profile.recommendation_actions_per_day,
                scale,
            ),
        },
        "generators": {
            "m_load_cutover": {
                "target": "services.ingest.synthetic.cutover_load",
                "qps": peak_webhook_qps,
                "duration_s": duration_s,
                "tenant_count": 1,
                "providers": webhook_providers,
                "provider_weights": webhook_weights,
                "expected_events": peak_webhook_qps * duration_s,
            },
            "product_reads": {
                "ask_requests": _scaled_count(profile.ask_requests_per_day, scale),
                "today_requests": _scaled_count(profile.today_requests_per_day, scale),
                "recommendation_actions": _scaled_count(
                    profile.recommendation_actions_per_day,
                    scale,
                ),
            },
            "think": {
                "triggers": _scaled_count(profile.think_triggers_per_day, scale),
            },
        },
        "cost_budgets": {
            "think_llm_spend_usd_per_day": round(
                profile.think_llm_spend_budget_usd_per_day * scale,
                4,
            ),
            "think_llm_tokens_per_day": _scaled_count(
                profile.think_llm_token_budget_per_day,
                scale,
            ),
            "think_llm_requests_per_day": _scaled_count(
                profile.think_llm_request_budget_per_day,
                scale,
            ),
            "embedding_spend_usd_per_day": round(
                profile.embedding_spend_budget_usd_per_day * scale,
                4,
            ),
            "object_storage_growth_gb_per_month": _scaled_count(
                profile.object_storage_growth_gb_per_month,
                scale,
            ),
            "postgres_storage_growth_gb_per_month": _scaled_count(
                profile.postgres_storage_growth_gb_per_month,
                scale,
            ),
        },
    }


def cutover_env(plan: dict[str, object]) -> dict[str, str]:
    generators = plan["generators"]
    assert isinstance(generators, dict)
    m_load = generators["m_load_cutover"]
    assert isinstance(m_load, dict)
    weights = m_load["provider_weights"]
    assert isinstance(weights, dict)
    return {
        "CUTOVER_DRYRUN_QPS": str(m_load["qps"]),
        "CUTOVER_DRYRUN_DURATION_S": str(m_load["duration_s"]),
        "CUTOVER_DRYRUN_TENANTS": str(m_load["tenant_count"]),
        "CUTOVER_DRYRUN_PROVIDER_WEIGHTS": ",".join(
            f"{key}={value}" for key, value in weights.items()
        ),
    }


__all__ = [
    "PROFILES",
    "ProfileName",
    "TenantLoadProfile",
    "build_load_plan",
    "cutover_env",
]
