"""Registered per-tenant and expensive-worker concurrency controls."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ControlKind = Literal["per_tenant_concurrency", "per_tenant_budget", "batch_limit"]


@dataclass(frozen=True, slots=True)
class ConcurrencyControl:
    name: str
    process: str
    env_key: str
    default_value: str
    kind: ControlKind
    scope: str
    enforcement: str


CONCURRENCY_CONTROLS: tuple[ConcurrencyControl, ...] = (
    ConcurrencyControl(
        name="think_worker_per_tenant",
        process="think_worker",
        env_key="THINK_MAX_CONCURRENCY_PER_TENANT",
        default_value="1",
        kind="per_tenant_concurrency",
        scope="tenant_id",
        enforcement="asyncio.Semaphore keyed by tenant_id",
    ),
    ConcurrencyControl(
        name="anomaly_processor_t3_budget",
        process="anomaly_processor_worker",
        env_key="ANOMALY_T3_BUDGET_PER_MIN",
        default_value="20",
        kind="per_tenant_budget",
        scope="tenant_id",
        enforcement="TenantRateLimiter before T3 Think enqueue",
    ),
    ConcurrencyControl(
        name="entity_resolver_llm_budget",
        process="entity_resolver_worker",
        env_key="ENTITY_RESOLVER_LLM_BUDGET_PER_MIN",
        default_value="30",
        kind="per_tenant_budget",
        scope="tenant_id",
        enforcement="ResolverLLMBudget token bucket before LLM call",
    ),
    ConcurrencyControl(
        name="topology_sweeper_limit",
        process="topology_sweeper_worker",
        env_key="TOPOLOGY_SWEEPER_LIMIT_PER_TENANT",
        default_value="50",
        kind="batch_limit",
        scope="tenant_id",
        enforcement="relationship-field sweep limit per tenant per cycle",
    ),
    ConcurrencyControl(
        name="relationship_ontology_proposals_limit",
        process="relationship_ontology_proposals_worker",
        env_key="RELATIONSHIP_ONTOLOGY_PROPOSALS_LIMIT_PER_TENANT",
        default_value="500",
        kind="batch_limit",
        scope="tenant_id",
        enforcement="proposal aggregation limit per tenant per cycle",
    ),
    ConcurrencyControl(
        name="sage_topology_optimizer_limit",
        process="sage_topology_optimizer_worker",
        env_key="SAGE_TOPOLOGY_OPTIMIZER_LIMIT",
        default_value="50",
        kind="batch_limit",
        scope="tenant_id",
        enforcement="bounded candidate optimization batch",
    ),
)

EXPENSIVE_WORKER_GATES: dict[str, str] = {
    "HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS": "0",
}


__all__ = [
    "CONCURRENCY_CONTROLS",
    "EXPENSIVE_WORKER_GATES",
    "ConcurrencyControl",
]
