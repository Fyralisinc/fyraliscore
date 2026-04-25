"""services/workers/entity_resolver — deferred entity resolution worker.

See ARCHITECTURE §15 for the spec and BUILD-PLAN §3 Prompt 2.B for
the surface Agent 2-B implements.
"""
from services.workers.entity_resolver.worker import (  # noqa: F401
    EntityResolution,
    EntityResolverWorker,
    ResolverDecision,
    ResolverLLMBudget,
)

__all__ = [
    "EntityResolution",
    "EntityResolverWorker",
    "ResolverDecision",
    "ResolverLLMBudget",
]
