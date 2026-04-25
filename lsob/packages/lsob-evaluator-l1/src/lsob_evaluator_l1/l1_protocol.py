"""Layer-1-local Protocol for retrieval-capable SUTs.

We deliberately do NOT extend the shared `SystemUnderTest` protocol in
`lsob_contracts`. Not every SUT exposes its retrieval subcomponent; Layer 1
simply reports `layer_not_applicable` when the SUT lacks this surface.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RetrievalCapableSUT(Protocol):
    """Optional extra surface a SUT may expose so Layer 1 can probe it."""

    async def retrieval_semantic(self, query: str, k: int) -> list[str]:
        """Return up to `k` candidate item ids ranked by semantic relevance."""
        ...

    async def retrieval_entity_resolve(
        self, phrase: str, author_id: str
    ) -> str | None:
        """Resolve an ambiguous phrase to a canonical entity id, or None."""
        ...

    async def retrieval_rerank(
        self, items: list[str], query: str
    ) -> list[str]:
        """Return `items` reordered by relevance to `query`."""
        ...
