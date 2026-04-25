"""In-package deterministic mock SUT so Layer 1 tests are hermetic.

The mock derives its answers from the corpus it was initialized with. This is
*not* a baseline — it exists only so integration tests have something that
implements `RetrievalCapableSUT` without pulling in the full baselines package.
"""

from __future__ import annotations

import re

from lsob_contracts import Corpus


class MockRetrievalSUT:
    """Canned, deterministic retrieval subcomponent derived from the corpus."""

    name: str = "mock-retrieval-sut"
    max_concurrent_ingestion: int = 1

    def __init__(self, corpus: Corpus) -> None:
        self._corpus = corpus
        self._commitment_ids: list[str] = []
        self._customer_ids: list[str] = []
        self._owners: set[str] = set()
        for gt in corpus.ground_truth:
            for c in gt.commitments:
                if c["id"] not in self._commitment_ids:
                    self._commitment_ids.append(c["id"])
                if c.get("owner"):
                    self._owners.add(c["owner"])
            for cust in gt.customers:
                if cust["id"] not in self._customer_ids:
                    self._customer_ids.append(cust["id"])

    async def retrieval_semantic(self, query: str, k: int) -> list[str]:
        """Return commitment/customer/owner models mentioned in `query`.

        Items whose id appears literally in the query come first; the rest of
        the universe follows in a stable order. This means a well-formed probe
        like "what do we know about commitment C-ingest" puts
        `model:commitment:C-ingest` at rank 1.
        """
        q = query.lower()
        universe: list[str] = []
        for cid in self._commitment_ids:
            universe.append(f"model:commitment:{cid}")
        for owner in sorted(self._owners):
            universe.append(f"model:owner:{owner}")
        for cust in self._customer_ids:
            universe.append(f"model:customer:{cust}")

        def _score(item: str) -> tuple[int, int]:
            # Lower tuple sorts first. (0, idx) for mentioned items.
            tail = item.split(":")[-1].lower()
            mentioned = 0 if tail in q else 1
            return (mentioned, universe.index(item))

        ranked = sorted(universe, key=_score)
        return ranked[:k]

    async def retrieval_entity_resolve(
        self, phrase: str, author_id: str
    ) -> str | None:
        """Exact-substring match against known commitment/customer ids."""
        phrase_l = phrase.lower()
        # Commitments look like "C-ingest" — search by that.
        for cid in self._commitment_ids:
            if re.search(rf"\b{re.escape(cid)}\b", phrase, flags=re.IGNORECASE):
                return cid
        for cust in self._customer_ids:
            if cust.lower() in phrase_l:
                return cust
        return None

    async def retrieval_rerank(
        self, items: list[str], query: str
    ) -> list[str]:
        """Sort by (kind-priority, natural order) — commitments before customers."""

        def _priority(item: str) -> tuple[int, int]:
            parts = item.split(":")
            kind = parts[1] if len(parts) >= 2 else ""
            kind_rank = {"commitment": 0, "customer": 1, "owner": 2}.get(
                kind, 3
            )
            try:
                idx_in_input = items.index(item)
            except ValueError:
                idx_in_input = len(items)
            return (kind_rank, idx_in_input)

        return sorted(items, key=_priority)


class MockNonRetrievalSUT:
    """SUT that deliberately does NOT implement RetrievalCapableSUT."""

    name: str = "mock-non-retrieval-sut"
    max_concurrent_ingestion: int = 1

    async def startup(self, *args, **kwargs) -> None:  # pragma: no cover
        pass

    async def shutdown(self) -> None:  # pragma: no cover
        pass
