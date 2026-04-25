"""Deterministic helpers that derive query sets from a Corpus + ground truth.

These helpers are factored out so sub-evaluators can be unit-tested without
standing up a full `EvaluationContext`. Nothing here touches the SUT.
"""

from __future__ import annotations

from dataclasses import dataclass

from lsob_contracts import BeliefQuery, Corpus, EntityRef


@dataclass(frozen=True)
class SemanticProbe:
    """One deterministic probe derived from ground truth."""

    query: BeliefQuery
    query_text: str
    relevant_item_ids: list[str]  # gold-relevant model ids
    proposition_kind: str
    month: str  # YYYY-MM bucket for breakdown_by


@dataclass(frozen=True)
class EntityProbe:
    phrase: str
    author_id: str
    gold_entity_id: str | None  # None means "unresolvable"
    month: str


@dataclass(frozen=True)
class RerankerProbe:
    query_text: str
    candidates: list[str]  # shuffled deterministically
    gold_order: list[str]  # best → worst
    month: str


def _month_key(iso_timestamp: str) -> str:
    # Accepts ISO-8601 str; takes YYYY-MM.
    return iso_timestamp[:7]


def build_semantic_probes(corpus: Corpus) -> list[SemanticProbe]:
    """For every (commitment, checkpoint) pair emit one probe.

    Gold-relevant model ids are derived deterministically:
        model:commitment:<id>  (primary)
        model:owner:<owner>    (secondary, if present)
        model:customer:<id>    (for customer ground truth rows)
    The SUT is expected to hand back some of these ids. Deterministic because
    we walk ground truth in declared order and never sample.
    """
    probes: list[SemanticProbe] = []
    for gt in corpus.ground_truth:
        ts = gt.timestamp.isoformat()
        month = _month_key(ts)
        for c in gt.commitments:
            cid = c["id"]
            owner = c.get("owner")
            relevant: list[str] = [f"model:commitment:{cid}"]
            if owner:
                relevant.append(f"model:owner:{owner}")
            probes.append(
                SemanticProbe(
                    query=BeliefQuery(
                        query_id=f"q-commit-{cid}-{month}",
                        entity_ref=EntityRef(kind="commitment", id=cid),
                        timestamp=gt.timestamp,
                        proposition_kind="commitment_state",
                        k=20,
                    ),
                    query_text=f"what do we know about commitment {cid}",
                    relevant_item_ids=relevant,
                    proposition_kind="commitment_state",
                    month=month,
                )
            )
        for cust in gt.customers:
            custid = cust["id"]
            probes.append(
                SemanticProbe(
                    query=BeliefQuery(
                        query_id=f"q-cust-{custid}-{month}",
                        entity_ref=EntityRef(kind="customer", id=custid),
                        timestamp=gt.timestamp,
                        proposition_kind="customer_health",
                        k=20,
                    ),
                    query_text=f"what do we know about customer {custid}",
                    relevant_item_ids=[f"model:customer:{custid}"],
                    proposition_kind="customer_health",
                    month=month,
                )
            )
    return probes


def build_entity_probes(corpus: Corpus) -> list[EntityProbe]:
    """Derive phrase-resolution probes from signals that carry entity refs.

    A signal whose metadata has `commitment_ref` or `customer_ref` is assumed
    to have been authored with that entity in mind; its content_text is the
    ambiguous phrase, the metadata value is the gold resolution. Signals with
    no such metadata produce an unresolvable probe (gold = None) so we can
    exercise negative cases too.
    """
    probes: list[EntityProbe] = []
    for sig in corpus.signals:
        month = _month_key(sig.timestamp.isoformat())
        gold: str | None = sig.metadata.get(
            "commitment_ref"
        ) or sig.metadata.get("customer_ref")
        probes.append(
            EntityProbe(
                phrase=sig.content_text,
                author_id=sig.author_id,
                gold_entity_id=gold,
                month=month,
            )
        )
    return probes


def build_reranker_probes(corpus: Corpus) -> list[RerankerProbe]:
    """Per-checkpoint: gold order = [commitment_ids..., customer_ids...].

    We reverse the gold list to produce the candidate set input to the
    reranker — any correct reranker should recover the original order. This is
    deterministic without RNG.
    """
    probes: list[RerankerProbe] = []
    for gt in corpus.ground_truth:
        month = _month_key(gt.timestamp.isoformat())
        gold: list[str] = []
        for c in gt.commitments:
            gold.append(f"model:commitment:{c['id']}")
        for cust in gt.customers:
            gold.append(f"model:customer:{cust['id']}")
        if len(gold) < 2:
            continue
        candidates = list(reversed(gold))
        probes.append(
            RerankerProbe(
                query_text=f"rank items relevant to checkpoint {month}",
                candidates=candidates,
                gold_order=gold,
                month=month,
            )
        )
    return probes
