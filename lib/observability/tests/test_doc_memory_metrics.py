"""Phase-2 observability coverage for the document-memory substrate.

Covers (docs/plans/document-memory-substrate.md §7 step 12 / §10):
  (i)   the ``doc_memory_*`` metric families render with the right names,
        label sets, and values on the default registry;
  (ii)  the worker wiring — the renamed DISPATCH counter
        (``doc_memory_enriched_t1_total``) increments at enriched-T1 dispatch,
        and the TRUE mint counter (``doc_memory_models_minted_total``)
        increments at the real Think-mint site via the dedicated helper;
  (iii) the source-label cardinality collapse (bounded enum).

The DocMemoryMintFailure alert YAML and the deadline_resolver compose service
are covered by sibling tests (test_doc_memory_alert.py / test_doc_memory_compose
.py). Everything here runs with plain python (no DB, no Kafka).
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.observability.metrics import (
    DOC_MEMORY_ENRICHED_T1,
    DOC_MEMORY_MAPREDUCE_SECTIONS,
    DOC_MEMORY_MINT_FAILURE,
    DOC_MEMORY_MODELS_MINTED,
    DOC_MEMORY_SCOPE_UNRESOLVED,
    doc_memory_source_label,
    record_doc_memory_model_minted,
    render_default,
    reset_default_for_tests,
)
from services.ingest.ingestion.writers.summarization_worker.doc_memory import (
    DocMemoryScope,
)
from services.ingest.ingestion.writers.summarization_worker.summarization_worker import (
    _enrich_t1_payload,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_default_for_tests()
    yield
    reset_default_for_tests()


def _metric_lines(name: str) -> list[str]:
    return [
        ln
        for ln in render_default().splitlines()
        if ln.startswith(name + "{") or ln == name or ln.startswith(name + " ")
    ]


# --- (i) families render with the right names / labels / values ------------


def test_doc_memory_metric_family_names_and_types():
    record_doc_memory_model_minted("fireflies:transcript")
    DOC_MEMORY_ENRICHED_T1.inc(source="fireflies")
    DOC_MEMORY_SCOPE_UNRESOLVED.inc(source="notion")
    DOC_MEMORY_MINT_FAILURE.inc(source="other")
    DOC_MEMORY_MAPREDUCE_SECTIONS.observe(3)
    out = render_default()

    # Counters carry the `_total` suffix; histogram does not.
    assert "# TYPE doc_memory_enriched_t1_total counter" in out
    assert "# TYPE doc_memory_models_minted_total counter" in out
    assert "# TYPE doc_memory_scope_unresolved_total counter" in out
    assert "# TYPE doc_memory_mint_failure_total counter" in out
    assert "# TYPE doc_memory_mapreduce_sections histogram" in out
    # The OLD misleading name must NOT appear as a *dispatch* counter anymore;
    # models_minted is the TRUE mint counter and enriched_t1 is the dispatch one.
    assert "doc_memory_enriched_t1_total" in out


def test_counters_render_with_source_label_and_value():
    record_doc_memory_model_minted("google_drive:file:abc")
    record_doc_memory_model_minted("google_drive:file:def")
    lines = _metric_lines("doc_memory_models_minted_total")
    assert 'doc_memory_models_minted_total{source="google_drive"} 2' in lines


def test_histogram_renders_buckets_and_count():
    for n in (1, 2, 3):
        DOC_MEMORY_MAPREDUCE_SECTIONS.observe(n)
    out = render_default()
    assert "doc_memory_mapreduce_sections_count 3" in out
    assert 'doc_memory_mapreduce_sections_bucket{le="+Inf"} 3' in out


# --- (ii) worker wiring: dispatch vs true mint -----------------------------


def test_enriched_t1_counter_increments_at_dispatch_not_mint():
    """`_enrich_t1_payload` is the worker DISPATCH site (Option A): it bumps
    enriched_t1, and must NOT bump the true mint counter."""
    scope = DocMemoryScope(
        scope_entities=[{"type": "customer", "id": str(uuid4())}],
        scope_actors=[str(uuid4())],
    )
    payload: dict = {"scope_actors": []}
    _enrich_t1_payload(
        payload,
        scope,
        {"summary": "doc"},
        source_channel="fireflies:transcript",
    )
    enriched = _metric_lines("doc_memory_enriched_t1_total")
    assert 'doc_memory_enriched_t1_total{source="fireflies"} 1' in enriched
    # Dispatch must NOT be counted as a mint.
    assert _metric_lines("doc_memory_models_minted_total") == []


def test_enriched_t1_dispatch_with_empty_scope_bumps_unresolved():
    scope = DocMemoryScope()  # no entities, no actors
    payload: dict = {}
    _enrich_t1_payload(payload, scope, {"summary": "d"}, source_channel="notion:object")
    assert (
        'doc_memory_enriched_t1_total{source="notion"} 1'
        in _metric_lines("doc_memory_enriched_t1_total")
    )
    assert (
        'doc_memory_scope_unresolved_total{source="notion"} 1'
        in _metric_lines("doc_memory_scope_unresolved_total")
    )


def test_true_mint_counter_increments_at_mint_site_helper():
    """The TRUE mint site (Think apply path) records via the dedicated helper —
    once per document-derived Model actually inserted."""
    for _ in range(3):  # e.g. a situation anchor + a prediction + a concern
        record_doc_memory_model_minted("fireflies:transcript")
    assert (
        'doc_memory_models_minted_total{source="fireflies"} 3'
        in _metric_lines("doc_memory_models_minted_total")
    )
    # Minting must NOT touch the dispatch counter.
    assert _metric_lines("doc_memory_enriched_t1_total") == []


# --- (iii) source-label cardinality collapse -------------------------------


def test_source_label_collapses_to_bounded_enum():
    assert doc_memory_source_label("google_drive:file") == "google_drive"
    assert doc_memory_source_label("notion:object") == "notion"
    assert doc_memory_source_label("fireflies:transcript") == "fireflies"
    assert doc_memory_source_label("slack:message") == "other"
    assert doc_memory_source_label(None) == "other"
    assert doc_memory_source_label("") == "other"
