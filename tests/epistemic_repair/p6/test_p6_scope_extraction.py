from __future__ import annotations

import inspect

from lib.evaluation.epistemic_repair import p6_postfreeze_evidence


def test_p6_scope_extraction_prefers_durable_canonical_provenance() -> None:
    source = inspect.getsource(
        p6_postfreeze_evidence.extract_p6_postfreeze_evidence
    )

    assert "COALESCE(binding.canonical_ref" in source
    assert "'display_label',binding.display_label" in source
    assert "'canonical_ref_status',binding.canonical_ref_status" in source
    assert "LEFT JOIN resources resource" in source


def test_p6_scope_extraction_does_not_promote_provisional_coordinates() -> None:
    source = inspect.getsource(
        p6_postfreeze_evidence.extract_p6_postfreeze_evidence
    )

    assert 'resolved_scope_statuses = {"resolved", "accepted"}' in source
    assert '"provisional": len(provisional_scope_coordinates)' in source
    assert "len(resolved_scope_coordinates) == len(scope_coordinates)" in source
