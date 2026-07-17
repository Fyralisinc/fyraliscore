from dataclasses import replace

import pytest

from lib.evaluation.epistemic_repair.p8_evidence import bind_fault_execution_evidence
from lib.evaluation.epistemic_repair.p8_postgres_runner import (
    DurableFaultReceipt,
    P8_DB_COVERED_BOUNDARIES,
    P8_DB_UNCOVERED_BOUNDARIES,
    PostgresFaultSlice,
)
from lib.evaluation.epistemic_repair.p8_provider_runner import (
    DurableProviderFaultReceipt,
    PROVIDER_BOUNDARIES,
    ProviderFaultSlice,
)


def _slices():
    db_rows = tuple(
        DurableFaultReceipt(boundary, duplicate, "tenant", f"batch:{boundary}", "fault",
                            1, 1, 0, "a" * 64, "b" * 64)
        for boundary in P8_DB_COVERED_BOUNDARIES for duplicate in (False, True)
    )
    provider_rows = tuple(
        DurableProviderFaultReceipt(boundary, duplicate, "tenant", f"call:{boundary}",
                                    f"attempt:{boundary}", "timeout", 1, 1, 1, 1, "c" * 64)
        for boundary in PROVIDER_BOUNDARIES for duplicate in (False, True)
    )
    return (
        PostgresFaultSlice("db-run", P8_DB_COVERED_BOUNDARIES, P8_DB_UNCOVERED_BOUNDARIES, db_rows, "d" * 64, False),
        ProviderFaultSlice("provider-run", "codex-cli", "gpt-5.4", provider_rows, "e" * 64),
    )


def test_binder_requires_all_24_schedule_executions() -> None:
    postgres, provider = _slices()
    evidence = bind_fault_execution_evidence(postgres=postgres, provider=provider, commit_sha="f" * 40)
    assert len(evidence.fault_execution_keys) == 24
    assert evidence.attempt_receipts_persisted is True
    with pytest.raises(ValueError, match="denominator complete"):
        bind_fault_execution_evidence(
            postgres=postgres,
            provider=replace(provider, receipts=provider.receipts[:-1]),
            commit_sha="f" * 40,
        )


def test_binder_rejects_unbound_post_restart_state() -> None:
    postgres, provider = _slices()
    broken = replace(postgres.receipts[0], post_restart_pending_count=1)
    with pytest.raises(ValueError, match="durable-state binding"):
        bind_fault_execution_evidence(
            postgres=replace(postgres, receipts=(broken, *postgres.receipts[1:])),
            provider=provider,
            commit_sha="f" * 40,
        )
