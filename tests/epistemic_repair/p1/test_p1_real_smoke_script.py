from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "run_epistemic_repair_p1_real_smoke.py"


def test_real_smoke_is_bounded_batch_only_and_secret_safe():
    source = SCRIPT.read_text()

    assert '"batch_count": 1' in source
    assert '"signal_count": len(batch)' in source
    assert '"individual_signal_calls": 0' in source
    assert "max_attempts=3" in source
    assert "deadline_s=240" in source
    assert "max_tokens=500" in source
    assert "api_key" not in source
    assert "DEEPSEEK_API_KEY" not in source
    assert "OPENAI_API_KEY" not in source


def test_real_smoke_persists_failure_receipts_in_report_contract():
    source = SCRIPT.read_text()

    assert "attempt_history" in source
    assert "logical_outcome" in source
    assert "attempt_outcomes" in source
    assert "usage_exactness" in source
    assert "context_digest_present" in source
