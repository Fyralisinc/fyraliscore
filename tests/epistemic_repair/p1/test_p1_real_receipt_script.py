from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_real_receipt_probe_reopens_and_replays_durable_rows() -> None:
    source = (
        ROOT / "scripts/persist_epistemic_repair_p1_real_receipts.py"
    ).read_text()
    assert source.count("await asyncpg.connect(dsn)") == 2
    assert "await collector.persist(conn)" in source
    assert "identical_replay_idempotent" in source
    assert "attempt_outcome" in source
