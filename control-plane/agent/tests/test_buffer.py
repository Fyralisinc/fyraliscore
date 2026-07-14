"""HeartbeatBuffer: durability, ordering, bounding, flush semantics (I3)."""

from __future__ import annotations

from buffer import HeartbeatBuffer


def _rec(i: int) -> dict:
    return {"deployment_id": f"d{i}", "n": i}


def test_append_and_count(tmp_path):
    buf = HeartbeatBuffer(tmp_path / "buf.jsonl")
    assert buf.is_empty()
    buf.append(_rec(1))
    buf.append(_rec(2))
    assert buf.count() == 2
    assert [r["n"] for r in buf.read_all()] == [1, 2]  # oldest-first


def test_survives_reopen(tmp_path):
    p = tmp_path / "buf.jsonl"
    HeartbeatBuffer(p).append(_rec(1))
    # A fresh instance (simulating an agent restart) sees the parked record.
    assert HeartbeatBuffer(p).count() == 1


def test_bounded_drops_oldest(tmp_path):
    buf = HeartbeatBuffer(tmp_path / "buf.jsonl", max_records=3)
    evicted_any = False
    for i in range(6):
        evicted_any |= buf.append(_rec(i))
    assert evicted_any
    assert buf.count() == 3
    # The newest 3 survive (0,1,2 dropped).
    assert [r["n"] for r in buf.read_all()] == [3, 4, 5]


def test_flush_delivers_all_when_sender_ok(tmp_path):
    buf = HeartbeatBuffer(tmp_path / "buf.jsonl")
    for i in range(3):
        buf.append(_rec(i))
    seen = []
    delivered, remaining = buf.flush(lambda r: (seen.append(r["n"]) or True))
    assert delivered == 3 and remaining == 0
    assert seen == [0, 1, 2]  # oldest-first
    assert buf.is_empty()


def test_flush_stops_on_first_failure_and_preserves_order(tmp_path):
    buf = HeartbeatBuffer(tmp_path / "buf.jsonl")
    for i in range(4):
        buf.append(_rec(i))

    # Deliver first 2, then fail.
    calls = {"n": 0}

    def sender(_r):
        calls["n"] += 1
        return calls["n"] <= 2

    delivered, remaining = buf.flush(sender)
    assert delivered == 2 and remaining == 2
    # The undelivered tail is retained, in order.
    assert [r["n"] for r in buf.read_all()] == [2, 3]


def test_flush_treats_sender_exception_as_failure(tmp_path):
    buf = HeartbeatBuffer(tmp_path / "buf.jsonl")
    buf.append(_rec(0))

    def boom(_r):
        raise RuntimeError("network down")

    delivered, remaining = buf.flush(boom)
    assert delivered == 0 and remaining == 1
    assert buf.count() == 1  # retained, not lost


def test_corrupt_line_is_skipped(tmp_path):
    p = tmp_path / "buf.jsonl"
    p.write_text('{"n":1}\nnot-json\n{"n":2}\n', encoding="utf-8")
    buf = HeartbeatBuffer(p)
    assert [r["n"] for r in buf.read_all()] == [1, 2]
