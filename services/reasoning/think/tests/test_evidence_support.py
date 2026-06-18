from __future__ import annotations

from uuid import uuid4

from services.reasoning.think.evidence_support import compact_supporting_event_ids


def _ids(n: int):
    return [uuid4() for _ in range(n)]


def test_compact_supporting_event_ids_dedupes_without_compaction() -> None:
    ids = _ids(5)

    compacted = compact_supporting_event_ids(ids[:3], ids[2:], max_ids=10)

    assert compacted.compacted is False
    assert compacted.event_ids == ids
    assert compacted.dropped_count == 0
    assert compacted.policy == "dedupe_only"


def test_compact_supporting_event_ids_keeps_anchors_middle_and_recent() -> None:
    ids = _ids(100)

    compacted = compact_supporting_event_ids(
        ids,
        max_ids=20,
        preserve_anchors=4,
        preserve_recent=8,
    )

    assert compacted.compacted is True
    assert len(compacted.event_ids) == 20
    assert compacted.event_ids[:4] == ids[:4]
    assert compacted.event_ids[-8:] == ids[-8:]
    middle = compacted.event_ids[4:-8]
    assert len(middle) == 8
    assert middle[0] in ids[4:20]
    assert middle[-1] in ids[80:92]
    assert compacted.dropped_count == 80


def test_compact_supporting_event_ids_handles_strings_invalids_and_duplicates() -> None:
    ids = _ids(12)
    raw = [str(ids[0]), "not-a-uuid", None, ids[1], str(ids[0]), *ids[2:]]

    compacted = compact_supporting_event_ids(
        raw,
        max_ids=6,
        preserve_anchors=2,
        preserve_recent=2,
    )

    assert compacted.total_seen == 12
    assert compacted.event_ids[:2] == ids[:2]
    assert compacted.event_ids[-2:] == ids[-2:]
    assert len(set(compacted.event_ids)) == len(compacted.event_ids)


def test_compact_supporting_event_ids_low_cap_still_retains_recent_context() -> None:
    ids = _ids(20)

    compacted = compact_supporting_event_ids(
        ids,
        max_ids=3,
        preserve_anchors=1,
        preserve_recent=1,
    )

    assert compacted.event_ids[0] == ids[0]
    assert compacted.event_ids[-1] == ids[-1]
    assert len(compacted.event_ids) == 3
